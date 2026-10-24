//! Harness for driving `DenoExecutor` through the `rt_sched::Executor` contract.
//!
//! A test builds a fixture exactly like the node does (`DefaultBuilder` + `V8CodeCacheProducer`),
//! turns the result into a `DenoPrepared` (the T1 value), and calls `Executor::start` with a
//! hand-made `ExecCx` (memory reservation from `TestGovernor`, system clock). Everything the
//! executor reaches for (`HostServices`) is real where it is simple (per-service KV on a temp dir,
//! in-memory queue, `MemoryLogSink`) and stubbed where the executor must not need it (control
//! plane env, service calls, platform bindings).
//!
//! The executor is `!Send` and its task futures are bound to a `LocalSet`: use [`run_local`].
#![allow(dead_code)]

#[path = "../../../rt-build/tests/common/support.rs"]
pub mod support;

pub use support::*;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use rt_build::{BuiltArtifact, Bundle, CodeCache, ModuleFetcher, Upload};
use rt_config::ClockMode;
use rt_exec_deno::{engine_fingerprint, DenoExecutor, DenoExecutorConfig, DenoPrepared, V8CodeCacheProducer};
use rt_host::{
    BindingError, CallError, Calls, EgressGuard, EnqueueOpts, HostDeps, HostServices, PlatformBindings, Request,
    Response, TaskCtx,
};
pub use rt_mem::MemoryGovernor;
use rt_mem::{MemCategory, Owner};
use rt_obs::{LogRecord, MemoryLogSink, Metrics, MetricsOptions};
pub use rt_sched::Executor;
use rt_sched::{ExecCx, SystemClock};
use rt_store::{DataConfig, LocalServiceData, ServiceDataStore, SqliteQueueStore};
use rt_testkit::{ids, SpecBuilder, TestGovernor};
use rt_types::{
    BuildReport, ConfigStream, CpError, EnvBundle, MachineId, MsgId, NodeEvent, Outcome, PreparedKey, ProcId,
    RouteSnapshot, RuntimeKind, SecretsVersion, Service, ServiceId, TaskId, TaskTimings, UsageBatch,
};
use zeroize::Zeroizing;

pub const MIB: u64 = 1024 * 1024;

/// Run `f` on a current-thread runtime inside a `LocalSet` (executor tasks are `!Send`).
pub fn run_local<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, f)
}

/// Executor config for tests: no isolate pool unless a test asks for one, frozen clock.
pub fn cfg0() -> DenoExecutorConfig {
    DenoExecutorConfig {
        isolate_pool_per_p: 0,
        ..DenoExecutorConfig::default()
    }
}

pub fn cfg_with_pool(n: u32) -> DenoExecutorConfig {
    DenoExecutorConfig {
        isolate_pool_per_p: n,
        ..DenoExecutorConfig::default()
    }
}

pub fn cfg_with_clock(clock: ClockMode) -> DenoExecutorConfig {
    DenoExecutorConfig { clock, ..cfg0() }
}

/// A Deno HTTP spec for service 1 / tenant 1; chain `.service(n)`, `.memory_mb(..)`, ...
pub fn spec() -> SpecBuilder {
    SpecBuilder::deno()
}

pub fn get(path: &str) -> rt_types::TaskSpec {
    spec().http_get(path).finish()
}

/// POST `path` with a JSON-ish body.
pub fn post_payload(path: &str, content_type: &str, body: &str) -> rt_types::Payload {
    let mut req = rt_types::HttpRequest::get(path);
    req.method = http::Method::POST;
    req.headers
        .insert("content-type", content_type.parse().expect("header value"));
    req.body = rt_types::Body::from_bytes(Bytes::from(body.to_string()));
    rt_types::Payload::Http(req)
}

// ---------------------------------------------------------------------------------------------
// Stubs behind HostServices
// ---------------------------------------------------------------------------------------------

/// Control plane stub: serves `env_for` from a map (counting the calls), ignores everything else.
#[derive(Default)]
pub struct StubControl {
    env: Mutex<HashMap<ServiceId, EnvBundle>>,
    env_calls: std::sync::atomic::AtomicUsize,
    /// `env_for` waits this long first (a cold control-plane read).
    pub env_delay: Mutex<Option<Duration>>,
    /// `env_for` waits this long on another thread first (a read that does not need the caller's
    /// runtime to make progress, which a synchronous `Deno.env` read can wait for).
    pub env_thread_delay: Mutex<Option<Duration>>,
    /// `env_for` fails.
    pub env_fails: std::sync::atomic::AtomicBool,
    /// CPU milliseconds reported through `usage` (a streamed tail's CPU).
    pub usage_cpu_ms: std::sync::atomic::AtomicU64,
}

impl StubControl {
    /// How many times a task fetched its env bundle.
    pub fn env_calls(&self) -> usize {
        self.env_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_env(&self, service: ServiceId, env: &[(&str, &str)], secrets: &[(&str, &str)]) {
        let bundle = EnvBundle {
            env: env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            secrets: secrets.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        self.env.lock().expect("lock").insert(service, bundle);
    }
}

#[async_trait]
impl rt_types::ControlPlane for StubControl {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }
    async fn subscribe_config(&self) -> ConfigStream {
        futures_util::stream::empty().boxed()
    }
    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        Ok(None)
    }
    async fn env_for(&self, service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        self.env_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let delay = *self.env_delay.lock().expect("lock");
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let thread_delay = *self.env_thread_delay.lock().expect("lock");
        if let Some(d) = thread_delay {
            let (tx, rx) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                std::thread::sleep(d);
                let _ = tx.send(());
            });
            let _ = rx.await;
        }
        if self.env_fails.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(CpError::Internal("env store down".into()));
        }
        let bundle = self
            .env
            .lock()
            .expect("lock")
            .get(&service)
            .cloned()
            .unwrap_or_default();
        Ok(Zeroizing::new(bundle))
    }
    async fn report_build(&self, _key: &PreparedKey, _report: BuildReport) -> Result<(), CpError> {
        Ok(())
    }
    fn usage(&self, batch: UsageBatch) {
        let cpu: u64 = batch.items.iter().map(|(_, _, d)| d.cpu_ms).sum();
        self.usage_cpu_ms.fetch_add(cpu, std::sync::atomic::Ordering::SeqCst);
    }
    fn event(&self, _ev: NodeEvent) {}
}

pub struct NoCalls;

#[async_trait]
impl Calls for NoCalls {
    async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
        Err(CallError::NotFound)
    }
    async fn enqueue(&self, _from: &TaskCtx, _slug: &str, _body: Bytes, _o: EnqueueOpts) -> Result<MsgId, CallError> {
        Err(CallError::NotFound)
    }
}

/// Service calls the mock answers: `call` answers 201 naming the slug and the request (`missing` ->
/// `not_found`, `deep` -> call depth exceeded); `enqueue` records the message (slug `missing` -> `not_found`,
/// `full` -> `queue_full`); `call_object` answers 202 with a body naming the object (`missing` ->
/// `not_found`, `deep` -> call depth exceeded, `empty` -> an empty body, `big` -> a body that fails
/// `TooLarge` after its first chunk, `broken` -> one that is aborted after its first chunk); the SDK
/// `acme` answers `echo` with its argument and every other method `method_not_found`.
#[derive(Default)]
pub struct MockCalls {
    pub enqueued: Mutex<Vec<(String, Vec<u8>, EnqueueOpts)>>,
    pub objects: Mutex<Vec<(Option<String>, String, String)>>,
}

#[async_trait]
impl Calls for MockCalls {
    async fn call(&self, _from: &TaskCtx, slug: &str, req: Request) -> Result<Response, CallError> {
        match slug {
            "missing" => Err(CallError::NotFound),
            "deep" => Err(CallError::Depth),
            _ => Ok(http::Response::builder()
                .status(201)
                .header("x-callee", slug)
                .body(rt_types::Body::from(format!(
                    "{slug} got {} {}",
                    req.method(),
                    req.uri()
                )))
                .expect("mock response")),
        }
    }

    async fn enqueue(&self, _from: &TaskCtx, slug: &str, body: Bytes, o: EnqueueOpts) -> Result<MsgId, CallError> {
        match slug {
            "missing" => return Err(CallError::NotFound),
            "full" => return Err(CallError::QueueFull),
            _ => {}
        }
        let mut seen = self.enqueued.lock().expect("lock");
        seen.push((slug.to_string(), body.to_vec(), o));
        Ok(MsgId::from_u128(seen.len() as u128))
    }

    async fn sdk_invoke(
        &self,
        _from: &TaskCtx,
        sdk: &str,
        method: &str,
        args_json: &str,
    ) -> Result<String, rt_host::RpcError> {
        match (sdk, method) {
            ("acme", "echo") => Ok(args_json.to_string()),
            ("acme", _) => Err(rt_host::RpcError::new(
                "method_not_found",
                format!("no method {method}"),
            )),
            _ => Err(rt_host::RpcError::new(
                "sdk_not_found",
                format!("there is no SDK `{sdk}`"),
            )),
        }
    }

    async fn call_object(
        &self,
        _from: &TaskCtx,
        slug: Option<&str>,
        object: &str,
        req: Request,
    ) -> Result<Response, CallError> {
        match object {
            "missing" => return Err(CallError::NotFound),
            "deep" => return Err(CallError::Depth),
            _ => {}
        }
        self.objects
            .lock()
            .expect("lock")
            .push((slug.map(str::to_string), object.to_string(), req.uri().to_string()));
        let body = match object {
            "empty" => rt_types::Body::empty(),
            // A body that breaks (not by size) after its first chunk.
            "broken" => rt_types::Body::from_stream(futures_util::stream::iter([
                Ok(Bytes::from_static(b"some")),
                Err(rt_types::BodyError::Aborted("reset".into())),
            ])),
            // A body that fails past the size the egress guard allows, after its first chunk.
            "big" => rt_types::Body::from_stream(futures_util::stream::iter([
                Ok(Bytes::from_static(b"some")),
                Err(rt_types::BodyError::TooLarge { limit: 4 }),
            ])),
            _ => rt_types::Body::from(format!("{object} says hi")),
        };
        Ok(http::Response::builder()
            .status(202)
            .header("x-object", object)
            .body(body)
            .expect("mock response"))
    }
}

/// Operator bindings that refuse by name: `denied`, `notallowed`, `limited`, `upstream`; any
/// other name is unknown.
pub struct ScriptedBindings;

#[async_trait]
impl PlatformBindings for ScriptedBindings {
    async fn fetch(&self, _from: &TaskCtx, binding: &str, _req: Request) -> Result<Response, BindingError> {
        Err(match binding {
            "denied" => BindingError::Denied,
            "notallowed" => BindingError::NotAllowed,
            "limited" => BindingError::RateLimited,
            "upstream" => BindingError::Upstream,
            _ => BindingError::NotFound,
        })
    }
}

pub struct NoBindings;

#[async_trait]
impl PlatformBindings for NoBindings {
    async fn fetch(&self, _from: &TaskCtx, _binding: &str, _req: Request) -> Result<Response, BindingError> {
        Err(BindingError::NotFound)
    }
}

// ---------------------------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------------------------

pub struct Harness {
    pub exec: DenoExecutor,
    pub cfg: DenoExecutorConfig,
    pub gov: Arc<TestGovernor>,
    pub logs: Arc<MemoryLogSink>,
    pub data: Arc<LocalServiceData>,
    pub control: Arc<StubControl>,
    pub metrics: Option<Arc<Metrics>>,
    pub engine_fp: u64,
    pub tmp: tempfile::TempDir,
    next_task: u64,
    clock: Arc<SystemClock>,
}

/// One finished task: its outcome and the task id it ran under (log records carry it).
pub struct Ran {
    pub outcome: Outcome,
    pub task: TaskId,
    /// Time from `Executor::start` to the outcome (headers ready for `Ok`).
    pub took: Duration,
}

/// A fully read `Outcome::Ok`.
pub struct Got {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Bytes,
    pub timings: TaskTimings,
}

impl Got {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("body is not json ({e}): {}", self.text()))
    }
}

/// Read the whole reply of an `Outcome::Ok`; panics on any other outcome.
pub async fn read(outcome: Outcome) -> Got {
    match outcome {
        Outcome::Ok(reply) => {
            let body = reply.body.collect(16 * MIB).await.expect("read reply body");
            Got {
                status: reply.status.as_u16(),
                headers: reply.headers,
                body,
                timings: reply.timings,
            }
        }
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

impl Harness {
    pub fn new(cfg: DenoExecutorConfig) -> Self {
        Self::build(cfg, LoopbackEgress::new(&[]), None, |_| {}, |_| {})
    }

    pub fn with_egress(cfg: DenoExecutorConfig, egress: Arc<dyn EgressGuard>) -> Self {
        Self::build(cfg, egress, None, |_| {}, |_| {})
    }

    pub fn with_metrics(cfg: DenoExecutorConfig, metrics: Arc<Metrics>) -> Self {
        Self::build(cfg, LoopbackEgress::new(&[]), Some(metrics), |_| {}, |_| {})
    }

    /// With the service-data settings (`DataConfig`) adjusted by `data` (storage ceiling, result
    /// caps, SQL budgets).
    pub fn with_data(cfg: DenoExecutorConfig, data: impl FnOnce(&mut DataConfig)) -> Self {
        Self::build(cfg, LoopbackEgress::new(&[]), None, data, |_| {})
    }

    /// With the host dependencies (calls, platform bindings, socket hub, ...) adjusted by `deps`.
    pub fn with_deps(cfg: DenoExecutorConfig, deps: impl FnOnce(&mut HostDeps)) -> Self {
        Self::build(cfg, LoopbackEgress::new(&[]), None, |_| {}, deps)
    }

    fn build(
        cfg: DenoExecutorConfig,
        egress: Arc<dyn EgressGuard>,
        metrics: Option<Arc<Metrics>>,
        adjust_data: impl FnOnce(&mut DataConfig),
        adjust_deps: impl FnOnce(&mut HostDeps),
    ) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let gov = TestGovernor::with_mb(512);
        let logs = MemoryLogSink::new();
        let mut data_cfg = DataConfig::new(tmp.path());
        adjust_data(&mut data_cfg);
        let data = LocalServiceData::open(data_cfg).expect("open service data");
        let control = Arc::new(StubControl::default());
        let mut deps = HostDeps {
            data: data.clone() as Arc<dyn ServiceDataStore>,
            queue: SqliteQueueStore::open_in_memory(5).expect("in-memory queue"),
            delayed: SqliteQueueStore::open_in_memory(5).expect("in-memory queue"),
            egress,
            denylist: Arc::new(rt_sec::SelfDenylist::new(vec![], vec![])),
            calls: Arc::new(NoCalls),
            platform: Arc::new(NoBindings),
            logs: logs.clone(),
            control: control.clone(),
            mem: gov.clone(),
            sockets: Arc::new(rt_host::NoSockets),
            policy: None,
        };
        adjust_deps(&mut deps);
        let engine_fp = engine_fingerprint(&cfg.v8_flags);
        let mut exec = DenoExecutor::new(cfg.clone(), HostServices::new(deps));
        if let Some(m) = &metrics {
            exec = exec.with_metrics(m.clone());
        }
        Self {
            exec,
            cfg,
            gov,
            logs,
            data,
            control,
            metrics,
            engine_fp,
            tmp,
            next_task: 0,
            clock: Arc::new(SystemClock::new()),
        }
    }

    // ---- preparing code ------------------------------------------------------------------------

    /// Build `upload` like the node does (offline; real V8 code cache) and return the T1 value.
    pub async fn prepare(&self, upload: &Upload) -> Arc<DenoPrepared> {
        self.prepare_with(upload, Arc::new(NoNetworkFetcher), mirror_free_resolver(upload), true)
            .await
    }

    pub async fn prepare_fixture(&self, name: &str) -> Arc<DenoPrepared> {
        self.prepare(&fixture_upload(name)).await
    }

    /// Same, with remote dependencies served by `cdn` (loopback egress shim).
    pub async fn prepare_with_cdn(&self, upload: &Upload, cdn: &rt_testkit::MockCdn) -> Arc<DenoPrepared> {
        let fetcher = guarded_fetcher(LoopbackEgress::for_cdn(cdn), &["127.0.0.1"]);
        let mut cfg = mirror_resolver_cfg(cdn);
        cfg.import_map = import_map_of(upload);
        self.prepare_with(upload, fetcher, cfg, true).await
    }

    pub async fn prepare_with(
        &self,
        upload: &Upload,
        fetcher: Arc<dyn ModuleFetcher>,
        resolver: rt_build::ResolverConfig,
        with_code_cache: bool,
    ) -> Arc<DenoPrepared> {
        let builder = default_builder();
        let mut cx = build_cx(MemArtifacts::new(), fetcher, resolver);
        if with_code_cache {
            cx.code_cache = Some(Arc::new(V8CodeCacheProducer::new(self.engine_fp)));
        }
        let art = build_deno(&builder, &mut cx, upload)
            .await
            .unwrap_or_else(|f| panic!("build failed: {f}"));
        Arc::new(to_prepared(&art, self.engine_fp))
    }

    // ---- running tasks -------------------------------------------------------------------------

    fn next_cx(&mut self, spec: &rt_types::TaskSpec) -> (TaskId, ExecCx) {
        self.next_task += 1;
        let task = TaskId(self.next_task);
        let reservation = self
            .gov
            .reserve(
                MemCategory::DenoHeap,
                u64::from(self.cfg.isolate_base_mb) * MIB,
                Owner::task(task),
            )
            .expect("reserve the isolate base heap");
        let cx = ExecCx {
            task,
            machine: MachineId(0),
            proc: ProcId(0),
            clock: self.clock.clone(),
            reservation,
            deadline: Instant::now() + spec.limits.wall,
            time_slice: Duration::from_millis(10),
            cpu_budget: spec.limits.cpu,
        };
        (task, cx)
    }

    /// `Executor::start` without awaiting: several started tasks can then be polled together.
    pub fn start(&mut self, spec: rt_types::TaskSpec, prepared: &Arc<DenoPrepared>) -> (TaskId, rt_sched::TaskFuture) {
        let (task, cx) = self.next_cx(&spec);
        (task, self.exec.start(spec, prepared.clone(), cx))
    }

    /// `Executor::start` + await the outcome.
    pub async fn run(&mut self, spec: rt_types::TaskSpec, prepared: &Arc<DenoPrepared>) -> Ran {
        let (task, cx) = self.next_cx(&spec);
        let t0 = Instant::now();
        let fut = self.exec.start(spec, prepared.clone(), cx);
        let outcome = fut.await;
        Ran {
            outcome,
            task,
            took: t0.elapsed(),
        }
    }

    /// Run a GET and read the whole reply.
    pub async fn get(&mut self, path: &str, prepared: &Arc<DenoPrepared>) -> (Got, TaskId) {
        let ran = self.run(get(path), prepared).await;
        (read(ran.outcome).await, ran.task)
    }

    /// Cold-start V8 and the executor once so timing-sensitive assertions measure steady state.
    pub async fn warm_up(&mut self) {
        let p = self.prepare_fixture("hello").await;
        let _ = self.get("/", &p).await;
        self.settle(Duration::from_millis(30)).await;
        self.exec.on_idle(Duration::from_millis(200));
    }

    /// Let spawned work (response pumps, waitUntil, disposal queueing) make progress.
    pub async fn settle(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }

    pub fn log_records(&self) -> Vec<LogRecord> {
        self.logs.records()
    }

    /// Run a request, again if it failed only because `std:sqlite` was still opening a cold database.
    ///
    /// The first statement of a service waits at most its budget for the open (on the blocking pool) and
    /// otherwise fails fast with `sql_busy` ("database is busy"); the open goes on and the next call finds
    /// it. That is the documented behaviour; on a slow, instrumented runner it happens, so a test retries
    /// like a client would. Any other outcome, a CPU-limit kill included, is returned on the first try.
    pub async fn run_past_cold_open(
        &mut self,
        spec: impl Fn() -> rt_types::TaskSpec,
        prepared: &Arc<DenoPrepared>,
    ) -> Ran {
        for _ in 0..9 {
            let seen = self.log_records().len();
            let ran = self.run(spec(), prepared).await;
            let cold = matches!(ran.outcome, Outcome::GuestError)
                && self.log_records()[seen..]
                    .iter()
                    .any(|r| r.msg.contains("database is busy"));
            if !cold {
                return ran;
            }
        }
        self.run(spec(), prepared).await
    }
}

/// `rt.json` imports of an upload (what the resolver config needs).
pub fn import_map_of(upload: &Upload) -> std::collections::BTreeMap<String, String> {
    let b = default_builder();
    rt_build::Builder::validate(&b, RuntimeKind::Deno, upload)
        .map(|v| v.import_map)
        .unwrap_or_default()
}

/// Offline resolver config with the upload's own import map.
pub fn mirror_free_resolver(upload: &Upload) -> rt_build::ResolverConfig {
    let mut cfg = resolver_cfg("https://esm.sh", &["esm.sh"]);
    cfg.import_map = import_map_of(upload);
    cfg
}

/// A build artifact as the T1 value the executor runs.
pub fn to_prepared(art: &BuiltArtifact, engine_fp: u64) -> DenoPrepared {
    let bundle_file = built_file(art, "bundle").expect("artifact has a bundle");
    let bundle = Bundle::from_bytes(&bundle_file.bytes).expect("bundle parses");
    let code_cache = built_file(art, "code_cache.bin").map(|f| CodeCache::from_bytes(&f.bytes).expect("code cache"));
    DenoPrepared {
        key: PreparedKey {
            digest: art.key.digest,
            kind: RuntimeKind::Deno,
            engine_fp,
        },
        handler_form: bundle.handler_form,
        bundle,
        code_cache,
        accounted_bytes: art.files.iter().map(|f| f.bytes.len() as u64).sum(),
        reads_env: Default::default(),
    }
}

/// `DenoPrepared` is not `Clone`; copy it with a different code cache.
pub fn with_code_cache(p: &DenoPrepared, code_cache: Option<CodeCache>) -> Arc<DenoPrepared> {
    Arc::new(DenoPrepared {
        key: p.key,
        bundle: p.bundle.clone(),
        code_cache,
        handler_form: p.handler_form,
        accounted_bytes: p.accounted_bytes,
        reads_env: Default::default(),
    })
}

// ---------------------------------------------------------------------------------------------
// Metrics text helpers
// ---------------------------------------------------------------------------------------------

/// Value of the series `name{...label...}` in a Prometheus text exposition (0 when absent).
pub fn metric(text: &str, name: &str, label: &str) -> f64 {
    text.lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name) && l[name.len()..].starts_with('{') && l.contains(label))
        .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.trim().parse::<f64>().ok()))
        .sum()
}

pub fn new_metrics() -> Arc<Metrics> {
    Metrics::new(MetricsOptions::default()).expect("register metrics")
}

pub fn code_cache_count(m: &Metrics, result: &str) -> f64 {
    metric(&m.render(), "rt_deno_code_cache_total", &format!("result=\"{result}\""))
}

/// Service id helper (`ids::service(n)`), as the spec builder uses it.
pub fn svc(n: u128) -> ServiceId {
    ids::service(n)
}

// A socket hub that records what the guest asked for (shared with the WASM executor's tests).
#[path = "../../../rt-host/tests/support/recording_hub.rs"]
mod recording_hub;
#[allow(unused_imports)]
pub use recording_hub::{info, RecordingHub};
