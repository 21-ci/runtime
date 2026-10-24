//! Shared harness for the WSM tests: fixture loading, one process-wide `Engine`, mock host
//! dependencies, and a `Rig` that drives `WasmExecutor` through the `rt_sched::Executor` contract
//! (`start` -> `TaskFuture`) on a `LocalSet`, the way a Machine does.
//!
//! The scheduler itself (`Scheduler::build`) is not used: these tests are about the executor.
//! A test that needs "the same P" passes the same `proc`; "another P" a different one.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use http::HeaderMap;
use parking_lot::Mutex as PlMutex;
use rt_exec_wasm::{
    build_engine, build_proxy_pre, engine_fingerprint, EpochTicker, IdleSlots, PoolSizing, WasmEngineConfig,
    WasmExecutor, WasmPrepared, EPOCH_TICK,
};
use rt_host::{
    BindingError, CallError, Calls, EgressBudget, EgressError, EgressGuard, EgressPolicy, EnqueueOpts, HostDeps,
    HostServices, PlatformBindings, Request, Response, TaskCtx,
};
use rt_mem::{MemCategory, MemoryGovernor, Owner};
use rt_obs::{LogRecord, MemoryLogSink};
use rt_sched::{ExecCx, Executor, Interrupt, Interrupter, SystemClock, TaskFuture};
use rt_sec::SelfDenylist;
use rt_store::{DataConfig, LocalServiceData, ServiceDataStore, SqliteQueueStore};
use rt_testkit::{SpecBuilder, TestGovernor};
use rt_types::BuildReport;
use rt_types::{
    Body, BuildFailure, CallerStamp, ConfigStream, ControlPlane, CpError, Digest, EnvBundle, HttpRequest, KillReason,
    MachineId, MsgId, NodeEvent, Outcome, Payload, PreparedKey, ProcId, ProjectId, RejectReason, RouteSnapshot,
    RuntimeKind, SecretsVersion, Service, ServiceId, ServiceKnobs, TaskId, TenantId, Trigger, UsageBatch,
};
use tempfile::TempDir;
use tokio::task::LocalSet;
use wasmtime::component::Component;
use wasmtime::Engine;
use zeroize::Zeroizing;

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/wasm/out")
}

/// Bytes of `fixtures/wasm/out/<name>.wasm`; panics with the fix if it was not built.
pub fn fixture_bytes(name: &str) -> Vec<u8> {
    let path = fixtures_dir().join(format!("{name}.wasm"));
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "WASM fixture `{name}` is missing at {} ({e}). Build the guest fixtures first: scripts/build-fixtures.sh",
            path.display()
        )
    })
}

// ---------------------------------------------------------------------------------------------
// Process-wide engine + compiled components
// ---------------------------------------------------------------------------------------------

/// One `Engine` for the whole test binary (compiling is the slow part), like production.
pub struct Shared {
    pub engine: Engine,
    _ticker: EpochTicker,
    prepared: Mutex<HashMap<String, Arc<WasmPrepared>>>,
}

pub const MIB: u64 = 1024 * 1024;

pub fn engine_config() -> WasmEngineConfig {
    // 4 GiB wasm budget / 128 MiB plan maximum = 32 slots: every test in a binary shares this engine
    // and they run in parallel, so the pool must not be the thing that limits them.
    WasmEngineConfig {
        pool: PoolSizing::from_budget(4096 * MIB, 128 * MIB, 0),
        fuel: false,
    }
}

pub fn shared() -> &'static Shared {
    static SHARED: OnceLock<Shared> = OnceLock::new();
    SHARED.get_or_init(|| {
        let engine = build_engine(&engine_config()).expect("build the shared wasmtime engine");
        let ticker = EpochTicker::spawn(engine.clone(), EPOCH_TICK);
        Shared {
            engine,
            _ticker: ticker,
            prepared: Mutex::new(HashMap::new()),
        }
    })
}

/// Compile and link (`InstancePre`) a fixture once per process.
pub fn prepared(fixture: &str) -> Arc<WasmPrepared> {
    let sh = shared();
    if let Some(p) = sh.prepared.lock().expect("prepared cache").get(fixture) {
        return p.clone();
    }
    let p = prepared_on(&sh.engine, fixture);
    sh.prepared
        .lock()
        .expect("prepared cache")
        .insert(fixture.to_string(), p.clone());
    p
}

/// Compile and link a fixture for `engine` (not cached).
pub fn prepared_on(engine: &Engine, fixture: &str) -> Arc<WasmPrepared> {
    let bytes = fixture_bytes(fixture);
    let component = Component::new(engine, &bytes)
        .unwrap_or_else(|e| panic!("fixture `{fixture}` does not compile as a component: {e:#}"));
    let pre = build_proxy_pre(engine, &component)
        .unwrap_or_else(|e| panic!("fixture `{fixture}` cannot be linked (import policy, contracts C-7): {e:#}"));
    let key = PreparedKey {
        digest: Digest::of(&bytes),
        kind: RuntimeKind::Wasm,
        engine_fp: engine_fingerprint(engine),
    };
    Arc::new(WasmPrepared::new(key, component, pre, bytes.len() as u64).expect("the fixture exports the HTTP handler"))
}

/// Like [`prepared_on`], under a key of its own. The process-wide table of warmable artifacts is
/// keyed by digest and engine fingerprint, which two engines of one configuration share: tests
/// that warm instances on engines of their own use distinct keys so they do not overwrite each
/// other's entries (a node has one engine).
pub fn prepared_unique(engine: &Engine, fixture: &str) -> Arc<WasmPrepared> {
    static SALT: AtomicU64 = AtomicU64::new(0);
    let bytes = fixture_bytes(fixture);
    let component =
        Component::new(engine, &bytes).unwrap_or_else(|e| panic!("fixture `{fixture}` does not compile: {e:#}"));
    let pre =
        build_proxy_pre(engine, &component).unwrap_or_else(|e| panic!("fixture `{fixture}` cannot be linked: {e:#}"));
    let mut salted = bytes.clone();
    salted.extend_from_slice(&SALT.fetch_add(1, Ordering::SeqCst).to_le_bytes());
    let key = PreparedKey {
        digest: Digest::of(&salted),
        kind: RuntimeKind::Wasm,
        engine_fp: engine_fingerprint(engine),
    };
    Arc::new(WasmPrepared::new(key, component, pre, bytes.len() as u64).expect("the fixture exports the HTTP handler"))
}

/// An engine whose pool has exactly `slots` slots of 16 MiB (pool-exhaustion tests), with its
/// own epoch ticker.
pub fn small_engine(slots: u64) -> (Engine, EpochTicker) {
    let cfg = WasmEngineConfig {
        pool: PoolSizing::from_budget(slots * 16 * MIB, 16 * MIB, 0),
        fuel: false,
    };
    let engine = build_engine(&cfg).expect("build a small engine");
    let ticker = EpochTicker::spawn(engine.clone(), EPOCH_TICK);
    (engine, ticker)
}

/// An engine nobody ticks: the epoch never moves, so no guest code is ever asked to yield.
pub fn untick_engine(slots: u64) -> Engine {
    let cfg = WasmEngineConfig {
        pool: PoolSizing::from_budget(slots * 16 * MIB, 16 * MIB, 0),
        fuel: false,
    };
    build_engine(&cfg).expect("build an engine without a ticker")
}

/// An engine that meters fuel (the deterministic CPU backstop), with its own epoch ticker.
pub fn fuel_engine() -> (Engine, EpochTicker) {
    let cfg = WasmEngineConfig {
        pool: PoolSizing::from_budget(512 * MIB, 128 * MIB, 0),
        fuel: true,
    };
    let engine = build_engine(&cfg).expect("build a fuel-metering engine");
    let ticker = EpochTicker::spawn(engine.clone(), EPOCH_TICK);
    (engine, ticker)
}

// ---------------------------------------------------------------------------------------------
// Mock host dependencies
// ---------------------------------------------------------------------------------------------

/// Outbound HTTP: records the URL, waits `delay` of real time (an I/O boundary), answers 200.
pub struct MockEgress {
    pub delay: Duration,
    pub requests: PlMutex<Vec<String>>,
    /// Headers of each request, in order.
    pub headers: PlMutex<Vec<HeaderMap>>,
}

impl MockEgress {
    pub fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            delay,
            requests: PlMutex::new(Vec::new()),
            headers: PlMutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl EgressGuard for MockEgress {
    async fn send(
        &self,
        req: Request,
        _policy: &EgressPolicy,
        _budget: &mut EgressBudget,
    ) -> Result<Response, EgressError> {
        self.requests.lock().push(req.uri().to_string());
        self.headers.lock().push(req.headers().clone());
        tokio::time::sleep(self.delay).await;
        Ok(http::Response::builder()
            .status(200)
            .body(Body::from("mock upstream"))
            .expect("mock response"))
    }
}

/// One `std:host/call` the mock received.
#[derive(Clone, Debug)]
pub struct SeenCall {
    pub from: ServiceId,
    pub slug: String,
    pub method: String,
    pub path: String,
    pub x_from: Option<String>,
    pub body: Vec<u8>,
}

/// One `std:host/queue` enqueue the mock received.
#[derive(Clone, Debug)]
pub struct SeenEnqueue {
    pub from: ServiceId,
    pub slug: String,
    pub body: Vec<u8>,
    pub opts: EnqueueOpts,
}

/// Service-to-service calls and enqueues, standing in for `rt-triggers`' dispatcher (which owns
/// the access rules: `call.allowed_callers`, environments, plan triggers). Slugs script the answer:
/// `missing` → `not_found`, `private` → `call_not_allowed`, `deep` → call depth exceeded,
/// `big` → a response over the buffered limit, `full` → `queue_full`; anything else answers.
#[derive(Default)]
pub struct MockCalls {
    pub calls: PlMutex<Vec<SeenCall>>,
    pub enqueued: PlMutex<Vec<SeenEnqueue>>,
    /// `(service slug, object, path)` of each request to a durable object.
    pub object_calls: PlMutex<Vec<(Option<String>, String, String)>>,
}

#[async_trait]
impl Calls for MockCalls {
    async fn call(&self, from: &TaskCtx, slug: &str, req: Request) -> Result<Response, CallError> {
        match slug {
            "missing" => return Err(CallError::NotFound),
            "private" => return Err(CallError::NotAllowed),
            "deep" => return Err(CallError::Depth),
            _ => {}
        }
        let (parts, body) = req.into_parts();
        let body = body.collect(MIB).await.expect("call body").to_vec();
        let rpc = parts.uri.path().starts_with("/__rt/rpc/");
        let seen = SeenCall {
            from: from.service,
            slug: slug.to_string(),
            method: parts.method.to_string(),
            path: parts.uri.to_string(),
            x_from: parts
                .headers
                .get("x-from")
                .map(|v| v.to_str().unwrap_or_default().to_string()),
            body: body.clone(),
        };
        self.calls.lock().push(seen);
        if rpc {
            return Ok(rpc_answer(
                slug,
                parts.uri.path().trim_start_matches("/__rt/rpc/"),
                &body,
            ));
        }
        if slug == "big" {
            let huge = vec![b'x'; (rt_types::Limits::default().max_response_buffered_bytes + 1) as usize];
            return Ok(http::Response::builder()
                .status(200)
                .body(Body::from(huge))
                .expect("mock response"));
        }
        let text = format!(
            "{slug} got {} {} {}",
            parts.method,
            parts.uri,
            String::from_utf8_lossy(&body)
        );
        Ok(http::Response::builder()
            .status(201)
            .header("x-callee", slug)
            .body(Body::from(text))
            .expect("mock response"))
    }

    /// Objects script the answer like slugs do: `missing` -> `not_found`, `deep` -> call depth
    /// exceeded, `big` -> a response over the buffered limit; anything else answers 202.
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
        self.object_calls
            .lock()
            .push((slug.map(str::to_string), object.to_string(), req.uri().to_string()));
        if object == "big" {
            let huge = vec![b'x'; (rt_types::Limits::default().max_response_buffered_bytes + 1) as usize];
            return Ok(http::Response::builder()
                .status(200)
                .body(Body::from(huge))
                .expect("mock response"));
        }
        Ok(http::Response::builder()
            .status(202)
            .header("x-object", object)
            .body(Body::from(format!("{object} says hi")))
            .expect("mock response"))
    }

    async fn enqueue(&self, from: &TaskCtx, slug: &str, body: Bytes, o: EnqueueOpts) -> Result<MsgId, CallError> {
        match slug {
            "missing" => return Err(CallError::NotFound),
            "full" => return Err(CallError::QueueFull),
            _ => {}
        }
        let mut seen = self.enqueued.lock();
        seen.push(SeenEnqueue {
            from: from.service,
            slug: slug.to_string(),
            body: body.to_vec(),
            opts: o,
        });
        Ok(MsgId::from_u128(seen.len() as u128))
    }
}

/// The mock's answer to an RPC (`POST /__rt/rpc/<method>`): the method `fail` answers the callee's
/// error, others echo the argument; the slug `down` failed (502 `guest_error`), `garbage` does not
/// answer an RPC envelope.
fn rpc_answer(slug: &str, method: &str, body: &[u8]) -> Response {
    let (status, reason, text) = match (slug, method) {
        ("down", _) => (502, Some("guest_error"), String::new()),
        ("garbage", _) => (200, None, "not json".to_string()),
        (_, "fail") => (
            200,
            None,
            r#"{"ok":false,"error":{"code":"out_of_stock","message":"no stock left"}}"#.to_string(),
        ),
        _ => (
            200,
            None,
            format!("{{\"ok\":true,\"value\":{}}}", String::from_utf8_lossy(body)),
        ),
    };
    let mut b = http::Response::builder().status(status);
    if let Some(r) = reason {
        b = b.header("x-rt-reason", r);
    }
    b.body(Body::from(text)).expect("mock response")
}

/// The caller a [`Req::rpc`] carries (`ctx.caller` / the `rpc.handle` caller record).
pub fn caller_stamp() -> CallerStamp {
    CallerStamp {
        service: ServiceId::from_u128(42),
        tenant: TenantId::from_u128(1),
        slug: Some("the-caller".to_string()),
        project: Some(ProjectId::from_u128(9)),
        environment: Some("production".to_string()),
        plan: Some("pro".to_string()),
        sdk: None,
    }
}

struct NoBindings;

#[async_trait]
impl PlatformBindings for NoBindings {
    async fn fetch(&self, _from: &TaskCtx, _binding: &str, _req: Request) -> Result<Response, BindingError> {
        Err(BindingError::NotFound)
    }
}

/// Control plane stub: an env bundle tests can change (secrets rotation) and the usage batches
/// the host reports (egress, CPU after the outcome).
#[derive(Default)]
pub struct StubControl {
    pub env: PlMutex<EnvBundle>,
    pub usage: PlMutex<Vec<UsageBatch>>,
    /// While set the control plane cannot produce the service's environment.
    pub env_unavailable: std::sync::atomic::AtomicBool,
    /// How many times the environment was asked for (a task, or a pre-instantiation, each reads it).
    pub env_reads: AtomicU64,
}

impl StubControl {
    /// CPU milliseconds reported for `service` so far.
    pub fn cpu_ms(&self, service: ServiceId) -> u64 {
        self.usage
            .lock()
            .iter()
            .flat_map(|b| b.items.iter())
            .filter(|(_, s, _)| *s == service)
            .map(|(_, _, d)| d.cpu_ms)
            .sum()
    }
}

#[async_trait]
impl ControlPlane for StubControl {
    fn routes(&self) -> Arc<RouteSnapshot> {
        Arc::new(RouteSnapshot::default())
    }

    async fn subscribe_config(&self) -> ConfigStream {
        stream::empty().boxed()
    }

    async fn service(&self, _id: ServiceId) -> Result<Option<Service>, CpError> {
        Ok(None)
    }

    async fn env_for(&self, _service: ServiceId, _version: SecretsVersion) -> Result<Zeroizing<EnvBundle>, CpError> {
        self.env_reads.fetch_add(1, Ordering::SeqCst);
        if self.env_unavailable.load(Ordering::SeqCst) {
            return Err(CpError::Unavailable);
        }
        Ok(Zeroizing::new(self.env.lock().clone()))
    }

    async fn report_build(&self, _key: &PreparedKey, _report: BuildReport) -> Result<(), CpError> {
        Ok(())
    }

    fn usage(&self, batch: UsageBatch) {
        self.usage.lock().push(batch);
    }

    fn event(&self, _ev: NodeEvent) {}
}

// ---------------------------------------------------------------------------------------------
// Requests and results
// ---------------------------------------------------------------------------------------------

/// One HTTP task to run. Defaults: tenant 1, service 1, `balanced` limits, no instance reuse.
#[derive(Clone, Debug)]
pub struct Req {
    pub method: http::Method,
    pub path: String,
    pub body: Vec<u8>,
    pub tenant: u128,
    pub service: u128,
    pub memory_mb: u32,
    pub wall_ms: u64,
    pub cpu_ms: u64,
    pub reuse: bool,
    pub proc: u32,
    /// `cache.idle_ttl` of the service (`None`: the built-in default).
    pub idle_ttl: Option<Duration>,
    /// `limits.max_response_stream_bytes` (`None`: the default).
    pub max_stream_bytes: Option<u64>,
    /// `scaling.min_run_time_ms` of the service (`None`: inherit).
    pub min_run_time_ms: Option<u64>,
    /// The service version (default 1).
    pub version: u32,
    /// Request headers (as ingress would pass them on).
    pub headers: Vec<(String, String)>,
    /// `None`: an HTTP task. `Some`: this trigger (a call: [`Req::rpc`]).
    pub trigger: Option<Trigger>,
    /// The caller stamp of a call task.
    pub caller: Option<CallerStamp>,
    /// The object the task acts for (`TaskSpec::object`).
    pub object: Option<String>,
    /// A socket event or alarm instead of a request.
    pub event: Option<rt_types::SocketEvent>,
}

impl Req {
    pub fn get(path: &str) -> Self {
        Self {
            method: http::Method::GET,
            path: path.to_string(),
            body: Vec::new(),
            tenant: 1,
            service: 1,
            memory_mb: 128,
            wall_ms: 30_000,
            cpu_ms: 1_000,
            reuse: false,
            proc: 0,
            idle_ttl: None,
            max_stream_bytes: None,
            min_run_time_ms: None,
            version: 1,
            headers: Vec::new(),
            trigger: None,
            caller: None,
            object: None,
            event: None,
        }
    }

    /// An event task of `object` (a socket event or an alarm), as the socket hub delivers it.
    pub fn event(object: &str, kind: rt_types::SocketEventKind, socket: Option<rt_types::SocketInfo>) -> Self {
        let trigger = match kind {
            rt_types::SocketEventKind::Alarm { .. } => Trigger::Alarm { object: object.into() },
            _ => Trigger::Socket { object: object.into() },
        };
        Self {
            trigger: Some(trigger),
            object: Some(object.to_string()),
            event: Some(rt_types::SocketEvent {
                socket,
                kind,
                attempt: 1,
            }),
            ..Self::get("/")
        }
    }

    /// The task acts for `object` (a request to a durable object).
    pub fn object(mut self, name: &str) -> Self {
        self.object = Some(name.to_string());
        self
    }

    /// An RPC to this service, as the call dispatcher delivers it: a call task for
    /// `POST /__rt/rpc/<method>` with the JSON argument as the body, from [`caller_stamp`].
    pub fn rpc(method: &str, args: &[u8]) -> Self {
        Self {
            method: http::Method::POST,
            body: args.to_vec(),
            trigger: Some(Trigger::Call {
                parent: TaskId(1),
                depth: 1,
            }),
            caller: Some(caller_stamp()),
            ..Self::get(&format!("/__rt/rpc/{method}"))
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn put(path: &str, body: &[u8]) -> Self {
        Self {
            method: http::Method::PUT,
            body: body.to_vec(),
            ..Self::get(path)
        }
    }

    pub fn service(mut self, n: u128) -> Self {
        self.service = n;
        self
    }

    pub fn tenant(mut self, n: u128) -> Self {
        self.tenant = n;
        self
    }

    pub fn memory_mb(mut self, mb: u32) -> Self {
        self.memory_mb = mb;
        self
    }

    pub fn wall_ms(mut self, ms: u64) -> Self {
        self.wall_ms = ms;
        self
    }

    pub fn cpu_ms(mut self, ms: u64) -> Self {
        self.cpu_ms = ms;
        self
    }

    /// `wasm.reuse_instances = true` (the `min_latency` profile).
    pub fn reuse(mut self) -> Self {
        self.reuse = true;
        self
    }

    pub fn proc(mut self, p: u32) -> Self {
        self.proc = p;
        self
    }

    pub fn idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = Some(ttl);
        self
    }

    /// `scaling.min_run_time_ms` (keep-alive after the answer; `0`: only while work waits).
    pub fn min_run_time_ms(mut self, ms: u64) -> Self {
        self.min_run_time_ms = Some(ms);
        self
    }

    pub fn max_stream_bytes(mut self, n: u64) -> Self {
        self.max_stream_bytes = Some(n);
        self
    }

    pub fn version(mut self, v: u32) -> Self {
        self.version = v;
        self
    }
}

#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    /// `key=value` lines (clock / fs_socket fixtures).
    pub fn kv(&self, key: &str) -> String {
        let prefix = format!("{key}=");
        self.text()
            .lines()
            .find_map(|l| l.strip_prefix(&prefix).map(str::to_string))
            .unwrap_or_else(|| panic!("no `{key}=` line in response body: {:?}", self.text()))
    }
}

/// An `Outcome` with its body read.
#[derive(Debug)]
pub enum Res {
    Ok(Reply),
    GuestError,
    Killed(KillReason),
    Rejected(RejectReason),
    BuildFailed(BuildFailure),
}

#[derive(Debug)]
pub struct Done {
    pub task: TaskId,
    pub res: Res,
}

impl Done {
    /// The reply of an `Outcome::Ok`; panics (with the outcome) otherwise.
    #[track_caller]
    pub fn reply(self) -> Reply {
        match self.res {
            Res::Ok(r) => r,
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }
    }

    #[track_caller]
    pub fn text(self) -> String {
        self.reply().text()
    }

    #[track_caller]
    pub fn killed(self) -> KillReason {
        match self.res {
            Res::Killed(k) => k,
            other => panic!("expected Outcome::Killed, got {other:?}"),
        }
    }

    #[track_caller]
    pub fn expect_guest_error(self) {
        match self.res {
            Res::GuestError => {}
            other => panic!("expected Outcome::GuestError, got {other:?}"),
        }
    }
}

async fn drain(outcome: Outcome) -> Res {
    match outcome {
        Outcome::Ok(reply) => {
            let body = reply.body.collect(32 * MIB).await.expect("read the response body");
            Res::Ok(Reply {
                status: reply.status.as_u16(),
                headers: reply.headers,
                body,
            })
        }
        Outcome::GuestError => Res::GuestError,
        Outcome::Killed(k) => Res::Killed(k),
        Outcome::Rejected(r) => Res::Rejected(r),
        Outcome::BuildFailed(f) => Res::BuildFailed(f),
    }
}

// ---------------------------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------------------------

static NEXT_TASK: AtomicU64 = AtomicU64::new(1);

pub struct Rig {
    pub exec: WasmExecutor,
    pub governor: Arc<TestGovernor>,
    pub logs: Arc<MemoryLogSink>,
    pub egress: Arc<MockEgress>,
    pub calls: Arc<MockCalls>,
    pub control: Arc<StubControl>,
    deps: HostDeps,
    _data_dir: TempDir,
}

impl Rig {
    pub fn new() -> Self {
        Self::with(512, Duration::from_millis(25))
    }

    /// `budget_mb` is the governor budget; `egress_delay` the real time the mock upstream takes.
    pub fn with(budget_mb: u64, egress_delay: Duration) -> Self {
        Self::on_engine(shared().engine.clone(), budget_mb, egress_delay)
    }

    /// A rig whose executor runs on `engine` (fixtures must be compiled for it: [`prepared_on`]).
    pub fn on_engine(engine: Engine, budget_mb: u64, egress_delay: Duration) -> Self {
        let data_dir = tempfile::tempdir().expect("temp data dir");
        let governor = TestGovernor::with_mb(budget_mb);
        let logs = MemoryLogSink::new();
        let egress = MockEgress::new(egress_delay);
        let calls = Arc::new(MockCalls::default());
        let control = Arc::new(StubControl::default());
        let data: Arc<dyn ServiceDataStore> =
            LocalServiceData::open(DataConfig::new(data_dir.path())).expect("open per-service data store");
        let deps = HostDeps {
            data,
            queue: SqliteQueueStore::open_in_memory(5).expect("in-memory queue store"),
            delayed: SqliteQueueStore::open_in_memory(5).expect("in-memory queue store"),
            egress: egress.clone(),
            denylist: Arc::new(SelfDenylist::new(Vec::new(), Vec::new())),
            calls: calls.clone(),
            platform: Arc::new(NoBindings),
            logs: logs.clone(),
            control: control.clone(),
            mem: governor.clone(),
            sockets: Arc::new(rt_host::NoSockets),
            policy: None,
        };
        let exec = WasmExecutor::new(engine, HostServices::new(deps.clone()));
        Self {
            exec,
            governor,
            logs,
            egress,
            calls,
            control,
            deps,
            _data_dir: data_dir,
        }
    }

    /// Operator bindings (`std:host/platform`): rebuilds the executor with `platform`.
    pub fn platform(mut self, platform: Arc<dyn PlatformBindings>) -> Self {
        self.deps.platform = platform;
        self.exec = WasmExecutor::new(self.exec.engine().clone(), HostServices::new(self.deps.clone()));
        self
    }

    /// The node's socket hub (`NoSockets` by default): rebuilds the executor.
    pub fn sockets(mut self, sockets: Arc<dyn rt_host::SocketHost>) -> Self {
        self.deps.sockets = sockets;
        self.exec = WasmExecutor::new(self.exec.engine().clone(), HostServices::new(self.deps.clone()));
        self
    }

    /// A different egress guard (outbound WebSockets need a real one): rebuilds the executor.
    pub fn egress(mut self, egress: Arc<dyn rt_host::EgressGuard>) -> Self {
        self.deps.egress = egress;
        self.exec = WasmExecutor::new(self.exec.engine().clone(), HostServices::new(self.deps.clone()));
        self
    }

    /// `wasm.reuse_max_idle`: idle used instances kept per service and P.
    pub fn reuse_max_idle(mut self, n: usize) -> Self {
        self.exec = self.exec.with_reuse_max_idle(n);
        self
    }

    /// Record `rt_wasm_instantiate_seconds` on `metrics`.
    pub fn metrics(mut self, metrics: Arc<rt_obs::Metrics>) -> Self {
        self.exec = self.exec.with_metrics(metrics);
        self
    }

    /// Share a process-wide idle-slot budget (one per node in production).
    pub fn idle_slots(mut self, slots: Arc<IdleSlots>) -> Self {
        self.exec = self.exec.with_idle_slots(slots);
        self
    }

    /// The executor with the autoscaler's warm targets (`WasmExecutor::with_warm_targets`).
    pub fn warm(mut self, targets: Arc<rt_sched::WarmTargets>) -> Self {
        self.exec = self.exec.with_warm_targets(targets);
        self
    }

    /// One `between_tasks` hook call, inside a `LocalSet` like a Machine's.
    pub async fn between_tasks(&mut self, budget: Duration) {
        let exec = &mut self.exec;
        LocalSet::new().run_until(async { exec.between_tasks(budget) }).await;
    }

    pub fn log_records(&self) -> Vec<LogRecord> {
        self.logs.records()
    }

    /// `Executor::start` for one request; returns the task id and its (not yet polled) future.
    pub fn start(&mut self, prepared: &Arc<WasmPrepared>, req: &Req) -> (TaskId, TaskFuture) {
        let task = TaskId(NEXT_TASK.fetch_add(1, Ordering::SeqCst));
        let uri: http::Uri = req.path.parse().expect("request path");
        let http_req = HttpRequest {
            method: req.method.clone(),
            uri,
            headers: req
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        http::HeaderName::from_bytes(k.as_bytes()).expect("header name"),
                        v.parse().expect("header value"),
                    )
                })
                .collect(),
            remote_addr: None,
            body: Body::from(req.body.clone()),
        };
        let mut knobs = ServiceKnobs {
            reuse_instances: req.reuse,
            ..ServiceKnobs::default()
        };
        if let Some(ttl) = req.idle_ttl {
            knobs.idle_ttl = ttl;
        }
        knobs.min_run_time = req.min_run_time_ms.map(Duration::from_millis);
        let mut spec = SpecBuilder::wasm()
            .tenant(req.tenant)
            .service(req.service)
            .memory_mb(req.memory_mb)
            .wall_limit(req.wall_ms)
            .cpu_limit(req.cpu_ms)
            .knobs(knobs)
            .payload(Payload::Http(http_req))
            .finish();
        if let Some(n) = req.max_stream_bytes {
            spec.limits.max_response_stream_bytes = n;
        }
        if let Some(t) = &req.trigger {
            spec.trigger = t.clone();
        }
        spec.caller = req.caller.clone();
        spec.service.version = rt_types::VersionNo(req.version);
        if let Some(o) = &req.object {
            spec.object = Some(rt_types::ObjectRef::named(o));
        }
        if let Some(ev) = &req.event {
            spec.payload = Payload::Socket(ev.clone());
        }
        let reservation = self
            .governor
            .reserve(MemCategory::WasmLinear, MIB, Owner::task(task))
            .expect("reserve the task's initial memory");
        let cx = ExecCx {
            task,
            machine: MachineId(0),
            proc: ProcId(req.proc),
            clock: Arc::new(SystemClock::new()),
            reservation,
            deadline: Instant::now() + Duration::from_millis(req.wall_ms),
            time_slice: Duration::from_millis(10),
            cpu_budget: Duration::from_millis(req.cpu_ms),
        };
        (task, self.exec.start(spec, prepared.clone(), cx))
    }

    /// Run one task to completion on a `LocalSet` (like a Machine) and read its body there: the
    /// executor streams the body from a local driver task.
    pub async fn run(&mut self, prepared: &Arc<WasmPrepared>, req: Req) -> Done {
        let (task, fut) = self.start(prepared, &req);
        let local = LocalSet::new();
        let res = local.run_until(async move { drain(fut.await).await }).await;
        Done { task, res }
    }

    /// Like [`Rig::run`], also returning the thread CPU time spent inside polls of the task future
    /// itself (what the scheduler measures and bills), not in tasks it spawned.
    pub async fn run_measured(&mut self, prepared: &Arc<WasmPrepared>, req: Req) -> (Done, Duration) {
        let (task, fut) = self.start(prepared, &req);
        let cpu = Arc::new(AtomicU64::new(0));
        let measured = MeasuredFuture { fut, cpu: cpu.clone() };
        let local = LocalSet::new();
        let res = local.run_until(async move { drain(measured.await).await }).await;
        (Done { task, res }, Duration::from_nanos(cpu.load(Ordering::SeqCst)))
    }

    /// Run several tasks concurrently on one `LocalSet`; results come back in input order, each with
    /// the instant its future completed.
    pub async fn run_all(&mut self, tasks: Vec<(Arc<WasmPrepared>, Req)>) -> Vec<(Done, Instant)> {
        let local = LocalSet::new();
        let mut handles = Vec::new();
        for (prepared, req) in &tasks {
            let (task, fut) = self.start(prepared, req);
            handles.push((
                task,
                local.spawn_local(async move {
                    let outcome = fut.await;
                    (outcome, Instant::now())
                }),
            ));
        }
        local
            .run_until(async move {
                let mut out = Vec::new();
                for (task, h) in handles {
                    let (outcome, at) = h.await.expect("task join");
                    out.push((
                        Done {
                            task,
                            res: drain(outcome).await,
                        },
                        at,
                    ));
                }
                out
            })
            .await
    }

    /// Ask the executor's interrupter (as sysmon would, from another OS thread) to kill `task`
    /// after `after`.
    pub fn kill_later(&self, task: TaskId, after: Duration, reason: KillReason) -> std::thread::JoinHandle<()> {
        let interrupter: Arc<dyn Interrupter> = self.exec.interrupter();
        std::thread::spawn(move || {
            std::thread::sleep(after);
            interrupter.interrupt(task, Interrupt::Kill(reason));
        })
    }
}

impl Default for Rig {
    fn default() -> Self {
        Self::new()
    }
}

/// Run `req` against `fixture` on a fresh default rig.
pub async fn run_once(fixture: &str, req: Req) -> Done {
    let mut rig = Rig::new();
    let p = prepared(fixture);
    rig.run(&p, req).await
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer for the duration of the call.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
}

/// Charges the thread CPU time of each poll of `fut` to `cpu` (like the scheduler's `Tracked`).
struct MeasuredFuture {
    fut: TaskFuture,
    cpu: Arc<AtomicU64>,
}

impl std::future::Future for MeasuredFuture {
    type Output = Outcome;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Outcome> {
        let t0 = thread_cpu_ns();
        let r = self.fut.as_mut().poll(cx);
        let spent = thread_cpu_ns().saturating_sub(t0);
        self.cpu.fetch_add(spent, Ordering::SeqCst);
        r
    }
}

// ---------------------------------------------------------------------------------------------
// Outbound WebSockets to a server on this machine
// ---------------------------------------------------------------------------------------------

/// A resolver that knows one public name (`echo.test`, TEST-NET-3).
pub struct EchoDns;

#[async_trait]
impl rt_host::Resolver for EchoDns {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<std::net::IpAddr>> {
        match host {
            "echo.test" => Ok(vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 9))]),
            _ => Err(std::io::Error::other("no such host")),
        }
    }
}

/// The real egress guard with `echo.test` resolving to TEST-NET-3 and every connection to that
/// range rewritten to this machine, so `ws://echo.test:<port>/` reaches a local test server.
pub fn local_ws_guard() -> Arc<rt_host::GuardedEgress> {
    use std::net::{IpAddr, Ipv4Addr};
    let denylist = Arc::new(rt_sec::SelfDenylist::new(vec![], vec![]));
    Arc::new(
        rt_host::GuardedEgress::new(rt_host::EgressGuardConfig::default(), denylist)
            .with_resolver(Arc::new(EchoDns))
            .with_connect_rewrite(Arc::new(|ip: IpAddr| match ip {
                IpAddr::V4(a) if a.octets()[..3] == [203, 0, 113] => IpAddr::V4(Ipv4Addr::LOCALHOST),
                other => other,
            })),
    )
}

// A socket hub that records what the guest asked for (shared with the other executors' tests).
#[path = "../../../rt-host/tests/support/recording_hub.rs"]
mod recording_hub;
#[allow(
    unused_imports,
    reason = "`info` is for the socket tests of other executors sharing this file"
)]
pub use recording_hub::{info, RecordingHub};
