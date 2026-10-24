//! `WasmExecutor`: one instance per Machine (thread-local), sharing one process-wide `Engine`.
//!
//! Per task: an idle instance of the same `(tenant, service, version,
//! artifact, P)` built with the same environment — a warm (fresh, never used) one, or with
//! `reuse_instances` a used one — else a new `Store` (service ctx, `ResourceTable`, limiter bound
//! to the governor reservation) instantiated from `InstancePre` → `wasi:http` `handle`.
//!
//! Preemption: `epoch_deadline_callback` yields to the Machine's `LocalSet` at each slice and
//! turns a kill (interrupter, wall deadline, CPU budget) into a trap. Tasks run to completion on
//! the Machine that started them.
//!
//! Response (WSM-14): the outcome is decided by the response head. Until the head the guest runs
//! **inside the task future** (WSM-18), so the scheduler's per-poll accounting measures, bills
//! and limits it. A guest that finishes within that run hands over a buffered body; otherwise
//! the head is returned and the same driver keeps running the guest as a local task, forwarding
//! body chunks through a bounded channel (backpressure) up to `limits.max_response_stream_bytes`.
//! The CPU limit still applies to that tail (the epoch callback counts every poll of the guest),
//! and the tail's CPU is charged to the tenant when the driver ends (`HostHandle::charge_cpu`).
//! A failure after the head (trap, kill, size limit, a body the guest never finished) aborts the
//! body stream. The driver's last act ends the body, so the instance is back on its idle list
//! (with a detached host handle) before the client sees the end of the response.
//!
//! Warm capacity (D-A3/D-A4, WSM-16): with [`WasmExecutor::with_warm_targets`] the
//! `between_tasks` / `on_idle` hooks converge on the autoscaler's per-P targets by
//! pre-instantiating fresh instances off the request path (see [`crate::warm`]). Idle instances
//! of all Machines share one slot budget ([`IdleSlots`], WSM-19).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures_util::future::poll_immediate;
use futures_util::task::noop_waker_ref;
use futures_util::StreamExt;
use http::{HeaderName, HeaderValue};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use parking_lot::Mutex;
use rt_host::rpc::{
    check_args, envelope_err, envelope_ok, envelope_response as rpc_envelope_response, rpc_method_of, valid_method,
    RPC_JSON_MAX_BYTES,
};
use rt_host::{HostHandle, HostServices, RpcError};
use rt_mem::{MemCategory, Owner, Pressure, Reservation};
use rt_obs::{LogLevel, Metrics};
use rt_sched::{
    after_answer, AfterAnswer, ExecCx, Executor, Interrupter, TaskFuture, WarmKey, WarmTarget, WarmTargets,
};
use rt_types::{
    Body, BodyError, BodySender, EnvBundle, HttpRequest, KillReason, Outcome, Payload, PreparedKey, ProcId, Reply,
    RuntimeKind, ServiceId, ServiceRef, TaskId, TaskKind, TaskSpec, TaskTimings, Trigger,
};
use tokio::sync::oneshot;
use wasmtime::component::ResourceTable;
use wasmtime::{Engine, Store, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};
use wasmtime_wasi_http::p2::bindings::http::types::{ErrorCode, Scheme};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::{HostIncomingRequest, HostResponseOutparam};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::WasiHttpView;

use crate::clock::FrozenClock;
use crate::engine::EPOCH_TICK;
use crate::host_wit::wit::exports::std::host::rpc as rpc_wit;
use crate::interrupt::{KillSwitch, WasmInterrupter};
use crate::prepared::{GuestExports, WasmPrepared};
use crate::state::{GovernorLimiter, GuardedHttpHooks, WasmState};
use crate::stdio::GuestLogs;
use crate::warm::{
    self, prewarm_job, rotation, same_env, Backoff, Env, IdleInstance, IdleKey, IdlePool, IdleSlots, PrewarmJob, Taken,
    DEFAULT_IDLE_SLOTS, MIN_IDLE_BYTES,
};

/// Idle-list default (`wasm.reuse_max_idle`).
const DEFAULT_REUSE_MAX_IDLE: usize = 8;
/// An instance is retired after this many tasks (bounds leaked guest state and fragmentation).
const MAX_REUSES: u32 = 1000;
/// Fuel is a deterministic backstop next to the epoch/CPU limits: instructions per CPU ms.
const FUEL_PER_CPU_MS: u64 = 10_000_000;
/// Above this many artifacts a Machine drops the ones it remembers whose cache entry is gone.
const SEEN_PRUNE_AT: usize = 1024;
/// Warm instances per key and P at most, whatever the target says.
const MAX_WARM_PER_KEY: usize = 16;
/// Chunks in flight between the driver and the client (like the Deno executor).
const BODY_CHANNEL_CAPACITY: usize = 4;
/// Idle instances one instantiation may evict when it finds the pool exhausted.
const MAX_EVICTIONS: u32 = 8;

type RespResult = Result<http::Response<HyperOutgoingBody>, ErrorCode>;

pub struct WasmExecutor {
    engine: Engine,
    host: HostServices,
    interrupter: Arc<WasmInterrupter>,
    idle: Arc<Mutex<IdlePool>>,
    /// The process-wide idle-slot budget (shared by every Machine in production).
    slots: Arc<IdleSlots>,
    reuse_max_idle: usize,
    /// [`Executor::wake_signal`]: an instance was stashed (its expiry may be the next deadline).
    signal: Arc<tokio::sync::Notify>,
    metrics: Option<Arc<Metrics>>,
    /// The autoscaler's per-P warm targets (`None`: no warm pool).
    warm: Option<Arc<WarmTargets>>,
    /// Processor of the latest task this Machine started: warm instances are built for it.
    proc: Option<ProcId>,
    /// Artifacts already registered for pre-instantiation (skips the shared lock per task).
    seen: HashMap<PreparedKey, Weak<WasmPrepared>>,
    /// Pre-instantiations that yielded; polled again by the next hook call.
    prewarming: Vec<(IdleKey, PrewarmJob)>,
    /// Keys whose last pre-instantiation failed: not retried before `until`.
    backoff: HashMap<IdleKey, Backoff>,
    /// Where the next hook call starts in the list of warm keys (rotation).
    cursor: usize,
}

impl WasmExecutor {
    /// `engine` is shared by all Machines (cheap `Arc` clone).
    pub fn new(engine: Engine, host: HostServices) -> Self {
        let interrupter = WasmInterrupter::new(engine.clone());
        let idle = Arc::new(Mutex::new(IdlePool::default()));
        let slots = IdleSlots::new(DEFAULT_IDLE_SLOTS);
        slots.register(&idle);
        Self {
            engine,
            host,
            interrupter,
            idle,
            slots,
            reuse_max_idle: DEFAULT_REUSE_MAX_IDLE,
            signal: Arc::default(),
            metrics: None,
            warm: None,
            proc: None,
            seen: HashMap::new(),
            prewarming: Vec::new(),
            backoff: HashMap::new(),
            cursor: 0,
        }
    }

    /// `wasm.reuse_max_idle`: idle used instances kept per service and P.
    pub fn with_reuse_max_idle(mut self, n: usize) -> Self {
        self.reuse_max_idle = n;
        self
    }

    /// Record `rt_wasm_instantiate_seconds`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Keep warm instances for the autoscaler's targets (D-A3/D-A4): the `between_tasks` and
    /// `on_idle` hooks pre-instantiate fresh instances up to each WASM key's `per_proc` on this
    /// Machine's Processor, and drop them once a key has no target and its idle TTL passed.
    pub fn with_warm_targets(mut self, targets: Arc<WarmTargets>) -> Self {
        self.warm = Some(targets);
        self
    }

    /// Share the node's idle-slot budget ([`IdleSlots::for_pool`]): idle instances of every
    /// Machine together hold at most that many pool slots.
    pub fn with_idle_slots(mut self, slots: Arc<IdleSlots>) -> Self {
        slots.register(&self.idle);
        self.slots = slots;
        self
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn host(&self) -> &HostServices {
        &self.host
    }

    /// Idle instances currently held, warm and reusable (diagnostics).
    pub fn idle_instances(&self) -> usize {
        self.idle.lock().instances()
    }

    /// Fresh (pre-instantiated, never used) instances currently held (diagnostics).
    pub fn warm_instances(&self) -> usize {
        self.idle.lock().map.values().map(|l| l.fresh.len()).sum()
    }

    fn note_artifact(&mut self, p: &Arc<WasmPrepared>) {
        if self
            .seen
            .get(&p.key)
            .is_some_and(|w| std::ptr::eq(w.as_ptr(), Arc::as_ptr(p)))
        {
            return;
        }
        warm::insert_pruning(&mut self.seen, p.key, Arc::downgrade(p), SEEN_PRUNE_AT);
        warm::remember(p);
    }

    /// Shared by `between_tasks` and `on_idle`: shed at pressure, then (with a non-zero budget)
    /// finish in-flight pre-instantiations, scale down, and pre-instantiate. One unit of work is
    /// one poll of a pre-instantiation; another starts only while less than `budget` elapsed (a
    /// unit itself is bounded by one epoch tick of guest start code, see `warm::prewarm_job`).
    fn housekeep(&mut self, budget: Duration) {
        if self.host.deps().mem.pressure() >= Pressure::High {
            // Idle and warm instances are evictable memory.
            self.prewarming.clear();
            let shed = {
                let mut pool = self.idle.lock();
                for list in pool.map.values_mut() {
                    list.pending = 0;
                }
                pool.drain_all()
            };
            keepalive(&self.metrics, "shed", shed.iter().filter(|i| i.uses > 0).count());
            drop(shed);
            return;
        }
        if budget.is_zero() {
            return;
        }
        let started = Instant::now();
        let mut units = 0u32;
        self.poll_prewarming(started, budget, &mut units);

        let targets = self.warm.as_ref().map(|w| w.snapshot());
        let want = |k: &IdleKey| -> usize { want_of(targets.as_deref(), k) };
        let warm = self.warm.clone();
        let has_waiting = |s: ServiceId| warm.as_ref().is_some_and(|w| w.waiting(s) > 0);
        let dropped = self.idle.lock().scale_down(Instant::now(), want, has_waiting);
        keepalive(
            &self.metrics,
            "expired",
            dropped.iter().filter(|i| i.expires.is_some()).count(),
        );
        drop(dropped);
        self.backoff.retain(|k, _| want(k) > 0);

        let (Some(targets), Some(proc)) = (targets.as_ref(), self.proc) else {
            return;
        };
        let keys: Vec<(IdleKey, ServiceRef, usize)> = targets
            .iter()
            .filter(|(wk, t)| wk.key.kind == RuntimeKind::Wasm && t.service.service == wk.service && t.per_proc > 0)
            .map(|(wk, t)| {
                let key = IdleKey {
                    tenant: t.service.tenant,
                    service: wk.service,
                    version: t.service.version,
                    prepared: wk.key,
                    proc,
                };
                (key, t.service, (t.per_proc as usize).min(MAX_WARM_PER_KEY))
            })
            .collect();
        let start = self.cursor;
        self.cursor = self.cursor.wrapping_add(1);
        for i in rotation(keys.len(), start) {
            let (key, svc, target) = keys[i];
            loop {
                // A failed pre-instantiation rests the key, within this call as well as across calls.
                if self.backoff.get(&key).is_some_and(|b| Instant::now() < b.until) {
                    break;
                }
                if units > 0 && started.elapsed() >= budget {
                    return;
                }
                if self.idle.lock().ready(&key) >= target {
                    break;
                }
                let Some(prepared) = warm::lookup(&key.prepared, &self.engine) else {
                    break;
                };
                // The node's idle budget is full: warm capacity never evicts (only tasks do).
                let Some(slot) = self.slots.try_acquire() else { return };
                // The governor refused even the smallest reservation: nothing more this round.
                let Some(job) = prewarm_job(&self.engine, &self.host, svc, prepared, slot) else {
                    return;
                };
                self.idle.lock().list(key).pending += 1;
                units += 1;
                self.poll_job(key, job);
            }
        }
    }

    /// Poll a pre-instantiation once; a job that yielded is kept for the next hook call.
    fn poll_job(&mut self, key: IdleKey, mut job: PrewarmJob) {
        let mut cx = Context::from_waker(noop_waker_ref());
        match job.as_mut().poll(&mut cx) {
            Poll::Ready(inst) => self.finish_prewarm(key, inst),
            Poll::Pending => self.prewarming.push((key, job)),
        }
    }

    fn poll_prewarming(&mut self, started: Instant, budget: Duration, units: &mut u32) {
        let mut i = 0;
        while i < self.prewarming.len() {
            if *units > 0 && started.elapsed() >= budget {
                return;
            }
            *units += 1;
            let mut cx = Context::from_waker(noop_waker_ref());
            if let Poll::Ready(inst) = self.prewarming[i].1.as_mut().poll(&mut cx) {
                let (key, _) = self.prewarming.swap_remove(i);
                self.finish_prewarm(key, inst);
            } else {
                i += 1;
            }
        }
    }

    fn finish_prewarm(&mut self, key: IdleKey, inst: Option<IdleInstance>) {
        let ok = inst.is_some();
        {
            let mut pool = self.idle.lock();
            let list = pool.list(key);
            list.pending = list.pending.saturating_sub(1);
            if let Some(inst) = inst {
                list.fresh.push(inst);
                list.last_used = Instant::now();
            }
        }
        if ok {
            self.backoff.remove(&key);
        } else {
            let prev = self.backoff.get(&key).copied();
            self.backoff.insert(key, Backoff::after_failure(prev, Instant::now()));
        }
    }
}

/// The warm target of `k` on its P (0 without one), capped at [`MAX_WARM_PER_KEY`].
fn want_of(targets: Option<&HashMap<WarmKey, WarmTarget>>, k: &IdleKey) -> usize {
    targets
        .and_then(|t| {
            t.get(&WarmKey {
                service: k.service,
                key: k.prepared,
            })
        })
        .filter(|t| t.service.tenant == k.tenant && t.service.version == k.version)
        .map_or(0, |t| (t.per_proc as usize).min(MAX_WARM_PER_KEY))
}

/// Count `n` `rt_instance_keepalive_total{kind="wasm",outcome}`.
fn keepalive(metrics: &Option<Arc<Metrics>>, outcome: &str, n: usize) {
    if let (Some(m), true) = (metrics, n > 0) {
        m.instance_keepalive_total
            .with_label_values(&["wasm", outcome])
            .inc_by(n as u64);
    }
}

impl Executor for WasmExecutor {
    const KIND: TaskKind = TaskKind::WASM;
    type Prepared = WasmPrepared;

    fn start(&mut self, spec: TaskSpec, prepared: Arc<WasmPrepared>, cx: ExecCx) -> TaskFuture {
        self.proc = Some(cx.proc);
        if self.warm.is_some() {
            self.note_artifact(&prepared);
        }
        Box::pin(run_task(Args {
            spec,
            prepared,
            cx,
            engine: self.engine.clone(),
            host: self.host.clone(),
            interrupter: self.interrupter.clone(),
            idle: self.idle.clone(),
            slots: self.slots.clone(),
            reuse_max_idle: self.reuse_max_idle,
            signal: self.signal.clone(),
            metrics: self.metrics.clone(),
            warm: self.warm.clone(),
        }))
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        self.interrupter.clone()
    }

    /// The earliest own keep-alive end (`min_run_time`) of a used idle instance that is still
    /// ahead: past ones were dropped by the housekeeping before the park, or are kept by the warm
    /// floor (never expire), so they are not reported (that would spin the Machine).
    fn next_deadline(&self) -> Option<Instant> {
        let now = Instant::now();
        let pool = self.idle.lock();
        pool.map
            .values()
            .flat_map(|l| l.used.iter())
            .filter_map(|i| i.expires)
            .filter(|e| *e > now)
            .min()
    }

    fn wake_signal(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.signal.clone())
    }

    fn on_idle(&mut self, budget: Duration) {
        self.housekeep(budget);
    }

    fn between_tasks(&mut self, budget: Duration) {
        self.housekeep(budget);
    }
}

// ---------------------------------------------------------------------------------------------
// CPU accounting
// ---------------------------------------------------------------------------------------------

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a valid out-pointer for the duration of the call.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
        return 0; // LCOV_EXCL_LINE: clock_gettime on the thread CPU clock does not fail on supported platforms
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// Thread CPU time spent *inside this task's polls* (other tasks interleaved on the same Machine
/// are excluded). Read from the epoch-deadline callback, which runs inside a poll.
struct CpuAcc {
    total_ns: AtomicU64,
    /// Thread CPU clock at the start of the current poll; `u64::MAX` outside a poll.
    poll_start: AtomicU64,
    polls: AtomicU32,
    preemptions: AtomicU32,
}

impl CpuAcc {
    fn new() -> Self {
        Self {
            total_ns: AtomicU64::new(0),
            poll_start: AtomicU64::new(u64::MAX),
            polls: AtomicU32::new(0),
            preemptions: AtomicU32::new(0),
        }
    }

    fn begin(&self) {
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.poll_start.store(thread_cpu_ns(), Ordering::Relaxed);
    }

    fn end(&self) {
        let start = self.poll_start.swap(u64::MAX, Ordering::Relaxed);
        if start != u64::MAX {
            self.total_ns
                .fetch_add(thread_cpu_ns().saturating_sub(start), Ordering::Relaxed);
        }
    }

    fn current(&self) -> Duration {
        let start = self.poll_start.load(Ordering::Relaxed);
        let live = if start == u64::MAX {
            0
        } else {
            thread_cpu_ns().saturating_sub(start)
        };
        Duration::from_nanos(self.total_ns.load(Ordering::Relaxed).saturating_add(live))
    }
}

/// Polls `inner` while charging the thread CPU time of each poll to `acc`.
struct Measured<'a, F> {
    inner: Pin<&'a mut F>,
    acc: &'a CpuAcc,
}

impl<F: Future> Future for Measured<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let acc = self.acc;
        acc.begin();
        let r = self.inner.as_mut().poll(cx);
        acc.end();
        r
    }
}

// ---------------------------------------------------------------------------------------------
// One task
// ---------------------------------------------------------------------------------------------

struct Args {
    spec: TaskSpec,
    prepared: Arc<WasmPrepared>,
    cx: ExecCx,
    engine: Engine,
    host: HostServices,
    interrupter: Arc<WasmInterrupter>,
    idle: Arc<Mutex<IdlePool>>,
    slots: Arc<IdleSlots>,
    reuse_max_idle: usize,
    signal: Arc<tokio::sync::Notify>,
    metrics: Option<Arc<Metrics>>,
    warm: Option<Arc<WarmTargets>>,
}

struct Unregister {
    interrupter: Arc<WasmInterrupter>,
    task: TaskId,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        self.interrupter.unregister(self.task);
    }
}

pub(crate) fn build_wasi_ctx(clock: &Arc<FrozenClock>, logs: &Arc<GuestLogs>, env: &EnvBundle) -> WasiCtx {
    let mut b = WasiCtxBuilder::new();
    // No preopens, no sockets (every address denied), empty stdin: the builder's defaults.
    b.stdout(logs.stdout_stream()).stderr(logs.stderr_stream());
    b.monotonic_clock(clock.monotonic()).wall_clock(clock.wall());
    for (k, v) in env.env.iter().chain(env.secrets.iter()) {
        b.env(k, v);
    }
    b.build()
}

/// The request the guest's `incoming-handler` receives. Non-HTTP triggers arrive as synthetic
/// requests.
fn guest_request(spec: &mut TaskSpec, task: TaskId, deadline_ms: i64) -> http::Request<Body> {
    let trigger = spec.trigger.label();
    // `wasi:http` hides `Upgrade` from guests: a WebSocket upgrade offer is `x-rt-websocket: 1`.
    let websocket = matches!(&spec.payload, Payload::Http(r)
        if r.headers.get(http::header::UPGRADE).and_then(|v| v.to_str().ok()).is_some_and(|v| v.trim().eq_ignore_ascii_case("websocket")));
    // A synthetic trigger is a JSON `POST` to a reserved path.
    let json_post = |path: &'static str, json: serde_json::Value| {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let body = Body::from(serde_json::to_vec(&json).unwrap_or_default());
        (http::Method::POST, http::Uri::from_static(path), headers, body)
    };
    let (method, uri, mut headers, body) = match std::mem::take(&mut spec.payload) {
        Payload::Http(HttpRequest {
            method,
            uri,
            headers,
            body,
            ..
        }) => (method, uri, headers, body),
        Payload::Schedule(ev) => json_post(
            "/__rt/scheduled",
            serde_json::json!({
                "scheduleId": ev.schedule_id.to_string(),
                "scheduledTime": ev.scheduled_time_ms,
                "payload": ev.payload,
            }),
        ),
        Payload::Delayed(ev) => json_post(
            "/__rt/scheduled",
            serde_json::json!({ "id": ev.id, "scheduledAt": ev.scheduled_at_ms, "payload": ev.payload }),
        ),
        Payload::Queue(messages) => {
            let items: Vec<serde_json::Value> = messages
                .iter()
                .map(|m| {
                    let body = serde_json::from_slice::<serde_json::Value>(&m.body)
                        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&m.body).into_owned()));
                    serde_json::json!({ "id": m.id.to_string(), "body": body, "attempt": m.attempt })
                })
                .collect();
            json_post("/__rt/queue", serde_json::json!({ "messages": items }))
        }
        Payload::Pubsub(m) => json_post("/__rt/pubsub", m.guest_json()),
        Payload::Stream(b) => json_post("/__rt/stream", b.guest_json()),
        // Socket events reach a component through its `socket-handler` export, never this path
        // (an HTTP-only component gets none: the hub only runs events of components that export it).
        Payload::Socket(_) | Payload::None => (
            http::Method::GET,
            http::Uri::from_static("/"),
            http::HeaderMap::new(),
            Body::empty(),
        ),
    };
    // `wasi:http` needs an authority: the handler URI is path + query, so without a `Host` header
    // (synthetic triggers, hygiene-stripped requests) the guest gets `localhost`, like the Deno executor.
    if uri.authority().is_none() && !headers.contains_key(http::header::HOST) {
        headers.insert(http::header::HOST, HeaderValue::from_static("localhost"));
    }
    // Runtime-set headers (any guest-visible `x-rt-*` the caller put there is overwritten).
    headers.insert(HeaderName::from_static("x-rt-task-id"), HeaderValue::from(task.0));
    headers.insert(
        HeaderName::from_static("x-rt-trigger"),
        HeaderValue::from_static(trigger),
    );
    if websocket {
        headers.insert(HeaderName::from_static("x-rt-websocket"), HeaderValue::from_static("1"));
    } else {
        headers.remove("x-rt-websocket");
    }
    headers.insert(HeaderName::from_static("x-rt-deadline"), HeaderValue::from(deadline_ms));
    let mut req = http::Request::new(body);
    *req.method_mut() = method;
    *req.uri_mut() = uri;
    *req.headers_mut() = headers;
    req
}

/// An RPC call to run through the `std:host/rpc` export.
struct RpcRun {
    method: String,
    args: String,
    caller: Option<rpc_wit::Caller>,
}

enum RpcCall {
    /// Not an RPC: the HTTP handler runs.
    No,
    /// Answered without guest code (this envelope).
    Answer(String),
    Run(RpcRun),
}

/// The empty `200` of an event task: the event was handled (or the component takes none).
fn event_ok_response() -> http::Response<HyperOutgoingBody> {
    let body = http_body_util::Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed_unsync();
    http::Response::new(body)
}

/// The RPC a call task carries: a call trigger for `/__rt/rpc/<method>`. Its
/// body (the JSON argument, at most 256 KiB) is read here.
async fn rpc_call(spec: &mut TaskSpec) -> RpcCall {
    if !matches!(spec.trigger, Trigger::Call { .. }) {
        return RpcCall::No;
    }
    let Payload::Http(req) = &mut spec.payload else {
        return RpcCall::No;
    };
    let Some(method) = rpc_method_of(req.uri.path()) else {
        return RpcCall::No;
    };
    let refused = |m: &str| RpcCall::Answer(envelope_err(&RpcError::new("validation_failed", m)));
    if !valid_method(&method) {
        return RpcCall::Answer(envelope_err(&RpcError::new(
            "method_not_found",
            format!("no RPC method {method}"),
        )));
    }
    let body = std::mem::take(&mut req.body);
    let Ok(bytes) = body.collect(RPC_JSON_MAX_BYTES as u64).await else {
        return refused("the argument is larger than 256 KiB");
    };
    let Ok(text) = String::from_utf8(bytes.to_vec()) else {
        return refused("the argument is not JSON");
    };
    let args = match check_args(&text) {
        Ok(args) => args.to_string(),
        Err(e) => return RpcCall::Answer(envelope_err(&e)),
    };
    let caller = spec.caller.as_ref().map(|c| rpc_wit::Caller {
        service: c.service.to_string(),
        slug: c.slug.clone(),
        project: c.project.map(|p| p.to_string()),
        environment: c.environment.clone(),
        tenant: c.tenant.to_string(),
        plan: c.plan.clone(),
    });
    RpcCall::Run(RpcRun { method, args, caller })
}

/// The task's outcome when the host answers an RPC itself.
fn rpc_answer(text: String) -> Outcome {
    let reply = rpc_envelope_response(text);
    let (parts, body) = reply.into_parts();
    let mut r = Reply::new(parts.status, body);
    r.headers = parts.headers;
    Outcome::Ok(r)
}

/// An RPC envelope as the guest response a `wasi:http` handler would have sent.
fn rpc_http_response(text: String) -> http::Response<HyperOutgoingBody> {
    let body = http_body_util::Full::new(Bytes::from(text))
        .map_err(|never| match never {})
        .boxed_unsync();
    let mut resp = http::Response::new(body);
    resp.headers_mut()
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn deadline_unix_ms(deadline: Instant) -> i64 {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (now + remaining).as_millis() as i64
}

/// What a failed guest call means for the task (kill reasons first, then limiter, then fuel).
fn failure(store: &Store<WasmState>, switch: &KillSwitch, e: &wasmtime::Error) -> Outcome {
    if let Some(reason) = switch.reason() {
        return Outcome::Killed(reason);
    }
    if store.data().limiter.oom {
        return Outcome::Killed(KillReason::Oom);
    }
    if matches!(e.downcast_ref::<wasmtime::Trap>(), Some(wasmtime::Trap::OutOfFuel)) {
        return Outcome::Killed(KillReason::CpuLimit);
    }
    Outcome::GuestError
}

/// The pooling allocator had no free slot (instances, memories, tables or stacks).
fn pool_exhausted(e: &wasmtime::Error) -> bool {
    e.downcast_ref::<wasmtime::PoolConcurrencyLimitError>().is_some()
        || format!("{e:#}").contains("maximum concurrent limit of")
}

/// The task's limits on a store, before any guest code runs on it (instantiation included):
/// deterministic fuel backstop (only when the engine meters fuel), epoch slice, and the callback
/// that yields at each slice or turns a kill into a trap.
struct TaskLimits {
    deadline: Instant,
    time_slice: Duration,
    cpu_budget: Duration,
}

fn set_limits(
    store: &mut Store<WasmState>,
    l: &TaskLimits,
    acc: &Arc<CpuAcc>,
    switch: &KillSwitch,
    handle: &HostHandle,
) {
    let fuel = (l.cpu_budget.as_millis() as u64)
        .saturating_mul(FUEL_PER_CPU_MS)
        .max(FUEL_PER_CPU_MS);
    let _ = store.set_fuel(fuel);
    let ticks = ((l.time_slice.as_micros() / EPOCH_TICK.as_micros().max(1)) as u64).max(1);
    store.set_epoch_deadline(ticks);
    let (acc, switch, handle) = (acc.clone(), switch.clone(), handle.clone());
    let (deadline, cpu_budget) = (l.deadline, l.cpu_budget);
    store.epoch_deadline_callback(move |_cx| {
        if switch.reason().is_some() {
            return Ok(UpdateDeadline::Interrupt);
        }
        if Instant::now() >= deadline {
            switch.set(KillReason::WallTimeout);
            return Ok(UpdateDeadline::Interrupt);
        }
        if acc.current() + handle.host_cpu() >= cpu_budget {
            switch.set(KillReason::CpuLimit);
            return Ok(UpdateDeadline::Interrupt);
        }
        acc.preemptions.fetch_add(1, Ordering::Relaxed);
        Ok(UpdateDeadline::Yield(ticks))
    });
}

fn new_store(
    engine: &Engine,
    reservation: Reservation,
    limit_bytes: usize,
    handle: &HostHandle,
    env: &EnvBundle,
) -> Store<WasmState> {
    let clock = FrozenClock::new();
    let logs = GuestLogs::new();
    logs.attach(handle.clone());
    let state = WasmState {
        wasi_ctx: build_wasi_ctx(&clock, &logs, env),
        http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        limiter: GovernorLimiter::new(reservation, limit_bytes),
        http_hooks: GuardedHttpHooks { host: handle.clone() },
        host: handle.clone(),
        clock,
        logs,
    };
    let mut store = Store::new(engine, state);
    store.limiter(|s| &mut s.limiter);
    store
}

async fn run_task(a: Args) -> Outcome {
    let Args {
        mut spec,
        prepared,
        cx,
        engine,
        host,
        interrupter,
        idle,
        slots,
        reuse_max_idle,
        signal,
        metrics,
        warm,
    } = a;
    let ExecCx {
        task,
        proc,
        reservation,
        deadline,
        time_slice,
        cpu_budget,
        ..
    } = cx;
    let switch = interrupter.register(task);
    let unregister = Unregister {
        interrupter: interrupter.clone(),
        task,
    };

    let cfg = host.config_for(&spec).await;
    let handle = host.handle_for(&spec, task, deadline, cfg);
    let env: Env = match handle.env().await {
        Ok(env) => env,
        Err(e) => {
            handle.log_system(LogLevel::Error, format!("environment unavailable: {}", e.code));
            return Outcome::GuestError;
        }
    };
    let svc = spec.service;
    let limit_bytes = (spec.limits.memory_mb as usize) << 20;
    let reuse = spec.knobs.reuse_instances;
    let idle_ttl = spec.knobs.idle_ttl;
    let min_run_time = spec.knobs.min_run_time;
    let min_instances = spec.knobs.min_instances;
    let queue_timeout = spec.knobs.queue_timeout;
    let key = IdleKey {
        tenant: svc.tenant,
        service: svc.service,
        version: svc.version,
        prepared: prepared.key,
        proc,
    };
    let limits = TaskLimits {
        deadline,
        time_slice,
        cpu_budget,
    };
    let acc = Arc::new(CpuAcc::new());

    let t_init = Instant::now();
    let mut reservation = Some(reservation);

    // 1. An idle instance of this service on this P: a warm one, or a used one with reuse on.
    // An RPC to this service (`POST /__rt/rpc/<method>` from the call dispatcher): its method,
    // JSON argument and caller, read before an instance is taken. A component without the `rpc`
    // export, or a refused argument, is answered here without running guest code.
    let rpc = match rpc_call(&mut spec).await {
        RpcCall::No => None,
        RpcCall::Answer(text) => return rpc_answer(text),
        RpcCall::Run(call) if prepared.rpc.is_none() => {
            let e = RpcError::new("method_not_found", format!("no RPC method {}", call.method));
            return rpc_answer(envelope_err(&e));
        }
        RpcCall::Run(call) => Some(call),
    };
    // A socket event or alarm: through the `socket-handler` export. A component
    // without it takes no events: the event is answered done without running guest code.
    let event = match std::mem::take(&mut spec.payload) {
        Payload::Socket(ev) => {
            if prepared.socket.is_none() {
                handle.log_system(
                    LogLevel::Warn,
                    "socket event dropped: the component does not export socket-handler".to_string(),
                );
                let mut reply = Reply::new(http::StatusCode::OK, Body::empty());
                reply.timings = TaskTimings::default();
                return Outcome::Ok(reply);
            }
            Some(ev)
        }
        other => {
            spec.payload = other;
            None
        }
    };
    let object = spec
        .object
        .as_ref()
        .map_or_else(|| rt_types::DEFAULT_OBJECT.to_string(), |o| o.name.to_string());

    let mut taken: Option<(Store<WasmState>, GuestExports, u32)> = None;
    let Taken { inst, discard } = idle.lock().take(&key, reuse, idle_ttl);
    drop(discard);
    if let Some(inst) = inst {
        let IdleInstance {
            mut store,
            guest,
            uses,
            env: built_with,
            since: _,
            slot,
            expires: _,
            handoff: _,
        } = inst;
        drop(slot);
        let mut res = reservation.take().expect("reservation is taken once");
        let used = store.data().limiter.used;
        // A rotated secret (or any env change) discards the instance: wasi-libc caches the env.
        let fits = same_env(&built_with, &env)
            && used <= limit_bytes
            && (used as u64 <= res.bytes() || res.try_grow(used as u64 - res.bytes()));
        if fits {
            let st = store.data_mut();
            // Dropping the old reservation releases the idle (`wasm_reuse`) charge.
            st.limiter.reservation = res;
            st.limiter.oom = false;
            st.limiter.limit_bytes = limit_bytes;
            st.host = handle.clone();
            st.http_hooks.host = handle.clone();
            st.logs.attach(handle.clone());
            st.clock.resume();
            st.wasi_ctx = build_wasi_ctx(&st.clock, &st.logs, &env);
            set_limits(&mut store, &limits, &acc, &switch, &handle);
            if uses > 0 {
                keepalive(&metrics, "reused", 1);
            }
            taken = Some((store, guest, uses + 1));
        } else {
            reservation = Some(res);
        }
    }

    // 2. Otherwise a new store + instance. A pool exhausted by idle instances (of any Machine)
    // evicts the least recently used ones and retries; other failures evict nothing.
    let (store, guest, uses) = match taken {
        Some(t) => t,
        None => {
            let mut res = reservation.take().expect("reservation is taken once");
            let mut evictions = 0;
            loop {
                let mut store = new_store(&engine, res, limit_bytes, &handle, &env);
                set_limits(&mut store, &limits, &acc, &switch, &handle);
                match prepared.instantiate(&mut store).await {
                    Ok(guest) => break (store, guest, 1),
                    Err(e) => {
                        if pool_exhausted(&e)
                            && evictions < MAX_EVICTIONS
                            && switch.reason().is_none()
                            && slots.evict_lru()
                        {
                            evictions += 1;
                            res = store.into_data().limiter.reservation;
                            continue;
                        }
                        let outcome = failure(&store, &switch, &e);
                        handle.log_system(LogLevel::Error, format!("instantiation failed: {e:#}"));
                        store.data().logs.detach();
                        return outcome;
                    }
                }
            }
        }
    };
    let mut store = store;
    let init = t_init.elapsed();
    if let Some(m) = &metrics {
        m.wasm_instantiate_seconds.observe(init.as_secs_f64());
    }

    // The request: the HTTP handler's, or the RPC export's call.
    let (tx, resp_rx) = oneshot::channel();
    let entry = match (rpc, event) {
        (_, Some(ev)) => Entry::Event { ev, object, reply: tx },
        (Some(call), None) => Entry::Rpc { call, reply: tx },
        (None, None) => {
            let (parts, body) = guest_request(&mut spec, task, deadline_unix_ms(deadline)).into_parts();
            let frames = body.map(|r| {
                r.map(Frame::data)
                    .map_err(|e| ErrorCode::InternalError(Some(e.to_string())))
            });
            let hyper_req = http::Request::from_parts(parts, StreamBody::new(frames));
            // LCOV_EXCL_START: both fail only when the store's resource table cannot take one more entry, which a fresh store never reaches
            let Ok(incoming) = store.data_mut().http().new_incoming_request(Scheme::Http, hyper_req) else {
                store.data().logs.detach();
                return Outcome::GuestError;
            };
            let Ok(outparam) = store.data_mut().http().new_response_outparam(tx) else {
                store.data().logs.detach();
                return Outcome::GuestError;
            };
            // LCOV_EXCL_STOP
            Entry::Http { incoming, outparam }
        }
    };

    // Run: the driver owns the instance from here on; the outcome is decided by the head.
    let (head_tx, mut head_rx) = oneshot::channel();
    // Guest CPU measured inside this future (and so by the scheduler); `u64::MAX` until the head.
    let head_cpu = Arc::new(AtomicU64::new(u64::MAX));
    let t_exec = Instant::now();
    let driver = Driver {
        store,
        guest,
        uses,
        entry,
        resp_rx,
        head_tx,
        cap: spec.limits.max_response_stream_bytes,
        switch,
        acc: acc.clone(),
        head_cpu: head_cpu.clone(),
        deadline,
        handle: handle.clone(),
        host,
        idle,
        slots,
        key,
        svc,
        env,
        reuse,
        idle_ttl,
        reuse_max_idle,
        signal,
        unregister,
        warm,
        min_run_time,
        min_instances,
        queue_timeout,
        metrics: metrics.clone(),
    };
    // Until the head the guest runs inside this future's polls: the scheduler's per-poll
    // accounting measures, bills and limits it (WSM-18).
    let mut driver = Box::pin(driver.run());
    let mut done = false;
    let head = loop {
        tokio::select! {
            biased;
            h = &mut head_rx => break h,
            () = &mut driver, if !done => done = true,
        }
    };
    head_cpu.store(
        acc.current().as_nanos().min(u128::from(u64::MAX - 1)) as u64,
        Ordering::Release,
    );
    if !done {
        // The tail (streaming the body) continues as a local task; its CPU is charged when it ends.
        tokio::task::spawn_local(driver);
    }
    let timings = TaskTimings {
        init,
        exec: t_exec.elapsed(),
        cpu: acc.current() + handle.host_cpu(),
        polls: acc.polls.load(Ordering::Relaxed),
        preemptions: acc.preemptions.load(Ordering::Relaxed),
        ..TaskTimings::default()
    };
    // The driver runs inside this future until the head arrives, so a dropped sender means the
    // driver is gone: the Machine is shutting down.
    match head.unwrap_or(HeadMsg::Ended(Outcome::Killed(KillReason::Shutdown))) {
        HeadMsg::Ok { parts, body } => {
            let mut reply = Reply::new(parts.status, body);
            reply.headers = parts.headers;
            // A guest that accepted a WebSocket answers 101; the ticket travels with it.
            if reply.status == http::StatusCode::SWITCHING_PROTOCOLS
                && !reply.headers.contains_key(rt_types::TICKET_HEADER)
            {
                if let Some(v) = handle.accepted_ticket().and_then(|t| HeaderValue::from_str(&t).ok()) {
                    reply
                        .headers
                        .insert(HeaderName::from_static(rt_types::TICKET_HEADER), v);
                }
            }
            reply.timings = timings;
            Outcome::Ok(reply)
        }
        HeadMsg::Ended(outcome) => outcome,
    }
}

// ---------------------------------------------------------------------------------------------
// Driving the guest and streaming its response
// ---------------------------------------------------------------------------------------------

/// What the driver tells the task future.
enum HeadMsg {
    /// The response head; the body is buffered or streams through a channel.
    Ok { parts: http::response::Parts, body: Body },
    /// The task ended without a response.
    Ended(Outcome),
}

enum HeadState {
    /// The guest never sent a response.
    Missing,
    /// The guest answered with a `wasi:http` error code instead of a response.
    Error(ErrorCode),
    /// The guest finished within the run: head + whole body, handed over after cleanup.
    Buffered(http::response::Parts, Bytes),
    /// The head went out while the guest ran; the body streams.
    Streaming,
}

#[derive(Clone, Copy)]
enum StreamError {
    /// Past `max_response_stream_bytes`.
    TooLarge,
    /// The guest's outgoing body failed (e.g. dropped without `finish`).
    Body,
    /// The guest returned while still holding an unfinished outgoing body.
    Unfinished,
}

struct Streamed {
    run: wasmtime::Result<()>,
    head: HeadState,
    error: Option<StreamError>,
}

/// One response frame toward the client.
async fn forward<E>(
    frame: Result<Frame<Bytes>, E>,
    cap: u64,
    streamed: &mut u64,
    sender: &mut Option<BodySender>,
) -> Result<(), StreamError> {
    let Ok(frame) = frame else {
        return Err(StreamError::Body);
    };
    // Trailers are not forwarded.
    let Ok(data) = frame.into_data() else { return Ok(()) };
    *streamed = streamed.saturating_add(data.len() as u64);
    if *streamed > cap {
        return Err(StreamError::TooLarge);
    }
    if let Some(s) = sender.as_ref() {
        if s.send(data).await.is_err() {
            // The client went away: the guest runs on (within its limits), its output is dropped.
            *sender = None;
        }
    }
    Ok(())
}

/// Drive the guest call, receive the response head and the body frames. Returns when the guest
/// returned (and what it wrote is forwarded) or a frame failed. `run` is polled first each round,
/// so a guest that completes in one run takes the buffered path. After the guest returned, its
/// body must have ended: one still open (never finished nor dropped) is
/// [`StreamError::Unfinished`].
///
/// `gate` (a request to a durable object, the output gate): `run` ends with the storage
/// commit, and nothing of the response may leave before it. The head and the frames written so
/// far are then held here (up to `cap`) and handed over buffered once `run` returned.
async fn stream_response<R>(
    run: &mut R,
    rx: oneshot::Receiver<RespResult>,
    cap: u64,
    head_tx: &mut Option<oneshot::Sender<HeadMsg>>,
    sender: &mut Option<BodySender>,
    gate: bool,
) -> Streamed
where
    R: Future<Output = wasmtime::Result<()>> + Unpin,
{
    let mut head_rx = Some(rx);
    let mut head = HeadState::Missing;
    let mut stream: Option<HyperOutgoingBody> = None;
    let mut streamed = 0u64;
    // Gated: the head and the body so far, held until the run (and its commit) finished.
    let mut held: Option<(http::response::Parts, BytesMut)> = None;

    loop {
        tokio::select! {
            biased;
            r = &mut *run => {
                // A trap or kill decides the task; the body's state does not matter then.
                if r.is_err() {
                    return Streamed { run: r, head, error: None };
                }
                if let Some((parts, mut buf)) = held.take() {
                    if let Some(body) = stream.as_mut() {
                        loop {
                            match poll_immediate(body.frame()).await {
                                Some(Some(Ok(frame))) => {
                                    if let Ok(data) = frame.into_data() {
                                        if (buf.len() + data.len()) as u64 > cap {
                                            return Streamed { run: r, head, error: Some(StreamError::TooLarge) };
                                        }
                                        buf.extend_from_slice(&data);
                                    }
                                }
                                Some(Some(Err(_))) => return Streamed { run: r, head, error: Some(StreamError::Body) },
                                Some(None) => break,
                                None => return Streamed { run: r, head, error: Some(StreamError::Unfinished) },
                            }
                        }
                    }
                    return Streamed { run: r, head: HeadState::Buffered(parts, buf.freeze()), error: None };
                }
                // The guest returned: the head may still sit in the channel, and whatever the
                // body pipe holds now is all there will ever be.
                if let Some(Ok(resp)) = head_rx.take().map(|mut rx| rx.try_recv()) {
                    match resp {
                        Ok(resp) => {
                            let (parts, mut body) = resp.into_parts();
                            let mut buf = BytesMut::new();
                            loop {
                                match poll_immediate(body.frame()).await {
                                    Some(Some(Ok(frame))) => {
                                        if let Ok(data) = frame.into_data() {
                                            if (buf.len() + data.len()) as u64 > cap {
                                                return Streamed { run: r, head, error: Some(StreamError::TooLarge) };
                                            }
                                            buf.extend_from_slice(&data);
                                        }
                                    }
                                    Some(Some(Err(_))) => return Streamed { run: r, head, error: Some(StreamError::Body) },
                                    Some(None) => break,
                                    None => return Streamed { run: r, head, error: Some(StreamError::Unfinished) },
                                }
                            }
                            head = HeadState::Buffered(parts, buf.freeze());
                        }
                        Err(code) => head = HeadState::Error(code),
                    }
                } else if let Some(body) = stream.as_mut() {
                    loop {
                        match poll_immediate(body.frame()).await {
                            Some(Some(frame)) => {
                                if let Err(e) = forward(frame, cap, &mut streamed, sender).await {
                                    return Streamed { run: r, head, error: Some(e) };
                                }
                            }
                            Some(None) => break,
                            None => return Streamed { run: r, head, error: Some(StreamError::Unfinished) },
                        }
                    }
                }
                return Streamed { run: r, head, error: None };
            }
            h = async { head_rx.as_mut().expect("guarded by the branch condition").await }, if head_rx.is_some() => {
                head_rx = None;
                match h {
                    Ok(Ok(resp)) if gate => {
                        let (parts, body) = resp.into_parts();
                        held = Some((parts, BytesMut::new()));
                        stream = Some(body);
                    }
                    Ok(Ok(resp)) => {
                        let (parts, body) = resp.into_parts();
                        let (tx, client_body) = rt_types::body_channel(BODY_CHANNEL_CAPACITY);
                        if let Some(h) = head_tx.take() {
                            let _ = h.send(HeadMsg::Ok { parts, body: client_body });
                        }
                        *sender = Some(tx);
                        stream = Some(body);
                        head = HeadState::Streaming;
                    }
                    Ok(Err(code)) => head = HeadState::Error(code),
                    Err(_) => {}
                }
            }
            f = async { stream.as_mut().expect("guarded by the branch condition").frame().await }, if stream.is_some() => {
                match f {
                    Some(frame) if held.is_some() => {
                        let Ok(frame) = frame else {
                            let run = Err(wasmtime::Error::msg("response stream failed"));
                            return Streamed { run, head, error: Some(StreamError::Body) };
                        };
                        if let (Ok(data), Some((_, buf))) = (frame.into_data(), held.as_mut()) {
                            if (buf.len() + data.len()) as u64 > cap {
                                let run = Err(wasmtime::Error::msg("response stream failed"));
                                return Streamed { run, head, error: Some(StreamError::TooLarge) };
                            }
                            buf.extend_from_slice(&data);
                        }
                    }
                    Some(frame) => {
                        if let Err(e) = forward(frame, cap, &mut streamed, sender).await {
                            let run = Err(wasmtime::Error::msg("response stream failed"));
                            return Streamed { run, head, error: Some(e) };
                        }
                    }
                    None => stream = None,
                }
            }
        }
    }
}

/// What the task calls in the guest.
enum Entry {
    /// `wasi:http/incoming-handler` with the request and the response outparam.
    Http {
        incoming: wasmtime::component::Resource<HostIncomingRequest>,
        outparam: wasmtime::component::Resource<HostResponseOutparam>,
    },
    /// The `std:host/rpc` export; its envelope is sent on `reply` like a guest response.
    Rpc {
        call: RpcRun,
        reply: oneshot::Sender<RespResult>,
    },
    /// The `std:host/socket-handler` export for one event of `object`; then the commit, then an
    /// empty `200` on `reply`.
    Event {
        ev: rt_types::SocketEvent,
        object: String,
        reply: oneshot::Sender<RespResult>,
    },
}

/// Owns the instance of one task from the guest call to cleanup.
struct Driver {
    store: Store<WasmState>,
    guest: GuestExports,
    uses: u32,
    entry: Entry,
    resp_rx: oneshot::Receiver<RespResult>,
    head_tx: oneshot::Sender<HeadMsg>,
    cap: u64,
    switch: KillSwitch,
    acc: Arc<CpuAcc>,
    head_cpu: Arc<AtomicU64>,
    deadline: Instant,
    handle: HostHandle,
    host: HostServices,
    idle: Arc<Mutex<IdlePool>>,
    slots: Arc<IdleSlots>,
    key: IdleKey,
    svc: ServiceRef,
    env: Env,
    reuse: bool,
    idle_ttl: Duration,
    reuse_max_idle: usize,
    /// [`Executor::wake_signal`]: notified when an instance is stashed with its own expiry.
    signal: Arc<tokio::sync::Notify>,
    unregister: Unregister,
    /// Warm targets (the floor) and the admission backlog (`WarmTargets::waiting`).
    warm: Option<Arc<WarmTargets>>,
    min_run_time: Option<Duration>,
    /// `scaling.min_instances`: the only floor a `min_run_time = 0` instance is kept for.
    min_instances: u32,
    queue_timeout: Duration,
    metrics: Option<Arc<Metrics>>,
}

impl Driver {
    async fn run(self) {
        let Driver {
            mut store,
            guest,
            uses,
            entry,
            resp_rx,
            head_tx,
            cap,
            switch,
            acc,
            head_cpu,
            deadline,
            handle,
            host,
            idle,
            slots,
            key,
            svc,
            env,
            reuse,
            idle_ttl,
            reuse_max_idle,
            signal,
            unregister,
            warm,
            min_run_time,
            min_instances,
            queue_timeout,
            metrics,
        } = self;
        let mut head_tx = Some(head_tx);
        let mut sender: Option<BodySender> = None;

        // A request to an object answers only after its commit: no streamed head before it.
        let gate = matches!(entry, Entry::Http { .. }) && handle.object().is_some();
        let streamed: Result<Streamed, KillReason> = {
            let call: Pin<Box<dyn Future<Output = wasmtime::Result<()>> + '_>> = match entry {
                Entry::Http { incoming, outparam } => {
                    // A request to an object commits its storage writes and opens its output gate
                    // when the handler returned; its response is held until then (`gate`).
                    let commit = handle.object().is_some().then(|| handle.clone());
                    let (store, guest) = (&mut store, &guest);
                    Box::pin(async move {
                        guest
                            .proxy
                            .wasi_http_incoming_handler()
                            .call_handle(&mut *store, incoming, outparam)
                            .await?;
                        if let Some(h) = commit {
                            h.commit()
                                .await
                                .map_err(|e| wasmtime::Error::msg(format!("commit failed: {}", e.code)))?;
                        }
                        Ok(())
                    })
                }
                Entry::Event { ev, object, reply } => {
                    let export = guest.socket.as_ref();
                    let (store, h) = (&mut store, handle.clone());
                    Box::pin(async move {
                        let Some(export) = export else {
                            return Err(wasmtime::Error::msg("no socket-handler export"));
                        };
                        let r = call_socket_handler(export, &mut *store, &object, ev).await?;
                        if let Err(msg) = r {
                            h.log_system(LogLevel::Error, format!("event handler failed: {msg}"));
                            return Err(wasmtime::Error::msg("event handler failed"));
                        }
                        h.commit()
                            .await
                            .map_err(|e| wasmtime::Error::msg(format!("commit failed: {}", e.code)))?;
                        let _ = reply.send(Ok(event_ok_response()));
                        Ok(())
                    })
                }
                Entry::Rpc { call, reply } => {
                    let export = guest.rpc.as_ref();
                    let store = &mut store;
                    Box::pin(async move {
                        let Some(export) = export else {
                            return Err(wasmtime::Error::msg("no rpc export"));
                        };
                        let r = export
                            .call_handle(&mut *store, &call.method, &call.args, call.caller.as_ref())
                            .await?;
                        let text = match r {
                            Ok(value) => envelope_ok(&value),
                            Err(e) => envelope_err(&RpcError::new(e.code, e.message)),
                        };
                        let _ = reply.send(Ok(rpc_http_response(text)));
                        Ok(())
                    })
                }
            };
            let mut call = call;
            let mut run = Measured {
                inner: Pin::new(&mut call),
                acc: &acc,
            };
            let work = stream_response(&mut run, resp_rx, cap, &mut head_tx, &mut sender, gate);
            let killed = switch.notify.notified();
            let wall = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
            tokio::select! {
                s = work => Ok(s),
                _ = killed => Err(switch.reason().unwrap_or(KillReason::Shutdown)),
                _ = wall => Err(KillReason::WallTimeout),
            }
        };
        store.data().logs.detach();

        // Decide; log while the store is still here.
        let mut buffered: Option<(http::response::Parts, Bytes)> = None;
        let failed: Option<(Outcome, BodyError)> = match streamed {
            Err(reason) => {
                handle.log_system(LogLevel::Warn, format!("task killed: {}", reason.reason()));
                Some((Outcome::Killed(reason), BodyError::Aborted(reason.reason().to_string())))
            }
            Ok(Streamed {
                error: Some(StreamError::TooLarge),
                ..
            }) => {
                handle.log_system(LogLevel::Error, "response exceeds the size limit".to_string());
                Some((Outcome::GuestError, BodyError::TooLarge { limit: cap }))
            }
            Ok(Streamed {
                error: Some(StreamError::Body),
                ..
            }) => {
                handle.log_system(LogLevel::Error, "response body failed".to_string());
                Some((Outcome::GuestError, BodyError::Aborted("guest_error".into())))
            }
            Ok(Streamed {
                error: Some(StreamError::Unfinished),
                ..
            }) => {
                let msg = "response body failed: the guest returned without finishing it";
                handle.log_system(LogLevel::Error, msg.to_string());
                Some((Outcome::GuestError, BodyError::Aborted("guest_error".into())))
            }
            Ok(Streamed { run: Err(e), .. }) => {
                let outcome = failure(&store, &switch, &e);
                match &outcome {
                    Outcome::Killed(reason) => {
                        handle.log_system(LogLevel::Warn, format!("task killed: {}", reason.reason()))
                    }
                    _ => handle.log_system(LogLevel::Error, format!("guest trapped: {e:#}")),
                }
                let code = outcome.reason().unwrap_or("guest_error").to_string();
                Some((outcome, BodyError::Aborted(code)))
            }
            Ok(Streamed {
                run: Ok(()),
                head,
                error: None,
            }) => match head {
                HeadState::Missing => {
                    handle.log_system(LogLevel::Error, "guest returned without sending a response".to_string());
                    Some((Outcome::GuestError, BodyError::Aborted("guest_error".into())))
                }
                HeadState::Error(code) => {
                    handle.log_system(LogLevel::Error, format!("guest returned an http error: {code}"));
                    Some((Outcome::GuestError, BodyError::Aborted("guest_error".into())))
                }
                HeadState::Buffered(parts, body) => {
                    buffered = Some((parts, body));
                    None
                }
                HeadState::Streaming => None,
            },
        };

        // CPU after the head ran outside the task future: charge it to the tenant (WSM-18).
        let head_ns = head_cpu.load(Ordering::Acquire);
        if head_ns != u64::MAX {
            let total = acc.current().as_nanos().min(u128::from(u64::MAX)) as u64;
            handle.charge_cpu(Duration::from_nanos(total.saturating_sub(head_ns)));
        }

        // Clean completion: keep the instance for the next task of this service on this P, with
        // nothing of this task's left on it. Everything else drops it (and the task's memory).
        // `min_run_time` (the decision at the answer) only applies to such a clean instance.
        if failed.is_none() && reuse && uses < MAX_REUSES {
            let now = Instant::now();
            let waiting = warm.as_ref().map_or(0, |w| w.waiting(svc.service));
            let keep = match after_answer(min_run_time, waiting, queue_timeout) {
                AfterAnswer::Inherit => Some((None, false, "kept")),
                AfterAnswer::For(t) => Some((Some(now + t), false, "kept")),
                AfterAnswer::Handoff { cap } => Some((Some(now + cap), true, "handoff")),
                // Below the `min_instances` floor the instance stays (protected by the target, expired);
                // the autoscaler's demand target alone keeps nothing.
                AfterAnswer::Dispose => {
                    let want =
                        want_of(warm.as_ref().map(|w| w.snapshot()).as_deref(), &key).min(min_instances as usize);
                    (idle.lock().ready(&key) < want).then_some((Some(now), false, "kept"))
                }
            };
            match keep {
                Some((expires, handoff, outcome)) => {
                    let parked = Parked {
                        idle: &idle,
                        slots: &slots,
                        host: &host,
                        key,
                        svc,
                        max_idle: reuse_max_idle,
                        idle_ttl,
                        expires,
                        handoff,
                    };
                    if parked.stash(store, guest, uses, env) {
                        keepalive(&metrics, outcome, 1);
                        // A parked Machine re-reads `next_deadline` (this expiry may be the next).
                        signal.notify_one();
                    }
                }
                None => {
                    keepalive(&metrics, "disposed", 1);
                    drop(store);
                }
            }
        } else {
            drop(store);
        }
        // The task's `std:lock` holds go with it: an idle instance may keep the handle a while.
        handle.release_locks();
        // The task's host handle goes now: its usage (egress, tail CPU) is recorded here.
        drop(handle);
        drop(unregister);

        // Last: tell the client. Ending the body only now means the instance is already back on
        // its idle list when the response completes.
        match (failed, head_tx.take()) {
            (Some((outcome, _)), Some(tx)) => {
                let _ = tx.send(HeadMsg::Ended(outcome));
            }
            (Some((_, err)), None) => {
                if let Some(s) = sender.take() {
                    s.abort(err).await;
                }
            }
            (None, Some(tx)) => {
                // A clean run with the head still unsent always ended with a buffered response.
                let msg = buffered.map_or(HeadMsg::Ended(Outcome::GuestError), |(parts, body)| HeadMsg::Ok {
                    parts,
                    body: Body::from_bytes(body),
                });
                let _ = tx.send(msg);
            }
            (None, None) => drop(sender),
        }
    }
}

/// Call the export of one event: message, close, error or alarm.
async fn call_socket_handler(
    export: &crate::prepared::SocketExport,
    store: &mut Store<WasmState>,
    object: &str,
    ev: rt_types::SocketEvent,
) -> wasmtime::Result<Result<(), String>> {
    use crate::host_wit::wit::exports::std::host::socket_handler as sh;
    use crate::host_wit::wit::std::host::sockets as ws;
    let socket = ev.socket.map(crate::host_ws::info_wit);
    let need = |s: Option<ws::SocketInfo>| s.ok_or_else(|| wasmtime::Error::msg("socket event without a socket"));
    match ev.kind {
        rt_types::SocketEventKind::Message(d) => {
            let data = match d {
                rt_types::WsData::Text(t) => ws::Payload::Text(t),
                rt_types::WsData::Binary(b) => ws::Payload::Binary(b.to_vec()),
            };
            export.call_on_message(store, object, &need(socket)?, &data).await
        }
        rt_types::SocketEventKind::Close {
            code,
            reason,
            was_clean,
        } => {
            export
                .call_on_close(
                    store,
                    object,
                    &need(socket)?,
                    &sh::CloseInfo {
                        code,
                        reason,
                        was_clean,
                    },
                )
                .await
        }
        rt_types::SocketEventKind::Error { code, message } => {
            export
                .call_on_error(store, object, &need(socket)?, &code, &message)
                .await
        }
        rt_types::SocketEventKind::Alarm { scheduled_ms, retry } => {
            export.call_on_alarm(store, object, scheduled_ms, retry).await
        }
    }
}

/// Where a clean instance goes after its task.
struct Parked<'a> {
    idle: &'a Arc<Mutex<IdlePool>>,
    slots: &'a Arc<IdleSlots>,
    host: &'a HostServices,
    key: IdleKey,
    svc: ServiceRef,
    max_idle: usize,
    idle_ttl: Duration,
    expires: Option<Instant>,
    handoff: bool,
}

impl Parked<'_> {
    /// Return a clean instance to the per-service, per-P idle list. It keeps nothing of the task
    /// that ran on it: a detached host handle (no capabilities, no usage), no log target, an
    /// epoch callback without the task's state. Its memory stays reserved under `wasm_reuse`
    /// (evictable), and it holds a slot of the node's idle budget, taken from the least recently
    /// used idle instance of any Machine when the budget is full. Dropped instead when the node
    /// is under pressure, the governor refuses, or the list is full. `true` when parked.
    fn stash(self, mut store: Store<WasmState>, guest: GuestExports, uses: u32, env: Env) -> bool {
        let mem = &self.host.deps().mem;
        if self.max_idle == 0 || mem.pressure() >= Pressure::High {
            return false;
        }
        if self
            .idle
            .lock()
            .map
            .get(&self.key)
            .is_some_and(|l| l.used.len() >= self.max_idle)
        {
            return false;
        }
        let bytes = store.data().limiter.used.max(MIN_IDLE_BYTES) as u64;
        let owner = Owner::service(self.key.tenant, self.key.service);
        let Ok(reservation) = mem.reserve(MemCategory::WasmReuse, bytes, owner) else {
            return false;
        };
        let Some(slot) = self.slots.acquire_evicting() else {
            return false;
        };
        let parked = self.host.detached_handle(self.svc);
        let st = store.data_mut();
        // The task's `wasm_linear` reservation is released by this replacement.
        st.limiter.reservation = reservation;
        st.host = parked.clone();
        st.http_hooks.host = parked;
        store.epoch_deadline_callback(|_cx| Ok(UpdateDeadline::Interrupt));
        let inst = IdleInstance {
            store,
            guest,
            uses,
            env,
            since: Instant::now(),
            slot,
            expires: self.expires,
            handoff: self.handoff,
        };
        // The list had room above and only this Machine adds to it (others only evict).
        let mut pool = self.idle.lock();
        let list = pool.list(self.key);
        list.touch(self.idle_ttl);
        list.used.push(inst);
        true
    }
}

#[cfg(test)]
mod gate_tests {
    use super::*;

    /// A response whose body streams frames from `rx`.
    fn streaming_response(rx: tokio::sync::mpsc::Receiver<Bytes>) -> http::Response<HyperOutgoingBody> {
        let frames = tokio_stream_from(rx).map(|b| Ok::<_, wasmtime_wasi_http::Error>(Frame::data(b)));
        http::Response::new(http_body_util::StreamBody::new(frames).boxed_unsync())
    }

    fn tokio_stream_from(rx: tokio::sync::mpsc::Receiver<Bytes>) -> impl futures_util::Stream<Item = Bytes> {
        futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|b| (b, rx)) })
    }

    /// A request to a durable object (the output gate): the head and the body frames the guest
    /// wrote before its handler returned (and its storage committed) are held, not streamed, and
    /// go out together afterwards (security review known gap: WASM streams left before the commit).
    #[tokio::test]
    async fn a_gated_response_leaves_only_after_the_run() {
        let (resp_tx, resp_rx) = oneshot::channel::<RespResult>();
        let (head_tx, mut head_rx) = oneshot::channel::<HeadMsg>();
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let run = async move {
            let _ = resp_tx.send(Ok(streaming_response(frame_rx)));
            frame_tx.send(Bytes::from_static(b"one ")).await.expect("frame");
            let _ = done_rx.await; // the handler returns (and commits) only now
            frame_tx.send(Bytes::from_static(b"two")).await.expect("frame");
            drop(frame_tx);
            Ok::<(), wasmtime::Error>(())
        };
        let mut run = Box::pin(run);
        let mut head_tx = Some(head_tx);
        let mut sender = None;
        let work = stream_response(&mut run, resp_rx, 1 << 20, &mut head_tx, &mut sender, true);
        tokio::pin!(work);
        // Let the guest send its head and first frame.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut work)
                .await
                .is_err(),
            "still running"
        );
        assert!(head_rx.try_recv().is_err(), "nothing left before the handler returned");
        done_tx.send(()).expect("done");
        let s = work.await;
        assert!(s.run.is_ok() && s.error.is_none());
        match s.head {
            HeadState::Buffered(_, body) => assert_eq!(&body[..], b"one two"),
            _ => panic!("the gated response is buffered"),
        }
    }

    type Item = Result<Frame<Bytes>, wasmtime_wasi_http::Error>;

    fn data(s: &'static str) -> Item {
        Ok(Frame::data(Bytes::from_static(s.as_bytes())))
    }

    fn trailers() -> Item {
        Ok(Frame::trailers(http::HeaderMap::new()))
    }

    fn response_of(rx: tokio::sync::mpsc::Receiver<Item>) -> http::Response<HyperOutgoingBody> {
        let frames = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|f| (f, rx)) });
        http::Response::new(http_body_util::StreamBody::new(frames).boxed_unsync())
    }

    /// `stream_response` over a guest that sends its head, yields once (so the head is taken) and
    /// then queues `frames` and returns without yielding again, optionally pausing `linger` first.
    async fn run_guest(frames: Vec<Item>, gate: bool, linger: Option<Duration>, yield_first: bool) -> Streamed {
        let (resp_tx, resp_rx) = oneshot::channel::<RespResult>();
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<Item>(8);
        let run = async move {
            let _ = resp_tx.send(Ok(response_of(frame_rx)));
            if yield_first {
                tokio::task::yield_now().await;
            }
            for f in frames {
                frame_tx.send(f).await.expect("frame");
            }
            if let Some(d) = linger {
                tokio::time::sleep(d).await;
            }
            drop(frame_tx);
            Ok::<(), wasmtime::Error>(())
        };
        let mut run = Box::pin(run);
        let (head_tx, _head_rx) = oneshot::channel::<HeadMsg>();
        let mut head_tx = Some(head_tx);
        let mut sender = None;
        stream_response(&mut run, resp_rx, 1 << 20, &mut head_tx, &mut sender, gate).await
    }

    #[tokio::test]
    async fn a_held_response_whose_body_fails_as_the_run_ends_is_a_body_error() {
        let s = run_guest(
            vec![
                data("a"),
                Err(wasmtime_wasi_http::Error::InternalError(Some("cut".into()))),
            ],
            true,
            None,
            true,
        )
        .await;
        assert!(matches!(s.error, Some(StreamError::Body)));
    }

    #[tokio::test]
    async fn trailers_are_not_part_of_a_held_or_buffered_body_wherever_they_arrive() {
        // Queued when the run ends, after the head was taken (the output gate).
        let s = run_guest(vec![data("a"), trailers(), data("b")], true, None, true).await;
        assert!(
            matches!(&s.head, HeadState::Buffered(_, body) if &body[..] == b"ab"),
            "gated, at the end"
        );
        // Arriving while the guest still runs.
        let s = run_guest(vec![data("a"), trailers()], true, Some(Duration::from_millis(30)), true).await;
        assert!(
            matches!(&s.head, HeadState::Buffered(_, body) if &body[..] == b"a"),
            "gated, mid-run"
        );
        // A buffered response of a guest that finished in one run.
        let s = run_guest(vec![data("a"), trailers()], false, None, false).await;
        assert!(
            matches!(&s.head, HeadState::Buffered(_, body) if &body[..] == b"a"),
            "ungated"
        );
    }
}

#[cfg(test)]
mod guest_request_tests {
    use super::*;
    use rt_testkit::SpecBuilder;
    use rt_types::{DelayedEvent, MsgId, QueueMessage, ScheduleId, ScheduledEvent, Trigger};

    async fn synth(trigger: Trigger, payload: Payload) -> (http::request::Parts, serde_json::Value) {
        let mut spec = SpecBuilder::wasm().trigger(trigger).payload(payload).finish();
        let req = guest_request(&mut spec, TaskId(5), 1234);
        let (parts, body) = req.into_parts();
        let bytes = body.collect(1 << 20).await.expect("body");
        (parts, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    fn header<'a>(parts: &'a http::request::Parts, name: &str) -> &'a str {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn an_interval_fire_arrives_as_a_json_post_with_the_schedule_id_time_and_payload() {
        let ev = ScheduledEvent {
            schedule_id: ScheduleId::from_u128(9),
            scheduled_time_ms: 42,
            payload: serde_json::json!({"k": 1}),
        };
        let id = ev.schedule_id.to_string();
        let (parts, json) = synth(
            Trigger::Interval {
                id: ScheduleId::from_u128(9),
            },
            Payload::Schedule(ev),
        )
        .await;
        assert_eq!((parts.method.as_str(), parts.uri.path()), ("POST", "/__rt/scheduled"));
        assert_eq!(header(&parts, "content-type"), "application/json");
        assert_eq!(
            json,
            serde_json::json!({"scheduleId": id, "scheduledTime": 42, "payload": {"k": 1}})
        );
        assert_eq!(header(&parts, "x-rt-trigger"), "interval");
        assert_eq!(header(&parts, "x-rt-deadline"), "1234");
        assert_eq!(
            header(&parts, "host"),
            "localhost",
            "a synthetic request has an authority"
        );
    }

    #[tokio::test]
    async fn a_one_off_schedule_arrives_with_its_id_due_time_and_payload() {
        let ev = DelayedEvent {
            id: "job-1".into(),
            scheduled_at_ms: 7,
            payload: serde_json::json!("p"),
        };
        let (parts, json) = synth(Trigger::Scheduled { id: "job-1".into() }, Payload::Delayed(ev)).await;
        assert_eq!((parts.method.as_str(), parts.uri.path()), ("POST", "/__rt/scheduled"));
        assert_eq!(
            json,
            serde_json::json!({"id": "job-1", "scheduledAt": 7, "payload": "p"})
        );
        assert_eq!(header(&parts, "x-rt-trigger"), "schedule");
    }

    #[tokio::test]
    async fn queue_messages_arrive_as_a_batch_with_json_bodies_parsed_and_other_bodies_as_text() {
        let m = |n: u128, body: &'static [u8], attempt| QueueMessage {
            id: MsgId::from_u128(n),
            body: Bytes::from_static(body),
            attempt,
        };
        let batch = vec![m(1, br#"{"a":1}"#, 1), m(2, b"plain text", 3)];
        let ids: Vec<String> = batch.iter().map(|b| b.id.to_string()).collect();
        let trigger = Trigger::Queue {
            batch: batch.iter().map(|b| b.id).collect(),
            attempt: 1,
        };
        let (parts, json) = synth(trigger, Payload::Queue(batch)).await;
        assert_eq!((parts.method.as_str(), parts.uri.path()), ("POST", "/__rt/queue"));
        assert_eq!(
            json,
            serde_json::json!({"messages": [
                {"id": ids[0], "body": {"a": 1}, "attempt": 1},
                {"id": ids[1], "body": "plain text", "attempt": 3},
            ]})
        );
        assert_eq!(header(&parts, "x-rt-trigger"), "queue");
    }

    #[tokio::test]
    async fn a_task_without_input_gets_a_plain_get_of_the_root() {
        let (parts, json) = synth(Trigger::Http, Payload::None).await;
        assert_eq!((parts.method.as_str(), parts.uri.path()), ("GET", "/"));
        assert_eq!(json, serde_json::Value::Null);
    }

    fn http_with(headers: &[(&str, &str)]) -> Payload {
        let mut req = HttpRequest::get("/chat?room=1");
        for (k, v) in headers {
            req.headers.insert(
                HeaderName::from_bytes(k.as_bytes()).expect("name"),
                HeaderValue::from_str(v).expect("value"),
            );
        }
        Payload::Http(req)
    }

    #[tokio::test]
    async fn a_websocket_upgrade_offer_is_marked_for_the_guest_and_a_forged_mark_is_removed() {
        let (parts, _) = synth(Trigger::Http, http_with(&[("upgrade", " WebSocket ")])).await;
        assert_eq!(header(&parts, "x-rt-websocket"), "1");
        assert_eq!(parts.uri.to_string(), "/chat?room=1");

        let (parts, _) = synth(
            Trigger::Http,
            http_with(&[("x-rt-websocket", "1"), ("x-rt-trigger", "forged")]),
        )
        .await;
        assert!(
            !parts.headers.contains_key("x-rt-websocket"),
            "a client cannot claim an upgrade"
        );
        assert_eq!(
            header(&parts, "x-rt-trigger"),
            "http",
            "runtime headers overwrite the caller's"
        );
        assert_eq!(header(&parts, "x-rt-task-id"), "5");
    }

    #[tokio::test]
    async fn a_request_that_names_its_host_keeps_it() {
        let (parts, _) = synth(Trigger::Http, http_with(&[("host", "api.example.com")])).await;
        assert_eq!(header(&parts, "host"), "api.example.com");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::events::{PubsubMessage, StreamBatch, StreamRecord};
    use rt_types::{Digest, MsgId, RuntimeKind, ServiceId, ServiceRef, TaskKind, TenantId, VersionNo};

    fn spec(trigger: Trigger, payload: Payload) -> TaskSpec {
        let svc = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            version: VersionNo(1),
            digest: Digest::of(b"x"),
        };
        TaskSpec::new(TaskKind::Exec(RuntimeKind::Wasm), svc, trigger).with_payload(payload)
    }

    fn body_json(req: http::Request<Body>) -> serde_json::Value {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let buf = rt.block_on(req.into_body().collect(u64::MAX)).unwrap();
        serde_json::from_slice(&buf).unwrap()
    }

    /// Pub/sub and stream tasks reach a component as JSON `POST`s to `/__rt/pubsub` and
    /// `/__rt/stream` with `x-rt-trigger` set and the guest JSON as the body.
    #[test]
    fn guest_request_maps_pubsub_and_stream_to_synthetic_posts() {
        let msg = PubsubMessage {
            id: MsgId::from_u128(3),
            subject: "a.b".into(),
            body: bytes::Bytes::from_static(b"\"hi\""),
            headers: vec![],
            replyable: false,
            published_at_ms: 9,
        };
        let mut s = spec(
            Trigger::Pubsub {
                subject: "a.b".into(),
                depth: 1,
                reply: false,
            },
            Payload::Pubsub(msg.clone()),
        );
        let req = guest_request(&mut s, TaskId(1), 0);
        assert_eq!((req.method(), req.uri().path()), (&http::Method::POST, "/__rt/pubsub"));
        assert_eq!(req.headers()["x-rt-trigger"], "pubsub");
        assert_eq!(req.headers()[http::header::CONTENT_TYPE], "application/json");
        assert_eq!(body_json(req), msg.guest_json());

        let batch = StreamBatch {
            topic: "orders".into(),
            group: "default".into(),
            partition: 0,
            attempt: 1,
            records: vec![StreamRecord {
                partition: 0,
                offset: 3,
                key: None,
                headers: vec![("h".into(), "v".into())],
                body: bytes::Bytes::from_static(b"{}"),
                ts_ms: 1,
            }],
        };
        let mut s = spec(
            Trigger::Stream {
                topic: "orders".into(),
                partition: 0,
                attempt: 1,
            },
            Payload::Stream(batch),
        );
        let req = guest_request(&mut s, TaskId(2), 0);
        assert_eq!((req.method(), req.uri().path()), (&http::Method::POST, "/__rt/stream"));
        assert_eq!(req.headers()["x-rt-trigger"], "stream");
        let v = body_json(req);
        assert_eq!(
            (v["topic"].clone(), v["messages"][0]["offset"].clone()),
            (serde_json::json!("orders"), serde_json::json!(3))
        );
        assert_eq!(v["messages"][0]["headers"], serde_json::json!({"h": "v"}));
    }
}
