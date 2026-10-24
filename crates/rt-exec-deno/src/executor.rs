//! `DenoExecutor`: one instance per Machine (thread-local, `!Send`).
//!
//! Task lifecycle:
//! 1. **Isolate:** with `deno.reuse_isolates` (D-A4, `warm.rs`) an isolate kept for this service
//!    version, if its env is still current and its memory fits; else one from the pre-created
//!    pool or a fresh `JsRuntime` from the base snapshot. The V8 heap limit is topped up through
//!    the governor in 8 MB steps up to `limits.memory_mb` (near-heap-limit callback); a refusal
//!    terminates the task with `Killed(Oom)`.
//! 2. **Context:** per-task `OpState` with the `HostHandle`, env (fetched on the guest's first
//!    `Deno.env` read, zeroized on drop), the frozen clock (reset per task), the response
//!    channel, the request body (streamed from a resource, B4.4).
//! 3. **Load:** the module loader serves **only** from the prepared bundle; anything else fails
//!    with `module_not_in_bundle`. No network or disk at runtime.
//! 4. **Evaluate + dispatch:** `__rt.start(payload)` imports the entry module (already evaluated
//!    in a kept isolate) and calls the handler (Form A / Form B, synthetic requests for non-HTTP
//!    triggers).
//! 5. **Drive:** a local driver task polls the event loop (advancing the frozen clock first),
//!    enforces CPU/wall/memory limits (per task, also on a kept isolate), streams the response
//!    through a bounded channel and waits for `ctx.waitUntil` promises.
//! 6. **Finish:** a clean finish with reuse on keeps the isolate (see [`Driver::keep`]);
//!    otherwise the isolate goes to the shared dispose queue; it is disposed between tasks
//!    (`between_tasks`, one per call) or on `on_idle`, and inline only when the queue exceeds
//!    `deno.dispose_backlog` (a Machine saturated for long). The reservation is released with it.
//!
//! The pool (`deno.isolate_pool_per_p`) is refilled by the same two hooks: `between_tasks` keeps
//! it warm on a loaded Machine (which rarely parks), `on_idle` tops it up. Pooled isolates reserve
//! only `deno.isolate_base_mb`. The same hooks expire kept isolates (`reuse_idle_ttl`, warm
//! targets); pooled and kept isolates are shed at memory pressure `High`.
//!
//! **Warm floor (D-B2).** The scheduler publishes a warm target per service artifact
//! (`WarmTargets::per_proc`, never below the floor of `scaling.min_instances`). The executor holds
//! *at least that many* kept isolates of the service regardless of `reuse_idle_ttl` (the TTL only
//! expires the ones above the target), builds them between tasks when it holds fewer
//! ([`DenoExecutor::between_tasks`]: an isolate evaluates the entry module through a warm-up
//! dispatch that runs no handler, from the spec and artifact of the service's latest task; only
//! for a service with `reuse_isolates`), and keeps them through `High` pressure; `Critical`, a new
//! version or a falling target drops them. For a service without reuse the same target means
//! blank pooled isolates: the pool grows to the sum of those targets (a fresh isolate per task is
//! then taken from the pool instead of being created on the request path).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::{poll_fn, Future};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use deno_core::{v8, PollEventLoopOptions};
use futures_util::FutureExt;
use rt_host::{HostHandle, HostServices};
use rt_mem::{MemCategory, Owner, Pressure};
use rt_obs::{LogLevel, Metrics};
use rt_sched::{after_answer, AfterAnswer, ExecCx, Executor, Interrupter, TaskFuture, WarmKey};
use rt_types::{
    Body, BodyError, KillReason, Limits, Outcome, Payload, PreparedKey, Reply, ServiceId, ServiceKnobs, ServiceRef,
    TaskId, TaskKind, TaskSpec, TaskTimings, TenantId, Trigger,
};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::clock::ClockState;
use crate::config::DenoExecutorConfig;
use crate::cpu::CpuGuard;
use crate::interrupt::DenoInterrupter;
use crate::isolate::{create_isolate, Isolate, MIB};
use crate::memory::BUFFER_ALLOWANCE;
use crate::ops::{add_request_body, Head, TaskOps, ENV_FETCH_MAX_WAIT};
use crate::prepared::{init_v8_once, DenoPrepared};
use crate::warm::{env_hash, Keep, WarmId, WarmStore, REUSE_MAX_GROWTH};

/// State shared between the executor and its task drivers (both live on the Machine thread).
#[derive(Default)]
struct Shared {
    /// Idle, tenant-neutral isolates (single-use).
    pool: VecDeque<Isolate>,
    /// Finished isolates waiting for idle-time disposal.
    dispose: VecDeque<Isolate>,
    /// Evaluated isolates kept per service version (`deno.reuse_isolates`, D-A4).
    warm: WarmStore,
    /// What the warm floor builds from, per service: the latest task's spec and artifact (D-B2).
    templates: HashMap<(TenantId, ServiceId), Template>,
    /// Warm-ups running, per service (local tasks building a kept isolate).
    warming: HashMap<(TenantId, ServiceId), usize>,
    /// Numbers the ids of warm-up tasks.
    warmups: u64,
}

/// What a warm-up needs of a service: the limits and knobs of its latest task and its artifact.
struct Template {
    id: WarmId,
    prepared: Arc<DenoPrepared>,
    limits: Limits,
    knobs: ServiceKnobs,
    slug: Arc<str>,
    /// The latest task of the service.
    seen: Instant,
    /// No warm-up before this time (the last one did not produce a kept isolate).
    retry_at: Instant,
}

/// A template of a service that has no target and has not run for this long is forgotten.
const TEMPLATE_IDLE: Duration = Duration::from_secs(300);
/// After a warm-up that ends without a kept isolate (the module throws, leaves timers) the
/// service is left alone this long.
const WARMUP_BACKOFF: Duration = Duration::from_secs(30);

/// Ready isolates per Processor the scheduler wants kept for `id` (0: no target published).
fn target_of(cfg: &DenoExecutorConfig, id: &WarmId) -> usize {
    cfg.warm_targets.as_ref().map_or(0, |t| {
        t.per_proc(&WarmKey {
            service: id.service.service,
            key: id.key,
        }) as usize
    })
}

/// Tasks of `id`'s service waiting in admission (0 without a scheduler).
fn waiting_of(cfg: &DenoExecutorConfig, id: &WarmId) -> u32 {
    cfg.warm_targets.as_ref().map_or(0, |t| t.waiting(id.service.service))
}

/// Count `n` keep-alive events of `outcome` (`rt_instance_keepalive_total{kind="deno"}`).
fn keepalive(metrics: Option<&Metrics>, outcome: &str, n: usize) {
    if let (Some(m), true) = (metrics, n > 0) {
        m.instance_keepalive_total
            .with_label_values(&["deno", outcome])
            .inc_by(n as u64);
    }
}

/// How many isolates to keep for `id`: the scheduler's warm target when one is published, else
/// `reuse_max_idle`; the target's worth is exempt from the idle TTL.
fn warm_keep(cfg: &DenoExecutorConfig, id: &WarmId) -> Keep {
    let target = target_of(cfg, id);
    Keep {
        bound: if target > 0 {
            target
        } else {
            cfg.reuse_max_idle as usize
        },
        protected: target,
    }
}

pub struct DenoExecutor {
    cfg: DenoExecutorConfig,
    host: HostServices,
    interrupter: Arc<DenoInterrupter>,
    shared: Rc<RefCell<Shared>>,
    metrics: Option<Arc<Metrics>>,
}

impl DenoExecutor {
    /// Created on the Machine's own thread by the scheduler's factory. Initializes V8 once per
    /// process (flags, platform with `v8_platform_threads` workers).
    pub fn new(cfg: DenoExecutorConfig, host: HostServices) -> Self {
        init_v8_once(&cfg);
        Self {
            cfg,
            host,
            interrupter: DenoInterrupter::new(),
            shared: Rc::default(),
            metrics: None,
        }
    }

    /// Report `rt_deno_code_cache_total{result=hit|miss|rejected}` and
    /// `rt_deno_isolate_create_seconds` to `metrics`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Pre-created isolates currently waiting in this Machine's pool (DNO-11).
    pub fn pool_len(&self) -> usize {
        self.shared.borrow().pool.len()
    }

    /// Isolates waiting for disposal.
    pub fn dispose_backlog_len(&self) -> usize {
        self.shared.borrow().dispose.len()
    }

    /// Evaluated isolates kept for reuse on this Machine, all services (D-A4).
    pub fn warm_len(&self) -> usize {
        self.shared.borrow().warm.len()
    }

    /// Evaluated isolates kept for one service version and artifact.
    pub fn warm_len_of(&self, service: ServiceRef, key: PreparedKey) -> usize {
        self.shared.borrow().warm.len_of(&WarmId { service, key })
    }

    /// Services the warm store tracks (kept isolates, or an emptied entry not yet trimmed).
    pub fn warm_services(&self) -> usize {
        self.shared.borrow().warm.services()
    }
}

impl Executor for DenoExecutor {
    const KIND: TaskKind = TaskKind::DENO;
    type Prepared = DenoPrepared;

    fn start(&mut self, spec: TaskSpec, prepared: Arc<DenoPrepared>, cx: ExecCx) -> TaskFuture {
        self.note_template(&spec, &prepared);
        let run = TaskRun {
            cfg: self.cfg.clone(),
            host: self.host.clone(),
            interrupter: self.interrupter.clone(),
            shared: self.shared.clone(),
            metrics: self.metrics.clone(),
            started: Instant::now(),
            warmup: false,
        };
        Box::pin(run_task(run, spec, prepared, cx))
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        self.interrupter.clone()
    }

    fn on_idle(&mut self, budget: Duration) {
        let started = Instant::now();
        if self.relieve_pressure() >= Pressure::High {
            // Disposal below frees the shed isolates; refilling would only re-take the memory.
            self.drain_dispose(started, budget);
            return;
        }
        self.trim_warm();
        // 1. dispose finished isolates (the response is long gone: this is off the hot path)
        if !self.drain_dispose(started, budget) {
            return;
        }
        // 2. build a kept isolate the warm floor is short of (a local task)
        self.prewarm();
        // 3. refill the pool, never above its size (`isolate_pool_per_p`, or the warm floor of
        // services without reuse). One isolate is always created when the pool is empty so an
        // idle P converges even with a tiny budget.
        while self.pool_wants_more() {
            if self.pool_len() > 0 && started.elapsed() >= budget {
                break;
            }
            if !self.refill_one() {
                break;
            }
        }
    }

    /// B4.1: under load a Machine rarely parks, so the scheduler calls this between tasks. One
    /// unit = pre-create a pooled isolate when the pool is empty, else dispose one finished
    /// isolate, else top the pool up; further units only while within `budget` (a unit usually
    /// takes longer, so a call mostly does one). Also starts at most one warm-up of a kept
    /// isolate for a service whose warm target is not met.
    fn between_tasks(&mut self, budget: Duration) {
        let started = Instant::now();
        let shed = self.relieve_pressure() >= Pressure::High;
        if !shed {
            self.trim_warm();
            self.prewarm();
        }
        // One unit at a time, then the budget. An empty pool is refilled first (otherwise the
        // next task creates its isolate on its own path); else a finished isolate is disposed
        // (deferrable: past `dispose_backlog` it is disposed after its task anyway).
        loop {
            let refill = !shed && self.pool_wants_more();
            let did = if refill && self.pool_len() == 0 {
                self.refill_one() || self.dispose_one()
            } else {
                self.dispose_one() || (refill && self.refill_one())
            };
            if !did || started.elapsed() >= budget {
                break;
            }
        }
    }
}

impl DenoExecutor {
    /// Dispose one finished isolate (releasing its reservation). `false` = nothing queued.
    fn dispose_one(&self) -> bool {
        let next = self.shared.borrow_mut().dispose.pop_front();
        next.is_some()
    }

    /// Dispose until the queue is empty or `budget` is spent; `true` = the queue is empty.
    fn drain_dispose(&self, started: Instant, budget: Duration) -> bool {
        while self.dispose_one() {
            if started.elapsed() >= budget {
                return self.shared.borrow().dispose.is_empty();
            }
        }
        true
    }

    fn pool_wants_more(&self) -> bool {
        self.pool_len() < self.pool_size()
    }

    /// Blank isolates the pool keeps: `deno.isolate_pool_per_p`, or the warm floor of the services
    /// that run without reuse if that is more (their targets are summed: a blank isolate is
    /// tenant-neutral), at most `reuse_max_idle_total`.
    fn pool_size(&self) -> usize {
        let base = self.cfg.isolate_pool_per_p as usize;
        base.max(self.pool_floor().min(self.cfg.reuse_max_idle_total as usize))
    }

    /// Pooled isolates the scheduler's warm targets ask for on behalf of services that do not
    /// reuse isolates (D-B2).
    fn pool_floor(&self) -> usize {
        let s = self.shared.borrow();
        s.templates
            .values()
            .filter(|t| !t.knobs.reuse_isolates)
            .map(|t| target_of(&self.cfg, &t.id))
            .sum()
    }

    /// Pre-create one pooled isolate. It reserves only `deno.isolate_base_mb` (an idle isolate
    /// from the base snapshot is small); the task's reservation is topped up to the V8 heap limit
    /// at `attach`, so a pool of N pins N × base, not N × the V8 heap floor. `false` = the
    /// governor refused or creation failed.
    fn refill_one(&self) -> bool {
        let base = u64::from(self.cfg.isolate_base_mb) * MIB;
        let Ok(reservation) = self.host.deps().mem.reserve(MemCategory::DenoHeap, base, Owner::NONE) else {
            return false;
        };
        match create_isolate(&self.cfg, Some(reservation)) {
            Ok(iso) => {
                self.shared.borrow_mut().pool.push_back(iso);
                true
            }
            // LCOV_EXCL_START: V8 refusing to create an isolate cannot be provoked
            Err(e) => {
                tracing::warn!("pre-creating an isolate failed: {e}");
                false
            } // LCOV_EXCL_STOP
        }
    }

    /// Idle pooled and kept isolates are evictable memory. At `Pressure::High` or worse the pool and
    /// the kept isolates are cut to the warm floor (the scheduler has already cut the targets to
    /// it); at `Critical` everything idle goes. Callers stop refilling at `High`. Returns the
    /// pressure.
    fn relieve_pressure(&self) -> Pressure {
        let pressure = self.host.deps().mem.pressure();
        if pressure < Pressure::High {
            return pressure;
        }
        let critical = pressure >= Pressure::Critical;
        let pool_floor = if critical {
            0
        } else {
            self.pool_floor().min(self.pool_size())
        };
        let mut s = self.shared.borrow_mut();
        while s.pool.len() > pool_floor {
            if let Some(iso) = s.pool.pop_back() {
                s.dispose.push_back(iso);
            }
        }
        let kept = if critical {
            s.warm.drain()
        } else {
            s.warm.shed_to(|id| target_of(&self.cfg, id))
        };
        keepalive(self.metrics.as_deref(), "shed", kept.len());
        s.dispose.extend(kept);
        pressure
    }

    /// Kept isolates past their keep-alive (`min_run_time` or `reuse_idle_ttl`, a handoff once
    /// nothing of its service waits; those above their service's warm target) or above
    /// their service's bound go to the dispose queue; templates of services that went quiet are
    /// forgotten; a pool larger than it should be shrinks (cheap when nothing is kept).
    fn trim_warm(&self) {
        let now = Instant::now();
        let cfg = &self.cfg;
        let pool_size = self.pool_size();
        let mut s = self.shared.borrow_mut();
        let Shared {
            warm,
            dispose,
            pool,
            templates,
            ..
        } = &mut *s;
        if !warm.is_empty() {
            let out = warm.trim(now, |id| warm_keep(cfg, id), |id| waiting_of(cfg, id) > 0);
            keepalive(self.metrics.as_deref(), "expired", out.len());
            dispose.extend(out);
        }
        templates.retain(|_, t| {
            target_of(cfg, &t.id) > 0 || warm.len_of(&t.id) > 0 || now.duration_since(t.seen) < TEMPLATE_IDLE
        });
        while pool.len() > pool_size {
            if let Some(iso) = pool.pop_back() {
                dispose.push_back(iso);
            }
        }
    }

    /// Remember the spec and artifact of a service the scheduler keeps warm, to build its warm
    /// instances off the request path.
    fn note_template(&self, spec: &TaskSpec, prepared: &Arc<DenoPrepared>) {
        let id = WarmId {
            service: spec.service,
            key: prepared.key,
        };
        if target_of(&self.cfg, &id) == 0 {
            return;
        }
        let key = (id.service.tenant, id.service.service);
        let now = Instant::now();
        let mut s = self.shared.borrow_mut();
        if let Some(t) = s.templates.get_mut(&key) {
            if t.id == id && t.knobs == spec.knobs && t.limits == spec.limits {
                t.seen = now;
                return;
            }
        }
        s.templates.insert(
            key,
            Template {
                id,
                prepared: prepared.clone(),
                limits: spec.limits,
                knobs: spec.knobs,
                slug: spec.slug.clone(),
                seen: now,
                retry_at: now,
            },
        );
    }

    /// Start building one kept isolate for a service with reuse whose warm target the Machine
    /// does not meet (kept plus warm-ups running), unless the Machine already holds
    /// `reuse_max_idle_total`. The isolate evaluates the entry module through a warm-up dispatch
    /// (no handler runs) as a local task and parks itself like any task's.
    fn prewarm(&self) {
        let now = Instant::now();
        let picked = {
            let s = self.shared.borrow();
            if s.templates.is_empty() {
                return;
            }
            let building: usize = s.warming.values().sum();
            if s.warm.len() + building >= self.cfg.reuse_max_idle_total as usize {
                return;
            }
            s.templates.iter().find_map(|(key, t)| {
                let missing =
                    target_of(&self.cfg, &t.id) > s.warm.len_of(&t.id) + s.warming.get(key).copied().unwrap_or(0);
                (t.knobs.reuse_isolates && missing && now >= t.retry_at)
                    .then(|| (*key, t.id, t.prepared.clone(), t.limits, t.knobs, t.slug.clone()))
            })
        };
        let Some((key, id, prepared, limits, knobs, slug)) = picked else {
            return;
        };
        let base = u64::from(self.cfg.isolate_base_mb) * MIB;
        let Ok(reservation) = self.host.deps().mem.reserve(MemCategory::DenoHeap, base, Owner::NONE) else {
            return;
        };
        let task = {
            let mut s = self.shared.borrow_mut();
            s.warmups += 1;
            *s.warming.entry(key).or_insert(0) += 1;
            TaskId(u64::MAX - s.warmups)
        };
        let spec = TaskSpec {
            kind: TaskKind::DENO,
            service: id.service,
            trigger: Trigger::Http,
            limits,
            knobs,
            payload: Payload::None,
            trace: None,
            slug,
            caller: None,
            object: None,
        };
        let run = TaskRun {
            cfg: self.cfg.clone(),
            host: self.host.clone(),
            interrupter: self.interrupter.clone(),
            shared: self.shared.clone(),
            metrics: self.metrics.clone(),
            started: now,
            warmup: true,
        };
        let deadline = now + limits.wall;
        let building = Building {
            shared: self.shared.clone(),
            key,
        };
        let spawned = std::panic::catch_unwind(AssertUnwindSafe(move || {
            tokio::task::spawn_local(async move {
                let _building = building;
                let _ = run_task_as(run, spec, prepared, task, reservation, deadline).await;
            });
        }));
        if spawned.is_err() {
            // Outside a `LocalSet` (the Machine is shutting down): the closure, with the
            // reservation and the counter guard, was dropped.
            tracing::warn!("a Deno warm-up was requested outside its Machine's LocalSet");
        }
    }
}

/// Counts a running warm-up of a service; the count drops when the warm-up task ends.
struct Building {
    shared: Rc<RefCell<Shared>>,
    key: (TenantId, ServiceId),
}

impl Drop for Building {
    fn drop(&mut self) {
        let Ok(mut s) = self.shared.try_borrow_mut() else {
            return;
        };
        let Some(n) = s.warming.get_mut(&self.key) else { return };
        *n = n.saturating_sub(1);
        if *n == 0 {
            s.warming.remove(&self.key);
        }
    }
}

/// A handler's settled promise: its value, or the error as text (`Some`; `None` is "never settled").
fn settled<T, E: ToString>(r: Result<T, E>) -> Result<T, Option<String>> {
    r.map_err(|e| Some(e.to_string()))
}

/// The frozen clock moves once per event-loop turn (skipped should an op hold the state).
fn advance_clock(op_state: &Rc<RefCell<deno_core::OpState>>) {
    let Ok(mut s) = op_state.try_borrow_mut() else { return };
    let _ = s.try_borrow_mut::<ClockState>().map(ClockState::advance);
}

/// Why a response body still open when its task ended is aborted (`None`: it ended cleanly).
fn abort_reason(ended: &Ended) -> Option<String> {
    match ended {
        Ended::Done { .. } => None,
        Ended::GuestError(_) => Some("guest_error".to_string()),
        Ended::Killed(r) => Some(format!("{r:?}")),
    }
}

struct TaskRun {
    cfg: DenoExecutorConfig,
    host: HostServices,
    interrupter: Arc<DenoInterrupter>,
    shared: Rc<RefCell<Shared>>,
    metrics: Option<Arc<Metrics>>,
    started: Instant,
    /// Not a task: builds a kept isolate for the warm floor (evaluates the module, runs no
    /// handler, sends no response).
    warmup: bool,
}

/// Header values as WebIDL ByteStrings: one char per byte (latin-1), as `Deno.serve` hands them
/// out. A lossy UTF-8 decode turned obs-text bytes (0x80-0xFF) into U+FFFD, which the guest's
/// `Request` refused, failing the task (DREQ-006).
fn header_pairs(h: &http::HeaderMap) -> Vec<Value> {
    h.iter()
        .map(|(k, v)| {
            json!([
                k.as_str(),
                v.as_bytes().iter().map(|&b| char::from(b)).collect::<String>()
            ])
        })
        .collect()
}

/// `ctx.invocation` of an async HTTP invocation (`{id, attempt}` from the runtime headers), else
/// `null`. HTTP trigger only: a service call's headers come from guest code.
fn invocation_ctx(h: &http::HeaderMap) -> Value {
    let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok());
    match (
        get(rt_types::events::INVOCATION_ID_HEADER),
        get(rt_types::events::INVOCATION_ATTEMPT_HEADER).and_then(|a| a.parse::<u32>().ok()),
    ) {
        (Some(id), Some(attempt)) => json!({ "id": id, "attempt": attempt }),
        _ => Value::Null,
    }
}

/// The dispatch payload for `__rt.start` plus the request body, still streaming (B4.4): the
/// caller registers it as a resource and sets `request.bodyRid`; and the binary message of a
/// socket event (read with `op_rt_ws_event_data`).
fn build_payload(
    spec: &mut TaskSpec,
    prepared: &DenoPrepared,
    task: TaskId,
    slug: &str,
    deadline: Instant,
    warmup: bool,
) -> (Value, Option<Body>, Option<bytes::Bytes>) {
    let mut socket = Value::Null;
    let mut event_data = None;
    let mut request = Value::Null;
    let mut schedule = Value::Null;
    let mut queue = Value::Null;
    let mut req_body: Option<Body> = None;
    let mut invoker = Value::Null;
    let mut client = Value::Null;
    let mut invocation = Value::Null;
    let mut pubsub = Value::Null;
    let mut stream = Value::Null;

    match std::mem::take(&mut spec.payload) {
        Payload::Http(mut req) => {
            // Runtime-set request header; a caller-supplied value never survives.
            if let Ok(v) = http::HeaderValue::from_str(&task.to_string()) {
                req.headers.insert(http::HeaderName::from_static("x-rt-task-id"), v);
            }
            if let Some(v) = req.headers.get("x-rt-invoker").and_then(|v| v.to_str().ok()) {
                invoker = Value::String(v.to_string());
            }
            // `ctx.client`: the address ingress resolved behind `ingress.trusted_proxies` (it
            // strips any client-sent `x-rt-client-ip` first). HTTP trigger only: a call's
            // headers come from the calling guest.
            if matches!(spec.trigger, Trigger::Http) {
                invocation = invocation_ctx(&req.headers);
                if let Some(ip) = req
                    .headers
                    .get("x-rt-client-ip")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<std::net::IpAddr>().ok())
                {
                    client = json!({ "ip": ip.to_string() });
                }
            }
            let host = req
                .headers
                .get(http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("localhost")
                .to_string();
            let url = if req.uri.scheme().is_some() {
                req.uri.to_string()
            } else {
                let pq = req.uri.path_and_query().map_or("/", |p| p.as_str());
                format!("http://{host}{pq}")
            };
            let has_body = !matches!(req.method, http::Method::GET | http::Method::HEAD);
            let headers = header_pairs(&req.headers);
            if has_body {
                req_body = Some(std::mem::take(&mut req.body));
            }
            request = json!({
                "method": req.method.as_str(),
                "url": url,
                "headers": headers,
                "hasBody": has_body,
                "bodyRid": Value::Null,
            });
        }
        Payload::Schedule(ev) => {
            schedule = json!({
                "scheduleId": ev.schedule_id.to_string(),
                "scheduledTime": ev.scheduled_time_ms,
                "payload": ev.payload,
            });
        }
        Payload::Delayed(ev) => {
            schedule = json!({ "id": ev.id, "scheduledAt": ev.scheduled_at_ms, "payload": ev.payload });
        }
        Payload::Queue(msgs) => {
            queue = Value::Array(
                msgs.iter()
                    .map(|m| {
                        let body = serde_json::from_slice::<Value>(&m.body)
                            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&m.body).to_string()));
                        json!({ "id": m.id.to_string(), "body": body, "attempt": m.attempt })
                    })
                    .collect(),
            );
        }
        Payload::Socket(ev) => {
            let mut out = json!({
                "kind": ev.kind.label(),
                "attempt": ev.attempt,
                "ws": ev.socket.map_or(Value::Null, |i| serde_json::to_value(crate::ops_ws::info_out(i)).unwrap_or(Value::Null)),
            });
            match ev.kind {
                rt_types::SocketEventKind::Message(rt_types::WsData::Text(t)) => {
                    out["text"] = Value::String(t);
                }
                rt_types::SocketEventKind::Message(rt_types::WsData::Binary(b)) => {
                    out["binary"] = Value::Bool(true);
                    event_data = Some(b);
                }
                rt_types::SocketEventKind::Close {
                    code,
                    reason,
                    was_clean,
                } => {
                    out["code"] = json!(code);
                    out["reason"] = json!(reason);
                    out["wasClean"] = json!(was_clean);
                }
                rt_types::SocketEventKind::Error { code, message } => {
                    out["error"] = json!({ "code": code, "message": message });
                }
                rt_types::SocketEventKind::Alarm { scheduled_ms, retry } => {
                    out["scheduledTime"] = json!(scheduled_ms);
                    out["retryCount"] = json!(retry);
                }
            }
            socket = out;
        }
        Payload::Pubsub(m) => pubsub = m.guest_json(),
        Payload::Stream(b) => stream = b.guest_json(),
        Payload::None => {
            if matches!(spec.trigger, Trigger::Http | Trigger::Call { .. }) {
                request = json!({ "method": "GET", "url": "http://localhost/", "headers": [], "hasBody": false });
            }
        }
    }

    // `ctx.caller`: stamped by the call dispatcher for a service-to-service call (never a header).
    let caller = spec.caller.as_ref().map_or(Value::Null, |c| {
        json!({
            "service": { "id": serde_json::to_value(c.service).unwrap_or(Value::Null), "slug": c.slug },
            "tenant": c.tenant.to_string(),
            "plan": c.plan,
            "sdk": c.sdk.as_ref().map(|s| json!({ "name": s.name, "version": s.version })),
            "project": c.project.map(|p| p.to_string()),
            "environment": c.environment,
        })
    });
    let remaining = deadline.saturating_duration_since(Instant::now());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    let payload = json!({
        "entry": prepared.bundle.entry,
        "ctx": {
            "taskId": task.to_string(),
            "service": {
                "id": serde_json::to_value(spec.service.service).unwrap_or(Value::Null),
                "slug": slug,
                "version": spec.service.version.0,
            },
            "trigger": spec.trigger.label(),
            "invoker": invoker,
            "client": client,
            "caller": caller,
            "invocation": invocation,
            "deadline": (now_ms + remaining.as_secs_f64() * 1000.0).floor(),
            // The object this task acts for, with its turn (warm objects, see `ObjectRef`).
            "object": spec.object.as_ref().map_or(Value::Null, |o| json!({ "name": &*o.name, "seq": o.seq, "prev": o.prev })),
        },
        "request": request,
        "socket": socket,
        "schedule": schedule,
        "queue": queue,
        "pubsub": pubsub,
        "stream": stream,
        "warmup": warmup,
    });
    (payload, req_body, event_data)
}

/// Close the task's own handles (`rt…` resources: outbound sockets, unread bodies, an open SQL
/// transaction or blob upload) now, not when the isolate is finally disposed at idle time: an
/// outbound WebSocket closes when its task ends (SECURITY.md, DWARM-003).
fn close_task_resources(iso: &mut Isolate) {
    let state = iso.runtime.op_state();
    let mut s = state.borrow_mut();
    let open: Vec<deno_core::ResourceId> = s
        .resource_table
        .names()
        .filter(|(_, name)| name.starts_with("rt"))
        .map(|(rid, _)| rid)
        .collect();
    for rid in open {
        if let Ok(r) = s.resource_table.take_any(rid) {
            r.close();
        }
    }
}

fn dispose_later(shared: &Rc<RefCell<Shared>>, backlog: u32, mut iso: Isolate) {
    close_task_resources(&mut iso);
    let overflow = {
        let mut s = shared.borrow_mut();
        s.dispose.push_back(iso);
        if s.dispose.len() > backlog as usize {
            s.dispose.pop_front()
        } else {
            None
        }
    };
    // Over the backlog limit: dispose inline (outside the borrow).
    drop(overflow);
}

/// A pooled isolate, or a new one from the base snapshot.
fn fresh_isolate(
    cfg: &DenoExecutorConfig,
    shared: &Rc<RefCell<Shared>>,
    metrics: Option<&Metrics>,
) -> Result<Isolate, String> {
    let pooled = shared.borrow_mut().pool.pop_front();
    if let Some(i) = pooled {
        return Ok(i);
    }
    let t = Instant::now();
    let iso = create_isolate(cfg, None)?;
    if let Some(m) = metrics {
        m.deno_isolate_create_seconds.observe(t.elapsed().as_secs_f64());
    }
    Ok(iso)
}

/// The warmest isolate kept for `id` that this task may use: its charged memory fits the task's
/// limit, and when the guest read env in it, the service's env (secrets version) is unchanged
/// (module state may hold env values). Others are disposed on the way.
async fn take_kept(
    cfg: &DenoExecutorConfig,
    shared: &Rc<RefCell<Shared>>,
    metrics: Option<&Metrics>,
    id: &WarmId,
    handle: &HostHandle,
    limit_bytes: u64,
) -> Option<Isolate> {
    loop {
        let (kept, expired) = {
            let mut out = Vec::new();
            let protected = target_of(cfg, id);
            let kept = shared.borrow_mut().warm.take(id, Instant::now(), protected, &mut out);
            (kept, out)
        };
        keepalive(metrics, "expired", expired.len());
        for iso in expired {
            dispose_later(shared, cfg.dispose_backlog, iso);
        }
        let iso = kept?.iso;
        let env_ok = match iso.reuse.env_seen {
            None => Some(true),
            Some(seen) => match handle.env().await {
                Ok(env) => Some(env_hash(&env) == seen),
                Err(_) => None, // cannot tell: no kept isolate that read env is usable now
            },
        };
        if iso.mem.0.charged() <= limit_bytes && env_ok == Some(true) {
            // Defensive: a termination request can only come from a kill, and killed isolates
            // are never kept, but a stale one must not fail this task.
            iso.handle.cancel_terminate_execution();
            keepalive(metrics, "reused", 1);
            return Some(iso);
        }
        dispose_later(shared, cfg.dispose_backlog, iso);
        env_ok?;
    }
}

async fn run_task(run: TaskRun, spec: TaskSpec, prepared: Arc<DenoPrepared>, cx: ExecCx) -> Outcome {
    let ExecCx {
        task,
        reservation,
        deadline,
        ..
    } = cx;
    run_task_as(run, spec, prepared, task, reservation, deadline).await
}

async fn run_task_as(
    run: TaskRun,
    mut spec: TaskSpec,
    prepared: Arc<DenoPrepared>,
    task: TaskId,
    reservation: rt_mem::Reservation,
    deadline: Instant,
) -> Outcome {
    let TaskRun {
        cfg,
        host,
        interrupter,
        shared,
        metrics,
        started,
        warmup,
    } = run;

    // 1. context (before the isolate: a kept isolate is checked against the current env)
    let host_cfg = host.config_for(&spec).await;
    let slug = spec.slug.clone();
    let handle: HostHandle = host.handle_for(&spec, task, deadline, host_cfg);
    let warm_id = spec.knobs.reuse_isolates.then_some(WarmId {
        service: spec.service,
        key: prepared.key,
    });
    let limit_bytes = u64::from(spec.limits.memory_mb) * MIB;
    // `Deno.env` reads are synchronous: fetch the env here, asynchronously, whenever the code may
    // read it, so a read never parks the Machine thread (`ops::env_of`).
    let env = if prepared.may_read_env() {
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(ENV_FETCH_MAX_WAIT);
        Some(match tokio::time::timeout(wait, handle.env()).await {
            Ok(Ok(env)) => Ok(env),
            _ => Err(()),
        })
    } else {
        None
    };

    // 2. isolate: one kept for this service version (D-A4), a pooled one, or a new one
    let kept = match &warm_id {
        // A warm-up builds a new isolate: it never takes one that is kept already.
        Some(id) if !warmup => take_kept(&cfg, &shared, metrics.as_deref(), id, &handle, limit_bytes).await,
        _ => None,
    };
    let mut iso = match kept {
        Some(i) => i,
        None => match fresh_isolate(&cfg, &shared, metrics.as_deref()) {
            Ok(i) => i,
            // LCOV_EXCL_START: V8 refusing to create an isolate cannot be provoked
            Err(e) => {
                tracing::error!(%task, "{e}");
                return Outcome::GuestError;
            } // LCOV_EXCL_STOP
        },
    };
    if iso
        .attach(reservation, spec.limits.memory_mb, ClockState::new(cfg.clock))
        .is_err()
    {
        dispose_later(&shared, cfg.dispose_backlog, iso);
        return Outcome::Killed(KillReason::Oom);
    }
    iso.loader.attach(prepared.clone());
    if !warmup {
        iso.reuse.uses += 1;
    }

    let (mut payload, req_body, event_data) = build_payload(&mut spec, &prepared, task, &slug, deadline, warmup);
    let (head_tx, mut head_rx) = oneshot::channel::<Head>();
    {
        let op_state = iso.runtime.op_state();
        let mut s = op_state.borrow_mut();
        if let Some(body) = req_body {
            let rid = add_request_body(&mut s, body, spec.limits.max_request_body_bytes);
            payload["request"]["bodyRid"] = json!(rid);
        }
        // A kept isolate still holds the previous event's message.
        let _ = s.try_take::<crate::ops_ws::WsEventData>();
        if let Some(data) = event_data {
            s.put(crate::ops_ws::WsEventData(data));
        }
        s.put(TaskOps {
            host: handle.clone(),
            task,
            env,
            env_read: false,
            deadline,
            head_tx: Some(head_tx),
            body_tx: None,
            streamed: 0,
            max_stream: spec.limits.max_response_stream_bytes,
        });
    }

    // 3. limits and interruption (per task, also on a kept isolate)
    let (kill_reason, waker_cell) = interrupter.register(task, iso.handle.clone());
    let guard = match iso.cpu.clone() {
        Some(g) => {
            g.reset(spec.limits.cpu);
            g
        }
        None => {
            let g = CpuGuard::new(spec.limits.cpu, iso.handle.clone());
            g.watch_memory(iso.mem.0.clone());
            iso.cpu = Some(g.clone());
            g
        }
    };
    guard.activate();

    // 4. dispatch: runs the synchronous part of `__rt.start` (everything up to its first await)
    guard.begin_poll();
    let script = format!("globalThis.__rt.start({payload})");
    let started_promise = {
        let _entered = iso.enter();
        iso.runtime.execute_script("[rt:start]", script)
    };
    guard.end_poll();
    crate::ops_sqlite::settle_transactions();
    let promise = match started_promise {
        Ok(p) => p,
        // LCOV_EXCL_START: `__rt.start` is an async function: running it cannot throw synchronously
        Err(e) => {
            handle.log(LogLevel::Error, format!("dispatch failed: {e}"));
            guard.deactivate();
            interrupter.unregister(task);
            iso.loader.flush_cache_stats(metrics.as_deref());
            dispose_later(&shared, cfg.dispose_backlog, iso);
            return Outcome::GuestError;
        } // LCOV_EXCL_STOP
    };
    let init = started.elapsed();

    // 5. drive the isolate inside this task's future until the response head exists, so the
    // scheduler's per-poll accounting (CPU limit, billing, autoscaler load) sees the guest's work;
    // the rest (body streaming, `waitUntil`) continues on a local task, billed as the tail.
    let head_cpu = Rc::new(Cell::new(None));
    let driver = Driver {
        cfg,
        host,
        shared,
        interrupter,
        metrics,
        handle,
        guard: guard.clone(),
        kill_reason,
        waker_cell,
        task,
        deadline,
        warm_id,
        min_run_time: spec.knobs.min_run_time,
        queue_timeout: spec.knobs.queue_timeout,
        limit_bytes,
        head_cpu: head_cpu.clone(),
        warmup,
    };
    let mut slot = DriverSlot(Some(Box::pin(driver.run(iso, promise))));

    // 6. the outcome is decided by the response head
    let head = slot.until_head(&mut head_rx).await;
    head_cpu.set(Some(guard.used()));
    drop(slot);
    let elapsed = started.elapsed();
    let timings = TaskTimings {
        init,
        exec: elapsed.saturating_sub(init),
        cpu: guard.used(),
        polls: 0,
        ..TaskTimings::default()
    };
    match head {
        Ok(Head::Ok { status, headers, body }) => {
            let mut reply = Reply::new(status, body);
            reply.headers = headers;
            reply.timings = timings;
            Outcome::Ok(reply)
        }
        Ok(Head::Killed(reason)) => Outcome::Killed(reason),
        Ok(Head::Failed) | Err(_) => Outcome::GuestError,
    }
}

/// A task's driver while the task future polls it (until the response head exists). Dropped,
/// it hands what is left to a local task: after the head, and also when the scheduler drops the
/// task future (a kill), so the isolate is never dropped in the middle of a poll.
struct DriverSlot(Option<Pin<Box<dyn Future<Output = ()>>>>);

impl DriverSlot {
    fn until_head<'a>(
        &'a mut self,
        head_rx: &'a mut oneshot::Receiver<Head>,
    ) -> impl Future<Output = Result<Head, oneshot::error::RecvError>> + 'a {
        poll_fn(move |cx| {
            if let Poll::Ready(head) = head_rx.poll_unpin(cx) {
                return Poll::Ready(head);
            }
            // `self.0` is `Some` until the slot is dropped or the driver finished.
            let Some(driver) = self.0.as_mut() else {
                return Poll::Pending;
            };
            if driver.as_mut().poll(cx).is_ready() {
                self.0 = None;
                // a finished driver has settled the head (or dropped its sender)
                return head_rx.poll_unpin(cx);
            }
            Poll::Pending
        })
    }
}

impl Drop for DriverSlot {
    fn drop(&mut self) {
        let Some(driver) = self.0.take() else { return };
        // Outside a `LocalSet` (the Machine is shutting down) `spawn_local` panics; the driver is
        // then dropped as the shutdown would drop it anyway.
        let spawned = std::panic::catch_unwind(AssertUnwindSafe(move || {
            tokio::task::spawn_local(driver);
        }));
        if spawned.is_err() {
            tracing::warn!("a Deno task driver was dropped outside its Machine's LocalSet");
        }
    }
}

struct Driver {
    cfg: DenoExecutorConfig,
    host: HostServices,
    shared: Rc<RefCell<Shared>>,
    interrupter: Arc<DenoInterrupter>,
    metrics: Option<Arc<Metrics>>,
    handle: HostHandle,
    guard: Arc<CpuGuard>,
    kill_reason: Arc<parking_lot::Mutex<Option<KillReason>>>,
    /// Where the driver leaves its waker on every poll, so a kill reaches a task parked on I/O.
    waker_cell: crate::interrupt::WakerCell,
    task: TaskId,
    deadline: Instant,
    /// Reuse is on for this service (`deno.reuse_isolates`): where a clean finish is kept.
    warm_id: Option<WarmId>,
    /// The service's `min_run_time` (`None`: inherit `reuse_idle_ttl`), see [`Driver::park`].
    min_run_time: Option<Duration>,
    /// The service's `queue_timeout`: how long a handoff isolate waits at most.
    queue_timeout: Duration,
    /// `limits.memory_mb` in bytes.
    limit_bytes: u64,
    /// Guest CPU when the task future returned the head; what the driver uses afterwards (the
    /// streamed tail, `waitUntil`) is charged to the tenant separately.
    head_cpu: Rc<Cell<Option<Duration>>>,
    /// A warm-up of the warm floor (see [`TaskRun::warmup`]): there is no response to settle.
    warmup: bool,
}

enum Ended {
    /// `__rt.start` resolved; `clean` = it reported no guest failure (handler, body, waitUntil).
    Done {
        clean: bool,
    },
    GuestError(String),
    Killed(KillReason),
}

impl Driver {
    async fn run(self, mut iso: Isolate, promise: deno_core::v8::Global<deno_core::v8::Value>) {
        let mut fut = {
            let _entered = iso.enter();
            Box::pin(iso.runtime.resolve(promise))
        };
        let wall = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline));
        tokio::pin!(wall);
        let op_state = iso.runtime.op_state();
        let guard = self.guard.clone();
        let handle = self.handle.clone();
        let isolate_handle = iso.handle.clone();
        let mem = iso.mem.0.clone();
        let tasks = iso.tasks.0.clone();
        let mut walled = false;

        let ended = poll_fn(|cx| {
            {
                let mut w = self.waker_cell.lock();
                if !w.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *w = Some(cx.waker().clone());
                }
            }
            // This isolate is current only while it is polled: several isolates share this thread
            // (see `isolate::Entered`). The guard never outlives the synchronous poll.
            let _entered = iso.enter();
            // the frozen clock moves only here: once per event-loop turn
            advance_clock(&op_state);
            guard.begin_poll();
            // V8 foreground tasks (async wasm compilation, finalizers, GC): see `platform.rs`.
            tasks.set_waker(cx.waker());
            tasks.run_pending();
            let polled: Poll<Result<v8::Global<v8::Value>, Option<String>>> =
                match iso.runtime.poll_event_loop(cx, PollEventLoopOptions::default()) {
                    Poll::Ready(Err(e)) => Poll::Ready(Err(Some(e.to_string()))),
                    // The loop has nothing left to run: the handler's promise is settled or never will be.
                    Poll::Ready(Ok(())) => match fut.poll_unpin(cx) {
                        Poll::Ready(r) => Poll::Ready(settled(r)),
                        Poll::Pending => Poll::Ready(Err(None)),
                    },
                    Poll::Pending => fut.poll_unpin(cx).map(settled),
                };
            guard.end_poll();
            // A `std:sqlite` transaction never spans a poll; one still open was cut off by a kill.
            crate::ops_sqlite::settle_transactions();

            // memory: heap + buffers (allocator and external) + blob parts + wasm memories
            // (B5.1, D-A2); the CPU watchdog also samples it while JS runs, this catches growth
            // by bursts shorter than its tick.
            let within = {
                let rt = &mut iso.runtime;
                deno_core::scope!(scope, rt);
                mem.check(scope)
            };
            if !within {
                isolate_handle.terminate_execution();
            }
            // limits: CPU (guest + host-side work charged to the task), wall
            if guard.over_budget(handle.host_cpu()) && !guard.tripped() {
                guard.trip();
            }
            if !walled && wall.as_mut().poll(cx).is_ready() {
                walled = true;
                isolate_handle.terminate_execution();
                *self.kill_reason.lock() = Some(KillReason::WallTimeout);
            }
            let killed = self.kill_reason.lock().is_some() || guard.tripped() || mem.oom();
            if killed {
                return Poll::Ready(Ended::Killed(KillReason::Shutdown)); // refined below
            }
            match polled {
                Poll::Ready(Ok(value)) => {
                    // `__rt.start` resolves to `true` for a clean finish (bootstrap.js)
                    let rt = &mut iso.runtime;
                    deno_core::scope!(scope, rt);
                    let clean = v8::Local::new(scope, &value).is_true();
                    Poll::Ready(Ended::Done { clean })
                }
                Poll::Ready(Err(Some(msg))) => Poll::Ready(Ended::GuestError(msg)),
                Poll::Ready(Err(None)) => Poll::Ready(Ended::GuestError("handler never settled".to_string())),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        {
            let _entered = iso.enter();
            drop(fut);
        }

        // Why was it killed? sysmon / wall first, then the CPU guard, then memory.
        let ended = match ended {
            Ended::Killed(_) => {
                let reason = self
                    .kill_reason
                    .lock()
                    .take()
                    .or_else(|| guard.tripped().then_some(KillReason::CpuLimit))
                    .or_else(|| iso.oom().then_some(KillReason::Oom));
                Ended::Killed(reason.unwrap_or(KillReason::Shutdown))
            }
            // The guest did not handle the `RangeError` of a refused `ArrayBuffer` allocation: the
            // task ran out of memory (`502 oom`), it did not fail on its own.
            Ended::GuestError(msg) if mem.buffers_refused() => {
                self.handle.log(
                    LogLevel::Error,
                    format!("guest error after an allocation over the memory limit: {msg}"),
                );
                Ended::Killed(KillReason::Oom)
            }
            other => other,
        };

        // The task ended (done, failed, killed or over a limit): its `std:lock` holds go first.
        self.handle.release_locks();
        // Settle whatever the guest had not finished: head not sent -> outcome; body open -> abort.
        let mut answered = false;
        {
            let mut s = op_state.borrow_mut();
            if let Some(t) = s.try_borrow_mut::<TaskOps>() {
                match &ended {
                    Ended::Done { .. } => {}
                    Ended::GuestError(msg) => {
                        self.handle.log(LogLevel::Error, format!("guest error: {msg}"));
                        if let Some(tx) = t.head_tx.take() {
                            let _ = tx.send(Head::Failed);
                        }
                    }
                    Ended::Killed(r) => {
                        if let Some(tx) = t.head_tx.take() {
                            let _ = tx.send(Head::Killed(*r));
                        }
                    }
                }
                // an unsent head after a clean finish means the handler produced nothing (a warm-up
                // sends none: it has no caller, and dropping the sender frees its future)
                // An unsent head is dropped with its sender: the task future then answers a guest
                // error (a warm-up sends none: it has no caller).
                let unsent = t.head_tx.take().is_some();
                answered = (!unsent || self.warmup) && matches!(ended, Ended::Done { .. });
                // A body still open after a kill or failure must reach the client as an aborted
                // stream, not a clean end: dropping this sender alone would not close it (pending
                // chunk ops hold clones until the isolate is disposed) and a clean end would pass
                // a truncated body off as complete.
                if let (Some(sender), Some(reason)) = (t.body_tx.take(), abort_reason(&ended)) {
                    tokio::task::spawn_local(async move { sender.abort(BodyError::Aborted(reason)).await });
                }
            } // LCOV_EXCL_LINE: the task's ops state is present until the isolate is disposed
        }

        // Reuse (D-A4): after a clean finish one more event-loop turn, still under this task's
        // limits, must find nothing left (timers, pending ops, unhandled rejections): anything
        // pending would otherwise run inside the next task.
        let mut reusable = self.warm_id.is_some() && answered && matches!(ended, Ended::Done { clean: true });
        if reusable {
            let _entered = iso.enter();
            guard.begin_poll();
            // V8 foreground tasks (finished async wasm compiles, finalizers) run now, as part of
            // this task; any posted by the turn itself mean work that would land in the next one.
            iso.tasks.0.run_pending();
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            let turn = iso.runtime.poll_event_loop(&mut cx, PollEventLoopOptions::default());
            guard.end_poll();
            crate::ops_sqlite::settle_transactions();
            reusable = matches!(turn, Poll::Ready(Ok(()))) && !iso.tasks.0.has_pending();
        }

        guard.deactivate();
        self.interrupter.unregister(self.task);
        if let Some(at_head) = self.head_cpu.get() {
            self.handle.charge_cpu(guard.used().saturating_sub(at_head));
        }
        iso.loader.flush_cache_stats(self.metrics.as_deref());
        let kept = match self.warm_id {
            Some(id) if reusable && self.keep(&mut iso, &op_state) => self.park(id, iso),
            _ => {
                dispose_later(&self.shared, self.cfg.dispose_backlog, iso);
                false
            }
        };
        if self.warmup && !kept {
            // The module throws, hangs on a timer, ...: do not retry at every hook.
            if let Some(id) = self.warm_id {
                if let Some(t) = self
                    .shared
                    .borrow_mut()
                    .templates
                    .get_mut(&(id.service.tenant, id.service.service))
                {
                    t.retry_at = Instant::now() + WARMUP_BACKOFF;
                }
            } // LCOV_EXCL_LINE: a warm-up always has its warm id
        }
    }

    /// May this cleanly finished isolate serve the next task of its service version (D-A4)? It
    /// must not have been killed or run out of memory (checked after `unregister`, so no kill can
    /// still arrive), must be under `reuse_max_uses`, the node not under memory pressure, its
    /// charged memory within 3/4 of the task limit and not grown more than [`REUSE_MAX_GROWTH`]
    /// (or half) past what it held after its first task. If so, the task context is removed
    /// (remembering whether the guest read env) and what the task left open is closed.
    fn keep(&self, iso: &mut Isolate, op_state: &Rc<RefCell<deno_core::OpState>>) -> bool {
        let killed = self.kill_reason.lock().is_some()
            || self.guard.tripped()
            || iso.oom()
            || iso.mem.0.buffers_refused()
            || iso.handle.is_execution_terminating();
        if killed || iso.reuse.uses >= self.cfg.reuse_max_uses || self.host.deps().mem.pressure() >= Pressure::High {
            return false;
        }
        let limit = self.limit_bytes;
        let over = |charged: u64, baseline: u64| {
            charged > limit / 4 * 3 || charged.saturating_sub(baseline) > REUSE_MAX_GROWTH.max(baseline / 2)
        };
        // A fresh sample: buffers made without the allocator, blob parts and wasm memories the
        // task left in module state count against the next task too.
        let within = {
            let _entered = iso.enter();
            let rt = &mut iso.runtime;
            deno_core::scope!(scope, rt);
            iso.mem.0.check(scope)
        };
        if !within {
            return false; // LCOV_EXCL_LINE: the last poll's sample already killed a task over its limit
        }
        let mut charged = iso.mem.0.charged();
        if over(charged, iso.reuse.baseline.unwrap_or(charged)) && iso.mem.0.buffer_bytes() > BUFFER_ALLOWANCE {
            // Garbage `ArrayBuffer`s count until V8 collects them: collect once, then judge.
            let _entered = iso.enter();
            let rt = &mut iso.runtime;
            deno_core::scope!(scope, rt);
            scope.low_memory_notification();
            iso.mem.0.check(scope); // re-sample (external memory, wasm) after the collection
            charged = iso.mem.0.charged();
        }
        let baseline = *iso.reuse.baseline.get_or_insert(charged);
        if over(charged, baseline) {
            return false;
        }
        let mut s = op_state.borrow_mut();
        if let Some(t) = s.try_take::<TaskOps>() {
            if let (true, Some(Ok(env))) = (t.env_read, &t.env) {
                iso.reuse.env_seen = Some(env_hash(env));
            }
        } // LCOV_EXCL_LINE: the task's ops state is present until the isolate is disposed
          // The task's own handles (unread bodies, an unfinished SQL transaction or blob upload);
          // resources of module state (e.g. a `MessagePort` made at top level) stay.
        let open: Vec<deno_core::ResourceId> = s
            .resource_table
            .names()
            .filter(|(_, name)| name.starts_with("rt"))
            .map(|(rid, _)| rid)
            .collect();
        for rid in open {
            if let Ok(r) = s.resource_table.take_any(rid) {
                r.close();
            }
        }
        true
    }

    /// Keep a reusable isolate per the service's `min_run_time` (`rt_sched::after_answer`): unset,
    /// for `reuse_idle_ttl` (today's rule); `t > 0`, for `t`; `0` while work of the service waits,
    /// until that work is gone (at most `queue_timeout`); `0` with nothing waiting, disposed,
    /// unless the service holds fewer than its warm floor (then kept, protected, expiring now).
    /// Returns whether it was handed to the store.
    fn park(&self, id: WarmId, iso: Isolate) -> bool {
        let cfg = &self.cfg;
        let metrics = self.metrics.as_deref();
        let now = Instant::now();
        let (expires, handoff) = match after_answer(self.min_run_time, waiting_of(cfg, &id), self.queue_timeout) {
            AfterAnswer::Inherit => (now + cfg.reuse_idle_ttl, false),
            AfterAnswer::For(t) => (now + t, false),
            AfterAnswer::Handoff { cap } => (now + cap, true),
            AfterAnswer::Dispose => {
                if target_of(cfg, &id) <= self.shared.borrow().warm.len_of(&id) {
                    keepalive(metrics, "disposed", 1);
                    dispose_later(&self.shared, cfg.dispose_backlog, iso);
                    return false;
                }
                (now, false)
            }
        };
        let bound = warm_keep(cfg, &id).bound;
        let total = cfg.reuse_max_idle_total as usize;
        let out = self
            .shared
            .borrow_mut()
            .warm
            .park(&id, iso, now, expires, handoff, bound, total, |i| target_of(cfg, i));
        keepalive(metrics, if handoff { "handoff" } else { "kept" }, 1);
        for i in out {
            dispose_later(&self.shared, cfg.dispose_backlog, i);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_still_open_is_aborted_with_the_reason_the_task_ended() {
        assert_eq!(abort_reason(&Ended::Done { clean: true }), None);
        assert_eq!(
            abort_reason(&Ended::GuestError("boom".into())).as_deref(),
            Some("guest_error")
        );
        assert_eq!(
            abort_reason(&Ended::Killed(KillReason::WallTimeout)).as_deref(),
            Some("WallTimeout")
        );
    }

    fn prepared() -> DenoPrepared {
        DenoPrepared {
            key: PreparedKey {
                digest: rt_types::Digest::of(b"x"),
                kind: rt_types::RuntimeKind::Deno,
                engine_fp: 0,
            },
            bundle: rt_build::Bundle {
                format: "esm".into(),
                entry: "main.js".into(),
                handler_form: rt_types::HandlerForm::B,
                lock_digest: None,
                modules: Vec::new(),
            },
            code_cache: None,
            handler_form: rt_types::HandlerForm::B,
            accounted_bytes: 0,
            reads_env: std::sync::OnceLock::new(),
        }
    }

    fn spec(trigger: Trigger, payload: Payload) -> TaskSpec {
        let svc = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            version: rt_types::VersionNo(1),
            digest: rt_types::Digest::of(b"x"),
        };
        TaskSpec::new(TaskKind::Exec(rt_types::RuntimeKind::Deno), svc, trigger).with_payload(payload)
    }

    fn payload_of(mut s: TaskSpec) -> Value {
        let deadline = Instant::now() + Duration::from_secs(1);
        build_payload(&mut s, &prepared(), TaskId(1), "svc", deadline, false).0
    }

    fn invocation_request() -> rt_types::HttpRequest {
        let mut req = rt_types::HttpRequest::get("/hook");
        req.headers
            .insert("x-rt-invocation-id", http::HeaderValue::from_static("inv_01"));
        req.headers
            .insert("x-rt-invocation-attempt", http::HeaderValue::from_static("2"));
        req
    }

    /// Pub/sub and stream payloads land in `pubsub` / `stream` with the guest JSON shape; an async
    /// HTTP invocation gets `ctx.invocation` from the runtime headers.
    #[test]
    fn build_payload_maps_pubsub_stream_and_invocation() {
        use rt_types::events::{PubsubMessage, StreamBatch, StreamRecord};
        let msg = PubsubMessage {
            id: rt_types::MsgId::from_u128(3),
            subject: "orders.created".into(),
            body: bytes::Bytes::from_static(br#"{"n":1}"#),
            headers: vec![("k".into(), "v".into())],
            replyable: true,
            published_at_ms: 42,
        };
        let p = payload_of(spec(
            Trigger::Pubsub {
                subject: Arc::from("orders.created"),
                depth: 1,
                reply: true,
            },
            Payload::Pubsub(msg.clone()),
        ));
        assert_eq!(p["pubsub"], msg.guest_json());
        assert_eq!(p["pubsub"]["body"], json!({"n": 1}));
        assert_eq!(p["pubsub"]["headers"], json!({"k": "v"}));
        assert_eq!(
            (p["pubsub"]["replyable"].clone(), p["pubsub"]["publishedAt"].clone()),
            (json!(true), json!(42))
        );
        assert_eq!(p["ctx"]["trigger"], "pubsub");
        assert!(p["stream"].is_null() && p["request"].is_null() && p["ctx"]["invocation"].is_null());

        let batch = StreamBatch {
            topic: "orders".into(),
            group: "default".into(),
            partition: 1,
            attempt: 2,
            records: vec![StreamRecord {
                partition: 1,
                offset: 7,
                key: Some("k".into()),
                headers: vec![],
                body: bytes::Bytes::from_static(b"not json"),
                ts_ms: 5,
            }],
        };
        let p = payload_of(spec(
            Trigger::Stream {
                topic: Arc::from("orders"),
                partition: 1,
                attempt: 2,
            },
            Payload::Stream(batch),
        ));
        let s = &p["stream"];
        assert_eq!(
            (s["topic"].clone(), s["group"].clone()),
            (json!("orders"), json!("default"))
        );
        assert_eq!((s["partition"].clone(), s["attempt"].clone()), (json!(1), json!(2)));
        assert_eq!(
            s["messages"],
            json!([{"partition": 1, "offset": 7, "key": "k", "body": "not json", "headers": {}, "timestamp": 5}])
        );
        assert_eq!(p["ctx"]["trigger"], "stream");
        assert!(p["pubsub"].is_null());

        let p = payload_of(spec(Trigger::Http, Payload::Http(invocation_request())));
        assert_eq!(p["ctx"]["invocation"], json!({"id": "inv_01", "attempt": 2}));
        let p = payload_of(spec(Trigger::Http, Payload::Http(rt_types::HttpRequest::get("/"))));
        assert!(p["ctx"]["invocation"].is_null());
    }

    /// `ctx.invocation` is built only for the HTTP trigger: a service call carrying the same
    /// headers (set by guest code) gets `null`.
    #[test]
    fn invocation_ctx_only_for_http_trigger() {
        let call = Trigger::Call {
            parent: TaskId(9),
            depth: 1,
        };
        let p = payload_of(spec(call, Payload::Http(invocation_request())));
        assert!(p["ctx"]["invocation"].is_null());
        let mut bad = invocation_request();
        bad.headers
            .insert("x-rt-invocation-attempt", http::HeaderValue::from_static("x"));
        assert!(payload_of(spec(Trigger::Http, Payload::Http(bad)))["ctx"]["invocation"].is_null());
    }
}
