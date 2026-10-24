//! The public scheduler API: [`Scheduler`] + [`SchedulerBuilder`] (rt-sched).
//!
//! ```ignore
//! let sched = Scheduler::builder(cfg, clock, governor)
//!     .executor(|| DenoExecutor::new(..), cache_source.clone(), deno_engine_fp)
//!     .executor(|| WasmExecutor::new(..), wasm_source.clone(), wasm_engine_fp)
//!     .executor(|| BuildExecutor::new(..), Arc::new(UnitSource), 0)
//!     .stats(stats).metrics(metrics)
//!     .build()?;
//! let handle = sched.submit(spec)?;          // admission happens here
//! let outcome = handle.await;
//! ```

use std::any::Any;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rt_mem::MemoryGovernor;
use rt_obs::{LogSink, Metrics, StatsStore};
use rt_types::{
    BuildFailure, DegradationMode, PreparedKey, RejectReason, SchedSnapshot, ServiceId, TaskId, TaskKind, TaskSpec,
};
use thiserror::Error;

use crate::admission::{AdmissionLimits, ServiceAdmission, ServiceScaling};
use crate::clock::{Clock, SimClock};
use crate::config::{LiveSchedConfig, SchedConfig};
use crate::executor::{ExecCx, Executor, Interrupter, Lookup, PreparedSource, TaskFuture};
use crate::inner::{CoreOpts, SchedInner};
use crate::sim::SimScheduler;
use crate::task::TaskHandle;
use crate::warm::WarmTargets;

#[derive(Debug, Error)]
pub enum SchedError {
    #[error("no executor registered")]
    NoExecutors,
    #[error("executor for kind `{0}` registered twice")]
    DuplicateKind(TaskKind),
    #[error("invalid scheduler configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to spawn scheduler thread: {0}")]
    Spawn(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------------------------
// Type erasure of executors and prepared sources
// ---------------------------------------------------------------------------------------------

pub(crate) trait ErasedExecutor {
    fn start(&mut self, spec: TaskSpec, prepared: Arc<dyn Any + Send + Sync>, cx: ExecCx) -> TaskFuture;
    fn interrupter(&self) -> Arc<dyn Interrupter>;
    fn on_idle(&mut self, budget: Duration);
    fn between_tasks(&mut self, budget: Duration);
    fn next_deadline(&self) -> Option<std::time::Instant>;
    fn wake_signal(&self) -> Option<Arc<tokio::sync::Notify>>;
}

impl<E: Executor> ErasedExecutor for E {
    fn start(&mut self, spec: TaskSpec, prepared: Arc<dyn Any + Send + Sync>, cx: ExecCx) -> TaskFuture {
        let prepared = prepared
            .downcast::<E::Prepared>()
            .unwrap_or_else(|_| panic!("prepared value type does not match executor for {}", E::KIND));
        Executor::start(self, spec, prepared, cx)
    }

    fn interrupter(&self) -> Arc<dyn Interrupter> {
        Executor::interrupter(self)
    }

    fn on_idle(&mut self, budget: Duration) {
        Executor::on_idle(self, budget)
    }

    fn between_tasks(&mut self, budget: Duration) {
        Executor::between_tasks(self, budget)
    }

    fn next_deadline(&self) -> Option<std::time::Instant> {
        Executor::next_deadline(self)
    }

    fn wake_signal(&self) -> Option<Arc<tokio::sync::Notify>> {
        Executor::wake_signal(self)
    }
}

pub(crate) enum ErasedLookup {
    Ready(Arc<dyn Any + Send + Sync>),
    Pending,
    Failed(BuildFailure),
}

pub(crate) trait ErasedSource: Send + Sync {
    fn lookup(&self, key: &PreparedKey, waiter: TaskId) -> ErasedLookup;
    fn is_ready(&self, key: &PreparedKey) -> bool;
}

struct SourceAdapter<P, S: ?Sized> {
    src: Arc<S>,
    _p: PhantomData<fn() -> P>,
}

impl<P, S> ErasedSource for SourceAdapter<P, S>
where
    P: Send + Sync + 'static,
    S: PreparedSource<P> + ?Sized,
{
    fn lookup(&self, key: &PreparedKey, waiter: TaskId) -> ErasedLookup {
        match self.src.lookup(key, waiter) {
            Lookup::Ready(a) => ErasedLookup::Ready(a),
            Lookup::Pending => ErasedLookup::Pending,
            Lookup::Failed(f) => ErasedLookup::Failed(f),
        }
    }

    fn is_ready(&self, key: &PreparedKey) -> bool {
        self.src.is_ready(key)
    }
}

/// One registered task kind.
pub(crate) struct KindRegistration {
    pub kind: TaskKind,
    pub engine_fp: u64,
    pub factory: Box<dyn Fn() -> Box<dyn ErasedExecutor> + Send + Sync>,
    pub source: Arc<dyn ErasedSource>,
}

// ---------------------------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------------------------

pub struct SchedulerBuilder {
    cfg: SchedConfig,
    clock: Arc<dyn Clock>,
    mem: Arc<dyn MemoryGovernor>,
    kinds: Vec<KindRegistration>,
    stats: Option<Arc<dyn StatsStore>>,
    metrics: Option<Arc<Metrics>>,
    logs: Option<Arc<dyn LogSink>>,
    keep_trace: bool,
    schedtrace: crate::inner::SchedtraceSink,
    warm: Option<Arc<WarmTargets>>,
}

impl SchedulerBuilder {
    pub(crate) fn new(cfg: SchedConfig, clock: Arc<dyn Clock>, mem: Arc<dyn MemoryGovernor>) -> Self {
        Self {
            cfg,
            clock,
            mem,
            kinds: Vec::new(),
            stats: None,
            metrics: None,
            logs: None,
            keep_trace: false,
            schedtrace: None,
            warm: None,
        }
    }

    /// Where the autoscaler publishes warm-capacity targets (decisions D-A3/D-A4). Create the
    /// `Arc` first and hand clones to the executor factories so they read the same targets;
    /// without this call the scheduler makes its own ([`Scheduler::warm_targets`]).
    pub fn warm_targets(mut self, warm: Arc<WarmTargets>) -> Self {
        self.warm = Some(warm);
        self
    }

    /// `RT_SCHEDTRACE=<ms>`: every `every` (clock time, virtual in simulation) sysmon calls
    /// `sink` with one `rt_obs::format_schedtrace(&snapshot)` line (SCH-22). `rt-server` wires
    /// the sink to stderr when the env var is set; tests capture the lines.
    pub fn schedtrace(mut self, every: Duration, sink: Arc<dyn Fn(&str) + Send + Sync>) -> Self {
        self.schedtrace = Some((every, sink));
        self
    }

    /// Register the executor of one task kind.
    ///
    /// `factory` runs once per Machine **on that Machine's thread**, so the executor may be
    /// `!Send`. `source` serves prepared artifacts (`rt-cache`, or `UnitSource` for builds).
    /// `engine_fp` is the engine fingerprint used in `PreparedKey`.
    pub fn executor<E, S>(
        mut self,
        factory: impl Fn() -> E + Send + Sync + 'static,
        source: Arc<S>,
        engine_fp: u64,
    ) -> Self
    where
        E: Executor,
        S: PreparedSource<E::Prepared> + ?Sized,
    {
        self.kinds.push(KindRegistration {
            kind: E::KIND,
            engine_fp,
            factory: Box::new(move || Box::new(factory()) as Box<dyn ErasedExecutor>),
            source: Arc::new(SourceAdapter::<E::Prepared, S> {
                src: source,
                _p: PhantomData,
            }),
        });
        self
    }

    /// Per-service stats sink (`GET …/stats`).
    pub fn stats(mut self, s: Arc<dyn StatsStore>) -> Self {
        self.stats = Some(s);
        self
    }

    /// Prometheus metrics.
    pub fn metrics(mut self, m: Arc<Metrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    /// Where `kind=system` notes about a service (e.g. `cpu_limit`) are written.
    pub fn log_sink(mut self, l: Arc<dyn LogSink>) -> Self {
        self.logs = Some(l);
        self
    }

    /// Keep the full event trace (not just its hash). Used by `SimScheduler`.
    pub fn keep_trace(mut self, keep: bool) -> Self {
        self.keep_trace = keep;
        self
    }

    /// Validate, create Processors and Machines, spawn Machine threads and sysmon.
    pub fn build(self) -> Result<Scheduler, SchedError> {
        self.validate()?;
        let inner = SchedInner::new(self.into_opts(false))?;
        crate::real::spawn(&inner)?;
        Ok(Scheduler { inner })
    }

    /// Build a **deterministic, single-threaded** scheduler for simulation: no threads are
    /// spawned; the returned [`SimScheduler`] steps Machines in a seed-determined order against
    /// virtual time. `clock` must be the same object that was passed to
    /// [`Scheduler::builder`].
    pub fn build_sim(self, clock: Arc<dyn SimClock>) -> Result<SimScheduler, SchedError> {
        self.validate()?;
        let inner = SchedInner::new(self.into_opts(true))?;
        Ok(SimScheduler::new(inner, clock))
    }

    fn into_opts(self, sim: bool) -> CoreOpts {
        CoreOpts {
            cfg: self.cfg,
            clock: self.clock,
            mem: self.mem,
            kinds: self.kinds,
            stats: self.stats,
            metrics: self.metrics,
            logs: self.logs,
            schedtrace: self.schedtrace,
            keep_trace: self.keep_trace,
            sim,
            warm: self.warm,
        }
    }

    fn validate(&self) -> Result<(), SchedError> {
        if self.kinds.is_empty() {
            return Err(SchedError::NoExecutors);
        }
        for (i, k) in self.kinds.iter().enumerate() {
            if self.kinds[..i].iter().any(|o| o.kind == k.kind) {
                return Err(SchedError::DuplicateKind(k.kind));
            }
        }
        if self.cfg.processors == 0 {
            return Err(SchedError::InvalidConfig("processors must be >= 1".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------------------------

/// Cheap-to-clone handle to the running scheduler.
#[derive(Clone)]
pub struct Scheduler {
    pub(crate) inner: Arc<SchedInner>,
}

impl Scheduler {
    pub fn builder(cfg: SchedConfig, clock: Arc<dyn Clock>, mem: Arc<dyn MemoryGovernor>) -> SchedulerBuilder {
        SchedulerBuilder::new(cfg, clock, mem)
    }

    /// Admit and enqueue a task. Admission checks run **here, synchronously**: a
    /// rejection is returned immediately and no task is created. On success the returned
    /// [`TaskHandle`] resolves to the task's `Outcome`. Placement: power-of-two-choices over
    /// the kind's Ps, else GRQ; a spawn from a running task of the same kind uses `runnext`.
    pub fn submit(&self, spec: TaskSpec) -> Result<TaskHandle, RejectReason> {
        self.inner.submit(spec, None)
    }

    /// Called by `rt-cache` when a load/build for `key` completes (or fails): every
    /// `WaitingRuntime` waiter for the key is moved to its kind's GRQ, FIFO by `enqueued_at`.
    pub fn wake_runtime_waiters(&self, key: &PreparedKey) {
        self.inner.wake_runtime_waiters(key);
    }

    /// Node degradation mode (`normal` | `shed` | `drain`). An explicit mode also ends an
    /// automatic shed (entered at `Pressure::Critical`): sysmon will not undo it.
    pub fn set_mode(&self, mode: DegradationMode) {
        self.inner.auto_shed.store(false, Ordering::Release);
        self.inner.set_mode(mode);
    }

    pub fn mode(&self) -> DegradationMode {
        self.inner.mode()
    }

    /// Apply live-updatable knobs (spin, time slice, sysmon tick, rebalance interval, …).
    pub fn reconfigure(&self, live: LiveSchedConfig) {
        self.inner.live.store(Arc::new(live));
        if let Some(t) = self.inner.sysmon_thread.lock().as_ref() {
            t.unpark();
        }
    }

    /// Current live knobs.
    pub fn live_config(&self) -> Arc<LiveSchedConfig> {
        self.inner.live.load_full()
    }

    /// Update the node-wide admission bounds live (`sched.max_inflight`, `sched.max_queued`,
    /// `sched.max_queued_mb`; resolve `0 = auto` with [`AdmissionLimits::resolve`]). `rt-server`
    /// applies them on `SIGHUP` and profile changes; waiting tasks that now fit start.
    pub fn set_admission_limits(&self, limits: AdmissionLimits) {
        let core = &self.inner;
        let now = core.now();
        let granted = core.admission.set_limits(limits, core.rel_ns(now));
        core.promote_all(granted, now);
    }

    /// Current node-wide admission bounds.
    pub fn admission_limits(&self) -> AdmissionLimits {
        self.inner.admission.limits()
    }

    /// Admission state of one service: adaptive limit, running and waiting tasks.
    pub fn service_admission(&self, service: ServiceId) -> Option<ServiceAdmission> {
        self.inner.admission.service(service)
    }

    /// One service's autoscaler view: admission state, learned latency baseline, demand, smoothed
    /// queue wait, queue timeouts (`GET /v1/services/{s}`, `…/stats`).
    pub fn service_scaling(&self, service: ServiceId) -> Option<ServiceScaling> {
        let core = &self.inner;
        core.admission.service_scaling(service, core.rel_ns(core.now()))
    }

    /// Keep a service's `min_instances` floor without traffic (D-B2): call it with the task spec
    /// the service would run with now (live version, resolved knobs) at node start, when a version
    /// is activated and when its scaling config or plan changes. A service with a floor gets
    /// admission state and a warm target at once; one without releases its floor. Returns the
    /// prepared artifact the executors need (load it into T1 so they can pre-warm instances).
    pub fn pin_service(&self, spec: &TaskSpec) -> Option<PreparedKey> {
        self.inner.pin_service(spec)
    }

    /// Release a service's `min_instances` floor (D-B2) because the service was updated or deleted:
    /// its warm target and admission state follow demand and the idle TTL again, and its next
    /// request applies the knobs it then carries. Returns whether it had a floor.
    pub fn release_floor(&self, service: ServiceId) -> bool {
        let core = &self.inner;
        core.admission.release_floor(service, core.rel_ns(core.now()))
    }

    /// Release the `min_instances` floor of every service not in `live` (the services that should
    /// hold one now). A service with a floor keeps its admission state and warm target forever,
    /// so call this periodically (about every minute) in case a release was missed (a delete on
    /// another node, a lost event). Returns how many floors were released.
    pub fn reconcile_floors(&self, live: &std::collections::HashSet<ServiceId>) -> usize {
        let core = &self.inner;
        core.admission.release_floors_except(live, core.rel_ns(core.now()))
    }

    /// The autoscaler view of every service with admission state (`GET /v1/node/scheduler`).
    pub fn services_scaling(&self) -> Vec<ServiceScaling> {
        let core = &self.inner;
        core.admission.services_scaling(core.rel_ns(core.now()))
    }

    /// Tasks waiting for a run slot node-wide, and the bytes charged to them.
    pub fn queued(&self) -> (u32, u64) {
        self.inner.admission.queued()
    }

    /// The warm-capacity targets the autoscaler publishes (the `Arc` given to
    /// [`SchedulerBuilder::warm_targets`], or the scheduler's own).
    pub fn warm_targets(&self) -> Arc<WarmTargets> {
        self.inner.warm.clone()
    }

    /// Warm instances the published targets keep per service (D-B2 `target`): `per_proc` times the
    /// Ps of the artifact's kind, summed over the service's artifacts. A service missing from the
    /// map has no target (it scales to zero).
    pub fn warm_instances(&self) -> std::collections::HashMap<ServiceId, u32> {
        let core = &self.inner;
        let mut out = std::collections::HashMap::new();
        for key in core.warm.snapshot().keys() {
            let procs = u32::try_from(core.count_procs(TaskKind::Exec(key.key.kind))).unwrap_or(u32::MAX);
            let n: &mut u32 = out.entry(key.service).or_insert(0);
            *n = n.saturating_add(core.warm.instances(key, procs));
        }
        out
    }

    /// `Retry-After` for a capacity refusal (`503 overloaded` / `queue_timeout`): about the time
    /// the waiting tasks need to drain at the recent completion rate, within 1–60 s, plus up to
    /// 50 % random jitter (capped at 60 s) so refused clients do not come back in lockstep.
    pub fn retry_after_hint(&self) -> Duration {
        use rand::Rng;
        let base = self.inner.retry_after_ms.load(Ordering::Acquire) as f64;
        let jitter = rand::thread_rng().gen_range(0.0..0.5);
        Duration::from_millis((base * (1.0 + jitter)).clamp(1_000.0, 60_000.0) as u64)
    }

    /// Persistent spinner-count mismatches found by the sysmon self-check (0 when healthy).
    pub fn spinner_mismatches(&self) -> u64 {
        self.inner.spinner_mismatches.load(Ordering::Acquire)
    }

    /// Point-in-time state for schedtrace and `GET /v1/node/scheduler`.
    pub fn snapshot(&self) -> SchedSnapshot {
        self.inner.snapshot()
    }

    /// Stop accepting tasks, wait up to `grace` for running tasks, then kill the rest with
    /// `KillReason::Shutdown`, and join all threads. Needs a tokio runtime with timers.
    pub async fn shutdown(&self, grace: Duration) {
        let core = self.inner.clone();
        core.shutdown.store(true, Ordering::Release);
        let sleep = || tokio::time::sleep(Duration::from_millis(1));

        let deadline = std::time::Instant::now() + grace;
        while core.admission.inflight() > 0 && std::time::Instant::now() < deadline {
            sleep().await;
        }
        core.kill_all();
        let settle = std::time::Instant::now() + Duration::from_millis(500);
        while core.admission.inflight() > 0 && std::time::Instant::now() < settle {
            sleep().await;
        }

        core.stop.store(true, Ordering::Release);
        for m in &core.machines {
            m.notify.notify_one();
        }
        if let Some(t) = core.sysmon_thread.lock().as_ref() {
            t.unpark();
        }
        if let Some(t) = core.autoscale_thread.lock().as_ref() {
            t.unpark();
        }
        let handles = std::mem::take(&mut *core.threads.lock());
        for h in handles {
            let until = std::time::Instant::now() + Duration::from_secs(2);
            while !h.is_finished() && std::time::Instant::now() < until {
                sleep().await;
            }
            if h.is_finished() {
                let _ = h.join();
            }
            // A thread stuck in a guest poll is left behind: it holds nothing the caller needs.
        }
    }
}
