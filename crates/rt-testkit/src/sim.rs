//! `SimRuntime`: the deterministic whole-scheduler simulation.
//!
//! One thread, virtual time, seeded RNG. Wraps `rt_sched::SimScheduler` with a
//! [`VirtualClock`], a [`TestGovernor`], scripted mock executors for the three task kinds, and a
//! [`MockPreparedSource`]. Equal seed + workload ⇒ identical `trace_hash()` (SCH-21).
//!
//! ```ignore
//! let mut sim = SimRuntime::new(SimConfig::default().seed(7));
//! let t = sim.submit(SpecBuilder::wasm().service(1).finish(), script![cpu(5), io(10)])?;
//! sim.run_until_idle();
//! assert!(sim.outcome(&t).unwrap().is_ok());
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use parking_lot::Mutex;
use rt_sched::trace::TraceEvent;
use rt_sched::{
    KindRange, SchedConfig, Scheduler, SimClock, SimScheduler, SimStats, StepResult, TaskHandle, TaskState, UnitSource,
};
use rt_types::{
    DegradationMode, KillReason, Outcome, PreparedKey, ProcId, RejectReason, SchedSnapshot, TaskId, TaskKind, TaskSpec,
    TaskTimings,
};

use crate::clock::VirtualClock;
use crate::governor::TestGovernor;
use crate::mock::{MockBuildExecutor, MockDenoExecutor, MockPreparedSource, MockWasmExecutor, Script, ScriptBook};

/// Simulation parameters.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub sched: SchedConfig,
    /// Memory budget of the `TestGovernor`.
    pub memory_mb: u64,
    /// Keep the full event trace (not only its hash).
    pub keep_trace: bool,
    /// Unknown prepared keys are ready immediately (no build waits).
    pub auto_ready: bool,
    /// `RT_SCHEDTRACE`: capture one schedtrace line per interval of virtual time
    /// (read them with [`SimRuntime::schedtrace_lines`]).
    pub schedtrace: Option<Duration>,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            sched: SchedConfig::default(),
            memory_mb: 1024,
            keep_trace: true,
            auto_ready: true,
            schedtrace: None,
        }
    }
}

impl SimConfig {
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self.sched.seed = seed;
        self
    }

    pub fn processors(mut self, n: u32) -> Self {
        self.sched.processors = n;
        self
    }

    pub fn memory_mb(mut self, mb: u64) -> Self {
        self.memory_mb = mb;
        self
    }

    pub fn auto_ready(mut self, on: bool) -> Self {
        self.auto_ready = on;
        self
    }

    pub fn keep_trace(mut self, keep: bool) -> Self {
        self.keep_trace = keep;
        self
    }

    /// Exactly `deno` + `wasm` processors with fixed kinds (`min == max`): no re-kinding, so
    /// tests that are not about re-kinding see a stable P set.
    pub fn fixed_split(mut self, deno: u32, wasm: u32) -> Self {
        self.sched.processors = deno + wasm;
        self.sched.deno = KindRange { min: deno, max: deno };
        self.sched.wasm = KindRange { min: wasm, max: wasm };
        self
    }

    /// Capture a schedtrace line every `every` of virtual time (`RT_SCHEDTRACE=<ms>`).
    pub fn schedtrace(mut self, every: Duration) -> Self {
        self.schedtrace = Some(every);
        self
    }

    pub fn sched(mut self, f: impl FnOnce(&mut SchedConfig)) -> Self {
        f(&mut self.sched);
        self
    }
}

/// A submitted task. Poll its outcome with [`SimRuntime::outcome`].
pub struct SimTask {
    pub id: TaskId,
    handle: TaskHandle,
    outcome: Option<Outcome>,
}

impl SimTask {
    /// Phase timings recorded so far (queue wait, exec, CPU; complete once finished).
    pub fn timings(&self) -> TaskTimings {
        self.handle.timings()
    }

    /// Current lifecycle state.
    pub fn state(&self) -> TaskState {
        self.handle.state()
    }

    /// `TaskHandle::abort`: ask the scheduler to kill the task with `reason`.
    pub fn abort(&self, reason: KillReason) {
        self.handle.abort(reason);
    }

    /// `TaskCanceller::cancel_if_waiting` (the client went away before the task started).
    pub fn cancel_if_waiting(&self) -> bool {
        self.handle.canceller().cancel_if_waiting()
    }
}

pub struct SimRuntime {
    clock: Arc<VirtualClock>,
    governor: Arc<TestGovernor>,
    book: Arc<ScriptBook>,
    source: Arc<MockPreparedSource>,
    sim: SimScheduler,
    trace_lines: Arc<Mutex<Vec<String>>>,
}

impl SimRuntime {
    /// Build the simulated scheduler with mock Deno, WASM and Build executors.
    pub fn new(cfg: SimConfig) -> Self {
        let clock = Arc::new(VirtualClock::new());
        let governor = TestGovernor::with_mb(cfg.memory_mb);
        let book = ScriptBook::new();
        let source = MockPreparedSource::new();
        source.set_auto_ready(cfg.auto_ready);

        let mut sched_cfg = cfg.sched.clone();
        sched_cfg.seed = cfg.seed;

        let b1 = book.clone();
        let b2 = book.clone();
        let b3 = book.clone();
        let trace_lines = Arc::new(Mutex::new(Vec::new()));
        let mut builder = Scheduler::builder(sched_cfg, clock.clone(), governor.clone())
            .executor(move || MockDenoExecutor::new(b1.clone()), source.clone(), 0)
            .executor(move || MockWasmExecutor::new(b2.clone()), source.clone(), 0)
            .executor(move || MockBuildExecutor::new(b3.clone()), Arc::new(UnitSource), 0)
            .keep_trace(cfg.keep_trace);
        if let Some(every) = cfg.schedtrace {
            let lines = trace_lines.clone();
            builder = builder.schedtrace(every, Arc::new(move |l: &str| lines.lock().push(l.to_string())));
        }
        let sim = builder
            .build_sim(clock.clone() as Arc<dyn SimClock>)
            .expect("valid sim configuration");

        Self {
            clock,
            governor,
            book,
            source,
            sim,
            trace_lines,
        }
    }

    pub fn clock(&self) -> &Arc<VirtualClock> {
        &self.clock
    }

    pub fn governor(&self) -> &Arc<TestGovernor> {
        &self.governor
    }

    /// Per-service scripts and the mock event log.
    pub fn scripts(&self) -> &Arc<ScriptBook> {
        &self.book
    }

    /// Prepared-artifact source (build waiting tests).
    pub fn source(&self) -> &Arc<MockPreparedSource> {
        &self.source
    }

    pub fn scheduler(&self) -> &Scheduler {
        self.sim.scheduler()
    }

    /// Submit `spec`, running `script` for its service (scripts are per service: use different
    /// services for tasks that must behave differently).
    pub fn submit(&mut self, spec: TaskSpec, script: Script) -> Result<SimTask, RejectReason> {
        self.book.set_for_service(spec.service.service, script);
        self.submit_existing(spec)
    }

    /// Submit using whatever script the service already has (default: no-op).
    pub fn submit_existing(&mut self, spec: TaskSpec) -> Result<SimTask, RejectReason> {
        let handle = self.sim.scheduler().submit(spec)?;
        Ok(SimTask {
            id: handle.id,
            handle,
            outcome: None,
        })
    }

    /// One scheduling quantum.
    pub fn step(&mut self) -> StepResult {
        self.sim.step()
    }

    /// One quantum on the Machine whose home P is `p` (see `SimScheduler::step_proc`).
    pub fn step_proc(&mut self, p: ProcId) -> StepResult {
        self.sim.step_proc(p)
    }

    /// Ids of the processors currently of `kind` (from the snapshot), in P-id order.
    pub fn procs_of(&self, kind: TaskKind) -> Vec<ProcId> {
        self.snapshot()
            .procs
            .iter()
            .filter(|p| p.kind == kind)
            .map(|p| p.id)
            .collect()
    }

    /// Like [`SimRuntime::submit`] but the task is placed directly on `p`'s LRQ.
    pub fn submit_to_proc(&mut self, p: ProcId, spec: TaskSpec, script: Script) -> Result<SimTask, RejectReason> {
        self.book.set_for_service(spec.service.service, script);
        self.submit_existing_to_proc(p, spec)
    }

    /// Like [`SimRuntime::submit_existing`] but the task is placed directly on `p`'s LRQ.
    pub fn submit_existing_to_proc(&mut self, p: ProcId, spec: TaskSpec) -> Result<SimTask, RejectReason> {
        let handle = self.sim.submit_to_proc(p, spec)?;
        Ok(SimTask {
            id: handle.id,
            handle,
            outcome: None,
        })
    }

    /// Run until quiescent, jumping virtual time to each next timer. Panics after `max_steps`.
    pub fn run_until_idle(&mut self) -> SimStats {
        self.sim.run_until_idle(10_000_000)
    }

    /// Run for `d` of virtual time.
    pub fn run_for(&mut self, d: Duration) -> SimStats {
        self.sim.run_for(d)
    }

    /// Advance virtual time without running tasks.
    pub fn advance(&mut self, d: Duration) {
        self.sim.advance(d);
    }

    /// The outcome if the task has finished, polling its handle once.
    pub fn outcome<'a>(&mut self, t: &'a mut SimTask) -> Option<&'a Outcome> {
        if t.outcome.is_none() {
            t.outcome = (&mut t.handle).now_or_never();
        }
        t.outcome.as_ref()
    }

    pub fn snapshot(&self) -> SchedSnapshot {
        self.sim.scheduler().snapshot()
    }

    pub fn set_mode(&self, mode: DegradationMode) {
        self.sim.scheduler().set_mode(mode);
    }

    /// `Scheduler::wake_runtime_waiters` for `key`.
    pub fn wake_runtime_waiters(&self, key: &PreparedKey) {
        self.sim.scheduler().wake_runtime_waiters(key);
    }

    /// Hash of every scheduling event so far (SCH-21).
    pub fn trace_hash(&self) -> u64 {
        self.sim.trace_hash()
    }

    pub fn trace(&self) -> Vec<TraceEvent> {
        self.sim.trace()
    }

    /// The events with their virtual timestamps (since scheduler start).
    pub fn trace_timed(&self) -> Vec<(Duration, TraceEvent)> {
        self.sim.trace_timed()
    }

    /// Schedtrace lines captured so far (`SimConfig::schedtrace`).
    pub fn schedtrace_lines(&self) -> Vec<String> {
        self.trace_lines.lock().clone()
    }
}
