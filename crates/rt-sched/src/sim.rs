//! Deterministic single-thread simulation driver.
//!
//! All Ps and Ms are simulated on one thread: Machines are stepped in a seed-determined order,
//! time is virtual (`SimClock`), steal victims and placement choices come from a seeded RNG.
//! The same Processors, queues, admission and re-kinding code runs as in production; only the
//! thread driver differs. Equal seed + workload ⇒ identical event-trace hash (SCH-21).
//!
//! `rt-testkit::SimRuntime` wraps this with `VirtualClock` and the `MockExecutor` script DSL.
//!
//! ## Time model (the contract the W2 tests assume)
//!
//! - A poll runs instantly in virtual time, but CPU it burns (`Clock::burn_cpu`) keeps its
//!   Machine **busy** for that long: other Machines keep stepping and sysmon keeps ticking
//!   (hog detection, CPU/wall limits, queue timeouts) while the busy Machine's poll result
//!   becomes visible only when its busy interval ends.
//! - `advance(d)` moves virtual time without stepping Machines; sysmon ticks inside it.
//! - `step_proc` / `submit_to_proc` give tests deterministic control over *which* P acts.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rt_types::{KillReason, Outcome, ProcId, RejectReason, TaskId, TaskSpec};

use crate::clock::SimClock;
use crate::executor::TaskFuture;
use crate::inner::{LiveTask, MachineRt, SchedInner};
use crate::machine::Machine;
use crate::monitor::SysmonState;
use crate::pick::Begun;
use crate::scheduler::Scheduler;
use crate::task::{Task, TaskHandle, TaskState};
use crate::trace::{PickSource, TraceEvent};

/// Result of one [`SimScheduler::step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    /// A Machine ran one quantum (picked/started/polled something).
    Progressed,
    /// Nothing runnable now, but timers are pending: virtual time can jump forward.
    Idle { next_timer_in: Duration },
    /// Nothing runnable and no timers: the simulation is quiescent.
    Quiescent,
}

/// Counters of a driven run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimStats {
    pub steps: u64,
    pub virtual_elapsed: Duration,
}

/// A poll whose CPU burn keeps its Machine busy until `until`.
struct Busy {
    until: Instant,
    task: Arc<Task>,
    live: Arc<LiveTask>,
    /// `Some(outcome)` if the poll completed the task, `None` if it returned `Pending`.
    result: Option<Outcome>,
    fut: TaskFuture,
    /// CPU of this poll. Credited to the task's `cpu_ns` only when the interval ends: sysmon adds
    /// the in-flight wall time on top of `cpu_ns`, so crediting early would count it twice.
    burn: Duration,
}

struct SimM {
    rt: MachineRt,
    busy: Option<Busy>,
}

/// The waker of a task future: a wake races the `Running → Waiting` commit through the state word.
struct TaskWaker {
    core: Arc<SchedInner>,
    task: Arc<Task>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.core.wake_task(&self.task);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.core.wake_task(&self.task);
    }
}

/// Single-threaded, virtual-time scheduler. Created by `SchedulerBuilder::build_sim`.
pub struct SimScheduler {
    scheduler: Scheduler,
    core: Arc<SchedInner>,
    clock: Arc<dyn SimClock>,
    ms: Vec<SimM>,
    futs: BTreeMap<TaskId, TaskFuture>,
    lives: BTreeMap<TaskId, Arc<LiveTask>>,
    rng: StdRng,
    sysmon: SysmonState,
    steps: u64,
}

impl SimScheduler {
    pub(crate) fn new(core: Arc<SchedInner>, clock: Arc<dyn SimClock>) -> Self {
        let ms = core
            .machines
            .iter()
            .map(|_| SimM {
                rt: MachineRt::new(),
                busy: None,
            })
            .collect();
        let sysmon = SysmonState::new(&core);
        let rng = StdRng::seed_from_u64(core.cfg.seed ^ 0x51ed_270b_a5e1_0001);
        Self {
            scheduler: Scheduler { inner: core.clone() },
            core,
            clock,
            ms,
            futs: BTreeMap::new(),
            lives: BTreeMap::new(),
            rng,
            sysmon,
            steps: 0,
        }
    }

    /// The same `submit` / `snapshot` / `set_mode` / `reconfigure` API as a real scheduler.
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    fn now(&self) -> Instant {
        self.core.now()
    }

    // -----------------------------------------------------------------------------------------
    // Stepping
    // -----------------------------------------------------------------------------------------

    /// Run one scheduling quantum on one Machine, chosen by the seeded RNG.
    pub fn step(&mut self) -> StepResult {
        self.steps += 1;
        self.complete_due_busy();
        let mut candidates: Vec<usize> = (0..self.ms.len())
            .filter(|&i| self.ms[i].busy.is_none() && !self.core.machines[i].parked.load(Ordering::Acquire))
            .collect();
        candidates.shuffle(&mut self.rng);
        for i in candidates {
            if self.run_quantum(i, false) {
                return StepResult::Progressed;
            }
        }
        self.idle_result()
    }

    /// Run one quantum on the Machine whose home P is `p` (1:1 in v1), ignoring the seeded order
    /// and even if that Machine is parked. It searches exactly like `find_runnable` (bound-ready,
    /// runnext, LRQ, GRQ, steal) and parks when nothing is found.
    /// `Progressed` if it ran something; `Idle`/`Quiescent` if it found nothing (it parked, or
    /// is busy burning CPU).
    pub fn step_proc(&mut self, p: ProcId) -> StepResult {
        self.steps += 1;
        self.complete_due_busy();
        let machine = self.core.proc(p).machine().map(|m| m.0 as usize);
        if let Some(mi) = machine {
            if self.run_quantum(mi, true) {
                // A short burn (within the time slice) is waited out inline so that a stepped
                // task's outcome is visible when the call returns; a hog stays busy.
                let slice = self.core.live.load().time_slice;
                let until = self.ms[mi].busy.as_ref().map(|b| b.until);
                if let Some(until) = until {
                    let now = self.now();
                    if until.saturating_duration_since(now) <= slice {
                        self.advance(until.saturating_duration_since(now));
                    }
                }
                return StepResult::Progressed;
            }
        }
        self.idle_result()
    }

    /// Place `spec` directly on `p`'s LRQ, bypassing power-of-two-choices (same admission, same
    /// overflow-to-GRQ and `wakep` behavior as an external submit). Test control for SCH-01/03/06.
    pub fn submit_to_proc(&self, p: ProcId, spec: TaskSpec) -> Result<TaskHandle, RejectReason> {
        self.core.submit(spec, Some(p))
    }

    /// Step until quiescent, advancing virtual time to each next timer.
    /// Panics (with a diagnostic) after `max_steps` to catch livelocks in tests.
    pub fn run_until_idle(&mut self, max_steps: u64) -> SimStats {
        let start = self.now();
        let steps0 = self.steps;
        loop {
            if self.steps - steps0 >= max_steps {
                let snap = self.core.snapshot();
                panic!(
                    "simulation not idle after {max_steps} steps at {:?}: inflight={} grq={:?} waiting_runtime={} waiting_memory={}",
                    self.now().saturating_duration_since(start),
                    snap.inflight,
                    snap.grq,
                    snap.waiting_runtime,
                    snap.waiting_memory
                );
            }
            match self.step() {
                StepResult::Progressed => {}
                StepResult::Idle { next_timer_in } => {
                    self.advance_until_granted(next_timer_in.max(Duration::from_nanos(1)));
                }
                StepResult::Quiescent => break,
            }
        }
        SimStats {
            steps: self.steps - steps0,
            virtual_elapsed: self.now().saturating_duration_since(start),
        }
    }

    /// Run for `d` of virtual time (stepping and jumping to timers up to the horizon).
    pub fn run_for(&mut self, d: Duration) -> SimStats {
        let start = self.now();
        let horizon = start + d;
        let steps0 = self.steps;
        loop {
            match self.step() {
                StepResult::Progressed => {}
                StepResult::Idle { next_timer_in } => {
                    let now = self.now();
                    if now + next_timer_in >= horizon {
                        if self.advance_until_granted(horizon.saturating_duration_since(now)) {
                            continue;
                        }
                        break;
                    }
                    self.advance_until_granted(next_timer_in.max(Duration::from_nanos(1)));
                }
                StepResult::Quiescent => {
                    if self.advance_until_granted(horizon.saturating_duration_since(self.now())) {
                        continue;
                    }
                    break;
                }
            }
        }
        SimStats {
            steps: self.steps - steps0,
            virtual_elapsed: self.now().saturating_duration_since(start),
        }
    }

    /// Advance virtual time by `d` without running anything (fires due timers, sysmon ticks).
    pub fn advance(&mut self, d: Duration) {
        self.advance_inner(d, false);
    }

    /// `advance`, stopping early (returns `true`) when an autoscaler tick granted waiting tasks a
    /// run slot: the drivers (`run_for`, `run_until_idle`) then step them. Timers and other
    /// sysmon duties keep their usual effect (stepping resumes at the next timer as before).
    fn advance_until_granted(&mut self, d: Duration) -> bool {
        self.advance_inner(d, true)
    }

    fn advance_inner(&mut self, d: Duration, stop_on_grant: bool) -> bool {
        let target = self.now() + d;
        let grants = self.core.sysmon_grants.load(Ordering::Acquire);
        loop {
            let now = self.now();
            let mut next = target.min(self.sysmon.next_wake());
            if let Some(t) = self.clock.next_deadline() {
                next = next.min(t);
            }
            for b in self.ms.iter().filter_map(|m| m.busy.as_ref()) {
                next = next.min(b.until);
            }
            if next > now {
                self.clock.advance(next - now);
            }
            self.complete_due_busy();
            if self.now() >= self.sysmon.next_wake() {
                let core = self.core.clone();
                core.sysmon_run(&mut self.sysmon);
                self.apply_drop_queue();
                if stop_on_grant && self.core.sysmon_grants.load(Ordering::Acquire) != grants {
                    return true;
                }
            }
            if self.now() >= target && self.sysmon.next_wake() > self.now() {
                break;
            }
        }
        false
    }

    /// Hash of every scheduling event so far.
    pub fn trace_hash(&self) -> u64 {
        self.core.trace.as_ref().map_or(0, |t| t.lock().rec.hash())
    }

    /// The recorded events (requires `SchedulerBuilder::keep_trace(true)`).
    pub fn trace(&self) -> Vec<TraceEvent> {
        self.core
            .trace
            .as_ref()
            .map_or_else(Vec::new, |t| t.lock().rec.events().to_vec())
    }

    /// The recorded events with the virtual time (since scheduler start) at which each happened
    /// (requires `SchedulerBuilder::keep_trace(true)`). Same order as [`SimScheduler::trace`].
    pub fn trace_timed(&self) -> Vec<(Duration, TraceEvent)> {
        self.core
            .trace
            .as_ref()
            .map_or_else(Vec::new, |t| t.lock().timed.clone())
    }

    // -----------------------------------------------------------------------------------------
    // One quantum of one Machine
    // -----------------------------------------------------------------------------------------

    fn park(&self, m: &Arc<Machine>) {
        self.core.stop_spinning(m);
        if let Some(p) = m.proc() {
            self.core.proc(p).set_status(rt_types::ProcStatus::Idle);
        }
        if !m.parked.swap(true, Ordering::AcqRel) {
            self.core.trace(TraceEvent::Park { machine: m.id });
        }
    }

    /// `true` if the Machine ran something.
    fn run_quantum(&mut self, mi: usize, force: bool) -> bool {
        if self.ms[mi].busy.is_some() {
            return false;
        }
        let m = self.core.machines[mi].clone();
        let was_parked = m.parked.load(Ordering::Acquire);
        let Some((task, src)) = self.core.find_runnable(&m, force) else {
            self.park(&m);
            return false;
        };
        if was_parked && m.parked.swap(false, Ordering::AcqRel) {
            self.core.trace(TraceEvent::Unpark { machine: m.id });
        }
        self.execute(mi, &m, &task, src);
        true
    }

    /// Start (or resume) the picked task and poll it once.
    fn execute(&mut self, mi: usize, m: &Arc<Machine>, task: &Arc<Task>, src: PickSource) {
        let id = task.id();
        let live = if self.futs.contains_key(&id) {
            // Resuming a started task.
            self.core.note_pick(m, task, src);
            self.lives.get(&id).cloned()
        } else {
            let pid = m.proc().unwrap_or(ProcId(u32::MAX));
            match self.core.begin(task, m, pid, &mut self.ms[mi].rt, src) {
                Begun::Started(fut, live) => {
                    self.futs.insert(id, fut);
                    self.lives.insert(id, live.clone());
                    Some(live)
                }
                Begun::NotStarted => None,
            }
        };
        if let Some(live) = live {
            self.poll_task(mi, m, task, &live);
        }
    }

    fn poll_task(&mut self, mi: usize, m: &Arc<Machine>, task: &Arc<Task>, live: &Arc<LiveTask>) {
        let id = task.id();
        let Some(mut fut) = self.futs.remove(&id) else { return };
        let waker = Waker::from(Arc::new(TaskWaker {
            core: self.core.clone(),
            task: task.clone(),
        }));
        let mut cx = Context::from_waker(&waker);
        let t0 = self.now();
        let cpu0 = self.core.clock.thread_cpu_time();
        m.begin_poll(id, task.shared.kind, self.core.rel_ns(t0), cpu0);
        let polled = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx)));
        let burn = self.core.clock.thread_cpu_time().saturating_sub(cpu0);

        {
            let mut t = task.shared.timings.lock();
            t.polls += 1;
            t.cpu += burn;
        }
        if let Some(p) = m.proc() {
            self.core
                .proc(p)
                .stats
                .busy_ns
                .fetch_add(burn.as_nanos() as u64, Ordering::Relaxed);
        }
        let result = match polled {
            Ok(Poll::Ready(o)) => Some(o),
            Ok(Poll::Pending) => None,
            Err(_) => Some(Outcome::GuestError),
        };
        if burn.is_zero() {
            self.finish_poll(m, task, live, result, fut, burn);
        } else {
            self.ms[mi].busy = Some(Busy {
                until: t0 + burn,
                task: task.clone(),
                live: live.clone(),
                result,
                fut,
                burn,
            });
        }
    }

    /// The poll's result becomes visible (immediately, or when the busy interval ended).
    fn finish_poll(
        &mut self,
        m: &Arc<Machine>,
        task: &Arc<Task>,
        live: &Arc<LiveTask>,
        result: Option<Outcome>,
        fut: TaskFuture,
        burn: Duration,
    ) {
        live.cpu_ns
            .fetch_add(burn.as_nanos().min(u128::from(u64::MAX / 4)) as u64, Ordering::AcqRel);
        m.end_poll();
        let id = task.id();
        match result {
            Some(outcome) => {
                // The future (and its reservation) goes first, then the outcome is delivered.
                drop(fut);
                self.lives.remove(&id);
                self.core.finish(task, outcome, Some(live), false);
            }
            None => {
                self.futs.insert(id, fut);
                if task.shared.state.park() {
                    self.core.trace(TraceEvent::Wait { task: id });
                    task.shared.timings.lock().waits += 1;
                } else {
                    // A wake raced the commit (NOTIFIED): requeue instead of parking.
                    self.core.trace(TraceEvent::Yield { task: id });
                    task.shared.state.store(TaskState::Runnable);
                    self.core.enqueue_woken(task);
                }
                if live.cpu_used() >= live.cpu_limit {
                    let _ = self.core.kill_started(live, KillReason::CpuLimit);
                    self.apply_drop_queue();
                }
            }
        }
    }

    fn complete_due_busy(&mut self) {
        let now = self.now();
        for mi in 0..self.ms.len() {
            if self.ms[mi].busy.as_ref().is_some_and(|b| b.until <= now) {
                if let Some(b) = self.ms[mi].busy.take() {
                    let m = self.core.machines[mi].clone();
                    self.finish_poll(&m, &b.task, &b.live, b.result, b.fut, b.burn);
                }
            }
        }
    }

    /// Drop the futures sysmon asked to kill while they were not inside a poll.
    fn apply_drop_queue(&mut self) {
        loop {
            let items = std::mem::take(&mut *self.core.drop_queue.lock());
            if items.is_empty() {
                break;
            }
            for (live, reason) in items {
                if let Some(fut) = self.futs.remove(&live.id) {
                    drop(fut);
                }
                self.lives.remove(&live.id);
                self.core
                    .finish(&live.task, Outcome::Killed(reason), Some(&live), false);
            }
        }
    }

    /// Nothing to run: when is the next thing that can change that?
    fn idle_result(&self) -> StepResult {
        let now = self.now();
        let mut next: Option<Instant> = self.clock.next_deadline();
        let mut consider = |t: Instant| next = Some(next.map_or(t, |n| n.min(t)));
        for b in self.ms.iter().filter_map(|m| m.busy.as_ref()) {
            consider(b.until);
        }
        // Started tasks end at their wall deadline; sysmon enforces it on its next tick.
        for l in self.lives.values() {
            if let Some(d) = l.deadline {
                consider(if d > now { d } else { self.sysmon.next_tick.max(now) });
            }
        }
        match next {
            Some(t) => StepResult::Idle {
                next_timer_in: t.saturating_duration_since(now),
            },
            None => StepResult::Quiescent,
        }
    }
}
