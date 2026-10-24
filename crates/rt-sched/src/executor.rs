//! The executor contract (rt-sched, MVP shape).
//!
//! Each Machine (OS thread) runs a **current-thread tokio runtime + `LocalSet`**. An [`Executor`]
//! is created once per Machine through a factory, is `!Send`, and lives on that thread. The
//! scheduler decides *which Machine starts a task* (placement, stealing, admission); after
//! `start` the returned future is bound to that Machine's `LocalSet` and runs to completion
//! there. This holds for Deno (isolate is thread-pinned) and, in the MVP, for WASM too
//! (migration between Machines is a post-MVP optimization).
//!
//! Preemption: the scheduler never preempts a future itself. Time-slice and limits are enforced
//! through the [`Interrupter`] the executor hands out (WASM: epoch bump → async yield or trap;
//! Deno: `terminate_execution`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_mem::Reservation;
use rt_types::{BuildFailure, KillReason, MachineId, Outcome, PreparedKey, ProcId, TaskId, TaskKind, TaskSpec};

use crate::clock::Clock;

/// A task future. `!Send`: it lives on one Machine's `LocalSet`.
pub type TaskFuture = Pin<Box<dyn Future<Output = Outcome>>>;

/// Per-task context handed to [`Executor::start`].
pub struct ExecCx {
    pub task: TaskId,
    /// The Machine and Processor the task starts on.
    pub machine: MachineId,
    pub proc: ProcId,
    /// Time source (virtual in simulation). Executors must use `clock.sleep` for timers.
    pub clock: Arc<dyn Clock>,
    /// Memory taken at start (Deno: `deno.isolate_base_mb`; WASM: declared initial memory).
    /// Dropping it releases the memory; the executor keeps it for the life of the task.
    pub reservation: Reservation,
    /// Wall-clock deadline (`start + limits.wall`); the scheduler also enforces it.
    pub deadline: Instant,
    /// Current preemption slice (`sched.time_slice`).
    pub time_slice: Duration,
    /// CPU budget (`limits.cpu`); the scheduler also enforces it by thread-CPU accounting.
    pub cpu_budget: Duration,
}

/// What the scheduler asks an executor to do to a running task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interrupt {
    /// Time slice elapsed: WASM bumps the epoch so the guest yields; Deno cannot yield
    /// mid-JS and ignores it (the scheduler does hog handoff instead).
    Yield,
    /// Stop the task now; its future must resolve to `Outcome::Killed(reason)`.
    Kill(KillReason),
}

/// Cross-thread handle used by sysmon (never by the task itself).
pub trait Interrupter: Send + Sync + 'static {
    /// Idempotent and cheap; must not block. Unknown/finished tasks are ignored.
    fn interrupt(&self, task: TaskId, what: Interrupt);
}

/// One instance per Machine (thread-local). May be `!Send`.
pub trait Executor: 'static {
    /// Which task kind this executor runs (`Exec(Deno)`, `Exec(Wasm)`, or `Build`).
    const KIND: TaskKind;

    /// The prepared (compiled) artifact: the T1 cache value. For `Build` executors use `()`.
    type Prepared: Send + Sync + 'static;

    /// Start `spec` and return its future. Setup that fails should resolve the future to an
    /// error outcome rather than panic. The future is spawned on this Machine's `LocalSet`.
    fn start(&mut self, spec: TaskSpec, prepared: Arc<Self::Prepared>, cx: ExecCx) -> TaskFuture;

    /// Handle for interrupting this executor's running tasks from sysmon. Created once per
    /// Machine at registration.
    fn interrupter(&self) -> Arc<dyn Interrupter>;

    /// Called when the Machine has nothing to run: refill isolate pools, dispose finished
    /// isolates, `mi_collect`, trim. Must return within `budget`.
    fn on_idle(&mut self, budget: Duration);

    /// Bounded housekeeping **between two tasks**, for Machines that never park under load
    /// (so [`Executor::on_idle`] never runs). The real-thread driver calls it when
    /// `find_runnable` comes back empty, before spinning, and after every
    /// [`BETWEEN_TASKS_EVERY`] consecutive picks, so deferred work (Deno: dispose finished
    /// isolates, refill the isolate pool) keeps up with the task stream. While tasks wait in the
    /// Machine's own queue (LRQ, `runnext`, bound tasks ready) the period is
    /// [`BETWEEN_TASKS_EVERY_BUSY`] picks instead: a unit can take milliseconds (an isolate).
    ///
    /// Work is done in small units: an executor with pending work does one unit (one isolate
    /// disposed or created) and starts another only while less than `budget` has elapsed. A new
    /// task may be waiting meanwhile, so never do more. The simulation never calls it.
    fn between_tasks(&mut self, _budget: Duration) {}

    /// The earliest instant this executor has housekeeping due on its own (a kept instance's
    /// keep-alive ends), so an idle Machine wakes for it instead of only at the next task. After
    /// waking, the driver runs [`Executor::between_tasks`] as usual. `None`: nothing timed. Must
    /// not return an instant that housekeeping leaves due (that would spin the Machine).
    fn next_deadline(&self) -> Option<Instant> {
        None
    }

    /// Notified by the executor when its [`Executor::next_deadline`] moved earlier or deferred
    /// work appeared while the Machine may be parked (a task finished and kept or retired its
    /// instance). The driver re-reads the deadline and housekeeps. Created once per executor.
    fn wake_signal(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }
}

/// The real-thread driver runs [`Executor::between_tasks`] at least once per this many
/// consecutive picks, so a Machine that always finds work still drains its deferred work.
pub const BETWEEN_TASKS_EVERY: u32 = 16;

/// The period of [`Executor::between_tasks`] while tasks wait in the Machine's own queue.
pub const BETWEEN_TASKS_EVERY_BUSY: u32 = 8 * BETWEEN_TASKS_EVERY;

/// Budget the real-thread driver passes to [`Executor::between_tasks`]: about one unit of work.
pub const BETWEEN_TASKS_BUDGET: Duration = Duration::from_micros(250);

/// Result of a prepared-artifact lookup.
#[derive(Debug)]
pub enum Lookup<P> {
    Ready(Arc<P>),
    /// Load/compile in flight; the task becomes `WaitingRuntime` until
    /// [`crate::Scheduler::wake_runtime_waiters`] is called for the key.
    Pending,
    Failed(BuildFailure),
}

/// Implemented by `rt-cache`: T1/T2/T3 tiering with single-flight loads.
pub trait PreparedSource<P>: Send + Sync + 'static {
    /// Look up `key` on behalf of `waiter`. On a miss the source starts the load/build
    /// (single-flight) and returns `Pending`.
    fn lookup(&self, key: &PreparedKey, waiter: TaskId) -> Lookup<P>;

    /// Is `key` already ready (T1 hit)? Used by `shed` mode admission (in `shed`
    /// only tasks whose artifact is in T1 are admitted). Must not trigger loads.
    fn is_ready(&self, key: &PreparedKey) -> bool;
}

/// Source for executors without a prepared artifact (`Build`, mocks): always ready.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnitSource;

impl PreparedSource<()> for UnitSource {
    fn lookup(&self, _key: &PreparedKey, _waiter: TaskId) -> Lookup<()> {
        Lookup::Ready(Arc::new(()))
    }
    fn is_ready(&self, _key: &PreparedKey) -> bool {
        true
    }
}
