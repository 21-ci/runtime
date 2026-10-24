//! rt-sched (L2): the GMP scheduler.
//!
//! Vocabulary mirrors Go's runtime: **Task** (G), **Machine** (M, OS thread running a
//! current-thread tokio runtime + `LocalSet`), **Processor** (P, with a *kind*, a local run
//! queue and `runnext`), global run queue per kind, work stealing among same-kind Ps, a
//! `sysmon` thread, and elastic re-kinding. In front of the run queues, admission
//! ([`admission`]) queues tasks per service under adaptive concurrency limits ([`autoscale`],
//! decisions D-A3 and D-B2: a service's instances move between `min_instances` and
//! `max_instances` on queue, latency, CPU and arrival-rate signals) and publishes warm-capacity
//! targets ([`warm`]), with `min_instances` as their floor.
//!
//! Public surface used by other crates:
//! - [`Executor`] / [`PreparedSource`] / [`Clock`] — implemented by executors, `rt-cache`, tests;
//! - [`Scheduler`] / [`SchedulerBuilder`] — `submit` → [`TaskHandle`] (a future of `Outcome`);
//! - [`WarmTargets`] — `SchedulerBuilder::warm_targets` / `Scheduler::warm_targets`;
//! - [`SchedSnapshot`] — schedtrace and `GET /v1/node/scheduler`;
//! - [`SimScheduler`] — deterministic single-thread driver for `rt-testkit::SimRuntime`.
//!
//! Internal modules (`task`, `processor`, `machine`, `queues`, `steal`, `admission`, `autoscale`,
//! `sysmon`, `rekind`, `trace`) are public so lock-free structures and pure decision functions
//! can be tested directly.
//!
//! # Invariants
//!
//! 1. **Spinners.** For every kind `k`, `spinning[k]` equals the number of Machines whose `spin`
//!    is `k`. Only `claim_spinner` and `stop_spinning` (`place.rs`) change a Machine's `spin`,
//!    and they move flag and count together: a reserved count slot is either claimed by a CAS on
//!    `spin` or given back (B0). sysmon checks the equality once per second and counts
//!    persistent mismatches (`rt_sched_spinner_mismatch_total`).
//! 2. **One finisher.** Every state change of a task is a CAS on its state word. Whoever moves
//!    it to `Done` calls `finish` exactly once: the outcome is delivered once and the task's
//!    admission slots, memory reservation, deadline entry and bound count are released once.
//! 3. **Admission ownership.** A task waiting for a run slot (`New`) is owned by its service
//!    queue: only the dispatcher (slot granted → `Runnable`) or `cancel` (queue timeout, client
//!    gone, shutdown → `Done`) takes it out, under the waitlist lock. A running ticket is released
//!    once; the global, tenant and service counters move with the tickets.
//! 4. **No lost wakeups.** Producers publish work before `wakep` reads the `parked` flags, and a
//!    parking Machine publishes `parked` before its final re-check (`SeqCst` fences). In
//!    admission a waiter bumps `queued` before trying the slots under the waitlist lock, and a
//!    releaser frees its slots before reading `queued`.
//! 5. **Lock order.** Service shard → waitlist → deadline shard → task `ext` (a leaf). No queue
//!    lock is held while a Machine is woken; placement happens after the waitlist is released.
//! 6. **Binding.** A started Deno task resumes only on its Machine; a P is re-kinded only while
//!    it is idle and its Machine has `bound_count == 0`.
//! 7. **Determinism.** In simulation every random choice comes from the seeded RNG and every map
//!    iterated by a decision is ordered (`BTreeMap`), so equal seed and workload give an equal
//!    trace hash (SCH-21).
//!
//! # Simulation-only paths
//!
//! On real threads a started task's future lives on its Machine's `LocalSet` and is woken by
//! tokio. Only the simulation driver wakes tasks through the scheduler: `SchedInner::wake_task`
//! (the sim's waker), `enqueue_woken`, `Machine::bound_ready` (filled only by `enqueue_woken`)
//! and `Task::last_p` (read only by `enqueue_woken`). They model the wakeup rules
//! (SCH-05/06) so the simulation can assert them.

pub mod admission;
pub mod autoscale;
pub mod clock;
pub mod config;
pub mod executor;
mod finish;
mod inner;
pub mod machine;
mod monitor;
mod pick;
mod place;
pub mod processor;
pub mod queues;
mod real;
pub mod rekind;
mod sched_metrics;
pub mod scheduler;
pub mod sim;
pub mod steal;
pub mod sysmon;
pub mod task;
pub mod trace;
pub mod warm;

pub use admission::{AdmissionLimits, LimitChange, ServiceAdmission, ServiceScaling, SlotClass};
pub use autoscale::Cause;
pub use clock::{BoxFuture, Clock, SimClock, SystemClock};
pub use config::{KindRange, LiveSchedConfig, SchedConfig};
pub use executor::{
    ExecCx, Executor, Interrupt, Interrupter, Lookup, PreparedSource, TaskFuture, UnitSource, BETWEEN_TASKS_BUDGET,
    BETWEEN_TASKS_EVERY, BETWEEN_TASKS_EVERY_BUSY,
};
pub use rt_types::SchedSnapshot;
pub use scheduler::{SchedError, Scheduler, SchedulerBuilder};
pub use sim::{SimScheduler, SimStats, StepResult};
pub use task::{TaskCanceller, TaskHandle, TaskState};
pub use warm::{after_answer, set_machine_rank, AfterAnswer, Backlog, WarmKey, WarmSpread, WarmTarget, WarmTargets};
