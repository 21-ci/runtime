//! Scheduler-internal task: state word, bookkeeping and the user-facing [`TaskHandle`].
//!
//! State machine:
//!
//! ```text
//!  New ─admit→ Runnable ─pick→ Running ─Ready→ Done
//!                ▲ ▲ ▲          │ │ └─ kill → Done(Killed)
//!                │ │ └─Waiting◄─┘ └─ yield → Runnable (LRQ tail)
//!                │ └─ WaitingRuntime (artifact not in T1, before start)
//!                └─── WaitingMemory  (reservation refused, before start)
//! ```
//!
//! A wake that arrives before `Running → Waiting` is committed sets `NOTIFIED`; the P then
//! requeues instead of parking the task (no lost wakeups, SCH-07).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;
use rt_mem::Reservation;
use rt_types::{
    KillReason, Outcome, PreparedKey, ServiceId, TaskId, TaskKind, TaskSpec, TaskTimings, TenantId, VersionNo,
};
use tokio::sync::oneshot;

use crate::admission::AdmissionTicket;
use crate::inner::SchedInner;

/// Task lifecycle states (low bits of the state word).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TaskState {
    New = 0,
    Runnable = 1,
    Running = 2,
    /// Waiting for I/O, a timer or a child task.
    Waiting = 3,
    /// Artifact not in T1; single-flight build/load in progress.
    WaitingRuntime = 4,
    /// Memory reservation refused; node-level FIFO.
    WaitingMemory = 5,
    Done = 6,
}

impl TaskState {
    fn from_bits(b: u32) -> TaskState {
        match b {
            0 => TaskState::New,
            1 => TaskState::Runnable,
            2 => TaskState::Running,
            3 => TaskState::Waiting,
            4 => TaskState::WaitingRuntime,
            5 => TaskState::WaitingMemory,
            _ => TaskState::Done,
        }
    }
}

const STATE_MASK: u32 = 0xff;
/// A wake arrived while the task was `Running` / transitioning to `Waiting`.
const NOTIFIED: u32 = 1 << 8;

/// Atomic state + `NOTIFIED` bit.
#[derive(Debug)]
pub struct StateWord(AtomicU32);

impl StateWord {
    pub fn new(s: TaskState) -> Self {
        Self(AtomicU32::new(s as u32))
    }

    pub fn state(&self) -> TaskState {
        TaskState::from_bits(self.0.load(Ordering::Acquire) & STATE_MASK)
    }

    pub fn is_notified(&self) -> bool {
        self.0.load(Ordering::Acquire) & NOTIFIED != 0
    }

    /// Unconditionally set the state, clearing `NOTIFIED`.
    pub fn store(&self, s: TaskState) {
        self.0.store(s as u32, Ordering::Release);
    }

    /// CAS `from → to` (keeping `NOTIFIED` clear). Returns whether it succeeded.
    pub fn transition(&self, from: TaskState, to: TaskState) -> bool {
        self.0
            .compare_exchange(from as u32, to as u32, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// A wake. If the task is `Running`, sets `NOTIFIED` and returns `false` (the P will requeue
    /// it instead of parking). If `Waiting`, moves it to `Runnable` and returns `true`: the
    /// caller must enqueue it. Other states: no-op, `false`.
    pub fn wake(&self) -> bool {
        let mut cur = self.0.load(Ordering::Acquire);
        loop {
            let st = TaskState::from_bits(cur & STATE_MASK);
            let (next, enqueue) = match st {
                TaskState::Running => (cur | NOTIFIED, false),
                TaskState::Waiting => (TaskState::Runnable as u32, true),
                _ => return false,
            };
            match self.0.compare_exchange(cur, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return enqueue,
                Err(actual) => cur = actual,
            }
        }
    }

    /// `Running → Waiting`, committed only if no wake arrived in between. Returns `false` when
    /// `NOTIFIED` was set: the task stays `Running` (flag cleared) and must be requeued.
    pub fn park(&self) -> bool {
        let mut cur = self.0.load(Ordering::Acquire);
        loop {
            debug_assert_eq!(TaskState::from_bits(cur & STATE_MASK), TaskState::Running);
            if cur & NOTIFIED != 0 {
                match self
                    .0
                    .compare_exchange(cur, TaskState::Running as u32, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return false,
                    Err(actual) => cur = actual,
                }
            } else {
                match self
                    .0
                    .compare_exchange(cur, TaskState::Waiting as u32, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return true,
                    Err(actual) => cur = actual,
                }
            }
        }
    }
}

impl StateWord {
    /// Force the terminal state. Returns `false` if the task was already `Done` (somebody else
    /// finished it first), `true` if this call finished it.
    pub fn mark_done(&self) -> bool {
        let mut cur = self.0.load(Ordering::Acquire);
        loop {
            if TaskState::from_bits(cur & STATE_MASK) == TaskState::Done {
                return false;
            }
            match self
                .0
                .compare_exchange(cur, TaskState::Done as u32, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// Where a started task lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Binding {
    Unbound,
    Bound(rt_types::MachineId),
}

/// State shared between the scheduler and a [`TaskHandle`].
#[derive(Debug)]
pub struct TaskShared {
    pub id: TaskId,
    pub kind: TaskKind,
    pub state: StateWord,
    pub timings: Mutex<TaskTimings>,
    /// Set by `TaskHandle::abort`; the scheduler kills the task at its next opportunity.
    pub abort: Mutex<Option<KillReason>>,
    /// Scheduler bookkeeping (limits copied from the spec, admission ticket, reservation, ...).
    pub(crate) ext: Mutex<TaskExt>,
}

/// What the scheduler needs to know about a task after its `TaskSpec` was consumed at start.
#[derive(Clone, Debug)]
pub(crate) struct TaskInfo {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub version: VersionNo,
    pub trigger: &'static str,
    pub cpu_limit: Duration,
    pub wall_limit: Duration,
    /// Bytes reserved when the task starts.
    pub reserve_bytes: u64,
    /// Key of the prepared artifact (guest tasks) or of the artifact a build produces.
    pub key: PreparedKey,
    /// `(enqueued_at + queue_timeout, id)` while the task has not started.
    pub queue_deadline: Option<Instant>,
}

/// Mutable scheduler-side state of a task.
#[derive(Debug, Default)]
pub(crate) struct TaskExt {
    pub info: Option<TaskInfo>,
    pub ticket: Option<AdmissionTicket>,
    /// Memory taken for the task while it waits for a P (`WaitingMemory` → woken).
    pub reservation: Option<Reservation>,
    pub started_at: Option<Instant>,
    pub wait_rt_since: Option<Instant>,
}

impl TaskShared {
    pub fn new(id: TaskId, kind: TaskKind) -> Arc<Self> {
        Arc::new(Self {
            id,
            kind,
            state: StateWord::new(TaskState::New),
            timings: Mutex::new(TaskTimings::default()),
            abort: Mutex::new(None),
            ext: Mutex::new(TaskExt::default()),
        })
    }
}

/// Scheduler-internal task record (queued in LRQs/GRQs).
pub struct Task {
    pub shared: Arc<TaskShared>,
    /// Taken at start. `TaskSpec` is `Send` but not `Sync`.
    pub spec: Mutex<Option<TaskSpec>>,
    pub enqueued_at: std::time::Instant,
    pub binding: Mutex<Binding>,
    /// P the task last started on. Read only by the simulation's `enqueue_woken` (SCH-06).
    pub last_p: Mutex<Option<rt_types::ProcId>>,
    /// Delivers the outcome to the [`TaskHandle`].
    pub done_tx: Mutex<Option<oneshot::Sender<Outcome>>>,
}

impl Task {
    pub fn id(&self) -> TaskId {
        self.shared.id
    }
}

/// Handle returned by `Scheduler::submit`: a future of the task's [`Outcome`].
///
/// Dropping the handle does **not** cancel the task; call [`TaskHandle::abort`] (e.g. on client
/// disconnect).
pub struct TaskHandle {
    pub id: TaskId,
    shared: Arc<TaskShared>,
    rx: oneshot::Receiver<Outcome>,
    canceller: TaskCanceller,
}

/// Takes a task that has not started out of the queue it waits in (`TaskHandle::canceller`):
/// for a client that went away while its request waited, so the place is not held until the
/// queue timeout. Cheap to clone; holds no strong reference to the task or the scheduler.
#[derive(Clone)]
pub struct TaskCanceller {
    task: Weak<Task>,
    core: Weak<SchedInner>,
}

impl TaskCanceller {
    /// Cancel the task if it is still waiting (for a run slot, a P, its artifact or memory); it
    /// ends `Killed(Shutdown)` and never starts. Returns whether it was cancelled. A started task
    /// is left alone.
    pub fn cancel_if_waiting(&self) -> bool {
        let (Some(task), Some(core)) = (self.task.upgrade(), self.core.upgrade()) else {
            return false;
        };
        core.cancel_pending(&task, Outcome::Killed(KillReason::Shutdown))
    }
}

impl std::fmt::Debug for TaskCanceller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskCanceller").finish_non_exhaustive()
    }
}

impl TaskHandle {
    pub(crate) fn new(
        shared: Arc<TaskShared>,
        rx: oneshot::Receiver<Outcome>,
        task: &Arc<Task>,
        core: Weak<SchedInner>,
    ) -> Self {
        Self {
            id: shared.id,
            shared,
            rx,
            canceller: TaskCanceller {
                task: Arc::downgrade(task),
                core,
            },
        }
    }

    /// A [`TaskCanceller`] for this task (e.g. armed by the ingress while it awaits the outcome).
    pub fn canceller(&self) -> TaskCanceller {
        self.canceller.clone()
    }

    /// Phase timings recorded so far (complete once the future has resolved).
    pub fn timings(&self) -> TaskTimings {
        *self.shared.timings.lock()
    }

    /// Current lifecycle state (diagnostics, tests).
    pub fn state(&self) -> TaskState {
        self.shared.state.state()
    }

    /// Ask the scheduler to kill the task with `reason` (best effort; races with completion).
    pub fn abort(&self, reason: KillReason) {
        *self.shared.abort.lock() = Some(reason);
    }
}

impl Future for TaskHandle {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(o)) => Poll::Ready(o),
            // Scheduler dropped the sender without an outcome: it shut down.
            Poll::Ready(Err(_)) => Poll::Ready(Outcome::Killed(KillReason::Shutdown)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl std::fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle").field("id", &self.id).finish()
    }
}
