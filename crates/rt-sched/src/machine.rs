//! Machine (M): an OS thread running a current-thread tokio runtime + `LocalSet`
//! (MVP shape in [`crate::executor`]).

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use rt_types::{MachineId, MachineSnapshot, ProcId, TaskId, TaskKind};

use crate::clock::ThreadCpuClock;
use crate::inner::LiveTask;
use crate::task::Task;

/// `poll_started_ns` value while no poll is running.
pub(crate) const NO_POLL: u64 = u64::MAX;
/// `Machine::spin` value of a Machine that is not counted as a spinner.
pub(crate) const NOT_SPINNING: u8 = u8::MAX;

pub struct Machine {
    pub id: MachineId,
    proc: AtomicI64,
    /// Live bound tasks (each owns an isolate on this thread). Capped by
    /// `deno.max_bound_per_m`; at the cap the M only resumes bound tasks.
    pub bound_count: AtomicU32,
    /// The kind (`TaskKind::idx`) this Machine is counted under in the per-kind spinner count,
    /// or [`NOT_SPINNING`]. "Is a spinner" and "counted for which kind" are one atomic, claimed by
    /// CAS and released by swap, so racing claimers (a Machine's own `try_spin` and another
    /// thread's `wakep`) can never count one Machine twice: a leaked count would make `wakep` and
    /// the lost-wakeup guard skip the kind for good.
    pub(crate) spin: AtomicU8,
    pub parked: AtomicBool,
    /// Bound tasks whose waits completed. Simulation only: filled by `enqueue_woken` (the sim's
    /// waker); on real threads a bound task is woken by tokio on its Machine's `LocalSet`.
    pub bound_ready: Mutex<VecDeque<Arc<Task>>>,
    /// Parker of the real-thread driver (`notify_one` stores a permit: no lost wakeups).
    pub(crate) notify: tokio::sync::Notify,
    /// Nanoseconds since scheduler start at which the poll in progress began (`NO_POLL` if none).
    /// Wall time: hog detection only, never CPU limits.
    pub(crate) poll_started_ns: AtomicU64,
    /// The Machine thread's CPU clock reading when the poll in progress began (ns).
    pub(crate) poll_cpu0_ns: AtomicU64,
    /// The Machine thread's CPU clock (real threads; unset in simulation, where a poll's virtual
    /// time is the CPU it burns).
    pub(crate) cpu_clock: OnceLock<ThreadCpuClock>,
    /// Task being polled (`TaskId.0`, 0 = none) and its kind index.
    pub(crate) cur_task: AtomicU64,
    pub(crate) cur_kind: AtomicU8,
    /// sysmon already handled the hog of the poll in progress.
    pub(crate) hog_handled: AtomicBool,
    /// Last `Interrupt::Yield` sent for the poll in progress (ns since start).
    pub(crate) last_yield_ns: AtomicU64,
    /// Started tasks living on this Machine (limit checks by sysmon).
    pub(crate) live: Mutex<BTreeMap<TaskId, Arc<LiveTask>>>,
}

impl Machine {
    pub fn new(id: MachineId) -> Self {
        Self {
            id,
            proc: AtomicI64::new(-1),
            bound_count: AtomicU32::new(0),
            spin: AtomicU8::new(NOT_SPINNING),
            parked: AtomicBool::new(false),
            bound_ready: Mutex::new(VecDeque::new()),
            notify: tokio::sync::Notify::new(),
            poll_started_ns: AtomicU64::new(NO_POLL),
            poll_cpu0_ns: AtomicU64::new(0),
            cpu_clock: OnceLock::new(),
            cur_task: AtomicU64::new(0),
            cur_kind: AtomicU8::new(0),
            hog_handled: AtomicBool::new(false),
            last_yield_ns: AtomicU64::new(0),
            live: Mutex::new(BTreeMap::new()),
        }
    }

    /// A poll of `task` begins at wall time `at_ns` (hog detection) with the thread's CPU clock
    /// at `cpu0` (CPU estimate of the poll in progress).
    pub(crate) fn begin_poll(&self, task: TaskId, kind: TaskKind, at_ns: u64, cpu0: Duration) {
        self.cur_task.store(task.0, Ordering::Release);
        self.cur_kind.store(kind.idx() as u8, Ordering::Release);
        self.hog_handled.store(false, Ordering::Release);
        self.last_yield_ns.store(at_ns, Ordering::Release);
        self.poll_cpu0_ns
            .store(cpu0.as_nanos().min(u128::from(u64::MAX)) as u64, Ordering::Release);
        self.poll_started_ns.store(at_ns, Ordering::Release);
    }

    /// CPU used so far by the poll in progress, by the Machine thread's CPU clock; `None` without
    /// one (simulation).
    pub(crate) fn poll_cpu(&self) -> Option<Duration> {
        let now = self.cpu_clock.get()?.read()?;
        Some(now.saturating_sub(Duration::from_nanos(self.poll_cpu0_ns.load(Ordering::Acquire))))
    }

    /// The poll ended.
    pub(crate) fn end_poll(&self) {
        self.poll_started_ns.store(NO_POLL, Ordering::Release);
        self.cur_task.store(0, Ordering::Release);
    }

    pub fn proc(&self) -> Option<ProcId> {
        let p = self.proc.load(Ordering::Acquire);
        (p >= 0).then_some(ProcId(p as u32))
    }

    pub fn set_proc(&self, p: Option<ProcId>) {
        self.proc.store(p.map_or(-1, |p| i64::from(p.0)), Ordering::Release);
    }

    /// Counted as a spinner (of some kind).
    pub fn is_spinning(&self) -> bool {
        self.spin.load(Ordering::Acquire) != NOT_SPINNING
    }

    pub fn snapshot(&self) -> MachineSnapshot {
        MachineSnapshot {
            id: self.id,
            proc: self.proc(),
            bound_count: self.bound_count.load(Ordering::Relaxed),
            spinning: self.is_spinning(),
            parked: self.parked.load(Ordering::Relaxed),
        }
    }
}
