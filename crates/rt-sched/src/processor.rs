//! Processor (P): a scheduling slot with a *kind*, a local run queue and a `runnext` slot
//!.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use rt_types::{MachineId, ProcId, ProcSnapshot, ProcStatus, TaskKind};

use crate::queues::LocalQueue;
use crate::task::Task;

/// Per-P counters.
#[derive(Debug, Default)]
pub struct ProcStats {
    pub busy_ns: AtomicU64,
    pub picks: AtomicU64,
    pub steals_in: AtomicU64,
    pub steals_out: AtomicU64,
    pub preemptions: AtomicU64,
    pub rekinds: AtomicU64,
}

pub struct Processor {
    pub id: ProcId,
    kind: AtomicU8,
    status: AtomicU8,
    /// Local run queue (capacity `sched.lrq_capacity`, 256).
    pub lrq: LocalQueue<Arc<Task>>,
    /// One slot for children/wakes produced by the task currently running on this P.
    /// Never stolen in v1.
    pub runnext: Mutex<Option<Arc<Task>>>,
    /// Incremented per pick (fairness against the GRQ every 61 ticks).
    pub schedtick: AtomicU64,
    machine: AtomicI64,
    pub stats: ProcStats,
    /// When this P last changed kind (re-kinding cooldown).
    pub(crate) last_rekind: Mutex<Option<Instant>>,
}

impl Processor {
    pub fn new(id: ProcId, kind: TaskKind, lrq_capacity: usize) -> Self {
        Self {
            id,
            kind: AtomicU8::new(kind.idx() as u8),
            status: AtomicU8::new(status_bits(ProcStatus::Idle)),
            lrq: LocalQueue::new(lrq_capacity),
            runnext: Mutex::new(None),
            schedtick: AtomicU64::new(0),
            machine: AtomicI64::new(-1),
            stats: ProcStats::default(),
            last_rekind: Mutex::new(None),
        }
    }

    pub fn kind(&self) -> TaskKind {
        TaskKind::ALL[self.kind.load(Ordering::Acquire) as usize]
    }

    /// Only sysmon re-kinding changes a P's kind.
    pub fn set_kind(&self, kind: TaskKind) {
        self.kind.store(kind.idx() as u8, Ordering::Release);
    }

    pub fn status(&self) -> ProcStatus {
        status_from_bits(self.status.load(Ordering::Acquire))
    }

    pub fn set_status(&self, s: ProcStatus) {
        self.status.store(status_bits(s), Ordering::Release);
    }

    /// `from → to` if the status is still `from`.
    pub fn cas_status(&self, from: ProcStatus, to: ProcStatus) -> bool {
        self.status
            .compare_exchange(status_bits(from), status_bits(to), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// The Machine currently owning this P.
    pub fn machine(&self) -> Option<MachineId> {
        let m = self.machine.load(Ordering::Acquire);
        (m >= 0).then_some(MachineId(m as u32))
    }

    pub fn set_machine(&self, m: Option<MachineId>) {
        self.machine.store(m.map_or(-1, |m| i64::from(m.0)), Ordering::Release);
    }

    pub fn snapshot(&self) -> ProcSnapshot {
        ProcSnapshot {
            id: self.id,
            kind: self.kind(),
            status: self.status(),
            lrq_len: self.lrq.len() as u32,
            runnext: self.runnext.lock().as_ref().map(|t| t.id()),
            schedtick: self.schedtick.load(Ordering::Relaxed),
            machine: self.machine(),
            busy_ns: self.stats.busy_ns.load(Ordering::Relaxed),
            steals_in: self.stats.steals_in.load(Ordering::Relaxed),
            steals_out: self.stats.steals_out.load(Ordering::Relaxed),
            preemptions: self.stats.preemptions.load(Ordering::Relaxed),
            rekinds: self.stats.rekinds.load(Ordering::Relaxed),
        }
    }
}

fn status_bits(s: ProcStatus) -> u8 {
    match s {
        ProcStatus::Idle => 0,
        ProcStatus::Running => 1,
        ProcStatus::Spinning => 2,
        ProcStatus::Rekinding => 3,
    }
}

fn status_from_bits(b: u8) -> ProcStatus {
    match b {
        0 => ProcStatus::Idle,
        1 => ProcStatus::Running,
        2 => ProcStatus::Spinning,
        _ => ProcStatus::Rekinding,
    }
}
