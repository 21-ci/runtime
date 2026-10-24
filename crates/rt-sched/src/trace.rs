//! Event trace of scheduling decisions. In simulation the trace hash identifies a run:
//! equal seed + workload ⇒ identical hash (SCH-21).

use rt_types::{MachineId, ProcId, RejectReason, TaskId, TaskKind};

/// One scheduling event (stable encoding: the hash must not change between runs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TraceEvent {
    Submit {
        task: TaskId,
        kind: TaskKind,
    },
    Reject {
        task: TaskId,
        reason: RejectReason,
    },
    /// Placed on a P's LRQ / runnext / the GRQ.
    Place {
        task: TaskId,
        proc: Option<ProcId>,
        grq: bool,
        runnext: bool,
    },
    Overflow {
        proc: ProcId,
        moved: u32,
    },
    Pick {
        task: TaskId,
        proc: ProcId,
        machine: MachineId,
        from: PickSource,
    },
    Steal {
        thief: ProcId,
        victim: ProcId,
        count: u32,
    },
    Start {
        task: TaskId,
        machine: MachineId,
    },
    Wait {
        task: TaskId,
    },
    Wake {
        task: TaskId,
    },
    Yield {
        task: TaskId,
    },
    WaitRuntime {
        task: TaskId,
    },
    WaitMemory {
        task: TaskId,
    },
    Kill {
        task: TaskId,
    },
    Finish {
        task: TaskId,
    },
    Rekind {
        proc: ProcId,
        from: TaskKind,
        to: TaskKind,
    },
    HogHandoff {
        proc: ProcId,
        moved: u32,
    },
    Park {
        machine: MachineId,
    },
    Unpark {
        machine: MachineId,
    },
}

/// Where a pick found its task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PickSource {
    GrqFairness,
    BoundReady,
    Runnext,
    Lrq,
    GrqBatch,
    Steal,
}

/// Records events and keeps a running FNV-1a hash of them.
#[derive(Clone, Debug)]
pub struct TraceRecorder {
    keep_events: bool,
    events: Vec<TraceEvent>,
    hash: u64,
}

impl TraceRecorder {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    /// `keep_events = false` only maintains the hash (cheap).
    pub fn new(keep_events: bool) -> Self {
        Self {
            keep_events,
            events: Vec::new(),
            hash: Self::FNV_OFFSET,
        }
    }

    pub fn record(&mut self, ev: TraceEvent) {
        // `Debug` output is stable for this enum and avoids a hand-written encoder.
        for b in format!("{ev:?}").bytes() {
            self.hash ^= u64::from(b);
            self.hash = self.hash.wrapping_mul(Self::FNV_PRIME);
        }
        if self.keep_events {
            self.events.push(ev);
        }
    }

    /// Hash of all events so far.
    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }
}
