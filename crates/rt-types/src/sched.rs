//! Plain-data scheduler snapshot (docs/api.md API-14).
//!
//! Defined here (not in `rt-sched`) so `rt-obs` (schedtrace formatter) and `rt-api`
//! (`GET /v1/node/scheduler`) can use it without depending on the scheduler.

use serde::{Deserialize, Serialize};

use crate::ids::{MachineId, ProcId, TaskId};
use crate::kinds::{DegradationMode, TaskKind};

/// Processor state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProcStatus {
    Idle,
    Running,
    Spinning,
    Rekinding,
}

/// One processor (P).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcSnapshot {
    pub id: ProcId,
    pub kind: TaskKind,
    pub status: ProcStatus,
    /// Entries in the local run queue.
    pub lrq_len: u32,
    pub runnext: Option<TaskId>,
    pub schedtick: u64,
    /// The machine currently owning this P, if any.
    pub machine: Option<MachineId>,
    pub busy_ns: u64,
    pub steals_in: u64,
    pub steals_out: u64,
    pub preemptions: u64,
    pub rekinds: u64,
}

/// One machine (M, OS thread).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineSnapshot {
    pub id: MachineId,
    /// P currently held (`None` when parked or handed off).
    pub proc: Option<ProcId>,
    /// Live bound (thread-pinned) tasks, e.g. Deno isolates.
    pub bound_count: u32,
    pub spinning: bool,
    pub parked: bool,
}

/// Global run queue lengths per kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrqLens {
    pub deno: u32,
    pub wasm: u32,
    pub build: u32,
}

impl GrqLens {
    pub fn get(&self, kind: TaskKind) -> u32 {
        match kind.idx() {
            0 => self.deno,
            1 => self.wasm,
            _ => self.build,
        }
    }
}

/// Point-in-time scheduler state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedSnapshot {
    /// Time since scheduler start (virtual in sim), milliseconds.
    pub uptime_ms: u64,
    pub mode: DegradationMode,
    pub procs: Vec<ProcSnapshot>,
    pub machines: Vec<MachineSnapshot>,
    pub grq: GrqLens,
    pub idle: u32,
    pub spinning: u32,
    /// Tasks in `WaitingRuntime` (artifact not in T1).
    pub waiting_runtime: u32,
    /// Tasks in `WaitingMemory`.
    pub waiting_memory: u32,
    /// Admitted and not yet finished.
    pub inflight: u32,
    pub tasks_submitted: u64,
    pub tasks_completed: u64,
}

impl Default for SchedSnapshot {
    fn default() -> Self {
        Self {
            uptime_ms: 0,
            mode: DegradationMode::Normal,
            procs: Vec::new(),
            machines: Vec::new(),
            grq: GrqLens::default(),
            idle: 0,
            spinning: 0,
            waiting_runtime: 0,
            waiting_memory: 0,
            inflight: 0,
            tasks_submitted: 0,
            tasks_completed: 0,
        }
    }
}

impl SchedSnapshot {
    /// Number of processors of `kind`.
    pub fn procs_of(&self, kind: TaskKind) -> usize {
        self.procs.iter().filter(|p| p.kind == kind).count()
    }
}
