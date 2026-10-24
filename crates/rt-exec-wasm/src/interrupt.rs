//! Cross-thread interruption of running WASM tasks via epoch bumps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rt_sched::{Interrupt, Interrupter};
use rt_types::{KillReason, TaskId};
use tokio::sync::Notify;
use wasmtime::Engine;

/// Encoding of the per-task kill cell (0 = running).
fn kill_code(r: KillReason) -> u8 {
    match r {
        KillReason::CpuLimit => 1,
        KillReason::Oom => 2,
        KillReason::WallTimeout => 3,
        KillReason::Shutdown => 4,
    }
}

pub fn kill_from_code(c: u8) -> Option<KillReason> {
    match c {
        1 => Some(KillReason::CpuLimit),
        2 => Some(KillReason::Oom),
        3 => Some(KillReason::WallTimeout),
        4 => Some(KillReason::Shutdown),
        _ => None,
    }
}

/// Per-task kill state: the cell its epoch-deadline callback reads, and a wake-up for a guest
/// that is blocked inside a host call (no epoch is reached while no wasm runs).
#[derive(Clone)]
pub struct KillSwitch {
    pub cell: Arc<AtomicU8>,
    pub notify: Arc<Notify>,
}

impl KillSwitch {
    fn new() -> Self {
        Self {
            cell: Arc::new(AtomicU8::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// The reason the task was told to die, if any.
    pub fn reason(&self) -> Option<KillReason> {
        kill_from_code(self.cell.load(Ordering::Acquire))
    }

    /// Record a kill from inside the task (wall deadline, CPU budget).
    pub fn set(&self, reason: KillReason) {
        self.cell.store(kill_code(reason), Ordering::Release);
    }
}

/// One per Machine. `Yield` bumps the epoch so tasks reach their async-yield epoch deadline;
/// `Kill` additionally sets the task's kill cell, which its epoch-deadline callback checks and
/// turns into a trap (`Killed(reason)`).
pub struct WasmInterrupter {
    engine: Engine,
    tasks: Mutex<HashMap<TaskId, KillSwitch>>,
}

impl WasmInterrupter {
    pub fn new(engine: Engine) -> Arc<Self> {
        Arc::new(Self {
            engine,
            tasks: Mutex::new(HashMap::new()),
        })
    }

    /// Register a task; returns the kill switch its epoch-deadline callback and its driver read.
    pub fn register(&self, task: TaskId) -> KillSwitch {
        let switch = KillSwitch::new();
        self.tasks.lock().insert(task, switch.clone());
        switch
    }

    pub fn unregister(&self, task: TaskId) {
        self.tasks.lock().remove(&task);
    }
}

impl Interrupter for WasmInterrupter {
    fn interrupt(&self, task: TaskId, what: Interrupt) {
        if let Interrupt::Kill(reason) = what {
            if let Some(switch) = self.tasks.lock().get(&task) {
                switch.set(reason);
                switch.notify.notify_one();
            }
        }
        self.engine.increment_epoch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kill_switch_reads_back_every_reason_and_nothing_before_one_is_set() {
        let switch = KillSwitch::new();
        assert_eq!(switch.reason(), None);
        for reason in [
            KillReason::CpuLimit,
            KillReason::Oom,
            KillReason::WallTimeout,
            KillReason::Shutdown,
        ] {
            switch.set(reason);
            assert_eq!(switch.reason(), Some(reason));
        }
        assert_eq!(kill_from_code(0), None);
        assert_eq!(kill_from_code(200), None, "an unknown code is not a kill");
    }

    #[test]
    fn yield_bumps_the_epoch_but_kills_nobody_while_kill_sets_the_reason() {
        let interrupter = WasmInterrupter::new(Engine::default());
        let switch = interrupter.register(TaskId(7));
        interrupter.interrupt(TaskId(7), Interrupt::Yield);
        assert_eq!(switch.reason(), None, "a yield is not a kill");
        interrupter.interrupt(TaskId(7), Interrupt::Kill(KillReason::Oom));
        assert_eq!(switch.reason(), Some(KillReason::Oom));
        interrupter.unregister(TaskId(7));
        interrupter.interrupt(TaskId(7), Interrupt::Kill(KillReason::Shutdown));
        assert_eq!(
            switch.reason(),
            Some(KillReason::Oom),
            "an unregistered task is left alone"
        );
    }
}
