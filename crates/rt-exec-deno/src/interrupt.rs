//! Cross-thread interruption of running isolates (sysmon → `terminate_execution`).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rt_sched::{Interrupt, Interrupter};
use rt_types::{KillReason, TaskId};

/// Where a task's driver leaves its current waker, so a kill reaches a task that is parked on
/// I/O (where `terminate_execution` alone has nothing to interrupt) right away.
pub type WakerCell = Arc<Mutex<Option<std::task::Waker>>>;

/// A running isolate registered with the interrupter.
struct Slot {
    handle: deno_core::v8::IsolateHandle,
    /// Why the task was killed; read by the task's own future to build `Outcome::Killed`.
    reason: Arc<Mutex<Option<KillReason>>>,
    waker: WakerCell,
}

/// One per Machine. `Send + Sync`: sysmon holds it and calls `interrupt` from its own thread.
#[derive(Default)]
pub struct DenoInterrupter {
    tasks: Mutex<HashMap<TaskId, Slot>>,
}

impl DenoInterrupter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register the isolate of `task`; returns the cell the future reads the kill reason from and
    /// the cell its driver stores its waker in on every poll.
    pub fn register(
        &self,
        task: TaskId,
        handle: deno_core::v8::IsolateHandle,
    ) -> (Arc<Mutex<Option<KillReason>>>, WakerCell) {
        let reason = Arc::new(Mutex::new(None));
        let waker: WakerCell = Arc::new(Mutex::new(None));
        self.tasks.lock().insert(
            task,
            Slot {
                handle,
                reason: reason.clone(),
                waker: waker.clone(),
            },
        );
        (reason, waker)
    }

    /// Forget a finished task.
    pub fn unregister(&self, task: TaskId) {
        self.tasks.lock().remove(&task);
    }

    pub fn running(&self) -> usize {
        self.tasks.lock().len()
    }
}

impl Interrupter for DenoInterrupter {
    fn interrupt(&self, task: TaskId, what: Interrupt) {
        match what {
            // V8 cannot yield mid-JS: the scheduler handles hogs by moving the P's queue (hog
            // handoff), not by preempting the isolate.
            Interrupt::Yield => {}
            Interrupt::Kill(reason) => {
                let waker = {
                    let tasks = self.tasks.lock();
                    let Some(slot) = tasks.get(&task) else { return };
                    *slot.reason.lock() = Some(reason);
                    slot.handle.terminate_execution();
                    let w = slot.waker.lock().take();
                    w
                };
                // Running JS is stopped by `terminate_execution`; a driver parked on I/O is
                // polled again now and sees the kill reason.
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }
    }
}
