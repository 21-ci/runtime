//! Finishing, cancelling and killing tasks.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rt_obs::{LogKind, LogLevel, LogRecord};
use rt_types::{KillReason, Outcome};

use crate::admission::SlotClass;
use crate::executor::Interrupt;
use crate::inner::{LiveTask, SchedInner};
use crate::task::{Task, TaskState};
use crate::trace::TraceEvent;

impl SchedInner {
    /// Deliver the final outcome of `task` exactly once and release everything it holds:
    /// admission slot (and promote the service's next queued task), queue-timeout entry, bound
    /// count, memory waiters. `live` is the started-task record, if the task ever started.
    /// `claimed`: the caller already moved the task to `Done` itself.
    ///
    /// Callers that own the task's future must drop it *before* calling this, so its memory
    /// reservation is released when the memory waiters are woken.
    pub fn finish(&self, task: &Arc<Task>, outcome: Outcome, live: Option<&Arc<LiveTask>>, claimed: bool) {
        if !claimed && !task.shared.state.mark_done() {
            return;
        }
        let now = self.now();
        let (info, ticket, reservation, started_at, wait_since) = {
            let mut e = task.shared.ext.lock();
            (
                e.info.clone(),
                e.ticket.take(),
                e.reservation.take(),
                e.started_at,
                e.wait_rt_since.take(),
            )
        };
        self.remove_deadline(task);
        if let Some(l) = live {
            let m = &self.machines[l.machine];
            m.live.lock().remove(&l.id);
            if l.bound {
                m.bound_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let timings = {
            let mut t = task.shared.timings.lock();
            match started_at {
                // `init` (the synchronous `Executor::start`) is reported separately.
                Some(s) => t.exec = now.saturating_duration_since(s).saturating_sub(t.init),
                // Never started: everything was waiting.
                None => t.queue = now.saturating_duration_since(task.enqueued_at),
            }
            if let Some(since) = wait_since {
                t.wait_runtime += now.saturating_duration_since(since);
            }
            *t
        };
        match &outcome {
            Outcome::Killed(_) => self.trace(TraceEvent::Kill { task: task.id() }),
            Outcome::Rejected(r) => self.trace(TraceEvent::Reject {
                task: task.id(),
                reason: *r,
            }),
            _ => self.trace(TraceEvent::Finish { task: task.id() }),
        }
        // A waiting (queued) ticket was taken by `cancel_service_queued`: only running ones here.
        let granted = match &ticket {
            Some(tk) if !tk.queued => {
                // Grant → finish, artifact waits excluded: what the autoscaler compares to its baseline.
                let ran = matches!(outcome, Outcome::Ok(_) | Outcome::GuestError) && tk.class != SlotClass::Build;
                let latency = ran.then(|| {
                    let granted = self.start + Duration::from_nanos(tk.granted_ns);
                    now.saturating_duration_since(granted.max(task.enqueued_at))
                        .saturating_sub(timings.wait_runtime)
                });
                self.admission.release(tk, self.rel_ns(now), latency)
            }
            _ => Vec::new(),
        };
        self.completed.fetch_add(1, Ordering::AcqRel);

        if let Some(info) = &info {
            if let Some(s) = &self.stats {
                s.record(info.service, &timings, &outcome);
            }
            if let Some(mt) = &self.metrics {
                mt.task_finished(task.shared.kind, info.trigger, info.service, &outcome, &timings);
            }
            if let (Some(logs), Outcome::Killed(r)) = (&self.logs, &outcome) {
                if *r != KillReason::Shutdown {
                    let mut rec = LogRecord::new(
                        info.service,
                        info.version,
                        LogLevel::Warn,
                        LogKind::System,
                        format!("task killed: {}", r.reason()),
                    );
                    rec.ts_ms = self.clock.unix_ms();
                    rec.tenant_id = Some(info.tenant);
                    rec.task_id = Some(task.id());
                    rec.trigger = info.trigger.to_string();
                    logs.append(rec);
                }
            }
        }
        // Memory taken while waiting for a P but never used.
        drop(reservation);
        if let Some(tx) = task.done_tx.lock().take() {
            let _ = tx.send(outcome);
        }
        self.promote_all(granted, now);
        self.wake_memory_waiters();
    }

    /// Take a task that has not started out of the queue it waits in and finish it with
    /// `outcome` (queue timeout, shutdown). Returns `false` if the task already started or
    /// finished (it lost the race to a picker).
    pub fn cancel_pending(&self, task: &Arc<Task>, outcome: Outcome) -> bool {
        let claimed = match task.shared.state.state() {
            // Stays in its queue as a stale entry; pickers skip tasks they cannot claim.
            TaskState::Runnable => task.shared.state.transition(TaskState::Runnable, TaskState::Done),
            TaskState::New => self.cancel_service_queued(
                task,
                matches!(outcome, Outcome::Rejected(rt_types::RejectReason::QueueTimeout)),
            ),
            TaskState::WaitingRuntime => self.cancel_wait_runtime(task),
            TaskState::WaitingMemory => self.cancel_wait_memory(task),
            _ => false,
        };
        if claimed {
            self.finish(task, outcome, None, true);
        }
        claimed
    }

    /// Take a task out of its service's admission queue (it never got a run slot).
    fn cancel_service_queued(&self, task: &Arc<Task>, timed_out: bool) -> bool {
        let Some(ticket) = task.shared.ext.lock().ticket.clone() else {
            return false;
        };
        if !ticket.queued || !self.admission.cancel(&ticket, timed_out, |t| Arc::ptr_eq(t, task)) {
            // Granted meanwhile: the dispatcher's caller makes it `Runnable`.
            return false;
        }
        // Out of the queue, nobody else can move it: `New → Done` is ours.
        task.shared.ext.lock().ticket = None;
        task.shared.state.transition(TaskState::New, TaskState::Done)
    }

    fn cancel_wait_runtime(&self, task: &Arc<Task>) -> bool {
        let Some(key) = task.shared.ext.lock().info.as_ref().map(|i| i.key) else {
            return false;
        };
        let removed = {
            let mut w = self.wait_rt.lock();
            let mut removed = false;
            let mut empty = false;
            if let Some(v) = w.get_mut(&key) {
                let before = v.len();
                v.retain(|t| !Arc::ptr_eq(t, task));
                removed = v.len() < before;
                empty = v.is_empty();
            }
            if empty {
                w.remove(&key);
            }
            removed
        };
        if !removed {
            return false;
        }
        self.wait_rt_count.fetch_sub(1, Ordering::AcqRel);
        task.shared.state.transition(TaskState::WaitingRuntime, TaskState::Done)
    }

    fn cancel_wait_memory(&self, task: &Arc<Task>) -> bool {
        let removed = self.wait_mem.lock().remove(&task.id()).is_some();
        if !removed {
            return false;
        }
        self.wait_mem_count.fetch_sub(1, Ordering::AcqRel);
        task.shared.state.transition(TaskState::WaitingMemory, TaskState::Done)
    }

    /// Ask a started task to stop with `reason`: interrupt its executor (once), and if the task is
    /// not inside a poll, have its future dropped right away.
    /// Returns whether this call did something (first interrupt, or first drop request).
    pub fn kill_started(&self, live: &Arc<LiveTask>, reason: KillReason) -> bool {
        let mut acted = false;
        if !live.interrupted.swap(true, Ordering::AcqRel) {
            live.interrupter.interrupt(live.id, Interrupt::Kill(reason));
            acted = true;
        }
        let st = live.task.shared.state.state();
        if st != TaskState::Running && st != TaskState::Done && !live.cancelled.swap(true, Ordering::AcqRel) {
            *live.cancel_reason.lock() = Some(reason);
            if self.sim {
                self.drop_queue.lock().push((live.clone(), reason));
            } else {
                live.cancel.notify_one();
            }
            acted = true;
        }
        acted
    }

    /// Shutdown: kill everything that is still around with `KillReason::Shutdown`.
    pub fn kill_all(&self) {
        // Not started yet.
        let mut pending: Vec<Arc<Task>> = Vec::new();
        for shard in &self.deadlines {
            pending.extend(shard.lock().values().cloned());
        }
        for t in pending {
            self.cancel_pending(&t, Outcome::Killed(KillReason::Shutdown));
        }
        // Started.
        for m in &self.machines {
            let lives: Vec<Arc<LiveTask>> = m.live.lock().values().cloned().collect();
            for l in lives {
                let _ = self.kill_started(&l, KillReason::Shutdown);
            }
        }
    }
}
