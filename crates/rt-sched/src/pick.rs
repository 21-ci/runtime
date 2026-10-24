//! Finding work (`find_runnable`) and starting tasks.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rt_mem::Owner;
use rt_types::{Outcome, ProcId, TaskKind};

use crate::executor::{ExecCx, TaskFuture};
use crate::inner::{mem_category, LiveTask, MachineRt, SchedInner};
use crate::machine::Machine;
use crate::queues::{GlobalQueue, Push};
use crate::scheduler::ErasedLookup;
use crate::steal::{victim_order, STEAL_PASSES};
use crate::task::{Binding, Task, TaskState};
use crate::trace::{PickSource, TraceEvent};

/// Result of [`SchedInner::begin`].
pub(crate) enum Begun {
    /// The executor produced a future; it lives on the Machine from now on.
    Started(TaskFuture, Arc<LiveTask>),
    /// The task waits (artifact, memory) or already finished (failed lookup, abort).
    NotStarted,
}

/// `Runnable → Running`: the picker owns the task from here. Fails for tasks that were shed or
/// aborted while they sat in a queue (lazy deletion).
fn claim(t: &Arc<Task>) -> bool {
    t.shared.state.transition(TaskState::Runnable, TaskState::Running)
}

impl SchedInner {
    fn pop_grq(&self, k: usize) -> Option<Arc<Task>> {
        while let Some(t) = self.grq[k].pop_front() {
            if claim(&t) {
                return Some(t);
            }
        }
        None
    }

    fn pop_bound_ready(&self, m: &Machine) -> Option<Arc<Task>> {
        loop {
            let t = m.bound_ready.lock().pop_front()?;
            if claim(&t) {
                return Some(t);
            }
        }
    }

    /// `find_runnable(P)` for the P owned by `m`: the first hit wins.
    ///
    /// 1. every `grq_fairness_tick`-th pick takes from `GRQ[kind]` first;
    /// 2. `M.bound_ready` (bound Deno tasks hold isolate memory);
    /// 3. `P.runnext`; 4. `P.lrq`; 5. a `GRQ[kind]` batch;
    /// 6. steal half of a same-kind victim's LRQ if the spinning bound allows.
    ///
    /// A Machine at `max_bound_per_m` only resumes bound tasks. `force_spin` skips the spinning
    /// bound (simulation control). The caller parks the Machine when `None` is returned.
    pub fn find_runnable(&self, m: &Arc<Machine>, force_spin: bool) -> Option<(Arc<Task>, PickSource)> {
        let proc = m.proc().map(|id| self.proc(id).clone());

        // 1. fairness against the GRQ.
        if let Some(p) = &proc {
            let kind = p.kind();
            if self.can_start_new(m, kind) {
                let tick = p.schedtick.load(Ordering::Relaxed);
                if tick % self.cfg.grq_fairness_tick.max(1) == 0 {
                    if let Some(t) = self.pop_grq(kind.idx()) {
                        return Some((t, PickSource::GrqFairness));
                    }
                }
            }
        }
        // 2. bound tasks whose waits completed.
        if let Some(t) = self.pop_bound_ready(m) {
            return Some((t, PickSource::BoundReady));
        }
        let p = proc?;
        let kind = p.kind();
        if !self.can_start_new(m, kind) {
            return None;
        }
        let k = kind.idx();

        // 3. runnext.
        let next = p.runnext.lock().take();
        if let Some(t) = next {
            if claim(&t) {
                return Some((t, PickSource::Runnext));
            }
        }
        // 4. local run queue.
        while let Some(t) = p.lrq.pop_front() {
            if claim(&t) {
                return Some((t, PickSource::Lrq));
            }
        }
        // 5. a batch from the global queue: run one, queue the rest locally.
        let glen = self.grq[k].len();
        if glen > 0 {
            let n = GlobalQueue::<Arc<Task>>::refill_batch_size(glen, self.count_procs(kind), self.cfg.lrq_capacity);
            let mut first: Option<Arc<Task>> = None;
            for t in self.grq[k].take_batch(n) {
                if first.is_none() {
                    if claim(&t) {
                        first = Some(t);
                    }
                } else if let Push::Overflow(batch) = p.lrq.push_back(t) {
                    self.grq[k].push_batch(batch);
                }
            }
            if let Some(t) = first {
                return Some((t, PickSource::GrqBatch));
            }
        }
        // 6. steal from the other Ps of the same kind.
        let peers: Vec<_> = self.procs.iter().filter(|q| q.kind() == kind).cloned().collect();
        if peers.len() > 1 && self.try_spin(m, &p, force_spin) {
            let me = peers.iter().position(|q| q.id == p.id)?;
            for _ in 0..STEAL_PASSES {
                let order = self.with_rng(|rng| victim_order(peers.len(), me, rng));
                for v in order {
                    let victim = &peers[v];
                    if victim.lrq.is_empty() {
                        continue;
                    }
                    let n = victim.lrq.steal_half_into(&p.lrq);
                    if n == 0 {
                        continue;
                    }
                    self.trace(TraceEvent::Steal {
                        thief: p.id,
                        victim: victim.id,
                        count: n as u32,
                    });
                    victim.stats.steals_out.fetch_add(n as u64, Ordering::Relaxed);
                    p.stats.steals_in.fetch_add(n as u64, Ordering::Relaxed);
                    if let Some(mt) = &self.metrics {
                        if let Ok(c) = mt.sched_steals_total.get_metric_with_label_values(&[kind.as_str()]) {
                            c.inc_by(n as u64);
                        }
                    }
                    while let Some(t) = p.lrq.pop_front() {
                        if claim(&t) {
                            t.shared.timings.lock().steals += 1;
                            return Some((t, PickSource::Steal));
                        }
                    }
                }
            }
        }
        None
    }

    /// Bookkeeping after a successful `find_runnable`: trace, `schedtick`, P status, and
    /// "a spinning M that found work wakes another spinner".
    pub fn note_pick(&self, m: &Arc<Machine>, task: &Arc<Task>, src: PickSource) {
        let p = m.proc().map(|id| self.proc(id).clone());
        self.trace(TraceEvent::Pick {
            task: task.id(),
            proc: p.as_ref().map_or(ProcId(u32::MAX), |p| p.id),
            machine: m.id,
            from: src,
        });
        if let Some(p) = &p {
            p.schedtick.fetch_add(1, Ordering::Relaxed);
            p.stats.picks.fetch_add(1, Ordering::Relaxed);
            p.set_status(rt_types::ProcStatus::Running);
        }
        if self.stop_spinning(m) {
            if let Some(p) = &p {
                self.wakep(p.kind());
            }
        }
    }

    /// A picked task did not start (it waits for its artifact or memory, or it was aborted or its
    /// build failed): that is not a pick, but the Machine stops spinning and keeps looking.
    fn note_unstarted(&self, m: &Arc<Machine>) {
        self.stop_spinning(m);
        if let Some(p) = m.proc() {
            self.proc(p).set_status(rt_types::ProcStatus::Running);
        }
    }

    /// Start a picked (`Running`) task on `m`: look up the artifact (`WaitingRuntime`), take its
    /// memory (`WaitingMemory`), then hand it to this Machine's executor. Only a task that really
    /// starts counts as a pick (`Pick` event, `schedtick`).
    pub fn begin(
        &self,
        task: &Arc<Task>,
        m: &Arc<Machine>,
        pid: ProcId,
        mrt: &mut MachineRt,
        src: PickSource,
    ) -> Begun {
        let begun = self.begin_inner(task, m, pid, mrt, src);
        if matches!(begun, Begun::NotStarted) {
            self.note_unstarted(m);
        }
        begun
    }

    fn begin_inner(
        &self,
        task: &Arc<Task>,
        m: &Arc<Machine>,
        pid: ProcId,
        mrt: &mut MachineRt,
        src: PickSource,
    ) -> Begun {
        let kind = task.shared.kind;
        let id = task.id();
        let aborted = *task.shared.abort.lock();
        if let Some(reason) = aborted {
            self.finish(task, Outcome::Killed(reason), None, false);
            return Begun::NotStarted;
        }
        let info = task
            .shared
            .ext
            .lock()
            .info
            .clone()
            .expect("a submitted task carries its info");
        let Some(slot) = self.kind_slot[kind.idx()] else {
            self.finish(task, Outcome::Rejected(rt_types::RejectReason::Overloaded), None, false);
            return Begun::NotStarted;
        };
        let reg = &self.kinds[slot];

        // Artifact in T1?
        let prepared = match reg.source.lookup(&info.key, id) {
            ErasedLookup::Ready(a) => a,
            ErasedLookup::Pending => {
                self.park_runtime(task, info.key);
                // The load may have completed between the lookup and the registration.
                if reg.source.is_ready(&info.key) {
                    self.wake_runtime_waiters(&info.key);
                }
                return Begun::NotStarted;
            }
            ErasedLookup::Failed(f) => {
                self.finish(task, Outcome::BuildFailed(f), None, false);
                return Begun::NotStarted;
            }
        };
        // Memory.
        let pre = task.shared.ext.lock().reservation.take();
        let reservation = match pre {
            Some(r) => r,
            None => match self
                .mem
                .reserve(mem_category(kind), info.reserve_bytes, Owner::task(id))
            {
                Ok(r) => r,
                Err(_) => {
                    self.park_memory(task);
                    return Begun::NotStarted;
                }
            },
        };
        let Some(spec) = task.spec.lock().take() else {
            self.finish(task, Outcome::GuestError, None, false);
            return Begun::NotStarted;
        };

        // The task starts: it leaves the queue-timeout index.
        let now = self.now();
        self.remove_deadline(task);
        self.note_pick(m, task, src);
        {
            let mut e = task.shared.ext.lock();
            e.started_at = Some(now);
            e.wait_rt_since = None;
        }
        {
            let q = self.queue_wait(task, now);
            task.shared.timings.lock().queue = q;
            let granted = task.shared.ext.lock().ticket.as_ref().map(|t| t.granted_ns);
            self.qwait[kind.idx()].record(self.run_queue_wait(task, now, granted));
        }
        let bound = kind == TaskKind::DENO;
        if bound {
            m.bound_count.fetch_add(1, Ordering::AcqRel);
            *task.binding.lock() = Binding::Bound(m.id);
        }
        *task.last_p.lock() = Some(pid);
        self.trace(TraceEvent::Start {
            task: id,
            machine: m.id,
        });

        let live_cfg = self.live.load();
        let deadline = now.checked_add(info.wall_limit);
        let cx = ExecCx {
            task: id,
            machine: m.id,
            proc: pid,
            clock: self.clock.clone(),
            reservation,
            deadline: deadline.unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 3600)),
            time_slice: live_cfg.time_slice,
            cpu_budget: info.cpu_limit,
        };
        let exec = mrt.executor(self, kind);
        let started = catch_unwind(AssertUnwindSafe(|| {
            let fut = exec.start(spec, prepared, cx);
            (fut, exec.interrupter())
        }));
        // `TaskTimings.init`: the synchronous part of starting (instance / isolate setup).
        task.shared.timings.lock().init = self.now().saturating_duration_since(now);
        let (fut, interrupter) = match started {
            Ok(x) => x,
            Err(_) => {
                if bound {
                    m.bound_count.fetch_sub(1, Ordering::AcqRel);
                }
                self.finish(task, Outcome::GuestError, None, false);
                return Begun::NotStarted;
            }
        };
        let live = Arc::new(LiveTask {
            task: task.clone(),
            id,
            kind,
            machine: m.id.0 as usize,
            interrupter,
            deadline,
            cpu_limit: info.cpu_limit,
            cpu_ns: std::sync::atomic::AtomicU64::new(0),
            bound,
            interrupted: std::sync::atomic::AtomicBool::new(false),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            cancel_reason: parking_lot::Mutex::new(None),
            cancel: tokio::sync::Notify::new(),
        });
        m.live.lock().insert(id, live.clone());
        Begun::Started(fut, live)
    }
}
