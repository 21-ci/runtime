//! Submit, placement, wakeups and the waiting lists.

use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_mem::Owner;
use rt_types::{DegradationMode, PreparedKey, RejectReason, RuntimeKind, TaskId, TaskKind, TaskSpec, Trigger};
use tokio::sync::oneshot;

use crate::admission::{AdmitContext, AdmitRequest};
use crate::inner::{mem_category, reserve_bytes, SchedInner, DEADLINE_SHARDS};
use crate::machine::{Machine, NOT_SPINNING, NO_POLL};
use crate::processor::Processor;
use crate::queues::Push;
use crate::task::{Binding, Task, TaskHandle, TaskInfo, TaskShared, TaskState};
use crate::trace::TraceEvent;

/// Where a freshly runnable task goes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Target {
    /// Power of two choices over the kind's Ps, else the GRQ.
    External,
    /// A given P's LRQ (simulation control, `submit_to_proc`).
    Proc(rt_types::ProcId),
    /// `runnext` of the P running the parent task.
    Runnext(rt_types::ProcId),
}

impl SchedInner {
    // -----------------------------------------------------------------------------------------
    // submit
    // -----------------------------------------------------------------------------------------

    fn reject(&self, id: TaskId, r: RejectReason) -> RejectReason {
        self.trace(TraceEvent::Reject { task: id, reason: r });
        r
    }

    /// Pin a service's `min_instances` floor from `spec` (a task spec of the service as it would
    /// run now: version, digest and resolved knobs), without running anything. A service with a
    /// floor gets (or keeps) admission state, and with it a warm target, whether or not it has
    /// traffic; one without releases what it held. Returns the artifact the executors need
    /// loaded (`None` for a spec that is not a guest task, or a kind with no executor).
    pub fn pin_service(&self, spec: &TaskSpec) -> Option<PreparedKey> {
        let kind = spec.kind;
        let slot = self.kind_slot[kind.idx()]?;
        let key = spec.prepared_key(self.kinds[slot].engine_fp)?;
        if spec.knobs.min_instances == 0 {
            self.admission
                .release_floor(spec.service.service, self.rel_ns(self.now()));
        } else {
            let req = AdmitRequest::from_spec(
                spec,
                Some(key),
                reserve_bytes(kind, spec.limits.memory_mb),
                self.rel_ns(self.now()),
            );
            self.admission.seed(&req);
        }
        Some(key)
    }

    /// Admit and enqueue a task. `target` forces the LRQ of a given P (simulation).
    pub fn submit(
        self: &Arc<Self>,
        spec: TaskSpec,
        target: Option<rt_types::ProcId>,
    ) -> Result<TaskHandle, RejectReason> {
        let kind = spec.kind;
        let id = TaskId(self.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        if self.shutdown.load(Ordering::Acquire) {
            return Err(self.reject(id, RejectReason::Draining));
        }
        let Some(slot) = self.kind_slot[kind.idx()] else {
            return Err(self.reject(id, RejectReason::Overloaded));
        };
        let reg = &self.kinds[slot];

        let key = match &spec.trigger {
            Trigger::Build { key } => *key,
            _ => spec.prepared_key(reg.engine_fp).unwrap_or(PreparedKey {
                digest: spec.service.digest,
                kind: RuntimeKind::Deno,
                engine_fp: reg.engine_fp,
            }),
        };
        let mode = self.mode();
        // Only `shed` needs to know whether the artifact is already in T1.
        let t1_ready = match (mode, kind) {
            (DegradationMode::Shed, TaskKind::Exec(_)) => reg.source.is_ready(&key),
            _ => true,
        };
        // Placement decision first: `spec` moves into the task below.
        let target = match (target, &spec.trigger) {
            (Some(p), _) if self.proc(p).kind() == kind => Target::Proc(p),
            (_, Trigger::Call { parent, .. }) => match self.proc_running(*parent, kind) {
                Some(p) => Target::Runnext(p),
                None => Target::External,
            },
            _ => Target::External,
        };

        let enqueued_at = self.now();
        let reserve = reserve_bytes(kind, spec.limits.memory_mb);
        let guest_key = matches!(kind, TaskKind::Exec(_)).then_some(key);
        let req = AdmitRequest::from_spec(&spec, guest_key, reserve, self.rel_ns(enqueued_at));
        let info = TaskInfo {
            tenant: spec.service.tenant,
            service: spec.service.service,
            version: spec.service.version,
            trigger: spec.trigger.label(),
            cpu_limit: spec.limits.cpu,
            wall_limit: spec.limits.wall,
            reserve_bytes: reserve,
            key,
            queue_deadline: enqueued_at.checked_add(spec.knobs.queue_timeout),
        };
        let mut spec = Some(spec);
        let mut made: Option<(Arc<Task>, oneshot::Receiver<rt_types::Outcome>)> = None;
        // The task is built inside `admit`, with its ticket, before any other thread can see it.
        let admitted = self.admission.admit(&req, AdmitContext { mode, t1_ready }, |ticket| {
            let shared = TaskShared::new(id, kind);
            {
                let mut e = shared.ext.lock();
                e.info = Some(info);
                e.ticket = Some(ticket.clone());
            }
            let (tx, rx) = oneshot::channel();
            let task = Arc::new(Task {
                shared,
                spec: Mutex::new(spec.take()),
                enqueued_at,
                binding: Mutex::new(Binding::Unbound),
                last_p: Mutex::new(None),
                done_tx: Mutex::new(Some(tx)),
            });
            self.register_deadline(&task);
            made = Some((task.clone(), rx));
            task
        });
        let admitted = match admitted {
            Ok(a) => a,
            Err(refused) => {
                self.promote_all(refused.granted, enqueued_at);
                return Err(self.reject(id, refused.reason));
            }
        };
        let Some((task, rx)) = made else {
            unreachable!("admit builds the task on success")
        };

        self.submitted.fetch_add(1, Ordering::AcqRel);
        self.trace(TraceEvent::Submit { task: id, kind });
        if let Some(t) = admitted.run {
            t.shared.state.store(TaskState::Runnable);
            self.place_new(&t, target);
        }
        // Waiting tasks granted a slot meanwhile (possibly this one: it queued, then got a slot).
        self.promote_all(admitted.granted, enqueued_at);
        Ok(TaskHandle::new(task.shared.clone(), rx, &task, Arc::downgrade(self)))
    }

    /// Start tasks that were granted a run slot after waiting in their service queue.
    pub fn promote_all(&self, granted: Vec<Arc<Task>>, now: Instant) {
        for task in granted {
            self.promote(&task, now);
        }
    }

    fn promote(&self, task: &Arc<Task>, now: Instant) {
        let granted_ns = self.rel_ns(now);
        if let Some(tk) = task.shared.ext.lock().ticket.as_mut() {
            tk.queued = false;
            tk.granted_ns = granted_ns;
        }
        if let Some(m) = &self.sched_metrics {
            m.admission_queue_wait_seconds
                .observe(now.saturating_duration_since(task.enqueued_at).as_secs_f64());
        }
        if task.shared.state.transition(TaskState::New, TaskState::Runnable) {
            self.place_new(task, Target::External);
        }
    }

    /// The P currently running a poll of `parent`, if it is a P of `kind`.
    fn proc_running(&self, parent: TaskId, kind: TaskKind) -> Option<rt_types::ProcId> {
        let m = self
            .machines
            .iter()
            .find(|m| m.cur_task.load(Ordering::Acquire) == parent.0)?;
        let p = self.proc(m.proc()?);
        (p.kind() == kind).then_some(p.id)
    }

    // -----------------------------------------------------------------------------------------
    // Queue-timeout deadlines
    // -----------------------------------------------------------------------------------------

    fn shard_of(&self, id: TaskId) -> &crate::inner::DeadlineShard {
        &self.deadlines[(id.0 as usize) % DEADLINE_SHARDS]
    }

    /// Index the task by its queue deadline (idempotent).
    pub fn register_deadline(&self, task: &Arc<Task>) {
        let d = task.shared.ext.lock().info.as_ref().and_then(|i| i.queue_deadline);
        if let Some(d) = d {
            self.shard_of(task.id()).lock().insert((d, task.id()), task.clone());
        }
    }

    pub fn remove_deadline(&self, task: &Arc<Task>) {
        let d = task.shared.ext.lock().info.as_ref().and_then(|i| i.queue_deadline);
        if let Some(d) = d {
            self.shard_of(task.id()).lock().remove(&(d, task.id()));
        }
    }

    // -----------------------------------------------------------------------------------------
    // Placement
    // -----------------------------------------------------------------------------------------

    /// Put a `Runnable` task where the placement rules say, then `wakep(kind)`.
    pub fn place_new(&self, task: &Arc<Task>, target: Target) {
        let kind = task.shared.kind;
        match target {
            Target::Runnext(p) => self.place_runnext(task, p),
            Target::Proc(p) => self.place_on_proc(task, self.proc(p)),
            Target::External => self.place_external(task),
        }
        self.wakep(kind);
    }

    /// Power of two choices: two distinct random Ps of the kind, push to the shorter LRQ;
    /// if that one is full (so both are), to `GRQ[kind]`.
    fn place_external(&self, task: &Arc<Task>) {
        let kind = task.shared.kind;
        let n = self.count_procs(kind);
        if n == 0 {
            self.place_grq(task);
            return;
        }
        let (a, b) = self.with_rng(|rng| {
            let a = (rng.next_u64() % n as u64) as usize;
            if n == 1 {
                (a, a)
            } else {
                let mut b = (rng.next_u64() % (n as u64 - 1)) as usize;
                if b >= a {
                    b += 1;
                }
                (a, b)
            }
        });
        let nth = |i: usize| self.procs.iter().filter(|p| p.kind() == kind).nth(i).cloned();
        let (Some(pa), Some(pb)) = (nth(a), nth(b)) else {
            self.place_grq(task);
            return;
        };
        let chosen = if pb.lrq.len() < pa.lrq.len() { pb } else { pa };
        if chosen.lrq.len() >= chosen.lrq.capacity() {
            self.place_grq(task);
        } else {
            self.place_on_proc(task, &chosen);
        }
    }

    fn place_grq(&self, task: &Arc<Task>) {
        self.trace(TraceEvent::Place {
            task: task.id(),
            proc: None,
            grq: true,
            runnext: false,
        });
        self.grq[task.shared.kind.idx()].push_back(task.clone());
    }

    /// Push to `p`'s LRQ; on overflow half of the LRQ plus the task move to the GRQ.
    fn place_on_proc(&self, task: &Arc<Task>, p: &Arc<Processor>) {
        match p.lrq.push_back(task.clone()) {
            Push::Queued => {
                self.trace(TraceEvent::Place {
                    task: task.id(),
                    proc: Some(p.id),
                    grq: false,
                    runnext: false,
                });
            }
            Push::Overflow(batch) => {
                self.trace(TraceEvent::Overflow {
                    proc: p.id,
                    moved: batch.len() as u32,
                });
                self.trace(TraceEvent::Place {
                    task: task.id(),
                    proc: Some(p.id),
                    grq: true,
                    runnext: false,
                });
                self.grq[task.shared.kind.idx()].push_batch(batch);
            }
        }
    }

    /// A child of the task running on `p`: `runnext`; the displaced task goes to the LRQ tail.
    fn place_runnext(&self, task: &Arc<Task>, pid: rt_types::ProcId) {
        let p = self.proc(pid);
        let displaced = p.runnext.lock().replace(task.clone());
        self.trace(TraceEvent::Place {
            task: task.id(),
            proc: Some(pid),
            grq: false,
            runnext: true,
        });
        if let Some(old) = displaced {
            self.push_lrq_tail(p, old);
        }
        // The Machine of `pid` is the only one that drains its P's runnext. It can be parked by now:
        // the poll that placed the task belongs to a bound task, and the Machine's loop may have parked
        // before that poll ran. `wakep` would skip it (a poll is in progress), so wake it here.
        if let Some(m) = self.machine_of(p) {
            self.unpark_machine(m, false);
        }
    }

    /// Requeue at the LRQ tail without a `Place` event (displaced `runnext`, yields).
    fn push_lrq_tail(&self, p: &Arc<Processor>, task: Arc<Task>) {
        let kind_idx = task.shared.kind.idx();
        if let Push::Overflow(batch) = p.lrq.push_back(task) {
            self.trace(TraceEvent::Overflow {
                proc: p.id,
                moved: batch.len() as u32,
            });
            self.grq[kind_idx].push_batch(batch);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Waking Machines
    // -----------------------------------------------------------------------------------------

    /// `wakep(kind)`: if an idle P of `kind` exists and no Machine of that kind is spinning,
    /// wake one Machine (marked spinning) to look for work.
    pub fn wakep(&self, kind: TaskKind) {
        let k = kind.idx();
        // Dekker: the caller pushed work before this; `parked` flags are read after a full fence.
        fence(Ordering::SeqCst);
        if self.spinning[k].load(Ordering::SeqCst) > 0 {
            return;
        }
        // Reserve the (single) spinner slot first so racing wakers cannot both succeed.
        if self.spinning[k]
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        for p in self.procs.iter().filter(|p| p.kind() == kind) {
            if p.status() != rt_types::ProcStatus::Idle {
                continue;
            }
            let Some(m) = self.machine_of(p) else { continue };
            if m.poll_started_ns.load(Ordering::Acquire) != NO_POLL || !self.can_start_new(m, kind) {
                continue;
            }
            if m.parked
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            // The reserved slot becomes this Machine's spinner count, unless the Machine (in its
            // pre-park re-check) already became a spinner itself: then the slot is given back.
            if self.claim_spinner(m, kind) {
                p.set_status(rt_types::ProcStatus::Spinning);
            }
            self.trace(TraceEvent::Unpark { machine: m.id });
            m.notify.notify_one();
            return;
        }
        // Nobody to wake: give the spinner slot back.
        self.spinning[k].fetch_sub(1, Ordering::SeqCst);
    }

    /// Count `m` as a spinner of `kind`, for which the caller already holds one slot of the
    /// per-kind count. The claim is a CAS on `m.spin`: if `m` already counts as a spinner (a
    /// racing claimer won), the caller's slot is given back and `false` returned, so one Machine
    /// is never counted twice.
    fn claim_spinner(&self, m: &Machine, kind: TaskKind) -> bool {
        let k = kind.idx();
        if m.spin
            .compare_exchange(NOT_SPINNING, k as u8, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            true
        } else {
            self.spinning[k].fetch_sub(1, Ordering::SeqCst);
            false
        }
    }

    /// Unpark a specific Machine (bound wakeup, handoff, re-kind). `spin` marks it spinning when
    /// its kind has no spinner yet. Returns whether the Machine was parked.
    pub fn unpark_machine(&self, m: &Arc<Machine>, spin: bool) -> bool {
        fence(Ordering::SeqCst);
        if m.parked
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        self.trace(TraceEvent::Unpark { machine: m.id });
        let proc = m.proc().map(|id| self.proc(id).clone());
        let mut spinning = false;
        if spin {
            if let Some(p) = &proc {
                let kind = p.kind();
                if self.spinning[kind.idx()]
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    spinning = self.claim_spinner(m, kind);
                }
            }
        }
        if let Some(p) = &proc {
            p.set_status(if spinning {
                rt_types::ProcStatus::Spinning
            } else {
                rt_types::ProcStatus::Running
            });
        }
        m.notify.notify_one();
        true
    }

    /// Stop counting `m` as a spinner: release exactly the kind it was counted under (its P may
    /// have been re-kinded since). Returns whether it was spinning.
    pub fn stop_spinning(&self, m: &Machine) -> bool {
        let k = m.spin.swap(NOT_SPINNING, Ordering::AcqRel);
        if k == NOT_SPINNING {
            return false;
        }
        self.spinning[usize::from(k)].fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Try to become a spinning Machine while searching for work (`spinning(kind) < max(1,
    /// busy(kind)/2)`, SCH-08). `force` skips the bound (simulation control).
    pub fn try_spin(&self, m: &Machine, p: &Processor, force: bool) -> bool {
        if m.is_spinning() {
            return true;
        }
        let kind = p.kind();
        let k = kind.idx();
        let busy = self
            .procs
            .iter()
            .filter(|q| q.kind() == kind && q.status() != rt_types::ProcStatus::Idle)
            .count();
        let bound = crate::steal::max_spinning(busy) as u32;
        loop {
            let s = self.spinning[k].load(Ordering::SeqCst);
            if !force && s >= bound {
                return false;
            }
            if self.spinning[k]
                .compare_exchange(s, s + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        // A `wakep` on another thread may have made this Machine a spinner since the check above
        // (it does so while the Machine re-checks before parking): then it is counted already.
        if self.claim_spinner(m, kind) {
            p.set_status(rt_types::ProcStatus::Spinning);
        }
        true
    }

    /// A Machine goes idle (parks): clear its spinner role, mark its P `Idle`, then demote spinners
    /// above `max(1, busy/2)`. The bound is only checked when a Machine becomes a spinner, but
    /// `busy` drops when a P goes idle, so SCH-08 (the bound holds at any instant) needs this.
    pub fn mark_idle(&self, m: &Machine) {
        self.stop_spinning(m);
        let Some(pid) = m.proc() else { return };
        let p = self.proc(pid);
        p.set_status(rt_types::ProcStatus::Idle);
        let kind = p.kind();
        let k = kind.idx();
        let busy = self
            .procs
            .iter()
            .filter(|q| q.kind() == kind && q.status() != rt_types::ProcStatus::Idle)
            .count();
        let bound = crate::steal::max_spinning(busy) as u32;
        while self.spinning[k].load(Ordering::SeqCst) > bound {
            // A demoted Machine keeps searching; it only stops counting as a spinner (flag and
            // count move together, and `stop_spinning`'s swap makes the decrement happen once).
            let demoted = self
                .machines
                .iter()
                .any(|o| o.spin.load(Ordering::Acquire) == k as u8 && self.stop_spinning(o));
            if !demoted {
                break;
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Wakeups of started tasks (simulation: the waker of a task future)
    // -----------------------------------------------------------------------------------------

    /// A wake of `task`'s future. A `Running` task only gets its `NOTIFIED` bit set (the P then
    /// requeues instead of parking it); a `Waiting` one becomes `Runnable` and is enqueued here.
    ///
    /// Simulation only: on real threads a started task lives on its Machine's `LocalSet` and is
    /// woken by tokio (see "Simulation-only paths" in `lib.rs`).
    pub fn wake_task(&self, task: &Arc<Task>) {
        if task.shared.state.state() == TaskState::Done {
            return;
        }
        self.trace(TraceEvent::Wake { task: task.id() });
        if task.shared.state.wake() {
            self.enqueue_woken(task);
        }
    }

    /// Enqueue a started task that became `Runnable` again: bound tasks resume through
    /// `M.bound_ready`; migratable ones go to their last P's LRQ, or the GRQ if it is full.
    /// Simulation only (the sim's waker and its yield path), like `wake_task`.
    pub fn enqueue_woken(&self, task: &Arc<Task>) {
        let kind = task.shared.kind;
        let binding = *task.binding.lock();
        match binding {
            Binding::Bound(mid) => {
                let m = &self.machines[mid.0 as usize];
                m.bound_ready.lock().push_back(task.clone());
                self.unpark_machine(m, false);
            }
            Binding::Unbound => {
                let last = (*task.last_p.lock())
                    .map(|id| self.proc(id).clone())
                    .filter(|p| p.kind() == kind);
                match last {
                    Some(p) if p.lrq.len() < p.lrq.capacity() => self.place_on_proc(task, &p),
                    _ => self.place_grq(task),
                }
                self.wakep(kind);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Single-flight artifact waits (WaitingRuntime)
    // -----------------------------------------------------------------------------------------

    /// The task found its artifact missing: park it until `wake_runtime_waiters(key)`.
    pub fn park_runtime(&self, task: &Arc<Task>, key: PreparedKey) {
        let now = self.now();
        task.shared.ext.lock().wait_rt_since = Some(now);
        task.shared.state.store(TaskState::WaitingRuntime);
        self.wait_rt.lock().entry(key).or_default().push(task.clone());
        self.wait_rt_count.fetch_add(1, Ordering::AcqRel);
        self.trace(TraceEvent::WaitRuntime { task: task.id() });
        // The pick path removed nothing, but sysmon may have dropped the deadline entry meanwhile.
        self.register_deadline(task);
    }

    /// Called by `rt-cache` when the load/build of `key` completed or failed: every waiter moves
    /// to `GRQ[kind]`, FIFO by `enqueued_at`.
    pub fn wake_runtime_waiters(&self, key: &PreparedKey) {
        let mut waiters = {
            let mut w = self.wait_rt.lock();
            let Some(v) = w.remove(key) else { return };
            self.wait_rt_count.fetch_sub(v.len() as u32, Ordering::AcqRel);
            v
        };
        waiters.sort_by_key(|t| (t.enqueued_at, t.id()));
        let now = self.now();
        let mut kinds = [false; 3];
        for t in waiters {
            if !t
                .shared
                .state
                .transition(TaskState::WaitingRuntime, TaskState::Runnable)
            {
                continue;
            }
            {
                let mut e = t.shared.ext.lock();
                if let Some(since) = e.wait_rt_since.take() {
                    t.shared.timings.lock().wait_runtime += now.saturating_duration_since(since);
                }
            }
            self.trace(TraceEvent::Wake { task: t.id() });
            self.place_grq(&t);
            kinds[t.shared.kind.idx()] = true;
        }
        for kind in TaskKind::ALL {
            if kinds[kind.idx()] {
                self.wakep(kind);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Memory waits (WaitingMemory)
    // -----------------------------------------------------------------------------------------

    /// The reservation was refused: join the node-level FIFO. Re-checks once so a release that
    /// raced with the refusal is not missed.
    pub fn park_memory(&self, task: &Arc<Task>) {
        task.shared.state.store(TaskState::WaitingMemory);
        self.wait_mem.lock().insert(task.id(), task.clone());
        self.wait_mem_count.fetch_add(1, Ordering::AcqRel);
        self.trace(TraceEvent::WaitMemory { task: task.id() });
        self.register_deadline(task);
        self.wake_memory_waiters();
    }

    /// Reserve for the waiters in FIFO order while memory lasts and hand them to their GRQ.
    pub fn wake_memory_waiters(&self) {
        if self.wait_mem_count.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut woken: Vec<Arc<Task>> = Vec::new();
        {
            let mut w = self.wait_mem.lock();
            while let Some(task) = w.values().next().cloned() {
                let (bytes, kind) = {
                    let e = task.shared.ext.lock();
                    (e.info.as_ref().map_or(0, |i| i.reserve_bytes), task.shared.kind)
                };
                match self.mem.reserve(mem_category(kind), bytes, Owner::task(task.id())) {
                    Ok(r) => {
                        w.remove(&task.id());
                        self.wait_mem_count.fetch_sub(1, Ordering::AcqRel);
                        task.shared.ext.lock().reservation = Some(r);
                        woken.push(task);
                    }
                    // FIFO: the head blocks the rest.
                    Err(_) => break,
                }
            }
        }
        if woken.is_empty() {
            return;
        }
        let mut kinds = [false; 3];
        for t in woken {
            if !t.shared.state.transition(TaskState::WaitingMemory, TaskState::Runnable) {
                // Cannot happen (removal from the map is the ownership token), but never leak
                // the reservation if it does.
                t.shared.ext.lock().reservation = None;
                continue;
            }
            self.trace(TraceEvent::Wake { task: t.id() });
            self.place_grq(&t);
            kinds[t.shared.kind.idx()] = true;
        }
        for kind in TaskKind::ALL {
            if kinds[kind.idx()] {
                self.wakep(kind);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Queue waits
    // -----------------------------------------------------------------------------------------

    /// How long the task waited between submit and `now` (queue wait, excluding runtime waits).
    pub fn queue_wait(&self, task: &Task, now: Instant) -> Duration {
        let total = now.saturating_duration_since(task.enqueued_at);
        total.saturating_sub(task.shared.timings.lock().wait_runtime)
    }

    /// Of that, the time spent waiting for a P after the run slot was granted (`granted_ns`).
    pub fn run_queue_wait(&self, task: &Task, now: Instant, granted_ns: Option<u64>) -> Duration {
        let since = granted_ns
            .map_or(task.enqueued_at, |g| self.start + Duration::from_nanos(g))
            .max(task.enqueued_at);
        now.saturating_duration_since(since)
            .saturating_sub(task.shared.timings.lock().wait_runtime)
    }
}
