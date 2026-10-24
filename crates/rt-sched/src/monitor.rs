//! sysmon duties, shared by the sysmon thread and the simulation driver.
//!
//! One *tick* (adaptive: 20 µs … `sysmon.max_tick`, see [`crate::sysmon::TickState`]) does:
//! queue timeouts, limit enforcement (CPU / wall / abort), hog detection (WASM: yield interrupt,
//! Deno: hog handoff) and the lost-wakeup guard. Periodic duties: re-kinding and the autoscaler
//! (`rebalance.interval`), memory (purge requests, automatic shed, memory waiters), stats (with
//! the spinner self-check) and the schedtrace sink.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_mem::{Pressure, PurgeLevel};
use rt_types::{DegradationMode, KillReason, Outcome, RejectReason, RuntimeKind, ServiceId, TaskId, TaskKind};

use crate::admission::{NodeSignals, ServiceReport};
use crate::autoscale::{self, WarmWant};
use crate::config::LiveSchedConfig;
use crate::executor::Interrupt;
use crate::inner::{LiveTask, SchedInner, MEMORY_CHECK_INTERVAL, RETRY_AFTER_MAX, RETRY_AFTER_MIN, STATS_INTERVAL};
use crate::machine::{Machine, NO_POLL};
use crate::rekind::{decide, DonorCandidate, KindLoad, Rekind};
use crate::sysmon::{TickState, MIN_TICK};
use crate::task::Task;
use crate::trace::TraceEvent;
use crate::warm::{WarmKey, WarmSpread, WarmTarget};

/// Sysmon self-check of invariant 1 (`lib.rs`): a kind's spinner count against the Machines
/// whose `spin` names that kind. The two move together but not atomically, so one differing
/// sample can be a claim in flight; the same difference seen at two consecutive checks (1 s
/// apart) is a leak like B0.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SpinnerCheck {
    last: [Option<(u32, u32)>; 3],
}

impl SpinnerCheck {
    /// Record one sample `(count, flagged)` per kind; returns the kinds whose mismatch persisted.
    pub fn observe(&mut self, samples: [(u32, u32); 3]) -> Vec<usize> {
        let mut persistent = Vec::new();
        for (k, &(count, flagged)) in samples.iter().enumerate() {
            let now = (count != flagged).then_some((count, flagged));
            if now.is_some() && now == self.last[k] {
                persistent.push(k);
            }
            self.last[k] = now;
        }
        persistent
    }
}

/// Cadence state of one sysmon (thread or simulated).
pub(crate) struct SysmonState {
    pub tick: TickState,
    pub next_tick: Instant,
    pub next_rebalance: Instant,
    pub next_stats: Instant,
    pub next_mem: Instant,
    pub next_trace: Option<Instant>,
    pub spinners: SpinnerCheck,
    /// Run the autoscaler inside sysmon (simulation). The real-thread driver gives it a thread of
    /// its own so a tick over many services never delays the hog and limit duties.
    pub autoscale_inline: bool,
}

impl SysmonState {
    pub fn new(core: &SchedInner) -> Self {
        let live = core.live.load();
        let s = core.start;
        Self {
            tick: TickState::new(),
            next_tick: s + MIN_TICK,
            next_rebalance: s + live.rebalance_interval.max(Duration::from_millis(1)),
            next_stats: s + STATS_INTERVAL,
            next_mem: s + MEMORY_CHECK_INTERVAL,
            next_trace: core
                .schedtrace
                .as_ref()
                .map(|(every, _)| s + (*every).max(Duration::from_millis(1))),
            spinners: SpinnerCheck::default(),
            autoscale_inline: core.sim,
        }
    }

    /// When sysmon has to run next.
    pub fn next_wake(&self) -> Instant {
        let mut t = self
            .next_tick
            .min(self.next_rebalance)
            .min(self.next_stats)
            .min(self.next_mem);
        if let Some(tr) = self.next_trace {
            t = t.min(tr);
        }
        t
    }
}

impl SchedInner {
    /// Run every duty that is due at the current clock reading.
    pub fn sysmon_run(&self, st: &mut SysmonState) {
        let now = self.now();
        let live = self.live.load_full();
        if now >= st.next_tick {
            let acted = self.sysmon_tick(now, &live);
            st.tick = st.tick.next(acted, live.sysmon_max_tick);
            st.next_tick = now + st.tick.tick;
        }
        if now >= st.next_rebalance {
            self.rebalance(now);
            if st.autoscale_inline {
                self.autoscale(now, &live);
            }
            let every = live.rebalance_interval.max(Duration::from_millis(1));
            while st.next_rebalance <= now {
                st.next_rebalance += every;
            }
        }
        if now >= st.next_mem {
            let pressure = self.mem.pressure();
            if self.wait_mem_count.load(Ordering::Acquire) > 0 && pressure != Pressure::Critical {
                self.wake_memory_waiters();
            }
            self.memory_ladder(now, pressure);
            while st.next_mem <= now {
                st.next_mem += MEMORY_CHECK_INTERVAL;
            }
        }
        if now >= st.next_stats {
            if let Some(m) = &self.metrics {
                m.observe_sched(&self.snapshot());
            }
            self.spinner_self_check(&mut st.spinners);
            while st.next_stats <= now {
                st.next_stats += STATS_INTERVAL;
            }
        }
        if let (Some(t), Some((every, sink))) = (st.next_trace, &self.schedtrace) {
            if now >= t {
                sink(&rt_obs::format_schedtrace(&self.snapshot()));
                let every = (*every).max(Duration::from_millis(1));
                let mut next = t;
                while next <= now {
                    next += every;
                }
                st.next_trace = Some(next);
            }
        }
    }

    /// The per-tick duties. Returns whether sysmon *acted* (which resets its tick to 20 µs).
    fn sysmon_tick(&self, now: Instant, live: &LiveSchedConfig) -> bool {
        let mut acted = false;
        acted |= self.expire_pending(now);
        acted |= self.check_limits(now);
        acted |= self.detect_hogs(now, live);
        acted |= self.wake_guard();
        acted
    }

    /// Tasks not started within their `queue_timeout` are shed (`503 queue_timeout`).
    fn expire_pending(&self, now: Instant) -> bool {
        let mut acted = false;
        for shard in &self.deadlines {
            loop {
                let task: Option<Arc<Task>> = {
                    let mut g = shard.lock();
                    match g.first_key_value() {
                        Some((&(d, _), _)) if d <= now => g.pop_first().map(|(_, t)| t),
                        _ => None,
                    }
                };
                let Some(task) = task else { break };
                if self.cancel_pending(&task, Outcome::Rejected(RejectReason::QueueTimeout)) {
                    acted = true;
                }
            }
        }
        acted
    }

    /// CPU / wall limits and aborts of started tasks.
    fn check_limits(&self, now: Instant) -> bool {
        let now_ns = self.rel_ns(now);
        let mut acted = false;
        for m in &self.machines {
            let lives: Vec<Arc<LiveTask>> = {
                let g = m.live.lock();
                if g.is_empty() {
                    continue;
                }
                g.values().cloned().collect()
            };
            let polling = m.cur_task.load(Ordering::Acquire);
            let polling_since = m.poll_started_ns.load(Ordering::Acquire);
            for l in lives {
                let abort = *l.task.shared.abort.lock();
                let reason = if let Some(r) = abort {
                    Some(r)
                } else if l.deadline.is_some_and(|d| now >= d) {
                    Some(KillReason::WallTimeout)
                } else {
                    let mut used = l.cpu_used();
                    if polling == l.id.0 && polling_since != NO_POLL {
                        // The poll in progress counts with the CPU its thread used, never its wall
                        // time (that grows with contention). Simulation has no thread clock: there
                        // a poll's virtual time is exactly the CPU it burns.
                        used += m
                            .poll_cpu()
                            .unwrap_or_else(|| Duration::from_nanos(now_ns.saturating_sub(polling_since)));
                    }
                    (used >= l.cpu_limit).then_some(KillReason::CpuLimit)
                };
                if let Some(r) = reason {
                    if self.kill_started(&l, r) {
                        acted = true;
                    }
                }
            }
        }
        acted
    }

    /// A poll running longer than the time slice: WASM gets a yield interrupt (epoch bump),
    /// Deno cannot yield mid-JS, so its P's queue is handed off.
    fn detect_hogs(&self, now: Instant, live: &LiveSchedConfig) -> bool {
        let now_ns = self.rel_ns(now);
        let slice_ns = live.time_slice.as_nanos().min(u128::from(u64::MAX / 2)) as u64;
        let mut acted = false;
        for m in &self.machines {
            let started = m.poll_started_ns.load(Ordering::Acquire);
            if started == NO_POLL || now_ns.saturating_sub(started) < slice_ns {
                continue;
            }
            match TaskKind::ALL[(m.cur_kind.load(Ordering::Acquire) as usize) % 3] {
                TaskKind::Build => {}
                TaskKind::Exec(RuntimeKind::Wasm) => {
                    if now_ns.saturating_sub(m.last_yield_ns.load(Ordering::Acquire)) < slice_ns {
                        continue;
                    }
                    m.last_yield_ns.store(now_ns, Ordering::Release);
                    let id = TaskId(m.cur_task.load(Ordering::Acquire));
                    let live = m.live.lock().get(&id).cloned();
                    if let Some(l) = live {
                        l.interrupter.interrupt(id, Interrupt::Yield);
                        l.task.shared.timings.lock().preemptions += 1;
                        if let Some(p) = m.proc() {
                            self.proc(p).stats.preemptions.fetch_add(1, Ordering::Relaxed);
                        }
                        acted = true;
                    }
                }
                TaskKind::Exec(RuntimeKind::Deno) => acted |= self.hog_handoff(m, live),
            }
        }
        acted
    }

    /// Move the hogged P's queue to the GRQ (and, with `handoff_on_hog`, give the P to a spare
    /// Machine; the hog keeps running on its old Machine).
    fn hog_handoff(&self, m: &Arc<Machine>, live: &LiveSchedConfig) -> bool {
        let Some(pid) = m.proc() else { return false };
        let p = self.proc(pid).clone();
        let kind = p.kind();
        let mut moved: Vec<Arc<Task>> = p.lrq.drain_all();
        if let Some(t) = p.runnext.lock().take() {
            moved.push(t);
        }
        let n = moved.len();
        if n > 0 {
            self.grq[kind.idx()].push_batch(moved);
        }
        let mut detached = false;
        if live.handoff_on_hog && !m.hog_handled.load(Ordering::Acquire) {
            let spare = self
                .machines
                .iter()
                .find(|s| s.id != m.id && s.proc().is_none() && s.parked.load(Ordering::Acquire))
                .cloned();
            if let Some(spare) = spare {
                m.hog_handled.store(true, Ordering::Release);
                m.set_proc(None);
                spare.set_proc(Some(pid));
                p.set_machine(Some(spare.id));
                // The spare takes over the P and looks for work.
                self.unpark_machine(&spare, false);
                detached = true;
            }
        }
        if n == 0 && !detached {
            return false;
        }
        self.trace(TraceEvent::HogHandoff {
            proc: pid,
            moved: n as u32,
        });
        p.stats.preemptions.fetch_add(1, Ordering::Relaxed);
        let hog = m.live.lock().get(&TaskId(m.cur_task.load(Ordering::Acquire))).cloned();
        if let Some(l) = hog {
            l.task.shared.timings.lock().preemptions += 1;
        }
        if let Some(mt) = &self.metrics {
            mt.sched_hog_handoffs_total.inc();
        }
        if n > 0 {
            self.wakep(kind);
        }
        true
    }

    /// Lost-wakeup guard: queued work, an idle P that could take it and no spinning Machine →
    /// `wakep`. Returns whether a Machine was woken.
    fn wake_guard(&self) -> bool {
        let mut acted = false;
        for kind in TaskKind::ALL {
            if !self.registered(kind) || self.spinning[kind.idx()].load(Ordering::Acquire) > 0 {
                continue;
            }
            let work =
                !self.grq[kind.idx()].is_empty() || self.procs.iter().any(|p| p.kind() == kind && !p.lrq.is_empty());
            if !work {
                continue;
            }
            self.wakep(kind);
            if self.spinning[kind.idx()].load(Ordering::Acquire) > 0 {
                acted = true;
            }
        }
        acted
    }

    // -----------------------------------------------------------------------------------------
    // Elastic re-kinding
    // -----------------------------------------------------------------------------------------

    /// Sample the load of every guest kind and apply the decisions of [`decide`].
    pub fn rebalance(&self, now: Instant) -> bool {
        let mut loads: Vec<KindLoad> = Vec::with_capacity(2);
        for kind in [TaskKind::DENO, TaskKind::WASM] {
            if !self.registered(kind) {
                continue;
            }
            let (min, max) = self.cfg.kind_bounds(kind);
            let mut procs = 0u32;
            let mut running = 0u32;
            let mut runnable = self.grq[kind.idx()].len() as u32;
            let mut candidates = Vec::new();
            for p in self.procs.iter().filter(|p| p.kind() == kind) {
                procs += 1;
                let status = p.status();
                if status == rt_types::ProcStatus::Running {
                    running += 1;
                }
                runnable += p.lrq.len() as u32 + u32::from(p.runnext.lock().is_some());
                let rekindable = status == rt_types::ProcStatus::Idle
                    && self.machine_of(p).is_some_and(|m| {
                        m.bound_count.load(Ordering::Acquire) == 0 && m.parked.load(Ordering::Acquire)
                    });
                let since = (*p.last_rekind.lock()).map_or(Duration::MAX, |t| now.saturating_duration_since(t));
                candidates.push(DonorCandidate {
                    proc: p.id,
                    rekindable,
                    since_last_rekind: since,
                });
            }
            let waiting_memory = self.wait_mem.lock().values().filter(|t| t.shared.kind == kind).count() as u32;
            let p90_queue_wait = self.update_p90_wait(kind, runnable);
            loads.push(KindLoad {
                kind,
                procs,
                min,
                max,
                runnable,
                running,
                waiting_memory,
                p90_queue_wait,
                candidates,
            });
        }
        let mut acted = false;
        for r in decide(&loads, self.cfg.rebalance_up_threshold, self.cfg.rebalance_cooldown) {
            acted |= self.apply_rekind(r, now);
        }
        acted
    }

    /// Switch an idle P to another kind (`Idle → Rekinding → Idle`) and wake its Machine.
    fn apply_rekind(&self, r: Rekind, now: Instant) -> bool {
        let p = self.proc(r.proc).clone();
        if p.kind() != r.from || !p.cas_status(rt_types::ProcStatus::Idle, rt_types::ProcStatus::Rekinding) {
            return false;
        }
        let machine = self.machine_of(&p).cloned();
        if let Some(m) = &machine {
            // Re-check under the Rekinding status: a bound task may have arrived meanwhile.
            if m.bound_count.load(Ordering::Acquire) != 0 || !m.parked.load(Ordering::Acquire) {
                p.set_status(rt_types::ProcStatus::Idle);
                return false;
            }
        }
        // Whatever still sits in the old kind's queues goes back to that kind's GRQ.
        let mut left: Vec<Arc<Task>> = p.lrq.drain_all();
        if let Some(t) = p.runnext.lock().take() {
            left.push(t);
        }
        let had_left = !left.is_empty();
        if had_left {
            self.grq[r.from.idx()].push_batch(left);
        }
        p.set_kind(r.to);
        p.stats.rekinds.fetch_add(1, Ordering::Relaxed);
        *p.last_rekind.lock() = Some(now);
        self.trace(TraceEvent::Rekind {
            proc: r.proc,
            from: r.from,
            to: r.to,
        });
        tracing::debug!(proc = r.proc.0, from = %r.from, to = %r.to, "re-kinded processor");
        if let Some(mt) = &self.metrics {
            if let Ok(c) = mt
                .sched_rekinds_total
                .get_metric_with_label_values(&[r.from.as_str(), r.to.as_str()])
            {
                c.inc();
            }
        }
        p.set_status(rt_types::ProcStatus::Idle);
        match &machine {
            Some(m) => {
                self.unpark_machine(m, true);
            }
            None => self.wakep(r.to),
        }
        if had_left {
            self.wakep(r.from);
        }
        true
    }
}

// ---------------------------------------------------------------------------------------------
// Autoscaler, warm targets, memory ladder, self-check
// ---------------------------------------------------------------------------------------------

/// Weight of a new p90 sample in the run-queue-wait EWMA.
const P90_ALPHA: f64 = 0.3;

impl SchedInner {
    /// Fold the run-queue waits recorded since the last call into the kind's p90 EWMA. Without
    /// samples the EWMA holds while tasks are runnable (nothing started: still saturated) and
    /// decays otherwise.
    fn update_p90_wait(&self, kind: TaskKind, runnable: u32) -> Duration {
        let k = kind.idx();
        let old = self.p90_wait_ns[k].load(Ordering::Acquire) as f64;
        let next = match self.qwait[k].take_p90() {
            Some(p) => old + P90_ALPHA * (p.as_nanos() as f64 - old),
            None if runnable > 0 => old,
            None => old * (1.0 - P90_ALPHA),
        };
        self.p90_wait_ns[k].store(next as u64, Ordering::Release);
        Duration::from_nanos(next as u64)
    }

    /// The kinds whose Ps are saturated: p90 run-queue wait above twice the time slice.
    fn cpu_saturated(&self, live: &LiveSchedConfig) -> [bool; 3] {
        let bound = (live.time_slice * 2).max(Duration::from_millis(1));
        let mut out = [false; 3];
        for kind in [TaskKind::DENO, TaskKind::WASM] {
            out[kind.idx()] = Duration::from_nanos(self.p90_wait_ns[kind.idx()].load(Ordering::Acquire)) > bound;
        }
        out
    }

    /// One autoscaler step (decision D-A3): adapt every service's concurrency limit, start the
    /// waiting tasks that fit the new limits, publish warm targets and the admission metrics.
    pub fn autoscale(&self, now: Instant, live: &LiveSchedConfig) {
        let pressure = self.mem.pressure();
        let sig = NodeSignals {
            now_ns: self.rel_ns(now),
            pressure,
            cpu_saturated: self.cpu_saturated(live),
        };
        let mut reports = Vec::new();
        let granted = self.admission.tick(&sig, &mut reports);
        if !granted.is_empty() {
            self.sysmon_grants.fetch_add(granted.len() as u64, Ordering::AcqRel);
            self.promote_all(granted, now);
        }
        for r in &reports {
            if let Some((old, cause)) = r.change {
                tracing::debug!(
                    service = %r.admission.service,
                    from = old,
                    to = r.admission.limit,
                    cause = cause.as_str(),
                    queued = r.admission.queued,
                    rps = r.rps,
                    "instance limit adjusted"
                );
            }
        }
        self.update_retry_after(now);
        let (warm, per_service) = self.publish_warm(&reports, pressure);
        if let (Some(sm), Some(m)) = (&self.sched_metrics, &self.metrics) {
            sm.observe_tick(
                m,
                self.admission.running(),
                self.admission.queued(),
                &reports,
                warm,
                &per_service,
            );
        }
    }

    /// `Retry-After` base: the time the queue needs to drain at the recent completion rate,
    /// within `[1 s, 60 s]` (60 s when nothing completes).
    fn update_retry_after(&self, now: Instant) {
        let completed = self.completed.load(Ordering::Acquire);
        let mut d = self.drain.lock();
        if let Some(at) = d.at {
            let dt = now.saturating_duration_since(at).as_secs_f64();
            if dt <= 0.0 {
                return;
            }
            let sample = completed.saturating_sub(d.completed) as f64 / dt;
            d.rate = if d.rate == 0.0 {
                sample
            } else {
                d.rate + 0.3 * (sample - d.rate)
            };
        }
        d.at = Some(now);
        d.completed = completed;
        let queued = f64::from(self.admission.queued().0);
        let secs = if queued == 0.0 {
            RETRY_AFTER_MIN.as_secs_f64()
        } else if d.rate > 0.0 {
            queued / d.rate
        } else {
            RETRY_AFTER_MAX.as_secs_f64()
        };
        let ms = (secs.clamp(RETRY_AFTER_MIN.as_secs_f64(), RETRY_AFTER_MAX.as_secs_f64()) * 1000.0) as u64;
        self.retry_after_ms.store(ms, Ordering::Release);
    }

    /// Warm targets from recent demand and each service's `min_instances` floor (see
    /// [`crate::autoscale`]); returns warm instances per kind and per service.
    fn publish_warm(&self, reports: &[ServiceReport], pressure: Pressure) -> ([u64; 3], HashMap<ServiceId, u32>) {
        let mut keys = Vec::new();
        let mut wants = Vec::new();
        for r in reports {
            let m = &r.meta;
            let (TaskKind::Exec(_), Some(key)) = (m.kind, m.key) else {
                continue;
            };
            let procs = self.count_procs(m.kind) as u32;
            // `min_instances` is kept warm whatever the demand and the idle TTL (D-B2), within
            // the floors' share of the warm budget
            let floor = if procs == 0 { 0 } else { m.floor_instances() };
            let per_proc = if r.idle_for > m.idle_ttl {
                0 // scale to zero
            } else {
                autoscale::warm_per_proc(
                    r.demand,
                    m.scaling.warm_headroom_pct,
                    procs,
                    m.scaling.warm_max_per_proc,
                )
            };
            if per_proc == 0 && floor == 0 {
                continue;
            }
            keys.push((
                WarmKey {
                    service: m.service.service,
                    key,
                },
                m.service,
                m.kind,
            ));
            wants.push(WarmWant {
                demand: r.demand,
                per_proc,
                floor,
                procs,
                instance_bytes: m.instance_bytes,
            });
        }
        let budget = (self.mem.budget_bytes() as f64 * self.cfg.warm_share.clamp(0.0, 1.0)) as u64;
        autoscale::fit_warm(&mut wants, pressure, budget);
        let mut totals = [0u64; 3];
        let mut per_service: HashMap<ServiceId, u32> = HashMap::new();
        let mut targets = HashMap::with_capacity(keys.len());
        let mut spread = HashMap::new();
        for ((wk, service, kind), w) in keys.into_iter().zip(wants) {
            let n = w.instances();
            if n == 0 {
                continue;
            }
            let (base, extra) = w.spread();
            targets.insert(
                wk,
                WarmTarget {
                    service,
                    per_proc: base + u32::from(extra > 0),
                },
            );
            if extra > 0 {
                spread.insert(
                    wk,
                    WarmSpread {
                        base,
                        extra,
                        procs: w.procs,
                    },
                );
            }
            totals[kind.idx()] += n;
            let s = per_service.entry(wk.service).or_insert(0);
            *s = s.saturating_add(u32::try_from(n).unwrap_or(u32::MAX));
        }
        self.warm.publish_spread(targets, spread);
        (totals, per_service)
    }

    /// The memory ladder: at `High` and above ask Machines to purge their allocators
    /// (at most once per `memory.purge_min_interval`; `Full` at `Critical`). At `Critical` enter
    /// node `shed` mode automatically; leave it again once pressure is back below `High`. A mode
    /// set by the operator is never overridden.
    fn memory_ladder(&self, now: Instant, pressure: Pressure) {
        if pressure >= Pressure::High {
            let mut last = self.last_purge.lock();
            if last.is_none_or(|t| now.saturating_duration_since(t) >= self.cfg.purge_min_interval) {
                *last = Some(now);
                let level = if pressure == Pressure::Critical {
                    PurgeLevel::Full
                } else {
                    PurgeLevel::Light
                };
                self.mem.request_purge(level);
            }
        }
        match pressure {
            Pressure::Critical if self.mode() == DegradationMode::Normal => {
                self.set_mode(DegradationMode::Shed);
                self.auto_shed.store(true, Ordering::Release);
                tracing::warn!("memory pressure critical: node enters shed mode");
            }
            Pressure::Normal
                if self.auto_shed.swap(false, Ordering::AcqRel) && self.mode() == DegradationMode::Shed =>
            {
                self.set_mode(DegradationMode::Normal);
                tracing::info!("memory pressure back below high: node leaves shed mode");
            }
            _ => {}
        }
    }

    /// Invariant 1 (`lib.rs`): per kind, spinner count == Machines whose `spin` names the kind.
    fn spinner_self_check(&self, check: &mut SpinnerCheck) {
        let samples: [(u32, u32); 3] = std::array::from_fn(|k| {
            let count = self.spinning[k].load(Ordering::SeqCst);
            let flagged = self
                .machines
                .iter()
                .filter(|m| m.spin.load(Ordering::SeqCst) == k as u8)
                .count() as u32;
            (count, flagged)
        });
        for k in check.observe(samples) {
            self.spinner_mismatches.fetch_add(1, Ordering::AcqRel);
            if let Some(sm) = &self.sched_metrics {
                sm.spinner_mismatch_total.inc();
            }
            tracing::error!(
                kind = TaskKind::ALL[k].as_str(),
                count = samples[k].0,
                spinning_machines = samples[k].1,
                "scheduler invariant broken: spinner count disagrees with the spinning Machines"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inner::WaitHist;

    #[test]
    fn a_spinner_mismatch_counts_only_when_it_persists() {
        let mut c = SpinnerCheck::default();
        assert!(c.observe([(1, 1), (0, 0), (0, 0)]).is_empty());
        assert!(
            c.observe([(1, 0), (0, 0), (0, 0)]).is_empty(),
            "one sample may be a claim in flight"
        );
        assert_eq!(
            c.observe([(1, 0), (0, 0), (0, 0)]),
            vec![0],
            "the same mismatch twice: a leak (B0)"
        );
        assert_eq!(c.observe([(1, 0), (2, 1), (0, 0)]), vec![0]);
        assert_eq!(c.observe([(0, 0), (2, 1), (0, 0)]), vec![1]);
        assert!(c.observe([(0, 0), (0, 0), (0, 0)]).is_empty());
    }

    #[test]
    fn the_wait_histogram_reports_the_p90_bucket_and_resets() {
        let h = WaitHist::new();
        assert_eq!(h.take_p90(), None);
        for _ in 0..9 {
            h.record(Duration::from_micros(100)); // bucket < 128 µs
        }
        h.record(Duration::from_millis(50));
        assert_eq!(h.take_p90(), Some(Duration::from_micros(128)));
        for _ in 0..5 {
            h.record(Duration::from_millis(50));
        }
        h.record(Duration::from_micros(1));
        assert_eq!(h.take_p90(), Some(Duration::from_micros(65_536)));
        assert_eq!(h.take_p90(), None, "taking resets");
    }
}
