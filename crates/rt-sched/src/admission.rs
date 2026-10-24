//! Admission and backpressure (decision D-A3).
//!
//! The platform does not refuse a request because capacity is busy: it **queues** it. Checked in
//! order at submit:
//!
//! | # | Check | Outcome |
//! |---|---|---|
//! | 0 | Call depth > 15 | `508 call_depth` |
//! | 1 | Node mode `drain` | `503 draining` |
//! | 2 | Mode `shed` and the task's artifact is not in T1 | `503 shed` |
//! | 3 | A run slot: the service's adaptive limit, the plan's tenant cap, `sched.max_inflight` less the call reserve | run now, else **wait** in the service's FIFO queue |
//! | 4 | Room to wait: node `sched.max_queued` / `max_queued_mb`; one service and one tenant may each hold half of either (an explicit `scaling.max_queued` only lowers the service's share) | else `503 overloaded` + `Retry-After` |
//!
//! A waiting task that gets no slot within its `queue_timeout` ends `503 queue_timeout` +
//! `Retry-After` (sysmon); a waiting task whose client went away is taken out at once
//! (`TaskCanceller`). 429 is left to the plan quotas checked before submit (ingress).
//!
//! **Slot classes.** External triggers (HTTP, interval, cron, queue) are [`SlotClass::Throttled`]:
//! all three limits apply, and they may use `max_inflight` less 1/8, at least one slot (the call
//! reserve). A
//! service-to-service call ([`SlotClass::Call`]) runs on behalf of a request that already holds
//! slots: it is held by the service ceiling (`max_instances`), by the tenant's call allowance
//! (calls in flight at each call depth ≤ the plan cap) and by all of `max_inflight`; it counts against the tenant cap
//! without being held by it and is served before the service's waiting requests, so nested calls
//! cannot deadlock behind their own parents (per task, `rt-host` allows 16 calls in flight). Builds
//! ([`SlotClass::Build`]) count in `max_inflight` only and never wait: the tasks waiting for the
//! artifact already hold slots. Only requests carry the tenant's plan cap (refreshed on each);
//! a build's default knobs or a call never change it.
//!
//! **Re-entrant calls.** A call or a pub/sub request reply that a task of the *same* service
//! submitted ([`TaskSpec::reentrant`]) is not held by the service ceiling: its parent waits for it
//! while holding one of the service's slots, so at `max_instances: 1` the slot could never free
//! (the self-call would end `queue_timeout`). Such a call may run past `max_instances`; it still
//! takes the tenant's call allowance at its depth, a `max_inflight` slot and its own memory
//! reservation, so the overshoot is bounded by the call depth limit × the plan cap. Calls from
//! other services keep the ceiling as a hard cap; an indirect cycle (A calls B calls A) is not
//! detected and still waits on A's ceiling.
//!
//! **Fairness.** Waiting work is ordered, not just queued:
//!
//! 1. Within a tenant, calls go first (FIFO among themselves); builds never wait.
//! 2. Between tenants, the backlogged tenant with the least *virtual slot time* goes first; ties
//!    go to the one listed first, so equal tenants alternate. A tenant is charged its task's
//!    predicted run time when the task is granted, corrected to the actual time when it finishes.
//!    A tenant that was idle comes back at most [`TENANT_QUANTUM`] behind the others.
//! 3. Within a tenant, across all its services, the earliest *deadline key* goes first:
//!    `arrival + min(aging_factor × predicted, queue_timeout / 2)`. A task predicted short gets an
//!    early key; a task predicted long gets at most half its queue timeout of delay credit, so it
//!    can only be overtaken by work that arrived within that window. `aging_factor = 0` is pure
//!    arrival order.
//! 4. A service at its own limit, or a tenant at its plan cap, is skipped until one of its tasks
//!    finishes (a capped tenant's waiting calls are still served); a global refusal stops the round.
//!
//! The prediction is an EWMA (gain 1/8) of the service version's slot hold time, kept in two
//! lanes: interactive work (HTTP, calls, pub/sub) and background work (queue, stream, schedules),
//! so slow batch consumers do not inflate the estimate of the service's requests. Before any
//! sample it is [`DEFAULT_PRED`]; it is bounded by [`PRED_MIN`] and the task's wall limit.
//!
//! **Concurrency design.** Per-service and per-tenant state lives in sharded maps (64 shards,
//! read-locked on the hot path) of atomics; a task that finds its slots free takes them with three
//! compare-and-swaps and no global lock. The wait queues and the ordered sets sit behind one
//! mutex that is only taken when something waits (slow path, releases while tasks wait). Lost
//! wakeups are excluded Dekker-style: a waiter bumps `queued` and then tries the slots under the
//! lock; a releaser frees its slots and then reads `queued` (all `SeqCst`).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use rt_mem::Pressure;
use rt_types::events::admission_lane;
use rt_types::{
    DegradationMode, Payload, PreparedKey, RejectReason, ScalingKnobs, ServiceId, ServiceKnobs, ServiceRef, TaskKind,
    TaskSpec, TenantId, Trigger, MAX_CALL_DEPTH,
};

use crate::autoscale::{self, Cause, ScaleInput, ScaleState};

const SHARDS: usize = 64;
/// Call depths 0..=MAX_CALL_DEPTH (index of [`TenantSlot::calls`]).
const CALL_DEPTHS: usize = MAX_CALL_DEPTH as usize + 1;
/// Bytes charged for a waiting task on top of its known body: the task record, spec and headers.
pub const QUEUED_TASK_OVERHEAD: u64 = 4 * 1024;
/// A service's admission state (learned limit, latency baseline) is forgotten after it has been
/// idle for its idle TTL, but never sooner than this.
pub const SLOT_MIN_TTL: Duration = Duration::from_secs(60);
/// Predicted slot time of a service lane without samples.
pub const DEFAULT_PRED: Duration = Duration::from_millis(50);
/// Smallest prediction (and sample) used.
pub const PRED_MIN: Duration = Duration::from_millis(1);
/// Largest sample fed to the estimator.
pub const PRED_SAMPLE_MAX: Duration = Duration::from_secs(3600);
/// The most virtual slot time an idle tenant can bank: it is listed again at most this far behind
/// the virtual time of the latest grant.
pub const TENANT_QUANTUM: Duration = Duration::from_millis(20);
/// Default `sched.aging_factor`.
pub const DEFAULT_AGING: u32 = 4;
/// Estimator lanes: 0 interactive, 1 background ([`admission_lane`]).
pub const LANES: usize = 2;

const fn ns(d: Duration) -> u64 {
    d.as_nanos() as u64
}

fn dur_ns(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Next EWMA value (gain 1/8): the first sample (`prev == 0`) is taken as is. Never 0.
pub fn ewma_next(prev_ns: u64, sample_ns: u64) -> u64 {
    if prev_ns == 0 {
        return sample_ns.max(1);
    }
    let next = i128::from(prev_ns) + (i128::from(sample_ns) - i128::from(prev_ns)) / 8;
    (next.max(1)).min(i128::from(u64::MAX)) as u64
}

/// The prediction for one task: the lane's EWMA (or [`DEFAULT_PRED`] without samples), bounded
/// by [`PRED_MIN`] and the task's wall limit.
pub fn predicted_ns(ewma_ns: u64, wall: Duration) -> u64 {
    let v = if ewma_ns == 0 { ns(DEFAULT_PRED) } else { ewma_ns };
    v.clamp(ns(PRED_MIN), dur_ns(wall).max(ns(PRED_MIN)))
}

/// Deadline key of a waiting request: `since + min(aging × pred, queue_timeout / 2)` (saturating).
pub fn wait_key(since_ns: u64, pred_ns: u64, aging: u32, queue_timeout: Duration) -> u64 {
    let credit = pred_ns.saturating_mul(u64::from(aging)).min(dur_ns(queue_timeout) / 2);
    since_ns.saturating_add(credit)
}

/// How a task is held against the limits (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotClass {
    Throttled,
    Call,
    Build,
}

impl SlotClass {
    pub fn of(kind: TaskKind, trigger: &Trigger) -> Self {
        match (kind, trigger) {
            (TaskKind::Build, _) | (_, Trigger::Build { .. }) => SlotClass::Build,
            (_, Trigger::Call { .. }) | (_, Trigger::Pubsub { reply: true, .. }) => SlotClass::Call,
            _ => SlotClass::Throttled,
        }
    }
}

/// Node-wide bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Run slots node-wide (`sched.max_inflight`).
    pub max_inflight: u32,
    /// Waiting tasks node-wide (`sched.max_queued`).
    pub max_queued: u32,
    /// Bytes held by waiting tasks node-wide (`sched.max_queued_mb`).
    pub max_queued_bytes: u64,
}

impl AdmissionLimits {
    /// Resolve the `0 = auto` values: `max_queued = 4 × max_inflight`, `max_queued_bytes =`
    /// 1/16 of the memory budget (at least 16 MiB).
    pub fn resolve(max_inflight: u32, max_queued: u32, max_queued_bytes: u64, budget_bytes: u64) -> Self {
        let max_inflight = max_inflight.max(1);
        Self {
            max_inflight,
            max_queued: if max_queued == 0 {
                max_inflight.saturating_mul(4)
            } else {
                max_queued
            },
            max_queued_bytes: if max_queued_bytes == 0 {
                (budget_bytes / 16).max(16 << 20)
            } else {
                max_queued_bytes
            },
        }
    }
}

/// Inputs to the decision that are not part of the `TaskSpec`.
#[derive(Clone, Copy, Debug)]
pub struct AdmitContext {
    pub mode: DegradationMode,
    /// The task's prepared artifact is already in T1 (always `true` for builds).
    pub t1_ready: bool,
}

/// What admission needs to know about a task.
#[derive(Clone, Copy, Debug)]
pub struct AdmitRequest {
    pub service: ServiceRef,
    pub kind: TaskKind,
    pub class: SlotClass,
    pub call_depth: u8,
    pub knobs: ServiceKnobs,
    /// Prepared artifact of a guest task (warm targets are kept per service artifact).
    pub key: Option<PreparedKey>,
    /// Bytes the task is charged while it waits.
    pub bytes: u64,
    /// Bytes one running instance reserves (warm-target memory estimate).
    pub instance_bytes: u64,
    /// Scheduler clock, nanoseconds since start.
    pub now_ns: u64,
    /// The task's wall limit (bounds its prediction).
    pub wall: Duration,
    /// Estimator lane ([`admission_lane`]).
    pub lane: u8,
    /// A [`SlotClass::Call`] submitted by a task of the same service ([`TaskSpec::reentrant`]):
    /// not held by the service ceiling (see the module doc).
    pub reentrant: bool,
}

impl AdmitRequest {
    /// `key` is the guest task's prepared artifact (`None` for builds).
    pub fn from_spec(spec: &TaskSpec, key: Option<PreparedKey>, instance_bytes: u64, now_ns: u64) -> Self {
        let body = match &spec.payload {
            Payload::Http(r) => r.body.exact_len().unwrap_or(0),
            Payload::Queue(msgs) => msgs.iter().map(|m| m.body.len() as u64).sum(),
            Payload::Pubsub(m) => m.body.len() as u64,
            Payload::Stream(b) => b.records.iter().map(|r| r.body.len() as u64).sum(),
            _ => 0,
        };
        Self {
            service: spec.service,
            kind: spec.kind,
            class: SlotClass::of(spec.kind, &spec.trigger),
            call_depth: spec.trigger.call_depth(),
            knobs: spec.knobs,
            key,
            bytes: QUEUED_TASK_OVERHEAD.saturating_add(body),
            instance_bytes,
            now_ns,
            wall: spec.limits.wall,
            lane: admission_lane(&spec.trigger),
            reentrant: spec.reentrant && SlotClass::of(spec.kind, &spec.trigger) == SlotClass::Call,
        }
    }
}

/// Proof of admission. A queued ticket becomes a running one when its task is granted a slot
/// (the scheduler flips `queued` and stamps `granted_ns`); hand a running ticket back to
/// [`Admission::release`] when the task finishes.
#[derive(Clone, Debug)]
pub struct AdmissionTicket {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub class: SlotClass,
    /// Waiting in the service queue (`true`) or holding a run slot.
    pub queued: bool,
    /// When the run slot was granted (ns since scheduler start).
    pub granted_ns: u64,
    /// Call depth of a [`SlotClass::Call`] (its tenant allowance is per depth); 0 otherwise.
    pub call_depth: u8,
    /// Deadline key and sequence in the wait queue: `Some` once it queued (kept after the grant:
    /// the tenant was charged `pred_ns` then, corrected at release).
    pub wait: Option<(u64, u64)>,
    /// Predicted slot time charged at the grant (0 on the fast path).
    pub pred_ns: u64,
    /// Estimator lane.
    pub lane: u8,
    pub(crate) slot: Arc<SvcSlot>,
}

/// Result of a successful [`Admission::admit`].
#[derive(Debug)]
pub struct Admitted<T> {
    pub ticket: AdmissionTicket,
    /// The new item when it may run now (`None`: it waits in its service's queue).
    pub run: Option<T>,
    /// Waiting items granted a slot during this call (possibly the new one); start them.
    pub granted: Vec<T>,
}

/// A refused [`Admission::admit`]. `granted` holds waiting items that got a slot while the call
/// held the waitlist (a slot it had briefly taken and given back): start them anyway.
#[derive(Debug)]
pub struct Refused<T> {
    pub reason: RejectReason,
    pub granted: Vec<T>,
}

impl<T> Refused<T> {
    fn of(reason: RejectReason) -> Self {
        Self {
            reason,
            granted: Vec::new(),
        }
    }
}

/// One service's autoscaler view, as reported by [`Admission::service_scaling`]: the admission
/// state plus what the autoscaler has learned (`GET /v1/node/scheduler`, the service view).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServiceScaling {
    pub admission: ServiceAdmission,
    /// Kind of the service's latest admitted task.
    pub kind: TaskKind,
    /// `scaling.max_instances` resolved (the hard cap of the adaptive limit).
    pub ceiling: u32,
    /// `scaling.min_instances` resolved: instances kept warm, the floor of the limit (D-B2).
    pub floor: u32,
    /// Bound of the service's wait queue (`scaling.max_queued` resolved).
    pub max_queued: u32,
    /// Decaying peak of running tasks (what the warm targets follow).
    pub demand: f64,
    /// The autoscaler's moving latency baseline (`None` before the first samples).
    pub latency_baseline: Option<Duration>,
    /// Smoothed wait of the requests that had to queue (fades to zero while requests start
    /// without waiting).
    pub queue_wait: Duration,
    /// Queue timeouts (`503 queue_timeout`) since the admission state was created.
    pub queue_timeouts: u64,
    /// Overload refusals (`503 overloaded`) since the admission state was created; not counted
    /// in `rps`.
    pub rejects: u64,
    /// Time since the service last started or finished a task.
    pub idle_for: Duration,
    /// Smoothed arrival rate, requests per second (the autoscaler's fast average).
    pub rps: f64,
    /// The latest move of the limit and why (`None`: it never moved).
    pub last_change: Option<LimitChange>,
    /// Predicted slot time per lane (0 interactive, 1 background); `None` before samples.
    pub pred: [Option<Duration>; LANES],
}

/// One move of a service's limit: from → to, the signal that caused it and how long ago.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitChange {
    pub from: u32,
    pub to: u32,
    pub cause: Cause,
    pub ago: Duration,
}

/// [`LimitChange`] as recorded (a point in scheduler time).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChangeRecord {
    from: u32,
    to: u32,
    cause: Cause,
    at_ns: u64,
}

/// One service's admission state, as reported by [`Admission::service`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceAdmission {
    pub service: ServiceId,
    pub tenant: TenantId,
    /// Current adaptive limit and its bounds.
    pub limit: u32,
    pub min: u32,
    pub max: u32,
    pub running: u32,
    pub queued: u32,
    pub queued_bytes: u64,
}

/// Policy and identity of a service, from its latest admitted guest task.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SvcMeta {
    pub service: ServiceRef,
    pub kind: TaskKind,
    pub key: Option<PreparedKey>,
    pub instance_bytes: u64,
    /// `ServiceKnobs::max_instances`.
    pub ceiling: u32,
    /// `ServiceKnobs::min_instances`: kept warm, floor of the limit.
    pub floor: u32,
    pub max_queued: u32,
    pub scaling: ScalingKnobs,
    pub tenant_cap: u32,
    pub idle_ttl: Duration,
    /// `ServiceKnobs::queue_timeout`: waits of a quarter of it mean the limit is far too low.
    pub queue_timeout: Duration,
}

impl SvcMeta {
    fn of(req: &AdmitRequest) -> Self {
        Self {
            service: req.service,
            kind: req.kind,
            key: req.key,
            instance_bytes: req.instance_bytes,
            ceiling: req.knobs.max_instances,
            floor: req.knobs.min_instances,
            max_queued: req.knobs.max_queued,
            scaling: req.knobs.scaling,
            tenant_cap: req.knobs.tenant_max_concurrency,
            idle_ttl: req.knobs.idle_ttl,
            queue_timeout: req.knobs.queue_timeout,
        }
    }

    /// `(min, max)` of the adaptive limit: the ceiling and the tenant cap bound both; `min` is the
    /// profile floor raised to `min_instances` (and never above `max`).
    pub fn bounds(&self) -> (u32, u32) {
        let max = if self.tenant_cap > 0 {
            self.ceiling.min(self.tenant_cap)
        } else {
            self.ceiling
        };
        (self.scaling.min.max(self.floor).min(max), max)
    }

    /// `min_instances` as far as the limit can honor it (at most `max`).
    pub fn floor_instances(&self) -> u32 {
        self.bounds().0.min(self.floor)
    }
}

/// Counters of the current autoscaler tick.
#[derive(Debug, Default)]
struct Window {
    done: AtomicU32,
    latency_ns: AtomicU64,
    waited: AtomicU32,
    wait_ns: AtomicU64,
    peak_running: AtomicU32,
    peak_queued: AtomicU32,
    timeouts: AtomicU32,
    /// Requests admitted or queued (the arrival rate).
    arrivals: AtomicU32,
    /// Requests refused for lack of room (`503 overloaded`); not part of the arrival rate.
    rejects: AtomicU32,
}

#[derive(Debug, Default)]
struct SlotScale {
    st: ScaleState,
    last_ns: Option<u64>,
    /// Smoothed admission wait (EWMA over ticks, ns): what a request that has to queue waits.
    wait_ewma_ns: f64,
    /// Queue timeouts since the slot was created.
    timeouts: u64,
    /// Overload refusals since the slot was created.
    rejects: u64,
    /// The latest move of the limit.
    last_change: Option<ChangeRecord>,
}

/// Weight of the newest tick in [`ServiceScaling::queue_wait`].
const WAIT_EWMA_ALPHA: f64 = 0.3;

/// Per-tenant counters.
#[derive(Debug)]
pub(crate) struct TenantSlot {
    tenant: TenantId,
    /// Requests and calls holding a run slot (builds are not the tenant's work).
    running: AtomicU32,
    /// Calls holding a run slot, per call depth: each depth has its own allowance (= the plan
    /// cap), so the calls of one level never starve the calls their own callees make (ADM-004).
    calls: [AtomicU32; CALL_DEPTHS],
    queued: AtomicU32,
    queued_bytes: AtomicU64,
    /// Plan cap (0 = unlimited), refreshed by every request; never taken from a call or build.
    cap: AtomicU32,
    /// Virtual slot time (ns of predicted, then actual, slot time granted from the wait queue).
    vtime: AtomicU64,
}

impl TenantSlot {
    fn new(tenant: TenantId) -> Self {
        Self {
            tenant,
            running: AtomicU32::new(0),
            calls: std::array::from_fn(|_| AtomicU32::new(0)),
            queued: AtomicU32::new(0),
            queued_bytes: AtomicU64::new(0),
            cap: AtomicU32::new(0),
            vtime: AtomicU64::new(0),
        }
    }
}

/// Per-service counters, policy and autoscaler state.
#[derive(Debug)]
pub(crate) struct SvcSlot {
    service: ServiceId,
    tenant: Arc<TenantSlot>,
    running: AtomicU32,
    queued: AtomicU32,
    queued_bytes: AtomicU64,
    /// The adaptive limit.
    limit: AtomicU32,
    /// `SvcMeta::ceiling` (the bound of calls).
    ceiling: AtomicU32,
    meta: ArcSwap<SvcMeta>,
    win: Window,
    last_active_ns: AtomicU64,
    scale: Mutex<SlotScale>,
    /// Slot-time EWMA per lane (ns, 0 = no sample), reset on a new version.
    pred_ns: [AtomicU64; LANES],
}

impl SvcSlot {
    fn new(req: &AdmitRequest, tenant: Arc<TenantSlot>) -> Self {
        let mut meta = SvcMeta::of(req);
        if req.class == SlotClass::Throttled {
            tenant.cap.store(meta.tenant_cap, SeqCst);
        } else {
            // A build carries default knobs and a call is the callee's: neither is the plan.
            meta.tenant_cap = tenant.cap.load(SeqCst);
        }
        let (min, max) = meta.bounds();
        Self {
            service: req.service.service,
            tenant,
            running: AtomicU32::new(0),
            queued: AtomicU32::new(0),
            queued_bytes: AtomicU64::new(0),
            limit: AtomicU32::new(meta.scaling.start.clamp(min, max)),
            ceiling: AtomicU32::new(meta.ceiling),
            meta: ArcSwap::from_pointee(meta),
            win: Window::default(),
            last_active_ns: AtomicU64::new(req.now_ns),
            // the first tick measures the arrival rate from the slot's creation
            scale: Mutex::new(SlotScale {
                last_ns: Some(req.now_ns),
                ..SlotScale::default()
            }),
            pred_ns: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    /// Refresh policy and identity from a guest task. Builds change nothing; only requests
    /// (`Throttled`) carry the tenant's plan cap, refreshed on every one (a plan change applies
    /// with the next request), so neither a build's default knobs nor a call can lift it.
    fn observe(&self, req: &AdmitRequest) {
        touch(&self.last_active_ns, req.now_ns);
        if req.class == SlotClass::Build {
            return;
        }
        if req.class == SlotClass::Throttled {
            let cap = req.knobs.tenant_max_concurrency;
            if self.tenant.cap.load(Ordering::Relaxed) != cap {
                self.tenant.cap.store(cap, SeqCst);
            }
        }
        let mut next = SvcMeta::of(req);
        next.tenant_cap = self.tenant.cap.load(Ordering::Relaxed);
        let cur = self.meta.load();
        if **cur == next {
            return;
        }
        if cur.service.version != next.service.version {
            // A new version: what the old one took says nothing about it.
            for p in &self.pred_ns {
                p.store(0, SeqCst);
            }
        }
        drop(cur);
        let (min, max) = next.bounds();
        let before = self
            .limit
            .fetch_update(SeqCst, SeqCst, |l| Some(l.clamp(min, max)))
            .unwrap_or(0);
        self.ceiling.store(next.ceiling, SeqCst);
        self.meta.store(Arc::new(next));
        let after = before.clamp(min, max);
        if after != before {
            // The service was updated (or its plan): the new bounds moved the limit.
            let mut sc = self.scale.lock();
            sc.last_change = Some(ChangeRecord {
                from: before,
                to: after,
                cause: Cause::Bounds,
                at_ns: req.now_ns,
            });
            sc.st.since_change = Some(Duration::ZERO);
        }
    }

    fn scaling(&self, now_ns: u64) -> ServiceScaling {
        let meta = **self.meta.load();
        let sc = self.scale.lock();
        ServiceScaling {
            admission: self.snapshot(),
            kind: meta.kind,
            ceiling: meta.ceiling,
            floor: meta.floor_instances(),
            max_queued: meta.max_queued,
            demand: sc.st.demand,
            latency_baseline: sc.st.baseline,
            queue_wait: Duration::from_nanos(sc.wait_ewma_ns.max(0.0) as u64),
            queue_timeouts: sc.timeouts,
            rejects: sc.rejects,
            idle_for: Duration::from_nanos(now_ns.saturating_sub(self.last_active_ns.load(SeqCst))),
            rps: sc.st.rps_fast,
            last_change: sc.last_change.map(|c| LimitChange {
                from: c.from,
                to: c.to,
                cause: c.cause,
                ago: Duration::from_nanos(now_ns.saturating_sub(c.at_ns)),
            }),
            pred: std::array::from_fn(|i| match self.pred_ns[i].load(SeqCst) {
                0 => None,
                v => Some(Duration::from_nanos(v)),
            }),
        }
    }

    fn snapshot(&self) -> ServiceAdmission {
        let (min, max) = self.meta.load().bounds();
        ServiceAdmission {
            service: self.service,
            tenant: self.tenant.tenant,
            limit: self.limit.load(SeqCst),
            min,
            max,
            running: self.running.load(SeqCst),
            queued: self.queued.load(SeqCst),
            queued_bytes: self.queued_bytes.load(SeqCst),
        }
    }
}

fn inc_below(a: &AtomicU32, max: u32) -> bool {
    let mut cur = a.load(SeqCst);
    loop {
        if cur >= max {
            return false;
        }
        match a.compare_exchange_weak(cur, cur + 1, SeqCst, SeqCst) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
}

fn dec(a: &AtomicU32) {
    // One RMW in the normal case; an underflow (a bookkeeping bug) is undone, not wrapped.
    if a.fetch_sub(1, SeqCst) == 0 {
        a.fetch_add(1, SeqCst);
        debug_assert!(false, "admission counter underflow");
    }
}

/// `fetch_max` without the read-modify-write when `v` is not larger (hot, shared counters).
fn raise(a: &AtomicU32, v: u32) {
    if a.load(Ordering::Relaxed) < v {
        a.fetch_max(v, Ordering::Relaxed);
    }
}

/// Monotonic timestamp, written at most once per millisecond (hot, shared).
fn touch(a: &AtomicU64, now_ns: u64) {
    if now_ns > a.load(Ordering::Relaxed).saturating_add(1_000_000) {
        a.fetch_max(now_ns, Ordering::Relaxed);
    }
}

/// Run slots kept for calls and builds: requests may use `max_inflight` less 1/8 of it, so parents
/// holding every request slot can still run their calls.
fn call_reserve(max_inflight: u32) -> u32 {
    // At least one slot whenever requests can keep one (ADM-003: 1/8 of 2..7 rounds to 0).
    if max_inflight >= 2 {
        (max_inflight / 8).max(1)
    } else {
        0
    }
}

fn sub64(a: &AtomicU64, n: u64) {
    let _ = a.fetch_update(SeqCst, SeqCst, |c| Some(c.saturating_sub(n)));
}

/// Which limit refused a slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Take {
    Ok,
    Service,
    Tenant,
    Global,
}

/// Position of a waiting item in its service queue: `(deadline key, seq)`.
type ItemKey = (u64, u64);
/// A service's head in its tenant's ready set: `(0 = call, 1 = request, key, seq)`.
type HeadKey = (u8, u64, u64);

/// A waiting item.
#[derive(Debug)]
struct Waiting<T> {
    item: T,
    bytes: u64,
    class: SlotClass,
    depth: u8,
    reentrant: bool,
    since_ns: u64,
    key: u64,
    pred_ns: u64,
}

#[derive(Debug)]
struct SvcQ<T> {
    slot: Arc<SvcSlot>,
    /// Waiting calls: served before `items` (a call's parent already holds a slot), FIFO.
    calls: BTreeMap<ItemKey, Waiting<T>>,
    /// Waiting requests by deadline key.
    items: BTreeMap<ItemKey, Waiting<T>>,
    /// Listed in its tenant's `ready` set under this head.
    ready: Option<HeadKey>,
}

impl<T> SvcQ<T> {
    fn is_empty(&self) -> bool {
        self.calls.is_empty() && self.items.is_empty()
    }

    fn head_key(&self) -> Option<HeadKey> {
        if let Some((&(k, s), _)) = self.calls.first_key_value() {
            return Some((0, k, s));
        }
        self.items.first_key_value().map(|(&(k, s), _)| (1, k, s))
    }

    fn head(&self) -> Option<(SlotClass, u8, bool)> {
        self.calls
            .first_key_value()
            .or(self.items.first_key_value())
            .map(|(_, x)| (x.class, x.depth, x.reentrant))
    }

    fn pop_head(&mut self) -> Option<Waiting<T>> {
        self.calls
            .pop_first()
            .or_else(|| self.items.pop_first())
            .map(|(_, x)| x)
    }
}

#[derive(Debug)]
struct TenantQ {
    slot: Arc<TenantSlot>,
    /// Services whose head may be dispatchable, by head key.
    ready: BTreeSet<(HeadKey, ServiceId)>,
    /// Listed in `order` under `(vtime, seq)`.
    listed: Option<(u64, u64)>,
}

/// Everything that waits, behind one mutex (the slow path).
#[derive(Debug)]
struct Waitlist<T> {
    /// Tenants with a dispatchable service, by virtual time (then listing order).
    order: BTreeSet<(u64, u64, TenantId)>,
    tenants: BTreeMap<TenantId, TenantQ>,
    services: BTreeMap<ServiceId, SvcQ<T>>,
    /// Monotonic: item and listing sequence numbers (deterministic ties).
    seq: u64,
    /// The largest virtual time a grant was made at.
    vmin: u64,
}

impl<T> Waitlist<T> {
    fn new() -> Self {
        Self {
            order: BTreeSet::new(),
            tenants: BTreeMap::new(),
            services: BTreeMap::new(),
            seq: 0,
            vmin: 0,
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// List `sid` as dispatchable (its queue is non-empty and a slot of it may be free), or move
    /// it to its current head key when it is listed.
    fn mark_ready(&mut self, sid: ServiceId) {
        let Some(sq) = self.services.get_mut(&sid) else { return };
        let Some(head) = sq.head_key() else { return };
        if sq.ready == Some(head) {
            return;
        }
        let old = sq.ready.replace(head);
        let tslot = sq.slot.tenant.clone();
        let tq = self.tenants.entry(tslot.tenant).or_insert_with(|| TenantQ {
            slot: tslot,
            ready: BTreeSet::new(),
            listed: None,
        });
        if let Some(o) = old {
            tq.ready.remove(&(o, sid));
        }
        tq.ready.insert((head, sid));
    }

    /// After the head of `sid` changed: move its listing (when listed), or drop the service when
    /// its queue is empty.
    fn rehead(&mut self, sid: ServiceId) {
        let Some(sq) = self.services.get_mut(&sid) else { return };
        let (tid, old) = (sq.slot.tenant.tenant, sq.ready);
        if sq.is_empty() {
            self.services.remove(&sid);
            if let (Some(o), Some(tq)) = (old, self.tenants.get_mut(&tid)) {
                tq.ready.remove(&(o, sid));
            }
            return;
        }
        if old.is_some() {
            self.mark_ready(sid);
        }
    }

    /// Put `tid` in `order` (a slot of it freed, or it has new work): at its virtual time, but
    /// at most [`TENANT_QUANTUM`] behind the latest grant.
    fn ring_tenant(&mut self, tid: TenantId) {
        let floor = self.vmin.saturating_sub(ns(TENANT_QUANTUM));
        let seq = self.seq + 1;
        let Some(tq) = self.tenants.get_mut(&tid) else { return };
        if tq.listed.is_some() || tq.ready.is_empty() {
            return;
        }
        let vt = tq.slot.vtime.fetch_max(floor, SeqCst).max(floor);
        tq.listed = Some((vt, seq));
        self.seq = seq;
        self.order.insert((vt, seq, tid));
    }

    fn unlist_tenant(&mut self, tid: TenantId) {
        if let Some((vt, seq)) = self.tenants.get_mut(&tid).and_then(|tq| tq.listed.take()) {
            self.order.remove(&(vt, seq, tid));
        }
    }

    /// Re-key a listed tenant at its current virtual time (after a charge or a correction).
    fn rekey_tenant(&mut self, tid: TenantId) {
        if self.tenants.get(&tid).is_some_and(|tq| tq.listed.is_some()) {
            self.unlist_tenant(tid);
            self.ring_tenant(tid);
        }
    }

    /// Drop a tenant with nothing ready from `order` and the map.
    fn drop_if_idle(&mut self, tid: TenantId) {
        if self.tenants.get(&tid).is_some_and(|tq| tq.ready.is_empty()) {
            self.unlist_tenant(tid);
            self.tenants.remove(&tid);
        }
    }

    /// Re-list every waiting service and tenant (the autoscaler's periodic self-heal).
    fn refresh_all(&mut self) {
        let sids: Vec<ServiceId> = self.services.keys().copied().collect();
        for sid in sids {
            self.mark_ready(sid);
        }
        let tids: Vec<TenantId> = self.tenants.keys().copied().collect();
        for tid in tids {
            self.ring_tenant(tid);
        }
    }
}

/// Inputs of an autoscaler tick that are node-wide.
#[derive(Clone, Copy, Debug)]
pub struct NodeSignals {
    pub now_ns: u64,
    pub pressure: Pressure,
    /// Per `TaskKind::idx()`: the kind's Ps are saturated.
    pub cpu_saturated: [bool; 3],
}

/// What a tick saw and did for one service.
#[derive(Clone, Copy, Debug)]
pub struct ServiceReport {
    pub admission: ServiceAdmission,
    pub meta: SvcMeta,
    /// Decaying peak of running tasks.
    pub demand: f64,
    /// Smoothed arrival rate (requests per second).
    pub rps: f64,
    /// Mean admission wait of the tasks granted during the tick.
    pub mean_wait: Option<Duration>,
    pub idle_for: Duration,
    /// `(old limit, cause)` when the limit moved.
    pub change: Option<(u32, Cause)>,
    /// Queue timeouts during the tick.
    pub timeouts: u32,
}

type ServiceShard = RwLock<BTreeMap<ServiceId, Arc<SvcSlot>>>;
type TenantShard = RwLock<BTreeMap<TenantId, Arc<TenantSlot>>>;

/// Admission controller (see the module doc). `T` is what waits: the scheduler's task.
pub struct Admission<T> {
    max_inflight: AtomicU32,
    max_queued: AtomicU32,
    max_queued_bytes: AtomicU64,
    running: AtomicU32,
    queued: AtomicU32,
    queued_bytes: AtomicU64,
    /// Tenants in the ring (waiting for a global slot): new arrivals queue behind them.
    ringed: AtomicU32,
    services: Box<[ServiceShard]>,
    tenants: Box<[TenantShard]>,
    wait: Mutex<Waitlist<T>>,
    /// `sched.aging_factor`: `k` of the deadline key.
    aging: u32,
    /// Requests granted at or past their deadline key.
    aged_grants: AtomicU64,
}

impl<T> std::fmt::Debug for Admission<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission")
            .field("running", &self.running.load(SeqCst))
            .field("queued", &self.queued.load(SeqCst))
            .finish_non_exhaustive()
    }
}

fn shard_of(id: u128) -> usize {
    // ULIDs end in random bits; deterministic test ids are small numbers.
    ((id ^ (id >> 64)) as usize) % SHARDS
}

impl<T> Admission<T> {
    pub fn new(limits: AdmissionLimits) -> Self {
        Self {
            max_inflight: AtomicU32::new(limits.max_inflight),
            max_queued: AtomicU32::new(limits.max_queued),
            max_queued_bytes: AtomicU64::new(limits.max_queued_bytes),
            running: AtomicU32::new(0),
            queued: AtomicU32::new(0),
            queued_bytes: AtomicU64::new(0),
            ringed: AtomicU32::new(0),
            services: (0..SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect(),
            tenants: (0..SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect(),
            wait: Mutex::new(Waitlist::new()),
            aging: DEFAULT_AGING,
            aged_grants: AtomicU64::new(0),
        }
    }

    /// Set `k` of the deadline key (`sched.aging_factor`; 0 = arrival order within a tenant).
    pub fn with_aging(mut self, k: u32) -> Self {
        self.aging = k;
        self
    }

    /// Requests granted at or past their deadline key (a counter; the metric takes deltas).
    pub fn aged_grants(&self) -> u64 {
        self.aged_grants.load(Ordering::Relaxed)
    }

    /// Tasks of `service` waiting for a run slot (0 without admission state).
    pub fn waiting(&self, service: ServiceId) -> u32 {
        self.services[shard_of(u128::from(service.ulid()))]
            .read()
            .get(&service)
            .map_or(0, |s| s.queued.load(SeqCst))
    }

    // -----------------------------------------------------------------------------------------
    // Observers
    // -----------------------------------------------------------------------------------------

    /// Tasks admitted and not finished (running + waiting).
    pub fn inflight(&self) -> u32 {
        self.running.load(SeqCst).saturating_add(self.queued.load(SeqCst))
    }

    /// Tasks holding a run slot.
    pub fn running(&self) -> u32 {
        self.running.load(SeqCst)
    }

    /// Tasks waiting in service queues, and the bytes they hold.
    pub fn queued(&self) -> (u32, u64) {
        (self.queued.load(SeqCst), self.queued_bytes.load(SeqCst))
    }

    pub fn limits(&self) -> AdmissionLimits {
        AdmissionLimits {
            max_inflight: self.max_inflight.load(SeqCst),
            max_queued: self.max_queued.load(SeqCst),
            max_queued_bytes: self.max_queued_bytes.load(SeqCst),
        }
    }

    /// One service's state (`None` if the service has no admission state).
    pub fn service(&self, id: ServiceId) -> Option<ServiceAdmission> {
        self.services[shard_of(u128::from(id.ulid()))]
            .read()
            .get(&id)
            .map(|s| s.snapshot())
    }

    /// One service's autoscaler view (`None` if the service has no admission state: never run,
    /// or idle long enough to be forgotten). `now_ns` is the scheduler's relative clock.
    pub fn service_scaling(&self, id: ServiceId, now_ns: u64) -> Option<ServiceScaling> {
        let slot = self.services[shard_of(u128::from(id.ulid()))].read().get(&id).cloned();
        slot.map(|s| s.scaling(now_ns))
    }

    /// Create or refresh a service's admission state from `req` without admitting a task, so a
    /// service with a `min_instances` floor has its state, and with it its warm target, before its
    /// first request (node start, version activation, config change).
    pub fn seed(&self, req: &AdmitRequest) {
        let id = req.service.service;
        let shard = &self.services[shard_of(u128::from(id.ulid()))];
        if let Some(slot) = shard.read().get(&id) {
            slot.observe(req);
            return;
        }
        let tenant = self.tenant_slot(req.service.tenant);
        let slot = shard
            .write()
            .entry(id)
            .or_insert_with(|| Arc::new(SvcSlot::new(req, tenant)))
            .clone();
        slot.observe(req);
    }

    /// Release a service's `min_instances` floor (the service was updated or deleted): its warm
    /// target and state follow demand and the idle TTL again. The next request brings the service's
    /// current knobs back, floor included. The limit the floor held up goes back to the service's
    /// `start` (never below what runs now), recorded as a `Bounds` change at `now_ns`. Returns
    /// whether the service had a floor.
    pub fn release_floor(&self, id: ServiceId, now_ns: u64) -> bool {
        let slot = self.services[shard_of(u128::from(id.ulid()))].read().get(&id).cloned();
        let Some(slot) = slot else { return false };
        Self::release_slot_floor(&slot, now_ns)
    }

    fn release_slot_floor(slot: &SvcSlot, now_ns: u64) -> bool {
        // Compare-and-swap: a request may store new knobs meanwhile (`observe`); the release clears
        // the floor of what it read, never writes back metadata that was replaced.
        let mut cur = slot.meta.load_full();
        let base = loop {
            if cur.floor == 0 {
                return false;
            }
            let mut meta = *cur;
            meta.floor = 0;
            let (min, max) = meta.bounds();
            let base = meta.scaling.start.clamp(min, max);
            let prev = slot.meta.compare_and_swap(&cur, Arc::new(meta));
            if Arc::ptr_eq(&prev, &cur) {
                break base;
            }
            cur = arc_swap::Guard::into_inner(prev);
        };
        let running = slot.running.load(SeqCst);
        let to = base.max(running);
        let before = slot.limit.swap(to, SeqCst);
        if before != to {
            let mut sc = slot.scale.lock();
            sc.last_change = Some(ChangeRecord {
                from: before,
                to,
                cause: Cause::Bounds,
                at_ns: now_ns,
            });
            sc.st.since_change = Some(Duration::ZERO);
        }
        true
    }

    /// Release the floor of every service not in `live` (deleted, no longer routable, or its
    /// floor removed without the release reaching this node): a slot with a floor is never
    /// retired, so a missed release would keep its warm instances forever. Call it periodically
    /// with the services that should hold a floor. Returns how many floors were released.
    pub fn release_floors_except(&self, live: &HashSet<ServiceId>, now_ns: u64) -> usize {
        let mut slots: Vec<Arc<SvcSlot>> = Vec::new();
        for shard in self.services.iter() {
            slots.extend(
                shard
                    .read()
                    .values()
                    .filter(|s| s.meta.load().floor > 0 && !live.contains(&s.service))
                    .cloned(),
            );
        }
        slots.iter().filter(|s| Self::release_slot_floor(s, now_ns)).count()
    }

    /// Every service with admission state, ordered by service id (read-only; slot locks are not
    /// held while reading).
    pub fn services_scaling(&self, now_ns: u64) -> Vec<ServiceScaling> {
        let mut slots: Vec<Arc<SvcSlot>> = Vec::new();
        for shard in self.services.iter() {
            slots.extend(shard.read().values().cloned());
        }
        let mut out: Vec<ServiceScaling> = slots.iter().map(|s| s.scaling(now_ns)).collect();
        out.sort_by_key(|s| s.admission.service);
        out
    }

    // -----------------------------------------------------------------------------------------
    // Admit
    // -----------------------------------------------------------------------------------------

    /// Apply checks 0–4. On success `make` is called exactly once with the ticket (before the
    /// item can be seen by any other thread) and builds the item; it runs now
    /// ([`Admitted::run`]) or waits in its service queue.
    pub fn admit(
        &self,
        req: &AdmitRequest,
        cx: AdmitContext,
        make: impl FnOnce(&AdmissionTicket) -> T,
    ) -> Result<Admitted<T>, Refused<T>> {
        if req.call_depth > MAX_CALL_DEPTH {
            return Err(Refused::of(RejectReason::CallDepth));
        }
        if cx.mode == DegradationMode::Drain {
            return Err(Refused::of(RejectReason::Draining));
        }
        if cx.mode == DegradationMode::Shed && !cx.t1_ready {
            return Err(Refused::of(RejectReason::Shed));
        }
        let id = req.service.service;
        let shard = &self.services[shard_of(u128::from(id.ulid()))];
        // The shard lock is held while slots are taken: GC (write lock) cannot retire the slot.
        {
            let g = shard.read();
            if let Some(slot) = g.get(&id) {
                return self.admit_to(slot, req, make);
            }
        }
        let tenant = self.tenant_slot(req.service.tenant);
        let mut g = shard.write();
        let slot = g
            .entry(id)
            .or_insert_with(|| Arc::new(SvcSlot::new(req, tenant)))
            .clone();
        self.admit_to(&slot, req, make)
    }

    fn tenant_slot(&self, id: TenantId) -> Arc<TenantSlot> {
        let shard = &self.tenants[shard_of(u128::from(id.ulid()))];
        if let Some(t) = shard.read().get(&id) {
            return t.clone();
        }
        shard
            .write()
            .entry(id)
            .or_insert_with(|| Arc::new(TenantSlot::new(id)))
            .clone()
    }

    fn ticket(slot: &Arc<SvcSlot>, req: &AdmitRequest, wait: Option<(ItemKey, u64)>) -> AdmissionTicket {
        let queued = wait.is_some();
        AdmissionTicket {
            tenant: slot.tenant.tenant,
            service: slot.service,
            class: req.class,
            queued,
            granted_ns: if queued { 0 } else { req.now_ns },
            call_depth: if req.class == SlotClass::Call {
                req.call_depth
            } else {
                0
            },
            wait: wait.map(|(k, _)| k),
            pred_ns: wait.map_or(0, |(_, p)| p),
            lane: req.lane,
            slot: slot.clone(),
        }
    }

    fn admit_to(
        &self,
        slot: &Arc<SvcSlot>,
        req: &AdmitRequest,
        make: impl FnOnce(&AdmissionTicket) -> T,
    ) -> Result<Admitted<T>, Refused<T>> {
        slot.observe(req);
        // the arrival rate the autoscaler follows (D-B2) counts admitted and queued requests only:
        // refused ones would inflate the rate (and the trend growth) exactly while overloaded
        let counted = req.class != SlotClass::Build;
        // Fast path: nobody of this service or tenant waits, nobody waits for a global slot. Calls
        // and builds do not line up behind waiting requests: their parent request (or the tasks
        // waiting for the artifact) already holds slots.
        let line_up = req.class == SlotClass::Throttled
            && (slot.queued.load(SeqCst) > 0 || slot.tenant.queued.load(SeqCst) > 0 || self.ringed.load(SeqCst) > 0);
        if !line_up && self.try_take(slot, req.class, req.call_depth, req.reentrant) == Take::Ok {
            if counted {
                slot.win.arrivals.fetch_add(1, Ordering::Relaxed);
            }
            let ticket = Self::ticket(slot, req, None);
            let item = make(&ticket);
            return Ok(Admitted {
                ticket,
                run: Some(item),
                granted: Vec::new(),
            });
        }
        // Slow path: wait in line, then let the dispatcher decide (it may grant right away).
        let mut w = self.wait.lock();
        let limits = self.limits();
        let (node_q, node_b) = (self.queued.load(SeqCst), self.queued_bytes.load(SeqCst));
        // One service and one tenant may each hold half of the node queue (count and bytes); an
        // explicit `scaling.max_queued` can only lower the service's share.
        let share_q = (limits.max_queued / 2).max(1);
        let share_b = (limits.max_queued_bytes / 2).max(QUEUED_TASK_OVERHEAD);
        let explicit = slot.meta.load().max_queued;
        let svc_max = if explicit > 0 { explicit.min(share_q) } else { share_q };
        let (svc_q, svc_b) = (slot.queued.load(SeqCst), slot.queued_bytes.load(SeqCst));
        let tenant = &slot.tenant;
        let (ten_q, ten_b) = (tenant.queued.load(SeqCst), tenant.queued_bytes.load(SeqCst));
        if node_q >= limits.max_queued
            || node_b.saturating_add(req.bytes) > limits.max_queued_bytes
            || svc_q >= svc_max
            || svc_b.saturating_add(req.bytes) > share_b
            || ten_q >= share_q
            || ten_b.saturating_add(req.bytes) > share_b
        {
            // The fast path above may have held a slot for a moment and given it back without
            // looking at the waiters: hand it on before refusing.
            if counted {
                slot.win.rejects.fetch_add(1, Ordering::Relaxed);
            }
            let mut refused = Refused::of(RejectReason::Overloaded);
            w.mark_ready(slot.service);
            w.ring_tenant(slot.tenant.tenant);
            self.dispatch(&mut w, req.now_ns, &mut refused.granted);
            return Err(refused);
        }
        if counted {
            slot.win.arrivals.fetch_add(1, Ordering::Relaxed);
        }
        let lane = usize::from(req.lane).min(LANES - 1);
        let pred = predicted_ns(slot.pred_ns[lane].load(SeqCst), req.wall);
        let key = if req.class == SlotClass::Throttled {
            wait_key(req.now_ns, pred, self.aging, req.knobs.queue_timeout)
        } else {
            req.now_ns
        };
        let seq = w.next_seq();
        let ticket = Self::ticket(slot, req, Some(((key, seq), pred)));
        let item = make(&ticket);
        self.queued.fetch_add(1, SeqCst);
        self.queued_bytes.fetch_add(req.bytes, SeqCst);
        slot.tenant.queued.fetch_add(1, SeqCst);
        slot.tenant.queued_bytes.fetch_add(req.bytes, SeqCst);
        slot.queued_bytes.fetch_add(req.bytes, SeqCst);
        let q = slot.queued.fetch_add(1, SeqCst) + 1;
        raise(&slot.win.peak_queued, q);
        let sq = w.services.entry(slot.service).or_insert_with(|| SvcQ {
            slot: slot.clone(),
            calls: BTreeMap::new(),
            items: BTreeMap::new(),
            ready: None,
        });
        let waiting = Waiting {
            item,
            bytes: req.bytes,
            class: req.class,
            depth: req.call_depth,
            reentrant: req.reentrant,
            since_ns: req.now_ns,
            key,
            pred_ns: pred,
        };
        if req.class == SlotClass::Throttled {
            sq.items.insert((key, seq), waiting);
        } else {
            sq.calls.insert((key, seq), waiting);
        }
        w.mark_ready(slot.service);
        // The service may already be listed while its tenant is out of the ring (capped): a call
        // is not held by that cap, so let the dispatcher look at the tenant again.
        w.ring_tenant(slot.tenant.tenant);
        let mut granted = Vec::new();
        self.dispatch(&mut w, req.now_ns, &mut granted);
        Ok(Admitted {
            ticket,
            run: None,
            granted,
        })
    }

    /// Take the slots one task of `class` needs, all or nothing (see the module doc):
    /// - `Throttled`: the adaptive limit, the tenant cap, `max_inflight` less the call reserve;
    /// - `Call`: the service ceiling, the tenant's call allowance (= its cap), all of `max_inflight`;
    ///   counted against the tenant cap without being held by it. A `reentrant` call (submitted by
    ///   a task of the same service) is not held by the ceiling: its parent waits for it holding
    ///   one of the service's slots, so at `max_instances: 1` the ceiling would never free for it
    ///   (a self-call or self-request would wait to `queue_timeout`). It still takes the tenant's
    ///   call allowance at its depth and a `max_inflight` slot, and its memory reservation, so the
    ///   overshoot is bounded by `MAX_CALL_DEPTH` × the plan cap (and `max_inflight`). An
    ///   indirect cycle (A calls B calls A) is not seen here and still waits on A's ceiling;
    /// - `Build`: `max_inflight` counts it, nothing holds it (not the service's, not the tenant's).
    fn try_take(&self, slot: &SvcSlot, class: SlotClass, depth: u8, reentrant: bool) -> Take {
        let calls = &slot.tenant.calls[usize::from(depth).min(CALL_DEPTHS - 1)];
        let max_inflight = self.max_inflight.load(SeqCst);
        match class {
            SlotClass::Build => {
                self.running.fetch_add(1, SeqCst);
                return Take::Ok;
            }
            SlotClass::Throttled => {
                if !inc_below(&slot.running, slot.limit.load(SeqCst)) {
                    return Take::Service;
                }
            }
            SlotClass::Call if reentrant => {
                slot.running.fetch_add(1, SeqCst);
            }
            SlotClass::Call => {
                if !inc_below(&slot.running, slot.ceiling.load(SeqCst)) {
                    return Take::Service;
                }
            }
        }
        let tenant = &slot.tenant;
        let cap = match tenant.cap.load(SeqCst) {
            0 => u32::MAX,
            c => c,
        };
        // Unlimited counters take a plain increment (no compare-and-swap loop).
        let take = |a: &AtomicU32| {
            if cap == u32::MAX {
                a.fetch_add(1, SeqCst);
                true
            } else {
                inc_below(a, cap)
            }
        };
        let tenant_ok = if class == SlotClass::Throttled {
            take(&tenant.running)
        } else if take(calls) {
            tenant.running.fetch_add(1, SeqCst);
            true
        } else {
            false
        };
        if !tenant_ok {
            dec(&slot.running);
            return Take::Tenant;
        }
        let node_cap = match class {
            SlotClass::Throttled => max_inflight - call_reserve(max_inflight),
            _ => max_inflight,
        };
        if !inc_below(&self.running, node_cap) {
            dec(&tenant.running);
            if class == SlotClass::Call {
                dec(calls);
            }
            dec(&slot.running);
            return Take::Global;
        }
        raise(&slot.win.peak_running, slot.running.load(Ordering::Relaxed));
        Take::Ok
    }

    /// Grant slots to waiting items: tenants by virtual time, a tenant's services by head key.
    fn dispatch(&self, w: &mut Waitlist<T>, now_ns: u64, out: &mut Vec<T>) {
        while let Some(&(vt, _, tid)) = w.order.first() {
            let Some(tq) = w.tenants.get(&tid) else {
                w.order.pop_first();
                continue;
            };
            let Some(&(hk, sid)) = tq.ready.first() else {
                w.drop_if_idle(tid);
                continue;
            };
            let Some(sq) = w.services.get_mut(&sid) else {
                if let Some(tq) = w.tenants.get_mut(&tid) {
                    tq.ready.remove(&(hk, sid));
                }
                continue;
            };
            let Some((class, depth, reentrant)) = sq.head() else {
                // LCOV_EXCL_START: an empty queue is removed with its last item
                w.services.remove(&sid);
                if let Some(tq) = w.tenants.get_mut(&tid) {
                    tq.ready.remove(&(hk, sid));
                }
                continue;
                // LCOV_EXCL_STOP
            };
            match self.try_take(&sq.slot, class, depth, reentrant) {
                Take::Ok => {
                    let x = sq.pop_head().expect("the head was just seen");
                    let slot = sq.slot.clone();
                    self.unqueue(&slot, x.bytes);
                    slot.win.waited.fetch_add(1, SeqCst);
                    slot.win.wait_ns.fetch_add(now_ns.saturating_sub(x.since_ns), SeqCst);
                    if x.class == SlotClass::Throttled && now_ns >= x.key {
                        self.aged_grants.fetch_add(1, Ordering::Relaxed);
                    }
                    w.vmin = w.vmin.max(vt);
                    let _ = slot
                        .tenant
                        .vtime
                        .fetch_update(SeqCst, SeqCst, |v| Some(v.saturating_add(x.pred_ns)));
                    w.rehead(sid);
                    w.rekey_tenant(tid);
                    w.drop_if_idle(tid);
                    out.push(x.item);
                }
                Take::Service => {
                    // Waits for one of its own tasks to finish (or the autoscaler to grow it).
                    sq.ready = None;
                    if let Some(tq) = w.tenants.get_mut(&tid) {
                        tq.ready.remove(&(hk, sid));
                    }
                    w.drop_if_idle(tid);
                }
                Take::Tenant => {
                    // Calls rank first in the ready set, so a request held by the tenant cap hides
                    // no call of the tenant: it waits for one of its tasks to finish.
                    w.unlist_tenant(tid);
                }
                Take::Global => break,
            }
        }
        self.ringed.store(w.order.len() as u32, SeqCst);
    }

    fn unqueue(&self, slot: &SvcSlot, bytes: u64) {
        dec(&self.queued);
        sub64(&self.queued_bytes, bytes);
        dec(&slot.queued);
        sub64(&slot.queued_bytes, bytes);
        dec(&slot.tenant.queued);
        sub64(&slot.tenant.queued_bytes, bytes);
    }

    // -----------------------------------------------------------------------------------------
    // Release / cancel
    // -----------------------------------------------------------------------------------------

    /// Give back a running ticket's slots (completion, kill). `latency` (grant → finish, without
    /// artifact waits) feeds the autoscaler and the estimator. Returns waiting items granted the
    /// freed slots.
    pub fn release(&self, ticket: &AdmissionTicket, now_ns: u64, latency: Option<Duration>) -> Vec<T> {
        self.release_with(ticket, now_ns, latency, latency)
    }

    /// [`Admission::release`] with a separate estimator `sample` (the slot hold time; `None`: the
    /// task tells nothing about its run time). A sample moves the lane's EWMA and, for a task
    /// granted from the wait queue, corrects its tenant's charge from the prediction to it.
    pub fn release_with(
        &self,
        ticket: &AdmissionTicket,
        now_ns: u64,
        latency: Option<Duration>,
        sample: Option<Duration>,
    ) -> Vec<T> {
        if ticket.queued {
            debug_assert!(false, "a queued ticket is cancelled, not released");
            return Vec::new();
        }
        let slot = &ticket.slot;
        if ticket.class != SlotClass::Build {
            dec(&slot.running);
            dec(&slot.tenant.running);
            if ticket.class == SlotClass::Call {
                dec(&slot.tenant.calls[usize::from(ticket.call_depth).min(CALL_DEPTHS - 1)]);
            }
        }
        dec(&self.running);
        touch(&slot.last_active_ns, now_ns);
        if let Some(l) = latency {
            slot.win.done.fetch_add(1, SeqCst);
            slot.win
                .latency_ns
                .fetch_add(l.as_nanos().min(u128::from(u64::MAX / 4)) as u64, SeqCst);
        }
        let mut corrected = false;
        if let Some(sample) = sample.filter(|_| ticket.class != SlotClass::Build) {
            let s = dur_ns(sample.clamp(PRED_MIN, PRED_SAMPLE_MAX));
            let lane = usize::from(ticket.lane).min(LANES - 1);
            let _ = slot.pred_ns[lane].fetch_update(SeqCst, SeqCst, |p| Some(ewma_next(p, s)));
            if ticket.wait.is_some() && s != ticket.pred_ns {
                let p = ticket.pred_ns;
                let _ = slot.tenant.vtime.fetch_update(SeqCst, SeqCst, |v| {
                    Some(if s > p {
                        v.saturating_add(s - p)
                    } else {
                        v.saturating_sub(p - s)
                    })
                });
                corrected = true;
            }
        }
        let mut out = Vec::new();
        if self.queued.load(SeqCst) == 0 {
            return out;
        }
        let mut w = self.wait.lock();
        if corrected {
            w.rekey_tenant(slot.tenant.tenant);
        }
        w.mark_ready(slot.service);
        w.ring_tenant(slot.tenant.tenant);
        self.dispatch(&mut w, now_ns, &mut out);
        out
    }

    /// Take a waiting item out of its queue (queue timeout, shutdown); `is` recognizes it.
    /// Returns whether it was still waiting.
    pub fn cancel(&self, ticket: &AdmissionTicket, timed_out: bool, is: impl Fn(&T) -> bool) -> bool {
        let mut w = self.wait.lock();
        let Some(sq) = w.services.get_mut(&ticket.service) else {
            return false;
        };
        let by_key = ticket.wait.filter(|k| {
            let q = if ticket.class == SlotClass::Throttled {
                &sq.items
            } else {
                &sq.calls
            };
            q.get(k).is_some_and(|x| is(&x.item))
        });
        let x = if let Some(k) = by_key {
            let q = if ticket.class == SlotClass::Throttled {
                &mut sq.items
            } else {
                &mut sq.calls
            };
            q.remove(&k)
        } else {
            // A ticket without its key (not taken from this queue): look for the item.
            let found = |q: &BTreeMap<ItemKey, Waiting<T>>| q.iter().find(|(_, x)| is(&x.item)).map(|(k, _)| *k);
            if let Some(k) = found(&sq.items) {
                sq.items.remove(&k)
            } else if let Some(k) = found(&sq.calls) {
                sq.calls.remove(&k)
            } else {
                None
            }
        };
        let Some(x) = x else { return false };
        let slot = sq.slot.clone();
        self.unqueue(&slot, x.bytes);
        if timed_out {
            slot.win.timeouts.fetch_add(1, SeqCst);
        }
        let tid = slot.tenant.tenant;
        w.rehead(ticket.service);
        w.drop_if_idle(tid);
        self.ringed.store(w.order.len() as u32, SeqCst);
        true
    }

    /// Update the node-wide bounds live; returns waiting items granted by a larger
    /// `max_inflight`.
    pub fn set_limits(&self, limits: AdmissionLimits, now_ns: u64) -> Vec<T> {
        self.max_inflight.store(limits.max_inflight.max(1), SeqCst);
        self.max_queued.store(limits.max_queued, SeqCst);
        self.max_queued_bytes.store(limits.max_queued_bytes, SeqCst);
        let mut out = Vec::new();
        if self.queued.load(SeqCst) > 0 {
            let mut w = self.wait.lock();
            w.refresh_all();
            self.dispatch(&mut w, now_ns, &mut out);
        }
        out
    }

    // -----------------------------------------------------------------------------------------
    // Autoscaler tick
    // -----------------------------------------------------------------------------------------

    /// One autoscaler step over every service (deterministic order): adapt each limit
    /// ([`autoscale::decide`]), refresh demand, report, forget services idle past their TTL, then
    /// grant waiting items the new room. Returns the granted items.
    pub fn tick(&self, sig: &NodeSignals, reports: &mut Vec<ServiceReport>) -> Vec<T> {
        // Snapshot the slots, then work without the shard locks (admits of new services take them
        // for writing).
        let mut slots: Vec<(usize, Arc<SvcSlot>)> = Vec::new();
        for (si, shard) in self.services.iter().enumerate() {
            slots.extend(shard.read().values().map(|s| (si, s.clone())));
        }
        let mut idle: Vec<(usize, ServiceId)> = Vec::new();
        reports.reserve(slots.len());
        for (si, slot) in &slots {
            let report = self.tick_service(slot, sig);
            let ttl = report.meta.idle_ttl.max(SLOT_MIN_TTL);
            // a service with `min_instances` keeps its state (and so its warm target) while idle
            if report.admission.running == 0
                && report.admission.queued == 0
                && report.idle_for > ttl
                && report.meta.floor == 0
            {
                idle.push((*si, slot.service));
            }
            reports.push(report);
        }
        drop(slots);
        for (si, sid) in idle {
            let mut g = self.services[si].write();
            let retire = g.get(&sid).is_some_and(|s| {
                s.running.load(SeqCst) == 0
                    && s.queued.load(SeqCst) == 0
                    && s.meta.load().floor == 0
                    && Duration::from_nanos(sig.now_ns.saturating_sub(s.last_active_ns.load(SeqCst)))
                        > s.meta.load().idle_ttl.max(SLOT_MIN_TTL)
            });
            if retire {
                g.remove(&sid);
            }
        }
        for shard in self.tenants.iter() {
            // Only the map holds it: no service slot (and so no ticket) refers to the tenant.
            shard
                .write()
                .retain(|_, t| Arc::strong_count(t) > 1 || t.running.load(SeqCst) > 0 || t.queued.load(SeqCst) > 0);
        }
        let mut out = Vec::new();
        if self.queued.load(SeqCst) > 0 {
            let mut w = self.wait.lock();
            w.refresh_all();
            self.dispatch(&mut w, sig.now_ns, &mut out);
        }
        out
    }

    fn tick_service(&self, slot: &SvcSlot, sig: &NodeSignals) -> ServiceReport {
        let meta = **slot.meta.load();
        let (min, max) = meta.bounds();
        let running = slot.running.load(SeqCst);
        let queued = slot.queued.load(SeqCst);
        let w = &slot.win;
        let done = w.done.swap(0, SeqCst);
        let latency_ns = w.latency_ns.swap(0, SeqCst);
        let waited = w.waited.swap(0, SeqCst);
        let wait_ns = w.wait_ns.swap(0, SeqCst);
        let peak_running = w.peak_running.swap(running, SeqCst).max(running);
        let peak_queued = w.peak_queued.swap(queued, SeqCst).max(queued);
        let timeouts = w.timeouts.swap(0, SeqCst);
        let arrivals = w.arrivals.swap(0, Ordering::Relaxed);
        let rejects = w.rejects.swap(0, Ordering::Relaxed);
        let limit = slot.limit.load(SeqCst);

        let mut sc = slot.scale.lock();
        let elapsed = sc
            .last_ns
            .map_or(Duration::ZERO, |t| Duration::from_nanos(sig.now_ns.saturating_sub(t)));
        sc.last_ns = Some(sig.now_ns);
        let input = ScaleInput {
            limit,
            min,
            max,
            start: meta.scaling.start.clamp(min, max),
            peak_running,
            peak_queued,
            latency: (done > 0).then(|| Duration::from_nanos(latency_ns / u64::from(done))),
            samples: done,
            tolerance_pct: meta.scaling.latency_tolerance_pct,
            pressure: sig.pressure,
            cpu_saturated: sig.cpu_saturated[meta.kind.idx()],
            elapsed,
            arrivals,
            mean_wait: (waited > 0).then(|| Duration::from_nanos(wait_ns / u64::from(waited))),
            wait_urgent: (meta.queue_timeout / 4).max(Duration::from_millis(10)),
        };
        let d = autoscale::decide(&input, &mut sc.st);
        // Only the autoscaler stores a grown limit; `observe` may have clamped it meanwhile.
        let change = match d.cause {
            Some(cause) if slot.limit.compare_exchange(limit, d.limit, SeqCst, SeqCst).is_ok() => Some((limit, cause)),
            _ => None,
        };
        if let Some((from, cause)) = change {
            sc.last_change = Some(ChangeRecord {
                from,
                to: d.limit,
                cause,
                at_ns: sig.now_ns,
            });
        }
        sc.st.demand = autoscale::update_demand(sc.st.demand, peak_running, elapsed);
        let demand = sc.st.demand;
        let rps_fast = sc.st.rps_fast;
        if waited > 0 {
            let mean = (wait_ns / u64::from(waited)) as f64;
            sc.wait_ewma_ns = WAIT_EWMA_ALPHA * mean + (1.0 - WAIT_EWMA_ALPHA) * sc.wait_ewma_ns;
        } else if done > 0 {
            // requests ran without waiting: the queue wait fades out
            sc.wait_ewma_ns *= 1.0 - WAIT_EWMA_ALPHA;
        }
        sc.timeouts += u64::from(timeouts);
        sc.rejects += u64::from(rejects);
        drop(sc);

        ServiceReport {
            admission: slot.snapshot(),
            meta,
            demand,
            rps: rps_fast,
            mean_wait: (waited > 0).then(|| Duration::from_nanos(wait_ns / u64::from(waited))),
            idle_for: Duration::from_nanos(sig.now_ns.saturating_sub(slot.last_active_ns.load(SeqCst))),
            change,
            timeouts,
        }
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    /// ADM-011: releasing below zero (a bookkeeping bug) is undone, never wrapped to u32::MAX, and
    /// asserts in debug builds.
    #[test]
    fn adm_011_a_counter_underflow_is_undone() {
        let a = AtomicU32::new(0);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| dec(&a)));
        assert_eq!(r.is_err(), cfg!(debug_assertions), "asserts in debug only");
        assert_eq!(a.load(SeqCst), 0, "not wrapped");
        let b = AtomicU32::new(2);
        dec(&b);
        assert_eq!(b.load(SeqCst), 1);
    }

    #[test]
    fn ewma_next_starts_at_the_first_sample_and_moves_an_eighth() {
        assert_eq!(ewma_next(0, 800), 800);
        assert_eq!(ewma_next(0, 0), 1, "never 0 (0 means no sample)");
        assert_eq!(ewma_next(800, 1600), 900);
        assert_eq!(ewma_next(800, 0), 700);
        assert_eq!(ewma_next(1, 0), 1);
        assert_eq!(ewma_next(u64::MAX, u64::MAX), u64::MAX);
    }

    #[test]
    fn predicted_is_the_prior_without_samples_and_bounded_by_one_ms_and_the_wall() {
        let wall = Duration::from_secs(30);
        assert_eq!(predicted_ns(0, wall), ns(DEFAULT_PRED));
        assert_eq!(predicted_ns(10, wall), ns(PRED_MIN));
        assert_eq!(predicted_ns(ns(Duration::from_secs(60)), wall), ns(wall));
        assert_eq!(predicted_ns(0, Duration::from_millis(10)), 10_000_000);
        assert_eq!(
            predicted_ns(0, Duration::ZERO),
            ns(PRED_MIN),
            "a zero wall still allows PRED_MIN"
        );
        assert_eq!(predicted_ns(7_000_000, wall), 7_000_000);
    }

    #[test]
    fn wait_key_adds_k_times_pred_capped_at_half_the_queue_timeout() {
        let q = Duration::from_secs(5);
        assert_eq!(wait_key(100, 10, 4, q), 140);
        assert_eq!(wait_key(100, ns(Duration::from_secs(20)), 4, q), 100 + ns(q) / 2);
    }

    #[test]
    fn wait_key_with_aging_zero_is_arrival() {
        assert_eq!(wait_key(123, 1_000_000, 0, Duration::from_secs(5)), 123);
    }

    #[test]
    fn wait_key_saturates() {
        assert_eq!(wait_key(u64::MAX - 1, u64::MAX, 64, Duration::MAX), u64::MAX);
        assert_eq!(dur_ns(Duration::MAX), u64::MAX);
    }
}
