//! Warm-capacity targets: how many ready instances each service keeps per processor.
//!
//! The scheduler's autoscaler publishes, per service artifact ([`WarmKey`]: the service plus its
//! [`PreparedKey`]), how many ready instances each Processor of the artifact's kind should keep,
//! derived from recent demand. Targets are per service, never per artifact alone: identical code
//! of two services (or tenants) shares a `PreparedKey`, but warm instances are never shared.
//! Executors read the targets in [`crate::Executor::between_tasks`] / [`crate::Executor::on_idle`]
//! and converge on them: WASM pre-instantiates fresh instances (or keeps reusable idle ones),
//! Deno keeps reusable isolates when reuse is on. A key missing from the snapshot has target 0
//! (scale to zero), so executors drop what they keep for it once its idle TTL passes.
//!
//! Writers replace the whole snapshot (one `ArcSwap` store); readers never block.
//!
//! A `min_instances` floor below one per P is spread: every P keeps `base`, and `extra` of them
//! one more, so exactly `min_instances` are kept node-wide. [`WarmTarget::per_proc`] in the
//! snapshot is the rounded-up value; [`WarmTargets::per_proc`] called on a Machine thread returns
//! that Machine's share (which Machines keep the extra one rotates with the service).

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rt_types::{PreparedKey, ServiceId, ServiceRef};

/// How many tasks of a service wait in admission (implemented by the scheduler, read by
/// executors when a task's answer is done, see [`after_answer`]).
pub trait Backlog: Send + Sync {
    /// Tasks of `service` queued in admission right now (all versions).
    fn waiting(&self, service: ServiceId) -> u32;
}

/// What an executor does with a reusable instance once its task's answer is done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterAnswer {
    /// No `min_run_time`: the executor's own keep rule, unchanged.
    Inherit,
    /// Park the instance; it expires `Duration` after the answer.
    For(Duration),
    /// `min_run_time = 0` with work of the service waiting: park for at most `cap`, and drop it
    /// at the first housekeeping that sees nothing waiting.
    Handoff { cap: Duration },
    /// `min_run_time = 0` and nothing waits: dispose now (unless the warm floor needs it).
    Dispose,
}

/// The keep decision at the answer, shared by both executors. The executors' own guards (kills,
/// guest errors, unclean finish, reuse caps, pressure, memory growth, stale version) run first
/// and win: this is only consulted for an instance that may be kept at all.
pub fn after_answer(min_run_time: Option<Duration>, waiting: u32, queue_timeout: Duration) -> AfterAnswer {
    match min_run_time {
        None => AfterAnswer::Inherit,
        Some(t) if !t.is_zero() => AfterAnswer::For(t),
        Some(_) if waiting > 0 => AfterAnswer::Handoff { cap: queue_timeout },
        Some(_) => AfterAnswer::Dispose,
    }
}

/// The registered backlog probe (a `Weak`, so executors never keep the scheduler alive).
#[derive(Default)]
struct BacklogSlot(RwLock<Option<Weak<dyn Backlog>>>);

impl std::fmt::Debug for BacklogSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BacklogSlot")
            .field("set", &self.0.read().is_some())
            .finish()
    }
}

/// One service's artifact: the unit warm capacity is kept for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WarmKey {
    pub service: ServiceId,
    pub key: PreparedKey,
}

/// What the autoscaler wants kept ready for one service artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WarmTarget {
    /// The service and version the artifact belongs to (executors use it to build instances
    /// off the request path).
    pub service: ServiceRef,
    /// Ready instances per Processor of the artifact's kind.
    pub per_proc: u32,
}

/// How a target below one per P is spread: every P keeps `base`, `extra` of the `procs` Ps one
/// more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WarmSpread {
    pub base: u32,
    pub extra: u32,
    pub procs: u32,
}

impl WarmSpread {
    /// The share of the Machine with index `rank` (Machines of a kind are numbered contiguously,
    /// so `procs` consecutive ranks keep exactly `base * procs + extra`).
    pub fn share(&self, service: ServiceId, rank: u32) -> u32 {
        if self.procs == 0 {
            return self.base;
        }
        let salt = (u128::from(service.ulid()) % u128::from(self.procs)) as u64;
        let pos = (u64::from(rank) + salt) % u64::from(self.procs);
        self.base + u32::from(pos < u64::from(self.extra))
    }
}

thread_local! {
    static MACHINE_RANK: Cell<Option<u32>> = const { Cell::new(None) };
}

/// Mark the calling thread as the Machine with index `rank` (see [`WarmTargets::per_proc`]).
pub fn set_machine_rank(rank: Option<u32>) {
    MACHINE_RANK.with(|r| r.set(rank));
}

/// The published targets. Shared by the scheduler (writer) and every executor (readers).
#[derive(Debug, Default)]
pub struct WarmTargets {
    snapshot: ArcSwap<HashMap<WarmKey, WarmTarget>>,
    spread: ArcSwap<HashMap<WarmKey, WarmSpread>>,
    backlog: BacklogSlot,
}

impl WarmTargets {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Ready instances per Processor wanted for `key` (0 when it has no target). On a Machine
    /// thread a spread target ([`WarmSpread`]) gives this Machine's share; elsewhere the
    /// rounded-up per-P value.
    pub fn per_proc(&self, key: &WarmKey) -> u32 {
        let Some(per_proc) = self.snapshot.load().get(key).map(|t| t.per_proc) else {
            return 0;
        };
        match (self.spread.load().get(key), MACHINE_RANK.with(Cell::get)) {
            (Some(sp), Some(rank)) => sp.share(key.service, rank),
            _ => per_proc,
        }
    }

    /// Instances node-wide the targets keep for `key` with `procs` Ps of its kind.
    pub fn instances(&self, key: &WarmKey, procs: u32) -> u32 {
        if let Some(sp) = self.spread.load().get(key) {
            return sp.base.saturating_mul(sp.procs).saturating_add(sp.extra);
        }
        self.snapshot
            .load()
            .get(key)
            .map_or(0, |t| t.per_proc.saturating_mul(procs))
    }

    /// Every current target (cheap: one `Arc` clone).
    pub fn snapshot(&self) -> Arc<HashMap<WarmKey, WarmTarget>> {
        self.snapshot.load_full()
    }

    /// Replace all targets at once (the autoscaler's tick).
    pub fn publish(&self, targets: HashMap<WarmKey, WarmTarget>) {
        self.publish_spread(targets, HashMap::new());
    }

    /// Replace all targets and their spreads at once (a key without a spread is uniform).
    pub fn publish_spread(&self, targets: HashMap<WarmKey, WarmTarget>, spread: HashMap<WarmKey, WarmSpread>) {
        self.spread.store(Arc::new(spread));
        self.snapshot.store(Arc::new(targets));
    }

    /// Register the scheduler's backlog probe (replaces any earlier one).
    pub fn set_backlog(&self, backlog: Weak<dyn Backlog>) {
        *self.backlog.0.write() = Some(backlog);
    }

    /// Tasks of `service` waiting in admission; 0 without a registered (live) probe.
    pub fn waiting(&self, service: ServiceId) -> u32 {
        let probe = self.backlog.0.read().as_ref().and_then(Weak::upgrade);
        probe.map_or(0, |b| b.waiting(service))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::{Digest, RuntimeKind, TenantId, VersionNo};
    use std::sync::atomic::{AtomicU32, Ordering};

    const Q: Duration = Duration::from_secs(5);

    /// No `min_run_time`: the executor's own rule, whatever waits.
    #[test]
    fn after_answer_unset_inherits() {
        assert_eq!(after_answer(None, 0, Q), AfterAnswer::Inherit);
        assert_eq!(after_answer(None, 9, Q), AfterAnswer::Inherit);
    }

    /// A positive `min_run_time` keeps the instance exactly that long, whatever waits.
    #[test]
    fn after_answer_positive_keeps_for_that_long() {
        let t = Duration::from_millis(250);
        assert_eq!(after_answer(Some(t), 0, Q), AfterAnswer::For(t));
        assert_eq!(after_answer(Some(t), 3, Q), AfterAnswer::For(t));
    }

    /// `min_run_time = 0` hands the instance to waiting work (capped at the queue timeout) and
    /// disposes it when nothing waits.
    #[test]
    fn after_answer_zero_hands_off_only_while_work_waits() {
        assert_eq!(
            after_answer(Some(Duration::ZERO), 1, Q),
            AfterAnswer::Handoff { cap: Q }
        );
        assert_eq!(after_answer(Some(Duration::ZERO), 0, Q), AfterAnswer::Dispose);
    }

    struct Fixed(AtomicU32, ServiceId);

    impl Backlog for Fixed {
        fn waiting(&self, s: ServiceId) -> u32 {
            if s == self.1 {
                self.0.load(Ordering::Relaxed)
            } else {
                0
            }
        }
    }

    /// Without a registered probe, or once the scheduler behind it is gone, nothing waits.
    #[test]
    fn waiting_is_zero_without_a_backlog_or_after_it_is_dropped() {
        let svc = ServiceId::from_u128(5);
        let warm = WarmTargets::new();
        assert_eq!(warm.waiting(svc), 0);
        let probe = Arc::new(Fixed(AtomicU32::new(4), svc));
        let as_dyn: Arc<dyn Backlog> = probe.clone();
        warm.set_backlog(Arc::downgrade(&as_dyn));
        assert_eq!(warm.waiting(svc), 4);
        assert_eq!(warm.waiting(ServiceId::from_u128(6)), 0);
        probe.0.store(1, Ordering::Relaxed);
        assert_eq!(warm.waiting(svc), 1);
        assert!(format!("{warm:?}").contains("set: true"));
        drop(as_dyn);
        drop(probe);
        assert_eq!(warm.waiting(svc), 0, "a dropped scheduler leaves no backlog");
    }

    #[test]
    fn a_published_snapshot_replaces_the_previous_one() {
        let prepared = PreparedKey {
            kind: RuntimeKind::Wasm,
            digest: Digest::of(b"a"),
            engine_fp: 1,
        };
        let service = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            version: VersionNo(1),
            digest: prepared.digest,
        };
        let key = WarmKey {
            service: service.service,
            key: prepared,
        };
        let warm = WarmTargets::new();
        assert_eq!(warm.per_proc(&key), 0, "no target = scale to zero");
        warm.publish(HashMap::from([(key, WarmTarget { service, per_proc: 3 })]));
        assert_eq!(warm.per_proc(&key), 3);
        warm.publish(HashMap::new());
        assert_eq!(warm.per_proc(&key), 0);
    }

    #[test]
    fn a_spread_target_keeps_exactly_its_instances_over_consecutive_machines() {
        let prepared = PreparedKey {
            kind: RuntimeKind::Wasm,
            digest: Digest::of(b"a"),
            engine_fp: 1,
        };
        let service = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(7),
            version: VersionNo(1),
            digest: prepared.digest,
        };
        let key = WarmKey {
            service: service.service,
            key: prepared,
        };
        let warm = WarmTargets::new();
        let sp = WarmSpread {
            base: 0,
            extra: 1,
            procs: 32,
        };
        warm.publish_spread(
            HashMap::from([(key, WarmTarget { service, per_proc: 1 })]),
            HashMap::from([(key, sp)]),
        );
        assert_eq!(warm.per_proc(&key), 1, "off a Machine thread: rounded up");
        assert_eq!(warm.instances(&key, 32), 1);
        let kept: u32 = (4..36)
            .map(|rank| {
                set_machine_rank(Some(rank));
                warm.per_proc(&key)
            })
            .sum();
        set_machine_rank(None);
        assert_eq!(kept, 1, "min 1 on 32 Ps: one Machine keeps it");
        let sp = WarmSpread {
            base: 1,
            extra: 3,
            procs: 4,
        };
        assert_eq!((0..4).map(|r| sp.share(service.service, r)).sum::<u32>(), 7);
    }
}
