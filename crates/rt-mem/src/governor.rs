//! The memory governor trait and its default implementation.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::reservation::{Reservation, ReservationBackend};
use crate::types::{MemCategory, MemError, MemStats, Owner, Pressure, PurgeLevel, PurgeRequest};

/// Subscription to pressure level changes.
pub struct PressureWatch {
    rx: watch::Receiver<Pressure>,
}

impl PressureWatch {
    pub fn new(rx: watch::Receiver<Pressure>) -> Self {
        Self { rx }
    }

    /// Current level.
    pub fn current(&self) -> Pressure {
        *self.rx.borrow()
    }

    /// Wait until the level changes; returns the new level, or `None` if the governor is gone.
    pub async fn changed(&mut self) -> Option<Pressure> {
        self.rx.changed().await.ok()?;
        Some(*self.rx.borrow_and_update())
    }
}

/// One governed pool for every runtime.
pub trait MemoryGovernor: Send + Sync + 'static {
    /// Charge `bytes` to `cat`. Fails when the budget (or the pressure ladder) refuses.
    fn reserve(&self, cat: MemCategory, bytes: u64, owner: Owner) -> Result<Reservation, MemError>;

    /// Current pressure level.
    fn pressure(&self) -> Pressure;

    /// Watch pressure changes (sysmon, cache, scheduler admission).
    fn subscribe(&self) -> PressureWatch;

    /// Accounting snapshot (per category, RSS, fragmentation ratio).
    fn stats(&self) -> MemStats;

    /// Ask every M to purge its thread-local allocator heap when it next parks.
    ///
    /// The scheduler's sysmon calls this under pressure (`Light` at `High`, `Full` at `Critical`,
    /// at most once per `memory.purge_min_interval`). Serving it is up to the owners of
    /// thread-local heaps and pools via [`MemoryGovernor::purge_request`]; no executor serves it
    /// yet.
    fn request_purge(&self, level: PurgeLevel);

    /// Latest purge request; an M compares `epoch` with the last one it served.
    fn purge_request(&self) -> PurgeRequest;

    /// Total budget in bytes.
    fn budget_bytes(&self) -> u64;
}

/// Watermarks and cadence for [`Governor`].
#[derive(Clone, Debug)]
pub struct GovernorConfig {
    pub budget_bytes: u64,
    /// Fraction of the budget (e.g. 0.80) at which `Pressure::High` starts.
    pub high: f64,
    /// Fraction at which `Pressure::Critical` starts.
    pub critical: f64,
    /// How long usage must stay below `high` before leaving Critical (5 s).
    pub critical_exit_hold: Duration,
}

impl GovernorConfig {
    /// `balanced` watermarks with the given budget.
    pub fn balanced(budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            high: 0.80,
            critical: 0.92,
            critical_exit_hold: Duration::from_secs(5),
        }
    }
}

/// Shared accounting state. Reservations hold an `Arc` to this (not to the [`Governor`]), so a
/// reservation outliving its governor still releases correctly.
#[derive(Debug)]
struct Inner {
    budget: u64,
    committed: AtomicU64,
    by_cat: [AtomicU64; MemCategory::ALL.len()],
    live: AtomicU64,
    /// Last published level (`Pressure as u8`).
    level: AtomicU8,
    /// Watermarks as `f64` bits.
    high: AtomicU64,
    critical: AtomicU64,
    rss: AtomicU64,
    /// Smallest RSS not explained by committed bytes seen so far (`u64::MAX` = unknown).
    baseline_rss: AtomicU64,
}

impl Inner {
    fn high(&self) -> f64 {
        f64::from_bits(self.high.load(Ordering::Relaxed))
    }

    fn critical(&self) -> f64 {
        f64::from_bits(self.critical.load(Ordering::Relaxed))
    }

    fn level(&self) -> Pressure {
        match self.level.load(Ordering::Acquire) {
            0 => Pressure::Normal,
            1 => Pressure::High,
            _ => Pressure::Critical,
        }
    }

    /// `committed >= fraction * budget`.
    fn at_or_above(&self, committed: u64, fraction: f64) -> bool {
        self.budget > 0 && committed as f64 >= fraction * self.budget as f64
    }

    /// CAS-charge `bytes` against the budget. `Err(available)` when it does not fit.
    fn charge(&self, bytes: u64) -> Result<u64, u64> {
        let mut cur = self.committed.load(Ordering::Acquire);
        loop {
            match cur.checked_add(bytes) {
                Some(next) if next <= self.budget => {
                    match self
                        .committed
                        .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => return Ok(next),
                        Err(actual) => cur = actual,
                    }
                }
                _ => return Err(self.budget.saturating_sub(cur)),
            }
        }
    }

    fn uncharge(&self, bytes: u64) {
        let _ = self
            .committed
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| Some(c.saturating_sub(bytes)));
    }
}

impl ReservationBackend for Inner {
    fn try_grow(&self, cat: MemCategory, by: u64) -> bool {
        if by == 0 {
            return true;
        }
        if self.charge(by).is_err() {
            return false;
        }
        self.by_cat[cat.idx()].fetch_add(by, Ordering::AcqRel);
        true
    }

    fn release(&self, cat: MemCategory, bytes: u64) {
        let _ =
            self.by_cat[cat.idx()].fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| Some(c.saturating_sub(bytes)));
        self.uncharge(bytes);
    }

    fn reservation_dropped(&self) {
        let _ = self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| Some(c.saturating_sub(1)));
    }
}

#[derive(Debug)]
struct TickState {
    /// Since when usage has been continuously below `high` while `Critical` (hold clock).
    below_high_since: Option<Instant>,
}

/// Default [`MemoryGovernor`]: atomic per-category counters against one budget.
///
/// Admission is a single compare-and-swap on the total, so concurrent reservations can never
/// jointly exceed the budget (MEM-02). The pressure level is recomputed by [`Governor::tick`].
#[derive(Debug)]
pub struct Governor {
    inner: Arc<Inner>,
    hold: Duration,
    /// Reservations at least this large trigger a tick.
    auto_tick_bytes: u64,
    tx: watch::Sender<Pressure>,
    tick_state: Mutex<TickState>,
    purge: Mutex<PurgeRequest>,
}

impl Governor {
    pub fn new(cfg: GovernorConfig) -> Arc<Self> {
        let inner = Arc::new(Inner {
            budget: cfg.budget_bytes,
            committed: AtomicU64::new(0),
            by_cat: std::array::from_fn(|_| AtomicU64::new(0)),
            live: AtomicU64::new(0),
            level: AtomicU8::new(Pressure::Normal.level()),
            high: AtomicU64::new(cfg.high.to_bits()),
            critical: AtomicU64::new(cfg.critical.to_bits()),
            rss: AtomicU64::new(0),
            baseline_rss: AtomicU64::new(u64::MAX),
        });
        let (tx, _rx) = watch::channel(Pressure::Normal);
        Arc::new(Self {
            inner,
            hold: cfg.critical_exit_hold,
            auto_tick_bytes: (cfg.budget_bytes / 64).max(1 << 20),
            tx,
            tick_state: Mutex::new(TickState { below_high_since: None }),
            purge: Mutex::new(PurgeRequest {
                epoch: 0,
                level: PurgeLevel::Light,
            }),
        })
    }

    /// Recompute pressure from `committed / budget` and publish a change (called by sysmon on its
    /// `memory.check_interval`, and after every large reservation).
    ///
    /// Rising is immediate. Leaving `High` needs no hold; leaving `Critical` requires usage to have
    /// stayed below `high` for `critical_exit_hold`.
    pub fn tick(&self) -> Pressure {
        let inner = &self.inner;
        let mut st = self.tick_state.lock();
        let committed = inner.committed.load(Ordering::Acquire);
        let current = inner.level();

        let by_usage = if inner.at_or_above(committed, inner.critical()) {
            Pressure::Critical
        } else if inner.at_or_above(committed, inner.high()) {
            Pressure::High
        } else {
            Pressure::Normal
        };

        let next = if current == Pressure::Critical && by_usage != Pressure::Critical {
            if by_usage == Pressure::High {
                // still above `high`: the hold clock restarts
                st.below_high_since = None;
                Pressure::Critical
            } else {
                let since = *st.below_high_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= self.hold {
                    st.below_high_since = None;
                    Pressure::Normal
                } else {
                    Pressure::Critical
                }
            }
        } else {
            st.below_high_since = None;
            by_usage
        };

        if next != current {
            inner.level.store(next.level(), Ordering::Release);
            self.tx.send_if_modified(|cur| {
                let changed = *cur != next;
                *cur = next;
                changed
            });
        }
        next
    }

    /// Update the watermarks live (`PUT /v1/node/config`).
    pub fn set_watermarks(&self, high: f64, critical: f64) {
        self.inner.high.store(high.to_bits(), Ordering::Relaxed);
        self.inner.critical.store(critical.to_bits(), Ordering::Relaxed);
    }

    /// Update RSS / baseline figures used for the fragmentation ratio.
    ///
    /// The baseline is the smallest RSS not explained by committed bytes (runtime text, shared
    /// libraries, ...); `fragmentation_ratio = rss / (committed + baseline)`. Fed every
    /// `memory.check_interval` by the node's maintenance loop where the process RSS is readable
    /// (Linux `/proc/self/status`); elsewhere RSS stays 0 and the ratio is not meaningful.
    pub fn observe_rss(&self, rss_bytes: u64) {
        let committed = self.inner.committed.load(Ordering::Acquire);
        self.inner.rss.store(rss_bytes, Ordering::Relaxed);
        self.inner
            .baseline_rss
            .fetch_min(rss_bytes.saturating_sub(committed), Ordering::Relaxed);
    }
}

impl MemoryGovernor for Governor {
    fn reserve(&self, cat: MemCategory, bytes: u64, owner: Owner) -> Result<Reservation, MemError> {
        let inner = &self.inner;
        if bytes > 0 {
            let committed = inner.committed.load(Ordering::Acquire);
            if committed.checked_add(bytes).is_none_or(|n| n > inner.budget) {
                return Err(MemError::OverBudget {
                    requested: bytes,
                    available: inner.budget.saturating_sub(committed),
                });
            }
            // Critical refuses new reservations until usage is back below `high`, even when they fit.
            if inner.level() == Pressure::Critical && inner.at_or_above(committed, inner.high()) {
                return Err(MemError::Critical);
            }
            // the CAS is the real admission check: concurrent reservations cannot jointly overshoot
            if let Err(available) = inner.charge(bytes) {
                return Err(MemError::OverBudget {
                    requested: bytes,
                    available,
                });
            }
            inner.by_cat[cat.idx()].fetch_add(bytes, Ordering::AcqRel);
        }
        inner.live.fetch_add(1, Ordering::AcqRel);
        let backend: Arc<dyn ReservationBackend> = inner.clone();
        let reservation = Reservation::new(backend, cat, owner, bytes);
        if bytes >= self.auto_tick_bytes {
            self.tick();
        }
        Ok(reservation)
    }

    fn pressure(&self) -> Pressure {
        self.inner.level()
    }

    fn subscribe(&self) -> PressureWatch {
        PressureWatch::new(self.tx.subscribe())
    }

    fn stats(&self) -> MemStats {
        let inner = &self.inner;
        let committed = inner.committed.load(Ordering::Acquire);
        let rss = inner.rss.load(Ordering::Relaxed);
        let baseline = inner.baseline_rss.load(Ordering::Relaxed);
        let denom = committed.saturating_add(if baseline == u64::MAX { 0 } else { baseline });
        let fragmentation_ratio = if rss == 0 || denom == 0 {
            1.0
        } else {
            rss as f64 / denom as f64
        };
        MemStats {
            budget_bytes: inner.budget,
            committed_bytes: committed,
            by_category: MemCategory::ALL
                .iter()
                .map(|c| (*c, inner.by_cat[c.idx()].load(Ordering::Acquire)))
                .collect(),
            rss_bytes: rss,
            fragmentation_ratio,
            pressure: inner.level(),
            live_reservations: inner.live.load(Ordering::Acquire),
        }
    }

    fn request_purge(&self, level: PurgeLevel) {
        let mut p = self.purge.lock();
        p.epoch += 1;
        p.level = level;
    }

    fn purge_request(&self) -> PurgeRequest {
        *self.purge.lock()
    }

    fn budget_bytes(&self) -> u64 {
        self.inner.budget
    }
}
