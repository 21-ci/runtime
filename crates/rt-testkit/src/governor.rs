//! `TestGovernor`: a small, fully working `MemoryGovernor` for tests that need memory
//! behavior (SCH-15, SCH-17, MEM-02) before `rt_mem::Governor` exists.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rt_mem::{
    MemCategory, MemError, MemStats, MemoryGovernor, Owner, Pressure, PressureWatch, PurgeLevel, PurgeRequest,
    Reservation, ReservationBackend,
};
use tokio::sync::watch;

struct Core {
    budget: u64,
    committed: AtomicU64,
    by_cat: Mutex<[u64; 8]>,
    high: f64,
    critical: f64,
    tx: watch::Sender<Pressure>,
    purge_epoch: AtomicU64,
    purge_level: Mutex<PurgeLevel>,
}

impl Core {
    fn pressure_now(&self) -> Pressure {
        let used = self.committed.load(Ordering::Relaxed) as f64;
        let b = self.budget.max(1) as f64;
        if used >= b * self.critical {
            Pressure::Critical
        } else if used >= b * self.high {
            Pressure::High
        } else {
            Pressure::Normal
        }
    }

    fn publish(&self) {
        let p = self.pressure_now();
        self.tx.send_if_modified(|cur| {
            if *cur != p {
                *cur = p;
                true
            } else {
                false
            }
        });
    }
}

impl ReservationBackend for Core {
    fn try_grow(&self, cat: MemCategory, by: u64) -> bool {
        let mut cur = self.committed.load(Ordering::Relaxed);
        loop {
            if cur + by > self.budget {
                return false;
            }
            match self
                .committed
                .compare_exchange(cur, cur + by, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(a) => cur = a,
            }
        }
        self.by_cat.lock()[cat.idx()] += by;
        self.publish();
        true
    }

    fn release(&self, cat: MemCategory, bytes: u64) {
        self.committed.fetch_sub(bytes, Ordering::AcqRel);
        {
            let mut g = self.by_cat.lock();
            g[cat.idx()] = g[cat.idx()].saturating_sub(bytes);
        }
        self.publish();
    }
}

/// Budgeted governor with `balanced` watermarks (0.80 / 0.92).
pub struct TestGovernor {
    core: Arc<Core>,
}

impl TestGovernor {
    pub fn new(budget_bytes: u64) -> Arc<Self> {
        let (tx, _rx) = watch::channel(Pressure::Normal);
        Arc::new(Self {
            core: Arc::new(Core {
                budget: budget_bytes,
                committed: AtomicU64::new(0),
                by_cat: Mutex::new([0; 8]),
                high: 0.80,
                critical: 0.92,
                tx,
                purge_epoch: AtomicU64::new(0),
                purge_level: Mutex::new(PurgeLevel::Light),
            }),
        })
    }

    pub fn with_mb(budget_mb: u64) -> Arc<Self> {
        Self::new(budget_mb * 1024 * 1024)
    }

    /// Bytes currently reserved (for "returns to baseline" assertions, SCH-15).
    pub fn committed(&self) -> u64 {
        self.core.committed.load(Ordering::Acquire)
    }
}

impl MemoryGovernor for TestGovernor {
    fn reserve(&self, cat: MemCategory, bytes: u64, owner: Owner) -> Result<Reservation, MemError> {
        let available = self.core.budget.saturating_sub(self.committed());
        if !self.core.try_grow(cat, bytes) {
            return Err(MemError::OverBudget {
                requested: bytes,
                available,
            });
        }
        Ok(Reservation::new(self.core.clone(), cat, owner, bytes))
    }

    fn pressure(&self) -> Pressure {
        self.core.pressure_now()
    }

    fn subscribe(&self) -> PressureWatch {
        PressureWatch::new(self.core.tx.subscribe())
    }

    fn stats(&self) -> MemStats {
        let g = self.core.by_cat.lock();
        MemStats {
            budget_bytes: self.core.budget,
            committed_bytes: self.committed(),
            by_category: MemCategory::ALL.iter().map(|c| (*c, g[c.idx()])).collect(),
            rss_bytes: 0,
            fragmentation_ratio: 1.0,
            pressure: self.core.pressure_now(),
            live_reservations: 0,
        }
    }

    fn request_purge(&self, level: PurgeLevel) {
        *self.core.purge_level.lock() = level;
        self.core.purge_epoch.fetch_add(1, Ordering::AcqRel);
    }

    fn purge_request(&self) -> PurgeRequest {
        PurgeRequest {
            epoch: self.core.purge_epoch.load(Ordering::Acquire),
            level: *self.core.purge_level.lock(),
        }
    }

    fn budget_bytes(&self) -> u64 {
        self.core.budget
    }
}
