//! Fault injection for tests (TK-002): make the next operations of a store fail the way a real
//! disk or database can, so error paths above the store (control handlers, host APIs, engines)
//! can be exercised without a broken filesystem.
//!
//! A [`Faults`] plan is attached to a store with `inject_faults` (`SqliteMetaStore`,
//! `SqliteQueueStore`, `LocalServiceData`). Every operation of that store first takes the next
//! planned fault, if any, and fails with it before touching the database; with nothing planned it
//! costs one uncontended lock. Production code never attaches a plan.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::StoreError;

/// A failure a store operation can be made to return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The disk is full (`storage_quota`).
    DiskFull,
    /// The database stayed locked (`sql_busy`).
    Busy,
    /// An I/O error (`internal`).
    Io,
    /// The database file is damaged (`internal`).
    Corrupt,
}

impl Fault {
    fn error(self) -> StoreError {
        match self {
            Fault::DiskFull => StoreError::StorageQuota,
            Fault::Busy => StoreError::SqlBusy,
            Fault::Io => StoreError::Io(std::io::Error::other("injected I/O fault")),
            Fault::Corrupt => StoreError::Corrupt("injected corruption".into()),
        }
    }
}

/// The faults planned for one store, consumed in order.
#[derive(Debug, Default)]
pub struct Faults {
    plan: Mutex<VecDeque<Option<Fault>>>,
    injected: AtomicU64,
}

impl Faults {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The next `n` operations fail with `fault`.
    pub fn fail_next(&self, n: usize, fault: Fault) {
        let mut plan = self.plan.lock();
        plan.extend(std::iter::repeat_n(Some(fault), n));
    }

    /// The next `n` operations succeed, then whatever is planned after them applies: to fail the
    /// third operation of a sequence, `pass_next(2)` then `fail_next(1, ..)`.
    pub fn pass_next(&self, n: usize) {
        self.plan.lock().extend(std::iter::repeat_n(None, n));
    }

    /// Drop every fault still planned.
    pub fn clear(&self) {
        self.plan.lock().clear();
    }

    /// Faults still planned.
    pub fn pending(&self) -> usize {
        self.plan.lock().len()
    }

    /// Faults returned so far.
    pub fn injected(&self) -> u64 {
        self.injected.load(Ordering::Relaxed)
    }

    /// `Err` with the next planned fault, if any.
    pub(crate) fn check(&self) -> Result<(), StoreError> {
        match self.plan.lock().pop_front() {
            Some(Some(f)) => {
                self.injected.fetch_add(1, Ordering::Relaxed);
                Err(f.error())
            }
            Some(None) | None => Ok(()),
        }
    }
}

/// A store's optional plan (set once).
#[derive(Debug, Default)]
pub(crate) struct FaultSlot(std::sync::OnceLock<Arc<Faults>>);

impl FaultSlot {
    pub(crate) fn set(&self, faults: Arc<Faults>) {
        let _ = self.0.set(faults);
    }

    pub(crate) fn check(&self) -> Result<(), StoreError> {
        match self.0.get() {
            Some(f) => f.check(),
            None => Ok(()),
        }
    }
}
