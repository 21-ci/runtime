//! RAII reservations.

use std::fmt;
use std::sync::Arc;

use crate::types::{MemCategory, Owner};

/// What a governor implementation provides to its reservations.
pub trait ReservationBackend: Send + Sync + 'static {
    /// Try to charge `by` more bytes to `cat`. `false` ⇒ refused (budget / pressure).
    fn try_grow(&self, cat: MemCategory, by: u64) -> bool;
    /// Return `bytes` to `cat` (called on shrink and drop).
    fn release(&self, cat: MemCategory, bytes: u64);
    /// A reservation of this backend was dropped (also called for empty ones, after the final
    /// `release`). Lets a governor count live reservations.
    fn reservation_dropped(&self) {}
}

/// Bytes charged to the governor. Dropping it releases them, including on unwind.
pub struct Reservation {
    backend: Option<Arc<dyn ReservationBackend>>,
    cat: MemCategory,
    owner: Owner,
    bytes: u64,
}

impl Reservation {
    /// Created by a [`crate::MemoryGovernor`] after it has already charged `bytes`.
    pub fn new(backend: Arc<dyn ReservationBackend>, cat: MemCategory, owner: Owner, bytes: u64) -> Self {
        Self {
            backend: Some(backend),
            cat,
            owner,
            bytes,
        }
    }

    /// A reservation not backed by any governor (tests, simulation): `try_grow` always succeeds.
    pub fn detached(cat: MemCategory, bytes: u64) -> Self {
        Self {
            backend: None,
            cat,
            owner: Owner::NONE,
            bytes,
        }
    }

    /// Currently reserved bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn category(&self) -> MemCategory {
        self.cat
    }

    pub fn owner(&self) -> Owner {
        self.owner
    }

    /// Reserve `by` more bytes. Returns `false` (and changes nothing) if the governor refuses.
    pub fn try_grow(&mut self, by: u64) -> bool {
        let ok = match &self.backend {
            Some(b) => b.try_grow(self.cat, by),
            None => true,
        };
        if ok {
            self.bytes = self.bytes.saturating_add(by);
        }
        ok
    }

    /// Give back `by` bytes (clamped to what is held).
    pub fn shrink(&mut self, by: u64) {
        let by = by.min(self.bytes);
        if by == 0 {
            return;
        }
        if let Some(b) = &self.backend {
            b.release(self.cat, by);
        }
        self.bytes -= by;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(b) = &self.backend {
            if self.bytes > 0 {
                b.release(self.cat, self.bytes);
            }
            b.reservation_dropped();
        }
        self.bytes = 0;
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation")
            .field("cat", &self.cat)
            .field("bytes", &self.bytes)
            .finish()
    }
}
