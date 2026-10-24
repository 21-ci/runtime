//! The lock table behind `std:lock`: one named mutex per (service, key) on this
//! node.
//!
//! * Waiting is async: a waiter parks on a oneshot and never blocks a thread.
//! * Grants are FIFO: the head waiter gets the key when the holder lets go.
//! * A hold has a lease. A holder that is killed, hangs, or never releases loses the key when the
//!   lease runs out; the next waiter is granted then (the state is re-checked by the waiters
//!   themselves, so no timer task exists per hold). A late release by a stale holder does nothing.
//! * A grant that is never taken (its waiter is dropped, e.g. the task is killed) is passed on.
//! * Caps: [`LOCK_KEYS_PER_SERVICE`] keys per service and [`LOCK_WAITERS_PER_KEY`] queued waiters per
//!   key answer `lock_limit`; keys with no holder and no waiter are forgotten.
//!
//! The state is authoritative: a waiter's oneshot only wakes it, and it returns only when the table
//! names it the holder.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rt_types::ServiceId;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::facade::HostError;

/// Longest key, in bytes.
pub const LOCK_KEY_MAX_BYTES: usize = 512;
/// Longest wait `acquire` accepts.
pub const LOCK_WAIT_MAX: Duration = Duration::from_secs(300);
/// Longest lease `acquire` accepts.
pub const LOCK_LEASE_MAX: Duration = Duration::from_secs(900);
/// Distinct keys one service may hold or wait on at once.
pub const LOCK_KEYS_PER_SERVICE: usize = 1024;
/// Callers queued behind one key.
pub const LOCK_WAITERS_PER_KEY: usize = 64;

type Key = (ServiceId, String);

struct Holder {
    token: u64,
    expires: Instant,
}

struct Waiter {
    token: u64,
    lease: Duration,
    wake: oneshot::Sender<()>,
}

#[derive(Default)]
struct Slot {
    holder: Option<Holder>,
    waiters: VecDeque<Waiter>,
}

#[derive(Default)]
struct State {
    slots: HashMap<Key, Slot>,
    /// Slots per service (the per-service key cap).
    per_service: HashMap<ServiceId, usize>,
}

impl State {
    /// Bring `k` up to date: an expired holder is dropped, the queue is granted in order while the
    /// key is free, and a slot with neither holder nor waiter is forgotten.
    fn settle(&mut self, k: &Key, now: Instant) {
        let Some(slot) = self.slots.get_mut(k) else { return };
        if slot.holder.as_ref().is_some_and(|h| h.expires <= now) {
            slot.holder = None;
        }
        if slot.holder.is_none() {
            while let Some(w) = slot.waiters.pop_front() {
                let (token, lease) = (w.token, w.lease);
                // A waiter whose future is gone cannot receive the grant: skip it.
                if w.wake.send(()).is_ok() {
                    slot.holder = Some(Holder {
                        token,
                        expires: now + lease,
                    });
                    break;
                }
            }
        }
        if slot.holder.is_none() && slot.waiters.is_empty() {
            self.slots.remove(k);
            if let Some(n) = self.per_service.get_mut(&k.0) {
                *n -= 1;
                if *n == 0 {
                    self.per_service.remove(&k.0);
                }
            }
        }
    }

    fn holder_of(&self, k: &Key) -> Option<(u64, Instant)> {
        self.slots
            .get(k)
            .and_then(|s| s.holder.as_ref())
            .map(|h| (h.token, h.expires))
    }
}

/// The node's lock table (one per node, shared by every task).
pub struct LockTable {
    state: Mutex<State>,
    next_token: AtomicU64,
}

/// A held lock. Dropping it releases the key, unless the lease already passed it on.
pub struct LockHold {
    table: Arc<LockTable>,
    key: Key,
    token: u64,
}

impl LockHold {
    /// The hold's ID, as the guest refers to it.
    pub fn id(&self) -> u64 {
        self.token
    }
}

impl Drop for LockHold {
    fn drop(&mut self) {
        self.table.release(&self.key, self.token);
    }
}

/// Dropped while queued or granted (a cancelled `acquire`): leave the queue and give up a grant.
struct Pending {
    table: Arc<LockTable>,
    key: Key,
    token: u64,
    armed: bool,
}

impl Drop for Pending {
    fn drop(&mut self) {
        if self.armed {
            self.table.abandon(&self.key, self.token);
        }
    }
}

fn validate(key: &str, wait: Duration, lease: Duration) -> Result<(), HostError> {
    if key.is_empty() || key.len() > LOCK_KEY_MAX_BYTES {
        return Err(HostError::new("validation_failed", "lock key must be 1 to 512 bytes"));
    }
    if wait > LOCK_WAIT_MAX {
        return Err(HostError::new(
            "validation_failed",
            "lock wait must be at most 300 seconds",
        ));
    }
    if lease.is_zero() || lease > LOCK_LEASE_MAX {
        return Err(HostError::new(
            "validation_failed",
            "lock lease must be 1 ms to 900 seconds",
        ));
    }
    Ok(())
}

impl LockTable {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            next_token: AtomicU64::new(1),
        })
    }

    /// Hold `key` of `service` for `lease`, waiting at most `wait` for it. `lock_timeout` when the
    /// key stays held for the whole wait (`wait` zero = try), `lock_limit` past a cap.
    pub async fn acquire(
        self: &Arc<Self>,
        service: ServiceId,
        key: &str,
        wait: Duration,
        lease: Duration,
    ) -> Result<LockHold, HostError> {
        validate(key, wait, lease)?;
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let k: Key = (service, key.to_owned());
        let hold = |table: &Arc<Self>| LockHold {
            table: table.clone(),
            key: k.clone(),
            token,
        };
        let deadline = Instant::now() + wait;

        let mut rx = {
            let mut st = self.state.lock();
            let now = Instant::now();
            st.settle(&k, now);
            if !st.slots.contains_key(&k) {
                if st.per_service.get(&service).copied().unwrap_or(0) >= LOCK_KEYS_PER_SERVICE {
                    return Err(HostError::new("lock_limit", "too many locks in use for this service"));
                }
                *st.per_service.entry(service).or_insert(0) += 1;
                st.slots.insert(k.clone(), Slot::default());
            }
            #[allow(clippy::expect_used, reason = "the slot was inserted just above under the same lock")]
            let slot = st.slots.get_mut(&k).expect("slot exists");
            if slot.holder.is_none() {
                slot.holder = Some(Holder {
                    token,
                    expires: now + lease,
                });
                return Ok(hold(self));
            }
            if wait.is_zero() {
                return Err(HostError::new("lock_timeout", "lock is held"));
            }
            if slot.waiters.len() >= LOCK_WAITERS_PER_KEY {
                return Err(HostError::new("lock_limit", "too many callers waiting for this lock"));
            }
            let (tx, rx) = oneshot::channel();
            slot.waiters.push_back(Waiter { token, lease, wake: tx });
            Some(rx)
        };

        let mut pending = Pending {
            table: self.clone(),
            key: k.clone(),
            token,
            armed: true,
        };
        loop {
            let now = Instant::now();
            let next_wake = {
                let mut st = self.state.lock();
                st.settle(&k, now);
                if let Some((holder, _)) = st.holder_of(&k) {
                    if holder == token {
                        pending.armed = false;
                        return Ok(hold(self));
                    }
                }
                if now >= deadline {
                    if let Some(slot) = st.slots.get_mut(&k) {
                        slot.waiters.retain(|w| w.token != token);
                    }
                    st.settle(&k, now);
                    pending.armed = false;
                    return Err(HostError::new("lock_timeout", "timed out waiting for the lock"));
                }
                st.holder_of(&k).map_or(deadline, |(_, expires)| expires.min(deadline))
            };
            let woken = match rx.as_mut() {
                Some(r) => tokio::select! {
                    _ = r => true,
                    _ = tokio::time::sleep_until(next_wake) => false,
                },
                None => {
                    tokio::time::sleep_until(next_wake).await;
                    false
                }
            };
            if woken {
                rx = None;
            }
        }
    }

    /// The hold `token` of `k` lets go (a stale hold, already passed on by its lease, does nothing).
    fn release(&self, k: &Key, token: u64) {
        let mut st = self.state.lock();
        if st.holder_of(k).is_some_and(|(holder, _)| holder == token) {
            if let Some(slot) = st.slots.get_mut(k) {
                slot.holder = None;
            }
            st.settle(k, Instant::now());
        }
    }

    /// A cancelled `acquire`: leave the queue, and release a grant it was given but never took.
    fn abandon(&self, k: &Key, token: u64) {
        let mut st = self.state.lock();
        if let Some(slot) = st.slots.get_mut(k) {
            slot.waiters.retain(|w| w.token != token);
        }
        if st.holder_of(k).is_some_and(|(holder, _)| holder == token) {
            if let Some(slot) = st.slots.get_mut(k) {
                slot.holder = None;
            }
        }
        st.settle(k, Instant::now());
    }
}
