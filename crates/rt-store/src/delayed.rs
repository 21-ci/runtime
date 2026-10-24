//! One-off delayed schedules of `std:schedule`, in the queue database
//! (`queue.db`, table `delayed`): persisted, at-least-once, one pending schedule per (service, id).
//!
//! A due schedule is leased for a while (its delivery is in flight); the delivery then acks it
//! ([`DelayedStore::done`]), or retries it ([`DelayedStore::retry`]), which drops it after
//! [`DELAYED_MAX_ATTEMPTS`]. A lease that runs out, e.g. after a node restart, delivers it again.

use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rt_types::ServiceId;
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::error::Result;
use crate::queue::SqliteQueueStore;
use crate::sqlutil::now_ms;

/// Pending schedules per service.
pub const MAX_DELAYED_PENDING: usize = 1000;
/// Attempts before a schedule is dropped.
pub const DELAYED_MAX_ATTEMPTS: u32 = 5;

/// A schedule to store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewDelayed {
    pub id: String,
    pub run_at_ms: i64,
    /// JSON text.
    pub payload: Bytes,
}

/// A due schedule, leased to one caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayedItem {
    pub id: String,
    /// Identifies one put: a re-put of the id is a new generation.
    pub gen: i64,
    pub run_at_ms: i64,
    /// Delivery attempt, 1-based.
    pub attempt: u32,
    pub payload: Bytes,
}

/// A pending schedule as listed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayedEntry {
    pub id: String,
    pub run_at_ms: i64,
}

/// What [`DelayedStore::retry`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Due again at the given time.
    Requeued,
    /// The last attempt failed: the schedule is gone.
    Dropped,
    /// The generation was not current (a newer put or a finished delivery): nothing changed.
    Gone,
}

/// Pending one-off schedules of every service on the node (`std:schedule`).
#[async_trait]
pub trait DelayedStore: Send + Sync {
    /// Store (or replace) schedule `m.id` of `s`. `None` when `s` is at [`MAX_DELAYED_PENDING`] and
    /// `m.id` is not pending yet. `Some(gen)` identifies the put.
    async fn put(&self, s: ServiceId, m: NewDelayed) -> Result<Option<i64>>;
    /// Remove a pending schedule; `false` when there is none.
    async fn cancel(&self, s: ServiceId, id: &str) -> Result<bool>;
    /// Pending schedules of `s` in id order, after `after`.
    async fn list(&self, s: ServiceId, after: Option<&str>, limit: usize) -> Result<Vec<DelayedEntry>>;
    /// Lease up to `max` schedules due at `now_ms` (of any service) for `lease`.
    async fn lease(&self, now_ms: i64, lease: Duration, max: usize) -> Result<Vec<(ServiceId, DelayedItem)>>;
    /// The delivery of generation `gen` succeeded: remove it. `false` if it is not current.
    async fn done(&self, s: ServiceId, id: &str, gen: i64) -> Result<bool>;
    /// The delivery of generation `gen` failed: due again at `run_at_ms`, or dropped after the
    /// last attempt.
    async fn retry(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> Result<RetryOutcome>;
    /// A lease that never started a delivery: due again at `run_at_ms`, and the attempt it took is
    /// given back. `false` if the generation is not current.
    async fn release(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> Result<bool>;
}

#[async_trait]
impl DelayedStore for SqliteQueueStore {
    async fn put(&self, s: ServiceId, m: NewDelayed) -> Result<Option<i64>> {
        let gen = next_gen();
        self.db
            .call(move |c| {
                let service = s.to_string();
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM delayed WHERE service_id = ?1 AND id = ?2)",
                    params![service, m.id],
                    |r| r.get(0),
                )?;
                if !exists {
                    let pending: i64 = tx.query_row(
                        "SELECT count(*) FROM delayed WHERE service_id = ?1",
                        params![service],
                        |r| r.get(0),
                    )?;
                    if pending as usize >= MAX_DELAYED_PENDING {
                        return Ok(None);
                    }
                }
                tx.execute(
                    "INSERT INTO delayed (service_id, id, gen, run_at, leased_until, attempts, payload) \
                     VALUES (?1, ?2, ?3, ?4, 0, 0, ?5) \
                     ON CONFLICT (service_id, id) DO UPDATE SET gen = excluded.gen, run_at = excluded.run_at, \
                       leased_until = 0, attempts = 0, payload = excluded.payload",
                    params![service, m.id, gen, m.run_at_ms, m.payload.as_ref()],
                )?;
                tx.commit()?;
                Ok(Some(gen))
            })
            .await
    }

    async fn cancel(&self, s: ServiceId, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                let n = c.execute(
                    "DELETE FROM delayed WHERE service_id = ?1 AND id = ?2",
                    params![s.to_string(), id],
                )?;
                Ok(n > 0)
            })
            .await
    }

    async fn list(&self, s: ServiceId, after: Option<&str>, limit: usize) -> Result<Vec<DelayedEntry>> {
        let after = after.unwrap_or("").to_string();
        self.db
            .call(move |c| {
                let mut stmt = c.prepare_cached(
                    "SELECT id, run_at FROM delayed WHERE service_id = ?1 AND id > ?2 ORDER BY id LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![s.to_string(), after, limit as i64], |r| {
                    Ok(DelayedEntry {
                        id: r.get(0)?,
                        run_at_ms: r.get(1)?,
                    })
                })?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn lease(&self, now_ms: i64, lease: Duration, max: usize) -> Result<Vec<(ServiceId, DelayedItem)>> {
        let until = now_ms + lease.as_millis() as i64;
        self.db
            .call(move |c| {
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let due: Vec<(String, String, i64, i64, i64, Vec<u8>)> = {
                    let mut stmt = tx.prepare_cached(
                        "SELECT service_id, id, gen, run_at, attempts, payload FROM delayed \
                         WHERE run_at <= ?1 AND leased_until <= ?1 ORDER BY run_at LIMIT ?2",
                    )?;
                    let rows = stmt.query_map(params![now_ms, max as i64], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
                    })?;
                    rows.collect::<rusqlite::Result<_>>()?
                };
                let mut out = Vec::with_capacity(due.len());
                for (service, id, gen, run_at_ms, attempts, payload) in due {
                    tx.execute(
                        "UPDATE delayed SET leased_until = ?1, attempts = attempts + 1 \
                         WHERE service_id = ?2 AND id = ?3 AND gen = ?4",
                        params![until, service, id, gen],
                    )?;
                    let Ok(s) = ServiceId::from_str(&service) else { continue };
                    let attempt = u32::try_from(attempts + 1).unwrap_or(u32::MAX);
                    out.push((
                        s,
                        DelayedItem {
                            id,
                            gen,
                            run_at_ms,
                            attempt,
                            payload: Bytes::from(payload),
                        },
                    ));
                }
                tx.commit()?;
                Ok(out)
            })
            .await
    }

    async fn done(&self, s: ServiceId, id: &str, gen: i64) -> Result<bool> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                let n = c.execute(
                    "DELETE FROM delayed WHERE service_id = ?1 AND id = ?2 AND gen = ?3",
                    params![s.to_string(), id, gen],
                )?;
                Ok(n > 0)
            })
            .await
    }

    async fn release(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> Result<bool> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                let n = c.execute(
                    "UPDATE delayed SET leased_until = 0, attempts = MAX(attempts - 1, 0), run_at = ?4 \
                     WHERE service_id = ?1 AND id = ?2 AND gen = ?3",
                    params![s.to_string(), id, gen, run_at_ms],
                )?;
                Ok(n > 0)
            })
            .await
    }

    async fn retry(&self, s: ServiceId, id: &str, gen: i64, run_at_ms: i64) -> Result<RetryOutcome> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                let service = s.to_string();
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let attempts: Option<i64> = tx
                    .query_row(
                        "SELECT attempts FROM delayed WHERE service_id = ?1 AND id = ?2 AND gen = ?3",
                        params![service, id, gen],
                        |r| r.get(0),
                    )
                    .optional()?;
                let outcome = match attempts {
                    None => RetryOutcome::Gone,
                    Some(n) if n as u32 >= DELAYED_MAX_ATTEMPTS => {
                        tx.execute(
                            "DELETE FROM delayed WHERE service_id = ?1 AND id = ?2 AND gen = ?3",
                            params![service, id, gen],
                        )?;
                        RetryOutcome::Dropped
                    }
                    Some(_) => {
                        tx.execute(
                            "UPDATE delayed SET leased_until = 0, run_at = ?4 WHERE service_id = ?1 AND id = ?2 AND gen = ?3",
                            params![service, id, gen, run_at_ms],
                        )?;
                        RetryOutcome::Requeued
                    }
                };
                tx.commit()?;
                Ok(outcome)
            })
            .await
    }
}

/// Generations are unique within the process and, from the start time, across restarts.
fn next_gen() -> i64 {
    static NEXT: OnceLock<AtomicI64> = OnceLock::new();
    NEXT.get_or_init(|| AtomicI64::new(now_ms() * 1_000))
        .fetch_add(1, Ordering::Relaxed)
}
