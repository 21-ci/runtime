//! `queue.db`: durable, per-service, at-least-once queue.
//!
//! Visibility timeout = task wall limit + 10 s. Retry backoff (`base 2 s × 2^attempt`, ±20 %
//! jitter) is computed by `rt-triggers`; this store only persists attempt counts and
//! visibility. After `max_attempts` a message moves to the per-service DLQ.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rt_types::{MsgId, ServiceId, UnixMs};
use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};
use crate::sqlutil::{migrate, now_ms, open_reader, parse_col, Db};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

/// Read-only connections of a file-backed `queue.db`.
const QUEUE_READERS: usize = 2;

pub const MIGRATIONS: &[(u32, &str)] = &[
    (1, include_str!("../migrations/queue_0001_init.sql")),
    (2, include_str!("../migrations/queue_0002_delayed.sql")),
    (3, include_str!("../migrations/queue_0003_invocations.sql")),
];

#[path = "invocations.rs"]
pub mod invocations;
pub use invocations::*;

/// Message size cap.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
/// Maximum enqueue delay (7 days).
pub const MAX_DELAY: Duration = Duration::from_secs(7 * 24 * 3600);
/// De-duplication window.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewMessage {
    pub body: Bytes,
    pub delay: Duration,
    /// Messages with the same `dedup_id` within 24 h are delivered once.
    pub dedup_id: Option<String>,
}

impl NewMessage {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            body: body.into(),
            delay: Duration::ZERO,
            dedup_id: None,
        }
    }
}

/// A leased message: invisible to other consumers until its visibility timeout elapses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leased {
    pub id: MsgId,
    pub body: Bytes,
    /// 1 for the first delivery.
    pub attempt: u16,
    pub enqueued_at_ms: UnixMs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryResult {
    /// Re-queued with the given attempt count, visible again after the delay.
    Requeued { attempt: u16 },
    /// `max_attempts` reached: moved to the DLQ.
    DeadLettered,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Messages waiting (visible or delayed).
    pub depth: u64,
    pub in_flight: u64,
    /// Age of the oldest waiting message.
    pub oldest_age_ms: u64,
    pub dlq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqEntry {
    pub id: MsgId,
    pub body: Bytes,
    pub attempts: u16,
    pub failed_at_ms: UnixMs,
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    /// `QuotaError::QueueFull` is the caller's job (`plan.max_queue_depth` via `stats`).
    async fn enqueue(&self, s: ServiceId, m: NewMessage) -> Result<MsgId>;
    /// [`QueueStore::enqueue`] unless the service already has `max_depth` waiting messages
    /// (`Conflict("queue_full")`), counted in the inserting transaction so concurrent enqueues
    /// cannot overshoot (CALL-011). A dedup hit adds nothing and always answers.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn enqueue_capped(&self, s: ServiceId, m: NewMessage, max_depth: u64) -> Result<MsgId> {
        if self.stats(s).await?.depth >= max_depth {
            return Err(StoreError::Conflict("queue_full"));
        }
        self.enqueue(s, m).await
    }
    // LCOV_EXCL_STOP
    /// Lease up to `max` visible messages for `visibility`.
    async fn lease(&self, s: ServiceId, max: usize, visibility: Duration) -> Result<Vec<Leased>>;
    /// Delete acknowledged messages.
    async fn ack(&self, ids: &[MsgId]) -> Result<()>;
    /// Release a leased message back to the queue after `delay`, or move it to the DLQ once its
    /// attempt count reaches the store's `max_attempts` (`queue.max_attempts`).
    async fn retry(&self, id: MsgId, delay: Duration) -> Result<RetryResult>;
    /// [`QueueStore::ack`] fenced by the lease: a message is deleted only while it is still on
    /// the delivery `attempt` the consumer leased. After a visibility timeout the message is
    /// re-leased with a higher attempt, so a slow consumer's late ack cannot delete a delivery
    /// another consumer is processing. Returns how many messages were deleted.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn ack_leased(&self, msgs: &[(MsgId, u16)]) -> Result<u64> {
        let ids: Vec<MsgId> = msgs.iter().map(|(id, _)| *id).collect();
        self.ack(&ids).await.map(|()| ids.len() as u64)
    }
    // LCOV_EXCL_STOP
    /// [`QueueStore::retry`] fenced by the lease (see [`QueueStore::ack_leased`]): `NotFound`
    /// when the message is no longer leased on `attempt`.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn retry_leased(&self, id: MsgId, attempt: u16, delay: Duration) -> Result<RetryResult> {
        let _ = attempt;
        self.retry(id, delay).await
    }
    // LCOV_EXCL_STOP
    /// Hand leased messages back without using up their delivery: the attempt the lease added
    /// is refunded (`attempt - 1`, never below 0) and the message is visible again after
    /// `delay`. Fenced like [`QueueStore::ack_leased`]; returns how many messages were released.
    /// Used when the delivery never ran (admission refusal, shutdown).
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    async fn release_leased(&self, msgs: &[(MsgId, u16)], delay: Duration) -> Result<u64> {
        let mut n = 0;
        for (id, attempt) in msgs {
            if self.retry_leased(*id, *attempt, delay).await.is_ok() {
                n += 1;
            }
        }
        Ok(n)
    }
    // LCOV_EXCL_STOP
    async fn stats(&self, s: ServiceId) -> Result<QueueStats>;
    async fn dlq_list(&self, s: ServiceId, limit: usize) -> Result<Vec<DlqEntry>>;
    /// Move DLQ messages back with `attempt = 1`; returns how many.
    async fn dlq_redrive(&self, s: ServiceId) -> Result<u64>;
    async fn dlq_purge(&self, s: ServiceId) -> Result<u64>;
    /// Services that currently have visible messages (engine polling).
    async fn services_with_ready(&self) -> Result<Vec<ServiceId>>;
}

/// Per service with messages: a lower bound of its earliest `visible_at` (B3.7). Only changed by
/// jobs on the writer thread, right after their commit, so it is never later than the truth:
/// writes that can make a message visible sooner (enqueue, retry, redrive) lower it, a lease and a
/// scan set it exactly, and deletes (ack, dead-letter) leave it low. `services_with_ready` reads it
/// and skips the database entirely while no service can be due.
type DueHint = Arc<Mutex<HashMap<ServiceId, i64>>>;

fn lower_due(due: &DueHint, s: ServiceId, at: i64) {
    due.lock().entry(s).and_modify(|d| *d = (*d).min(at)).or_insert(at);
}

/// Earliest `visible_at` of `s` (`messages_ready (service_id, visible_at)` answers it from the
/// index alone: a covering lookup, not a scan).
const EARLIEST_VISIBLE_SQL: &str = "SELECT min(visible_at) FROM messages WHERE service_id = ?1";

fn earliest_visible(c: &Connection, s: ServiceId) -> Result<Option<i64>> {
    Ok(c.prepare_cached(EARLIEST_VISIBLE_SQL)?
        .query_row([s.to_string()], |r| r.get(0))?)
}

/// Set `s`'s hint exactly (on the writer thread, after a commit).
fn refresh_due(due: &DueHint, c: &Connection, s: ServiceId) -> Result<Option<i64>> {
    let at = earliest_visible(c, s)?;
    let mut map = due.lock();
    match at {
        Some(at) => map.insert(s, at),
        None => map.remove(&s),
    };
    Ok(at)
}

/// `rusqlite`-backed [`QueueStore`].
pub struct SqliteQueueStore {
    pub(crate) db: Arc<Db>,
    max_attempts: u16,
    due: DueHint,
    /// Times `services_with_ready` went to the database (diagnostics, tests).
    scans: AtomicU64,
    /// Failed invocations evicted by the per-service cap, not yet taken by the engine.
    evicted: AtomicU64,
}

impl SqliteQueueStore {
    /// Make the next operations of this store (queue and one-off schedules) fail as `faults`
    /// plans (tests, TK-002).
    pub fn inject_faults(&self, faults: Arc<crate::faults::Faults>) {
        self.db.inject_faults(faults);
    }

    /// `max_attempts` = `queue.max_attempts` (5).
    pub fn open(path: &Path, max_attempts: u16) -> Result<Arc<Self>> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0))?;
        Self::init(conn, Some(path), max_attempts)
    }

    pub fn open_in_memory(max_attempts: u16) -> Result<Arc<Self>> {
        Self::init(Connection::open_in_memory()?, None, max_attempts)
    }

    fn init(mut conn: Connection, file: Option<&Path>, max_attempts: u16) -> Result<Arc<Self>> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.set_prepared_statement_cache_capacity(64);
        migrate(&mut conn, MIGRATIONS)?;
        // `stats` (checked on every enqueue for the depth quota) and `dlq_list` read on their own
        // connections; everything else writes.
        let readers = match file {
            Some(path) => (0..QUEUE_READERS)
                .map(|_| open_reader(path, 64))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        let mut due = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT service_id, min(visible_at) FROM messages GROUP BY service_id")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                let (s, at) = row?;
                due.insert(parse_col::<ServiceId>(&s, 0)?, at);
            }
        }
        Ok(Arc::new(Self {
            db: Db::new("rt-queue", conn, readers)?,
            max_attempts: max_attempts.max(1),
            due: Arc::new(Mutex::new(due)),
            scans: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
        }))
    }

    /// Drop everything queue.db holds for a deleted service: messages, dead letters, dedup ids and
    /// its one-off schedules (meta.db's cascade does not reach this file, META-010). Returns the
    /// number of rows removed.
    pub async fn purge_service(&self, s: ServiceId) -> Result<u64> {
        let due = self.due.clone();
        self.db
            .call(move |c| {
                let tx = c.transaction()?;
                let key = s.to_string();
                let mut n = 0u64;
                for table in ["messages", "dlq", "dedup", "delayed", "invocations"] {
                    n += tx.execute(&format!("DELETE FROM {table} WHERE service_id = ?1"), [&key])? as u64;
                }
                tx.commit()?;
                due.lock().remove(&s);
                Ok(n)
            })
            .await
    }

    /// How often `services_with_ready` had to query the database (B3.7 test/diagnostics hook).
    pub fn ready_scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn msg_id(s: &str) -> rusqlite::Result<MsgId> {
    parse_col(s, 0)
}

/// Move one message to the DLQ (inside a transaction).
fn dead_letter(c: &Connection, id: &str, now: i64) -> Result<()> {
    c.prepare_cached(
        "INSERT OR REPLACE INTO dlq (id, service_id, body, attempts, failed_at) \
         SELECT id, service_id, body, attempt, ?2 FROM messages WHERE id = ?1",
    )?
    .execute(params![id, now])?;
    c.prepare_cached("DELETE FROM messages WHERE id = ?1")?.execute([id])?;
    Ok(())
}

/// Returns the message id and, for a newly queued message, when it becomes visible. With
/// `max_depth`, a new message is refused (`Conflict("queue_full")`) once the service has that many
/// waiting messages (the same count as `stats().depth`).
fn enqueue_tx(c: &mut Connection, s: ServiceId, m: NewMessage, max_depth: Option<u64>) -> Result<(MsgId, Option<i64>)> {
    if m.body.len() > MAX_MESSAGE_BYTES {
        return Err(StoreError::Invalid(format!(
            "message body exceeds {MAX_MESSAGE_BYTES} bytes"
        )));
    }
    if m.delay > MAX_DELAY {
        return Err(StoreError::Invalid("delay exceeds 7 days".into()));
    }
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = now_ms();
    let service = s.to_string();
    if let Some(d) = &m.dedup_id {
        let live: Option<String> = tx
            .query_row(
                "SELECT msg_id FROM dedup WHERE service_id = ?1 AND dedup_id = ?2 AND expires_at > ?3",
                params![service, d, now],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(id) = live {
            return Ok((msg_id(&id)?, None));
        }
        let queued: Option<String> = tx
            .query_row(
                "SELECT id FROM messages WHERE service_id = ?1 AND dedup_id = ?2",
                params![service, d],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(id) = queued {
            tx.execute(
                "INSERT OR REPLACE INTO dedup (service_id, dedup_id, msg_id, expires_at) VALUES (?1, ?2, ?3, ?4)",
                params![service, d, id, now.saturating_add(ms(DEDUP_WINDOW))],
            )?;
            tx.commit()?;
            return Ok((msg_id(&id)?, None));
        }
    }
    if let Some(max) = max_depth {
        let depth: i64 = tx.query_row(
            "SELECT count(*) FROM messages WHERE service_id = ?1 AND (leased = 0 OR visible_at <= ?2)",
            params![service, now],
            |r| r.get(0),
        )?;
        if depth.max(0) as u64 >= max {
            return Err(StoreError::Conflict("queue_full"));
        }
    }
    let id = MsgId::new();
    let visible_at = now.saturating_add(ms(m.delay));
    tx.execute(
        "INSERT INTO messages (id, service_id, body, attempt, visible_at, enqueued_at, dedup_id, leased) \
         VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6, 0)",
        params![id.to_string(), service, m.body.as_ref(), visible_at, now, m.dedup_id],
    )?;
    if let Some(d) = &m.dedup_id {
        tx.execute(
            "INSERT OR REPLACE INTO dedup (service_id, dedup_id, msg_id, expires_at) VALUES (?1, ?2, ?3, ?4)",
            params![service, d, id.to_string(), now.saturating_add(ms(DEDUP_WINDOW))],
        )?;
        // Opportunistic cleanup of expired dedup records.
        tx.execute(
            "DELETE FROM dedup WHERE expires_at <= ?1 AND service_id = ?2",
            params![now, service],
        )?;
    }
    tx.commit()?;
    Ok((id, Some(visible_at)))
}

fn lease_tx(
    c: &mut Connection,
    max_attempts: u16,
    s: ServiceId,
    max: usize,
    visibility: Duration,
) -> Result<Vec<Leased>> {
    if max == 0 {
        return Ok(Vec::new());
    }
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = now_ms();
    let service = s.to_string();

    // Poison messages: the lease expired and no attempts are left.
    let exhausted: Vec<String> = tx
        .prepare_cached(
            "SELECT id FROM messages WHERE service_id = ?1 AND leased = 1 AND visible_at <= ?2 AND attempt >= ?3",
        )?
        .query_map(params![service, now, max_attempts], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for id in &exhausted {
        dead_letter(&tx, id, now)?;
    }

    let rows: Vec<(String, Vec<u8>, i64, i64)> = tx
        .prepare_cached(
            "SELECT id, body, attempt, enqueued_at FROM messages \
             WHERE service_id = ?1 AND visible_at <= ?2 ORDER BY visible_at, id LIMIT ?3",
        )?
        .query_map(params![service, now, i64::try_from(max).unwrap_or(i64::MAX)], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut out = Vec::with_capacity(rows.len());
    for (id, body, attempt, enqueued) in rows {
        tx.prepare_cached("UPDATE messages SET leased = 1, attempt = attempt + 1, visible_at = ?2 WHERE id = ?1")?
            .execute(params![id, now.saturating_add(ms(visibility))])?;
        out.push(Leased {
            id: msg_id(&id)?,
            body: Bytes::from(body),
            attempt: u16::try_from(attempt + 1).unwrap_or(u16::MAX),
            enqueued_at_ms: enqueued,
        });
    }
    tx.commit()?;
    Ok(out)
}

/// `expect_attempt`: the delivery the caller leased (`None` = any current lease). A requeued
/// message's hint is lowered to its new visibility (on the writer thread, after the commit).
fn retry_tx(
    c: &mut Connection,
    due: &DueHint,
    max_attempts: u16,
    id: MsgId,
    expect_attempt: Option<u16>,
    delay: Duration,
) -> Result<RetryResult> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = now_ms();
    let key = id.to_string();
    let row: Option<(i64, i64, String)> = tx
        .query_row(
            "SELECT attempt, leased, service_id FROM messages WHERE id = ?1",
            [&key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((attempt, leased, service)) = row else {
        return Err(StoreError::NotFound);
    };
    if leased == 0 || expect_attempt.is_some_and(|a| i64::from(a) != attempt) {
        return Err(StoreError::NotFound);
    }
    let (result, visible_at) = if attempt >= i64::from(max_attempts) {
        dead_letter(&tx, &key, now)?;
        (RetryResult::DeadLettered, None)
    } else {
        let at = now.saturating_add(ms(delay));
        tx.execute(
            "UPDATE messages SET leased = 0, visible_at = ?2 WHERE id = ?1",
            params![key, at],
        )?;
        (
            RetryResult::Requeued {
                attempt: u16::try_from(attempt).unwrap_or(u16::MAX),
            },
            Some(at),
        )
    };
    tx.commit()?;
    if let Some(at) = visible_at {
        lower_due(due, parse_col(&service, 0)?, at);
    }
    Ok(result)
}

#[async_trait]
impl QueueStore for SqliteQueueStore {
    async fn enqueue(&self, s: ServiceId, m: NewMessage) -> Result<MsgId> {
        let due = self.due.clone();
        self.db
            .call(move |c| {
                let (id, visible_at) = enqueue_tx(c, s, m, None)?;
                if let Some(at) = visible_at {
                    lower_due(&due, s, at);
                }
                Ok(id)
            })
            .await
    }

    async fn enqueue_capped(&self, s: ServiceId, m: NewMessage, max_depth: u64) -> Result<MsgId> {
        let due = self.due.clone();
        self.db
            .call(move |c| {
                let (id, visible_at) = enqueue_tx(c, s, m, Some(max_depth))?;
                if let Some(at) = visible_at {
                    lower_due(&due, s, at);
                }
                Ok(id)
            })
            .await
    }

    async fn lease(&self, s: ServiceId, max: usize, visibility: Duration) -> Result<Vec<Leased>> {
        let (max_attempts, due) = (self.max_attempts, self.due.clone());
        self.db
            .call(move |c| {
                let leased = lease_tx(c, max_attempts, s, max, visibility)?;
                // Leased messages are invisible until their lease ends; the rest may be due now.
                refresh_due(&due, c, s)?;
                Ok(leased)
            })
            .await
    }

    async fn ack(&self, ids: &[MsgId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
        self.db
            .write(move |c| {
                let mut stmt = c.prepare_cached("DELETE FROM messages WHERE id = ?1")?;
                for id in &ids {
                    stmt.execute([id])?;
                }
                Ok(())
            })
            .await
    }

    async fn ack_leased(&self, msgs: &[(MsgId, u16)]) -> Result<u64> {
        if msgs.is_empty() {
            return Ok(0);
        }
        let msgs: Vec<(String, i64)> = msgs.iter().map(|(id, a)| (id.to_string(), i64::from(*a))).collect();
        self.db
            .write(move |c| {
                let mut stmt =
                    c.prepare_cached("DELETE FROM messages WHERE id = ?1 AND attempt = ?2 AND leased = 1")?;
                let mut deleted = 0u64;
                for (id, attempt) in &msgs {
                    deleted += stmt.execute(params![id, attempt])? as u64;
                }
                Ok(deleted)
            })
            .await
    }

    async fn retry_leased(&self, id: MsgId, attempt: u16, delay: Duration) -> Result<RetryResult> {
        let (max_attempts, due) = (self.max_attempts, self.due.clone());
        self.db
            .call(move |c| retry_tx(c, &due, max_attempts, id, Some(attempt), delay))
            .await
    }

    async fn release_leased(&self, msgs: &[(MsgId, u16)], delay: Duration) -> Result<u64> {
        if msgs.is_empty() {
            return Ok(0);
        }
        let msgs: Vec<(String, i64)> = msgs.iter().map(|(id, a)| (id.to_string(), i64::from(*a))).collect();
        let due = self.due.clone();
        self.db
            .call(move |c| {
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let at = now_ms().saturating_add(ms(delay));
                let mut released = 0u64;
                let mut services = Vec::new();
                {
                    let mut stmt = tx.prepare_cached(
                        "UPDATE messages SET leased = 0, attempt = max(attempt - 1, 0), visible_at = ?3 \
                         WHERE id = ?1 AND leased = 1 AND attempt = ?2 RETURNING service_id",
                    )?;
                    for (id, attempt) in &msgs {
                        let s: Option<String> = stmt.query_row(params![id, attempt, at], |r| r.get(0)).optional()?;
                        if let Some(s) = s {
                            released += 1;
                            services.push(s);
                        }
                    }
                }
                tx.commit()?;
                for s in services {
                    lower_due(&due, parse_col(&s, 0)?, at);
                }
                Ok(released)
            })
            .await
    }

    async fn retry(&self, id: MsgId, delay: Duration) -> Result<RetryResult> {
        let (max_attempts, due) = (self.max_attempts, self.due.clone());
        self.db
            .call(move |c| retry_tx(c, &due, max_attempts, id, None, delay))
            .await
    }

    async fn stats(&self, s: ServiceId) -> Result<QueueStats> {
        self.db
            .read(move |c| {
                let now = now_ms();
                let service = s.to_string();
                let (depth, oldest): (i64, Option<i64>) = c.query_row(
                    "SELECT count(*), min(enqueued_at) FROM messages \
                     WHERE service_id = ?1 AND (leased = 0 OR visible_at <= ?2)",
                    params![service, now],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                let in_flight: i64 = c.query_row(
                    "SELECT count(*) FROM messages WHERE service_id = ?1 AND leased = 1 AND visible_at > ?2",
                    params![service, now],
                    |r| r.get(0),
                )?; // LCOV_EXCL_LINE: the statement before reads the same table and columns, so it fails first
                let dlq: i64 = c.query_row("SELECT count(*) FROM dlq WHERE service_id = ?1", [&service], |r| {
                    r.get(0)
                })?;
                Ok(QueueStats {
                    depth: depth.max(0) as u64,
                    in_flight: in_flight.max(0) as u64,
                    oldest_age_ms: oldest.map(|t| (now - t).max(0) as u64).unwrap_or(0),
                    dlq: dlq.max(0) as u64,
                })
            })
            .await
    }

    async fn dlq_list(&self, s: ServiceId, limit: usize) -> Result<Vec<DlqEntry>> {
        self.db
            .read(move |c| {
                let rows = c
                    .prepare_cached(
                        "SELECT id, body, attempts, failed_at FROM dlq WHERE service_id = ?1 \
                         ORDER BY failed_at, id LIMIT ?2",
                    )?
                    .query_map(params![s.to_string(), i64::try_from(limit).unwrap_or(i64::MAX)], |r| {
                        let id: String = r.get(0)?;
                        let body: Vec<u8> = r.get(1)?;
                        let attempts: i64 = r.get(2)?;
                        Ok(DlqEntry {
                            id: msg_id(&id)?,
                            body: Bytes::from(body),
                            attempts: u16::try_from(attempts).unwrap_or(u16::MAX),
                            failed_at_ms: r.get(3)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    async fn dlq_redrive(&self, s: ServiceId) -> Result<u64> {
        let due = self.due.clone();
        self.db
            .write(move |c| {
                let now = now_ms();
                let service = s.to_string();
                // attempt = 0 so the next lease reports attempt 1.
                let n = c.execute(
                    "INSERT OR REPLACE INTO messages (id, service_id, body, attempt, visible_at, enqueued_at, dedup_id, leased) \
                     SELECT id, service_id, body, 0, ?2, ?2, NULL, 0 FROM dlq WHERE service_id = ?1",
                    params![service, now],
                )?;
                c.execute("DELETE FROM dlq WHERE service_id = ?1", [&service])?;
                if n > 0 {
                    // Lowering early is safe (the hint is a lower bound); only raising waits for commits.
                    lower_due(&due, s, now);
                }
                Ok(n as u64)
            })
            .await
    }

    async fn dlq_purge(&self, s: ServiceId) -> Result<u64> {
        self.db
            .call(move |c| Ok(c.execute("DELETE FROM dlq WHERE service_id = ?1", [s.to_string()])? as u64))
            .await
    }

    /// The engine polls this every loop: services whose hint says nothing can be due are not
    /// looked up at all, and the rest cost one index lookup each (no scan of `messages`).
    async fn services_with_ready(&self) -> Result<Vec<ServiceId>> {
        let now = now_ms();
        let candidates: Vec<ServiceId> = self
            .due
            .lock()
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(s, _)| *s)
            .collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.scans.fetch_add(1, Ordering::Relaxed);
        let due = self.due.clone();
        // On the writer thread: setting a hint exactly must not race with the commits that lower it.
        self.db
            .call(move |c| {
                let now = now_ms();
                let mut ready = Vec::new();
                for s in candidates {
                    if refresh_due(&due, c, s)?.is_some_and(|at| at <= now) {
                        ready.push(s);
                    }
                }
                ready.sort_by_key(|s| s.to_string());
                Ok(ready)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_readiness_lookup_is_answered_by_the_covering_index() {
        let mut c = Connection::open_in_memory().expect("open");
        migrate(&mut c, MIGRATIONS).expect("migrate");
        let plan: Vec<String> = c
            .prepare(&format!("EXPLAIN QUERY PLAN {EARLIEST_VISIBLE_SQL}"))
            .expect("prepare")
            .query_map(["svc"], |r| r.get::<_, String>(3))
            .expect("plan")
            .collect::<rusqlite::Result<_>>()
            .expect("rows");
        let plan = plan.join(" | ");
        assert!(plan.contains("COVERING INDEX messages_ready"), "plan: {plan}");
        assert!(!plan.contains("SCAN"), "no table or index scan: {plan}");
    }
}
