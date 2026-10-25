//! `streams.db`: persistent, partitioned, replayable topic logs (`std:stream`) with per-consumer
//! cursors and dead letters.
//!
//! Offsets are dense per partition from 0; a keyed record goes to `fnv1a64(key) % partitions`,
//! an unkeyed one round robin. Appends are all-or-nothing per call and refused with
//! `Conflict("stream_full")` once the tenant's records plus dead letters would exceed the cap.
//! Cursor updates are fenced by `gen` (bumped by `reset`) so a late settle cannot move a cursor
//! that was reset meanwhile. Retention is time and size based ([`StreamStore::trim`]), independent
//! of consumption.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rt_types::events::{
    partition_for, valid_group_name, valid_topic_name, validate_headers, Namespace, StartPosition, StreamRecord,
    APPEND_BATCH_MAX, APPEND_BATCH_MAX_BYTES, DEAD_MAX_PER_CONSUMER, DEAD_RETENTION_MS, DEDUP_ID_MAX_BYTES,
    EVENT_BODY_MAX_BYTES, PARTITIONS_HARD_MAX, READ_LIMIT_MAX, STREAM_DEDUP_WINDOW_MS, STREAM_KEY_MAX_BYTES,
};
use rt_types::{ProjectId, ServiceId, TenantId, TopicId};
use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::error::{Result, StoreError};
use crate::sqlutil::{migrate, now_ms, open_reader, parse_col, Db};

/// Read-only connections of a file-backed `streams.db`.
pub const STREAM_READERS: usize = 2;

pub const STREAM_MIGRATIONS: &[(u32, &str)] = &[(1, include_str!("../migrations/stream_0001_init.sql"))];

/// Rows deleted per trim chunk (keeps write transactions short).
const TRIM_CHUNK: i64 = 5_000;

/// A topic to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicSpec {
    pub name: String,
    pub partitions: u32,
    pub retention_ms: u64,
    /// `0` = only the tenant cap.
    pub max_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topic {
    pub id: TopicId,
    pub ns: Namespace,
    pub name: String,
    pub partitions: u32,
    pub retention_ms: u64,
    pub max_bytes: u64,
    /// Bytes of its retained records.
    pub bytes: u64,
    /// Bytes of its consumers' dead letters.
    pub dead_bytes: u64,
    pub created_at_ms: i64,
}

/// A record to append.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewRecord {
    pub key: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    /// The same id on the topic within [`STREAM_DEDUP_WINDOW_MS`] returns the original offset.
    pub dedup_id: Option<String>,
}

impl NewRecord {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub partition: u32,
    pub offset: u64,
    pub duplicate: bool,
}

/// `first` = first retained offset, `next` = offset of the next append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionHead {
    pub partition: u32,
    pub first: u64,
    pub next: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFrom {
    Earliest,
    Latest,
    Offset(u64),
    /// First record with a timestamp at or after this unix ms.
    Time(i64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CursorKey {
    pub topic: TopicId,
    pub service: ServiceId,
    pub group: String,
    pub partition: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub key: CursorKey,
    /// Next offset to deliver.
    pub committed: u64,
    /// Failed deliveries of the record at `committed`.
    pub attempt: u16,
    /// Not before (unix ms, backoff); `0` = now.
    pub next_at_ms: i64,
    pub blocked: bool,
    pub last_error: Option<String>,
    pub gen: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadRecord {
    pub record: StreamRecord,
    pub attempts: u16,
    pub error: String,
    pub failed_at_ms: i64,
}

/// What one [`StreamStore::trim`] pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrimStats {
    pub records: u64,
    pub dedup: u64,
    pub dead: u64,
    /// Cursors that were behind the first retained offset and moved up to it.
    pub cursors_moved: u64,
}

#[async_trait]
pub trait StreamStore: Send + Sync {
    /// `Conflict("topic_exists")` for a taken name in the namespace, `Conflict("topic_limit")` when
    /// the tenant already has `max_topics`, `Invalid` for a bad name / partitions / retention.
    async fn create_topic(&self, ns: &Namespace, spec: TopicSpec, max_topics: u32) -> Result<Topic>;
    async fn get_topic(&self, ns: &Namespace, name: &str) -> Result<Option<Topic>>;
    async fn topic_by_id(&self, id: TopicId) -> Result<Option<Topic>>;
    async fn list_topics(
        &self,
        tenant: TenantId,
        project: Option<ProjectId>,
        environment: Option<&str>,
    ) -> Result<Vec<Topic>>;
    async fn update_topic(&self, id: TopicId, retention_ms: Option<u64>, max_bytes: Option<u64>) -> Result<Topic>;
    /// The topic with its records, cursors and dead letters (`NotFound` when absent).
    async fn delete_topic(&self, id: TopicId) -> Result<()>;
    /// Every topic of the tenant; returns how many topics.
    async fn purge_tenant(&self, tenant: TenantId) -> Result<u64>;
    /// The service's cursors and dead letters; returns the rows removed.
    async fn purge_service(&self, service: ServiceId) -> Result<u64>;
    async fn count_topics_in(&self, tenant: TenantId, project: ProjectId, environment: Option<&str>) -> Result<u64>;
    /// All-or-nothing. `Conflict("stream_full")` when the tenant's bytes (records + dead letters)
    /// plus this batch would exceed `tenant_max_bytes`.
    async fn append(&self, topic: TopicId, records: Vec<NewRecord>, tenant_max_bytes: u64) -> Result<Vec<Appended>>;
    async fn heads(&self, topic: TopicId) -> Result<Vec<PartitionHead>>;
    /// Up to `limit` (<= [`READ_LIMIT_MAX`]) records, stopping before `max_bytes` of bodies
    /// (always at least one record when there is one).
    async fn read(
        &self,
        topic: TopicId,
        partition: u32,
        from: ReadFrom,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<StreamRecord>>;
    /// Get or create the cursor (a new one starts at the partition's first or next offset).
    async fn cursor(&self, key: &CursorKey, start: StartPosition) -> Result<Cursor>;
    async fn cursors_of(&self, service: ServiceId) -> Result<Vec<Cursor>>;
    async fn cursors_on(&self, topic: TopicId) -> Result<Vec<Cursor>>;
    /// `committed = offset`, attempt 0, no backoff. `false` = gen mismatch / gone.
    async fn commit(&self, key: &CursorKey, gen: u64, offset: u64) -> Result<bool>;
    /// One failed delivery of the record at `at`: `attempt + 1` when `at == committed`, else
    /// `committed = at, attempt = 1`; not before `now + delay`. Returns the new attempt.
    async fn fail(&self, key: &CursorKey, gen: u64, at: u64, delay: Duration, error: &str) -> Result<Option<u16>>;
    /// Not before `now + delay`, no attempt counted.
    async fn defer(&self, key: &CursorKey, gen: u64, delay: Duration) -> Result<bool>;
    /// Copy `record` to the consumer's dead letters and move the cursor past it. When the tenant's
    /// records plus dead letters would exceed `tenant_max_bytes`, the copy keeps only its position,
    /// attempts and error (no key, headers or body), so dead letters never grow past the cap.
    async fn dead_letter(
        &self,
        key: &CursorKey,
        gen: u64,
        record: &StreamRecord,
        attempts: u16,
        error: &str,
        tenant_max_bytes: u64,
    ) -> Result<bool>;
    async fn block(&self, key: &CursorKey, gen: u64, error: &str) -> Result<bool>;
    /// Move the group's cursors (one partition or all; created when missing): gen + 1, active,
    /// attempt 0.
    async fn reset(
        &self,
        topic: TopicId,
        service: ServiceId,
        group: &str,
        partition: Option<u32>,
        to: ReadFrom,
    ) -> Result<Vec<Cursor>>;
    /// Oldest first.
    async fn dead_list(&self, topic: TopicId, service: ServiceId, group: &str, limit: usize)
        -> Result<Vec<DeadRecord>>;
    async fn dead_count(&self, topic: TopicId, service: ServiceId, group: &str) -> Result<u64>;
    async fn dead_purge(&self, topic: TopicId, service: ServiceId, group: &str) -> Result<u64>;
    /// Retention by age and size, dedup expiry, dead-letter expiry and caps, incremental vacuum.
    async fn trim(&self, now_ms: i64) -> Result<TrimStats>;
}

/// `rusqlite`-backed [`StreamStore`].
pub struct SqliteStreamStore {
    db: Arc<Db>,
    /// Next round-robin partition per topic (unkeyed appends).
    rr: Arc<Mutex<HashMap<String, u32>>>,
}

impl SqliteStreamStore {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        Self::set_auto_vacuum(&conn)?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0))?;
        Self::init(conn, Some(path))
    }

    pub fn open_in_memory() -> Result<Arc<Self>> {
        let conn = Connection::open_in_memory()?;
        Self::set_auto_vacuum(&conn)?;
        Self::init(conn, None)
    }

    /// Make the next operations of this store fail as `faults` plans (tests).
    pub fn inject_faults(&self, faults: Arc<crate::faults::Faults>) {
        self.db.inject_faults(faults);
    }

    /// `auto_vacuum` only takes effect before the first table exists.
    fn set_auto_vacuum(conn: &Connection) -> Result<()> {
        let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 {
            conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
        }
        Ok(())
    }

    fn init(mut conn: Connection, file: Option<&Path>) -> Result<Arc<Self>> {
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.set_prepared_statement_cache_capacity(64);
        migrate(&mut conn, STREAM_MIGRATIONS)?;
        let readers = match file {
            Some(path) => (0..STREAM_READERS)
                .map(|_| open_reader(path, 64))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        Ok(Arc::new(Self {
            db: Db::new("rt-stream", conn, readers)?,
            rr: Arc::new(Mutex::new(HashMap::new())),
        }))
    }
}

// ---- helpers ---------------------------------------------------------------------------------

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn u(v: i64) -> u64 {
    v.max(0) as u64
}

fn i(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

const TOPIC_COLS: &str =
    "id, tenant_id, project_id, environment, name, partitions, retention_ms, max_bytes, bytes, dead_bytes, created_at";

fn topic_row(r: &Row<'_>) -> rusqlite::Result<Topic> {
    Ok(Topic {
        id: parse_col(&r.get::<_, String>(0)?, 0)?,
        ns: Namespace {
            tenant: parse_col(&r.get::<_, String>(1)?, 1)?,
            project: parse_col(&r.get::<_, String>(2)?, 2)?,
            environment: r.get(3)?,
        },
        name: r.get(4)?,
        partitions: r.get(5)?,
        retention_ms: u(r.get(6)?),
        max_bytes: u(r.get(7)?),
        bytes: u(r.get(8)?),
        dead_bytes: u(r.get(9)?),
        created_at_ms: r.get(10)?,
    })
}

fn load_topic(c: &Connection, id: &str) -> Result<Option<Topic>> {
    Ok(
        c.prepare_cached(&format!("SELECT {TOPIC_COLS} FROM topics WHERE id = ?1"))?
            .query_row([id], topic_row)
            .optional()?,
    )
}

fn need_topic(c: &Connection, id: &str) -> Result<Topic> {
    load_topic(c, id)?.ok_or(StoreError::NotFound)
}

fn check_partition(t: &Topic, p: u32) -> Result<()> {
    if p >= t.partitions {
        return Err(StoreError::Invalid(format!(
            "partition {p} is out of range: topic `{}` has {} partitions",
            t.name, t.partitions
        )));
    }
    Ok(())
}

fn head(c: &Connection, topic: &str, p: u32) -> Result<(u64, u64, i64)> {
    let (f, n, ts): (i64, i64, i64) = c
        .prepare_cached("SELECT first_off, next_off, last_ts FROM partitions WHERE topic_id = ?1 AND part = ?2")?
        .query_row(params![topic, p], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    Ok((u(f), u(n), ts))
}

/// The offset `from` names in partition `p` (clamped to the retained range).
fn resolve(c: &Connection, topic: &str, p: u32, from: ReadFrom) -> Result<u64> {
    let (first, next, _) = head(c, topic, p)?;
    Ok(match from {
        ReadFrom::Earliest => first,
        ReadFrom::Latest => next,
        ReadFrom::Offset(o) => o.clamp(first, next),
        ReadFrom::Time(t) => {
            let off: Option<i64> = c
                .prepare_cached("SELECT min(off) FROM records WHERE topic_id = ?1 AND part = ?2 AND ts >= ?3")?
                .query_row(params![topic, p, t], |r| r.get(0))?;
            off.map(u).unwrap_or(next)
        }
    })
}

fn headers_blob(h: &[(String, String)]) -> Result<Option<Vec<u8>>> {
    if h.is_empty() {
        return Ok(None);
    }
    serde_json::to_vec(h)
        .map(Some)
        .map_err(|e| StoreError::Internal(format!("json encode: {e}")))
}

fn headers_of(b: Option<Vec<u8>>, col: usize) -> rusqlite::Result<Vec<(String, String)>> {
    match b {
        None => Ok(Vec::new()),
        Some(b) => serde_json::from_slice(&b).map_err(|e| crate::sqlutil::bad_col(col, e)),
    }
}

fn record_size(key: Option<&str>, headers: Option<&[u8]>, body: &[u8]) -> u64 {
    (body.len() + headers.map_or(0, <[u8]>::len) + key.map_or(0, str::len)) as u64
}

fn validate_record(r: &NewRecord) -> Result<()> {
    if r.body.len() > EVENT_BODY_MAX_BYTES {
        return Err(StoreError::Invalid(format!(
            "record body is larger than {EVENT_BODY_MAX_BYTES} bytes"
        )));
    }
    if r.key.as_ref().is_some_and(|k| k.len() > STREAM_KEY_MAX_BYTES) {
        return Err(StoreError::Invalid(format!(
            "record key is longer than {STREAM_KEY_MAX_BYTES} bytes"
        )));
    }
    if let Some(d) = &r.dedup_id {
        if d.is_empty() || d.len() > DEDUP_ID_MAX_BYTES {
            return Err(StoreError::Invalid(format!(
                "dedup id must be 1..={DEDUP_ID_MAX_BYTES} bytes"
            )));
        }
    }
    validate_headers(&r.headers).map_err(StoreError::Invalid)
}

fn validate_group(g: &str) -> Result<()> {
    if !valid_group_name(g) {
        return Err(StoreError::Invalid("group must match [a-z0-9][a-z0-9_-]{0,62}".into()));
    }
    Ok(())
}

const CURSOR_COLS: &str = "topic_id, service_id, grp, part, committed, attempt, next_at, state, last_error, gen";

fn cursor_row(r: &Row<'_>) -> rusqlite::Result<Cursor> {
    Ok(Cursor {
        key: CursorKey {
            topic: parse_col(&r.get::<_, String>(0)?, 0)?,
            service: parse_col(&r.get::<_, String>(1)?, 1)?,
            group: r.get(2)?,
            partition: r.get(3)?,
        },
        committed: u(r.get(4)?),
        attempt: r.get(5)?,
        next_at_ms: r.get(6)?,
        blocked: r.get::<_, String>(7)? == "blocked",
        last_error: r.get(8)?,
        gen: u(r.get(9)?),
    })
}

fn load_cursor(c: &Connection, k: &CursorKey) -> Result<Option<Cursor>> {
    Ok(c.prepare_cached(&format!(
        "SELECT {CURSOR_COLS} FROM cursors WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3 AND part = ?4"
    ))?
    .query_row(
        params![k.topic.to_string(), k.service.to_string(), k.group, k.partition],
        cursor_row,
    )
    .optional()?)
}

fn cursors_where(c: &Connection, cond: &str, arg: String) -> Result<Vec<Cursor>> {
    let mut stmt = c.prepare_cached(&format!(
        "SELECT {CURSOR_COLS} FROM cursors WHERE {cond} ORDER BY topic_id, service_id, grp, part"
    ))?;
    let rows = stmt.query_map([arg], cursor_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Run a fenced single-row cursor update: `?1..?4` = the key, `?5` = gen, then `extra`.
fn fenced(c: &Connection, set: &str, k: &CursorKey, gen: u64, extra: &[&dyn rusqlite::ToSql]) -> Result<bool> {
    let sql = format!(
        "UPDATE cursors SET {set} WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3 AND part = ?4 AND gen = ?5"
    );
    let topic = k.topic.to_string();
    let service = k.service.to_string();
    let gen = i(gen);
    let mut args: Vec<&dyn rusqlite::ToSql> = vec![&topic, &service, &k.group, &k.partition, &gen];
    args.extend_from_slice(extra);
    Ok(c.prepare_cached(&sql)?.execute(args.as_slice())? > 0)
}

/// Delete dead letters by `cond` (params `args`), subtracting their size from `topics.dead_bytes`.
fn delete_dead(c: &Connection, cond: &str, args: &[&dyn rusqlite::ToSql]) -> Result<u64> {
    c.execute(
        &format!(
            "UPDATE topics SET dead_bytes = max(dead_bytes - coalesce((SELECT sum(size) FROM dead \
             WHERE dead.topic_id = topics.id AND ({cond})), 0), 0) \
             WHERE id IN (SELECT DISTINCT topic_id FROM dead WHERE {cond})"
        ),
        args,
    )?;
    Ok(c.execute(&format!("DELETE FROM dead WHERE {cond}"), args)? as u64)
}

/// Keep the newest [`DEAD_MAX_PER_CONSUMER`] dead letters of one consumer.
fn cap_dead(c: &Connection, topic: &str, service: &str, grp: &str) -> Result<u64> {
    let max = i(DEAD_MAX_PER_CONSUMER);
    // Composite primary key (WITHOUT ROWID): select the doomed keys explicitly.
    let cond = "topic_id = ?1 AND service_id = ?2 AND grp = ?3 AND (part, off) IN (\
                SELECT part, off FROM dead WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3 \
                ORDER BY failed_at DESC, part DESC, off DESC LIMIT -1 OFFSET ?4)";
    delete_dead(c, cond, &[&topic, &service, &grp, &max])
}

fn delete_topic_rows(c: &Connection, id: &str) -> Result<u64> {
    let mut n = 0u64;
    for table in ["records", "partitions", "dedup", "cursors", "dead"] {
        n += c.execute(&format!("DELETE FROM {table} WHERE topic_id = ?1"), [id])? as u64;
    }
    n += c.execute("DELETE FROM topics WHERE id = ?1", [id])? as u64;
    Ok(n)
}

fn append_tx(
    c: &Connection,
    rr: &Mutex<HashMap<String, u32>>,
    topic: &str,
    records: &[NewRecord],
    cap: u64,
) -> Result<Vec<Appended>> {
    let t = need_topic(c, topic)?;
    let tenant = t.ns.tenant.to_string();
    let used: i64 = c
        .prepare_cached("SELECT coalesce(sum(bytes + dead_bytes), 0) FROM topics WHERE tenant_id = ?1")?
        .query_row([&tenant], |r| r.get(0))?;
    let now = now_ms();
    let mut out = Vec::with_capacity(records.len());
    let mut added = 0u64;
    for r in records {
        if let Some(d) = &r.dedup_id {
            let hit: Option<(u32, i64)> = c
                .prepare_cached(
                    "SELECT part, off FROM dedup WHERE topic_id = ?1 AND dedup_id = ?2 AND expires_at > ?3",
                )?
                .query_row(params![topic, d, now], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            if let Some((partition, off)) = hit {
                out.push(Appended {
                    partition,
                    offset: u(off),
                    duplicate: true,
                });
                continue;
            }
        }
        let p = match &r.key {
            Some(k) => partition_for(k, t.partitions),
            None => {
                let mut map = rr.lock();
                let slot = map.entry(topic.to_string()).or_insert(0);
                let p = *slot % t.partitions;
                *slot = (p + 1) % t.partitions;
                p
            }
        };
        let headers = headers_blob(&r.headers)?;
        let size = record_size(r.key.as_deref(), headers.as_deref(), &r.body);
        added += size;
        if u(used).saturating_add(added) > cap {
            return Err(StoreError::Conflict("stream_full"));
        }
        let (_, next, last_ts) = head(c, topic, p)?;
        let ts = now.max(last_ts);
        c.prepare_cached(
            "INSERT INTO records (topic_id, part, off, ts, key, headers, body, size) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?
        .execute(params![topic, p, i(next), ts, r.key, headers, &r.body[..], i(size)])?;
        c.prepare_cached("UPDATE partitions SET next_off = ?3, last_ts = ?4 WHERE topic_id = ?1 AND part = ?2")?
            .execute(params![topic, p, i(next + 1), ts])?;
        if let Some(d) = &r.dedup_id {
            c.prepare_cached(
                "INSERT OR REPLACE INTO dedup (topic_id, dedup_id, part, off, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?
            .execute(params![
                topic,
                d,
                p,
                i(next),
                now.saturating_add(STREAM_DEDUP_WINDOW_MS)
            ])?;
        }
        out.push(Appended {
            partition: p,
            offset: next,
            duplicate: false,
        });
    }
    c.prepare_cached("UPDATE topics SET bytes = bytes + ?2 WHERE id = ?1")?
        .execute(params![topic, i(added)])?;
    Ok(out)
}

fn read_tx(
    c: &Connection,
    topic: &str,
    p: u32,
    from: ReadFrom,
    limit: usize,
    max_bytes: usize,
) -> Result<Vec<StreamRecord>> {
    let t = need_topic(c, topic)?;
    check_partition(&t, p)?;
    let start = resolve(c, topic, p, from)?;
    let limit = limit.clamp(1, READ_LIMIT_MAX);
    let mut stmt = c.prepare_cached(
        "SELECT off, ts, key, headers, body FROM records WHERE topic_id = ?1 AND part = ?2 AND off >= ?3 \
         ORDER BY off LIMIT ?4",
    )?;
    let rows = stmt.query_map(params![topic, p, i(start), limit as i64], |r| {
        Ok(StreamRecord {
            partition: p,
            offset: u(r.get(0)?),
            ts_ms: r.get(1)?,
            key: r.get(2)?,
            headers: headers_of(r.get(3)?, 3)?,
            body: Bytes::from(r.get::<_, Vec<u8>>(4)?),
        })
    })?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        let rec = row?;
        bytes += rec.body.len();
        if !out.is_empty() && bytes > max_bytes {
            break;
        }
        out.push(rec);
    }
    Ok(out)
}

fn cursor_tx(c: &Connection, k: &CursorKey, start: StartPosition) -> Result<Cursor> {
    if let Some(cur) = load_cursor(c, k)? {
        return Ok(cur);
    }
    let topic = k.topic.to_string();
    let t = need_topic(c, &topic)?;
    check_partition(&t, k.partition)?;
    validate_group(&k.group)?;
    let from = match start {
        StartPosition::Earliest => ReadFrom::Earliest,
        StartPosition::Latest => ReadFrom::Latest,
    };
    let at = resolve(c, &topic, k.partition, from)?;
    c.prepare_cached(
        "INSERT INTO cursors (topic_id, service_id, grp, part, committed, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?
    .execute(params![
        topic,
        k.service.to_string(),
        k.group,
        k.partition,
        i(at),
        now_ms()
    ])?;
    load_cursor(c, k)?.ok_or(StoreError::NotFound) // LCOV_EXCL_LINE: just inserted in this transaction
}

fn reset_tx(
    c: &Connection,
    topic: &str,
    service: &str,
    group: &str,
    partition: Option<u32>,
    to: ReadFrom,
) -> Result<Vec<Cursor>> {
    let t = need_topic(c, topic)?;
    validate_group(group)?;
    let parts: Vec<u32> = match partition {
        Some(p) => {
            check_partition(&t, p)?;
            vec![p]
        }
        None => (0..t.partitions).collect(),
    };
    let now = now_ms();
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        let at = i(resolve(c, topic, p, to)?);
        c.prepare_cached(
            "INSERT INTO cursors (topic_id, service_id, grp, part, committed, gen, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6) \
             ON CONFLICT (topic_id, service_id, grp, part) DO UPDATE SET committed = ?5, attempt = 0, next_at = 0, \
             state = 'active', last_error = NULL, gen = gen + 1, updated_at = ?6",
        )?
        .execute(params![topic, service, group, p, at, now])?;
        let key = CursorKey {
            topic: t.id,
            service: parse_col(service, 1)?,
            group: group.to_string(),
            partition: p,
        };
        out.extend(load_cursor(c, &key)?);
    }
    Ok(out)
}

fn trim_records_before(c: &Connection, topic: &str, p: u32, target: u64) -> Result<u64> {
    let (first, _, _) = head(c, topic, p)?;
    if target <= first {
        return Ok(0);
    }
    let cut = i(target.min(first + TRIM_CHUNK as u64));
    let (n, bytes): (i64, i64) = c
        .prepare_cached(
            "SELECT count(*), coalesce(sum(size), 0) FROM records WHERE topic_id = ?1 AND part = ?2 AND off < ?3",
        )?
        .query_row(params![topic, p, cut], |r| Ok((r.get(0)?, r.get(1)?)))?;
    c.prepare_cached("DELETE FROM records WHERE topic_id = ?1 AND part = ?2 AND off < ?3")?
        .execute(params![topic, p, cut])?;
    c.prepare_cached("UPDATE topics SET bytes = max(bytes - ?2, 0) WHERE id = ?1")?
        .execute(params![topic, bytes])?;
    c.prepare_cached("UPDATE partitions SET first_off = ?3 WHERE topic_id = ?1 AND part = ?2")?
        .execute(params![topic, p, cut])?;
    Ok(u(n))
}

fn move_cursors_up(c: &Connection) -> Result<u64> {
    Ok(c.execute(
        "UPDATE cursors SET committed = (SELECT first_off FROM partitions p \
         WHERE p.topic_id = cursors.topic_id AND p.part = cursors.part), attempt = 0 \
         WHERE committed < (SELECT first_off FROM partitions p WHERE p.topic_id = cursors.topic_id AND p.part = cursors.part)",
        [],
    )? as u64)
}

// ---- the store -------------------------------------------------------------------------------

#[async_trait]
impl StreamStore for SqliteStreamStore {
    async fn create_topic(&self, ns: &Namespace, spec: TopicSpec, max_topics: u32) -> Result<Topic> {
        if !valid_topic_name(&spec.name) {
            return Err(StoreError::Invalid(
                "topic name must match [a-z0-9][a-z0-9._-]{0,62} and not start with `tpc_`".into(),
            ));
        }
        if !(1..=PARTITIONS_HARD_MAX).contains(&spec.partitions) {
            return Err(StoreError::Invalid(format!(
                "partitions must be 1..={PARTITIONS_HARD_MAX}"
            )));
        }
        if spec.retention_ms == 0 || spec.retention_ms > i64::MAX as u64 {
            return Err(StoreError::Invalid("retention must be positive".into()));
        }
        let ns = ns.clone();
        self.db
            .write(move |c| {
                let tenant = ns.tenant.to_string();
                let n: i64 = c.query_row("SELECT count(*) FROM topics WHERE tenant_id = ?1", [&tenant], |r| {
                    r.get(0)
                })?;
                let taken: bool = c.query_row(
                    "SELECT EXISTS (SELECT 1 FROM topics WHERE tenant_id = ?1 AND project_id = ?2 \
                     AND environment = ?3 AND name = ?4)",
                    params![tenant, ns.project.to_string(), ns.environment, spec.name],
                    |r| r.get(0),
                )?;
                if taken {
                    return Err(StoreError::Conflict("topic_exists"));
                }
                if u(n) >= u64::from(max_topics) {
                    return Err(StoreError::Conflict("topic_limit"));
                }
                let id = TopicId::new().to_string();
                c.execute(
                    "INSERT INTO topics (id, tenant_id, project_id, environment, name, partitions, retention_ms, \
                     max_bytes, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        id,
                        tenant,
                        ns.project.to_string(),
                        ns.environment,
                        spec.name,
                        spec.partitions,
                        i(spec.retention_ms),
                        i(spec.max_bytes),
                        now_ms()
                    ],
                )?;
                for p in 0..spec.partitions {
                    c.execute(
                        "INSERT INTO partitions (topic_id, part) VALUES (?1, ?2)",
                        params![id, p],
                    )?;
                }
                need_topic(c, &id)
            })
            .await
    }

    async fn get_topic(&self, ns: &Namespace, name: &str) -> Result<Option<Topic>> {
        let ns = ns.clone();
        let name = name.to_string();
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {TOPIC_COLS} FROM topics WHERE tenant_id = ?1 AND project_id = ?2 \
                     AND environment = ?3 AND name = ?4"
                ))?
                .query_row(
                    params![ns.tenant.to_string(), ns.project.to_string(), ns.environment, name],
                    topic_row,
                )
                .optional()?)
            })
            .await
    }

    async fn topic_by_id(&self, id: TopicId) -> Result<Option<Topic>> {
        self.db.read(move |c| load_topic(c, &id.to_string())).await
    }

    async fn list_topics(
        &self,
        tenant: TenantId,
        project: Option<ProjectId>,
        environment: Option<&str>,
    ) -> Result<Vec<Topic>> {
        let project = project.map(|p| p.to_string());
        let env = environment.map(str::to_string);
        self.db
            .read(move |c| {
                let mut stmt = c.prepare_cached(&format!(
                    "SELECT {TOPIC_COLS} FROM topics WHERE tenant_id = ?1 AND (?2 IS NULL OR project_id = ?2) \
                     AND (?3 IS NULL OR environment = ?3) ORDER BY project_id, environment, name"
                ))?;
                let rows = stmt.query_map(params![tenant.to_string(), project, env], topic_row)?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn update_topic(&self, id: TopicId, retention_ms: Option<u64>, max_bytes: Option<u64>) -> Result<Topic> {
        if retention_ms.is_some_and(|r| r == 0 || r > i64::MAX as u64) {
            return Err(StoreError::Invalid("retention must be positive".into()));
        }
        self.db
            .write(move |c| {
                let id = id.to_string();
                need_topic(c, &id)?;
                c.execute(
                    "UPDATE topics SET retention_ms = coalesce(?2, retention_ms), max_bytes = coalesce(?3, max_bytes) \
                     WHERE id = ?1",
                    params![id, retention_ms.map(i), max_bytes.map(i)],
                )?;
                need_topic(c, &id)
            })
            .await
    }

    async fn delete_topic(&self, id: TopicId) -> Result<()> {
        let rr = self.rr.clone();
        self.db
            .write(move |c| {
                let id = id.to_string();
                need_topic(c, &id)?;
                delete_topic_rows(c, &id)?;
                rr.lock().remove(&id);
                Ok(())
            })
            .await
    }

    async fn purge_tenant(&self, tenant: TenantId) -> Result<u64> {
        let rr = self.rr.clone();
        self.db
            .write(move |c| {
                let ids: Vec<String> = {
                    let mut stmt = c.prepare("SELECT id FROM topics WHERE tenant_id = ?1")?;
                    let rows = stmt.query_map([tenant.to_string()], |r| r.get(0))?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                for id in &ids {
                    delete_topic_rows(c, id)?;
                    rr.lock().remove(id);
                }
                Ok(ids.len() as u64)
            })
            .await
    }

    async fn purge_service(&self, service: ServiceId) -> Result<u64> {
        self.db
            .write(move |c| {
                let s = service.to_string();
                let n = c.execute("DELETE FROM cursors WHERE service_id = ?1", [&s])? as u64;
                Ok(n + delete_dead(c, "service_id = ?1", &[&s])?)
            })
            .await
    }

    async fn count_topics_in(&self, tenant: TenantId, project: ProjectId, environment: Option<&str>) -> Result<u64> {
        let env = environment.map(str::to_string);
        self.db
            .read(move |c| {
                let n: i64 = c.query_row(
                    "SELECT count(*) FROM topics WHERE tenant_id = ?1 AND project_id = ?2 \
                     AND (?3 IS NULL OR environment = ?3)",
                    params![tenant.to_string(), project.to_string(), env],
                    |r| r.get(0),
                )?;
                Ok(u(n))
            })
            .await
    }

    async fn append(&self, topic: TopicId, records: Vec<NewRecord>, tenant_max_bytes: u64) -> Result<Vec<Appended>> {
        if records.len() > APPEND_BATCH_MAX {
            return Err(StoreError::Invalid(format!(
                "at most {APPEND_BATCH_MAX} records per append"
            )));
        }
        let total: usize = records.iter().map(|r| r.body.len()).sum();
        if total > APPEND_BATCH_MAX_BYTES {
            return Err(StoreError::Invalid(format!(
                "an append is larger than {APPEND_BATCH_MAX_BYTES} bytes"
            )));
        }
        for r in &records {
            validate_record(r)?;
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let rr = self.rr.clone();
        self.db
            .write(move |c| append_tx(c, &rr, &topic.to_string(), &records, tenant_max_bytes))
            .await
    }

    async fn heads(&self, topic: TopicId) -> Result<Vec<PartitionHead>> {
        self.db
            .read(move |c| {
                let id = topic.to_string();
                need_topic(c, &id)?;
                let mut stmt = c.prepare_cached(
                    "SELECT part, first_off, next_off FROM partitions WHERE topic_id = ?1 ORDER BY part",
                )?;
                let rows = stmt.query_map([id], |r| {
                    Ok(PartitionHead {
                        partition: r.get(0)?,
                        first: u(r.get(1)?),
                        next: u(r.get(2)?),
                    })
                })?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn read(
        &self,
        topic: TopicId,
        partition: u32,
        from: ReadFrom,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<StreamRecord>> {
        self.db
            .read(move |c| read_tx(c, &topic.to_string(), partition, from, limit, max_bytes))
            .await
    }

    async fn cursor(&self, key: &CursorKey, start: StartPosition) -> Result<Cursor> {
        let key = key.clone();
        self.db.write(move |c| cursor_tx(c, &key, start)).await
    }

    async fn cursors_of(&self, service: ServiceId) -> Result<Vec<Cursor>> {
        self.db
            .read(move |c| cursors_where(c, "service_id = ?1", service.to_string()))
            .await
    }

    async fn cursors_on(&self, topic: TopicId) -> Result<Vec<Cursor>> {
        self.db
            .read(move |c| cursors_where(c, "topic_id = ?1", topic.to_string()))
            .await
    }

    async fn commit(&self, key: &CursorKey, gen: u64, offset: u64) -> Result<bool> {
        let key = key.clone();
        self.db
            .write(move |c| {
                fenced(
                    c,
                    "committed = ?6, attempt = 0, next_at = 0, last_error = NULL, updated_at = ?7",
                    &key,
                    gen,
                    &[&i(offset), &now_ms()],
                )
            })
            .await
    }

    async fn fail(&self, key: &CursorKey, gen: u64, at: u64, delay: Duration, error: &str) -> Result<Option<u16>> {
        let key = key.clone();
        let error = error.to_string();
        self.db
            .write(move |c| {
                let now = now_ms();
                let ok = fenced(
                    c,
                    "attempt = CASE WHEN committed = ?6 THEN attempt + 1 ELSE 1 END, committed = ?6, \
                     next_at = ?7, last_error = ?8, updated_at = ?9",
                    &key,
                    gen,
                    &[&i(at), &now.saturating_add(ms(delay)), &error, &now],
                )?;
                if !ok {
                    return Ok(None);
                }
                Ok(load_cursor(c, &key)?.map(|cur| cur.attempt))
            })
            .await
    }

    async fn defer(&self, key: &CursorKey, gen: u64, delay: Duration) -> Result<bool> {
        let key = key.clone();
        self.db
            .write(move |c| {
                let now = now_ms();
                fenced(
                    c,
                    "next_at = ?6, updated_at = ?7",
                    &key,
                    gen,
                    &[&now.saturating_add(ms(delay)), &now],
                )
            })
            .await
    }

    async fn dead_letter(
        &self,
        key: &CursorKey,
        gen: u64,
        record: &StreamRecord,
        attempts: u16,
        error: &str,
        tenant_max_bytes: u64,
    ) -> Result<bool> {
        let key = key.clone();
        let record = record.clone();
        let error = error.to_string();
        self.db
            .write(move |c| {
                let now = now_ms();
                let ok = fenced(
                    c,
                    "committed = ?6, attempt = 0, next_at = 0, last_error = ?7, updated_at = ?8",
                    &key,
                    gen,
                    &[&i(record.offset + 1), &error, &now],
                )?;
                if !ok {
                    return Ok(false);
                }
                let topic = key.topic.to_string();
                let service = key.service.to_string();
                let headers = headers_blob(&record.headers)?;
                let full = record_size(record.key.as_deref(), headers.as_deref(), &record.body);
                let old: Option<i64> = c
                    .query_row(
                        "SELECT size FROM dead WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3 AND part = ?4 AND off = ?5",
                        params![topic, service, key.group, record.partition, i(record.offset)],
                        |r| r.get(0),
                    )
                    .optional()?;
                let tenant: String =
                    c.query_row("SELECT tenant_id FROM topics WHERE id = ?1", [&topic], |r| r.get(0))?;
                let used: i64 = c
                    .prepare_cached("SELECT coalesce(sum(bytes + dead_bytes), 0) FROM topics WHERE tenant_id = ?1")?
                    .query_row([&tenant], |r| r.get(0))?;
                // A replaced copy frees its old size first.
                let fits = u(used).saturating_sub(u(old.unwrap_or(0))).saturating_add(full) <= tenant_max_bytes;
                let (rkey, headers, body, size) = if fits {
                    (record.key.clone(), headers, record.body.clone(), i(full))
                } else {
                    tracing::warn!(partition = record.partition, offset = record.offset, "stream: dead letter stored without body (tenant stream cap)");
                    (None, None, Bytes::new(), 0)
                };
                c.execute(
                    "INSERT OR REPLACE INTO dead (topic_id, service_id, grp, part, off, ts, key, headers, body, size, \
                     attempts, error, failed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        topic,
                        service,
                        key.group,
                        record.partition,
                        i(record.offset),
                        record.ts_ms,
                        rkey,
                        headers,
                        &body[..],
                        size,
                        attempts,
                        error,
                        now
                    ],
                )?;
                c.execute(
                    "UPDATE topics SET dead_bytes = max(dead_bytes + ?2 - ?3, 0) WHERE id = ?1",
                    params![topic, size, old.unwrap_or(0)],
                )?;
                cap_dead(c, &topic, &service, &key.group)?;
                Ok(true)
            })
            .await
    }

    async fn block(&self, key: &CursorKey, gen: u64, error: &str) -> Result<bool> {
        let key = key.clone();
        let error = error.to_string();
        self.db
            .write(move |c| {
                fenced(
                    c,
                    "state = 'blocked', last_error = ?6, updated_at = ?7",
                    &key,
                    gen,
                    &[&error, &now_ms()],
                )
            })
            .await
    }

    async fn reset(
        &self,
        topic: TopicId,
        service: ServiceId,
        group: &str,
        partition: Option<u32>,
        to: ReadFrom,
    ) -> Result<Vec<Cursor>> {
        let group = group.to_string();
        self.db
            .write(move |c| reset_tx(c, &topic.to_string(), &service.to_string(), &group, partition, to))
            .await
    }

    async fn dead_list(
        &self,
        topic: TopicId,
        service: ServiceId,
        group: &str,
        limit: usize,
    ) -> Result<Vec<DeadRecord>> {
        let group = group.to_string();
        self.db
            .read(move |c| {
                let mut stmt = c.prepare_cached(
                    "SELECT part, off, ts, key, headers, body, attempts, error, failed_at FROM dead \
                     WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3 ORDER BY failed_at, part, off LIMIT ?4",
                )?;
                let rows = stmt.query_map(
                    params![
                        topic.to_string(),
                        service.to_string(),
                        group,
                        limit.clamp(1, 1000) as i64
                    ],
                    |r| {
                        Ok(DeadRecord {
                            record: StreamRecord {
                                partition: r.get(0)?,
                                offset: u(r.get(1)?),
                                ts_ms: r.get(2)?,
                                key: r.get(3)?,
                                headers: headers_of(r.get(4)?, 4)?,
                                body: Bytes::from(r.get::<_, Vec<u8>>(5)?),
                            },
                            attempts: r.get(6)?,
                            error: r.get(7)?,
                            failed_at_ms: r.get(8)?,
                        })
                    },
                )?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    async fn dead_count(&self, topic: TopicId, service: ServiceId, group: &str) -> Result<u64> {
        let group = group.to_string();
        self.db
            .read(move |c| {
                let n: i64 = c.query_row(
                    "SELECT count(*) FROM dead WHERE topic_id = ?1 AND service_id = ?2 AND grp = ?3",
                    params![topic.to_string(), service.to_string(), group],
                    |r| r.get(0),
                )?;
                Ok(u(n))
            })
            .await
    }

    async fn dead_purge(&self, topic: TopicId, service: ServiceId, group: &str) -> Result<u64> {
        let group = group.to_string();
        self.db
            .write(move |c| {
                delete_dead(
                    c,
                    "topic_id = ?1 AND service_id = ?2 AND grp = ?3",
                    &[&topic.to_string(), &service.to_string(), &group],
                )
            })
            .await
    }

    async fn trim(&self, now_ms: i64) -> Result<TrimStats> {
        let mut stats = TrimStats::default();
        let topics: Vec<Topic> = self
            .db
            .read(|c| {
                let mut stmt = c.prepare_cached(&format!("SELECT {TOPIC_COLS} FROM topics"))?;
                let rows = stmt.query_map([], topic_row)?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await?;
        // 1. age.
        for t in &topics {
            let cutoff = now_ms.saturating_sub(i(t.retention_ms));
            for p in 0..t.partitions {
                loop {
                    let id = t.id.to_string();
                    let n = self
                        .db
                        .write(move |c| {
                            let target = resolve(c, &id, p, ReadFrom::Time(cutoff))?;
                            trim_records_before(c, &id, p, target)
                        })
                        .await?;
                    stats.records += n;
                    if n < TRIM_CHUNK as u64 {
                        break;
                    }
                }
            }
        }
        // 2. size: drop the oldest records (by the partitions' first timestamps) until under the cap.
        for t in topics.iter().filter(|t| t.max_bytes > 0) {
            loop {
                let id = t.id.to_string();
                let n = self
                    .db
                    .write(move |c| {
                        let t = match load_topic(c, &id)? {
                            Some(t) if t.bytes > t.max_bytes => t,
                            _ => return Ok(0),
                        };
                        let oldest: Option<u32> = c
                            .query_row(
                                "SELECT part FROM records WHERE topic_id = ?1 ORDER BY ts, part, off LIMIT 1",
                                [&id],
                                |r| r.get(0),
                            )
                            .optional()?;
                        let Some(p) = oldest else { return Ok(0) };
                        let excess = i(t.bytes - t.max_bytes);
                        // Oldest record of the other partitions: stop before passing it.
                        let others: Option<i64> = c.query_row(
                            "SELECT min(ts) FROM records WHERE topic_id = ?1 AND part != ?2",
                            params![id, p],
                            |r| r.get(0),
                        )?;
                        // Smallest prefix of the partition covering the excess (at most a chunk).
                        let mut stmt = c.prepare_cached(
                            "SELECT off, size, ts FROM records WHERE topic_id = ?1 AND part = ?2 ORDER BY off LIMIT ?3",
                        )?;
                        let mut rows = stmt.query(params![id, p, TRIM_CHUNK])?;
                        let (mut acc, mut cut) = (0i64, 0i64);
                        while let Some(r) = rows.next()? {
                            let (off, size, ts): (i64, i64, i64) = (r.get(0)?, r.get(1)?, r.get(2)?);
                            if acc > 0 && others.is_some_and(|o| ts > o) {
                                break;
                            }
                            acc += size;
                            cut = off + 1;
                            if acc >= excess {
                                break;
                            }
                        }
                        drop(rows);
                        trim_records_before(c, &id, p, u(cut))
                    })
                    .await?;
                if n == 0 {
                    break;
                }
                stats.records += n;
            }
        }
        stats.cursors_moved = self.db.write(move_cursors_up).await?;
        // 3. dedup.
        loop {
            let n = self
                .db
                .write(move |c| {
                    Ok(c.execute(
                        "DELETE FROM dedup WHERE (topic_id, dedup_id) IN \
                         (SELECT topic_id, dedup_id FROM dedup WHERE expires_at <= ?1 LIMIT ?2)",
                        params![now_ms, TRIM_CHUNK],
                    )? as u64)
                })
                .await?;
            stats.dedup += n;
            if n < TRIM_CHUNK as u64 {
                break;
            }
        }
        // 4. dead letters: age, then the per-consumer cap.
        stats.dead += self
            .db
            .write(move |c| {
                let mut n = delete_dead(c, "failed_at < ?1", &[&now_ms.saturating_sub(DEAD_RETENTION_MS)])?;
                let consumers: Vec<(String, String, String)> = {
                    let mut stmt =
                        c.prepare("SELECT topic_id, service_id, grp FROM dead GROUP BY 1, 2, 3 HAVING count(*) > ?1")?;
                    let rows = stmt.query_map([i(DEAD_MAX_PER_CONSUMER)], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()?
                };
                for (t, s, g) in consumers {
                    n += cap_dead(c, &t, &s, &g)?; // LCOV_EXCL_LINE: dead_letter already caps on insert
                }
                Ok(n)
            })
            .await?;
        // 5. give freed pages back.
        self.db
            .call(|c| Ok(c.execute_batch("PRAGMA incremental_vacuum(2000)")?))
            .await?;
        Ok(stats)
    }
}
