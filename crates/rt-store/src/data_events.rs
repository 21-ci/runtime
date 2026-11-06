//! The data-change outbox of one `data.db` (R14): generated `AFTER` triggers write `_rt_changes` in
//! the guest's own transaction, so only committed changes exist, and the pump in `rt-triggers`
//! drains them into a stream topic. Everything here runs with the guest authorizer off.
//!
//! Names and values of the schema are data: an identifier is always quoted and a trigger's own name
//! holds the table's name in hex, so a hostile table name reaches SQL only inside quotes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rt_types::events::{DataEventOp, DataEventPayload, DataEventsConfig};
use rt_types::ServiceId;
use rusqlite::{params, Connection};

use crate::data::LocalServiceData;
use crate::Result;

const TRIGGER_PREFIX: &str = "_rt_ev_";
/// A `row` above this becomes keys-only.
pub const ROW_MAX_BYTES: usize = 64 * 1024;
/// A `key` above this becomes `[{"truncated": true, "prefix": ...}]`: with `row` it must fit a
/// stream record whatever a guest uses as a primary key.
pub const KEY_MAX_BYTES: usize = 64 * 1024;
const KEY_PREFIX_BYTES: usize = 1024;
/// `json_object` takes at most 127 arguments: a wider table is keys-only, with a warning.
const ROW_MAX_COLUMNS: usize = 60;

/// Whether `name` is a trigger this module generates (the authorizer's carve-out).
pub(crate) fn is_generated_trigger(name: &str) -> bool {
    name.get(..TRIGGER_PREFIX.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(TRIGGER_PREFIX))
}

/// Per-service flags set by each commit. Entries go with [`DirtyFlags::forget`].
#[derive(Default)]
pub(crate) struct DirtyFlags(Mutex<HashMap<ServiceId, Arc<AtomicBool>>>);

impl DirtyFlags {
    pub(crate) fn flag(&self, s: ServiceId) -> Arc<AtomicBool> {
        // A new flag starts set: the first look after an open is the crash recovery.
        self.0
            .lock()
            .entry(s)
            .or_insert_with(|| Arc::new(AtomicBool::new(true)))
            .clone()
    }

    /// A service with no flag has had no writer since the start or its purge: nothing to look at.
    /// The look makes no entry, so that the map holds only services with a writer.
    fn take(&self, s: ServiceId) -> bool {
        self.0.lock().get(&s).is_some_and(|f| f.swap(false, Ordering::Relaxed))
    }

    /// The service's data is gone.
    pub(crate) fn forget(&self, s: ServiceId) {
        self.0.lock().remove(&s);
    }
}

/// One writer turn at the start of a pump's look at a service.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassStart {
    pub report: SyncReport,
    pub backlog: u64,
    /// The oldest rows, as [`DataEventsStore::read_changes`] gives them.
    pub rows: Vec<ChangeRow>,
}

/// One row of `_rt_changes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeRow {
    pub seq: i64,
    pub ts: i64,
    pub src: String,
    pub op: String,
    pub key: String,
    pub row: Option<String>,
}

/// What a sync of the triggers found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Subscribed tables that do not exist (yet), or are too wide for `row`.
    pub warnings: Vec<String>,
    pub paused: bool,
}

/// What the pump and the API need of a service's database.
#[async_trait]
pub trait DataEventsStore: Send + Sync {
    /// Make the triggers match `cfg` and the current schema. A no-op while nothing changed.
    async fn sync_triggers(&self, s: ServiceId, cfg: &DataEventsConfig) -> Result<SyncReport>;
    /// `sync_triggers`, `backlog` and `read_changes` in one job on the service's writer, so that a
    /// pass does not interleave turns with the guest's commits.
    async fn open_pass(
        &self,
        s: ServiceId,
        cfg: &DataEventsConfig,
        limit: usize,
        max_bytes: usize,
    ) -> Result<PassStart>;
    /// No more events: triggers and pending rows go.
    async fn disable(&self, s: ServiceId) -> Result<()>;
    /// Whether a commit happened since the last call.
    fn take_dirty(&self, s: ServiceId) -> bool;
    /// The oldest rows, at most `limit` and about `max_bytes` (always at least one).
    async fn read_changes(&self, s: ServiceId, limit: usize, max_bytes: usize) -> Result<Vec<ChangeRow>>;
    /// Forget rows up to `seq`.
    async fn ack(&self, s: ServiceId, seq: i64) -> Result<()>;
    async fn backlog(&self, s: ServiceId) -> Result<u64>;
    /// Overflow: triggers go, the rows stay to drain, and `sync_triggers` makes none until `resume`.
    async fn pause(&self, s: ServiceId) -> Result<()>;
    async fn resume(&self, s: ServiceId) -> Result<()>;
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A column for JSON: `json_array` refuses a BLOB, and must not fail the guest's write.
fn json_safe(col: &str) -> String {
    format!("CASE typeof({col}) WHEN 'blob' THEN hex({col}) ELSE {col} END")
}

fn drop_triggers(c: &Connection) -> rusqlite::Result<()> {
    let names = c
        .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' AND substr(lower(name), 1, 7) = '_rt_ev_'")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for n in names {
        c.execute_batch(&format!("DROP TRIGGER {}", ident(&n)))?;
    }
    Ok(())
}

/// Remove the triggers, rows and pump state of a copy (branch, bundle, import). Old files may lack
/// the tables.
pub fn strip(c: &Connection) -> rusqlite::Result<()> {
    drop_triggers(c)?;
    let has: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name IN ('_rt_changes', '_rt_changes_state')",
        [],
        |r| r.get(0),
    )?;
    if has == 2 {
        c.execute_batch("DELETE FROM _rt_changes; DELETE FROM _rt_changes_state;")?;
    }
    Ok(())
}

fn op_parts(op: DataEventOp) -> (&'static str, &'static str, &'static str, &'static str) {
    match op {
        DataEventOp::Insert => ("INSERT", "insert", "i", "new"),
        DataEventOp::Update => ("UPDATE", "update", "u", "new"),
        DataEventOp::Delete => ("DELETE", "delete", "d", "old"),
    }
}

fn insert_body(src: &str, op: &str, key: &str, row: &str) -> String {
    format!(
        "INSERT INTO _rt_changes(ts,src,op,key,row) VALUES (CAST(unixepoch('subsec')*1000 AS INTEGER),{},{},{key},{row});",
        literal(src),
        literal(op)
    )
}

fn key_expr(parts: &str) -> String {
    format!(
        "(SELECT CASE WHEN length(j) <= {KEY_MAX_BYTES} THEN j ELSE \
         json_array(json_object('truncated', json('true'), 'prefix', substr(j, 1, {KEY_PREFIX_BYTES}))) END \
         FROM (SELECT json_array({parts}) AS j))"
    )
}

fn read_rows(c: &Connection, limit: usize, max_bytes: usize) -> rusqlite::Result<Vec<ChangeRow>> {
    let mut st = c.prepare_cached("SELECT seq, ts, src, op, key, row FROM _rt_changes ORDER BY seq LIMIT ?1")?;
    let mut rows = st.query([limit as i64])?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    while let Some(r) = rows.next()? {
        let c = ChangeRow {
            seq: r.get(0)?,
            ts: r.get(1)?,
            src: r.get(2)?,
            op: r.get(3)?,
            key: r.get(4)?,
            row: r.get(5)?,
        };
        bytes += c.key.len() + c.row.as_ref().map_or(0, String::len) + c.src.len() + 128;
        if bytes > max_bytes && !out.is_empty() {
            break;
        }
        out.push(c);
    }
    Ok(out)
}

/// Rows only come at the end and go from the front, so the ends of the index count them: no scan.
fn backlog_of(c: &Connection) -> rusqlite::Result<u64> {
    let n: i64 = c.query_row(
        "SELECT COALESCE((SELECT max(seq) FROM _rt_changes) - (SELECT min(seq) FROM _rt_changes) + 1, 0)",
        [],
        |r| r.get(0),
    )?;
    Ok(n.max(0) as u64)
}

fn row_expr(columns: &[(String, String)]) -> String {
    let inner: Vec<String> = columns
        .iter()
        .map(|(name, col)| format!("{},{}", literal(name), json_safe(col)))
        .collect();
    format!(
        "(SELECT CASE WHEN length(j) <= {ROW_MAX_BYTES} THEN j END FROM (SELECT json_object({}) AS j))",
        inner.join(",")
    )
}

/// `(name, pk position)` of a table's columns; empty when there is no such table.
fn columns_of(c: &Connection, table: &str) -> rusqlite::Result<Vec<(String, i64)>> {
    c.prepare("SELECT name, pk FROM pragma_table_info(?1)")?
        .query_map([table], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

/// A table (not a view) by its stored name, whatever case the config used.
fn stored_table(c: &Connection, name: &str) -> rusqlite::Result<Option<String>> {
    use rusqlite::OptionalExtension;
    c.query_row(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
        [name],
        |r| r.get(0),
    )
    .optional()
}

fn is_without_rowid(c: &Connection, table: &str) -> rusqlite::Result<bool> {
    c.query_row(
        "SELECT wr FROM pragma_table_list(?1) WHERE schema = 'main'",
        [table],
        |r| r.get::<_, i64>(0),
    )
    .map(|w| w != 0)
}

fn generate(c: &Connection, cfg: &DataEventsConfig, warnings: &mut Vec<String>) -> rusqlite::Result<()> {
    for t in &cfg.tables {
        let Some(table) = stored_table(c, &t.name)? else {
            warnings.push(format!("table `{}` does not exist", t.name));
            continue;
        };
        let cols = columns_of(c, &table)?;
        let mut pk: Vec<(i64, String)> = cols
            .iter()
            .filter(|(_, p)| *p > 0)
            .map(|(n, p)| (*p, n.clone()))
            .collect();
        pk.sort();
        let wants_row = t.payload == DataEventPayload::Row;
        if wants_row && cols.len() > ROW_MAX_COLUMNS {
            warnings.push(format!("table `{table}` has too many columns for `row`: keys only"));
        }
        let name_hex = hex(table.as_bytes());
        for op in &t.ops {
            let (when, op_name, suffix, side) = op_parts(*op);
            let key_parts: Vec<String> = if pk.is_empty() {
                if is_without_rowid(c, &table)? {
                    Vec::new() // LCOV_EXCL_LINE: a WITHOUT ROWID table always has a primary key
                } else {
                    vec![format!("{side}.rowid")]
                }
            } else {
                pk.iter()
                    .map(|(_, n)| json_safe(&format!("{side}.{}", ident(n))))
                    .collect()
            };
            let key = key_expr(&key_parts.join(","));
            let row = if wants_row && cols.len() <= ROW_MAX_COLUMNS {
                let columns: Vec<(String, String)> = cols
                    .iter()
                    .map(|(n, _)| (n.clone(), format!("{side}.{}", ident(n))))
                    .collect();
                row_expr(&columns)
            } else {
                "NULL".to_string()
            };
            c.execute_batch(&format!(
                "CREATE TRIGGER \"{TRIGGER_PREFIX}t{name_hex}_{suffix}\" AFTER {when} ON {} BEGIN {} END;",
                ident(&table),
                insert_body(&format!("sql:{table}"), op_name, &key, &row)
            ))?;
        }
    }
    for (i, p) in cfg.kv_prefixes.iter().enumerate() {
        let bytes = p.prefix.as_bytes();
        for op in [DataEventOp::Insert, DataEventOp::Update, DataEventOp::Delete] {
            let (when, op_name, suffix, side) = op_parts(op);
            let key = key_expr(&format!("hex({side}.key)"));
            // A value may be bytes: hex, under the same cap.
            let row = if p.payload == DataEventPayload::Row {
                format!(
                    "CASE WHEN length({side}.value) <= {} THEN json_object('value_hex', hex({side}.value)) END",
                    ROW_MAX_BYTES / 2 - 32
                )
            } else {
                "NULL".to_string()
            };
            c.execute_batch(&format!(
                "CREATE TRIGGER \"{TRIGGER_PREFIX}kv{i}_{suffix}\" AFTER {when} ON _rt_kv WHEN substr({side}.key,1,{}) = x'{}' BEGIN {} END;",
                bytes.len(),
                hex(bytes),
                insert_body("kv", op_name, &key, &row)
            ))?;
        }
    }
    Ok(())
}

fn schema_version(c: &Connection) -> rusqlite::Result<i64> {
    c.query_row("PRAGMA schema_version", [], |r| r.get(0))
}

/// Blocking, on the writer.
fn sync(c: &Connection, cfg: &DataEventsConfig) -> rusqlite::Result<SyncReport> {
    let (stored, paused): (Option<String>, bool) = c
        .query_row(
            "SELECT fingerprint, paused FROM _rt_changes_state WHERE id = 1",
            [],
            |r| Ok((Some(r.get(0)?), r.get::<_, i64>(1)? != 0)),
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok((None, false)),
            e => Err(e), // LCOV_EXCL_LINE: the bootstrap made this table
        })?;
    let config = serde_json::to_string(cfg).unwrap_or_default();
    let now = format!("{}|{config}", schema_version(c)?);
    let mut report = SyncReport {
        warnings: Vec::new(),
        paused,
    };
    if paused {
        return Ok(report);
    }
    if stored.as_deref() == Some(now.as_str()) {
        // Nothing moved; only say what is still missing.
        for t in &cfg.tables {
            if stored_table(c, &t.name)?.is_none() {
                report.warnings.push(format!("table `{}` does not exist", t.name));
            }
        }
        return Ok(report);
    }
    c.execute_batch("BEGIN IMMEDIATE")?;
    let done = (|| {
        drop_triggers(c)?;
        generate(c, cfg, &mut report.warnings)?;
        c.execute(
            "INSERT INTO _rt_changes_state (id, fingerprint) VALUES (1, '') \
             ON CONFLICT(id) DO UPDATE SET fingerprint = ''",
            [],
        )?;
        c.execute_batch("COMMIT")
    })();
    if done.is_err() && !c.is_autocommit() {
        let _ = c.execute_batch("ROLLBACK");
    }
    done?;
    // The trigger changes moved the version: remember the one they left behind.
    let settled = format!("{}|{config}", schema_version(c)?);
    c.execute("UPDATE _rt_changes_state SET fingerprint = ?1 WHERE id = 1", [settled])?;
    Ok(report)
}

#[async_trait]
impl DataEventsStore for LocalServiceData {
    async fn sync_triggers(&self, s: ServiceId, cfg: &DataEventsConfig) -> Result<SyncReport> {
        let cfg = cfg.clone();
        self.shared()
            .with(s, move |conn| sync(&conn.c, &cfg).map_err(Into::into))
            .await
    }

    async fn open_pass(
        &self,
        s: ServiceId,
        cfg: &DataEventsConfig,
        limit: usize,
        max_bytes: usize,
    ) -> Result<PassStart> {
        let cfg = cfg.clone();
        self.shared()
            .with(s, move |conn| {
                Ok(PassStart {
                    report: sync(&conn.c, &cfg)?,
                    backlog: backlog_of(&conn.c)?,
                    rows: read_rows(&conn.c, limit, max_bytes)?,
                })
            })
            .await
    }

    async fn disable(&self, s: ServiceId) -> Result<()> {
        self.shared().with(s, |conn| strip(&conn.c).map_err(Into::into)).await
    }

    fn take_dirty(&self, s: ServiceId) -> bool {
        self.shared().opts.dirty.take(s)
    }

    async fn read_changes(&self, s: ServiceId, limit: usize, max_bytes: usize) -> Result<Vec<ChangeRow>> {
        self.shared()
            .with(s, move |conn| Ok(read_rows(&conn.c, limit, max_bytes)?))
            .await
    }

    async fn ack(&self, s: ServiceId, seq: i64) -> Result<()> {
        self.shared()
            .with(s, move |conn| {
                conn.c
                    .prepare_cached("DELETE FROM _rt_changes WHERE seq <= ?1")?
                    .execute(params![seq])?;
                Ok(())
            })
            .await
    }

    async fn backlog(&self, s: ServiceId) -> Result<u64> {
        self.shared().with(s, |conn| Ok(backlog_of(&conn.c)?)).await
    }

    async fn pause(&self, s: ServiceId) -> Result<()> {
        self.shared()
            .with(s, |conn| {
                drop_triggers(&conn.c)?;
                conn.c.execute(
                    "INSERT INTO _rt_changes_state (id, fingerprint, paused) VALUES (1, '', 1) \
                     ON CONFLICT(id) DO UPDATE SET paused = 1",
                    [],
                )?;
                Ok(())
            })
            .await
    }

    async fn resume(&self, s: ServiceId) -> Result<()> {
        self.shared()
            .with(s, |conn| {
                conn.c.execute(
                    "UPDATE _rt_changes_state SET paused = 0, fingerprint = '' WHERE id = 1",
                    [],
                )?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataConfig, ServiceDataStore, SqlStatement};

    /// M12: the flags are a long-lived map: one per service with a writer, gone with its data, and
    /// a pump's look at a service makes none.
    #[tokio::test]
    async fn dirty_flags_go_with_the_service() {
        let dir = tempfile::tempdir().expect("dir");
        let store = LocalServiceData::open(DataConfig::new(dir.path())).expect("store");
        let flags = || store.shared().opts.dirty.0.lock().len();
        let s = ServiceId::new();
        store
            .sql(s)
            .execute(SqlStatement {
                sql: "CREATE TABLE t (x)".into(),
                args: Vec::new(),
            })
            .await
            .expect("write");
        assert_eq!(flags(), 1);
        assert!(!store.take_dirty(ServiceId::new()));
        assert_eq!(flags(), 1);
        store.purge(s).expect("purge");
        assert_eq!(flags(), 0);
    }
}
