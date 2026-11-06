//! The data browser's read path (R9, `GET|POST /v1/services/{s}/data/*`): a person or an assistant
//! reading a service's data from outside the guest, so nothing here may write and nothing may
//! reach beyond the service's own tables. The layers are independent:
//!
//! 1. Every job gets its own connection, opened `SQLITE_OPEN_READ_ONLY` plus `query_only`, never
//!    one of the pool. So it cannot fall through to the writer, and its statement cache cannot hold
//!    a statement another (looser) authorizer judged: that cache is keyed by SQL text alone.
//! 2. One statement, `sqlite3_stmt_readonly`, and it returns columns: `BEGIN` and many pragmas
//!    count as read-only, and return none.
//! 3. An allowlist authorizer ([`strict_auth`]), armed for the life of the connection, which also
//!    judges what the `pragma_*` table-valued functions prepare when they run.
//! 4. A statement budget and a [`ResultBudget`] over what is materialized.

use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use crate::data::{map_sql_err, sanitize, to_value, ResultBudget, SqlStatement, PROGRESS_OPS};
use crate::error::{Result, StoreError};
use crate::sqlutil::prefix_end;

/// Bounds of one browse query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BrowseLimits {
    pub rows: usize,
    pub bytes: usize,
    /// Bytes kept of one text cell.
    pub cell: usize,
    pub budget: Duration,
}

impl Default for BrowseLimits {
    fn default() -> Self {
        Self {
            rows: 100,
            bytes: 128 * 1024,
            cell: 4096,
            budget: BROWSE_BUDGET,
        }
    }
}

/// The statement budget of a browse query, and of each listing.
pub const BROWSE_BUDGET: Duration = Duration::from_secs(2);
/// What is kept of a blob cell.
pub const BLOB_HEAD_BYTES: usize = 64;
/// Tables listed by `data/tables`: a guest can create many.
pub const MAX_BROWSE_TABLES: i64 = 1000;

/// One cell of a browse result, already cut to its limits.
#[derive(Clone, Debug, PartialEq)]
pub enum BrowseCell {
    Null,
    Integer(i64),
    Real(f64),
    Text { text: String, size: u64, cut: bool },
    Blob { head: Vec<u8>, size: u64 },
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrowseResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<BrowseCell>>,
    /// More rows exist than fit in the limits.
    pub truncated: bool,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowseColumn {
    pub name: String,
    pub decl_type: String,
    pub pk: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowseTable {
    pub name: String,
    /// `table` or `view`.
    pub kind: String,
    pub columns: Vec<BrowseColumn>,
    /// Never from a scan: `sqlite_stat1`, else `MAX(rowid)`, else `None`.
    pub rows_estimate: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowseKvEntry {
    pub key: Vec<u8>,
    /// `0` = JSON text, `1` = bytes.
    pub tag: i64,
    pub size: u64,
    pub expires_at: Option<i64>,
    /// The value's first bytes.
    pub head: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrowseKvPage {
    pub items: Vec<BrowseKvEntry>,
    pub next_cursor: Option<Vec<u8>>,
}

/// The pragmas a browse may run, by themselves or as `pragma_<name>(…)`.
const ALLOWED_PRAGMAS: &[&str] = &[
    "table_info",
    "table_xinfo",
    "index_list",
    "index_info",
    "foreign_key_list",
];

/// Functions refused besides unknown ones: native code and pointers, file access where a build has
/// it, and the `sqlite_*` introspection family.
const DENIED_FUNCTIONS: &[&str] = &["load_extension", "fts3_tokenizer", "readfile", "writefile", "edit"];

fn has_prefix(name: &str, prefix: &str) -> bool {
    name.as_bytes()
        .get(..prefix.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(prefix.as_bytes()))
}

fn mentions_rt(name: &str) -> bool {
    name.as_bytes().windows(4).any(|w| w.eq_ignore_ascii_case(b"_rt_"))
}

fn allowed_pragma(name: &str) -> bool {
    ALLOWED_PRAGMAS.iter().any(|p| p.eq_ignore_ascii_case(name))
}

/// Whether a browse may read `table`: not the runtime's, not SQLite's own (`sqlite_schema` holds
/// the DDL of `_rt_*`, `sqlite_dbpage` the raw file), not `dbstat`, and only allowlisted `pragma_*`.
fn readable(table: &str) -> bool {
    if mentions_rt(table) || has_prefix(table, "sqlite_") || table.eq_ignore_ascii_case("dbstat") {
        return false;
    }
    match has_prefix(table, "pragma_") {
        true => allowed_pragma(&table["pragma_".len()..]),
        false => true,
    }
}

/// The browse authorizer: everything is denied unless named here.
fn strict_auth(ctx: &AuthContext<'_>) -> Authorization {
    let allow = match ctx.action {
        AuthAction::Select | AuthAction::Recursive => true,
        AuthAction::Read { table_name, .. } => readable(table_name),
        AuthAction::Function { function_name } => {
            !has_prefix(function_name, "sqlite_")
                && !DENIED_FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(function_name))
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => allowed_pragma(pragma_name) && !pragma_value.is_some_and(mentions_rt),
        _ => false,
    };
    if allow {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

/// Install what bounds a browse connection: the budget always, the authorizer for guest-written SQL.
pub(crate) fn guard(c: &Connection, strict: bool, budget: Duration) -> Result<()> {
    let deadline = Instant::now() + budget;
    c.progress_handler(PROGRESS_OPS, Some(move || Instant::now() >= deadline))?;
    c.pragma_update(None, "query_only", true)?;
    if strict {
        c.authorizer(Some(|ctx: AuthContext<'_>| strict_auth(&ctx)))?;
    }
    Ok(())
}

/// The flags of a browse connection: no `CREATE`, no URI parsing, read-only for the OS.
pub(crate) fn open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
}

pub(crate) fn open_file(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(path, open_flags())?)
}

/// A SQLite error of a browse query as a stable error. A denial of any layer reads the same.
fn map_err(e: rusqlite::Error) -> StoreError {
    if let rusqlite::Error::SqlInputError { error, msg, offset, .. } = &e {
        if !msg.starts_with("not authorized") && error.code != rusqlite::ErrorCode::AuthorizationForStatementDenied {
            return StoreError::SqlFailed {
                code: error.extended_code,
                offset: Some(*offset),
                message: sanitize(msg),
            };
        }
    }
    let (extended, message) = match &e {
        rusqlite::Error::SqliteFailure(f, m) => (f.extended_code, m.clone().unwrap_or_else(|| f.to_string())),
        other => (1, other.to_string()),
    };
    // A `pragma_*` function reports its inner denial as plain text.
    if message.contains("not authorized") || message.contains("authorization denied") {
        return StoreError::SqlNotReadonly;
    }
    match map_sql_err(e) {
        StoreError::SqlNotAuthorized => StoreError::SqlNotReadonly,
        StoreError::Sql(_) => StoreError::SqlFailed {
            code: extended,
            offset: None,
            message: sanitize(&message),
        },
        other => other,
    }
}

fn cell_of(v: rusqlite::types::ValueRef<'_>, limit: usize) -> BrowseCell {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => BrowseCell::Null,
        ValueRef::Integer(i) => BrowseCell::Integer(i),
        ValueRef::Real(r) => BrowseCell::Real(r),
        ValueRef::Text(t) => {
            let mut end = t.len().min(limit);
            // Never cut inside a UTF-8 sequence.
            while end > 0 && end < t.len() && t[end] & 0xC0 == 0x80 {
                end -= 1;
            }
            BrowseCell::Text {
                text: String::from_utf8_lossy(&t[..end]).into_owned(),
                size: t.len() as u64,
                cut: end < t.len(),
            }
        }
        ValueRef::Blob(b) => BrowseCell::Blob {
            head: b[..b.len().min(BLOB_HEAD_BYTES)].to_vec(),
            size: b.len() as u64,
        },
    }
}

/// What a cell costs against the byte budget: about what it takes in the response.
fn cell_cost(c: &BrowseCell) -> usize {
    16 + match c {
        BrowseCell::Null | BrowseCell::Integer(_) | BrowseCell::Real(_) => 8,
        BrowseCell::Text { text, .. } => text.len(),
        BrowseCell::Blob { head, .. } => head.len() * 2,
    }
}

/// Run one read-only statement on a browse connection. Runs out of bytes after some rows: those
/// rows and `truncated`; before the first row: [`StoreError::SqlResultTooLarge`].
pub(crate) fn query(c: &Connection, st: &SqlStatement, lim: BrowseLimits) -> Result<BrowseResult> {
    let started = Instant::now();
    let mut batch = rusqlite::Batch::new(c, &st.sql);
    let mut prep = batch.next().map_err(map_err)?.ok_or(StoreError::SqlNotReadonly)?;
    // Whatever follows is refused unseen, even if it would be allowed alone.
    if !matches!(batch.next(), Ok(None)) {
        return Err(StoreError::SqlNotReadonly);
    }
    if !prep.readonly() || prep.column_count() == 0 {
        return Err(StoreError::SqlNotReadonly);
    }
    let params = rusqlite::params_from_iter(st.args.iter().map(to_value));
    let columns: Vec<String> = prep.column_names().into_iter().map(str::to_string).collect();
    let mut budget = ResultBudget {
        rows: lim.rows,
        bytes_left: lim.bytes,
    };
    let mut out = Vec::new();
    let mut truncated = false;
    let mut rows = prep.query(params).map_err(map_err)?;
    while let Some(row) = rows.next().map_err(map_err)? {
        if out.len() >= budget.rows {
            truncated = true;
            break;
        }
        let n = row.as_ref().column_count();
        let mut cells = Vec::with_capacity(n);
        let mut cost = 0usize;
        for i in 0..n {
            let cell = cell_of(row.get_ref(i).map_err(map_err)?, lim.cell);
            cost = cost.saturating_add(cell_cost(&cell));
            cells.push(cell);
        }
        match budget.bytes_left.checked_sub(cost) {
            Some(left) => budget.bytes_left = left,
            None if out.is_empty() => return Err(StoreError::SqlResultTooLarge),
            None => {
                truncated = true;
                break;
            }
        }
        out.push(cells);
    }
    Ok(BrowseResult {
        columns,
        rows: out,
        truncated,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn columns_of(c: &Connection, table: &str) -> Vec<BrowseColumn> {
    // A view over a dropped table cannot be described; it still lists, without columns.
    let run = || -> rusqlite::Result<Vec<BrowseColumn>> {
        let mut q = c.prepare("SELECT name, type, pk, hidden FROM pragma_table_xinfo(?1)")?;
        let cols = q
            .query_map([table], |r| {
                Ok((
                    BrowseColumn {
                        name: r.get(0)?,
                        decl_type: r.get(1)?,
                        pk: r.get::<_, i64>(2)? > 0,
                    },
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(cols
            .into_iter()
            .filter(|(_, hidden)| *hidden != 1)
            .map(|(col, _)| col)
            .collect())
    };
    run().unwrap_or_default()
}

/// The first number of a `sqlite_stat1.stat` value is the table's row count.
fn stat1_estimate(c: &Connection, table: &str) -> Option<i64> {
    let stat: Option<String> = c
        .query_row(
            "SELECT stat FROM sqlite_stat1 WHERE tbl = ?1 ORDER BY idx IS NOT NULL LIMIT 1",
            [table],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten();
    stat?.split_whitespace().next()?.parse::<i64>().ok().filter(|n| *n >= 0)
}

/// Rows of `table`, without counting them: `COUNT(*)` is a full B-tree scan per page view.
/// `MAX(rowid)` reads one leaf, unless a column is itself called `rowid` and shadows it: that
/// could scan, so such a table has no estimate. So has `WITHOUT ROWID`, where it does not prepare.
fn estimate(
    c: &Connection,
    table: &str,
    columns: &[BrowseColumn],
    virtual_table: bool,
    has_stat1: bool,
) -> Option<i64> {
    if virtual_table {
        return None;
    }
    if has_stat1 {
        if let Some(n) = stat1_estimate(c, table) {
            return Some(n);
        }
    }
    if columns.iter().any(|col| col.name.eq_ignore_ascii_case("rowid")) {
        return None;
    }
    c.query_row(&format!("SELECT MAX(rowid) FROM {}", quote(table)), [], |r| {
        r.get::<_, Option<i64>>(0)
    })
    .ok()
    .map(|n| n.unwrap_or(0))
}

/// The service's tables and views; runs without the authorizer, so only fixed SQL, on a connection
/// that is read-only all the same.
pub(crate) fn tables(c: &Connection) -> Result<Vec<BrowseTable>> {
    let has_stat1: bool = c
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_stat1'",
            [],
            |_| Ok(true),
        )
        .optional()
        .map_err(map_err)?
        .unwrap_or(false);
    let mut q = c
        .prepare(
            "SELECT name, type, sql FROM sqlite_schema \
             WHERE type IN ('table', 'view') AND name NOT LIKE '\\_rt\\_%' ESCAPE '\\' \
               AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' \
             ORDER BY name LIMIT ?1",
        )
        .map_err(map_err)?;
    let rows = q
        .query_map([MAX_BROWSE_TABLES], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(map_err)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(map_err)?;
    Ok(rows
        .into_iter()
        .map(|(name, kind, sql)| {
            let columns = columns_of(c, &name);
            let is_virtual = sql.as_deref().is_some_and(|s| has_prefix(s, "create virtual"));
            let rows_estimate = (kind == "table")
                .then(|| estimate(c, &name, &columns, is_virtual, has_stat1))
                .flatten();
            BrowseTable {
                name,
                kind,
                columns,
                rows_estimate,
            }
        })
        .collect())
}

/// Live KV entries with `prefix`, after `cursor`, in key order. Reads the first bytes of each value
/// only, so a page of large values costs what a page of small ones does.
pub(crate) fn kv_list(
    c: &Connection,
    prefix: &[u8],
    cursor: Option<&[u8]>,
    limit: usize,
    head: usize,
    now_ms: i64,
) -> Result<BrowseKvPage> {
    let lower = match cursor {
        Some(k) if k >= prefix => k,
        _ => prefix,
    };
    let lower_op = if cursor.is_some_and(|k| k >= prefix) { ">" } else { ">=" };
    let upper = prefix_end(prefix);
    let sql = format!(
        "SELECT key, tag, length(CAST(value AS BLOB)), expires_at, substr(CAST(value AS BLOB), 1, ?1) FROM _rt_kv \
         WHERE key {lower_op} ?2 AND (?3 IS NULL OR key < ?3) AND (expires_at IS NULL OR expires_at > ?4) \
         ORDER BY key LIMIT ?5"
    );
    let mut q = c.prepare(&sql).map_err(map_err)?;
    let want = limit.saturating_add(1);
    let mut items = q
        .query_map(
            rusqlite::params![head as i64, lower, upper, now_ms, want as i64],
            kv_entry,
        )
        .map_err(map_err)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(map_err)?;
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        items.last().map(|e| e.key.clone())
    } else {
        None
    };
    Ok(BrowseKvPage { items, next_cursor })
}

fn kv_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<BrowseKvEntry> {
    Ok(BrowseKvEntry {
        key: r.get(0)?,
        tag: r.get(1)?,
        size: r.get::<_, i64>(2)?.max(0) as u64,
        expires_at: r.get(3)?,
        head: r.get::<_, Option<Vec<u8>>>(4)?.unwrap_or_default(),
    })
}

/// One live KV entry, `head` holding at most `max` bytes of its value.
pub(crate) fn kv_get(c: &Connection, key: &[u8], max: usize, now_ms: i64) -> Result<Option<BrowseKvEntry>> {
    c.query_row(
        "SELECT key, tag, length(CAST(value AS BLOB)), expires_at, substr(CAST(value AS BLOB), 1, ?1) FROM _rt_kv \
         WHERE key = ?2 AND (expires_at IS NULL OR expires_at > ?3)",
        rusqlite::params![max as i64, key, now_ms],
        kv_entry,
    )
    .optional()
    .map_err(map_err)
}
