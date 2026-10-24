//! Per-service data: KV, SQL and blobs.
//!
//! Each service has its own `data.db` (WAL, `synchronous=NORMAL`, `cache_size` from the
//! `storage.sql_cache_mb` budget, `temp_store=FILE`). Per database there is one **writer**
//! connection behind an async gate (writes, write batches, interactive transactions) and up to
//! `storage.readers_per_db` read-only **readers** (`execute` of a query, read batches, KV
//! `get`/`get_many`/`list`, blob `get`/`head`/`list`), which WAL lets proceed while the writer
//! is busy. Connections open lazily (readers only under concurrency, next to an open writer);
//! `storage.max_open_dbs` caps all of them together and idle services are closed whole, least
//! recently used first. Every job runs on the blocking pool with per-tenant fair queueing
//! (queueing lives in `rt-host`). Guest connections carry `sqlite3_limit`s
//! ([`GUEST_SQL_LIMITS`]) and results are capped (`max_result_rows` / `max_result_bytes`).
//! Errors carry stable codes only (`sql_error`, `sql_timeout`, `sql_not_authorized`,
//! `sql_result_too_large`, `storage_quota`) — never host paths.

use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use parking_lot::Mutex;
use rt_types::{Body, BodyError, ServiceId, TenantId};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use crate::blocking::{BlockingRunner, TokioBlocking};
use crate::error::{Result, StoreError};
use crate::sqlutil::{now_ms, prefix_end, unique_name};

pub const SERVICE_DATA_BOOTSTRAP: &str = include_str!("../migrations/service_data_0001_init.sql");

/// Guest-facing limits (plan/config driven).
pub const MAX_KV_KEY_BYTES: usize = 2 * 1024;
pub const MAX_KV_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_BLOB_KEY_BYTES: usize = 1024;
pub const MAX_KV_LIST_LIMIT: usize = 1000;
/// Default result caps of guest SQL (B3.4): rows per statement, value bytes per call.
pub const MAX_RESULT_ROWS: usize = 10_000;
pub const MAX_RESULT_BYTES: usize = 10 * 1024 * 1024;

// ---- KV --------------------------------------------------------------------------------------

/// How a KV value was written (`Uint8Array` vs JSON).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueTag {
    Json,
    Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvValue {
    pub tag: ValueTag,
    pub data: Bytes,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvSetOptions {
    /// `ttlSeconds`.
    pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvListOptions {
    pub prefix: Bytes,
    /// Opaque cursor from a previous page.
    pub cursor: Option<Bytes>,
    /// ≤ [`MAX_KV_LIST_LIMIT`].
    pub limit: usize,
    /// Descending key order: the page continues below `cursor`.
    pub reverse: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvPage {
    pub entries: Vec<(Bytes, KvValue)>,
    pub next_cursor: Option<Bytes>,
}

#[async_trait]
pub trait KvBackend: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<KvValue>>;
    async fn get_many(&self, keys: &[Bytes]) -> Result<Vec<Option<KvValue>>>;
    async fn set(&self, key: &[u8], value: KvValue, opts: KvSetOptions) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<bool>;
    async fn list(&self, opts: KvListOptions) -> Result<KvPage>;
    /// Atomically add `delta` to the integer counter at `key` and return the new value. A missing
    /// (or expired) key starts at 0 and gets `opts.ttl`; an existing counter keeps its expiry. The
    /// counter is stored as JSON text, so `get` reads it as a number. A value that is not a JSON
    /// integer in the `i64` range fails with [`StoreError::KvNotInteger`], a result outside it
    /// with [`StoreError::KvOverflow`]; either leaves the value unchanged.
    async fn incr(&self, key: &[u8], delta: i64, opts: KvSetOptions) -> Result<i64>;
    /// Atomic compare-and-swap in one writer step: succeeds when the current value equals
    /// `expected` (same bytes and type tag; `None` means absent or expired). `next` `None`
    /// deletes; `Some` is written with `opts.ttl`. Returns whether the swap happened.
    async fn cas(
        &self,
        key: &[u8],
        expected: Option<KvValue>,
        next: Option<KvValue>,
        opts: KvSetOptions,
    ) -> Result<bool>;
    /// Set (`Some`) or clear (`None`) the expiry of a live key; `false` when the key is absent.
    async fn expire(&self, key: &[u8], ttl: Option<Duration>) -> Result<bool>;
    /// Time left before a live key expires; `None` when the key is absent or has no expiry.
    async fn ttl(&self, key: &[u8]) -> Result<Option<Duration>>;
    /// Delete expired keys (swept every 60 s on the blocking pool); returns how many.
    async fn sweep_expired(&self) -> Result<u64>;
}

pub use crate::durable::{
    DurableBackend, DurableListOptions, DurableValue, DurableWrite, DURABLE_FORMAT_BYTES, DURABLE_FORMAT_V8,
};

// ---- SQL -------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlStatement {
    pub sql: String,
    pub args: Vec<SqlValue>,
}

/// `{columns, rows, rowsAffected, lastInsertRowid}` (@libsql/client-like).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: u64,
    pub last_insert_rowid: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchMode {
    Read,
    Write,
}

/// Interactive transaction (≤ 5 s).
#[async_trait]
pub trait SqlTx: Send {
    async fn execute(&mut self, stmt: SqlStatement) -> Result<SqlResult>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub trait SqlBackend: Send + Sync {
    async fn execute(&self, stmt: SqlStatement) -> Result<SqlResult>;
    /// All statements in one transaction.
    async fn batch(&self, stmts: Vec<SqlStatement>, mode: BatchMode) -> Result<Vec<SqlResult>>;
    async fn begin(&self) -> Result<Box<dyn SqlTx>>;
    /// Consistent SQLite snapshot of the whole database file (`GET …/data/export`, API-11).
    async fn export(&self) -> Result<Bytes>;
    /// Current database size in bytes (storage quota accounting).
    async fn size_bytes(&self) -> Result<u64>;
}

// ---- Blob ------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
}

/// A readable blob: metadata + streamed body.
#[derive(Debug)]
pub struct BlobObject {
    pub meta: BlobMeta,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobListOptions {
    pub prefix: String,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobPage {
    pub items: Vec<BlobMeta>,
    pub next_cursor: Option<String>,
}

/// Streamed in both directions (no full buffering). Local: `$RT_DATA/blobs/<service>/…`;
/// S3 (stretch): `{prefix}/blobs/<service>/<key>` with a disk read cache.
#[async_trait]
pub trait BlobBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BlobObject>>;
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;
    /// `max_bytes` bounds the object (plan; ≤ 100 MB); exceeding it fails with `StorageQuota`.
    async fn put(&self, key: &str, body: Body, content_type: Option<String>, max_bytes: u64) -> Result<BlobMeta>;
    async fn delete(&self, key: &str) -> Result<bool>;
    async fn list(&self, opts: BlobListOptions) -> Result<BlobPage>;
    /// Total bytes stored for the service (quota accounting).
    async fn total_bytes(&self) -> Result<u64>;
}

// ---- Per-service handle ----------------------------------------------------------------------

/// Access to one node's service data.
pub trait ServiceDataStore: Send + Sync {
    fn kv(&self, s: ServiceId) -> Arc<dyn KvBackend>;
    fn sql(&self, s: ServiceId) -> Arc<dyn SqlBackend>;
    fn blob(&self, s: ServiceId) -> Arc<dyn BlobBackend>;
    /// Durable object storage of the service (rows keyed by object in its `data.db`).
    fn durable(&self, s: ServiceId) -> Arc<dyn DurableBackend>;
    /// Remove all data of a service (a data reset: the service may write again and start over).
    fn purge(&self, s: ServiceId) -> Result<()>;
    /// Remove all data of a deleted service for good: a task of it still running cannot re-create
    /// its files, its writes are `not_found` and its reads see nothing (KV-024).
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    fn retire(&self, s: ServiceId) -> Result<()> {
        self.purge(s)
    }
    // LCOV_EXCL_STOP
    /// Synchronous, Machine-local SQL connections (`std:sqlite`); `None`: this store has none.
    // LCOV_EXCL_START: trait default body; the SQLite store overrides it, so only other implementors reach it
    fn local_sql(&self) -> Option<Arc<dyn crate::sync_sql::LocalSqlSource>> {
        None
    }
    // LCOV_EXCL_STOP
}

/// Settings of [`LocalServiceData`].
#[derive(Clone)]
pub struct DataConfig {
    /// `$RT_DATA`; databases under `services/<svc>/data.db`, blobs under `blobs/<svc>/`.
    pub root: PathBuf,
    /// `storage.max_open_dbs`: cap on open SQLite connections across all services, writers and
    /// readers alike. Idle services are closed whole, least recently used first.
    pub max_open_dbs: usize,
    /// `storage.readers_per_db`: read-only connections per service database next to its one
    /// writer (opened on demand, under concurrency). 0 = every operation uses the writer.
    pub readers_per_db: usize,
    /// `storage.sql_cache_mb` in bytes: SQLite page-cache budget of all connections together.
    /// Each connection gets `sql_cache_bytes / max_open_dbs` (`PRAGMA cache_size`); 0 = SQLite's
    /// default per connection.
    pub sql_cache_bytes: u64,
    /// Rows one guest statement may return before failing with `sql_result_too_large`.
    pub max_result_rows: usize,
    /// Bytes of values one guest call (statement, or read/write batch) may return before failing
    /// with `sql_result_too_large`.
    pub max_result_bytes: usize,
    /// `storage.sql_budget_ms`: progress-handler budget per statement.
    pub sql_budget: Duration,
    /// Progress-handler budget per statement of a Machine-local connection (`std:sqlite`): it runs
    /// on the Machine thread, so it is short. Default 100 ms.
    pub sql_sync_budget: Duration,
    /// `busy_timeout` of a Machine-local connection waiting for the pool's writer (or another
    /// local connection) before the statement fails with `sql_busy`. Default 20 ms.
    pub sql_sync_busy: Duration,
    /// Prepared statements one Machine-local connection keeps. Default 64.
    pub sql_sync_stmt_cache: usize,
    /// Node-wide `PRAGMA max_page_count` ceiling of every database in bytes (0 = none); the
    /// tenant-wide quota comes from [`ServiceDirectory::db_growth_bytes`] on top.
    pub max_db_bytes: u64,
    /// `PRAGMA mmap_size` per connection (derived from the `sql_cache` budget; 0 = off).
    pub mmap_bytes: u64,
    /// Where blocking SQLite/file work runs. Default: tokio's blocking pool; the node injects
    /// `rt-host`'s per-tenant fair pool. CPU used is charged to the calling task's meter
    /// ([`crate::blocking::with_cpu_meter`]).
    pub runner: Arc<dyn BlockingRunner>,
    /// Who owns each service (the runner's fair-queue lane) and how far its database may grow.
    /// `None`: every job runs in the node lane and only `max_db_bytes` bounds the databases.
    pub directory: Option<Arc<dyn ServiceDirectory>>,
}

/// Per-service facts the data store needs from the node (service table + tenant plans).
pub trait ServiceDirectory: Send + Sync {
    /// The tenant that owns `s`.
    fn tenant_of(&self, s: ServiceId) -> Option<TenantId>;
    /// How many more bytes `s`'s database may grow by: what is left of the owning tenant's
    /// storage quota across all its services (plan `storage_mb` minus everything the tenant
    /// stores). The database ceiling (`PRAGMA max_page_count`) becomes its current size plus
    /// this, set when the connection opens and again on [`LocalServiceData::refresh_limits`].
    /// `None`: the plan sets no limit. A service with no known owner must answer `Some(0)`.
    fn db_growth_bytes(&self, s: ServiceId) -> Option<u64>;
}

impl DataConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_open_dbs: 256,
            readers_per_db: 4,
            sql_cache_bytes: 512 * 1024 * 1024,
            max_result_rows: MAX_RESULT_ROWS,
            max_result_bytes: MAX_RESULT_BYTES,
            sql_budget: Duration::from_millis(1000),
            sql_sync_budget: Duration::from_millis(100),
            sql_sync_busy: Duration::from_millis(20),
            sql_sync_stmt_cache: 64,
            max_db_bytes: 0,
            mmap_bytes: 64 * 1024 * 1024,
            runner: Arc::new(TokioBlocking),
            directory: None,
        }
    }
}

impl std::fmt::Debug for DataConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataConfig")
            .field("root", &self.root)
            .field("max_open_dbs", &self.max_open_dbs)
            .field("readers_per_db", &self.readers_per_db)
            .field("sql_cache_bytes", &self.sql_cache_bytes)
            .field("max_result_rows", &self.max_result_rows)
            .field("max_result_bytes", &self.max_result_bytes)
            .field("sql_budget", &self.sql_budget)
            .field("sql_sync_budget", &self.sql_sync_budget)
            .field("sql_sync_busy", &self.sql_sync_busy)
            .field("sql_sync_stmt_cache", &self.sql_sync_stmt_cache)
            .field("max_db_bytes", &self.max_db_bytes)
            .field("mmap_bytes", &self.mmap_bytes)
            .finish_non_exhaustive()
    }
}

// ---- implementation --------------------------------------------------------------------------

/// Index of the local blob backend (object bytes live in files next to it).
const BLOB_BOOTSTRAP: &str = "CREATE TABLE IF NOT EXISTS _rt_blobs (\n  key BLOB PRIMARY KEY,\n  size INTEGER NOT NULL,\n  content_type TEXT,\n  file TEXT NOT NULL\n) WITHOUT ROWID;";

/// Interactive transactions are rolled back after this long (STO-07).
const TX_CAP: Duration = Duration::from_secs(5);
/// Streamed blob reads use chunks of this size.
const BLOB_CHUNK: usize = 256 * 1024;
/// The progress handler runs every this many SQLite VM instructions.
const PROGRESS_OPS: i32 = 1000;
/// Pages of WAL after which a commit checkpoints (SQLite's default, set explicitly).
const WAL_AUTOCHECKPOINT_PAGES: i64 = 1000;
/// The WAL file is truncated back to this size after a checkpoint (otherwise it keeps its peak).
const WAL_SIZE_LIMIT: i64 = 64 * 1024 * 1024;

/// `sqlite3_limit` values on every guest-facing connection (B3.4). SQLite's defaults are sized for
/// trusted code (1 GB strings, 1 GB statements); these bound what one statement can allocate or
/// how long it can take to compile, well above what applications need:
///
/// | limit | value | SQLite default |
/// |---|---|---|
/// | `LENGTH` (string, BLOB or row) | 16 MiB | 1,000,000,000 |
/// | `SQL_LENGTH` (statement text) | 1 MiB | 1,000,000,000 |
/// | `COLUMN` (table, index, result, `ORDER BY` columns) | 1,000 | 2,000 |
/// | `EXPR_DEPTH` (expression tree depth) | 500 | 1,000 |
/// | `COMPOUND_SELECT` (terms of a `UNION`/`EXCEPT` chain; multi-row `VALUES` are exempt) | 100 | 500 |
/// | `VARIABLE_NUMBER` (highest `?NNN`) | 10,000 | 32,766 |
/// | `LIKE_PATTERN_LENGTH` (`LIKE`/`GLOB` pattern bytes) | 1,000 | 50,000 |
pub const GUEST_SQL_LIMITS: &[(Limit, i32)] = &[
    (Limit::SQLITE_LIMIT_LENGTH, 16 * 1024 * 1024),
    (Limit::SQLITE_LIMIT_SQL_LENGTH, 1024 * 1024),
    (Limit::SQLITE_LIMIT_COLUMN, 1000),
    (Limit::SQLITE_LIMIT_EXPR_DEPTH, 500),
    (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 100),
    (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 10_000),
    (Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 1000),
];

/// Pragmas guests may not touch (they change durability, limits or file layout).
const DENIED_PRAGMAS: &[&str] = &[
    "writable_schema",
    "max_page_count",
    "journal_mode",
    "synchronous",
    "locking_mode",
    "mmap_size",
    "page_size",
    "auto_vacuum",
    "incremental_vacuum",
    "schema_version",
    "trusted_schema",
    "query_only",
    "busy_timeout",
    "wal_autocheckpoint",
    "wal_checkpoint",
    "journal_size_limit",
    "secure_delete",
    "cache_size",
    "temp_store",
    "temp_store_directory",
    "data_store_directory",
    "threads",
    "soft_heap_limit",
    "hard_heap_limit",
    "cell_size_check",
    "shrink_memory",
    "optimize",
    "key",
    "rekey",
];

/// SQL functions guests may not call: native code (`load_extension`) and native pointers
/// (`fts3_tokenizer` returns the address of a tokenizer module: an ASLR leak in a process every
/// tenant shares, SQL-002).
const DENIED_FUNCTIONS: &[&str] = &["load_extension", "fts3_tokenizer"];

fn is_reserved(name: &str) -> bool {
    name.as_bytes()
        .get(..4)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"_rt_"))
}

/// The authorizer applied to guest SQL.
///
/// Temp objects are denied: a temp table, index, view, trigger or virtual table lives on one
/// connection outside the database file, so neither `max_page_count`, the storage scan nor the
/// memory governor would see it, and it would survive across requests on the writer (repeated
/// `CREATE TEMP TABLE … AS <large query>` could exhaust the node). Use ordinary tables.
fn guest_auth(ctx: &AuthContext<'_>) -> Authorization {
    use AuthAction::*;
    let temp_db = ctx.database_name.is_some_and(|d| d.eq_ignore_ascii_case("temp"));
    let names: [Option<&str>; 2] = match ctx.action {
        Attach { .. } | Detach { .. } => return Authorization::Deny,
        // Transactions are managed by `batch` / `begin`.
        Transaction { .. } | Savepoint { .. } => return Authorization::Deny,
        CreateTempTable { .. } | CreateTempIndex { .. } | CreateTempTrigger { .. } | CreateTempView { .. } => {
            return Authorization::Deny
        }
        CreateTable { .. } | CreateIndex { .. } | CreateTrigger { .. } | CreateView { .. } | CreateVtable { .. }
            if temp_db => // LCOV_EXCL_LINE: SQLite reports a temp-schema CREATE TABLE with its own action code, which the CreateTempTable arm denies first
        {
            return Authorization::Deny
        }
        Function { function_name } => {
            return if DENIED_FUNCTIONS.iter().any(|f| function_name.eq_ignore_ascii_case(f)) {
                Authorization::Deny
            } else {
                Authorization::Allow
            };
        }
        Pragma { pragma_name, pragma_value } => {
            let denied = DENIED_PRAGMAS.iter().any(|p| p.eq_ignore_ascii_case(pragma_name));
            return if denied || pragma_value.is_some_and(is_reserved) { Authorization::Deny } else { Authorization::Allow };
        }
        Read { table_name, .. }
        | Insert { table_name }
        | Delete { table_name }
        | Update { table_name, .. }
        | CreateTable { table_name }
        | DropTable { table_name }
        | DropTempTable { table_name } // LCOV_EXCL_LINE: temp tables cannot exist (every temp CREATE is denied), so no DROP of one reaches the authorizer
        | AlterTable { table_name, .. }
        | Analyze { table_name } => [Some(table_name), None],
        CreateIndex { index_name, table_name }
        | DropIndex { index_name, table_name }
        | DropTempIndex { index_name, table_name } => [Some(index_name), Some(table_name)],
        CreateTrigger { trigger_name, table_name }
        | DropTrigger { trigger_name, table_name }
        | DropTempTrigger { trigger_name, table_name } => [Some(trigger_name), Some(table_name)],
        CreateView { view_name } | DropView { view_name } | DropTempView { view_name } => [Some(view_name), None],
        CreateVtable { table_name, .. } | DropVtable { table_name, .. } => [Some(table_name), None],
        Reindex { index_name } => [Some(index_name), None],
        _ => [None, None],
    };
    if names.into_iter().flatten().any(is_reserved) {
        Authorization::Deny
    } else {
        Authorization::Allow
    }
}

/// `sql` without leading whitespace, comments and empty statements (`;`).
fn skip_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start_matches(|c: char| c == ';' || c.is_whitespace());
    loop {
        if let Some(r) = rest.strip_prefix("--") {
            rest = r.split_once('\n').map(|(_, t)| t).unwrap_or("");
        } else if let Some(r) = rest.strip_prefix("/*") {
            rest = r.split_once("*/").map(|(_, t)| t).unwrap_or("");
        } else {
            return rest;
        }
        rest = rest.trim_start_matches(|c: char| c == ';' || c.is_whitespace());
    }
}

/// Functions that report the state of one connection (the last insert, the rows changed): only
/// the writer, which ran the guest's writes, can answer them.
const CONNECTION_STATE_FNS: &[&[u8]] = &[b"last_insert_rowid", b"changes"];

/// Whether `execute()` should try a reader first: the statement starts like a query and asks
/// nothing about connection state. Only a hint; the reader still checks `sqlite3_stmt_readonly`
/// and hands anything else to the writer.
fn looks_like_query(sql: &str) -> bool {
    let rest = skip_comments(sql).trim_start_matches(|c: char| c == '(' || c.is_whitespace());
    let query = ["select", "with", "values", "explain"].iter().any(|k| {
        rest.as_bytes()
            .get(..k.len())
            .is_some_and(|p| p.eq_ignore_ascii_case(k.as_bytes()))
    });
    query
        && !CONNECTION_STATE_FNS
            .iter()
            .any(|f| sql.as_bytes().windows(f.len()).any(|w| w.eq_ignore_ascii_case(f)))
}

/// Text-level guard for things the authorizer cannot see: `VACUUM` (writes files via `INTO`)
/// and `load_extension` (the function may not even exist in this build).
pub(crate) fn precheck(sql: &str) -> Result<()> {
    let rest = skip_comments(sql);
    let starts_vacuum = rest
        .as_bytes()
        .get(..6)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"vacuum"));
    // `load_extension(…)` calls are denied by the authorizer (`DENIED_FUNCTIONS`), wherever they
    // appear; a text scan here also refused string literals that merely mention it (SQL-006).
    // The authorizer sees only the old name of `ALTER TABLE … RENAME TO`: a rename into the reserved
    // `_rt_` namespace is caught here (SQL-004).
    let alter_reserved = rest
        .as_bytes()
        .get(..5)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"alter"))
        && mentions_reserved(sql);
    if starts_vacuum || alter_reserved {
        return Err(StoreError::SqlNotAuthorized);
    }
    Ok(())
}

fn sanitize(msg: &str) -> String {
    // SQLite messages do not normally carry paths; strip file names defensively.
    let mut out = msg.replace("data.db", "database");
    if let Some(i) = out.find('/') {
        out.truncate(i);
        out.push_str("...");
    }
    out
}

/// Map a SQLite error of guest SQL to a stable store error.
pub(crate) fn map_sql_err(e: rusqlite::Error) -> StoreError {
    use rusqlite::ErrorCode;
    // Prepare-time failures arrive as `SqlInputError`, run-time ones as `SqliteFailure`.
    let (code, msg) = match e {
        rusqlite::Error::SqliteFailure(f, msg) => (f.code, msg.unwrap_or_else(|| f.to_string())),
        rusqlite::Error::SqlInputError { error, msg, .. } => (error.code, msg),
        other => return StoreError::Sql(sanitize(&other.to_string())),
    };
    match code {
        ErrorCode::DiskFull => StoreError::StorageQuota,
        ErrorCode::OperationInterrupted => StoreError::SqlTimeout,
        ErrorCode::AuthorizationForStatementDenied => StoreError::SqlNotAuthorized,
        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => StoreError::SqlBusy,
        // A denied function is reported at prepare time as a plain error.
        _ if msg.starts_with("not authorized") => StoreError::SqlNotAuthorized,
        _ => StoreError::Sql(sanitize(&msg)),
    }
}

/// Map a SQLite error of an internal (KV / blob index) statement.
pub(crate) fn map_internal_err(e: rusqlite::Error) -> StoreError {
    use rusqlite::ErrorCode;
    let code = match &e {
        rusqlite::Error::SqliteFailure(f, _) => Some(f.code),
        rusqlite::Error::SqlInputError { error, .. } => Some(error.code),
        _ => None,
    };
    match code {
        Some(ErrorCode::DiskFull) => StoreError::StorageQuota,
        Some(ErrorCode::OperationInterrupted) => StoreError::SqlTimeout,
        _ => StoreError::Sqlite(e),
    }
}

fn to_value(v: &SqlValue) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Integer(*i),
        SqlValue::Real(r) => Value::Real(*r),
        SqlValue::Text(t) => Value::Text(t.clone()),
        SqlValue::Blob(b) => Value::Blob(b.clone()),
    }
}

fn from_ref(v: rusqlite::types::ValueRef<'_>) -> SqlValue {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(i) => SqlValue::Integer(i),
        ValueRef::Real(r) => SqlValue::Real(r),
        ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

fn is_insert(sql: &str) -> bool {
    let t = sql.trim_start();
    ["insert", "replace"].iter().any(|k| {
        t.as_bytes()
            .get(..k.len())
            .is_some_and(|p| p.eq_ignore_ascii_case(k.as_bytes()))
    })
}

const READ_ONLY_VIOLATION: &str = "write statements are not allowed in a read batch";

/// In-memory stand-in for the database of a purged service: same guardrails, nothing on disk.
fn scratch_conn(o: &OpenOpts) -> Result<Conn> {
    let c = Connection::open_in_memory()?;
    set_guest_limits(&c)?;
    Conn::guarded(c, o, ConnRole::Scratch)
}

/// What a guest call may still materialize (B3.4): rows per statement, value bytes per call.
pub(crate) struct ResultBudget {
    pub(crate) rows: usize,
    pub(crate) bytes_left: usize,
}

impl ResultBudget {
    pub(crate) fn of(conn: &Conn) -> Self {
        Self {
            rows: conn.max_rows,
            bytes_left: conn.max_bytes,
        }
    }
}

/// Memory one materialized value takes: the `SqlValue` itself plus its heap payload. A NULL is not
/// free: a result of many NULL columns costs `size_of::<SqlValue>()` per cell.
fn value_bytes(v: &SqlValue) -> usize {
    std::mem::size_of::<SqlValue>()
        + match v {
            SqlValue::Null | SqlValue::Integer(_) | SqlValue::Real(_) => 0,
            SqlValue::Text(t) => t.len(),
            SqlValue::Blob(b) => b.len(),
        }
}

/// Memory one materialized row takes besides its values (its `Vec`).
pub(crate) const ROW_OVERHEAD: usize = std::mem::size_of::<Vec<SqlValue>>();

/// A prepared guest statement: from the connection's statement cache, or a fresh one.
pub(crate) enum Prepared<'c> {
    Cached(rusqlite::CachedStatement<'c>),
    Fresh(rusqlite::Statement<'c>),
}

impl<'c> std::ops::Deref for Prepared<'c> {
    type Target = rusqlite::Statement<'c>;

    fn deref(&self) -> &Self::Target {
        match self {
            Prepared::Cached(s) => s,
            Prepared::Fresh(s) => s,
        }
    }
}

impl std::ops::DerefMut for Prepared<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Prepared::Cached(s) => s,
            Prepared::Fresh(s) => s,
        }
    }
}

/// Whether `sql` mentions a reserved `_rt_` name anywhere (identifier, string or comment).
fn mentions_reserved(sql: &str) -> bool {
    sql.as_bytes().windows(4).any(|w| w.eq_ignore_ascii_case(b"_rt_"))
}

/// Prepare guest SQL through the connection's statement cache (`tune`: 32 statements), so a repeated
/// statement is parsed and planned once per connection instead of on every call.
///
/// The authorizer judges a statement when it is *prepared*, and the cache is keyed by SQL text
/// alone: the KV and blob code keeps its own statements in the same cache of the same connection.
/// Every one of those mentions a reserved `_rt_` table, and nothing else does, so guest SQL that
/// mentions `_rt_` anywhere is always prepared fresh (and denied by the authorizer if it touches
/// one); every other text can only be found in the cache if a guest prepared it under the
/// authorizer. Callers run guest statements only with the authorizer armed (`Conn::with_guest`).
pub(crate) fn prepare_guest<'c>(c: &'c Connection, sql: &str) -> Result<Prepared<'c>> {
    prepare_guest_raw(c, sql).map_err(map_sql_err)
}

/// [`prepare_guest`] with SQLite's own error (its codes and the offset of a syntax error).
pub(crate) fn prepare_guest_raw<'c>(c: &'c Connection, sql: &str) -> rusqlite::Result<Prepared<'c>> {
    if mentions_reserved(sql) {
        c.prepare(sql).map(Prepared::Fresh)
    } else {
        c.prepare_cached(sql).map(Prepared::Cached)
    }
}

/// Run one guest statement on `c` (authorizer already armed by the caller).
fn exec_one(c: &Connection, st: &SqlStatement, read_only: bool, budget: &mut ResultBudget) -> Result<SqlResult> {
    precheck(&st.sql)?;
    let mut prep = prepare_guest(c, &st.sql)?;
    if read_only && !prep.readonly() {
        return Err(StoreError::Sql(READ_ONLY_VIOLATION.into()));
    }
    run_prepared(c, &mut prep, st, budget)
}

/// On a reader: run `st` when it is read-only; `None` when it writes and belongs on the writer.
/// Readers see the same committed schema as the writer and guests have no temp objects, so a
/// statement that does not prepare here fails the same way on the writer.
fn exec_on_reader(c: &Connection, st: &SqlStatement, budget: &mut ResultBudget) -> Result<Option<SqlResult>> {
    precheck(&st.sql)?;
    let mut prep = prepare_guest(c, &st.sql)?;
    if !prep.readonly() {
        return Ok(None);
    }
    run_prepared(c, &mut prep, st, budget).map(Some)
}

fn run_prepared(
    c: &Connection,
    prep: &mut rusqlite::Statement<'_>,
    st: &SqlStatement,
    budget: &mut ResultBudget,
) -> Result<SqlResult> {
    let params = rusqlite::params_from_iter(st.args.iter().map(to_value));
    let before = c.total_changes();
    let mut out_rows: Vec<Vec<SqlValue>> = Vec::new();
    if prep.column_count() > 0 {
        let mut rows = prep.query(params).map_err(map_sql_err)?;
        while let Some(row) = rows.next().map_err(map_sql_err)? {
            if out_rows.len() >= budget.rows {
                return Err(StoreError::SqlResultTooLarge);
            }
            budget.bytes_left = budget
                .bytes_left
                .checked_sub(ROW_OVERHEAD)
                .ok_or(StoreError::SqlResultTooLarge)?;
            // The columns as the statement runs: another connection may have changed the schema
            // since it was prepared (SQLite re-prepares on its first step), so the count taken
            // before the step can be stale.
            let ncols = row.as_ref().column_count();
            let mut vals = Vec::with_capacity(ncols);
            for i in 0..ncols {
                let v = from_ref(row.get_ref(i).map_err(map_sql_err)?);
                budget.bytes_left = budget
                    .bytes_left
                    .checked_sub(value_bytes(&v))
                    .ok_or(StoreError::SqlResultTooLarge)?;
                vals.push(v);
            }
            out_rows.push(vals);
        }
    } else {
        prep.execute(params).map_err(map_sql_err)?;
    }
    let columns: Vec<String> = prep.column_names().into_iter().map(str::to_string).collect();
    let affected = if c.total_changes() > before { c.changes() } else { 0 };
    Ok(SqlResult {
        columns,
        rows: out_rows,
        rows_affected: affected,
        last_insert_rowid: (affected > 0 && is_insert(&st.sql)).then(|| c.last_insert_rowid()),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnRole {
    /// The one read-write connection of a service database.
    Writer,
    /// A read-write connection owned by one Machine thread (`std:sqlite`, `sync_sql.rs`): runs
    /// guest SQL inline with a short budget and a short busy timeout.
    Local,
    /// A read-only connection of the reader pool.
    Reader,
    /// In-memory stand-in for a purged service.
    Scratch,
}

/// One open `data.db` connection plus the knobs the authorizer / progress handler read.
pub(crate) struct Conn {
    pub(crate) c: Connection,
    guest: Arc<AtomicBool>,
    deadline: Arc<AtomicU64>,
    base: Instant,
    budget: Duration,
    pub(crate) role: ConnRole,
    pub(crate) max_rows: usize,
    pub(crate) max_bytes: usize,
}

impl Drop for Conn {
    fn drop(&mut self) {
        // Let SQLite refresh the planner statistics it found missing or stale while this
        // connection ran (bounded work since SQLite 3.46).
        if self.role == ConnRole::Writer {
            self.guest.store(false, Ordering::Relaxed);
            self.deadline.store(0, Ordering::Relaxed);
            let _ = self.c.execute_batch("PRAGMA optimize");
        }
    }
}

impl Conn {
    /// Wrap `c` with the guest authorizer and the statement-budget progress handler.
    fn guarded(c: Connection, o: &OpenOpts, role: ConnRole) -> Result<Self> {
        let guest = Arc::new(AtomicBool::new(false));
        let deadline = Arc::new(AtomicU64::new(0));
        let base = Instant::now();
        install_guards(&c, &guest, &deadline, base)?;
        Ok(Self {
            c,
            guest,
            deadline,
            base,
            budget: if role == ConnRole::Local {
                o.sync_budget
            } else {
                o.budget
            },
            role,
            max_rows: o.max_result_rows,
            max_bytes: o.max_result_bytes,
        })
    }

    /// Start the statement budget.
    pub(crate) fn arm(&self) {
        let ns = (self.base.elapsed() + self.budget).as_nanos() as u64;
        self.deadline.store(ns.max(1), Ordering::Relaxed);
    }

    pub(crate) fn disarm(&self) {
        self.deadline.store(0, Ordering::Relaxed);
    }

    /// Give back `spent` of the running statement's budget: time the caller used outside SQLite
    /// (building a result from a row) is not the statement's.
    pub(crate) fn credit(&self, spent: Duration) {
        let _ = self.deadline.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |d| {
            (d != 0).then(|| d.saturating_add(u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX)))
        });
    }

    /// Run guest SQL with the authorizer enforcing and a fresh statement budget.
    pub(crate) fn with_guest<R>(&self, f: impl FnOnce(&Connection) -> R) -> R {
        self.guest.store(true, Ordering::Relaxed);
        self.arm();
        let r = f(&self.c);
        self.guest.store(false, Ordering::Relaxed);
        r
    }
}

#[derive(Clone)]
pub(crate) struct OpenOpts {
    root: PathBuf,
    budget: Duration,
    pub(crate) sync_budget: Duration,
    sync_busy: Duration,
    sync_stmt_cache: usize,
    pub(crate) max_db_bytes: u64,
    mmap_bytes: u64,
    /// `PRAGMA cache_size` of each connection, in KiB (0 = SQLite's default).
    cache_kib: i64,
    readers_per_db: usize,
    max_result_rows: usize,
    max_result_bytes: usize,
}

fn service_dir(root: &Path, s: ServiceId) -> PathBuf {
    root.join("services").join(s.to_string())
}

fn blob_dir(root: &Path, s: ServiceId) -> PathBuf {
    root.join("blobs").join(s.to_string())
}

fn private_dirs(path: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)
}

/// Install the guest authorizer and the statement-budget progress handler.
fn install_guards(c: &Connection, guest: &Arc<AtomicBool>, deadline: &Arc<AtomicU64>, base: Instant) -> Result<()> {
    {
        let guest = guest.clone();
        c.authorizer(Some(move |ctx: AuthContext<'_>| {
            if guest.load(Ordering::Relaxed) {
                guest_auth(&ctx)
            } else {
                Authorization::Allow
            }
        }))?;
    }
    {
        let deadline = deadline.clone();
        c.progress_handler(
            PROGRESS_OPS,
            Some(move || {
                let d = deadline.load(Ordering::Relaxed);
                d != 0 && base.elapsed().as_nanos() as u64 >= d
            }),
        )?; // LCOV_EXCL_LINE: configuring a fresh connection's progress handler does not fail
    }
    Ok(())
}

pub(crate) fn set_guest_limits(c: &Connection) -> Result<()> {
    for (limit, value) in GUEST_SQL_LIMITS {
        c.set_limit(*limit, *value)?;
    }
    Ok(())
}

/// Settings every `data.db` connection gets, writer and readers alike. `temp_store = FILE`: a
/// guest's sort or materialized subquery works within the page cache and spills the rest to a
/// temp file that is gone when the statement ends (bounded by the statement budget), instead of
/// growing without bound in memory.
fn tune(c: &Connection, o: &OpenOpts) -> Result<()> {
    c.busy_timeout(Duration::from_millis(5000))?;
    if o.mmap_bytes > 0 {
        c.query_row(&format!("PRAGMA mmap_size={}", o.mmap_bytes), [], |r| {
            r.get::<_, i64>(0)
        })?;
    }
    if o.cache_kib > 0 {
        c.pragma_update(None, "cache_size", -o.cache_kib)?;
    }
    c.pragma_update(None, "temp_store", "FILE")?;
    c.set_prepared_statement_cache_capacity(32);
    set_guest_limits(c)
}

/// What bounds one database's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DbLimit {
    /// Node-wide ceiling in bytes (0 = none).
    pub(crate) node_max: u64,
    /// Bytes the database may still grow by: its tenant's remaining quota (`None` = no plan limit).
    pub(crate) growth: Option<u64>,
}

/// SQLite's own `max_page_count` maximum (no ceiling).
const NO_PAGE_LIMIT: u64 = 0xFFFF_FFFE;

/// Set the writer's `max_page_count` from `limit`: the node-wide cap and/or the current size plus
/// the tenant's remaining quota, whichever is lower. SQLite never lowers it below the pages in
/// use, so an over-quota database keeps serving reads and deletes but cannot grow.
pub(crate) fn apply_ceiling(c: &Connection, limit: DbLimit) -> rusqlite::Result<()> {
    let page_size = c.query_row("PRAGMA page_size", [], |r| r.get::<_, u64>(0))?.max(1);
    let mut ceiling = (limit.node_max > 0).then_some(limit.node_max);
    if let Some(growth) = limit.growth {
        let pages_now: u64 = c.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let by_quota = pages_now.saturating_mul(page_size).saturating_add(growth);
        ceiling = Some(ceiling.map_or(by_quota, |n| n.min(by_quota)));
    }
    let pages = ceiling.map_or(NO_PAGE_LIMIT, |b| (b / page_size).clamp(1, NO_PAGE_LIMIT));
    c.query_row(&format!("PRAGMA max_page_count={pages}"), [], |r| r.get::<_, i64>(0))?;
    Ok(())
}

/// The writer: creates the file, switches it to WAL, bootstraps the internal tables, then sets
/// the size ceiling (so the bootstrap of a new database never counts against a frozen quota).
fn open_conn(o: &OpenOpts, s: ServiceId, limit: DbLimit) -> Result<Conn> {
    open_rw(o, s, limit, ConnRole::Writer)
}

/// How long a Machine-local connection waits for another connection while it opens (creating the
/// file, switching to WAL, bootstrapping): rare, and once per connection.
const LOCAL_OPEN_BUSY: Duration = Duration::from_millis(250);

/// A read-write connection with the writer's setup, for `role` `Writer` or `Local`.
pub(crate) fn open_rw(o: &OpenOpts, s: ServiceId, limit: DbLimit, role: ConnRole) -> Result<Conn> {
    let dir = service_dir(&o.root, s);
    private_dirs(&dir)?;
    let c = Connection::open(dir.join("data.db"))?;
    tune(&c, o)?;
    if role == ConnRole::Local {
        c.busy_timeout(LOCAL_OPEN_BUSY)?;
    }
    c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0))?;
    c.pragma_update(None, "synchronous", "NORMAL")?;
    c.query_row(
        &format!("PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES}"),
        [],
        |r| r.get::<_, i64>(0),
    )?;
    c.query_row(&format!("PRAGMA journal_size_limit={WAL_SIZE_LIMIT}"), [], |r| {
        r.get::<_, i64>(0)
    })?;
    c.execute_batch(SERVICE_DATA_BOOTSTRAP)?;
    c.execute_batch(BLOB_BOOTSTRAP)?;
    apply_ceiling(&c, limit)?;
    if role == ConnRole::Local {
        // From here on a Machine thread never waits long for a lock held by the pool's writer.
        c.busy_timeout(o.sync_busy)?;
        c.set_prepared_statement_cache_capacity(o.sync_stmt_cache);
    }
    Conn::guarded(c, o, role)
}

/// A reader: read-only (`SQLITE_OPEN_READ_ONLY` plus `query_only`), same guards and limits as the
/// writer. Only opened while the service's writer is open, so the WAL and shared-memory files exist.
fn open_reader_conn(o: &OpenOpts, s: ServiceId) -> Result<Conn> {
    let c = Connection::open_with_flags(
        service_dir(&o.root, s).join("data.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )?;
    tune(&c, o)?;
    c.pragma_update(None, "query_only", true)?;
    Conn::guarded(c, o, ConnRole::Reader)
}

/// A Machine-local read-write connection of a service database (`std:sqlite`, `sync_sql.rs`):
/// counted in the pool's connection slots, closed with the service (eviction, purge). The Machine
/// thread that owns it runs guest SQL on it inline.
pub(crate) struct LocalSlot {
    pub(crate) conn: Mutex<Option<Conn>>,
    /// The pool deregistered this slot (its service was evicted or purged) since the owner last
    /// claimed it: the owner closes the connection when its statement ends, or does not install a
    /// connection it opened meanwhile. Nothing here ever blocks on the owner's statement.
    pub(crate) doomed: AtomicBool,
    /// Nesting depth of the transaction begun through the slot (0 = none; 1 = `BEGIN`, deeper =
    /// savepoints). A transaction still open when the task that began it is over is rolled back
    /// by the owner (`LocalDb::reset_open_tx`).
    pub(crate) tx_depth: AtomicU32,
    /// The transaction begun through the slot was lost while open (the pool closed the
    /// connection under it): its changes are gone. Sticky until the next `begin`, so a later
    /// `commit` reports the loss instead of succeeding on an empty transaction.
    pub(crate) tx_lost: AtomicBool,
}

impl LocalSlot {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            conn: Mutex::new(None),
            doomed: AtomicBool::new(false),
            tx_depth: AtomicU32::new(0),
            tx_lost: AtomicBool::new(false),
        })
    }
}

/// The connections of one service database: one writer behind `gate` and a pool of readers.
pub(crate) struct ServiceConn {
    /// Serialises users of the writer; an interactive transaction holds it for its life.
    gate: Arc<tokio::sync::Mutex<()>>,
    /// The writer connection (opened lazily by the first user of the gate).
    conn: Mutex<Option<Conn>>,
    /// The writer connection is open. Readers only open next to it: the writer created the file
    /// and keeps its WAL and shared-memory files in place.
    writer_open: AtomicBool,
    /// One permit per reader connection (`readers_per_db`): bounds concurrent readers. Holding all
    /// of them means no reader is in use (eviction).
    reader_permits: Arc<Semaphore>,
    /// Open reader connections not in use.
    idle_readers: Mutex<Vec<Conn>>,
    /// Bumped (under the `idle_readers` lock) whenever the service's connections are closed; a
    /// reader checked out before that is closed on return instead of pooled.
    epoch: AtomicU64,
    /// LRU stamp (global clock value at the last release).
    pub(crate) tick: AtomicU64,
    /// Coalescable writes waiting for the writer (group commit).
    queue: Mutex<VecDeque<Queued>>,
    /// Machine-local connections (`std:sqlite`) registered with the pool.
    pub(crate) locals: Mutex<Vec<Arc<LocalSlot>>>,
}

/// Connection slots one service holds (counted in [`PoolState::conns`]).
struct Slots {
    entry: Arc<ServiceConn>,
    writer: bool,
    readers: usize,
    /// Machine-local connections (`std:sqlite`).
    locals: usize,
}

impl Slots {
    fn total(&self) -> usize {
        usize::from(self.writer) + self.readers + self.locals
    }
}

#[derive(Default)]
pub(crate) struct PoolState {
    entries: HashMap<ServiceId, Arc<ServiceConn>>,
    /// Services holding (or about to open) connections: the LRU eviction candidates.
    open: HashMap<ServiceId, Slots>,
    /// Connection slots in use, writers and readers, including connections still being closed;
    /// never larger than `max_open_dbs`.
    conns: usize,
}

pub(crate) struct Shared {
    cfg: DataConfig,
    pub(crate) opts: OpenOpts,
    state: Mutex<PoolState>,
    freed: tokio::sync::Notify,
    pub(crate) clock: AtomicU64,
    /// Services purged in this process whose storage has not been written to since. Reads of
    /// such a service see "nothing" without re-creating any file; the first write revives it.
    purged: Mutex<HashSet<ServiceId>>,
    /// Transactions committed by queued writes (group commit diagnostics).
    commits: Arc<AtomicU64>,
    next_job: AtomicU64,
    /// Faults injected by tests ([`crate::faults`]); empty in production.
    faults: crate::faults::FaultSlot,
    /// Deleted services ([`ServiceDataStore::retire`]): never revived, never re-opened.
    retired: Mutex<HashSet<ServiceId>>,
}

/// Exclusive use of one service's connection slot.
struct Lease {
    shared: Arc<Shared>,
    entry: Arc<ServiceConn>,
    gate: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.entry
            .tick
            .store(self.shared.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
        drop(self.gate.take());
        self.shared.freed.notify_waiters();
    }
}

/// An idle service chosen for eviction: out of `open`, its slots still counted. Dropping it closes
/// the victim's connections and gives the slots back, then releases its gate and reader permits:
/// the accounting cannot be skipped by an evicting caller that is cancelled (or by a blocking
/// task that never runs).
struct Eviction {
    shared: Arc<Shared>,
    victim: Arc<ServiceConn>,
    slots: usize,
    _gate: OwnedMutexGuard<()>,
    _readers: OwnedSemaphorePermit,
}

impl Drop for Eviction {
    fn drop(&mut self) {
        close_connections(&self.victim);
        self.shared.state.lock().conns -= self.slots;
        self.shared.freed.notify_waiters();
    }
}

/// Roll back a transaction nobody owns any more (an abandoned `begin`, a job that panicked
/// between `BEGIN` and `COMMIT`) before the connection serves anything else.
pub(crate) fn reset_open_tx(c: &Connection, which: &str) -> bool {
    if c.is_autocommit() {
        return false;
    }
    tracing::warn!(connection = which, "rolling back a transaction its owner abandoned");
    let _ = c.execute_batch("ROLLBACK");
    true
}

/// Rolls back a transaction left open when the code that began it unwinds (a panic), so the
/// connection never goes back into use mid-transaction.
struct RollbackOnUnwind<'a>(&'a Connection);

impl Drop for RollbackOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            reset_open_tx(self.0, "unwinding");
        }
    }
}

/// A reader connection and its permit, checked out for one job. Dropping it returns the
/// connection to the idle pool (closing a read transaction left open), or closes it when the
/// service's connections were closed meanwhile, and gives back a slot that was claimed for a
/// connection that never opened; only then is the permit released. It lives inside the job, so a
/// caller that stops waiting releases nothing early.
struct ReaderLoan {
    shared: Arc<Shared>,
    svc: ServiceId,
    entry: Arc<ServiceConn>,
    epoch: u64,
    conn: Option<Conn>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for ReaderLoan {
    fn drop(&mut self) {
        if let Some(c) = &self.conn {
            reset_open_tx(&c.c, "reader");
        }
        let closed = {
            let mut idle = self.entry.idle_readers.lock();
            if self.entry.epoch.load(Ordering::Acquire) != self.epoch {
                self.conn.take() // LCOV_EXCL_LINE: a reader checked out while its service was closed; needs an eviction during a read
            } else if let Some(c) = self.conn.take() {
                idle.push(c);
                None
            } else {
                drop(idle);
                self.shared.release_reader_slot(self.svc);
                None
            }
        };
        drop(closed);
        drop(self.permit.take());
        self.entry
            .tick
            .store(self.shared.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
        self.shared.freed.notify_waiters();
    }
}

/// The writer connection in `g`, opened first if needed.
fn writer_conn<'a>(
    g: &'a mut Option<Conn>,
    entry: &ServiceConn,
    o: &OpenOpts,
    s: ServiceId,
    limit: DbLimit,
) -> Result<&'a mut Conn> {
    if g.is_none() {
        *g = Some(open_conn(o, s, limit)?);
        entry.writer_open.store(true, Ordering::Release);
    }
    g.as_mut()
        .ok_or_else(|| StoreError::Internal("connection closed".into()))
}

impl Shared {
    /// The fair-queue lane of `s`'s blocking work.
    fn tenant_of(&self, s: ServiceId) -> Option<TenantId> {
        self.cfg.directory.as_ref().and_then(|d| d.tenant_of(s))
    }

    /// What bounds `s`'s database now: the node-wide cap and its tenant's remaining quota.
    pub(crate) fn db_limit(&self, s: ServiceId) -> DbLimit {
        DbLimit {
            node_max: self.opts.max_db_bytes,
            growth: self.cfg.directory.as_ref().and_then(|d| d.db_growth_bytes(s)),
        }
    }

    pub(crate) fn is_purged(&self, s: ServiceId) -> bool {
        let purged = self.purged.lock();
        !purged.is_empty() && purged.contains(&s)
    }

    pub(crate) fn is_retired(&self, s: ServiceId) -> bool {
        let retired = self.retired.lock();
        !retired.is_empty() && retired.contains(&s)
    }

    pub(crate) fn unpurge(&self, s: ServiceId) {
        if self.is_retired(s) {
            return; // stays purged: reads see nothing, writes fail when they open the database
        }
        let mut purged = self.purged.lock();
        if !purged.is_empty() {
            purged.remove(&s);
        }
    }

    pub(crate) fn entry(&self, s: ServiceId) -> Arc<ServiceConn> {
        let mut st = self.state.lock();
        st.entries
            .entry(s)
            .or_insert_with(|| {
                Arc::new(ServiceConn {
                    gate: Arc::new(tokio::sync::Mutex::new(())),
                    conn: Mutex::new(None),
                    writer_open: AtomicBool::new(false),
                    reader_permits: Arc::new(Semaphore::new(self.opts.readers_per_db)),
                    idle_readers: Mutex::new(Vec::new()),
                    epoch: AtomicU64::new(0),
                    tick: AtomicU64::new(0),
                    queue: Mutex::new(VecDeque::new()),
                    locals: Mutex::new(Vec::new()),
                })
            })
            .clone()
    }

    fn all_reader_permits(&self) -> u32 {
        u32::try_from(self.opts.readers_per_db).unwrap_or(u32::MAX)
    }

    /// The least recently used service other than `s` with nothing in use (gate free, no reader
    /// out, no Machine-local connection in a statement), taken out of `open` for closing. `all` is
    /// computed per call because `readers_per_db` is fixed for the pool's life.
    fn pick_victim(self: &Arc<Self>, st: &mut PoolState, s: ServiceId) -> Option<Eviction> {
        let all = self.all_reader_permits();
        let mut best: Option<(u64, ServiceId, OwnedMutexGuard<()>, OwnedSemaphorePermit)> = None;
        for (vs, o) in st.open.iter() {
            let tick = o.entry.tick.load(Ordering::Relaxed);
            if *vs == s || best.as_ref().is_some_and(|b| tick >= b.0) {
                continue;
            }
            // A Machine-local connection is idle only between statements and outside a
            // transaction: closing one mid-transaction would lose its uncommitted half.
            if o.locals > 0
                && !o
                    .entry
                    .locals
                    .lock()
                    .iter()
                    .all(|l| l.tx_depth.load(Ordering::Acquire) == 0 && l.conn.try_lock().is_some())
            {
                continue;
            }
            let Ok(g) = o.entry.gate.clone().try_lock_owned() else {
                continue;
            };
            let Ok(p) = o.entry.reader_permits.clone().try_acquire_many_owned(all) else {
                continue;
            };
            best = Some((tick, *vs, g, p));
        }
        best.map(|(_, vs, g, p)| {
            let o = st.open.remove(&vs).expect("candidate is open");
            Eviction {
                shared: self.clone(),
                slots: o.total(),
                victim: o.entry,
                _gate: g,
                _readers: p,
            }
        })
    }

    /// Count a Machine-local connection of `s` and register `slot` with its service. Called on the
    /// blocking pool (`LocalDb::connect` never runs it on the Machine thread): an idle service is
    /// evicted inline, its connections closed here (the writer's `PRAGMA optimize`, the WAL
    /// checkpoint). `false`: every slot is in use.
    pub(crate) fn claim_local(self: &Arc<Self>, s: ServiceId, entry: &Arc<ServiceConn>, slot: &Arc<LocalSlot>) -> bool {
        let cap = self.cfg.max_open_dbs.max(1);
        loop {
            let eviction = {
                let mut st = self.state.lock();
                if st.conns < cap {
                    st.open
                        .entry(s)
                        .or_insert_with(|| Slots {
                            entry: entry.clone(),
                            writer: false,
                            readers: 0,
                            locals: 0,
                        })
                        .locals += 1;
                    st.conns += 1;
                    slot.doomed.store(false, Ordering::Release);
                    entry.locals.lock().push(slot.clone());
                    return true;
                }
                self.pick_victim(&mut st, s)
            };
            match eviction {
                Some(ev) => drop(ev),
                None => return false,
            }
        }
    }

    /// Give back the slot of a Machine-local connection that is closed (or never opened).
    pub(crate) fn release_local(&self, s: ServiceId, entry: &ServiceConn, slot: &Arc<LocalSlot>) {
        {
            let mut locals = entry.locals.lock();
            let Some(i) = locals.iter().position(|l| Arc::ptr_eq(l, slot)) else {
                return;
            };
            locals.swap_remove(i);
        }
        {
            let mut st = self.state.lock();
            if let Some(o) = st.open.get_mut(&s).filter(|o| o.locals > 0) {
                o.locals -= 1;
                st.conns -= 1;
            }
        }
        self.freed.notify_waiters();
    }

    /// Take the service's gate and make sure its writer owns one of the (at most `max_open_dbs`)
    /// connection slots, closing the least recently used idle service when necessary.
    async fn lease(self: &Arc<Self>, s: ServiceId) -> Result<Lease> {
        let entry = self.entry(s);
        let gate = entry.gate.clone().lock_owned().await;
        self.claim_writer(s, entry, gate).await
    }

    /// Second half of [`Shared::lease`], once the gate is held: make sure the writer owns a
    /// connection slot. Cancel-safe: an eviction in progress settles its accounting on its own.
    async fn claim_writer(
        self: &Arc<Self>,
        s: ServiceId,
        entry: Arc<ServiceConn>,
        gate: OwnedMutexGuard<()>,
    ) -> Result<Lease> {
        if self.is_retired(s) {
            return Err(StoreError::NotFound);
        }
        let lease = Lease {
            shared: self.clone(),
            entry: entry.clone(),
            gate: Some(gate),
        };
        let cap = self.cfg.max_open_dbs.max(1);
        loop {
            let notified = self.freed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let eviction = {
                let mut st = self.state.lock();
                if st.open.get(&s).is_some_and(|o| o.writer) {
                    return Ok(lease);
                }
                if st.conns < cap {
                    st.open
                        .entry(s)
                        .or_insert_with(|| Slots {
                            entry: entry.clone(),
                            writer: false,
                            readers: 0,
                            locals: 0,
                        })
                        .writer = true;
                    st.conns += 1;
                    return Ok(lease);
                }
                // (This service holds no reader slot here: readers only open next to a writer slot,
                // which it lacks.)
                self.pick_victim(&mut st, s)
            };
            match eviction {
                // Closing runs on a blocking thread; the slots come back even if this caller is
                // cancelled meanwhile. Then claim one (another waiter may win it: loop).
                Some(ev) => {
                    let _ = tokio::task::spawn_blocking(move || drop(ev)).await;
                }
                None => notified.await,
            }
        }
    }

    /// Run `f` on the writer with `lease` moved into the job: the gate stays held until the job
    /// has run (or was dropped unrun), even if the caller stops waiting, so no other user of the
    /// writer can slip in between. A transaction an abandoned caller left open is rolled back
    /// first. `keep`: hand the lease back with the result (an interactive transaction keeps the
    /// writer); if the caller is gone by then, dropping the result releases it.
    async fn run_leased<T, F>(
        self: &Arc<Self>,
        lease: Lease,
        s: ServiceId,
        keep: bool,
        f: F,
    ) -> Result<(T, Option<Lease>)>
    where
        T: Send + 'static,
        F: FnOnce(&mut Conn) -> Result<T> + Send + 'static,
    {
        let opts = self.opts.clone();
        let limit = self.db_limit(s);
        run_job(&self.cfg.runner, self.tenant_of(s), move || {
            let entry = lease.entry.clone();
            let mut g = entry.conn.lock();
            let res = writer_conn(&mut g, &entry, &opts, s, limit).and_then(|conn| {
                reset_open_tx(&conn.c, "writer");
                conn.arm();
                let res = f(conn);
                conn.disarm();
                res
            });
            drop(g);
            let lease = keep.then_some(lease);
            res.map(|t| (t, lease))
        })
        .await
    }

    /// Run `f` on the writer.
    pub(crate) async fn with<T, F>(self: &Arc<Self>, s: ServiceId, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Conn) -> Result<T> + Send + 'static,
    {
        self.with_checked(s, f, true).await
    }

    /// [`Self::with`]; `check` = take a planned fault first (false when the caller already did).
    async fn with_checked<T, F>(self: &Arc<Self>, s: ServiceId, f: F, check: bool) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Conn) -> Result<T> + Send + 'static,
    {
        if check {
            self.faults.check()?;
        }
        let lease = self.lease(s).await?;
        self.run_leased(lease, s, false, f).await.map(|(t, _)| t)
    }

    /// Run read-only `f` on a reader connection of `s`: an idle one, else a new one while the
    /// service has fewer than `readers_per_db` and a connection slot is free. Falls back to the
    /// writer when the service has no open writer yet (it creates and bootstraps the file) or no
    /// slot is free; `f` sees which one it got through `Conn::role`.
    pub(crate) async fn read<T, F>(self: &Arc<Self>, s: ServiceId, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Conn) -> Result<T> + Send + 'static,
    {
        let entry = self.entry(s);
        if self.opts.readers_per_db == 0 || !entry.writer_open.load(Ordering::Acquire) {
            return self.with(s, f).await;
        }
        self.faults.check()?;
        let permit = entry
            .reader_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::Internal("reader pool closed".into()))?;
        let (epoch, pooled) = {
            let mut idle = entry.idle_readers.lock();
            (entry.epoch.load(Ordering::Acquire), idle.pop())
        };
        if pooled.is_none() && !self.claim_reader_slot(s, &entry) {
            drop(permit);
            return self.with_checked(s, f, false).await;
        }
        let loan = ReaderLoan {
            shared: self.clone(),
            svc: s,
            entry,
            epoch,
            conn: pooled,
            permit: Some(permit),
        };
        let opts = self.opts.clone();
        run_job(&self.cfg.runner, self.tenant_of(s), move || {
            let mut loan = loan;
            let conn = match loan.conn.as_mut() {
                Some(c) => c,
                None => loan.conn.insert(open_reader_conn(&opts, s)?),
            };
            conn.arm();
            let res = f(conn);
            conn.disarm();
            res
        })
        .await
    }

    /// Count one more reader of `s` if its writer is open and a connection slot is free (readers
    /// never evict other services: under slot pressure reads fall back to the writer).
    fn claim_reader_slot(&self, s: ServiceId, entry: &ServiceConn) -> bool {
        let cap = self.cfg.max_open_dbs.max(1);
        let mut st = self.state.lock();
        if st.conns >= cap || !entry.writer_open.load(Ordering::Acquire) {
            return false;
        }
        match st.open.get_mut(&s) {
            Some(o) if o.writer && o.readers < self.opts.readers_per_db => {
                o.readers += 1;
                st.conns += 1;
                true
            }
            _ => false, // LCOV_EXCL_LINE: needs more concurrent readers than readers_per_db allows; not reproducible in-process
        }
    }

    /// Queue a coalescable write (KV set/delete/incr, blob index write) for `s` and wait for its
    /// result. Whoever gets the writer gate drains up to [`MAX_GROUP`] queued jobs into one
    /// transaction (group commit, see [`run_group`]); a caller whose job another caller ran just
    /// takes its result. No artificial delay: a job queued while the writer is idle runs at once,
    /// alone, exactly as before. A caller that goes away before its job ran takes the job back out.
    pub(crate) async fn write_queued(self: &Arc<Self>, s: ServiceId, f: GroupFn) -> Result<GroupOut> {
        self.faults.check()?;
        let entry = self.entry(s);
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        entry.queue.lock().push_back(Queued { id, f, done: tx });
        let mut unqueue = Unqueue {
            entry: &entry,
            id,
            armed: true,
        };
        loop {
            tokio::select! {
                biased;
                r = &mut rx => {
                    unqueue.armed = false;
                    return r.unwrap_or_else(|_| Err(StoreError::Internal("write job did not complete".into())));
                }
                gate = entry.gate.clone().lock_owned() => {
                    let lease = self.claim_writer(s, entry.clone(), gate).await?;
                    let jobs: Vec<Queued> = {
                        let mut q = entry.queue.lock();
                        let n = q.len().min(MAX_GROUP);
                        q.drain(..n).collect()
                    };
                    if jobs.is_empty() {
                        continue; // LCOV_EXCL_LINE: another leader drained the queue first; a race between leaders
                    }
                    // The lease travels with the group: if this leader is cancelled, nothing else
                    // (a guest's `begin` in particular) gets the writer before the group ran.
                    let (opts, limit, commits) = (self.opts.clone(), self.db_limit(s), self.commits.clone());
                    run_job(&self.cfg.runner, self.tenant_of(s), move || {
                        let e = lease.entry.clone();
                        let mut g = e.conn.lock();
                        match writer_conn(&mut g, &e, &opts, s, limit) {
                            Ok(conn) => {
                                reset_open_tx(&conn.c, "writer");
                                run_group(conn, jobs, &commits);
                            }
                            Err(err) => fail_all(jobs, &err),
                        }
                        drop(g);
                        drop(lease);
                        Ok(())
                    })
                    .await?;
                }
            }
        }
    }

    /// Give back a reader slot whose connection never opened.
    fn release_reader_slot(&self, s: ServiceId) {
        {
            let mut st = self.state.lock();
            if let Some(o) = st.open.get_mut(&s).filter(|o| o.readers > 0) {
                o.readers -= 1;
                st.conns -= 1;
            }
        }
        self.freed.notify_waiters();
    }
}

/// Most jobs one group commit takes (B3.2).
const MAX_GROUP: usize = 64;

/// What a coalescable write returns.
pub(crate) enum GroupOut {
    Done,
    /// KV delete: whether the key existed.
    Deleted(bool),
    /// KV increment: the counter's new value.
    Counter(i64),
    /// KV compare-and-swap or expire: whether it took effect.
    Flag(bool),
    /// Blob index write: the object file the row pointed to before (to remove after the commit).
    Replaced(Option<String>),
}

/// A coalescable write. `FnMut`: when another job's failure takes the whole group transaction
/// with it (an interrupt or a full disk can), the surviving jobs run again in a new transaction.
pub(crate) type GroupFn = Box<dyn FnMut(&Connection) -> Result<GroupOut> + Send>;

struct Queued {
    id: u64,
    f: GroupFn,
    done: tokio::sync::oneshot::Sender<Result<GroupOut>>,
}

/// Takes a job back out of the queue when its caller goes away before any leader ran it.
struct Unqueue<'a> {
    entry: &'a ServiceConn,
    id: u64,
    armed: bool,
}

impl Drop for Unqueue<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.entry.queue.lock().retain(|q| q.id != self.id);
        }
    }
}

/// The same error for several callers (`StoreError` is not `Clone`).
fn copy_err(e: &StoreError) -> StoreError {
    match e {
        StoreError::NotFound => StoreError::NotFound,
        StoreError::Conflict(c) => StoreError::Conflict(c),
        StoreError::PreconditionFailed => StoreError::PreconditionFailed,
        StoreError::Invalid(m) => StoreError::Invalid(m.clone()),
        StoreError::StorageQuota => StoreError::StorageQuota,
        StoreError::Sql(m) => StoreError::Sql(m.clone()),
        StoreError::SqlTimeout => StoreError::SqlTimeout,
        StoreError::SqlNotAuthorized => StoreError::SqlNotAuthorized,
        StoreError::SqlBusy => StoreError::SqlBusy,
        StoreError::SqlResultTooLarge => StoreError::SqlResultTooLarge,
        StoreError::KvNotInteger => StoreError::KvNotInteger,
        StoreError::KvOverflow => StoreError::KvOverflow,
        StoreError::Corrupt(m) => StoreError::Corrupt(m.clone()),
        StoreError::Sqlite(err) => StoreError::Internal(format!("database error: {err}")),
        StoreError::Io(err) => StoreError::Io(std::io::Error::new(err.kind(), err.to_string())),
        StoreError::Internal(m) => StoreError::Internal(m.clone()),
        StoreError::WarmFloorTotal { total, cap } => StoreError::WarmFloorTotal {
            total: *total,
            cap: *cap,
        },
    }
}

fn fail_all(jobs: Vec<Queued>, e: &StoreError) {
    for q in jobs {
        let _ = q.done.send(Err(copy_err(e)));
    }
}

/// Blocking, on the writer: run `jobs` in queue order. One job runs alone in autocommit (no
/// overhead over an ungrouped write). Several share one `BEGIN IMMEDIATE … COMMIT`, each inside
/// its own savepoint, so a failing job rolls back only its own changes and gets its own error.
/// If a failure ends the whole transaction (SQLite may roll everything back on an interrupt or a
/// full disk), that job fails and the others run again in a fresh transaction.
fn run_group(conn: &Conn, mut jobs: Vec<Queued>, commits: &AtomicU64) {
    if jobs.len() == 1 {
        if let Some(mut q) = jobs.pop() {
            conn.arm();
            let r = (q.f)(&conn.c);
            conn.disarm();
            commits.fetch_add(1, Ordering::Relaxed);
            let _ = q.done.send(r);
        }
        return;
    }
    let _unwind = RollbackOnUnwind(&conn.c);
    loop {
        if let Err(e) = conn.c.execute_batch("BEGIN IMMEDIATE") {
            fail_all(jobs, &map_internal_err(e)); // LCOV_EXCL_LINE: BEGIN IMMEDIATE on a healthy writer does not fail
            return; // LCOV_EXCL_LINE: BEGIN IMMEDIATE on a healthy writer does not fail
        }
        let mut results: Vec<Result<GroupOut>> = Vec::with_capacity(jobs.len());
        let mut lost: Option<(usize, Result<GroupOut>)> = None;
        for (i, q) in jobs.iter_mut().enumerate() {
            conn.arm();
            let r = conn
                .c
                .execute_batch("SAVEPOINT rt_job")
                .map_err(map_internal_err)
                .and_then(|()| (q.f)(&conn.c));
            conn.disarm();
            if conn.c.is_autocommit() {
                lost = Some((i, r));
                break;
            }
            let end = if r.is_ok() {
                "RELEASE rt_job"
            } else {
                "ROLLBACK TO rt_job; RELEASE rt_job"
            };
            let r = match conn.c.execute_batch(end) {
                Ok(()) => r,
                Err(e) => Err(map_internal_err(e)), // LCOV_EXCL_LINE: RELEASE of a savepoint whose statement succeeded does not fail
            };
            results.push(r);
        }
        if let Some((i, r)) = lost {
            let q = jobs.remove(i);
            let _ = q.done.send(match r {
                Err(e) => Err(e),
                Ok(_) => Err(StoreError::Internal("write transaction ended unexpectedly".into())), // LCOV_EXCL_LINE: COMMIT and ROLLBACK are denied to guest SQL, so a job that succeeds never ends the transaction
            });
            if jobs.is_empty() {
                return; // LCOV_EXCL_LINE: a group of one takes the single-job path, so removing one job always leaves another
            }
            continue;
        }
        match conn.c.execute_batch("COMMIT") {
            Ok(()) => {
                commits.fetch_add(1, Ordering::Relaxed);
                for (q, r) in jobs.into_iter().zip(results) {
                    let _ = q.done.send(r);
                }
            }
            Err(e) => {
                // LCOV_EXCL_LINE: COMMIT over the internal tables fails only on a disk error; they have no deferred constraints
                if !conn.c.is_autocommit() {
                    // LCOV_EXCL_LINE: COMMIT over the internal tables fails only on a disk error; they have no deferred constraints
                    let _ = conn.c.execute_batch("ROLLBACK"); // LCOV_EXCL_LINE: COMMIT over the internal tables fails only on a disk error; they have no deferred constraints
                } // LCOV_EXCL_LINE: COMMIT over the internal tables fails only on a disk error; they have no deferred constraints
                fail_all(jobs, &map_internal_err(e)); // LCOV_EXCL_LINE: COMMIT over the internal tables fails only on a disk error; they have no deferred constraints
            }
        }
        return;
    }
}

/// Blocking: close `e`'s idle readers, its Machine-local connections and its writer. Readers still
/// checked out are closed when they come back (the epoch moved on).
fn close_connections(e: &ServiceConn) {
    let readers = {
        let mut idle = e.idle_readers.lock();
        e.epoch.fetch_add(1, Ordering::AcqRel);
        std::mem::take(&mut *idle)
    };
    // Machine-local connections: closed here when idle; one in a statement right now is closed by
    // its owner when the statement ends (never wait for a Machine thread's statement).
    let locals = std::mem::take(&mut *e.locals.lock());
    for l in locals {
        l.doomed.store(true, Ordering::Release);
        if let Some(mut g) = l.conn.try_lock() {
            *g = None;
        }
    }
    // Readers first: the writer, closing last, checkpoints the WAL and removes it.
    drop(readers);
    e.writer_open.store(false, Ordering::Release);
    drop(e.conn.lock().take());
}

/// Local-disk `ServiceDataStore`: SQLite files per service + local blob directory.
pub struct LocalServiceData {
    shared: Arc<Shared>,
}

impl LocalServiceData {
    /// Make the next KV, SQL, blob and durable operations fail as `faults` plans (tests, TK-002).
    pub fn inject_faults(&self, faults: Arc<crate::faults::Faults>) {
        self.shared.faults.set(faults);
    }

    pub fn open(cfg: DataConfig) -> Result<Arc<Self>> {
        private_dirs(&cfg.root.join("services"))?;
        private_dirs(&cfg.root.join("blobs"))?;
        // The page-cache budget is shared by at most `max_open_dbs` connections.
        let cache_kib = match cfg.sql_cache_bytes {
            0 => 0,
            b => i64::try_from(b / cfg.max_open_dbs.max(1) as u64 / 1024)
                .unwrap_or(i64::MAX)
                .max(64),
        };
        let opts = OpenOpts {
            root: cfg.root.clone(),
            budget: cfg.sql_budget,
            sync_budget: cfg.sql_sync_budget.max(Duration::from_millis(1)),
            sync_busy: cfg.sql_sync_busy,
            sync_stmt_cache: cfg.sql_sync_stmt_cache.max(1),
            max_db_bytes: cfg.max_db_bytes,
            mmap_bytes: cfg.mmap_bytes,
            cache_kib,
            readers_per_db: cfg.readers_per_db,
            max_result_rows: cfg.max_result_rows.max(1),
            max_result_bytes: cfg.max_result_bytes.max(1),
        };
        let shared = Arc::new(Shared {
            cfg,
            opts,
            state: Mutex::new(PoolState::default()),
            freed: tokio::sync::Notify::new(),
            clock: AtomicU64::new(1),
            purged: Mutex::new(HashSet::new()),
            commits: Arc::new(AtomicU64::new(0)),
            next_job: AtomicU64::new(0),
            faults: crate::faults::FaultSlot::default(),
            retired: Mutex::new(HashSet::new()),
        });
        Ok(Arc::new(Self { shared }))
    }
}

impl LocalServiceData {
    /// Number of SQLite connections currently open across all services, writers and readers
    /// (STO-14: never above `max_open_dbs`). Test/diagnostics hook.
    pub fn open_connections(&self) -> usize {
        self.shared.state.lock().conns
    }

    /// Re-apply every open database's size ceiling from the directory (the tenants' remaining
    /// quota): call it after each storage scan, so plan changes and other services' growth reach
    /// open connections. A tenant over quota then has every database frozen at its size.
    pub async fn refresh_limits(&self) {
        let ids: Vec<ServiceId> = self
            .shared
            .state
            .lock()
            .open
            .iter()
            .filter(|(_, o)| o.writer)
            .map(|(s, _)| *s)
            .collect();
        for s in ids {
            let limit = self.shared.db_limit(s);
            if let Err(e) = self
                .shared
                .with(s, move |conn| apply_ceiling(&conn.c, limit).map_err(map_internal_err))
                .await
            {
                tracing::warn!(service = %s, error = %e, "could not refresh the database size ceiling");
                // LCOV_EXCL_LINE: a size ceiling on a healthy open connection does not fail
            }
        }
        // Machine-local connections (`std:sqlite`): one in a statement right now is refreshed by
        // its next task (`LocalDb::refresh_ceiling`).
        let locals: Vec<(ServiceId, Vec<Arc<LocalSlot>>)> = {
            let st = self.shared.state.lock();
            st.open
                .iter()
                .filter(|(_, o)| o.locals > 0)
                .map(|(s, o)| (*s, o.entry.locals.lock().clone()))
                .collect()
        };
        for (s, slots) in locals {
            let limit = self.shared.db_limit(s);
            for slot in slots {
                if let Some(Some(conn)) = slot.conn.try_lock().as_deref() {
                    if let Err(e) = apply_ceiling(&conn.c, limit) {
                        tracing::warn!(service = %s, error = %e, "could not refresh the database size ceiling of a local connection");
                        // LCOV_EXCL_LINE: a size ceiling on a healthy open connection does not fail
                    }
                } // LCOV_EXCL_LINE: a local connection that is not open is skipped; the refresh only sees open ones
            }
        }
    }

    /// Transactions committed by KV and blob-index writes so far: with group commit, fewer than
    /// the writes when they queue up behind a busy writer. Test/diagnostics hook.
    pub fn write_commits(&self) -> u64 {
        self.shared.commits.load(Ordering::Relaxed)
    }

    /// Read a PRAGMA (`journal_mode`, `synchronous`, `busy_timeout`, `page_size`,
    /// `max_page_count`, …) from the service's `data.db` connection (STO-16 test hook).
    pub async fn pragma(&self, s: ServiceId, name: &str) -> Result<SqlValue> {
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(StoreError::Invalid("bad pragma name".into()));
        }
        let sql = format!("PRAGMA {name}");
        self.shared.unpurge(s);
        self.shared
            .with(s, move |conn| {
                conn.c
                    .query_row(&sql, [], |r| Ok(from_ref(r.get_ref(0)?)))
                    .map_err(map_internal_err)
            })
            .await
    }

    /// Delete expired KV rows of every currently open service database (the node's 60 s sweep;
    /// each service's work runs in its tenant's lane). Returns how many rows were deleted; a
    /// service that fails is logged and skipped.
    pub async fn sweep_open(&self) -> Result<u64> {
        let ids: Vec<ServiceId> = self.shared.state.lock().open.keys().copied().collect();
        let mut total = 0;
        for s in ids {
            match self.kv(s).sweep_expired().await {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(service = %s, error = %e, "expired-KV sweep failed"),
            }
        }
        Ok(total)
    }
}

impl ServiceDataStore for LocalServiceData {
    fn kv(&self, s: ServiceId) -> Arc<dyn KvBackend> {
        Arc::new(KvHandle {
            shared: self.shared.clone(),
            svc: s,
        })
    }

    fn sql(&self, s: ServiceId) -> Arc<dyn SqlBackend> {
        Arc::new(SqlHandle {
            shared: self.shared.clone(),
            svc: s,
        })
    }

    fn local_sql(&self) -> Option<Arc<dyn crate::sync_sql::LocalSqlSource>> {
        Some(Arc::new(crate::sync_sql::LocalSource::new(self.shared.clone())))
    }

    fn blob(&self, s: ServiceId) -> Arc<dyn BlobBackend> {
        Arc::new(BlobHandle {
            shared: self.shared.clone(),
            svc: s,
            dir: blob_dir(&self.shared.opts.root, s),
        })
    }

    fn durable(&self, s: ServiceId) -> Arc<dyn DurableBackend> {
        Arc::new(crate::durable::DurableHandle {
            shared: self.shared.clone(),
            svc: s,
        })
    }

    fn retire(&self, s: ServiceId) -> Result<()> {
        self.shared.retired.lock().insert(s);
        self.purge(s)
    }

    fn purge(&self, s: ServiceId) -> Result<()> {
        let entry = {
            let mut st = self.shared.state.lock();
            if let Some(o) = st.open.remove(&s) {
                st.conns -= o.total();
            }
            st.entries.get(&s).cloned()
        };
        if let Some(e) = entry {
            // Closes the connections (waits for a writer statement that is running right now; a
            // reader in use is closed when it comes back).
            close_connections(&e);
        }
        self.shared.purged.lock().insert(s);
        for dir in [
            service_dir(&self.shared.opts.root, s),
            blob_dir(&self.shared.opts.root, s),
        ] {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.shared.freed.notify_waiters();
        Ok(())
    }
}

// ---- KV --------------------------------------------------------------------------------------

struct KvHandle {
    shared: Arc<Shared>,
    svc: ServiceId,
}

fn tag_of(n: i64) -> ValueTag {
    if n == 1 {
        ValueTag::Bytes
    } else {
        ValueTag::Json
    }
}

/// A KV key a write may use: 1 to [`MAX_KV_KEY_BYTES`] bytes (KV-003: the facade refuses an empty
/// key too; the store does not rely on it).
fn check_kv_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(StoreError::Invalid("a key must not be empty".into()));
    }
    if key.len() > MAX_KV_KEY_BYTES {
        return Err(StoreError::Invalid(format!("key exceeds {MAX_KV_KEY_BYTES} bytes")));
    }
    Ok(())
}

fn kv_get(c: &Connection, key: &[u8], now: i64) -> Result<Option<KvValue>> {
    c.prepare_cached("SELECT value, tag FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)")
        .and_then(|mut st| {
            st.query_row(params![key, now], |r| {
                let data: Vec<u8> = r.get(0)?;
                Ok(KvValue {
                    tag: tag_of(r.get(1)?),
                    data: Bytes::from(data),
                })
            })
            .optional()
        })
        .map_err(map_internal_err)
}

/// Absolute expiry (unix ms) of a key written at `now` with `ttl`.
fn expiry(now: i64, ttl: Option<Duration>) -> Option<i64> {
    ttl.map(|t| now.saturating_add(i64::try_from(t.as_millis()).unwrap_or(i64::MAX)))
}

/// Blocking, on the writer, inside the caller's transaction: the read-modify-write of
/// [`KvBackend::incr`]. Nothing is written when the current value is refused.
fn kv_incr(c: &Connection, key: &[u8], delta: i64, ttl: Option<Duration>) -> Result<i64> {
    let now = now_ms();
    let current = c
        .prepare_cached(
            "SELECT value, tag, expires_at FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)",
        )
        .and_then(|mut st| {
            st.query_row(params![key, now], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                ))
            })
            .optional()
        })
        .map_err(map_internal_err)?;
    let (next, expires) = match current {
        Some((value, tag, expires)) => {
            if tag_of(tag) != ValueTag::Json {
                return Err(StoreError::KvNotInteger);
            }
            // JSON integers only: `1.5`, `1e3`, `"7"` and anything past `i64` are refused.
            let n: i64 = serde_json::from_slice(&value).map_err(|_| StoreError::KvNotInteger)?;
            (n.checked_add(delta).ok_or(StoreError::KvOverflow)?, expires)
        }
        // Missing or expired: a new counter, with this call's TTL.
        None => (delta, expiry(now, ttl)),
    };
    c.prepare_cached(
        "INSERT INTO _rt_kv (key, value, tag, expires_at) VALUES (?1, ?2, 0, ?3) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, tag = 0, expires_at = excluded.expires_at",
    )
    .and_then(|mut st| st.execute(params![key, next.to_string().as_bytes(), expires]))
    .map_err(map_internal_err)?;
    Ok(next)
}

/// Blocking, on the writer, inside the caller's transaction: the compare-and-swap of
/// [`KvBackend::cas`]. Nothing is written when `expected` does not match.
fn kv_cas(
    c: &Connection,
    key: &[u8],
    expected: Option<&KvValue>,
    next: Option<&KvValue>,
    ttl: Option<Duration>,
) -> Result<bool> {
    let now = now_ms();
    let current = c
        .prepare_cached("SELECT value, tag FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)")
        .and_then(|mut st| {
            st.query_row(params![key, now], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?))
            })
            .optional()
        })
        .map_err(map_internal_err)?;
    let matched = match (expected, &current) {
        (None, None) => true,
        (Some(e), Some((data, tag))) => tag_of(*tag) == e.tag && data.as_slice() == e.data.as_ref(),
        _ => false,
    };
    if !matched {
        return Ok(false);
    }
    match next {
        None => {
            c.prepare_cached("DELETE FROM _rt_kv WHERE key = ?1")
                .and_then(|mut st| st.execute(params![key]))
                .map_err(map_internal_err)?;
        }
        Some(v) => {
            c.prepare_cached(
                "INSERT INTO _rt_kv (key, value, tag, expires_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, tag = excluded.tag, expires_at = excluded.expires_at",
            )
            .and_then(|mut st| st.execute(params![key, v.data.as_ref(), i64::from(v.tag == ValueTag::Bytes), expiry(now, ttl)]))
            .map_err(map_internal_err)?;
        }
    }
    Ok(true)
}

#[async_trait]
impl KvBackend for KvHandle {
    async fn get(&self, key: &[u8]) -> Result<Option<KvValue>> {
        if self.shared.is_purged(self.svc) {
            return Ok(None);
        }
        let key = key.to_vec();
        self.shared
            .read(self.svc, move |conn| kv_get(&conn.c, &key, now_ms()))
            .await
    }

    async fn get_many(&self, keys: &[Bytes]) -> Result<Vec<Option<KvValue>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if self.shared.is_purged(self.svc) {
            return Ok(vec![None; keys.len()]);
        }
        let keys = keys.to_vec();
        self.shared
            .read(self.svc, move |conn| {
                let now = now_ms();
                // The same key may be named many times: stop at the result cap, not at the end.
                let mut left = MAX_RESULT_BYTES;
                let mut out = Vec::with_capacity(keys.len());
                for k in &keys {
                    let v = kv_get(&conn.c, k, now)?;
                    let n = v.as_ref().map_or(0, |v| v.data.len());
                    left = left.checked_sub(n).ok_or_else(|| {
                        StoreError::Invalid(format!("getMany returns at most {MAX_RESULT_BYTES} bytes"))
                    })?;
                    out.push(v);
                }
                Ok(out)
            })
            .await
    }

    async fn set(&self, key: &[u8], value: KvValue, opts: KvSetOptions) -> Result<()> {
        check_kv_key(key)?;
        if value.data.len() > MAX_KV_VALUE_BYTES {
            return Err(StoreError::Invalid(format!("value exceeds {MAX_KV_VALUE_BYTES} bytes")));
        }
        self.shared.unpurge(self.svc);
        let key = key.to_vec();
        let job: GroupFn = Box::new(move |c| {
            let expires = expiry(now_ms(), opts.ttl);
            c.prepare_cached(
                "INSERT INTO _rt_kv (key, value, tag, expires_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, tag = excluded.tag, expires_at = excluded.expires_at",
            )
            .and_then(|mut st| st.execute(params![key, value.data.as_ref(), i64::from(value.tag == ValueTag::Bytes), expires]))
            .map_err(map_internal_err)?;
            Ok(GroupOut::Done)
        });
        self.shared.write_queued(self.svc, job).await.map(|_| ())
    }

    async fn delete(&self, key: &[u8]) -> Result<bool> {
        if self.shared.is_purged(self.svc) {
            return Ok(false);
        }
        let key = key.to_vec();
        let job: GroupFn = Box::new(move |c| {
            let now = now_ms();
            // An expired-but-unswept row counts as absent.
            c.prepare_cached("DELETE FROM _rt_kv WHERE key = ?1 AND expires_at IS NOT NULL AND expires_at <= ?2")
                .and_then(|mut st| st.execute(params![key, now]))
                .map_err(map_internal_err)?;
            let n = c
                .prepare_cached("DELETE FROM _rt_kv WHERE key = ?1")
                .and_then(|mut st| st.execute([&key]))
                .map_err(map_internal_err)?;
            Ok(GroupOut::Deleted(n > 0))
        });
        match self.shared.write_queued(self.svc, job).await? {
            GroupOut::Deleted(existed) => Ok(existed),
            _ => Err(StoreError::Internal("unexpected write result".into())),
        }
    }

    async fn incr(&self, key: &[u8], delta: i64, opts: KvSetOptions) -> Result<i64> {
        check_kv_key(key)?;
        self.shared.unpurge(self.svc);
        let key = key.to_vec();
        // A coalescable write like `set`: the read and the write share one transaction on the
        // writer (a savepoint, nested in the group commit's own when the job is grouped), and
        // every write of this database goes through that writer, so no increment is lost.
        let job: GroupFn = Box::new(move |c| {
            c.execute_batch("SAVEPOINT rt_incr").map_err(map_internal_err)?;
            let r = kv_incr(c, &key, delta, opts.ttl);
            let end = if r.is_ok() {
                "RELEASE rt_incr"
            } else {
                "ROLLBACK TO rt_incr; RELEASE rt_incr"
            };
            c.execute_batch(end).map_err(map_internal_err)?;
            r.map(GroupOut::Counter)
        });
        match self.shared.write_queued(self.svc, job).await? {
            GroupOut::Counter(n) => Ok(n),
            _ => Err(StoreError::Internal("unexpected write result".into())),
        }
    }

    async fn cas(
        &self,
        key: &[u8],
        expected: Option<KvValue>,
        next: Option<KvValue>,
        opts: KvSetOptions,
    ) -> Result<bool> {
        check_kv_key(key)?;
        if next.as_ref().is_some_and(|v| v.data.len() > MAX_KV_VALUE_BYTES) {
            return Err(StoreError::Invalid(format!("value exceeds {MAX_KV_VALUE_BYTES} bytes")));
        }
        if next.is_none() && self.shared.is_purged(self.svc) {
            // A purged service has no rows: only "absent" can match, and nothing is written.
            return Ok(expected.is_none());
        }
        self.shared.unpurge(self.svc);
        let key = key.to_vec();
        // One writer job, a savepoint like `incr`: the compare and the write cannot interleave with
        // another writer, so of concurrent swaps on one key exactly one matches.
        let job: GroupFn = Box::new(move |c| {
            c.execute_batch("SAVEPOINT rt_cas").map_err(map_internal_err)?;
            let r = kv_cas(c, &key, expected.as_ref(), next.as_ref(), opts.ttl);
            let end = if r.is_ok() {
                "RELEASE rt_cas"
            } else {
                "ROLLBACK TO rt_cas; RELEASE rt_cas"
            };
            c.execute_batch(end).map_err(map_internal_err)?;
            r.map(GroupOut::Flag)
        });
        match self.shared.write_queued(self.svc, job).await? {
            GroupOut::Flag(swapped) => Ok(swapped),
            _ => Err(StoreError::Internal("unexpected write result".into())),
        }
    }

    async fn expire(&self, key: &[u8], ttl: Option<Duration>) -> Result<bool> {
        check_kv_key(key)?;
        if self.shared.is_purged(self.svc) {
            return Ok(false);
        }
        let key = key.to_vec();
        let job: GroupFn = Box::new(move |c| {
            let now = now_ms();
            let n = c
                .prepare_cached(
                    "UPDATE _rt_kv SET expires_at = ?2 WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?3)",
                )
                .and_then(|mut st| st.execute(params![key, expiry(now, ttl), now]))
                .map_err(map_internal_err)?;
            Ok(GroupOut::Flag(n > 0))
        });
        match self.shared.write_queued(self.svc, job).await? {
            GroupOut::Flag(found) => Ok(found),
            _ => Err(StoreError::Internal("unexpected write result".into())),
        }
    }

    async fn ttl(&self, key: &[u8]) -> Result<Option<Duration>> {
        if self.shared.is_purged(self.svc) {
            return Ok(None);
        }
        let key = key.to_vec();
        self.shared
            .read(self.svc, move |conn| {
                let now = now_ms();
                let row: Option<Option<i64>> = conn
                    .c
                    .prepare_cached(
                        "SELECT expires_at FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)",
                    )
                    .and_then(|mut st| {
                        st.query_row(params![key, now], |r| r.get::<_, Option<i64>>(0))
                            .optional()
                    })
                    .map_err(map_internal_err)?;
                Ok(row.flatten().map(|at| Duration::from_millis((at - now).max(0) as u64)))
            })
            .await
    }

    async fn list(&self, opts: KvListOptions) -> Result<KvPage> {
        let limit = opts.limit.clamp(1, MAX_KV_LIST_LIMIT);
        if self.shared.is_purged(self.svc) {
            return Ok(KvPage {
                entries: Vec::new(),
                next_cursor: None,
            });
        }
        self.shared
            .read(self.svc, move |conn| {
                let prefix = opts.prefix.to_vec();
                let hi = prefix_end(&prefix);
                // Both directions are one range scan of the primary key: `[prefix, hi)`, narrowed
                // by the cursor (forward: keys after it; reverse: keys before it, walked downwards).
                let mut conds = vec!["(expires_at IS NULL OR expires_at > ?)"];
                let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(now_ms())];
                if opts.reverse {
                    conds.push("key >= ?");
                    args.push(Box::new(prefix.clone()));
                    if let Some(cur) = opts.cursor {
                        conds.push("key < ?");
                        args.push(Box::new(cur.to_vec()));
                    }
                } else {
                    match opts.cursor {
                        Some(cur) if cur.as_ref() >= prefix.as_slice() => {
                            conds.push("key > ?");
                            args.push(Box::new(cur.to_vec()));
                        }
                        _ => {
                            conds.push("key >= ?");
                            args.push(Box::new(prefix.clone()));
                        }
                    }
                }
                if let Some(h) = hi {
                    conds.push("key < ?");
                    args.push(Box::new(h));
                }
                args.push(Box::new(limit as i64 + 1));
                let sql = format!(
                    "SELECT key, value, tag FROM _rt_kv WHERE {} ORDER BY key {} LIMIT ?",
                    conds.join(" AND "),
                    if opts.reverse { "DESC" } else { "ASC" }
                );
                let mut entries = conn
                    .c
                    .prepare_cached(&sql)
                    .and_then(|mut st| {
                        st.query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), |r| {
                            let k: Vec<u8> = r.get(0)?;
                            let v: Vec<u8> = r.get(1)?;
                            Ok((
                                Bytes::from(k),
                                KvValue {
                                    tag: tag_of(r.get(2)?),
                                    data: Bytes::from(v),
                                },
                            ))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(map_internal_err)?;
                let next_cursor = if entries.len() > limit {
                    entries.truncate(limit);
                    entries.last().map(|(k, _)| k.clone())
                } else {
                    None
                };
                Ok(KvPage { entries, next_cursor })
            })
            .await
    }

    async fn sweep_expired(&self) -> Result<u64> {
        if self.shared.is_purged(self.svc) {
            return Ok(0);
        }
        // Look first on a reader (the partial `_rt_kv_expires` index answers it): most databases
        // have nothing expired, and the sweep must not queue behind writes for nothing.
        let now = now_ms();
        let any = self
            .shared
            .read(self.svc, move |conn| {
                conn.c
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM _rt_kv WHERE expires_at IS NOT NULL AND expires_at <= ?1)",
                    )
                    .and_then(|mut st| st.query_row([now], |r| r.get::<_, bool>(0)))
                    .map_err(map_internal_err)
            })
            .await?;
        if !any {
            return Ok(0);
        }
        // In chunks, each its own short writer job under the statement budget: any backlog
        // shrinks, and other writes interleave instead of waiting for one huge DELETE.
        let mut total = 0;
        loop {
            let n = self
                .shared
                .with(self.svc, move |conn| sweep_chunk(&conn.c, now, SWEEP_CHUNK))
                .await?;
            total += n as u64;
            if n < SWEEP_CHUNK {
                return Ok(total);
            }
        }
    }
}

/// Expired KV rows one sweep job deletes at most.
const SWEEP_CHUNK: usize = 1000;

/// Delete up to `limit` KV rows expired at `now` (the partial `_rt_kv_expires` index finds them).
fn sweep_chunk(c: &Connection, now: i64, limit: usize) -> Result<usize> {
    c.prepare_cached(
        "DELETE FROM _rt_kv WHERE key IN \
         (SELECT key FROM _rt_kv WHERE expires_at IS NOT NULL AND expires_at <= ?1 LIMIT ?2)",
    )
    .and_then(|mut st| st.execute(params![now, i64::try_from(limit).unwrap_or(i64::MAX)]))
    .map_err(map_internal_err)
}

// ---- SQL -------------------------------------------------------------------------------------

struct SqlHandle {
    shared: Arc<Shared>,
    svc: ServiceId,
}

fn run_batch(conn: &Conn, stmts: &[SqlStatement], mode: BatchMode) -> Result<Vec<SqlResult>> {
    let read = mode == BatchMode::Read;
    conn.c
        .execute_batch(if read { "BEGIN" } else { "BEGIN IMMEDIATE" })
        .map_err(map_sql_err)?;
    let _unwind = RollbackOnUnwind(&conn.c);
    let mut out = Vec::with_capacity(stmts.len());
    let mut budget = ResultBudget::of(conn);
    for st in stmts {
        match conn.with_guest(|c| exec_one(c, st, read, &mut budget)) {
            Ok(r) => out.push(r),
            Err(e) => {
                if !conn.c.is_autocommit() {
                    let _ = conn.c.execute_batch("ROLLBACK");
                }
                return Err(e);
            }
        }
    }
    if let Err(e) = conn.c.execute_batch("COMMIT") {
        if !conn.c.is_autocommit() {
            let _ = conn.c.execute_batch("ROLLBACK");
        }
        return Err(map_sql_err(e));
    }
    Ok(out)
}

#[async_trait]
impl SqlBackend for SqlHandle {
    async fn execute(&self, stmt: SqlStatement) -> Result<SqlResult> {
        if self.shared.is_purged(self.svc) {
            // Reading a purged service must not re-create its files; writes revive it.
            let scratch = scratch_conn(&self.shared.opts)?;
            match scratch.with_guest(|c| exec_one(c, &stmt, true, &mut ResultBudget::of(&scratch))) {
                Err(StoreError::Sql(m)) if m == READ_ONLY_VIOLATION => self.shared.unpurge(self.svc),
                other => return other,
            }
        }
        // A query tries a reader first; whatever turns out to write (or needs the writer's own
        // connection state, such as temp tables) runs on the writer.
        let stmt = if self.shared.opts.readers_per_db > 0 && looks_like_query(&stmt.sql) {
            let routed = self
                .shared
                .read(self.svc, move |conn| {
                    let mut budget = ResultBudget::of(conn);
                    if conn.role != ConnRole::Reader {
                        // No reader was free to take it: this is the writer already.
                        return conn.with_guest(|c| exec_one(c, &stmt, false, &mut budget)).map(Ok);
                    }
                    Ok(conn.with_guest(|c| exec_on_reader(c, &stmt, &mut budget))?.ok_or(stmt))
                })
                .await?;
            match routed {
                Ok(result) => return Ok(result),
                Err(stmt) => stmt,
            }
        } else {
            stmt
        };
        self.shared
            .with(self.svc, move |conn| {
                conn.with_guest(|c| exec_one(c, &stmt, false, &mut ResultBudget::of(conn)))
            })
            .await
    }

    async fn batch(&self, stmts: Vec<SqlStatement>, mode: BatchMode) -> Result<Vec<SqlResult>> {
        if self.shared.is_purged(self.svc) {
            if mode == BatchMode::Read {
                let scratch = scratch_conn(&self.shared.opts)?;
                return run_batch(&scratch, &stmts, mode);
            }
            self.shared.unpurge(self.svc);
        }
        match mode {
            BatchMode::Read => {
                self.shared
                    .read(self.svc, move |conn| run_batch(conn, &stmts, mode))
                    .await
            }
            BatchMode::Write => {
                self.shared
                    .with(self.svc, move |conn| run_batch(conn, &stmts, mode))
                    .await
            }
        }
    }

    async fn begin(&self) -> Result<Box<dyn SqlTx>> {
        self.shared.unpurge(self.svc);
        let lease = self.shared.lease(self.svc).await?;
        // The lease comes back only once BEGIN ran; abandoned before that, the job releases it
        // and the next writer job rolls the orphaned transaction back.
        let ((), lease) = self
            .shared
            .run_leased(lease, self.svc, true, |conn| {
                conn.c.execute_batch("BEGIN IMMEDIATE").map_err(map_sql_err)
            })
            .await?;
        let lease = lease.ok_or_else(|| StoreError::Internal("transaction lease lost".into()))?; // LCOV_EXCL_LINE: run_leased returns the lease for a job that ran
        let tx = Arc::new(TxShared {
            entry: lease.entry.clone(),
            lease: Mutex::new(Some(lease)),
            done: AtomicBool::new(false),
            timer: Mutex::new(None),
        });
        let timer = {
            let tx = tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(TX_CAP).await;
                let _ = tokio::task::spawn_blocking(move || tx.finish(false)).await;
            }) // LCOV_EXCL_LINE: the timer task's end is observed only after TX_CAP, which the tests do not wait for
        };
        *tx.timer.lock() = Some(timer);
        Ok(Box::new(TxHandle {
            tx,
            runner: self.shared.cfg.runner.clone(),
            tenant: self.shared.tenant_of(self.svc),
        }))
    }

    async fn export(&self) -> Result<Bytes> {
        self.shared.unpurge(self.svc);
        let dir = service_dir(&self.shared.opts.root, self.svc);
        self.shared
            .with(self.svc, move |conn| {
                let tmp = dir.join(format!("export-{}.tmp", unique_name()));
                let res = (|| -> Result<Vec<u8>> {
                    conn.c
                        .execute("VACUUM INTO ?1", [tmp.to_string_lossy().as_ref()])
                        .map_err(map_internal_err)?;
                    // The copy must not announce WAL: it is a standalone file.
                    let copy = Connection::open(&tmp)?;
                    copy.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get::<_, String>(0))?;
                    drop(copy);
                    Ok(std::fs::read(&tmp)?)
                })();
                let _ = std::fs::remove_file(&tmp);
                Ok(Bytes::from(res?))
            })
            .await
    }

    async fn size_bytes(&self) -> Result<u64> {
        if self.shared.is_purged(self.svc) {
            return Ok(0);
        }
        self.shared
            .read(self.svc, move |conn| {
                let pages: i64 = conn
                    .c
                    .query_row("PRAGMA page_count", [], |r| r.get(0))
                    .map_err(map_internal_err)?;
                let size: i64 = conn
                    .c
                    .query_row("PRAGMA page_size", [], |r| r.get(0))
                    .map_err(map_internal_err)?;
                Ok((pages.max(0) as u64) * (size.max(0) as u64))
            })
            .await
    }
}

/// State of one interactive transaction. The lease keeps every other user of the connection out
/// until the transaction ends (commit, rollback, error that ended it, or the 5 s cap).
struct TxShared {
    entry: Arc<ServiceConn>,
    lease: Mutex<Option<Lease>>,
    done: AtomicBool,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TxShared {
    /// Blocking: end the transaction and release the connection.
    fn finish(&self, commit: bool) -> Result<()> {
        let result = {
            let mut g = self.entry.conn.lock();
            if self.done.swap(true, Ordering::AcqRel) {
                Err(StoreError::Sql("transaction is no longer active".into()))
            } else if let Some(conn) = g.as_mut() {
                let r = if commit {
                    conn.c.execute_batch("COMMIT")
                } else if !conn.c.is_autocommit() {
                    conn.c.execute_batch("ROLLBACK")
                } else {
                    Ok(()) // LCOV_EXCL_LINE: execute marks a transaction done as soon as SQLite ends it, so finish never meets an autocommit connection
                };
                match r {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        if !conn.c.is_autocommit() {
                            let _ = conn.c.execute_batch("ROLLBACK");
                        }
                        Err(map_sql_err(e))
                    }
                }
            } else {
                Err(StoreError::Internal("connection closed".into()))
            }
        };
        self.release();
        result
    }

    fn release(&self) {
        if let Some(t) = self.timer.lock().take() {
            t.abort();
        }
        drop(self.lease.lock().take());
    }
}

struct TxHandle {
    tx: Arc<TxShared>,
    runner: Arc<dyn BlockingRunner>,
    /// Fair-queue lane of the owning tenant.
    tenant: Option<TenantId>,
}

async fn run_job<T: Send + 'static>(
    runner: &Arc<dyn BlockingRunner>,
    tenant: Option<TenantId>,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let slot: Arc<Mutex<Option<Result<T>>>> = Arc::new(Mutex::new(None));
    let out = slot.clone();
    runner
        .run(
            tenant,
            Box::new(move || {
                *out.lock() = Some(f());
            }),
        )
        .await;
    let res = slot.lock().take();
    res.unwrap_or_else(|| Err(StoreError::Internal("blocking job did not complete".into())))
}

#[async_trait]
impl SqlTx for TxHandle {
    async fn execute(&mut self, stmt: SqlStatement) -> Result<SqlResult> {
        let tx = self.tx.clone();
        run_job(&self.runner, self.tenant, move || {
            let mut g = tx.entry.conn.lock();
            if tx.done.load(Ordering::Acquire) {
                return Err(StoreError::Sql("transaction is no longer active".into()));
            }
            let Some(conn) = g.as_mut() else {
                return Err(StoreError::Internal("connection closed".into()));
            };
            conn.arm();
            let res = conn.with_guest(|c| exec_one(c, &stmt, false, &mut ResultBudget::of(conn)));
            conn.disarm();
            // A statement that aborted the whole transaction (e.g. the budget interrupt) ends it.
            let ended = conn.c.is_autocommit();
            drop(g);
            if ended && !tx.done.swap(true, Ordering::AcqRel) {
                tx.release();
            }
            res
        })
        .await
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let tx = self.tx.clone();
        run_job(&self.runner, self.tenant, move || tx.finish(true)).await
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        let tx = self.tx.clone();
        run_job(&self.runner, self.tenant, move || tx.finish(false)).await
    }
}

impl Drop for TxHandle {
    fn drop(&mut self) {
        if !self.tx.done.load(Ordering::Acquire) {
            let _ = self.tx.finish(false);
        }
    }
}

// ---- Blob ------------------------------------------------------------------------------------

struct BlobHandle {
    shared: Arc<Shared>,
    svc: ServiceId,
    dir: PathBuf,
}

fn check_blob_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(StoreError::Invalid("blob key is empty".into()));
    }
    if key.len() > MAX_BLOB_KEY_BYTES {
        return Err(StoreError::Invalid(format!(
            "blob key exceeds {MAX_BLOB_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn body_err(e: BodyError) -> StoreError {
    match e {
        BodyError::TooLarge { .. } => StoreError::StorageQuota,
        BodyError::Aborted(m) | BodyError::Io(m) => {
            StoreError::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, m))
        }
    }
}

/// Stream `body` into a new file, enforcing `max_bytes`; returns the byte count.
async fn write_body(path: &Path, mut body: Body, max_bytes: u64) -> Result<u64> {
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await?;
    let mut total: u64 = 0;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(body_err)?;
        total = total.saturating_add(chunk.len() as u64);
        if total > max_bytes {
            return Err(StoreError::StorageQuota);
        }
        f.write_all(&chunk).await?;
    }
    f.flush().await?;
    f.sync_data().await?;
    Ok(total)
}

fn meta_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<BlobMeta> {
    let key: Vec<u8> = r.get(0)?;
    Ok(BlobMeta {
        key: String::from_utf8_lossy(&key).into_owned(),
        size: r.get::<_, i64>(1)?.max(0) as u64,
        content_type: r.get(2)?,
    })
}

#[async_trait]
impl BlobBackend for BlobHandle {
    async fn get(&self, key: &str) -> Result<Option<BlobObject>> {
        check_blob_key(key)?;
        if self.shared.is_purged(self.svc) {
            return Ok(None);
        }
        // An overwrite commits the new file and then deletes the old one, so the file a row named
        // can vanish between reading the row and opening the file: read the row again then, and
        // answer "absent" only when the row is gone (BLOB-005).
        let mut attempts = 0;
        let (meta, f) = loop {
            let k = key.as_bytes().to_vec();
            let row = self
                .shared
                .read(self.svc, move |conn| {
                    conn.c
                        .prepare_cached("SELECT key, size, content_type, file FROM _rt_blobs WHERE key = ?1")
                        .and_then(|mut st| {
                            st.query_row([k], |r| Ok((meta_row(r)?, r.get::<_, String>(3)?)))
                                .optional()
                        })
                        .map_err(map_internal_err)
                })
                .await?;
            let Some((meta, file)) = row else { return Ok(None) };
            match tokio::fs::File::open(self.dir.join("objects").join(file)).await {
                Ok(f) => break (meta, f),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempts < 8 => attempts += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        };
        let stream = futures_util::stream::unfold(Some(f), |st| async move {
            let mut f = st?;
            let mut buf = BytesMut::with_capacity(BLOB_CHUNK);
            match f.read_buf(&mut buf).await {
                Ok(0) => None,
                Ok(_) => Some((Ok(buf.freeze()), Some(f))),
                Err(e) => Some((Err(BodyError::Io(e.kind().to_string())), None)), // LCOV_EXCL_LINE: a read error from an open blob file needs a failing disk
            }
        });
        let size = meta.size;
        Ok(Some(BlobObject {
            meta,
            body: Body::from_stream_sized(stream, size),
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        check_blob_key(key)?;
        if self.shared.is_purged(self.svc) {
            return Ok(None);
        }
        let k = key.as_bytes().to_vec();
        self.shared
            .read(self.svc, move |conn| {
                conn.c
                    .prepare_cached("SELECT key, size, content_type FROM _rt_blobs WHERE key = ?1")
                    .and_then(|mut st| st.query_row([k], meta_row).optional())
                    .map_err(map_internal_err)
            })
            .await
    }

    async fn put(&self, key: &str, body: Body, content_type: Option<String>, max_bytes: u64) -> Result<BlobMeta> {
        check_blob_key(key)?;
        if self.shared.is_retired(self.svc) {
            return Err(StoreError::NotFound);
        }
        if body.exact_len().is_some_and(|n| n > max_bytes) {
            return Err(StoreError::StorageQuota);
        }
        self.shared.unpurge(self.svc);
        let (tmp_dir, obj_dir) = (self.dir.join("tmp"), self.dir.join("objects"));
        for d in [&tmp_dir, &obj_dir] {
            tokio::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(d)
                .await?;
        }
        let file = unique_name();
        let tmp = tmp_dir.join(&file);
        let size = match write_body(&tmp, body, max_bytes).await {
            Ok(n) => n,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(e);
            }
        };
        let dest = obj_dir.join(&file);
        if let Err(e) = tokio::fs::rename(&tmp, &dest).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e.into());
        }
        let meta = BlobMeta {
            key: key.to_string(),
            size,
            content_type: content_type.clone(),
        };
        let (k, f2) = (key.as_bytes().to_vec(), file.clone());
        let job: GroupFn = Box::new(move |c| {
            let old: Option<String> = c
                .query_row("SELECT file FROM _rt_blobs WHERE key = ?1", [&k], |r| r.get(0))
                .optional()
                .map_err(map_internal_err)?;
            c.execute(
                "INSERT INTO _rt_blobs (key, size, content_type, file) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(key) DO UPDATE SET size = excluded.size, content_type = excluded.content_type, file = excluded.file",
                params![k, size as i64, content_type, f2],
            )
            .map_err(map_internal_err)?;
            Ok(GroupOut::Replaced(old))
        });
        let old = match self.shared.write_queued(self.svc, job).await {
            Ok(GroupOut::Replaced(old)) => Ok(old),
            Ok(_) => Err(StoreError::Internal("unexpected write result".into())),
            Err(e) => Err(e),
        };
        match old {
            Ok(old) => {
                if let Some(o) = old {
                    let _ = tokio::fs::remove_file(obj_dir.join(o)).await;
                }
                Ok(meta)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&dest).await;
                Err(e)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        check_blob_key(key)?;
        if self.shared.is_purged(self.svc) {
            return Ok(false);
        }
        let k = key.as_bytes().to_vec();
        let job: GroupFn = Box::new(move |c| {
            let old: Option<String> = c
                .query_row("SELECT file FROM _rt_blobs WHERE key = ?1", [&k], |r| r.get(0))
                .optional()
                .map_err(map_internal_err)?;
            if old.is_some() {
                c.execute("DELETE FROM _rt_blobs WHERE key = ?1", [&k])
                    .map_err(map_internal_err)?;
            }
            Ok(GroupOut::Replaced(old))
        });
        let old = match self.shared.write_queued(self.svc, job).await? {
            GroupOut::Replaced(old) => old,
            _ => return Err(StoreError::Internal("unexpected write result".into())),
        };
        match old {
            Some(file) => {
                let _ = tokio::fs::remove_file(self.dir.join("objects").join(file)).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn list(&self, opts: BlobListOptions) -> Result<BlobPage> {
        let limit = opts.limit.clamp(1, MAX_KV_LIST_LIMIT);
        if self.shared.is_purged(self.svc) {
            return Ok(BlobPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        self.shared
            .read(self.svc, move |conn| {
                let prefix = opts.prefix.into_bytes();
                let (lo, op) = match opts.cursor {
                    Some(cur) if cur.as_bytes() >= prefix.as_slice() => (cur.into_bytes(), ">"),
                    _ => (prefix.clone(), ">="),
                };
                let hi = prefix_end(&prefix);
                let upper = if hi.is_some() { "AND key < ?3" } else { "" };
                let sql = format!(
                    "SELECT key, size, content_type FROM _rt_blobs WHERE key {op} ?1 {upper} ORDER BY key LIMIT ?2"
                );
                let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(lo), Box::new(limit as i64 + 1)];
                if let Some(h) = hi {
                    args.push(Box::new(h));
                }
                let mut items = conn
                    .c
                    .prepare_cached(&sql)
                    .and_then(|mut st| {
                        st.query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), meta_row)?
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(map_internal_err)?;
                let next_cursor = if items.len() > limit {
                    items.truncate(limit);
                    items.last().map(|m| m.key.clone())
                } else {
                    None
                };
                Ok(BlobPage { items, next_cursor })
            })
            .await
    }

    async fn total_bytes(&self) -> Result<u64> {
        if self.shared.is_purged(self.svc) {
            return Ok(0);
        }
        self.shared
            .read(self.svc, move |conn| {
                let n: i64 = conn
                    .c
                    .query_row("SELECT COALESCE(SUM(size), 0) FROM _rt_blobs", [], |r| r.get(0))
                    .map_err(map_internal_err)?;
                Ok(n.max(0) as u64)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn opts(root: &Path) -> OpenOpts {
        OpenOpts {
            root: root.to_path_buf(),
            budget: Duration::from_secs(5),
            sync_budget: Duration::from_millis(100),
            sync_busy: Duration::from_millis(20),
            sync_stmt_cache: 64,
            max_db_bytes: 0,
            mmap_bytes: 0,
            cache_kib: 0,
            readers_per_db: 0,
            max_result_rows: MAX_RESULT_ROWS,
            max_result_bytes: MAX_RESULT_BYTES,
        }
    }

    fn insert(key: &'static str, runs: Arc<AtomicUsize>) -> GroupFn {
        Box::new(move |c| {
            runs.fetch_add(1, Ordering::Relaxed);
            c.execute("INSERT INTO _rt_kv (key, value) VALUES (?1, x'00')", [key.as_bytes()])
                .map_err(map_internal_err)?;
            Ok(GroupOut::Done)
        })
    }

    #[test]
    fn a_job_that_ends_the_group_transaction_fails_alone_and_the_others_run_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let conn = open_conn(
            &opts(dir.path()),
            ServiceId::from_u128(1),
            DbLimit {
                node_max: 0,
                growth: None,
            },
        )
        .expect("open");
        let (runs_a, runs_c) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        // Like an interrupt or a full disk can: the failure rolls back the whole transaction.
        let killer: GroupFn = Box::new(|c| {
            c.execute_batch("ROLLBACK").map_err(map_internal_err)?;
            Err(StoreError::SqlTimeout)
        });
        let mut rxs = Vec::new();
        let mut jobs = Vec::new();
        for (id, f) in [
            (1, insert("a", runs_a.clone())),
            (2, killer),
            (3, insert("c", runs_c.clone())),
        ] {
            let (tx, rx) = tokio::sync::oneshot::channel();
            jobs.push(Queued { id, f, done: tx });
            rxs.push(rx);
        }
        let commits = AtomicU64::new(0);
        run_group(&conn, jobs, &commits);

        let results: Vec<Result<GroupOut>> = rxs
            .into_iter()
            .map(|mut rx| rx.try_recv().expect("every caller is answered"))
            .collect();
        assert!(results[0].is_ok() && results[2].is_ok(), "neighbours succeed");
        assert!(
            matches!(results[1], Err(StoreError::SqlTimeout)),
            "the culprit gets its own error"
        );
        assert_eq!(
            runs_a.load(Ordering::Relaxed),
            2,
            "a ran again in the second transaction"
        );
        assert_eq!(runs_c.load(Ordering::Relaxed), 1);
        assert_eq!(commits.load(Ordering::Relaxed), 1);
        assert!(conn.c.is_autocommit(), "no transaction is left open");
        let n: i64 = conn
            .c
            .query_row("SELECT count(*) FROM _rt_kv", [], |r| r.get(0))
            .expect("count");
        assert_eq!(n, 2, "a and c are committed exactly once");
    }

    #[test]
    fn a_sweep_chunk_deletes_at_most_its_limit() {
        let c = Connection::open_in_memory().expect("open");
        c.execute_batch(SERVICE_DATA_BOOTSTRAP).expect("bootstrap");
        for i in 0..25 {
            c.execute(
                "INSERT INTO _rt_kv (key, value, expires_at) VALUES (?1, x'00', 1)",
                [format!("k{i}").as_bytes()],
            )
            .expect("insert");
        }
        c.execute("INSERT INTO _rt_kv (key, value) VALUES ('keep', x'00')", [])
            .expect("insert");
        assert_eq!(sweep_chunk(&c, 10, 10).expect("chunk"), 10);
        assert_eq!(sweep_chunk(&c, 10, 10).expect("chunk"), 10);
        assert_eq!(sweep_chunk(&c, 10, 10).expect("chunk"), 5);
        assert_eq!(sweep_chunk(&c, 10, 10).expect("chunk"), 0);
        let left: i64 = c
            .query_row("SELECT count(*) FROM _rt_kv", [], |r| r.get(0))
            .expect("count");
        assert_eq!(left, 1);
    }

    #[test]
    fn a_panic_mid_transaction_rolls_it_back_and_an_orphaned_one_is_reset() {
        let c = Connection::open_in_memory().expect("open");
        c.execute_batch("CREATE TABLE t (v INTEGER)").expect("create");
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
                .expect("begin");
            let _unwind = RollbackOnUnwind(&c);
            panic!("job failed mid-batch");
        }));
        assert!(res.is_err());
        assert!(c.is_autocommit(), "the transaction did not survive the panic");
        let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("count");
        assert_eq!(n, 0);

        c.execute_batch("BEGIN; INSERT INTO t VALUES (2);").expect("orphan");
        assert!(
            reset_open_tx(&c, "test"),
            "an open transaction is reported and rolled back"
        );
        assert!(!reset_open_tx(&c, "test"), "nothing to do in autocommit");
        let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).expect("count");
        assert_eq!(n, 0);
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn sanitize_names_the_database_and_drops_any_path() {
        assert_eq!(
            sanitize("open data.db failed at /var/lib/x"),
            "open database failed at ..."
        );
        assert_eq!(sanitize("plain message"), "plain message");
    }

    #[test]
    fn a_non_sqlite_error_from_guest_sql_is_a_sanitized_sql_error() {
        let e = map_sql_err(rusqlite::Error::QueryReturnedNoRows);
        assert!(matches!(&e, StoreError::Sql(m) if !m.is_empty()), "{e:?}");
    }

    #[test]
    fn internal_errors_keep_disk_full_and_interrupts_apart_and_wrap_the_rest() {
        let full = rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL);
        assert!(matches!(
            map_internal_err(rusqlite::Error::SqliteFailure(full, None)),
            StoreError::StorageQuota
        ));
        let interrupt = rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT);
        assert!(matches!(
            map_internal_err(rusqlite::Error::SqliteFailure(interrupt, None)),
            StoreError::SqlTimeout
        ));
        let input = rusqlite::Error::SqlInputError {
            error: rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            msg: "full".into(),
            sql: "INSERT".into(),
            offset: 0,
        };
        assert!(matches!(map_internal_err(input), StoreError::StorageQuota));
        assert!(matches!(
            map_internal_err(rusqlite::Error::QueryReturnedNoRows),
            StoreError::Sqlite(_)
        ));
    }

    #[test]
    fn copying_a_store_error_keeps_its_code() {
        let cases = [
            StoreError::NotFound,
            StoreError::Conflict("c"),
            StoreError::PreconditionFailed,
            StoreError::Invalid("i".into()),
            StoreError::StorageQuota,
            StoreError::Sql("s".into()),
            StoreError::SqlTimeout,
            StoreError::SqlNotAuthorized,
            StoreError::SqlBusy,
            StoreError::SqlResultTooLarge,
            StoreError::KvNotInteger,
            StoreError::KvOverflow,
            StoreError::Corrupt("c".into()),
            StoreError::Internal("n".into()),
            StoreError::WarmFloorTotal { total: 3, cap: 2 },
        ];
        for e in &cases {
            assert_eq!(copy_err(e).code(), e.code(), "{e:?}");
        }
        assert_eq!(
            copy_err(&StoreError::Io(std::io::Error::other("io"))).code(),
            StoreError::Io(std::io::Error::other("x")).code()
        );
        assert_eq!(
            copy_err(&StoreError::Sqlite(rusqlite::Error::QueryReturnedNoRows)).code(),
            "internal"
        );
    }

    #[test]
    fn blob_keys_are_one_to_the_limit_in_bytes() {
        assert!(check_blob_key("").is_err());
        assert!(check_blob_key(&"k".repeat(MAX_BLOB_KEY_BYTES)).is_ok());
        assert!(check_blob_key(&"k".repeat(MAX_BLOB_KEY_BYTES + 1)).is_err());
    }

    #[test]
    fn a_body_that_is_too_large_is_a_quota_and_an_aborted_one_is_an_io_error() {
        assert!(matches!(
            body_err(BodyError::TooLarge { limit: 1 }),
            StoreError::StorageQuota
        ));
        assert!(matches!(body_err(BodyError::Aborted("gone".into())), StoreError::Io(_)));
        assert!(matches!(body_err(BodyError::Io("disk".into())), StoreError::Io(_)));
    }

    #[test]
    fn the_data_config_debug_output_names_its_knobs() {
        let s = format!("{:?}", DataConfig::new("/tmp/rt-data-test"));
        assert!(s.contains("DataConfig") && s.contains("max_open_dbs"), "{s}");
    }
}

#[cfg(test)]
mod purge_edges {
    use super::*;

    #[test]
    fn purging_a_service_whose_directory_is_a_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalServiceData::open(DataConfig::new(dir.path())).expect("open");
        let s = ServiceId::from_u128(9);
        let d = service_dir(dir.path(), s);
        std::fs::create_dir_all(d.parent().expect("parent")).expect("parent directory");
        std::fs::write(&d, b"not a directory").expect("a file where the directory goes");
        assert!(store.purge(s).is_err());
    }
}
