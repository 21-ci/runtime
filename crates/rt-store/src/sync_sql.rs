//! Synchronous SQL on the calling thread (`std:sqlite`, decision D-B1).
//!
//! The async `std:sql` path hands every call to the blocking pool and back: two thread hand-offs,
//! a queue behind other tenants' jobs, and a (de)serialisation of the result, tens of µs per call
//! for a statement SQLite runs in a few. This module serves the same database from the thread
//! that runs the guest. A [`LocalDb`] is one read-write connection to a service's `data.db`,
//! owned by one Machine thread and shared by every isolate of that service on it. JS is
//! synchronous, so two statements of one connection never interleave, and a transaction that has
//! begun is finished (or rolled back by the host) before any other guest code runs.
//!
//! The connection carries the same guards as the pool's guest connections (authorizer: no temp
//! objects, no `ATTACH`, no `_rt_*` tables, no schema-changing pragmas, no guest-issued `BEGIN`;
//! `sqlite3_limit`s; result caps; the `max_page_count` ceiling from the storage quota), and two
//! guards of its own, because a Machine thread cannot wait: a statement budget of
//! `DataConfig::sql_sync_budget` (100 ms) and a `busy_timeout` of `sql_sync_busy` (20 ms) for the
//! pool's writer, after which the statement fails with `sql_busy`.
//!
//! The pool counts each connection in `storage.max_open_dbs`, evicts it with its service when it
//! is idle, and closes it when the service is deleted; the owner reopens it on its next call.
//! Opening (claiming a slot, which may close an idle service's connections; creating the file, WAL,
//! bootstrap, up to 250 ms waiting for a lock) runs on the blocking pool: the Machine thread waits
//! for it at most the statement budget and otherwise fails fast with `sql_busy`, the open going on
//! for its next call. Connections the owner lets go are closed there too.
//! CPU is not metered here: the statement runs inside the task's poll, which the executor already
//! charges to the task.

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use rt_types::ServiceId;
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::{Batch, Connection};

use crate::data::{
    apply_ceiling, map_sql_err, open_rw, precheck, prepare_guest_raw, reset_open_tx, Conn, ConnRole, DbLimit,
    LocalSlot, ResultBudget, ServiceConn, Shared, ROW_OVERHEAD,
};
use crate::error::StoreError;

/// A value bound to a parameter or read from a column, borrowed (SQLite's own types; text is the
/// raw UTF-8 bytes).
pub use rusqlite::types::ValueRef as SqlRef;

/// A failed synchronous statement: the store error (stable `code()`, sanitized message) plus what
/// SQLite said, for `SQLiteError.errno` / `.code` / `.byteOffset`.
#[derive(Debug)]
pub struct LocalError {
    pub error: StoreError,
    /// SQLite's extended result code; 0 when the failure was not SQLite's.
    pub errno: i32,
    /// Byte offset of a syntax error in the SQL text; -1 when unknown.
    pub offset: i32,
}

impl LocalError {
    fn from_sqlite(e: rusqlite::Error) -> Self {
        let (errno, offset) = match &e {
            rusqlite::Error::SqliteFailure(f, _) => (f.extended_code, -1),
            rusqlite::Error::SqlInputError { error, offset, .. } => (error.extended_code, *offset),
            _ => (0, -1),
        };
        Self {
            error: map_sql_err(e),
            errno,
            offset,
        }
    }

    /// The stable error code (`sql_error`, `sql_timeout`, `sql_busy`, `sql_not_authorized`,
    /// `sql_result_too_large`, `storage_quota`, …).
    pub fn code(&self) -> &'static str {
        self.error.code()
    }

    /// SQLite's symbolic name of the failure (`SQLITE_CONSTRAINT_UNIQUE`, `SQLITE_BUSY`, …).
    pub fn sqlite_name(&self) -> &'static str {
        let primary = match self.errno & 0xff {
            1 => "SQLITE_ERROR",
            2 => "SQLITE_INTERNAL",
            3 => "SQLITE_PERM",
            4 => "SQLITE_ABORT",
            5 => "SQLITE_BUSY",
            6 => "SQLITE_LOCKED",
            7 => "SQLITE_NOMEM",
            8 => "SQLITE_READONLY",
            9 => "SQLITE_INTERRUPT",
            10 => "SQLITE_IOERR",
            11 => "SQLITE_CORRUPT",
            13 => "SQLITE_FULL",
            14 => "SQLITE_CANTOPEN",
            17 => "SQLITE_SCHEMA",
            18 => "SQLITE_TOOBIG",
            19 => "SQLITE_CONSTRAINT",
            20 => "SQLITE_MISMATCH",
            21 => "SQLITE_MISUSE",
            23 => "SQLITE_AUTH",
            25 => "SQLITE_RANGE",
            _ => "",
        };
        match self.errno {
            275 => "SQLITE_CONSTRAINT_CHECK",
            531 => "SQLITE_CONSTRAINT_COMMITHOOK",
            787 => "SQLITE_CONSTRAINT_FOREIGNKEY",
            1043 => "SQLITE_CONSTRAINT_FUNCTION",
            1299 => "SQLITE_CONSTRAINT_NOTNULL",
            1555 => "SQLITE_CONSTRAINT_PRIMARYKEY",
            1811 => "SQLITE_CONSTRAINT_TRIGGER",
            2067 => "SQLITE_CONSTRAINT_UNIQUE",
            2323 => "SQLITE_CONSTRAINT_VTAB",
            2579 => "SQLITE_CONSTRAINT_ROWID",
            517 => "SQLITE_BUSY_SNAPSHOT",
            261 => "SQLITE_BUSY_RECOVERY",
            _ if !primary.is_empty() => primary,
            _ => match self.error {
                StoreError::SqlTimeout => "SQLITE_INTERRUPT",
                StoreError::SqlBusy => "SQLITE_BUSY",
                StoreError::SqlNotAuthorized => "SQLITE_AUTH",
                StoreError::StorageQuota => "SQLITE_FULL",
                StoreError::SqlResultTooLarge => "SQLITE_TOOBIG",
                _ => "SQLITE_ERROR",
            },
        }
    }

    /// Sanitized message (never a host path).
    pub fn message(&self) -> String {
        match &self.error {
            StoreError::Sql(m) => m.clone(),
            other => other.to_string(),
        }
    }
}

impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for LocalError {}

impl From<StoreError> for LocalError {
    fn from(error: StoreError) -> Self {
        Self {
            error,
            errno: 0,
            offset: -1,
        }
    }
}

/// What a prepared statement looks like.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtInfo {
    pub columns: Vec<String>,
    /// One entry per parameter, in index order: its name with the prefix (`$a`, `:a`, `@a`, `?1`),
    /// `None` for an anonymous `?`.
    pub param_names: Vec<Option<String>>,
    /// The statement does not write.
    pub readonly: bool,
}

/// What a finished statement did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Rows the statement returned.
    pub rows: usize,
    /// Rows the statement changed (0 for one that changed nothing).
    pub changes: u64,
    /// The connection's last inserted rowid.
    pub last_insert_rowid: i64,
}

/// A row as the statement produced it, valid inside the row callback.
pub struct RowRef<'r, 's>(&'r rusqlite::Row<'s>);

impl RowRef<'_, '_> {
    /// Columns of this row (read from the statement as it ran: a schema change another connection
    /// made since it was prepared is already in effect).
    pub fn len(&self) -> usize {
        self.0.as_ref().column_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Name of column `i`.
    pub fn name(&self, i: usize) -> &str {
        self.0.as_ref().column_name(i).unwrap_or("")
    }

    /// Column `i`.
    pub fn get(&self, i: usize) -> SqlRef<'_> {
        self.0.get_ref(i).unwrap_or(SqlRef::Null)
    }
}

/// How a transaction step is issued ([`LocalDb::tx`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxMode {
    Deferred,
    Immediate,
    Exclusive,
}

/// Opens [`LocalDb`]s: a store that can serve SQL on the calling thread.
pub trait LocalSqlSource: Send + Sync {
    /// Identity of the store (a registry on a thread keys its connections by it, so two stores
    /// with the same service ids never mix).
    fn id(&self) -> usize;
    /// A connection of `svc`'s database for the calling thread, opened now (the file is created
    /// and bootstrapped if the service never used its database).
    fn open(&self, svc: ServiceId) -> Result<LocalDb, LocalError>;
}

/// [`LocalSqlSource`] of the local-disk store.
pub struct LocalSource {
    shared: Arc<Shared>,
}

impl LocalSource {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl LocalSqlSource for LocalSource {
    fn id(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    fn open(&self, svc: ServiceId) -> Result<LocalDb, LocalError> {
        // Opening a database for writing revives a purged service, like any write.
        self.shared.unpurge(svc);
        let db = LocalDb {
            shared: self.shared.clone(),
            svc,
            entry: self.shared.entry(svc),
            slot: LocalSlot::new(),
            ops: Cell::new(0),
            in_call: Cell::new(false),
            opening: RefCell::new(None),
        };
        match db.connect() {
            // Still opening on the blocking pool: the first statement waits for it (or fails fast).
            Err(e) if db.opening.borrow().is_some() && matches!(e.error, StoreError::SqlBusy) => Ok(db),
            Err(e) => Err(e),
            Ok(()) => Ok(db),
        }
    }
}

/// One read-write connection of a service database, owned by the calling Machine thread.
///
/// All methods take `&self`; the connection sits behind a mutex only so the pool can close it
/// from another thread (eviction, deletion) without ever waiting for a statement.
pub struct LocalDb {
    shared: Arc<Shared>,
    svc: ServiceId,
    entry: Arc<ServiceConn>,
    slot: Arc<LocalSlot>,
    ops: Cell<u32>,
    /// A call is running on the connection (its lock is held): a nested call is refused, never a
    /// self-deadlock.
    in_call: Cell<bool>,
    /// The open running on the blocking pool ([`LocalDb::connect`]).
    opening: RefCell<Option<mpsc::Receiver<Result<Conn, StoreError>>>>,
}

/// Run `f` off the calling (Machine) thread: on the blocking pool of the current runtime, or a
/// thread of its own outside one.
fn off_thread(f: impl FnOnce() + Send + 'static) {
    match tokio::runtime::Handle::try_current() {
        Ok(h) => drop(h.spawn_blocking(f)),
        Err(_) => {
            let _ = std::thread::Builder::new().name("rt-sqlite-open".into()).spawn(f);
        }
    }
}

/// Close a connection off the Machine thread (the last connection of a database checkpoints it).
fn dispose(conn: Conn) {
    off_thread(move || drop(conn));
}

/// The open of a Machine-local connection, on the blocking pool: claim a slot (closing the least
/// recently used idle service when the node is at `max_open_dbs`) and open the connection.
fn open_local(
    shared: &Arc<Shared>,
    svc: ServiceId,
    entry: &Arc<ServiceConn>,
    slot: &Arc<LocalSlot>,
) -> Result<Conn, StoreError> {
    if shared.is_retired(svc) {
        return Err(StoreError::NotFound); // LCOV_EXCL_LINE: a purge racing the open is refused here; connect's purged check runs first for every other open
    }
    if !shared.claim_local(svc, entry, slot) {
        // Every connection slot of the node is in a statement right now.
        return Err(StoreError::SqlBusy);
    }
    match open_rw(&shared.opts, svc, shared.db_limit(svc), ConnRole::Local) {
        Ok(c) => Ok(c),
        Err(e) => {
            shared.release_local(svc, entry, slot);
            Err(match e {
                StoreError::Sqlite(e) => map_sql_err(e),
                other => other,
            })
        }
    }
}

/// Marks a [`LocalDb`] call in progress for its duration.
struct InCall<'a>(&'a Cell<bool>);

impl Drop for InCall<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

fn nested_error() -> LocalError {
    StoreError::Sql("nested call: the connection is in use by a statement".into()).into()
}

/// Attempts to find the connection open (the pool may close it between two attempts).
const OPEN_ATTEMPTS: usize = 3;

impl LocalDb {
    pub fn service(&self) -> ServiceId {
        self.svc
    }

    /// Make sure the connection is open: claim a slot (idle services are evicted if the node is at
    /// `max_open_dbs`) and open it, both on the blocking pool. Waits for that at most the statement
    /// budget, then fails fast with `sql_busy`; the open goes on and a later call installs it.
    fn connect(&self) -> Result<(), LocalError> {
        if self.slot.conn.lock().is_some() {
            return Ok(()); // LCOV_EXCL_LINE: defensive: every caller reaches connect() with no connection installed
        }
        let pending = self.opening.borrow_mut().take();
        let rx = match pending {
            Some(rx) => rx,
            None => {
                // A deleted service's database is not recreated by a connection that outlived it:
                // opening one again is an explicit `LocalSqlSource::open`.
                if self.shared.is_purged(self.svc) {
                    return Err(StoreError::Sql("the database was deleted".into()).into());
                }
                self.start_open()
            }
        };
        match rx.recv_timeout(self.shared.opts.sync_budget) {
            Ok(Ok(conn)) => {
                let mut g = self.slot.conn.lock();
                // Deregistered while it opened (its service was evicted or deleted): not installed;
                // the caller tries again.
                if self.slot.doomed.swap(false, Ordering::AcqRel) {
                    drop(g); // LCOV_EXCL_LINE: a purge that lands between the slot claim and the install: a race with the background open
                    dispose(conn); // LCOV_EXCL_LINE: a purge that lands between the slot claim and the install: a race with the background open
                } else {
                    *g = Some(conn);
                }
                Ok(())
            }
            Ok(Err(e)) => Err(e.into()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                *self.opening.borrow_mut() = Some(rx);
                Err(StoreError::SqlBusy.into())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(StoreError::Internal("the open was abandoned".into()).into())
            } // LCOV_EXCL_LINE: the open thread drops its sender only by panicking
        }
    }

    /// Start opening the connection on the blocking pool. A result nobody takes any more (the
    /// owner was dropped) gives its slot back.
    fn start_open(&self) -> mpsc::Receiver<Result<Conn, StoreError>> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (shared, svc, entry, slot) = (self.shared.clone(), self.svc, self.entry.clone(), self.slot.clone());
        off_thread(move || {
            let r = open_local(&shared, svc, &entry, &slot);
            if let Err(mpsc::SendError(Ok(conn))) = tx.send(r) {
                // LCOV_EXCL_START: an open whose LocalDb was dropped first: a race with the background open
                drop(conn);
                shared.release_local(svc, &entry, &slot);
                // LCOV_EXCL_STOP
            }
        });
        rx
    }

    /// Whether the connection is open or being opened (a connection the pool closed and nothing
    /// reopens is not: the owner opens a new one).
    pub fn is_live(&self) -> bool {
        self.opening.borrow().is_some() || self.is_open()
    }

    /// Run `f` on the open connection.
    fn with_conn<R>(&self, f: impl FnOnce(&mut Conn) -> Result<R, LocalError>) -> Result<R, LocalError> {
        if self.in_call.replace(true) {
            return Err(nested_error());
        }
        let _call = InCall(&self.in_call);
        for _ in 0..OPEN_ATTEMPTS {
            {
                let mut g = self.slot.conn.lock();
                if g.is_none() && self.slot.tx_depth.load(Ordering::Acquire) > 0 {
                    // The pool closed the connection under an open transaction (its service was
                    // deleted): its changes are gone. Never reconnect into autocommit and run
                    // the rest of the transaction's statements one by one.
                    self.lose_tx();
                    return Err(tx_lost_error());
                }
                if let Some(conn) = g.as_mut() {
                    self.touch();
                    let r = f(conn);
                    conn.disarm();
                    if self.slot.doomed.load(Ordering::Acquire) {
                        // The pool closed this service while the statement ran.
                        if let Some(c) = g.take() {
                            dispose(c);
                        }
                        if self.slot.tx_depth.load(Ordering::Acquire) > 0 {
                            self.lose_tx();
                        }
                    }
                    return r;
                }
            }
            self.connect()?;
        }
        Err(StoreError::SqlBusy.into()) // LCOV_EXCL_LINE: every retry finds the connection closed again; needs the pool to close it on each attempt
    }

    /// The open transaction is gone with its connection: no level is open any more, and the next
    /// `commit` reports the loss (see [`LocalSlot::tx_lost`]).
    fn lose_tx(&self) {
        self.slot.tx_lost.store(true, Ordering::Release);
        self.slot.tx_depth.store(0, Ordering::Release);
    }

    /// Mark the service recently used (its eviction order), every 64th call.
    fn touch(&self) {
        let n = self.ops.get().wrapping_add(1);
        self.ops.set(n);
        if n & 63 == 1 {
            self.entry
                .tick
                .store(self.shared.clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    /// Prepare `sql` (through the statement cache) and describe it.
    pub fn prepare(&self, sql: &str) -> Result<StmtInfo, LocalError> {
        self.with_conn(|conn| {
            precheck(sql)?;
            conn.with_guest(|c| {
                let prep = prepare_guest_raw(c, sql).map_err(LocalError::from_sqlite)?;
                let columns = prep.column_names().into_iter().map(str::to_string).collect();
                let param_names = (1..=prep.parameter_count())
                    .map(|i| prep.parameter_name(i).map(str::to_string))
                    .collect();
                Ok(StmtInfo {
                    columns,
                    param_names,
                    readonly: prep.readonly(),
                })
            })
        })
    }

    /// Run one statement with positional `args`, calling `on_row` for each row it returns until
    /// that answers `false`. Row and byte caps apply (`sql_result_too_large`); the statement budget
    /// bounds the whole call.
    pub fn run(
        &self,
        sql: &str,
        args: &[SqlRef<'_>],
        on_row: &mut dyn FnMut(RowRef<'_, '_>) -> Result<bool, LocalError>,
    ) -> Result<RunStats, LocalError> {
        self.with_conn(|conn| {
            precheck(sql)?;
            let mut budget = ResultBudget::of(conn);
            conn.with_guest(|c| run_statement(c, conn, sql, args, &mut budget, on_row))
        })
    }

    /// Run `sql`, which may hold several statements and no parameters.
    pub fn exec_batch(&self, sql: &str) -> Result<RunStats, LocalError> {
        self.with_conn(|conn| {
            precheck(sql)?;
            let before = conn.c.total_changes();
            // One statement at a time, each checked before it runs: `precheck` of the whole text
            // sees only its first statement (`SELECT 1; VACUUM INTO '...'`).
            conn.with_guest(|c| -> Result<(), LocalError> {
                let mut batch = Batch::new(c, sql);
                while let Some(mut stmt) = batch.next().map_err(LocalError::from_sqlite)? {
                    let text = stmt.expanded_sql().ok_or(StoreError::SqlNotAuthorized)?;
                    precheck(&text)?;
                    // Like `execute_batch`: one step (a statement may return rows, e.g. a PRAGMA).
                    stmt.raw_query().next().map_err(LocalError::from_sqlite)?;
                }
                Ok(())
            })?;
            Ok(RunStats {
                rows: 0,
                changes: if conn.c.total_changes() > before {
                    conn.c.changes()
                } else {
                    0
                },
                last_insert_rowid: conn.c.last_insert_rowid(),
            })
        })
    }

    /// Begin a transaction: `BEGIN` at depth 0, else a savepoint nested in the open one. Guests
    /// cannot issue `BEGIN` / `SAVEPOINT` themselves (the authorizer denies them): this is how
    /// `db.transaction(fn)` does it, with the lock held only for the synchronous run of `fn`.
    pub fn begin(&self, mode: TxMode) -> Result<(), LocalError> {
        self.with_conn(|conn| {
            let depth = self.slot.tx_depth.load(Ordering::Acquire);
            if depth == 0 {
                self.slot.tx_lost.store(false, Ordering::Release);
            }
            let sql = if depth == 0 {
                match mode {
                    TxMode::Deferred => "BEGIN DEFERRED".to_string(),
                    TxMode::Immediate => "BEGIN IMMEDIATE".to_string(),
                    TxMode::Exclusive => "BEGIN EXCLUSIVE".to_string(),
                }
            } else {
                format!("SAVEPOINT rt_sp{depth}")
            };
            conn.arm();
            let r = conn.c.execute_batch(&sql);
            if r.is_ok() {
                self.slot.tx_depth.store(depth + 1, Ordering::Release);
            }
            r.map_err(LocalError::from_sqlite)
        })
    }

    /// End the innermost transaction level by keeping its changes: `COMMIT`, or `RELEASE` of a
    /// savepoint. A `COMMIT` that fails rolls the transaction back before it reports the error.
    pub fn commit(&self) -> Result<(), LocalError> {
        self.end_tx(true)
    }

    /// End the innermost transaction level by discarding its changes (`ROLLBACK`, or `ROLLBACK TO`
    /// and release of its savepoint). Nothing to do when an error already ended the transaction.
    pub fn rollback(&self) -> Result<(), LocalError> {
        self.end_tx(false)
    }

    fn end_tx(&self, commit: bool) -> Result<(), LocalError> {
        if self.in_call.get() {
            return Err(nested_error());
        }
        {
            let g = self.slot.conn.lock();
            if g.is_none() && self.slot.tx_depth.load(Ordering::Acquire) > 0 {
                self.lose_tx();
            }
        }
        if self.slot.tx_lost.load(Ordering::Acquire) {
            // The transaction went with its connection: nothing to roll back, and nothing of it
            // can be committed.
            return if commit { Err(tx_lost_error()) } else { Ok(()) };
        }
        self.with_conn(|conn| {
            let depth = self.slot.tx_depth.load(Ordering::Acquire);
            if depth == 0 || conn.c.is_autocommit() {
                // Nothing open (an interrupt or a full disk ended the transaction already).
                self.slot.tx_depth.store(0, Ordering::Release);
                return if commit && depth > 0 {
                    Err(StoreError::Sql("the transaction was ended by an earlier error".into()).into())
                } else {
                    Ok(())
                };
            }
            let level = depth - 1;
            self.slot.tx_depth.store(level, Ordering::Release);
            let sql = match (level, commit) {
                (0, true) => "COMMIT".to_string(),
                (0, false) => "ROLLBACK".to_string(),
                (n, true) => format!("RELEASE rt_sp{n}"),
                (n, false) => format!("ROLLBACK TO rt_sp{n}; RELEASE rt_sp{n}"),
            };
            conn.arm();
            let r = conn.c.execute_batch(&sql);
            if r.is_err() {
                // A failed COMMIT / RELEASE leaves the level open: undo it, so the connection is
                // never left in a transaction the guest believes is over.
                if level == 0 {
                    if !conn.c.is_autocommit() {
                        let _ = conn.c.execute_batch("ROLLBACK");
                    }
                // LCOV_EXCL_START: a RELEASE of a savepoint above depth one does not fail in process
                } else {
                    let _ = conn
                        .c
                        .execute_batch(&format!("ROLLBACK TO rt_sp{level}; RELEASE rt_sp{level}"));
                }
                // LCOV_EXCL_STOP
            }
            r.map_err(LocalError::from_sqlite)
        })
    }

    /// Nesting depth of the transaction begun through this connection (0 = none).
    pub fn tx_depth(&self) -> u32 {
        self.slot.tx_depth.load(Ordering::Acquire)
    }

    /// A transaction is open on the connection (`db.inTransaction`).
    pub fn in_transaction(&self) -> bool {
        if self.in_call.get() {
            return self.may_have_open_tx();
        }
        self.slot.conn.lock().as_ref().is_some_and(|c| !c.c.is_autocommit())
    }

    /// A transaction may have been begun through this connection and not ended (a cheap flag: the
    /// executor asks after every poll).
    pub fn may_have_open_tx(&self) -> bool {
        self.slot.tx_depth.load(Ordering::Acquire) > 0
    }

    /// Roll back a transaction the guest's code never ended (the task was killed between `BEGIN`
    /// and `COMMIT`, where no JS `finally` runs): the connection goes back to autocommit and the
    /// write lock is released. `true`: one was open.
    pub fn reset_open_tx(&self) -> bool {
        if self.in_call.get() {
            return false;
        }
        self.slot.tx_lost.store(false, Ordering::Release);
        if self.slot.tx_depth.swap(0, Ordering::AcqRel) == 0 {
            return false;
        }
        self.slot
            .conn
            .lock()
            .as_ref()
            .is_some_and(|c| reset_open_tx(&c.c, "local"))
    }

    /// Set the database's size ceiling (`max_page_count`) from the storage quota left to the
    /// tenant: its current size plus `growth` (`None`: no plan limit), within the node-wide cap.
    /// Called before a task's first write; the pool's own refresh after each storage scan reaches
    /// idle connections too.
    pub fn refresh_ceiling(&self, growth: Option<u64>) -> Result<(), LocalError> {
        self.with_conn(|conn| {
            let limit = DbLimit {
                node_max: self.shared.opts.max_db_bytes,
                growth,
            };
            apply_ceiling(&conn.c, limit).map_err(LocalError::from_sqlite)
        })
    }

    /// Whether the connection is open right now (it is closed when the pool evicts or deletes
    /// the service; the next call reopens it).
    pub fn is_open(&self) -> bool {
        self.in_call.get() || self.slot.conn.lock().is_some()
    }

    /// Close the connection (on the blocking pool) and give its slot back (the owner is done with
    /// it). An open still running gives its slot back when it ends.
    pub fn close(&self) {
        if self.in_call.get() {
            return;
        }
        self.reset_open_tx();
        if let Some(rx) = self.opening.borrow_mut().take() {
            if let Ok(Ok(conn)) = rx.try_recv() {
                dispose(conn);
            }
        }
        if let Some(c) = self.slot.conn.lock().take() {
            dispose(c);
        }
        self.shared.release_local(self.svc, &self.entry, &self.slot);
    }
}

impl Drop for LocalDb {
    fn drop(&mut self) {
        self.close();
    }
}

/// Memory one column value takes in a materialized result (same accounting as the async path).
fn ref_bytes(v: &SqlRef<'_>) -> usize {
    std::mem::size_of::<crate::data::SqlValue>()
        + match v {
            SqlRef::Text(t) => t.len(),
            SqlRef::Blob(b) => b.len(),
            _ => 0,
        }
}

fn run_statement(
    c: &Connection,
    conn: &Conn,
    sql: &str,
    args: &[SqlRef<'_>],
    budget: &mut ResultBudget,
    on_row: &mut dyn FnMut(RowRef<'_, '_>) -> Result<bool, LocalError>,
) -> Result<RunStats, LocalError> {
    let mut prep = prepare_guest_raw(c, sql).map_err(LocalError::from_sqlite)?;
    for (i, arg) in args.iter().enumerate() {
        prep.raw_bind_parameter(i + 1, rusqlite::types::ToSqlOutput::Borrowed(*arg))
            .map_err(LocalError::from_sqlite)?;
    }
    let ncols = prep.column_count();
    let before = c.total_changes();
    let mut rows_seen = 0usize;
    if ncols == 0 {
        prep.raw_execute().map_err(LocalError::from_sqlite)?;
    } else {
        let mut rows = prep.raw_query();
        while let Some(row) = rows.next().map_err(LocalError::from_sqlite)? {
            if rows_seen >= budget.rows {
                return Err(StoreError::SqlResultTooLarge.into());
            }
            let mut bytes = ROW_OVERHEAD;
            for i in 0..row.as_ref().column_count() {
                bytes += ref_bytes(&row.get_ref(i).map_err(LocalError::from_sqlite)?);
            }
            budget.bytes_left = budget
                .bytes_left
                .checked_sub(bytes)
                .ok_or(StoreError::SqlResultTooLarge)?;
            rows_seen += 1;
            // Building the guest's row is not the statement's time (the result caps bound it).
            let started = Instant::now();
            let more = on_row(RowRef(row))?;
            conn.credit(started.elapsed());
            if !more {
                break;
            }
        }
    }
    Ok(RunStats {
        rows: rows_seen,
        changes: if c.total_changes() > before { c.changes() } else { 0 },
        last_insert_rowid: c.last_insert_rowid(),
    })
}

/// The error of a statement or `commit` in a transaction the pool closed under the guest.
fn tx_lost_error() -> LocalError {
    StoreError::Sql("the transaction was lost: its connection was closed (no change was kept)".into()).into()
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn a_failure_without_an_sqlite_code_has_no_errno_or_offset() {
        let e = LocalError::from_sqlite(rusqlite::Error::QueryReturnedNoRows);
        assert_eq!((e.errno, e.offset), (0, -1));
    }
}
