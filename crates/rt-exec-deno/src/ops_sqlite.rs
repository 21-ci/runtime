//! Ops behind `std:sqlite` (`js/sqlite.js`): SQL on the Machine thread,
//! no event-loop hop and no promise.
//!
//! One read-write connection per (service, Machine thread) lives in a thread-local registry and is
//! shared by every isolate of the service on that thread (`rt_store::LocalDb`: guest authorizer,
//! `sqlite3_limit`s, result caps, a 100 ms statement budget, a short busy timeout against the
//! pool's writer, counted in `storage.max_open_dbs`). A statement runs inside the task's poll, so
//! the executor already charges its thread CPU to the task; nothing is charged here.
//!
//! No guest code runs inside these ops: they hold the isolate's `OpState` borrow and the
//! connection's lock, so a guest callback that re-entered one would abort the node (a second
//! `OpState` borrow) or deadlock the Machine thread. JS snapshots the arguments first
//! (`snapshotBinds`), the ops read only primitives and buffers, and results are built from own
//! data properties (no inherited setter runs); `LocalDb` refuses a nested call besides.
//!
//! JS is synchronous, so no two statements interleave and a transaction that has begun ends (or
//! throws) before anything else runs. The one way out is a termination (CPU / wall / memory limit,
//! shutdown): V8 stops the guest between two ops and no JS `finally` runs. The executor therefore
//! calls [`settle_transactions`] after every poll, which rolls back what the guest left open.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use deno_core::{op2, v8, OpState};
use deno_error::JsErrorBox;
use rt_store::{LocalDb, LocalError, RunStats, SqlRef, TxMode};
use rt_types::{ServiceId, TaskId};
use serde::Serialize;

use crate::ops::TaskOps;
use crate::v8sql::{binds_of, cell_to_v8, object_of, Bind};

/// Result of a call: `{ changes, lastInsertRowid }` (run), the rows as objects (all / get) or as
/// arrays (values).
pub const MODE_ALL: u32 = 0;
pub const MODE_GET: u32 = 1;
pub const MODE_VALUES: u32 = 2;
pub const MODE_RUN: u32 = 3;

/// Integers come back as `bigint`.
pub const FLAG_SAFE_INTEGERS: u32 = 1;
/// The statement may write: the database's size ceiling is set from the tenant's storage quota
/// before the task's first write.
pub const FLAG_WRITE: u32 = 2;

/// Past this many services a Machine forgets the connections that are closed (their pool slot is
/// already free).
const REGISTRY_PRUNE_AT: usize = 256;

thread_local! {
    /// The Machine thread's connections, by (store, service). Held for the thread's life: the pool
    /// evicts them when they are idle and the owner reopens on its next call.
    static CONNS: RefCell<HashMap<(usize, ServiceId), Rc<LocalDb>>> = RefCell::new(HashMap::new());
}

/// Per-isolate: the connection of the isolate's service and what has been done for this task.
struct SqliteState {
    db: Rc<LocalDb>,
    /// The task the database's size ceiling was last set for.
    ceiling_for: Option<TaskId>,
}

// LCOV_EXCL_START: only unreachable defensive errors use it (the JS side opens before it queries, ops run inside a task)
fn code_err(code: &'static str) -> JsErrorBox {
    JsErrorBox::new("Error", code)
}
// LCOV_EXCL_STOP

/// A failed statement as the error JS turns into a `SQLiteError`:
/// `@sqlite:<rtCode>:<SQLITE_NAME>:<errno>:<byteOffset>:<message>`.
fn sql_err(e: LocalError) -> JsErrorBox {
    let (code, name, message) = (e.code(), e.sqlite_name(), e.message());
    JsErrorBox::new(
        "Error",
        format!("@sqlite:{code}:{name}:{}:{}:{message}", e.errno, e.offset),
    )
}

fn db_of(state: &OpState) -> Result<Rc<LocalDb>, JsErrorBox> {
    state
        .try_borrow::<SqliteState>()
        .map(|s| s.db.clone())
        .ok_or_else(|| code_err("sqlite_not_open"))
}

/// `new Database()`: attach the service's connection (opened now when this Machine has none).
#[op2(nofast)]
pub fn op_rt_sqlite_open(state: &mut OpState) -> Result<(), JsErrorBox> {
    let host = state
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| code_err("no_task_context"))?;
    let source = host
        .services()
        .deps()
        .data
        .local_sql()
        .ok_or_else(|| code_err("not_supported"))?;
    let key = (source.id(), host.service());
    let known = CONNS.with(|c| c.borrow().get(&key).cloned());
    let db = match known {
        // A connection the pool closed for good (the service was deleted) is opened afresh; one
        // still opening on the blocking pool is kept (its first statement waits for it).
        Some(db) if db.is_live() => db,
        _ => {
            let db = Rc::new(source.open(key.1).map_err(sql_err)?);
            CONNS.with(|c| {
                let mut c = c.borrow_mut();
                if c.len() >= REGISTRY_PRUNE_AT {
                    c.retain(|_, d| Rc::strong_count(d) > 1 || d.is_live());
                }
                c.insert(key, db.clone());
            });
            db
        }
    };
    state.put(SqliteState { db, ceiling_for: None });
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedOut {
    columns: Vec<String>,
    /// Per parameter: its name with the prefix (`$a`), `null` for an anonymous `?`.
    param_names: Vec<Option<String>>,
    readonly: bool,
}

/// Compile `sql` through the connection's statement cache and describe it.
#[op2]
#[serde]
pub fn op_rt_sqlite_prepare(state: &mut OpState, #[string] sql: String) -> Result<PreparedOut, JsErrorBox> {
    let info = db_of(state)?.prepare(&sql).map_err(sql_err)?;
    Ok(PreparedOut {
        columns: info.columns,
        param_names: info.param_names,
        readonly: info.readonly,
    })
}

/// Before a task's first write: lower the database's size ceiling to what is left of the
/// tenant's storage quota.
fn ensure_ceiling(state: &mut OpState) -> Result<(), JsErrorBox> {
    let (task, left) = match state.try_borrow::<TaskOps>() {
        Some(t) => (t.task, t.host.storage_left()),
        None => return Err(code_err("no_task_context")), // LCOV_EXCL_LINE: ops run only inside a task
    };
    let Some(st) = state.try_borrow_mut::<SqliteState>() else {
        return Err(code_err("sqlite_not_open"));
    };
    if st.ceiling_for != Some(task) {
        st.db.refresh_ceiling(left).map_err(sql_err)?;
        st.ceiling_for = Some(task);
    }
    Ok(())
}

fn run_result<'s>(scope: &mut v8::PinScope<'s, '_>, stats: RunStats, safe: bool) -> v8::Local<'s, v8::Value> {
    let changes = v8::Number::new(scope, stats.changes as f64);
    let rowid = cell_to_v8(scope, SqlRef::Integer(stats.last_insert_rowid), safe);
    object_of(scope, &[("changes", changes.into()), ("lastInsertRowid", rowid)]).into()
}

/// Run a statement with positional `args` (a JS array; named parameters are resolved to positions
/// in JS). `mode`: [`MODE_ALL`] rows as objects, [`MODE_GET`] the first row or `null`,
/// [`MODE_VALUES`] rows as arrays, [`MODE_RUN`] `{ changes, lastInsertRowid }`.
#[op2]
pub fn op_rt_sqlite_run<'a>(
    scope: &mut v8::PinScope<'a, '_>,
    state: &mut OpState,
    #[string] sql: &str,
    args: v8::Local<'a, v8::Value>,
    #[smi] mode: u32,
    #[smi] flags: u32,
) -> Result<v8::Local<'a, v8::Value>, JsErrorBox> {
    let db = db_of(state)?;
    if flags & FLAG_WRITE != 0 {
        ensure_ceiling(state)?;
    }
    let safe = flags & FLAG_SAFE_INTEGERS != 0;
    let binds = binds_of(scope, args)?;
    let refs: Vec<SqlRef<'_>> = binds.iter().map(Bind::as_ref).collect();

    if mode == MODE_RUN {
        let stats = db.run(sql, &refs, &mut |_| Ok(true)).map_err(sql_err)?;
        return Ok(run_result(scope, stats, safe));
    }

    // Column names are read once per call and interned, so every row object shares one shape.
    let mut keys: Vec<v8::Local<'a, v8::Name>> = Vec::new();
    let mut duplicate_names = false;
    let mut proto: Option<v8::Local<'a, v8::Value>> = None;
    let mut rows: Vec<v8::Local<'a, v8::Value>> = Vec::new();
    let mut vals: Vec<v8::Local<'a, v8::Value>> = Vec::new();
    db.run(sql, &refs, &mut |row| {
        let n = row.len();
        vals.clear();
        for i in 0..n {
            vals.push(cell_to_v8(scope, row.get(i), safe));
        }
        if mode == MODE_VALUES {
            rows.push(v8::Array::new_with_elements(scope, &vals).into());
            return Ok(true);
        }
        if keys.len() != n {
            keys.clear();
            let mut seen: Vec<&str> = Vec::with_capacity(n);
            for i in 0..n {
                let name = row.name(i);
                duplicate_names |= seen.contains(&name);
                seen.push(name);
                let key = v8::String::new_from_utf8(scope, name.as_bytes(), v8::NewStringType::Internalized)
                    .unwrap_or_else(|| v8::String::empty(scope));
                keys.push(key.into());
            }
        }
        let obj: v8::Local<'a, v8::Value> = if duplicate_names {
            // `SELECT a.id, b.id`: the later column wins, as in an object literal. Defined, never
            // set: an inherited setter (`Object.prototype.id`) would run guest code here, with the
            // connection locked and the op state borrowed.
            let o = v8::Object::new(scope);
            for (k, v) in keys.iter().zip(vals.iter()) {
                o.create_data_property(scope, *k, *v);
            }
            o.into()
        } else {
            let p = match proto {
                Some(p) => p,
                None => {
                    let p = v8::Object::new(scope)
                        .get_prototype(scope)
                        .unwrap_or_else(|| v8::null(scope).into());
                    proto = Some(p);
                    p
                }
            };
            v8::Object::with_prototype_and_properties(scope, p, &keys, &vals).into()
        };
        rows.push(obj);
        Ok(mode != MODE_GET)
    })
    .map_err(sql_err)?;

    Ok(if mode == MODE_GET {
        rows.first().copied().unwrap_or_else(|| v8::null(scope).into())
    } else {
        v8::Array::new_with_elements(scope, &rows).into()
    })
}

/// `db.exec(sql)` / `db.run(sql)` without parameters: one or more statements, `{ changes,
/// lastInsertRowid }` of the last.
#[op2]
pub fn op_rt_sqlite_exec<'a>(
    scope: &mut v8::PinScope<'a, '_>,
    state: &mut OpState,
    #[string] sql: &str,
    #[smi] flags: u32,
) -> Result<v8::Local<'a, v8::Value>, JsErrorBox> {
    let db = db_of(state)?;
    ensure_ceiling(state)?;
    let stats = db.exec_batch(sql).map_err(sql_err)?;
    Ok(run_result(scope, stats, flags & FLAG_SAFE_INTEGERS != 0))
}

pub const TX_BEGIN: u32 = 0;
pub const TX_COMMIT: u32 = 1;
pub const TX_ROLLBACK: u32 = 2;

/// One step of `db.transaction(fn)`: begin (`BEGIN`, or a savepoint inside an open transaction;
/// `mode` 0 deferred, 1 immediate, 2 exclusive), commit or roll back the innermost level.
#[op2(nofast)]
pub fn op_rt_sqlite_tx(state: &mut OpState, #[smi] action: u32, #[smi] mode: u32) -> Result<(), JsErrorBox> {
    let db = db_of(state)?;
    match action {
        TX_BEGIN => {
            // The transaction will most likely write: set the ceiling before it takes the lock.
            ensure_ceiling(state)?;
            let mode = match mode {
                1 => TxMode::Immediate,
                2 => TxMode::Exclusive,
                _ => TxMode::Deferred,
            };
            db.begin(mode)
        }
        TX_COMMIT => db.commit(),
        _ => db.rollback(),
    }
    .map_err(sql_err)
}

/// `db.inTransaction`.
#[op2(fast)]
pub fn op_rt_sqlite_in_tx(state: &mut OpState) -> bool {
    state.try_borrow::<SqliteState>().is_some_and(|s| s.db.in_transaction())
}

/// Roll back every transaction a guest left open on this thread's connections. The executor calls
/// it after each poll of a task: a transaction never spans a poll (JS cannot await inside one), so
/// one still open means the guest was terminated between `BEGIN` and `COMMIT`.
pub fn settle_transactions() {
    CONNS.with(|c| {
        let c = c.borrow();
        if c.is_empty() {
            return;
        }
        for db in c.values() {
            if db.may_have_open_tx() {
                db.reset_open_tx();
            }
        }
    });
}

/// Connections this thread holds (tests).
pub fn thread_connections() -> usize {
    CONNS.with(|c| c.borrow().len())
}
