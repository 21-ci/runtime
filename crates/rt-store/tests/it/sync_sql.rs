//! Machine-local synchronous SQL connections (`std:sqlite`, D-B1;
//! STO-21 … STO-24): the same guards as the pool's guest connections, a short budget and busy
//! timeout, visibility next to the async path, and the connection's life in the pool.

use crate::common;

use std::time::{Duration, Instant};

use common::*;
use rt_store::{
    BatchMode, DataConfig, KvBackend, KvSetOptions, KvValue, LocalDb, LocalError, LocalServiceData, LocalSqlSource,
    ServiceDataStore, SqlRef, SqlValue, TxMode, ValueTag,
};

fn source(store: &LocalServiceData) -> std::sync::Arc<dyn LocalSqlSource> {
    store.local_sql().expect("the local store serves synchronous SQL")
}

fn owned(v: SqlRef<'_>) -> SqlValue {
    match v {
        SqlRef::Null => SqlValue::Null,
        SqlRef::Integer(i) => SqlValue::Integer(i),
        SqlRef::Real(r) => SqlValue::Real(r),
        SqlRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
        SqlRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

/// Run `sql` with `args` and collect the rows.
fn rows(db: &LocalDb, sql: &str, args: &[SqlRef<'_>]) -> Result<Vec<Vec<SqlValue>>, LocalError> {
    let mut out = Vec::new();
    db.run(sql, args, &mut |row| {
        out.push((0..row.len()).map(|i| owned(row.get(i))).collect());
        Ok(true)
    })?;
    Ok(out)
}

fn exec(db: &LocalDb, sql: &str) {
    db.run(sql, &[], &mut |_| Ok(true))
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn count(db: &LocalDb, table: &str) -> i64 {
    match rows(db, &format!("SELECT count(*) FROM {table}"), &[])
        .expect("count")
        .first()
        .and_then(|r| r.first())
    {
        Some(SqlValue::Integer(n)) => *n,
        other => panic!("count(*) returned {other:?}"),
    }
}

fn int(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

fn text(s: &str) -> SqlValue {
    SqlValue::Text(s.to_string())
}

// ---- STO-21: the guest guards -------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_21_the_local_connection_enforces_the_guest_authorizer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    for (what, sql) in [
        ("ATTACH", "ATTACH DATABASE ':memory:' AS other"),
        ("temp table", "CREATE TEMP TABLE scratch (x)"),
        ("temp view", "CREATE TEMP VIEW v AS SELECT 1"),
        (
            "temp trigger",
            "CREATE TEMP TRIGGER g AFTER INSERT ON main.t BEGIN SELECT 1; END",
        ),
        ("_rt_ read", "SELECT * FROM _rt_kv"),
        ("_rt_ write", "INSERT INTO _rt_kv (key, value) VALUES (x'01', x'02')"),
        ("_rt_ drop", "DROP TABLE _rt_kv"),
        ("_rt_ blobs", "SELECT * FROM _RT_blobs"),
        ("journal_mode", "PRAGMA journal_mode = DELETE"),
        ("synchronous", "PRAGMA synchronous = OFF"),
        ("max_page_count", "PRAGMA max_page_count = 1000000"),
        ("writable_schema", "PRAGMA writable_schema = ON"),
        ("busy_timeout", "PRAGMA busy_timeout = 60000"),
        ("guest BEGIN", "BEGIN"),
        ("guest SAVEPOINT", "SAVEPOINT x"),
        ("VACUUM", "VACUUM"),
        ("load_extension", "SELECT load_extension('x')"),
    ] {
        let err = db.run(sql, &[], &mut |_| Ok(true)).expect_err(what);
        assert_eq!(err.code(), "sql_not_authorized", "{what}: {err}");
    }
    // The denials leave the connection usable and the schema as it was.
    exec(&db, "INSERT INTO t (v) VALUES ('ok')");
    assert_eq!(count(&db, "t"), 1);
    // A multi-statement batch is judged statement by statement.
    let err = db
        .exec_batch("INSERT INTO t (v) VALUES ('a'); ATTACH DATABASE ':memory:' AS o2;")
        .expect_err("batch");
    assert_eq!(err.code(), "sql_not_authorized");
    // Replaying the text of an internal statement is denied too (the statement cache is not a way around it).
    let kv = store.kv(svc(1));
    kv_set(&kv, "k").await;
    let err = rows(
        &db,
        "SELECT value, tag FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)",
        &[SqlRef::Text(b"k"), SqlRef::Integer(0)],
    )
    .expect_err("internal text");
    assert_eq!(err.code(), "sql_not_authorized");
}

async fn kv_set(kv: &std::sync::Arc<dyn KvBackend>, key: &str) {
    kv.set(
        key.as_bytes(),
        KvValue {
            tag: ValueTag::Json,
            data: "1".into(),
        },
        KvSetOptions::default(),
    )
    .await
    .expect("kv set");
}

#[test]
fn ac_sto_21_sqlite_limits_and_pragmas_match_the_pool_connections() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    // SQL_LENGTH 1 MiB
    let long = format!("SELECT 1 /* {} */", "x".repeat(1024 * 1024 + 16));
    assert!(rows(&db, &long, &[]).is_err(), "a statement over 1 MiB is refused");
    // LIKE_PATTERN_LENGTH 1000
    let pat = "a".repeat(1100);
    assert!(
        rows(&db, "SELECT 'a' LIKE ?1", &[SqlRef::Text(pat.as_bytes())]).is_err(),
        "a long LIKE pattern is refused"
    );
    // VARIABLE_NUMBER 10,000
    assert!(rows(&db, "SELECT ?10001", &[]).is_err());
    // Pragmas that change durability or layout are denied (even to read) on this connection too.
    for name in [
        "journal_mode",
        "synchronous",
        "temp_store",
        "busy_timeout",
        "cache_size",
    ] {
        let err = rows(&db, &format!("PRAGMA {name}"), &[]).expect_err(name);
        assert_eq!(err.code(), "sql_not_authorized", "{name}");
    }
}

// ---- STO-22: budget, result caps, quota ---------------------------------------------------------

#[test]
fn ac_sto_22_a_statement_is_aborted_at_the_short_sync_budget() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        sql_sync_budget: Duration::from_millis(60),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let db = source(&store).open(svc(1)).expect("open");

    let started = Instant::now();
    let err = rows(
        &db,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c",
        &[],
    )
    .expect_err("infinite recursion must be aborted");
    assert_eq!(err.code(), "sql_timeout", "{err}");
    assert_eq!(err.sqlite_name(), "SQLITE_INTERRUPT");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "abort took {:?}",
        started.elapsed()
    );
    // The budget is per statement: the connection recovers, and a statement that fits runs.
    assert_eq!(rows(&db, "SELECT 7", &[]).expect("select"), vec![vec![int(7)]]);
    // An expired budget does not leak into transaction steps or the next statement.
    db.begin(TxMode::Immediate).expect("begin");
    db.commit().expect("commit");
}

#[test]
fn ac_sto_22_time_spent_building_the_result_is_not_the_statements() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        sql_sync_budget: Duration::from_millis(60),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let db = source(&store).open(svc(1)).expect("open");

    // 3,000 rows whose consumer takes 100 us each: 300 ms in all, five times the budget, but the
    // statement itself is quick. Only SQLite's own time is budgeted.
    let mut n = 0;
    let started = Instant::now();
    db.run(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000) SELECT x FROM c",
        &[],
        &mut |_| {
            n += 1;
            std::thread::sleep(Duration::from_micros(100));
            Ok(true)
        },
    )
    .expect("the consumer's time does not count against the budget");
    assert_eq!(n, 3000);
    assert!(
        started.elapsed() > Duration::from_millis(200),
        "the consumer really was slow: {:?}",
        started.elapsed()
    );
}

#[test]
fn ac_sto_22_result_caps_apply() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_result_rows: 100,
        max_result_bytes: 64 * 1024,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE n (v INTEGER)");
    exec(
        &db,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 500) INSERT INTO n SELECT x FROM c",
    );

    assert_eq!(
        rows(&db, "SELECT v FROM n LIMIT 100", &[]).expect("at the cap").len(),
        100
    );
    let err = rows(&db, "SELECT v FROM n LIMIT 101", &[]).expect_err("over the row cap");
    assert_eq!(err.code(), "sql_result_too_large");
    let err = rows(&db, "SELECT zeroblob(40000) FROM n LIMIT 3", &[]).expect_err("over the byte cap");
    assert_eq!(err.code(), "sql_result_too_large");
    // A callback that stops after the first row never trips the cap.
    let mut seen = 0;
    db.run("SELECT v FROM n", &[], &mut |_| {
        seen += 1;
        Ok(false)
    })
    .expect("get");
    assert_eq!(seen, 1);
    // Writes are not capped by the row counters.
    exec(&db, "UPDATE n SET v = v + 1");
}

#[test]
fn ac_sto_22_the_storage_ceiling_applies_to_the_local_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Node-wide ceiling of 256 KiB per database.
    let store = LocalServiceData::open(DataConfig {
        max_db_bytes: 256 * 1024,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE blobs (b BLOB)");
    let mut quota = None;
    for _ in 0..64 {
        match db.run("INSERT INTO blobs VALUES (zeroblob(32768))", &[], &mut |_| Ok(true)) {
            Ok(_) => {}
            Err(e) => {
                quota = Some(e);
                break;
            }
        }
    }
    let err = quota.expect("the ceiling must stop the growth");
    assert_eq!(err.code(), "storage_quota", "{err}");
    // Deleting still works over quota, and the connection keeps serving reads.
    exec(&db, "DELETE FROM blobs");
    assert_eq!(count(&db, "blobs"), 0);
    // The tenant's remaining quota (growth) lowers the ceiling before a task's first write.
    let dir2 = tempfile::tempdir().expect("tempdir");
    let store2 = data(dir2.path());
    let db2 = source(&store2).open(svc(1)).expect("open");
    exec(&db2, "CREATE TABLE blobs (b BLOB)");
    db2.refresh_ceiling(Some(64 * 1024)).expect("refresh");
    let mut quota = None;
    for _ in 0..64 {
        if let Err(e) = db2.run("INSERT INTO blobs VALUES (zeroblob(32768))", &[], &mut |_| Ok(true)) {
            quota = Some(e);
            break;
        }
    }
    assert_eq!(quota.expect("growth limit").code(), "storage_quota");
    db2.refresh_ceiling(None).expect("lift");
    exec(&db2, "INSERT INTO blobs VALUES (zeroblob(32768))");
}

// ---- statement API -----------------------------------------------------------------------------

#[test]
fn local_statements_bind_run_and_describe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, photo BLOB)",
    );

    let info = db
        .prepare("SELECT id, name AS who FROM t WHERE id = $id AND name = :name AND score > ?3")
        .expect("prepare");
    assert_eq!(info.columns, vec!["id", "who"]);
    assert_eq!(
        info.param_names,
        vec![
            Some("$id".to_string()),
            Some(":name".to_string()),
            Some("?3".to_string())
        ]
    );
    assert!(info.readonly);
    assert!(!db.prepare("INSERT INTO t (name) VALUES (?)").expect("prepare").readonly);
    assert_eq!(
        db.prepare("INSERT INTO t (name) VALUES (?)")
            .expect("prepare")
            .param_names,
        vec![None]
    );

    // All five value types round-trip; BLOB and TEXT are bound as bytes.
    let stats = db
        .run(
            "INSERT INTO t (name, score, photo) VALUES (?1, ?2, ?3)",
            &[
                SqlRef::Text("h\u{e9}llo".as_bytes()),
                SqlRef::Real(1.5),
                SqlRef::Blob(&[0, 1, 2, 255]),
            ],
            &mut |_| Ok(true),
        )
        .expect("insert");
    assert_eq!((stats.changes, stats.last_insert_rowid), (1, 1));
    let stats = db
        .run("INSERT INTO t (name) VALUES (NULL)", &[], &mut |_| Ok(true))
        .expect("insert null");
    assert_eq!((stats.changes, stats.last_insert_rowid), (1, 2));
    assert_eq!(
        rows(&db, "SELECT id, name, score, photo FROM t ORDER BY id", &[]).expect("select"),
        vec![
            vec![
                int(1),
                text("h\u{e9}llo"),
                SqlValue::Real(1.5),
                SqlValue::Blob(vec![0, 1, 2, 255])
            ],
            vec![int(2), SqlValue::Null, SqlValue::Null, SqlValue::Null],
        ]
    );
    // A statement that changes nothing reports 0 changes.
    assert_eq!(
        db.run("UPDATE t SET name = 'x' WHERE id = 99", &[], &mut |_| Ok(true))
            .expect("update")
            .changes,
        0
    );
    // The same text again with other bindings, many times (the statement cache).
    for i in 1..=2 {
        assert_eq!(
            rows(&db, "SELECT id FROM t WHERE id = ?1", &[SqlRef::Integer(i)]).expect("select"),
            vec![vec![int(i)]]
        );
    }
    // Too many arguments, and a syntax error with its offset.
    assert!(rows(&db, "SELECT ?1", &[SqlRef::Integer(1), SqlRef::Integer(2)]).is_err());
    let err = rows(&db, "SELEC 1", &[]).expect_err("syntax");
    assert_eq!(err.code(), "sql_error");
    assert!(err.offset >= 0, "the byte offset of the syntax error is reported");
    // A constraint error carries SQLite's extended code.
    exec(&db, "CREATE TABLE u (k TEXT UNIQUE)");
    exec(&db, "INSERT INTO u VALUES ('a')");
    let err = db
        .run("INSERT INTO u VALUES ('a')", &[], &mut |_| Ok(true))
        .expect_err("unique");
    assert_eq!(
        (err.code(), err.sqlite_name()),
        ("sql_error", "SQLITE_CONSTRAINT_UNIQUE")
    );
    // exec_batch: several statements, no parameters.
    let stats = db
        .exec_batch("INSERT INTO u VALUES ('b'); INSERT INTO u VALUES ('c'); UPDATE u SET k = k || '!';")
        .expect("batch");
    assert_eq!(stats.changes, 3);
    assert_eq!(count(&db, "u"), 3);
    // Column names come from the statement as it runs.
    let mut names = Vec::new();
    db.run("SELECT id AS a, name AS b FROM t LIMIT 1", &[], &mut |row| {
        names = (0..row.len()).map(|i| row.name(i).to_string()).collect();
        Ok(true)
    })
    .expect("names");
    assert_eq!(names, vec!["a", "b"]);
}

#[test]
fn local_transactions_nest_with_savepoints_and_report_their_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");

    assert!(!db.in_transaction());
    db.begin(TxMode::Deferred).expect("begin");
    assert!(db.in_transaction() && db.may_have_open_tx());
    assert_eq!(db.tx_depth(), 1);
    exec(&db, "INSERT INTO t VALUES (1)");
    db.begin(TxMode::Deferred).expect("nested = savepoint");
    assert_eq!(db.tx_depth(), 2);
    exec(&db, "INSERT INTO t VALUES (2)");
    db.rollback().expect("rollback to the savepoint");
    assert!(
        db.in_transaction(),
        "the outer transaction survives a savepoint rollback"
    );
    assert_eq!(db.tx_depth(), 1);
    db.begin(TxMode::Deferred).expect("savepoint again");
    exec(&db, "INSERT INTO t VALUES (3)");
    db.begin(TxMode::Deferred).expect("two levels deep");
    exec(&db, "INSERT INTO t VALUES (4)");
    db.commit().expect("release the inner one");
    db.commit().expect("release the savepoint");
    db.commit().expect("commit");
    assert!(!db.in_transaction() && !db.may_have_open_tx());
    assert_eq!(
        rows(&db, "SELECT v FROM t ORDER BY v", &[]).expect("select"),
        vec![vec![int(1)], vec![int(3)], vec![int(4)]]
    );

    // Rollback of everything; a second rollback with nothing active is a no-op.
    db.begin(TxMode::Immediate).expect("begin");
    exec(&db, "DELETE FROM t");
    db.rollback().expect("rollback");
    db.rollback().expect("rollback with nothing active");
    assert_eq!(count(&db, "t"), 3);
}

// ---- STO-23: next to the async path --------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_23_sync_and_async_sql_see_each_others_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");
    let src = source(&store);
    let db = src.open(svc(1)).expect("open");

    // async -> sync
    sql.execute(st("INSERT INTO t VALUES (1)")).await.expect("async insert");
    assert_eq!(
        rows(&db, "SELECT v FROM t", &[]).expect("sync select"),
        vec![vec![int(1)]]
    );
    // sync -> async (read on a pool reader as well as the writer)
    exec(&db, "INSERT INTO t VALUES (2)");
    for _ in 0..3 {
        let r = sql.execute(st("SELECT count(*) FROM t")).await.expect("async count");
        assert_eq!(r.rows, vec![vec![int(2)]]);
    }
    let r = sql
        .batch(vec![st("SELECT sum(v) FROM t")], BatchMode::Read)
        .await
        .expect("async read batch");
    assert_eq!(r[0].rows, vec![vec![int(3)]]);
    // sync -> async schema change and back
    exec(&db, "ALTER TABLE t ADD COLUMN w TEXT DEFAULT 'd'");
    let r = sql
        .execute(st("SELECT * FROM t WHERE v = 1"))
        .await
        .expect("async select *");
    assert_eq!(r.rows, vec![vec![int(1), text("d")]]);
    // KV through the pool's group commit, next to local writes
    let kv = store.kv(svc(1));
    for i in 0..20 {
        kv_set(&kv, &format!("k{i}")).await;
        exec(&db, "INSERT INTO t (v) VALUES (9)");
    }
    assert_eq!(count(&db, "t"), 22);
    assert!(kv.get(b"k19").await.expect("kv get").is_some());
    // The file is still WAL, and a second local connection (another Machine) sees the same data.
    assert_eq!(store.pragma(svc(1), "journal_mode").await.expect("pragma"), text("wal"));
    let other = src.open(svc(1)).expect("second connection");
    assert_eq!(count(&other, "t"), 22);
    exec(&other, "INSERT INTO t (v) VALUES (10)");
    assert_eq!(count(&db, "t"), 23);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_23_a_local_write_behind_the_pool_writer_fails_fast_with_sql_busy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        sql_sync_busy: Duration::from_millis(30),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");
    let db = source(&store).open(svc(1)).expect("open");

    // An interactive async transaction holds the write lock for up to 5 s.
    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO t VALUES (1)")).await.expect("insert");

    // Reads proceed (WAL) ...
    assert_eq!(count(&db, "t"), 0);
    // ... a write waits `sql_sync_busy` and fails with `sql_busy`, not after the pool's 5 s.
    let started = Instant::now();
    let err = db
        .run("INSERT INTO t VALUES (2)", &[], &mut |_| Ok(true))
        .expect_err("busy");
    let took = started.elapsed();
    assert_eq!((err.code(), err.sqlite_name()), ("sql_busy", "SQLITE_BUSY"), "{err}");
    assert!(
        took >= Duration::from_millis(20) && took < Duration::from_millis(800),
        "waited {took:?}"
    );
    // An immediate transaction fails the same way.
    let err = db.begin(TxMode::Immediate).expect_err("busy begin");
    assert_eq!(err.code(), "sql_busy");
    assert!(!db.in_transaction() && !db.may_have_open_tx());

    tx.commit().await.expect("commit");
    exec(&db, "INSERT INTO t VALUES (2)");
    assert_eq!(count(&db, "t"), 2);
}

// ---- STO-24: the connection's life in the pool ------------------------------------------------------

#[test]
fn ac_sto_24_an_open_transaction_is_rolled_back_by_reset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");

    db.begin(TxMode::Immediate).expect("begin");
    exec(&db, "INSERT INTO t VALUES (1)");
    assert!(db.may_have_open_tx());
    assert!(db.reset_open_tx(), "an open transaction is rolled back");
    assert!(!db.in_transaction() && !db.may_have_open_tx());
    assert_eq!(count(&db, "t"), 0, "nothing of it persists");
    assert!(!db.reset_open_tx(), "nothing to roll back the second time");
    // The write lock is released: another connection can write at once.
    let other = source(&store).open(svc(1)).expect("second");
    exec(&other, "INSERT INTO t VALUES (7)");
    assert_eq!(count(&db, "t"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_24_local_connections_count_in_max_open_dbs_and_are_evicted_idle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: 2,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let src = source(&store);
    assert_eq!(store.open_connections(), 0);

    let a = src.open(svc(1)).expect("open 1");
    exec(&a, "CREATE TABLE t (v INTEGER)");
    exec(&a, "INSERT INTO t VALUES (1)");
    let b = src.open(svc(2)).expect("open 2");
    exec(&b, "CREATE TABLE t (v INTEGER)");
    assert_eq!(store.open_connections(), 2, "each local connection holds a slot");

    // A third service needs a slot: the least recently used idle one is closed whole, for the
    // async path and for another local connection alike.
    store
        .sql(svc(3))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .expect("service 3 through the pool");
    assert!(
        store.open_connections() <= 2,
        "never above max_open_dbs: {}",
        store.open_connections()
    );
    let c = src.open(svc(4)).expect("open 4");
    exec(&c, "CREATE TABLE t (v INTEGER)");
    assert!(store.open_connections() <= 2);
    // The evicted owners reopen transparently on their next call, and their data is intact.
    assert_eq!(count(&a, "t"), 1);
    exec(&b, "INSERT INTO t VALUES (5)");
    assert_eq!(count(&b, "t"), 1);
    assert!(store.open_connections() <= 2);

    // Closing a connection gives its slot back.
    drop(a);
    drop(b);
    drop(c);
    store.sql(svc(3)).execute(st("SELECT 1")).await.expect("select");
    assert!(
        store.open_connections() <= 1,
        "only service 3's connections remain: {}",
        store.open_connections()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_24_deleting_a_service_closes_its_local_connections() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let src = source(&store);
    let db = src.open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");
    exec(&db, "INSERT INTO t VALUES (1)");
    assert_eq!(store.open_connections(), 1);

    store.purge(svc(1)).expect("purge");
    assert_eq!(
        store.open_connections(),
        0,
        "the purge closed the connection and released its slot"
    );
    assert!(
        !dir.path().join("services").join(svc(1).to_string()).exists(),
        "the data directory is gone"
    );
    // The old handle does not recreate a deleted service's files ...
    let err = rows(&db, "SELECT 1", &[]).expect_err("deleted");
    assert_eq!(err.code(), "sql_error");
    assert!(!dir.path().join("services").join(svc(1).to_string()).exists());
    // ... opening a database again is explicit, and starts empty.
    let fresh = src.open(svc(1)).expect("open again");
    assert!(rows(&fresh, "SELECT * FROM t", &[]).is_err(), "the table is gone");
    exec(&fresh, "CREATE TABLE t (v INTEGER)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_24_a_pool_ceiling_refresh_reaches_idle_local_connections() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `ServiceDirectory` that lets service 1 grow by 64 KiB.
    struct Grow;
    impl rt_store::ServiceDirectory for Grow {
        fn tenant_of(&self, _s: rt_types::ServiceId) -> Option<rt_types::TenantId> {
            None
        }
        fn db_growth_bytes(&self, _s: rt_types::ServiceId) -> Option<u64> {
            Some(64 * 1024)
        }
    }
    let store = LocalServiceData::open(DataConfig {
        directory: Some(std::sync::Arc::new(Grow)),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE blobs (b BLOB)");
    exec(&db, "INSERT INTO blobs VALUES (zeroblob(16384))");
    store.refresh_limits().await; // size now + 64 KiB
    let mut stopped = false;
    for _ in 0..16 {
        if let Err(e) = db.run("INSERT INTO blobs VALUES (zeroblob(16384))", &[], &mut |_| Ok(true)) {
            assert_eq!(e.code(), "storage_quota");
            stopped = true;
            break;
        }
    }
    assert!(stopped, "the refreshed ceiling stops the growth");
}

// ---- perf ------------------------------------------------------------------------------------------

/// Point SELECT on the local connection vs through the pool, µs per call. Run with
/// `--release --ignored --nocapture` for real numbers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "perf"]
async fn perf_point_select_local_vs_pool() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .expect("create");
    let seed: Vec<_> = (1..=1000)
        .map(|i| {
            st_args(
                "INSERT INTO t (id, v) VALUES (?1, ?2)",
                vec![SqlValue::Integer(i), text(&format!("value-{i}"))],
            )
        })
        .collect();
    sql.batch(seed, BatchMode::Write).await.expect("seed");
    let db = source(&store).open(svc(1)).expect("open");

    let n = 20_000;
    for round in 0..3 {
        let started = Instant::now();
        for i in 0..n {
            let mut hits = 0;
            db.run(
                "SELECT v FROM t WHERE id = ?1",
                &[SqlRef::Integer(i % 1000 + 1)],
                &mut |row| {
                    hits += matches!(row.get(0), SqlRef::Text(_)) as usize;
                    Ok(true)
                },
            )
            .expect("select");
            assert_eq!(hits, 1);
        }
        let local = started.elapsed().as_secs_f64() * 1e6 / n as f64;
        let started = Instant::now();
        let m = 5_000;
        for i in 0..m {
            sql.execute(st_args(
                "SELECT v FROM t WHERE id = ?1",
                vec![SqlValue::Integer(i % 1000 + 1)],
            ))
            .await
            .expect("select");
        }
        let pool = started.elapsed().as_secs_f64() * 1e6 / m as f64;
        eprintln!("round {round}: local {local:.2} us/call, pool {pool:.2} us/call");
    }
}

#[test]
fn vacuum_is_refused_in_every_statement_of_a_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");
    let out = dir.path().join("escape.db");
    let p = out.display();
    let attempts = [
        format!("VACUUM INTO '{p}'"),
        format!(";VACUUM INTO '{p}'"),
        format!(" ; ;\n VACUUM INTO '{p}'"),
        format!("-- c\n; /* c */ ; vacuum INTO '{p}'"),
        format!("SELECT 1; VACUUM INTO '{p}'"),
        format!("INSERT INTO t VALUES (1); /* c */ VACUUM INTO '{p}';"),
        format!("SELECT 1;\n-- c\n;VACUUM main INTO '{p}'"),
    ];
    for sql in &attempts {
        let err = db.exec_batch(sql).expect_err(sql);
        assert_eq!(err.code(), "sql_not_authorized", "{sql}");
        assert!(!out.exists(), "{sql} wrote a file");
        if !sql.starts_with("SELECT") && !sql.starts_with("INSERT") {
            let err = rows(&db, sql, &[]).expect_err(sql);
            assert_eq!(err.code(), "sql_not_authorized", "{sql}");
            assert!(!out.exists(), "{sql} wrote a file");
        }
    }
    // Statements before the refused one ran (a batch is not atomic); a clean batch still works.
    db.exec_batch("INSERT INTO t VALUES (2); ; INSERT INTO t VALUES (3);")
        .expect("batch");
    assert_eq!(count(&db, "t"), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_connection_in_a_transaction_is_never_evicted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: 2,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let src = source(&store);
    // `a` is the least recently used, but it is in a transaction between statements.
    let a = src.open(svc(1)).expect("open 1");
    exec(&a, "CREATE TABLE t (v INTEGER)");
    a.begin(TxMode::Immediate).expect("begin");
    exec(&a, "INSERT INTO t VALUES (1)");
    let b = src.open(svc(2)).expect("open 2");
    exec(&b, "CREATE TABLE t (v INTEGER)");

    store
        .sql(svc(3))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .expect("service 3 evicts b, not a");
    assert!(a.in_transaction(), "the transaction is still open on its connection");
    exec(&a, "INSERT INTO t VALUES (2)");
    a.rollback().expect("rollback");
    assert_eq!(count(&a, "t"), 0, "nothing of the transaction was applied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_lost_with_its_connection_cannot_be_committed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");
    db.begin(TxMode::Immediate).expect("begin");
    exec(&db, "INSERT INTO t VALUES (1)");
    db.begin(TxMode::Deferred).expect("savepoint");

    store.purge(svc(1)).expect("purge");
    let err = rows(&db, "INSERT INTO t VALUES (2)", &[]).expect_err("no reconnect into autocommit");
    assert!(err.to_string().contains("transaction was lost"), "{err}");
    db.rollback().expect("rollback of a lost level is a no-op");
    assert!(db
        .commit()
        .expect_err("commit after the loss")
        .to_string()
        .contains("transaction was lost"));
    assert!(db.commit().is_err(), "sticky until the next begin");
    assert!(!dir.path().join("services").join(svc(1).to_string()).exists());
}

#[test]
fn a_doomed_connection_ends_its_transaction_as_lost() {
    // Same as above, but the connection is doomed while it is locked (mid-statement): the owner
    // closes it when the statement ends.
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");
    db.begin(TxMode::Immediate).expect("begin");
    let mut purged = false;
    db.run("SELECT 1", &[], &mut |_| {
        if !purged {
            store.purge(svc(1)).expect("purge");
            purged = true;
        }
        Ok(true)
    })
    .expect("statement");
    assert!(!db.may_have_open_tx());
    assert!(db.commit().is_err(), "commit after doom errors");
    db.rollback().expect("rollback is a no-op");
}

#[test]
fn a_nested_call_from_a_row_callback_fails_cleanly_instead_of_deadlocking() {
    // The callback of a statement must not reach the connection again (in `std:sqlite` it builds the
    // guest's row; a guest setter that ran there could call back in). Refused, never a self-deadlock
    // on the connection's lock.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::thread::spawn(move || {
        let store = data(&root);
        let db = source(&store).open(svc(1)).expect("open");
        let mut nested = None;
        db.run("SELECT 1", &[], &mut |_| {
            nested = Some(rows(&db, "SELECT 2", &[]).map_err(|e| e.to_string()));
            Ok(true)
        })
        .expect("outer statement");
        let in_tx = db.in_transaction();
        let after = rows(&db, "SELECT 3", &[]).expect("usable afterwards");
        let _ = done_tx.send((nested, in_tx, after));
    });
    let (nested, in_tx, after) = done_rx.recv_timeout(Duration::from_secs(10)).expect("no deadlock");
    let err = nested.expect("the callback ran").expect_err("a nested call is refused");
    assert!(err.contains("nested"), "{err}");
    assert!(!in_tx);
    assert_eq!(after, vec![vec![int(3)]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_sto_24_a_slow_open_fails_fast_with_sql_busy_and_completes_later() {
    // Opening a local connection (file, WAL, bootstrap: up to 250 ms waiting for a lock) and the
    // eviction it may need run on the blocking pool. The Machine thread waits at most the statement
    // budget, then fails fast with `sql_busy`; a later call finds the connection open.
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: 1,
        sql_sync_budget: Duration::from_millis(40),
        ..data_cfg(dir.path())
    })
    .expect("open");
    store
        .sql(svc(1))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .expect("create");
    let path = find_files(dir.path(), "data.db").into_iter().next().expect("data.db");
    // Service 2 takes the only slot: service 1's pool connections are closed.
    store.sql(svc(2)).execute(st("SELECT 1")).await.expect("service 2");

    // Another process holds service 1's database exclusively.
    let blocker = rusqlite::Connection::open(&path).expect("blocker");
    blocker
        .execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE; INSERT INTO t VALUES (1);")
        .expect("lock");

    let src = source(&store);
    let started = Instant::now();
    // A slow open is not an error yet: the connection is still opening ...
    let db = src.open(svc(1)).expect("open");
    assert!(db.is_live() && !db.is_open());
    // ... and a statement fails fast with `sql_busy` while it is.
    let err = rows(&db, "SELECT count(*) FROM t", &[]).expect_err("busy");
    let took = started.elapsed();
    assert_eq!(err.code(), "sql_busy", "{err}");
    assert!(took < Duration::from_millis(200), "the Machine thread waited {took:?}");

    blocker.execute_batch("COMMIT").expect("commit");
    drop(blocker);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match rows(&db, "SELECT 1", &[]) {
            Ok(_) => break,
            Err(e) if e.code() == "sql_busy" && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("a statement after the lock is gone: {e}"),
        }
    }
    assert_eq!(count(&db, "t"), 1);
    assert!(
        store.open_connections() <= 1,
        "never above max_open_dbs: {}",
        store.open_connections()
    );
}

/// SSQL-005: 1000 nested `db.transaction` levels work and unwind to depth 0.
#[test]
fn ssql_005_a_thousand_nested_savepoints_unwind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER)");
    for i in 0..1000 {
        db.begin(TxMode::Deferred).expect("begin");
        assert_eq!(db.tx_depth(), i + 1);
    }
    exec(&db, "INSERT INTO t VALUES (1)");
    for _ in 0..999 {
        db.commit().expect("release");
    }
    db.commit().expect("commit");
    assert_eq!(db.tx_depth(), 0);
    assert!(!db.in_transaction());
    assert_eq!(count(&db, "t"), 1);
}

/// SSQL-006 (pinned, docs/handlers.md `db.exec`): several statements in one `exec` run one by one
/// in autocommit: a failing statement stops the rest and the earlier ones stay. Wrapped in
/// `db.transaction`, the batch is all or nothing.
#[test]
fn ssql_006_a_multi_statement_exec_is_not_atomic_outside_a_transaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let db = source(&store).open(svc(1)).expect("open");
    exec(&db, "CREATE TABLE t (v INTEGER UNIQUE)");
    let r = db.exec_batch("INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
    assert!(r.is_err(), "the duplicate fails");
    assert_eq!(count(&db, "t"), 1, "the first statement stays, the third never ran");
    db.begin(TxMode::Deferred).expect("begin");
    let r = db.exec_batch("INSERT INTO t VALUES (3); INSERT INTO t VALUES (3)");
    assert!(r.is_err());
    db.rollback().expect("rollback");
    assert_eq!(
        count(&db, "t"),
        1,
        "inside a transaction the caller rolls everything back"
    );
}
