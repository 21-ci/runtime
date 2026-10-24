//! rt-store SQL tests (STO-04, STO-05, STO-06, STO-07, STO-14, STO-16; contract
//! C-5): result shape, authorizer denials, page quota, interactive-transaction timeout, pragmas,
//! open-connection cap, statement budget and CPU charging.

use crate::common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use rt_store::{
    with_cpu_meter, BatchMode, CpuMeter, DataConfig, KvBackend, KvSetOptions, KvValue, LocalServiceData,
    ServiceDataStore, SqlBackend, SqlResult, SqlValue, StoreError, ValueTag,
};

async fn count(sql: &Arc<dyn SqlBackend>, table: &str) -> i64 {
    let r = sql
        .execute(st(&format!("SELECT count(*) FROM {table}")))
        .await
        .expect("count query");
    match r.rows.first().and_then(|row| row.first()) {
        Some(SqlValue::Integer(n)) => *n,
        other => panic!("count(*) returned {other:?}"),
    }
}

fn text(s: &str) -> SqlValue {
    SqlValue::Text(s.to_string())
}

// ---- STO-04: result shape ----------------------------------------------------------------------

#[tokio::test]
async fn ac_sto_04_batch_result_shape_matches_the_spec() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));

    let results: Vec<SqlResult> = sql
        .batch(
            vec![
                st("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, photo BLOB)"),
                st_args(
                    "INSERT INTO people (name, score, photo) VALUES (?, ?, ?)",
                    vec![text("ann"), SqlValue::Real(1.5), SqlValue::Blob(vec![0, 1, 2])],
                ),
                st_args(
                    "INSERT INTO people (name, score, photo) VALUES (?, ?, NULL), (?, ?, NULL)",
                    vec![text("bob"), SqlValue::Real(2.0), text("cy"), SqlValue::Real(3.0)],
                ),
                st("SELECT id, name, score, photo FROM people ORDER BY id"),
            ],
            BatchMode::Write,
        )
        .await
        .expect("batch");

    assert_eq!(results.len(), 4, "one result per statement");

    // CREATE TABLE: no columns, no rows, nothing affected.
    assert!(results[0].columns.is_empty());
    assert!(results[0].rows.is_empty());
    assert_eq!(results[0].rows_affected, 0);

    // INSERTs: rowsAffected and lastInsertRowid.
    assert_eq!(results[1].rows_affected, 1);
    assert_eq!(results[1].last_insert_rowid, Some(1));
    assert!(results[1].rows.is_empty());
    assert_eq!(results[2].rows_affected, 2);
    assert_eq!(results[2].last_insert_rowid, Some(3));

    // SELECT: columns, typed rows, no rows affected.
    assert_eq!(results[3].columns, vec!["id", "name", "score", "photo"]);
    assert_eq!(results[3].rows_affected, 0);
    assert_eq!(
        results[3].rows,
        vec![
            vec![
                SqlValue::Integer(1),
                text("ann"),
                SqlValue::Real(1.5),
                SqlValue::Blob(vec![0, 1, 2])
            ],
            vec![SqlValue::Integer(2), text("bob"), SqlValue::Real(2.0), SqlValue::Null],
            vec![SqlValue::Integer(3), text("cy"), SqlValue::Real(3.0), SqlValue::Null],
        ]
    );
}

#[tokio::test]
async fn ac_sto_04_execute_reports_rows_affected_for_update_and_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.batch(
        vec![
            st("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"),
            st("INSERT INTO t (v) VALUES (1), (2), (3), (4)"),
        ],
        BatchMode::Write,
    )
    .await
    .expect("seed");

    let updated = sql
        .execute(st("UPDATE t SET v = v * 10 WHERE id <= 3"))
        .await
        .expect("update");
    assert_eq!(updated.rows_affected, 3);
    let deleted = sql
        .execute(st_args("DELETE FROM t WHERE v = ?", vec![SqlValue::Integer(20)]))
        .await
        .expect("delete");
    assert_eq!(deleted.rows_affected, 1);
    let none = sql
        .execute(st("DELETE FROM t WHERE id > 1000"))
        .await
        .expect("delete none");
    assert_eq!(none.rows_affected, 0);

    let sum = sql
        .execute(st("SELECT sum(v) AS total, count(*) AS n FROM t"))
        .await
        .expect("select");
    assert_eq!(sum.columns, vec!["total", "n"]);
    assert_eq!(
        sum.rows,
        vec![vec![SqlValue::Integer(10 + 30 + 4), SqlValue::Integer(3)]]
    );
}

#[tokio::test]
async fn ac_sto_04_every_value_type_binds_and_roundtrips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE v (i INTEGER, r REAL, t TEXT, b BLOB, n)"))
        .await
        .expect("create");
    let args = vec![
        SqlValue::Integer(i64::MIN),
        SqlValue::Real(-0.125),
        text("h\u{00e9}llo \u{1F980}"),
        SqlValue::Blob(vec![0, 0, 255, 0]),
        SqlValue::Null,
    ];
    sql.execute(st_args("INSERT INTO v VALUES (?, ?, ?, ?, ?)", args.clone()))
        .await
        .expect("insert");
    let got = sql.execute(st("SELECT i, r, t, b, n FROM v")).await.expect("select");
    assert_eq!(got.rows, vec![args]);
}

#[tokio::test]
async fn ac_sto_04_failed_batch_rolls_back_every_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE keep (id INTEGER PRIMARY KEY)"))
        .await
        .expect("create keep");

    let err = sql
        .batch(
            vec![
                st("CREATE TABLE half (id INTEGER)"),
                st("INSERT INTO keep (id) VALUES (1)"),
                st("INSERT INTO table_that_does_not_exist VALUES (1)"),
            ],
            BatchMode::Write,
        )
        .await
        .expect_err("third statement fails");
    assert!(matches!(err, StoreError::Sql(_)), "got {err:?}");
    assert_eq!(err.code(), "sql_error");

    assert_eq!(count(&sql, "keep").await, 0, "the INSERT must have been rolled back");
    assert!(
        sql.execute(st("SELECT * FROM half")).await.is_err(),
        "the CREATE TABLE must have been rolled back"
    );
}

#[tokio::test]
async fn ac_sto_04_read_mode_batches_cannot_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE r (v INTEGER)")).await.expect("create");
    sql.execute(st("INSERT INTO r VALUES (1)")).await.expect("insert");

    let ok = sql
        .batch(
            vec![st("SELECT v FROM r"), st("SELECT count(*) FROM r")],
            BatchMode::Read,
        )
        .await
        .expect("read batch");
    assert_eq!(ok.len(), 2);
    assert_eq!(ok[0].rows, vec![vec![SqlValue::Integer(1)]]);

    assert!(
        sql.batch(vec![st("INSERT INTO r VALUES (2)")], BatchMode::Read)
            .await
            .is_err(),
        "writes in a read batch must fail"
    );
    assert_eq!(count(&sql, "r").await, 1);
}

#[tokio::test]
async fn ac_sto_04_sql_errors_have_a_stable_code_and_do_not_leak_host_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));

    for bad in ["SELEC 1", "SELECT * FROM nonexistent_table", "INSERT INTO"] {
        let err = sql.execute(st(bad)).await.expect_err(bad);
        assert!(matches!(err, StoreError::Sql(_)), "{bad}: got {err:?}");
        assert_eq!(err.code(), "sql_error");
        let shown = format!("{err} {err:?}");
        assert!(
            !shown.contains(&dir.path().display().to_string()),
            "{bad}: error leaks the data directory: {shown}"
        );
        assert!(!shown.contains("data.db"), "{bad}: error leaks a file name: {shown}");
    }
    // The connection is still usable after errors.
    assert_eq!(
        sql.execute(st("SELECT 1")).await.expect("select 1").rows,
        vec![vec![SqlValue::Integer(1)]]
    );
}

// ---- STO-05: authorizer ------------------------------------------------------------------------

#[tokio::test]
async fn ac_sto_05_authorizer_denies_attach_load_extension_rt_tables_and_writable_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    // Make sure `_rt_kv` exists and has data (it is created by the KV bootstrap).
    store
        .kv(svc(1))
        .set(
            b"secret-key",
            KvValue {
                tag: ValueTag::Json,
                data: bytes::Bytes::from_static(b"1"),
            },
            KvSetOptions::default(),
        )
        .await
        .expect("kv set");

    let denied = [
        "ATTACH DATABASE ':memory:' AS other",
        "ATTACH DATABASE '/etc/passwd' AS pw",
        "SELECT load_extension('/tmp/evil')",
        "SELECT * FROM _rt_kv",
        "SELECT key FROM _rt_kv WHERE 1",
        "INSERT INTO _rt_kv (key, value) VALUES (x'00', x'00')",
        "UPDATE _rt_kv SET value = x''",
        "DELETE FROM _rt_kv",
        "DROP TABLE _rt_kv",
        "CREATE TABLE _rt_mine (x)",
        "ALTER TABLE _rt_kv RENAME TO stolen",
        "PRAGMA writable_schema=1",
        "PRAGMA writable_schema = ON",
    ];
    for stmt in denied {
        let err = sql.execute(st(stmt)).await.expect_err(stmt);
        assert!(
            matches!(err, StoreError::SqlNotAuthorized),
            "{stmt}: expected sql_not_authorized, got {err:?}"
        );
        assert_eq!(err.code(), "sql_not_authorized", "{stmt}");
    }

    // Same enforcement inside batches and interactive transactions.
    let err = sql
        .batch(vec![st("SELECT 1"), st("SELECT * FROM _rt_kv")], BatchMode::Read)
        .await
        .expect_err("batch");
    assert!(matches!(err, StoreError::SqlNotAuthorized), "batch: {err:?}");
    let mut tx = sql.begin().await.expect("begin");
    let err = tx
        .execute(st("ATTACH DATABASE ':memory:' AS tx_other"))
        .await
        .expect_err("tx attach");
    assert!(matches!(err, StoreError::SqlNotAuthorized), "tx: {err:?}");
    tx.rollback().await.expect("rollback");

    // Denials neither break the connection nor the KV store, and ordinary SQL keeps working.
    assert_eq!(
        sql.execute(st("SELECT 41 + 1")).await.expect("select").rows,
        vec![vec![SqlValue::Integer(42)]]
    );
    sql.execute(st("CREATE TABLE mine (x)"))
        .await
        .expect("normal DDL is allowed");
    assert!(
        store.kv(svc(1)).get(b"secret-key").await.expect("kv get").is_some(),
        "KV must still see its own table"
    );
}

#[tokio::test]
async fn ac_sto_05_user_tables_with_rt_like_names_that_are_not_prefixed_are_allowed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE rt_notes (x)"))
        .await
        .expect("rt_ without a leading underscore is a normal name");
    sql.execute(st("CREATE TABLE notes_rt_kv (x)"))
        .await
        .expect("only the _rt_ prefix is reserved");
    assert_eq!(count(&sql, "rt_notes").await, 0);
}

// ---- STO-06: quota -----------------------------------------------------------------------------

#[tokio::test]
async fn ac_sto_06_inserting_past_the_page_quota_fails_with_storage_quota_but_reads_work() {
    let dir = tempfile::tempdir().expect("tempdir");
    let limit: u64 = 1024 * 1024;
    let store = LocalServiceData::open(DataConfig {
        max_db_bytes: limit,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE big (id INTEGER PRIMARY KEY, payload BLOB)"))
        .await
        .expect("create");

    let mut inserted = 0i64;
    let mut failure = None;
    for _ in 0..400 {
        match sql
            .execute(st_args(
                "INSERT INTO big (payload) VALUES (?)",
                vec![SqlValue::Blob(vec![7u8; 8192])],
            ))
            .await
        {
            Ok(_) => inserted += 1,
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    let err = failure.expect("400 x 8 KB rows (3.2 MB) must exceed a 1 MiB quota");
    assert!(matches!(err, StoreError::StorageQuota), "got {err:?}");
    assert_eq!(err.code(), "storage_quota");
    assert!(
        inserted > 50,
        "a good part of the quota is usable, only {inserted} rows fit"
    );

    // Reads still work and see exactly the rows that were committed.
    assert_eq!(count(&sql, "big").await, inserted);
    let first = sql
        .execute(st("SELECT length(payload) FROM big WHERE id = 1"))
        .await
        .expect("read");
    assert_eq!(first.rows, vec![vec![SqlValue::Integer(8192)]]);

    // Usage reports about the plan limit.
    let size = sql.size_bytes().await.expect("size_bytes");
    assert!(size <= limit, "size {size} must not exceed the {limit} byte quota");
    assert!(
        size * 10 >= limit * 9,
        "size {size} should be within 10% of the quota {limit}"
    );

    // KV shares data.db and therefore the quota.
    let kv: Arc<dyn KvBackend> = store.kv(svc(1));
    let err = kv
        .set(
            b"too-much",
            KvValue {
                tag: ValueTag::Bytes,
                data: bytes::Bytes::from(vec![1u8; 64 * 1024]),
            },
            KvSetOptions::default(),
        )
        .await
        .expect_err("kv write into a full database");
    assert!(matches!(err, StoreError::StorageQuota), "got {err:?}");

    // Another service has its own quota.
    let other = store.sql(svc(2));
    other
        .execute(st("CREATE TABLE ok (x)"))
        .await
        .expect("other service is unaffected");
}

#[tokio::test]
async fn ac_sto_06_size_bytes_grows_with_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE g (payload BLOB)")).await.expect("create");
    let before = sql.size_bytes().await.expect("size");
    assert!(before > 0, "an initialised database is not empty");
    for _ in 0..50 {
        sql.execute(st_args(
            "INSERT INTO g VALUES (?)",
            vec![SqlValue::Blob(vec![1u8; 16 * 1024])],
        ))
        .await
        .expect("insert");
    }
    let after = sql.size_bytes().await.expect("size");
    assert!(
        after >= before + 50 * 16 * 1024,
        "800 KB of payload must show up: {before} -> {after}"
    );
}

// ---- STO-07: interactive transactions ----------------------------------------------------------

#[tokio::test]
async fn ac_sto_07_transaction_left_open_is_rolled_back_after_five_seconds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE tx_t (v INTEGER)")).await.expect("create");

    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO tx_t VALUES (1)"))
        .await
        .expect("insert in tx");

    tokio::time::sleep(Duration::from_millis(5500)).await;

    // The transaction was rolled back by the cap: the write is gone, the lock is released and the
    // connection serves new work without waiting for busy_timeout.
    assert!(
        tx.execute(st("INSERT INTO tx_t VALUES (2)")).await.is_err(),
        "an expired transaction cannot be used any more"
    );
    let started = Instant::now();
    sql.execute(st("INSERT INTO tx_t VALUES (3)"))
        .await
        .expect("connection is free again");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the write lock must have been released, took {:?}",
        started.elapsed()
    );
    let rows = sql.execute(st("SELECT v FROM tx_t ORDER BY v")).await.expect("select");
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Integer(3)]],
        "the open transaction's insert must have been rolled back"
    );
    assert!(tx.commit().await.is_err(), "committing an expired transaction fails");
}

#[tokio::test]
async fn ac_sto_07_commit_makes_changes_visible_and_rollback_discards_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE c (v INTEGER)")).await.expect("create");

    let mut tx = sql.begin().await.expect("begin");
    let r = tx.execute(st("INSERT INTO c VALUES (1)")).await.expect("insert");
    assert_eq!((r.rows_affected, r.last_insert_rowid), (1, Some(1)));
    let inside = tx.execute(st("SELECT count(*) FROM c")).await.expect("select in tx");
    assert_eq!(
        inside.rows,
        vec![vec![SqlValue::Integer(1)]],
        "a transaction sees its own writes"
    );
    tx.commit().await.expect("commit");
    assert_eq!(count(&sql, "c").await, 1);

    let mut tx = sql.begin().await.expect("begin 2");
    tx.execute(st("INSERT INTO c VALUES (2)")).await.expect("insert");
    tx.execute(st("DELETE FROM c WHERE v = 1")).await.expect("delete");
    tx.rollback().await.expect("rollback");
    let rows = sql.execute(st("SELECT v FROM c")).await.expect("select");
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Integer(1)]],
        "rollback restores the pre-transaction state"
    );
}

// ---- statement budget, CPU charging, export -----------------------------------------------------

#[tokio::test]
async fn ac_sql_01_runaway_statement_is_aborted_by_the_budget_with_sql_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        sql_budget: Duration::from_millis(200),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));

    let started = Instant::now();
    let err = sql
        .execute(st(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c",
        ))
        .await
        .expect_err("infinite recursion must be aborted");
    assert!(matches!(err, StoreError::SqlTimeout), "got {err:?}");
    assert_eq!(err.code(), "sql_timeout");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "abort took {:?}",
        started.elapsed()
    );

    // The connection recovers.
    assert_eq!(
        sql.execute(st("SELECT 7")).await.expect("select").rows,
        vec![vec![SqlValue::Integer(7)]]
    );
}

#[tokio::test]
async fn ac_sec_29_sql_thread_cpu_is_charged_to_the_calling_tasks_meter() {
    let dir = tempfile::tempdir().expect("tempdir");
    // SEC-29 is about CPU charging, not the statement budget (STO budget test above): use a generous
    // budget so a loaded CI box (debug SQLite, parallel builds) cannot turn the heavy query into `sql_timeout`.
    let store = LocalServiceData::open(DataConfig {
        sql_budget: Duration::from_secs(60),
        ..data_cfg(dir.path())
    })
    .expect("open");
    let sql = store.sql(svc(1));

    let meter = CpuMeter::new();
    let res = with_cpu_meter(
        meter.clone(),
        sql.execute(st(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 300000) SELECT count(*) FROM c",
        )),
    )
    .await
    .expect("heavy query");
    assert_eq!(res.rows, vec![vec![SqlValue::Integer(300_000)]]);
    assert!(
        meter.total() > Duration::ZERO,
        "host-side SQL work must be charged to the task"
    );

    // Outside a meter scope nothing is charged anywhere (and nothing panics).
    sql.execute(st("SELECT 1")).await.expect("unmetered query");
}

#[tokio::test]
async fn ac_sql_02_export_is_a_consistent_sqlite_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.batch(
        vec![
            st("CREATE TABLE exp (id INTEGER PRIMARY KEY, name TEXT)"),
            st("INSERT INTO exp (name) VALUES ('one'), ('two'), ('three')"),
        ],
        BatchMode::Write,
    )
    .await
    .expect("seed");

    let bytes = sql.export().await.expect("export");
    assert!(
        bytes.starts_with(b"SQLite format 3\0"),
        "export must be a SQLite database file"
    );

    let copy = dir.path().join("exported.db");
    std::fs::write(&copy, &bytes).expect("write export");
    let conn =
        rusqlite::Connection::open_with_flags(&copy, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).expect("open export");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM exp ORDER BY id")
        .expect("prepare")
        .query_map([], |r| r.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");
    assert_eq!(
        names,
        vec!["one", "two", "three"],
        "committed WAL content must be part of the snapshot"
    );
}

// ---- STO-03 (SQL part) ---------------------------------------------------------------------------

#[tokio::test]
async fn ac_sto_03_sql_databases_are_isolated_per_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let (a, b) = (store.sql(svc(1)), store.sql(svc(2)));

    a.execute(st("CREATE TABLE secrets_of_a (x)"))
        .await
        .expect("create in a");
    a.execute(st("INSERT INTO secrets_of_a VALUES (1)"))
        .await
        .expect("insert in a");
    let err = b
        .execute(st("SELECT * FROM secrets_of_a"))
        .await
        .expect_err("b has no such table");
    assert!(matches!(err, StoreError::Sql(_)), "got {err:?}");
    b.execute(st("CREATE TABLE secrets_of_a (x)"))
        .await
        .expect("b can create the same name");
    assert_eq!(count(&b, "secrets_of_a").await, 0);
    assert_eq!(count(&a, "secrets_of_a").await, 1);
}

// ---- STO-14: open-connection cap -------------------------------------------------------------------

async fn run_cap_scenario(max_open: usize, services: u128) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalServiceData::open(DataConfig {
        max_open_dbs: max_open,
        ..data_cfg(dir.path())
    })
    .expect("open");

    let done = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (store, done, peak) = (store.clone(), done.clone(), peak.clone());
        tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(store.open_connections(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let mut handles = Vec::new();
    for n in 1..=services {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let sql = store.sql(svc(n));
            sql.execute(st("CREATE TABLE IF NOT EXISTS one (v INTEGER)"))
                .await
                .expect("create");
            sql.execute(st_args("INSERT INTO one VALUES (?)", vec![SqlValue::Integer(n as i64)]))
                .await
                .expect("insert");
            let r = sql.execute(st("SELECT v FROM one")).await.expect("select");
            assert_eq!(
                r.rows,
                vec![vec![SqlValue::Integer(n as i64)]],
                "service {n} sees its own row"
            );
            peak_check(&store, max_open);
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    done.store(true, Ordering::Relaxed);
    sampler.await.expect("sampler");

    let peak = peak.load(Ordering::Relaxed).max(store.open_connections());
    assert!(peak <= max_open, "open connections peaked at {peak}, cap is {max_open}");
    assert!(peak >= 1, "connections are opened lazily but used ones are kept");

    // Evicted databases reopen transparently with their data intact.
    let first = store
        .sql(svc(1))
        .execute(st("SELECT v FROM one"))
        .await
        .expect("reopen evicted service");
    assert_eq!(first.rows, vec![vec![SqlValue::Integer(1)]]);
    assert!(store.open_connections() <= max_open);
}

fn peak_check(store: &LocalServiceData, max_open: usize) {
    assert!(
        store.open_connections() <= max_open,
        "cap exceeded: {} > {max_open}",
        store.open_connections()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sto_14_open_connections_never_exceed_the_lru_cap() {
    run_cap_scenario(8, 120).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-scale STO-14 (1,000 services, cap 256) needs `ulimit -n` >= 2048; run in wave 4"]
async fn ac_sto_14_full_scale_thousand_services_cap_256() {
    run_cap_scenario(256, 1000).await;
}

#[tokio::test]
async fn ac_sto_14_connections_open_lazily() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    assert_eq!(store.open_connections(), 0, "nothing is opened at startup");
    let sql = store.sql(svc(1));
    assert_eq!(
        store.open_connections(),
        0,
        "obtaining a handle does not open a connection"
    );
    sql.execute(st("SELECT 1")).await.expect("first query");
    assert_eq!(store.open_connections(), 1);
    store.sql(svc(2)).execute(st("SELECT 1")).await.expect("second service");
    assert_eq!(store.open_connections(), 2);
}

// ---- STO-16: pragmas and throughput -----------------------------------------------------------------

#[tokio::test]
async fn ac_sto_16_connection_pragmas_match_the_spec() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    store
        .sql(svc(1))
        .execute(st("SELECT 1"))
        .await
        .expect("open the connection");

    assert_eq!(
        store.pragma(svc(1), "journal_mode").await.expect("journal_mode"),
        text("wal")
    );
    assert_eq!(
        store.pragma(svc(1), "synchronous").await.expect("synchronous"),
        SqlValue::Integer(1),
        "synchronous=NORMAL"
    );
    assert_eq!(
        store.pragma(svc(1), "busy_timeout").await.expect("busy_timeout"),
        SqlValue::Integer(5000)
    );
    match store.pragma(svc(1), "max_page_count").await.expect("max_page_count") {
        SqlValue::Integer(n) => assert!(n > 1_000_000, "unlimited plans keep SQLite's huge default, got {n}"),
        other => panic!("max_page_count returned {other:?}"),
    }
}

#[tokio::test]
async fn ac_sto_16_max_page_count_is_derived_from_the_plan_storage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let limit: u64 = 10 * 1024 * 1024;
    let store = LocalServiceData::open(DataConfig {
        max_db_bytes: limit,
        ..data_cfg(dir.path())
    })
    .expect("open");
    store
        .sql(svc(1))
        .execute(st("SELECT 1"))
        .await
        .expect("open the connection");

    let page_size = match store.pragma(svc(1), "page_size").await.expect("page_size") {
        SqlValue::Integer(n) => n as u64,
        other => panic!("page_size returned {other:?}"),
    };
    let pages = match store.pragma(svc(1), "max_page_count").await.expect("max_page_count") {
        SqlValue::Integer(n) => n as u64,
        other => panic!("max_page_count returned {other:?}"),
    };
    assert!(pages * page_size <= limit, "{pages} x {page_size} exceeds {limit}");
    assert!(
        pages * page_size + page_size > limit,
        "{pages} x {page_size} wastes more than a page of {limit}"
    );
}

#[tokio::test]
async fn ac_b32_connections_are_tuned_and_the_page_cache_comes_from_the_budget() {
    let dir = tempfile::tempdir().expect("tempdir");
    // 64 MiB shared by at most 16 connections: 4 MiB (4096 KiB) each.
    let cfg = DataConfig {
        sql_cache_bytes: 64 * 1024 * 1024,
        max_open_dbs: 16,
        ..data_cfg(dir.path())
    };
    let store = LocalServiceData::open(cfg).expect("open");
    store
        .sql(svc(1))
        .execute(st("SELECT 1"))
        .await
        .expect("open the connection");
    assert_eq!(
        store.pragma(svc(1), "cache_size").await.expect("cache_size"),
        SqlValue::Integer(-4096)
    );
    assert_eq!(
        store.pragma(svc(1), "temp_store").await.expect("temp_store"),
        SqlValue::Integer(1),
        "temp_store=FILE: transient space spills to disk, not memory"
    );
    assert_eq!(
        store
            .pragma(svc(1), "wal_autocheckpoint")
            .await
            .expect("wal_autocheckpoint"),
        SqlValue::Integer(1000)
    );
    assert_eq!(
        store
            .pragma(svc(1), "journal_size_limit")
            .await
            .expect("journal_size_limit"),
        SqlValue::Integer(64 * 1024 * 1024)
    );

    // No budget: SQLite's own default page cache.
    let plain = LocalServiceData::open(DataConfig {
        sql_cache_bytes: 0,
        ..data_cfg(&dir.path().join("plain"))
    })
    .expect("open");
    plain
        .sql(svc(1))
        .execute(st("SELECT 1"))
        .await
        .expect("open the connection");
    assert_eq!(
        plain.pragma(svc(1), "cache_size").await.expect("cache_size"),
        SqlValue::Integer(-2000)
    );
}

async fn insert_workers(store: &Arc<LocalServiceData>, workers: u32, per_worker: u32) {
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE ins (id INTEGER PRIMARY KEY, w INTEGER, n INTEGER)"))
        .await
        .expect("create");
    let mut handles = Vec::new();
    for w in 0..workers {
        let sql = sql.clone();
        handles.push(tokio::spawn(async move {
            for n in 0..per_worker {
                sql.execute(st_args(
                    "INSERT INTO ins (w, n) VALUES (?, ?)",
                    vec![SqlValue::Integer(w as i64), SqlValue::Integer(n as i64)],
                ))
                .await
                .expect("concurrent insert");
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sto_16_concurrent_single_row_inserts_are_all_durable_and_fast_enough() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());

    let started = Instant::now();
    insert_workers(&store, 8, 1250).await;
    let elapsed = started.elapsed();

    let sql = store.sql(svc(1));
    assert_eq!(count(&sql, "ins").await, 10_000, "no insert may be lost");
    let distinct = sql
        .execute(st("SELECT count(DISTINCT w || ':' || n) FROM ins"))
        .await
        .expect("distinct");
    assert_eq!(distinct.rows, vec![vec![SqlValue::Integer(10_000)]]);
    let rate = 10_000.0 / elapsed.as_secs_f64();
    assert!(rate >= 1000.0, "only {rate:.0} inserts/s: WAL + synchronous=NORMAL should be far faster (STO-16 asks >= 5,000/s on the reference disk)");
}

// ---- B3.4: guest SQL limits and result caps --------------------------------------------------------

fn rows_sql(n: u32) -> String {
    format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT x FROM c")
}

#[tokio::test]
async fn ac_b34_results_over_ten_thousand_rows_fail_with_sql_result_too_large() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v INTEGER)")).await.expect("create");

    assert_eq!(
        sql.execute(st(&rows_sql(10_000))).await.expect("at the cap").rows.len(),
        10_000
    );
    let err = sql.execute(st(&rows_sql(10_001))).await.expect_err("over the cap");
    assert!(matches!(err, StoreError::SqlResultTooLarge), "got {err:?}");
    assert_eq!(err.code(), "sql_result_too_large");

    // Every path that materializes rows is capped: read and write batches and transactions.
    let err = sql
        .batch(vec![st(&rows_sql(10_001))], BatchMode::Read)
        .await
        .expect_err("read batch");
    assert_eq!(err.code(), "sql_result_too_large");
    let err = sql
        .batch(
            vec![st("INSERT INTO t VALUES (1)"), st(&rows_sql(10_001))],
            BatchMode::Write,
        )
        .await
        .expect_err("write batch");
    assert_eq!(err.code(), "sql_result_too_large");
    assert_eq!(count(&sql, "t").await, 0, "the failed write batch rolled back");
    let mut tx = sql.begin().await.expect("begin");
    assert_eq!(
        tx.execute(st(&rows_sql(10_001))).await.expect_err("tx").code(),
        "sql_result_too_large"
    );
    tx.rollback().await.expect("rollback");
}

#[tokio::test]
async fn ac_b34_results_over_the_byte_cap_fail_with_sql_result_too_large() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    // Default cap: 10 MiB of values per call.
    let blobs = |n: u32| {
        format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) SELECT zeroblob(1048576) FROM c")
    };
    assert_eq!(sql.execute(st(&blobs(9))).await.expect("9 MiB").rows.len(), 9);
    assert_eq!(
        sql.execute(st(&blobs(11))).await.expect_err("11 MiB").code(),
        "sql_result_too_large"
    );
    // The byte budget spans a whole batch.
    let err = sql
        .batch(vec![st(&blobs(6)), st(&blobs(6))], BatchMode::Read)
        .await
        .expect_err("12 MiB in one batch");
    assert_eq!(err.code(), "sql_result_too_large");

    // Both caps are configurable.
    let small = LocalServiceData::open(DataConfig {
        max_result_rows: 5,
        max_result_bytes: 400,
        ..data_cfg(&dir.path().join("small"))
    })
    .expect("open");
    let sql = small.sql(svc(1));
    assert_eq!(
        sql.execute(st(&rows_sql(6))).await.expect_err("rows").code(),
        "sql_result_too_large"
    );
    // A row costs its values' payload plus the bookkeeping around them (~56 bytes for one column).
    assert_eq!(
        sql.execute(st("SELECT zeroblob(360)")).await.expect_err("bytes").code(),
        "sql_result_too_large"
    );
    assert_eq!(
        sql.execute(st(&rows_sql(5)))
            .await
            .expect("within both caps")
            .rows
            .len(),
        5
    );
}

#[tokio::test]
async fn ac_b34_guest_connections_enforce_sqlite_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (v TEXT)")).await.expect("create");

    let deep_expr = format!("SELECT 1{}", "+1".repeat(600));
    let compound = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(100));
    let wide_table = format!(
        "CREATE TABLE wide ({})",
        (0..1001).map(|i| format!("c{i}")).collect::<Vec<_>>().join(", ")
    );
    let long_sql = format!("SELECT 1 /* {} */", "x".repeat(1024 * 1024));
    let over: Vec<(&str, SqlStatementOwned)> = vec![
        ("LENGTH", (String::from("SELECT zeroblob(17 * 1024 * 1024)"), vec![])),
        ("SQL_LENGTH", (long_sql, vec![])),
        ("COLUMN", (wide_table, vec![])),
        ("EXPR_DEPTH", (deep_expr, vec![])),
        ("COMPOUND_SELECT", (compound, vec![])),
        ("VARIABLE_NUMBER", (String::from("SELECT ?10001"), vec![])),
        (
            "LIKE_PATTERN_LENGTH",
            (String::from("SELECT 'a' LIKE ?"), vec![text(&"%".repeat(1001))]),
        ),
    ];
    for (limit, (q, args)) in over {
        let err = sql.execute(st_args(&q, args.clone())).await.expect_err(limit);
        assert_eq!(err.code(), "sql_error", "{limit}: {err:?}");
        // Readers carry the same limits (a read batch runs on one).
        if !q.starts_with("CREATE") {
            let err = sql
                .batch(vec![st_args(&q, args)], BatchMode::Read)
                .await
                .expect_err(limit);
            assert_eq!(err.code(), "sql_error", "{limit} on a reader: {err:?}");
        }
    }
    // Just under each limit works.
    sql.execute(st(&format!("SELECT 1{}", "+1".repeat(400))))
        .await
        .expect("expression depth 400");
    sql.execute(st(&format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(99))))
        .await
        .expect("100 compound terms");
    sql.execute(st_args("SELECT ?10000", vec![SqlValue::Null; 10_000]))
        .await
        .expect("variable 10000");
    sql.execute(st_args("SELECT 'a' LIKE ?", vec![text(&"%".repeat(1000))]))
        .await
        .expect("1000-byte pattern");
    // Multi-row VALUES are not compound terms.
    let values = (0..500).map(|i| format!("('{i}')")).collect::<Vec<_>>().join(", ");
    sql.execute(st(&format!("INSERT INTO t VALUES {values}")))
        .await
        .expect("500-row VALUES");
}

type SqlStatementOwned = (String, Vec<SqlValue>);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "STO-16 throughput bar (>= 5,000 inserts/s for 10 s) is defined for the reference disk; run in wave 4"]
async fn ac_sto_16_reference_disk_sustains_five_thousand_inserts_per_second() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE ins (id INTEGER PRIMARY KEY, v INTEGER)"))
        .await
        .expect("create");

    let deadline = Instant::now() + Duration::from_secs(10);
    let total = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for w in 0..16 {
        let (sql, total) = (sql.clone(), total.clone());
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                sql.execute(st_args("INSERT INTO ins (v) VALUES (?)", vec![SqlValue::Integer(w)]))
                    .await
                    .expect("insert");
                total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let n = total.load(Ordering::Relaxed);
    assert!(n >= 50_000, "only {n} inserts in 10 s");
    assert_eq!(count(&sql, "ins").await, n as i64);
}

/// SQL-002 / SQL-004: guest SQL cannot get native pointers (`fts3_tokenizer` returns the address
/// of a tokenizer module: an ASLR leak in a process shared with every tenant) and cannot rename
/// its own table into the reserved `_rt_` namespace.
#[tokio::test]
async fn sql_002_no_native_pointers_and_no_renames_into_the_reserved_namespace() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (x)")).await.expect("create");
    for stmt in [
        "SELECT fts3_tokenizer('simple')",
        "SELECT hex(fts3_tokenizer('porter'))",
        "SELECT FTS3_TOKENIZER('simple')",
        "ALTER TABLE t RENAME TO _rt_hidden",
        "ALTER TABLE t RENAME TO \"_RT_hidden\"",
        "/* c */ alter table t rename to _rt_x",
    ] {
        let err = sql.execute(st(stmt)).await.expect_err(stmt);
        assert_eq!(err.code(), "sql_not_authorized", "{stmt}: {err:?}");
    }
    sql.execute(st("ALTER TABLE t RENAME TO t2"))
        .await
        .expect("an ordinary rename");
    sql.execute(st("SELECT x FROM t2")).await.expect("renamed");
}
