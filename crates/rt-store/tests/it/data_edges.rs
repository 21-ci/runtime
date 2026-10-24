//! Per-service data store edges: the guest authorizer's DDL arms (indexes, triggers, views,
//! temp schema), transactions that end in a failed COMMIT, by a statement that rolls the whole
//! transaction back, or by a purge, and maintenance over a damaged KV table.

use crate::common;

use std::path::PathBuf;
use std::sync::Arc;

use common::*;
use rt_store::{KvSetOptions, KvValue, ServiceDataStore, SqlBackend, SqlValue, StoreError, ValueTag};

fn db_file(root: &std::path::Path) -> PathBuf {
    let found = find_files(root, "data.db");
    assert_eq!(found.len(), 1, "one service database: {found:?}");
    found.into_iter().next().expect("one file")
}

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

#[tokio::test]
async fn a_bad_pragma_name_is_refused_and_a_good_one_reads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    assert!(matches!(store.pragma(svc(1), "").await, Err(StoreError::Invalid(_))));
    assert!(matches!(
        store.pragma(svc(1), "temp store").await,
        Err(StoreError::Invalid(_))
    ));
    assert!(store.pragma(svc(1), "temp_store").await.is_ok());
}

#[tokio::test]
async fn guest_ddl_for_indexes_triggers_views_and_the_temp_schema_is_judged_by_the_authorizer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (a INTEGER, b TEXT)"))
        .await
        .expect("table");

    for ok in [
        "CREATE INDEX ix_t_a ON t (a)",
        "DROP INDEX ix_t_a",
        "CREATE TRIGGER trg_t AFTER INSERT ON t BEGIN SELECT 1; END",
        "DROP TRIGGER trg_t",
        "CREATE VIEW v_t AS SELECT a FROM t",
        "DROP VIEW v_t",
        "REINDEX t",
        "ANALYZE t",
        "ALTER TABLE t ADD COLUMN c INTEGER",
        "SELECT '_rt_kv' AS name",
    ] {
        sql.execute(st(ok)).await.unwrap_or_else(|e| panic!("{ok}: {e:?}"));
    }
    for denied in ["CREATE TEMP TABLE scratch (a)", "CREATE TABLE temp.scratch2 (a)"] {
        assert!(sql.execute(st(denied)).await.is_err(), "{denied} is refused");
    }
}

#[tokio::test]
async fn a_commit_that_fails_leaves_nothing_behind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(2));
    sql.execute(st("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        .await
        .expect("parent");
    sql.execute(st(
        "CREATE TABLE child (pid INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)",
    ))
    .await
    .expect("child");

    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO child (pid) VALUES (7)"))
        .await
        .expect("a deferred violation is not yet an error");
    assert!(tx.commit().await.is_err(), "the deferred foreign key fails at COMMIT");
    assert_eq!(count(&sql, "child").await, 0, "the failed commit left no row");
}

#[tokio::test]
async fn a_statement_that_rolls_the_transaction_back_ends_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(3));
    sql.execute(st("CREATE TABLE t (a INTEGER)")).await.expect("table");
    sql.execute(st(
        "CREATE TRIGGER stop BEFORE INSERT ON t WHEN NEW.a < 0 BEGIN SELECT RAISE(ROLLBACK, 'negative'); END",
    ))
    .await
    .expect("trigger");

    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO t (a) VALUES (1)")).await.expect("first row");
    assert!(
        tx.execute(st("INSERT INTO t (a) VALUES (-1)")).await.is_err(),
        "RAISE(ROLLBACK) aborts the transaction"
    );
    assert!(
        tx.execute(st("INSERT INTO t (a) VALUES (2)")).await.is_err(),
        "the transaction is no longer active"
    );
    assert_eq!(count(&sql, "t").await, 0, "the first row was rolled back with it");
    assert!(
        tx.rollback().await.is_err(),
        "an ended transaction cannot be rolled back again"
    );
}

#[tokio::test]
async fn dropping_an_open_transaction_rolls_it_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(4));
    sql.execute(st("CREATE TABLE t (a INTEGER)")).await.expect("table");
    {
        let mut tx = sql.begin().await.expect("begin");
        tx.execute(st("INSERT INTO t (a) VALUES (1)")).await.expect("insert");
    }
    assert_eq!(count(&sql, "t").await, 0);
}

#[tokio::test]
async fn a_transaction_whose_service_was_purged_is_an_error_to_use_or_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(5));
    sql.execute(st("CREATE TABLE t (a INTEGER)")).await.expect("table");
    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO t (a) VALUES (1)")).await.expect("insert");
    store
        .purge(svc(5))
        .expect("purge the service under the open transaction");
    assert!(tx.execute(st("INSERT INTO t (a) VALUES (2)")).await.is_err());
    assert!(tx.commit().await.is_err());
}

#[tokio::test]
async fn maintenance_survives_a_damaged_kv_table_and_sweeps_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(6));
    let v = KvValue {
        tag: ValueTag::Json,
        data: bytes::Bytes::from_static(b"1"),
    };
    kv.set(b"k", v, KvSetOptions::default()).await.expect("kv");
    let file = db_file(dir.path());
    rusqlite::Connection::open(&file)
        .expect("second connection")
        .execute_batch("ALTER TABLE _rt_kv RENAME TO _rt_kv_gone")
        .expect("break kv");
    assert_eq!(store.sweep_open().await.expect("the sweep reports and continues"), 0);
}

#[tokio::test]
async fn a_batch_whose_commit_fails_is_an_error_and_leaves_no_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(7));
    sql.execute(st("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        .await
        .expect("parent");
    sql.execute(st(
        "CREATE TABLE child (pid INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)",
    ))
    .await
    .expect("child");
    let r = sql
        .batch(
            vec![st("INSERT INTO child (pid) VALUES (8)")],
            rt_store::BatchMode::Write,
        )
        .await;
    assert!(r.is_err(), "the deferred foreign key fails the batch's COMMIT");
    assert_eq!(count(&sql, "child").await, 0);
}

#[tokio::test]
async fn concurrent_writes_that_each_break_a_deferred_key_all_fail_and_leave_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(17));
    sql.execute(st("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
        .await
        .expect("parent");
    sql.execute(st(
        "CREATE TABLE child (pid INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)",
    ))
    .await
    .expect("child");
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let sql = sql.clone();
        tasks.push(tokio::spawn(async move {
            sql.execute(st("INSERT INTO child (pid) VALUES (1)")).await
        }));
    }
    for t in tasks {
        assert!(t.await.expect("task").is_err(), "a deferred violation fails each write");
    }
    assert_eq!(count(&sql, "child").await, 0);
}

#[tokio::test]
async fn virtual_tables_follow_the_authorizer_and_a_read_of_a_removed_database_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(18));
    sql.execute(st("CREATE TABLE t (a INTEGER)")).await.expect("table");
    sql.execute(st("CREATE VIRTUAL TABLE docs USING fts5 (body)"))
        .await
        .expect("fts5 is an ordinary module here");
    assert!(
        sql.execute(st("CREATE VIRTUAL TABLE grid USING rtree (id, a0, a1)"))
            .await
            .is_err(),
        "rtree is refused"
    );

    let kv = store.kv(svc(19));
    kv.set(
        b"k",
        KvValue {
            tag: ValueTag::Json,
            data: bytes::Bytes::from_static(b"1"),
        },
        KvSetOptions::default(),
    )
    .await
    .expect("kv");
    for f in find_files(dir.path(), "data.db") {
        std::fs::remove_file(f).expect("remove the database file");
    }
    assert!(
        kv.get(b"k").await.is_err(),
        "a read cannot open a database file that is gone"
    );
}
