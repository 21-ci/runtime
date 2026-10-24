//! Machine-local synchronous SQL edges: the symbolic name of every SQLite error code, transaction
//! levels and what ends them, a connection used from inside its own statement, slot exhaustion,
//! and an open that the disk refuses.

use crate::common;

use std::sync::Arc;

use common::*;
use rt_store::{
    DataConfig, KvSetOptions, KvValue, LocalDb, LocalError, LocalServiceData, LocalSqlSource, ServiceDataStore,
    StoreError, TxMode, ValueTag,
};

fn local(root: &std::path::Path) -> Arc<LocalServiceData> {
    LocalServiceData::open(DataConfig::new(root)).expect("service data store opens")
}

fn source(store: &LocalServiceData) -> Arc<dyn LocalSqlSource> {
    store.local_sql().expect("the local store serves synchronous SQL")
}

fn err(errno: i32, error: StoreError) -> LocalError {
    LocalError {
        error,
        errno,
        offset: -1,
    }
}

fn count(db: &LocalDb, table: &str) -> i64 {
    let mut n = -1;
    db.run(&format!("SELECT count(*) FROM {table}"), &[], &mut |row| {
        n = match row.get(0) {
            rt_store::SqlRef::Integer(i) => i,
            other => panic!("count returned {other:?}"),
        };
        Ok(true)
    })
    .expect("count");
    n
}

#[test]
fn sqlite_error_codes_have_their_symbolic_names_and_unknown_ones_fall_back_to_the_store_error() {
    let primary = [
        (1, "SQLITE_ERROR"),
        (2, "SQLITE_INTERNAL"),
        (3, "SQLITE_PERM"),
        (4, "SQLITE_ABORT"),
        (5, "SQLITE_BUSY"),
        (6, "SQLITE_LOCKED"),
        (7, "SQLITE_NOMEM"),
        (8, "SQLITE_READONLY"),
        (9, "SQLITE_INTERRUPT"),
        (10, "SQLITE_IOERR"),
        (11, "SQLITE_CORRUPT"),
        (13, "SQLITE_FULL"),
        (14, "SQLITE_CANTOPEN"),
        (17, "SQLITE_SCHEMA"),
        (18, "SQLITE_TOOBIG"),
        (19, "SQLITE_CONSTRAINT"),
        (20, "SQLITE_MISMATCH"),
        (21, "SQLITE_MISUSE"),
        (23, "SQLITE_AUTH"),
        (25, "SQLITE_RANGE"),
    ];
    let extended = [
        (275, "SQLITE_CONSTRAINT_CHECK"),
        (531, "SQLITE_CONSTRAINT_COMMITHOOK"),
        (787, "SQLITE_CONSTRAINT_FOREIGNKEY"),
        (1043, "SQLITE_CONSTRAINT_FUNCTION"),
        (1299, "SQLITE_CONSTRAINT_NOTNULL"),
        (1555, "SQLITE_CONSTRAINT_PRIMARYKEY"),
        (1811, "SQLITE_CONSTRAINT_TRIGGER"),
        (2067, "SQLITE_CONSTRAINT_UNIQUE"),
        (2323, "SQLITE_CONSTRAINT_VTAB"),
        (2579, "SQLITE_CONSTRAINT_ROWID"),
        (517, "SQLITE_BUSY_SNAPSHOT"),
        (261, "SQLITE_BUSY_RECOVERY"),
    ];
    for (code, name) in primary.into_iter().chain(extended) {
        assert_eq!(
            err(code, StoreError::Internal("x".into())).sqlite_name(),
            name,
            "errno {code}"
        );
    }
    assert_eq!(err(0, StoreError::SqlTimeout).sqlite_name(), "SQLITE_INTERRUPT");
    assert_eq!(err(0, StoreError::SqlBusy).sqlite_name(), "SQLITE_BUSY");
    assert_eq!(err(0, StoreError::SqlNotAuthorized).sqlite_name(), "SQLITE_AUTH");
    assert_eq!(err(0, StoreError::StorageQuota).sqlite_name(), "SQLITE_FULL");
    assert_eq!(err(0, StoreError::SqlResultTooLarge).sqlite_name(), "SQLITE_TOOBIG");
    assert_eq!(err(0, StoreError::Internal("n".into())).sqlite_name(), "SQLITE_ERROR");
    assert_eq!(
        err(99, StoreError::Internal("n".into())).sqlite_name(),
        "SQLITE_ERROR",
        "an unknown code"
    );
    assert_eq!(err(0, StoreError::Sql("boom".into())).message(), "boom");
    assert_eq!(err(0, StoreError::SqlBusy).message(), StoreError::SqlBusy.to_string());
}

#[test]
fn a_source_has_a_stable_identity_per_store_and_a_connection_knows_its_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let other = local(&dir.path().join("other"));
    let src = source(&store);
    assert_eq!(src.id(), source(&store).id(), "one store, one identity");
    assert_ne!(src.id(), source(&other).id(), "two stores never share one");

    let db = src.open(svc(3)).expect("open");
    assert_eq!(db.service(), svc(3));
    let mut seen = 0;
    db.run("SELECT 1 AS one", &[], &mut |row| {
        assert!(!row.is_empty());
        seen += 1;
        Ok(true)
    })
    .expect("run");
    assert_eq!(seen, 1);
}

#[test]
fn transaction_levels_are_begun_exclusive_or_as_savepoints_and_each_end_is_its_own() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(4)).expect("open");
    db.run("CREATE TABLE t (a INTEGER)", &[], &mut |_| Ok(true))
        .expect("table");

    db.begin(TxMode::Exclusive).expect("outer");
    assert_eq!(db.tx_depth(), 1);
    assert!(db.in_transaction());
    db.begin(TxMode::Deferred).expect("inner, a savepoint");
    assert_eq!(db.tx_depth(), 2);
    db.run("INSERT INTO t (a) VALUES (1)", &[], &mut |_| Ok(true))
        .expect("insert");
    db.commit().expect("release the savepoint");
    assert_eq!(db.tx_depth(), 1);
    db.rollback().expect("roll back the outer level");
    assert_eq!(db.tx_depth(), 0);
    assert!(!db.in_transaction());
    assert_eq!(count(&db, "t"), 0, "the savepoint's rows went with the outer rollback");
}

#[test]
fn a_connection_used_from_inside_its_own_statement_refuses_nested_work() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(5)).expect("open");
    db.begin(TxMode::Deferred).expect("begin");
    let mut checked = false;
    db.run("SELECT 1", &[], &mut |_| {
        assert!(
            db.in_transaction(),
            "the transaction is visible from inside the statement"
        );
        assert!(db.commit().is_err(), "a nested commit is refused");
        assert!(db.begin(TxMode::Deferred).is_err(), "a nested begin is refused");
        assert!(!db.reset_open_tx(), "nothing is reset from inside a call");
        checked = true;
        Ok(true)
    })
    .expect("run");
    assert!(checked);
    db.rollback().expect("the outer transaction still ends normally");
}

#[test]
fn a_commit_after_an_error_ended_the_transaction_reports_it_and_a_rollback_is_quiet() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(6)).expect("open");
    db.run("CREATE TABLE u (a INTEGER)", &[], &mut |_| Ok(true))
        .expect("table");
    db.run(
        "CREATE TRIGGER stop BEFORE INSERT ON u BEGIN SELECT RAISE(ROLLBACK, 'no'); END",
        &[],
        &mut |_| Ok(true),
    )
    .expect("trigger");

    db.begin(TxMode::Deferred).expect("begin");
    assert!(db.run("INSERT INTO u (a) VALUES (1)", &[], &mut |_| Ok(true)).is_err());
    let e = db.commit().expect_err("the transaction was ended by the error");
    assert!(matches!(e.error, StoreError::Sql(_)), "{e:?}");
    assert_eq!(db.tx_depth(), 0);

    db.begin(TxMode::Deferred).expect("begin again");
    assert!(db.run("INSERT INTO u (a) VALUES (2)", &[], &mut |_| Ok(true)).is_err());
    db.rollback().expect("a rollback after an ended transaction is quiet");
    assert_eq!(db.tx_depth(), 0);
}

#[test]
fn a_failed_commit_rolls_the_transaction_back_so_nothing_stays_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(7)).expect("open");
    db.run("CREATE TABLE parent (id INTEGER PRIMARY KEY)", &[], &mut |_| Ok(true))
        .expect("parent");
    db.run(
        "CREATE TABLE child (pid INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)",
        &[],
        &mut |_| Ok(true),
    )
    .expect("child");
    db.begin(TxMode::Deferred).expect("begin");
    db.run("INSERT INTO child (pid) VALUES (9)", &[], &mut |_| Ok(true))
        .expect("a deferred violation is not yet an error");
    assert!(db.commit().is_err(), "the deferred foreign key fails at COMMIT");
    assert!(!db.in_transaction(), "the failed COMMIT rolled the transaction back");
    assert_eq!(count(&db, "child"), 0);
}

#[test]
fn a_transaction_outlives_its_slot_only_as_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = DataConfig::new(dir.path());
    cfg.max_open_dbs = 1;
    let store = LocalServiceData::open(cfg).expect("open");
    let src = source(&store);

    let a = src.open(svc(1)).expect("the first service takes the only slot");
    a.begin(TxMode::Deferred).expect("a transaction on it");
    let e = open_err(src.open(svc(2)));
    assert!(matches!(e.error, StoreError::SqlBusy), "{e:?}");
    a.rollback().expect("end the transaction");
    src.open(svc(2)).expect("the slot is free again");
}

#[tokio::test]
async fn an_open_that_the_disk_refuses_is_an_error_and_frees_its_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let kv = store.kv(svc(8));
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
    let file = find_files(dir.path(), "data.db");
    assert_eq!(file.len(), 1, "one service database");
    std::fs::remove_file(&file[0]).expect("remove the file");
    std::fs::create_dir(&file[0]).expect("a directory where the database is");
    let e = open_err(source(&store).open(svc(8)));
    assert!(
        matches!(
            e.error,
            StoreError::Sql(_) | StoreError::Sqlite(_) | StoreError::Io(_) | StoreError::Internal(_)
        ),
        "{e:?}"
    );
}

/// The error of an open (a `LocalDb` has no `Debug`, so `expect_err` is not available).
fn open_err(r: Result<LocalDb, LocalError>) -> LocalError {
    match r {
        Ok(_) => panic!("the open should have failed"),
        Err(e) => e,
    }
}

#[test]
fn a_connection_whose_open_finished_late_is_installed_or_disposed_when_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = DataConfig::new(dir.path());
    cfg.sql_sync_budget = std::time::Duration::ZERO;
    let store = LocalServiceData::open(cfg).expect("open");
    let db = source(&store)
        .open(svc(10))
        .expect("the open goes on in the background");
    std::thread::sleep(std::time::Duration::from_millis(300));
    db.close();
    assert!(!db.is_open(), "closing drops the connection that finished opening");
}

#[tokio::test]
async fn a_pending_open_of_a_purged_service_does_not_install_a_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = DataConfig::new(dir.path());
    cfg.sql_sync_budget = std::time::Duration::ZERO;
    let store = LocalServiceData::open(cfg).expect("open");
    let kv = store.kv(svc(11));
    kv.set(
        b"k",
        KvValue {
            tag: ValueTag::Json,
            data: bytes::Bytes::from_static(b"1"),
        },
        KvSetOptions::default(),
    )
    .await
    .expect("the service has a database");
    let db = source(&store).open(svc(11)).expect("pending open");
    std::thread::sleep(std::time::Duration::from_millis(300));
    store.purge(svc(11)).expect("purge under the open");
    assert!(
        db.run("SELECT 1", &[], &mut |_| Ok(true)).is_err(),
        "a purged service's connection is refused"
    );
}

#[tokio::test]
async fn a_retired_service_has_no_synchronous_connection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    store.retire(svc(12)).expect("retire");
    let e = open_err(source(&store).open(svc(12)));
    assert!(matches!(&e.error, StoreError::Sql(m) if m.contains("deleted")), "{e:?}");
}

#[test]
fn a_purge_under_an_open_local_transaction_takes_the_transaction_with_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(14)).expect("open");
    db.run("CREATE TABLE t (a INTEGER)", &[], &mut |_| Ok(true))
        .expect("table");
    db.begin(TxMode::Deferred).expect("begin");
    db.run("INSERT INTO t (a) VALUES (1)", &[], &mut |_| Ok(true))
        .expect("insert");
    store.purge(svc(14)).expect("purge");
    assert!(
        db.commit().is_err(),
        "a commit after the purge is an error, not a silent no-op"
    );
}

#[test]
fn closing_from_inside_the_statement_in_progress_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    let db = source(&store).open(svc(16)).expect("open");
    db.run("SELECT 1", &[], &mut |_| {
        db.close();
        Ok(true)
    })
    .expect("run");
    assert!(db.is_open(), "the connection in use stays open");
}

#[test]
fn an_open_that_cannot_create_its_service_directory_reports_the_io_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local(dir.path());
    std::fs::remove_dir_all(dir.path().join("services")).expect("the services directory exists after open");
    std::fs::write(dir.path().join("services"), b"a file, not a directory").expect("a file in its place");
    let e = open_err(source(&store).open(svc(15)));
    assert!(matches!(e.error, StoreError::Io(_)), "{e:?}");
}
