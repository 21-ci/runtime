//! `SqlBackend::copy_into`: branch `copy_data`, on the heavy-job thread through `snapshot_db`.

use crate::common;

use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{LocalServiceData, ServiceDataStore};
use rt_types::{Body, ServiceId};

const BLOB_MAX: u64 = 1 << 30;

async fn seed(store: &LocalServiceData, s: ServiceId) {
    let sql = store.sql(s);
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .expect("create");
    sql.execute(st("INSERT INTO t (v) VALUES ('one')"))
        .await
        .expect("insert");
}

async fn count(store: &LocalServiceData, s: ServiceId) -> i64 {
    let r = store.sql(s).execute(st("SELECT count(*) FROM t")).await.expect("count");
    match r.rows[0][0] {
        rt_store::SqlValue::Integer(n) => n,
        ref other => panic!("{other:?}"),
    }
}

async fn put(store: &LocalServiceData, s: ServiceId, key: &str, body: &'static [u8]) {
    store
        .blob(s)
        .put(key, Body::from_bytes(Bytes::from_static(body)), None, BLOB_MAX)
        .await
        .expect("put");
}

async fn read_blob(store: &LocalServiceData, s: ServiceId, key: &str) -> Option<Vec<u8>> {
    let obj = store.blob(s).get(key).await.expect("get")?;
    Some(obj.body.collect(BLOB_MAX).await.expect("body").to_vec())
}

fn db(root: &std::path::Path, s: ServiceId) -> std::path::PathBuf {
    root.join("services").join(s.to_string()).join("data.db")
}

fn blob_files(root: &std::path::Path, s: ServiceId) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(root.join("blobs").join(s.to_string()).join("objects"))
        .map(|d| d.map(|e| e.expect("entry").path()).collect())
        .unwrap_or_default()
}

/// Rows and blobs come along, each store then goes its own way; the bytes count both.
#[tokio::test]
async fn copy_brings_rows_and_blobs_and_the_two_diverge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    put(&store, svc(1), "a", b"alpha").await;
    put(&store, svc(1), "b", b"beta!").await;

    let bytes = store.sql(svc(1)).copy_into(svc(2)).await.expect("copy");
    let db_size = std::fs::metadata(db(dir.path(), svc(2))).expect("stat").len();
    assert_eq!(bytes, db_size + 10);

    assert_eq!(count(&store, svc(2)).await, 1);
    assert_eq!(read_blob(&store, svc(2), "a").await.as_deref(), Some(&b"alpha"[..]));
    assert_eq!(read_blob(&store, svc(2), "b").await.as_deref(), Some(&b"beta!"[..]));

    store
        .sql(svc(2))
        .execute(st("INSERT INTO t (v) VALUES ('br')"))
        .await
        .expect("write");
    store.blob(svc(2)).delete("a").await.expect("delete");
    assert_eq!(count(&store, svc(1)).await, 1, "the source did not move");
    assert_eq!(read_blob(&store, svc(1), "a").await.as_deref(), Some(&b"alpha"[..]));
}

/// Timers belong to the running service; the copy has none, and is a plain file.
#[tokio::test]
async fn copy_drops_durable_alarms_and_stands_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    store
        .pragma(svc(1), "journal_mode")
        .await
        .expect("opens the writer, so the reserved tables exist");
    {
        let c = rusqlite::Connection::open(db(dir.path(), svc(1))).expect("open");
        c.execute("INSERT INTO _rt_durable_alarm (object, at_ms) VALUES ('o', 1)", [])
            .expect("alarm");
    }

    store.sql(svc(1)).copy_into(svc(2)).await.expect("copy");

    let head = std::fs::read(db(dir.path(), svc(2))).expect("read");
    assert_eq!((head[18], head[19]), (1, 1), "the file on disk does not announce WAL");
    let dir2 = db(dir.path(), svc(2));
    let names: Vec<String> = std::fs::read_dir(dir2.parent().expect("parent"))
        .expect("dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["data.db"], "no temp file left");

    let c = rusqlite::Connection::open(&dir2).expect("open");
    let n: i64 = c
        .query_row("SELECT count(*) FROM _rt_durable_alarm", [], |r| r.get(0))
        .expect("alarms");
    assert_eq!(n, 0);
    let c = rusqlite::Connection::open(db(dir.path(), svc(1))).expect("open");
    let n: i64 = c
        .query_row("SELECT count(*) FROM _rt_durable_alarm", [], |r| r.get(0))
        .expect("alarms");
    assert_eq!(n, 1, "the source keeps its timers");
}

/// A row whose file was already lost does not come along, as a read would treat it.
#[tokio::test]
async fn copy_drops_a_blob_row_whose_file_is_gone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    put(&store, svc(1), "lost", b"x").await;
    for f in blob_files(dir.path(), svc(1)) {
        std::fs::remove_file(f).expect("rm");
    }
    put(&store, svc(1), "kept", b"y").await;

    store.sql(svc(1)).copy_into(svc(2)).await.expect("copy");
    assert!(store.blob(svc(2)).head("lost").await.expect("head").is_none());
    assert!(store.blob(svc(2)).head("kept").await.expect("head").is_some());
}

/// The job does not hold the source's writer, and a blob deleted after the snapshot row was
/// taken still copies: the hold defers its unlink.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_and_blob_deletes_go_on_during_a_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    put(&store, svc(1), "a", b"alpha").await;

    let (release, parked) = mpsc::channel::<()>();
    let (entered_tx, entered) = mpsc::channel::<()>();
    store.run_on_heavy_thread(Box::new(move || {
        let _ = entered_tx.send(());
        let _ = parked.recv();
    }));
    entered.recv().expect("parked");

    let sql = store.sql(svc(1));
    let copy = tokio::spawn(async move { sql.copy_into(svc(2)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.holds().is_held(svc(1)), "held from before the snapshot");

    tokio::time::timeout(Duration::from_secs(5), async {
        store
            .sql(svc(1))
            .execute(st("INSERT INTO t (v) VALUES ('w')"))
            .await
            .expect("write");
        assert!(store.blob(svc(1)).delete("a").await.expect("delete"));
    })
    .await
    .expect("the source stalled behind a copy");
    assert_eq!(blob_files(dir.path(), svc(1)).len(), 1, "unlink deferred");

    drop(release);
    copy.await.expect("join").expect("copy");
    assert_eq!(blob_files(dir.path(), svc(1)).len(), 0, "deferred delete drained");
    assert!(!store.holds().is_held(svc(1)));
}

/// A failure leaves no database behind and no half of the blobs; the hold is released.
#[tokio::test]
async fn a_failed_copy_leaves_no_database_and_no_blobs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    put(&store, svc(1), "a", b"alpha").await;
    let files = blob_files(dir.path(), svc(1));

    // A directory where the blob file must go makes the copy fail.
    let dst = dir.path().join("blobs").join(svc(2).to_string()).join("objects");
    std::fs::create_dir_all(dst.join(files[0].file_name().expect("name"))).expect("mkdir");

    let err = store.sql(svc(1)).copy_into(svc(2)).await.expect_err("fails");
    assert_eq!(err.code(), "internal", "no paths reach the caller: {err:?}");
    assert!(!db(dir.path(), svc(2)).exists());
    assert!(!store.holds().is_held(svc(1)));
    let left = std::fs::read_dir(db(dir.path(), svc(2)).parent().expect("parent"))
        .expect("dir")
        .count();
    assert_eq!(left, 0, "no temp file");
}

/// A retired source is gone.
#[tokio::test]
async fn copying_a_retired_service_is_not_found() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&store, svc(1)).await;
    store.retire(svc(1)).expect("retire");
    let err = store.sql(svc(1)).copy_into(svc(2)).await.expect_err("gone");
    assert_eq!(err.code(), "not_found");
    assert!(!db(dir.path(), svc(2)).exists());
}

/// A service with no database yet copies as an empty one.
#[tokio::test]
async fn copying_a_service_with_no_data_yet_gives_an_empty_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    store.sql(svc(1)).copy_into(svc(2)).await.expect("copy");
    let r = store.sql(svc(2)).execute(st("SELECT 1")).await.expect("usable");
    assert_eq!(r.rows.len(), 1);
}
