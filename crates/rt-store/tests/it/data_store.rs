//! rt-store per-service data lifecycle (STO-15 data side): the three backends share one
//! per-service namespace and `purge` removes all of it without touching other services.

use crate::common;

use bytes::Bytes;
use common::*;
use rt_store::{KvBackend, KvListOptions, KvSetOptions, KvValue, ServiceDataStore, SqlValue, ValueTag};
use rt_types::Body;

fn kv_val(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

async fn seed(store: &dyn ServiceDataStore, n: u128) {
    store
        .kv(svc(n))
        .set(b"k", kv_val("1"), KvSetOptions::default())
        .await
        .expect("kv set");
    store
        .sql(svc(n))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .expect("sql create");
    store
        .sql(svc(n))
        .execute(st_args("INSERT INTO t VALUES (?)", vec![SqlValue::Integer(n as i64)]))
        .await
        .expect("sql insert");
    store
        .blob(svc(n))
        .put(
            "b",
            Body::from_bytes(Bytes::from(vec![n as u8; 100])),
            Some("x/y".into()),
            1 << 20,
        )
        .await
        .expect("blob put");
}

#[tokio::test]
async fn ac_sto_15_purge_removes_kv_sql_and_blobs_of_that_service_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&*store, 1).await;
    seed(&*store, 2).await;

    store.purge(svc(1)).expect("purge");

    // Fresh handles see an empty service.
    assert_eq!(store.kv(svc(1)).get(b"k").await.expect("kv get"), None);
    assert!(store
        .kv(svc(1))
        .list(KvListOptions {
            prefix: Bytes::new(),
            cursor: None,
            limit: 10,
            reverse: false
        })
        .await
        .expect("kv list")
        .entries
        .is_empty());
    assert!(
        store.sql(svc(1)).execute(st("SELECT * FROM t")).await.is_err(),
        "the SQL database must be gone"
    );
    assert!(store.blob(svc(1)).get("b").await.expect("blob get").is_none());
    assert_eq!(store.blob(svc(1)).total_bytes().await.expect("total"), 0);

    // On disk: nothing of service 1 is left.
    let needle = svc(1).bare();
    let leftovers: Vec<_> = all_files(dir.path())
        .into_iter()
        .filter(|p| p.to_string_lossy().contains(&needle))
        .collect();
    assert!(
        leftovers.is_empty(),
        "files of the purged service remain: {leftovers:?}"
    );

    // Service 2 is untouched.
    assert_eq!(store.kv(svc(2)).get(b"k").await.expect("kv get"), Some(kv_val("1")));
    let rows = store
        .sql(svc(2))
        .execute(st("SELECT v FROM t"))
        .await
        .expect("sql select");
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(2)]]);
    assert_eq!(
        store.blob(svc(2)).head("b").await.expect("head").expect("exists").size,
        100
    );

    // The purged service can start over.
    store
        .sql(svc(1))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .expect("recreate");
    store
        .kv(svc(1))
        .set(b"k", kv_val("2"), KvSetOptions::default())
        .await
        .expect("kv set again");
    assert_eq!(store.kv(svc(1)).get(b"k").await.expect("kv get"), Some(kv_val("2")));
}

#[tokio::test]
async fn ac_sto_15_purging_a_service_that_never_stored_anything_is_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    store.purge(svc(42)).expect("purge of an unknown service");
    seed(&*store, 1).await;
    store.purge(svc(42)).expect("purge must not disturb others");
    assert_eq!(store.kv(svc(1)).get(b"k").await.expect("kv get"), Some(kv_val("1")));
}

#[tokio::test]
async fn ac_data_01_kv_sql_and_blob_of_one_service_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = data(dir.path());
        seed(&*store, 7).await;
    }
    let store = data(dir.path());
    assert_eq!(store.kv(svc(7)).get(b"k").await.expect("kv"), Some(kv_val("1")));
    let rows = store.sql(svc(7)).execute(st("SELECT v FROM t")).await.expect("sql");
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(7)]]);
    let obj = store.blob(svc(7)).get("b").await.expect("blob").expect("exists");
    assert_eq!(obj.meta.content_type.as_deref(), Some("x/y"));
    assert_eq!(
        obj.body.collect(u64::MAX).await.expect("body").as_ref(),
        vec![7u8; 100].as_slice()
    );
}

#[tokio::test]
async fn ac_data_02_each_service_gets_its_own_data_db_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    for n in 1..=3 {
        seed(&*store, n).await;
    }
    assert_eq!(find_files(dir.path(), "data.db").len(), 3, "one data.db per service");
    let kv: std::sync::Arc<dyn KvBackend> = store.kv(svc(1));
    assert!(kv.get(b"k").await.expect("get").is_some());
}

/// KV-024: a retired (deleted) service never comes back: a task of it still running after the
/// delete cannot re-create its database or blob directory, every write is `not_found`, and reads
/// see nothing. `purge` alone (data reset) still lets the service start over.
#[tokio::test]
async fn kv_024_a_retired_service_never_comes_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    seed(&*store, 1).await;
    store.retire(svc(1)).expect("retire");
    let needle = svc(1).to_string();
    let none_left = |what: &str| {
        let left: Vec<_> = all_files(dir.path())
            .into_iter()
            .filter(|p| p.to_string_lossy().contains(&needle))
            .collect();
        assert!(left.is_empty(), "{what}: files of the retired service: {left:?}");
    };
    none_left("after retire");

    let e = store
        .kv(svc(1))
        .set(b"k", kv_val("2"), KvSetOptions::default())
        .await
        .expect_err("kv write");
    assert_eq!(e.code(), "not_found");
    assert!(store.kv(svc(1)).incr(b"n", 1, KvSetOptions::default()).await.is_err());
    assert!(store
        .sql(svc(1))
        .execute(st("CREATE TABLE t (v INTEGER)"))
        .await
        .is_err());
    assert!(store.sql(svc(1)).begin().await.is_err());
    let body = Body::from_bytes(Bytes::from_static(b"blob"));
    assert!(store.blob(svc(1)).put("b", body, None, 1 << 20).await.is_err());
    assert_eq!(
        store.kv(svc(1)).get(b"k").await.expect("read"),
        None,
        "reads see nothing"
    );
    none_left("after writes");

    // Other services are untouched; plain purge still lets a service start over.
    assert_eq!(store.kv(svc(2)).get(b"k").await.expect("kv get"), None);
    seed(&*store, 3).await;
    store.purge(svc(3)).expect("purge");
    store
        .kv(svc(3))
        .set(b"k", kv_val("2"), KvSetOptions::default())
        .await
        .expect("starts over");
}
