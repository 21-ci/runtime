//! TK-002: fault injection. A store with a [`Faults`] plan fails its next operations with the
//! planned errors and then works normally again, so callers' error paths can be tested.

use crate::common;

use bytes::Bytes;
use common::*;
use rt_store::{
    Fault, Faults, KvSetOptions, KvValue, MetaStore, NewMessage, QueueStore, ServiceDataStore, SqliteQueueStore,
    StoreError, ValueTag,
};

#[tokio::test]
async fn planned_faults_fail_the_next_data_operations_then_the_store_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let faults = Faults::new();
    store.inject_faults(faults.clone());
    let kv = store.kv(svc(1));
    let v = KvValue {
        tag: ValueTag::Json,
        data: Bytes::from_static(b"1"),
    };

    faults.fail_next(1, Fault::DiskFull);
    faults.fail_next(1, Fault::Busy);
    let e = kv
        .set(b"k", v.clone(), KvSetOptions::default())
        .await
        .expect_err("disk full");
    assert!(matches!(e, StoreError::StorageQuota), "{e:?}");
    let e = kv.get(b"k").await.expect_err("busy");
    assert!(matches!(e, StoreError::SqlBusy), "{e:?}");
    assert_eq!(faults.pending(), 0);
    assert_eq!(faults.injected(), 2);

    kv.set(b"k", v.clone(), KvSetOptions::default())
        .await
        .expect("recovered");
    assert_eq!(kv.get(b"k").await.expect("get"), Some(v));

    faults.fail_next(1, Fault::Corrupt);
    let e = store.sql(svc(1)).execute(st("SELECT 1")).await.expect_err("corrupt");
    assert_eq!(e.code(), "internal");
    store.sql(svc(1)).execute(st("SELECT 1")).await.expect("recovered");
}

#[tokio::test]
async fn planned_faults_fail_meta_and_queue_operations() {
    let m = meta();
    let faults = Faults::new();
    m.inject_faults(faults.clone());
    faults.fail_next(1, Fault::Io);
    let e = m.create_tenant(new_tenant("t")).await.expect_err("io");
    assert_eq!(e.code(), "internal");
    let t = m.create_tenant(new_tenant("t")).await.expect("recovered");
    assert_eq!(m.get_tenant(t.id).await.expect("get").map(|x| x.id), Some(t.id));

    let q = SqliteQueueStore::open_in_memory(5).expect("queue");
    let qf = Faults::new();
    q.inject_faults(qf.clone());
    qf.fail_next(2, Fault::DiskFull);
    for _ in 0..2 {
        let e = q
            .enqueue(svc(1), NewMessage::new(&b"x"[..]))
            .await
            .expect_err("planned");
        assert!(matches!(e, StoreError::StorageQuota), "{e:?}");
    }
    q.enqueue(svc(1), NewMessage::new(&b"x"[..])).await.expect("recovered");
    qf.fail_next(3, Fault::Busy);
    qf.clear();
    assert_eq!(qf.pending(), 0, "clear drops what is still planned");
    assert_eq!(q.stats(svc(1)).await.expect("stats").depth, 1);
}

#[tokio::test]
async fn pass_next_lets_the_first_operations_through_so_a_later_one_can_be_the_one_that_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let faults = Faults::new();
    store.inject_faults(faults.clone());
    let kv = store.kv(svc(1));

    faults.pass_next(2);
    faults.fail_next(1, Fault::Io);
    kv.get(b"a").await.expect("first passes");
    kv.get(b"b").await.expect("second passes");
    let e = kv.get(b"c").await.expect_err("third fails");
    assert_eq!(e.code(), "internal");
    assert_eq!((faults.pending(), faults.injected()), (0, 1));
    kv.get(b"d").await.expect("recovered");
}
