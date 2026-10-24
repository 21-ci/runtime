//! Group commit (B3.2): KV and blob-index writes queued for the same database while its writer is
//! busy run in one `BEGIN IMMEDIATE … COMMIT`, each in its own savepoint, so one failing job rolls
//! back only itself and every caller still gets its own result.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{
    BlobBackend, DataConfig, KvBackend, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, ValueTag,
};
use rt_types::Body;

fn val(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

/// Holds the writer of `svc(1)` with an interactive transaction until it is committed or rolled back.
async fn hold_writer(store: &Arc<LocalServiceData>) -> Box<dyn rt_store::SqlTx> {
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE IF NOT EXISTS hold (v INTEGER)"))
        .await
        .expect("create");
    sql.begin().await.expect("begin")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b32_queued_kv_writes_share_commits_and_are_all_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let tx = hold_writer(&store).await;
    let before = store.write_commits();

    let mut handles = Vec::new();
    for i in 0..100 {
        let kv = store.kv(svc(1));
        handles.push(tokio::spawn(async move {
            kv.set(format!("k{i}").as_bytes(), val(&i.to_string()), KvSetOptions::default())
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.rollback().await.expect("release the writer");
    for h in handles {
        h.await.expect("join").expect("every queued write succeeds");
    }
    let commits = store.write_commits() - before;
    assert!(
        commits < 100,
        "100 queued writes must share transactions, took {commits} commits"
    );
    assert!(commits >= 2, "a group holds at most 64 jobs, took {commits} commits");

    // Durable: a fresh store over the same files sees every value.
    drop(store);
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    for i in 0..100 {
        assert_eq!(
            kv.get(format!("k{i}").as_bytes()).await.expect("get"),
            Some(val(&i.to_string()))
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b32_a_failing_job_does_not_fail_its_neighbours() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A 256 KiB database: one 900 KB value cannot fit, small ones can.
    let store = LocalServiceData::open(DataConfig {
        max_db_bytes: 256 * 1024,
        ..data_cfg(dir.path())
    })
    .expect("open");
    let tx = hold_writer(&store).await;
    let before = store.write_commits();

    let mut small = Vec::new();
    for i in 0..10 {
        let kv = store.kv(svc(1));
        small.push(tokio::spawn(async move {
            kv.set(format!("s{i}").as_bytes(), val("ok"), KvSetOptions::default())
                .await
        }));
        if i == 4 {
            let kv = store.kv(svc(1));
            let big = KvValue {
                tag: ValueTag::Bytes,
                data: Bytes::from(noise(900 * 1024, 7)),
            };
            small.push(tokio::spawn(async move {
                kv.set(b"big", big, KvSetOptions::default()).await
            }));
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.rollback().await.expect("release the writer");

    let mut failures = Vec::new();
    for h in small {
        if let Err(e) = h.await.expect("join") {
            failures.push(e.code());
        }
    }
    assert_eq!(failures, vec!["storage_quota"], "only the oversized write fails");
    assert!(store.write_commits() - before < 11, "the writes were grouped");
    let kv = store.kv(svc(1));
    for i in 0..10 {
        assert_eq!(
            kv.get(format!("s{i}").as_bytes()).await.expect("get"),
            Some(val("ok")),
            "neighbour s{i} kept"
        );
    }
    assert_eq!(kv.get(b"big").await.expect("get"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b32_writes_to_one_key_apply_in_queue_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let tx = hold_writer(&store).await;

    let mut handles = Vec::new();
    for i in 0..20 {
        let kv = store.kv(svc(1));
        handles.push(tokio::spawn(async move {
            if i % 5 == 4 {
                kv.delete(b"key").await.map(|_| ())
            } else {
                kv.set(b"key", val(&i.to_string()), KvSetOptions::default()).await
            }
        }));
        // Let each write reach the queue before the next one starts.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tx.rollback().await.expect("release the writer");
    for h in handles {
        h.await.expect("join").expect("write");
    }
    // Writes 0..=18 then a delete at 19: the key is gone; drop the delete and 18 must win.
    assert_eq!(
        store.kv(svc(1)).get(b"key").await.expect("get"),
        None,
        "the last queued write (a delete) wins"
    );

    let tx = hold_writer(&store).await;
    let mut handles = Vec::new();
    for i in 0..20 {
        let kv = store.kv(svc(1));
        handles.push(tokio::spawn(async move {
            kv.set(b"key", val(&i.to_string()), KvSetOptions::default()).await
        }));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tx.rollback().await.expect("release the writer");
    for h in handles {
        h.await.expect("join").expect("write");
    }
    assert_eq!(
        store.kv(svc(1)).get(b"key").await.expect("get"),
        Some(val("19")),
        "the last queued set wins"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_b32_blob_index_writes_are_grouped_too_and_interactive_transactions_never_are() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let tx = hold_writer(&store).await;
    let before = store.write_commits();
    let mut handles = Vec::new();
    for i in 0..20u8 {
        let blob: Arc<dyn BlobBackend> = store.blob(svc(1));
        handles.push(tokio::spawn(async move {
            blob.put(
                &format!("b{i}"),
                Body::from_bytes(Bytes::from(vec![i; 10])),
                None,
                1 << 20,
            )
            .await
            .map(|_| ())
        }));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    // The transaction still owns the writer: its own statements are not mixed into a group.
    let mut tx = tx;
    tx.execute(st("INSERT INTO hold VALUES (1)"))
        .await
        .expect("tx still usable");
    tx.rollback().await.expect("release the writer");
    for h in handles {
        h.await.expect("join").expect("blob put");
    }
    assert!(store.write_commits() - before < 20, "blob index writes were grouped");
    for i in 0..20u8 {
        assert_eq!(
            store
                .blob(svc(1))
                .head(&format!("b{i}"))
                .await
                .expect("head")
                .expect("exists")
                .size,
            10
        );
    }
    let kv: Arc<dyn KvBackend> = store.kv(svc(1));
    assert_eq!(kv.get(b"nothing").await.expect("get"), None);
    let rows = store
        .sql(svc(1))
        .execute(st("SELECT count(*) FROM hold"))
        .await
        .expect("count");
    assert_eq!(
        rows.rows,
        vec![vec![rt_store::SqlValue::Integer(0)]],
        "the rolled-back transaction left nothing"
    );
}
