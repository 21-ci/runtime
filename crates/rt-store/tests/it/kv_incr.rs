//! Atomic KV increment (STO-17 … STO-19): `incr` is a coalescable write on the
//! service's writer, so concurrent increments lose nothing (also when they are grouped into one
//! commit), a missing key starts at 0, the TTL is set when the counter is created, and a value
//! that is not a JSON integer is refused without being touched.
//!
//! TTL tests use short real sleeps: the store has no clock injection.

use crate::common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, StoreError, ValueTag};

fn json(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

fn ttl(secs: u64) -> KvSetOptions {
    KvSetOptions {
        ttl: Some(Duration::from_secs(secs)),
    }
}

/// Holds the writer of `svc(1)` with an interactive transaction until it is rolled back.
async fn hold_writer(store: &Arc<LocalServiceData>) -> Box<dyn rt_store::SqlTx> {
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE IF NOT EXISTS hold (v INTEGER)"))
        .await
        .expect("create");
    sql.begin().await.expect("begin")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sto_17_concurrent_increments_from_many_tasks_lose_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    let mut handles = Vec::new();
    for _ in 0..16 {
        let kv = kv.clone();
        handles.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..50 {
                seen.push(kv.incr(b"hits", 1, KvSetOptions::default()).await.expect("incr"));
            }
            seen
        }));
    }
    let mut all = HashSet::new();
    for h in handles {
        for n in h.await.expect("join") {
            assert!(all.insert(n), "value {n} was returned twice: an increment was lost");
        }
    }
    assert_eq!(all.len(), 800);
    assert_eq!(all.iter().copied().min(), Some(1));
    assert_eq!(all.iter().copied().max(), Some(800));
    assert_eq!(
        kv.get(b"hits").await.expect("get"),
        Some(json("800")),
        "the counter reads as a JSON number"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_sto_17_grouped_increments_each_get_their_own_value_and_a_refused_one_fails_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    store
        .kv(svc(1))
        .set(b"text", json("\"x\""), KvSetOptions::default())
        .await
        .expect("seed");
    let tx = hold_writer(&store).await;
    let before = store.write_commits();

    let mut handles = Vec::new();
    for i in 0..100 {
        let kv = store.kv(svc(1));
        handles.push(tokio::spawn(
            async move { kv.incr(b"n", 2, KvSetOptions::default()).await },
        ));
        if i == 50 {
            let kv = store.kv(svc(1));
            handles.push(tokio::spawn(async move {
                kv.incr(b"text", 1, KvSetOptions::default()).await
            }));
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.rollback().await.expect("release the writer");

    let mut values = HashSet::new();
    let mut failures = Vec::new();
    for h in handles {
        match h.await.expect("join") {
            Ok(n) => assert!(values.insert(n), "{n} returned twice"),
            Err(e) => failures.push(e.code()),
        }
    }
    assert_eq!(
        failures,
        vec!["kv_not_integer"],
        "only the increment of the string fails"
    );
    assert_eq!(values, (1..=100).map(|i| i * 2).collect::<HashSet<i64>>());
    let commits = store.write_commits() - before;
    assert!(
        commits < 101,
        "the queued increments were grouped, took {commits} commits"
    );

    // Durable, and the refused value is untouched.
    drop(store);
    let store = data(dir.path());
    assert_eq!(store.kv(svc(1)).get(b"n").await.expect("get"), Some(json("200")));
    assert_eq!(store.kv(svc(1)).get(b"text").await.expect("get"), Some(json("\"x\"")));
}

#[tokio::test]
async fn ac_sto_19_a_missing_key_starts_at_zero_and_counters_interoperate_with_get_and_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    assert_eq!(kv.incr(b"c", 5, KvSetOptions::default()).await.expect("incr"), 5);
    assert_eq!(kv.incr(b"c", -7, KvSetOptions::default()).await.expect("decr"), -2);
    assert_eq!(kv.incr(b"c", 0, KvSetOptions::default()).await.expect("read"), -2);
    assert_eq!(kv.get(b"c").await.expect("get"), Some(json("-2")));

    // A number written by `set` (any JSON integer spelling) is a counter too.
    kv.set(b"s", json("41"), KvSetOptions::default()).await.expect("set");
    assert_eq!(kv.incr(b"s", 1, KvSetOptions::default()).await.expect("incr"), 42);
    kv.set(b"w", json(" 7 "), KvSetOptions::default()).await.expect("set");
    assert_eq!(kv.incr(b"w", 1, KvSetOptions::default()).await.expect("incr"), 8);

    // A deleted counter starts over.
    assert!(kv.delete(b"c").await.expect("delete"));
    assert_eq!(kv.incr(b"c", 1, KvSetOptions::default()).await.expect("incr"), 1);

    // The full i64 range.
    assert_eq!(
        kv.incr(b"min", i64::MIN, KvSetOptions::default()).await.expect("min"),
        i64::MIN
    );
    assert_eq!(
        kv.incr(b"min", i64::MAX, KvSetOptions::default()).await.expect("to -1"),
        -1
    );
}

#[tokio::test]
async fn ac_sto_19_non_integer_values_and_overflow_are_refused_and_left_unchanged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    let bytes = KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::from_static(b"1"),
    };
    kv.set(b"bytes", bytes.clone(), KvSetOptions::default())
        .await
        .expect("set bytes");
    let err = kv
        .incr(b"bytes", 1, KvSetOptions::default())
        .await
        .expect_err("bytes are not a counter");
    assert!(matches!(err, StoreError::KvNotInteger), "got {err:?}");
    assert_eq!(err.code(), "kv_not_integer");
    assert_eq!(kv.get(b"bytes").await.expect("get"), Some(bytes));

    for (i, text) in [
        "\"7\"",
        "1.5",
        "1e3",
        "-0",
        "true",
        "null",
        "{}",
        "[1]",
        "9223372036854775808",
        "1 2",
    ]
    .iter()
    .enumerate()
    {
        let key = format!("bad{i}");
        kv.set(key.as_bytes(), json(text), KvSetOptions::default())
            .await
            .expect("set");
        let err = kv
            .incr(key.as_bytes(), 1, KvSetOptions::default())
            .await
            .expect_err(text);
        assert!(matches!(err, StoreError::KvNotInteger), "{text}: got {err:?}");
        assert_eq!(
            kv.get(key.as_bytes()).await.expect("get"),
            Some(json(text)),
            "{text} unchanged"
        );
    }

    kv.set(b"max", json(&i64::MAX.to_string()), KvSetOptions::default())
        .await
        .expect("set");
    let err = kv
        .incr(b"max", 1, KvSetOptions::default())
        .await
        .expect_err("past i64::MAX");
    assert!(matches!(err, StoreError::KvOverflow), "got {err:?}");
    assert_eq!(err.code(), "kv_overflow");
    assert_eq!(kv.get(b"max").await.expect("get"), Some(json(&i64::MAX.to_string())));
    assert_eq!(
        kv.incr(b"max", -1, KvSetOptions::default()).await.expect("down"),
        i64::MAX - 1
    );

    let err = kv
        .incr(&vec![b'k'; 2 * 1024 + 1], 1, KvSetOptions::default())
        .await
        .expect_err("key over 2 KB");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");
}

#[tokio::test]
async fn ac_sto_18_the_ttl_is_set_when_the_counter_is_created_and_kept_by_later_increments() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    assert_eq!(kv.incr(b"win", 1, ttl(1)).await.expect("create"), 1);
    assert_eq!(
        kv.incr(b"win", 1, ttl(3600)).await.expect("incr"),
        2,
        "an existing counter keeps its expiry"
    );
    assert_eq!(
        kv.incr(b"forever", 1, KvSetOptions::default()).await.expect("no ttl"),
        1
    );
    assert_eq!(kv.incr(b"forever", 1, ttl(1)).await.expect("ttl on an existing key"), 2);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        kv.get(b"win").await.expect("get"),
        None,
        "the first increment's 1 s TTL holds"
    );
    assert_eq!(
        kv.get(b"forever").await.expect("get"),
        Some(json("2")),
        "a counter created without TTL keeps none"
    );

    // Expired (swept or not): the next increment starts a new counter with its own TTL.
    assert_eq!(kv.incr(b"win", 1, ttl(1)).await.expect("new window"), 1);
    assert_eq!(kv.get(b"win").await.expect("get"), Some(json("1")));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        kv.sweep_expired().await.expect("sweep"),
        1,
        "the new counter expires on its own TTL"
    );
    assert_eq!(
        kv.incr(b"win", 3, KvSetOptions::default())
            .await
            .expect("after the sweep"),
        3
    );
}
