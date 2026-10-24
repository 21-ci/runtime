//! KV compare-and-swap, expiry control and reverse listing (`std:kv` `cas`, `expire`, `ttl`,
//! `list({reverse})`): `cas` is one step on the service's writer, so concurrent swaps from many
//! tasks let exactly one win; an expired key counts as absent; the TTL of a swapped-in value is
//! the one passed to `cas`.
//!
//! TTL tests use short real sleeps: the store has no clock injection.

use crate::common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{KvListOptions, KvSetOptions, KvValue, ServiceDataStore, ValueTag};

fn json(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

fn raw(b: &[u8]) -> KvValue {
    KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::copy_from_slice(b),
    }
}

fn ttl(secs: u64) -> KvSetOptions {
    KvSetOptions {
        ttl: Some(Duration::from_secs(secs)),
    }
}

fn list(prefix: &str, cursor: Option<Bytes>, limit: usize, reverse: bool) -> KvListOptions {
    KvListOptions {
        prefix: Bytes::from(prefix.to_string()),
        cursor,
        limit,
        reverse,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_10_cas_from_an_absent_key_wins_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    assert!(kv
        .cas(b"k", None, Some(json("1")), KvSetOptions::default())
        .await
        .expect("cas"));
    assert!(
        !kv.cas(b"k", None, Some(json("2")), KvSetOptions::default())
            .await
            .expect("cas"),
        "the key exists now"
    );
    assert_eq!(kv.get(b"k").await.expect("get"), Some(json("1")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_11_cas_compares_bytes_and_type_tag_and_leaves_a_mismatch_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    kv.set(b"k", json("1"), KvSetOptions::default()).await.expect("seed");

    assert!(
        !kv.cas(b"k", Some(json("2")), Some(json("9")), KvSetOptions::default())
            .await
            .expect("cas"),
        "other bytes"
    );
    assert!(
        !kv.cas(b"k", Some(raw(b"1")), Some(json("9")), KvSetOptions::default())
            .await
            .expect("cas"),
        "same bytes, other tag"
    );
    assert!(
        !kv.cas(b"k", None, Some(json("9")), KvSetOptions::default())
            .await
            .expect("cas"),
        "expected absent, key present"
    );
    assert_eq!(
        kv.get(b"k").await.expect("get"),
        Some(json("1")),
        "a refused swap writes nothing"
    );

    assert!(kv
        .cas(b"k", Some(json("1")), Some(json("2")), KvSetOptions::default())
        .await
        .expect("cas"));
    assert_eq!(kv.get(b"k").await.expect("get"), Some(json("2")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_12_cas_with_null_next_deletes_and_a_bytes_value_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    kv.set(b"b", raw(&[0, 255, 7]), KvSetOptions::default())
        .await
        .expect("seed");

    assert!(kv
        .cas(b"b", Some(raw(&[0, 255, 7])), None, KvSetOptions::default())
        .await
        .expect("cas"));
    assert_eq!(kv.get(b"b").await.expect("get"), None, "next null deletes the key");
    assert!(kv
        .cas(b"b", None, Some(raw(&[1, 2])), KvSetOptions::default())
        .await
        .expect("cas"));
    assert_eq!(kv.get(b"b").await.expect("get"), Some(raw(&[1, 2])));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_13_an_expired_key_is_absent_for_cas_and_the_swapped_in_ttl_applies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    kv.set(b"e", json("1"), ttl(1)).await.expect("seed");
    assert!(!kv
        .cas(b"e", Some(json("2")), Some(json("3")), KvSetOptions::default())
        .await
        .expect("cas"));

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        kv.cas(b"e", None, Some(json("fresh")), ttl(1)).await.expect("cas"),
        "expired counts as absent"
    );
    assert_eq!(kv.get(b"e").await.expect("get"), Some(json("fresh")));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        kv.get(b"e").await.expect("get"),
        None,
        "the ttl passed to cas expired it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_14_concurrent_cas_from_many_tasks_exactly_one_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    let mut handles = Vec::new();
    for i in 0..32 {
        let kv = kv.clone();
        handles.push(tokio::spawn(async move {
            let won = kv
                .cas(
                    b"lock",
                    None,
                    Some(json(&format!("\"owner-{i}\""))),
                    KvSetOptions::default(),
                )
                .await
                .expect("cas");
            (i, won)
        }));
    }
    let mut winners = Vec::new();
    for h in handles {
        let (i, won) = h.await.expect("join");
        if won {
            winners.push(i);
        }
    }
    assert_eq!(winners.len(), 1, "exactly one swap succeeds: {winners:?}");
    assert_eq!(
        kv.get(b"lock").await.expect("get"),
        Some(json(&format!("\"owner-{}\"", winners[0])))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_15_expire_and_ttl_set_read_and_clear_the_expiry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    assert_eq!(kv.ttl(b"x").await.expect("ttl"), None, "missing key");
    assert!(
        !kv.expire(b"x", Some(Duration::from_secs(5))).await.expect("expire"),
        "missing key"
    );

    kv.set(b"x", json("1"), KvSetOptions::default()).await.expect("seed");
    assert_eq!(kv.ttl(b"x").await.expect("ttl"), None, "no expiry");
    assert!(kv.expire(b"x", Some(Duration::from_secs(60))).await.expect("expire"));
    let left = kv.ttl(b"x").await.expect("ttl").expect("has a ttl");
    assert!(
        left > Duration::from_secs(59) && left <= Duration::from_secs(60),
        "{left:?}"
    );
    assert!(kv.expire(b"x", None).await.expect("expire"), "null clears the expiry");
    assert_eq!(kv.ttl(b"x").await.expect("ttl"), None);
    assert_eq!(kv.get(b"x").await.expect("get"), Some(json("1")), "the value is kept");

    kv.expire(b"x", Some(Duration::from_millis(200))).await.expect("expire");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !kv.expire(b"x", Some(Duration::from_secs(5))).await.expect("expire"),
        "an expired key is absent"
    );
    assert_eq!(kv.get(b"x").await.expect("get"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_16_reverse_list_pages_from_the_end_of_a_prefix_and_skips_expired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    for k in ["a1", "a2", "a3", "a4", "a5", "b1"] {
        kv.set(k.as_bytes(), json("1"), KvSetOptions::default())
            .await
            .expect("seed");
    }
    kv.set(b"a3", json("1"), ttl(1)).await.expect("ttl");
    kv.set(b"a0", json("1"), KvSetOptions::default()).await.expect("seed");
    kv.set(b"a6", json("1"), KvSetOptions::default()).await.expect("seed");
    kv.set(b"a7", json("1"), ttl(0)).await.expect("ttl");
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let names = |page: &rt_store::KvPage| {
        page.entries
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect::<Vec<_>>()
    };

    let first = kv.list(list("a", None, 2, true)).await.expect("list");
    assert_eq!(names(&first), vec!["a6", "a5"]);
    let second = kv
        .list(list("a", first.next_cursor.clone(), 2, true))
        .await
        .expect("list");
    assert_eq!(names(&second), vec!["a4", "a2"], "a3 expired, so it is skipped");
    let third = kv
        .list(list("a", second.next_cursor.clone(), 2, true))
        .await
        .expect("list");
    assert_eq!(names(&third), vec!["a1", "a0"]);
    assert!(third.next_cursor.is_none());

    let all = kv.list(list("", None, 100, true)).await.expect("list");
    assert_eq!(names(&all), vec!["b1", "a6", "a5", "a4", "a2", "a1", "a0"]);
    let forward = kv.list(list("a", None, 100, false)).await.expect("list");
    assert_eq!(
        names(&forward),
        vec!["a0", "a1", "a2", "a4", "a5", "a6"],
        "forward unchanged"
    );
}
