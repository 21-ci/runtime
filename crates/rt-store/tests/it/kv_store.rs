//! rt-store KV tests (STO-01, STO-02, STO-03; contracts C-3): `_rt_kv` in the
//! service's `data.db`, lexicographic cursor listing, TTL hiding and sweeping, per-service
//! isolation, size limits.
//!
//! TTL tests use short real sleeps: the store has no clock injection.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{KvBackend, KvListOptions, KvSetOptions, KvValue, ServiceDataStore, StoreError, ValueTag};

fn json(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

fn raw(b: &[u8]) -> KvValue {
    KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::from(b.to_vec()),
    }
}

fn opts(prefix: &str, cursor: Option<Bytes>, limit: usize) -> KvListOptions {
    KvListOptions {
        prefix: Bytes::from(prefix.to_string()),
        cursor,
        limit,
        reverse: false,
    }
}

async fn set(kv: &Arc<dyn KvBackend>, key: &str, v: KvValue) {
    kv.set(key.as_bytes(), v, KvSetOptions::default())
        .await
        .expect("kv set");
}

#[tokio::test]
async fn ac_sto_01_list_2500_keys_in_three_lexicographic_pages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    // Insert in a scrambled order (7919 is coprime with 2500) so sorted output comes from the store.
    let n = 2500u64;
    for i in 0..n {
        let j = (i * 7919) % n;
        set(&kv, &format!("a/{j:05}"), json(&format!("{j}"))).await;
    }
    // Decoys that must never appear under the prefix `a/`.
    for decoy in ["a", "a0", "b/00001", "A/00001", "aa/00001"] {
        set(&kv, decoy, json("\"decoy\"")).await;
    }

    let mut cursor: Option<Bytes> = None;
    let mut sizes = Vec::new();
    let mut keys: Vec<Bytes> = Vec::new();
    loop {
        let page = kv.list(opts("a/", cursor.clone(), 1000)).await.expect("list page");
        sizes.push(page.entries.len());
        for (k, v) in &page.entries {
            let expected = std::str::from_utf8(k)
                .expect("utf8 key")
                .trim_start_matches("a/")
                .trim_start_matches('0')
                .to_string();
            let expected = if expected.is_empty() { "0".to_string() } else { expected };
            assert_eq!(v, &json(&expected), "value of {k:?}");
        }
        keys.extend(page.entries.into_iter().map(|(k, _)| k));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(sizes.len() <= 5, "pagination did not terminate");
    }

    assert_eq!(sizes, vec![1000, 1000, 500], "3 pages");
    assert_eq!(keys.len(), 2500);
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "keys must be unique and in strictly increasing lexicographic order"
    );
    assert_eq!(keys.first().expect("first"), &Bytes::from_static(b"a/00000"));
    assert_eq!(keys.last().expect("last"), &Bytes::from_static(b"a/02499"));
}

#[tokio::test]
async fn ac_sto_01_list_without_prefix_returns_everything_in_byte_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    for k in ["b", "a/2", "a/1", "A", "\u{00e9}"] {
        set(&kv, k, json("1")).await;
    }
    let page = kv.list(opts("", None, 100)).await.expect("list");
    let keys: Vec<Vec<u8>> = page.entries.iter().map(|(k, _)| k.to_vec()).collect();
    let mut expected: Vec<Vec<u8>> = ["b", "a/2", "a/1", "A", "\u{00e9}"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    expected.sort();
    assert_eq!(keys, expected, "plain byte-wise ordering");
    assert_eq!(page.next_cursor, None);
}

#[tokio::test]
async fn ac_sto_01_list_limit_is_capped_at_one_thousand() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    for i in 0..1100 {
        set(&kv, &format!("k/{i:04}"), json("0")).await;
    }
    match kv.list(opts("k/", None, 5000)).await {
        Ok(page) => {
            assert!(
                page.entries.len() <= 1000,
                "limit must be clamped to 1000, got {}",
                page.entries.len()
            );
            assert!(page.next_cursor.is_some(), "100 more keys remain");
        }
        Err(e) => assert!(
            matches!(e, StoreError::Invalid(_)),
            "an oversized limit may only be rejected as invalid, got {e:?}"
        ),
    }
}

#[tokio::test]
async fn ac_kv_01_get_set_delete_roundtrip_preserves_type_tags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    assert_eq!(kv.get(b"missing").await.expect("get"), None);

    set(&kv, "doc", json(r#"{"a":[1,2,3]}"#)).await;
    assert_eq!(kv.get(b"doc").await.expect("get"), Some(json(r#"{"a":[1,2,3]}"#)));

    let binary = [0u8, 255, 1, 0, 128, 7];
    set(&kv, "bin", raw(&binary)).await;
    let got = kv.get(b"bin").await.expect("get").expect("exists");
    assert_eq!(got.tag, ValueTag::Bytes);
    assert_eq!(got.data.as_ref(), &binary);

    // Overwrite changes value and tag.
    set(&kv, "doc", raw(b"now bytes")).await;
    assert_eq!(kv.get(b"doc").await.expect("get"), Some(raw(b"now bytes")));

    // Binary keys are fine too.
    kv.set(&[0, 1, 2, 255], json("1"), KvSetOptions::default())
        .await
        .expect("binary key");
    assert_eq!(kv.get(&[0, 1, 2, 255]).await.expect("get"), Some(json("1")));

    assert!(kv.delete(b"doc").await.expect("delete"));
    assert!(
        !kv.delete(b"doc").await.expect("delete twice"),
        "deleting an absent key reports false"
    );
    assert_eq!(kv.get(b"doc").await.expect("get"), None);
}

#[tokio::test]
async fn ac_kv_02_get_many_is_positional_with_nulls_for_missing_keys() {
    // contracts C-3: getMany(keys) -> (T | null)[]
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    set(&kv, "k1", json("1")).await;
    set(&kv, "k2", json("2")).await;

    let got = kv
        .get_many(&[
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"nope"),
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"k2"),
        ])
        .await
        .expect("get_many");
    assert_eq!(got, vec![Some(json("2")), None, Some(json("1")), Some(json("2"))]);
    assert!(kv.get_many(&[]).await.expect("empty").is_empty());
}

#[tokio::test]
async fn ac_kv_03_key_and_value_size_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    let max_key = vec![b'k'; 2 * 1024];
    kv.set(&max_key, json("1"), KvSetOptions::default())
        .await
        .expect("2 KB key is allowed");
    assert!(kv.get(&max_key).await.expect("get").is_some());
    let err = kv
        .set(&vec![b'k'; 2 * 1024 + 1], json("1"), KvSetOptions::default())
        .await
        .expect_err("key over 2 KB");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");
    assert_eq!(err.code(), "validation_failed");

    let max_value = KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::from(vec![9u8; 1024 * 1024]),
    };
    kv.set(b"big", max_value.clone(), KvSetOptions::default())
        .await
        .expect("1 MB value is allowed");
    assert_eq!(kv.get(b"big").await.expect("get"), Some(max_value));
    let too_big = KvValue {
        tag: ValueTag::Bytes,
        data: Bytes::from(vec![9u8; 1024 * 1024 + 1]),
    };
    let err = kv
        .set(b"bigger", too_big, KvSetOptions::default())
        .await
        .expect_err("value over 1 MB");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");
    assert_eq!(
        kv.get(b"bigger").await.expect("get"),
        None,
        "a rejected write stores nothing"
    );
}

#[tokio::test]
async fn ac_sto_02_expired_keys_are_hidden_on_read_and_removed_by_the_sweep() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    kv.set(
        b"short",
        json("1"),
        KvSetOptions {
            ttl: Some(Duration::from_secs(1)),
        },
    )
    .await
    .expect("set ttl 1s");
    kv.set(
        b"long",
        json("2"),
        KvSetOptions {
            ttl: Some(Duration::from_secs(3600)),
        },
    )
    .await
    .expect("set ttl 1h");
    set(&kv, "forever", json("3")).await;
    assert_eq!(
        kv.get(b"short").await.expect("get"),
        Some(json("1")),
        "readable before it expires"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Hidden on every read path, before any sweep ran.
    assert_eq!(kv.get(b"short").await.expect("get"), None);
    assert_eq!(
        kv.get_many(&[Bytes::from_static(b"short"), Bytes::from_static(b"long")])
            .await
            .expect("get_many"),
        vec![None, Some(json("2"))]
    );
    let listed: Vec<Bytes> = kv
        .list(opts("", None, 100))
        .await
        .expect("list")
        .entries
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        listed,
        vec![Bytes::from_static(b"forever"), Bytes::from_static(b"long")]
    );

    // The sweep deletes exactly the expired row, once.
    assert_eq!(kv.sweep_expired().await.expect("sweep"), 1);
    assert_eq!(kv.sweep_expired().await.expect("second sweep"), 0);
    assert_eq!(kv.get(b"long").await.expect("get"), Some(json("2")));
    assert_eq!(kv.get(b"forever").await.expect("get"), Some(json("3")));

    // The row is physically gone from data.db.
    let dbs = find_files(dir.path(), "data.db");
    assert_eq!(dbs.len(), 1, "one data.db for the one service, found {dbs:?}");
    let conn = rusqlite::Connection::open_with_flags(&dbs[0], rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .expect("open data.db");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM _rt_kv", [], |r| r.get(0))
        .expect("count rows");
    assert_eq!(rows, 2, "only `long` and `forever` remain after the sweep");
}

/// B3.7: the node's maintenance loop calls `sweep_open` every 60 s; it deletes the expired rows of
/// every open service database and leaves live rows alone.
#[tokio::test]
async fn ac_b37_sweep_open_deletes_expired_rows_of_every_open_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    for n in 1..=3u128 {
        let kv = store.kv(svc(n));
        for i in 0..n {
            kv.set(
                format!("tmp{i}").as_bytes(),
                json("x"),
                KvSetOptions {
                    ttl: Some(Duration::from_millis(200)),
                },
            )
            .await
            .expect("ttl");
        }
        set(&kv, "keep", json("y")).await;
    }
    assert_eq!(
        store.sweep_open().await.expect("early sweep"),
        0,
        "nothing has expired yet"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(store.sweep_open().await.expect("sweep"), 1 + 2 + 3);
    assert_eq!(store.sweep_open().await.expect("again"), 0);
    for n in 1..=3u128 {
        assert_eq!(store.kv(svc(n)).get(b"keep").await.expect("get"), Some(json("y")));
    }
}

#[tokio::test]
async fn ac_sto_02_overwriting_without_ttl_clears_the_expiry_and_ttl_can_be_added() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    kv.set(
        b"k",
        json("1"),
        KvSetOptions {
            ttl: Some(Duration::from_secs(1)),
        },
    )
    .await
    .expect("set ttl");
    set(&kv, "k", json("2")).await; // no ttl: persistent from now on
    kv.set(b"j", json("1"), KvSetOptions::default()).await.expect("set j");
    kv.set(
        b"j",
        json("2"),
        KvSetOptions {
            ttl: Some(Duration::from_secs(1)),
        },
    )
    .await
    .expect("add ttl later");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        kv.get(b"k").await.expect("get"),
        Some(json("2")),
        "an overwrite without ttl removes the old expiry"
    );
    assert_eq!(
        kv.get(b"j").await.expect("get"),
        None,
        "an overwrite with ttl sets the expiry"
    );
}

#[tokio::test]
async fn ac_sto_03_same_key_in_two_services_is_isolated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let (a, b) = (store.kv(svc(1)), store.kv(svc(2)));

    set(&a, "k", json("\"from a\"")).await;
    assert_eq!(b.get(b"k").await.expect("get b"), None, "B must not see A's key");
    assert!(b.list(opts("", None, 100)).await.expect("list b").entries.is_empty());
    assert_eq!(
        b.get_many(&[Bytes::from_static(b"k")]).await.expect("get_many b"),
        vec![None]
    );
    assert!(!b.delete(b"k").await.expect("delete in b"), "B cannot delete A's key");
    assert_eq!(a.get(b"k").await.expect("get a"), Some(json("\"from a\"")));

    // Independent values under the same key.
    set(&b, "k", json("\"from b\"")).await;
    assert_eq!(a.get(b"k").await.expect("get a"), Some(json("\"from a\"")));
    assert_eq!(b.get(b"k").await.expect("get b"), Some(json("\"from b\"")));
}

#[tokio::test]
async fn ac_kv_04_data_is_durable_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = data(dir.path());
        let kv = store.kv(svc(1));
        set(&kv, "persist", json("42")).await;
    }
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    assert_eq!(kv.get(b"persist").await.expect("get"), Some(json("42")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_kv_05_concurrent_writers_lose_no_updates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));

    let mut handles = Vec::new();
    for w in 0..16u32 {
        let kv = kv.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50u32 {
                kv.set(
                    format!("w{w:02}/{i:03}").as_bytes(),
                    KvValue {
                        tag: ValueTag::Json,
                        data: Bytes::from(format!("{}", w * 1000 + i)),
                    },
                    KvSetOptions::default(),
                )
                .await
                .expect("concurrent set");
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let page = kv.list(opts("w", None, 1000)).await.expect("list");
    assert_eq!(page.entries.len(), 800);
    assert_eq!(page.next_cursor, None);
    assert_eq!(kv.get(b"w15/049").await.expect("get"), Some(json("15049")));
}
