//! Store-level edge cases of KV, durable storage, blobs and guest SQL (scratchpad
//! edge-cases.md §3): key bounds, TTL boundaries, cursors and limits, exact size caps.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::{
    DurableListOptions, DurableValue, DurableWrite, KvBackend, KvListOptions, KvSetOptions, KvValue, ServiceDataStore,
    SqlValue, StoreError, ValueTag,
};
use rt_types::Body;

fn json(s: &str) -> KvValue {
    KvValue {
        tag: ValueTag::Json,
        data: Bytes::from(s.to_string()),
    }
}

fn list(prefix: &str, cursor: Option<&str>, limit: usize, reverse: bool) -> KvListOptions {
    KvListOptions {
        prefix: Bytes::from(prefix.to_string()),
        cursor: cursor.map(|c| Bytes::from(c.to_string())),
        limit,
        reverse,
    }
}

fn keys(p: &rt_store::KvPage) -> Vec<String> {
    p.entries
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect()
}

async fn put_all(kv: &Arc<dyn KvBackend>, ks: &[&str]) {
    for k in ks {
        kv.set(k.as_bytes(), json("1"), KvSetOptions::default())
            .await
            .expect("set");
    }
}

/// KV-003: the store refuses an empty key on every write (defence in depth under the facade).
#[tokio::test]
async fn kv_003_the_store_refuses_empty_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kv = data(dir.path()).kv(svc(1));
    let invalid =
        |r: Result<(), StoreError>, op: &str| assert!(matches!(r, Err(StoreError::Invalid(_))), "{op}: {r:?}");
    invalid(kv.set(b"", json("1"), KvSetOptions::default()).await, "set");
    invalid(kv.incr(b"", 1, KvSetOptions::default()).await.map(|_| ()), "incr");
    invalid(
        kv.cas(b"", None, Some(json("1")), KvSetOptions::default())
            .await
            .map(|_| ()),
        "cas",
    );
    invalid(kv.expire(b"", Some(Duration::from_secs(1))).await.map(|_| ()), "expire");
    assert_eq!(
        kv.get(b"").await.expect("get"),
        None,
        "nothing was stored under the empty key"
    );
}

/// KV-005 / KV-019: a TTL of 0 writes a key that is already gone; deleting an expired key that the
/// sweeper has not removed yet reports `false`.
#[tokio::test]
async fn kv_005_019_zero_ttl_and_deleting_an_expired_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kv = data(dir.path()).kv(svc(1));
    kv.set(
        b"z",
        json("1"),
        KvSetOptions {
            ttl: Some(Duration::ZERO),
        },
    )
    .await
    .expect("set");
    assert_eq!(kv.get(b"z").await.expect("get"), None, "ttl 0 is expired at once");
    assert_eq!(kv.ttl(b"z").await.expect("ttl"), None);
    kv.set(
        b"e",
        json("1"),
        KvSetOptions {
            ttl: Some(Duration::from_millis(30)),
        },
    )
    .await
    .expect("set");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!kv.delete(b"e").await.expect("delete"), "an expired key is absent");
}

/// KV-006: a huge TTL saturates: the key lives and `ttl()` is finite.
#[tokio::test]
async fn kv_006_a_huge_ttl_saturates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kv = data(dir.path()).kv(svc(1));
    kv.set(
        b"h",
        json("1"),
        KvSetOptions {
            ttl: Some(Duration::from_secs(u64::MAX)),
        },
    )
    .await
    .expect("set");
    assert_eq!(kv.get(b"h").await.expect("get"), Some(json("1")));
    let left = kv.ttl(b"h").await.expect("ttl").expect("has an expiry");
    assert!(left > Duration::from_secs(100 * 365 * 86_400), "{left:?}");
    assert!(kv
        .expire(b"h", Some(Duration::from_secs(u64::MAX)))
        .await
        .expect("expire"));
    assert_eq!(
        kv.incr(
            b"n",
            1,
            KvSetOptions {
                ttl: Some(Duration::from_secs(u64::MAX))
            }
        )
        .await
        .expect("incr"),
        1
    );
}

/// KV-010 / KV-011 / KV-012: cursors never leak keys outside the prefix in either direction; a
/// limit of 0 is 1; keys deleted or added behind the cursor are neither repeated nor returned.
#[tokio::test]
async fn kv_010_011_012_cursors_and_limits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kv = data(dir.path()).kv(svc(1));
    put_all(&kv, &["a1", "b1", "b2", "b3", "c1"]).await;
    let p = kv.list(list("b", Some("a"), 10, false)).await.expect("list");
    assert_eq!(
        keys(&p),
        ["b1", "b2", "b3"],
        "a cursor below the prefix starts at the prefix"
    );
    let p = kv.list(list("b", Some("z"), 10, true)).await.expect("list");
    assert_eq!(
        keys(&p),
        ["b3", "b2", "b1"],
        "a reverse cursor above the range starts at its end"
    );
    let p = kv.list(list("b", Some("c0"), 10, false)).await.expect("list");
    assert!(
        keys(&p).is_empty(),
        "a cursor past the prefix yields nothing, never c1: {:?}",
        keys(&p)
    );
    let p = kv.list(list("b", Some("a9"), 10, true)).await.expect("list");
    assert!(
        keys(&p).is_empty(),
        "reverse below the prefix yields nothing, never a1: {:?}",
        keys(&p)
    );

    let p = kv.list(list("b", None, 0, false)).await.expect("list");
    assert_eq!(keys(&p), ["b1"], "limit 0 is 1");

    let first = kv.list(list("b", None, 2, false)).await.expect("list");
    assert_eq!(keys(&first), ["b1", "b2"]);
    kv.delete(b"b1").await.expect("delete");
    kv.set(b"b0", json("1"), KvSetOptions::default())
        .await
        .expect("set behind the cursor");
    let rest = kv
        .list(KvListOptions {
            cursor: first.next_cursor.clone(),
            ..list("b", None, 10, false)
        })
        .await
        .expect("list");
    assert_eq!(keys(&rest), ["b3"], "no repeats, nothing from behind the cursor");
}

/// KV-015: `incr` by 0 on a missing key creates the counter 0 with the TTL.
#[tokio::test]
async fn kv_015_incr_by_zero_creates_the_counter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let kv = data(dir.path()).kv(svc(1));
    assert_eq!(
        kv.incr(
            b"c",
            0,
            KvSetOptions {
                ttl: Some(Duration::from_secs(60))
            }
        )
        .await
        .expect("incr"),
        0
    );
    assert_eq!(kv.get(b"c").await.expect("get"), Some(json("0")));
    assert!(kv
        .ttl(b"c")
        .await
        .expect("ttl")
        .is_some_and(|t| t > Duration::from_secs(55)));
}

/// DUR-002 / DUR-004 / DUR-005: durable keys are 1 to 2048 bytes; object prefixes match literally
/// (`%` and `_` are no wildcards); list bounds outside the prefix are clipped to it.
#[tokio::test]
async fn dur_002_004_005_durable_keys_object_prefixes_and_ranges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = data(dir.path()).durable(svc(1));
    let v = DurableValue {
        format: 1,
        data: Bytes::from_static(b"x"),
    };
    let write = |k: &[u8]| DurableWrite {
        writes: vec![(Bytes::copy_from_slice(k), Some(v.clone()))],
        ..Default::default()
    };
    for bad in [Vec::new(), vec![b'k'; 2049]] {
        let r = d.commit("o", write(&bad)).await;
        assert!(
            matches!(r, Err(StoreError::Invalid(_))),
            "key of {} bytes: {r:?}",
            bad.len()
        );
    }
    d.commit("o", write(&[b'k'; 2048])).await.expect("2048 bytes is fine");

    for name in ["a%b", "a_b", "axb", "ä_ö"] {
        d.commit(name, write(b"k")).await.expect("commit");
    }
    let (objs, _) = d.objects("a%", None, 10).await.expect("objects");
    assert_eq!(objs, ["a%b"], "`%` is literal");
    let (objs, _) = d.objects("a_", None, 10).await.expect("objects");
    assert_eq!(objs, ["a_b"], "`_` is literal");
    let (objs, _) = d.objects("ä", None, 10).await.expect("objects");
    assert_eq!(objs, ["ä_ö"]);

    for k in ["p1", "p2", "p3", "q1", "o9"] {
        d.commit("r", write(k.as_bytes())).await.expect("commit");
    }
    let ks = |v: Vec<(Bytes, DurableValue)>| {
        v.into_iter()
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect::<Vec<_>>()
    };
    let opts = |start: Option<&str>, end: Option<&str>, reverse: bool, limit: usize| DurableListOptions {
        prefix: Bytes::from_static(b"p"),
        start: start.map(|s| Bytes::from(s.to_string())),
        end: end.map(|s| Bytes::from(s.to_string())),
        limit,
        reverse,
    };
    assert_eq!(
        ks(d.list("r", opts(Some("a"), Some("z"), false, 10)).await.expect("list")),
        ["p1", "p2", "p3"]
    );
    assert_eq!(
        ks(d.list("r", opts(Some("a"), Some("z"), true, 10)).await.expect("list")),
        ["p3", "p2", "p1"]
    );
    assert_eq!(
        ks(d.list("r", opts(None, None, false, 0)).await.expect("list")).len(),
        1,
        "limit 0 is 1"
    );
}

/// BLOB-004: a body of exactly `max_bytes` is stored; one byte more is refused.
#[tokio::test]
async fn blob_004_the_size_cap_is_exact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let b = data(dir.path()).blob(svc(1));
    b.put("k", Body::from_bytes(Bytes::from(vec![7u8; 4096])), None, 4096)
        .await
        .expect("exactly max_bytes");
    assert_eq!(b.head("k").await.expect("head").expect("stored").size, 4096);
    let r = b
        .put("k2", Body::from_bytes(Bytes::from(vec![7u8; 4097])), None, 4096)
        .await;
    assert!(r.is_err(), "one byte over: {r:?}");
    assert!(b.head("k2").await.expect("head").is_none());
}

/// SQL-006 / SQL-007 / SQL-010: a string that merely mentions a denied function is data, not a
/// call; harmless pragmas work; exactly 10 000 rows pass and one more is refused.
#[tokio::test]
async fn sql_006_007_010_literals_pragmas_and_the_row_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sql = data(dir.path()).sql(svc(1));
    sql.execute(st("CREATE TABLE t (v TEXT)")).await.expect("create");
    sql.execute(st_args(
        "INSERT INTO t VALUES (?)",
        vec![SqlValue::Text("load_extension".into())],
    ))
    .await
    .expect("bound value");
    let r = sql
        .execute(st("INSERT INTO t VALUES ('call load_extension later')"))
        .await;
    assert!(r.is_ok(), "a literal mentioning load_extension is data: {r:?}");
    let r = sql.execute(st("SELECT load_extension('x')")).await;
    assert!(r.is_err(), "the function itself stays denied");

    for p in [
        "PRAGMA foreign_keys = ON",
        "PRAGMA recursive_triggers = ON",
        "PRAGMA user_version = 7",
        "PRAGMA integrity_check",
    ] {
        sql.execute(st(p)).await.unwrap_or_else(|e| panic!("{p}: {e:?}"));
    }
    let v = sql.execute(st("PRAGMA user_version")).await.expect("read");
    assert_eq!(v.rows, vec![vec![SqlValue::Integer(7)]]);

    sql.execute(st("CREATE TABLE n (i INTEGER)")).await.expect("create");
    sql.execute(st(
        "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 10001) INSERT INTO n SELECT i FROM c",
    ))
    .await
    .expect("fill");
    let ok = sql
        .execute(st("SELECT i FROM n WHERE i <= 10000"))
        .await
        .expect("exactly 10 000 rows");
    assert_eq!(ok.rows.len(), 10_000);
    let over = sql.execute(st("SELECT i FROM n")).await;
    assert!(
        matches!(over, Err(StoreError::SqlResultTooLarge)),
        "{:?}",
        over.as_ref().map(|r| r.rows.len())
    );
}

/// PERS-003: a corrupted `data.db` fails that service's operations with an error (never a
/// panic) and leaves the other services working.
#[tokio::test]
async fn pers_003_a_corrupt_database_fails_only_its_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("services").join(svc(1).to_string());
    std::fs::create_dir_all(&bad).expect("dir");
    std::fs::write(
        bad.join("data.db"),
        b"this is not a sqlite database, just garbage bytes ...........",
    )
    .expect("write");
    let store = data(dir.path());
    let r = store.kv(svc(1)).set(b"k", json("1"), KvSetOptions::default()).await;
    assert!(r.is_err(), "a corrupt database refuses writes: {r:?}");
    let r = store.kv(svc(1)).get(b"k").await;
    assert!(r.is_err() || r.as_ref().is_ok_and(Option::is_none), "{r:?}");
    store
        .kv(svc(2))
        .set(b"k", json("2"), KvSetOptions::default())
        .await
        .expect("another service works");
    assert_eq!(store.kv(svc(2)).get(b"k").await.expect("get"), Some(json("2")));
}
