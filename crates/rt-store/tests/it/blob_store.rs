//! rt-store blob tests (STO-03 blob part, STO-08; contract C-4): streamed put/get,
//! head/list/delete, limits and isolation. The S3 backend (STO-09) is a stretch goal and not
//! covered here.

use crate::common;

use bytes::Bytes;
use common::*;
use futures_util::StreamExt;
use rt_store::{BlobListOptions, ServiceDataStore, StoreError};
use rt_types::{Body, BodyError};
use sha2::{Digest as _, Sha256};

const MAX: u64 = 100 * 1024 * 1024;

fn list_opts(prefix: &str, cursor: Option<String>, limit: usize) -> BlobListOptions {
    BlobListOptions {
        prefix: prefix.to_string(),
        cursor,
        limit,
    }
}

async fn read_all(obj: rt_store::BlobObject) -> Bytes {
    obj.body.collect(u64::MAX).await.expect("blob body streams")
}

#[tokio::test]
async fn ac_blob_01_put_get_head_roundtrip_with_content_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    let meta = blob
        .put(
            "docs/readme.txt",
            Body::from_bytes(Bytes::from_static(b"hello blob")),
            Some("text/plain".into()),
            MAX,
        )
        .await
        .expect("put");
    assert_eq!(meta.key, "docs/readme.txt");
    assert_eq!(meta.size, 10);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));

    let head = blob.head("docs/readme.txt").await.expect("head").expect("exists");
    assert_eq!(head, meta);

    let obj = blob.get("docs/readme.txt").await.expect("get").expect("exists");
    assert_eq!(obj.meta, meta);
    assert_eq!(obj.body.exact_len().unwrap_or(10), 10);
    assert_eq!(read_all(obj).await.as_ref(), b"hello blob");

    // No content type given: stays None. Empty object is valid.
    let empty = blob.put("empty", Body::empty(), None, MAX).await.expect("put empty");
    assert_eq!((empty.size, empty.content_type), (0, None));
    assert_eq!(
        read_all(blob.get("empty").await.expect("get").expect("exists"))
            .await
            .len(),
        0
    );
}

#[tokio::test]
async fn ac_blob_02_missing_keys_are_none_and_delete_reports_presence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    assert!(blob.get("nope").await.expect("get").is_none());
    assert!(blob.head("nope").await.expect("head").is_none());
    assert!(!blob.delete("nope").await.expect("delete missing"));

    blob.put("k", Body::from_bytes(Bytes::from_static(b"v")), None, MAX)
        .await
        .expect("put");
    assert!(blob.delete("k").await.expect("delete"));
    assert!(blob.get("k").await.expect("get").is_none());
    assert!(!blob.delete("k").await.expect("delete twice"));
}

#[tokio::test]
async fn ac_blob_03_overwrite_replaces_content_size_and_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    blob.put(
        "k",
        Body::from_bytes(Bytes::from(vec![1u8; 1000])),
        Some("a/b".into()),
        MAX,
    )
    .await
    .expect("put 1");
    let m2 = blob
        .put(
            "k",
            Body::from_bytes(Bytes::from_static(b"short")),
            Some("c/d".into()),
            MAX,
        )
        .await
        .expect("put 2");
    assert_eq!((m2.size, m2.content_type.as_deref()), (5, Some("c/d")));
    assert_eq!(blob.head("k").await.expect("head").expect("exists"), m2);
    assert_eq!(
        read_all(blob.get("k").await.expect("get").expect("exists"))
            .await
            .as_ref(),
        b"short"
    );
    assert_eq!(
        blob.total_bytes().await.expect("total"),
        5,
        "the old content no longer counts"
    );
}

#[tokio::test]
async fn ac_blob_04_list_filters_by_prefix_and_paginates_in_key_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    for i in (0..25).rev() {
        let body = vec![b'x'; i + 1];
        blob.put(
            &format!("img/{i:02}.png"),
            Body::from_bytes(Bytes::from(body)),
            Some("image/png".into()),
            MAX,
        )
        .await
        .expect("put img");
    }
    for i in 0..3 {
        blob.put(
            &format!("other/{i}"),
            Body::from_bytes(Bytes::from_static(b"o")),
            None,
            MAX,
        )
        .await
        .expect("put other");
    }

    let mut cursor = None;
    let mut sizes = Vec::new();
    let mut items = Vec::new();
    loop {
        let page = blob.list(list_opts("img/", cursor.clone(), 10)).await.expect("list");
        sizes.push(page.items.len());
        items.extend(page.items);
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(sizes.len() <= 5);
    }
    assert_eq!(sizes, vec![10, 10, 5]);
    let keys: Vec<&str> = items.iter().map(|m| m.key.as_str()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "keys come back in lexicographic order");
    assert_eq!(keys.len(), 25);
    assert_eq!(keys[0], "img/00.png");
    // C-4: list entries carry {key, size, contentType}.
    assert_eq!(items[0].size, 1);
    assert_eq!(items[24].size, 25);
    assert!(items.iter().all(|m| m.content_type.as_deref() == Some("image/png")));

    let all = blob.list(list_opts("", None, 100)).await.expect("list all");
    assert_eq!(all.items.len(), 28);
    assert_eq!(all.next_cursor, None);
}

#[tokio::test]
async fn ac_blob_05_total_bytes_tracks_puts_and_deletes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));
    assert_eq!(blob.total_bytes().await.expect("total"), 0);
    blob.put("a", Body::from_bytes(Bytes::from(vec![1u8; 1000])), None, MAX)
        .await
        .expect("put a");
    blob.put("b", Body::from_bytes(Bytes::from(vec![2u8; 2500])), None, MAX)
        .await
        .expect("put b");
    assert_eq!(blob.total_bytes().await.expect("total"), 3500);
    blob.delete("a").await.expect("delete a");
    assert_eq!(blob.total_bytes().await.expect("total"), 2500);
}

#[tokio::test]
async fn ac_blob_06_oversized_stream_fails_with_storage_quota_and_leaves_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));
    blob.put("existing", Body::from_bytes(Bytes::from_static(b"old")), None, MAX)
        .await
        .expect("put existing");

    let chunks = (0..4).map(|_| Ok::<Bytes, BodyError>(Bytes::from(vec![0u8; 1024])));
    let err = blob
        .put(
            "too-big",
            Body::from_stream(futures_util::stream::iter(chunks)),
            None,
            2048,
        )
        .await
        .expect_err("4 KB into a 2 KB cap");
    assert!(matches!(err, StoreError::StorageQuota), "got {err:?}");
    assert_eq!(err.code(), "storage_quota");
    assert!(
        blob.head("too-big").await.expect("head").is_none(),
        "no partial object may remain"
    );

    // Overwriting an existing key with an oversized stream keeps the old value.
    let chunks = (0..4).map(|_| Ok::<Bytes, BodyError>(Bytes::from(vec![0u8; 1024])));
    blob.put(
        "existing",
        Body::from_stream(futures_util::stream::iter(chunks)),
        None,
        2048,
    )
    .await
    .expect_err("oversized overwrite");
    assert_eq!(
        read_all(blob.get("existing").await.expect("get").expect("exists"))
            .await
            .as_ref(),
        b"old"
    );
    assert_eq!(blob.total_bytes().await.expect("total"), 3);
}

#[tokio::test]
async fn ac_blob_07_aborted_upload_stream_leaves_no_object() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    let chunks = vec![
        Ok(Bytes::from_static(b"part one")),
        Err(BodyError::Aborted("client went away".into())),
    ];
    let res = blob
        .put("half", Body::from_stream(futures_util::stream::iter(chunks)), None, MAX)
        .await;
    assert!(res.is_err(), "a failing body stream must fail the put");
    assert!(blob.head("half").await.expect("head").is_none());
    assert!(blob.get("half").await.expect("get").is_none());
    assert_eq!(blob.total_bytes().await.expect("total"), 0);
}

#[tokio::test]
async fn ac_blob_08_key_length_limit_and_path_traversal_are_contained() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    let max_key = "k".repeat(1024);
    blob.put(&max_key, Body::from_bytes(Bytes::from_static(b"x")), None, MAX)
        .await
        .expect("1 KB key is allowed");
    assert!(blob.head(&max_key).await.expect("head").is_some());
    let err = blob
        .put(&"k".repeat(1025), Body::from_bytes(Bytes::from_static(b"x")), None, MAX)
        .await
        .expect_err("key over 1 KB");
    assert!(matches!(err, StoreError::Invalid(_)), "got {err:?}");

    // Hostile keys must never write outside the service's blob area.
    for key in [
        "../escape",
        "../../escape",
        "a/../../escape",
        "/etc/escape",
        "..",
        "a\0b",
    ] {
        let _ = blob
            .put(key, Body::from_bytes(Bytes::from_static(b"pwned")), None, MAX)
            .await;
    }
    let outside = all_files(dir.path())
        .into_iter()
        .filter(|p| p.file_name().is_some_and(|n| n == "escape"))
        .collect::<Vec<_>>();
    assert!(outside.is_empty(), "traversal escaped the blob directory: {outside:?}");
    assert!(!dir.path().parent().expect("parent").join("escape").exists());
}

#[tokio::test]
async fn ac_sto_03_blob_namespaces_are_isolated_per_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let (a, b) = (store.blob(svc(1)), store.blob(svc(2)));

    a.put(
        "shared/key",
        Body::from_bytes(Bytes::from_static(b"a's data")),
        None,
        MAX,
    )
    .await
    .expect("put a");
    assert!(b.get("shared/key").await.expect("get b").is_none());
    assert!(b.head("shared/key").await.expect("head b").is_none());
    assert!(b.list(list_opts("", None, 100)).await.expect("list b").items.is_empty());
    assert!(!b.delete("shared/key").await.expect("delete in b"));
    assert_eq!(b.total_bytes().await.expect("total b"), 0);
    assert_eq!(
        read_all(a.get("shared/key").await.expect("get a").expect("exists"))
            .await
            .as_ref(),
        b"a's data"
    );
}

#[tokio::test]
async fn ac_sto_08_fifty_megabyte_blob_streams_both_ways_and_hash_matches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));

    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 800; // 50 MiB
    let mut expected = Sha256::new();
    for i in 0..CHUNKS {
        expected.update(vec![(i % 251) as u8; CHUNK]);
    }
    let expected = expected.finalize();

    let upload = (0..CHUNKS).map(|i| Ok::<Bytes, BodyError>(Bytes::from(vec![(i % 251) as u8; CHUNK])));
    let meta = blob
        .put(
            "big.bin",
            Body::from_stream(futures_util::stream::iter(upload)),
            Some("application/octet-stream".into()),
            MAX,
        )
        .await
        .expect("put 50 MiB");
    assert_eq!(meta.size, (CHUNK * CHUNKS) as u64);
    assert_eq!(blob.total_bytes().await.expect("total"), (CHUNK * CHUNKS) as u64);

    let obj = blob.get("big.bin").await.expect("get").expect("exists");
    assert_eq!(obj.meta.size, (CHUNK * CHUNKS) as u64);
    let mut stream = obj.body;
    let (mut hasher, mut chunks, mut max_chunk, mut total) = (Sha256::new(), 0usize, 0usize, 0usize);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        hasher.update(&chunk);
        chunks += 1;
        max_chunk = max_chunk.max(chunk.len());
        total += chunk.len();
    }
    assert_eq!(total, CHUNK * CHUNKS);
    assert_eq!(
        hasher.finalize(),
        expected,
        "content hash must match after the round trip"
    );
    assert!(
        chunks >= 12,
        "the body must be streamed in chunks, not buffered whole (got {chunks} chunk(s))"
    );
    assert!(
        max_chunk <= 4 * 1024 * 1024,
        "chunk of {max_chunk} bytes is too large for a streamed read"
    );
}

/// BLOB-005: a `get` racing overwrites of the same key returns the old or the new object, never
/// "absent" (the key exists the whole time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_005_get_racing_an_overwrite_never_reads_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let blob = store.blob(svc(1));
    blob.put(
        "k",
        rt_types::Body::from_bytes(bytes::Bytes::from_static(b"v0")),
        None,
        1 << 20,
    )
    .await
    .expect("put");
    let writer = {
        let blob = store.blob(svc(1));
        tokio::spawn(async move {
            for i in 0..300u32 {
                let body = rt_types::Body::from_bytes(bytes::Bytes::from(format!("v{i}")));
                blob.put("k", body, None, 1 << 20).await.expect("overwrite");
            }
        })
    };
    let mut reads = 0;
    while !writer.is_finished() {
        let got = blob.get("k").await.expect("get");
        assert!(got.is_some(), "the key exists the whole time (read {reads})");
        reads += 1;
    }
    writer.await.expect("writer");
    assert!(reads > 0);
}
