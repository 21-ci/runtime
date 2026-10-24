//! rt-store artifact store tests: content-addressed, immutable,
//! deduplicated, verified on read.

use crate::common;

use std::os::unix::fs::PermissionsExt;

use bytes::Bytes;
use common::*;
use rt_store::{ArtifactStore, LocalArtifactStore, StoreError};
use rt_types::Digest;

fn store() -> (tempfile::TempDir, std::sync::Arc<LocalArtifactStore>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = LocalArtifactStore::open(&dir.path().join("artifacts")).expect("open artifact store");
    (dir, s)
}

#[tokio::test]
async fn ac_artifact_01_put_returns_content_digest_and_get_returns_same_bytes() {
    let (_dir, s) = store();
    let content = Bytes::from_static(b"export default () => new Response('hi');");

    let d = s.put(content.clone()).await.expect("put");
    assert_eq!(d, Digest::of(&content), "the artifact key is the sha256 of the content");
    assert_eq!(s.get(&d).await.expect("get"), content);
    assert_eq!(s.size(&d).await.expect("size"), Some(content.len() as u64));
}

#[tokio::test]
async fn an_uploads_content_type_is_recorded_and_read_back_for_the_build() {
    let (_dir, s) = store();
    let js = Bytes::from_static(b"export default () => new Response('js');");
    let d = s
        .put_typed(js.clone(), "application/javascript")
        .await
        .expect("put typed");
    assert_eq!(d, Digest::of(&js));
    assert_eq!(
        s.content_type(&d).await.expect("ct").as_deref(),
        Some("application/javascript")
    );
    // The first upload of the bytes decides, except that TypeScript (the superset the bytes were
    // validated as) replaces JavaScript; other types are never rewritten.
    s.put_typed(js.clone(), "application/zip").await.expect("put again");
    assert_eq!(
        s.content_type(&d).await.expect("ct").as_deref(),
        Some("application/javascript")
    );
    s.put_typed(js.clone(), "application/typescript")
        .await
        .expect("put as ts");
    assert_eq!(
        s.content_type(&d).await.expect("ct").as_deref(),
        Some("application/typescript")
    );
    // Untyped puts (dependencies, locks) and unknown digests have none.
    let lock = s
        .put(Bytes::from_static(b"{\"version\":1}"))
        .await
        .expect("put untyped");
    assert_eq!(s.content_type(&lock).await.expect("ct"), None);
    assert_eq!(s.content_type(&Digest::of(b"absent")).await.expect("ct"), None);
}

#[tokio::test]
async fn ac_artifact_02_identical_content_is_stored_once() {
    let (dir, s) = store();
    let content = Bytes::from(noise(4096, 1));

    let d1 = s.put(content.clone()).await.expect("put 1");
    let d2 = s.put(content.clone()).await.expect("put 2 is idempotent");
    assert_eq!(d1, d2);

    let blobs = find_files(&dir.path().join("artifacts"), "blob");
    assert_eq!(blobs.len(), 1, "dedup: one stored copy, found {blobs:?}");
    assert_eq!(blobs[0], s.blob_path(&d1));
    assert_eq!(std::fs::read(s.blob_path(&d1)).expect("read blob"), content.to_vec());

    // A different artifact gets its own directory.
    let other = s.put(Bytes::from(noise(4096, 2))).await.expect("put other");
    assert_ne!(other, d1);
    assert_eq!(find_files(&dir.path().join("artifacts"), "blob").len(), 2);
}

#[tokio::test]
async fn ac_artifact_03_layout_is_sha256_dir_with_blob_and_meta_json() {
    let (dir, s) = store();
    let d = s.put(Bytes::from_static(b"layout")).await.expect("put");

    let entry_dir = dir.path().join("artifacts").join(d.hex());
    assert!(entry_dir.is_dir(), "artifacts/<sha256>/ must exist");
    assert_eq!(s.blob_path(&d), entry_dir.join("blob"));
    assert!(entry_dir.join("blob").is_file());
    assert!(entry_dir.join("meta.json").is_file());
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).expect("read meta.json"))
            .expect("meta.json is JSON");
    assert!(meta.is_object());
}

#[tokio::test]
async fn ac_artifact_04_exists_and_size_reflect_presence() {
    let (_dir, s) = store();
    let missing = Digest::of(b"never stored");
    assert!(!s.exists(&missing).await.expect("exists"));
    assert_eq!(s.size(&missing).await.expect("size"), None);

    let d = s.put(Bytes::from_static(b"present")).await.expect("put");
    assert!(s.exists(&d).await.expect("exists"));
    assert_eq!(s.size(&d).await.expect("size"), Some(7));
}

#[tokio::test]
async fn ac_artifact_05_get_of_unknown_digest_is_not_found() {
    let (_dir, s) = store();
    let err = s.get(&Digest::of(b"absent")).await.expect_err("unknown digest");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    assert_eq!(err.code(), "not_found");
}

#[tokio::test]
async fn ac_artifact_06_delete_removes_only_that_artifact() {
    let (_dir, s) = store();
    let keep = s.put(Bytes::from_static(b"keep")).await.expect("put keep");
    let gone = s.put(Bytes::from_static(b"drop")).await.expect("put drop");

    s.delete(&gone).await.expect("delete");
    assert!(!s.exists(&gone).await.expect("exists"));
    assert!(matches!(s.get(&gone).await, Err(StoreError::NotFound)));
    assert!(!s.blob_path(&gone).exists(), "the blob file must be gone");
    assert!(s.exists(&keep).await.expect("exists"));
    assert_eq!(s.get(&keep).await.expect("get").as_ref(), b"keep");

    // Re-uploading after deletion works.
    assert_eq!(s.put(Bytes::from_static(b"drop")).await.expect("re-put"), gone);
    assert_eq!(s.get(&gone).await.expect("get").as_ref(), b"drop");
}

#[tokio::test]
async fn ac_artifact_07_empty_and_multi_megabyte_artifacts_roundtrip() {
    let (_dir, s) = store();
    let empty = s.put(Bytes::new()).await.expect("put empty");
    assert_eq!(empty, Digest::of(b""));
    assert_eq!(s.get(&empty).await.expect("get empty").len(), 0);
    assert_eq!(s.size(&empty).await.expect("size"), Some(0));

    let big = Bytes::from(noise(8 * 1024 * 1024, 77));
    let d = s.put(big.clone()).await.expect("put big");
    assert_eq!(d, Digest::of(&big));
    assert_eq!(s.size(&d).await.expect("size"), Some(8 * 1024 * 1024));
    assert_eq!(s.get(&d).await.expect("get big"), big);
}

#[tokio::test]
async fn ac_artifact_08_corrupted_blob_is_detected_on_get() {
    let (_dir, s) = store();
    let content = Bytes::from(noise(1024, 3));
    let d = s.put(content.clone()).await.expect("put");

    // Flip one byte on disk (same length, so only the digest check can notice).
    let mut on_disk = std::fs::read(s.blob_path(&d)).expect("read blob");
    on_disk[100] ^= 0xFF;
    std::fs::write(s.blob_path(&d), &on_disk).expect("corrupt blob");

    let err = s.get(&d).await.expect_err("corruption must not be served");
    assert!(matches!(err, StoreError::Corrupt(_)), "got {err:?}");
    assert_eq!(
        err.code(),
        "internal",
        "corruption is an internal error code, never exposing paths"
    );
    assert!(
        !err.to_string().contains(&*dir_str(&s)),
        "the error must not leak host paths"
    );
}

fn dir_str(s: &LocalArtifactStore) -> String {
    s.blob_path(&Digest::of(b"x"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

#[tokio::test]
async fn ac_artifact_09_files_are_private_and_writes_leave_no_temp_files() {
    let (dir, s) = store();
    let d = s.put(Bytes::from_static(b"private")).await.expect("put");

    let mode = std::fs::metadata(s.blob_path(&d)).expect("stat").permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "artifact files are mode 0600");

    for f in all_files(&dir.path().join("artifacts")) {
        let name = f.file_name().expect("name").to_string_lossy().to_string();
        assert!(
            name == "blob" || name == "meta.json",
            "unexpected leftover file {f:?} (atomic writes must clean up)"
        );
    }
}

#[tokio::test]
async fn ac_artifact_10_concurrent_puts_dedup_and_all_readable() {
    let (dir, s) = store();
    let shared = Bytes::from(noise(64 * 1024, 11));

    let mut handles = Vec::new();
    for i in 0..32u64 {
        let s = s.clone();
        let shared = shared.clone();
        handles.push(tokio::spawn(async move {
            let a = s.put(shared).await.expect("put shared");
            let own = Bytes::from(noise(2048, 100 + i));
            let b = s.put(own.clone()).await.expect("put own");
            (a, b, own)
        }));
    }
    let mut shared_digests = std::collections::HashSet::new();
    for h in handles {
        let (a, b, own) = h.await.expect("join");
        shared_digests.insert(a);
        assert_eq!(b, Digest::of(&own));
        assert_eq!(s.get(&b).await.expect("get own"), own);
    }
    assert_eq!(shared_digests.len(), 1);
    assert_eq!(
        s.get(shared_digests.iter().next().expect("one"))
            .await
            .expect("get shared"),
        shared
    );
    assert_eq!(
        find_files(&dir.path().join("artifacts"), "blob").len(),
        33,
        "1 shared + 32 distinct"
    );
}

#[tokio::test]
async fn ac_artifact_11_artifacts_survive_reopening_the_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("artifacts");
    let d = {
        let s = LocalArtifactStore::open(&root).expect("open");
        s.put(Bytes::from_static(b"durable")).await.expect("put")
    };
    let s = LocalArtifactStore::open(&root).expect("reopen");
    assert!(s.exists(&d).await.expect("exists"));
    assert_eq!(s.get(&d).await.expect("get").as_ref(), b"durable");
}

/// FETCH-003: a vendored module remembers the URLs it was fetched from; an upload of the same
/// store does not count as vendored from any URL.
#[tokio::test]
async fn fetch_003_vendored_modules_remember_their_urls() {
    let (_dir, s) = store();
    let upload = s
        .put_typed(Bytes::from_static(b"export const x = 1;"), "application/typescript")
        .await
        .expect("upload");
    assert!(
        !s.vendored_from(&upload, "https://esm.sh/x").await.expect("read"),
        "an upload is not vendored"
    );
    let m = s
        .put_vendored(Bytes::from_static(b"export const m = 1;"), "https://esm.sh/m@1")
        .await
        .expect("vendor");
    assert!(s.vendored_from(&m, "https://esm.sh/m@1").await.expect("read"));
    assert!(!s.vendored_from(&m, "https://esm.sh/other").await.expect("read"));
    s.put_vendored(Bytes::from_static(b"export const m = 1;"), "https://cdn.jsdelivr.net/m")
        .await
        .expect("again");
    assert!(
        s.vendored_from(&m, "https://esm.sh/m@1").await.expect("read"),
        "both URLs are kept"
    );
    assert!(s.vendored_from(&m, "https://cdn.jsdelivr.net/m").await.expect("read"));
    assert_eq!(
        s.get(&m).await.expect("get"),
        Bytes::from_static(b"export const m = 1;")
    );
    assert!(!s
        .vendored_from(&Digest::of(b"absent"), "https://esm.sh/m@1")
        .await
        .expect("read"));
}

/// ART-004: re-uploading the exact bytes of an artifact whose file was corrupted in place (same
/// length) repairs it, instead of only touching the damaged file.
#[tokio::test]
async fn art_004_a_put_of_the_same_bytes_repairs_a_corrupted_artifact() {
    let (_dir, s) = store();
    let content = Bytes::from(noise(4096, 7));
    let d = s.put(content.clone()).await.expect("put");
    let mut on_disk = std::fs::read(s.blob_path(&d)).expect("read blob");
    on_disk[10] ^= 0xFF;
    std::fs::write(s.blob_path(&d), &on_disk).expect("corrupt blob");
    assert!(matches!(s.get(&d).await, Err(StoreError::Corrupt(_))));
    assert_eq!(s.put(content.clone()).await.expect("re-upload"), d);
    assert_eq!(s.get(&d).await.expect("repaired"), content);
}
