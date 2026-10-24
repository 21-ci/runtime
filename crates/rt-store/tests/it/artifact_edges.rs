//! Artifact store edge paths: the trait's default methods, filesystem states the store must
//! survive (a directory where a file belongs, a read-only subtree), and GC when a reference check
//! or a deletion fails.

use crate::common;

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::*;
use rt_store::artifact::delete_unreferenced;
use rt_store::{ArtifactStore, Fault, Faults, LocalArtifactStore, MetaStore, StoreError};
use rt_types::Digest;

/// An artifact store that implements only the required methods, so every provided default runs.
struct Minimal(std::sync::Arc<LocalArtifactStore>);

#[async_trait]
impl ArtifactStore for Minimal {
    async fn put(&self, bytes: Bytes) -> rt_store::Result<Digest> {
        self.0.put(bytes).await
    }
    async fn get(&self, d: &Digest) -> rt_store::Result<Bytes> {
        self.0.get(d).await
    }
    async fn exists(&self, d: &Digest) -> rt_store::Result<bool> {
        self.0.exists(d).await
    }
    async fn delete(&self, d: &Digest) -> rt_store::Result<()> {
        self.0.delete(d).await
    }
    async fn size(&self, d: &Digest) -> rt_store::Result<Option<u64>> {
        self.0.size(d).await
    }
}

/// Forwards everything except `delete_if_idle`, which always fails.
struct FailingDelete(std::sync::Arc<LocalArtifactStore>);

#[async_trait]
impl ArtifactStore for FailingDelete {
    async fn put(&self, bytes: Bytes) -> rt_store::Result<Digest> {
        self.0.put(bytes).await
    }
    async fn get(&self, d: &Digest) -> rt_store::Result<Bytes> {
        self.0.get(d).await
    }
    async fn exists(&self, d: &Digest) -> rt_store::Result<bool> {
        self.0.exists(d).await
    }
    async fn delete(&self, d: &Digest) -> rt_store::Result<()> {
        self.0.delete(d).await
    }
    async fn size(&self, d: &Digest) -> rt_store::Result<Option<u64>> {
        self.0.size(d).await
    }
    async fn delete_if_idle(&self, _d: &Digest, _idle_for: Duration) -> rt_store::Result<bool> {
        Err(StoreError::Internal("injected delete failure".into()))
    }
}

fn open(dir: &std::path::Path) -> std::sync::Arc<LocalArtifactStore> {
    LocalArtifactStore::open(&dir.join("artifacts")).expect("open artifact store")
}

#[tokio::test]
async fn the_provided_defaults_fall_back_to_put_and_say_no_when_a_store_cannot_tell() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = Minimal(open(dir.path()));
    let content = Bytes::from_static(b"export default 1;");
    let d = Digest::of(&content);

    assert_eq!(
        s.put_typed(content.clone(), "application/typescript")
            .await
            .expect("typed"),
        d
    );
    assert_eq!(
        s.put_vendored(content.clone(), "https://deno.land/x/m.ts")
            .await
            .expect("vendored"),
        d
    );
    assert!(
        !s.vendored_from(&d, "https://deno.land/x/m.ts")
            .await
            .expect("vendored_from"),
        "a store that cannot tell says no"
    );
    assert_eq!(s.content_type(&d).await.expect("content_type"), None);
    s.touch(&d).await.expect("touch is a no-op by default");
    assert!(
        !s.delete_if_idle(&d, Duration::ZERO).await.expect("delete_if_idle"),
        "a store that cannot tell keeps everything"
    );
    assert!(s.idle(Duration::ZERO).await.expect("idle").is_empty());
    assert!(s.exists(&d).await.expect("the default put really stored it"));
}

#[tokio::test]
async fn a_vendored_url_already_recorded_is_not_recorded_twice() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let content = Bytes::from_static(b"vendored module");
    let d = s
        .put_vendored(content.clone(), "https://example.test/a.ts")
        .await
        .expect("first");
    assert_eq!(
        s.put_vendored(content, "https://example.test/a.ts")
            .await
            .expect("again"),
        d
    );
    assert!(s
        .vendored_from(&d, "https://example.test/a.ts")
        .await
        .expect("vendored_from"));
    assert!(!s
        .vendored_from(&d, "https://example.test/b.ts")
        .await
        .expect("other url"));
}

#[tokio::test]
async fn a_put_whose_blob_path_is_occupied_by_a_directory_fails_and_leaves_no_temp_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let content = Bytes::from_static(b"blocked");
    let d = Digest::of(&content);
    std::fs::create_dir_all(s.blob_path(&d)).expect("a directory where the blob goes");

    assert!(
        s.put(content).await.is_err(),
        "the blob cannot be renamed over a directory"
    );
    let stray: Vec<_> = all_files(&dir.path().join("artifacts"))
        .into_iter()
        .filter(|p| p.to_string_lossy().contains(".tmp-"))
        .collect();
    assert!(stray.is_empty(), "the temp file is removed: {stray:?}");
}

#[tokio::test]
async fn metadata_that_cannot_be_read_surfaces_as_an_error_not_as_absence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let content = Bytes::from_static(b"broken metadata");
    let d = s.put_vendored(content, "https://example.test/x.ts").await.expect("put");
    let meta = s.blob_path(&d).parent().expect("entry dir").join("meta.json");
    std::fs::remove_file(&meta).expect("drop meta.json");
    std::fs::create_dir(&meta).expect("a directory where meta.json belongs");

    assert!(s.vendored_from(&d, "https://example.test/x.ts").await.is_err());
    assert!(s.content_type(&d).await.is_err());
}

#[tokio::test]
async fn a_blob_that_is_a_directory_is_an_error_to_read_but_has_no_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let content = Bytes::from_static(b"not a file");
    let d = Digest::of(&content);
    std::fs::create_dir_all(s.blob_path(&d)).expect("a directory where the blob is");

    assert!(s.get(&d).await.is_err(), "reading a directory fails");
    assert_eq!(s.size(&d).await.expect("size"), None, "a directory is not an artifact");
    assert!(!s.exists(&d).await.expect("exists"));
}

#[tokio::test]
async fn an_entry_that_is_a_file_fails_size_touch_and_delete_but_a_missing_one_is_fine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let d = Digest::of(b"entry is a file");
    let entry = s.blob_path(&d).parent().expect("entry dir").to_path_buf();
    std::fs::write(&entry, b"oops").expect("a file where the entry directory goes");

    assert!(s.size(&d).await.is_err(), "a path below a file is not absent");
    assert!(s.touch(&d).await.is_err());
    assert!(s.delete(&d).await.is_err());
    assert!(s.delete_if_idle(&d, Duration::ZERO).await.is_err());
}

#[tokio::test]
async fn touch_and_delete_of_a_missing_artifact_are_no_ops() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let d = Digest::of(b"never stored");
    s.touch(&d).await.expect("touching nothing is fine");
    s.delete(&d).await.expect("deleting nothing is fine");
    assert!(!s.delete_if_idle(&d, Duration::ZERO).await.expect("nothing to delete"));
    assert_eq!(s.size(&d).await.expect("size"), None);
}

#[tokio::test]
async fn an_entry_directory_that_cannot_be_emptied_fails_the_idle_delete() {
    if running_as_root() {
        return; // root bypasses the permission check this relies on
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let content = Bytes::from_static(b"stuck entry");
    let d = s.put(content).await.expect("put");
    let entry = s.blob_path(&d).parent().expect("entry dir").to_path_buf();
    let locked = entry.join("locked");
    std::fs::create_dir(&locked).expect("subdirectory");
    std::fs::write(locked.join("inside"), b"x").expect("a file to keep");
    std::fs::set_permissions(&locked, std::os::unix::fs::PermissionsExt::from_mode(0o500)).expect("read-only");

    let res = s.delete_if_idle(&d, Duration::ZERO).await;
    std::fs::set_permissions(&locked, std::os::unix::fs::PermissionsExt::from_mode(0o700)).expect("restore");
    assert!(res.is_err(), "a subtree that cannot be removed is an error");
}

fn running_as_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[tokio::test]
async fn gc_keeps_an_artifact_whose_reference_check_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let m = meta();
    let d = s.put(Bytes::from_static(b"referenced check fails")).await.expect("put");
    let faults = Faults::new();
    m.inject_faults(faults.clone());
    faults.fail_next(1, Fault::Io);

    let deleted = delete_unreferenced(m.as_ref() as &dyn MetaStore, s.as_ref(), [d], Duration::ZERO).await;
    assert_eq!(deleted, 0, "an unknown reference state keeps the artifact");
    assert!(s.exists(&d).await.expect("exists"));
}

#[tokio::test]
async fn gc_counts_only_the_deletions_that_succeed_and_survives_a_failing_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = open(dir.path());
    let m = meta();
    let d = base.put(Bytes::from_static(b"gc target")).await.expect("put");
    let failing = FailingDelete(base.clone());

    let deleted = delete_unreferenced(m.as_ref() as &dyn MetaStore, &failing, [d], Duration::ZERO).await;
    assert_eq!(deleted, 0);
    assert!(base.exists(&d).await.expect("kept for the next sweep"));
}

#[tokio::test]
async fn gc_on_a_store_that_reports_a_missing_artifact_counts_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = open(dir.path());
    let m = meta();
    let missing = Digest::of(b"absent");
    assert_eq!(
        delete_unreferenced(m.as_ref() as &dyn MetaStore, s.as_ref(), [missing], Duration::ZERO).await,
        0
    );
}
