//! Artifact deletion is safe against races (review findings 6 and 7): a digest version GC freed is
//! re-checked right before it is deleted, anything written or touched within the grace period is
//! kept (a concurrent re-upload, a build in flight), and an orphan sweep frees what failed builds
//! vendored but never recorded.

use crate::common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use rt_store::artifact::{delete_unreferenced, sweep_orphans};
use rt_store::{ArtifactStore, LocalArtifactStore, MetaStore};
use rt_types::{BuildState, CompileMode, NewVersion};

const LONG: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn a_digest_gc_freed_is_kept_when_a_concurrent_upload_references_it_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, a) = (meta(), LocalArtifactStore::open(dir.path()).expect("artifacts"));
    let t = mk_tenant(&m, "t").await;
    let (svc, other) = (mk_service(&m, &t, "svc").await, mk_service(&m, &t, "other").await);
    let d1 = a
        .put_typed(Bytes::from_static(b"export default () => 1"), "application/typescript")
        .await
        .expect("put v1");
    m.add_version(
        svc.id,
        NewVersion {
            source_digest: d1,
            compile: CompileMode::Lazy,
        },
    )
    .await
    .expect("v1");
    let d2 = a
        .put_typed(Bytes::from_static(b"export default () => 2"), "application/typescript")
        .await
        .expect("put v2");
    let v2 = m
        .add_version(
            svc.id,
            NewVersion {
                source_digest: d2,
                compile: CompileMode::Lazy,
            },
        )
        .await
        .expect("v2");
    m.set_live(svc.id, v2.number, None).await.expect("live");
    let freed = m.gc_versions(svc.id, 1).await.expect("gc");
    assert_eq!(freed, vec![d1]);

    // Meanwhile another service uploads the same bytes: the put is a no-op, the version is new.
    a.put_typed(Bytes::from_static(b"export default () => 1"), "application/typescript")
        .await
        .expect("re-upload");
    m.add_version(
        other.id,
        NewVersion {
            source_digest: d1,
            compile: CompileMode::Lazy,
        },
    )
    .await
    .expect("other v1");

    assert_eq!(
        delete_unreferenced(&*m, &*a, freed, Duration::ZERO).await,
        0,
        "referenced again: kept"
    );
    assert!(a.exists(&d1).await.expect("exists"));
}

#[tokio::test]
async fn an_unreferenced_artifact_written_or_touched_within_the_grace_period_is_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, a) = (meta(), LocalArtifactStore::open(dir.path()).expect("artifacts"));
    // Uploaded, version not added yet (or a module a running build just vendored).
    let d = a.put(Bytes::from_static(b"just written")).await.expect("put");
    assert_eq!(delete_unreferenced(&*m, &*a, vec![d], LONG).await, 0, "fresh: kept");
    assert!(a.exists(&d).await.expect("exists"));
    a.touch(&d).await.expect("touch");
    assert!(
        !a.delete_if_idle(&d, LONG).await.expect("delete_if_idle"),
        "touched: kept"
    );
    assert_eq!(
        delete_unreferenced(&*m, &*a, vec![d], Duration::ZERO).await,
        1,
        "past the grace period: deleted"
    );
    assert!(!a.exists(&d).await.expect("exists"));
}

#[tokio::test]
async fn the_orphan_sweep_frees_what_failed_builds_left_and_keeps_everything_referenced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, a) = (meta(), LocalArtifactStore::open(dir.path()).expect("artifacts"));
    let t = mk_tenant(&m, "t").await;
    let svc = mk_service(&m, &t, "svc").await;
    // A successful build: source, lock and one vendored module, all referenced.
    let source = a
        .put(Bytes::from_static(b"import x from 'https://esm.sh/x'"))
        .await
        .expect("source");
    let v = m
        .add_version(
            svc.id,
            NewVersion {
                source_digest: source,
                compile: CompileMode::Lazy,
            },
        )
        .await
        .expect("v");
    let module = a.put(Bytes::from_static(b"export default 3")).await.expect("module");
    let lock = a.put(Bytes::from_static(b"{\"version\":1}")).await.expect("lock");
    m.record_lock_modules(&lock, &[module]).await.expect("record");
    m.set_build_state(svc.id, v.number, BuildState::Ready, None, Some(lock))
        .await
        .expect("ready");
    // A failed build: modules vendored and a lock stored, nothing recorded.
    let orphans = [
        a.put(Bytes::from_static(b"export default 'orphan 1'"))
            .await
            .expect("orphan module 1"),
        a.put(Bytes::from_static(b"export default 'orphan 2'"))
            .await
            .expect("orphan module 2"),
        a.put(Bytes::from_static(b"{\"version\":1,\"failed\":true}"))
            .await
            .expect("orphan lock"),
    ];

    assert_eq!(
        sweep_orphans(&*m, &*a, LONG).await.expect("sweep"),
        0,
        "everything is still within the grace period"
    );
    assert_eq!(sweep_orphans(&*m, &*a, Duration::ZERO).await.expect("sweep"), 3);
    for d in orphans {
        assert!(!a.exists(&d).await.expect("exists"), "orphan {d} freed");
    }
    for d in [source, module, lock] {
        assert!(a.exists(&d).await.expect("exists"), "referenced {d} kept");
    }
}

#[tokio::test]
async fn a_later_typescript_upload_of_the_same_bytes_records_typescript() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = LocalArtifactStore::open(dir.path()).expect("artifacts");
    let bytes = Bytes::from_static(b"export default (n) => n + 1;");
    let d = a.put_typed(bytes.clone(), "application/javascript").await.expect("js");
    assert_eq!(
        a.content_type(&d).await.expect("ct").as_deref(),
        Some("application/javascript")
    );
    // TypeScript is the superset: bytes accepted as TypeScript build as TypeScript.
    a.put_typed(bytes.clone(), "application/typescript").await.expect("ts");
    assert_eq!(
        a.content_type(&d).await.expect("ct").as_deref(),
        Some("application/typescript")
    );
    // A later JavaScript upload does not downgrade it.
    a.put_typed(bytes, "text/javascript").await.expect("js again");
    assert_eq!(
        a.content_type(&d).await.expect("ct").as_deref(),
        Some("application/typescript")
    );
}
