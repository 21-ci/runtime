//! STO-13: version retention. `gc_versions` keeps `live` + the newest `keep` versions; the artifact
//! refcount is computed live from `versions` (`digest_referenced`), and unreferenced artifacts are
//! deleted by the caller. These tests drive the same sequence the server runs after a deploy.

use crate::common;

use std::collections::HashSet;

use bytes::Bytes;
use common::*;
use rt_store::{ArtifactStore, LocalArtifactStore, MetaStore, SqliteMetaStore, TenantScope};
use rt_types::{Digest, NewVersion, Service, ServiceId, Tenant, VersionNo};

async fn setup() -> (std::sync::Arc<SqliteMetaStore>, Tenant, Service) {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "gc").await;
    (m, t, s)
}

async fn numbers(m: &SqliteMetaStore, s: ServiceId) -> Vec<u32> {
    m.list_versions(s)
        .await
        .expect("list versions")
        .into_iter()
        .map(|v| v.number.0)
        .collect()
}

fn set_of(v: Vec<Digest>) -> HashSet<Digest> {
    v.into_iter().collect()
}

#[tokio::test]
async fn ac_sto_13_twelfth_deploy_drops_the_two_oldest_non_live_versions() {
    let (m, _t, s) = setup().await;
    mk_versions(&m, s.id, 1, 12).await; // v1..v12, digests d1..d12
    m.set_live(s.id, VersionNo(12), None).await.expect("live = newest");

    let removed = m.gc_versions(s.id, 10).await.expect("gc");

    assert_eq!(
        numbers(&m, s.id).await,
        (3..=12).collect::<Vec<_>>(),
        "v1 and v2 are beyond the newest 10"
    );
    assert_eq!(
        set_of(removed),
        HashSet::from([digest_n(1), digest_n(2)]),
        "their (unshared) source digests are returned"
    );
    assert!(!m.digest_referenced(&digest_n(1)).await.expect("referenced"));
    assert!(!m.digest_referenced(&digest_n(2)).await.expect("referenced"));
    assert!(m.digest_referenced(&digest_n(3)).await.expect("referenced"));
    assert!(m.digest_referenced(&digest_n(12)).await.expect("referenced"));

    // Idempotent: a second run has nothing to do.
    assert!(m.gc_versions(s.id, 10).await.expect("gc again").is_empty());
    assert_eq!(numbers(&m, s.id).await.len(), 10);
}

#[tokio::test]
async fn ac_sto_13_below_the_retention_count_nothing_is_removed() {
    let (m, _t, s) = setup().await;
    mk_versions(&m, s.id, 1, 10).await;
    m.set_live(s.id, VersionNo(10), None).await.expect("live");
    assert!(m.gc_versions(s.id, 10).await.expect("gc").is_empty());
    assert_eq!(numbers(&m, s.id).await.len(), 10);
}

#[tokio::test]
async fn ac_sto_13_the_live_version_is_kept_even_when_it_is_older_than_the_window() {
    let (m, _t, s) = setup().await;
    mk_versions(&m, s.id, 1, 12).await;
    m.set_live(s.id, VersionNo(1), None)
        .await
        .expect("live = oldest (rolled back)");

    let removed = m.gc_versions(s.id, 10).await.expect("gc");

    let mut expected: Vec<u32> = vec![1];
    expected.extend(3..=12);
    assert_eq!(
        numbers(&m, s.id).await,
        expected,
        "live v1 + newest 10 (v3..v12); only v2 goes"
    );
    assert_eq!(set_of(removed), HashSet::from([digest_n(2)]));
    assert!(
        m.digest_referenced(&digest_n(1)).await.expect("referenced"),
        "the live version's digest stays referenced"
    );
    assert_eq!(
        m.get_service(TenantScope::Operator, s.id)
            .await
            .expect("get")
            .expect("exists")
            .live_version,
        Some(VersionNo(1))
    );
}

#[tokio::test]
async fn ac_sto_13_digests_still_used_by_another_version_or_service_are_not_returned() {
    let (m, t, s) = setup().await;
    let other = mk_service(&m, &t, "other-svc").await;

    // v1 shares its digest with v12 of the same service and with a version of another service;
    // v2 shares nothing; v3 is also used by the other service.
    m.add_version(
        s.id,
        NewVersion {
            source_digest: digest_n(100),
            ..new_version(0)
        },
    )
    .await
    .expect("v1 = d100");
    m.add_version(s.id, new_version(2)).await.expect("v2");
    m.add_version(s.id, new_version(3)).await.expect("v3");
    for n in 4..=11 {
        m.add_version(s.id, new_version(n)).await.expect("filler");
    }
    m.add_version(
        s.id,
        NewVersion {
            source_digest: digest_n(100),
            ..new_version(0)
        },
    )
    .await
    .expect("v12 = d100");
    m.add_version(
        other.id,
        NewVersion {
            source_digest: digest_n(3),
            ..new_version(0)
        },
    )
    .await
    .expect("other uses d3");
    m.set_live(s.id, VersionNo(12), None).await.expect("live");
    // Make v1/v2 (and v3) fall out of the window: add two more versions.
    m.add_version(s.id, new_version(13)).await.expect("v13");
    m.add_version(s.id, new_version(14)).await.expect("v14");

    let removed = m.gc_versions(s.id, 10).await.expect("gc");
    assert_eq!(numbers(&m, s.id).await, (5..=14).collect::<Vec<_>>());
    // v1..v4 are removed. d100 is still used by v12; d3 by the other service; d2 and d4 are free.
    assert_eq!(set_of(removed), HashSet::from([digest_n(2), digest_n(4)]));
    assert!(m.digest_referenced(&digest_n(100)).await.expect("referenced"));
    assert!(m.digest_referenced(&digest_n(3)).await.expect("referenced"));
    assert!(!m.digest_referenced(&digest_n(2)).await.expect("referenced"));
}

#[tokio::test]
async fn ac_sto_13_gc_only_touches_the_given_service() {
    let (m, t, s) = setup().await;
    let other = mk_service(&m, &t, "untouched").await;
    mk_versions(&m, s.id, 1, 12).await;
    mk_versions(&m, other.id, 100, 12).await;
    m.set_live(s.id, VersionNo(12), None).await.expect("live");

    m.gc_versions(s.id, 10).await.expect("gc");
    assert_eq!(numbers(&m, s.id).await.len(), 10);
    assert_eq!(
        numbers(&m, other.id).await.len(),
        12,
        "another service's history is not collected"
    );
}

#[tokio::test]
async fn ac_sto_13_unreferenced_artifacts_are_deleted_and_referenced_ones_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let artifacts = LocalArtifactStore::open(&dir.path().join("artifacts")).expect("artifact store");
    let (m, _t, s) = setup().await;

    // 12 deploys, each with its own artifact; v2 and v12 share one artifact.
    let mut digests = Vec::new();
    for n in 1..=12u64 {
        let content = if n == 12 {
            format!("source-{}", 2)
        } else {
            format!("source-{n}")
        };
        let d = artifacts.put(Bytes::from(content)).await.expect("put artifact");
        m.add_version(
            s.id,
            NewVersion {
                source_digest: d,
                compile: rt_types::CompileMode::Lazy,
            },
        )
        .await
        .expect("add version");
        digests.push(d);
    }
    assert_eq!(digests[1], digests[11], "v2 and v12 are the same artifact");
    m.set_live(s.id, VersionNo(12), None).await.expect("live");

    // The server's retention step: gc, then delete what nothing references any more.
    let candidates = m.gc_versions(s.id, 10).await.expect("gc");
    assert_eq!(
        set_of(candidates.clone()),
        HashSet::from([digests[0]]),
        "only v1's artifact is unreferenced; v2's is shared with v12"
    );
    for d in &candidates {
        if !m.digest_referenced(d).await.expect("referenced") {
            artifacts.delete(d).await.expect("delete unreferenced artifact");
        }
    }

    assert!(
        !artifacts.exists(&digests[0]).await.expect("exists"),
        "unreferenced artifact deleted"
    );
    for d in &digests[1..] {
        assert!(
            artifacts.exists(d).await.expect("exists"),
            "referenced artifact {d} must be kept"
        );
        assert!(artifacts.get(d).await.is_ok());
    }
    assert_eq!(numbers(&m, s.id).await, (3..=12).collect::<Vec<_>>());
    // v2 was collected but its artifact lives on through v12.
    assert!(m.digest_referenced(&digests[1]).await.expect("referenced"));
}
