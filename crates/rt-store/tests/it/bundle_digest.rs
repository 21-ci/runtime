//! B7: the digest of a version's built bundle is stored in `versions` for audit
//! (migration 0005, `Version::bundle_digest`, `MetaStore::set_bundle_digest`).

use crate::common;

use common::*;
use rt_store::{MetaStore, SqliteMetaStore, StoreError};
use rt_types::{BuildState, VersionNo};

#[tokio::test]
async fn the_bundle_digest_is_recorded_per_version_and_survives_a_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("meta.db");
    let (service, bundle) = {
        let m = SqliteMetaStore::open(&path).expect("open meta.db");
        let t = mk_tenant(&m, "acme").await;
        let s = mk_service(&m, &t, "audited").await;
        let v = mk_versions(&m, s.id, 1, 2).await;
        assert!(
            v.iter().all(|v| v.bundle_digest.is_none()),
            "unknown until a build reports it"
        );

        let bundle = digest_n(77);
        m.set_build_state(s.id, v[0].number, BuildState::Ready, None, None)
            .await
            .expect("ready");
        m.set_bundle_digest(s.id, v[0].number, bundle)
            .await
            .expect("record bundle digest");
        let got = m.get_version(s.id, v[0].number).await.expect("get").expect("version");
        assert_eq!(got.bundle_digest, Some(bundle));
        assert_eq!(
            got.build_state,
            BuildState::Ready,
            "recording the digest leaves the build state alone"
        );
        let other = m.get_version(s.id, v[1].number).await.expect("get").expect("version");
        assert_eq!(other.bundle_digest, None);
        (s, bundle)
    };
    let m = SqliteMetaStore::open(&path).expect("reopen");
    let versions = m.list_versions(service.id).await.expect("list");
    assert_eq!(
        versions
            .iter()
            .find(|v| v.number == VersionNo(1))
            .and_then(|v| v.bundle_digest),
        Some(bundle)
    );
}

#[tokio::test]
async fn recording_a_bundle_digest_for_an_unknown_version_is_not_found() {
    let m = meta();
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "nothing-yet").await;
    let err = m
        .set_bundle_digest(s.id, VersionNo(9), digest_n(1))
        .await
        .expect_err("no such version");
    assert!(matches!(err, StoreError::NotFound), "{err:?}");
}
