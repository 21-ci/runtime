//! `meta.db` operator SDKs: the system tenant, SDKs, releases,
//! channels, grants, and the SDK part of the route snapshot.

use crate::common;

use common::*;
use rt_store::{MetaStore, StoreError, TenantScope};
use rt_types::{
    system_tenant, GrantEffect, GrantScope, NewSdkRelease, ProjectId, SdkGrant, SdkState, VersionNo, SYSTEM_PLAN,
};

fn release(sdk: &str, version: &str, n: u32) -> NewSdkRelease {
    NewSdkRelease {
        sdk: sdk.into(),
        version: version.into(),
        version_no: VersionNo(n),
        members: vec!["ping".into(), "invoices".into()],
        contract: format!("declare module \"sdk:{sdk}\" {{}} // {version}"),
    }
}

fn grant(scope: GrantScope, sdk: &str, effect: Option<GrantEffect>) -> SdkGrant {
    SdkGrant {
        scope,
        sdk: sdk.into(),
        effect,
        version_req: None,
        channel: None,
        created_by: "root".into(),
        updated_at_ms: 0,
    }
}

/// The system tenant and an SDK service in it with two versions.
async fn sdk_service(m: &rt_store::SqliteMetaStore) -> rt_types::Service {
    m.ensure_tenant(system_tenant(), "sys", SYSTEM_PLAN)
        .await
        .expect("ensure sys");
    let mut s = new_service("sys-sdk-acme");
    s.project = "default".into();
    let svc = m
        .create_service(TenantScope::Tenant(system_tenant()), system_tenant(), s)
        .await
        .expect("create");
    mk_versions(m, svc.id, 1, 3).await;
    svc
}

#[tokio::test]
async fn the_system_tenant_is_created_once_with_a_default_project() {
    let m = meta();
    m.ensure_tenant(system_tenant(), "sys", SYSTEM_PLAN)
        .await
        .expect("first");
    m.ensure_tenant(system_tenant(), "other", "other")
        .await
        .expect("second is a no-op");
    let t = m.get_tenant(system_tenant()).await.expect("get").expect("exists");
    assert_eq!((t.name.as_str(), t.plan.as_str()), ("sys", SYSTEM_PLAN));
    assert!(m.get_project(system_tenant(), "default").await.expect("get").is_some());
}

#[tokio::test]
async fn releases_channels_and_removal() {
    let m = meta();
    let svc = sdk_service(&m).await;
    let sdk = m
        .upsert_sdk("acme", svc.id, Some("Acme API".into()))
        .await
        .expect("register");
    assert_eq!((sdk.state, sdk.service_id), (SdkState::Active, svc.id));

    m.add_sdk_release(release("acme", "1.0.0", 1)).await.expect("1.0.0");
    m.add_sdk_release(release("acme", "1.1.0", 2)).await.expect("1.1.0");
    match m.add_sdk_release(release("acme", "1.1.0", 3)).await {
        Err(StoreError::Conflict(c)) => assert_eq!(c, "sdk_release_exists"),
        other => panic!("{other:?}"),
    }
    let rels = m.list_sdk_releases("acme").await.expect("list");
    assert_eq!(
        rels.iter().map(|r| r.version.as_str()).collect::<Vec<_>>(),
        ["1.0.0", "1.1.0"]
    );
    assert_eq!(rels[0].members, ["ping", "invoices"]);
    assert!(m
        .get_sdk_release("acme", "1.1.0")
        .await
        .expect("get")
        .unwrap()
        .contract
        .contains("1.1.0"));

    let c = m.set_sdk_channel("acme", "stable", "1.0.0").await.expect("promote");
    assert_eq!((c.version.as_str(), c.previous.as_deref()), ("1.0.0", None));
    let c = m
        .set_sdk_channel("acme", "stable", "1.1.0")
        .await
        .expect("promote again");
    assert_eq!((c.version.as_str(), c.previous.as_deref()), ("1.1.0", Some("1.0.0")));
    assert!(matches!(
        m.set_sdk_channel("acme", "stable", "9.9.9").await,
        Err(StoreError::NotFound)
    ));

    // The route snapshot carries what the data plane needs (no contract text).
    let snap = m.route_snapshot().await.expect("snapshot").sdk;
    assert_eq!(snap.sdks.len(), 1);
    let stable = snap.channel_release("acme", "stable").expect("stable release");
    assert_eq!(
        (stable.version.as_str(), stable.version_no, stable.service),
        ("1.1.0", VersionNo(2), svc.id)
    );
    assert_eq!(stable.digest, digest_n(2));

    assert!(m.remove_sdk("acme").await.expect("remove"));
    assert!(!m.remove_sdk("nope").await.expect("unknown"));
    assert_eq!(m.get_sdk("acme").await.expect("get").unwrap().state, SdkState::Removed);
    // Publishing again makes it active.
    assert_eq!(
        m.upsert_sdk("acme", svc.id, None).await.expect("again").state,
        SdkState::Active
    );
}

#[tokio::test]
async fn grants_are_upserted_per_scope_and_deleted_when_empty() {
    let m = meta();
    let p = ProjectId::from_u128(5);
    let g = m
        .put_sdk_grant(grant(GrantScope::Node, "acme", Some(GrantEffect::Allow)))
        .await
        .expect("put")
        .expect("kept");
    assert!(g.updated_at_ms > 0);
    let mut env = grant(GrantScope::Environment(p, "staging".into()), "acme", None);
    env.channel = Some("canary".into());
    m.put_sdk_grant(env.clone()).await.expect("env");
    let mut node = grant(GrantScope::Node, "acme", Some(GrantEffect::Allow));
    node.version_req = Some("^1".into());
    m.put_sdk_grant(node).await.expect("replace");

    let all = m.list_sdk_grants().await.expect("list");
    assert_eq!(all.len(), 2);
    let node = all.iter().find(|g| g.scope == GrantScope::Node).expect("node grant");
    assert_eq!(node.version_req.as_deref(), Some("^1"));
    assert_eq!(
        all.iter()
            .find(|g| g.scope == env.scope)
            .and_then(|g| g.channel.clone())
            .as_deref(),
        Some("canary")
    );

    // All fields empty: the grant is gone.
    assert!(m
        .put_sdk_grant(grant(GrantScope::Node, "acme", None))
        .await
        .expect("clear")
        .is_none());
    assert_eq!(m.list_sdk_grants().await.expect("list").len(), 1);
    assert_eq!(m.route_snapshot().await.expect("snapshot").sdk.grants.len(), 1);
}
