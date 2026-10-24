//! `meta.db` schema v4 (decision D-A1, review handoff B5.3): the service environment label, the
//! `call` block in the route snapshot, and the custom-host claim lifecycle the maintenance jobs
//! use (`created_at`, pending listing, DNS activation, expiry).

use crate::common;

use common::*;
use rt_store::{MetaStore, StoreError, TenantScope};
use rt_types::{CallAccess, HostMapping, HostStatus, ProjectPatch, ServiceId, ServicePatch, DEFAULT_ENVIRONMENT};

fn pending(host: &str, svc: ServiceId, token: &str, created_at_ms: i64) -> HostMapping {
    HostMapping {
        host: host.into(),
        service_id: svc,
        status: HostStatus::PendingVerification,
        verify_token: Some(token.into()),
        created_at_ms,
    }
}

#[tokio::test]
async fn the_environment_is_stored_patched_validated_and_routed() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    assert_eq!(a.environment, DEFAULT_ENVIRONMENT, "services default to production");

    // D-B3: a service's environment is one of its project's, so the project lists them first.
    let envs = ["production", "staging", "dev"].map(String::from).to_vec();
    m.update_project(
        t.id,
        "default",
        ProjectPatch {
            name: None,
            environments: Some(envs),
        },
    )
    .await
    .expect("project envs");

    let mut ns = new_service("b");
    ns.environment = "staging".into();
    ns.config.call = CallAccess {
        allowed_callers: vec!["env:dev".into(), "a".into()],
    };
    let b = m
        .create_service(TenantScope::Tenant(t.id), t.id, ns)
        .await
        .expect("create");
    assert_eq!(b.environment, "staging");
    let got = m
        .get_service(TenantScope::Operator, b.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        (got.environment.as_str(), &got.config.call.allowed_callers[..]),
        ("staging", &["env:dev".to_string(), "a".into()][..])
    );

    let mut bad = new_service("c");
    bad.environment = "Not Valid".into();
    assert!(matches!(
        m.create_service(TenantScope::Tenant(t.id), t.id, bad).await,
        Err(StoreError::Invalid(_))
    ));

    let patched = m
        .patch_service(
            TenantScope::Tenant(t.id),
            a.id,
            ServicePatch {
                environment: Some("dev".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("patch env");
    assert_eq!(patched.environment, "dev");
    assert!(patched.config.call.allowed_callers.is_empty(), "other blocks unchanged");
    let bad_patch = ServicePatch {
        environment: Some("".into()),
        ..Default::default()
    };
    assert!(matches!(
        m.patch_service(TenantScope::Tenant(t.id), a.id, bad_patch, None).await,
        Err(StoreError::Invalid(_))
    ));
    let call = CallAccess {
        allowed_callers: vec!["b".into()],
    };
    let patched = m
        .patch_service(
            TenantScope::Tenant(t.id),
            a.id,
            ServicePatch {
                call: Some(call.clone()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("patch call");
    assert_eq!(
        (patched.environment.as_str(), &patched.config.call),
        ("dev", &call),
        "`call` replaced, environment kept"
    );

    for s in [a.id, b.id] {
        let v = mk_versions(&m, s, if s == a.id { 1 } else { 10 }, 1).await;
        m.set_live(s, v[0].number, None).await.expect("live");
    }
    let snap = m.route_snapshot().await.expect("snapshot");
    let ea = snap.entries.iter().find(|e| e.service == a.id).expect("a routed");
    let eb = snap.entries.iter().find(|e| e.service == b.id).expect("b routed");
    assert_eq!((ea.environment.as_str(), &ea.call), ("dev", &call));
    assert_eq!(eb.environment, "staging");
    assert_eq!(eb.call.allowed_callers, vec!["env:dev".to_string(), "a".into()]);
}

#[tokio::test]
async fn host_claims_record_when_they_were_made() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let before = now_ms();
    m.put_host(pending("new.example.com", a.id, "tok", 0))
        .await
        .expect("claim");
    let got = m.find_host("new.example.com").await.expect("find").expect("exists");
    assert!(
        got.created_at_ms >= before && got.created_at_ms <= now_ms() + 1,
        "0 means now: {}",
        got.created_at_ms
    );

    m.put_host(pending("old.example.com", a.id, "tok", 1_234))
        .await
        .expect("claim with a time");
    assert_eq!(
        m.find_host("old.example.com")
            .await
            .expect("find")
            .expect("exists")
            .created_at_ms,
        1_234
    );
}

#[tokio::test]
async fn ac_sec_44_a_matching_token_activates_only_the_current_pending_claim() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;
    let v = mk_versions(&m, a.id, 1, 1).await;
    m.set_live(a.id, v[0].number, None).await.expect("live");
    m.put_host(pending("shop.example.com", a.id, "right", 0))
        .await
        .expect("claim");
    m.put_host(pending("other.example.com", b.id, "tok-b", 0))
        .await
        .expect("claim b");

    let all = m.pending_hosts().await.expect("pending");
    assert_eq!(all.iter().map(|h| h.host.as_str()).collect::<Vec<_>>().len(), 2);
    assert!(all.iter().all(|h| h.status == HostStatus::PendingVerification));

    assert!(!m
        .activate_host("shop.example.com", a.id, "wrong")
        .await
        .expect("wrong token"));
    assert!(!m
        .activate_host("shop.example.com", b.id, "right")
        .await
        .expect("wrong service"));
    assert!(
        m.route_snapshot().await.expect("snap").entries[0].hosts.is_empty(),
        "still pending: not routed"
    );

    assert!(m
        .activate_host("shop.example.com", a.id, "right")
        .await
        .expect("activate"));
    let h = m.find_host("shop.example.com").await.expect("find").expect("exists");
    assert_eq!((h.status, h.verify_token), (HostStatus::Active, None));
    assert_eq!(
        m.route_snapshot().await.expect("snap").entries[0].hosts,
        vec!["shop.example.com".to_string()]
    );
    assert!(
        !m.activate_host("shop.example.com", a.id, "right").await.expect("twice"),
        "already active"
    );
    assert_eq!(
        m.pending_hosts().await.expect("pending").len(),
        1,
        "only b's claim is still pending"
    );
}

#[tokio::test]
async fn ac_sec_43_expired_pending_claims_are_deleted_and_active_ones_never_expire() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;
    m.put_host(pending("stale.example.com", a.id, "tok", 1_000))
        .await
        .expect("old claim");
    m.put_host(pending("fresh.example.com", a.id, "tok", 50_000))
        .await
        .expect("fresh claim");
    m.put_host(HostMapping {
        status: HostStatus::Active,
        verify_token: None,
        ..pending("live.example.com", a.id, "x", 1)
    })
    .await
    .expect("old but active");

    // While it exists, the stale claim still blocks another pending claim.
    assert_eq!(
        m.put_host(pending("stale.example.com", b.id, "t2", 0))
            .await
            .expect_err("taken")
            .code(),
        "host_taken"
    );

    let expired = m.expire_pending_hosts(10_000).await.expect("expire");
    assert_eq!(
        expired.iter().map(|h| h.host.as_str()).collect::<Vec<_>>(),
        vec!["stale.example.com"]
    );
    assert_eq!(m.find_host("stale.example.com").await.expect("find"), None);
    assert!(m.find_host("fresh.example.com").await.expect("find").is_some());
    assert_eq!(
        m.find_host("live.example.com")
            .await
            .expect("find")
            .expect("active")
            .status,
        HostStatus::Active
    );

    // Once expired, the host no longer blocks other services.
    m.put_host(pending("stale.example.com", b.id, "t2", 0))
        .await
        .expect("b claims the freed host");
    assert!(m.expire_pending_hosts(10_000).await.expect("again").is_empty());
}
