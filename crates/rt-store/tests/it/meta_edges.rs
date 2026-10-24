//! `meta.db` error paths: validation the store enforces before it writes, quota and cap checks,
//! and statements that fail because a table is damaged. A second connection renames a table away
//! for the duration of a test (see [`Broken`]); a row with a value the decoder does not know is
//! written with CHECK constraints off, the way a database from another version could hold it.

use crate::common;

use std::path::{Path, PathBuf};

use common::*;
use rt_store::{HostRelease, MetaStore, PageReq, SqliteMetaStore, StoreError, TenantPatch, TenantScope, WarmFloorCap};
use rt_types::{
    GrantEffect, GrantScope, HostMapping, HostStatus, NewSchedule, ScheduleKind, SdkGrant, ServiceId, VersionNo,
    SYSTEM_PLAN,
};

/// A file-backed meta store in `dir`.
fn file_meta(dir: &Path) -> (std::sync::Arc<SqliteMetaStore>, PathBuf) {
    let p = dir.join("meta.db");
    (SqliteMetaStore::open(&p).expect("file meta store opens"), p)
}

fn raw(p: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(p).expect("second connection to the same file")
}

/// Renames `table` away until dropped: every statement on it fails meanwhile.
struct Broken {
    path: PathBuf,
    table: &'static str,
}

impl Broken {
    fn new(path: &Path, table: &'static str) -> Self {
        raw(path)
            .execute_batch(&format!("ALTER TABLE {table} RENAME TO {table}_gone"))
            .expect("break table");
        Self {
            path: path.to_path_buf(),
            table,
        }
    }
}

impl Drop for Broken {
    fn drop(&mut self) {
        let _ = raw(&self.path).execute_batch(&format!("ALTER TABLE {0}_gone RENAME TO {0}", self.table));
    }
}

fn every(seconds: u64) -> NewSchedule {
    NewSchedule {
        kind: ScheduleKind::Interval { every_seconds: seconds },
        payload: serde_json::Value::Null,
        enabled: true,
    }
}

fn pending_host(host: &str, svc: ServiceId) -> HostMapping {
    HostMapping {
        host: host.into(),
        service_id: svc,
        status: HostStatus::PendingVerification,
        verify_token: Some("tok".into()),
        created_at_ms: 0,
    }
}

fn host_release(host: &str, tenant: rt_types::TenantId) -> HostRelease {
    HostRelease {
        host: host.into(),
        tenant,
        created_at_ms: 0,
        verify_token: None,
        released_at_ms: now_ms(),
    }
}

#[tokio::test]
async fn a_schedule_is_refused_before_it_is_written_when_it_cannot_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, _) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "sched").await;

    let e = m.create_schedule(s.id, every(0)).await.expect_err("zero interval");
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    let blank = NewSchedule {
        kind: ScheduleKind::Cron {
            expr: "   ".into(),
            tz: "UTC".into(),
        },
        payload: serde_json::Value::Null,
        enabled: true,
    };
    assert!(
        matches!(m.create_schedule(s.id, blank).await, Err(StoreError::Invalid(_))),
        "blank cron"
    );
    let big = NewSchedule {
        kind: ScheduleKind::Interval { every_seconds: 60 },
        payload: serde_json::Value::String("x".repeat(70_000)),
        enabled: true,
    };
    assert!(
        matches!(m.create_schedule(s.id, big).await, Err(StoreError::Invalid(_))),
        "payload over 64 KB"
    );
    assert!(
        m.list_schedules(s.id).await.expect("list").is_empty(),
        "nothing was written"
    );
}

#[tokio::test]
async fn a_schedule_on_a_damaged_table_is_an_error_on_every_schedule_operation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "sched").await;
    let sch = m.create_schedule(s.id, every(60)).await.expect("schedule");
    {
        let _b = Broken::new(&p, "schedules");
        assert!(m.create_schedule(s.id, every(60)).await.is_err());
        assert!(m.update_schedule(s.id, sch.id, every(120)).await.is_err());
        assert!(m.busiest_service_schedules(t.id).await.is_err());
    }
    assert_eq!(m.list_schedules(s.id).await.expect("list").len(), 1);
}

#[tokio::test]
async fn a_stored_schedule_kind_the_decoder_does_not_know_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "sched").await;
    m.create_schedule(s.id, every(60)).await.expect("schedule");
    raw(&p)
        .execute_batch("PRAGMA ignore_check_constraints = ON; UPDATE schedules SET kind = 'bogus'")
        .expect("write a value the CHECK forbids");
    assert!(m.list_schedules(s.id).await.is_err(), "decoding an unknown kind fails");
    assert!(m.all_schedules().await.is_err());
}

#[tokio::test]
async fn a_stored_host_status_and_a_build_state_the_decoders_do_not_know_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "hosts").await;
    m.put_host(pending_host("a.example.test", s.id)).await.expect("claim");
    mk_versions(&m, s.id, 1, 1).await;

    raw(&p)
        .execute_batch("PRAGMA ignore_check_constraints = ON; UPDATE hosts SET status = 'bogus'")
        .expect("write");
    assert!(m.list_hosts(s.id).await.is_err());
    raw(&p)
        .execute_batch("PRAGMA ignore_check_constraints = ON; UPDATE versions SET build_state = 'bogus'")
        .expect("write");
    assert!(m.list_versions(s.id).await.is_err());
    assert!(m.get_version(s.id, VersionNo(1)).await.is_err());
}

#[test]
fn a_warm_floor_cap_without_a_per_service_limit_passes_the_floor_through() {
    assert_eq!(
        WarmFloorCap {
            total: 10,
            per_service: 0
        }
        .clamp(3),
        3
    );
    assert_eq!(
        WarmFloorCap {
            total: 10,
            per_service: 2
        }
        .clamp(3),
        2
    );
}

#[tokio::test]
async fn a_warm_floor_total_that_cannot_be_read_fails_the_capped_create() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let mut ns = new_service("warm");
    ns.config.scaling.min_instances = Some(2);
    let _b = Broken::new(&p, "services");
    let r = m
        .create_service_capped(
            TenantScope::Tenant(t.id),
            t.id,
            ns,
            WarmFloorCap {
                total: 4,
                per_service: 0,
            },
        )
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn the_service_quota_is_enforced_and_its_count_must_be_readable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    m.create_service_limited(
        TenantScope::Tenant(t.id),
        t.id,
        new_service("one"),
        WarmFloorCap::NONE,
        1,
    )
    .await
    .expect("the first service fits the quota");
    let e = m
        .create_service_limited(
            TenantScope::Tenant(t.id),
            t.id,
            new_service("two"),
            WarmFloorCap::NONE,
            1,
        )
        .await
        .expect_err("over quota");
    assert!(matches!(e, StoreError::Conflict("quota_exceeded")), "{e:?}");
    let _b = Broken::new(&p, "services");
    assert!(m
        .create_service_limited(
            TenantScope::Tenant(t.id),
            t.id,
            new_service("three"),
            WarmFloorCap::NONE,
            5
        )
        .await
        .is_err());
}

#[tokio::test]
async fn tenant_operations_on_a_damaged_tenants_table_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    {
        let _b = Broken::new(&p, "tenants");
        assert!(m.create_tenant(new_tenant("other")).await.is_err());
        assert!(m
            .update_tenant(
                t.id,
                TenantPatch {
                    name: Some("x".into()),
                    ..Default::default()
                }
            )
            .await
            .is_err());
        assert!(m.ensure_tenant(t.id, "acme", "free").await.is_err());
    }
    assert!(m.get_tenant(t.id).await.expect("get").is_some());
}

#[tokio::test]
async fn deleting_a_tenant_that_still_owns_services_is_refused_without_a_purge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, _) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    mk_service(&m, &t, "kept").await;
    let e = m.delete_tenant(t.id, false).await.expect_err("not empty");
    assert!(matches!(e, StoreError::Conflict("tenant_not_empty")), "{e:?}");
}

#[tokio::test]
async fn key_operations_on_a_damaged_api_keys_table_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let key = rt_types::NewKey {
        id: rt_types::KeyId::from_u128(1),
        name: None,
        scopes: rt_types::Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        hmac: vec![1; 32],
    };
    m.create_key(t.id, key.clone()).await.expect("key");
    {
        let _b = Broken::new(&p, "api_keys");
        let mut other = key.clone();
        other.id = rt_types::KeyId::from_u128(2);
        assert!(m.create_key(t.id, other).await.is_err());
        assert!(m.count_live_keys(t.id, now_ms()).await.is_err());
        assert!(m.revoke_key(t.id, key.id).await.is_err());
        assert!(m.revoke_key_tree(t.id, key.id).await.is_err());
    }
    assert_eq!(m.count_live_keys(t.id, now_ms()).await.expect("count"), 1);
}

#[tokio::test]
async fn project_operations_on_damaged_tables_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let pr = m
        .create_project(t.id, rt_types::NewProject::new("web"))
        .await
        .expect("project");
    {
        let _envs = Broken::new(&p, "project_environments");
        assert!(m.create_project(t.id, rt_types::NewProject::new("api")).await.is_err());
        assert!(m
            .update_project(
                t.id,
                "web",
                rt_types::ProjectPatch {
                    environments: Some(vec!["staging".into()]),
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }
    {
        let _p = Broken::new(&p, "projects");
        assert!(m.tenant_projects_among(t.id, &[pr.id]).await.is_err());
        assert!(m
            .update_project(t.id, "web", rt_types::ProjectPatch::default())
            .await
            .is_err());
    }
    assert_eq!(
        m.tenant_projects_among(t.id, &[pr.id]).await.expect("works again"),
        vec![pr.id]
    );
}

#[tokio::test]
async fn service_patches_set_each_config_section_and_a_damaged_services_table_fails_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "patched").await;
    let patch = rt_types::ServicePatch {
        access: Some(Default::default()),
        triggers: Some(Default::default()),
        ingress: Some(Default::default()),
        egress: Some(Default::default()),
        deno: Some(Default::default()),
        durable: Some(true),
        ws: Some(Default::default()),
        ..Default::default()
    };
    m.patch_service(TenantScope::Tenant(t.id), s.id, patch, None)
        .await
        .expect("patch every section");
    let _b = Broken::new(&p, "services");
    assert!(m
        .patch_service(TenantScope::Tenant(t.id), s.id, rt_types::ServicePatch::default(), None)
        .await
        .is_err());
    assert!(m.set_live(s.id, VersionNo(1), None).await.is_err());
    assert!(m.route_snapshot().await.is_err());
    assert!(m.add_version(s.id, new_version(77)).await.is_err());
}

#[tokio::test]
async fn version_operations_on_a_damaged_versions_table_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "versions").await;
    mk_versions(&m, s.id, 1, 2).await;
    {
        let _b = Broken::new(&p, "versions");
        assert!(m.add_version(s.id, new_version(9)).await.is_err());
        assert!(m
            .set_build_state(s.id, VersionNo(1), rt_types::BuildState::Ready, None, None)
            .await
            .is_err());
        assert!(m.count_builds_since(t.id, 0).await.is_err());
        assert!(m.set_bundle_digest(s.id, VersionNo(1), digest_n(3)).await.is_err());
    }
    assert_eq!(m.list_versions(s.id).await.expect("list").len(), 2);
}

#[tokio::test]
async fn sdk_operations_on_damaged_sdk_tables_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let svc = ensure_sdk_service(&m).await;
    m.upsert_sdk("ping", svc, None).await.expect("sdk");
    m.list_sdks().await.expect("list");
    let grant = SdkGrant {
        scope: GrantScope::Node,
        sdk: "ping".into(),
        effect: Some(GrantEffect::Deny),
        version_req: None,
        channel: None,
        created_by: "root".into(),
        updated_at_ms: 0,
    };
    m.put_sdk_grant(grant).await.expect("grant");
    assert!(m
        .list_sdk_grants()
        .await
        .expect("grants")
        .iter()
        .any(|g| g.effect == Some(GrantEffect::Deny)));

    {
        let _sdks = Broken::new(&p, "sdks");
        assert!(m.upsert_sdk("other", svc, None).await.is_err());
        assert!(m.list_sdks().await.is_err());
        assert!(m.remove_sdk("ping").await.is_err());
        assert!(m.route_snapshot().await.is_err());
    }
    {
        let _grants = Broken::new(&p, "sdk_grants");
        assert!(m
            .put_sdk_grant(SdkGrant {
                scope: GrantScope::Node,
                sdk: "ping".into(),
                effect: None,
                version_req: None,
                channel: None,
                created_by: "root".into(),
                updated_at_ms: 0,
            })
            .await
            .is_err());
    }
    {
        let _chan = Broken::new(&p, "sdk_channels");
        assert!(m.list_sdk_channels("ping").await.is_err());
        assert!(m.set_sdk_channel("ping", "stable", "1.0.0").await.is_err());
    }
    assert!(
        m.add_sdk_release(release_for("nope", "1.0.0")).await.is_err(),
        "an unknown sdk has no release"
    );
}

fn release_for(sdk: &str, version: &str) -> rt_types::NewSdkRelease {
    rt_types::NewSdkRelease {
        sdk: sdk.into(),
        version: version.into(),
        version_no: VersionNo(1),
        members: vec!["ping".into()],
        contract: "export {};".into(),
    }
}

async fn ensure_sdk_service(m: &SqliteMetaStore) -> ServiceId {
    m.ensure_tenant(rt_types::system_tenant(), "sys", SYSTEM_PLAN)
        .await
        .expect("system tenant");
    let mut s = new_service("sys-sdk");
    s.project = "default".into();
    let svc = m
        .create_service(
            TenantScope::Tenant(rt_types::system_tenant()),
            rt_types::system_tenant(),
            s,
        )
        .await
        .expect("sdk service");
    mk_versions(m, svc.id, 1, 1).await;
    svc.id
}

#[tokio::test]
async fn secrets_are_capped_and_a_damaged_table_fails_every_secret_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "secrets").await;
    m.put_secret(s.id, "ONE", vec![1]).await.expect("put");
    assert!(m
        .replace_secret_if(s.id, "ONE", vec![1], vec![2])
        .await
        .expect("replace"));
    assert!(!m
        .replace_secret_if(s.id, "ONE", vec![9], vec![3])
        .await
        .expect("stale expected value"));
    assert!(matches!(
        m.delete_secret(s.id, "MISSING").await,
        Err(StoreError::NotFound)
    ));
    {
        let _b = Broken::new(&p, "secrets");
        assert!(m.put_secret(s.id, "TWO", vec![1]).await.is_err());
        assert!(m.replace_secret_if(s.id, "ONE", vec![2], vec![3]).await.is_err());
        assert!(m.delete_secret(s.id, "ONE").await.is_err());
    }
}

#[tokio::test]
async fn hosts_claims_and_releases_on_a_damaged_table_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "hosts").await;
    assert!(
        m.put_host(pending_host("", s.id)).await.is_err(),
        "an empty host is refused"
    );
    m.put_host(pending_host("claim.example.test", s.id))
        .await
        .expect("claim");
    m.record_host_release(host_release("gone.example.test", t.id))
        .await
        .expect("release");
    m.prune_host_releases(now_ms() + 1).await.expect("prune");
    assert_eq!(
        m.count_pending_hosts(t.id).await.expect("count"),
        1,
        "one claim is pending"
    );
    {
        let _h = Broken::new(&p, "hosts");
        assert!(m.put_host(pending_host("two.example.test", s.id)).await.is_err());
        assert!(m.activate_host("claim.example.test", s.id, "tok").await.is_err());
        assert!(m.expire_pending_hosts(now_ms()).await.is_err());
        assert!(m.count_pending_hosts(t.id).await.is_err());
    }
    {
        let _r = Broken::new(&p, "host_releases");
        assert!(m
            .record_host_release(host_release("x.example.test", t.id))
            .await
            .is_err());
        assert!(m.host_release("gone.example.test", t.id).await.is_err());
        assert!(m.prune_host_releases(now_ms()).await.is_err());
    }
}

#[tokio::test]
async fn usage_events_and_lock_records_on_damaged_tables_are_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "usage").await;
    let batch = rt_types::UsageBatch {
        items: vec![(t.id, s.id, Default::default())],
    };
    m.add_usage("2026-01-01", batch.clone()).await.expect("usage");
    {
        let _u = Broken::new(&p, "usage");
        assert!(m.add_usage("2026-01-01", batch).await.is_err());
        assert!(m.get_usage(t.id, "2026-01-01", "2026-01-02").await.is_err());
    }
    {
        let _e = Broken::new(&p, "events");
        let ev = rt_types::NodeEvent {
            id: rt_types::EventId::from_u128(5),
            ts_ms: now_ms(),
            tenant: Some(t.id),
            service: Some(s.id),
            kind: "deploy".into(),
            data: serde_json::json!({}),
        };
        assert!(m.append_event(ev).await.is_err());
        assert!(m.list_events(s.id, PageReq::default()).await.is_err());
    }
    {
        let _l = Broken::new(&p, "versions");
        assert!(m.lock_for_source(&digest_n(1)).await.is_err());
    }
}

#[tokio::test]
async fn a_stored_lock_digest_that_is_not_a_digest_is_an_error_to_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "locked").await;
    mk_versions(&m, s.id, 1, 1).await;
    raw(&p)
        .execute_batch("UPDATE versions SET lock_digest = 'junk', build_state = 'ready'")
        .expect("a lock digest and a ready build");
    assert!(m.lock_for_source(&digest_n(1)).await.is_err());
}

/// Makes writes to `table` fail (reads still work): a trigger that raises for each row of `op`,
/// or only for the rows `when` selects. Dropped with the guard.
struct NoWrite {
    path: PathBuf,
    trigger: String,
}

impl NoWrite {
    fn new(path: &Path, table: &str, op: &str, when: Option<&str>) -> Self {
        let trigger = format!("rt_nowrite_{table}_{}", op.to_lowercase());
        let when = when.map(|w| format!(" WHEN {w}")).unwrap_or_default();
        raw(path)
            .execute_batch(&format!(
                "CREATE TRIGGER {trigger} BEFORE {op} ON {table}{when} BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;"
            ))
            .expect("install write failure");
        Self {
            path: path.to_path_buf(),
            trigger,
        }
    }
}

impl Drop for NoWrite {
    fn drop(&mut self) {
        let _ = raw(&self.path).execute_batch(&format!("DROP TRIGGER IF EXISTS {}", self.trigger));
    }
}

#[tokio::test]
async fn a_secret_count_at_the_cap_refuses_the_next_secret() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, _) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "capped").await;
    for i in 0..100 {
        m.put_secret(s.id, &format!("S{i}"), vec![1])
            .await
            .expect("within the cap");
    }
    let e = m.put_secret(s.id, "S100", vec![1]).await.expect_err("the cap is 100");
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
}

#[tokio::test]
async fn a_write_that_fails_after_its_read_is_an_error_for_each_write_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "writes").await;
    let pr = m
        .create_project(t.id, rt_types::NewProject::new("web"))
        .await
        .expect("project");
    mk_versions(&m, s.id, 1, 1).await;
    let svc = ensure_sdk_service(&m).await;
    m.upsert_sdk("ping", svc, None).await.expect("sdk");
    m.put_host(pending_host("ok.example.test", s.id)).await.expect("host");
    m.add_usage(
        "2026-01-01",
        rt_types::UsageBatch {
            items: vec![(t.id, s.id, Default::default())],
        },
    )
    .await
    .expect("usage");

    {
        let _w = NoWrite::new(&p, "tenants", "UPDATE", None);
        assert!(m
            .update_tenant(
                t.id,
                TenantPatch {
                    name: Some("x".into()),
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "tenants", "INSERT", None);
        assert!(m
            .ensure_tenant(rt_types::TenantId::from_u128(4242), "fresh", "free")
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "projects", "UPDATE", None);
        assert!(m
            .update_project(
                t.id,
                "web",
                rt_types::ProjectPatch {
                    name: Some("web2".into()),
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "project_environments", "DELETE", None);
        assert!(m
            .update_project(
                t.id,
                "web",
                rt_types::ProjectPatch {
                    environments: Some(vec!["staging".into()]),
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "services", "UPDATE", None);
        assert!(m
            .patch_service(
                TenantScope::Tenant(t.id),
                s.id,
                rt_types::ServicePatch {
                    durable: Some(true),
                    ..Default::default()
                },
                None
            )
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "versions", "INSERT", None);
        assert!(m.add_version(s.id, new_version(55)).await.is_err());
    }
    {
        let _w = NoWrite::new(&p, "versions", "UPDATE", Some("NEW.is_live = 1"));
        assert!(
            m.set_live(s.id, VersionNo(1), None).await.is_err(),
            "the new live flag is refused"
        );
    }
    {
        m.add_sdk_release(release_for("ping", "1.0.0"))
            .await
            .expect("a release to point the channel at");
        m.set_sdk_channel("ping", "stable", "1.0.0")
            .await
            .expect("the channel moves while writes work");
        let _w = NoWrite::new(&p, "sdk_channels", "INSERT", None);
        assert!(m.set_sdk_channel("ping", "stable", "1.0.0").await.is_err());
    }
    {
        let _w = NoWrite::new(&p, "sdk_grants", "INSERT", None);
        assert!(m
            .put_sdk_grant(SdkGrant {
                scope: GrantScope::Node,
                sdk: "ping".into(),
                effect: Some(GrantEffect::Allow),
                version_req: None,
                channel: None,
                created_by: "root".into(),
                updated_at_ms: 0,
            })
            .await
            .is_err());
    }
    {
        let _w = NoWrite::new(&p, "secrets", "INSERT", None);
        assert!(m.put_secret(s.id, "NEW", vec![1]).await.is_err());
    }
    {
        let _w = NoWrite::new(&p, "hosts", "INSERT", None);
        assert!(m.put_host(pending_host("two.example.test", s.id)).await.is_err());
    }
    {
        let _w = NoWrite::new(&p, "hosts", "DELETE", None);
        assert!(m.expire_pending_hosts(now_ms() + 1).await.is_err());
    }
    {
        let _w = NoWrite::new(&p, "usage", "INSERT", None);
        assert!(m
            .add_usage(
                "2026-01-02",
                rt_types::UsageBatch {
                    items: vec![(t.id, s.id, Default::default())]
                }
            )
            .await
            .is_err());
    }
    let _ = pr;
}

#[tokio::test]
async fn a_child_key_whose_parent_cannot_be_read_is_an_error_and_so_is_a_route_snapshot_without_releases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let parent = rt_types::KeyId::from_u128(1);
    let key = |n: u128| rt_types::NewKey {
        id: rt_types::KeyId::from_u128(n),
        name: None,
        scopes: rt_types::Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        hmac: vec![n as u8; 32],
    };
    m.create_key(t.id, key(1)).await.expect("parent");
    {
        let _b = Broken::new(&p, "api_keys");
        assert!(m.create_child_key(t.id, key(2), Some(parent)).await.is_err());
    }
    {
        let _b = Broken::new(&p, "sdk_releases");
        assert!(m.route_snapshot().await.is_err());
    }
}

#[tokio::test]
async fn gc_keeps_a_lock_that_a_surviving_version_still_uses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "gc-shared").await;
    mk_versions(&m, s.id, 1, 3).await;
    let lock = digest_n(100).to_string();
    raw(&p)
        .execute_batch(&format!(
            "UPDATE versions SET lock_digest = '{lock}' WHERE number IN (1, 3)"
        ))
        .expect("versions 1 and 3 share a lock");
    let freed = m.gc_versions(s.id, 1).await.expect("gc keeps the newest version");
    assert!(freed.contains(&digest_n(1)), "the doomed source is freed");
    assert!(!freed.contains(&digest_n(100)), "the lock is still used by version 3");
}

#[tokio::test]
async fn gc_cannot_list_doomed_versions_when_one_of_their_columns_is_gone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, p) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "gc-broken").await;
    mk_versions(&m, s.id, 1, 2).await;
    raw(&p)
        .execute_batch("DROP INDEX versions_lock_digest; ALTER TABLE versions DROP COLUMN lock_digest")
        .expect("drop a column only the listing reads");
    assert!(
        m.gc_versions(s.id, 1).await.is_err(),
        "the listing fails after the live lookup succeeded"
    );
    raw(&p)
        .execute_batch("ALTER TABLE versions ADD COLUMN lock_digest TEXT; CREATE INDEX versions_lock_digest ON versions(lock_digest)")
        .expect("restore");
}

#[tokio::test]
async fn a_store_opened_without_a_directory_part_is_an_anonymous_database() {
    let m = SqliteMetaStore::open(Path::new("")).expect("anonymous database");
    m.create_tenant(new_tenant("anon"))
        .await
        .expect("the writer connection has the schema");
}

#[tokio::test]
async fn a_node_event_is_recorded_and_listed_for_its_service() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (m, _) = file_meta(dir.path());
    let t = mk_tenant(&m, "acme").await;
    let s = mk_service(&m, &t, "events").await;
    let ev = rt_types::NodeEvent {
        id: rt_types::EventId::from_u128(77),
        ts_ms: now_ms(),
        tenant: Some(t.id),
        service: Some(s.id),
        kind: "deploy".into(),
        data: serde_json::json!({ "version": 1 }),
    };
    m.append_event(ev).await.expect("append");
    let found = m
        .event_by_id(rt_types::EventId::from_u128(77))
        .await
        .expect("find")
        .expect("recorded");
    assert_eq!(found.kind, "deploy");
    assert_eq!(
        m.list_events(s.id, PageReq::default()).await.expect("list").items.len(),
        1
    );
}
