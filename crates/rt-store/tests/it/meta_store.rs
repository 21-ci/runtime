//! rt-store `meta.db` tests (contracts C-2, C-9, C-10): CRUD with tenant
//! scoping, unique slugs, versions and build state, the live pointer with `If-Match`, env replace
//! semantics, opaque secrets, hosts, schedules, usage, events and cache frequencies.

use crate::common;

use std::collections::HashSet;

use common::*;
use rt_sec::{secret_aad, LocalSecrets, MasterKey, Sealed, SecretsBackend};
use rt_store::{
    MetaStore, PageReq, ServiceFilter, SqliteMetaStore, StoreError, TenantPatch, TenantScope, META_MIGRATIONS,
};
use rt_types::{
    Access, BuildFailReason, BuildFailure, BuildState, CompileMode, Diagnostic, Etag, EventId, HostMapping, HostStatus,
    KeyId, LimitsConfig, NewKey, NewSchedule, NewVersion, NodeEvent, Profile, ScheduleKind, Scope, Scopes,
    SecretsVersion, ServiceConfig, ServicePatch, UsageBatch, UsageDelta, VersionNo,
};
use serde_json::json;

/// The service with this slug, from any tenant (slugs are global).
async fn by_slug(m: &SqliteMetaStore, slug: &str) -> Option<rt_types::Service> {
    let filter = ServiceFilter {
        slug: Some(slug.into()),
        ..ServiceFilter::default()
    };
    m.list_services(TenantScope::Operator, filter, PageReq::default())
        .await
        .expect("list")
        .items
        .pop()
}

// ---- schema / file ---------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_01_schema_version_matches_newest_embedded_migration() {
    let m = meta();
    let newest = META_MIGRATIONS
        .iter()
        .map(|(v, _)| *v)
        .max()
        .expect("at least one embedded migration");
    assert_eq!(m.schema_version().await.expect("schema version"), newest);
}

#[tokio::test]
async fn ac_meta_02_file_store_persists_across_reopen_and_runs_in_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("meta.db");

    let (tenant, service) = {
        let m = SqliteMetaStore::open(&path).expect("open meta.db");
        let t = mk_tenant(&m, "acme").await;
        let s = mk_service(&m, &t, "persisted").await;
        mk_versions(&m, s.id, 1, 2).await;
        m.save_cache_freq(vec![9, 8, 7]).await.expect("save freq");
        (t, s)
    };
    assert!(path.exists(), "meta.db must be created at the given path");

    let m = SqliteMetaStore::open(&path).expect("reopen meta.db");
    assert_eq!(m.get_tenant(tenant.id).await.expect("get tenant"), Some(tenant.clone()));
    assert_eq!(
        m.get_service(TenantScope::Operator, service.id)
            .await
            .expect("get service"),
        Some(service.clone())
    );
    assert_eq!(m.list_versions(service.id).await.expect("versions").len(), 2);
    assert_eq!(m.load_cache_freq().await.expect("load freq"), Some(vec![9, 8, 7]));
    assert_eq!(
        m.schema_version().await.expect("schema version"),
        META_MIGRATIONS.last().expect("migration").0
    );

    // WAL is a persistent property of the database file.
    let conn = rusqlite::Connection::open(&path).expect("second connection");
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("journal_mode");
    assert_eq!(mode.to_lowercase(), "wal");
}

/// B2.2: a file-backed store reads on read-only connections next to the writer. Concurrent writes
/// and reads all succeed (no `SQLITE_BUSY` reaches callers) and a read after a write sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_and_writes_on_a_file_store_all_succeed_and_read_their_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let m = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("open meta.db");
    let t = mk_tenant(&m, "t").await;
    let mut handles = Vec::new();
    for i in 0..32 {
        let m = m.clone();
        handles.push(tokio::spawn(async move {
            let s = m
                .create_service(TenantScope::Tenant(t.id), t.id, new_service(&format!("svc-{i}")))
                .await
                .expect("create");
            // Read-your-writes across the writer and reader connections.
            assert_eq!(
                m.get_service(TenantScope::Operator, s.id).await.expect("get"),
                Some(s.clone())
            );
            for _ in 0..20 {
                m.list_services(TenantScope::Tenant(t.id), ServiceFilter::default(), PageReq::default())
                    .await
                    .expect("list");
                m.route_snapshot().await.expect("snapshot");
            }
            m.add_version(s.id, new_version(i)).await.expect("version");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    assert_eq!(m.count_services(t.id).await.expect("count"), 32);
}

#[tokio::test]
async fn ac_meta_03_in_memory_stores_are_independent() {
    let a = meta();
    let b = meta();
    let t = mk_tenant(&a, "only-in-a").await;
    assert!(a.get_tenant(t.id).await.expect("get").is_some());
    assert!(b.get_tenant(t.id).await.expect("get").is_none());
    assert!(b.list_tenants(PageReq::default()).await.expect("list").items.is_empty());
}

// ---- tenants ---------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_04_tenant_create_get_update_delete() {
    let m = meta();
    let created = m.create_tenant(new_tenant("acme")).await.expect("create");
    assert_eq!(created.name, "acme");
    assert_eq!(created.plan, "free");
    assert_eq!(created.overrides, serde_json::Value::Null);
    assert!(!created.disabled);
    assert!(
        created.created_at_ms > 1_600_000_000_000,
        "created_at_ms must be a unix-ms timestamp"
    );

    assert_eq!(m.get_tenant(created.id).await.expect("get"), Some(created.clone()));
    assert_eq!(
        m.get_tenant(rt_types::TenantId::from_u128(404)).await.expect("get"),
        None
    );

    // Partial patch changes only the given fields.
    let patched = m
        .update_tenant(
            created.id,
            TenantPatch {
                name: Some("acme2".into()),
                disabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    assert_eq!(patched.name, "acme2");
    assert!(patched.disabled);
    assert_eq!(patched.plan, "free", "unpatched fields keep their value");
    assert_eq!(patched.created_at_ms, created.created_at_ms);

    let patched = m
        .update_tenant(
            created.id,
            TenantPatch {
                plan: Some("pro".into()),
                overrides: Some(json!({"max_services": 3})),
                ..Default::default()
            },
        )
        .await
        .expect("update plan");
    assert_eq!(patched.plan, "pro");
    assert_eq!(patched.overrides, json!({"max_services": 3}));
    assert_eq!(patched.name, "acme2");
    assert_eq!(m.get_tenant(created.id).await.expect("get"), Some(patched));

    let err = m
        .update_tenant(rt_types::TenantId::from_u128(404), TenantPatch::default())
        .await
        .expect_err("unknown tenant");
    assert!(matches!(err, StoreError::NotFound));

    m.delete_tenant(created.id, false).await.expect("delete");
    assert_eq!(m.get_tenant(created.id).await.expect("get"), None);
}

#[tokio::test]
async fn ac_meta_05_tenant_list_paginates_without_duplicates() {
    let m = meta();
    let mut expected = HashSet::new();
    for i in 0..5 {
        expected.insert(mk_tenant(&m, &format!("t{i}")).await.id);
    }

    let mut seen = HashSet::new();
    let mut pages = 0;
    let mut cursor = None;
    loop {
        let page = m
            .list_tenants(PageReq {
                limit: 2,
                cursor: cursor.clone(),
            })
            .await
            .expect("list page");
        assert!(page.items.len() <= 2, "page must respect the limit");
        pages += 1;
        for t in &page.items {
            assert!(seen.insert(t.id), "tenant {} listed twice", t.id);
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }
    assert_eq!(seen, expected);
    assert_eq!(pages, 3, "5 tenants at limit 2 = pages of 2, 2, 1");
}

#[tokio::test]
async fn ac_meta_06_tenant_purge_removes_services_keys_and_frees_slugs() {
    let m = meta();
    let t = mk_tenant(&m, "doomed").await;
    let s = mk_service(&m, &t, "doomed-svc").await;
    mk_versions(&m, s.id, 1, 1).await;
    let key_id = KeyId::from_u128(1);
    m.create_key(
        t.id,
        NewKey {
            id: key_id,
            name: None,
            scopes: Scopes::all(),
            project_ids: None,
            environments: None,
            expires_at_ms: None,
            hmac: vec![1; 32],
        },
    )
    .await
    .expect("create key");

    m.delete_tenant(t.id, true).await.expect("purge");

    assert_eq!(m.get_tenant(t.id).await.expect("get"), None);
    assert_eq!(
        m.get_service(TenantScope::Operator, s.id).await.expect("get service"),
        None
    );
    assert!(m.list_keys(t.id).await.expect("list keys").is_empty());
    assert_eq!(m.find_key(key_id).await.expect("find key"), None);
    assert!(m.list_versions(s.id).await.expect("versions").is_empty());

    // The slug is free again.
    let t2 = mk_tenant(&m, "heir").await;
    mk_service(&m, &t2, "doomed-svc").await;
}

// ---- api keys --------------------------------------------------------------------------------

#[tokio::test]
async fn revoke_key_tree_revokes_every_descendant_in_one_go() {
    let m = meta();
    let t = mk_tenant(&m, "tree").await;
    let nk = |n: u128| NewKey {
        id: KeyId::from_u128(n),
        name: None,
        scopes: Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        hmac: vec![1; 32],
    };
    let root = m.create_key(t.id, nk(1)).await.expect("root");
    assert_eq!(root.parent_key_id, None);
    let child = m.create_child_key(t.id, nk(2), Some(root.id)).await.expect("child");
    assert_eq!(child.parent_key_id, Some(root.id));
    m.create_child_key(t.id, nk(3), Some(child.id))
        .await
        .expect("grandchild");
    m.create_key(t.id, nk(4)).await.expect("unrelated");
    assert_eq!(
        m.find_key(KeyId::from_u128(3))
            .await
            .expect("find")
            .expect("k")
            .parent_key_id,
        Some(child.id)
    );
    assert_eq!(m.count_live_keys(t.id, 0).await.expect("count"), 4);

    let mut gone = m.revoke_key_tree(t.id, root.id).await.expect("revoke");
    assert_eq!(gone[0], root.id, "the root comes first");
    gone.sort_by_key(ToString::to_string);
    assert_eq!(
        gone,
        vec![KeyId::from_u128(1), KeyId::from_u128(2), KeyId::from_u128(3)]
    );
    assert_eq!(m.count_live_keys(t.id, 0).await.expect("count"), 1);
    assert!(
        m.revoke_key_tree(t.id, root.id).await.expect("again").is_empty(),
        "already revoked"
    );

    let other = mk_tenant(&m, "other").await;
    assert!(m
        .revoke_key_tree(other.id, KeyId::from_u128(4))
        .await
        .expect("cross")
        .is_empty());
    let p = m
        .get_project(t.id, "default")
        .await
        .expect("project")
        .expect("default project")
        .id;
    assert_eq!(m.tenant_projects_among(t.id, &[p]).await.expect("among"), vec![p]);
    assert!(m.tenant_projects_among(other.id, &[p]).await.expect("among").is_empty());
}

#[tokio::test]
async fn ac_meta_07_api_key_roundtrip_revoke_touch_and_tenant_scoping() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    mk_service(&m, &a, "keyed").await;

    let id = KeyId::from_u128(7);
    let new = NewKey {
        id,
        name: Some("ci".into()),
        scopes: [Scope::Deploy, Scope::Invoke].into_iter().collect(),
        project_ids: Some(vec![
            m.get_project(a.id, "default")
                .await
                .expect("project")
                .expect("default")
                .id,
        ]),
        environments: Some(vec!["production".into()]),
        expires_at_ms: Some(4_000_000_000_000),
        hmac: (0u8..32).collect(),
    };
    let key = m.create_key(a.id, new.clone()).await.expect("create key");
    assert_eq!(key.id, id);
    assert_eq!(key.tenant_id, a.id);
    assert_eq!(key.name.as_deref(), Some("ci"));
    assert_eq!(key.scopes, new.scopes);
    assert_eq!(key.project_ids, new.project_ids);
    assert_eq!(key.environments, Some(vec!["production".to_string()]));
    assert_eq!(key.expires_at_ms, Some(4_000_000_000_000));
    assert_eq!(
        key.hmac, new.hmac,
        "the keyed hash is stored verbatim for authentication"
    );
    assert!(!key.revoked);
    assert_eq!(key.last_used_at_ms, None);
    assert!(key.created_at_ms > 0);

    assert_eq!(m.find_key(id).await.expect("find"), Some(key.clone()));
    assert_eq!(m.list_keys(a.id).await.expect("list a").len(), 1);
    assert!(
        m.list_keys(b.id).await.expect("list b").is_empty(),
        "keys are listed per tenant"
    );

    m.touch_key(id, 1_234_567).await.expect("touch");
    assert_eq!(
        m.find_key(id).await.expect("find").expect("exists").last_used_at_ms,
        Some(1_234_567)
    );

    assert!(
        !m.revoke_key(b.id, id).await.expect("revoke as wrong tenant"),
        "another tenant cannot revoke"
    );
    assert!(!m.find_key(id).await.expect("find").expect("exists").revoked);
    assert!(m.revoke_key(a.id, id).await.expect("revoke"));
    assert!(m.find_key(id).await.expect("find").expect("exists").revoked);
    assert!(!m.revoke_key(a.id, KeyId::from_u128(999)).await.expect("revoke unknown"));
}

// ---- services --------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_08_service_roundtrip_and_unique_slug() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;

    let mut ns = new_service("api");
    ns.name = Some("My API".into());
    ns.profile = Some(Profile::MinLatency);
    ns.config = ServiceConfig {
        access: Access::Key,
        limits: LimitsConfig {
            wall_ms: Some(5000),
            ..Default::default()
        },
        ..Default::default()
    };
    let created = m
        .create_service(TenantScope::Tenant(a.id), a.id, ns.clone())
        .await
        .expect("create");
    assert_eq!(created.tenant_id, a.id);
    assert_eq!(created.slug, "api");
    assert_eq!(created.name.as_deref(), Some("My API"));
    assert_eq!(created.profile, Some(Profile::MinLatency));
    assert_eq!(created.config, ns.config);
    assert_eq!(created.live_version, None);
    assert_eq!(created.etag, Etag(1), "a new service starts at ETag 1");
    assert!(created.created_at_ms > 0);

    assert_eq!(
        m.get_service(TenantScope::Tenant(a.id), created.id).await.expect("get"),
        Some(created.clone())
    );
    assert_eq!(by_slug(&m, "api").await, Some(created.clone()));
    assert_eq!(by_slug(&m, "nope").await, None);

    // Slugs are unique node-wide: same tenant and other tenants both conflict.
    for (tenant, who) in [(&a, "same tenant"), (&b, "other tenant")] {
        let err = m
            .create_service(TenantScope::Tenant(tenant.id), tenant.id, new_service("api"))
            .await
            .expect_err("duplicate slug must be rejected");
        assert!(matches!(err, StoreError::Conflict(_)), "{who}: got {err:?}");
        assert_eq!(err.code(), "slug_taken", "{who}");
    }
    assert_eq!(m.count_services(a.id).await.expect("count"), 1);
}

#[tokio::test]
async fn ac_meta_09_list_services_slug_filter_scope_and_pagination() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    let mut a_ids = HashSet::new();
    for slug in ["alpha", "beta", "gamma"] {
        a_ids.insert(mk_service(&m, &a, slug).await.id);
    }
    let b_svc = mk_service(&m, &b, "delta").await;

    // C-9: slug filter returns 0 or 1 items.
    let hit = m
        .list_services(
            TenantScope::Operator,
            ServiceFilter {
                slug: Some("beta".into()),
                ..Default::default()
            },
            PageReq::default(),
        )
        .await
        .expect("filtered list");
    assert_eq!(hit.items.len(), 1);
    assert_eq!(hit.items[0].slug, "beta");
    assert_eq!(hit.next_cursor, None);

    let miss = m
        .list_services(
            TenantScope::Operator,
            ServiceFilter {
                slug: Some("missing".into()),
                ..Default::default()
            },
            PageReq::default(),
        )
        .await
        .expect("filtered list");
    assert!(miss.items.is_empty());
    assert_eq!(miss.next_cursor, None);

    // A tenant never sees (or filters to) another tenant's slug.
    let cross = m
        .list_services(
            TenantScope::Tenant(b.id),
            ServiceFilter {
                slug: Some("alpha".into()),
                ..Default::default()
            },
            PageReq::default(),
        )
        .await
        .expect("cross-tenant filtered list");
    assert!(cross.items.is_empty());

    let only_b = m
        .list_services(TenantScope::Tenant(b.id), ServiceFilter::default(), PageReq::default())
        .await
        .expect("list b");
    assert_eq!(only_b.items.iter().map(|s| s.id).collect::<Vec<_>>(), vec![b_svc.id]);

    let all = m
        .list_services(TenantScope::Operator, ServiceFilter::default(), PageReq::default())
        .await
        .expect("list all");
    assert_eq!(all.items.len(), 4);

    // Pagination over tenant A's services.
    let mut seen = HashSet::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = m
            .list_services(
                TenantScope::Tenant(a.id),
                ServiceFilter::default(),
                PageReq {
                    limit: 2,
                    cursor: cursor.clone(),
                },
            )
            .await
            .expect("page");
        assert!(page.items.len() <= 2);
        pages += 1;
        for s in &page.items {
            assert!(seen.insert(s.id), "service listed twice");
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(pages < 10);
    }
    assert_eq!(seen, a_ids);
    assert_eq!(pages, 2);
}

#[tokio::test]
async fn ac_meta_10_cross_tenant_access_behaves_as_not_found() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    let s = mk_service(&m, &a, "private").await;

    assert_eq!(
        m.get_service(TenantScope::Tenant(b.id), s.id).await.expect("get as b"),
        None
    );
    assert!(m
        .get_service(TenantScope::Tenant(a.id), s.id)
        .await
        .expect("get as a")
        .is_some());
    assert!(m
        .get_service(TenantScope::Operator, s.id)
        .await
        .expect("get as operator")
        .is_some());

    let err = m
        .patch_service(
            TenantScope::Tenant(b.id),
            s.id,
            ServicePatch {
                name: Some("hijack".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("patch as other tenant");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    let err = m
        .delete_service(TenantScope::Tenant(b.id), s.id, false)
        .await
        .expect_err("delete as other tenant");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");

    // Nothing changed.
    let still = m
        .get_service(TenantScope::Operator, s.id)
        .await
        .expect("get")
        .expect("still there");
    assert_eq!(still, s);
    assert_eq!(m.count_services(a.id).await.expect("count"), 1);
    assert_eq!(m.count_services(b.id).await.expect("count"), 0);

    // A tenant scope cannot create into another tenant.
    assert!(m
        .create_service(TenantScope::Tenant(b.id), a.id, new_service("sneaky"))
        .await
        .is_err());
    assert_eq!(by_slug(&m, "sneaky").await, None);
}

#[tokio::test]
async fn ac_meta_11_patch_service_replaces_blocks_and_bumps_etag() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let mut ns = new_service("patchy");
    ns.config.access = Access::Key;
    let s = m
        .create_service(TenantScope::Tenant(t.id), t.id, ns)
        .await
        .expect("create");

    let patched = m
        .patch_service(
            TenantScope::Tenant(t.id),
            s.id,
            ServicePatch {
                name: Some("renamed".into()),
                limits: Some(LimitsConfig {
                    wall_ms: Some(1000),
                    cpu_ms: Some(50),
                    ..Default::default()
                }),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("patch");
    assert_eq!(patched.name.as_deref(), Some("renamed"));
    assert_eq!(patched.config.limits.wall_ms, Some(1000));
    assert_eq!(patched.config.limits.cpu_ms, Some(50));
    assert_eq!(
        patched.config.access,
        Access::Key,
        "blocks not in the patch are untouched"
    );
    assert!(patched.etag > s.etag, "every write bumps the ETag");
    assert_eq!(
        m.get_service(TenantScope::Operator, s.id).await.expect("get"),
        Some(patched.clone())
    );

    // A block present in a later patch replaces that whole block.
    let patched2 = m
        .patch_service(
            TenantScope::Tenant(t.id),
            s.id,
            ServicePatch {
                limits: Some(LimitsConfig {
                    memory_mb: Some(64),
                    ..Default::default()
                }),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("patch 2");
    assert_eq!(
        patched2.config.limits,
        LimitsConfig {
            memory_mb: Some(64),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn ac_meta_12_patch_service_if_match_mismatch_is_precondition_failed() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "etagged").await;
    let scope = TenantScope::Tenant(t.id);

    let ok = m
        .patch_service(
            scope,
            s.id,
            ServicePatch {
                name: Some("v2".into()),
                ..Default::default()
            },
            Some(s.etag),
        )
        .await
        .expect("matching If-Match succeeds");
    assert!(ok.etag > s.etag);

    // Using the now-stale ETag fails and changes nothing.
    let err = m
        .patch_service(
            scope,
            s.id,
            ServicePatch {
                name: Some("v3".into()),
                ..Default::default()
            },
            Some(s.etag),
        )
        .await
        .expect_err("stale If-Match");
    assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");
    assert_eq!(err.code(), "precondition_failed");
    let current = m.get_service(scope, s.id).await.expect("get").expect("exists");
    assert_eq!(current.name.as_deref(), Some("v2"));
    assert_eq!(current.etag, ok.etag);

    let err = m
        .patch_service(scope, s.id, ServicePatch::default(), Some(Etag(ok.etag.0 + 100)))
        .await
        .expect_err("future ETag");
    assert!(matches!(err, StoreError::PreconditionFailed));
}

#[tokio::test]
async fn ac_meta_13_delete_service_cascades_to_everything_it_owns() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let doomed = mk_service(&m, &t, "doomed").await;
    let survivor = mk_service(&m, &t, "survivor").await;
    for s in [&doomed, &survivor] {
        mk_versions(&m, s.id, 1, 1).await;
        m.put_env(s.id, vec![("A".into(), "1".into())]).await.expect("env");
        m.put_secret(s.id, "S", vec![0xAB; 40]).await.expect("secret");
        m.create_schedule(
            s.id,
            NewSchedule {
                kind: ScheduleKind::Interval { every_seconds: 60 },
                payload: json!(null),
                enabled: true,
            },
        )
        .await
        .expect("schedule");
    }
    m.put_host(HostMapping {
        host: "doomed.example.com".into(),
        service_id: doomed.id,
        status: HostStatus::Active,
        verify_token: None,
        created_at_ms: 0,
    })
    .await
    .expect("host");

    m.delete_service(TenantScope::Tenant(t.id), doomed.id, false)
        .await
        .expect("delete");

    assert_eq!(
        m.get_service(TenantScope::Operator, doomed.id).await.expect("get"),
        None
    );
    assert_eq!(by_slug(&m, "doomed").await, None);
    assert!(m.list_versions(doomed.id).await.expect("versions").is_empty());
    assert!(m.get_env(doomed.id).await.expect("env").is_empty());
    assert!(m.list_secrets(doomed.id).await.expect("secrets").is_empty());
    assert!(m.list_schedules(doomed.id).await.expect("schedules").is_empty());
    assert!(m.list_hosts(doomed.id).await.expect("hosts").is_empty());
    assert_eq!(m.find_host("doomed.example.com").await.expect("find host"), None);
    assert_eq!(
        m.all_schedules().await.expect("all schedules").len(),
        1,
        "only the survivor's schedule remains"
    );

    // The other service is untouched.
    assert_eq!(m.list_versions(survivor.id).await.expect("versions").len(), 1);
    assert_eq!(m.get_env(survivor.id).await.expect("env").len(), 1);
    assert_eq!(m.list_secrets(survivor.id).await.expect("secrets").len(), 1);
    // Slug is reusable.
    mk_service(&m, &t, "doomed").await;
}

// ---- versions and build state ----------------------------------------------------------------

#[tokio::test]
async fn ac_meta_14_versions_are_numbered_per_service_and_start_in_the_right_state() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;

    let v1 = m.add_version(a.id, new_version(1)).await.expect("v1");
    let v2 = m.add_version(a.id, new_version(2)).await.expect("v2");
    let eager = m
        .add_version(
            a.id,
            NewVersion {
                source_digest: digest_n(3),
                compile: CompileMode::Eager,
            },
        )
        .await
        .expect("eager");
    let b1 = m.add_version(b.id, new_version(1)).await.expect("b1");

    assert_eq!(
        (v1.number, v2.number, eager.number),
        (VersionNo(1), VersionNo(2), VersionNo(3))
    );
    assert_eq!(b1.number, VersionNo(1), "numbering is per service");
    assert_eq!(v1.service_id, a.id);
    assert_eq!(v1.source_digest, digest_n(1));
    assert_eq!(
        v1.build_state,
        BuildState::Pending,
        "lazy versions start pending (C-10)"
    );
    assert_eq!(
        eager.build_state,
        BuildState::Building,
        "eager versions start building (C-10)"
    );
    assert_eq!(v1.lock_digest, None);
    assert_eq!(v1.failure, None);

    assert_eq!(m.get_version(a.id, VersionNo(2)).await.expect("get"), Some(v2.clone()));
    assert_eq!(m.get_version(a.id, VersionNo(9)).await.expect("get"), None);
    let listed = m.list_versions(a.id).await.expect("list");
    assert_eq!(
        listed.iter().map(|v| v.number.0).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "ascending by number"
    );

    let err = m
        .add_version(svc(404), new_version(9))
        .await
        .expect_err("unknown service");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn ac_meta_15_set_build_state_records_lock_digest_and_failure() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "builds").await;
    let vs = mk_versions(&m, s.id, 1, 2).await;

    m.set_build_state(s.id, vs[0].number, BuildState::Building, None, None)
        .await
        .expect("building");
    assert_eq!(
        m.get_version(s.id, vs[0].number)
            .await
            .expect("get")
            .expect("exists")
            .build_state,
        BuildState::Building
    );

    m.set_build_state(s.id, vs[0].number, BuildState::Ready, None, Some(digest_n(100)))
        .await
        .expect("ready");
    let ready = m.get_version(s.id, vs[0].number).await.expect("get").expect("exists");
    assert_eq!(ready.build_state, BuildState::Ready);
    assert_eq!(ready.lock_digest, Some(digest_n(100)));

    let failure = BuildFailure {
        reason: BuildFailReason::Syntax,
        message: "unexpected token".into(),
        diagnostics: vec![Diagnostic {
            file: "main.ts".into(),
            line: 3,
            col: 9,
            message: "expected `)`".into(),
        }],
    };
    m.set_build_state(s.id, vs[1].number, BuildState::Failed, Some(failure.clone()), None)
        .await
        .expect("failed");
    let failed = m.get_version(s.id, vs[1].number).await.expect("get").expect("exists");
    assert_eq!(failed.build_state, BuildState::Failed);
    assert_eq!(failed.failure, Some(failure));
    // The sibling version is unaffected.
    assert_eq!(
        m.get_version(s.id, vs[0].number)
            .await
            .expect("get")
            .expect("exists")
            .failure,
        None
    );

    let err = m
        .set_build_state(s.id, VersionNo(77), BuildState::Ready, None, None)
        .await
        .expect_err("unknown version");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn ac_meta_16_count_builds_since_counts_only_the_tenants_started_builds() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    let sa = mk_service(&m, &a, "sa").await;
    let sb = mk_service(&m, &b, "sb").await;
    for n in 1..=3 {
        m.add_version(
            sa.id,
            NewVersion {
                source_digest: digest_n(n),
                compile: CompileMode::Eager,
            },
        )
        .await
        .expect("eager a");
    }
    m.add_version(
        sb.id,
        NewVersion {
            source_digest: digest_n(9),
            compile: CompileMode::Eager,
        },
    )
    .await
    .expect("eager b");

    let an_hour_ago = now_ms() - 3_600_000;
    assert_eq!(m.count_builds_since(a.id, an_hour_ago).await.expect("count a"), 3);
    assert_eq!(m.count_builds_since(b.id, an_hour_ago).await.expect("count b"), 1);
    assert_eq!(
        m.count_builds_since(a.id, now_ms() + 3_600_000)
            .await
            .expect("count future"),
        0
    );
}

// ---- live pointer ----------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_17_set_live_moves_pointer_and_enforces_if_match() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s0 = mk_service(&m, &t, "live").await;
    let vs = mk_versions(&m, s0.id, 1, 3).await;

    let s1 = m.set_live(s0.id, vs[0].number, None).await.expect("first live");
    assert_eq!(s1.live_version, Some(VersionNo(1)));
    assert!(s1.etag > s0.etag, "the live pointer move bumps the ETag");

    let s2 = m
        .set_live(s0.id, vs[1].number, Some(s1.etag))
        .await
        .expect("matching If-Match");
    assert_eq!(s2.live_version, Some(VersionNo(2)));
    assert!(s2.etag > s1.etag);

    // Stale ETag: rejected, pointer unchanged.
    let err = m
        .set_live(s0.id, vs[2].number, Some(s1.etag))
        .await
        .expect_err("stale If-Match");
    assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");
    assert_eq!(err.code(), "precondition_failed");
    let now = m
        .get_service(TenantScope::Operator, s0.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(now.live_version, Some(VersionNo(2)));
    assert_eq!(now.etag, s2.etag);

    // Rollback to an older version is a normal set_live.
    let back = m.set_live(s0.id, vs[0].number, None).await.expect("rollback");
    assert_eq!(back.live_version, Some(VersionNo(1)));

    let err = m
        .set_live(s0.id, VersionNo(42), None)
        .await
        .expect_err("unknown version");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn ac_meta_18_set_live_rejects_failed_versions_but_accepts_pending() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "ready-check").await;
    let vs = mk_versions(&m, s.id, 1, 2).await;
    m.set_build_state(
        s.id,
        vs[1].number,
        BuildState::Failed,
        Some(BuildFailure::new(BuildFailReason::Resolve, "module not found")),
        None,
    )
    .await
    .expect("mark failed");

    let err = m.set_live(s.id, vs[1].number, None).await.expect_err("failed version");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(err.code(), "version_not_ready");
    assert_eq!(
        m.get_service(TenantScope::Operator, s.id)
            .await
            .expect("get")
            .expect("exists")
            .live_version,
        None
    );

    // Pending is accepted (lazy compile on first request).
    assert_eq!(vs[0].build_state, BuildState::Pending);
    let live = m
        .set_live(s.id, vs[0].number, None)
        .await
        .expect("pending is live-able");
    assert_eq!(live.live_version, Some(vs[0].number));
}

#[tokio::test]
async fn ac_meta_19_route_snapshot_lists_live_services_with_active_hosts() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let live = mk_service(&m, &t, "live").await;
    let dark = mk_service(&m, &t, "dark").await; // no live version
    let other = mk_service(&m, &t, "other").await;
    let lv = mk_versions(&m, live.id, 1, 2).await;
    mk_versions(&m, dark.id, 10, 1).await;
    let ov = mk_versions(&m, other.id, 20, 1).await;
    m.set_live(live.id, lv[1].number, None).await.expect("live v2");
    m.set_live(other.id, ov[0].number, None).await.expect("other live");

    m.put_host(HostMapping {
        host: "ok.example.com".into(),
        service_id: live.id,
        status: HostStatus::Active,
        verify_token: None,
        created_at_ms: 0,
    })
    .await
    .expect("active host");
    m.put_host(HostMapping {
        host: "pending.example.com".into(),
        service_id: live.id,
        status: HostStatus::PendingVerification,
        verify_token: Some("tok".into()),
        created_at_ms: 0,
    })
    .await
    .expect("pending host");

    let snap = m.route_snapshot().await.expect("snapshot");
    let slugs: HashSet<_> = snap.entries.iter().map(|e| e.slug.as_str()).collect();
    assert_eq!(
        slugs,
        HashSet::from(["live", "other"]),
        "services without a live version are not routable"
    );

    let e = snap.entries.iter().find(|e| e.slug == "live").expect("live entry");
    assert_eq!(e.tenant, t.id);
    assert_eq!(e.service, live.id);
    assert_eq!(e.version, VersionNo(2));
    assert_eq!(e.digest, lv[1].source_digest);
    assert_eq!(e.runtime, rt_types::RuntimeKind::Deno);
    assert_eq!(
        e.hosts,
        vec!["ok.example.com".to_string()],
        "only verified hosts are routable"
    );

    // Moving the pointer changes the entry; deleting a service removes it.
    m.set_live(live.id, lv[0].number, None).await.expect("rollback");
    let snap = m.route_snapshot().await.expect("snapshot");
    let e = snap.entries.iter().find(|e| e.slug == "live").expect("live entry");
    assert_eq!((e.version, e.digest), (VersionNo(1), lv[0].source_digest));

    m.delete_service(TenantScope::Operator, other.id, false)
        .await
        .expect("delete");
    assert_eq!(m.route_snapshot().await.expect("snapshot").entries.len(), 1);
}

// ---- env, secrets ----------------------------------------------------------------------------

fn sorted(mut v: Vec<(String, String)>) -> Vec<(String, String)> {
    v.sort();
    v
}

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[tokio::test]
async fn ac_meta_20_put_env_replaces_the_whole_map() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "envy").await;
    let other = mk_service(&m, &t, "other").await;

    assert!(m.get_env(s.id).await.expect("empty env").is_empty());

    m.put_env(s.id, env(&[("A", "1"), ("B", "2")])).await.expect("put 1");
    assert_eq!(
        sorted(m.get_env(s.id).await.expect("get")),
        env(&[("A", "1"), ("B", "2")])
    );

    // STO-11: A=1 -> A=2 is visible to the next read; B is gone (replace, not merge).
    m.put_env(s.id, env(&[("A", "2")])).await.expect("put 2");
    assert_eq!(m.get_env(s.id).await.expect("get"), env(&[("A", "2")]));

    m.put_env(other.id, env(&[("Z", "9")])).await.expect("other service");
    assert_eq!(
        m.get_env(s.id).await.expect("get"),
        env(&[("A", "2")]),
        "services do not share env"
    );
    assert_eq!(m.get_env(other.id).await.expect("get"), env(&[("Z", "9")]));

    m.put_env(s.id, Vec::new()).await.expect("clear");
    assert!(m.get_env(s.id).await.expect("get").is_empty());
}

#[tokio::test]
async fn ac_meta_21_env_and_secret_writes_bump_one_generation_counter() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "gen").await;
    let other = mk_service(&m, &t, "gen-other").await;

    let g0 = m.secrets_version(s.id).await.expect("initial version");
    let g1 = m.put_env(s.id, env(&[("A", "1")])).await.expect("env");
    let g2 = m.put_secret(s.id, "TOKEN", vec![1; 40]).await.expect("secret");
    let g3 = m.put_env(s.id, env(&[("A", "2")])).await.expect("env again");
    let g4 = m.delete_secret(s.id, "TOKEN").await.expect("delete secret");
    assert!(
        g0 < g1 && g1 < g2 && g2 < g3 && g3 < g4,
        "generation must strictly increase: {g0:?} {g1:?} {g2:?} {g3:?} {g4:?}"
    );
    assert_eq!(m.secrets_version(s.id).await.expect("current"), g4);
    assert_eq!(
        m.secrets_version(other.id).await.expect("other"),
        g0,
        "other service's generation is independent"
    );
    let _: SecretsVersion = g4;
}

#[tokio::test]
async fn ac_meta_22_env_validation_rejects_bad_names_and_oversized_values() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "validated").await;
    m.put_env(s.id, env(&[("KEEP", "me")])).await.expect("seed");

    let too_many: Vec<(String, String)> = (0..101).map(|i| (format!("K{i}"), "v".into())).collect();
    let bad: Vec<(&str, Vec<(String, String)>)> = vec![
        ("lowercase name", env(&[("lower", "x")])),
        ("leading digit", env(&[("1ABC", "x")])),
        ("reserved RT_ prefix", env(&[("RT_SECRET", "x")])),
        ("value over 32 KB", vec![("BIG".to_string(), "x".repeat(32 * 1024 + 1))]),
        ("more than 100 entries", too_many),
    ];
    for (why, payload) in bad {
        let err = m.put_env(s.id, payload).await.expect_err(why);
        assert!(matches!(err, StoreError::Invalid(_)), "{why}: got {err:?}");
        assert_eq!(
            m.get_env(s.id).await.expect("env intact"),
            env(&[("KEEP", "me")]),
            "{why}: a rejected write changes nothing"
        );
    }

    // Boundaries are accepted.
    let max_entries: Vec<(String, String)> = (0..100).map(|i| (format!("K{i}"), "v".into())).collect();
    m.put_env(s.id, max_entries).await.expect("100 entries are allowed");
    m.put_env(s.id, vec![("_OK_1".to_string(), "x".repeat(32 * 1024))])
        .await
        .expect("32 KB value is allowed");
}

#[tokio::test]
async fn ac_meta_23_secrets_are_opaque_at_rest_and_list_names_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("meta.db");
    let m = SqliteMetaStore::open(&path).expect("open");
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "vault").await;

    let backend = LocalSecrets::new(&MasterKey::from_bytes([7u8; 32]));
    let plaintext = "correct-horse-battery-staple-9f31";
    let sealed = backend
        .seal(&secret_aad(t.id, s.id, "API_KEY"), plaintext.as_bytes())
        .to_bytes();
    m.put_secret(s.id, "API_KEY", sealed.clone()).await.expect("put secret");
    m.put_env(s.id, env(&[("PLAIN", "visible")])).await.expect("env");

    // STO-10: the database bytes (main file + WAL) hold the sealed blob, never the plaintext.
    let mut raw = std::fs::read(&path).expect("read meta.db");
    for suffix in ["-wal", "-shm"] {
        let mut p = path.clone().into_os_string();
        p.push(suffix);
        if let Ok(bytes) = std::fs::read(&p) {
            raw.extend(bytes);
        }
    }
    let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
    assert!(
        contains(&raw, &sealed),
        "the sealed bytes must be what is stored (and the scan must look in the right files)"
    );
    assert!(
        !contains(&raw, plaintext.as_bytes()),
        "plaintext secret found in meta.db bytes"
    );

    // The API view lists names and timestamps only.
    let listed = m.list_secrets(s.id).await.expect("list secrets");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "API_KEY");
    assert!(
        (listed[0].updated_at_ms - now_ms()).abs() < 60_000,
        "updated_at_ms must be a current unix-ms timestamp"
    );

    // The sealed bytes come back verbatim and still open to the plaintext.
    let stored = m.get_sealed_secrets(s.id).await.expect("sealed secrets");
    assert_eq!(stored, vec![("API_KEY".to_string(), sealed)]);
    let opened = backend
        .open(
            &secret_aad(t.id, s.id, "API_KEY"),
            &Sealed::from_bytes(&stored[0].1).expect("well-formed"),
        )
        .expect("decrypts with the right key and AAD");
    assert_eq!(opened.as_slice(), plaintext.as_bytes());
}

#[tokio::test]
async fn ac_meta_24_secret_overwrite_delete_and_per_service_scope() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;

    m.put_secret(a.id, "ONE", vec![1, 0, 0, 1]).await.expect("put one");
    m.put_secret(a.id, "TWO", vec![2, 0, 0, 2]).await.expect("put two");
    m.put_secret(b.id, "ONE", vec![9, 9, 9, 9]).await.expect("b one");

    let mut names: Vec<String> = m
        .list_secrets(a.id)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["ONE", "TWO"]);

    // Overwrite replaces the bytes (binary-safe, NULs kept) and does not duplicate the name.
    m.put_secret(a.id, "ONE", vec![0, 0, 5, 0, 0]).await.expect("overwrite");
    let mut sealed = m.get_sealed_secrets(a.id).await.expect("sealed");
    sealed.sort();
    assert_eq!(
        sealed,
        vec![
            ("ONE".to_string(), vec![0, 0, 5, 0, 0]),
            ("TWO".to_string(), vec![2, 0, 0, 2])
        ]
    );
    assert_eq!(
        m.get_sealed_secrets(b.id).await.expect("b sealed"),
        vec![("ONE".to_string(), vec![9, 9, 9, 9])]
    );

    m.delete_secret(a.id, "ONE").await.expect("delete");
    let names: Vec<String> = m
        .list_secrets(a.id)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, vec!["TWO"]);
    assert_eq!(
        m.list_secrets(b.id).await.expect("b list").len(),
        1,
        "deleting in one service leaves the other"
    );
}

#[tokio::test]
async fn ac_meta_25_secrets_and_env_are_independent_tables() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "mix").await;
    m.put_secret(s.id, "SAME", vec![1; 30]).await.expect("secret");
    m.put_env(s.id, env(&[("SAME", "plain")])).await.expect("env");
    m.put_env(s.id, Vec::new()).await.expect("clear env");
    assert_eq!(
        m.list_secrets(s.id)
            .await
            .expect("secrets survive an env replace")
            .len(),
        1
    );
    m.delete_secret(s.id, "SAME").await.expect("delete secret");
    m.put_env(s.id, env(&[("SAME", "plain")])).await.expect("env again");
    assert_eq!(
        m.get_env(s.id).await.expect("env survives a secret delete"),
        env(&[("SAME", "plain")])
    );
}

// ---- hosts -----------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_26_hosts_are_unique_and_deleted_per_service() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;

    let host = HostMapping {
        host: "app.example.com".into(),
        service_id: a.id,
        status: HostStatus::PendingVerification,
        verify_token: Some("abc".into()),
        created_at_ms: 1_000,
    };
    m.put_host(host.clone()).await.expect("put host");
    assert_eq!(m.find_host("app.example.com").await.expect("find"), Some(host.clone()));
    assert_eq!(m.list_hosts(a.id).await.expect("list a"), vec![host.clone()]);
    assert!(m.list_hosts(b.id).await.expect("list b").is_empty());

    // Same service re-puts (e.g. verification completes): status is updated.
    let verified = HostMapping {
        status: HostStatus::Active,
        verify_token: None,
        ..host.clone()
    };
    m.put_host(verified.clone()).await.expect("update host");
    assert_eq!(
        m.find_host("app.example.com").await.expect("find"),
        Some(verified.clone())
    );
    assert_eq!(m.list_hosts(a.id).await.expect("list a").len(), 1);

    // Another service cannot take the host.
    let err = m
        .put_host(HostMapping {
            service_id: b.id,
            ..verified.clone()
        })
        .await
        .expect_err("host taken");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert_eq!(err.code(), "host_taken");
    assert_eq!(
        m.find_host("app.example.com")
            .await
            .expect("find")
            .expect("exists")
            .service_id,
        a.id
    );

    // Deleting through the wrong service is a no-op; through the owner it removes the mapping.
    assert!(!m.delete_host(b.id, "app.example.com").await.expect("wrong service"));
    assert!(m.find_host("app.example.com").await.expect("find").is_some());
    assert!(m.delete_host(a.id, "app.example.com").await.expect("delete"));
    assert_eq!(m.find_host("app.example.com").await.expect("find"), None);
    assert!(!m.delete_host(a.id, "app.example.com").await.expect("delete twice"));
}

#[tokio::test]
async fn an_active_host_is_never_downgraded_and_only_an_approval_takes_over_a_pending_claim() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;
    let pending = |svc| HostMapping {
        host: "shop.example.com".into(),
        service_id: svc,
        status: HostStatus::PendingVerification,
        verify_token: Some("tok".into()),
        created_at_ms: 1_000,
    };
    let active = |svc| HostMapping {
        status: HostStatus::Active,
        verify_token: None,
        ..pending(svc)
    };

    // A's unverified claim blocks another unverified claim…
    m.put_host(pending(a.id)).await.expect("A claims");
    let err = m.put_host(pending(b.id)).await.expect_err("B cannot squat over A");
    assert_eq!(err.code(), "host_taken");
    // …but the operator's approval for B wins over A's pending claim.
    m.put_host(active(b.id)).await.expect("operator approves B");
    assert_eq!(m.find_host("shop.example.com").await.expect("find"), Some(active(b.id)));
    assert!(
        m.list_hosts(a.id).await.expect("list a").is_empty(),
        "A's claim moved to B"
    );

    // An active mapping is final for others, and B re-claiming it as a tenant keeps it active.
    assert_eq!(
        m.put_host(active(a.id)).await.expect_err("active is taken").code(),
        "host_taken"
    );
    m.put_host(pending(b.id)).await.expect("B re-puts its own host");
    assert_eq!(
        m.find_host("shop.example.com").await.expect("find"),
        Some(active(b.id)),
        "not downgraded to pending"
    );
}

#[tokio::test]
async fn the_route_snapshot_leaves_out_every_service_of_a_disabled_tenant() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let svc = mk_service(&m, &t, "svc").await;
    let v = mk_versions(&m, svc.id, 1, 1).await;
    m.set_live(svc.id, v[0].number, None).await.expect("live");
    assert_eq!(m.route_snapshot().await.expect("snapshot").entries.len(), 1);

    let patch = |disabled| rt_store::TenantPatch {
        name: None,
        plan: None,
        overrides: None,
        disabled: Some(disabled),
    };
    m.update_tenant(t.id, patch(true)).await.expect("disable");
    assert!(
        m.route_snapshot().await.expect("snapshot").entries.is_empty(),
        "a disabled tenant serves nothing"
    );
    m.update_tenant(t.id, patch(false)).await.expect("enable");
    assert_eq!(
        m.route_snapshot().await.expect("snapshot").entries.len(),
        1,
        "re-enabling restores the routes"
    );
}

#[tokio::test]
async fn the_lock_of_a_ready_build_is_found_by_source_digest_and_freed_with_its_last_version() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let svc = mk_service(&m, &t, "svc").await;
    let vs = mk_versions(&m, svc.id, 1, 3).await;
    let lock = rt_types::Digest::of(b"rt.lock of v1");
    assert_eq!(
        m.lock_for_source(&vs[0].source_digest).await.expect("lookup"),
        None,
        "no build yet"
    );
    m.set_build_state(svc.id, vs[0].number, BuildState::Ready, None, Some(lock))
        .await
        .expect("v1 ready");
    assert_eq!(
        m.lock_for_source(&vs[0].source_digest).await.expect("lookup"),
        Some(lock)
    );

    // Keep only the newest version: v1's source and its lock are both unreferenced now.
    m.set_live(svc.id, vs[2].number, None).await.expect("live v3");
    let freed = m.gc_versions(svc.id, 1).await.expect("gc");
    assert!(
        freed.contains(&vs[0].source_digest) && freed.contains(&lock),
        "freed: {freed:?}"
    );
    assert_eq!(m.lock_for_source(&vs[0].source_digest).await.expect("lookup"), None);
}

/// R1: every fetched module is vendored into the artifact store and listed in the build's
/// `rt.lock`. Version GC frees a module artifact once no remaining lock references it (modules are
/// content-addressed and shared between locks, services and tenants).
#[tokio::test]
async fn r1_module_artifacts_are_freed_with_the_last_lock_that_references_them() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let (a, b) = (mk_service(&m, &t, "a").await, mk_service(&m, &t, "b").await);
    let va = mk_versions(&m, a.id, 1, 2).await;
    let vb = mk_versions(&m, b.id, 10, 2).await;
    let (shared, only_a, pending) = (
        rt_types::Digest::of(b"module shared"),
        rt_types::Digest::of(b"module of a"),
        rt_types::Digest::of(b"module in flight"),
    );
    let (lock_a, lock_b, lock_next) = (
        rt_types::Digest::of(b"lock a"),
        rt_types::Digest::of(b"lock b"),
        rt_types::Digest::of(b"lock of a build in flight"),
    );

    m.record_lock_modules(&lock_a, &[shared, only_a])
        .await
        .expect("record a");
    m.record_lock_modules(&lock_a, &[shared, only_a])
        .await
        .expect("recording is idempotent");
    m.set_build_state(a.id, va[0].number, BuildState::Ready, None, Some(lock_a))
        .await
        .expect("a v1 ready");
    m.record_lock_modules(&lock_b, &[shared]).await.expect("record b");
    m.set_build_state(b.id, vb[0].number, BuildState::Ready, None, Some(lock_b))
        .await
        .expect("b v1 ready");
    // A build that recorded its lock but has not reported yet: its modules are referenced too.
    m.record_lock_modules(&lock_next, &[pending, only_a])
        .await
        .expect("record in-flight lock");
    for d in [shared, only_a, pending] {
        assert!(
            m.digest_referenced(&d).await.expect("referenced"),
            "{d} is referenced by a lock"
        );
    }

    // a drops v1: its source and lock go; `only_a` stays (the in-flight lock needs it), `shared` stays (b).
    m.set_live(a.id, va[1].number, None).await.expect("live a v2");
    let freed: HashSet<_> = m.gc_versions(a.id, 1).await.expect("gc a").into_iter().collect();
    assert_eq!(freed, HashSet::from([va[0].source_digest, lock_a]), "freed by a's GC");

    // b drops v1: lock b goes and with it the last reference to `shared`.
    m.set_live(b.id, vb[1].number, None).await.expect("live b v2");
    let freed: HashSet<_> = m.gc_versions(b.id, 1).await.expect("gc b").into_iter().collect();
    assert_eq!(
        freed,
        HashSet::from([vb[0].source_digest, lock_b, shared]),
        "freed by b's GC"
    );
    assert!(!m.digest_referenced(&shared).await.expect("referenced"));
    assert!(
        m.digest_referenced(&only_a).await.expect("referenced"),
        "still listed by the in-flight lock"
    );
}

#[tokio::test]
async fn r1_a_module_whose_bytes_equal_a_live_source_is_not_freed() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let svc = mk_service(&m, &t, "svc").await;
    let vs = mk_versions(&m, svc.id, 1, 2).await;
    let lock = rt_types::Digest::of(b"lock");
    // Content addressing: a vendored module can be byte-identical to an uploaded source.
    m.record_lock_modules(&lock, &[vs[1].source_digest])
        .await
        .expect("record");
    m.set_build_state(svc.id, vs[0].number, BuildState::Ready, None, Some(lock))
        .await
        .expect("v1 ready");
    m.set_live(svc.id, vs[1].number, None).await.expect("live v2");
    let freed = m.gc_versions(svc.id, 1).await.expect("gc");
    assert!(
        freed.contains(&lock) && !freed.contains(&vs[1].source_digest),
        "freed: {freed:?}"
    );
}

// ---- schedules -------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_27_schedule_create_list_update_delete_and_next_due() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;

    let interval = m
        .create_schedule(
            a.id,
            NewSchedule {
                kind: ScheduleKind::Interval { every_seconds: 30 },
                payload: json!({"k": 1}),
                enabled: true,
            },
        )
        .await
        .expect("interval");
    assert_eq!(interval.service_id, a.id);
    assert_eq!(interval.kind, ScheduleKind::Interval { every_seconds: 30 });
    assert_eq!(interval.payload, json!({"k": 1}));
    assert!(interval.enabled);
    assert_eq!(
        interval.next_due_ms, None,
        "next_due is computed by the trigger engine, not the store"
    );

    let cron = m
        .create_schedule(
            a.id,
            NewSchedule {
                kind: ScheduleKind::Cron {
                    expr: "*/5 * * * *".into(),
                    tz: "UTC".into(),
                },
                payload: json!(null),
                enabled: false,
            },
        )
        .await
        .expect("cron");
    let on_b = m
        .create_schedule(
            b.id,
            NewSchedule {
                kind: ScheduleKind::Interval { every_seconds: 5 },
                payload: json!(null),
                enabled: true,
            },
        )
        .await
        .expect("b schedule");

    let mut ids: Vec<_> = m
        .list_schedules(a.id)
        .await
        .expect("list a")
        .into_iter()
        .map(|s| s.id)
        .collect();
    ids.sort();
    let mut expected = vec![interval.id, cron.id];
    expected.sort();
    assert_eq!(ids, expected);
    let listed_cron = m
        .list_schedules(a.id)
        .await
        .expect("list a")
        .into_iter()
        .find(|s| s.id == cron.id)
        .expect("cron listed");
    assert_eq!(
        listed_cron.kind,
        ScheduleKind::Cron {
            expr: "*/5 * * * *".into(),
            tz: "UTC".into()
        }
    );
    assert!(!listed_cron.enabled);
    assert_eq!(m.all_schedules().await.expect("all").len(), 3);

    // next_due persists and can be cleared.
    m.set_schedule_next_due(interval.id, Some(1_800_000_000_000))
        .await
        .expect("set next due");
    let got = m
        .list_schedules(a.id)
        .await
        .expect("list")
        .into_iter()
        .find(|s| s.id == interval.id)
        .expect("listed");
    assert_eq!(got.next_due_ms, Some(1_800_000_000_000));
    m.set_schedule_next_due(interval.id, None)
        .await
        .expect("clear next due");
    let got = m
        .list_schedules(a.id)
        .await
        .expect("list")
        .into_iter()
        .find(|s| s.id == interval.id)
        .expect("listed");
    assert_eq!(got.next_due_ms, None);

    // Update replaces kind, payload and flag.
    let updated = m
        .update_schedule(
            a.id,
            interval.id,
            NewSchedule {
                kind: ScheduleKind::Cron {
                    expr: "0 * * * *".into(),
                    tz: "Europe/Berlin".into(),
                },
                payload: json!({"v": 2}),
                enabled: false,
            },
        )
        .await
        .expect("update");
    assert_eq!(updated.id, interval.id);
    assert_eq!(
        updated.kind,
        ScheduleKind::Cron {
            expr: "0 * * * *".into(),
            tz: "Europe/Berlin".into()
        }
    );
    assert_eq!(updated.payload, json!({"v": 2}));
    assert!(!updated.enabled);

    // A schedule is addressed through its own service.
    let err = m
        .update_schedule(
            b.id,
            interval.id,
            NewSchedule {
                kind: ScheduleKind::Interval { every_seconds: 1 },
                payload: json!(null),
                enabled: true,
            },
        )
        .await
        .expect_err("wrong service");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    assert!(!m
        .delete_schedule(b.id, interval.id)
        .await
        .expect("delete via wrong service"));

    assert!(m.delete_schedule(a.id, interval.id).await.expect("delete"));
    assert!(!m.delete_schedule(a.id, interval.id).await.expect("delete twice"));
    assert_eq!(m.list_schedules(a.id).await.expect("list").len(), 1);
    assert_eq!(m.list_schedules(b.id).await.expect("list b"), vec![on_b]);
}

// ---- usage -----------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_28_usage_accumulates_per_day_and_filters_by_tenant_and_range() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    let sa = mk_service(&m, &a, "sa").await;
    let sa2 = mk_service(&m, &a, "sa2").await;
    let sb = mk_service(&m, &b, "sb").await;

    let delta = |inv, cpu, egress, storage| UsageDelta {
        invocations: inv,
        cpu_ms: cpu,
        egress_bytes: egress,
        storage_bytes: storage,
    };
    m.add_usage(
        "2026-10-01",
        UsageBatch {
            items: vec![
                (a.id, sa.id, delta(3, 10, 100, 50)),
                (a.id, sa2.id, delta(1, 1, 1, 1)),
                (b.id, sb.id, delta(9, 9, 9, 9)),
            ],
        },
    )
    .await
    .expect("day 1");
    // Same (tenant, service, day) accumulates; storage may shrink.
    m.add_usage(
        "2026-10-01",
        UsageBatch {
            items: vec![(a.id, sa.id, delta(2, 5, 25, -30))],
        },
    )
    .await
    .expect("day 1 again");
    for day in ["2026-10-02", "2026-10-03", "2026-10-05"] {
        m.add_usage(
            day,
            UsageBatch {
                items: vec![(a.id, sa.id, delta(1, 1, 1, 0))],
            },
        )
        .await
        .expect("later day");
    }

    let rows = m.get_usage(a.id, "2026-10-01", "2026-10-01").await.expect("day 1 rows");
    assert_eq!(rows.len(), 2, "one row per service for the day");
    let r = rows.iter().find(|r| r.service == sa.id).expect("sa row");
    assert_eq!((r.tenant, r.day.as_str()), (a.id, "2026-10-01"));
    assert_eq!(
        (r.invocations, r.cpu_ms, r.egress_bytes, r.storage_bytes),
        (5, 15, 125, 20)
    );
    assert!(
        rows.iter().all(|r| r.tenant == a.id),
        "other tenants' usage is never returned"
    );

    // Inclusive day range.
    let mut days: Vec<String> = m
        .get_usage(a.id, "2026-10-02", "2026-10-03")
        .await
        .expect("range")
        .into_iter()
        .map(|r| r.day)
        .collect();
    days.sort();
    assert_eq!(days, vec!["2026-10-02", "2026-10-03"]);
    assert_eq!(
        m.get_usage(a.id, "2026-10-01", "2026-10-31")
            .await
            .expect("month")
            .len(),
        2 + 3
    );
    assert_eq!(m.get_usage(b.id, "2026-10-01", "2026-10-31").await.expect("b").len(), 1);
    assert!(m
        .get_usage(a.id, "2027-01-01", "2027-01-31")
        .await
        .expect("empty range")
        .is_empty());
}

#[tokio::test]
async fn ac_meta_29_concurrent_usage_adds_do_not_lose_updates() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let s = mk_service(&m, &t, "busy").await;

    let mut handles = Vec::new();
    for _ in 0..50 {
        let m = m.clone();
        let (tid, sid) = (t.id, s.id);
        handles.push(tokio::spawn(async move {
            m.add_usage(
                "2026-10-01",
                UsageBatch {
                    items: vec![(
                        tid,
                        sid,
                        UsageDelta {
                            invocations: 1,
                            cpu_ms: 2,
                            egress_bytes: 3,
                            storage_bytes: 0,
                        },
                    )],
                },
            )
            .await
        }));
    }
    for h in handles {
        h.await.expect("join").expect("add usage");
    }
    let rows = m.get_usage(t.id, "2026-10-01", "2026-10-01").await.expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].invocations, rows[0].cpu_ms, rows[0].egress_bytes),
        (50, 100, 150)
    );
}

// ---- events ----------------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_30_events_roundtrip_filter_by_service_and_paginate() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let a = mk_service(&m, &t, "a").await;
    let b = mk_service(&m, &t, "b").await;

    let mk = |n: u128, service, kind: &str| NodeEvent {
        id: EventId::from_u128(n),
        ts_ms: 1_700_000_000_000 + n as i64,
        tenant: Some(t.id),
        service: Some(service),
        kind: kind.to_string(),
        data: json!({"n": n as u64}),
    };
    let a_events: Vec<NodeEvent> = (1..=5)
        .map(|n| mk(n, a.id, if n % 2 == 0 { "deploy" } else { "rollback.auto" }))
        .collect();
    for ev in &a_events {
        m.append_event(ev.clone()).await.expect("append a");
    }
    m.append_event(mk(100, b.id, "deploy")).await.expect("append b");

    // Fetch by id: exact round trip.
    assert_eq!(
        m.event_by_id(a_events[1].id).await.expect("by id"),
        Some(a_events[1].clone())
    );
    assert_eq!(m.event_by_id(EventId::from_u128(9999)).await.expect("by id"), None);

    // Pagination over service A: pages of 2, 2, 1; no duplicates; B's event never appears.
    let mut seen: Vec<EventId> = Vec::new();
    let mut sizes = Vec::new();
    let mut cursor = None;
    loop {
        let page = m
            .list_events(
                a.id,
                PageReq {
                    limit: 2,
                    cursor: cursor.clone(),
                },
            )
            .await
            .expect("page");
        sizes.push(page.items.len());
        seen.extend(page.items.iter().map(|e| e.id));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(sizes.len() < 10);
    }
    assert_eq!(sizes, vec![2, 2, 1]);
    let unique: HashSet<_> = seen.iter().copied().collect();
    assert_eq!(unique.len(), 5);
    assert_eq!(unique, a_events.iter().map(|e| e.id).collect::<HashSet<_>>());
    let ascending = seen.windows(2).all(|w| w[0] < w[1]);
    let descending = seen.windows(2).all(|w| w[0] > w[1]);
    assert!(
        ascending || descending,
        "events must be listed in event-id order: {seen:?}"
    );

    let listed_b = m.list_events(b.id, PageReq::default()).await.expect("b events");
    assert_eq!(listed_b.items.len(), 1);
    assert_eq!(listed_b.items[0].kind, "deploy");
}

// ---- cache frequencies -----------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_31_cache_freq_snapshot_roundtrips_and_is_overwritten() {
    let m = meta();
    assert_eq!(m.load_cache_freq().await.expect("load empty"), None);

    let big = noise(1024 * 1024, 5);
    m.save_cache_freq(big.clone()).await.expect("save big");
    assert_eq!(m.load_cache_freq().await.expect("load"), Some(big));

    m.save_cache_freq(vec![0, 1, 2, 255]).await.expect("save small");
    assert_eq!(
        m.load_cache_freq().await.expect("load"),
        Some(vec![0, 1, 2, 255]),
        "a new snapshot replaces the old one"
    );
}

// ---- concurrency -----------------------------------------------------------------------------

#[tokio::test]
async fn ac_meta_32_concurrent_creates_of_one_slug_yield_exactly_one_winner() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let m = m.clone();
        let tid = t.id;
        handles.push(tokio::spawn(async move {
            m.create_service(TenantScope::Tenant(tid), tid, new_service("race"))
                .await
        }));
    }
    let (mut ok, mut conflicts) = (0, 0);
    for h in handles {
        match h.await.expect("join") {
            Ok(_) => ok += 1,
            Err(StoreError::Conflict(code)) => {
                assert_eq!(code, "slug_taken");
                conflicts += 1;
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert_eq!((ok, conflicts), (1, 9));
    assert_eq!(m.count_services(t.id).await.expect("count"), 1);
}

#[tokio::test]
async fn ac_meta_33_concurrent_creates_of_distinct_slugs_all_succeed() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;

    let mut handles = Vec::new();
    for i in 0..50 {
        let m = m.clone();
        let tid = t.id;
        handles.push(tokio::spawn(async move {
            m.create_service(TenantScope::Tenant(tid), tid, new_service(&format!("svc-{i}")))
                .await
        }));
    }
    let mut ids = HashSet::new();
    for h in handles {
        ids.insert(h.await.expect("join").expect("create").id);
    }
    assert_eq!(ids.len(), 50);
    assert_eq!(m.count_services(t.id).await.expect("count"), 50);
}

// ---- caps checked in the write's own transaction ---------------------------------------------------

fn floor_service(slug: &str, min: u32) -> rt_types::NewService {
    let mut s = new_service(slug);
    s.config.scaling.min_instances = Some(min);
    s
}

fn floor_patch(min: u32) -> ServicePatch {
    ServicePatch {
        scaling: Some(rt_types::ScalingConfig {
            min_instances: Some(min),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_warm_floor_total_holds_under_concurrent_creates_and_patches() {
    let m = meta();
    let t = mk_tenant(&m, "floors").await;
    let cap = rt_store::WarmFloorCap {
        total: 4,
        per_service: 3,
    };
    let scope = TenantScope::Tenant(t.id);

    // 12 creates of floor 1 at once: exactly 4 fit.
    let creates = (0..12).map(|i| {
        let m = m.clone();
        tokio::spawn(async move {
            m.create_service_capped(scope, t.id, floor_service(&format!("f{i}"), 1), cap)
                .await
        })
    });
    let results: Vec<_> = futures_util::future::join_all(creates)
        .await
        .into_iter()
        .map(|r| r.expect("join"))
        .collect();
    let ok = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 4, "{results:?}");
    for r in results.iter().filter_map(|r| r.as_ref().err()) {
        assert!(matches!(r, StoreError::WarmFloorTotal { cap: 4, .. }), "{r:?}");
        assert_eq!(r.code(), "limit_exceeds_plan");
    }

    let big = m
        .create_service_capped(scope, t.id, floor_service("big", 0), cap)
        .await
        .expect("no floor always fits");
    let made: Vec<_> = results.into_iter().filter_map(Result::ok).collect();
    // Lowering always fits (total 3), then two raises 1 -> 2 at once: only one fits (4; 5 does not).
    m.patch_service_capped(scope, made[0].id, floor_patch(0), None, cap)
        .await
        .expect("lowering fits");
    let raises = [made[1].id, made[2].id].map(|id| {
        let m = m.clone();
        tokio::spawn(async move { m.patch_service_capped(scope, id, floor_patch(2), None, cap).await })
    });
    let raised: Vec<_> = futures_util::future::join_all(raises)
        .await
        .into_iter()
        .map(|r| r.expect("join"))
        .collect();
    assert_eq!(raised.iter().filter(|r| r.is_ok()).count(), 1, "{raised:?}");
    // A floor above the per-service bound counts as the bound (what the scheduler keeps): 4 + 3.
    let err = m
        .patch_service_capped(scope, big.id, floor_patch(9), None, cap)
        .await
        .expect_err("over the cap");
    assert!(
        matches!(err, StoreError::WarmFloorTotal { total: 7, cap: 4 }),
        "{err:?}"
    );
    // Without a cap anything goes.
    m.patch_service_capped(scope, big.id, floor_patch(9), None, rt_store::WarmFloorCap::NONE)
        .await
        .expect("no cap");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_live_key_cap_holds_under_concurrent_creates() {
    let m = meta();
    let t = mk_tenant(&m, "keys").await;
    let nk = |n: u128| NewKey {
        id: KeyId::from_u128(1000 + n),
        name: None,
        scopes: Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        hmac: vec![n as u8; 32],
    };
    let now = now_ms();
    let creates = (0..16u128).map(|n| {
        let m = m.clone();
        let tenant = t.id;
        tokio::spawn(async move { m.create_child_key_capped(tenant, nk(n), None, 5, now).await })
    });
    let results: Vec<_> = futures_util::future::join_all(creates)
        .await
        .into_iter()
        .map(|r| r.expect("join"))
        .collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 5, "{results:?}");
    for r in results.iter().filter_map(|r| r.as_ref().err()) {
        assert!(matches!(r, StoreError::Conflict("quota_exceeded")), "{r:?}");
    }
    assert_eq!(m.count_live_keys(t.id, now).await.expect("count"), 5);
}

/// KEY-035: a key minted by a parent that is no longer live (revoked, or revoked while the request
/// was in flight) is refused in the inserting transaction, so it can never outlive the parent's
/// revocation.
#[tokio::test]
async fn key_035_a_revoked_parent_cannot_mint_a_child() {
    let m = meta();
    let t = mk_tenant(&m, "k035").await;
    let nk = |n: u128| NewKey {
        id: KeyId::from_u128(3500 + n),
        name: None,
        scopes: Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
        hmac: vec![n as u8; 32],
    };
    let parent = m.create_key(t.id, nk(1)).await.expect("parent");
    assert_eq!(
        m.revoke_key_tree(t.id, parent.id).await.expect("revoke"),
        vec![parent.id]
    );
    let err = m
        .create_child_key(t.id, nk(2), Some(parent.id))
        .await
        .expect_err("revoked parent");
    assert!(matches!(err, StoreError::NotFound), "{err:?}");
    let err = m
        .create_child_key_capped(t.id, nk(3), Some(parent.id), 200, now_ms())
        .await
        .expect_err("revoked parent");
    assert!(matches!(err, StoreError::NotFound), "{err:?}");
    let err = m
        .create_child_key(t.id, nk(4), Some(KeyId::from_u128(9_999)))
        .await
        .expect_err("unknown parent");
    assert!(matches!(err, StoreError::NotFound), "{err:?}");
    assert!(
        m.find_key(KeyId::from_u128(3502)).await.unwrap().is_none(),
        "nothing was stored"
    );
}

/// META-008: a sealed secret of exactly 32 KiB + 64 bytes (a 32 KiB plaintext) is stored; one byte
/// more is refused.
#[tokio::test]
async fn meta_008_the_sealed_secret_cap_is_exact() {
    let m = meta();
    let t = mk_tenant(&m, "m008").await;
    let s = mk_service(&m, &t, "m008").await;
    m.put_secret(s.id, "MAX", vec![1; 32 * 1024 + 64])
        .await
        .expect("exactly the cap");
    let err = m
        .put_secret(s.id, "OVER", vec![1; 32 * 1024 + 65])
        .await
        .expect_err("one byte over");
    assert!(matches!(err, StoreError::Invalid(_)), "{err:?}");
}

/// META-009: version numbers never come back after GC removed older versions.
#[tokio::test]
async fn meta_009_version_numbers_are_not_reused_after_gc() {
    let m = meta();
    let t = mk_tenant(&m, "m009").await;
    let s = mk_service(&m, &t, "m009").await;
    mk_versions(&m, s.id, 1, 3).await;
    m.gc_versions(s.id, 1).await.expect("gc");
    let next = m.add_version(s.id, new_version(99)).await.expect("add");
    assert_eq!(next.number, VersionNo(4));
}

/// PERS-004: a meta.db written by a newer binary (schema version above this one's) is refused
/// with a clear error instead of being used or "migrated".
#[test]
fn pers_004_a_newer_schema_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("meta.db");
    drop(SqliteMetaStore::open(&path).expect("create"));
    let c = rusqlite::Connection::open(&path).expect("open");
    c.pragma_update(None, "user_version", 9_999).expect("bump");
    drop(c);
    let err = match SqliteMetaStore::open(&path) {
        Ok(_) => panic!("a newer schema must be refused"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("9999") && err.contains("newer"), "{err}");
}
