//! Branching services (`docs/design/branching-services-design.md` §3, §5, §7): adopting a tested
//! version into its parent by digest, the `version_switches` rows, startup recovery and the GC hold
//! on rollback targets.

use crate::common;

use common::*;
use rt_store::{Fault, Faults, MetaStore, PageReq, SqliteMetaStore, StoreError, TenantScope};
use rt_types::{
    BuildFailReason, BuildFailure, BuildState, NewProject, NewSwitch, RuntimeKind, Service, ServiceId, SwitchId,
    SwitchKind, SwitchPatch, SwitchState, Tenant, Version, VersionNo, VersionOrigin,
};

/// A version of `s` that finished building, with lock and bundle digests of its own.
async fn ready_version(m: &SqliteMetaStore, s: ServiceId, n: u64) -> Version {
    let v = m.add_version(s, new_version(n)).await.expect("add version");
    m.set_build_state(s, v.number, BuildState::Ready, None, Some(digest_n(n + 1000)))
        .await
        .expect("ready");
    m.set_bundle_digest(s, v.number, digest_n(n + 2000))
        .await
        .expect("bundle");
    m.get_version(s, v.number).await.expect("get").expect("exists")
}

async fn family() -> (std::sync::Arc<SqliteMetaStore>, Tenant, Service, Service) {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let parent = mk_service(&m, &t, "parent").await;
    let branch = mk_service(&m, &t, "parent-br-staging").await;
    (m, t, parent, branch)
}

fn promote(to: u32) -> NewSwitch {
    NewSwitch {
        kind: SwitchKind::Promote,
        source: None,
        from_version: None,
        to_version: VersionNo(to),
        created_by: None,
    }
}

fn to_state(state: SwitchState) -> SwitchPatch {
    SwitchPatch {
        state: Some(state),
        ..SwitchPatch::default()
    }
}

// ---- adoption ----------------------------------------------------------------------------------

/// §7: the digests of an adopted row are the source row's, read by the store. No digest is passed
/// in, so a client cannot substitute one.
#[tokio::test]
async fn adopted_version_rows_equal_source_digests() {
    let (m, _t, parent, branch) = family().await;
    mk_versions(&m, parent.id, 50, 2).await;
    let src = ready_version(&m, branch.id, 1).await;

    let v = m.adopt_version(parent.id, branch.id, src.number).await.expect("adopt");

    assert_eq!(v.service_id, parent.id);
    assert_eq!(v.number, VersionNo(3), "next number of the parent");
    assert_eq!(v.source_digest, src.source_digest);
    assert_eq!(v.lock_digest, src.lock_digest);
    assert!(v.lock_digest.is_some());
    assert_eq!(v.bundle_digest, src.bundle_digest);
    assert!(v.bundle_digest.is_some());
    assert_eq!(v.build_state, BuildState::Ready);
    assert_eq!(
        v.origin,
        Some(VersionOrigin {
            service: branch.id,
            version: src.number
        })
    );
    assert_eq!(
        m.get_version(parent.id, v.number).await.expect("get"),
        Some(v),
        "the returned row is the stored row"
    );
    assert_eq!(
        m.get_service(TenantScope::Operator, parent.id)
            .await
            .expect("get")
            .expect("exists")
            .live_version,
        None,
        "adoption does not flip"
    );
    let plain = m.add_version(parent.id, new_version(99)).await.expect("upload");
    assert_eq!(plain.origin, None, "only adoption writes origin");
    assert_eq!(
        m.get_version(parent.id, plain.number)
            .await
            .expect("get")
            .expect("row")
            .origin,
        None
    );
}

/// An adopted version keeps the shared artifacts referenced after the branch's row is gone.
#[tokio::test]
async fn an_adopted_version_keeps_the_digests_referenced_when_the_branch_is_deleted() {
    let (m, t, parent, branch) = family().await;
    let src = ready_version(&m, branch.id, 1).await;
    m.adopt_version(parent.id, branch.id, src.number).await.expect("adopt");
    m.delete_service(TenantScope::Tenant(t.id), branch.id, false)
        .await
        .expect("delete");
    assert!(m.digest_referenced(&src.source_digest).await.expect("referenced"));
    assert!(m
        .digest_referenced(&src.lock_digest.expect("lock"))
        .await
        .expect("referenced"));
}

/// §5: a version that is not `ready` is never adopted.
#[tokio::test]
async fn adoption_refuses_every_source_that_is_not_ready() {
    let (m, _t, parent, branch) = family().await;
    let pending = m.add_version(branch.id, new_version(1)).await.expect("pending");
    let building = m.add_version(branch.id, new_version(2)).await.expect("building");
    m.set_build_state(branch.id, building.number, BuildState::Building, None, None)
        .await
        .expect("building");
    let failed = m.add_version(branch.id, new_version(3)).await.expect("failed");
    m.set_build_state(
        branch.id,
        failed.number,
        BuildState::Failed,
        Some(BuildFailure::new(BuildFailReason::Internal, "x")),
        None,
    )
    .await
    .expect("failed");

    for v in [pending.number, building.number, failed.number] {
        let e = m.adopt_version(parent.id, branch.id, v).await.expect_err("not ready");
        assert!(matches!(e, StoreError::Conflict("version_not_ready")), "{v:?}: {e:?}");
    }
    assert!(
        m.list_versions(parent.id).await.expect("list").is_empty(),
        "no row was added"
    );
}

/// §5 step 2: a second switch of the same source version reuses the row.
#[tokio::test]
async fn adoption_is_idempotent_per_origin() {
    let (m, _t, parent, branch) = family().await;
    let one = ready_version(&m, branch.id, 1).await;
    let two = ready_version(&m, branch.id, 2).await;

    let first = m.adopt_version(parent.id, branch.id, one.number).await.expect("adopt");
    let again = m.adopt_version(parent.id, branch.id, one.number).await.expect("again");
    assert_eq!(first, again);
    let other = m.adopt_version(parent.id, branch.id, two.number).await.expect("other");
    assert_ne!(other.number, first.number, "another source version is another row");
    assert_eq!(m.list_versions(parent.id).await.expect("list").len(), 2);
}

/// Adoption is not a build: it does not spend `plan.builds_per_hour`.
#[tokio::test]
async fn adoption_does_not_count_as_a_build() {
    let (m, t, parent, branch) = family().await;
    let src = ready_version(&m, branch.id, 1).await;
    let before = m.count_builds_since(t.id, 0).await.expect("count");
    m.adopt_version(parent.id, branch.id, src.number).await.expect("adopt");
    assert_eq!(m.count_builds_since(t.id, 0).await.expect("count"), before);
}

/// §7: the store refuses a cross-tenant or cross-project source as missing, even if a caller's
/// check was skipped; existence is not revealed.
#[tokio::test]
async fn adoption_across_tenants_or_projects_behaves_as_missing() {
    let (m, t, parent, branch) = family().await;

    let other_tenant = mk_tenant(&m, "other").await;
    let foreign = mk_service(&m, &other_tenant, "foreign").await;
    let foreign_v = ready_version(&m, foreign.id, 1).await;
    let e = m
        .adopt_version(parent.id, foreign.id, foreign_v.number)
        .await
        .expect_err("tenant");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");

    m.create_project(
        t.id,
        NewProject {
            slug: "web".into(),
            name: None,
            environments: vec!["production".into()],
        },
    )
    .await
    .expect("project");
    let mut in_web = new_service("in-web");
    in_web.project = "web".into();
    let in_web = m
        .create_service(TenantScope::Tenant(t.id), t.id, in_web)
        .await
        .expect("service");
    let web_v = ready_version(&m, in_web.id, 2).await;
    let e = m
        .adopt_version(parent.id, in_web.id, web_v.number)
        .await
        .expect_err("project");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");

    for (p, s, v) in [
        (parent.id, branch.id, VersionNo(9)),
        (parent.id, svc(404), VersionNo(1)),
        (svc(404), branch.id, VersionNo(1)),
    ] {
        let e = m.adopt_version(p, s, v).await.expect_err("missing");
        assert!(matches!(e, StoreError::NotFound), "{e:?}");
    }
}

#[tokio::test]
async fn adoption_refuses_another_runtime_and_the_service_itself() {
    let (m, t, parent, branch) = family().await;
    let mut wasm = new_service("wasm-one");
    wasm.runtime = RuntimeKind::Wasm;
    let wasm = m
        .create_service(TenantScope::Tenant(t.id), t.id, wasm)
        .await
        .expect("service");
    let v = ready_version(&m, wasm.id, 1).await;
    let e = m
        .adopt_version(parent.id, wasm.id, v.number)
        .await
        .expect_err("runtime");
    assert!(matches!(e, StoreError::Conflict("runtime_mismatch")), "{e:?}");

    let own = ready_version(&m, branch.id, 2).await;
    let e = m
        .adopt_version(branch.id, branch.id, own.number)
        .await
        .expect_err("itself");
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
}

// ---- switch rows -------------------------------------------------------------------------------

#[tokio::test]
async fn a_switch_row_round_trips_and_starts_preparing() {
    let (m, _t, parent, branch) = family().await;
    let src = ready_version(&m, branch.id, 1).await;
    let v = m.adopt_version(parent.id, branch.id, src.number).await.expect("adopt");
    let origin = VersionOrigin {
        service: branch.id,
        version: src.number,
    };

    let s = m
        .create_switch(
            parent.id,
            NewSwitch {
                kind: SwitchKind::Switch,
                source: Some(origin),
                from_version: None,
                to_version: v.number,
                created_by: Some("key_x".into()),
            },
        )
        .await
        .expect("create");
    assert_eq!(s.state, SwitchState::Preparing);
    assert_eq!(s.kind, SwitchKind::Switch);
    assert_eq!(s.source, Some(origin));
    assert_eq!(s.to_version, v.number);
    assert_eq!(s.created_by.as_deref(), Some("key_x"));
    assert!(!s.interrupted);
    assert!(s.id.to_string().starts_with("swt_"), "{}", s.id);
    assert_eq!(m.get_switch(parent.id, s.id).await.expect("get"), Some(s.clone()));

    let flipped = m
        .update_switch(
            parent.id,
            s.id,
            SwitchPatch {
                state: Some(SwitchState::Draining),
                from_version: Some(VersionNo(7)),
                drain_deadline_ms: Some(123_456),
                error: None,
            },
        )
        .await
        .expect("flip");
    assert_eq!(flipped.state, SwitchState::Draining);
    assert_eq!(flipped.from_version, Some(VersionNo(7)));
    assert_eq!(flipped.drain_deadline_ms, Some(123_456));
    assert_eq!(flipped.created_at_ms, s.created_at_ms);
    assert_eq!(m.get_switch(parent.id, s.id).await.expect("get"), Some(flipped.clone()));

    // An empty patch keeps every field.
    let same = m
        .update_switch(parent.id, s.id, SwitchPatch::default())
        .await
        .expect("noop");
    assert_eq!(same.state, flipped.state);
    assert_eq!(same.from_version, flipped.from_version);

    let done = m
        .update_switch(
            parent.id,
            s.id,
            SwitchPatch {
                state: Some(SwitchState::Failed),
                error: Some("migration_failed"),
                ..SwitchPatch::default()
            },
        )
        .await
        .expect("fail");
    assert_eq!(done.error.as_deref(), Some("migration_failed"));
}

#[tokio::test]
async fn every_kind_and_state_survives_the_database() {
    let (m, _t, parent, _b) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    for kind in [SwitchKind::Promote, SwitchKind::Switch, SwitchKind::Rollback] {
        let s = m
            .create_switch(parent.id, NewSwitch { kind, ..promote(1) })
            .await
            .expect("create");
        for state in [
            SwitchState::Migrating,
            SwitchState::Draining,
            SwitchState::DrainTimedOut,
            SwitchState::Completed,
            SwitchState::Failed,
            SwitchState::Preparing,
            SwitchState::Completed,
        ] {
            let got = m.update_switch(parent.id, s.id, to_state(state)).await.expect("update");
            assert_eq!(got.state, state);
            assert_eq!(got.kind, kind);
        }
    }
}

/// §7 "concurrent switch/rollback race": one `preparing|migrating` row per service, per the index.
#[tokio::test]
async fn only_one_switch_of_a_service_is_active() {
    let (m, _t, parent, branch) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    m.add_version(branch.id, new_version(2)).await.expect("v1");

    let first = m.create_switch(parent.id, promote(1)).await.expect("first");
    let e = m.create_switch(parent.id, promote(1)).await.expect_err("second");
    assert!(matches!(e, StoreError::Conflict("switch_in_progress")), "{e:?}");

    m.update_switch(parent.id, first.id, to_state(SwitchState::Migrating))
        .await
        .expect("migrating");
    let e = m.create_switch(parent.id, promote(1)).await.expect_err("still active");
    assert!(matches!(e, StoreError::Conflict("switch_in_progress")), "{e:?}");

    m.create_switch(branch.id, promote(1))
        .await
        .expect("another service is free");

    // A drain does not hold the index: a second flip may start during it.
    m.update_switch(parent.id, first.id, to_state(SwitchState::Draining))
        .await
        .expect("draining");
    let second = m.create_switch(parent.id, promote(1)).await.expect("during a drain");

    // Moving an old row back to active collides the same way.
    let e = m
        .update_switch(parent.id, first.id, to_state(SwitchState::Preparing))
        .await
        .expect_err("collides");
    assert!(matches!(e, StoreError::Conflict("switch_in_progress")), "{e:?}");
    assert_eq!(
        m.get_switch(parent.id, second.id)
            .await
            .expect("get")
            .expect("row")
            .state,
        SwitchState::Preparing
    );
}

#[tokio::test]
async fn a_switch_needs_an_existing_service_and_target_version() {
    let (m, _t, parent, _b) = family().await;
    let e = m.create_switch(svc(404), promote(1)).await.expect_err("service");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");
    let e = m.create_switch(parent.id, promote(1)).await.expect_err("version");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");
}

/// A switch id of another service reads and writes as missing.
#[tokio::test]
async fn a_switch_is_only_visible_through_its_own_service() {
    let (m, _t, parent, branch) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    let s = m.create_switch(parent.id, promote(1)).await.expect("create");

    assert_eq!(m.get_switch(branch.id, s.id).await.expect("get"), None);
    let e = m
        .update_switch(branch.id, s.id, to_state(SwitchState::Failed))
        .await
        .expect_err("other service");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");
    assert_eq!(
        m.get_switch(parent.id, s.id).await.expect("get").expect("row").state,
        SwitchState::Preparing,
        "the refused update changed nothing"
    );
    let e = m
        .update_switch(parent.id, SwitchId::new(), SwitchPatch::default())
        .await
        .expect_err("unknown");
    assert!(matches!(e, StoreError::NotFound), "{e:?}");
    assert!(m
        .list_switches(branch.id, PageReq::default())
        .await
        .expect("list")
        .items
        .is_empty());
}

/// Newest first, even for rows created in one millisecond (ids are strictly increasing per
/// service), and the pages join up.
#[tokio::test]
async fn switches_list_newest_first_across_pages() {
    let (m, _t, parent, _b) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    let mut made = Vec::new();
    for _ in 0..25 {
        let s = m.create_switch(parent.id, promote(1)).await.expect("create");
        m.update_switch(parent.id, s.id, to_state(SwitchState::Completed))
            .await
            .expect("done");
        made.push(s.id);
    }
    made.reverse();

    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let page = m
            .list_switches(parent.id, PageReq { limit: 10, cursor })
            .await
            .expect("page");
        seen.extend(page.items.iter().map(|s| s.id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, made);
}

#[tokio::test]
async fn deleting_a_service_drops_its_switches() {
    let (m, t, parent, _b) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    let s = m.create_switch(parent.id, promote(1)).await.expect("create");
    m.delete_service(TenantScope::Tenant(t.id), parent.id, false)
        .await
        .expect("delete");
    assert_eq!(m.get_switch(parent.id, s.id).await.expect("get"), None);
}

// ---- recovery ----------------------------------------------------------------------------------

/// §5 crash recovery: `preparing|migrating` → `failed`/`interrupted`, `draining` → `completed` with
/// the flag; finished rows and the live version stay.
#[tokio::test]
async fn recover_switches_maps_states_and_leaves_live_alone() {
    let (m, t, parent, _b) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    m.set_live(parent.id, VersionNo(1), None).await.expect("live");

    let mut cases = Vec::new();
    for (i, state) in [
        SwitchState::Preparing,
        SwitchState::Migrating,
        SwitchState::Draining,
        SwitchState::Completed,
        SwitchState::DrainTimedOut,
        SwitchState::Failed,
    ]
    .into_iter()
    .enumerate()
    {
        let s = mk_service(&m, &t, &format!("recover-{i}")).await;
        m.add_version(s.id, new_version(10 + i as u64)).await.expect("version");
        let row = m.create_switch(s.id, promote(1)).await.expect("create");
        let row = m.update_switch(s.id, row.id, to_state(state)).await.expect("state");
        cases.push((s.id, row, state));
    }

    assert_eq!(m.recover_switches().await.expect("recover"), 3);

    for (svc_id, before, state) in cases {
        let got = m.get_switch(svc_id, before.id).await.expect("get").expect("row");
        match state {
            SwitchState::Preparing | SwitchState::Migrating => {
                assert_eq!(got.state, SwitchState::Failed, "{state:?}");
                assert_eq!(got.error.as_deref(), Some("interrupted"));
                assert!(got.interrupted);
            }
            SwitchState::Draining => {
                assert_eq!(got.state, SwitchState::Completed);
                assert_eq!(got.error, None);
                assert!(got.interrupted);
            }
            _ => assert_eq!(got, before, "a finished row is not touched"),
        }
    }
    assert_eq!(
        m.get_service(TenantScope::Operator, parent.id)
            .await
            .expect("get")
            .expect("exists")
            .live_version,
        Some(VersionNo(1))
    );
    assert_eq!(m.recover_switches().await.expect("again"), 0, "idempotent");
}

/// A crash mid-switch must not leave the service locked: after recovery a new switch can start.
#[tokio::test]
async fn recovery_releases_the_active_index() {
    let (m, _t, parent, _b) = family().await;
    m.add_version(parent.id, new_version(1)).await.expect("v1");
    m.create_switch(parent.id, promote(1)).await.expect("create");
    m.recover_switches().await.expect("recover");
    m.create_switch(parent.id, promote(1)).await.expect("free again");
}

#[tokio::test]
async fn store_faults_reach_the_caller_of_every_new_write() {
    let (m, _t, parent, branch) = family().await;
    let src = ready_version(&m, branch.id, 1).await;
    let faults = Faults::new();
    m.inject_faults(faults.clone());

    faults.fail_next(1, Fault::DiskFull);
    let e = m
        .adopt_version(parent.id, branch.id, src.number)
        .await
        .expect_err("adopt");
    assert!(matches!(e, StoreError::StorageQuota), "{e:?}");
    faults.fail_next(1, Fault::DiskFull);
    let e = m.recover_switches().await.expect_err("recover");
    assert!(matches!(e, StoreError::StorageQuota), "{e:?}");
    assert!(m.list_versions(parent.id).await.expect("list").is_empty());
}

// ---- GC hold -----------------------------------------------------------------------------------

/// §1, §7: the `from_version` of the last 3 switches survives `gc_versions`, even with a retention
/// of 1; the older one and anything else out of the window go.
#[tokio::test]
async fn gc_keeps_from_version_of_last_three_switches() {
    let (m, _t, parent, _b) = family().await;
    mk_versions(&m, parent.id, 1, 8).await;
    m.set_live(parent.id, VersionNo(8), None).await.expect("live");
    for from in 1..=4 {
        let s = m
            .create_switch(
                parent.id,
                NewSwitch {
                    from_version: Some(VersionNo(from)),
                    ..promote(from + 1)
                },
            )
            .await
            .expect("switch");
        m.update_switch(parent.id, s.id, to_state(SwitchState::Completed))
            .await
            .expect("done");
    }
    // A row with no `from_version` (first deploy, or never flipped) does not use up a hold.
    let s = m.create_switch(parent.id, promote(8)).await.expect("no from");
    m.update_switch(parent.id, s.id, to_state(SwitchState::Failed))
        .await
        .expect("done");

    let freed = m.gc_versions(parent.id, 1).await.expect("gc");

    let left: Vec<u32> = m
        .list_versions(parent.id)
        .await
        .expect("list")
        .into_iter()
        .map(|v| v.number.0)
        .collect();
    assert_eq!(left, vec![2, 3, 4, 8], "held v2..v4 + live/newest v8");
    assert!(freed.contains(&digest_n(1)), "v1 is past the last three");
    assert!(!freed.contains(&digest_n(2)));
    assert!(m.digest_referenced(&digest_n(2)).await.expect("referenced"));
}

/// The hold is per service: another service's switches do not protect this one's numbers.
#[tokio::test]
async fn the_gc_hold_only_reads_the_services_own_switches() {
    let (m, _t, parent, branch) = family().await;
    mk_versions(&m, parent.id, 1, 3).await;
    mk_versions(&m, branch.id, 11, 3).await;
    m.create_switch(
        branch.id,
        NewSwitch {
            from_version: Some(VersionNo(1)),
            ..promote(2)
        },
    )
    .await
    .expect("switch");
    m.gc_versions(parent.id, 1).await.expect("gc");
    assert_eq!(m.list_versions(parent.id).await.expect("list").len(), 1);
}

// ---- upgrade -----------------------------------------------------------------------------------

/// A populated schema-1 `meta.db` upgrades in place: rows survive with `origin = NULL`, and the new
/// table and column work.
#[tokio::test]
async fn a_populated_v1_database_upgrades_in_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("meta.db");
    let (parent, branch, old) = {
        let m = SqliteMetaStore::open(&path).expect("create");
        let t = mk_tenant(&m, "t").await;
        let parent = mk_service(&m, &t, "parent").await;
        let branch = mk_service(&m, &t, "branch").await;
        let old = ready_version(&m, parent.id, 1).await;
        ready_version(&m, branch.id, 2).await;
        m.set_live(parent.id, old.number, None).await.expect("live");
        (parent, branch, old)
    };
    // Back to the exact schema-1 shape, with the rows in place.
    let c = rusqlite::Connection::open(&path).expect("open");
    c.execute_batch("DROP TABLE secret_refs; DROP TABLE pending_imports; DROP TABLE version_switches; DROP TABLE IF EXISTS backups; ALTER TABLE versions DROP COLUMN origin; ALTER TABLE versions DROP COLUMN migrations; ALTER TABLE versions DROP COLUMN migrations_sql; ALTER TABLE versions DROP COLUMN meta; ALTER TABLE versions DROP COLUMN activate_after_build; PRAGMA user_version = 1;")
        .expect("downgrade");
    drop(c);

    let m = SqliteMetaStore::open(&path).expect("upgrade");

    let newest = rt_store::META_MIGRATIONS.iter().map(|(v, _)| *v).max().expect("newest");
    assert!(newest >= 2);
    assert_eq!(m.schema_version().await.expect("version"), newest);
    assert_eq!(m.get_version(parent.id, old.number).await.expect("get"), Some(old));
    assert_eq!(
        m.get_service(TenantScope::Operator, parent.id)
            .await
            .expect("get")
            .expect("exists")
            .live_version,
        Some(VersionNo(1))
    );
    let adopted = m
        .adopt_version(parent.id, branch.id, VersionNo(1))
        .await
        .expect("adopt");
    assert_eq!(adopted.number, VersionNo(2));
    m.create_switch(parent.id, promote(2)).await.expect("switch");
}

// ---- the flip -----------------------------------------------------------------------------------

/// §5 step 5: the live version and the row change together; the row records what was live.
#[tokio::test]
async fn flip_sets_live_and_moves_the_row_to_draining_with_from_and_deadline() {
    let (m, _t, s, _b) = family().await;
    mk_versions(&m, s.id, 50, 2).await;
    m.set_live(s.id, VersionNo(1), None).await.expect("live");
    let row = m.create_switch(s.id, promote(2)).await.expect("create");

    let (svc, flipped) = m.flip_switch(s.id, row.id, None, 5_000).await.expect("flip");

    assert_eq!(svc.live_version, Some(VersionNo(2)));
    assert_eq!(flipped.state, SwitchState::Draining);
    assert_eq!(flipped.from_version, Some(VersionNo(1)));
    assert_eq!(flipped.drain_deadline_ms, Some(5_000));
    assert_eq!(m.get_switch(s.id, row.id).await.expect("get"), Some(flipped));
    let got = m
        .get_service(TenantScope::Operator, s.id)
        .await
        .expect("get")
        .expect("svc");
    assert_eq!(got.live_version, Some(VersionNo(2)));
    assert_eq!(got.etag, svc.etag);
}

/// Nothing was running on another version, so there is nothing to drain.
#[tokio::test]
async fn flip_with_nothing_to_drain_completes_at_once() {
    let (m, _t, s, _b) = family().await;
    mk_versions(&m, s.id, 50, 1).await;
    let first = m.create_switch(s.id, promote(1)).await.expect("create");
    let (_, row) = m.flip_switch(s.id, first.id, None, 5_000).await.expect("first flip");
    assert_eq!((row.state, row.from_version), (SwitchState::Completed, None));

    let same = m.create_switch(s.id, promote(1)).await.expect("create");
    let (_, row) = m.flip_switch(s.id, same.id, None, 5_000).await.expect("same version");
    assert_eq!(row.state, SwitchState::Completed);
    assert_eq!(row.from_version, Some(VersionNo(1)));
}

/// A refused flip changes neither the live version nor the row: both are one transaction.
#[tokio::test]
async fn a_refused_flip_changes_nothing() {
    let (m, _t, s, _b) = family().await;
    mk_versions(&m, s.id, 50, 2).await;
    m.set_live(s.id, VersionNo(1), None).await.expect("live");
    let row = m.create_switch(s.id, promote(2)).await.expect("create");

    let stale = rt_types::Etag(s.etag.0 + 1000);
    assert!(matches!(
        m.flip_switch(s.id, row.id, Some(stale), 5_000).await,
        Err(StoreError::PreconditionFailed)
    ));
    m.set_build_state(
        s.id,
        VersionNo(2),
        BuildState::Failed,
        Some(BuildFailure::new(BuildFailReason::Internal, "x")),
        None,
    )
    .await
    .expect("failed");
    assert!(matches!(
        m.flip_switch(s.id, row.id, None, 5_000).await,
        Err(StoreError::Conflict("version_not_ready"))
    ));

    let got = m
        .get_service(TenantScope::Operator, s.id)
        .await
        .expect("get")
        .expect("svc");
    assert_eq!(got.live_version, Some(VersionNo(1)));
    let unchanged = m.get_switch(s.id, row.id).await.expect("get").expect("row");
    assert_eq!(
        (unchanged.state, unchanged.from_version),
        (SwitchState::Preparing, None)
    );
}

/// A store failure at the commit point leaves the previous version live.
#[tokio::test]
async fn a_flip_the_store_fails_leaves_the_previous_version_live() {
    let (m, _t, s, _b) = family().await;
    mk_versions(&m, s.id, 50, 2).await;
    m.set_live(s.id, VersionNo(1), None).await.expect("live");
    let row = m.create_switch(s.id, promote(2)).await.expect("create");
    let faults = Faults::new();
    m.inject_faults(faults.clone());
    faults.fail_next(1, Fault::Io);
    assert!(m.flip_switch(s.id, row.id, None, 5_000).await.is_err());
    let got = m
        .get_service(TenantScope::Operator, s.id)
        .await
        .expect("get")
        .expect("svc");
    assert_eq!(got.live_version, Some(VersionNo(1)));
    assert_eq!(
        m.get_switch(s.id, row.id).await.expect("get").expect("row").state,
        SwitchState::Preparing
    );
}

/// A row that already flipped, failed, or is another service's cannot flip.
#[tokio::test]
async fn only_an_open_row_of_the_service_can_flip() {
    let (m, _t, s, b) = family().await;
    mk_versions(&m, s.id, 50, 2).await;
    mk_versions(&m, b.id, 60, 1).await;
    let row = m.create_switch(s.id, promote(1)).await.expect("create");
    assert!(matches!(
        m.flip_switch(b.id, row.id, None, 5_000).await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        m.flip_switch(s.id, SwitchId::new(), None, 5_000).await,
        Err(StoreError::NotFound)
    ));
    m.flip_switch(s.id, row.id, None, 5_000).await.expect("flip");
    assert!(matches!(
        m.flip_switch(s.id, row.id, None, 5_000).await,
        Err(StoreError::Conflict("switch_not_open"))
    ));
    assert!(matches!(
        m.flip_switch(ServiceId::from_u128(77), row.id, None, 5_000).await,
        Err(StoreError::NotFound)
    ));
}

/// A deploy's `meta` is on the version and survives adoption; the `after_build` activation is
/// handed out once and is not adopted.
#[tokio::test]
async fn version_deploy_info_is_kept_and_an_activation_is_taken_once() {
    let (m, _tenant, parent, source) = family().await;
    let (a, b) = (source.id, parent.id);
    let v = ready_version(&m, a, 1).await;
    let meta = serde_json::json!({"git": {"sha": "abc1234"}});
    m.set_version_deploy(a, v.number, Some(meta.clone()), Some("key:1".into()))
        .await
        .unwrap();
    let read = m.get_version(a, v.number).await.unwrap().unwrap();
    assert_eq!(read.meta.as_ref(), Some(&meta));
    assert_eq!(read.git_sha(), Some("abc1234"));
    assert_eq!(m.list_versions(a).await.unwrap()[0].git_sha(), Some("abc1234"));

    let adopted = m.adopt_version(b, a, v.number).await.unwrap();
    assert_eq!(adopted.git_sha(), Some("abc1234"));
    let stored = m.get_version(b, adopted.number).await.unwrap().unwrap();
    assert_eq!(stored.git_sha(), Some("abc1234"));
    assert_eq!(m.take_version_activation(b, adopted.number).await.unwrap(), None);

    assert_eq!(
        m.take_version_activation(a, v.number).await.unwrap().as_deref(),
        Some("key:1")
    );
    assert_eq!(m.take_version_activation(a, v.number).await.unwrap(), None);
    assert!(m
        .set_version_deploy(a, rt_types::VersionNo(99), None, None)
        .await
        .is_err());
}
