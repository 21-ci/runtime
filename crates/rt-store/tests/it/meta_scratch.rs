//! `expired_scratch` and the project environment methods behind branch services.

use crate::common;

use common::*;
use rt_store::{MetaStore, SqliteMetaStore, StoreError, TenantScope};
use rt_types::{ScratchConfig, ScratchKind, Service, ServiceConfig, Tenant, DEFAULT_PROJECT, MAX_PROJECT_ENVIRONMENTS};

async fn scratch(m: &SqliteMetaStore, t: &Tenant, slug: &str, expires_at: Option<i64>, environment: &str) -> Service {
    let parent = svc(0xABC);
    let config = ServiceConfig {
        scratch: Some(ScratchConfig {
            of: Some(parent),
            kind: ScratchKind::Branch,
            name: slug.to_string(),
            expires_at,
            copied_data: false,
            schedules: false,
            events: false,
            own_environment: false,
        }),
        ..ServiceConfig::default()
    };
    m.add_project_environment(t.id, DEFAULT_PROJECT, environment)
        .await
        .expect("environment");
    m.create_service(
        TenantScope::Tenant(t.id),
        t.id,
        rt_types::NewService {
            config,
            environment: environment.to_string(),
            ..new_service(slug)
        },
    )
    .await
    .expect("scratch service")
}

fn ids(v: &[Service]) -> Vec<String> {
    v.iter().map(|s| s.slug.clone()).collect()
}

/// Only scratch services at or past their time are due: not a later one, one with no expiry, or a
/// plain service; any tenant's count.
#[tokio::test]
async fn expired_scratch_finds_only_due_services_of_any_tenant() {
    let m = meta();
    let (a, b) = (mk_tenant(&m, "a").await, mk_tenant(&m, "b").await);
    mk_service(&m, &a, "plain").await;
    scratch(&m, &a, "due-a", Some(1000), "br-1").await;
    scratch(&m, &b, "due-b", Some(999), "br-1").await;
    scratch(&m, &a, "later", Some(1001), "br-2").await;
    scratch(&m, &a, "never", None, "br-3").await;

    let mut due = ids(&m.expired_scratch(1000, 10).await.expect("due"));
    due.sort();
    assert_eq!(due, vec!["due-a", "due-b"], "the boundary counts as expired");
    assert!(m.expired_scratch(998, 10).await.expect("none").is_empty());
}

/// `limit` bounds a pass, and a deleted one stops being due, so the next pass gets the rest.
#[tokio::test]
async fn expired_scratch_is_bounded_and_makes_progress() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    for n in 0..3 {
        scratch(&m, &t, &format!("s{n}"), Some(5), "br-x").await;
    }
    let first = m.expired_scratch(5, 2).await.expect("first");
    assert_eq!(first.len(), 2);
    for s in &first {
        m.delete_service(TenantScope::Tenant(t.id), s.id, false)
            .await
            .expect("delete");
    }
    let rest = m.expired_scratch(5, 2).await.expect("rest");
    assert_eq!(rest.len(), 1);
    assert!(!first.iter().any(|s| s.id == rest[0].id));
    assert!(m.expired_scratch(5, 0).await.expect("limit 0").is_empty());
}

/// Adding is idempotent: `true` once, then `false`, without a second entry.
#[tokio::test]
async fn add_project_environment_reports_whether_it_added() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    assert!(m
        .add_project_environment(t.id, DEFAULT_PROJECT, "br-a")
        .await
        .expect("add"));
    assert!(!m
        .add_project_environment(t.id, DEFAULT_PROJECT, "br-a")
        .await
        .expect("again"));
    let p = m
        .get_project(t.id, DEFAULT_PROJECT)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(p.environments.iter().filter(|e| *e == "br-a").count(), 1);
}

/// An unknown project is `not_found`, a bad label and a project past the cap `validation_failed`.
#[tokio::test]
async fn add_project_environment_refusals() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    assert!(matches!(
        m.add_project_environment(t.id, "nope", "br-a").await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        m.add_project_environment(t.id, DEFAULT_PROJECT, "Not Valid").await,
        Err(StoreError::Invalid(_))
    ));

    let have = m
        .get_project(t.id, DEFAULT_PROJECT)
        .await
        .expect("get")
        .expect("exists")
        .environments
        .len();
    for n in have..MAX_PROJECT_ENVIRONMENTS {
        m.add_project_environment(t.id, DEFAULT_PROJECT, &format!("e{n}"))
            .await
            .expect("up to the cap");
    }
    assert!(matches!(
        m.add_project_environment(t.id, DEFAULT_PROJECT, "one-more").await,
        Err(StoreError::Invalid(_))
    ));
}

/// Concurrent adds keep each other's: one transaction each.
#[tokio::test]
async fn concurrent_adds_keep_each_others_environment() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let mut tasks = Vec::new();
    for n in 0..8 {
        let (m, id) = (m.clone(), t.id);
        tasks.push(tokio::spawn(async move {
            m.add_project_environment(id, DEFAULT_PROJECT, &format!("br-{n}")).await
        }));
    }
    for task in tasks {
        assert!(task.await.expect("join").expect("add"));
    }
    let p = m
        .get_project(t.id, DEFAULT_PROJECT)
        .await
        .expect("get")
        .expect("exists");
    for n in 0..8 {
        assert!(p.environments.contains(&format!("br-{n}")), "br-{n} lost");
    }
}

/// Dropping removes the label; one still in use, an unknown label or project is left alone quietly.
#[tokio::test]
async fn drop_project_environment_keeps_labels_in_use() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let in_use = scratch(&m, &t, "s", None, "br-used").await;
    m.add_project_environment(t.id, DEFAULT_PROJECT, "br-free")
        .await
        .expect("add");
    let envs = || async {
        m.get_project(t.id, DEFAULT_PROJECT)
            .await
            .expect("get")
            .expect("exists")
            .environments
    };

    m.drop_project_environment(t.id, DEFAULT_PROJECT, "br-free")
        .await
        .expect("drop");
    assert!(!envs().await.contains(&"br-free".to_string()));

    let before = envs().await;
    m.drop_project_environment(t.id, DEFAULT_PROJECT, "br-used")
        .await
        .expect("in use");
    m.drop_project_environment(t.id, DEFAULT_PROJECT, "no-such")
        .await
        .expect("unknown");
    m.drop_project_environment(t.id, "no-project", "br-used")
        .await
        .expect("no project");
    assert_eq!(envs().await, before);

    m.delete_service(TenantScope::Tenant(t.id), in_use.id, false)
        .await
        .expect("delete");
    m.drop_project_environment(t.id, DEFAULT_PROJECT, "br-used")
        .await
        .expect("free now");
    assert!(!envs().await.contains(&"br-used".to_string()));
}

/// Another tenant's project of the same name is another row.
#[tokio::test]
async fn project_environments_stay_within_their_tenant() {
    let m = meta();
    let (a, b) = (mk_tenant(&m, "a").await, mk_tenant(&m, "b").await);
    m.add_project_environment(a.id, DEFAULT_PROJECT, "br-a")
        .await
        .expect("add");
    let p = m
        .get_project(b.id, DEFAULT_PROJECT)
        .await
        .expect("get")
        .expect("exists");
    assert!(!p.environments.contains(&"br-a".to_string()));
}
