//! `meta.db` projects (decision D-B3): first-class per tenant (slug, name, environments); every
//! service belongs to one project and to one of its environments; every tenant has a `default`
//! project.

use crate::common;

use common::*;
use rt_store::{MetaStore, PageReq, ServiceFilter, SqliteMetaStore, StoreError, TenantScope};
use rt_types::{default_project_id, NewProject, ProjectId, ProjectPatch, ServicePatch, MAX_PROJECTS_PER_TENANT};

fn envs(v: &[&str]) -> Vec<String> {
    v.iter().map(|e| e.to_string()).collect()
}

fn project(slug: &str, environments: &[&str]) -> NewProject {
    NewProject {
        slug: slug.into(),
        name: None,
        environments: envs(environments),
    }
}

/// The service with this slug, from any tenant.
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

fn in_project(slug: &str, project: &str, environment: &str) -> rt_types::NewService {
    let mut s = new_service(slug);
    s.project = project.into();
    s.environment = environment.into();
    s
}

fn conflict(r: Result<impl std::fmt::Debug, StoreError>) -> &'static str {
    match r {
        Err(StoreError::Conflict(c)) => c,
        other => panic!("expected a conflict, got {other:?}"),
    }
}

// ---- migration ---------------------------------------------------------------------------------

#[tokio::test]
async fn a_new_tenant_has_a_default_project_with_the_production_environment() {
    let m = meta();
    let t = mk_tenant(&m, "acme").await;
    let p = m
        .get_project(t.id, "default")
        .await
        .expect("get")
        .expect("auto-created");
    assert_eq!(
        (p.slug.as_str(), p.name.as_str(), p.environments.clone()),
        ("default", "Default", envs(&["production"]))
    );
    assert_eq!(p.id, default_project_id(t.id));
    assert_eq!(m.get_project_by_id(t.id, p.id).await.expect("by id"), Some(p.clone()));
    let page = m.list_projects(t.id, PageReq::default()).await.expect("list");
    assert_eq!(page.items, vec![p]);
}

#[tokio::test]
async fn projects_are_created_read_listed_and_unique_per_tenant() {
    let m = meta();
    let (a, b) = (mk_tenant(&m, "a").await, mk_tenant(&m, "b").await);

    let web = m
        .create_project(a.id, project("web", &["production", "staging"]))
        .await
        .expect("create");
    assert_eq!(
        (web.slug.as_str(), web.name.as_str(), web.tenant_id),
        ("web", "web", a.id),
        "the name defaults to the slug"
    );
    assert_eq!(web.environments, envs(&["production", "staging"]));
    assert!(web.id.to_string().starts_with("prj_") && web.created_at_ms > 0 && web.updated_at_ms == web.created_at_ms);
    let mut named = NewProject::new("api");
    named.name = Some("The API".into());
    let api = m.create_project(a.id, named).await.expect("create");
    assert_eq!(
        (api.name.as_str(), api.environments.clone()),
        ("The API", envs(&["production"]))
    );

    assert_eq!(
        conflict(m.create_project(a.id, project("web", &["production"])).await),
        "project_slug_taken"
    );
    m.create_project(b.id, project("web", &["production"]))
        .await
        .expect("another tenant may use the slug");

    assert_eq!(m.get_project(a.id, "web").await.expect("get"), Some(web.clone()));
    assert_eq!(
        m.get_project(b.id, "api").await.expect("get"),
        None,
        "another tenant's project does not exist"
    );
    assert_eq!(
        m.get_project_by_id(b.id, web.id).await.expect("get"),
        None,
        "not even by id"
    );
    assert_eq!(m.get_project(a.id, "nope").await.expect("get"), None);

    let all = m.list_projects(a.id, PageReq::default()).await.expect("list");
    let slugs: Vec<&str> = all.items.iter().map(|p| p.slug.as_str()).collect();
    assert_eq!(slugs, ["api", "default", "web"], "ordered by slug");
    let first = m
        .list_projects(a.id, PageReq { limit: 2, cursor: None })
        .await
        .expect("page 1");
    assert_eq!(first.items.len(), 2);
    let cursor = first.next_cursor.clone().expect("more");
    let rest = m
        .list_projects(
            a.id,
            PageReq {
                limit: 2,
                cursor: Some(cursor),
            },
        )
        .await
        .expect("page 2");
    assert_eq!(rest.items.iter().map(|p| p.slug.as_str()).collect::<Vec<_>>(), ["web"]);
    assert_eq!(rest.next_cursor, None);
}

#[tokio::test]
async fn invalid_projects_are_rejected() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    for bad in ["", "a", "Web", "1x", "has space", &"x".repeat(64)] {
        let r = m.create_project(t.id, project(bad, &["production"])).await;
        assert!(matches!(r, Err(StoreError::Invalid(_))), "slug {bad:?}: {r:?}");
    }
    for bad in [
        envs(&[]),
        envs(&["Prod"]),
        envs(&["a", "a"]),
        (0..21).map(|i| format!("e{i}")).collect(),
    ] {
        let r = m
            .create_project(
                t.id,
                NewProject {
                    slug: "ok".into(),
                    name: None,
                    environments: bad.clone(),
                },
            )
            .await;
        assert!(matches!(r, Err(StoreError::Invalid(_))), "{bad:?}: {r:?}");
    }
    let long = NewProject {
        slug: "ok".into(),
        name: Some("n".repeat(101)),
        environments: envs(&["production"]),
    };
    assert!(
        matches!(m.create_project(t.id, long).await, Err(StoreError::Invalid(_))),
        "name over 100 characters"
    );
    let blank = NewProject {
        slug: "ok".into(),
        name: Some("  ".into()),
        environments: envs(&["production"]),
    };
    assert!(
        matches!(m.create_project(t.id, blank).await, Err(StoreError::Invalid(_))),
        "blank name"
    );
    assert!(matches!(
        m.create_project(rt_types::TenantId::new(), project("ok", &["production"]))
            .await,
        Err(StoreError::NotFound)
    ));
}

#[tokio::test]
async fn a_tenant_may_own_a_bounded_number_of_projects() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    for i in 0..MAX_PROJECTS_PER_TENANT - 1 {
        m.create_project(t.id, project(&format!("p{i:03}"), &["production"]))
            .await
            .expect("under the cap (default counts)");
    }
    assert_eq!(
        conflict(m.create_project(t.id, project("one-too-many", &["production"])).await),
        "quota_exceeded"
    );
}

#[tokio::test]
async fn update_replaces_the_name_and_the_environment_list() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    let web = m
        .create_project(t.id, project("web", &["production", "staging"]))
        .await
        .expect("create");

    let renamed = m
        .update_project(
            t.id,
            "web",
            ProjectPatch {
                name: Some("Web".into()),
                environments: None,
            },
        )
        .await
        .expect("rename");
    assert_eq!(
        (renamed.name.as_str(), renamed.environments.clone(), renamed.id),
        ("Web", envs(&["production", "staging"]), web.id)
    );
    assert!(renamed.updated_at_ms >= web.updated_at_ms);

    let grown = m
        .update_project(
            t.id,
            "web",
            ProjectPatch {
                name: None,
                environments: Some(envs(&["production", "staging", "dev"])),
            },
        )
        .await
        .expect("add an environment");
    assert_eq!(
        (grown.name.as_str(), grown.environments),
        ("Web", envs(&["production", "staging", "dev"]))
    );
    assert_eq!(
        m.get_project(t.id, "web")
            .await
            .expect("get")
            .expect("exists")
            .environments
            .len(),
        3,
        "persisted"
    );

    for bad in [envs(&[]), envs(&["Dev"]), envs(&["a", "a"])] {
        let r = m
            .update_project(
                t.id,
                "web",
                ProjectPatch {
                    name: None,
                    environments: Some(bad.clone()),
                },
            )
            .await;
        assert!(matches!(r, Err(StoreError::Invalid(_))), "{bad:?}: {r:?}");
    }
    assert!(matches!(
        m.update_project(t.id, "nope", ProjectPatch::default()).await,
        Err(StoreError::NotFound)
    ));
    let other = mk_tenant(&m, "other").await;
    assert!(matches!(
        m.update_project(other.id, "web", ProjectPatch::default()).await,
        Err(StoreError::NotFound)
    ));
}

#[tokio::test]
async fn an_environment_a_service_is_in_cannot_be_removed() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production", "staging", "dev"]))
        .await
        .expect("create");
    m.create_service(TenantScope::Tenant(t.id), t.id, in_project("front", "web", "staging"))
        .await
        .expect("service");

    let r = m
        .update_project(
            t.id,
            "web",
            ProjectPatch {
                name: None,
                environments: Some(envs(&["production", "dev"])),
            },
        )
        .await;
    assert_eq!(conflict(r), "environment_in_use");
    let kept = m.get_project(t.id, "web").await.expect("get").expect("exists");
    assert_eq!(
        kept.environments,
        envs(&["production", "staging", "dev"]),
        "a refused update changes nothing"
    );

    let ok = m
        .update_project(
            t.id,
            "web",
            ProjectPatch {
                name: None,
                environments: Some(envs(&["staging", "production"])),
            },
        )
        .await;
    assert_eq!(
        ok.expect("dev is unused").environments,
        envs(&["staging", "production"]),
        "order is the caller's"
    );
}

#[tokio::test]
async fn a_project_is_deleted_only_when_it_has_no_services() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production"]))
        .await
        .expect("create");
    let svc = m
        .create_service(
            TenantScope::Tenant(t.id),
            t.id,
            in_project("front", "web", "production"),
        )
        .await
        .expect("service");

    assert_eq!(conflict(m.delete_project(t.id, "web").await), "project_not_empty");
    assert!(m.get_project(t.id, "web").await.expect("get").is_some());

    m.delete_service(TenantScope::Tenant(t.id), svc.id, false)
        .await
        .expect("delete service");
    m.delete_project(t.id, "web").await.expect("empty now");
    assert_eq!(m.get_project(t.id, "web").await.expect("get"), None);
    assert!(
        matches!(m.delete_project(t.id, "web").await, Err(StoreError::NotFound)),
        "twice"
    );
    let other = mk_tenant(&m, "other").await;
    m.create_project(other.id, project("web", &["production"]))
        .await
        .expect("create");
    assert!(
        matches!(m.delete_project(t.id, "web").await, Err(StoreError::NotFound)),
        "another tenant's project is not ours"
    );
}

#[tokio::test]
async fn deleting_the_default_project_is_allowed_when_empty_and_it_comes_back_on_demand() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.delete_project(t.id, "default").await.expect("empty default project");
    assert_eq!(m.get_project(t.id, "default").await.expect("get"), None);
    let svc = m
        .create_service(TenantScope::Tenant(t.id), t.id, new_service("x"))
        .await
        .expect("a service without a project");
    assert_eq!(svc.project, "default");
    let back = m.get_project(t.id, "default").await.expect("get").expect("re-created");
    assert_eq!(
        (back.id, back.environments),
        (default_project_id(t.id), envs(&["production"]))
    );
}

// ---- services in projects ----------------------------------------------------------------------

#[tokio::test]
async fn a_service_is_created_in_a_project_and_one_of_its_environments() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production", "staging"]))
        .await
        .expect("create");

    let plain = mk_service(&m, &t, "plain").await;
    assert_eq!(
        (plain.project.as_str(), plain.environment.as_str()),
        ("default", "production")
    );

    let svc = m
        .create_service(TenantScope::Tenant(t.id), t.id, in_project("front", "web", "staging"))
        .await
        .expect("create");
    assert_eq!((svc.project.as_str(), svc.environment.as_str()), ("web", "staging"));
    let got = m
        .get_service(TenantScope::Operator, svc.id)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(got.project, "web");
    assert_eq!(by_slug(&m, "front").await.expect("exists").project, "web");

    // `dev` is not an environment of `web`; `staging` is not one of `default`.
    let r = m
        .create_service(TenantScope::Tenant(t.id), t.id, in_project("back", "web", "dev"))
        .await;
    assert!(matches!(r, Err(StoreError::Invalid(_))), "{r:?}");
    let r = m
        .create_service(
            TenantScope::Tenant(t.id),
            t.id,
            in_project("back", "default", "staging"),
        )
        .await;
    assert!(matches!(r, Err(StoreError::Invalid(_))), "{r:?}");
    let r = m
        .create_service(
            TenantScope::Tenant(t.id),
            t.id,
            in_project("back", "nope", "production"),
        )
        .await;
    assert!(matches!(r, Err(StoreError::Invalid(_))), "an unknown project: {r:?}");

    // Another tenant's project is not there to use.
    let other = mk_tenant(&m, "other").await;
    m.create_project(other.id, project("theirs", &["production"]))
        .await
        .expect("create");
    let r = m
        .create_service(
            TenantScope::Tenant(t.id),
            t.id,
            in_project("back", "theirs", "production"),
        )
        .await;
    assert!(matches!(r, Err(StoreError::Invalid(_))), "{r:?}");
    assert!(by_slug(&m, "back").await.is_none(), "nothing was created");
}

#[tokio::test]
async fn patching_a_services_environment_checks_its_projects_environments() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production", "staging"]))
        .await
        .expect("create");
    let svc = m
        .create_service(
            TenantScope::Tenant(t.id),
            t.id,
            in_project("front", "web", "production"),
        )
        .await
        .expect("service");
    let scope = TenantScope::Tenant(t.id);

    let to = |e: &str| ServicePatch {
        environment: Some(e.into()),
        ..Default::default()
    };
    assert_eq!(
        m.patch_service(scope, svc.id, to("staging"), None)
            .await
            .expect("a listed environment")
            .environment,
        "staging"
    );
    let r = m.patch_service(scope, svc.id, to("dev"), None).await;
    assert!(
        matches!(r, Err(StoreError::Invalid(_))),
        "an environment the project lacks: {r:?}"
    );
    assert_eq!(
        m.get_service(scope, svc.id)
            .await
            .expect("get")
            .expect("exists")
            .environment,
        "staging",
        "unchanged"
    );

    // After adding it to the project it is fine.
    m.update_project(
        t.id,
        "web",
        ProjectPatch {
            name: None,
            environments: Some(envs(&["production", "staging", "dev"])),
        },
    )
    .await
    .expect("add dev");
    assert_eq!(
        m.patch_service(scope, svc.id, to("dev"), None)
            .await
            .expect("now listed")
            .environment,
        "dev"
    );
    // A patch that leaves the environment alone never re-checks it.
    let p = ServicePatch {
        name: Some("Front".into()),
        ..Default::default()
    };
    assert_eq!(
        m.patch_service(scope, svc.id, p, None)
            .await
            .expect("rename")
            .name
            .as_deref(),
        Some("Front")
    );
}

#[tokio::test]
async fn services_are_filtered_by_project_and_environment() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production", "staging"]))
        .await
        .expect("create");
    m.create_project(t.id, project("jobs", &["production"]))
        .await
        .expect("create");
    for (slug, project, env) in [
        ("w-prod", "web", "production"),
        ("w-stg", "web", "staging"),
        ("j-prod", "jobs", "production"),
        ("d-prod", "default", "production"),
    ] {
        m.create_service(TenantScope::Tenant(t.id), t.id, in_project(slug, project, env))
            .await
            .expect("service");
    }
    let list = |project: Option<&str>, environment: Option<&str>| {
        let m = m.clone();
        let filter = ServiceFilter {
            slug: None,
            project: project.map(str::to_string),
            environment: environment.map(str::to_string),
            project_ids: None,
            environments: None,
        };
        async move {
            let mut slugs: Vec<String> = m
                .list_services(TenantScope::Tenant(t.id), filter, PageReq::default())
                .await
                .expect("list")
                .items
                .into_iter()
                .map(|s| s.slug)
                .collect();
            slugs.sort();
            slugs
        }
    };
    assert_eq!(list(None, None).await, ["d-prod", "j-prod", "w-prod", "w-stg"]);
    assert_eq!(list(Some("web"), None).await, ["w-prod", "w-stg"]);
    assert_eq!(list(None, Some("production")).await, ["d-prod", "j-prod", "w-prod"]);
    assert_eq!(list(Some("web"), Some("staging")).await, ["w-stg"]);
    assert_eq!(list(Some("jobs"), Some("staging")).await, Vec::<String>::new());
    assert_eq!(list(Some("nope"), None).await, Vec::<String>::new());
}

#[tokio::test]
async fn deleting_a_tenant_takes_its_projects_along() {
    let m = meta();
    let t = mk_tenant(&m, "t").await;
    m.create_project(t.id, project("web", &["production"]))
        .await
        .expect("create");
    m.delete_tenant(t.id, false).await.expect("no services: allowed");
    assert_eq!(m.get_project(t.id, "web").await.expect("get"), None);
    assert_eq!(m.get_project_by_id(t.id, ProjectId::new()).await.expect("get"), None);
}

#[tokio::test]
async fn the_busiest_service_is_the_one_with_the_most_schedules() {
    use rt_types::{NewSchedule, ScheduleKind};
    let m = meta();
    let (a, b) = (mk_tenant(&m, "a").await, mk_tenant(&m, "b").await);
    assert_eq!(
        m.busiest_service_schedules(a.id).await.expect("count"),
        0,
        "nothing scheduled"
    );
    let (s1, s2) = (mk_service(&m, &a, "one").await, mk_service(&m, &a, "two").await);
    let other = mk_service(&m, &b, "theirs").await;
    let every = |n: u64| NewSchedule {
        kind: ScheduleKind::Interval { every_seconds: n },
        payload: serde_json::Value::Null,
        enabled: true,
    };
    for n in [60, 120, 180] {
        m.create_schedule(s1.id, every(n)).await.expect("schedule");
    }
    m.create_schedule(s2.id, every(60)).await.expect("schedule");
    for n in [60, 120, 180, 240, 300] {
        m.create_schedule(other.id, every(n)).await.expect("schedule");
    }
    assert_eq!(m.busiest_service_schedules(a.id).await.expect("count"), 3);
    assert_eq!(m.busiest_service_schedules(b.id).await.expect("count"), 5, "per tenant");
}
