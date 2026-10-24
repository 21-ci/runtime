//! `meta.db` key scope and integrity (SECURITY.md, review P1/P2): a key is scoped by
//! projects and environments through rows that cascade with their project, a service can only
//! belong to a project of its own tenant, and a tenant's host releases go with the tenant.

use crate::common;

use common::*;
use rt_store::{HostRelease, MetaStore, PageReq, ServiceFilter, SqliteMetaStore, StoreError, TenantScope};
use rt_types::{KeyId, NewKey, NewProject, ProjectId, ProjectPatch, Scopes};

fn key(n: u128, project_ids: Option<Vec<ProjectId>>, environments: Option<Vec<&str>>) -> NewKey {
    NewKey {
        id: KeyId::from_u128(n),
        name: None,
        scopes: Scopes::all(),
        project_ids,
        environments: environments.map(|e| e.into_iter().map(String::from).collect()),
        expires_at_ms: None,
        hmac: vec![n as u8; 32],
    }
}

async fn project_id(m: &SqliteMetaStore, tenant: rt_types::TenantId, slug: &str) -> ProjectId {
    m.create_project(tenant, NewProject::new(slug))
        .await
        .expect("project")
        .id
}

fn sorted_names(items: &[rt_types::Service]) -> Vec<String> {
    let mut v: Vec<String> = items.iter().map(|s| s.slug.clone()).collect();
    v.sort();
    v
}

#[tokio::test]
async fn a_key_round_trips_none_as_all_and_an_empty_list_as_nothing() {
    let m = meta();
    let t = mk_tenant(&m, "shape").await;
    let p = project_id(&m, t.id, "web").await;

    let all = m.create_key(t.id, key(1, None, None)).await.expect("all");
    assert_eq!((all.project_ids.clone(), all.environments.clone()), (None, None));
    let some = m
        .create_key(t.id, key(2, Some(vec![p]), Some(vec!["staging"])))
        .await
        .expect("some");
    assert_eq!(some.project_ids, Some(vec![p]));
    assert_eq!(some.environments, Some(vec!["staging".to_string()]));
    let none = m
        .create_key(t.id, key(3, Some(vec![]), Some(vec![])))
        .await
        .expect("none");
    assert_eq!(
        (none.project_ids.clone(), none.environments.clone()),
        (Some(vec![]), Some(vec![]))
    );

    let back = m.find_key(KeyId::from_u128(2)).await.expect("find").expect("key");
    assert_eq!(
        (back.project_ids, back.environments),
        (some.project_ids, some.environments)
    );
    let back = m.find_key(KeyId::from_u128(3)).await.expect("find").expect("key");
    assert_eq!((back.project_ids, back.environments), (Some(vec![]), Some(vec![])));
}

#[tokio::test]
async fn deleting_a_project_removes_the_key_rows_that_named_it() {
    let m = meta();
    let t = mk_tenant(&m, "cascade").await;
    let a = project_id(&m, t.id, "alpha").await;
    let b = project_id(&m, t.id, "beta").await;
    m.create_key(t.id, key(1, Some(vec![a, b]), None)).await.expect("key");

    m.delete_project(t.id, "alpha").await.expect("delete project");

    let k = m.find_key(KeyId::from_u128(1)).await.expect("find").expect("key");
    assert_eq!(k.project_ids, Some(vec![b]), "the key lost alpha and kept beta");
}

#[tokio::test]
async fn a_key_cannot_name_a_project_of_another_tenant() {
    let m = meta();
    let a = mk_tenant(&m, "owner").await;
    let b = mk_tenant(&m, "other").await;
    let foreign = project_id(&m, b.id, "theirs").await;
    let r = m.create_key(a.id, key(1, Some(vec![foreign]), None)).await;
    assert!(
        r.is_err(),
        "the composite foreign key refuses another tenant's project: {r:?}"
    );
    assert!(
        m.find_key(KeyId::from_u128(1)).await.expect("find").is_none(),
        "nothing was stored"
    );
}

#[tokio::test]
async fn a_project_key_lists_only_the_services_of_its_projects_and_environments() {
    let m = meta();
    let t = mk_tenant(&m, "scoped").await;
    let web = project_id(&m, t.id, "web").await;
    let api = project_id(&m, t.id, "api").await;
    m.update_project(
        t.id,
        "web",
        ProjectPatch {
            environments: Some(vec!["production".into(), "staging".into()]),
            ..ProjectPatch::default()
        },
    )
    .await
    .expect("envs");
    for (slug, project, env) in [
        ("w-prod", "web", "production"),
        ("w-stage", "web", "staging"),
        ("a-prod", "api", "production"),
    ] {
        let mut s = new_service(slug);
        s.project = project.into();
        s.environment = env.into();
        m.create_service(TenantScope::Operator, t.id, s).await.expect("service");
    }
    let list = |filter: ServiceFilter| {
        let m = m.clone();
        async move {
            let page = m
                .list_services(TenantScope::Tenant(t.id), filter, PageReq::default())
                .await
                .expect("list");
            sorted_names(&page.items)
        }
    };
    let by_project = ServiceFilter {
        project_ids: Some(vec![web]),
        ..ServiceFilter::default()
    };
    assert_eq!(list(by_project).await, vec!["w-prod", "w-stage"]);
    let prod_only = ServiceFilter {
        project_ids: Some(vec![web, api]),
        environments: Some(vec!["production".into()]),
        ..ServiceFilter::default()
    };
    assert_eq!(list(prod_only).await, vec!["a-prod", "w-prod"]);
    let nothing = ServiceFilter {
        project_ids: Some(vec![]),
        ..ServiceFilter::default()
    };
    assert!(
        list(nothing).await.is_empty(),
        "an explicit empty project list lists nothing"
    );
}

#[tokio::test]
async fn a_service_cannot_be_created_in_another_tenants_project() {
    let m = meta();
    let a = mk_tenant(&m, "a").await;
    let b = mk_tenant(&m, "b").await;
    let theirs = m
        .get_project(a.id, "default")
        .await
        .expect("project")
        .expect("default")
        .id;
    let mut s = new_service("intruder");
    s.project = theirs.to_string();
    let r = m.create_service(TenantScope::Operator, b.id, s).await;
    assert!(matches!(r, Err(StoreError::Invalid(_))), "{r:?}");
    assert!(m
        .get_service(TenantScope::Operator, rt_types::ServiceId::new())
        .await
        .expect("get")
        .is_none());
    assert_eq!(m.count_services(b.id).await.expect("count"), 0);
}

#[tokio::test]
async fn project_environments_keep_their_order_and_belong_to_their_project() {
    let m = meta();
    let t = mk_tenant(&m, "envs").await;
    let p = m
        .create_project(
            t.id,
            NewProject {
                environments: vec!["production".into(), "staging".into(), "pr-1".into()],
                ..NewProject::new("ordered")
            },
        )
        .await
        .expect("project");
    let got = m.get_project(t.id, "ordered").await.expect("get").expect("exists");
    assert_eq!(got.environments, vec!["production", "staging", "pr-1"]);
    assert_eq!(got.id, p.id);

    // The same label in another project is another row.
    let other = m
        .create_project(
            t.id,
            NewProject {
                environments: vec!["staging".into()],
                ..NewProject::new("other")
            },
        )
        .await
        .expect("other");
    assert_eq!(other.environments, vec!["staging"]);

    m.update_project(
        t.id,
        "ordered",
        ProjectPatch {
            environments: Some(vec!["staging".into(), "production".into()]),
            ..ProjectPatch::default()
        },
    )
    .await
    .expect("reorder");
    let got = m.get_project(t.id, "ordered").await.expect("get").expect("exists");
    assert_eq!(
        got.environments,
        vec!["staging", "production"],
        "the list is the order the caller gave"
    );
}

#[tokio::test]
async fn deleting_a_tenant_takes_its_host_releases_along() {
    let m = meta();
    let t = mk_tenant(&m, "released").await;
    mk_service(&m, &t, "released-svc").await;
    m.record_host_release(HostRelease {
        host: "gone.example.com".into(),
        tenant: t.id,
        created_at_ms: now_ms(),
        verify_token: Some("tok".into()),
        released_at_ms: now_ms(),
    })
    .await
    .expect("release");
    assert!(m.host_release("gone.example.com", t.id).await.expect("find").is_some());

    m.delete_tenant(t.id, true).await.expect("purge");
    assert!(
        m.host_release("gone.example.com", t.id).await.expect("find").is_none(),
        "the release cascades with the tenant"
    );
}
