//! Projects (decision D-B3, docs/api.md): `POST|GET /v1/projects`,
//! `GET|PATCH|DELETE /v1/projects/{p}` and `GET /v1/projects/{p}/environments`.
//!
//! A project groups the services of one tenant and lists the environments they may be in. `{p}` is
//! the project's slug or its `prj_…` id. Reads need `services:read`, changes `services:write` and
//! a key that is not limited to some projects or environments (a project is a tenant-wide resource).

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use rt_types::{NewProject, Project, ProjectId, ProjectPatch, Scope, TenantId};
use serde_json::{json, Value};

use crate::auth::Auth;
use crate::control::{audit, fingerprint, idem_replay, idem_store, page_req, value_resp};
use crate::dto::{CreateProjectBody, EnvironmentsView, PageQuery, PatchProjectBody, ProjectPage, ProjectView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::state::ApiState;

type R<T> = Result<Json<T>, ApiError>;

pub(crate) fn project_view(p: &Project) -> ProjectView {
    ProjectView {
        id: p.id.to_string(),
        tenant_id: p.tenant_id.to_string(),
        slug: p.slug.clone(),
        name: p.name.clone(),
        environments: p.environments.clone(),
        created_at: p.created_at_ms,
        updated_at: p.updated_at_ms,
    }
}

/// A project of `tenant` by slug or `prj_…` id.
pub(crate) async fn find_project(s: &ApiState, tenant: TenantId, r: &str) -> Result<Option<Project>, ApiError> {
    if r.starts_with("prj_") {
        return match r.parse::<ProjectId>() {
            Ok(id) => Ok(s.meta.get_project_by_id(tenant, id).await?),
            Err(_) => Ok(None),
        };
    }
    Ok(s.meta.get_project(tenant, r).await?)
}

/// The slug of the project a request names (`None`: `default`). The project must exist; the
/// tenant's `default` project always passes, the store re-creates it when it was deleted.
pub(crate) async fn project_slug_of(s: &ApiState, tenant: TenantId, named: Option<&str>) -> Result<String, ApiError> {
    let Some(named) = named else {
        return Ok(rt_types::default_project());
    };
    match find_project(s, tenant, named).await? {
        Some(p) => Ok(p.slug),
        None if named == rt_types::DEFAULT_PROJECT => Ok(rt_types::default_project()),
        None => Err(ApiError::invalid(format!(
            "project `{named}` does not exist; create it with POST /v1/projects"
        ))
        .with_details(json!({"project": named}))),
    }
}

/// `422 validation_failed` unless `environment` is one of the project's environments (the store
/// checks again atomically).
pub(crate) async fn check_environment_in_project(
    s: &ApiState,
    tenant: TenantId,
    project: &str,
    environment: &str,
) -> Result<(), ApiError> {
    let allowed = match s.meta.get_project(tenant, project).await? {
        Some(p) => p.environments,
        None if project == rt_types::DEFAULT_PROJECT => rt_types::default_project_environments(),
        None => {
            return Err(ApiError::invalid(format!("project `{project}` does not exist"))
                .with_details(json!({"project": project})))
        }
    };
    if allowed.iter().any(|e| e == environment) {
        return Ok(());
    }
    Err(ApiError::invalid(format!(
        "environment `{environment}` is not an environment of project `{project}` (allowed: {}); add it with \
         PATCH /v1/projects/{project}",
        allowed.join(", ")
    ))
    .with_details(json!({"project": project, "environment": environment, "environments": allowed})))
}

/// Projects are tenant-wide: a key limited to some projects or environments may read them, not change them.
fn require_unrestricted(a: &Auth) -> Result<(), ApiError> {
    if crate::control::restricted(&a.principal) {
        return Err(ApiError::new(
            "insufficient_scope",
            "changing projects needs the operator or a tenant key that is not limited to some projects or environments",
        ));
    }
    Ok(())
}

fn check_slug(slug: &str) -> Result<(), ApiError> {
    if rt_types::valid_project_slug(slug) {
        Ok(())
    } else {
        Err(ApiError::invalid("`slug` must match ^[a-z][a-z0-9-]{1,62}$"))
    }
}

fn check_name(name: &str) -> Result<(), ApiError> {
    crate::control::check_display_name("name", name)
}

fn check_environments(envs: &[String]) -> Result<(), ApiError> {
    rt_types::check_project_environments(envs).map_err(ApiError::invalid)
}

async fn load(s: &ApiState, a: &Auth, p: &str) -> Result<Project, ApiError> {
    let tenant = a.tenant()?;
    find_project(s, tenant, p).await?.ok_or_else(ApiError::not_found)
}

/// Create a project. Its slug is unique within the tenant and cannot change.
#[utoipa::path(post, path = "/v1/projects", tag = "projects", request_body = CreateProjectBody,
    responses((status = 201, body = ProjectView), (status = 403, body = ErrorEnvelope),
        (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn projects_create(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateProjectBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    require_unrestricted(&a)?;
    let tenant = a.tenant()?;
    let fp = fingerprint(&[b"projects", tenant.to_string().as_bytes(), format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    check_slug(&b.slug)?;
    if let Some(n) = &b.name {
        check_name(n)?;
    }
    let environments = b.environments.unwrap_or_else(rt_types::default_project_environments);
    check_environments(&environments)?;
    let project = s
        .meta
        .create_project(
            tenant,
            NewProject {
                slug: b.slug,
                name: b.name,
                environments,
            },
        )
        .await?;
    audit(
        &s,
        &a,
        "project.created",
        Some(tenant),
        None,
        json!({"project": project.slug, "id": project.id.to_string(), "environments": project.environments}),
    );
    let view = serde_json::to_value(project_view(&project)).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

/// The tenant's projects, ordered by slug.
#[utoipa::path(get, path = "/v1/projects", tag = "projects", params(PageQuery),
    responses((status = 200, body = ProjectPage)))]
pub(crate) async fn projects_list(
    State(s): State<ApiState>,
    a: Auth,
    ApiQuery(q): ApiQuery<PageQuery>,
) -> R<ProjectPage> {
    a.require_scope(Scope::ServicesRead)?;
    let tenant = a.tenant()?;
    let page = s.meta.list_projects(tenant, page_req(q.limit, q.cursor)).await?;
    Ok(Json(ProjectPage {
        items: page.items.iter().map(project_view).collect(),
        next_cursor: page.next_cursor,
    }))
}

#[utoipa::path(get, path = "/v1/projects/{p}", tag = "projects",
    params(("p" = String, Path, description = "project slug or prj_ id")),
    responses((status = 200, body = ProjectView), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn projects_get(State(s): State<ApiState>, a: Auth, Path(p): Path<String>) -> R<ProjectView> {
    a.require_scope(Scope::ServicesRead)?;
    Ok(Json(project_view(&load(&s, &a, &p).await?)))
}

/// Rename a project or replace its environment list. A service in an environment you drop makes
/// the change `409 environment_in_use`.
#[utoipa::path(patch, path = "/v1/projects/{p}", tag = "projects", request_body = PatchProjectBody,
    params(("p" = String, Path, description = "project slug or prj_ id")),
    responses((status = 200, body = ProjectView), (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn projects_patch(
    State(s): State<ApiState>,
    a: Auth,
    Path(p): Path<String>,
    ApiJson(b): ApiJson<PatchProjectBody>,
) -> R<ProjectView> {
    a.require_scope(Scope::ServicesWrite)?;
    require_unrestricted(&a)?;
    let cur = load(&s, &a, &p).await?;
    if let Some(n) = &b.name {
        check_name(n)?;
    }
    if let Some(e) = &b.environments {
        check_environments(e)?;
    }
    let updated = s
        .meta
        .update_project(
            cur.tenant_id,
            &cur.slug,
            ProjectPatch {
                name: b.name,
                environments: b.environments,
            },
        )
        .await?;
    audit(
        &s,
        &a,
        "project.updated",
        Some(cur.tenant_id),
        None,
        json!({"project": cur.slug, "environments": updated.environments, "was": cur.environments}),
    );
    Ok(Json(project_view(&updated)))
}

/// Delete a project that has no services.
#[utoipa::path(delete, path = "/v1/projects/{p}", tag = "projects",
    params(("p" = String, Path, description = "project slug or prj_ id")),
    responses((status = 204), (status = 404, body = ErrorEnvelope), (status = 409, body = ErrorEnvelope)))]
pub(crate) async fn projects_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(p): Path<String>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    require_unrestricted(&a)?;
    let cur = load(&s, &a, &p).await?;
    s.meta.delete_project(cur.tenant_id, &cur.slug).await?;
    audit(
        &s,
        &a,
        "project.deleted",
        Some(cur.tenant_id),
        None,
        json!({"project": cur.slug}),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// The environments services of the project may be in.
#[utoipa::path(get, path = "/v1/projects/{p}/environments", tag = "projects",
    params(("p" = String, Path, description = "project slug or prj_ id")),
    responses((status = 200, body = EnvironmentsView), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn environments_get(State(s): State<ApiState>, a: Auth, Path(p): Path<String>) -> R<EnvironmentsView> {
    a.require_scope(Scope::ServicesRead)?;
    let project = load(&s, &a, &p).await?;
    Ok(Json(EnvironmentsView {
        project: project.slug,
        environments: project.environments,
    }))
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};

    use super::*;
    use crate::test_support;

    /// A malformed `prj_` id names no project (it is not an error to look one up).
    #[tokio::test]
    async fn a_malformed_project_id_finds_no_project() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        assert!(find_project(&node.state, t, "prj_not-a-project-id")
            .await
            .expect("lookup")
            .is_none());
    }

    /// The `default` project always exists with its own environments; any other unknown project
    /// is refused by name, and an environment outside the project's list is a validation error.
    #[tokio::test]
    async fn environments_are_checked_against_the_project_that_names_them() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        let env = rt_types::default_project_environments().remove(0);
        check_environment_in_project(&node.state, t, rt_types::DEFAULT_PROJECT, &env)
            .await
            .expect("default environment");

        let e = check_environment_in_project(&node.state, t, rt_types::DEFAULT_PROJECT, "not-a-default")
            .await
            .expect_err("not one of the default project's environments");
        assert_eq!(e.code, "validation_failed");

        let e = check_environment_in_project(&node.state, t, "ghost", &env)
            .await
            .expect_err("no such project");
        assert_eq!(e.code, "validation_failed");
        assert_eq!(e.details["project"], "ghost");
    }

    /// A request that names no project gets `default`; an unknown name is refused with its name.
    #[tokio::test]
    async fn a_request_without_a_project_lands_in_default_and_unknown_names_are_refused() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        assert_eq!(
            project_slug_of(&node.state, t, None).await.expect("default"),
            rt_types::default_project()
        );
        let e = project_slug_of(&node.state, t, Some("ghost"))
            .await
            .expect_err("unknown project");
        assert_eq!(e.details["project"], "ghost");
    }

    /// A repeated `POST /v1/projects` with the same `Idempotency-Key` answers the first response
    /// and creates nothing new.
    #[tokio::test]
    async fn a_repeated_project_create_with_the_same_key_replays_the_first_answer() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        let body = serde_json::json!({"slug": "shop"});
        let headers = [("idempotency-key", "create-shop-1")];
        let (first, _, one) = node
            .call_with(Method::POST, "/v1/projects", Some(t), &headers, Some(body.clone()))
            .await;
        assert_eq!(first, StatusCode::CREATED, "{one}");
        let (second, _, two) = node
            .call_with(Method::POST, "/v1/projects", Some(t), &headers, Some(body))
            .await;
        assert_eq!(second, StatusCode::CREATED);
        assert_eq!(one, two, "the replay is the stored answer");
        let (_, _, listed) = node.call(Method::GET, "/v1/projects", Some(t), None).await;
        let shops = listed["items"]
            .as_array()
            .expect("items")
            .iter()
            .filter(|p| p["slug"] == "shop")
            .count();
        assert_eq!(shops, 1, "the replay created nothing");
    }
}
