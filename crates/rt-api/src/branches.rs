//! Branch services (`docs/design/branching-services-design.md` §4, §6): a service with
//! `config.scratch`, created from a parent whose config it mirrors under a forced policy.
//!
//! A branch starts with no data unless `copy_data` asked otherwise, and never holds the parent's
//! secret values. Its forced fields are re-applied by the store on every `PATCH`
//! ([`rt_types::ServiceConfig::enforce_scratch_policy`]).

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{delete, post};
use axum::{Json, Router};
use rt_sec::{Actor, AuditEvent, QuotaOp};
use rt_store::{ServiceFilter, StoreError, TenantScope};
use rt_types::{
    EgressRules, NewService, Scope, ScratchConfig, ScratchKind, Service, ServiceConfig, TenantId, UnixMs,
    VersionOrigin, NO_EGRESS_HOST,
};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{
    audit, check_events_allowed, check_limits, check_scaling_bounds, fingerprint, idem_replay, idem_store, load_branch,
    load_service, not_system_parent, page_req, parse_environment, refresh_routes, remove_service, service_view,
    switch_actor, valid_branch_name, valid_slug, value_resp, warm_floor_cap,
};
use crate::dto::{PageQuery, ServicePage, ServiceView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::projects::check_environment_in_project;
use crate::promote::PromoteReq;
use crate::state::{ApiState, ControlEvent};

/// Expired branches removed per sweep; the rest wait for the next one.
const SWEEP_BATCH: u32 = 50;

/// `POST /v1/services/{s}/branches`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateBranchBody {
    /// `^[a-z][a-z0-9-]{0,23}$` without `--`; the service's slug is `<parent-slug>-br-<name>`.
    pub name: String,
    /// One of the project's environments; default a new label `br-<name>`, removed with the branch.
    pub environment: Option<String>,
    /// Copy the parent's data and blobs (`data:admin`; forces `access: key`). Default empty.
    #[serde(default)]
    pub copy_data: bool,
    /// Start from the parent's live version: adopted by digest and live on the branch before this
    /// call answers (`deploy`). Default none: the branch has no code until a push. A parent with
    /// nothing live leaves it so.
    #[serde(default)]
    pub code_of_parent: bool,
    /// Let interval and cron triggers run. Default off.
    #[serde(default)]
    pub schedules: bool,
    /// Keep the parent's pub/sub subscriptions and stream consumers (`deploy`). Default none: in a
    /// shared environment the branch would answer messages meant for the parent.
    #[serde(default)]
    pub events_of_parent: bool,
    /// Delete the branch and its data after this time (unix ms).
    pub expires_at: Option<UnixMs>,
    /// Add the names of the parent's env and secrets with empty values. Never a value.
    #[serde(default)]
    pub env_names_from_parent: bool,
    /// Keep the parent's egress rules (default); `false` allows no host.
    pub egress_of_parent: Option<bool>,
}

/// The parent's config under the branch's policy.
fn branch_config(parent: &Service, scratch: ScratchConfig, egress_of_parent: bool) -> ServiceConfig {
    let mut config = parent.config.clone();
    if !egress_of_parent {
        config.egress = EgressRules {
            allowed_hosts: vec![NO_EGRESS_HOST.to_string()],
            denied_hosts: Vec::new(),
        };
    }
    config.scratch = Some(scratch);
    config.enforce_scratch_policy();
    config
}

/// `409 branch_exists` while `svc` has branches: they would stay behind, hidden and counted.
pub(crate) async fn refuse_with_branches(s: &ApiState, svc: &Service) -> Result<(), ApiError> {
    let filter = ServiceFilter {
        scratch_of: Some(svc.id),
        ..ServiceFilter::default()
    };
    let page = s
        .meta
        .list_services(TenantScope::Tenant(svc.tenant_id), filter, page_req(Some(1), None))
        .await?;
    if page.items.is_empty() {
        return Ok(());
    }
    Err(ApiError::new(
        "branch_exists",
        "delete this service's branches first: GET /v1/services/{s}/branches",
    ))
}

/// The bytes `copy_data` would copy, against the node's cap and the tenant's storage.
pub(crate) async fn check_copy_size(s: &ApiState, parent: &Service) -> Result<(), ApiError> {
    let db = s.data.sql(parent.id).size_bytes().await?;
    let blobs = s.data.blob(parent.id).total_bytes().await?;
    let total = db.saturating_add(blobs);
    let max = u64::from(s.config.load().deploy.dry_run_max_mb).saturating_mul(1 << 20);
    if total > max {
        return Err(ApiError::new(
            "copy_data_too_large",
            format!("the parent holds {total} bytes; `deploy.dry_run_max_mb` allows {max}"),
        ));
    }
    s.quotas
        .check(parent.tenant_id, QuotaOp::Storage { delta_bytes: total })?;
    Ok(())
}

/// Puts the parent's live version live on the new branch. Its migrations run on the branch's own
/// data through the usual promote.
async fn adopt_parent_code(s: &ApiState, a: &Auth, parent: &Service, branch: &Service) -> Result<(), ApiError> {
    let Some(version) = parent.live_version else {
        return Ok(());
    };
    let origin = VersionOrigin {
        service: parent.id,
        version,
    };
    s.promoter
        .promote(branch, PromoteReq::adopt(origin).by(switch_actor(a)).no_wait())
        .await?;
    Ok(())
}

/// What runs after the row exists. A failure here takes the branch back out.
async fn fill_branch(s: &ApiState, parent: &Service, branch: &Service, b: &CreateBranchBody) -> Result<u64, ApiError> {
    if b.env_names_from_parent {
        let names = s
            .meta
            .get_env(parent.id)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .chain(s.meta.list_secrets(parent.id).await?.into_iter().map(|m| m.name))
            .collect::<std::collections::BTreeSet<_>>();
        let empty = names.into_iter().map(|n| (n, String::new())).collect();
        s.meta.put_env(branch.id, empty).await?;
        s.node.notify(ControlEvent::SecretsChanged(branch.id));
    }
    if b.copy_data {
        return Ok(s.data.sql(parent.id).copy_into(branch.id).await?);
    }
    Ok(0)
}

#[utoipa::path(post, path = "/v1/services/{s}/branches", tag = "services", request_body = CreateBranchBody,
    params(("s" = String, Path, description = "the parent service")),
    responses((status = 201, body = ServiceView), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope),
        (status = 409, body = ErrorEnvelope), (status = 413, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 429, body = ErrorEnvelope), (status = 507, body = ErrorEnvelope)))]
async fn branches_create(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateBranchBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    if b.copy_data {
        a.require_scope(Scope::DataAdmin)?;
    }
    if b.events_of_parent || b.code_of_parent {
        a.require_scope(Scope::Deploy)?;
    }
    let fp = fingerprint(&[b"branch", id.as_bytes(), format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let tenant = a.tenant()?;
    let parent = load_service(&s, &a, &id).await?;
    if b.copy_data && crate::control::is_system_service(&parent) {
        return Err(ApiError::new(
            "scratch_source_reserved",
            "the data of a system service is not copied",
        ));
    }
    not_system_parent(&parent)?;
    if parent.config.scratch.is_some() {
        return Err(ApiError::invalid("a branch has no branches of its own"));
    }
    let invalid_name = |msg: &str| ApiError::new("branch_name_invalid", msg);
    if !valid_branch_name(&b.name) {
        return Err(invalid_name(
            "a branch name matches ^[a-z][a-z0-9-]{0,23}$, without `--`",
        ));
    }
    let slug = format!("{}-br-{}", parent.slug, b.name);
    if !valid_slug(&slug) {
        return Err(invalid_name(
            "this name makes the branch's slug longer than 63 characters",
        ));
    }
    if b.expires_at.is_some_and(|at| at <= s.clock.unix_ms()) {
        return Err(ApiError::invalid("`expires_at` is in the past"));
    }
    let own_environment = b.environment.is_none();
    let environment = match b.environment.as_deref() {
        Some(e) => parse_environment(e)?,
        None => format!("br-{}", b.name),
    };
    if !a.principal.can_touch(parent.project_id, &environment) {
        return Err(ApiError::new(
            "insufficient_scope",
            format!("this key may not create services in environment `{environment}`"),
        ));
    }
    if !own_environment {
        check_environment_in_project(&s, tenant, &parent.project, &environment).await?;
    }
    if b.copy_data {
        check_copy_size(&s, &parent).await?;
    }
    let plan = s.plans.plan_for(tenant);
    let current = s.meta.count_services(tenant).await?;
    s.quotas.check(tenant, QuotaOp::CreateService { current })?;
    let counts = s.meta.tenant_counts(tenant).await?;
    crate::limits::check_count("max_branches", counts.branches, plan.max_branches)?;

    let config = branch_config(
        &parent,
        ScratchConfig {
            of: Some(parent.id),
            kind: ScratchKind::Branch,
            name: b.name.clone(),
            expires_at: b.expires_at,
            copied_data: b.copy_data,
            schedules: b.schedules,
            events: b.events_of_parent,
            own_environment: false,
        },
        b.egress_of_parent.unwrap_or(true),
    );
    // The plan may have shrunk since the parent was written.
    check_limits(&plan, &config.limits)?;
    check_scaling_bounds(&plan, s.config.load().sched.min_run_time_max_ms, &config.scaling)?;
    // A branch publishes nothing: its data is its own and no topic of the parent's may see it.
    let mut config = config;
    config.events.data_events = None;
    check_events_allowed(&plan, config.durable, &config.events)?;

    // Only a label added by this call goes away with the branch: an earlier one may have other users.
    let added = own_environment
        && s.meta
            .add_project_environment(tenant, &parent.project, &environment)
            .await?;
    if let Some(c) = config.scratch.as_mut() {
        c.own_environment = added;
    }
    let created = s
        .meta
        .create_service_limited(
            a.scope,
            tenant,
            NewService {
                slug,
                runtime: parent.runtime,
                name: parent.name.clone(),
                profile: parent.profile,
                environment: environment.clone(),
                project: parent.project.clone(),
                config,
            },
            warm_floor_cap(&plan),
            plan.max_services,
        )
        .await;
    let branch = match created {
        Ok(svc) => svc,
        Err(e) => {
            if added {
                let _ = s
                    .meta
                    .drop_project_environment(tenant, &parent.project, &environment)
                    .await;
            }
            return Err(match e {
                StoreError::Conflict("slug_taken") => {
                    ApiError::new("branch_exists", "a service already has this branch's slug")
                }
                other => other.into(),
            });
        }
    };
    let copied = match fill_branch(&s, &parent, &branch, &b).await {
        Ok(n) => n,
        Err(e) => {
            let _ = remove_service(&s, a.scope, &branch, false).await;
            return Err(e);
        }
    };
    refresh_routes(&s).await?;
    if b.code_of_parent {
        if let Err(e) = adopt_parent_code(&s, &a, &parent, &branch).await {
            let _ = remove_service(&s, a.scope, &branch, false).await;
            return Err(e);
        }
    }
    // The promote moved its live version and etag.
    let branch = s
        .meta
        .get_service(a.scope, branch.id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    audit(
        &s,
        &a,
        "scratch.created",
        Some(tenant),
        Some(branch.id),
        json!({"of": parent.id.to_string(), "name": b.name, "environment": environment}),
    );
    if b.copy_data {
        audit(
            &s,
            &a,
            "scratch.copied_data",
            Some(tenant),
            Some(branch.id),
            json!({"source": parent.id.to_string(), "bytes": copied}),
        );
    }
    let view = serde_json::to_value(service_view(&s, &branch).await?).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

/// The branches of `{s}` the key may see.
#[utoipa::path(get, path = "/v1/services/{s}/branches", tag = "services",
    params(("s" = String, Path, description = "the parent service"), PageQuery),
    responses((status = 200, body = ServicePage), (status = 404, body = ErrorEnvelope)))]
async fn branches_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<PageQuery>,
) -> Result<Json<ServicePage>, ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let parent = load_service(&s, &a, &id).await?;
    let (project_ids, environments) = match &a.principal {
        rt_sec::Principal::Tenant {
            project_ids,
            environments,
            ..
        } => (project_ids.clone(), environments.clone()),
        _ => (None, None),
    };
    let filter = ServiceFilter {
        scratch_of: Some(parent.id),
        project_ids,
        environments,
        ..ServiceFilter::default()
    };
    let page = s
        .meta
        .list_services(
            TenantScope::Tenant(parent.tenant_id),
            filter,
            page_req(q.limit, q.cursor),
        )
        .await?;
    let mut items = Vec::with_capacity(page.items.len());
    for svc in &page.items {
        items.push(service_view(&s, svc).await?);
    }
    Ok(Json(ServicePage {
        items,
        next_cursor: page.next_cursor,
    }))
}

/// Deletes the branch service and its data; the parent keeps its own.
#[utoipa::path(delete, path = "/v1/services/{s}/branches/{name}", tag = "services",
    params(("s" = String, Path, description = "the parent service"), ("name" = String, Path, description = "branch name")),
    responses((status = 204), (status = 404, body = ErrorEnvelope), (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn branches_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    let parent = load_service(&s, &a, &id).await?;
    let branch = load_branch(&s, &a, &parent, &name).await?;
    remove_service(&s, a.scope, &branch, false).await?;
    audit(
        &s,
        &a,
        "scratch.deleted",
        Some(branch.tenant_id),
        Some(branch.id),
        json!({"of": parent.id.to_string(), "name": name, "reason": "requested"}),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// One pass of the node's sweep: delete the branches past `expires_at`, with their data. A failure
/// is logged, and the next pass finds the branch again.
pub async fn sweep_expired(s: &ApiState) {
    let now = s.clock.unix_ms();
    let expired = match s.meta.expired_scratch(now, SWEEP_BATCH).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(code = e.code(), "could not list expired branches");
            return;
        }
    };
    for svc in expired {
        let scope = TenantScope::Tenant(svc.tenant_id);
        if let Err(e) = remove_service(s, scope, &svc, false).await {
            let _ = e;
            tracing::warn!(service = %svc.id, "could not remove an expired branch");
            continue;
        }
        emit_expired(s, svc.tenant_id, &svc, now);
    }
}

fn emit_expired(s: &ApiState, tenant: TenantId, svc: &Service, now: UnixMs) {
    s.audit.emit(AuditEvent {
        ts_ms: now,
        actor: Actor::System,
        tenant: Some(tenant),
        service: Some(svc.id),
        action: "scratch.deleted".to_string(),
        data: json!({
            "of": svc.config.scratch.as_ref().and_then(|c| c.of).map(|of| of.to_string()),
            "name": svc.config.scratch.as_ref().map(|c| c.name.clone()),
            "reason": "expired",
        }),
    });
}

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/services/{s}/branches", post(branches_create).get(branches_list))
        .route("/v1/services/{s}/branches/{name}", delete(branches_delete))
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(branches_create, branches_list, branches_delete),
    components(schemas(CreateBranchBody))
)]
pub struct Doc;
