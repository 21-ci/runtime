//! Previews (`docs/design/platform-plan.md` R5, `platform-enhancements.md`): a service with
//! `config.scratch.kind = "preview"`, made in one call, from a parent's config or standing alone.
//!
//! The isolation is the runtime's: [`rt_types::ServiceConfig::enforce_scratch_policy`] forces the
//! config at create and after every `PATCH`, `rt-triggers` and `rt-host` refuse the capabilities, and
//! [`refuse_policy_patch`] says so to a client that tries.

use std::str::FromStr;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use rt_sec::QuotaOp;
use rt_store::StoreError;
use rt_types::{
    EgressRules, NewService, RuntimeKind, Scope, ScratchConfig, ScratchKind, Service, ServiceConfig, TriggerSwitches,
    UnixMs, MAX_ENVIRONMENT_LEN,
};
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::branches::check_copy_size;
use crate::control::{
    audit, check_events_allowed, check_ingress_egress, check_limits, check_scaling_bounds, fingerprint, idem_replay,
    idem_store, is_system_service, load_service, remove_service, service_view, valid_branch_name, valid_slug,
    value_resp, warm_floor_cap,
};
use crate::dto::{PatchServiceBody, ServiceView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::ApiState;

/// r6's limit on a slug that serves as a host label.
const MAX_PREVIEW_SLUG: usize = 52;
/// The life of a preview when neither the request nor the plan names one.
const DEFAULT_TTL_SECONDS: u64 = 7 * 24 * 3600;

/// `POST /v1/scratch`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateScratchBody {
    /// The service to mirror (`svc_…`); its config, never its secrets. Absent: stands alone.
    pub of: Option<String>,
    /// `deno` or `wasm`; required without `of`, refused with it.
    pub runtime: Option<String>,
    /// Project of a preview without `of` (default project otherwise).
    pub project: Option<String>,
    /// `^[a-z][a-z0-9-]{0,23}$` without `--`, part of the slug. Default `preview`.
    pub name: Option<String>,
    /// Delete it and its data at this time (unix ms); at most `scratch_ttl_max` away.
    pub expires_at: Option<UnixMs>,
    /// Instead of `expires_at`. Neither: the plan's `scratch_ttl_max`.
    pub ttl_seconds: Option<u64>,
    /// Copy the parent's data and blobs (`data:admin`; forces `access: key`). Default empty.
    #[serde(default)]
    pub copy_data: bool,
    /// Hosts its code may reach. Default none.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

/// `422 preview_policy`: what the runtime forces on a preview is not the client's to set. The store
/// forces it regardless; this answers instead of dropping the change silently.
pub(crate) fn refuse_policy_patch(svc: &Service, b: &PatchServiceBody) -> Result<(), ApiError> {
    if !svc.config.is_preview() {
        return Ok(());
    }
    let refuse = |what: &str| Err(ApiError::new("preview_policy", format!("a preview has no {what}")));
    if b.environment.as_deref().is_some_and(|e| e != svc.environment) {
        return refuse("other environment than its own");
    }
    if b.call.is_some() {
        return refuse("`call` block: nothing calls it, and it calls nothing");
    }
    if b.events.is_some() {
        return refuse("`events` block: no subscriptions and no stream consumers");
    }
    if b.async_invoke.is_some() {
        return refuse("`async` block");
    }
    if let Some(t) = &b.triggers {
        let t: TriggerSwitches = serde_json::from_value(t.clone())
            .map_err(|_| ApiError::invalid("`triggers` is not a valid trigger block"))?;
        if t.interval || t.cron || t.queue || t.call || t.websocket {
            return refuse("trigger but `http`: send the others as false");
        }
    }
    Ok(())
}

/// When it goes: `expires_at` or now + `ttl_seconds`, or the plan's longest, never past that.
fn expiry(now: UnixMs, b: &CreateScratchBody, ttl_max: u64) -> Result<UnixMs, ApiError> {
    let ms = |secs: u64| i64::try_from(secs.saturating_mul(1000)).unwrap_or(i64::MAX);
    let at = match (b.expires_at, b.ttl_seconds) {
        (Some(_), Some(_)) => return Err(ApiError::invalid("give `expires_at` or `ttl_seconds`, not both")),
        (Some(at), None) => at,
        (None, Some(0)) => return Err(ApiError::invalid("`ttl_seconds` must be at least 1")),
        (None, Some(secs)) => now.saturating_add(ms(secs)),
        (None, None) => now.saturating_add(ms(if ttl_max > 0 { ttl_max } else { DEFAULT_TTL_SECONDS })),
    };
    if at <= now {
        return Err(ApiError::invalid("`expires_at` is in the past"));
    }
    if ttl_max > 0 && at > now.saturating_add(ms(ttl_max)) {
        return Err(ApiError::new(
            "scratch_ttl_too_long",
            format!("this plan keeps a preview for at most {ttl_max} seconds (`scratch_ttl_max`)"),
        ));
    }
    Ok(at)
}

/// `<base>-<name>-<8 hex>` within [`MAX_PREVIEW_SLUG`]: the suffix makes it unique, `--` stays
/// reserved. It is not access control.
fn preview_slug(base: &str, name: &str, suffix: &str) -> String {
    let room = MAX_PREVIEW_SLUG - name.len() - suffix.len() - 2;
    let cut: String = base.chars().take(room).collect();
    let base = cut.split('-').filter(|p| !p.is_empty()).collect::<Vec<_>>().join("-");
    format!("{base}-{name}-{suffix}")
}

/// A preview's copy of its parent's data; a failure takes the preview back out.
async fn copy_parent_data(s: &ApiState, a: &Auth, parent: &Service, preview: &Service) -> Result<(), ApiError> {
    match s.data.sql(parent.id).copy_into(preview.id).await {
        Ok(bytes) => {
            audit(
                s,
                a,
                "scratch.copied_data",
                Some(preview.tenant_id),
                Some(preview.id),
                json!({"source": parent.id.to_string(), "bytes": bytes}),
            );
            Ok(())
        }
        Err(e) => {
            let _ = remove_service(s, a.scope, preview, false).await;
            Err(e.into())
        }
    }
}

#[utoipa::path(post, path = "/v1/scratch", tag = "services", request_body = CreateScratchBody,
    responses((status = 201, body = ServiceView), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope),
        (status = 409, body = ErrorEnvelope), (status = 413, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope),
        (status = 507, body = ErrorEnvelope)))]
async fn scratch_create(
    State(s): State<ApiState>,
    a: Auth,
    h: HeaderMap,
    ApiJson(b): ApiJson<CreateScratchBody>,
) -> Result<Response, ApiError> {
    a.require_scope(Scope::ServicesWrite)?;
    if b.copy_data {
        a.require_scope(Scope::DataAdmin)?;
    }
    let fp = fingerprint(&[b"scratch", format!("{b:?}").as_bytes()]);
    let (replay, _idem_turn) = idem_replay(&s, &a, &h, fp).await?;
    if let Some(resp) = replay {
        return Ok(resp);
    }
    let tenant = a.tenant()?;
    let parent = match b.of.as_deref() {
        Some(of) => Some(load_service(&s, &a, of).await?),
        None => None,
    };
    let name = b.name.clone().unwrap_or_else(|| "preview".to_string());
    if !valid_branch_name(&name) {
        return Err(ApiError::invalid(
            "`name` must match ^[a-z][a-z0-9-]{0,23}$, without `--`",
        ));
    }
    let egress = EgressRules {
        allowed_hosts: b.allowed_hosts.clone(),
        denied_hosts: Vec::new(),
    };
    check_ingress_egress(None, Some(&egress))?;
    let plan = s.plans.plan_for(tenant);
    let expires_at = expiry(s.clock.unix_ms(), &b, plan.scratch_ttl_max)?;

    let (runtime, project, parent_name, profile, base_config) = match &parent {
        Some(p) => {
            if b.runtime.is_some() || b.project.is_some() {
                return Err(ApiError::invalid(
                    "a preview takes its `runtime` and `project` from `of`",
                ));
            }
            if b.copy_data && is_system_service(p) {
                return Err(ApiError::new(
                    "scratch_source_reserved",
                    "the data of a system service is not copied",
                ));
            }
            if is_system_service(p) {
                return Err(ApiError::new("reserved", "a system service has no previews"));
            }
            if p.config.scratch.is_some() {
                return Err(ApiError::invalid("`of` is itself a branch or a preview"));
            }
            (
                p.runtime,
                p.project.clone(),
                p.name.clone(),
                p.profile,
                p.config.clone(),
            )
        }
        None => {
            if b.copy_data {
                return Err(ApiError::invalid("`copy_data` needs `of`"));
            }
            let runtime = b
                .runtime
                .as_deref()
                .ok_or_else(|| ApiError::invalid("a preview without `of` names its `runtime`"))?;
            let runtime = RuntimeKind::from_str(runtime).map_err(ApiError::invalid)?;
            let project = crate::projects::project_slug_of(&s, tenant, b.project.as_deref()).await?;
            (runtime, project, None, None, ServiceConfig::default())
        }
    };

    let suffix = format!("{:08x}", ulid::Ulid::new().random() as u32);
    let slug = preview_slug(parent.as_ref().map_or("scratch", |p| p.slug.as_str()), &name, &suffix);
    if !valid_slug(&slug) {
        return Err(ApiError::invalid("this name makes no valid slug"));
    }
    let environment = format!("preview-{suffix}");
    debug_assert!(environment.len() <= MAX_ENVIRONMENT_LEN);
    let project_id = match crate::projects::find_project(&s, tenant, &project).await? {
        Some(p) => p.id,
        None => rt_types::default_project_id(tenant),
    };
    if !a.principal.can_touch(project_id, &environment) {
        return Err(ApiError::new(
            "insufficient_scope",
            "a preview gets its own environment: this key is limited to others",
        ));
    }
    if let (true, Some(p)) = (b.copy_data, &parent) {
        check_copy_size(&s, p).await?;
    }
    let current = s.meta.count_services(tenant).await?;
    s.quotas.check(tenant, QuotaOp::CreateService { current })?;

    let mut config = base_config;
    config.egress = egress;
    config.scratch = Some(ScratchConfig {
        of: parent.as_ref().map(|p| p.id),
        kind: ScratchKind::Preview,
        name,
        expires_at: Some(expires_at),
        copied_data: b.copy_data,
        schedules: false,
        events: false,
        own_environment: true,
    });
    config.enforce_scratch_policy();
    // The plan may have shrunk since the parent was written.
    check_limits(&plan, &config.limits)?;
    check_scaling_bounds(&plan, s.config.load().sched.min_run_time_max_ms, &config.scaling)?;
    check_events_allowed(&plan, config.durable, &config.events)?;

    s.meta.add_project_environment(tenant, &project, &environment).await?;
    let created = s
        .meta
        .create_preview_limited(
            a.scope,
            tenant,
            NewService {
                slug,
                runtime,
                name: parent_name,
                profile,
                environment: environment.clone(),
                project: project.clone(),
                config,
            },
            warm_floor_cap(&plan),
            plan.max_services,
            plan.max_scratch,
        )
        .await;
    let preview = match created {
        Ok(svc) => svc,
        Err(e) => {
            let _ = s.meta.drop_project_environment(tenant, &project, &environment).await;
            return Err(match e {
                StoreError::Conflict("scratch_limit") => ApiError::new(
                    "quota_exceeded",
                    format!("this plan allows {} previews (`max_scratch`)", plan.max_scratch),
                ),
                other => other.into(),
            });
        }
    };
    audit(
        &s,
        &a,
        "scratch.created",
        Some(tenant),
        Some(preview.id),
        json!({"of": parent.as_ref().map(|p| p.id.to_string()), "kind": "preview", "environment": environment}),
    );
    if let (true, Some(p)) = (b.copy_data, &parent) {
        copy_parent_data(&s, &a, p, &preview).await?;
    }
    crate::control::refresh_routes(&s).await?;
    let view = serde_json::to_value(service_view(&s, &preview).await?).unwrap_or(Value::Null);
    idem_store(&s, &a, &h, fp, StatusCode::CREATED, &view);
    Ok(value_resp(StatusCode::CREATED, &view))
}

pub fn routes() -> Router<ApiState> {
    Router::new().route("/v1/scratch", post(scratch_create))
}

#[derive(utoipa::OpenApi)]
#[openapi(paths(scratch_create), components(schemas(CreateScratchBody)))]
pub struct Doc;

#[cfg(test)]
mod tests {
    use super::preview_slug;

    #[test]
    fn a_preview_slug_fits_and_keeps_double_hyphens_out() {
        let long = "a".repeat(63);
        for (base, name) in [
            (long.as_str(), "x"),
            ("ab-cd", "pr-7"),
            (long.as_str(), "abcdefghijklmnopqrstuvwx"),
        ] {
            let slug = preview_slug(base, name, "0123abcd");
            assert!(slug.len() <= 52, "{slug}");
            assert!(!slug.contains("--"), "{slug}");
            assert!(slug.ends_with("-0123abcd"), "{slug}");
        }
        // A cut that ends on a hyphen does not make `--`.
        let slug = preview_slug(&format!("{}-tail", "b".repeat(41)), "name", "0123abcd");
        assert!(!slug.contains("--"), "{slug}");
    }
}
