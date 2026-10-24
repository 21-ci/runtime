//! Operator SDKs (docs/api.md): runtime-owned services of the system
//! tenant, published, promoted and granted by the operator; tenants read what their grants select.
//!
//! Operator (root token): `POST /v1/sdks` (publish a release: source + contract),
//! `GET /v1/sdks`, `GET /v1/sdks/{name}/releases`, `PUT /v1/sdks/{name}/channels/{channel}`
//! (promote, roll back), `DELETE /v1/sdks/{name}`, `GET|PUT /v1/sdk-grants`.
//! Tenant (`services:read`): `GET /v1/sdks/effective`, `GET /v1/sdks/{name}/contract`.
//!
//! Every change is a control-plane row: the route snapshot carries it to the data plane, so no
//! tenant service is rebuilt or redeployed.

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine as _;
use rt_sec::Principal;
use rt_store::{StoreError, TenantScope};
use rt_types::{
    sdk_service_slug, system_tenant, valid_channel, valid_sdk_name, Access, BuildState, CompileMode, ContractTarget,
    GrantEffect, GrantScope, NewSdkRelease, NewService, NewVersion, ProjectId, RuntimeKind, Scope, Sdk, SdkCaller,
    SdkGrant, SdkRelease, SdkState, ServiceConfig, TenantId, ValidationError, DEFAULT_CHANNEL,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::auth::Auth;
use crate::control::{audit, refresh_routes, typescript_resp};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::{ApiJson, ApiQuery};
use crate::projects::find_project;
use crate::state::{ApiState, ControlEvent};

type R<T> = Result<Json<T>, ApiError>;

// ---- bodies and views --------------------------------------------------------------------------

/// `POST /v1/sdks`: one release of an SDK.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishSdkBody {
    /// `^[a-z][a-z0-9_]{1,31}$`: what tenants import (`std:sdk` export, `sdk:<name>`).
    pub name: String,
    /// Semver, `1.4.2`.
    pub version: String,
    /// `deno` (default) or `wasm`.
    #[serde(default)]
    pub runtime: Option<String>,
    /// The upload's format, as for a version upload (default `application/typescript` for Deno,
    /// `application/wasm` for WASM).
    #[serde(default)]
    pub content_type: Option<String>,
    /// The upload, base64.
    pub source: String,
    /// The `.d.ts` tenants download (default: generated from the `defineSdk` methods). Required
    /// for a WASM SDK, with `members`.
    #[serde(default)]
    pub contract: Option<String>,
    /// Top-level contract names (default: from the `defineSdk` methods).
    #[serde(default)]
    pub members: Option<Vec<String>>,
    #[serde(default)]
    pub description: Option<String>,
    /// Point this channel at the release (default: `stable` for the SDK's first release).
    #[serde(default)]
    pub channel: Option<String>,
}

/// A release.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SdkReleaseView {
    pub sdk: String,
    pub version: String,
    /// The SDK service's version that runs it.
    pub version_no: u32,
    pub members: Vec<String>,
    /// The channels that point at it.
    pub channels: Vec<String>,
    pub created_at: i64,
}

/// A channel.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SdkChannelView {
    pub channel: String,
    pub version: String,
    /// The version it pointed to before the last move (`21ci sdk rollback`).
    pub previous: Option<String>,
    pub updated_at: i64,
}

/// An SDK (operator view).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SdkView {
    pub name: String,
    /// `active` | `removed`.
    pub state: String,
    pub description: Option<String>,
    /// Its service in the system tenant.
    pub service_id: String,
    #[schema(value_type = Object)]
    pub channels: BTreeMap<String, SdkChannelView>,
    /// Number of releases.
    pub releases: usize,
    pub latest: Option<String>,
    pub created_at: i64,
}

/// `PUT /v1/sdks/{name}/channels/{channel}`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SetChannelBody {
    pub version: String,
}

/// A grant: what a scope may use of an SDK (or of all, `*`). A field left out or `null` inherits.
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct SdkGrantBody {
    /// `node`, `plan:<name>`, `tenant:ten_…`, `project:prj_…` or `environment:prj_…/<env>`.
    pub scope: String,
    /// An SDK name or `*`.
    pub sdk: String,
    /// `allow` | `deny`.
    #[serde(default)]
    pub effect: Option<String>,
    /// Semver requirement (`^1`, `~1.4`, `1.4.2`, `*`). An `allow` without one is pinned to the
    /// major of the SDK's stable release (a new major is a grant change).
    #[serde(default)]
    pub version_req: Option<String>,
    /// The channel followed (default `stable`).
    #[serde(default)]
    pub channel: Option<String>,
}

/// A stored grant.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SdkGrantView {
    pub scope: String,
    pub sdk: String,
    pub effect: Option<String>,
    pub version_req: Option<String>,
    pub channel: Option<String>,
    pub created_by: String,
    pub updated_at: i64,
}

/// An SDK the caller's grants allow, and the release they select (tenant view).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct EffectiveSdkView {
    pub name: String,
    pub description: Option<String>,
    /// The release a call runs now; `null` when none satisfies the grants (`reason`).
    pub version: Option<String>,
    pub channel: String,
    pub version_reqs: Vec<String>,
    pub members: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct EffectiveQuery {
    /// Project slug or `prj_…` id (default: `default`).
    pub project: Option<String>,
    /// Environment (default: `production`).
    pub environment: Option<String>,
    /// `GET …/contract`, operator: a release instead of the resolved one.
    pub version: Option<String>,
}

fn grant_view(g: &SdkGrant) -> SdkGrantView {
    SdkGrantView {
        scope: g.scope.to_string(),
        sdk: g.sdk.clone(),
        effect: g.effect.map(|e| e.as_str().to_string()),
        version_req: g.version_req.clone(),
        channel: g.channel.clone(),
        created_by: g.created_by.clone(),
        updated_at: g.updated_at_ms,
    }
}

fn release_view(r: &SdkRelease, channels: Vec<String>) -> SdkReleaseView {
    SdkReleaseView {
        sdk: r.sdk.clone(),
        version: r.version.clone(),
        version_no: r.version_no.0,
        members: r.members.clone(),
        channels,
        created_at: r.created_at_ms,
    }
}

fn reserved_name(name: &str) -> Result<(), ApiError> {
    if valid_sdk_name(name) {
        Ok(())
    } else {
        Err(ApiError::invalid("an SDK name matches ^[a-z][a-z0-9_]{1,31}$"))
    }
}

async fn load_sdk(s: &ApiState, name: &str) -> Result<Sdk, ApiError> {
    s.meta.get_sdk(name).await?.ok_or_else(ApiError::not_found)
}

// ---- operator ----------------------------------------------------------------------------------

/// Publish a release: the source is validated like a version upload, its contract read from the
/// `defineSdk` methods (or given), and it runs as a version of the SDK's system service. The first
/// release becomes `stable`; later ones only move the `channel` asked for.
#[utoipa::path(post, path = "/v1/sdks", tag = "sdks", request_body = PublishSdkBody,
    responses((status = 201, body = SdkReleaseView), (status = 409, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn sdks_publish(
    State(s): State<ApiState>,
    a: Auth,
    ApiJson(b): ApiJson<PublishSdkBody>,
) -> Result<Response, ApiError> {
    a.require_operator()?;
    reserved_name(&b.name)?;
    rt_types::sdk::parse_version(&b.version).map_err(ApiError::invalid)?;
    if let Some(c) = &b.channel {
        if !valid_channel(c) {
            return Err(ApiError::invalid("a channel matches ^[a-z][a-z0-9-]{0,31}$"));
        }
    }
    let runtime = match b.runtime.as_deref() {
        None => RuntimeKind::Deno,
        Some(r) => r.parse::<RuntimeKind>().map_err(ApiError::invalid)?,
    };
    let content_type = b.content_type.clone().unwrap_or_else(|| match runtime {
        RuntimeKind::Deno => "application/typescript".to_string(),
        RuntimeKind::Wasm => "application/wasm".to_string(),
    });
    let body = bytes::Bytes::from(
        base64::engine::general_purpose::STANDARD
            .decode(b.source.trim())
            .map_err(|_| ApiError::invalid("`source` is not base64"))?,
    );
    let existing = s.meta.get_sdk(&b.name).await?;
    if s.meta.get_sdk_release(&b.name, &b.version).await?.is_some() {
        return Err(ApiError::new(
            "sdk_release_exists",
            format!("{} {} was published before", b.name, b.version),
        ));
    }
    if let Err(e) = s.validator.validate(runtime, &content_type, &body) {
        return Err(match e {
            ValidationError::Invalid(d) => ApiError::new("validation_failed", "the upload failed validation")
                .with_details(json!({"diagnostics": d})),
            ValidationError::InvalidArtifact(m) => ApiError::new("invalid_artifact", m),
            ValidationError::TooLarge(m) => ApiError::new("too_large", m),
        });
    }

    // The contract: the `.d.ts` tenants download and the top-level members their shims export.
    let target = ContractTarget::Sdk {
        name: b.name.clone(),
        version: b.version.clone(),
    };
    let derived = match runtime {
        RuntimeKind::Deno => s.validator.rpc_contract(&content_type, &body, &target).ok(),
        RuntimeKind::Wasm => None,
    };
    if let Some(d) = &derived {
        if !d.unchecked.is_empty() {
            return Err(ApiError::new(
                "sdk_unchecked_caller",
                format!(
                    "SDK methods must authorize by ctx.caller; these never read it: {} (or give defineSdk an authorize(caller) hook)",
                    d.unchecked.join(", ")
                ),
            )
            .with_details(json!({"methods": d.unchecked})));
        }
    }
    let (members, contract) = match (b.members.clone(), b.contract.clone(), derived) {
        (Some(m), Some(c), _) => (m, c),
        (m, c, Some(d)) => (m.unwrap_or(d.members), c.unwrap_or(d.dts)),
        _ => {
            return Err(ApiError::new(
                "sdk_contract_missing",
                "no contract: export `defineSdk({ methods })`, or give `contract` and `members`",
            ))
        }
    };

    // The SDK's service in the system tenant: created with the first release.
    let sys = system_tenant();
    let service = match &existing {
        Some(sdk) => {
            let svc = s
                .meta
                .get_service(TenantScope::Operator, sdk.service_id)
                .await?
                .ok_or_else(ApiError::not_found)?;
            if svc.runtime != runtime {
                return Err(ApiError::invalid(format!(
                    "the SDK `{}` runs on {}",
                    b.name, svc.runtime
                )));
            }
            svc
        }
        None => {
            let config = ServiceConfig {
                access: Access::Internal,
                ..ServiceConfig::default()
            };
            let new = NewService {
                slug: sdk_service_slug(&b.name),
                runtime,
                name: Some(format!("operator SDK {}", b.name)),
                profile: None,
                environment: rt_types::default_environment(),
                project: rt_types::default_project(),
                config,
            };
            s.meta.create_service(TenantScope::Tenant(sys), sys, new).await?
        }
    };
    let digest = s.artifacts.put_typed(body, &content_type).await?;
    let version = s
        .meta
        .add_version(
            service.id,
            NewVersion {
                source_digest: digest,
                compile: CompileMode::Eager,
            },
        )
        .await?;
    // Built now, off the call path: the first call of the release does not wait for a build.
    s.meta
        .set_build_state(service.id, version.number, BuildState::Building, None, None)
        .await?;
    s.node.notify(ControlEvent::BuildRequested {
        service: service.id,
        version: version.number,
        digest,
        runtime,
        rebuild: false,
    });

    s.meta.upsert_sdk(&b.name, service.id, b.description.clone()).await?;
    let release = s
        .meta
        .add_sdk_release(NewSdkRelease {
            sdk: b.name.clone(),
            version: b.version.clone(),
            version_no: version.number,
            members,
            contract,
        })
        .await
        .map_err(|e| match e {
            StoreError::Conflict(_) => ApiError::new(
                "sdk_release_exists",
                format!("{} {} was published before", b.name, b.version),
            ),
            e => e.into(),
        })?;
    let has_stable = s
        .meta
        .list_sdk_channels(&b.name)
        .await?
        .iter()
        .any(|c| c.channel == DEFAULT_CHANNEL);
    let channel = b
        .channel
        .clone()
        .or_else(|| (!has_stable).then(|| DEFAULT_CHANNEL.to_string()));
    if let Some(c) = &channel {
        s.meta.set_sdk_channel(&b.name, c, &b.version).await?;
    }
    refresh_routes(&s).await?;
    let data = json!({"sdk": b.name, "version": b.version, "channel": channel});
    audit(&s, &a, "sdk.published", Some(sys), Some(service.id), data);
    let view = release_view(&release, channel.into_iter().collect());
    Ok((StatusCode::CREATED, Json(view)).into_response())
}

/// Every SDK with its channels.
#[utoipa::path(get, path = "/v1/sdks", tag = "sdks", responses((status = 200, body = Vec<SdkView>)))]
pub(crate) async fn sdks_list(State(s): State<ApiState>, a: Auth) -> R<Vec<SdkView>> {
    a.require_operator()?;
    let mut out = Vec::new();
    for sdk in s.meta.list_sdks().await? {
        let releases = s.meta.list_sdk_releases(&sdk.name).await?;
        let channels = s
            .meta
            .list_sdk_channels(&sdk.name)
            .await?
            .into_iter()
            .map(|c| {
                let v = SdkChannelView {
                    channel: c.channel.clone(),
                    version: c.version,
                    previous: c.previous,
                    updated_at: c.updated_at_ms,
                };
                (c.channel, v)
            })
            .collect();
        out.push(SdkView {
            name: sdk.name,
            state: sdk.state.as_str().to_string(),
            description: sdk.description,
            service_id: sdk.service_id.to_string(),
            channels,
            releases: releases.len(),
            latest: releases.last().map(|r| r.version.clone()),
            created_at: sdk.created_at_ms,
        });
    }
    Ok(Json(out))
}

/// The releases of an SDK, oldest first.
#[utoipa::path(get, path = "/v1/sdks/{name}/releases", tag = "sdks",
    params(("name" = String, Path, description = "SDK name")), responses((status = 200, body = Vec<SdkReleaseView>)))]
pub(crate) async fn sdks_releases(
    State(s): State<ApiState>,
    a: Auth,
    Path(name): Path<String>,
) -> R<Vec<SdkReleaseView>> {
    a.require_operator()?;
    load_sdk(&s, &name).await?;
    let channels = s.meta.list_sdk_channels(&name).await?;
    let releases = s.meta.list_sdk_releases(&name).await?;
    Ok(Json(
        releases
            .iter()
            .map(|r| {
                release_view(
                    r,
                    channels
                        .iter()
                        .filter(|c| c.version == r.version)
                        .map(|c| c.channel.clone())
                        .collect(),
                )
            })
            .collect(),
    ))
}

/// Point a channel at a release: promote, or roll back (the old version is kept as `previous`).
#[utoipa::path(put, path = "/v1/sdks/{name}/channels/{channel}", tag = "sdks", request_body = SetChannelBody,
    params(("name" = String, Path, description = "SDK name"), ("channel" = String, Path, description = "channel")),
    responses((status = 200, body = SdkChannelView), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn sdks_set_channel(
    State(s): State<ApiState>,
    a: Auth,
    Path((name, channel)): Path<(String, String)>,
    ApiJson(b): ApiJson<SetChannelBody>,
) -> R<SdkChannelView> {
    a.require_operator()?;
    if !valid_channel(&channel) {
        return Err(ApiError::invalid("a channel matches ^[a-z][a-z0-9-]{0,31}$"));
    }
    let sdk = load_sdk(&s, &name).await?;
    let c = s.meta.set_sdk_channel(&name, &channel, &b.version).await?;
    refresh_routes(&s).await?;
    let data = json!({"sdk": name, "channel": channel, "version": c.version, "previous": c.previous});
    audit(
        &s,
        &a,
        "sdk.channel_changed",
        Some(system_tenant()),
        Some(sdk.service_id),
        data,
    );
    Ok(Json(SdkChannelView {
        channel: c.channel,
        version: c.version,
        previous: c.previous,
        updated_at: c.updated_at_ms,
    }))
}

/// Remove an SDK: calls answer `sdk_unavailable`, new deploys cannot import it. Its releases are
/// kept; publishing again restores it.
#[utoipa::path(delete, path = "/v1/sdks/{name}", tag = "sdks",
    params(("name" = String, Path, description = "SDK name")), responses((status = 204), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn sdks_remove(
    State(s): State<ApiState>,
    a: Auth,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    let sdk = load_sdk(&s, &name).await?;
    s.meta.remove_sdk(&name).await?;
    refresh_routes(&s).await?;
    audit(
        &s,
        &a,
        "sdk.removed",
        Some(system_tenant()),
        Some(sdk.service_id),
        json!({"sdk": name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// Every grant.
#[utoipa::path(get, path = "/v1/sdk-grants", tag = "sdks", responses((status = 200, body = Vec<SdkGrantView>)))]
pub(crate) async fn grants_list(State(s): State<ApiState>, a: Auth) -> R<Vec<SdkGrantView>> {
    a.require_operator()?;
    Ok(Json(s.meta.list_sdk_grants().await?.iter().map(grant_view).collect()))
}

/// Write the grant of `(scope, sdk)`; with no effect, requirement or channel it is deleted (`204`).
#[utoipa::path(put, path = "/v1/sdk-grants", tag = "sdks", request_body = SdkGrantBody,
    responses((status = 200, body = SdkGrantView), (status = 204), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn grants_put(
    State(s): State<ApiState>,
    a: Auth,
    ApiJson(b): ApiJson<SdkGrantBody>,
) -> Result<Response, ApiError> {
    a.require_operator()?;
    let scope: GrantScope = b.scope.parse().map_err(ApiError::invalid)?;
    if b.sdk != "*" {
        reserved_name(&b.sdk)?;
    }
    let effect = b
        .effect
        .as_deref()
        .map(str::parse::<GrantEffect>)
        .transpose()
        .map_err(ApiError::invalid)?;
    if let Some(r) = &b.version_req {
        rt_types::sdk::parse_version_req(r).map_err(ApiError::invalid)?;
    }
    if let Some(c) = &b.channel {
        if !valid_channel(c) {
            return Err(ApiError::invalid("a channel matches ^[a-z][a-z0-9-]{0,31}$"));
        }
    }
    // An allow pins the major of the SDK's stable release unless it names a requirement: tenants
    // get minors on the channel, and a new major is a grant change.
    let mut version_req = b.version_req.clone();
    if effect == Some(GrantEffect::Allow) && version_req.is_none() && b.sdk != "*" {
        let snap = s.routes.sdk();
        if let Some(stable) = snap.channel_release(&b.sdk, DEFAULT_CHANNEL) {
            if let Ok(v) = rt_types::sdk::parse_version(&stable.version) {
                version_req = Some(format!("^{}", v.major));
            }
        }
    }
    let grant = SdkGrant {
        scope,
        sdk: b.sdk.clone(),
        effect,
        version_req,
        channel: b.channel.clone(),
        created_by: a.identity(),
        updated_at_ms: 0,
    };
    let stored = s.meta.put_sdk_grant(grant).await?;
    refresh_routes(&s).await?;
    audit(
        &s,
        &a,
        "sdk.grant_changed",
        None,
        None,
        serde_json::to_value(&b).unwrap_or_default(),
    );
    Ok(match stored {
        Some(g) => (StatusCode::OK, Json(grant_view(&g))).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

// ---- tenant side -------------------------------------------------------------------------------

/// The scope a tenant-side read resolves grants for: the key's tenant, a project (default
/// `default`) and an environment (default `production`), both within the key's scope.
async fn caller_scope(
    s: &ApiState,
    a: &Auth,
    q: &EffectiveQuery,
) -> Result<(TenantId, String, ProjectId, String), ApiError> {
    a.require_scope(Scope::ServicesRead)?;
    let tenant = a.tenant()?;
    let named = q.project.as_deref().unwrap_or(rt_types::DEFAULT_PROJECT);
    let project = find_project(s, tenant, named).await?.ok_or_else(ApiError::not_found)?;
    let environment = q.environment.clone().unwrap_or_else(rt_types::default_environment);
    if !a.principal.can_touch(project.id, &environment) {
        return Err(ApiError::not_found());
    }
    let plan = s.plans.plan_name(tenant);
    Ok((tenant, plan, project.id, environment))
}

/// The SDKs this key's tenant may call from a project and environment, and the release each runs.
#[utoipa::path(get, path = "/v1/sdks/effective", tag = "sdks", params(EffectiveQuery),
    responses((status = 200, body = Vec<EffectiveSdkView>)))]
pub(crate) async fn sdks_effective(
    State(s): State<ApiState>,
    a: Auth,
    ApiQuery(q): ApiQuery<EffectiveQuery>,
) -> R<Vec<EffectiveSdkView>> {
    let (tenant, plan, project, environment) = caller_scope(&s, &a, &q).await?;
    let caller = SdkCaller {
        tenant,
        plan: &plan,
        project,
        environment: &environment,
    };
    let snap = s.routes.sdk();
    let mut out = Vec::new();
    for sdk in snap.sdks.iter().filter(|x| x.state == SdkState::Active) {
        let grant = snap.effective_grant(&caller, &sdk.name);
        if !grant.allowed {
            continue;
        }
        let (version, members, reason) = match snap.resolve(&caller, &sdk.name) {
            Ok(t) => (Some(t.version.clone()), t.members.clone(), None),
            Err(d) => (None, Vec::new(), Some(d.code().to_string())),
        };
        out.push(EffectiveSdkView {
            name: sdk.name.clone(),
            description: sdk.description.clone(),
            version,
            channel: grant.channel,
            version_reqs: grant.version_reqs,
            members,
            reason,
        });
    }
    Ok(Json(out))
}

/// The `.d.ts` of the release this key's grants select (`21ci sdk types`); the operator may name
/// any release with `?version=`. An SDK the tenant is not granted does not exist for it (404).
#[utoipa::path(get, path = "/v1/sdks/{name}/contract", tag = "sdks",
    params(("name" = String, Path, description = "SDK name"), EffectiveQuery),
    responses((status = 200, description = "the contract (`.d.ts`)", content_type = "text/plain"), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn sdks_contract(
    State(s): State<ApiState>,
    a: Auth,
    Path(name): Path<String>,
    ApiQuery(q): ApiQuery<EffectiveQuery>,
) -> Result<Response, ApiError> {
    let version = match (&a.principal, &q.version) {
        (Principal::Operator, Some(v)) => v.clone(),
        (Principal::Operator, None) => {
            let snap = s.routes.sdk();
            let r = snap
                .channel_release(&name, DEFAULT_CHANNEL)
                .ok_or_else(ApiError::not_found)?;
            r.version.clone()
        }
        _ => {
            let (tenant, plan, project, environment) = caller_scope(&s, &a, &q).await?;
            let caller = SdkCaller {
                tenant,
                plan: &plan,
                project,
                environment: &environment,
            };
            let target = s
                .routes
                .sdk_resolve(&caller, &name)
                .map_err(|_| ApiError::not_found())?;
            target.version
        }
    };
    let release = s
        .meta
        .get_sdk_release(&name, &version)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok(typescript_resp(release.contract))
}
