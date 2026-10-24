//! Keys and self-service tokens (decision D-B4, SECURITY.md).
//!
//! The operator creates keys for any tenant (`/v1/tenants/{t}/keys`). A tenant key with the scope
//! `keys:write` manages the keys of its own tenant (`/v1/keys`): it can create keys that hold at
//! most its own scopes, project and environment restriction and lifetime ([`rt_sec::delegate`]), list them and
//! revoke them. Both paths issue and revoke through the functions here, so a key is created, cached
//! and audited the same way whoever asks. The secret is returned once, at creation.

use std::str::FromStr;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use rt_sec::{delegate, Grant, KeySpec, Principal};
use rt_store::StoreError;
use rt_types::{ApiKey, KeyId, ProjectId, Scope, Scopes, TenantId};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{audit, key_view, rfc3339_ms};
use crate::dto::{CreateKeyBody, CreateTokenBody, KeyCreated, KeyView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::{ApiState, ControlEvent};

type R<T> = Result<Json<T>, ApiError>;

/// Most keys a tenant may hold at once (not revoked, not expired) when they are created through
/// `POST /v1/keys`: tokens are cheap to mint, and a runaway agent must not fill `meta.db`.
pub const MAX_ACTIVE_KEYS_PER_TENANT: usize = 200;

/// A key as `GET /v1/keys` lists it: the usual view plus the key that minted it.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TokenView {
    #[serde(flatten)]
    pub key: KeyView,
    /// The key that created this one through `POST /v1/keys`; revoking it revokes this one too.
    /// `null` for keys the operator created.
    pub parent_key_id: Option<String>,
}

/// What the caller may hand to a key it creates. A tenant key's own expiry is not carried by the
/// `Principal`, so it is read once here (the only key lookup on the issue path).
async fn parent_grant(s: &ApiState, a: &Auth) -> Result<Grant, ApiError> {
    match &a.principal {
        Principal::Operator => Ok(Grant::unrestricted()),
        Principal::Tenant {
            key,
            scopes,
            project_ids,
            environments,
            ..
        } => {
            let own = s
                .meta
                .find_key(*key)
                .await?
                .ok_or_else(|| ApiError::new("unauthorized", "unauthorized"))?;
            Ok(Grant {
                scopes: scopes.clone(),
                project_ids: project_ids.clone(),
                environments: environments.clone(),
                expires_at_ms: own.expires_at_ms,
            })
        }
        Principal::Internal { .. } | Principal::Anonymous => Err(ApiError::new("unauthorized", "unauthorized")),
    }
}

/// May the caller see (list, and address) `key`? The operator and an unrestricted key see every
/// key of the tenant; a key limited to some projects or environments sees only keys limited to a
/// part of those (an unlimited key is not within a limited one).
fn sees(a: &Auth, key: &ApiKey) -> bool {
    match &a.principal {
        Principal::Tenant {
            project_ids,
            environments,
            ..
        } => {
            let within_projects = match project_ids {
                None => true,
                Some(own) => key
                    .project_ids
                    .as_ref()
                    .is_some_and(|theirs| theirs.iter().all(|p| own.contains(p))),
            };
            let within_envs = match environments {
                None => true,
                Some(own) => key
                    .environments
                    .as_ref()
                    .is_some_and(|theirs| theirs.iter().all(|e| own.contains(e))),
            };
            within_projects && within_envs
        }
        _ => true,
    }
}

/// May the caller revoke a key it [`sees`]? Only when the key is no stronger: every scope it holds
/// the caller holds too. The operator revokes anything.
fn outranks(a: &Auth, key: &ApiKey) -> bool {
    match &a.principal {
        Principal::Operator => true,
        Principal::Tenant { scopes, .. } => key.scopes.iter().all(|sc| scopes.contains(sc)),
        Principal::Internal { .. } | Principal::Anonymous => false,
    }
}

/// Issue a key for `tenant` on behalf of the caller: validate the request, check it against what
/// the caller holds, store it, tell the node and audit it. `self_service` marks `POST /v1/keys`.
pub(crate) async fn issue_key(
    s: &ApiState,
    a: &Auth,
    tenant: TenantId,
    body: CreateKeyBody,
    self_service: bool,
) -> Result<KeyCreated, ApiError> {
    s.meta.get_tenant(tenant).await?.ok_or_else(ApiError::not_found)?;
    if let Some(n) = &body.name {
        crate::control::check_display_name("name", n)?;
    }
    let mut scopes = Vec::new();
    for sc in &body.scopes {
        scopes.push(Scope::from_str(sc).map_err(ApiError::invalid)?);
    }
    if scopes.is_empty() {
        return Err(ApiError::invalid("`scopes` must not be empty"));
    }
    let project_ids = match &body.project_ids {
        None => None,
        Some(list) => {
            let mut ids = Vec::with_capacity(list.len());
            for raw in list {
                let id: ProjectId = raw
                    .parse()
                    .map_err(|_| ApiError::invalid(format!("`{raw}` is not a project id")))?;
                ids.push(id);
            }
            let found = s.meta.tenant_projects_among(tenant, &ids).await?;
            if let Some((raw, _)) = list.iter().zip(&ids).find(|(_, id)| !found.contains(id)) {
                return Err(ApiError::invalid(format!(
                    "project `{raw}` does not belong to the tenant"
                )));
            }
            Some(ids)
        }
    };
    if let Some(envs) = &body.environments {
        if let Some(bad) = envs.iter().find(|e| !rt_types::valid_environment(e)) {
            return Err(ApiError::invalid(format!(
                "environment `{bad}` must match ^[a-z][a-z0-9-]{{0,31}}$"
            )));
        }
    }
    let now = s.clock.unix_ms();
    let expires_at_ms = match &body.expires_at {
        None => None,
        Some(e) => {
            let ms = rfc3339_ms(e)?;
            if ms <= now {
                return Err(ApiError::invalid("`expires_at` is in the past"));
            }
            Some(ms)
        }
    };
    let parent = parent_grant(s, a).await?;
    let requested = Grant {
        scopes: scopes.into_iter().collect::<Scopes>(),
        project_ids,
        environments: body.environments.clone(),
        expires_at_ms,
    };
    let granted = delegate(&parent, &requested).map_err(|e| {
        ApiError::new("insufficient_scope", e.to_string()).with_details(json!({"reason": "escalation"}))
    })?;
    let (secret, new) = s.hasher.issue(KeySpec {
        name: body.name,
        scopes: granted.scopes,
        project_ids: granted.project_ids,
        environments: granted.environments,
        expires_at_ms: granted.expires_at_ms,
    });
    let parent_key = match &a.principal {
        Principal::Tenant { key, .. } => Some(*key),
        _ => None,
    };
    // Self-service keys are capped; the count and the insert are one transaction, so requests at
    // once cannot overshoot it.
    let created = if self_service {
        s.meta
            .create_child_key_capped(tenant, new, parent_key, MAX_ACTIVE_KEYS_PER_TENANT as u64, now)
            .await
    } else {
        s.meta.create_child_key(tenant, new, parent_key).await
    };
    let key = created.map_err(|e| match e {
        StoreError::Conflict("quota_exceeded") => ApiError::new(
            "quota_exceeded",
            format!("a tenant may hold at most {MAX_ACTIVE_KEYS_PER_TENANT} active keys; revoke some first"),
        ),
        // The minting key was revoked while this request was in flight (KEY-035).
        StoreError::NotFound if parent_key.is_some() => ApiError::new("unauthorized", "unauthorized"),
        other => other.into(),
    })?;
    s.node.notify(ControlEvent::KeyUpserted(key.clone()));
    let view = key_view(&key);
    audit(
        s,
        a,
        "key.created",
        Some(tenant),
        None,
        json!({
            "key_id": key.id.to_string(),
            "scopes": view.scopes,
            "project_ids": view.project_ids,
            "environments": view.environments,
            "expires_at": key.expires_at_ms,
            "self_service": self_service,
        }),
    );
    Ok(KeyCreated {
        key: view,
        secret: secret.expose().to_string(),
    })
}

/// Revoke `key` of `tenant` for the caller, and with it every key it minted (transitively, in one
/// transaction). `404` when it does not exist in the tenant, is already revoked or is outside the
/// caller's project and environment restriction; `403` when it holds a scope the caller does not.
pub(crate) async fn revoke_key(
    s: &ApiState,
    a: &Auth,
    tenant: TenantId,
    key: KeyId,
    self_service: bool,
) -> Result<(), ApiError> {
    // A revoked key is gone from the API: deleting it again is a 404 (SEC-06).
    let found = s
        .meta
        .find_key(key)
        .await?
        .filter(|k| k.tenant_id == tenant && !k.revoked);
    let Some(k) = found else {
        return Err(ApiError::not_found());
    };
    if !sees(a, &k) {
        return Err(ApiError::not_found());
    }
    if !outranks(a, &k) {
        return Err(
            ApiError::new("insufficient_scope", "the key holds more than the caller")
                .with_details(json!({"reason": "escalation"})),
        );
    }
    let revoked = s.meta.revoke_key_tree(tenant, key).await?;
    if revoked.is_empty() {
        return Err(ApiError::not_found());
    }
    for id in &revoked {
        s.node.notify(ControlEvent::KeyRevoked(*id));
    }
    let cascaded: Vec<String> = revoked.iter().skip(1).map(ToString::to_string).collect();
    audit(
        s,
        a,
        "key.deleted",
        Some(tenant),
        None,
        json!({"key_id": key.to_string(), "self_service": self_service, "cascaded": cascaded}),
    );
    Ok(())
}

/// Create a key in the caller's own tenant. The new key holds at most the caller's scopes and
/// project and environment restriction and does not outlive it; an omitted restriction or expiry is the caller's
/// own. The secret is shown once (API-13).
#[utoipa::path(post, path = "/v1/keys", tag = "keys", request_body = CreateTokenBody,
    responses((status = 201, body = KeyCreated), (status = 403, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn tokens_create(
    State(s): State<ApiState>,
    a: Auth,
    ApiJson(b): ApiJson<CreateTokenBody>,
) -> Result<(StatusCode, Json<KeyCreated>), ApiError> {
    a.require_scope(Scope::KeysWrite)?;
    let tenant = a.tenant()?;
    let body = CreateKeyBody {
        scopes: b.scopes,
        project_ids: b.project_ids,
        environments: b.environments,
        expires_at: b.expires_at,
        name: b.name,
    };
    Ok((StatusCode::CREATED, Json(issue_key(&s, &a, tenant, body, true).await?)))
}

/// The keys of the caller's own tenant that are not revoked (a key limited to some projects sees
/// only the keys limited to some of those). Never shows a secret.
#[utoipa::path(get, path = "/v1/keys", tag = "keys",
    responses((status = 200, body = Vec<TokenView>), (status = 403, body = ErrorEnvelope)))]
pub(crate) async fn tokens_list(State(s): State<ApiState>, a: Auth) -> R<Vec<TokenView>> {
    a.require_scope(Scope::KeysWrite)?;
    let tenant = a.tenant()?;
    let keys = s.meta.list_keys(tenant).await?;
    Ok(Json(
        keys.iter()
            .filter(|k| !k.revoked && sees(&a, k))
            .map(|k| TokenView {
                key: key_view(k),
                parent_key_id: k.parent_key_id.map(|p| p.to_string()),
            })
            .collect(),
    ))
}

/// Revoke a key of the caller's own tenant; it stops authenticating at once (SEC-06).
#[utoipa::path(delete, path = "/v1/keys/{key}", tag = "keys",
    params(("key" = String, Path, description = "key id")),
    responses((status = 204), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn tokens_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(k): Path<String>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::KeysWrite)?;
    let tenant = a.tenant()?;
    let key: KeyId = k.parse().map_err(|_| ApiError::not_found())?;
    revoke_key(&s, &a, tenant, key, true).await?;
    Ok(StatusCode::NO_CONTENT)
}
