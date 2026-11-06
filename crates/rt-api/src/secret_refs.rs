//! Secrets synced from an external source (R15): `…/secrets/{name}/ref` and `…/secrets/sync`. The
//! value lands in the sealed store by the usual path, so tasks read it with no I/O and a Vault
//! outage touches nothing that runs. No handler returns a value or a source's own text.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{post, put};
use axum::{Json, Router};
use rt_sec::secret_source::{full_path, valid_ref_path, SecretSource, SourceError};
use rt_sec::secrets::{secret_aad, Sealed, SecretsBackend};
use rt_store::{MetaStore, SecretRefTarget};
use rt_types::{Scope, Service};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{audit, load_service, valid_secret_name};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::{ApiState, ControlEvent};

/// What a node with an external source offers the API.
pub trait SecretSync: Send + Sync {
    fn source(&self) -> Arc<dyn SecretSource>;
    fn path_template(&self) -> String;
    /// Ask for a sync of the service soon. `false` over the limit of 6 a minute.
    fn request_sync(&self, service: rt_types::ServiceId) -> bool;
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PutSecretRefBody {
    /// Relative to the node's prefix for this tenant, project and environment.
    pub path: String,
    pub field: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SecretRefView {
    pub name: String,
    pub source: String,
    pub last_synced_at: Option<i64>,
}

fn source_err(e: SourceError) -> ApiError {
    let msg = match e {
        SourceError::Unavailable => "the secret source cannot be reached",
        SourceError::NotFound => "the secret source has no such secret or field",
    };
    ApiError::new(e.code(), msg)
}

/// Read one ref and seal its value if it changed. `Ok(true)` when it did.
pub async fn sync_one(
    meta: &Arc<dyn MetaStore>,
    secrets: &Arc<dyn SecretsBackend>,
    source: &Arc<dyn SecretSource>,
    template: &str,
    target: &SecretRefTarget,
    now_ms: i64,
) -> Result<bool, SourceError> {
    let r = &target.secret;
    let path = full_path(
        template,
        &target.tenant.to_string(),
        &target.project.to_string(),
        &target.environment,
        &r.path,
    )
    .ok_or(SourceError::NotFound)?;
    let got = source.read(&path, &r.field).await?;
    let aad = secret_aad(target.tenant, r.service, &r.name);
    let current = meta
        .get_sealed_secrets(r.service)
        .await
        .map_err(|_| SourceError::Unavailable)?
        .into_iter()
        .find(|(n, _)| *n == r.name)
        .and_then(|(_, b)| Sealed::from_bytes(&b).ok())
        .and_then(|s| secrets.open(&aad, &s).ok());
    let same = current.is_some_and(|c| c.as_slice() == got.value.as_bytes());
    if !same {
        let sealed = secrets.seal(&aad, got.value.as_bytes());
        meta.put_secret(r.service, &r.name, sealed.to_bytes())
            .await
            .map_err(|_| SourceError::Unavailable)?;
    }
    meta.set_secret_ref_status(r.service, &r.name, got.version, Some(now_ms), None)
        .await
        .map_err(|_| SourceError::Unavailable)?;
    Ok(!same)
}

fn writable(a: &Auth, svc: &Service) -> Result<(), ApiError> {
    if a.principal.can_touch(svc.project_id, &svc.environment) {
        Ok(())
    } else {
        Err(ApiError::new("insufficient_scope", "the key cannot touch this service"))
    }
}

#[utoipa::path(put, path = "/v1/services/{s}/secrets/{name}/ref", tag = "secrets", request_body = PutSecretRefBody,
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "secret name")),
    responses((status = 200, body = SecretRefView), (status = 404, body = ErrorEnvelope),
        (status = 422, body = ErrorEnvelope), (status = 503, body = ErrorEnvelope)))]
async fn ref_put(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
    ApiJson(b): ApiJson<PutSecretRefBody>,
) -> Result<Json<SecretRefView>, ApiError> {
    a.require_scope(Scope::SecretsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    writable(&a, &svc)?;
    if !valid_secret_name(&name) {
        return Err(ApiError::invalid(
            "secret names match [A-Z_][A-Z0-9_]* and must not start with RT_",
        ));
    }
    if b.field.is_empty() || b.field.len() > 256 {
        return Err(ApiError::invalid("`field`: 1..=256 bytes"));
    }
    if !valid_ref_path(&b.path) {
        return Err(ApiError::new(
            "ref_outside_prefix",
            "`path` must stay under the node's prefix: no `..`, `//`, `%` or leading `/`",
        ));
    }
    let Some(sync) = s.node.secret_sync() else {
        return Err(ApiError::invalid("this node has no external secret source"));
    };
    crate::limits::check_new_secret(&s, &svc, &name).await?;
    // The first read happens now: a bad ref is refused, and the old value stays.
    let now = chrono::Utc::now().timestamp_millis();
    let target = SecretRefTarget {
        secret: rt_store::SecretRef {
            service: svc.id,
            name: name.clone(),
            path: b.path.clone(),
            field: b.field.clone(),
            kv_version: None,
            last_synced_at_ms: None,
            last_error: None,
        },
        tenant: svc.tenant_id,
        project: svc.project_id,
        environment: svc.environment.clone(),
    };
    sync_one(&s.meta, &s.secrets, &sync.source(), &sync.path_template(), &target, now)
        .await
        .map_err(source_err)?;
    s.meta.put_secret_ref(svc.id, &name, &b.path, &b.field).await?;
    s.meta
        .set_secret_ref_status(svc.id, &name, None, Some(now), None)
        .await?;
    s.node.notify(ControlEvent::SecretsChanged(svc.id));
    audit(
        &s,
        &a,
        "secret.ref_written",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"name": name}),
    );
    Ok(Json(SecretRefView {
        name,
        source: "vault".into(),
        last_synced_at: Some(now),
    }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/secrets/{name}/ref", tag = "secrets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "secret name")),
    responses((status = 204), (status = 404, body = ErrorEnvelope)))]
async fn ref_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::SecretsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    writable(&a, &svc)?;
    if !s.meta.delete_secret_ref(svc.id, &name).await? {
        return Err(ApiError::new("not_found", "no such secret ref"));
    }
    audit(
        &s,
        &a,
        "secret.ref_deleted",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"name": name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/v1/services/{s}/secrets/sync", tag = "secrets",
    params(("s" = String, Path, description = "service id")),
    responses((status = 202), (status = 429, body = ErrorEnvelope), (status = 503, body = ErrorEnvelope)))]
async fn sync_now(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::SecretsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    writable(&a, &svc)?;
    let Some(sync) = s.node.secret_sync() else {
        return Err(ApiError::invalid("this node has no external secret source"));
    };
    if !sync.request_sync(svc.id) {
        return Err(ApiError::new("rate_limited", "at most 6 syncs a minute"));
    }
    Ok(StatusCode::ACCEPTED)
}

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/services/{s}/secrets/{name}/ref", put(ref_put).delete(ref_delete))
        .route("/v1/services/{s}/secrets/sync", post(sync_now))
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(ref_put, ref_delete, sync_now),
    components(schemas(PutSecretRefBody, SecretRefView))
)]
pub struct Doc;
