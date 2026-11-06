//! SQL migrations of a version (R6): status, the WASM upload route and the dry runs. They run in the
//! `migrating` step of [`crate::promote::Promoter::promote`].
//!
//! The list of a version is set only from upload validation or from this write-once route. Audit
//! and events carry names and hashes, never SQL.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use rt_types::{build_migrations, plan_migrations, MigrationEntry, RuntimeKind, Scope, Service, VersionNo};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{audit, load_service, version_view};
use crate::dto::{MigrationView, VersionView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::ApiState;

type R<T> = Result<Json<T>, ApiError>;
type Created<T> = Result<(StatusCode, Json<T>), ApiError>;

/// `POST /v1/services/{s}/versions/{v}/migrations`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MigrationsBody {
    pub files: Vec<MigrationFileBody>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MigrationFileBody {
    /// `NNNN_name.sql`.
    pub name: String,
    pub sql: String,
}

/// One row of `_rt_migrations`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppliedMigrationView {
    pub name: String,
    pub sha256: String,
    pub applied_at: i64,
    /// The version that applied it.
    pub version: u32,
}

/// `GET /v1/services/{s}/migrations`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MigrationsStatus {
    pub applied: Vec<AppliedMigrationView>,
    /// What the newest version would run, in order.
    pub pending_for_latest: Vec<MigrationView>,
}

pub(crate) fn views(list: &[MigrationEntry]) -> Vec<MigrationView> {
    list.iter()
        .map(|m| MigrationView {
            name: m.name.clone(),
            sha256: m.sha256.clone(),
        })
        .collect()
}

fn store_failure(f: rt_types::MigrationFailure) -> ApiError {
    rt_store::StoreError::Migration(Box::new(f)).into()
}

/// What `list` would still run on `svc`; `migration_drift` or `migration_out_of_order` if it cannot.
pub(crate) async fn pending_names(
    s: &ApiState,
    svc: &Service,
    list: &[MigrationEntry],
) -> Result<Vec<String>, ApiError> {
    if list.is_empty() {
        return Ok(Vec::new());
    }
    let applied = s.data.sql(svc.id).applied_migrations().await?;
    let pending = plan_migrations(list, &applied).map_err(store_failure)?;
    Ok(pending.into_iter().map(|m| m.name).collect())
}

/// `PUT …/live` with `dry_run`: run what is pending on a copy, writing nothing.
pub(crate) async fn dry_run(s: &ApiState, svc: &Service, target: VersionNo) -> Result<Vec<String>, ApiError> {
    let version = s
        .meta
        .get_version(svc.id, target)
        .await?
        .ok_or_else(ApiError::not_found)?;
    if version.migrations.is_empty() {
        return Ok(Vec::new());
    }
    let _ = s.promoter.refresh().await; // the size limit of a service that was never live
    let files = s.meta.version_migration_files(svc.id, target).await?;
    let (budget, max_mb) = {
        let cfg = s.config.load();
        (cfg.deploy.migration_budget, u64::from(cfg.deploy.dry_run_max_mb))
    };
    let got = s
        .data
        .sql(svc.id)
        .migrate_dry_run(files, target, budget, max_mb.saturating_mul(1024 * 1024))
        .await?;
    Ok(got.pending)
}

#[utoipa::path(get, path = "/v1/services/{s}/migrations", tag = "versions",
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = MigrationsStatus), (status = 404, body = ErrorEnvelope),
        (status = 409, description = "`migration_drift` or `migration_out_of_order` for the newest version", body = ErrorEnvelope)))]
async fn migrations_status(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<MigrationsStatus> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let applied = s.data.sql(svc.id).applied_migrations().await?;
    let versions = s.meta.list_versions(svc.id).await?;
    let latest = versions.iter().max_by_key(|v| v.number);
    let pending = match latest {
        Some(v) => plan_migrations(&v.migrations, &applied).map_err(store_failure)?,
        None => Vec::new(),
    };
    Ok(Json(MigrationsStatus {
        applied: applied
            .into_iter()
            .map(|m| AppliedMigrationView {
                name: m.name,
                sha256: m.sha256,
                applied_at: m.applied_at_ms,
                version: m.version,
            })
            .collect(),
        pending_for_latest: views(&pending),
    }))
}

/// A component has no room for files, so its migrations come here, once, before it goes live.
#[utoipa::path(post, path = "/v1/services/{s}/versions/{v}/migrations", tag = "versions",
    params(("s" = String, Path, description = "service id"), ("v" = u32, Path, description = "version number")),
    request_body = MigrationsBody,
    responses((status = 201, body = VersionView), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope),
        (status = 409, description = "`migrations_from_source`, `migrations_already_set` or `version_is_live`", body = ErrorEnvelope),
        (status = 413, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn versions_migrations_set(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, v)): Path<(String, u32)>,
    ApiJson(b): ApiJson<MigrationsBody>,
) -> Created<VersionView> {
    a.require_scope(Scope::Deploy)?;
    let svc = load_service(&s, &a, &id).await?;
    if svc.runtime != RuntimeKind::Wasm {
        return Err(ApiError::new(
            "migrations_from_source",
            "a Deno version takes its migrations from migrations/ in its source",
        ));
    }
    if b.files.is_empty() {
        return Err(ApiError::invalid("`files` is empty"));
    }
    let files = build_migrations(b.files.iter().map(|f| (f.name.as_str(), f.sql.as_str())))
        .map_err(|e| ApiError::invalid(e.message).with_details(json!({"file": e.file})))?;
    let number = VersionNo(v);
    let entries: Vec<MigrationEntry> = files.iter().map(rt_types::MigrationFile::entry).collect();
    s.meta.set_version_migrations(svc.id, number, files).await?;
    audit(
        &s,
        &a,
        "version.migrations_set",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"version": v, "migrations": entries}),
    );
    let version = s
        .meta
        .get_version(svc.id, number)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok((StatusCode::CREATED, Json(version_view(&version))))
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(migrations_status, versions_migrations_set),
    components(schemas(MigrationsBody, MigrationFileBody, MigrationsStatus, AppliedMigrationView))
)]
pub(crate) struct Doc;

pub(crate) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/services/{s}/migrations", get(migrations_status))
        .route(
            "/v1/services/{s}/versions/{v}/migrations",
            post(versions_migrations_set),
        )
}
