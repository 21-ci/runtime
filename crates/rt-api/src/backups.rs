//! `/v1/node/backups*`: operator only. Rows and archives come from the node's own backup manager,
//! which `rt-server` provides. `…/archive` and `DELETE` are never for the console's binding: a
//! console bug must not become whole-node exfiltration or backup destruction (SECURITY.md).

use std::path::PathBuf;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rt_store::{BackupRow, Page, PageReq};
use serde::Serialize;
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{audit, page_req};
use crate::dto::PageQuery;
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiQuery;
use crate::state::ApiState;

/// The paths a console binding (`allowed_paths`) lists for backups, spelled out one by one: `*`
/// spans segments, so a glob would reach `…/archive`.
pub const CONSOLE_BINDING_PATHS: &[&str] = &["/v1/node/backups", "/v1/node/backups/status"];

/// Facts behind `GET /v1/node/backups/status`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BackupFacts {
    pub enabled: bool,
    pub schedule: String,
    pub last_ok_at: Option<i64>,
    pub last_ok_id: Option<String>,
    pub last_failure: Option<Value>,
    pub next_at: Option<i64>,
    pub running_id: Option<String>,
    pub last_restored_at: Option<i64>,
    pub remote_host: Option<String>,
    pub remote_bucket: Option<String>,
}

/// Backups of this node. `rt-api` reaches the backup machinery only through this; of `rt-backup` it
/// uses the app bundle format (`rt_backup::app`).
#[async_trait]
pub trait BackupControl: Send + Sync {
    async fn create(&self) -> Result<BackupRow, ApiError>;
    async fn list(&self, page: PageReq) -> Result<Page<BackupRow>, ApiError>;
    async fn get(&self, id: &str) -> Result<Option<BackupRow>, ApiError>;
    async fn status(&self) -> Result<BackupFacts, ApiError>;
    /// Where the archive of `id` would be: built from the id, not read from a row.
    fn archive_path(&self, id: &str) -> Option<PathBuf>;
    /// `false`: no such backup. `backup_running` while it runs.
    async fn delete(&self, id: &str) -> Result<bool, ApiError>;
    /// Stop a running backup at its next file (shutdown).
    fn cancel(&self);
}

/// A node with no backup code wired (API unit tests).
pub struct NoBackups;

#[async_trait]
impl BackupControl for NoBackups {
    async fn create(&self) -> Result<BackupRow, ApiError> {
        Err(ApiError::new("backup_disabled", "backups are off"))
    }
    async fn list(&self, _: PageReq) -> Result<Page<BackupRow>, ApiError> {
        Ok(Page {
            items: Vec::new(),
            next_cursor: None,
        })
    }
    async fn get(&self, _: &str) -> Result<Option<BackupRow>, ApiError> {
        Ok(None)
    }
    async fn status(&self) -> Result<BackupFacts, ApiError> {
        Ok(BackupFacts::default())
    }
    fn archive_path(&self, _: &str) -> Option<PathBuf> {
        None
    }
    async fn delete(&self, _: &str) -> Result<bool, ApiError> {
        Ok(false)
    }
    fn cancel(&self) {}
}

/// One backup run.
#[derive(Debug, Serialize, ToSchema)]
pub struct BackupView {
    pub id: String,
    /// `running`, `verifying`, `ok`, `failed`.
    pub state: String,
    /// `schedule` or `api`.
    pub trigger: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub bytes: Option<u64>,
    pub sha256: Option<String>,
    pub encrypted: bool,
    pub verified_at: Option<i64>,
    pub rt_version: String,
    /// `{code, message}` of a failed run.
    #[schema(value_type = Option<Object>)]
    pub failure: Option<Value>,
}

impl From<BackupRow> for BackupView {
    fn from(b: BackupRow) -> Self {
        Self {
            id: b.id,
            state: b.state,
            trigger: b.trigger,
            started_at: b.started_at,
            finished_at: b.finished_at,
            bytes: b.bytes,
            sha256: b.sha256,
            encrypted: b.encrypted,
            verified_at: b.verified_at,
            rt_version: b.rt_version,
            failure: b.failure,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupPage {
    pub items: Vec<BackupView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupStarted {
    pub id: String,
    pub state: String,
}

/// Where archives go: `local` or `s3`.
#[derive(Debug, Serialize, ToSchema)]
pub struct BackupDestination {
    pub kind: String,
    pub endpoint_host: Option<String>,
    pub bucket: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupStatusView {
    pub enabled: bool,
    pub schedule: String,
    pub last_ok_at: Option<i64>,
    pub last_ok_id: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub last_failure: Option<Value>,
    pub next_at: Option<i64>,
    /// The run in progress.
    pub running_id: Option<String>,
    /// The `node.restored` event: this data directory came out of an archive then.
    pub last_restored_at: Option<i64>,
    pub destination: BackupDestination,
}

/// Start a backup now (operator). It runs in the background: poll `GET /v1/node/backups/{id}`.
#[utoipa::path(post, path = "/v1/node/backups", tag = "node", responses(
    (status = 202, body = BackupStarted),
    (status = 403, body = ErrorEnvelope),
    (status = 409, description = "`backup_running`", body = ErrorEnvelope),
    (status = 503, description = "`backup_disabled`", body = ErrorEnvelope),
    (status = 507, description = "`insufficient_storage`", body = ErrorEnvelope)))]
pub(crate) async fn backups_create(State(s): State<ApiState>, a: Auth) -> Result<Response, ApiError> {
    a.require_operator()?;
    let row = s.backups.create().await?;
    audit(&s, &a, "backup.created", None, None, json!({"backup": row.id}));
    Ok((
        StatusCode::ACCEPTED,
        Json(BackupStarted {
            id: row.id,
            state: row.state,
        }),
    )
        .into_response())
}

/// Backups of this node, newest first (operator).
#[utoipa::path(get, path = "/v1/node/backups", tag = "node", params(PageQuery), responses(
    (status = 200, body = BackupPage), (status = 403, body = ErrorEnvelope)))]
pub(crate) async fn backups_list(
    State(s): State<ApiState>,
    a: Auth,
    ApiQuery(q): ApiQuery<PageQuery>,
) -> Result<Json<BackupPage>, ApiError> {
    a.require_operator()?;
    let page = s.backups.list(page_req(q.limit, q.cursor)).await?;
    Ok(Json(BackupPage {
        items: page.items.into_iter().map(Into::into).collect(),
        next_cursor: page.next_cursor,
    }))
}

/// Whether backups are on, the last good one, the last failure, the next run (operator).
#[utoipa::path(get, path = "/v1/node/backups/status", tag = "node", responses(
    (status = 200, body = BackupStatusView), (status = 403, body = ErrorEnvelope)))]
pub(crate) async fn backups_status(State(s): State<ApiState>, a: Auth) -> Result<Json<BackupStatusView>, ApiError> {
    a.require_operator()?;
    let f = s.backups.status().await?;
    Ok(Json(BackupStatusView {
        enabled: f.enabled,
        schedule: f.schedule,
        last_ok_at: f.last_ok_at,
        last_ok_id: f.last_ok_id,
        last_failure: f.last_failure,
        next_at: f.next_at,
        running_id: f.running_id,
        last_restored_at: f.last_restored_at,
        destination: BackupDestination {
            kind: if f.remote_host.is_some() { "s3" } else { "local" }.to_string(),
            endpoint_host: f.remote_host,
            bucket: f.remote_bucket,
        },
    }))
}

/// One backup (operator).
#[utoipa::path(get, path = "/v1/node/backups/{id}", tag = "node", params(("id" = String, Path)), responses(
    (status = 200, body = BackupView), (status = 403, body = ErrorEnvelope), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn backups_get(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
) -> Result<Json<BackupView>, ApiError> {
    a.require_operator()?;
    let row = s.backups.get(&id).await?;
    row.map(|b| Json(b.into()))
        .ok_or_else(|| ApiError::new("not_found", "no such backup"))
}

/// The archive, streamed; `Range` and `If-Range` work against its sha256 (operator, audited). Holds
/// every tenant's data: not for a console binding.
#[utoipa::path(get, path = "/v1/node/backups/{id}/archive", tag = "node", params(("id" = String, Path)), responses(
    (status = 200, content_type = "application/x-tar", description = "the archive"),
    (status = 206, description = "the requested range"),
    (status = 403, body = ErrorEnvelope),
    (status = 404, body = ErrorEnvelope),
    (status = 410, description = "`archive_gone`: retention or a restore removed the file", body = ErrorEnvelope)))]
pub(crate) async fn backups_archive(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
) -> Result<Response, ApiError> {
    a.require_operator()?;
    let range = match crate::file_response::parse_range(&h) {
        Ok(r) => r,
        Err(refused) => return Ok(refused),
    };
    let row = s
        .backups
        .get(&id)
        .await?
        .ok_or_else(|| ApiError::new("not_found", "no such backup"))?;
    let gone = || ApiError::new("archive_gone", "this backup has no archive on this node");
    if row.state != "ok" {
        return Err(gone());
    }
    let path = s.backups.archive_path(&row.id).ok_or_else(gone)?;
    let size = tokio::fs::metadata(&path).await.map_err(|_| gone())?.len();
    audit(
        &s,
        &a,
        "backup.downloaded",
        None,
        None,
        json!({"backup": row.id, "bytes": size}),
    );
    crate::file_response::file_response(&path, size, "application/x-tar", row.sha256.as_deref(), range, ()).await
}

/// Delete a backup and its archive (operator, audited). Not for a console binding.
#[utoipa::path(delete, path = "/v1/node/backups/{id}", tag = "node", params(("id" = String, Path)), responses(
    (status = 204, description = "deleted"),
    (status = 403, body = ErrorEnvelope),
    (status = 404, body = ErrorEnvelope),
    (status = 409, description = "`backup_running`", body = ErrorEnvelope)))]
pub(crate) async fn backups_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    if !s.backups.delete(&id).await? {
        return Err(ApiError::new("not_found", "no such backup"));
    }
    audit(&s, &a, "backup.deleted", None, None, json!({"backup": id}));
    Ok(StatusCode::NO_CONTENT)
}
