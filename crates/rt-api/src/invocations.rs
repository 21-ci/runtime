//! Async invocation routes (`/v1/services/{s}/invocations…`): list, get, redrive and purge the
//! records accepted through `Prefer: respond-async` / `X-Rt-Invocation-Type: Event` on the invoke
//! path (`invoke.rs`).
//!
//! A view never carries the request headers, the body or the query string: they may hold the
//! end user's credentials (they stay sealed in `queue.db`).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use rt_store::queue::{InvocationRecord, InvocationState};
use rt_types::{InvocationId, Scope};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::auth::Auth;
use crate::control::{audit, load_service};
use crate::dto::Redriven;
use crate::error::ApiError;
use crate::extract::ApiQuery;
use crate::state::{ApiState, ControlEvent};

type R<T> = Result<Json<T>, ApiError>;

/// One async invocation, without its request headers, body or query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct InvocationView {
    pub id: String,
    /// `pending` | `succeeded` | `failed`.
    pub state: String,
    pub method: String,
    /// The request path, without the query.
    pub path: String,
    /// The request had a query string (not shown).
    pub has_query: bool,
    /// Deliveries started.
    pub attempts: u16,
    /// Status of the last delivery that answered.
    pub status: Option<u16>,
    /// `success` | `retries_exhausted` | `event_age_exceeded` | `unreadable`.
    pub condition: Option<String>,
    /// `status_5xx` | `status_429` | `guest_error` | `timeout` | `cpu_limit` | `oom` |
    /// `build_failed` | `crashed` | `sealed_unreadable`.
    pub error: Option<String>,
    pub body_bytes: u64,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: i64,
}

impl From<InvocationRecord> for InvocationView {
    fn from(r: InvocationRecord) -> Self {
        let (path, has_query) = split_query(&r.uri);
        Self {
            id: r.id.to_string(),
            state: r.state.as_str().to_string(),
            method: r.method,
            path,
            has_query,
            attempts: r.attempts,
            status: r.status,
            condition: r.condition,
            error: r.error,
            body_bytes: r.body_bytes,
            created_at: r.created_at_ms,
            updated_at: r.updated_at_ms,
            expires_at: r.expires_at_ms,
        }
    }
}

/// `(path, has_query)` of a stored request URI.
fn split_query(uri: &str) -> (String, bool) {
    match uri.split_once('?') {
        Some((p, _)) => (p.to_string(), true),
        None => (uri.to_string(), false),
    }
}

/// Pending and failed records of a service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct InvocationCountsView {
    pub pending: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct InvocationList {
    pub items: Vec<InvocationView>,
    pub next_cursor: Option<String>,
    pub counts: InvocationCountsView,
}

/// `GET /v1/services/{s}/invocations?state&limit&cursor`.
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct InvocationsQuery {
    /// `pending` | `succeeded` | `failed` (default all).
    pub state: Option<String>,
    /// Page size (default 50, max 200).
    pub limit: Option<u32>,
    /// Opaque cursor from a previous `next_cursor`.
    pub cursor: Option<String>,
}

/// `DELETE /v1/services/{s}/invocations?state=failed`.
#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PurgeQuery {
    /// Only `failed` is accepted.
    pub state: Option<String>,
}

fn parse_state(s: Option<&str>) -> Result<Option<InvocationState>, ApiError> {
    match s {
        None | Some("") => Ok(None),
        Some(v) => InvocationState::parse(v)
            .map(Some)
            .ok_or_else(|| ApiError::invalid("state must be pending, succeeded or failed")),
    }
}

#[utoipa::path(get, path = "/v1/services/{s}/invocations", tag = "invocations",
    params(("s" = String, Path, description = "service id"), InvocationsQuery),
    responses((status = 200, body = InvocationList)))]
async fn invocations_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<InvocationsQuery>,
) -> R<InvocationList> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let state = parse_state(q.state.as_deref())?;
    let limit = q.limit.unwrap_or(50).clamp(1, 200) as usize;
    let page = s
        .invocations
        .list_invocations(svc.id, state, limit, q.cursor.filter(|c| !c.is_empty()))
        .await?;
    Ok(Json(InvocationList {
        items: page.items.into_iter().map(InvocationView::from).collect(),
        next_cursor: page.next_cursor,
        counts: InvocationCountsView {
            pending: page.counts.pending,
            failed: page.counts.failed,
        },
    }))
}

#[utoipa::path(get, path = "/v1/services/{s}/invocations/{id}", tag = "invocations",
    params(("s" = String, Path, description = "service id"), ("id" = String, Path, description = "invocation id")),
    responses((status = 200, body = InvocationView)))]
async fn invocations_get(
    State(s): State<ApiState>,
    a: Auth,
    Path((sid, iid)): Path<(String, String)>,
) -> R<InvocationView> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &sid).await?;
    let iid = iid.parse::<InvocationId>().map_err(|_| ApiError::not_found())?;
    let rec = s
        .invocations
        .invocation(svc.id, iid)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(rec.into()))
}

#[utoipa::path(post, path = "/v1/services/{s}/invocations/redrive", tag = "invocations",
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = Redriven)))]
async fn invocations_redrive(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<Redriven> {
    a.require_scope(Scope::QueueWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let n = s.invocations.redrive_invocations(svc.id).await?;
    if n > 0 {
        s.node.notify(ControlEvent::AsyncAccepted(svc.id));
    }
    audit(
        &s,
        &a,
        "invocations.redriven",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"redriven": n}),
    );
    Ok(Json(Redriven { redriven: n }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/invocations", tag = "invocations",
    params(("s" = String, Path, description = "service id"), PurgeQuery),
    responses((status = 204)))]
async fn invocations_purge(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<PurgeQuery>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::QueueWrite)?;
    if q.state.as_deref() != Some("failed") {
        return Err(ApiError::invalid("state=failed is required"));
    }
    let svc = load_service(&s, &a, &id).await?;
    let n = s.invocations.purge_failed(svc.id).await?;
    audit(
        &s,
        &a,
        "invocations.purged",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"purged": n}),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// Invocation routes.
pub fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/v1/services/{s}/invocations",
            get(invocations_list).delete(invocations_purge),
        )
        .route("/v1/services/{s}/invocations/redrive", post(invocations_redrive))
        .route("/v1/services/{s}/invocations/{id}", get(invocations_get))
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(invocations_list, invocations_get, invocations_redrive, invocations_purge),
    components(schemas(InvocationView, InvocationCountsView, InvocationList))
)]
pub struct Doc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn views_drop_the_query() {
        assert_eq!(split_query("/hook?token=x"), ("/hook".to_string(), true));
        assert_eq!(split_query("/hook"), ("/hook".to_string(), false));
        assert_eq!(split_query("/?"), ("/".to_string(), true));
    }

    #[test]
    fn state_filter_takes_the_three_states() {
        assert_eq!(parse_state(None).unwrap(), None);
        assert_eq!(parse_state(Some("failed")).unwrap(), Some(InvocationState::Failed));
        assert_eq!(parse_state(Some("pending")).unwrap(), Some(InvocationState::Pending));
        assert!(parse_state(Some("dead")).is_err());
    }
}
