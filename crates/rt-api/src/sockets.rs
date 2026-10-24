//! WebSockets and durable objects in the control API (docs/api.md):
//!
//! - `GET /v1/services/{s}/objects` — the service's objects: those holding sockets and (durable
//!   services) those with storage (`services:read`);
//! - `GET /v1/services/{s}/objects/{name}/sockets` — an object's open sockets (`services:read`);
//! - `DELETE /v1/services/{s}/objects/{name}/sockets/{ws}` and `…/sockets?tag=` — close one socket or
//!   several (`sockets:write`, audited as `sockets.closed_by_api`);
//! - `DELETE /v1/services/{s}/objects/{name}` — drop an object's storage and alarm (`data:admin`).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use rt_types::{valid_app_close_code, valid_object_name, Scope, SocketInfo, MAX_CLOSE_REASON_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::auth::Auth;
use crate::control::{audit, load_service};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiQuery;
use crate::state::ApiState;

type R<T> = Result<Json<T>, ApiError>;

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ObjectsQuery {
    /// Only objects whose name starts with this.
    pub prefix: Option<String>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Page size, 1-1000 (default 100).
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ObjectView {
    pub name: String,
    /// Open sockets of the object.
    pub sockets: u32,
    /// The object has durable storage (keys or an alarm).
    pub storage: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ObjectPage {
    pub items: Vec<ObjectView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SocketsQuery {
    /// Only sockets with this tag.
    pub tag: Option<String>,
    pub cursor: Option<String>,
    /// Page size, 1-1000 (default 100).
    pub limit: Option<u32>,
}

/// One open socket.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SocketView {
    /// `ws_…`
    pub id: String,
    pub object: String,
    pub tags: Vec<String>,
    pub protocol: Option<String>,
    /// Unix ms.
    pub created_at: i64,
    pub last_message_at: Option<i64>,
    pub auto_response_at: Option<i64>,
    /// Bytes queued for the client.
    pub buffered_amount: u64,
    pub remote_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SocketPage {
    pub items: Vec<SocketView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct CloseQuery {
    /// Close code: 1000 (default) or 3000-4999.
    pub code: Option<u16>,
    /// Close reason, at most 123 bytes.
    pub reason: Option<String>,
    /// `DELETE …/sockets` only: close the sockets with this tag (default every socket of the object).
    pub tag: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ClosedView {
    pub closed: u32,
}

pub(crate) fn socket_view(i: SocketInfo) -> SocketView {
    SocketView {
        id: i.id,
        object: i.object,
        tags: i.tags,
        protocol: i.protocol,
        created_at: i.created_at_ms,
        last_message_at: i.last_message_at_ms,
        auto_response_at: i.auto_response_at_ms,
        buffered_amount: i.buffered_amount,
        remote_ip: i.remote_ip,
    }
}

fn object_name(name: &str) -> Result<&str, ApiError> {
    if valid_object_name(name) {
        Ok(name)
    } else {
        Err(ApiError::invalid(
            "an object name is 1 to 256 bytes without control, invisible or text-direction characters",
        ))
    }
}

fn close_args(q: &CloseQuery) -> Result<(u16, String), ApiError> {
    let code = q.code.unwrap_or(1000);
    if !valid_app_close_code(code) {
        return Err(ApiError::invalid("`code` is 1000 or 3000-4999"));
    }
    let reason = q.reason.clone().unwrap_or_default();
    if reason.len() > MAX_CLOSE_REASON_BYTES {
        return Err(ApiError::invalid(format!(
            "`reason` is at most {MAX_CLOSE_REASON_BYTES} bytes"
        )));
    }
    Ok((code, reason))
}

/// The objects of a durable service with storage, from its data (none for other services).
async fn storage_objects(s: &ApiState, svc: &rt_types::Service, prefix: &str) -> Result<Vec<String>, ApiError> {
    if !svc.config.durable {
        return Ok(Vec::new());
    }
    Ok(s.data.durable(svc.id).objects(prefix, None, 1000).await?.0)
}

#[utoipa::path(get, path = "/v1/services/{s}/objects", tag = "sockets",
    params(("s" = String, Path, description = "service id"), ObjectsQuery),
    responses((status = 200, body = ObjectPage), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn objects_list(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    ApiQuery(q): ApiQuery<ObjectsQuery>,
) -> R<ObjectPage> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let prefix = q.prefix.unwrap_or_default();
    let limit = q.limit.unwrap_or(100).clamp(1, 1000) as usize;
    let mut all: std::collections::BTreeMap<String, ObjectView> = std::collections::BTreeMap::new();
    for (name, sockets) in s.sockets.objects_of(svc.id, &prefix, 1000, None).0 {
        all.insert(
            name.clone(),
            ObjectView {
                name,
                sockets,
                storage: false,
            },
        );
    }
    for name in storage_objects(&s, &svc, &prefix).await? {
        all.entry(name.clone())
            .or_insert(ObjectView {
                name,
                sockets: 0,
                storage: false,
            })
            .storage = true;
    }
    let mut items: Vec<ObjectView> = all
        .into_values()
        .filter(|o| q.cursor.as_deref().is_none_or(|c| o.name.as_str() > c))
        .take(limit + 1)
        .collect();
    let next_cursor = if items.len() > limit {
        items.truncate(limit);
        items.last().map(|o| o.name.clone())
    } else {
        None
    };
    Ok(Json(ObjectPage { items, next_cursor }))
}

#[utoipa::path(get, path = "/v1/services/{s}/objects/{name}/sockets", tag = "sockets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "object name"), SocketsQuery),
    responses((status = 200, body = SocketPage), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn sockets_list(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
    ApiQuery(q): ApiQuery<SocketsQuery>,
) -> R<SocketPage> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    let limit = q.limit.unwrap_or(100).clamp(1, 1000) as usize;
    let (items, next_cursor) = s.sockets.list_service(
        svc.id,
        Some(object_name(&name)?),
        q.tag.as_deref(),
        limit,
        q.cursor.as_deref(),
    );
    Ok(Json(SocketPage {
        items: items.into_iter().map(socket_view).collect(),
        next_cursor,
    }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/objects/{name}/sockets/{ws}", tag = "sockets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "object name"),
        ("ws" = String, Path, description = "socket id"), CloseQuery),
    responses((status = 204), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn socket_close(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name, ws)): Path<(String, String, String)>,
    ApiQuery(q): ApiQuery<CloseQuery>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::SocketsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let name = object_name(&name)?;
    let (code, reason) = close_args(&q)?;
    // By id, not through a listing page: an object may hold more than 1000 sockets (WSUP-010).
    let of_object = s.sockets.object_of_socket(svc.id, &ws).as_deref() == Some(name);
    if !of_object || !s.sockets.close_socket(svc.id, &ws, code, &reason) {
        return Err(ApiError::not_found());
    }
    audit(
        &s,
        &a,
        "sockets.closed_by_api",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"object": name, "socket": ws, "code": code}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(delete, path = "/v1/services/{s}/objects/{name}/sockets", tag = "sockets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "object name"), CloseQuery),
    responses((status = 200, body = ClosedView), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn sockets_close(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
    ApiQuery(q): ApiQuery<CloseQuery>,
) -> R<ClosedView> {
    a.require_scope(Scope::SocketsWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    let name = object_name(&name)?;
    let (code, reason) = close_args(&q)?;
    let closed = s
        .sockets
        .close_service(svc.id, Some(name), q.tag.as_deref(), code, &reason);
    audit(
        &s,
        &a,
        "sockets.closed_by_api",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"object": name, "tag": q.tag, "code": code, "closed": closed}),
    );
    Ok(Json(ClosedView { closed }))
}

#[utoipa::path(delete, path = "/v1/services/{s}/objects/{name}", tag = "sockets",
    params(("s" = String, Path, description = "service id"), ("name" = String, Path, description = "object name")),
    responses((status = 204), (status = 404, body = ErrorEnvelope)))]
pub(crate) async fn object_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path((id, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    a.require_scope(Scope::DataAdmin)?;
    let svc = load_service(&s, &a, &id).await?;
    let name = object_name(&name)?;
    if !svc.config.durable {
        return Err(ApiError::not_found());
    }
    let existed = s.data.durable(svc.id).delete_object(name).await?;
    s.sockets.set_alarm_for(svc.tenant_id, svc.id, name, None);
    if !existed {
        return Err(ApiError::not_found());
    }
    audit(
        &s,
        &a,
        "object.storage_deleted",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"object": name}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, StatusCode};
    use bytes::Bytes;
    use rt_store::{DurableValue, DurableWrite};

    use crate::test_support;

    /// Give durable `object` a stored key, so it is listed as having storage.
    async fn keep(node: &test_support::TestNode, svc: rt_types::ServiceId, object: &str) {
        let write = DurableWrite {
            delete_all: false,
            writes: vec![(
                Bytes::from_static(b"k"),
                Some(DurableValue {
                    format: 0,
                    data: Bytes::from_static(b"v"),
                }),
            )],
            alarm: None,
        };
        node.state
            .data
            .durable(svc)
            .commit(object, write)
            .await
            .expect("commit");
    }

    /// The object list is a page sorted by name: `limit` objects and the cursor of the next page,
    /// which starts after the last name returned.
    #[tokio::test]
    async fn durable_objects_are_listed_by_name_in_cursor_pages() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        let svc = test_support::service(&node, t, "rooms", serde_json::json!({"durable": true})).await;
        for name in ["a", "b", "c"] {
            keep(&node, svc, name).await;
        }
        let uri = format!("/v1/services/{svc}/objects");
        let (status, _, first) = node.call(Method::GET, &format!("{uri}?limit=2"), Some(t), None).await;
        assert_eq!(status, StatusCode::OK, "{first}");
        let names: Vec<&str> = first["items"]
            .as_array()
            .expect("items")
            .iter()
            .map(|o| o["name"].as_str().expect("name"))
            .collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(first["next_cursor"], "b");

        let (_, _, second) = node
            .call(Method::GET, &format!("{uri}?limit=2&cursor=b"), Some(t), None)
            .await;
        assert_eq!(second["items"][0]["name"], "c");
        assert!(second["next_cursor"].is_null(), "the last page has no cursor");
    }

    /// Deleting durable storage answers 204 once; the object is gone, so a second delete is 404.
    #[tokio::test]
    async fn deleting_an_object_twice_answers_204_then_404() {
        let node = test_support::node().await;
        let t = test_support::tenant(&node, "acme").await;
        let svc = test_support::service(&node, t, "rooms", serde_json::json!({"durable": true})).await;
        keep(&node, svc, "room-1").await;
        let uri = format!("/v1/services/{svc}/objects/room-1");
        let (status, _, _) = node.call(Method::DELETE, &uri, Some(t), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _, body) = node.call(Method::DELETE, &uri, Some(t), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["code"], "not_found");
    }
}
