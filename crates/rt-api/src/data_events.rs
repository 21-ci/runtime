//! `GET|PUT|DELETE /v1/services/{s}/data-events` (R14): data changes published to a stream topic.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::get;
use axum::{Json, Router};
use rt_types::events::{DataEventsConfig, Namespace};
use rt_types::{Scope, Service, ServicePatch, TriggerKind};
use serde::Serialize;
use serde_json::{json, Value};
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::{audit, if_match, load_service, refresh_routes, warm_floor_cap};
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::{ApiState, ControlEvent};

type R<T> = Result<Json<T>, ApiError>;

/// `state`: `disabled`, `pending`, `active`, `paused_overflow` or `error`.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct DataEventsView {
    pub state: String,
    /// Age in milliseconds of the oldest change not yet published.
    pub lag: u64,
    /// Changes not yet published.
    pub backlog: u64,
    pub last_error: Option<String>,
    /// The subscribed tables as configured.
    #[schema(value_type = Vec<Object>)]
    pub tables: Value,
    /// The subscribed KV prefixes as configured.
    #[schema(value_type = Vec<Object>)]
    pub kv_prefixes: Value,
    pub topic: Option<String>,
    /// Subscribed tables that do not exist yet, or are too wide for `row`.
    pub warnings: Vec<String>,
}

fn view(s: &ApiState, svc: &Service) -> DataEventsView {
    let Some(cfg) = &svc.config.events.data_events else {
        return DataEventsView {
            state: "disabled".into(),
            lag: 0,
            backlog: 0,
            last_error: None,
            tables: json!([]),
            kv_prefixes: json!([]),
            topic: None,
            warnings: Vec::new(),
        };
    };
    let st = s.node.data_events_status(svc.id).unwrap_or_default();
    DataEventsView {
        state: if st.state.is_empty() {
            "pending".into()
        } else {
            st.state
        },
        lag: st.lag_ms,
        backlog: st.backlog,
        last_error: st.last_error,
        tables: json!(cfg.tables),
        kv_prefixes: json!(cfg.kv_prefixes),
        topic: Some(cfg.topic.clone()),
        warnings: st.warnings,
    }
}

async fn save(
    s: &ApiState,
    a: &Auth,
    svc: &Service,
    h: &HeaderMap,
    cfg: Option<DataEventsConfig>,
) -> Result<Service, ApiError> {
    let mut events = svc.config.events.clone();
    events.data_events = cfg;
    let patch = ServicePatch {
        events: Some(events),
        ..ServicePatch::default()
    };
    let cap = warm_floor_cap(&s.plans.plan_for(svc.tenant_id));
    let updated = s
        .meta
        .patch_service_capped(a.scope, svc.id, patch, if_match(h)?, cap)
        .await?;
    refresh_routes(s).await?;
    s.node.notify(ControlEvent::DataEventsChanged(svc.id));
    Ok(updated)
}

#[utoipa::path(get, path = "/v1/services/{s}/data-events", tag = "services",
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = DataEventsView), (status = 404, body = ErrorEnvelope)))]
async fn data_events_get(State(s): State<ApiState>, a: Auth, Path(id): Path<String>) -> R<DataEventsView> {
    a.require_scope(Scope::ServicesRead)?;
    let svc = load_service(&s, &a, &id).await?;
    Ok(Json(view(&s, &svc)))
}

/// Saving also rearms events paused by an overflow.
#[utoipa::path(put, path = "/v1/services/{s}/data-events", tag = "services",
    params(("s" = String, Path, description = "service id")),
    request_body(content = Object, description = "`{topic, tables: [{name, ops, payload}], kv_prefixes: [{prefix, payload}], max_backlog}`"),
    responses((status = 200, body = DataEventsView), (status = 403, body = ErrorEnvelope),
        (status = 404, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
async fn data_events_put(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
    ApiJson(b): ApiJson<Value>,
) -> R<DataEventsView> {
    a.require_scope(Scope::ServicesWrite)?;
    // Keys can hold personal data, and a topic is readable with `services:read`: publishing is
    // reading.
    a.require_scope(Scope::DataRead)?;
    let svc = load_service(&s, &a, &id).await?;
    if !a.principal.can_touch(svc.project_id, &svc.environment) {
        return Err(ApiError::new("insufficient_scope", "the key cannot touch this service"));
    }
    let cfg: DataEventsConfig =
        serde_json::from_value(b).map_err(|e| ApiError::invalid(format!("`data-events`: {e}")))?;
    cfg.validate().map_err(ApiError::invalid)?;
    if svc.config.durable {
        return Err(ApiError::invalid("durable services cannot use data events in v1"));
    }
    if !s.plans.plan_for(svc.tenant_id).allows_trigger(TriggerKind::Stream) {
        return Err(ApiError::new(
            "trigger_not_allowed",
            "the plan does not allow the `stream` trigger (`events.data_events`)",
        ));
    }
    let ns = Namespace {
        tenant: svc.tenant_id,
        project: svc.project_id,
        environment: svc.environment.clone(),
    };
    if s.streams.get_topic(&ns, &cfg.topic).await?.is_none() {
        return Err(ApiError::invalid(
            "`topic`: no such topic in the service's project and environment",
        ));
    }
    let tables: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    let data = json!({"topic": cfg.topic, "tables": tables, "kv_prefixes": cfg.kv_prefixes.len()});
    let updated = save(&s, &a, &svc, &h, Some(cfg)).await?;
    audit(&s, &a, "data_events.updated", Some(svc.tenant_id), Some(svc.id), data);
    Ok(Json(view(&s, &updated)))
}

#[utoipa::path(delete, path = "/v1/services/{s}/data-events", tag = "services",
    params(("s" = String, Path, description = "service id")),
    responses((status = 200, body = DataEventsView), (status = 404, body = ErrorEnvelope)))]
async fn data_events_delete(
    State(s): State<ApiState>,
    a: Auth,
    Path(id): Path<String>,
    h: HeaderMap,
) -> R<DataEventsView> {
    a.require_scope(Scope::ServicesWrite)?;
    let svc = load_service(&s, &a, &id).await?;
    if !a.principal.can_touch(svc.project_id, &svc.environment) {
        return Err(ApiError::new("insufficient_scope", "the key cannot touch this service"));
    }
    let updated = save(&s, &a, &svc, &h, None).await?;
    audit(
        &s,
        &a,
        "data_events.updated",
        Some(svc.tenant_id),
        Some(svc.id),
        json!({"enabled": false}),
    );
    Ok(Json(view(&s, &updated)))
}

/// A create or `PATCH` setting `events.data_events` is held to the rule of `PUT`.
pub(crate) fn check_block_scope(
    a: &Auth,
    old: Option<&DataEventsConfig>,
    new: Option<&DataEventsConfig>,
) -> Result<(), ApiError> {
    if new.is_some() && new != old {
        a.require_scope(Scope::DataRead)?;
    }
    Ok(())
}

pub fn routes() -> Router<ApiState> {
    Router::new().route(
        "/v1/services/{s}/data-events",
        get(data_events_get).put(data_events_put).delete(data_events_delete),
    )
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(data_events_get, data_events_put, data_events_delete),
    components(schemas(DataEventsView))
)]
pub struct Doc;
