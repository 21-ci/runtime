//! Metrics listener: `/metrics`, `/healthz`, `/readyz`.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::state::ApiState;

pub fn health_router(state: ApiState) -> Router {
    let m = state.metrics.clone();
    let router = Router::new()
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state);
    crate::invoke::with_http_metrics(router, m, "metrics")
}

/// Prometheus text exposition.
async fn metrics(State(s): State<ApiState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        s.metrics.render(),
    )
}

/// Liveness: sysmon heartbeat within the last second (OBS-10).
async fn healthz(State(s): State<ApiState>) -> impl IntoResponse {
    if s.node.healthy() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "sysmon heartbeat stale")
    }
}

/// Readiness: migrations applied, base snapshot loaded, not draining.
async fn readyz(State(s): State<ApiState>) -> impl IntoResponse {
    if s.node.ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}
