//! Async invocation routes: accept, list, get and retry (filled by task T25 of plan-final.md).

/// Invocation routes; empty until task T25 fills them in.
pub fn routes() -> axum::Router<crate::state::ApiState> {
    axum::Router::new()
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct Doc;
