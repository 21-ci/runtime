//! Topic CRUD and topic messages on `/v1/topics` (filled by task T22 of plan-final.md).

/// Topic routes; empty until task T22 fills them in.
pub fn routes() -> axum::Router<crate::state::ApiState> {
    axum::Router::new()
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct Doc;
