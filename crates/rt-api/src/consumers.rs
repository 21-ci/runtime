//! Consumer routes and `POST /v1/pubsub/publish` (filled by task T23 of plan-final.md).

/// Consumer routes; empty until task T23 fills them in.
pub fn routes() -> axum::Router<crate::state::ApiState> {
    axum::Router::new()
}

/// OpenAPI paths and schemas of this module; merged into `control::api_doc()`.
#[derive(utoipa::OpenApi)]
#[openapi()]
pub struct Doc;
