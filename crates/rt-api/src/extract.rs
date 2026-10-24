//! Extractors that turn every rejection into the control error envelope.

use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::error::ApiError;

/// Largest JSON body the control API accepts.
pub const MAX_JSON_BODY: usize = 1024 * 1024;

/// `Json<T>` whose failures are `422 validation_failed` envelopes.
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let body = axum::body::to_bytes(req.into_body(), MAX_JSON_BODY)
            .await
            .map_err(|_| ApiError::new("too_large", "request body too large"))?;
        let v = serde_json::from_slice::<T>(&body).map_err(|e| ApiError::invalid(format!("invalid JSON body: {e}")))?;
        Ok(ApiJson(v))
    }
}

/// `Query<T>` whose failures are `422 validation_failed` envelopes.
pub struct ApiQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(v)| ApiQuery(v))
            .map_err(|e| ApiError::invalid(format!("invalid query: {e}")))
    }
}
