//! Control API errors: the `{"error": {"code", "message", "details"}}` envelope (docs/api.md).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

/// Documented control error codes → HTTP status.
pub fn status_for_code(code: &str) -> StatusCode {
    match code {
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "insufficient_scope"
        | "quota_exceeded"
        | "trigger_not_allowed"
        | "binding_not_allowed"
        | "host_not_allowed"
        | "reserved" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        "slug_taken" | "host_taken" | "host_cooldown" | "version_not_ready" | "tenant_not_empty"
        | "project_slug_taken" | "project_not_empty" | "environment_in_use" | "sdk_release_exists" => {
            StatusCode::CONFLICT
        }
        "precondition_failed" => StatusCode::PRECONDITION_FAILED,
        "too_large" | "storage_quota" | "egress_quota" => StatusCode::PAYLOAD_TOO_LARGE,
        // Guest-data errors a control route can meet (data export, KV/SQL inspection): the
        // request was understood but the data refuses it (QUOTA-013).
        "sql_error" | "sql_not_authorized" | "sql_result_too_large" | "kv_not_integer" | "kv_overflow" => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        "sql_busy" | "sql_timeout" => StatusCode::SERVICE_UNAVAILABLE,
        "validation_failed"
        | "invalid_artifact"
        | "missing_export"
        | "limit_exceeds_plan"
        | "schedule_too_frequent"
        | "idempotency_mismatch"
        | "no_rpc_surface"
        | "sdk_contract_missing"
        | "sdk_unchecked_caller" => StatusCode::UNPROCESSABLE_ENTITY,
        "rate_limited" | "build_quota" | "queue_full" => StatusCode::TOO_MANY_REQUESTS,
        "control_disabled" => StatusCode::SERVICE_UNAVAILABLE,
        "not_implemented" => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Body of an error response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorBody {
    /// Stable machine-readable code.
    pub code: String,
    pub message: String,
    /// Extra structured data (e.g. validation diagnostics `[{file,line,col,message}]`).
    #[schema(value_type = Object)]
    pub details: serde_json::Value,
}

/// The envelope returned for every control error.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

/// Error type of control handlers.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
    pub details: serde_json::Value,
    /// `retry-after` seconds for `429`.
    pub retry_after: Option<u64>,
}

impl ApiError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
            retry_after: None,
        }
    }

    pub fn with_details(mut self, d: serde_json::Value) -> Self {
        self.details = d;
        self
    }

    pub fn not_found() -> Self {
        Self::new("not_found", "not found")
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("validation_failed", message)
    }

    pub fn not_implemented(what: &str) -> Self {
        Self::new("not_implemented", format!("{what} is not implemented"))
    }

    pub fn status(&self) -> StatusCode {
        status_for_code(self.code)
    }

    pub fn envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            error: ErrorBody {
                code: self.code.to_string(),
                message: self.message.clone(),
                details: self.details.clone(),
            },
        }
    }
}

impl From<rt_store::StoreError> for ApiError {
    fn from(e: rt_store::StoreError) -> Self {
        if let rt_store::StoreError::WarmFloorTotal { total, cap } = e {
            return ApiError::new(e.code(), e.to_string())
                .with_details(serde_json::json!({"field": "min_instances", "total": total, "plan_max": cap}));
        }
        // Sanitized: only the stable code, never driver text or paths.
        ApiError::new(e.code(), e.code().replace('_', " "))
    }
}

impl From<rt_sec::AuthError> for ApiError {
    fn from(e: rt_sec::AuthError) -> Self {
        let mut a = ApiError::new(e.code(), e.to_string());
        if let rt_sec::AuthError::RateLimited { retry_after } = &e {
            a.retry_after = Some(retry_after.as_secs().max(1));
        }
        a
    }
}

impl From<rt_sec::QuotaError> for ApiError {
    fn from(e: rt_sec::QuotaError) -> Self {
        let mut a = ApiError::new(e.code(), e.to_string());
        if let rt_sec::QuotaError::RateLimited { retry_after } | rt_sec::QuotaError::InvocationsPerDay { retry_after } =
            &e
        {
            a.retry_after = Some(retry_after.as_secs().max(1));
        }
        a
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut resp = (status, Json(self.envelope())).into_response();
        if let Some(secs) = self.retry_after {
            if let Ok(v) = secs.to_string().parse() {
                resp.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;
    use std::time::Duration;

    /// QUOTA-013: every code a store or quota error can carry into a control response has a
    /// documented 4xx/503 status; only `internal` is a 500.
    #[test]
    fn quota_013_store_and_quota_codes_never_become_500() {
        use rt_store::StoreError as S;
        let store = [
            S::NotFound,
            S::Conflict("slug_taken"),
            S::PreconditionFailed,
            S::Invalid("x".into()),
            S::StorageQuota,
            S::Sql("x".into()),
            S::SqlTimeout,
            S::SqlNotAuthorized,
            S::SqlBusy,
            S::SqlResultTooLarge,
            S::KvNotInteger,
            S::KvOverflow,
        ];
        for e in store {
            let code = e.code();
            assert_ne!(ApiError::from(e).status(), StatusCode::INTERNAL_SERVER_ERROR, "{code}");
        }
        use rt_sec::QuotaError as Q;
        for e in [
            Q::StorageQuota,
            Q::EgressQuota,
            Q::BuildQuota,
            Q::QueueFull,
            Q::TriggerNotAllowed,
        ] {
            let want = e.http_status();
            assert_eq!(ApiError::from(e).status().as_u16(), want);
        }
        assert_eq!(status_for_code("internal"), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn a_not_implemented_error_names_the_missing_feature_and_answers_501() {
        let e = ApiError::not_implemented("the sandbox");
        assert_eq!(
            (e.code, e.message.as_str()),
            ("not_implemented", "the sandbox is not implemented")
        );
        assert_eq!(e.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(e.envelope().error.code, "not_implemented");
    }

    /// The `retry-after` header of a rate-limited auth failure is whole seconds, never zero.
    #[test]
    fn a_rate_limited_auth_failure_carries_whole_seconds_to_retry_after() {
        use rt_sec::AuthError;
        let fast = ApiError::from(AuthError::RateLimited {
            retry_after: Duration::from_millis(200),
        });
        assert_eq!(fast.retry_after, Some(1), "sub-second waits round up to one second");
        let slow = ApiError::from(AuthError::RateLimited {
            retry_after: Duration::from_secs(30),
        });
        assert_eq!(slow.retry_after, Some(30));
        assert_eq!(slow.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
