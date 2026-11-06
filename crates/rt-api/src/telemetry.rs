//! The anonymous daily report (SECURITY.md "Telemetry"): the one function that builds it, for the
//! sender and for the previews, and the two operator routes. The sender is in `rt-server`.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use rt_obs::StatsStore;
use rt_store::{MetaStore, TelemetryState};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

use crate::auth::Auth;
use crate::control::audit;
use crate::error::{ApiError, ErrorEnvelope};
use crate::extract::ApiJson;
use crate::state::ApiState;

const DAY_MS: i64 = 86_400_000;
/// A console that has not reported for this long no longer counts.
const CONSOLE_FRESH_MS: i64 = 2 * DAY_MS;
const MAX_REQUESTS: u64 = 1_000_000_000_000;
const MAX_COUNT: u64 = 1_000_000_000;
/// Shown by a preview while no id exists. A preview never creates one.
pub const PLACEHOLDER_INSTALL_ID: &str = "00000000000000000000000000000000";

/// Exactly what is sent: these seven keys and nothing else.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct TelemetryReport {
    pub install_id: String,
    pub version: String,
    /// Requests finished in the last 24 hours.
    pub requests: u64,
    pub projects: u64,
    pub services: u64,
    /// Tenants, without the system tenant.
    pub organizations: u64,
    /// Console users; `null` when no console reported in the last 48 hours.
    pub users: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TelemetryPreview {
    /// Whether this node sends reports.
    pub sending: bool,
    pub url: Option<String>,
    /// The next report.
    pub report: TelemetryReport,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConsoleBody {
    /// Console users now, at most 10^9.
    pub users: u64,
}

/// The report for `install_id`. Sending and every preview go through here, so they cannot differ.
pub async fn build_report(
    meta: &dyn MetaStore,
    stats: &dyn StatsStore,
    now_ms: i64,
    install_id: String,
) -> rt_store::Result<TelemetryReport> {
    let counts = meta.telemetry_counts().await?;
    let state = meta.telemetry_state().await?;
    let requests: u64 = stats
        .counts_all(now_ms - DAY_MS, now_ms)
        .iter()
        .map(|(_, c)| c.count)
        .sum();
    Ok(TelemetryReport {
        install_id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        requests: requests.min(MAX_REQUESTS),
        projects: counts.projects.min(MAX_COUNT),
        services: counts.services.min(MAX_COUNT),
        organizations: counts.organizations.min(MAX_COUNT),
        users: fresh_console_users(&state, now_ms),
    })
}

fn fresh_console_users(state: &TelemetryState, now_ms: i64) -> Option<u64> {
    let at = state.console_users_at?;
    (now_ms - at <= CONSOLE_FRESH_MS)
        .then_some(state.console_users?)
        .map(|n| n.min(MAX_COUNT))
}

/// The preview of `url`'s next report; writes nothing.
pub async fn preview(
    meta: &dyn MetaStore,
    stats: &dyn StatsStore,
    now_ms: i64,
    url: Option<String>,
) -> rt_store::Result<TelemetryPreview> {
    let id = meta
        .telemetry_state()
        .await?
        .install_id
        .unwrap_or_else(|| PLACEHOLDER_INSTALL_ID.to_string());
    Ok(TelemetryPreview {
        sending: url.is_some(),
        url,
        report: build_report(meta, stats, now_ms, id).await?,
    })
}

#[utoipa::path(get, path = "/v1/telemetry/preview", tag = "node", responses(
    (status = 200, body = TelemetryPreview), (status = 403, body = ErrorEnvelope)))]
pub(crate) async fn telemetry_preview(State(s): State<ApiState>, a: Auth) -> Result<Json<TelemetryPreview>, ApiError> {
    a.require_operator()?;
    let url = rt_config::telemetry_url(&s.config.load().telemetry);
    let now = chrono::Utc::now().timestamp_millis();
    Ok(Json(preview(&*s.meta, &*s.stats, now, url).await?))
}

#[utoipa::path(put, path = "/v1/telemetry/console", tag = "node", request_body = TelemetryConsoleBody, responses(
    (status = 204), (status = 403, body = ErrorEnvelope), (status = 422, body = ErrorEnvelope)))]
pub(crate) async fn telemetry_console(
    State(s): State<ApiState>,
    a: Auth,
    ApiJson(b): ApiJson<TelemetryConsoleBody>,
) -> Result<StatusCode, ApiError> {
    a.require_operator()?;
    if b.users > MAX_COUNT {
        return Err(ApiError::invalid("`users` is at most 1000000000"));
    }
    let now = chrono::Utc::now().timestamp_millis();
    s.meta.telemetry_set_console_users(b.users, now).await?;
    audit(&s, &a, "telemetry.console", None, None, json!({"users": b.users}));
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(users: u64, at: i64) -> TelemetryState {
        TelemetryState {
            console_users: Some(users),
            console_users_at: Some(at),
            ..TelemetryState::default()
        }
    }

    #[test]
    fn a_console_count_goes_stale_after_48_hours() {
        let at = 1_000;
        assert_eq!(fresh_console_users(&state(5, at), at + CONSOLE_FRESH_MS), Some(5));
        assert_eq!(fresh_console_users(&state(5, at), at + CONSOLE_FRESH_MS + 1), None);
        assert_eq!(fresh_console_users(&TelemetryState::default(), at), None);
    }

    #[test]
    fn the_report_has_exactly_the_seven_keys() {
        let report = TelemetryReport {
            install_id: PLACEHOLDER_INSTALL_ID.into(),
            version: "0.1.1".into(),
            requests: 10,
            projects: 2,
            services: 3,
            organizations: 1,
            users: None,
        };
        let v = serde_json::to_value(&report).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "install_id",
                "organizations",
                "projects",
                "requests",
                "services",
                "users",
                "version"
            ]
        );
        assert!(v["users"].is_null());
    }
}
