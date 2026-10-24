//! `GET /v1/limits`: the tenant's plan limits and what is left of them (SECURITY.md).

use axum::extract::State;
use axum::Json;
use rt_types::TenantId;

use crate::auth::Auth;
use crate::dto::{LimitsView, QuotaView};
use crate::error::{ApiError, ErrorEnvelope};
use crate::state::ApiState;

const DAY_MS: i64 = 86_400_000;
const MIB: u64 = 1024 * 1024;

fn quota(limit: Option<u64>, used: u64, period: &str) -> QuotaView {
    QuotaView {
        limit,
        used,
        remaining: limit.map(|l| l.saturating_sub(used)),
        period: period.to_string(),
    }
}

/// Counters the enforcer does not track (a custom enforcer) come from the usage rows flushed to
/// `meta.db`.
async fn persisted_usage(s: &ApiState, tenant: TenantId, day: &str) -> Result<rt_sec::QuotaUsage, ApiError> {
    let today = s.meta.get_usage(tenant, day, day).await?;
    let all = s.meta.get_usage(tenant, "0000-01-01", "9999-12-31").await?;
    Ok(rt_sec::QuotaUsage {
        invocations: today.iter().map(|r| r.invocations).sum(),
        egress_bytes: today.iter().map(|r| r.egress_bytes).sum(),
        storage_bytes: u64::try_from(all.iter().map(|r| r.storage_bytes).sum::<i64>()).unwrap_or(0),
    })
}

/// The caller's tenant: its plan, and per quota the limit, what is used and what remains. Daily
/// counters (invocations, egress) reset at UTC midnight (`resets_at`).
#[utoipa::path(get, path = "/v1/limits", tag = "meta",
    responses((status = 200, body = LimitsView), (status = 401, body = ErrorEnvelope)))]
pub(crate) async fn limits_get(State(s): State<ApiState>, a: Auth) -> Result<Json<LimitsView>, ApiError> {
    let tenant = a.tenant()?;
    let plan = s.plans.plan_for(tenant);
    let plan_name = match s.tenant_table.get(&tenant).map(|x| x.plan.clone()) {
        Some(n) => n,
        None => s.meta.get_tenant(tenant).await?.map(|x| x.plan).unwrap_or_default(),
    };
    let now = s.clock.unix_ms();
    let day = chrono::DateTime::from_timestamp_millis(now)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default();
    let used = match s.quotas.usage_today(tenant) {
        Some(u) => u,
        None => persisted_usage(&s, tenant, &day).await?,
    };
    let services = s.meta.count_services(tenant).await?;
    let schedules = s.meta.busiest_service_schedules(tenant).await?;
    let nonzero = |v: u64| (v > 0).then_some(v);
    Ok(Json(LimitsView {
        plan: plan_name,
        day,
        resets_at: (now.div_euclid(DAY_MS) + 1) * DAY_MS,
        // 0 invocations per day means unlimited; for egress and storage 0 means none allowed.
        invocations: quota(nonzero(plan.invocations_per_day), used.invocations, "day"),
        egress_bytes: quota(
            Some(plan.egress_mb_per_day.saturating_mul(MIB)),
            used.egress_bytes,
            "day",
        ),
        storage_bytes: quota(Some(plan.storage_mb.saturating_mul(MIB)), used.storage_bytes, "total"),
        services: quota(Some(u64::from(plan.max_services)), services, "total"),
        schedules: quota(Some(u64::from(plan.max_schedules)), schedules, "per_service"),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::{Method, StatusCode};
    use rt_sec::{QuotaEnforcer, QuotaError, QuotaOp};
    use rt_types::{NewTenant, ServiceId, UsageBatch, UsageDelta};

    use crate::test_support;

    /// Enforces like the real enforcer but keeps no counters of its own: usage has to come from
    /// the rows flushed to `meta.db`.
    struct NoCounters(Arc<dyn QuotaEnforcer>);

    impl QuotaEnforcer for NoCounters {
        fn check(&self, tenant: rt_types::TenantId, op: QuotaOp) -> Result<(), QuotaError> {
            self.0.check(tenant, op)
        }
        fn record(&self, tenant: rt_types::TenantId, service: ServiceId, delta: UsageDelta) {
            self.0.record(tenant, service, delta)
        }
    }

    /// A tenant the node has not cached (not in `tenant_table`) gets its plan from `meta.db`, and
    /// the daily counters come from today's usage rows while storage counts every row.
    #[tokio::test]
    async fn limits_read_the_plan_and_usage_from_meta_db_when_the_enforcer_keeps_no_counters() {
        let mut node = test_support::node().await;
        let tenant = node
            .state
            .meta
            .create_tenant(NewTenant {
                name: "stored".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant")
            .id;
        let svc = ServiceId::from_u128(77);
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let row = |invocations, egress_bytes, storage_bytes| {
            (
                tenant,
                svc,
                UsageDelta {
                    invocations,
                    cpu_ms: 0,
                    egress_bytes,
                    storage_bytes,
                },
            )
        };
        node.state
            .meta
            .add_usage(
                &today,
                UsageBatch {
                    items: vec![row(5, 1_000, 300)],
                },
            )
            .await
            .expect("today");
        node.state
            .meta
            .add_usage(
                "2001-01-01",
                UsageBatch {
                    items: vec![row(7, 50, 200)],
                },
            )
            .await
            .expect("old day");
        node.state.quotas = Arc::new(NoCounters(node.state.quotas.clone()));

        let (status, _, body) = node.call(Method::GET, "/v1/limits", Some(tenant), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["plan"], "free", "the plan comes from meta.db");
        assert_eq!(body["day"], today);
        assert_eq!(body["invocations"]["used"], 5, "today's invocations only");
        assert_eq!(body["egress_bytes"]["used"], 1_000);
        assert_eq!(body["storage_bytes"]["used"], 500, "storage counts every row");
    }
}
