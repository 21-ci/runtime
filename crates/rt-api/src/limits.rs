//! `GET /v1/limits`: the tenant's plan limits and what is left of them (SECURITY.md).

use axum::extract::State;
use axum::Json;
use rt_types::{Plan, Service, TenantId};
use serde_json::json;

use crate::auth::Auth;
use crate::dto::{LimitsView, QuotaView, TenantUsageView, UsageQuotas};
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

/// Storage is measured on disk, not counted: no task reports it to the enforcer. A node that does
/// not scan answers 0, and then the counters stand.
fn measured_storage(mut used: rt_sec::QuotaUsage, on_disk: u64) -> rt_sec::QuotaUsage {
    used.storage_bytes = used.storage_bytes.max(on_disk);
    used
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
        cpu_ms: today.iter().map(|r| r.cpu_ms).sum(),
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
    let used = measured_storage(used, s.node.tenant_storage_bytes(tenant));
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

/// `403 quota_exceeded` when `used` already reaches `max` (0 = no cap of the plan).
pub(crate) fn check_count(key: &'static str, used: u64, max: u32) -> Result<(), ApiError> {
    if max > 0 && used >= u64::from(max) {
        return Err(
            ApiError::new("quota_exceeded", format!("the plan allows {max} (`{key}`)"))
                .with_details(json!({"limit": key, "used": used, "max": max})),
        );
    }
    Ok(())
}

/// A project's environment list against the plan. A list that does not grow always passes, so a
/// tenant above a lowered cap can still rename or remove.
pub(crate) fn check_environment_count(plan: &Plan, wanted: usize, current: usize) -> Result<(), ApiError> {
    if wanted > current {
        check_count(
            "max_environments_per_project",
            wanted.saturating_sub(1) as u64,
            plan.max_environments_per_project,
        )?;
    }
    Ok(())
}

/// A secret `name` about to be written: overwriting never counts, a new name must fit the plan.
pub(crate) async fn check_new_secret(s: &ApiState, svc: &Service, name: &str) -> Result<(), ApiError> {
    let max = s.plans.plan_arc(svc.tenant_id).max_secrets_per_service;
    if max == 0 {
        return Ok(());
    }
    let have = s.meta.list_secrets(svc.id).await?;
    if have.iter().any(|m| m.name == name) {
        return Ok(());
    }
    check_count("max_secrets_per_service", have.len() as u64, max)
}

/// A custom host a tenant newly claims. The operator attaches without this check.
pub(crate) async fn check_new_host(s: &ApiState, tenant: TenantId) -> Result<(), ApiError> {
    let max = s.plans.plan_arc(tenant).max_custom_hosts;
    let used = s.meta.tenant_counts(tenant).await?.custom_hosts;
    check_count("max_custom_hosts", used, max)
}

/// Versions kept per service: the lower of the node's and the plan's value, at least one.
pub(crate) fn versions_to_keep(node: u32, plan: u32) -> u32 {
    let keep = if plan > 0 { node.min(plan) } else { node };
    keep.max(1)
}

async fn plan_name(s: &ApiState, tenant: TenantId) -> Result<String, ApiError> {
    Ok(match s.tenant_table.get(&tenant).map(|x| x.plan.clone()) {
        Some(n) => n,
        None => s.meta.get_tenant(tenant).await?.map(|x| x.plan).unwrap_or_default(),
    })
}

/// The caller's tenant: its whole effective plan and the use of every counted quota, for the
/// console. A `limit` of `null` means no cap.
#[utoipa::path(get, path = "/v1/usage", tag = "meta",
    responses((status = 200, body = TenantUsageView), (status = 401, body = ErrorEnvelope)))]
pub(crate) async fn usage_get(State(s): State<ApiState>, a: Auth) -> Result<Json<TenantUsageView>, ApiError> {
    let tenant = a.tenant()?;
    let plan = s.plans.plan_arc(tenant);
    let now = s.clock.unix_ms();
    let day = chrono::DateTime::from_timestamp_millis(now)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default();
    let used = match s.quotas.usage_today(tenant) {
        Some(u) => u,
        None => persisted_usage(&s, tenant, &day).await?,
    };
    let used = measured_storage(used, s.node.tenant_storage_bytes(tenant));
    let counts = s.meta.tenant_counts(tenant).await?;
    let services = s.meta.count_services(tenant).await?;
    let schedules = s.meta.busiest_service_schedules(tenant).await?;
    let cap = |v: u32| (v > 0).then_some(u64::from(v));
    let nonzero = |v: u64| (v > 0).then_some(v);
    Ok(Json(TenantUsageView {
        plan: plan_name(&s, tenant).await?,
        day,
        resets_at: (now.div_euclid(DAY_MS) + 1) * DAY_MS,
        quotas: UsageQuotas {
            invocations: quota(nonzero(plan.invocations_per_day), used.invocations, "day"),
            cpu_ms: quota(nonzero(plan.cpu_ms_per_day), used.cpu_ms, "day"),
            egress_bytes: quota(
                Some(plan.egress_mb_per_day.saturating_mul(MIB)),
                used.egress_bytes,
                "day",
            ),
            storage_bytes: quota(Some(plan.storage_mb.saturating_mul(MIB)), used.storage_bytes, "total"),
            services: quota(Some(u64::from(plan.max_services)), services, "total"),
            projects: quota(cap(plan.max_projects), counts.projects, "total"),
            branches: quota(cap(plan.max_branches), counts.branches, "total"),
            previews: quota(cap(plan.max_scratch), counts.previews, "total"),
            custom_hosts: quota(cap(plan.max_custom_hosts), counts.custom_hosts, "total"),
            schedules: quota(Some(u64::from(plan.max_schedules)), schedules, "per_service"),
            secrets: quota(
                cap(plan.max_secrets_per_service),
                counts.busiest_service_secrets,
                "per_service",
            ),
        },
        limits: serde_json::to_value(&*plan).unwrap_or_default(),
    }))
}

#[cfg(test)]
#[test]
fn storage_on_disk_outranks_a_counter_that_never_moves() {
    let zero = rt_sec::QuotaUsage::default();
    assert_eq!(measured_storage(zero, 1500).storage_bytes, 1500);
    let counted = rt_sec::QuotaUsage {
        storage_bytes: 2000,
        ..zero
    };
    assert_eq!(measured_storage(counted, 0).storage_bytes, 2000);
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

    async fn tenant(node: &test_support::TestNode) -> rt_types::TenantId {
        node.state
            .meta
            .create_tenant(NewTenant {
                name: "plan".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant")
            .id
    }

    #[test]
    fn caps_treat_zero_as_none_and_refuse_at_the_limit() {
        assert!(super::check_count("max_projects", 1_000, 0).is_ok());
        assert!(super::check_count("max_projects", 1, 2).is_ok());
        let e = super::check_count("max_projects", 2, 2).expect_err("full");
        assert_eq!(e.code, "quota_exceeded");
        assert_eq!(e.status(), StatusCode::FORBIDDEN);
        assert_eq!(super::versions_to_keep(10, 0), 10);
        assert_eq!(super::versions_to_keep(10, 3), 3);
        assert_eq!(super::versions_to_keep(2, 3), 2, "a plan cannot raise the node's value");
        assert_eq!(super::versions_to_keep(0, 0), 1);
    }

    #[test]
    fn a_shrunk_plan_still_allows_environment_lists_that_do_not_grow() {
        let plan = rt_types::Plan {
            max_environments_per_project: 2,
            ..rt_types::Plan::default()
        };
        assert!(super::check_environment_count(&plan, 2, 0).is_ok());
        assert!(super::check_environment_count(&plan, 3, 0).is_err());
        assert!(super::check_environment_count(&plan, 3, 2).is_err());
        assert!(super::check_environment_count(&plan, 5, 5).is_ok());
    }

    #[tokio::test]
    async fn usage_reports_the_plan_and_counts_and_projects_stop_at_the_cap() {
        let node = test_support::node().await;
        let t = tenant(&node).await;
        for i in 0..rt_types::Plan::default().max_projects {
            let body = serde_json::json!({"slug": format!("p{i}x")});
            let (status, _, b) = node.call(Method::POST, "/v1/projects", Some(t), Some(body)).await;
            if status != StatusCode::CREATED {
                assert_eq!(status, StatusCode::FORBIDDEN, "{b}");
                assert_eq!(b["error"]["code"], "quota_exceeded");
                assert_eq!(b["error"]["details"]["limit"], "max_projects");
                break;
            }
        }
        let body = serde_json::json!({"slug": "onemore"});
        let (status, _, b) = node.call(Method::POST, "/v1/projects", Some(t), Some(body)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{b}");

        let (status, _, u) = node.call(Method::GET, "/v1/usage", Some(t), None).await;
        assert_eq!(status, StatusCode::OK, "{u}");
        assert_eq!(u["plan"], "free");
        assert_eq!(u["quotas"]["projects"]["limit"], 20);
        assert_eq!(u["quotas"]["projects"]["used"], 20);
        assert_eq!(u["quotas"]["projects"]["remaining"], 0);
        assert!(u["quotas"]["cpu_ms"]["limit"].is_null(), "off by default: {u}");
        assert_eq!(u["quotas"]["cpu_ms"]["period"], "day");
        assert_eq!(u["limits"]["max_secrets_per_service"], 100);
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
