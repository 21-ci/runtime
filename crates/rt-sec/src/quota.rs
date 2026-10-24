//! Plan quotas (SECURITY.md).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use rt_types::{Plan, ServiceId, TenantId, TriggerKind, UnixMs, UsageDelta};
use thiserror::Error;

use crate::auth::{SystemClock, WallClock};

/// An operation that consumes quota.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuotaOp {
    /// Control API: create a service (`current` services already owned).
    CreateService { current: u64 },
    /// Control API: create a schedule.
    CreateSchedule { current: u64, every_seconds: u64 },
    /// Control API: use a trigger kind.
    UseTrigger(TriggerKind),
    /// Ingress: one invocation (daily counter + RPS bucket).
    Invoke,
    /// Version upload (`builds_per_hour`).
    Build,
    /// Queue enqueue with current depth.
    Enqueue { depth: u64 },
    /// Egress bytes about to be received.
    Egress { bytes: u64 },
    /// Storage growth in bytes.
    Storage { delta_bytes: u64 },
}

/// Which quota was hit; maps to HTTP per docs/api.md.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum QuotaError {
    /// Services, schedules: `403 quota_exceeded`.
    #[error("quota exceeded: {what}")]
    Exceeded { what: &'static str },
    /// `403 trigger_not_allowed`.
    #[error("trigger not allowed by plan")]
    TriggerNotAllowed,
    /// `422 schedule_too_frequent`.
    #[error("schedule more frequent than plan minimum of {min_seconds}s")]
    ScheduleTooFrequent { min_seconds: u32 },
    /// `429 rate_limited` + `retry-after`.
    #[error("rate limited")]
    RateLimited { retry_after: Duration },
    /// `429 quota_exceeded` + `retry-after` (until UTC midnight).
    #[error("daily invocation quota exceeded")]
    InvocationsPerDay { retry_after: Duration },
    /// `429 build_quota`.
    #[error("build quota exceeded")]
    BuildQuota,
    /// `429 queue_full`.
    #[error("queue full")]
    QueueFull,
    /// Guest sees `storage_quota`.
    #[error("storage quota exceeded")]
    StorageQuota,
    /// Guest sees `egress_quota`.
    #[error("egress quota exceeded")]
    EgressQuota,
}

impl QuotaError {
    pub fn http_status(&self) -> u16 {
        match self {
            QuotaError::Exceeded { .. } | QuotaError::TriggerNotAllowed => 403,
            QuotaError::ScheduleTooFrequent { .. } => 422,
            QuotaError::RateLimited { .. }
            | QuotaError::InvocationsPerDay { .. }
            | QuotaError::BuildQuota
            | QuotaError::QueueFull => 429,
            QuotaError::StorageQuota | QuotaError::EgressQuota => 413,
        }
    }

    /// Control/invoke error code.
    pub fn code(&self) -> &'static str {
        match self {
            QuotaError::Exceeded { .. } | QuotaError::InvocationsPerDay { .. } => "quota_exceeded",
            QuotaError::TriggerNotAllowed => "trigger_not_allowed",
            QuotaError::ScheduleTooFrequent { .. } => "schedule_too_frequent",
            QuotaError::RateLimited { .. } => "rate_limited",
            QuotaError::BuildQuota => "build_quota",
            QuotaError::QueueFull => "queue_full",
            QuotaError::StorageQuota => "storage_quota",
            QuotaError::EgressQuota => "egress_quota",
        }
    }
}

/// What a tenant has used against its plan today, as the enforcer counts it (the numbers its
/// checks compare with the plan: `GET /v1/limits`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuotaUsage {
    /// Invocations admitted since UTC midnight.
    pub invocations: u64,
    /// Bytes fetched by guests since UTC midnight.
    pub egress_bytes: u64,
    /// Bytes stored (KV, SQL, blobs) as the enforcer tracks them.
    pub storage_bytes: u64,
}

pub trait QuotaEnforcer: Send + Sync {
    fn check(&self, tenant: TenantId, op: QuotaOp) -> Result<(), QuotaError>;
    fn record(&self, tenant: TenantId, service: ServiceId, delta: UsageDelta);
    /// Bytes of `tenant`'s daily egress quota still available, for the host to cut a download
    /// at (`None` = not tracked by this enforcer).
    fn egress_left(&self, tenant: TenantId) -> Option<u64> {
        let _ = tenant;
        None
    }
    /// What `tenant` has used today (`None` = not tracked by this enforcer).
    fn usage_today(&self, tenant: TenantId) -> Option<QuotaUsage> {
        let _ = tenant;
        None
    }
}

/// Resolves a tenant's effective plan (config plan + per-tenant overrides). Re-read on SIGHUP.
pub trait PlanSource: Send + Sync {
    fn plan_for(&self, tenant: TenantId) -> Plan;

    /// The same plan behind a shared pointer. A source that caches resolved plans overrides it,
    /// so the per-request readers (quota check, task spec, host policy) share one resolution
    /// instead of cloning a `Plan` each.
    fn plan_arc(&self, tenant: TenantId) -> Arc<Plan> {
        Arc::new(self.plan_for(tenant))
    }

    /// The name of the tenant's plan (operator SDK grants attach to plans).
    fn plan_name(&self, tenant: TenantId) -> String {
        let _ = tenant;
        "free".to_string()
    }
}

const MIB: u64 = 1024 * 1024;
const DAY_MS: i64 = 86_400_000;
const HOUR: Duration = Duration::from_secs(3600);

/// Per-tenant counters.
struct TenantState {
    /// UTC day (days since the epoch) the daily counters belong to.
    day: i64,
    invocations: u64,
    egress_bytes: u64,
    /// Persistent: storage in use, adjusted by `record`.
    storage_bytes: u64,
    /// Token bucket (tenant-wide `plan.rps`).
    tokens: f64,
    refilled: Instant,
    /// Start times of builds within the last hour.
    builds: VecDeque<Instant>,
}

impl TenantState {
    fn new() -> Self {
        Self {
            day: i64::MIN,
            invocations: 0,
            egress_bytes: 0,
            storage_bytes: 0,
            tokens: f64::INFINITY,
            refilled: Instant::now(),
            builds: VecDeque::new(),
        }
    }

    /// Reset the daily counters when a later UTC day began. A clock stepping back (NTP) over
    /// midnight keeps today's counts (QUOTA-011).
    fn roll(&mut self, now_ms: UnixMs) {
        let day = now_ms.div_euclid(DAY_MS);
        if day > self.day {
            self.day = day;
            self.invocations = 0;
            self.egress_bytes = 0;
        }
    }
}

/// In-memory counters (daily invocations flushed to `usage` every 5 s by the server).
///
/// Plans are re-read from the [`PlanSource`] on every check, so SIGHUP plan reloads apply
/// without rebuilding the enforcer. `rps` and `invocations_per_day` of `0` mean unlimited.
pub struct LocalQuotaEnforcer {
    plans: Arc<dyn PlanSource>,
    clock: Arc<dyn WallClock>,
    tenants: DashMap<TenantId, Mutex<TenantState>>,
}

impl LocalQuotaEnforcer {
    pub fn new(plans: Arc<dyn PlanSource>) -> Self {
        Self::with_clock(plans, Arc::new(SystemClock))
    }

    /// Like [`LocalQuotaEnforcer::new`] with an injected wall clock (UTC-day boundaries).
    pub fn with_clock(plans: Arc<dyn PlanSource>, clock: Arc<dyn WallClock>) -> Self {
        Self {
            plans,
            clock,
            tenants: DashMap::new(),
        }
    }

    fn with_state<R>(&self, tenant: TenantId, f: impl FnOnce(&mut TenantState) -> R) -> R {
        let entry = self
            .tenants
            .entry(tenant)
            .or_insert_with(|| Mutex::new(TenantState::new()));
        let mut st = entry.lock();
        st.roll(self.clock.now_ms());
        f(&mut st)
    }

    /// Restore the daily counters of `tenant` from persisted usage (`meta.db` `usage` rows of
    /// the UTC day containing `day_ms`), so a restart does not reset `invocations_per_day` and
    /// `egress_mb_per_day` (SEC-54). Adds to what is already counted; call it once at startup,
    /// before serving. Usage of any other day than the enforcer's current one is ignored.
    pub fn seed_daily(&self, tenant: TenantId, day_ms: UnixMs, invocations: u64, egress_bytes: u64) {
        self.with_state(tenant, |st| {
            if st.day == day_ms.div_euclid(DAY_MS) {
                st.invocations = st.invocations.saturating_add(invocations);
                st.egress_bytes = st.egress_bytes.saturating_add(egress_bytes);
            }
        });
    }

    fn until_midnight(&self) -> Duration {
        let now = self.clock.now_ms();
        let left = (now.div_euclid(DAY_MS) + 1) * DAY_MS - now;
        Duration::from_millis(left.clamp(1, DAY_MS) as u64)
    }
}

impl QuotaEnforcer for LocalQuotaEnforcer {
    fn check(&self, tenant: TenantId, op: QuotaOp) -> Result<(), QuotaError> {
        let plan = self.plans.plan_arc(tenant);
        match op {
            QuotaOp::CreateService { current } => {
                if current >= u64::from(plan.max_services) {
                    return Err(QuotaError::Exceeded { what: "services" });
                }
                Ok(())
            }
            QuotaOp::CreateSchedule { current, every_seconds } => {
                if current >= u64::from(plan.max_schedules) {
                    return Err(QuotaError::Exceeded { what: "schedules" });
                }
                if every_seconds < u64::from(plan.min_schedule_seconds) {
                    return Err(QuotaError::ScheduleTooFrequent {
                        min_seconds: plan.min_schedule_seconds,
                    });
                }
                Ok(())
            }
            QuotaOp::UseTrigger(kind) => {
                if plan.allows_trigger(kind) {
                    Ok(())
                } else {
                    Err(QuotaError::TriggerNotAllowed)
                }
            }
            QuotaOp::Invoke => {
                let retry = self.until_midnight();
                self.with_state(tenant, |st| {
                    // The day's budget first: a refused request takes no rate token (QUOTA-009).
                    if plan.invocations_per_day > 0 && st.invocations >= plan.invocations_per_day {
                        return Err(QuotaError::InvocationsPerDay { retry_after: retry });
                    }
                    if plan.rps > 0 {
                        let rate = f64::from(plan.rps);
                        let now = Instant::now();
                        let elapsed = now.saturating_duration_since(st.refilled).as_secs_f64();
                        st.refilled = now;
                        st.tokens = (st.tokens + elapsed * rate).min(rate);
                        if st.tokens < 1.0 {
                            let wait = ((1.0 - st.tokens) / rate).max(0.001);
                            return Err(QuotaError::RateLimited {
                                retry_after: Duration::from_secs_f64(wait),
                            });
                        }
                        st.tokens -= 1.0;
                    }
                    st.invocations += 1;
                    Ok(())
                })
            }
            QuotaOp::Build => self.with_state(tenant, |st| {
                let now = Instant::now();
                while st
                    .builds
                    .front()
                    .is_some_and(|t| now.saturating_duration_since(*t) >= HOUR)
                {
                    st.builds.pop_front();
                }
                if st.builds.len() as u64 >= u64::from(plan.builds_per_hour) {
                    return Err(QuotaError::BuildQuota);
                }
                st.builds.push_back(now);
                Ok(())
            }),
            QuotaOp::Enqueue { depth } => {
                if depth >= plan.max_queue_depth {
                    Err(QuotaError::QueueFull)
                } else {
                    Ok(())
                }
            }
            QuotaOp::Egress { bytes } => {
                let limit = plan.egress_mb_per_day.saturating_mul(MIB);
                self.with_state(tenant, |st| {
                    if st.egress_bytes.saturating_add(bytes) > limit {
                        Err(QuotaError::EgressQuota)
                    } else {
                        Ok(())
                    }
                })
            }
            QuotaOp::Storage { delta_bytes } => {
                let limit = plan.storage_mb.saturating_mul(MIB);
                self.with_state(tenant, |st| {
                    if st.storage_bytes.saturating_add(delta_bytes) > limit {
                        Err(QuotaError::StorageQuota)
                    } else {
                        Ok(())
                    }
                })
            }
        }
    }

    fn egress_left(&self, tenant: TenantId) -> Option<u64> {
        let limit = self.plans.plan_arc(tenant).egress_mb_per_day.saturating_mul(MIB);
        Some(self.with_state(tenant, |st| limit.saturating_sub(st.egress_bytes)))
    }

    fn usage_today(&self, tenant: TenantId) -> Option<QuotaUsage> {
        Some(self.with_state(tenant, |st| QuotaUsage {
            invocations: st.invocations,
            egress_bytes: st.egress_bytes,
            storage_bytes: st.storage_bytes,
        }))
    }

    /// Accumulates egress and storage usage. Invocations are counted by `check(Invoke)` itself, so
    /// `delta.invocations` is not added again here.
    fn record(&self, tenant: TenantId, _service: ServiceId, delta: UsageDelta) {
        self.with_state(tenant, |st| {
            st.egress_bytes = st.egress_bytes.saturating_add(delta.egress_bytes);
            if delta.storage_bytes >= 0 {
                st.storage_bytes = st.storage_bytes.saturating_add(delta.storage_bytes.unsigned_abs());
            } else {
                st.storage_bytes = st.storage_bytes.saturating_sub(delta.storage_bytes.unsigned_abs());
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;

    struct FixedPlan(Plan);

    impl PlanSource for FixedPlan {
        fn plan_for(&self, _tenant: TenantId) -> Plan {
            self.0.clone()
        }
    }

    struct Clock(AtomicI64);

    impl WallClock for Clock {
        fn now_ms(&self) -> UnixMs {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// 2026-10-06T12:00:00Z
    const NOON: UnixMs = 1_791_288_000_000;

    fn enforcer(plan: Plan) -> (LocalQuotaEnforcer, Arc<Clock>) {
        let clock = Arc::new(Clock(AtomicI64::new(NOON)));
        (
            LocalQuotaEnforcer::with_clock(Arc::new(FixedPlan(plan)), clock.clone()),
            clock,
        )
    }

    #[test]
    fn ac_sec_54_seeded_invocations_count_against_the_daily_quota_after_a_restart() {
        let plan = Plan {
            rps: 0,
            invocations_per_day: 100,
            ..Plan::default()
        };
        let (q, _) = enforcer(plan);
        let t = TenantId::from_u128(1);
        // 98 invocations persisted before the restart, earlier the same UTC day.
        q.seed_daily(t, NOON - 3_600_000, 98, 0);
        assert!(q.check(t, QuotaOp::Invoke).is_ok(), "99th");
        assert!(q.check(t, QuotaOp::Invoke).is_ok(), "100th");
        assert!(
            matches!(q.check(t, QuotaOp::Invoke), Err(QuotaError::InvocationsPerDay { .. })),
            "101st after the restart"
        );
        assert!(
            q.check(TenantId::from_u128(2), QuotaOp::Invoke).is_ok(),
            "other tenants are unaffected"
        );
    }

    #[test]
    fn ac_sec_54_seeded_egress_reduces_what_is_left_today() {
        let plan = Plan {
            egress_mb_per_day: 2,
            ..Plan::default()
        };
        let (q, _) = enforcer(plan);
        let t = TenantId::from_u128(1);
        q.seed_daily(t, NOON, 0, MIB + 10);
        assert_eq!(q.egress_left(t), Some(MIB - 10));
        assert!(matches!(
            q.check(t, QuotaOp::Egress { bytes: MIB }),
            Err(QuotaError::EgressQuota)
        ));
    }

    #[test]
    fn usage_today_reports_what_the_checks_count_and_starts_over_at_midnight() {
        let plan = Plan {
            rps: 0,
            invocations_per_day: 100,
            ..Plan::default()
        };
        let (q, clock) = enforcer(plan);
        let t = TenantId::from_u128(1);
        assert_eq!(
            q.usage_today(t),
            Some(QuotaUsage::default()),
            "a tenant that did nothing"
        );
        q.seed_daily(t, NOON, 5, 7);
        q.check(t, QuotaOp::Invoke).expect("invoke");
        q.check(t, QuotaOp::Invoke).expect("invoke");
        q.record(
            t,
            ServiceId::from_u128(9),
            UsageDelta {
                egress_bytes: 3,
                storage_bytes: 40,
                ..UsageDelta::default()
            },
        );
        assert_eq!(
            q.usage_today(t),
            Some(QuotaUsage {
                invocations: 7,
                egress_bytes: 10,
                storage_bytes: 40
            })
        );
        assert_eq!(
            q.usage_today(TenantId::from_u128(2)),
            Some(QuotaUsage::default()),
            "per tenant"
        );
        clock.0.store(NOON + DAY_MS / 2 + 1, Ordering::SeqCst);
        assert_eq!(
            q.usage_today(t),
            Some(QuotaUsage {
                invocations: 0,
                egress_bytes: 0,
                storage_bytes: 40
            }),
            "the daily counters reset at UTC midnight; stored bytes persist"
        );
    }

    #[test]
    fn ac_sec_54_usage_of_another_day_is_not_seeded_and_the_seed_expires_at_midnight() {
        let plan = Plan {
            rps: 0,
            invocations_per_day: 10,
            egress_mb_per_day: 1,
            ..Plan::default()
        };
        let (q, clock) = enforcer(plan);
        let t = TenantId::from_u128(1);
        q.seed_daily(t, NOON - DAY_MS, 10, MIB);
        assert!(q.check(t, QuotaOp::Invoke).is_ok(), "yesterday's usage is not today's");
        assert_eq!(q.egress_left(t), Some(MIB));

        q.seed_daily(t, NOON, 9, MIB);
        assert!(matches!(
            q.check(t, QuotaOp::Invoke),
            Err(QuotaError::InvocationsPerDay { .. })
        ));
        assert_eq!(q.egress_left(t), Some(0));
        clock.0.store(NOON + DAY_MS / 2 + 1, Ordering::SeqCst);
        assert!(q.check(t, QuotaOp::Invoke).is_ok(), "a new UTC day starts from zero");
        assert_eq!(q.egress_left(t), Some(MIB));
    }
}
