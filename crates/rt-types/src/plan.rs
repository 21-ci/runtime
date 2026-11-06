//! Operator plans (SECURITY.md). Plans live in config (`[plans.<name>]`); tenants
//! reference a plan name plus optional overrides.

use serde::{Deserialize, Serialize};

/// A trigger kind that a plan can allow or forbid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TriggerKind {
    Http,
    Interval,
    Cron,
    Queue,
    Call,
    /// WebSocket upgrades and their events (`std:sockets`).
    Websocket,
    /// Pub/sub deliveries (`events.subscribe`, `std:pubsub`).
    Pubsub,
    /// Stream consumer batches (`events.consume`, `std:stream`).
    Stream,
}

impl TriggerKind {
    pub const ALL: [TriggerKind; 8] = [
        TriggerKind::Http,
        TriggerKind::Interval,
        TriggerKind::Cron,
        TriggerKind::Queue,
        TriggerKind::Call,
        TriggerKind::Websocket,
        TriggerKind::Pubsub,
        TriggerKind::Stream,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TriggerKind::Http => "http",
            TriggerKind::Interval => "interval",
            TriggerKind::Cron => "cron",
            TriggerKind::Queue => "queue",
            TriggerKind::Call => "call",
            TriggerKind::Websocket => "websocket",
            TriggerKind::Pubsub => "pubsub",
            TriggerKind::Stream => "stream",
        }
    }
}

/// Maximum per-service limits a plan allows (`limits_max`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PlanLimitsMax {
    pub wall_ms: u64,
    pub cpu_ms: u64,
    pub memory_mb: u32,
    /// Largest `scaling.min_run_time_ms` a service may set (keep-alive after the answer).
    pub min_run_time_ms: u64,
}

impl Default for PlanLimitsMax {
    fn default() -> Self {
        Self {
            wall_ms: 30_000,
            cpu_ms: 1_000,
            memory_mb: 128,
            min_run_time_ms: 60_000,
        }
    }
}

/// Default egress stance for services of a plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EgressDefault {
    /// Public internet (SSRF guard always on).
    #[default]
    Allow,
    /// Operator-only allowlists.
    Deny,
}

/// One plan. `Default` is the `free` plan from docs/api.md.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Plan {
    pub max_services: u32,
    /// Tenant-wide in-flight tasks.
    pub max_concurrency: u32,
    /// Most instances one service of the tenant may run at once (D-B2): bounds
    /// `scaling.max_instances` and `scaling.min_instances`. 0 = the node default (no plan cap).
    pub max_instances: u32,
    /// Most `scaling.min_instances` summed over all services of the tenant (warm floors kept
    /// without traffic). 0 = no tenant-wide cap.
    pub max_warm_floor_total: u32,
    /// Tenant-wide token bucket.
    pub rps: u32,
    pub invocations_per_day: u64,
    /// KV + SQL + blobs across all services.
    pub storage_mb: u64,
    pub egress_mb_per_day: u64,
    pub max_schedules: u32,
    pub min_schedule_seconds: u32,
    pub max_queue_depth: u64,
    pub max_pinned_mb: u64,
    pub allowed_triggers: Vec<TriggerKind>,
    pub limits_max: PlanLimitsMax,
    pub builds_per_hour: u32,
    /// Suffixes tenants may attach custom hosts under (default none = operator-only).
    pub custom_host_suffixes: Vec<String>,
    /// Max share of blocking-pool threads one tenant may occupy.
    pub blocking_share: f64,
    pub egress_default: EgressDefault,
    pub allow_cross_tenant_calls: bool,
    /// Open WebSockets across the tenant's services; an upgrade over it is refused
    /// (`limit_exceeded`). 0 = no tenant cap (the node's `ws.max_sockets` still applies).
    pub max_sockets: u32,
    /// Open WebSockets on one object. 0 = no cap.
    pub max_sockets_per_object: u32,
    /// Largest WebSocket message a client may send; a larger one closes the socket with 1009.
    pub ws_max_message_bytes: u32,
    /// Pending async invocations per service; more are refused (`429 async_backlog_full`).
    pub max_async_pending: u64,
    /// Stream topics per tenant.
    pub max_topics: u32,
    /// Partitions of one topic (at most [`crate::events::PARTITIONS_HARD_MAX`]).
    pub max_topic_partitions: u32,
    /// Stream record bytes across the tenant's topics (not part of `storage_mb`).
    pub max_stream_mb: u64,
    /// Longest retention a topic may set, in hours.
    pub max_stream_retention_hours: u32,
    /// Previews per tenant (they also count in `max_services`). 0 = no cap of its own.
    pub max_scratch: u32,
    /// Longest life of a preview, in seconds, and its default one. 0 = no cap (the default is 7 days).
    pub scratch_ttl_max: u64,
    /// CPU milliseconds per UTC day across the tenant; new invocations then get `429 cpu_quota`.
    /// 0 = no cap.
    pub cpu_ms_per_day: u64,
    /// Projects per tenant (the store's hard cap of 100 still applies). 0 = no cap of its own.
    pub max_projects: u32,
    /// Environments of one project (the hard cap of 20 still applies). 0 = no cap of its own.
    pub max_environments_per_project: u32,
    /// Branch services per tenant (they also count in `max_services`). 0 = no cap of its own.
    pub max_branches: u32,
    /// Secrets of one service, refs included. 0 = no cap.
    pub max_secrets_per_service: u32,
    /// Custom hosts a tenant may claim, active or pending. Operator attachments are not refused,
    /// but count. 0 = no cap.
    pub max_custom_hosts: u32,
    /// Versions kept per service; the lower of this and `deploy.retain_versions`. 0 = node value.
    pub max_versions_retained: u32,
    /// Days logs of the tenant are kept; the lower of this and `logs.retention_days`. 0 = node value.
    pub log_retention_days: u32,
    /// Outbound fetches of one call; the lower of this and the trigger's default. 0 = no cap.
    pub max_fetches_per_call: u32,
    /// Log lines of one call; the lower of this and the trigger's default. 0 = no cap.
    pub max_log_lines_per_call: u32,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            max_services: 10,
            max_concurrency: 10,
            max_instances: 4,
            max_warm_floor_total: 8,
            rps: 50,
            invocations_per_day: 100_000,
            storage_mb: 100,
            egress_mb_per_day: 1_000,
            max_schedules: 3,
            min_schedule_seconds: 60,
            max_queue_depth: 10_000,
            max_pinned_mb: 0,
            allowed_triggers: vec![
                TriggerKind::Http,
                TriggerKind::Interval,
                TriggerKind::Queue,
                TriggerKind::Call,
                TriggerKind::Websocket,
                TriggerKind::Pubsub,
                TriggerKind::Stream,
            ],
            limits_max: PlanLimitsMax::default(),
            builds_per_hour: 20,
            custom_host_suffixes: Vec::new(),
            blocking_share: 0.25,
            egress_default: EgressDefault::Allow,
            allow_cross_tenant_calls: false,
            max_sockets: 1_000,
            max_sockets_per_object: 1_000,
            ws_max_message_bytes: 1024 * 1024,
            max_async_pending: 10_000,
            max_topics: 10,
            max_topic_partitions: 4,
            max_stream_mb: 100,
            max_stream_retention_hours: 72,
            max_scratch: 5,
            scratch_ttl_max: 7 * 24 * 3600,
            cpu_ms_per_day: 0,
            max_projects: 20,
            max_environments_per_project: 10,
            max_branches: 10,
            max_secrets_per_service: 100,
            max_custom_hosts: 10,
            max_versions_retained: 0,
            log_retention_days: 0,
            max_fetches_per_call: 0,
            max_log_lines_per_call: 0,
        }
    }
}

impl Plan {
    /// The built-in plan of the system tenant (operator SDK services), used unless `[plans.sys]`
    /// is configured: no tenant-wide rate or daily caps, every trigger, room for many SDKs.
    pub fn system() -> Self {
        Self {
            max_services: 1_000,
            max_concurrency: 10_000,
            max_instances: 0,
            max_warm_floor_total: 0,
            rps: 0,
            invocations_per_day: 0,
            storage_mb: 10_240,
            egress_mb_per_day: 0,
            max_schedules: 100,
            min_schedule_seconds: 60,
            max_queue_depth: 100_000,
            allowed_triggers: TriggerKind::ALL.to_vec(),
            builds_per_hour: 1_000,
            blocking_share: 0.5,
            max_sockets: 0,
            max_sockets_per_object: 0,
            max_async_pending: 100_000,
            max_topics: 1_000,
            max_topic_partitions: 16,
            max_stream_mb: 10_240,
            max_stream_retention_hours: 720,
            limits_max: PlanLimitsMax {
                min_run_time_ms: 600_000,
                ..PlanLimitsMax::default()
            },
            max_projects: 0,
            max_environments_per_project: 0,
            max_branches: 0,
            max_secrets_per_service: 0,
            max_custom_hosts: 0,
            ..Self::default()
        }
    }

    /// Lower a task's per-call budgets to the plan's. Called once per task spec; allocates nothing.
    pub fn cap_call_budgets(&self, l: &mut crate::limits::Limits) {
        if self.max_fetches_per_call > 0 {
            l.max_fetches = l.max_fetches.min(self.max_fetches_per_call);
        }
        if self.max_log_lines_per_call > 0 {
            l.max_log_lines = l.max_log_lines.min(self.max_log_lines_per_call);
        }
    }

    pub fn allows_trigger(&self, t: TriggerKind) -> bool {
        self.allowed_triggers.contains(&t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_budgets_only_ever_lower_the_task_limits() {
        let base = crate::limits::Limits::default();
        let mut l = base;
        Plan::default().cap_call_budgets(&mut l);
        assert_eq!(l, base, "0 leaves the defaults");
        let plan = Plan {
            max_fetches_per_call: 5,
            max_log_lines_per_call: 1_000_000,
            ..Plan::default()
        };
        plan.cap_call_budgets(&mut l);
        assert_eq!(l.max_fetches, 5);
        assert_eq!(l.max_log_lines, base.max_log_lines, "a higher value does not raise it");
    }

    /// Plans cap `min_run_time_ms` at 60 s by default and 10 min for the system plan.
    #[test]
    fn plan_limits_default_min_run_time_is_60s() {
        assert_eq!(PlanLimitsMax::default().min_run_time_ms, 60_000);
        assert_eq!(Plan::system().limits_max.min_run_time_ms, 600_000);
        let p: PlanLimitsMax = serde_json::from_str(r#"{"wall_ms":1000}"#).unwrap();
        assert_eq!(p.min_run_time_ms, 60_000);
    }
}
