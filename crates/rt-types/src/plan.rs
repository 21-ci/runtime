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
}

impl TriggerKind {
    pub const ALL: [TriggerKind; 6] = [
        TriggerKind::Http,
        TriggerKind::Interval,
        TriggerKind::Cron,
        TriggerKind::Queue,
        TriggerKind::Call,
        TriggerKind::Websocket,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TriggerKind::Http => "http",
            TriggerKind::Interval => "interval",
            TriggerKind::Cron => "cron",
            TriggerKind::Queue => "queue",
            TriggerKind::Call => "call",
            TriggerKind::Websocket => "websocket",
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
}

impl Default for PlanLimitsMax {
    fn default() -> Self {
        Self {
            wall_ms: 30_000,
            cpu_ms: 1_000,
            memory_mb: 128,
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
            ..Self::default()
        }
    }

    pub fn allows_trigger(&self, t: TriggerKind) -> bool {
        self.allowed_triggers.contains(&t)
    }
}
