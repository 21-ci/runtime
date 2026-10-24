//! Resolved per-task limits and per-service scheduling knobs.
//!
//! These are *resolved* values (profile + plan + service already merged by `rt-config`).
//! Defaults below are the `balanced` HTTP limits from docs/handlers.md.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::task::Trigger;

/// Maximum service-to-service call depth; deeper calls get `508 call_depth`.
pub const MAX_CALL_DEPTH: u8 = 15;

/// Hard limits enforced on one task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Wall-clock deadline for the whole task (queue wait excluded; starts at `start`).
    pub wall: Duration,
    /// CPU time budget (thread CPU clock).
    pub cpu: Duration,
    /// Heap / linear memory ceiling.
    pub memory_mb: u32,
    pub max_request_body_bytes: u64,
    /// Buffered responses.
    pub max_response_buffered_bytes: u64,
    /// Streamed responses.
    pub max_response_stream_bytes: u64,
    /// Outbound fetches per task.
    pub max_fetches: u32,
    /// Concurrent outbound fetches per task.
    pub max_concurrent_fetches: u32,
    /// Log lines recorded per task (then dropped + counted).
    pub max_log_lines: u32,
}

impl Default for Limits {
    /// `balanced` HTTP defaults.
    fn default() -> Self {
        Self {
            wall: Duration::from_secs(30),
            cpu: Duration::from_millis(1_000),
            memory_mb: 128,
            max_request_body_bytes: 10 * 1024 * 1024,
            max_response_buffered_bytes: 10 * 1024 * 1024,
            max_response_stream_bytes: 100 * 1024 * 1024,
            max_fetches: 50,
            max_concurrent_fetches: 6,
            max_log_lines: 1_000,
        }
    }
}

impl Limits {
    /// Default limits for a trigger (table). `Call` uses the HTTP row; the caller must
    /// then clamp `wall` to the parent's remaining time.
    pub fn defaults_for(trigger: &Trigger) -> Self {
        let base = Self::default();
        match trigger {
            Trigger::Interval { .. } | Trigger::Cron { .. } | Trigger::Scheduled { .. } => Self {
                wall: Duration::from_secs(300),
                cpu: Duration::from_millis(10_000),
                max_fetches: 200,
                ..base
            },
            Trigger::Queue { .. } | Trigger::Stream { .. } => Self {
                wall: Duration::from_secs(300),
                cpu: Duration::from_millis(10_000),
                max_request_body_bytes: 1024 * 1024,
                max_fetches: 200,
                ..base
            },
            Trigger::Build { .. } => Self {
                wall: Duration::from_secs(120),
                cpu: Duration::from_secs(120),
                memory_mb: 256,
                ..base
            },
            Trigger::Http
            | Trigger::Call { .. }
            | Trigger::Socket { .. }
            | Trigger::Alarm { .. }
            | Trigger::Pubsub { .. } => base,
        }
    }
}

/// When a version is compiled.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompileMode {
    #[default]
    Lazy,
    Eager,
}

/// Autoscaling of one service (decision D-A3): the bounds
/// of its adaptive concurrency limit and of the warm capacity the scheduler asks executors to
/// keep. Resolved from the service's profile; [`ServiceKnobs::max_instances`] caps `start` and
/// `min`, and [`ServiceKnobs::min_instances`] raises them. `Default` is the `balanced` profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalingKnobs {
    /// Concurrency limit of a service the scheduler has no history for.
    pub start: u32,
    /// The adaptive limit never shrinks below this.
    pub min: u32,
    /// The limit shrinks when the recent mean latency exceeds the service's moving baseline by
    /// this factor, in percent (200 = twice the baseline).
    pub latency_tolerance_pct: u32,
    /// Warm instances kept per unit of recent demand, in percent (125 = 25 % headroom).
    pub warm_headroom_pct: u32,
    /// Upper bound of the warm target per processor.
    pub warm_max_per_proc: u32,
}

impl Default for ScalingKnobs {
    /// `balanced` profile.
    fn default() -> Self {
        Self {
            start: 16,
            min: 2,
            latency_tolerance_pct: 200,
            warm_headroom_pct: 125,
            warm_max_per_proc: 4,
        }
    }
}

/// Service-scope scheduling and cache knobs (resolved values).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceKnobs {
    /// Most instances (concurrently running copies) of the service: `scaling.max_instances`
    /// (the owner's hard cap, at most the plan's `max_instances`), else the plan's limit or the
    /// profile's ceiling. It is the ceiling of the adaptive concurrency limit; tasks beyond the
    /// current limit wait in the service's queue (D-A3, D-B2), they are never rejected for capacity.
    pub max_instances: u32,
    /// Instances kept warm (`scaling.min_instances`, D-B2): the adaptive limit never falls below
    /// it and the scheduler never lets the service's warm target drop below it (no scale to zero).
    /// 0 = scale to zero when idle.
    pub min_instances: u32,
    /// Explicit bound on this service's queued tasks (`scaling.max_queued`); 0 = the node share
    /// (half of `sched.max_queued`). Over the bound: `503 overloaded`.
    pub max_queued: u32,
    /// Shed tasks not started within this time (`sched.queue_timeout`, service scope).
    pub queue_timeout: Duration,
    /// Tenant-wide in-flight cap from the plan (`plan.max_concurrency`); 0 = unlimited.
    pub tenant_max_concurrency: u32,
    /// Exempt from T1 eviction (bounded by plan pin cap).
    pub keep_resident: bool,
    /// WASM: reuse instances after a clean completion (`wasm.reuse_instances`).
    pub reuse_instances: bool,
    /// Deno: keep an isolate with the evaluated module for the next task of the same service
    /// version after a clean completion (`deno.reuse_isolates`, decision D-A4).
    #[serde(default)]
    pub reuse_isolates: bool,
    /// T1 idle TTL (`cache.idle_ttl`).
    pub idle_ttl: Duration,
    /// Preload at startup (`cache.preload`).
    pub preload: bool,
    pub compile: CompileMode,
    /// Adaptive concurrency and warm-capacity bounds from the service's profile (D-A3).
    #[serde(default)]
    pub scaling: ScalingKnobs,
    /// Keep-alive of a reused instance after it answered (`scaling.min_run_time_ms`): `None` =
    /// inherit the executor's idle TTL, `Some(0)` = only while tasks of the service wait.
    #[serde(default)]
    pub min_run_time: Option<Duration>,
}

impl Default for ServiceKnobs {
    /// `balanced` profile, no explicit `scaling` values.
    fn default() -> Self {
        Self {
            max_instances: 1024,
            min_instances: 0,
            max_queued: 0,
            queue_timeout: Duration::from_secs(5),
            tenant_max_concurrency: 0,
            keep_resident: false,
            reuse_instances: false,
            reuse_isolates: false,
            idle_ttl: Duration::from_secs(600),
            preload: false,
            compile: CompileMode::Lazy,
            scaling: ScalingKnobs::default(),
            min_run_time: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Knobs serialized before `min_run_time` existed still deserialize (as inherit).
    #[test]
    fn knobs_without_min_run_time_deserialize() {
        let mut v = serde_json::to_value(ServiceKnobs::default()).unwrap();
        v.as_object_mut().unwrap().remove("min_run_time");
        let k: ServiceKnobs = serde_json::from_value(v).unwrap();
        assert_eq!(k.min_run_time, None);
    }
}
