//! Task outcomes and their mapping to HTTP (docs/api.md, mapping).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::body::Body;

/// Why the runtime killed a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KillReason {
    CpuLimit,
    Oom,
    WallTimeout,
    /// Node shutting down / executor restarting.
    Shutdown,
}

/// Why a task was refused before (or instead of) running.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    Draining,
    Shed,
    Overloaded,
    QueueTimeout,
    Memory,
    /// Legacy (429): not produced since decision D-A3; the plan's tenant cap queues instead.
    TenantConcurrency,
    /// Legacy (429): not produced since decision D-A3; per-service limits queue instead.
    ServiceConcurrency,
    RateLimited,
    QuotaExceeded,
    CallDepth,
}

impl RejectReason {
    pub const ALL: [RejectReason; 10] = [
        RejectReason::Draining,
        RejectReason::Shed,
        RejectReason::Overloaded,
        RejectReason::QueueTimeout,
        RejectReason::Memory,
        RejectReason::TenantConcurrency,
        RejectReason::ServiceConcurrency,
        RejectReason::RateLimited,
        RejectReason::QuotaExceeded,
        RejectReason::CallDepth,
    ];

    /// `x-rt-reason` value.
    pub const fn reason(self) -> &'static str {
        match self {
            RejectReason::Draining => "draining",
            RejectReason::Shed => "shed",
            RejectReason::Overloaded => "overloaded",
            RejectReason::QueueTimeout => "queue_timeout",
            RejectReason::Memory => "memory",
            RejectReason::TenantConcurrency => "tenant_concurrency",
            RejectReason::ServiceConcurrency => "service_concurrency",
            RejectReason::RateLimited => "rate_limited",
            RejectReason::QuotaExceeded => "quota_exceeded",
            RejectReason::CallDepth => "call_depth",
        }
    }

    /// HTTP status (docs/api.md).
    pub const fn http_status(self) -> u16 {
        match self {
            RejectReason::TenantConcurrency
            | RejectReason::ServiceConcurrency
            | RejectReason::RateLimited
            | RejectReason::QuotaExceeded => 429,
            RejectReason::CallDepth => 508,
            RejectReason::Draining
            | RejectReason::Shed
            | RejectReason::Overloaded
            | RejectReason::QueueTimeout
            | RejectReason::Memory => 503,
        }
    }

    /// `Retry-After` for capacity refusals (`overloaded`, `queue_timeout`, `memory`, `shed`, and
    /// the legacy concurrency reasons): the node sheds load it expects to absorb shortly
    ///. `None` for `draining` (retry elsewhere), call depth,
    /// and the plan quotas, whose enforcer computes its own value.
    pub const fn retry_after(self) -> Option<Duration> {
        match self {
            RejectReason::Overloaded
            | RejectReason::QueueTimeout
            | RejectReason::Memory
            | RejectReason::Shed
            | RejectReason::TenantConcurrency
            | RejectReason::ServiceConcurrency => Some(Duration::from_secs(1)),
            RejectReason::Draining
            | RejectReason::RateLimited
            | RejectReason::QuotaExceeded
            | RejectReason::CallDepth => None,
        }
    }
}

impl KillReason {
    /// `x-rt-reason` value.
    pub const fn reason(self) -> &'static str {
        match self {
            KillReason::CpuLimit => "cpu_limit",
            KillReason::Oom => "oom",
            KillReason::WallTimeout => "timeout",
            KillReason::Shutdown => "restarting",
        }
    }

    pub const fn http_status(self) -> u16 {
        match self {
            KillReason::CpuLimit | KillReason::Oom => 502,
            KillReason::WallTimeout => 504,
            KillReason::Shutdown => 503,
        }
    }
}

/// Failure reasons of a build.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildFailReason {
    Syntax,
    Resolve,
    Fetch,
    LockMismatch,
    Timeout,
    Oom,
    TooLarge,
    Internal,
}

impl BuildFailReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            BuildFailReason::Syntax => "syntax",
            BuildFailReason::Resolve => "resolve",
            BuildFailReason::Fetch => "fetch",
            BuildFailReason::LockMismatch => "lock_mismatch",
            BuildFailReason::Timeout => "timeout",
            BuildFailReason::Oom => "oom",
            BuildFailReason::TooLarge => "too_large",
            BuildFailReason::Internal => "internal",
        }
    }
}

/// A source diagnostic (`422` body entries, build logs).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub file: String,
    pub line: u32,
    pub col: u32,
    pub message: String,
}

/// Why a build (or a prepared-artifact load) failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFailure {
    pub reason: BuildFailReason,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl BuildFailure {
    pub fn new(reason: BuildFailReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            diagnostics: Vec::new(),
        }
    }
}

impl std::fmt::Display for BuildFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "build failed ({}): {}", self.reason.as_str(), self.message)
    }
}

impl std::error::Error for BuildFailure {}

/// Per-task phase timings (`server-timing`, stats, metrics).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTimings {
    pub queue: Duration,
    pub wait_runtime: Duration,
    pub init: Duration,
    pub exec: Duration,
    /// Thread CPU time charged to the task.
    pub cpu: Duration,
    pub polls: u32,
    pub preemptions: u32,
    pub steals: u32,
    pub waits: u32,
}

impl TaskTimings {
    /// `server-timing` header value: `queue;dur=…, wait_runtime;dur=…, init;dur=…, exec;dur=…`
    /// (milliseconds with 3 decimals).
    pub fn server_timing(&self) -> String {
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        format!(
            "queue;dur={:.3}, wait_runtime;dur={:.3}, init;dur={:.3}, exec;dur={:.3}",
            ms(self.queue),
            ms(self.wait_runtime),
            ms(self.init),
            ms(self.exec)
        )
    }

    /// Sum of the four reported phases.
    pub fn total(&self) -> Duration {
        self.queue + self.wait_runtime + self.init + self.exec
    }
}

/// The successful result of a guest run: an HTTP-shaped reply.
#[derive(Debug)]
pub struct Reply {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: Body,
    pub timings: TaskTimings,
}

impl Reply {
    pub fn new(status: http::StatusCode, body: Body) -> Self {
        Self {
            status,
            headers: http::HeaderMap::new(),
            body,
            timings: TaskTimings::default(),
        }
    }

    /// `200` with an empty body.
    pub fn ok() -> Self {
        Self::new(http::StatusCode::OK, Body::empty())
    }
}

/// Final result of a task.
#[derive(Debug)]
pub enum Outcome {
    /// Guest produced a reply (any HTTP status chosen by the guest).
    Ok(Reply),
    /// Guest threw / trapped, not a limit.
    GuestError,
    Killed(KillReason),
    Rejected(RejectReason),
    BuildFailed(BuildFailure),
}

impl Outcome {
    /// HTTP status for this outcome (table in docs/api.md).
    pub fn http_status(&self) -> u16 {
        match self {
            Outcome::Ok(r) => r.status.as_u16(),
            Outcome::GuestError => 500,
            Outcome::Killed(k) => k.http_status(),
            Outcome::Rejected(r) => r.http_status(),
            Outcome::BuildFailed(_) => 502,
        }
    }

    /// `x-rt-reason` for non-`Ok` outcomes.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            Outcome::Ok(_) => None,
            Outcome::GuestError => Some("guest_error"),
            Outcome::Killed(k) => Some(k.reason()),
            Outcome::Rejected(r) => Some(r.reason()),
            Outcome::BuildFailed(_) => Some("build_failed"),
        }
    }

    /// Label for `rt_tasks_total{outcome}`.
    pub fn label(&self) -> &'static str {
        match self {
            Outcome::Ok(_) => "ok",
            Outcome::GuestError => "guest_error",
            Outcome::Killed(k) => k.reason(),
            Outcome::Rejected(r) => r.reason(),
            Outcome::BuildFailed(_) => "build_failed",
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Outcome::Ok(_))
    }
}
