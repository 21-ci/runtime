//! Plain types of the memory governor.

use std::collections::BTreeMap;
use std::fmt;

use rt_types::{ServiceId, TaskId, TenantId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What a reservation is for (`rt_mem_committed_bytes{category}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemCategory {
    DenoHeap,
    WasmLinear,
    /// Idle reusable WASM instances (evictable).
    WasmReuse,
    T1Cache,
    IoBuffers,
    SqlCache,
    Logs,
    Other,
}

impl MemCategory {
    pub const ALL: [MemCategory; 8] = [
        MemCategory::DenoHeap,
        MemCategory::WasmLinear,
        MemCategory::WasmReuse,
        MemCategory::T1Cache,
        MemCategory::IoBuffers,
        MemCategory::SqlCache,
        MemCategory::Logs,
        MemCategory::Other,
    ];

    /// Dense index for per-category arrays.
    pub const fn idx(self) -> usize {
        self as usize
    }

    /// Metric label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            MemCategory::DenoHeap => "deno_heap",
            MemCategory::WasmLinear => "wasm_linear",
            MemCategory::WasmReuse => "wasm_reuse",
            MemCategory::T1Cache => "t1_cache",
            MemCategory::IoBuffers => "io_buffers",
            MemCategory::SqlCache => "sql_cache",
            MemCategory::Logs => "logs",
            MemCategory::Other => "other",
        }
    }
}

impl fmt::Display for MemCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who a reservation is charged to (diagnostics only).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Owner {
    pub tenant: Option<TenantId>,
    pub service: Option<ServiceId>,
    pub task: Option<TaskId>,
}

impl Owner {
    pub const NONE: Owner = Owner {
        tenant: None,
        service: None,
        task: None,
    };

    pub fn task(task: TaskId) -> Self {
        Self {
            task: Some(task),
            ..Self::NONE
        }
    }

    pub fn service(tenant: TenantId, service: ServiceId) -> Self {
        Self {
            tenant: Some(tenant),
            service: Some(service),
            task: None,
        }
    }
}

/// Pressure ladder level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Pressure {
    #[default]
    Normal,
    /// Above the `high` watermark: evict T1, drop idle reuse instances/pools, purge allocator;
    /// the scheduler shrinks concurrency limits and warm targets.
    High,
    /// Above the `critical` watermark: High + node `shed` mode (entered by the scheduler's sysmon,
    /// left once pressure is below `high` again) and no warm targets.
    Critical,
}

impl Pressure {
    /// Value of `rt_mem_pressure_level`.
    pub const fn level(self) -> u8 {
        self as u8
    }
}

/// How hard to purge (allocator `mi_collect`, V8 pools, pool decommit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PurgeLevel {
    Light,
    Full,
}

/// Monotonic purge request observed by each M when it parks (thread-local heaps must be
/// collected by their owner thread).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PurgeRequest {
    /// Increases on every `request_purge`.
    pub epoch: u64,
    pub level: PurgeLevel,
}

#[derive(Clone, Debug, PartialEq, Error)]
pub enum MemError {
    /// Not enough budget left for the request.
    #[error("memory budget exceeded: requested {requested} bytes, {available} available")]
    OverBudget { requested: u64, available: u64 },
    /// Reservations of this category are refused at `Critical` pressure.
    #[error("memory pressure is critical")]
    Critical,
}

/// Snapshot of governor accounting.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemStats {
    pub budget_bytes: u64,
    pub committed_bytes: u64,
    pub by_category: BTreeMap<MemCategory, u64>,
    /// Process RSS (0 when unknown).
    pub rss_bytes: u64,
    /// `rss / (committed + baseline_rss)`; 1.0 when unknown.
    pub fragmentation_ratio: f64,
    pub pressure: Pressure,
    pub live_reservations: u64,
}
