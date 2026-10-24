//! rt-obs (L1): guest/system logs, Prometheus metrics, per-service HDR stats, scheduler
//! trace formatting, tracing setup.

pub mod log;
pub mod metrics;
pub mod schedtrace;
pub mod stats;
pub mod tracing_init;

use thiserror::Error;

pub use log::{
    parse_log_fields, FileLogStore, LogCursor, LogFieldsError, LogKind, LogLevel, LogPage, LogQuery, LogRecord,
    LogSink, LogStore, LogStoreConfig, LogStream, MemoryLogSink, NullLogSink, SweepReport, MAX_LOG_FIELDS,
    MAX_LOG_FIELDS_BYTES, MAX_MSG_CHARS,
};
pub use metrics::{names as metric_names, Metrics, MetricsOptions, NODE_METRIC_NAMES};
pub use schedtrace::{format_schedtrace, format_schedtrace_detail};
pub use stats::{HdrStatsStore, Percentiles, ServiceStats, StatsStore, Window};
pub use tracing_init::{init_tracing, TracingOptions};

#[derive(Debug, Error)]
pub enum ObsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metrics registration failed: {0}")]
    Metrics(#[from] prometheus::Error),
    #[error("tracing init failed: {0}")]
    Tracing(String),
}
