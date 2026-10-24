//! Store errors. `code()` gives the stable, sanitized code surfaced to API clients and guests
//! (never SQLite paths or S3 details, SEC-26).

use thiserror::Error;

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum StoreError {
    /// Missing entity — or one owned by another tenant (existence is never revealed).
    #[error("not found")]
    NotFound,
    /// `slug_taken`, `host_taken`, duplicate name, …
    #[error("conflict: {0}")]
    Conflict(&'static str),
    /// `If-Match` did not match the current ETag (`412 precondition_failed`).
    #[error("precondition failed")]
    PreconditionFailed,
    /// Value rejected (key too large, bad name, …).
    #[error("invalid: {0}")]
    Invalid(String),
    /// `storage_quota`: max page count / blob size exceeded.
    #[error("storage quota exceeded")]
    StorageQuota,
    /// SQL guest error with a sanitized message (`sql_error`).
    #[error("sql error: {0}")]
    Sql(String),
    /// Statement aborted by the progress handler after `storage.sql_budget_ms` (`sql_timeout`).
    #[error("sql statement timed out")]
    SqlTimeout,
    /// Authorizer denied the statement (`ATTACH`, `load_extension`, `_rt_*`, schema pragmas).
    #[error("sql statement not authorized")]
    SqlNotAuthorized,
    /// The database stayed locked by another connection for the whole `busy_timeout` (`sql_busy`):
    /// a synchronous (`std:sqlite`) statement waits at most `storage.sql_sync_busy_ms`.
    #[error("database is busy")]
    SqlBusy,
    /// A guest statement's result exceeded the row or byte cap (`sql_result_too_large`); page
    /// with `LIMIT`/`OFFSET` or select fewer columns.
    #[error("sql result too large")]
    SqlResultTooLarge,
    /// `kv.incr` on a value that is not a JSON integer in the `i64` range (`kv_not_integer`).
    #[error("kv value is not an integer")]
    KvNotInteger,
    /// `kv.incr` whose result would leave the `i64` range (`kv_overflow`).
    #[error("kv counter overflow")]
    KvOverflow,
    /// Stored data failed an integrity check (artifact digest mismatch, schema too new).
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("database error")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("internal store error: {0}")]
    Internal(String),
    /// `scaling.min_instances` summed over the tenant's services would be `total`, above the
    /// plan's `max_warm_floor_total` (`limit_exceeds_plan`).
    #[error("`scaling.min_instances` summed over the tenant's services would be {total}, the plan allows {cap}")]
    WarmFloorTotal { total: u64, cap: u32 },
}

impl StoreError {
    /// Stable code for control API errors / guest-visible host errors.
    pub fn code(&self) -> &'static str {
        match self {
            StoreError::NotFound => "not_found",
            StoreError::Conflict(c) => c,
            StoreError::PreconditionFailed => "precondition_failed",
            StoreError::Invalid(_) => "validation_failed",
            StoreError::StorageQuota => "storage_quota",
            StoreError::Sql(_) => "sql_error",
            StoreError::SqlTimeout => "sql_timeout",
            StoreError::SqlNotAuthorized => "sql_not_authorized",
            StoreError::SqlBusy => "sql_busy",
            StoreError::SqlResultTooLarge => "sql_result_too_large",
            StoreError::KvNotInteger => "kv_not_integer",
            StoreError::KvOverflow => "kv_overflow",
            StoreError::WarmFloorTotal { .. } => "limit_exceeds_plan",
            StoreError::Corrupt(_) | StoreError::Sqlite(_) | StoreError::Io(_) | StoreError::Internal(_) => "internal",
        }
    }
}
