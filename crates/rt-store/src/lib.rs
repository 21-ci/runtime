//! rt-store (L2): everything persistent.
//!
//! - [`MetaStore`] — `meta.db`: tenants, keys, services, versions, env, sealed secrets, hosts,
//!   schedules, usage, events, cache frequencies (rusqlite, bundled SQLite, WAL);
//! - [`ArtifactStore`] — content-addressed source/bundle/wasm artifacts (local; S3 = stretch);
//! - [`QueueStore`] — `queue.db`, durable at-least-once queue + DLQ;
//! - [`StreamStore`] — `streams.db`, partitioned topic logs with consumer cursors and dead letters;
//! - [`ServiceDataStore`] — per-service `data.db` (KV + SQL with authorizer) and blobs.
//!
//! No other crate writes SQL against these databases.

pub mod artifact;
pub mod backups;
pub mod blocking;
pub mod browse;
pub mod data;
pub mod data_events;
pub mod delayed;
pub mod durable;
pub mod error;
pub mod faults;
pub mod import_check;
pub mod meta;
mod migrate;
pub mod queue;
pub mod snapshot;
mod sqlutil;
pub mod stream;
pub mod sync_sql;

pub use artifact::{ArtifactStore, LocalArtifactStore, ARTIFACT_GRACE};
pub use backups::BackupRow;
pub use blocking::{charge_cpu, with_cpu_meter, BlockingRunner, CpuMeter, TokioBlocking};
pub use browse::{
    BrowseCell, BrowseColumn, BrowseKvEntry, BrowseKvPage, BrowseLimits, BrowseResult, BrowseTable, BLOB_HEAD_BYTES,
    BROWSE_BUDGET, MAX_BROWSE_TABLES,
};
pub use data::{
    blob_file_name_ok, BatchMode, BlobBackend, BlobListOptions, BlobMeta, BlobObject, BlobPage, DataConfig,
    DurableBackend, DurableListOptions, DurableValue, DurableWrite, KvBackend, KvListOptions, KvPage, KvSetOptions,
    KvValue, LocalServiceData, MigrationDryRun, ServiceDataStore, ServiceDirectory, SqlBackend, SqlResult,
    SqlStatement, SqlTx, SqlValue, ValueTag, DURABLE_FORMAT_BYTES, DURABLE_FORMAT_V8,
};
pub use delayed::{
    DelayedEntry, DelayedItem, DelayedStore, NewDelayed, RetryOutcome, DELAYED_MAX_ATTEMPTS, MAX_DELAYED_PENDING,
};
pub use error::{Result, StoreError};
pub use faults::{Fault, Faults};
pub use meta::{
    HostRelease, MetaStore, Page, PageReq, SecretMeta, SecretRef, SecretRefTarget, ServiceFilter, SqliteMetaStore,
    TelemetryCounts, TelemetryState, TenantPatch, TenantScope, UsageRow, WarmFloorCap, MIGRATIONS as META_MIGRATIONS,
};
pub use queue::{
    DlqEntry, Leased, NewMessage, QueueStats, QueueStore, RetryResult, SqliteQueueStore, MIGRATIONS as QUEUE_MIGRATIONS,
};
pub use snapshot::{free_bytes, lower_this_thread, snapshot_db, BackupHolds, ExportFile, GcPause, ServiceHold};
pub use stream::{
    Appended, Cursor, CursorKey, DeadRecord, NewRecord, PartitionHead, ReadFrom, SqliteStreamStore, StreamStore, Topic,
    TopicSpec, TrimStats, STREAM_MIGRATIONS,
};
pub use sync_sql::{LocalDb, LocalError, LocalSqlSource, RowRef, RunStats, SqlRef, StmtInfo, TxMode};
