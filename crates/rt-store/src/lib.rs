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
pub mod blocking;
pub mod data;
pub mod delayed;
pub mod durable;
pub mod error;
pub mod faults;
pub mod meta;
pub mod queue;
mod sqlutil;
pub mod stream;
pub mod sync_sql;

pub use artifact::{ArtifactStore, LocalArtifactStore, ARTIFACT_GRACE};
pub use blocking::{charge_cpu, with_cpu_meter, BlockingRunner, CpuMeter, TokioBlocking};
pub use data::{
    BatchMode, BlobBackend, BlobListOptions, BlobMeta, BlobObject, BlobPage, DataConfig, DurableBackend,
    DurableListOptions, DurableValue, DurableWrite, KvBackend, KvListOptions, KvPage, KvSetOptions, KvValue,
    LocalServiceData, ServiceDataStore, ServiceDirectory, SqlBackend, SqlResult, SqlStatement, SqlTx, SqlValue,
    ValueTag, DURABLE_FORMAT_BYTES, DURABLE_FORMAT_V8,
};
pub use delayed::{
    DelayedEntry, DelayedItem, DelayedStore, NewDelayed, RetryOutcome, DELAYED_MAX_ATTEMPTS, MAX_DELAYED_PENDING,
};
pub use error::{Result, StoreError};
pub use faults::{Fault, Faults};
pub use meta::{
    HostRelease, MetaStore, Page, PageReq, SecretMeta, ServiceFilter, SqliteMetaStore, TenantPatch, TenantScope,
    UsageRow, WarmFloorCap, MIGRATIONS as META_MIGRATIONS,
};
pub use queue::{
    DlqEntry, Leased, NewMessage, QueueStats, QueueStore, RetryResult, SqliteQueueStore, MIGRATIONS as QUEUE_MIGRATIONS,
};
pub use sync_sql::{LocalDb, LocalError, LocalSqlSource, RowRef, RunStats, SqlRef, StmtInfo, TxMode};
pub use stream::{
    Appended, Cursor, CursorKey, DeadRecord, NewRecord, PartitionHead, ReadFrom, SqliteStreamStore, StreamStore, Topic,
    TopicSpec, TrimStats, STREAM_MIGRATIONS,
};
