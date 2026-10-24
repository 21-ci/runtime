//! Guest-facing KV / SQL / blob facades.
//!
//! They sit between executors and `rt-store` backends and add what the backends do not know:
//! size limits, stable sanitized error codes (no host paths — SEC-26), quota checks, usage
//! counters and CPU charging. Executors translate these to the `std:kv|sql|blob` JS modules and
//! the `std:host/*` WIT interfaces.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_store::{
    with_cpu_meter, BatchMode, BlobBackend, BlobListOptions, BlobMeta, BlobObject, BlobPage, CpuMeter, KvBackend,
    KvListOptions, KvPage, KvSetOptions, KvValue, SqlBackend, SqlResult, SqlStatement, SqlTx, StoreError,
};
use rt_types::Body;
use thiserror::Error;

/// Interactive SQL transactions are rolled back after this long (STO-07).
pub const SQL_TX_MAX: Duration = Duration::from_secs(5);

/// The longest KV counter as stored (`-9223372036854775808`): what `kv.incr` charges per call.
pub const MAX_COUNTER_BYTES: usize = 20;

/// Run `fut` with the task's CPU meter installed (host-side CPU is charged to the task, SEC-29).
async fn metered<F: Future>(meter: &Option<CpuMeter>, fut: F) -> F::Output {
    match meter {
        Some(m) => with_cpu_meter(m.clone(), fut).await,
        None => fut.await,
    }
}

/// A host-call failure with a stable code and a sanitized message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub struct HostError {
    /// `sql_error`, `sql_timeout`, `sql_not_authorized`, `storage_quota`, `kv_key_too_large`,
    /// `kv_value_too_large`, `blob_key_too_large`, `not_found`, `internal`, …
    pub code: &'static str,
    pub message: String,
}

impl HostError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Sanitize a store error: only the stable code survives, never paths or driver messages.
    pub fn from_store(e: &StoreError) -> Self {
        Self {
            code: e.code(),
            message: e.code().replace('_', " "),
        }
    }
}

impl From<StoreError> for HostError {
    fn from(e: StoreError) -> Self {
        HostError::from_store(&e)
    }
}

/// Limits applied by the facades (plan/config driven).
#[derive(Clone, Copy, Debug)]
pub struct FacadeLimits {
    pub max_kv_key: usize,
    pub max_kv_value: usize,
    pub max_blob_key: usize,
    /// Plan `storage_mb` remaining for this tenant, in bytes (`None` = unlimited).
    pub storage_bytes_left: Option<u64>,
    /// Blob object cap (plan; ≤ 100 MB).
    pub max_blob_bytes: u64,
}

impl Default for FacadeLimits {
    fn default() -> Self {
        Self {
            max_kv_key: rt_store::data::MAX_KV_KEY_BYTES,
            max_kv_value: rt_store::data::MAX_KV_VALUE_BYTES,
            max_blob_key: rt_store::data::MAX_BLOB_KEY_BYTES,
            storage_bytes_left: None,
            max_blob_bytes: 100 * 1024 * 1024,
        }
    }
}

/// What is left of the tenant's storage quota for one task: read at task start
/// ([`FacadeLimits::storage_bytes_left`]) and decremented by the KV / blob writes of the task, so
/// many writes cannot each pass the start-of-task check. A KV key is charged for its largest
/// value in the task, not per write: overwriting it again is charged only its growth (HOST-003).
/// Deletes do not give bytes back (the next task starts from the tenant's measured usage).
#[derive(Debug)]
pub struct StorageLeft {
    left: AtomicU64,
    /// Bytes charged per KV key in this task (at most [`MAX_TRACKED_KEYS`] keys; past that every
    /// write is charged in full, as before).
    keys: parking_lot::Mutex<std::collections::HashMap<Vec<u8>, u64>>,
}

/// KV keys whose charge a task remembers.
const MAX_TRACKED_KEYS: usize = 10_000;

impl StorageLeft {
    const UNLIMITED: u64 = u64::MAX;

    /// `None` = unlimited.
    pub fn new(left: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            left: AtomicU64::new(left.map_or(Self::UNLIMITED, |b| b.min(Self::UNLIMITED - 1))),
            keys: parking_lot::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Charge a write of `size` bytes under KV `key`: only what exceeds the key's largest charge
    /// in this task. `Some(charged)` (to give back with [`Self::refund_key`] if the write fails),
    /// `None` when the quota left does not cover it.
    pub fn charge_key(&self, key: &[u8], size: u64) -> Option<u64> {
        let mut keys = self.keys.lock();
        let prev = keys.get(key).copied().unwrap_or(0);
        let extra = size.saturating_sub(prev);
        if !self.try_charge(extra) {
            return None;
        }
        if extra > 0 && (keys.len() < MAX_TRACKED_KEYS || keys.contains_key(key)) {
            keys.insert(key.to_vec(), size);
        }
        Some(extra)
    }

    /// Undo [`Self::charge_key`] of a write that failed.
    pub fn refund_key(&self, key: &[u8], charged: u64) {
        if charged == 0 {
            return;
        }
        let mut keys = self.keys.lock();
        if let Some(v) = keys.get_mut(key) {
            *v = v.saturating_sub(charged);
            if *v == 0 {
                keys.remove(key);
            }
        }
        drop(keys);
        self.refund(charged);
    }

    /// Bytes left (`None` = unlimited).
    pub fn get(&self) -> Option<u64> {
        let v = self.left.load(Ordering::Acquire);
        (v != Self::UNLIMITED).then_some(v)
    }

    /// Take `n` bytes if that many are left.
    pub fn try_charge(&self, n: u64) -> bool {
        self.left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                if v == Self::UNLIMITED {
                    Some(v)
                } else {
                    v.checked_sub(n)
                }
            })
            .is_ok()
    }

    /// Account `n` bytes already written (saturating at zero).
    pub fn charge(&self, n: u64) {
        let _ = self.left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            if v == Self::UNLIMITED {
                None
            } else {
                Some(v.saturating_sub(n))
            }
        });
    }

    /// Give back `n` bytes of a write that failed.
    pub fn refund(&self, n: u64) {
        let _ = self.left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            if v == Self::UNLIMITED {
                None
            } else {
                Some(v.saturating_add(n).min(Self::UNLIMITED - 1))
            }
        });
    }
}

#[derive(Clone)]
pub struct KvFacade {
    backend: Arc<dyn KvBackend>,
    limits: FacadeLimits,
    meter: Option<CpuMeter>,
    left: Option<Arc<StorageLeft>>,
}

impl KvFacade {
    pub fn new(backend: Arc<dyn KvBackend>, limits: FacadeLimits) -> Self {
        Self {
            backend,
            limits,
            meter: None,
            left: None,
        }
    }

    /// Charge the CPU time of blocking store work to `meter` (the calling task's).
    pub fn with_cpu_meter(mut self, meter: CpuMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Check and decrement the task's remaining storage quota (instead of the start-of-task
    /// `limits.storage_bytes_left`) on every write.
    pub fn with_storage_left(mut self, left: Arc<StorageLeft>) -> Self {
        self.left = Some(left);
        self
    }

    fn check_key(&self, key: &[u8]) -> Result<(), HostError> {
        if key.is_empty() {
            return Err(HostError::new("validation_failed", "key must not be empty"));
        }
        if key.len() > self.limits.max_kv_key {
            return Err(HostError::new(
                "kv_key_too_large",
                format!("key exceeds {} bytes", self.limits.max_kv_key),
            ));
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<KvValue>, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.get(key)).await?)
    }

    /// At most [`rt_store::data::MAX_KV_LIST_LIMIT`] keys; the store stops past
    /// [`rt_store::data::MAX_RESULT_BYTES`] of values (`validation_failed`).
    pub async fn get_many(&self, keys: &[Bytes]) -> Result<Vec<Option<KvValue>>, HostError> {
        if keys.len() > rt_store::data::MAX_KV_LIST_LIMIT {
            return Err(HostError::new(
                "validation_failed",
                format!("getMany takes at most {} keys", rt_store::data::MAX_KV_LIST_LIMIT),
            ));
        }
        for k in keys {
            self.check_key(k)?;
        }
        Ok(metered(&self.meter, self.backend.get_many(keys)).await?)
    }

    /// Take `size` bytes of a write from the storage quota left (refunded by [`Self::refund`] when
    /// the write fails).
    fn reserve(&self, key: &[u8], size: u64) -> Result<u64, HostError> {
        match &self.left {
            Some(left) => left
                .charge_key(key, size)
                .ok_or_else(|| HostError::new("storage_quota", "storage quota exceeded")),
            None if self.limits.storage_bytes_left.is_some_and(|left| size > left) => {
                Err(HostError::new("storage_quota", "storage quota exceeded"))
            }
            None => Ok(0),
        }
    }

    fn refund(&self, key: &[u8], charged: u64) {
        if let Some(left) = &self.left {
            left.refund_key(key, charged);
        }
    }

    pub async fn set(&self, key: &[u8], value: KvValue, ttl: Option<Duration>) -> Result<(), HostError> {
        self.check_key(key)?;
        if value.data.len() > self.limits.max_kv_value {
            return Err(HostError::new(
                "kv_value_too_large",
                format!("value exceeds {} bytes", self.limits.max_kv_value),
            ));
        }
        let size = (key.len() + value.data.len()) as u64;
        let charged = self.reserve(key, size)?;
        let opts = KvSetOptions { ttl };
        let r = metered(&self.meter, self.backend.set(key, value, opts)).await;
        if r.is_err() {
            self.refund(key, charged);
        }
        Ok(r?)
    }

    /// `kv.incr`: add `delta` to the integer counter at `key` atomically and return the new value
    /// (`rt_store::KvBackend::incr`). Key limits and the storage quota apply as for [`Self::set`],
    /// charged for the largest counter text ([`MAX_COUNTER_BYTES`]). Codes: `kv_not_integer`,
    /// `kv_overflow`, `validation_failed`, `kv_key_too_large`, `storage_quota`.
    pub async fn incr(&self, key: &[u8], delta: i64, ttl: Option<Duration>) -> Result<i64, HostError> {
        self.check_key(key)?;
        let size = (key.len() + MAX_COUNTER_BYTES) as u64;
        let charged = self.reserve(key, size)?;
        let r = metered(&self.meter, self.backend.incr(key, delta, KvSetOptions { ttl })).await;
        if r.is_err() {
            self.refund(key, charged);
        }
        Ok(r?)
    }

    /// `kv.cas`: swap `next` in when the current value equals `expected` (`None`: absent or
    /// expired; `next` `None`: delete). Returns whether it swapped. The quota is charged for
    /// `next` and refunded unless the swap happened. Codes: `validation_failed`, `kv_key_too_large`,
    /// `kv_value_too_large`, `storage_quota`.
    pub async fn cas(
        &self,
        key: &[u8],
        expected: Option<KvValue>,
        next: Option<KvValue>,
        ttl: Option<Duration>,
    ) -> Result<bool, HostError> {
        self.check_key(key)?;
        if let Some(v) = &next {
            if v.data.len() > self.limits.max_kv_value {
                return Err(HostError::new(
                    "kv_value_too_large",
                    format!("value exceeds {} bytes", self.limits.max_kv_value),
                ));
            }
        }
        let size = next.as_ref().map(|v| (key.len() + v.data.len()) as u64);
        let charged = match size {
            Some(size) => self.reserve(key, size)?,
            None => 0,
        };
        let r = metered(&self.meter, self.backend.cas(key, expected, next, KvSetOptions { ttl })).await;
        if !matches!(r, Ok(true)) {
            self.refund(key, charged);
        }
        Ok(r?)
    }

    /// `kv.expire`: set (`Some`) or clear (`None`) the expiry of a live key; `false` when absent.
    pub async fn expire(&self, key: &[u8], ttl: Option<Duration>) -> Result<bool, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.expire(key, ttl)).await?)
    }

    /// `kv.ttl`: time left on a live key with an expiry; `None` when absent or without expiry.
    pub async fn ttl(&self, key: &[u8]) -> Result<Option<Duration>, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.ttl(key)).await?)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<bool, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.delete(key)).await?)
    }

    pub async fn list(&self, mut opts: KvListOptions) -> Result<KvPage, HostError> {
        if opts.prefix.len() > self.limits.max_kv_key {
            return Err(HostError::new(
                "kv_key_too_large",
                format!("prefix exceeds {} bytes", self.limits.max_kv_key),
            ));
        }
        opts.limit = opts.limit.clamp(1, rt_store::data::MAX_KV_LIST_LIMIT);
        Ok(metered(&self.meter, self.backend.list(opts)).await?)
    }
}

#[derive(Clone)]
pub struct SqlFacade {
    backend: Arc<dyn SqlBackend>,
    meter: Option<CpuMeter>,
}

impl SqlFacade {
    pub fn new(backend: Arc<dyn SqlBackend>) -> Self {
        Self { backend, meter: None }
    }

    /// Charge the CPU time of blocking store work to `meter` (the calling task's).
    pub fn with_cpu_meter(mut self, meter: CpuMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    pub async fn execute(&self, stmt: SqlStatement) -> Result<SqlResult, HostError> {
        Ok(metered(&self.meter, self.backend.execute(stmt)).await?)
    }

    pub async fn batch(&self, stmts: Vec<SqlStatement>, mode: BatchMode) -> Result<Vec<SqlResult>, HostError> {
        Ok(metered(&self.meter, self.backend.batch(stmts, mode)).await?)
    }

    /// Interactive transaction; aborted after 5 s.
    pub async fn begin(&self) -> Result<Box<dyn SqlTx>, HostError> {
        let tx = metered(&self.meter, self.backend.begin()).await?;
        Ok(Box::new(TimedTx {
            inner: tx,
            started: Instant::now(),
            meter: self.meter.clone(),
        }))
    }
}

/// Wraps a backend transaction: every operation is CPU-charged, and one that arrives after
/// [`SQL_TX_MAX`] rolls the transaction back and fails with `sql_timeout`.
struct TimedTx {
    inner: Box<dyn SqlTx>,
    started: Instant,
    meter: Option<CpuMeter>,
}

impl TimedTx {
    fn expired(&self) -> bool {
        self.started.elapsed() >= SQL_TX_MAX
    }
}

#[async_trait]
impl SqlTx for TimedTx {
    async fn execute(&mut self, stmt: SqlStatement) -> rt_store::Result<SqlResult> {
        if self.expired() {
            return Err(StoreError::SqlTimeout);
        }
        let meter = self.meter.clone();
        metered(&meter, self.inner.execute(stmt)).await
    }

    async fn commit(self: Box<Self>) -> rt_store::Result<()> {
        let TimedTx { inner, started, meter } = *self;
        if started.elapsed() >= SQL_TX_MAX {
            let _ = metered(&meter, inner.rollback()).await;
            return Err(StoreError::SqlTimeout);
        }
        metered(&meter, inner.commit()).await
    }

    async fn rollback(self: Box<Self>) -> rt_store::Result<()> {
        let TimedTx { inner, meter, .. } = *self;
        metered(&meter, inner.rollback()).await
    }
}

#[derive(Clone)]
pub struct BlobFacade {
    backend: Arc<dyn BlobBackend>,
    limits: FacadeLimits,
    meter: Option<CpuMeter>,
    left: Option<Arc<StorageLeft>>,
}

impl BlobFacade {
    pub fn new(backend: Arc<dyn BlobBackend>, limits: FacadeLimits) -> Self {
        Self {
            backend,
            limits,
            meter: None,
            left: None,
        }
    }

    /// Charge the CPU time of blocking store work to `meter` (the calling task's).
    pub fn with_cpu_meter(mut self, meter: CpuMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Bound each object by, and decrement, the task's remaining storage quota.
    pub fn with_storage_left(mut self, left: Arc<StorageLeft>) -> Self {
        self.left = Some(left);
        self
    }

    fn check_key(&self, key: &str) -> Result<(), HostError> {
        if key.is_empty() {
            return Err(HostError::new("validation_failed", "key must not be empty"));
        }
        if key.len() > self.limits.max_blob_key {
            return Err(HostError::new(
                "blob_key_too_large",
                format!("key exceeds {} bytes", self.limits.max_blob_key),
            ));
        }
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<BlobObject>, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.get(key)).await?)
    }

    pub async fn head(&self, key: &str) -> Result<Option<BlobMeta>, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.head(key)).await?)
    }

    pub async fn put(&self, key: &str, body: Body, content_type: Option<String>) -> Result<BlobMeta, HostError> {
        self.check_key(key)?;
        let left = match &self.left {
            Some(l) => l.get(),
            None => self.limits.storage_bytes_left,
        };
        let max_bytes = self.limits.max_blob_bytes.min(left.unwrap_or(u64::MAX));
        if body.exact_len().is_some_and(|len| len > max_bytes) {
            return Err(HostError::new("storage_quota", "storage quota exceeded"));
        }
        let meta = metered(&self.meter, self.backend.put(key, body, content_type, max_bytes)).await?;
        if let Some(l) = &self.left {
            l.charge(meta.size);
        }
        Ok(meta)
    }

    pub async fn delete(&self, key: &str) -> Result<bool, HostError> {
        self.check_key(key)?;
        Ok(metered(&self.meter, self.backend.delete(key)).await?)
    }

    pub async fn list(&self, mut opts: BlobListOptions) -> Result<BlobPage, HostError> {
        if opts.prefix.len() > self.limits.max_blob_key {
            return Err(HostError::new(
                "blob_key_too_large",
                format!("prefix exceeds {} bytes", self.limits.max_blob_key),
            ));
        }
        opts.limit = opts.limit.clamp(1, rt_store::data::MAX_KV_LIST_LIMIT);
        Ok(metered(&self.meter, self.backend.list(opts)).await?)
    }
}
