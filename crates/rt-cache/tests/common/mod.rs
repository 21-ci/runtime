//! Shared helpers for the rt-cache integration tests: a counting fake [`Loader`] with
//! configurable cost/size, cache construction, and polling helpers.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use rt_cache::{
    CacheSettings, LoadError, Loader, Lookup, NoTamperSink, PreparedCache, PreparedSource, T2Error, T2Store, WTinyLfu,
};
use rt_sec::{HkdfArtifactMac, MasterKey};
use rt_testkit::{ids, SpecBuilder, TestGovernor};
use rt_types::{PreparedKey, RuntimeKind, TaskId, TaskSpec, Trigger};

/// Name of the file the disk-backed fake loader reads from T2.
pub const BUNDLE: &str = "bundle";

/// The T1 value: remembers its payload and reports when it is dropped.
pub struct Prepared {
    pub key: PreparedKey,
    pub payload: Bytes,
    drops: Arc<AtomicUsize>,
}

impl Drop for Prepared {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl fmt::Debug for Prepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Prepared")
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// A [`Loader`] that counts every `load_t2` call. Load time equals the configured cost (it sleeps on
/// the blocking pool), so measured reload cost and the reported hint agree.
pub struct FakeLoader {
    disk: Option<Arc<T2Store>>,
    default_size: u64,
    default_cost: Duration,
    sizes: Mutex<HashMap<PreparedKey, u64>>,
    costs: Mutex<HashMap<PreparedKey, Duration>>,
    missing: Mutex<HashSet<PreparedKey>>,
    loads: AtomicUsize,
    per_key: Mutex<HashMap<PreparedKey, usize>>,
    created: AtomicUsize,
    build_specs: AtomicUsize,
    drops: Arc<AtomicUsize>,
    /// Injected failures: the next `.0` loads fail with `.1`.
    failures: Mutex<(usize, FailKind)>,
}

/// How an injected load failure presents itself.
#[derive(Clone, Copy)]
pub enum FailKind {
    Tampered,
    Corrupt,
    Io,
}

impl FakeLoader {
    /// In-memory source: every key is "on disk" unless marked missing; value size/cost are configurable.
    pub fn memory(default_size: u64, default_cost: Duration) -> Arc<Self> {
        Arc::new(Self::new(None, default_size, default_cost))
    }

    /// Reads (and MAC-verifies) `BUNDLE` from the given T2 store; accounted size = file length.
    pub fn with_disk(t2: Arc<T2Store>, default_cost: Duration) -> Arc<Self> {
        Arc::new(Self::new(Some(t2), 0, default_cost))
    }

    fn new(disk: Option<Arc<T2Store>>, default_size: u64, default_cost: Duration) -> Self {
        Self {
            disk,
            default_size,
            default_cost,
            sizes: Mutex::default(),
            costs: Mutex::default(),
            missing: Mutex::default(),
            loads: AtomicUsize::new(0),
            per_key: Mutex::default(),
            created: AtomicUsize::new(0),
            build_specs: AtomicUsize::new(0),
            drops: Arc::new(AtomicUsize::new(0)),
            failures: Mutex::new((0, FailKind::Io)),
        }
    }

    /// The next `n` loads (after their delay) fail with `kind`.
    pub fn fail_next(&self, n: usize, kind: FailKind) {
        *self.failures.lock() = (n, kind);
    }

    pub fn set_size(&self, key: &PreparedKey, size: u64) {
        self.sizes.lock().insert(*key, size);
    }

    pub fn set_cost(&self, key: &PreparedKey, cost: Duration) {
        self.costs.lock().insert(*key, cost);
    }

    pub fn mark_missing(&self, key: &PreparedKey) {
        self.missing.lock().insert(*key);
    }

    pub fn mark_present(&self, key: &PreparedKey) {
        self.missing.lock().remove(key);
    }

    /// Total `load_t2` invocations (including failed ones).
    pub fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }

    pub fn loads_for(&self, key: &PreparedKey) -> usize {
        self.per_key.lock().get(key).copied().unwrap_or(0)
    }

    /// Values successfully constructed (a rejected/tampered artifact never becomes one).
    pub fn created(&self) -> usize {
        self.created.load(Ordering::SeqCst)
    }

    /// Values dropped so far (the last `Arc` handle went away).
    pub fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }

    pub fn build_specs(&self) -> usize {
        self.build_specs.load(Ordering::SeqCst)
    }

    fn cost_of(&self, key: &PreparedKey) -> Duration {
        self.costs.lock().get(key).copied().unwrap_or(self.default_cost)
    }
}

impl Loader for FakeLoader {
    type Value = Prepared;

    fn load_t2(&self, key: &PreparedKey) -> Result<Option<(Prepared, u64)>, LoadError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        *self.per_key.lock().entry(*key).or_default() += 1;
        let delay = self.cost_of(key);
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        {
            let mut failures = self.failures.lock();
            if failures.0 > 0 {
                failures.0 -= 1;
                return Err(match failures.1 {
                    FailKind::Tampered => LoadError::Tampered,
                    FailKind::Corrupt => LoadError::Corrupt("injected".into()),
                    FailKind::Io => LoadError::Io(std::io::Error::other("injected")),
                });
            }
        }
        let (payload, size) = match &self.disk {
            Some(t2) => match t2.read_verified(key, BUNDLE) {
                Ok(Some(bytes)) => {
                    let len = bytes.len() as u64;
                    (bytes, len)
                }
                Ok(None) => return Ok(None),
                Err(T2Error::Tampered) => return Err(LoadError::Tampered),
                Err(T2Error::Io(e)) => return Err(LoadError::Io(e)),
            },
            None => {
                if self.missing.lock().contains(key) {
                    return Ok(None);
                }
                let size = self.sizes.lock().get(key).copied().unwrap_or(self.default_size);
                (Bytes::from(format!("payload:{}", key.digest.hex())), size)
            }
        };
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(Some((
            Prepared {
                key: *key,
                payload,
                drops: self.drops.clone(),
            },
            size,
        )))
    }

    fn build_spec(&self, key: &PreparedKey) -> TaskSpec {
        self.build_specs.fetch_add(1, Ordering::SeqCst);
        SpecBuilder::build().trigger(Trigger::Build { key: *key }).finish()
    }

    fn reload_cost_hint(&self, key: &PreparedKey) -> Option<Duration> {
        Some(self.cost_of(key))
    }
}

/// A cache wired to a [`FakeLoader`] and a generous [`TestGovernor`].
pub struct Rig {
    pub cache: Arc<PreparedCache<FakeLoader>>,
    pub loader: Arc<FakeLoader>,
    pub mem: Arc<TestGovernor>,
}

pub fn settings(t1_bytes: u64, idle_ttl: Duration) -> CacheSettings {
    let mut s = CacheSettings::balanced(t1_bytes);
    s.idle_ttl = idle_ttl;
    s
}

pub fn rig(loader: Arc<FakeLoader>, t1_bytes: u64, idle_ttl: Duration, capacity_entries: usize) -> Rig {
    rig_with(loader, settings(t1_bytes, idle_ttl), capacity_entries)
}

pub fn rig_with(loader: Arc<FakeLoader>, settings: CacheSettings, capacity_entries: usize) -> Rig {
    let mem = TestGovernor::with_mb(1024);
    let cache = PreparedCache::new(
        loader.clone(),
        settings,
        Box::new(WTinyLfu::new(capacity_entries)),
        mem.clone(),
        None,
    );
    Rig { cache, loader, mem }
}

pub fn key(n: u64) -> PreparedKey {
    PreparedKey {
        digest: ids::digest(n),
        kind: RuntimeKind::Deno,
        engine_fp: 0xE1,
    }
}

/// A T2 store under `root` signed with a fixed test master key.
pub fn t2_store(root: &std::path::Path, master: u8, sink: Arc<dyn rt_cache::TamperSink>) -> Arc<T2Store> {
    Arc::new(T2Store::new(
        root,
        Arc::new(HkdfArtifactMac::new(&MasterKey::from_bytes([master; 32]))),
        sink,
    ))
}

pub fn t2_store_quiet(root: &std::path::Path) -> Arc<T2Store> {
    t2_store(root, 9, Arc::new(NoTamperSink))
}

/// Poll `cond` every 2 ms for up to 10 s.
pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// `lookup` repeatedly (the way a `WaitingRuntime` task is re-polled) until the value is ready.
pub async fn get(cache: &PreparedCache<FakeLoader>, key: &PreparedKey, task: u64) -> Arc<Prepared> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match cache.lookup(key, TaskId(task)) {
            Lookup::Ready(v) => return v,
            Lookup::Pending => {}
            Lookup::Failed(f) => panic!("lookup failed: {f}"),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {key:?} to become ready"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// One counted `lookup`, then wait for readiness through `is_ready` (which must not count as an access).
pub async fn request_then_wait_ready(cache: &PreparedCache<FakeLoader>, key: &PreparedKey) {
    match cache.lookup(key, TaskId(1)) {
        Lookup::Ready(_) => return,
        Lookup::Pending => {}
        Lookup::Failed(f) => panic!("lookup failed: {f}"),
    }
    eventually("entry to become ready", || cache.is_ready(key)).await;
}

/// A lookup that must be a T1 hit right now.
pub fn hit(cache: &PreparedCache<FakeLoader>, key: &PreparedKey) -> Arc<Prepared> {
    match cache.lookup(key, TaskId(1)) {
        Lookup::Ready(v) => v,
        other => panic!("expected a T1 hit for {key:?}, got {other:?}"),
    }
}
