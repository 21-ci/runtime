//! Tiered prepared cache:
//!
//! | Tier | Deno | WASM | Medium |
//! |---|---|---|---|
//! | T1 | module map + V8 code cache | `Component` (mmap'd `.cwasm`) + `InstancePre` | RAM (`t1_cache` share) |
//! | T2 | `cache/<engine-fp>/<digest>/…` | `…/component.cwasm` | local disk, MAC-verified |
//! | T3 | source artifact | `.wasm` artifact | artifact store |
//!
//! T2 → T1 is a **load** (blocking pool); T3 → T2 is a **compile** (a `Build` task on the build
//! Ps). Both are single-flight per [`PreparedKey`]; waiting tasks are `WaitingRuntime` and are
//! woken through `Scheduler::wake_runtime_waiters`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_mem::{MemCategory, MemoryGovernor, Owner, Reservation};
use rt_obs::Metrics;
use rt_sched::{Lookup, PreparedSource, Scheduler};
use rt_types::{BuildFailReason, BuildFailure, Digest, PreparedKey, RuntimeKind, TaskId, TaskSpec};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::policy::{EntryMeta, EvictionPolicy};
use crate::t2::{T2Entry, T2Store};

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// MAC failure: the T2 entry was deleted and will be rebuilt from T3.
    #[error("artifact failed MAC verification")]
    Tampered,
    /// Deserialization/instantiate failed for another reason (engine mismatch, corruption).
    #[error("artifact could not be loaded: {0}")]
    Corrupt(String),
}

/// Supplies T1 values. Implemented by the executors' glue (`rt-exec-deno`, `rt-exec-wasm`).
pub trait Loader: Send + Sync + 'static {
    type Value: Send + Sync + 'static;

    /// Load from T2 into a T1 value; returns `(value, accounted_bytes)`. `Ok(None)` = not on
    /// disk (needs a build). Runs on the blocking pool. Must verify the MAC first
    /// (`T2Store::read_verified` / `verify_path`).
    fn load_t2(&self, key: &PreparedKey) -> Result<Option<(Self::Value, u64)>, LoadError>;

    /// The `Build` task to submit when T2 misses (T3 → T2 → T1).
    fn build_spec(&self, key: &PreparedKey) -> TaskSpec;

    /// Estimated reload cost from the next tier, used by cost-aware admission/eviction.
    fn reload_cost_hint(&self, key: &PreparedKey) -> Option<Duration>;
}

/// Why entries were evicted (`rt_cache_evictions_total{reason}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictReason {
    Pressure,
    Ttl,
    /// T2 disk GC.
    Disk,
    Manual,
}

impl EvictReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            EvictReason::Pressure => "pressure",
            EvictReason::Ttl => "ttl",
            EvictReason::Disk => "disk",
            EvictReason::Manual => "manual",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictReport {
    pub evicted: u32,
    pub bytes_freed: u64,
}

/// Cache sizing and defaults.
#[derive(Clone, Debug)]
pub struct CacheSettings {
    /// T1 budget in bytes (`memory budget × cache.t1_share`).
    pub t1_bytes: u64,
    /// Default idle TTL (per-service TTL comes from the entry's pin/ttl hints).
    pub idle_ttl: Duration,
    /// `cache.t2_max_mb`.
    pub t2_max_bytes: u64,
    /// `disk.low_free_mb` / `disk.high_free_mb`.
    pub disk_low_free: u64,
    pub disk_high_free: u64,
}

impl CacheSettings {
    /// `balanced` defaults for a 1 GiB T1.
    pub fn balanced(t1_bytes: u64) -> Self {
        Self {
            t1_bytes,
            idle_ttl: Duration::from_secs(600),
            t2_max_bytes: 4096 * 1024 * 1024,
            disk_low_free: 2048 * 1024 * 1024,
            disk_high_free: 4096 * 1024 * 1024,
        }
    }
}

/// A snapshot of cache state for `GET /v1/node/cache`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CacheStats {
    pub t1_entries: u64,
    pub t1_bytes: u64,
    pub t2_entries: u64,
    pub t2_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub loads_inflight: u64,
    pub builds_inflight: u64,
    /// T1 inserts held without a memory reservation: the governor refused and nothing was
    /// evictable (`rt_cache_unreserved_inserts_total`).
    #[serde(default)]
    pub t1_unreserved_inserts: u64,
}

/// How one cache sees a T2 entry, for a disk GC over every cache sharing the T2 store
/// ([`PreparedCache::gc_disk_with`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum T2Use {
    /// Not a key of this cache.
    Unknown,
    /// A running task holds its T1 value (or an evicted-but-held copy): never deleted.
    InUse,
    /// Keep-score `freq × cost / size` (lower goes first) and the last-seen sequence (ties:
    /// least recently seen first).
    Scored { score: f64, last_seq: u64 },
}

/// One cache's knowledge of the shared T2 store (Deno and WASM caches share one).
pub trait T2Usage: Send + Sync {
    /// One [`T2Use`] per entry of `entries`, in order.
    fn t2_usage(&self, entries: &[T2Entry]) -> Vec<T2Use>;
}

const SNAPSHOTS_MAGIC: &[u8; 4] = b"RTFS";

/// Frequency sketches of several caches in one `meta.db` `cache_freq` blob (CCH-11):
/// `RTFS`, part count (u32 LE), then each part as length (u32 LE) + bytes.
pub fn pack_snapshots(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + parts.iter().map(|p| 4 + p.len()).sum::<usize>());
    out.extend_from_slice(SNAPSHOTS_MAGIC);
    out.extend_from_slice(&(parts.len() as u32).to_le_bytes());
    for p in parts {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// Inverse of [`pack_snapshots`]; an unknown, truncated or trailing-garbage blob yields no parts
/// (a cold start).
pub fn unpack_snapshots(blob: &[u8]) -> Vec<Vec<u8>> {
    fn take<'a>(b: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if b.len() < n {
            return None;
        }
        let (head, rest) = b.split_at(n);
        *b = rest;
        Some(head)
    }
    fn u32_of(b: &mut &[u8]) -> Option<usize> {
        take(b, 4).map(|h| u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize)
    }
    let parse = || {
        let mut b = blob;
        if take(&mut b, 4)? != SNAPSHOTS_MAGIC {
            return None;
        }
        let n = u32_of(&mut b)?;
        let mut parts = Vec::with_capacity(n.min(16));
        for _ in 0..n {
            let len = u32_of(&mut b)?;
            parts.push(take(&mut b, len)?.to_vec());
        }
        b.is_empty().then_some(parts)
    };
    parse().unwrap_or_default()
}

// ---- internals -------------------------------------------------------------------------------

/// Costs below this are treated as this value, so a free reload still has a non-zero score.
const COST_FLOOR: Duration = Duration::from_micros(50);
/// EWMA weight of a new reload-time measurement.
const COST_ALPHA: f64 = 0.3;
/// Upper bound of remembered keys (frequency/cost bookkeeping for non-resident keys).
const MAX_KNOWN: usize = 65_536;
/// A prune trims to this, not to `MAX_KNOWN`: the next prune is then ~16k new keys away, so a flood of
/// distinct keys costs one table scan per 16k lookups instead of one per lookup.
const PRUNE_TO: usize = MAX_KNOWN - MAX_KNOWN / 4;
const MAX_WAITING: usize = 100_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Seg {
    Window,
    Probation,
    Protected,
}

struct Entry<V> {
    value: Arc<V>,
    size: u64,
    cost: Duration,
    pinned: bool,
    last_access: Instant,
    seg: Seg,
    seq: u64,
    reservation: Option<Reservation>,
}

/// An evicted entry a running task still holds: its bytes stay accounted until the last handle
/// is gone (CCH-08).
struct Zombie<V> {
    key: PreparedKey,
    value: Weak<V>,
    _reservation: Option<Reservation>,
}

enum Flight {
    /// T2 → T1 load running on the blocking pool. `after_build`: this load follows a build, so a
    /// second failure is final. `rebuilt`: a build finished while this load was running.
    Loading { after_build: bool, rebuilt: bool },
    /// T3 → T2 build submitted (or waiting for a scheduler); `on_build_done` continues.
    Building,
}

struct Known {
    cost: Option<Duration>,
    last_seq: u64,
}

struct State<V> {
    entries: HashMap<PreparedKey, Entry<V>>,
    window: BTreeMap<u64, PreparedKey>,
    probation: BTreeMap<u64, PreparedKey>,
    protected: BTreeMap<u64, PreparedKey>,
    window_bytes: u64,
    probation_bytes: u64,
    protected_bytes: u64,
    seq: u64,
    inflight: HashMap<PreparedKey, Flight>,
    failed: HashMap<PreparedKey, BuildFailure>,
    pins: HashSet<PreparedKey>,
    known: HashMap<PreparedKey, Known>,
    /// Tasks currently waiting for a key (a re-poll of a waiter is not a new access).
    waiting: HashMap<PreparedKey, HashSet<TaskId>>,
    zombies: Vec<Zombie<V>>,
    policy: Box<dyn EvictionPolicy>,
}

/// One eviction, for bookkeeping by the caller.
struct Evicted {
    size: u64,
}

impl<V> State<V> {
    fn total_bytes(&self) -> u64 {
        self.window_bytes + self.probation_bytes + self.protected_bytes
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn segment(&mut self, seg: Seg) -> (&mut BTreeMap<u64, PreparedKey>, &mut u64) {
        match seg {
            Seg::Window => (&mut self.window, &mut self.window_bytes),
            Seg::Probation => (&mut self.probation, &mut self.probation_bytes),
            Seg::Protected => (&mut self.protected, &mut self.protected_bytes),
        }
    }

    /// Move `key` to the most-recent end of `to`.
    fn relink(&mut self, key: &PreparedKey, to: Seg) {
        let seq = self.next_seq();
        let Some(e) = self.entries.get_mut(key) else { return };
        let (from, old_seq, size) = (e.seg, e.seq, e.size);
        e.seg = to;
        e.seq = seq;
        let (map, bytes) = self.segment(from);
        map.remove(&old_seq);
        *bytes = bytes.saturating_sub(size);
        let (map, bytes) = self.segment(to);
        map.insert(seq, *key);
        *bytes += size;
    }

    fn reap(&mut self) {
        if !self.zombies.is_empty() {
            self.zombies.retain(|z| z.value.strong_count() > 0);
        }
    }

    /// Remove an entry from every structure; a handle still held elsewhere becomes a zombie.
    fn remove_entry(&mut self, key: &PreparedKey) -> Option<Evicted> {
        let e = self.entries.remove(key)?;
        let (map, bytes) = self.segment(e.seg);
        map.remove(&e.seq);
        *bytes = bytes.saturating_sub(e.size);
        let size = e.size;
        if Arc::strong_count(&e.value) > 1 {
            self.zombies.push(Zombie {
                key: *key,
                value: Arc::downgrade(&e.value),
                _reservation: e.reservation,
            });
        }
        Some(Evicted { size })
    }

    fn meta(&self, key: &PreparedKey, e: &Entry<V>) -> EntryMeta {
        EntryMeta {
            key_hash: key.hash64(),
            size: e.size,
            cost: e.cost.max(COST_FLOOR),
            pinned: e.pinned,
        }
    }

    /// The unpinned entry with the lowest `freq × cost / size` (ties: least recently used).
    fn lowest_score(&self) -> Option<PreparedKey> {
        let mut best: Option<(PreparedKey, EntryMeta, Instant)> = None;
        for (k, e) in &self.entries {
            if e.pinned {
                continue;
            }
            let m = self.meta(k, e);
            let replace = match &best {
                None => true,
                Some((_, bm, bt)) => {
                    if self.policy.admit(bm, &m) {
                        true // current best scores strictly higher than `m`
                    } else if self.policy.admit(&m, bm) {
                        false
                    } else {
                        e.last_access < *bt
                    }
                }
            };
            if replace {
                best = Some((*k, m, e.last_access));
            }
        }
        best.map(|(k, _, _)| k)
    }

    /// Unpinned LRU key of a segment.
    fn lru_of(&self, seg: Seg) -> Option<PreparedKey> {
        let map = match seg {
            Seg::Window => &self.window,
            Seg::Probation => &self.probation,
            Seg::Protected => &self.protected,
        };
        map.values()
            .find(|k| self.entries.get(*k).is_some_and(|e| !e.pinned))
            .copied()
    }

    fn mru_of(&self, seg: Seg) -> Option<PreparedKey> {
        let map = match seg {
            Seg::Window => &self.window,
            Seg::Probation => &self.probation,
            Seg::Protected => &self.protected,
        };
        map.values()
            .rev()
            .find(|k| self.entries.get(*k).is_some_and(|e| !e.pinned))
            .copied()
    }

    /// Restore the segment invariants after an insert or a promotion; returns what was evicted
    /// to stay within `cap` bytes.
    fn rebalance(&mut self, cap: u64) -> Vec<Evicted> {
        let window_cap = cap / 100;
        let protected_cap = (cap - window_cap) / 10 * 8;
        let mut out = Vec::new();

        // Window overflow flows into probation (the "candidates").
        while self.window_bytes > window_cap && self.window.len() > 1 {
            let Some((_, k)) = self.window.iter().next().map(|(s, k)| (*s, *k)) else {
                break;
            };
            self.relink(&k, Seg::Probation);
        }
        // Protected overflow is demoted to probation.
        while self.protected_bytes > protected_cap && self.protected.len() > 1 {
            let Some(k) = self.protected.values().next().copied() else {
                break;
            };
            self.relink(&k, Seg::Probation);
        }
        // Over budget: candidate (newest probation entry) fights the probation victim.
        while self.total_bytes() > cap && self.entries.len() > 1 {
            let victim = self.lru_of(Seg::Probation);
            let candidate = self.mru_of(Seg::Probation);
            let doomed = match (victim, candidate) {
                (Some(v), Some(c)) if v != c => {
                    let (vm, cm) = match (self.entries.get(&v), self.entries.get(&c)) {
                        (Some(ve), Some(ce)) => (self.meta(&v, ve), self.meta(&c, ce)),
                        _ => break, // LCOV_EXCL_LINE: victim and candidate come from the probation map, whose keys are always resident
                    };
                    if self.policy.admit(&cm, &vm) {
                        v
                    } else {
                        c
                    }
                }
                (Some(v), _) => v,
                (None, _) => match self.lru_of(Seg::Protected).or_else(|| self.lru_of(Seg::Window)) {
                    Some(k) => k,
                    None => break, // everything left is pinned
                },
            };
            if let Some(ev) = self.remove_entry(&doomed) {
                out.push(ev);
            }
        }
        out
    }

    /// Prune bookkeeping of keys that are neither resident, in flight, pinned nor warm.
    fn prune_known(&mut self) {
        if self.known.len() <= MAX_KNOWN {
            return;
        }
        let keep: Vec<PreparedKey> = self
            .known
            .keys()
            .filter(|k| {
                self.entries.contains_key(*k)
                    || self.inflight.contains_key(*k)
                    || self.pins.contains(*k)
                    || self.policy.frequency(k.hash64()) >= 2
            })
            .copied()
            .collect();
        let keep: HashSet<PreparedKey> = keep.into_iter().collect();
        self.known.retain(|k, _| keep.contains(k));
        if self.known.len() > PRUNE_TO {
            let drop: Vec<PreparedKey> = self
                .known
                .keys()
                .filter(|k| !self.entries.contains_key(*k))
                .take(self.known.len() - PRUNE_TO)
                .copied()
                .collect();
            for k in drop {
                self.known.remove(&k);
            }
        }
    }
}

fn spawn_job(job: impl FnOnce() + Send + 'static) {
    match tokio::runtime::Handle::try_current() {
        Ok(h) => {
            h.spawn_blocking(job);
        }
        Err(_) => {
            std::thread::spawn(job);
        }
    }
}

struct Inner<L: Loader> {
    loader: Arc<L>,
    settings: CacheSettings,
    mem: Arc<dyn MemoryGovernor>,
    metrics: Option<Arc<Metrics>>,
    state: Mutex<State<L::Value>>,
    sched: Mutex<Option<Scheduler>>,
    t2: Mutex<Option<Arc<T2Store>>>,
    /// Evictions by reason (`EvictReason as usize`).
    evictions: [AtomicU64; 4],
    hits: AtomicU64,
    misses: AtomicU64,
    unreserved: AtomicU64,
    /// This cache's share of the T1 gauges, so several caches add up instead of overwriting.
    gauge_entries: AtomicI64,
    gauge_bytes: AtomicI64,
}

fn reason_idx(r: EvictReason) -> usize {
    match r {
        EvictReason::Pressure => 0,
        EvictReason::Ttl => 1,
        EvictReason::Disk => 2,
        EvictReason::Manual => 3,
    }
}

impl<L: Loader> Inner<L> {
    fn count_evictions(&self, reason: EvictReason, n: u64) {
        if n == 0 {
            return;
        }
        self.evictions[reason_idx(reason)].fetch_add(n, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            let tier = if reason == EvictReason::Disk { "t2" } else { "t1" };
            m.cache_evictions_total
                .with_label_values(&[tier, reason.as_str()])
                .inc_by(n);
        }
    }

    /// Publish this cache's T1 size as a delta: the Deno and WASM caches share the gauges.
    /// Called with the state lock held, so updates of one cache are serialized.
    fn sync_gauges(&self, st: &State<L::Value>) {
        if let Some(m) = &self.metrics {
            let entries = st.entries.len() as i64;
            let bytes = st.total_bytes() as i64;
            let de = entries - self.gauge_entries.swap(entries, Ordering::Relaxed);
            let db = bytes - self.gauge_bytes.swap(bytes, Ordering::Relaxed);
            if de != 0 {
                m.cache_entries.with_label_values(&["t1"]).add(de);
            }
            if db != 0 {
                m.cache_bytes.with_label_values(&["t1"]).add(db);
            }
        }
    }

    fn count_unreserved(&self) {
        self.unreserved.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.cache_unreserved_inserts_total.inc();
        }
    }

    /// Insert a freshly loaded value into T1 (window segment) unless it is already resident.
    fn insert_loaded(&self, st: &mut State<L::Value>, key: PreparedKey, value: L::Value, size: u64, took: Duration) {
        // Reload cost: seeded with the loader's hint, then an EWMA of measurements.
        let hint = self.loader.reload_cost_hint(&key);
        let known = st.known.entry(key).or_insert(Known {
            cost: None,
            last_seq: 0,
        });
        known.cost = Some(match known.cost {
            None => hint.unwrap_or(took),
            Some(old) => old.mul_f64(1.0 - COST_ALPHA) + took.mul_f64(COST_ALPHA),
        });
        let cost = known.cost.unwrap_or(took);
        // A racing insert (preload vs. request) may already have stored this key.
        if !st.entries.contains_key(&key) {
            let reservation = self.reserve(st, size);
            if reservation.is_none() && size > 0 {
                self.count_unreserved();
            }
            let seq = st.next_seq();
            let pinned = st.pins.contains(&key);
            st.window.insert(seq, key);
            st.window_bytes += size;
            st.entries.insert(
                key,
                Entry {
                    value: Arc::new(value),
                    size,
                    cost,
                    pinned,
                    last_access: Instant::now(),
                    seg: Seg::Window,
                    seq,
                    reservation,
                },
            );
            let evicted = st.rebalance(self.settings.t1_bytes);
            self.count_evictions(EvictReason::Pressure, evicted.len() as u64);
        }
        st.failed.remove(&key);
    }

    fn wake(&self, key: &PreparedKey) {
        if let Some(s) = self.sched.lock().as_ref() {
            s.wake_runtime_waiters(key);
        }
    }

    fn spawn_load(self: &Arc<Self>, key: PreparedKey) {
        let me = self.clone();
        spawn_job(move || me.run_load(key));
    }

    fn run_load(self: &Arc<Self>, key: PreparedKey) {
        let started = Instant::now();
        let res = self.loader.load_t2(&key);
        self.finish_load(key, res, started.elapsed());
    }

    /// Reserve `size` bytes of T1 memory, shrinking the cache if the governor refuses.
    fn reserve(&self, st: &mut State<L::Value>, size: u64) -> Option<Reservation> {
        loop {
            match self.mem.reserve(MemCategory::T1Cache, size, Owner::NONE) {
                Ok(r) => return Some(r),
                Err(_) => {
                    let victim = st.lowest_score()?;
                    let n = st.remove_entry(&victim).map_or(0, |_| 1);
                    self.count_evictions(EvictReason::Pressure, n);
                }
            }
        }
    }

    fn finish_load(
        self: &Arc<Self>,
        key: PreparedKey,
        res: Result<Option<(L::Value, u64)>, LoadError>,
        took: Duration,
    ) {
        enum Next {
            Wake,
            Reload,
            Build,
            DropT2AndBuild,
        }
        let next = {
            let mut st = self.state.lock();
            let flight = st.inflight.remove(&key);
            let (after_build, rebuilt) = match flight {
                Some(Flight::Loading { after_build, rebuilt }) => (after_build, rebuilt),
                // Invalidated (or never started) while loading: discard the result.
                _ => return,
            };
            let failed_load = |st: &mut State<L::Value>, why: &str| {
                let f = BuildFailure::new(
                    BuildFailReason::Internal,
                    format!("prepared artifact unusable after build: {why}"),
                );
                st.failed.insert(key, f);
                Next::Wake
            };
            match res {
                Ok(Some((value, size))) => {
                    self.insert_loaded(&mut st, key, value, size, took);
                    // Waiters re-look-up now; their next lookup is a hit, counted as such.
                    st.waiting.remove(&key);
                    self.sync_gauges(&st);
                    Next::Wake
                }
                Ok(None) | Err(LoadError::Tampered) | Err(LoadError::Corrupt(_)) | Err(LoadError::Io(_)) => {
                    let corrupt = matches!(res_kind(&res), ResKind::Corrupt);
                    if rebuilt && !after_build {
                        st.inflight.insert(
                            key,
                            Flight::Loading {
                                after_build: true,
                                rebuilt: false,
                            },
                        );
                        Next::Reload
                    } else if after_build {
                        failed_load(&mut st, "load failed again")
                    } else {
                        st.inflight.insert(key, Flight::Building);
                        if corrupt {
                            Next::DropT2AndBuild
                        } else {
                            Next::Build
                        }
                    }
                }
            }
        };
        match next {
            Next::Wake => self.wake(&key),
            Next::Reload => self.spawn_load(key),
            Next::Build => self.submit_build(key),
            Next::DropT2AndBuild => {
                if let Some(t2) = self.t2.lock().clone() {
                    let _ = t2.remove(&key);
                }
                self.submit_build(key);
            }
        }
    }

    /// T3 → T2: submit the `Build` task (without a scheduler the caller drives `on_build_done`).
    fn submit_build(self: &Arc<Self>, key: PreparedKey) {
        let spec = self.loader.build_spec(&key);
        let rejected = match self.sched.lock().as_ref() {
            Some(sched) => sched.submit(spec).err(),
            None => return,
        };
        if let Some(reason) = rejected {
            tracing::warn!(?reason, digest = %key.digest, "build task rejected; the next request retries");
            self.state.lock().inflight.remove(&key);
            self.wake(&key);
        }
    }
}

enum ResKind {
    Corrupt,
    Other,
}

fn res_kind<V>(res: &Result<Option<(V, u64)>, LoadError>) -> ResKind {
    match res {
        Err(LoadError::Corrupt(_)) | Err(LoadError::Io(_)) => ResKind::Corrupt,
        _ => ResKind::Other,
    }
}

pub struct PreparedCache<L: Loader> {
    inner: Arc<Inner<L>>,
}

impl<L: Loader> PreparedCache<L> {
    /// `policy` is injected so tests can use a deterministic one. The cache starts without a
    /// scheduler; call [`PreparedCache::bind_scheduler`] once the scheduler exists (the scheduler
    /// itself needs this cache as its `PreparedSource`).
    pub fn new(
        loader: Arc<L>,
        settings: CacheSettings,
        policy: Box<dyn EvictionPolicy>,
        mem: Arc<dyn MemoryGovernor>,
        metrics: Option<Arc<Metrics>>,
    ) -> Arc<Self> {
        let state = State {
            entries: HashMap::new(),
            window: BTreeMap::new(),
            probation: BTreeMap::new(),
            protected: BTreeMap::new(),
            window_bytes: 0,
            probation_bytes: 0,
            protected_bytes: 0,
            seq: 0,
            inflight: HashMap::new(),
            failed: HashMap::new(),
            pins: HashSet::new(),
            known: HashMap::new(),
            waiting: HashMap::new(),
            zombies: Vec::new(),
            policy,
        };
        Arc::new(Self {
            inner: Arc::new(Inner {
                loader,
                settings,
                mem,
                metrics,
                state: Mutex::new(state),
                sched: Mutex::new(None),
                t2: Mutex::new(None),
                evictions: Default::default(),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                unreserved: AtomicU64::new(0),
                gauge_entries: AtomicI64::new(0),
                gauge_bytes: AtomicI64::new(0),
            }),
        })
    }

    /// Give the cache the on-disk T2 store for accounting (`stats().t2_*`) and disk GC
    /// ([`PreparedCache::gc_disk`], CCH-09). The [`Loader`] keeps its own handle for reads.
    pub fn attach_t2(&self, t2: Arc<T2Store>) {
        *self.inner.t2.lock() = Some(t2);
    }

    /// Total entries evicted so far for `reason` (mirrors `rt_cache_evictions_total{reason}`;
    /// lets tests assert the reason without a metrics registry, CCH-07, MEM-03).
    pub fn eviction_count(&self, reason: EvictReason) -> u64 {
        self.inner.evictions[reason_idx(reason)].load(Ordering::Relaxed)
    }

    /// Provide the scheduler used to submit `Build` tasks and to wake `WaitingRuntime` waiters.
    pub fn bind_scheduler(&self, scheduler: Scheduler) {
        *self.inner.sched.lock() = Some(scheduler);
    }

    /// Exempt (or re-include) an entry from eviction (`keep_resident`). The pin also applies to
    /// the entry when it is loaded later. The caller bounds the pinned total (`plan.max_pinned_mb`).
    pub fn pin(&self, key: &PreparedKey, pinned: bool) {
        let mut st = self.inner.state.lock();
        if pinned {
            st.pins.insert(*key);
        } else {
            st.pins.remove(key);
        }
        if let Some(e) = st.entries.get_mut(key) {
            e.pinned = pinned;
        }
    }

    /// Evict by score until T1 holds at most `target_bytes`.
    pub fn evict_to(&self, target_bytes: u64, reason: EvictReason) -> EvictReport {
        let mut report = EvictReport::default();
        {
            let mut st = self.inner.state.lock();
            st.reap();
            while st.total_bytes() > target_bytes {
                let Some(victim) = st.lowest_score() else { break };
                if let Some(ev) = st.remove_entry(&victim) {
                    report.evicted += 1;
                    report.bytes_freed += ev.size;
                }
            }
            self.inner.sync_gauges(&st);
        }
        self.inner.count_evictions(reason, u64::from(report.evicted));
        report
    }

    /// Evict entries untouched for longer than their idle TTL (no pressure needed; CCH-07).
    pub fn sweep_idle(&self, now: Instant) -> EvictReport {
        let ttl = self.inner.settings.idle_ttl;
        let mut report = EvictReport::default();
        {
            let mut st = self.inner.state.lock();
            st.reap();
            let idle: Vec<PreparedKey> = st
                .entries
                .iter()
                .filter(|(_, e)| !e.pinned && now.saturating_duration_since(e.last_access) > ttl)
                .map(|(k, _)| *k)
                .collect();
            for k in idle {
                if let Some(ev) = st.remove_entry(&k) {
                    report.evicted += 1;
                    report.bytes_freed += ev.size;
                }
            }
            self.inner.sync_gauges(&st);
        }
        self.inner.count_evictions(EvictReason::Ttl, u64::from(report.evicted));
        report
    }

    /// Load a freshly built artifact T2 → T1 now, on the calling (blocking) thread, without waking
    /// anyone. The build path calls this **before** it reports the version `ready`, so a version
    /// is only ever marked ready once its artifact loaded; the error is the failure to report
    /// instead. [`PreparedCache::on_build_done`] then releases the waiters (no second load).
    pub fn promote_built(&self, key: &PreparedKey) -> Result<(), BuildFailure> {
        let started = Instant::now();
        let res = self.inner.loader.load_t2(key);
        let took = started.elapsed();
        let why = match res {
            Ok(Some((value, size))) => {
                let mut st = self.inner.state.lock();
                self.inner.insert_loaded(&mut st, *key, value, size, took);
                self.inner.sync_gauges(&st);
                return Ok(());
            }
            Ok(None) => "not found in T2".to_string(),
            Err(e) => e.to_string(),
        };
        Err(BuildFailure::new(
            BuildFailReason::Internal,
            format!("prepared artifact unusable after build: {why}"),
        ))
    }

    /// Called by the `Build` task path when a build finishes: promote to T1 and wake waiters, or
    /// record the failure (negative cache) and wake waiters to fail with `build_failed`.
    pub fn on_build_done(&self, key: &PreparedKey, res: Result<(), BuildFailure>) {
        let mut respawn = false;
        let mut wake = false;
        {
            let mut st = self.inner.state.lock();
            match res {
                Ok(()) => {
                    if st.entries.contains_key(key) {
                        // Already resident (a racing load found the fresh T2 entry).
                        st.inflight.remove(key);
                        wake = true;
                    } else {
                        match st.inflight.get_mut(key) {
                            Some(Flight::Loading { rebuilt, .. }) => *rebuilt = true,
                            Some(Flight::Building) | None => {
                                st.inflight.insert(
                                    *key,
                                    Flight::Loading {
                                        after_build: true,
                                        rebuilt: false,
                                    },
                                );
                                respawn = true;
                            }
                        }
                    }
                }
                Err(f) => {
                    st.inflight.remove(key);
                    st.failed.insert(*key, f);
                    wake = true;
                }
            }
        }
        if respawn {
            self.inner.spawn_load(*key);
        }
        if wake {
            self.inner.wake(key);
        }
    }

    /// Build `key` now with `spec` (an eager deploy or `POST …/rebuild`) through the same
    /// single-flight as lazy builds: requests arriving meanwhile park on this build instead of
    /// starting their own, and a build of `key` that is already running is not started twice.
    ///
    /// `None`: a build of `key` is already in flight (it reports to every version waiting on the
    /// digest) or no scheduler is bound. `Some(Err)`: the scheduler refused the build.
    pub fn request_build(
        &self,
        key: &PreparedKey,
        spec: TaskSpec,
    ) -> Option<Result<rt_sched::TaskHandle, rt_types::RejectReason>> {
        let sched = self.inner.sched.lock().clone()?;
        {
            let mut st = self.inner.state.lock();
            if matches!(st.inflight.get(key), Some(Flight::Building)) {
                return None;
            }
            // A T2→T1 load in flight is superseded: its result is discarded and the waiters are
            // woken by `on_build_done` once the fresh artifact is loaded.
            st.inflight.insert(*key, Flight::Building);
        }
        let submitted = sched.submit(spec);
        if submitted.is_err() {
            self.inner.state.lock().inflight.remove(key);
            self.inner.wake(key);
        }
        Some(submitted)
    }

    /// `POST …/rebuild`: forget a negative-cache entry so the next task rebuilds.
    pub fn clear_failure(&self, key: &PreparedKey) {
        self.inner.state.lock().failed.remove(key);
    }

    /// Serialize the frequency sketch (persisted to `meta.db` every 5 min).
    pub fn persist_frequencies(&self) -> Vec<u8> {
        self.inner.state.lock().policy.snapshot()
    }

    /// Restore a persisted sketch at startup (CCH-11).
    pub fn restore_frequencies(&self, snapshot: &[u8]) {
        self.inner.state.lock().policy.restore(snapshot);
    }

    /// Load the given keys into T1 ahead of traffic (`min_latency` preload, CCH-10).
    pub fn preload(&self, keys: &[PreparedKey]) {
        for key in keys {
            {
                let mut st = self.inner.state.lock();
                if st.entries.contains_key(key) || st.inflight.contains_key(key) || st.failed.contains_key(key) {
                    continue;
                }
                let seq = st.next_seq();
                st.known.entry(*key).or_insert(Known {
                    cost: None,
                    last_seq: seq,
                });
                st.inflight.insert(
                    *key,
                    Flight::Loading {
                        after_build: false,
                        rebuilt: false,
                    },
                );
            }
            self.inner.spawn_load(*key);
        }
    }

    /// Preload the `n` most frequent of `candidates` (by the frequency sketch, so a restored
    /// sketch ranks by the frequencies before the restart; ties keep the candidates' order) and
    /// return them. Startup preload for services whose `cache.preload` is on (CCH-10).
    pub fn preload_top(&self, candidates: &[PreparedKey], n: usize) -> Vec<PreparedKey> {
        let chosen: Vec<PreparedKey> = {
            let st = self.inner.state.lock();
            let mut ranked: Vec<(u32, usize, PreparedKey)> = candidates
                .iter()
                .enumerate()
                .map(|(i, k)| (st.policy.frequency(k.hash64()), i, *k))
                .collect();
            ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut seen = HashSet::new();
            ranked
                .into_iter()
                .map(|(_, _, k)| k)
                .filter(|k| seen.insert(*k))
                .take(n)
                .collect()
        };
        self.preload(&chosen);
        chosen
    }

    /// Keys by descending frequency (for persisting the preload set).
    pub fn top_keys(&self, n: usize) -> Vec<PreparedKey> {
        let st = self.inner.state.lock();
        let mut all: Vec<(u32, u64, PreparedKey)> = st
            .known
            .iter()
            .map(|(k, v)| (st.policy.frequency(k.hash64()), v.last_seq, *k))
            .collect();
        all.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        all.into_iter().take(n).map(|(_, _, k)| k).collect()
    }

    pub fn contains(&self, key: &PreparedKey) -> bool {
        self.inner.state.lock().entries.contains_key(key)
    }

    /// Drop a key from every tier (version deleted / not retained).
    pub fn invalidate(&self, key: &PreparedKey) {
        let removed = {
            let mut st = self.inner.state.lock();
            st.inflight.remove(key);
            st.failed.remove(key);
            st.waiting.remove(key);
            let removed = st.remove_entry(key).is_some();
            st.reap();
            self.inner.sync_gauges(&st);
            removed
        };
        if removed {
            self.inner.count_evictions(EvictReason::Manual, 1);
        }
        if let Some(t2) = self.inner.t2.lock().clone() {
            let _ = t2.remove(key);
        }
    }

    pub fn stats(&self) -> CacheStats {
        let (t1_entries, t1_bytes, loads, builds) = {
            let mut st = self.inner.state.lock();
            st.reap();
            let loads = st
                .inflight
                .values()
                .filter(|f| matches!(f, Flight::Loading { .. }))
                .count();
            let builds = st.inflight.len() - loads;
            (st.entries.len() as u64, st.total_bytes(), loads as u64, builds as u64)
        };
        let (t2_entries, t2_bytes) = match self.inner.t2.lock().clone() {
            Some(t2) => {
                let list = t2.list().unwrap_or_default();
                (list.len() as u64, list.iter().map(|e| e.bytes).sum())
            }
            None => (0, 0),
        };
        CacheStats {
            t1_entries,
            t1_bytes,
            t2_entries,
            t2_bytes,
            hits: self.inner.hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            loads_inflight: loads,
            builds_inflight: builds,
            t1_unreserved_inserts: self.inner.unreserved.load(Ordering::Relaxed),
        }
    }

    /// Delete the lowest-score T2 entries until free disk space exceeds `disk_high_free`
    /// (CCH-09). Entries in use are kept. Only this cache's view of T2: when several caches share
    /// the store, run [`PreparedCache::gc_disk_with`] once instead.
    pub fn gc_disk(&self, free_bytes: u64) -> EvictReport {
        self.gc_disk_with(free_bytes, &[])
    }

    /// One disk GC pass over the T2 store this cache is attached to, which `others` share too
    /// (the Deno and WASM caches): an entry any cache uses is kept, the others are ordered by the
    /// score of the cache that knows them, and the bytes to free are computed once. Evictions
    /// are counted on this cache.
    pub fn gc_disk_with(&self, free_bytes: u64, others: &[&dyn T2Usage]) -> EvictReport {
        let mut report = EvictReport::default();
        let Some(t2) = self.inner.t2.lock().clone() else {
            return report;
        };
        let cfg = &self.inner.settings;
        let entries = t2.list().unwrap_or_default();
        let total: u64 = entries.iter().map(|e| e.bytes).sum();
        let below_low = free_bytes < cfg.disk_low_free;
        let need_for_disk = if below_low {
            cfg.disk_high_free.saturating_sub(free_bytes).saturating_add(1)
        } else {
            0
        };
        let need = need_for_disk.max(total.saturating_sub(cfg.t2_max_bytes));
        if need == 0 {
            return report;
        }

        // Every cache's view of every entry: in use anywhere = kept; otherwise the score of the
        // cache that knows it (an entry no cache knows scores 0 and goes first).
        let mut views = vec![self.t2_usage(&entries)];
        views.extend(others.iter().map(|o| o.t2_usage(&entries)));
        let mut cands: Vec<(f64, u64, T2Entry)> = Vec::with_capacity(entries.len());
        for (i, e) in entries.into_iter().enumerate() {
            let uses = views.iter().filter_map(|v| v.get(i).copied());
            let mut keep_score: Option<(f64, u64)> = None;
            let mut in_use = false;
            for u in uses {
                match u {
                    T2Use::InUse => in_use = true,
                    T2Use::Scored { score, last_seq } => {
                        if keep_score.is_none_or(|(s, _)| score > s) {
                            keep_score = Some((score, last_seq));
                        }
                    }
                    T2Use::Unknown => {}
                }
            }
            if !in_use {
                let (score, last_seq) = keep_score.unwrap_or((0.0, 0));
                cands.push((score, last_seq, e));
            }
        }
        // Lowest score first (ties: least recently seen).
        cands.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        for (_, _, e) in cands {
            if report.bytes_freed >= need {
                break;
            }
            let key = PreparedKey {
                digest: e.digest,
                kind: RuntimeKind::Deno,
                engine_fp: e.engine_fp,
            };
            if t2.remove(&key).is_ok() {
                report.evicted += 1;
                report.bytes_freed += e.bytes;
            }
        }
        self.inner.count_evictions(EvictReason::Disk, u64::from(report.evicted));
        report
    }
}

impl<L: Loader> T2Usage for PreparedCache<L> {
    fn t2_usage(&self, entries: &[T2Entry]) -> Vec<T2Use> {
        let mut st = self.inner.state.lock();
        st.reap();
        let mut by_entry: HashMap<(u64, Digest), Vec<PreparedKey>> = HashMap::new();
        for k in st.known.keys().chain(st.entries.keys()).chain(st.inflight.keys()) {
            by_entry.entry((k.engine_fp, k.digest)).or_default().push(*k);
        }
        let held: HashSet<PreparedKey> = st
            .zombies
            .iter()
            .filter(|z| z.value.strong_count() > 0)
            .map(|z| z.key)
            .collect();
        entries
            .iter()
            .map(|e| {
                let Some(keys) = by_entry.get(&(e.engine_fp, e.digest)) else {
                    return T2Use::Unknown;
                };
                // In use: a running task holds the T1 value (or an evicted-but-held zombie of it), or
                // the key is in flight (a build writing this entry, or a load / promotion reading
                // it): deleting it then would fail a build that just succeeded.
                let in_use = keys.iter().any(|k| {
                    held.contains(k)
                        || st.inflight.contains_key(k)
                        || st.entries.get(k).is_some_and(|en| Arc::strong_count(&en.value) > 1)
                });
                if in_use {
                    return T2Use::InUse;
                }
                let best = keys
                    .iter()
                    .map(|k| {
                        let cost = st
                            .known
                            .get(k)
                            .and_then(|kn| kn.cost)
                            .unwrap_or(COST_FLOOR)
                            .max(COST_FLOOR);
                        f64::from(st.policy.frequency(k.hash64())) * cost.as_secs_f64() / e.bytes.max(1) as f64
                    })
                    .fold(0.0, f64::max);
                let last_seq = keys
                    .iter()
                    .filter_map(|k| st.known.get(k))
                    .map(|k| k.last_seq)
                    .max()
                    .unwrap_or(0);
                T2Use::Scored { score: best, last_seq }
            })
            .collect()
    }
}

impl<L: Loader> PreparedSource<L::Value> for PreparedCache<L> {
    fn lookup(&self, key: &PreparedKey, waiter: TaskId) -> Lookup<L::Value> {
        let inner = &self.inner;
        let hash = key.hash64();
        let now = Instant::now();
        let mut start_load = false;
        let result = {
            let mut st = inner.state.lock();
            st.reap();
            let seq = st.next_seq();
            let st = &mut *st;
            let known = st.known.entry(*key).or_insert(Known {
                cost: None,
                last_seq: seq,
            });
            known.last_seq = seq;
            if st.known.len() > MAX_KNOWN {
                st.prune_known();
            }

            if st.entries.contains_key(key) {
                st.policy.record_access(hash);
                if let Some(w) = st.waiting.get_mut(key) {
                    w.remove(&waiter);
                    if w.is_empty() {
                        st.waiting.remove(key);
                    }
                }
                inner.hits.fetch_add(1, Ordering::Relaxed);
                if let Some(m) = &inner.metrics {
                    m.cache_hits_total.with_label_values(&["t1"]).inc();
                }
                // Touch: LRU / promotion bookkeeping.
                let seg = st.entries.get(key).map(|e| e.seg);
                if let Some(e) = st.entries.get_mut(key) {
                    e.last_access = now;
                }
                match seg {
                    Some(Seg::Probation) => {
                        st.relink(key, Seg::Protected);
                        let evicted = st.rebalance(inner.settings.t1_bytes);
                        inner.count_evictions(EvictReason::Pressure, evicted.len() as u64);
                    }
                    Some(s) => st.relink(key, s),
                    None => {}
                }
                st.entries
                    .get(key)
                    .map(|e| Lookup::Ready(e.value.clone()))
                    .unwrap_or(Lookup::Pending)
            } else if let Some(f) = st.failed.get(key).cloned() {
                st.policy.record_access(hash);
                if let Some(w) = st.waiting.get_mut(key) {
                    w.remove(&waiter);
                    if w.is_empty() {
                        st.waiting.remove(key);
                    }
                }
                Lookup::Failed(f)
            } else {
                if st.waiting.len() > MAX_WAITING {
                    st.waiting.clear();
                }
                let first_poll = st.waiting.entry(*key).or_default().insert(waiter);
                if first_poll {
                    st.policy.record_access(hash);
                    inner.misses.fetch_add(1, Ordering::Relaxed);
                    if let Some(m) = &inner.metrics {
                        m.cache_misses_total.with_label_values(&["t1"]).inc();
                    }
                }
                if !st.inflight.contains_key(key) {
                    st.inflight.insert(
                        *key,
                        Flight::Loading {
                            after_build: false,
                            rebuilt: false,
                        },
                    );
                    start_load = true;
                }
                Lookup::Pending
            }
        };
        if start_load {
            inner.spawn_load(*key);
        }
        result
    }

    fn is_ready(&self, key: &PreparedKey) -> bool {
        self.inner.state.lock().entries.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    //! Segment bookkeeping that the public API cannot reach on its own: a bare [`State`] lets each
    //! rule be set up exactly.
    use super::*;
    use crate::policy::WTinyLfu;

    fn key(n: u64) -> PreparedKey {
        PreparedKey {
            digest: rt_testkit::ids::digest(n),
            kind: RuntimeKind::Deno,
            engine_fp: 0xE1,
        }
    }

    fn state(cap: usize) -> State<u32> {
        State {
            entries: HashMap::new(),
            window: BTreeMap::new(),
            probation: BTreeMap::new(),
            protected: BTreeMap::new(),
            window_bytes: 0,
            probation_bytes: 0,
            protected_bytes: 0,
            seq: 0,
            inflight: HashMap::new(),
            failed: HashMap::new(),
            pins: HashSet::new(),
            known: HashMap::new(),
            waiting: HashMap::new(),
            zombies: Vec::new(),
            policy: Box::new(WTinyLfu::new(cap)),
        }
    }

    /// A resident entry of `size` bytes in `seg`, without a value reservation.
    fn put(st: &mut State<u32>, n: u64, size: u64, seg: Seg, pinned: bool) -> PreparedKey {
        let k = key(n);
        let seq = st.next_seq();
        let (map, bytes) = st.segment(seg);
        map.insert(seq, k);
        *bytes += size;
        let entry = Entry {
            value: Arc::new(0),
            size,
            cost: Duration::from_millis(1),
            pinned,
            last_access: Instant::now(),
            seg,
            seq,
            reservation: None,
        };
        st.entries.insert(k, entry);
        k
    }

    #[test]
    fn evict_reasons_keep_their_metric_labels() {
        assert_eq!(EvictReason::Pressure.as_str(), "pressure");
        assert_eq!(EvictReason::Ttl.as_str(), "ttl");
        assert_eq!(EvictReason::Disk.as_str(), "disk");
        assert_eq!(EvictReason::Manual.as_str(), "manual");
    }

    /// Oldest and newest unpinned entry of each segment; pinned entries are never candidates.
    #[test]
    fn lru_and_mru_skip_pinned_entries_in_every_segment() {
        let mut st = state(100);
        assert_eq!(st.lru_of(Seg::Window), None, "empty segment has no victim");
        let w1 = put(&mut st, 1, 1, Seg::Window, false);
        let w2 = put(&mut st, 2, 1, Seg::Window, false);
        put(&mut st, 3, 1, Seg::Protected, true);
        let p = put(&mut st, 4, 1, Seg::Protected, false);
        let pr = put(&mut st, 5, 1, Seg::Probation, false);
        assert_eq!(st.lru_of(Seg::Window), Some(w1));
        assert_eq!(st.mru_of(Seg::Window), Some(w2));
        assert_eq!(st.lru_of(Seg::Protected), Some(p));
        assert_eq!(st.mru_of(Seg::Protected), Some(p));
        assert_eq!(st.lru_of(Seg::Probation), Some(pr));
        assert_eq!(st.mru_of(Seg::Probation), Some(pr));
    }

    #[test]
    fn protected_overflow_demotes_the_oldest_protected_entry() {
        // cap 1000: window 10 bytes, protected at most 792 bytes.
        let mut st = state(1000);
        let old = put(&mut st, 1, 500, Seg::Protected, false);
        let young = put(&mut st, 2, 400, Seg::Protected, false);
        assert!(st.rebalance(1000).is_empty(), "900 bytes fit in the budget");
        assert_eq!(st.entries[&old].seg, Seg::Probation);
        assert_eq!(st.entries[&young].seg, Seg::Protected);
    }

    /// The lone window entry is not moved (a window of one), and with no probation candidate the
    /// oldest unpinned protected entry is the victim.
    #[test]
    fn over_budget_without_probation_victims_evicts_protected_then_window() {
        let mut st = state(100);
        let w = put(&mut st, 1, 90, Seg::Window, false);
        let p = put(&mut st, 2, 20, Seg::Protected, false);
        let evicted = st.rebalance(100);
        assert_eq!(evicted.iter().map(|e| e.size).collect::<Vec<_>>(), vec![20]);
        assert!(!st.entries.contains_key(&p));
        assert!(st.entries.contains_key(&w));
    }

    #[test]
    fn over_budget_falls_back_to_the_window_when_protected_is_all_pinned() {
        let mut st = state(100);
        let w = put(&mut st, 1, 90, Seg::Window, false);
        let p = put(&mut st, 2, 20, Seg::Protected, true);
        let evicted = st.rebalance(100);
        assert_eq!(evicted.iter().map(|e| e.size).collect::<Vec<_>>(), vec![90]);
        assert!(!st.entries.contains_key(&w));
        assert!(st.entries.contains_key(&p));
    }

    #[test]
    fn over_budget_with_only_pinned_entries_evicts_nothing() {
        let mut st = state(100);
        put(&mut st, 1, 90, Seg::Window, true);
        put(&mut st, 2, 20, Seg::Protected, true);
        assert!(st.rebalance(100).is_empty());
        assert_eq!(st.entries.len(), 2);
    }

    #[test]
    fn prune_known_is_a_no_op_under_the_bound() {
        let mut st = state(100);
        for i in 0..3 {
            st.known.insert(
                key(i),
                Known {
                    cost: None,
                    last_seq: 0,
                },
            );
        }
        st.prune_known();
        assert_eq!(st.known.len(), 3);
    }

    /// Over the bound, bookkeeping of keys that are neither resident, in flight, pinned nor hot is
    /// dropped first; pinned keys beyond the bound are then trimmed too.
    #[test]
    fn prune_known_bounds_the_bookkeeping_and_keeps_pinned_keys() {
        let mut st = state(100);
        let total = MAX_KNOWN as u64 + 2;
        for i in 0..total {
            st.known.insert(
                key(i),
                Known {
                    cost: None,
                    last_seq: 0,
                },
            );
            if i <= MAX_KNOWN as u64 {
                st.pins.insert(key(i));
            }
        }
        st.prune_known();
        assert_eq!(
            st.known.len(),
            PRUNE_TO,
            "trimmed below the bound, so the next prune is far away"
        );
    }
}
