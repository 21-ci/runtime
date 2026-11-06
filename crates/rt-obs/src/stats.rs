//! Per-service stats for `GET /v1/services/{s}/stats`.
//!
//! In-memory HDR histograms over a 24 h ring: 1-minute buckets for the last hour, 5-minute ones
//! to 6 h, 1-hour ones to 24 h. Hot-path recording is thread-local per M; sysmon merges shards
//! into the shared store every second, and compacts once a minute, so no shared atomics sit on
//! the request path (RP-06).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use rt_types::{Outcome, ServiceId, TaskTimings};
use serde::{Deserialize, Serialize};

/// Reporting window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Window {
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "1h")]
    H1,
    #[serde(rename = "24h")]
    H24,
}

impl Window {
    pub const fn duration(self) -> Duration {
        match self {
            Window::M5 => Duration::from_secs(5 * 60),
            Window::H1 => Duration::from_secs(60 * 60),
            Window::H24 => Duration::from_secs(24 * 60 * 60),
        }
    }

    /// Parse `5m|1h|24h`.
    pub fn parse(s: &str) -> Option<Window> {
        match s {
            "5m" => Some(Window::M5),
            "1h" => Some(Window::H1),
            "24h" => Some(Window::H24),
            _ => None,
        }
    }
}

/// p50/p95/p99 in milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Percentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceStats {
    pub count: u64,
    pub errors: u64,
    pub error_rate: f64,
    /// `ok`, `guest_error`, `cpu_limit`, … (see `Outcome::label`).
    pub outcomes: BTreeMap<String, u64>,
    pub queue: Percentiles,
    pub init: Percentiles,
    pub exec: Percentiles,
    pub total: Percentiles,
}

/// Resolution of `GET /v1/services/{s}/stats?step=`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesStep {
    M1,
    M5,
    H1,
}

impl SeriesStep {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "1m" => Some(Self::M1),
            "5m" => Some(Self::M5),
            "1h" => Some(Self::H1),
            _ => None,
        }
    }

    pub const fn minutes(self) -> i64 {
        match self {
            Self::M1 => 1,
            Self::M5 => 5,
            Self::H1 => 60,
        }
    }
}

/// One step of a service's series. `ts` is the step's start (ms); the p95s are milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeriesPoint {
    pub ts: i64,
    pub count: u64,
    pub errors: u64,
    pub queue_p95: f64,
    pub init_p95: f64,
    pub exec_p95: f64,
    pub total_p95: f64,
}

/// Plain counters of a range: read without merging a histogram.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceCounts {
    pub count: u64,
    pub errors: u64,
}

/// What the ring holds right now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingFootprint {
    pub services: usize,
    pub buckets: usize,
}

pub trait StatsStore: Send + Sync {
    /// Record one finished task. Must be cheap and lock-free on the hot path.
    fn record(&self, service: ServiceId, timings: &TaskTimings, outcome: &Outcome);

    /// Aggregate the last `window`.
    fn window(&self, service: ServiceId, window: Window) -> ServiceStats;

    /// Points with traffic in `[from_ms, to_ms)`, oldest first. A bucket coarser than `step`
    /// (older data, see the module doc) stays one point at its own start.
    fn series(&self, service: ServiceId, from_ms: i64, to_ms: i64, step: SeriesStep) -> Vec<SeriesPoint>;

    /// Counters of every service with traffic in `[from_ms, to_ms)`. O(active services): no
    /// histogram is merged.
    fn counts_all(&self, from_ms: i64, to_ms: i64) -> Vec<(ServiceId, ServiceCounts)>;

    /// Percentiles of the total time in `[from_ms, to_ms)` (merges histograms: not for ticks).
    fn total_percentiles(&self, service: ServiceId, from_ms: i64, to_ms: i64) -> Percentiles;
}

const MINUTE_MS: i64 = 60_000;
/// The ring covers 24 h.
const RING_MINUTES: i64 = 24 * 60;
/// 1-minute buckets are kept for this long.
const FINE_MINUTES: i64 = 60;
/// 5-minute buckets are kept for this long; older ones are 1-hour buckets.
const MEDIUM_MINUTES: i64 = 6 * 60;
const MEDIUM_SPAN: i64 = 5;
const COARSE_SPAN: i64 = 60;
/// Two significant digits (<= 1 % error): comfortably inside the 5 % of OBS-08, and small.
const SIGFIG: u8 = 2;

/// One recorded task, as kept in a thread-local shard until the next merge.
struct Event {
    service: ServiceId,
    /// Minute (`ts_ms / 60_000`) at record time.
    minute: i64,
    queue_us: u64,
    init_us: u64,
    exec_us: u64,
    total_us: u64,
    outcome: &'static str,
    error: bool,
}

#[derive(Default)]
struct Shard {
    events: Vec<Event>,
}

struct Bucket {
    /// Minutes covered from the bucket's key: 1, 5 or 60.
    span: i64,
    count: u64,
    errors: u64,
    outcomes: BTreeMap<&'static str, u64>,
    queue: Histogram<u32>,
    init: Histogram<u32>,
    exec: Histogram<u32>,
    total: Histogram<u32>,
}

fn new_hist() -> Histogram<u32> {
    // auto-resizing: memory follows the observed range
    Histogram::new(SIGFIG).expect("valid significant figures")
}

impl Bucket {
    fn new(span: i64) -> Self {
        Self {
            span,
            count: 0,
            errors: 0,
            outcomes: BTreeMap::new(),
            queue: new_hist(),
            init: new_hist(),
            exec: new_hist(),
            total: new_hist(),
        }
    }

    fn add(&mut self, e: &Event) {
        self.count += 1;
        self.errors += u64::from(e.error);
        *self.outcomes.entry(e.outcome).or_insert(0) += 1;
        for (h, v) in [
            (&mut self.queue, e.queue_us),
            (&mut self.init, e.init_us),
            (&mut self.exec, e.exec_us),
            (&mut self.total, e.total_us),
        ] {
            // auto-resize makes recording infallible short of absurd values
            let _ = h.record(v);
        }
    }

    /// Take `other`'s records in.
    fn absorb(&mut self, other: &Bucket) {
        self.count += other.count;
        self.errors += other.errors;
        for (label, n) in &other.outcomes {
            *self.outcomes.entry(label).or_insert(0) += n;
        }
        let _ = (
            self.queue.add(&other.queue),
            self.init.add(&other.init),
            self.exec.add(&other.exec),
            self.total.add(&other.total),
        );
    }

    fn overlaps(&self, key: i64, from: i64) -> bool {
        key + self.span > from
    }
}

/// `(key, span)` of the bucket holding `minute` at `now`. A slot compacts only once it lies wholly
/// past the younger tier, so the 1-hour window stays exact.
fn slot_of(minute: i64, now: i64) -> (i64, i64) {
    let aligned = |span: i64| minute.div_euclid(span) * span;
    let hour = aligned(COARSE_SPAN);
    if hour + COARSE_SPAN <= now - (MEDIUM_MINUTES - 1) {
        return (hour, COARSE_SPAN);
    }
    let medium = aligned(MEDIUM_SPAN);
    if medium + MEDIUM_SPAN <= now - (FINE_MINUTES - 1) {
        return (medium, MEDIUM_SPAN);
    }
    (minute, 1)
}

/// Move buckets to the tier their age calls for and drop those past the ring. Runs on sysmon.
fn compact(buckets: &mut ServiceBuckets, now: i64) {
    let moved: Vec<(i64, i64, i64)> = buckets
        .iter()
        .filter_map(|(k, b)| {
            let (key, span) = slot_of(*k, now);
            (span > b.span).then_some((*k, key, span))
        })
        .collect();
    for (old, key, span) in moved {
        let Some(mut b) = buckets.remove(&old) else { continue };
        match buckets.get_mut(&key) {
            Some(t) => {
                t.span = t.span.max(span);
                t.absorb(&b);
            }
            None => {
                b.span = span;
                buckets.insert(key, b);
            }
        }
    }
}

/// Buckets that may overlap `from..=to` (minutes), by key.
fn in_range(buckets: &ServiceBuckets, from: i64, to: i64) -> impl Iterator<Item = (&i64, &Bucket)> {
    buckets
        .range((from - (COARSE_SPAN - 1))..=to)
        .filter(move |(k, b)| b.overlaps(**k, from))
}

/// `[from_ms, to_ms)` as an inclusive range of minutes.
fn minute_range(from_ms: i64, to_ms: i64) -> (i64, i64) {
    (from_ms.div_euclid(MINUTE_MS), (to_ms - 1).div_euclid(MINUTE_MS))
}

type ServiceBuckets = BTreeMap<i64, Bucket>;

static NEXT_STORE_ID: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    /// This thread's shard per store (keyed by store id), so the hot path touches no shared state
    /// except an uncontended per-shard mutex.
    static LOCAL_SHARDS: RefCell<HashMap<usize, Arc<Mutex<Shard>>>> = RefCell::new(HashMap::new());
}

/// HDR-histogram backed [`StatsStore`].
pub struct HdrStatsStore {
    id: usize,
    now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Every shard ever created. A shard outlives its thread here, so records of exited threads
    /// are still merged.
    shards: Mutex<Vec<Arc<Mutex<Shard>>>>,
    services: DashMap<ServiceId, Mutex<ServiceBuckets>>,
    compaction: bool,
    /// Minute of the last compaction: the walk over all services runs once per minute.
    last_compact: AtomicI64,
}

impl HdrStatsStore {
    /// `now_ms` supplies wall time (virtual in tests).
    pub fn new(now_ms: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        Self::build(now_ms, true)
    }

    /// 1-minute buckets for the whole ring: only the baseline of the memory measurement.
    #[doc(hidden)]
    pub fn without_compaction(now_ms: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        Self::build(now_ms, false)
    }

    fn build(now_ms: Arc<dyn Fn() -> i64 + Send + Sync>, compaction: bool) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            now_ms,
            shards: Mutex::new(Vec::new()),
            services: DashMap::new(),
            compaction,
            last_compact: AtomicI64::new(i64::MIN),
        })
    }

    /// Services and buckets the ring holds.
    pub fn footprint(&self) -> RingFootprint {
        let mut fp = RingFootprint::default();
        for svc in self.services.iter() {
            fp.services += 1;
            fp.buckets += svc.lock().len();
        }
        fp
    }

    fn local_shard(&self) -> Arc<Mutex<Shard>> {
        LOCAL_SHARDS.with(|m| {
            m.borrow_mut()
                .entry(self.id)
                .or_insert_with(|| {
                    let shard = Arc::new(Mutex::new(Shard::default()));
                    self.shards.lock().push(shard.clone());
                    shard
                })
                .clone()
        })
    }

    /// Merge all thread-local shards into the shared buckets (sysmon, every 1 s).
    pub fn merge_shards(&self) {
        let now = (self.now_ms)().div_euclid(MINUTE_MS);
        let shards: Vec<Arc<Mutex<Shard>>> = self.shards.lock().clone();
        for shard in &shards {
            let events = std::mem::take(&mut shard.lock().events);
            for e in events {
                let entry = self
                    .services
                    .entry(e.service)
                    .or_insert_with(|| Mutex::new(BTreeMap::new()));
                let (key, span) = if self.compaction {
                    slot_of(e.minute, now)
                } else {
                    (e.minute, 1)
                };
                let mut buckets = entry.lock();
                let bucket = buckets.entry(key).or_insert_with(|| Bucket::new(span));
                bucket.span = bucket.span.max(span);
                bucket.add(&e);
            }
        }
        // forget shards of dead threads (only the registry still holds them, and they are empty)
        self.shards
            .lock()
            .retain(|s| Arc::strong_count(s) > 1 || !s.lock().events.is_empty());
        if self.last_compact.swap(now, Ordering::Relaxed) == now {
            return;
        }
        // age out buckets beyond the 24 h ring, then services left without any
        let oldest = now - (RING_MINUTES - 1);
        for svc in self.services.iter() {
            let mut buckets = svc.lock();
            if self.compaction {
                compact(&mut buckets, now);
            }
            buckets.retain(|key, b| b.overlaps(*key, oldest));
        }
        self.services.retain(|_, b| !b.get_mut().is_empty());
    }
}

fn percentiles(h: &Histogram<u32>) -> Percentiles {
    if h.is_empty() {
        return Percentiles::default();
    }
    let ms = |q: f64| h.value_at_quantile(q) as f64 / 1000.0;
    Percentiles {
        p50_ms: ms(0.50),
        p95_ms: ms(0.95),
        p99_ms: ms(0.99),
    }
}

fn point(slot_minute: i64, b: &Bucket) -> SeriesPoint {
    SeriesPoint {
        ts: slot_minute * MINUTE_MS,
        count: b.count,
        errors: b.errors,
        queue_p95: percentiles(&b.queue).p95_ms,
        init_p95: percentiles(&b.init).p95_ms,
        exec_p95: percentiles(&b.exec).p95_ms,
        total_p95: percentiles(&b.total).p95_ms,
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

impl StatsStore for HdrStatsStore {
    fn record(&self, service: ServiceId, timings: &TaskTimings, outcome: &Outcome) {
        let queue_us = micros(timings.queue);
        let init_us = micros(timings.init);
        let exec_us = micros(timings.exec);
        let total_us = queue_us
            .saturating_add(micros(timings.wait_runtime))
            .saturating_add(init_us)
            .saturating_add(exec_us);
        let event = Event {
            service,
            minute: (self.now_ms)().div_euclid(MINUTE_MS),
            queue_us,
            init_us,
            exec_us,
            total_us,
            outcome: outcome.label(),
            error: !outcome.is_ok(),
        };
        self.local_shard().lock().events.push(event);
    }

    fn window(&self, service: ServiceId, window: Window) -> ServiceStats {
        let minutes = (window.duration().as_secs() / 60) as i64;
        let now_minute = (self.now_ms)().div_euclid(MINUTE_MS);
        let from = now_minute - (minutes - 1);
        let Some(entry) = self.services.get(&service) else {
            return ServiceStats::default();
        };
        let buckets = entry.lock();
        let (mut queue, mut init, mut exec, mut total) = (new_hist(), new_hist(), new_hist(), new_hist());
        let mut stats = ServiceStats::default();
        for (_, b) in in_range(&buckets, from, now_minute) {
            stats.count += b.count;
            stats.errors += b.errors;
            for (label, n) in &b.outcomes {
                *stats.outcomes.entry((*label).to_string()).or_insert(0) += n;
            }
            let _ = (
                queue.add(&b.queue),
                init.add(&b.init),
                exec.add(&b.exec),
                total.add(&b.total),
            );
        }
        if stats.count == 0 {
            return ServiceStats::default();
        }
        stats.error_rate = stats.errors as f64 / stats.count as f64;
        stats.queue = percentiles(&queue);
        stats.init = percentiles(&init);
        stats.exec = percentiles(&exec);
        stats.total = percentiles(&total);
        stats
    }

    fn series(&self, service: ServiceId, from_ms: i64, to_ms: i64, step: SeriesStep) -> Vec<SeriesPoint> {
        let Some(entry) = self.services.get(&service) else {
            return Vec::new();
        };
        let (from, to) = minute_range(from_ms, to_ms);
        let width = step.minutes();
        let buckets = entry.lock();
        let mut out = Vec::new();
        let mut group: Option<(i64, Bucket)> = None;
        for (key, b) in in_range(&buckets, from, to) {
            let slot = key.div_euclid(width) * width;
            match &mut group {
                Some((s, acc)) if *s == slot => acc.absorb(b),
                _ => {
                    if let Some((s, acc)) = group.take() {
                        out.push(point(s, &acc));
                    }
                    let mut acc = Bucket::new(width);
                    acc.absorb(b);
                    group = Some((slot, acc));
                }
            }
        }
        if let Some((s, acc)) = group {
            out.push(point(s, &acc));
        }
        out
    }

    fn counts_all(&self, from_ms: i64, to_ms: i64) -> Vec<(ServiceId, ServiceCounts)> {
        let (from, to) = minute_range(from_ms, to_ms);
        self.services
            .iter()
            .filter_map(|svc| {
                let mut c = ServiceCounts::default();
                for (_, b) in in_range(&svc.lock(), from, to) {
                    c.count += b.count;
                    c.errors += b.errors;
                }
                (c.count > 0).then_some((*svc.key(), c))
            })
            .collect()
    }

    fn total_percentiles(&self, service: ServiceId, from_ms: i64, to_ms: i64) -> Percentiles {
        let Some(entry) = self.services.get(&service) else {
            return Percentiles::default();
        };
        let (from, to) = minute_range(from_ms, to_ms);
        let mut total = new_hist();
        for (_, b) in in_range(&entry.lock(), from, to) {
            let _ = total.add(&b.total);
        }
        percentiles(&total)
    }
}
