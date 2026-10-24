//! Per-service stats for `GET /v1/services/{s}/stats`.
//!
//! In-memory HDR histograms in 1-minute buckets over a 24 h ring. Hot-path recording is
//! thread-local per M; sysmon merges shards into the shared store every second so no shared
//! atomics sit on the request path (RP-06).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
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

pub trait StatsStore: Send + Sync {
    /// Record one finished task. Must be cheap and lock-free on the hot path.
    fn record(&self, service: ServiceId, timings: &TaskTimings, outcome: &Outcome);

    /// Aggregate the last `window`.
    fn window(&self, service: ServiceId, window: Window) -> ServiceStats;
}

const MINUTE_MS: i64 = 60_000;
/// The ring holds 24 h of 1-minute buckets.
const RING_MINUTES: i64 = 24 * 60;
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
    fn new() -> Self {
        Self {
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
}

impl HdrStatsStore {
    /// `now_ms` supplies wall time (virtual in tests).
    pub fn new(now_ms: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            now_ms,
            shards: Mutex::new(Vec::new()),
            services: DashMap::new(),
        })
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
        let shards: Vec<Arc<Mutex<Shard>>> = self.shards.lock().clone();
        for shard in &shards {
            let events = std::mem::take(&mut shard.lock().events);
            for e in events {
                let entry = self
                    .services
                    .entry(e.service)
                    .or_insert_with(|| Mutex::new(BTreeMap::new()));
                entry.lock().entry(e.minute).or_insert_with(Bucket::new).add(&e);
            }
        }
        // forget shards of dead threads (only the registry still holds them, and they are empty)
        self.shards
            .lock()
            .retain(|s| Arc::strong_count(s) > 1 || !s.lock().events.is_empty());
        // age out buckets beyond the 24 h ring
        let oldest = (self.now_ms)().div_euclid(MINUTE_MS) - (RING_MINUTES - 1);
        for svc in self.services.iter() {
            svc.lock().retain(|minute, _| *minute >= oldest);
        }
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
        for (_, b) in buckets.range(from..=now_minute) {
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
}
