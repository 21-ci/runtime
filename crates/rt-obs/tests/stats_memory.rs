//! Heap held by a day of stats per service, ring compaction off and on (`docs/design/platform-plan.md` R2).
//! Run: `cargo test -p rt-obs --release --test stats_memory -- --ignored --nocapture`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_obs::{HdrStatsStore, StatsStore};
use rt_types::{Outcome, Reply, ServiceId, TaskTimings};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to `System`, only counting the sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Ordering::Relaxed);
        // SAFETY: same layout contract as the caller's.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        // SAFETY: same pointer and layout the caller got from `alloc`.
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const MINUTE: i64 = 60_000;

/// 24 h of `per_minute` requests with latencies spread around 50 ms; returns heap bytes held and
/// the slowest `merge_shards`.
fn day(compact: bool, per_minute: u64) -> (usize, usize, Duration) {
    let now = Arc::new(AtomicI64::new(1_760_000_000_000 / MINUTE * MINUTE));
    let clock = now.clone();
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || clock.load(Ordering::SeqCst));
    let before = LIVE.load(Ordering::SeqCst);
    let store = if compact {
        HdrStatsStore::new(clock)
    } else {
        HdrStatsStore::without_compaction(clock)
    };
    let svc = ServiceId::new();
    let mut slowest = Duration::ZERO;
    for m in 0..24 * 60u64 {
        for i in 0..per_minute {
            let t = TaskTimings {
                queue: Duration::from_micros(50 + i % 500),
                init: Duration::from_micros(200 + i % 3_000),
                exec: Duration::from_micros(20_000 + (m * 37 + i * 613) % 80_000),
                ..TaskTimings::default()
            };
            store.record(svc, &t, &Outcome::Ok(Reply::ok()));
        }
        let t0 = Instant::now();
        store.merge_shards();
        slowest = slowest.max(t0.elapsed());
        now.fetch_add(MINUTE, Ordering::SeqCst);
    }
    store.merge_shards();
    let bytes = LIVE.load(Ordering::SeqCst).saturating_sub(before);
    let buckets = store.footprint().buckets;
    drop(store);
    (bytes, buckets, slowest)
}

#[test]
#[ignore = "perf"]
fn memory_saving_per_busy_service() {
    for per_minute in [60, 6_000] {
        let (off, off_b, off_t) = day(false, per_minute);
        let (on, on_b, on_t) = day(true, per_minute);
        println!(
            "{per_minute} req/min: off {off} B in {off_b} buckets (max merge {off_t:?}); on {on} B in {on_b} buckets (max merge {on_t:?}); saving {:.1} %",
            100.0 * (1.0 - on as f64 / off as f64)
        );
        assert!(on < off / 4, "compaction must save most of the ring");
    }
}
