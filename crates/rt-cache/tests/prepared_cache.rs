//! rt-cache `PreparedCache` tests with a counting fake `Loader` (CCH-01 T1 hit, CCH-02 single-flight, CCH-03 shared key, CCH-04 cost-aware victim,
//! CCH-05 scan resistance, CCH-06 pins, CCH-07 idle TTL, CCH-08 evict-while-in-use, plus the
//! negative cache for failed builds.
//!
//! No scheduler is bound: `Pending` lookups are re-polled the way a `WaitingRuntime` task is
//! woken. The cache must tolerate a missing scheduler (it skips the `Build` submission and wake-up).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use rt_cache::{EvictReason, Loader, Lookup, PreparedSource};
use rt_sched::{Clock, SimClock};
use rt_testkit::VirtualClock;
use rt_types::{BuildFailReason, BuildFailure, PreparedKey, RuntimeKind, TaskId};

// ---- CCH-01 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_01_t1_hit_serves_from_memory_without_any_load_job() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);

    let first = get(&r.cache, &k, 1).await;
    assert_eq!(r.loader.loads(), 1, "the first request loads T2 -> T1 once");
    assert_eq!(first.key, k);

    for task in 2..12 {
        let again = hit(&r.cache, &k);
        assert!(
            Arc::ptr_eq(&again, &first),
            "task {task} must receive the T1 entry itself"
        );
    }
    assert_eq!(r.loader.loads(), 1, "a T1 hit issues no blocking-pool or I/O job");
    assert!(r.cache.is_ready(&k));
    assert_eq!(r.loader.loads(), 1, "is_ready must not trigger loads");

    let st = r.cache.stats();
    assert_eq!(st.t1_entries, 1);
    assert_eq!(st.t1_bytes, 1000);
    assert!(st.hits >= 10, "10 T1 hits expected, stats say {}", st.hits);
    assert_eq!(st.loads_inflight, 0);
    assert_eq!(
        r.mem.committed(),
        1000,
        "T1 bytes are reserved through the memory governor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_01_is_ready_is_false_for_unknown_keys_and_never_loads() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    assert!(!r.cache.is_ready(&key(5)));
    assert!(!r.cache.contains(&key(5)));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(r.loader.loads(), 0);
    assert_eq!(r.cache.stats().t1_entries, 0);
}

// ---- CCH-02 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_cch_02_fifty_concurrent_requests_share_one_load_job() {
    let r = rig(
        FakeLoader::memory(2000, Duration::from_millis(150)),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    let barrier = Arc::new(tokio::sync::Barrier::new(50));

    let mut handles = Vec::new();
    for i in 0..50u64 {
        let (cache, barrier) = (r.cache.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let t0 = Instant::now();
            let immediate = match cache.lookup(&k, TaskId(i)) {
                Lookup::Ready(v) => Some(v),
                Lookup::Pending => None,
                Lookup::Failed(f) => panic!("lookup failed: {f}"),
            };
            let first_call = t0.elapsed();
            let value = match immediate {
                Some(v) => v,
                None => get(&cache, &k, i).await,
            };
            (first_call, value)
        }));
    }
    let mut values = Vec::new();
    for h in handles {
        let (first_call, v) = h.await.expect("join");
        assert!(
            first_call < Duration::from_millis(100),
            "lookup must not block while the load runs, took {first_call:?}"
        );
        values.push(v);
    }

    assert_eq!(values.len(), 50, "all 50 requests are served");
    assert_eq!(r.loader.loads(), 1, "single-flight: exactly one load job");
    assert!(
        values.iter().all(|v| Arc::ptr_eq(v, &values[0])),
        "everyone gets the same T1 entry"
    );
    assert_eq!(r.cache.stats().t1_entries, 1);
    assert_eq!(r.cache.stats().loads_inflight, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_cch_02_different_keys_load_independently_in_parallel() {
    let r = rig(
        FakeLoader::memory(1000, Duration::from_millis(100)),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let started = Instant::now();
    let mut handles = Vec::new();
    for n in 0..5u64 {
        let cache = r.cache.clone();
        handles.push(tokio::spawn(async move { get(&cache, &key(n), n).await }));
    }
    for h in handles {
        h.await.expect("join");
    }
    assert_eq!(r.loader.loads(), 5, "one load per distinct key");
    for n in 0..5 {
        assert_eq!(r.loader.loads_for(&key(n)), 1);
    }
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "loads of distinct keys must overlap, took {:?}",
        started.elapsed()
    );
}

// ---- CCH-03 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_03_identical_code_shares_one_t1_entry_with_two_handles() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    // Two services deployed identical code: same digest, kind and engine fingerprint -> same key.
    let service_a_key = key(7);
    let service_b_key = key(7);

    let a = get(&r.cache, &service_a_key, 1).await;
    let b = get(&r.cache, &service_b_key, 2).await;

    assert!(Arc::ptr_eq(&a, &b), "one T1 entry");
    assert!(Arc::strong_count(&a) >= 2, "both tasks hold a handle (refcount >= 2)");
    assert_eq!(r.loader.loads(), 1);
    assert_eq!(r.cache.stats().t1_entries, 1);
    assert_eq!(r.cache.stats().t1_bytes, 1000, "the shared entry is accounted once");
    assert_eq!(r.mem.committed(), 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_03_key_includes_digest_kind_and_engine_fingerprint() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let base = key(1);
    let other_digest = key(2);
    let other_kind = PreparedKey {
        kind: RuntimeKind::Wasm,
        ..base
    };
    let other_engine = PreparedKey {
        engine_fp: base.engine_fp + 1,
        ..base
    };

    let handles = [
        get(&r.cache, &base, 1).await,
        get(&r.cache, &other_digest, 2).await,
        get(&r.cache, &other_kind, 3).await,
        get(&r.cache, &other_engine, 4).await,
    ];
    assert_eq!(
        r.loader.loads(),
        4,
        "keys differing in digest, kind or engine fp are distinct entries"
    );
    assert_eq!(r.cache.stats().t1_entries, 4);
    for i in 0..4 {
        for j in i + 1..4 {
            assert!(!Arc::ptr_eq(&handles[i], &handles[j]));
        }
    }
    assert_eq!(handles[2].key, other_kind);
}

// ---- CCH-04 ------------------------------------------------------------------------------------

struct Entry {
    size: u64,
    cost_ms: u64,
    hits: usize,
}

/// Loads A and B into T1, accesses them, then asks the cache to shrink to `target` bytes.
/// Returns `(a_survived, b_survived, evicted)`.
async fn evict_with(a: Entry, b: Entry, target: u64) -> (bool, bool, u32) {
    let loader = FakeLoader::memory(1000, Duration::ZERO);
    let (ka, kb) = (key(1), key(2));
    loader.set_size(&ka, a.size);
    loader.set_size(&kb, b.size);
    loader.set_cost(&ka, Duration::from_millis(a.cost_ms));
    loader.set_cost(&kb, Duration::from_millis(b.cost_ms));
    let r = rig(loader, 1_000_000, Duration::from_secs(600), 100);

    request_then_wait_ready(&r.cache, &ka).await;
    request_then_wait_ready(&r.cache, &kb).await;
    for _ in 0..a.hits {
        hit(&r.cache, &ka);
    }
    for _ in 0..b.hits {
        hit(&r.cache, &kb);
    }
    let report = r.cache.evict_to(target, EvictReason::Pressure);
    (r.cache.contains(&ka), r.cache.contains(&kb), report.evicted)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_04_equal_frequency_and_size_the_cheap_reload_is_evicted() {
    // A reloads in 200 ms, B in 2 ms; pressure must evict B.
    let (a, b, evicted) = evict_with(
        Entry {
            size: 1000,
            cost_ms: 200,
            hits: 5,
        },
        Entry {
            size: 1000,
            cost_ms: 2,
            hits: 5,
        },
        1000,
    )
    .await;
    assert_eq!(evicted, 1);
    assert!(a, "A (expensive to reload) must survive");
    assert!(!b, "B (cheap to reload) must be evicted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_04_the_choice_follows_cost_not_insertion_order() {
    let (a, b, evicted) = evict_with(
        Entry {
            size: 1000,
            cost_ms: 2,
            hits: 5,
        },
        Entry {
            size: 1000,
            cost_ms: 200,
            hits: 5,
        },
        1000,
    )
    .await;
    assert_eq!(evicted, 1);
    assert!(!a, "A is the cheap one this time and must be evicted");
    assert!(b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_04_equal_cost_and_size_the_less_frequent_entry_is_evicted() {
    let (a, b, evicted) = evict_with(
        Entry {
            size: 1000,
            cost_ms: 5,
            hits: 12,
        },
        Entry {
            size: 1000,
            cost_ms: 5,
            hits: 1,
        },
        1000,
    )
    .await;
    assert_eq!(evicted, 1);
    assert!(a, "the frequently used entry stays");
    assert!(!b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_04_equal_frequency_and_cost_the_larger_entry_is_evicted() {
    // Score = freq x cost / size: the 4000-byte entry has a quarter of the 1000-byte entry's score.
    let (a, b, evicted) = evict_with(
        Entry {
            size: 4000,
            cost_ms: 5,
            hits: 5,
        },
        Entry {
            size: 1000,
            cost_ms: 5,
            hits: 5,
        },
        4000,
    )
    .await;
    assert_eq!(evicted, 1);
    assert!(!a, "the large, low-score entry goes first");
    assert!(b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_04_evict_to_stops_as_soon_as_the_target_is_met_and_counts_pressure_evictions() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    for n in 0..6 {
        request_then_wait_ready(&r.cache, &key(n)).await;
    }
    assert_eq!(r.cache.stats().t1_bytes, 6000);

    let report = r.cache.evict_to(4000, EvictReason::Pressure);
    assert_eq!(
        report.evicted, 2,
        "6000 -> 4000 bytes needs exactly two 1000-byte victims"
    );
    assert_eq!(r.cache.stats().t1_bytes, 4000);
    assert_eq!(r.cache.stats().t1_entries, 4);
    assert_eq!(r.cache.eviction_count(EvictReason::Pressure), 2);
    assert_eq!(r.cache.eviction_count(EvictReason::Ttl), 0);

    let noop = r.cache.evict_to(10_000, EvictReason::Pressure);
    assert_eq!(noop.evicted, 0, "already below the target");
    assert_eq!(r.cache.stats().t1_entries, 4);

    // An evicted entry is simply reloaded on its next request.
    let victim = (0..6)
        .map(key)
        .find(|k| !r.cache.contains(k))
        .expect("one of the keys was evicted");
    let loads_before = r.loader.loads();
    get(&r.cache, &victim, 1).await;
    assert_eq!(r.loader.loads(), loads_before + 1);
}

// ---- CCH-05 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_cch_05_a_scan_of_one_hit_services_does_not_flush_the_hot_set() {
    const HOT: u64 = 20;
    const COLD: u64 = 500;
    // T1 holds 40 one-kilobyte entries; every service reloads in ~2 ms.
    let r = rig(
        FakeLoader::memory(1000, Duration::from_millis(2)),
        40_000,
        Duration::from_secs(600),
        40,
    );
    let hot: Vec<PreparedKey> = (0..HOT).map(key).collect();

    // Steady load: the hot set is established with plenty of accesses.
    for k in &hot {
        request_then_wait_ready(&r.cache, k).await;
        for _ in 0..20 {
            hit(&r.cache, k);
        }
    }
    assert_eq!(r.loader.loads(), HOT as usize);

    // Burst of one-hit cold services, interleaved with continued hot traffic.
    let mut hot_accesses = 0usize;
    let mut hot_misses = 0usize;
    for c in 0..COLD {
        get(&r.cache, &key(1000 + c), 1_000 + c).await;
        let k = &hot[(c % HOT) as usize];
        hot_accesses += 1;
        match r.cache.lookup(k, TaskId(c)) {
            Lookup::Ready(_) => {}
            Lookup::Pending => {
                hot_misses += 1;
                get(&r.cache, k, c).await;
            }
            Lookup::Failed(f) => panic!("hot lookup failed: {f}"),
        }
    }

    let hit_rate = 1.0 - hot_misses as f64 / hot_accesses as f64;
    assert!(
        hit_rate >= 0.95,
        "hot-set T1 hit rate {hit_rate:.3} ({hot_misses} misses in {hot_accesses}) fell below 95%"
    );
    assert!(
        r.cache.stats().t1_bytes <= 40_000,
        "the cache must respect its T1 budget during the scan"
    );
    let surviving = hot.iter().filter(|k| r.cache.contains(k)).count();
    assert!(surviving >= 19, "only {surviving} of 20 hot services are still in T1");
}

// ---- CCH-06 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_06_pinned_entries_survive_pressure_and_idle_sweeps_until_unpinned() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(60),
        100,
    );
    let keys: Vec<PreparedKey> = (0..6).map(key).collect();
    for k in &keys {
        request_then_wait_ready(&r.cache, k).await;
    }
    r.cache.pin(&keys[0], true);
    r.cache.pin(&keys[1], true);

    // Even the harshest pressure target leaves the pinned entries.
    let report = r.cache.evict_to(0, EvictReason::Pressure);
    assert_eq!(report.evicted, 4, "only the four unpinned entries are evictable");
    assert!(r.cache.contains(&keys[0]) && r.cache.contains(&keys[1]));
    assert!(keys[2..].iter().all(|k| !r.cache.contains(k)));
    assert_eq!(r.cache.stats().t1_entries, 2);
    assert_eq!(r.cache.eviction_count(EvictReason::Pressure), 4);

    // Idle TTL does not apply to pinned entries either.
    let far_future = Instant::now() + Duration::from_secs(24 * 3600);
    assert_eq!(r.cache.sweep_idle(far_future).evicted, 0);
    assert!(r.cache.contains(&keys[0]) && r.cache.contains(&keys[1]));

    // Unpinning makes an entry evictable again.
    r.cache.pin(&keys[0], false);
    assert_eq!(r.cache.evict_to(0, EvictReason::Pressure).evicted, 1);
    assert!(!r.cache.contains(&keys[0]));
    assert!(r.cache.contains(&keys[1]));
}

// ---- CCH-07 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_07_idle_entries_are_evicted_by_the_ttl_sweep_without_pressure() {
    let clock = VirtualClock::new();
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(60),
        100,
    );
    let k = key(1);
    request_then_wait_ready(&r.cache, &k).await;

    clock.advance(Duration::from_secs(30));
    let early = r.cache.sweep_idle(clock.now());
    assert_eq!(early.evicted, 0, "30 s idle is within the 60 s TTL");
    assert!(r.cache.contains(&k));

    clock.advance(Duration::from_secs(40));
    let late = r.cache.sweep_idle(clock.now());
    assert_eq!(late.evicted, 1, "70 s idle exceeds the 60 s TTL");
    assert!(!r.cache.contains(&k));
    assert_eq!(r.cache.stats().t1_entries, 0);
    assert_eq!(r.cache.eviction_count(EvictReason::Ttl), 1, "reason must be `ttl`");
    assert_eq!(r.cache.eviction_count(EvictReason::Pressure), 0);

    // The next request reloads it.
    get(&r.cache, &k, 1).await;
    assert_eq!(r.loader.loads(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_07_an_access_restarts_the_idle_timer() {
    // Real time: the cache stamps accesses with the wall clock. TTL 600 ms, checks 400 ms apart.
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_millis(600),
        100,
    );
    let k = key(1);
    request_then_wait_ready(&r.cache, &k).await;

    tokio::time::sleep(Duration::from_millis(400)).await;
    hit(&r.cache, &k); // touch at t = 400 ms
    tokio::time::sleep(Duration::from_millis(400)).await; // t = 800 ms: 800 ms since load, 400 ms since touch
    assert_eq!(
        r.cache.sweep_idle(Instant::now()).evicted,
        0,
        "the touch must have restarted the 600 ms idle timer"
    );
    assert!(r.cache.contains(&k));

    tokio::time::sleep(Duration::from_millis(400)).await; // 800 ms since the touch
    assert_eq!(r.cache.sweep_idle(Instant::now()).evicted, 1);
    assert!(!r.cache.contains(&k));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_07_the_sweep_only_evicts_entries_past_their_ttl() {
    let clock = VirtualClock::new();
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    for n in 0..5 {
        request_then_wait_ready(&r.cache, &key(n)).await;
    }
    clock.advance(Duration::from_secs(700));
    assert_eq!(
        r.cache.sweep_idle(clock.now()).evicted,
        5,
        "all five were idle past 600 s"
    );
    assert_eq!(r.cache.stats().t1_entries, 0);
    assert_eq!(
        r.mem.committed(),
        0,
        "swept entries with no running tasks release their memory"
    );
}

// ---- CCH-08 ------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_cch_08_evicting_an_entry_in_use_does_not_disturb_the_running_task() {
    let r = rig(
        FakeLoader::memory(10_000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    let handle = get(&r.cache, &k, 1).await;
    let expected = handle.payload.clone();
    assert_eq!(r.mem.committed(), 10_000);

    // A "2 s task" (shortened) that keeps using the entry after it has been evicted.
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let payload = handle.payload.clone();
        (payload, Arc::strong_count(&handle))
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let report = r.cache.evict_to(0, EvictReason::Pressure);
    assert_eq!(report.evicted, 1);
    assert!(!r.cache.contains(&k), "eviction unlinks the entry");
    assert_eq!(r.cache.stats().t1_entries, 0);
    assert_eq!(
        r.loader.drops(),
        0,
        "the running task still holds the value: it must not be dropped"
    );
    assert_eq!(r.mem.committed(), 10_000, "bytes stay accounted while the task runs");

    let (payload, _refs) = task.await.expect("the task completes normally");
    assert_eq!(payload, expected, "the task saw intact data after eviction");

    // After the task dropped its handle the memory is released and un-accounted.
    let (cache, mem, loader) = (r.cache.clone(), r.mem.clone(), r.loader.clone());
    eventually("memory to be un-accounted after the last handle drops", || {
        let _ = cache.stats(); // gives a lazy reaper a chance to run
        mem.committed() == 0 && loader.drops() == 1
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_08_invalidate_removes_the_entry_everywhere_and_the_next_request_reloads() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(3);
    get(&r.cache, &k, 1).await;
    assert!(r.cache.contains(&k));

    r.cache.invalidate(&k);
    assert!(!r.cache.contains(&k));
    assert!(!r.cache.is_ready(&k));
    assert_eq!(r.cache.stats().t1_entries, 0);

    get(&r.cache, &k, 2).await;
    assert_eq!(r.loader.loads(), 2, "an invalidated version is loaded again");
}

// ---- build path / negative cache -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_12_a_miss_waits_for_the_build_and_promotes_the_result_to_t1() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    r.loader.mark_missing(&k); // not in T2: needs a T3 -> T2 build first

    for task in 0..3 {
        match r.cache.lookup(&k, TaskId(task)) {
            Lookup::Pending => {}
            other => panic!("a T2 miss must wait for the build, got {other:?}"),
        }
    }
    assert!(!r.cache.is_ready(&k));
    assert_eq!(r.loader.created(), 0);

    // The build finishes: it wrote T2, now the entry is promoted to T1.
    r.loader.mark_present(&k);
    r.cache.on_build_done(&k, Ok(()));
    let v = get(&r.cache, &k, 9).await;
    assert_eq!(v.key, k);
    assert!(r.cache.contains(&k));
    assert_eq!(r.cache.stats().t1_entries, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_12_failed_builds_are_negatively_cached_until_cleared() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    r.loader.mark_missing(&k);
    assert!(matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending));

    let failure = BuildFailure::new(BuildFailReason::Syntax, "unexpected token at main.ts:3");
    r.cache.on_build_done(&k, Err(failure.clone()));

    for task in 2..5 {
        match r.cache.lookup(&k, TaskId(task)) {
            Lookup::Failed(f) => assert_eq!(f, failure, "waiters fail with the recorded build failure"),
            other => panic!("expected the cached failure, got {other:?}"),
        }
    }
    assert!(!r.cache.is_ready(&k));
    assert!(!r.cache.contains(&k));

    // `POST .../rebuild` forgets the failure: the next task waits for a fresh build.
    r.cache.clear_failure(&k);
    assert!(
        matches!(r.cache.lookup(&k, TaskId(9)), Lookup::Pending),
        "after clear_failure the key is rebuilt, not failed"
    );
    r.loader.mark_present(&k);
    r.cache.on_build_done(&k, Ok(()));
    assert_eq!(get(&r.cache, &k, 10).await.key, k);
}

// ---- edge paths: loads, promotions, reservations, bounded bookkeeping -----------------------------

/// A real scheduler that only has a build executor. The tests observe admission, not execution.
fn build_scheduler() -> rt_sched::Scheduler {
    let book = rt_testkit::ScriptBook::new();
    rt_sched::Scheduler::builder(
        rt_sched::SchedConfig::default(),
        Arc::new(rt_sched::SystemClock::new()),
        rt_testkit::TestGovernor::with_mb(256),
    )
    .executor(
        move || rt_testkit::MockBuildExecutor::new(book.clone()),
        Arc::new(rt_sched::UnitSource),
        0,
    )
    .build()
    .expect("scheduler builds")
}

/// Without a tokio runtime the load runs on a plain thread (the fallback of `spawn_job`).
#[test]
fn a_load_started_outside_a_runtime_runs_on_a_plain_thread() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(40);
    r.cache.preload(&[k]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !r.cache.is_ready(&k) {
        assert!(Instant::now() < deadline, "the thread-spawned load never finished");
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(r.loader.loads_for(&k), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preloading_a_resident_key_does_not_load_it_again() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(41);
    r.cache.preload(&[k]);
    get(&r.cache, &k, 1).await;
    r.cache.preload(&[k]);
    assert_eq!(r.loader.loads_for(&k), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoting_a_key_that_is_already_resident_keeps_the_resident_copy() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(42);
    let first = get(&r.cache, &k, 1).await;
    r.cache.promote_built(&k).expect("the artifact loads again");
    assert!(Arc::ptr_eq(&first, &hit(&r.cache, &k)), "the resident value is kept");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_promotion_is_a_build_failure_and_the_next_one_can_succeed() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(43);
    r.loader.fail_next(1, FailKind::Io);
    assert!(
        r.cache.promote_built(&k).is_err(),
        "the load error becomes the build failure"
    );
    assert!(r.cache.promote_built(&k).is_ok());
    assert_eq!(get(&r.cache, &k, 1).await.key, k);
}

/// The reservation governor refuses the second value: the lowest-score entry makes room and the
/// reservation is retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_reservation_evicts_the_lowest_score_entry_and_retries() {
    let loader = FakeLoader::memory(600_000, Duration::ZERO);
    let mem = rt_testkit::TestGovernor::with_mb(1);
    let policy = Box::new(rt_cache::WTinyLfu::new(100));
    let cache = rt_cache::PreparedCache::new(loader, settings(10 << 20, Duration::from_secs(600)), policy, mem, None);
    let (a, b) = (key(50), key(51));
    get(&cache, &a, 1).await;
    get(&cache, &b, 2).await;
    assert_eq!(cache.eviction_count(EvictReason::Pressure), 1);
    assert!(cache.is_ready(&b));
    assert!(!cache.is_ready(&a));
}

/// A build finishes while the first load is still running: the load's failure reloads instead of
/// giving up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_that_lands_during_a_failing_load_triggers_one_reload() {
    let loader = FakeLoader::memory(1000, Duration::ZERO);
    let k = key(44);
    loader.set_cost(&k, Duration::from_millis(200));
    loader.fail_next(1, FailKind::Tampered);
    let r = rig(loader, 100_000, Duration::from_secs(600), 100);
    r.cache.preload(&[k]);
    r.cache.on_build_done(&k, Ok(()));
    eventually("the reload to finish", || r.cache.is_ready(&k)).await;
    assert_eq!(r.loader.loads_for(&k), 2, "one failed load and one reload");
}

/// A corrupt T2 entry is removed before the rebuild; with no scheduler bound the rebuild waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_t2_entry_is_removed_and_not_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let t2 = t2_store_quiet(dir.path());
    let k = key(60);
    t2.write(&k, BUNDLE, b"stale bytes").expect("write");
    let loader = FakeLoader::memory(10, Duration::ZERO);
    loader.fail_next(1, FailKind::Corrupt);
    let r = rig(loader, 100_000, Duration::from_secs(600), 100);
    r.cache.attach_t2(t2.clone());
    r.cache.preload(&[k]);
    eventually("the corrupt entry to be removed", || {
        matches!(t2.read_verified(&k, BUNDLE), Ok(None))
    })
    .await;
    assert!(
        matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending),
        "waiting for the rebuild, not served"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_build_request_for_a_key_in_flight_is_refused() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    r.cache.bind_scheduler(build_scheduler());
    let k = key(70);
    assert!(
        matches!(r.cache.request_build(&k, r.loader.build_spec(&k)), Some(Ok(_))),
        "the first build is submitted"
    );
    assert!(
        r.cache.request_build(&k, r.loader.build_spec(&k)).is_none(),
        "a build of this key is already in flight"
    );
}

/// A scheduler in drain mode refuses every build: an explicit request gets the refusal, and a lazy
/// build (after a load found nothing in T2) releases the key so the next request tries again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_the_scheduler_refuses_is_released_for_the_next_request() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let sched = build_scheduler();
    sched.set_mode(rt_types::DegradationMode::Drain);
    r.cache.bind_scheduler(sched);
    let k = key(71);
    assert!(
        matches!(r.cache.request_build(&k, r.loader.build_spec(&k)), Some(Err(_))),
        "refused explicitly"
    );

    r.loader.mark_missing(&k);
    r.cache.preload(&[k]);
    eventually("the refused lazy build to be released", || {
        matches!(r.cache.request_build(&k, r.loader.build_spec(&k)), Some(Err(_)))
    })
    .await;
}

/// The waiter table is bounded: past its limit it is cleared rather than grown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_waiter_table_is_cleared_past_its_bound() {
    let loader = FakeLoader::memory(1000, Duration::ZERO);
    let n = 100_002u64;
    for i in 0..n {
        loader.mark_missing(&key(i));
    }
    let r = rig(loader, 100_000, Duration::from_secs(600), 100);
    for i in 0..n {
        assert!(matches!(r.cache.lookup(&key(i), TaskId(1)), Lookup::Pending));
    }
}

/// Bookkeeping for keys the cache has seen is bounded: past its limit, keys nobody holds are
/// forgotten. Failed keys keep their failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bookkeeping_for_unheld_keys_is_pruned_past_its_bound() {
    let r = rig(
        FakeLoader::memory(1000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let failure = BuildFailure::new(BuildFailReason::Internal, "boom");
    let n = 65_537u64;
    for i in 0..n {
        r.cache.on_build_done(&key(i), Err(failure.clone()));
    }
    for i in 0..n {
        let _ = r.cache.lookup(&key(i), TaskId(1));
    }
    assert!(matches!(r.cache.lookup(&key(0), TaskId(2)), Lookup::Failed(_)));
}
