//! B6.2 cache wiring: T1 inserts without a memory reservation are counted, a fresh build is
//! loaded into T1 before it is reported (`promote_built`), one disk GC pass covers every cache
//! sharing the T2 store, and persisted frequencies rank the startup preload.

mod common;

use std::time::Duration;

use common::*;
use rt_cache::{pack_snapshots, unpack_snapshots, Lookup, PreparedCache, PreparedSource, T2Usage, WTinyLfu};
use rt_obs::{Metrics, MetricsOptions};
use rt_testkit::{ids, TestGovernor};
use rt_types::{BuildFailReason, PreparedKey, RuntimeKind, TaskId};

fn wasm_key(n: u64) -> PreparedKey {
    PreparedKey {
        digest: ids::digest(1000 + n),
        kind: RuntimeKind::Wasm,
        engine_fp: 0xE2,
    }
}

// ---- unreserved T1 inserts ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_t1_insert_the_governor_refused_and_nothing_could_be_evicted_is_counted() {
    let loader = FakeLoader::memory(1_000, Duration::ZERO);
    let metrics = Metrics::new(MetricsOptions::default()).unwrap();
    // 500 bytes of budget, an empty cache: the 1,000-byte value cannot be reserved
    let cache = PreparedCache::new(
        loader.clone(),
        settings(1_000_000, Duration::from_secs(600)),
        Box::new(WTinyLfu::new(100)),
        TestGovernor::new(500),
        Some(metrics.clone()),
    );
    let k = key(1);
    get(&cache, &k, 1).await;
    assert_eq!(cache.stats().t1_unreserved_inserts, 1);
    assert_eq!(metrics.cache_unreserved_inserts_total.get(), 1);

    // a value that fits is reserved normally
    loader.set_size(&key(2), 100);
    get(&cache, &key(2), 2).await;
    assert_eq!(cache.stats().t1_unreserved_inserts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_caches_add_up_in_the_t1_gauges_instead_of_overwriting_each_other() {
    let metrics = Metrics::new(MetricsOptions::default()).unwrap();
    let mk = || {
        PreparedCache::new(
            FakeLoader::memory(100, Duration::ZERO),
            settings(1_000_000, Duration::from_secs(600)),
            Box::new(WTinyLfu::new(100)),
            TestGovernor::with_mb(16),
            Some(metrics.clone()),
        )
    };
    let (a, b) = (mk(), mk());
    get(&a, &key(1), 1).await;
    get(&a, &key(2), 2).await;
    get(&b, &wasm_key(1), 3).await;
    assert_eq!(metrics.cache_entries.with_label_values(&["t1"]).get(), 3);
    assert_eq!(metrics.cache_bytes.with_label_values(&["t1"]).get(), 300);
    a.invalidate(&key(1));
    assert_eq!(metrics.cache_entries.with_label_values(&["t1"]).get(), 2);
}

// ---- promote_built -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_built_loads_the_fresh_artifact_into_t1_before_the_build_is_reported() {
    let r = rig(
        FakeLoader::memory(1_000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    r.loader.mark_missing(&k);
    assert!(matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending));
    eventually("the T2 miss to turn into a build", || {
        r.loader.loads_for(&k) == 1 && r.cache.stats().builds_inflight == 1
    })
    .await;

    // the build wrote T2
    r.loader.mark_present(&k);
    r.cache.promote_built(&k).expect("the fresh artifact loads");
    assert!(
        r.cache.contains(&k),
        "resident before anyone is told the build is ready"
    );
    assert_eq!(r.loader.loads_for(&k), 2);

    // reporting the build releases the waiters without a second load
    r.cache.on_build_done(&k, Ok(()));
    let v = get(&r.cache, &k, 1).await;
    assert_eq!(v.key, k);
    assert_eq!(r.loader.loads_for(&k), 2, "no reload after the promotion");
    assert_eq!(r.cache.stats().builds_inflight + r.cache.stats().loads_inflight, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_built_returns_the_load_failure_so_the_version_is_not_marked_ready() {
    let r = rig(
        FakeLoader::memory(1_000, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let k = key(1);
    r.loader.mark_missing(&k);
    assert!(matches!(r.cache.lookup(&k, TaskId(1)), Lookup::Pending));
    eventually("the build flight", || r.cache.stats().builds_inflight == 1).await;

    let failure = r.cache.promote_built(&k).expect_err("T2 still misses the artifact");
    assert_eq!(failure.reason, BuildFailReason::Internal);
    assert!(!r.cache.contains(&k));

    r.cache.on_build_done(&k, Err(failure.clone()));
    match r.cache.lookup(&k, TaskId(1)) {
        Lookup::Failed(f) => assert_eq!(f, failure),
        other => panic!("expected the negative cache, got {other:?}"),
    }
}

// ---- combined T2 GC ----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_gc_pass_sees_the_in_use_and_hot_entries_of_every_cache_sharing_t2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let t2 = t2_store_quiet(dir.path());
    let deno_keys: Vec<PreparedKey> = (0..4).map(key).collect();
    let wasm_keys: Vec<PreparedKey> = (0..4).map(wasm_key).collect();
    for k in deno_keys.iter().chain(&wasm_keys) {
        t2.write(k, BUNDLE, &[7u8; 100]).expect("write T2");
    }
    let entry = t2.list().expect("list")[0].bytes; // bundle + MAC sidecar
    let mut cfg = settings(1_000_000, Duration::from_secs(600));
    cfg.disk_low_free = 1_000;
    // at 999 bytes free GC must free `3 × entry + 1` bytes: exactly four entries
    cfg.disk_high_free = 999 + 3 * entry;
    let deno = rig_with(FakeLoader::with_disk(t2.clone(), Duration::ZERO), cfg.clone(), 100);
    let wasm = rig_with(FakeLoader::with_disk(t2.clone(), Duration::ZERO), cfg, 100);
    deno.cache.attach_t2(t2.clone());
    wasm.cache.attach_t2(t2.clone());
    for k in &deno_keys {
        request_then_wait_ready(&deno.cache, k).await;
    }
    for k in &wasm_keys {
        request_then_wait_ready(&wasm.cache, k).await;
    }
    // every WASM entry is hot, and one is held by a running task
    for k in &wasm_keys {
        for _ in 0..10 {
            hit(&wasm.cache, k);
        }
    }
    let held = hit(&wasm.cache, &wasm_keys[3]);

    // 999 free < low: four entries go, in one pass over both caches
    let report = deno.cache.gc_disk_with(999, &[&*wasm.cache as &dyn T2Usage]);
    assert_eq!(report.evicted, 4, "{report:?}");
    for k in &deno_keys {
        assert!(
            t2.read_verified(k, BUNDLE).expect("read").is_none(),
            "cold deno entry {k:?} goes first"
        );
    }
    for k in &wasm_keys {
        assert!(
            t2.read_verified(k, BUNDLE).expect("read").is_some(),
            "hot wasm entry {k:?} stays"
        );
    }

    // nothing else is cold: the held WASM entry survives even a GC that must free everything
    let report = deno.cache.gc_disk_with(0, &[&*wasm.cache as &dyn T2Usage]);
    assert_eq!(report.evicted, 3);
    assert!(
        t2.read_verified(&wasm_keys[3], BUNDLE).expect("read").is_some(),
        "an entry in use by the other cache is kept"
    );
    drop(held);
}

// ---- persisted frequencies and preload ---------------------------------------------------------

#[test]
fn snapshots_of_several_caches_pack_into_one_blob_and_back() {
    let blob = pack_snapshots(&[b"deno-sketch".as_slice(), b"".as_slice(), b"wasm".as_slice()]);
    assert_eq!(
        unpack_snapshots(&blob),
        vec![b"deno-sketch".to_vec(), Vec::new(), b"wasm".to_vec()]
    );
    assert!(
        unpack_snapshots(b"garbage").is_empty(),
        "an unknown blob is a cold start"
    );
    assert!(
        unpack_snapshots(&blob[..blob.len() - 1]).is_empty(),
        "a truncated blob is a cold start"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preload_top_loads_the_most_frequent_candidates_after_a_restart() {
    let before = rig(
        FakeLoader::memory(100, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    let ks: Vec<PreparedKey> = (1..=4).map(key).collect();
    for (k, n) in ks.iter().zip([10, 6, 2, 0]) {
        request_then_wait_ready(&before.cache, k).await;
        for _ in 0..n {
            hit(&before.cache, k);
        }
    }
    let blob = pack_snapshots(&[before.cache.persist_frequencies().as_slice()]);

    let after = rig(
        FakeLoader::memory(100, Duration::ZERO),
        100_000,
        Duration::from_secs(600),
        100,
    );
    after.cache.restore_frequencies(&unpack_snapshots(&blob)[0]);
    let chosen = after.cache.preload_top(&[ks[3], ks[2], ks[1], ks[0]], 2);
    assert_eq!(chosen, vec![ks[0], ks[1]], "ranked by the persisted frequency");
    eventually("the preload", || ks[..2].iter().all(|k| after.cache.is_ready(k))).await;
    assert_eq!(
        after.loader.loads_for(&ks[2]) + after.loader.loads_for(&ks[3]),
        0,
        "only the top-N load"
    );
}
