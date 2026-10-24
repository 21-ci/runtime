//! CCH-10 / CCH-11: frequencies are persisted (`meta.db` `cache_freq`) and restored at startup; in
//! `min_latency` the top-N services are preloaded so their first request is a T1 hit.

mod common;

use std::time::Duration;

use common::*;
use rt_cache::{Lookup, PreparedSource};
use rt_types::{PreparedKey, TaskId};

const HOUR: Duration = Duration::from_secs(3600); // min_latency idle TTL

/// Loads `n` keys; key `i` gets `i * step` extra hits, so its frequency grows strictly with `i`.
async fn warm(r: &Rig, n: u64, step: usize) -> Vec<PreparedKey> {
    let keys: Vec<PreparedKey> = (0..n).map(key).collect();
    for (i, k) in keys.iter().enumerate() {
        request_then_wait_ready(&r.cache, k).await;
        for _ in 0..i * step {
            hit(&r.cache, k);
        }
    }
    keys
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_10_after_restart_the_top_n_services_are_t1_hits_on_their_first_request() {
    // Before the restart: five services with clearly different popularity (frequencies 1, 4, 7, 10, 13).
    let before = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    let keys = warm(&before, 5, 3).await;
    let snapshot = before.cache.persist_frequencies();
    assert!(!snapshot.is_empty());
    let top3 = before.cache.top_keys(3);
    assert_eq!(top3, vec![keys[4], keys[3], keys[2]], "top-N by descending frequency");

    // Restart: a cold cache with a new loader; restore the sketch and preload the persisted top-N.
    let after = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    assert_eq!(after.cache.stats().t1_entries, 0);
    after.cache.restore_frequencies(&snapshot);
    after.cache.preload(&top3);
    let (cache, loader) = (after.cache.clone(), after.loader.clone());
    eventually("preload of the top-3 to finish", || {
        loader.loads() == 3 && top3.iter().all(|k| cache.is_ready(k)) && cache.stats().loads_inflight == 0
    })
    .await;

    // First request to each top-N service: an immediate T1 hit, no further load.
    for (i, k) in top3.iter().enumerate() {
        match after.cache.lookup(k, TaskId(i as u64)) {
            Lookup::Ready(v) => assert_eq!(v.key, *k),
            other => panic!("first request to top-N service {i} must be a T1 hit, got {other:?}"),
        }
    }
    assert_eq!(after.loader.loads(), 3, "no load happened on the request path");

    // Only the top-N were preloaded; the others are still cold and must not have been loaded.
    assert!(!after.cache.is_ready(&keys[0]) && !after.cache.is_ready(&keys[1]));
    assert_eq!(after.loader.loads_for(&keys[0]), 0);
    assert_eq!(after.cache.stats().t1_entries, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_10_preload_is_single_flight_with_concurrent_requests() {
    let r = rig(
        FakeLoader::memory(1000, Duration::from_millis(100)),
        1_000_000,
        HOUR,
        100,
    );
    let keys: Vec<PreparedKey> = (0..3).map(key).collect();

    r.cache.preload(&keys);
    // A request racing the preload of the same key must join it, not start a second load.
    let racer = get(&r.cache, &keys[0], 1).await;
    assert_eq!(racer.key, keys[0]);
    let (cache, loader) = (r.cache.clone(), r.loader.clone());
    eventually("preload to finish", || {
        loader.loads() >= 3 && keys.iter().all(|k| cache.is_ready(k))
    })
    .await;
    assert_eq!(r.loader.loads(), 3, "each preloaded key is loaded exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_11_restored_frequencies_keep_the_top_ordering() {
    // Twelve services, frequencies 1..=12 (distinct, below the 4-bit cap).
    let before = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    let keys = warm(&before, 12, 1).await;
    let expected: Vec<PreparedKey> = keys.iter().rev().copied().collect();
    let order_before = before.cache.top_keys(12);
    assert_eq!(order_before, expected, "hotter keys first");
    let snapshot = before.cache.persist_frequencies();

    // An empty cache has a different (cold) sketch: the snapshot carries real information.
    let cold = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    assert_ne!(cold.cache.persist_frequencies(), snapshot);

    // Restart: restore, then let every service be requested once, in a scrambled order so that
    // neither insertion order nor recency (tie-breakers) can explain the resulting ordering.
    let after = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    after.cache.restore_frequencies(&snapshot);
    for i in 0..12usize {
        request_then_wait_ready(&after.cache, &keys[(i * 5) % 12]).await;
    }
    assert_eq!(
        after.cache.top_keys(12),
        order_before,
        "top ordering unchanged after restart"
    );
    assert_eq!(after.cache.top_keys(1), vec![keys[11]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ac_cch_11_a_corrupt_snapshot_is_ignored_and_the_cache_starts_cold() {
    let r = rig(FakeLoader::memory(1000, Duration::ZERO), 1_000_000, HOUR, 100);
    r.cache.restore_frequencies(&[0xFF, 0x00, 0x13, 0x37, 0x01]);
    r.cache.restore_frequencies(&[]);
    // Still fully functional.
    let k = key(1);
    assert_eq!(get(&r.cache, &k, 1).await.key, k);
    assert_eq!(r.cache.top_keys(5), vec![k]);
}
