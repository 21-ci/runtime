//! rt-cache eviction policy tests: the frequency sketch (4-bit counters,
//! doorkeeper, periodic halving) and the cost-aware W-TinyLFU admission rule
//! `freq x cost / size` (CCH-04), pins, snapshot/restore (CCH-11).
//!
//! `estimate()`/`frequency()` include the doorkeeper bit, so for `n` accesses of a lone key the
//! value is `n` or `n - 1`; assertions use that tolerance.

use std::time::Duration;

use proptest::prelude::*;
use rt_cache::{EntryMeta, EvictionPolicy, FrequencySketch, WTinyLfu};

fn meta(key_hash: u64, size: u64, cost_ms: u64) -> EntryMeta {
    EntryMeta {
        key_hash,
        size,
        cost: Duration::from_millis(cost_ms),
        pinned: false,
    }
}

/// A policy where `a` was accessed `na` times and `b` `nb` times.
fn policy_with(a: (u64, usize), b: (u64, usize)) -> WTinyLfu {
    let mut p = WTinyLfu::new(10_000);
    for _ in 0..a.1 {
        p.record_access(a.0);
    }
    for _ in 0..b.1 {
        p.record_access(b.0);
    }
    p
}

// ---- FrequencySketch ---------------------------------------------------------------------------

#[test]
fn ac_policy_01_sketch_counts_accesses_per_key() {
    let mut s = FrequencySketch::new(10_000);
    let (a, b) = (12345u64, 99u64);
    for _ in 0..5 {
        s.increment(a);
    }
    for _ in 0..12 {
        s.increment(b);
    }
    let (ea, eb) = (s.estimate(a), s.estimate(b));
    assert!((4..=5).contains(&ea), "5 accesses estimated as {ea}");
    assert!((11..=12).contains(&eb), "12 accesses estimated as {eb}");
    assert_eq!(s.estimate(777), 0, "an untouched key has frequency 0");
}

#[test]
fn ac_policy_02_counters_saturate_at_four_bits_instead_of_wrapping() {
    let mut s = FrequencySketch::new(10_000);
    for _ in 0..1000 {
        s.increment(999);
    }
    let e = s.estimate(999);
    assert!(
        (15..=16).contains(&e),
        "4-bit counter (+ doorkeeper bit) saturates at 15/16, got {e}"
    );
}

#[test]
fn ac_policy_03_halve_roughly_halves_every_counter() {
    let mut s = FrequencySketch::new(10_000);
    for _ in 0..14 {
        s.increment(1);
    }
    for _ in 0..6 {
        s.increment(2);
    }
    let (b1, b2) = (s.estimate(1), s.estimate(2));
    s.halve();
    let (a1, a2) = (s.estimate(1), s.estimate(2));
    assert!(a1 < b1 && a2 < b2, "halving must reduce counts: {b1}->{a1}, {b2}->{a2}");
    assert!(
        a1 + 2 >= b1 / 2 && a1 <= b1 / 2 + 2,
        "{b1} should halve to about {}, got {a1}",
        b1 / 2
    );
    assert!(
        a2 + 2 >= b2 / 2 && a2 <= b2 / 2 + 2,
        "{b2} should halve to about {}, got {a2}",
        b2 / 2
    );
    assert!(a1 > a2, "ordering between keys is preserved by halving");
}

#[test]
fn ac_policy_04_sketch_ages_itself_after_ten_times_capacity_accesses() {
    const A: u64 = 0xA5A5_1234_5678_9ABC;
    const B: u64 = 0x0F0F_C3C3_DEAD_BEEF;
    // capacity 100 => reset sample of 1,000 accesses.
    let mut s = FrequencySketch::new(100);
    for _ in 0..14 {
        s.increment(A);
    }
    let before = s.estimate(A);

    // 500 more accesses (of another key): still below the 1,000 sample, no aging yet.
    for _ in 0..500 {
        s.increment(B);
    }
    assert!(s.estimate(A) >= before, "no aging before 10 x capacity accesses");

    // Cross the threshold: every counter is halved once.
    for _ in 0..600 {
        s.increment(B);
    }
    let after = s.estimate(A);
    assert!(after < before, "counters must have been halved: {before} -> {after}");
    assert!(
        after >= 4 && after <= before / 2 + 2,
        "{before} should have halved to about {}, got {after}",
        before / 2
    );
}

// ---- WTinyLfu: frequency, decay, snapshot --------------------------------------------------------

#[test]
fn ac_policy_05_record_access_drives_frequency() {
    let mut p = WTinyLfu::new(10_000);
    let key = 0x1234_5678_9abc_def0u64;
    assert_eq!(p.frequency(key), 0);
    for _ in 0..6 {
        p.record_access(key);
    }
    assert!(
        (5..=6).contains(&p.frequency(key)),
        "6 accesses, frequency {}",
        p.frequency(key)
    );
    assert_eq!(p.frequency(key ^ 1), 0);
}

#[test]
fn ac_policy_06_decay_halves_frequencies() {
    let mut p = WTinyLfu::new(10_000);
    for _ in 0..14 {
        p.record_access(7);
    }
    let before = p.frequency(7);
    p.decay();
    let after = p.frequency(7);
    assert!(after < before, "decay must lower the frequency: {before} -> {after}");
    assert!(after + 2 >= before / 2 && after <= before / 2 + 2);
}

#[test]
fn ac_policy_07_snapshot_restore_reproduces_every_frequency() {
    let mut p1 = WTinyLfu::new(10_000);
    // 20 keys with counts 1..=15 (cycling).
    for k in 0..20u64 {
        for _ in 0..(k % 15 + 1) {
            p1.record_access(0x1000 + k);
        }
    }
    let snapshot = p1.snapshot();
    assert!(!snapshot.is_empty());

    let mut p2 = WTinyLfu::new(10_000);
    for _ in 0..5 {
        p2.record_access(0xDEAD); // pre-existing state is replaced, not merged
    }
    p2.restore(&snapshot);

    for k in 0..20u64 {
        assert_eq!(p2.frequency(0x1000 + k), p1.frequency(0x1000 + k), "key {k}");
    }
    assert_eq!(
        p2.frequency(0xDEAD),
        p1.frequency(0xDEAD),
        "restore replaces the previous sketch"
    );

    // Ordering of the 20 keys by frequency is identical (CCH-11: top-20 ordering unchanged).
    let order = |p: &WTinyLfu| {
        let mut v: Vec<(u32, u64)> = (0..20u64).map(|k| (p.frequency(0x1000 + k), k)).collect();
        v.sort();
        v
    };
    assert_eq!(order(&p1), order(&p2));
}

#[test]
fn ac_policy_08_corrupt_or_truncated_snapshots_are_ignored() {
    let mut src = WTinyLfu::new(10_000);
    for _ in 0..9 {
        src.record_access(42);
    }
    let good = src.snapshot();

    for (name, bad) in [
        ("garbage", vec![0xFFu8; 7]),
        ("empty", Vec::new()),
        ("truncated", good[..good.len() / 2].to_vec()),
    ] {
        let mut p = WTinyLfu::new(10_000);
        p.restore(&bad);
        assert_eq!(p.frequency(42), 0, "{name}: a bad snapshot must leave a cold start");
        p.record_access(42);
        assert!(p.frequency(42) <= 1, "{name}: the policy keeps working");
    }
}

// ---- WTinyLfu: cost-aware admission (CCH-04) ----------------------------------------------------

#[test]
fn ac_policy_09_equal_frequency_and_size_the_costly_reload_wins_admission() {
    // Both accessed 4 times; 200 ms vs 2 ms reload, same size.
    let p = policy_with((1, 4), (2, 4));
    let costly = meta(1, 1000, 200);
    let cheap = meta(2, 1000, 2);
    assert!(
        p.admit(&costly, &cheap),
        "a 200 ms reload beats a 2 ms one at equal frequency/size"
    );
    assert!(
        !p.admit(&cheap, &costly),
        "and not the other way round: the cheap entry is the victim"
    );
}

#[test]
fn ac_policy_10_equal_scores_do_not_displace_the_incumbent() {
    let p = policy_with((1, 4), (2, 4));
    assert!(
        !p.admit(&meta(1, 1000, 10), &meta(2, 1000, 10)),
        "admission requires a strictly higher score"
    );
    assert!(!p.admit(&meta(2, 1000, 10), &meta(1, 1000, 10)));
}

#[test]
fn ac_policy_11_higher_frequency_wins_at_equal_cost_and_size() {
    let p = policy_with((1, 8), (2, 2));
    assert!(p.admit(&meta(1, 1000, 10), &meta(2, 1000, 10)));
    assert!(!p.admit(&meta(2, 1000, 10), &meta(1, 1000, 10)));
}

#[test]
fn ac_policy_12_smaller_size_wins_at_equal_frequency_and_cost() {
    let p = policy_with((1, 5), (2, 5));
    assert!(p.admit(&meta(1, 1000, 10), &meta(2, 4000, 10)), "score divides by size");
    assert!(!p.admit(&meta(1, 4000, 10), &meta(2, 1000, 10)));
}

#[test]
fn ac_policy_13_cost_can_outweigh_frequency() {
    // candidate: freq ~4 x 500 ms; victim: freq ~8 x 100 ms -> 2000 vs 800.
    let p = policy_with((1, 4), (2, 8));
    assert!(p.admit(&meta(1, 1000, 500), &meta(2, 1000, 100)));
    // and frequency can outweigh cost: freq ~8 x 100 ms vs freq ~2 x 100 ms.
    let q = policy_with((1, 8), (2, 2));
    assert!(q.admit(&meta(1, 1000, 100), &meta(2, 1000, 100)));
    assert!(!q.admit(&meta(2, 1000, 100), &meta(1, 1000, 100)));
}

#[test]
fn ac_policy_14_a_pinned_victim_can_never_be_replaced() {
    let p = policy_with((1, 15), (2, 1));
    let candidate = meta(1, 100, 5_000); // enormous score
    let victim = EntryMeta {
        pinned: true,
        ..meta(2, 10_000, 1)
    };
    assert!(
        !p.admit(&candidate, &victim),
        "keep_resident entries are exempt from eviction"
    );
    // The same victim unpinned loses to that candidate: the pin is what protects it.
    assert!(p.admit(
        &candidate,
        &EntryMeta {
            pinned: false,
            ..victim
        }
    ));
}

// ---- property ---------------------------------------------------------------------------------------

proptest! {
    #[test]
    fn ac_policy_15_prop_estimates_never_undercount_beyond_the_doorkeeper(
        accesses in proptest::collection::vec(0u64..40, 1..300)
    ) {
        // capacity large enough that no aging happens within 300 accesses.
        let mut s = FrequencySketch::new(10_000);
        let mut counts = std::collections::HashMap::<u64, u32>::new();
        for k in &accesses {
            s.increment(*k);
            *counts.entry(*k).or_default() += 1;
        }
        for (k, n) in counts {
            let e = s.estimate(k);
            prop_assert!(e + 1 >= n.min(15), "key {k}: {n} accesses estimated as {e}");
            prop_assert!(e <= 16, "estimate {e} exceeds the 4-bit cap");
        }
    }
}
