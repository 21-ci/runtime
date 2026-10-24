//! TRG-18 at unit level: the per-M sharded token bucket (RP-06).
//!
//! The 40k-RPS-on-the-reference-box load test is a PRF benchmark; here we check the accuracy of
//! the limiter itself on virtual time: admitted rate within ±5 % of the limit under even and
//! skewed load (sysmon's 100 ms rebalance), no false rejections far below the limit, no
//! over-admission under concurrency, and classic token-bucket behavior with one shard.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use rt_sched::{Clock, SimClock};
use rt_testkit::VirtualClock;
use rt_triggers::ShardedBucket;

fn bucket(clock: &Arc<VirtualClock>, rate: u64, burst: u64, shards: usize) -> ShardedBucket {
    ShardedBucket::new(clock.clone() as Arc<dyn Clock>, rate, burst, shards)
}

/// Offer `offered_rps` requests per second for `secs` seconds in 1 ms ticks, choosing the shard
/// of request `n` with `pick`, calling `rebalance()` every 100 ms like sysmon. Returns the number
/// admitted in each whole second.
fn offer(
    b: &ShardedBucket,
    clock: &VirtualClock,
    offered_rps: u64,
    secs: u64,
    pick: impl Fn(u64) -> usize,
) -> Vec<u64> {
    let per_ms = offered_rps / 1000;
    let mut per_second = vec![0u64; secs as usize];
    let mut n = 0u64;
    for ms in 0..secs * 1000 {
        clock.advance(Duration::from_millis(1));
        for _ in 0..per_ms {
            if b.try_acquire(pick(n), 1) {
                per_second[(ms / 1000) as usize] += 1;
            }
            n += 1;
        }
        if ms % 100 == 99 {
            b.rebalance();
        }
    }
    per_second
}

fn within(actual: u64, expected: u64, pct: f64) -> bool {
    let (a, e) = (actual as f64, expected as f64);
    (a - e).abs() <= e * pct / 100.0
}

#[test]
fn ac_trg_18_even_load_admits_the_configured_rate_within_five_percent() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 40_000, 4_000, 8);
    assert_eq!(b.shards(), 8);

    // 50k offered per second, spread evenly over the 8 shards.
    let per_second = offer(&b, &clock, 50_000, 5, |n| (n % 8) as usize);
    let total: u64 = per_second.iter().sum();
    assert!(
        within(total, 5 * 40_000 + 4_000, 5.0),
        "admitted {total} in 5 s, expected ≈ 204,000 (rate × 5 s + burst)"
    );
    // Steady state (after the initial burst) is the limit itself, ±5 %.
    for (i, a) in per_second.iter().enumerate().skip(1) {
        assert!(
            within(*a, 40_000, 5.0),
            "second {i}: admitted {a}, expected 40,000 ± 5 %"
        );
    }
}

#[test]
fn ac_trg_18_skewed_load_is_rebalanced_to_the_full_rate() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 40_000, 4_000, 8);

    // The whole offered load hits shard 0 (one busy M); the other 7 shards are idle. Without
    // rebalancing only rate/8 = 5k/s would be admitted.
    let per_second = offer(&b, &clock, 50_000, 5, |_| 0);
    for (i, a) in per_second.iter().enumerate().skip(1) {
        assert!(
            within(*a, 40_000, 5.0),
            "second {i}: admitted {a} with all load on one shard, expected 40,000 ± 5 %"
        );
    }
}

#[test]
fn ac_trg_18_two_hot_shards_out_of_eight() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 40_000, 4_000, 8);
    let per_second = offer(&b, &clock, 50_000, 5, |n| (n % 2) as usize);
    for (i, a) in per_second.iter().enumerate().skip(1) {
        assert!(
            within(*a, 40_000, 5.0),
            "second {i}: admitted {a}, expected 40,000 ± 5 %"
        );
    }
}

#[test]
fn ac_trg_18_a_limit_far_above_the_offered_load_rejects_nothing() {
    // "throughput with a 1M RPS limit is within 5 % of no-limit runs": 50k offered, 1M limit.
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 1_000_000, 100_000, 8);
    let per_second = offer(&b, &clock, 50_000, 5, |n| (n % 8) as usize);
    for (i, a) in per_second.iter().enumerate() {
        assert_eq!(*a, 50_000, "second {i}: every offered request must be admitted");
    }
    // Same when all the load is on one shard.
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 1_000_000, 100_000, 8);
    let per_second = offer(&b, &clock, 50_000, 3, |_| 3);
    assert!(per_second.iter().all(|a| within(*a, 50_000, 0.5)), "{per_second:?}");
}

#[test]
fn burst_after_idle_is_bounded_by_the_configured_burst() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 40_000, 4_000, 8);
    // Drain, then stay idle for a long time: the bucket refills to `burst`, not beyond.
    let _ = offer(&b, &clock, 80_000, 1, |n| (n % 8) as usize);
    clock.advance(Duration::from_secs(60));
    b.rebalance();

    let mut admitted = 0u64;
    for n in 0..20_000u64 {
        if b.try_acquire((n % 8) as usize, 1) {
            admitted += 1;
        }
    }
    assert!(
        admitted <= 4_000 + 8,
        "a burst of 20k right after idling admitted {admitted} > burst 4,000"
    );
    assert!(
        admitted >= 3_600,
        "the full burst should be available after idling, got {admitted}"
    );
}

#[test]
fn a_single_shard_is_a_classic_token_bucket() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 100, 10, 1);
    assert_eq!(b.shards(), 1);

    let mut ok = 0;
    for _ in 0..30 {
        if b.try_acquire(0, 1) {
            ok += 1;
        }
    }
    assert_eq!(ok, 10, "the burst is available immediately, then the bucket is empty");
    assert!(!b.try_acquire(0, 1));

    clock.advance(Duration::from_millis(50)); // 100/s × 50 ms = 5 tokens
    let mut ok = 0;
    for _ in 0..30 {
        if b.try_acquire(0, 1) {
            ok += 1;
        }
    }
    assert_eq!(ok, 5);

    clock.advance(Duration::from_secs(10)); // refill is capped at the burst
    let mut ok = 0;
    for _ in 0..30 {
        if b.try_acquire(0, 1) {
            ok += 1;
        }
    }
    assert_eq!(ok, 10);
}

#[test]
fn acquiring_several_tokens_is_all_or_nothing() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 100, 10, 1);
    assert!(b.try_acquire(0, 7));
    assert!(!b.try_acquire(0, 7), "only 3 tokens left");
    assert!(
        b.try_acquire(0, 3),
        "the failed attempt must not have consumed anything"
    );
    assert!(!b.try_acquire(0, 1));
}

#[test]
fn shard_index_wraps_around_the_shard_count() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 1_000, 8, 4);
    // 4 shards × 2 tokens: shard 5 maps to shard 1 and shares its tokens.
    assert!(b.try_acquire(1, 1));
    assert!(b.try_acquire(5, 1));
    assert!(!b.try_acquire(9, 1), "shard 1's two tokens are gone");
}

#[test]
fn retry_after_is_positive_when_empty_and_small() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 100, 10, 1);
    while b.try_acquire(0, 1) {}
    let wait = b.retry_after();
    assert!(wait > Duration::ZERO, "an empty bucket asks the client to wait");
    assert!(
        wait <= Duration::from_millis(100),
        "at 100 tokens/s a token arrives within 10 ms, asked {wait:?}"
    );
    clock.advance(wait);
    assert!(b.try_acquire(0, 1), "after retry-after a token is available");
}

#[test]
fn retry_after_is_zero_while_a_token_is_available() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 100, 10, 1);
    assert_eq!(b.retry_after(), Duration::ZERO, "a full bucket needs no wait");
}

#[test]
fn an_empty_zero_rate_bucket_asks_for_a_second_because_it_never_refills() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 0, 2, 1);
    while b.try_acquire(0, 1) {}
    clock.advance(Duration::from_secs(60));
    assert!(!b.try_acquire(0, 1), "a zero rate never refills");
    assert_eq!(
        b.retry_after(),
        Duration::from_secs(1),
        "the client is asked to come back in a second"
    );
}

#[test]
fn set_limit_applies_to_subsequent_traffic() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 10_000, 1_000, 4);
    let first = offer(&b, &clock, 20_000, 2, |n| (n % 4) as usize);
    assert!(within(first[1], 10_000, 5.0), "{first:?}");

    b.set_limit(1_000, 100);
    let second = offer(&b, &clock, 20_000, 3, |n| (n % 4) as usize);
    assert!(
        within(second[2], 1_000, 10.0),
        "after set_limit(1000/s) the steady rate is 1000/s, got {second:?}"
    );
}

/// No over-admission under real concurrency: with a frozen clock a bucket holds exactly `burst`
/// tokens in total, however many threads race for them.
#[test]
fn concurrent_acquires_never_over_admit() {
    // One thread per shard.
    let clock = Arc::new(VirtualClock::new());
    let b = Arc::new(bucket(&clock, 10_000, 1_024, 8));
    let admitted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        for shard in 0..8usize {
            let (b, admitted) = (b.clone(), admitted.clone());
            s.spawn(move || {
                for _ in 0..10_000 {
                    if b.try_acquire(shard, 1) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    assert_eq!(
        admitted.load(Ordering::Relaxed),
        1_024,
        "8 shards × 128 tokens, nothing more"
    );

    // All threads on the same shard: still never more than that shard's share.
    let b = Arc::new(bucket(&clock, 10_000, 1_024, 8));
    let admitted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        for _ in 0..8 {
            let (b, admitted) = (b.clone(), admitted.clone());
            s.spawn(move || {
                for _ in 0..10_000 {
                    if b.try_acquire(0, 1) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let got = admitted.load(Ordering::Relaxed);
    assert!(
        got <= 128,
        "shard 0 holds at most burst/shards = 128 tokens, {got} were admitted"
    );
    assert!(got > 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// For any shard count, rate and load distribution the admitted amount over 3 s never
    /// exceeds `rate × 3 s + burst` (+ rounding slack) and, with rebalancing, stays close to it.
    #[test]
    fn admitted_rate_tracks_the_limit(
        shards in 1usize..=16,
        rate in 10_000u64..=100_000,
        skew in 0u8..3,
    ) {
        let clock = Arc::new(VirtualClock::new());
        let burst = rate / 10;
        let b = bucket(&clock, rate, burst, shards);
        let offered = rate + rate / 4;
        let per_second = offer(&b, &clock, offered, 3, |n| match skew {
            0 => (n % shards as u64) as usize,
            1 => 0,
            _ => ((n * 2_654_435_761) % shards as u64) as usize,
        });
        let total: u64 = per_second.iter().sum();
        let upper = rate * 3 + burst + 2 * shards as u64;
        let lower = (rate as f64 * 3.0 * 0.85) as u64;
        prop_assert!(total <= upper, "over-admitted: {total} > {upper}");
        prop_assert!(total >= lower, "under-admitted: {total} < {lower} (shards {shards}, skew {skew})");
    }
}

/// RL-002: a zero rate never refills; a zero burst still holds the one-token minimum; an
/// unchanged clock mints nothing.
#[test]
fn rl_002_zero_rate_and_burst_and_a_still_clock() {
    let clock = Arc::new(VirtualClock::new());
    let b = bucket(&clock, 0, 0, 1);
    assert!(b.try_acquire(0, 1), "the one-token minimum");
    assert!(!b.try_acquire(0, 1));
    clock.advance(Duration::from_secs(3_600));
    assert!(!b.try_acquire(0, 1), "rate 0 never refills");
    let b = bucket(&clock, 10, 10, 1);
    for _ in 0..10 {
        assert!(b.try_acquire(0, 1));
    }
    for _ in 0..100 {
        assert!(!b.try_acquire(0, 1), "no time passed, no tokens");
    }
}

/// SDKLIM-002: `rps = 0` is no per-second limit for SDK calls; a plan change 0 -> 5 -> 0 applies
/// on the next call.
#[test]
fn sdklim_002_zero_rps_is_unlimited_and_plan_changes_apply() {
    let clock = Arc::new(VirtualClock::new());
    let limits = rt_triggers::sdk_limits::SdkCallerLimits::new(clock.clone() as Arc<dyn Clock>);
    let t = rt_testkit::ids::tenant(1);
    let plan = |rps: u32| rt_types::Plan {
        rps,
        max_concurrency: 0,
        ..rt_types::Plan::default()
    };
    for _ in 0..1_000 {
        assert!(limits.admit(t, &plan(0)).is_ok());
    }
    let mut ok = 0;
    for _ in 0..20 {
        if limits.admit(t, &plan(5)).is_ok() {
            ok += 1;
        }
    }
    // The limit applies at once (the bucket starts from what it held, at most the new burst)...
    assert!((1..=5).contains(&ok), "rps 5 limits the next calls: {ok}");
    // ...and refills at 5 per second.
    clock.advance(Duration::from_secs(1));
    let refilled = (0..20).filter(|_| limits.admit(t, &plan(5)).is_ok()).count();
    assert_eq!(refilled, 5, "five per second");
    for _ in 0..100 {
        assert!(limits.admit(t, &plan(0)).is_ok(), "back to unlimited at once");
    }
}
