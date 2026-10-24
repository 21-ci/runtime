//! Per-service HDR stats (OBS-08): exact counts, percentile accuracy
//! within 5 %, outcome breakdown, reporting windows over a 24 h ring, thread-local shards merged
//! into the shared store.
//!
//! `HdrStatsStore` is `todo!()` in the skeleton: tests that construct it are expected to fail
//! until implemented. `Window` is real.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rt_obs::{HdrStatsStore, Percentiles, ServiceStats, StatsStore, Window};
use rt_types::{BuildFailReason, BuildFailure, KillReason, Outcome, RejectReason, Reply, ServiceId, TaskTimings};

const T0: i64 = 1_760_000_000_000 / 60_000 * 60_000; // minute aligned
const MINUTE: i64 = 60_000;

struct Fixture {
    store: Arc<HdrStatsStore>,
    now: Arc<AtomicI64>,
}

fn fixture() -> Fixture {
    let now = Arc::new(AtomicI64::new(T0));
    let clock = now.clone();
    let store = HdrStatsStore::new(Arc::new(move || clock.load(Ordering::SeqCst)));
    Fixture { store, now }
}

impl Fixture {
    fn advance(&self, ms: i64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
    fn stats(&self, svc: ServiceId, w: Window) -> ServiceStats {
        self.store.merge_shards();
        self.store.window(svc, w)
    }
}

fn ms(n: f64) -> Duration {
    Duration::from_secs_f64(n / 1000.0)
}

fn timings(queue_ms: f64, init_ms: f64, exec_ms: f64) -> TaskTimings {
    TaskTimings {
        queue: ms(queue_ms),
        init: ms(init_ms),
        exec: ms(exec_ms),
        ..TaskTimings::default()
    }
}

fn ok() -> Outcome {
    Outcome::Ok(Reply::ok())
}

/// `got` within 5 % of `want` (OBS-08).
fn within_5pct(got: f64, want: f64, what: &str) {
    assert!(
        (got - want).abs() <= want * 0.05,
        "{what}: got {got}, expected {want} +/- 5 %"
    );
}

fn record(f: &Fixture, svc: ServiceId, t: &TaskTimings, o: &Outcome) {
    f.store.record(svc, t, o);
}

// ---------------------------------------------------------------------------------------------
// Window (real code)
// ---------------------------------------------------------------------------------------------

#[test]
fn windows_parse_the_documented_spellings_and_have_the_right_length() {
    assert_eq!(Window::parse("5m"), Some(Window::M5));
    assert_eq!(Window::parse("1h"), Some(Window::H1));
    assert_eq!(Window::parse("24h"), Some(Window::H24));
    for bad in ["", "5", "5M", "1m", "10m", "60m", "1d", "24", "h", " 5m", "5m "] {
        assert_eq!(Window::parse(bad), None, "`{bad}`");
    }
    assert_eq!(Window::M5.duration(), Duration::from_secs(300));
    assert_eq!(Window::H1.duration(), Duration::from_secs(3_600));
    assert_eq!(Window::H24.duration(), Duration::from_secs(86_400));
    for (w, s) in [(Window::M5, "\"5m\""), (Window::H1, "\"1h\""), (Window::H24, "\"24h\"")] {
        assert_eq!(serde_json::to_string(&w).unwrap(), s);
        assert_eq!(serde_json::from_str::<Window>(s).unwrap(), w);
    }
}

#[test]
fn the_response_shape_has_counts_rate_outcomes_and_four_percentile_groups() {
    let v = serde_json::to_value(ServiceStats::default()).unwrap();
    for key in [
        "count",
        "errors",
        "error_rate",
        "outcomes",
        "queue",
        "init",
        "exec",
        "total",
    ] {
        assert!(v.get(key).is_some(), "stats response lacks `{key}`: {v}");
    }
    for group in ["queue", "init", "exec", "total"] {
        for p in ["p50_ms", "p95_ms", "p99_ms"] {
            assert!(v[group].get(p).is_some(), "{group}.{p} missing");
        }
    }
    assert_eq!(
        Percentiles::default(),
        Percentiles {
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0
        }
    );
}

// ---------------------------------------------------------------------------------------------
// OBS-08: known load
// ---------------------------------------------------------------------------------------------

#[test]
fn obs_08_counts_are_exact_and_percentiles_are_within_five_percent_of_the_generator() {
    let f = fixture();
    let svc = ServiceId::new();
    // exec latencies 1..=1000 ms (uniform); constant queue 2 ms and init 3 ms
    for i in 1..=1_000 {
        record(&f, svc, &timings(2.0, 3.0, i as f64), &ok());
    }
    let s = f.stats(svc, Window::M5);
    assert_eq!(s.count, 1_000, "counts are exact");
    assert_eq!(s.errors, 0);
    assert_eq!(s.error_rate, 0.0);
    assert_eq!(s.outcomes.get("ok"), Some(&1_000));

    within_5pct(s.exec.p50_ms, 500.0, "exec p50");
    within_5pct(s.exec.p95_ms, 950.0, "exec p95");
    within_5pct(s.exec.p99_ms, 990.0, "exec p99");
    for (name, p) in [("queue", s.queue), ("init", s.init)] {
        let want = if name == "queue" { 2.0 } else { 3.0 };
        within_5pct(p.p50_ms, want, &format!("{name} p50"));
        within_5pct(p.p95_ms, want, &format!("{name} p95"));
        within_5pct(p.p99_ms, want, &format!("{name} p99"));
    }
    // total = queue + wait_runtime + init + exec = exec + 5 ms
    within_5pct(s.total.p50_ms, 505.0, "total p50");
    within_5pct(s.total.p95_ms, 955.0, "total p95");
    within_5pct(s.total.p99_ms, 995.0, "total p99");
}

#[test]
fn percentiles_are_ordered_and_follow_the_recorded_distribution() {
    let f = fixture();
    let svc = ServiceId::new();
    // 980 fast tasks of 10 ms and 20 slow ones of 2,000 ms: p50/p95 fast, p99 slow
    for _ in 0..980 {
        record(&f, svc, &timings(0.0, 0.0, 10.0), &ok());
    }
    for _ in 0..20 {
        record(&f, svc, &timings(0.0, 0.0, 2_000.0), &ok());
    }
    let s = f.stats(svc, Window::M5);
    assert_eq!(s.count, 1_000);
    within_5pct(s.exec.p50_ms, 10.0, "p50");
    within_5pct(s.exec.p95_ms, 10.0, "p95");
    within_5pct(s.exec.p99_ms, 2_000.0, "p99");
    assert!(s.exec.p50_ms <= s.exec.p95_ms && s.exec.p95_ms <= s.exec.p99_ms);
}

#[test]
fn sub_millisecond_latencies_are_resolved() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..200 {
        record(&f, svc, &timings(0.0, 0.0, 0.25), &ok());
    }
    let s = f.stats(svc, Window::M5);
    within_5pct(s.exec.p50_ms, 0.25, "250 us p50");
    within_5pct(s.exec.p99_ms, 0.25, "250 us p99");
}

#[test]
fn latencies_up_to_a_build_budget_do_not_overflow_the_histogram() {
    let f = fixture();
    let svc = ServiceId::new();
    record(&f, svc, &timings(0.0, 0.0, 120_000.0), &ok()); // 2 minutes: the longest default wall (builds)
    record(&f, svc, &timings(0.0, 0.0, 10.0), &ok());
    let s = f.stats(svc, Window::M5);
    assert_eq!(s.count, 2);
    within_5pct(s.exec.p99_ms, 120_000.0, "p99");
}

#[test]
fn outcomes_are_counted_per_label_and_errors_are_the_failures() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..900 {
        record(&f, svc, &timings(1.0, 1.0, 5.0), &ok());
    }
    for _ in 0..50 {
        record(&f, svc, &timings(1.0, 1.0, 5.0), &Outcome::GuestError);
    }
    for _ in 0..30 {
        record(&f, svc, &timings(1.0, 1.0, 5.0), &Outcome::Killed(KillReason::CpuLimit));
    }
    for _ in 0..20 {
        record(&f, svc, &timings(1.0, 0.0, 0.0), &Outcome::Rejected(RejectReason::Shed));
    }
    let s = f.stats(svc, Window::M5);
    assert_eq!(s.count, 1_000);
    assert_eq!(s.outcomes.get("ok"), Some(&900));
    assert_eq!(s.outcomes.get("guest_error"), Some(&50));
    assert_eq!(s.outcomes.get("cpu_limit"), Some(&30));
    assert_eq!(s.outcomes.get("shed"), Some(&20));
    assert_eq!(
        s.outcomes.values().sum::<u64>(),
        s.count,
        "the breakdown partitions the count"
    );
    assert!(
        s.errors >= 80 && s.errors <= 100,
        "guest errors and kills are errors; rejects may be: got {}",
        s.errors
    );
    assert!(
        (s.error_rate - s.errors as f64 / s.count as f64).abs() < 1e-9,
        "error_rate = errors / count"
    );
}

#[test]
fn outcome_labels_in_the_breakdown_come_from_outcome_label() {
    let f = fixture();
    let svc = ServiceId::new();
    let outcomes = [
        Outcome::Killed(KillReason::Oom),
        Outcome::Killed(KillReason::WallTimeout),
        Outcome::Rejected(RejectReason::RateLimited),
        Outcome::BuildFailed(BuildFailure::new(BuildFailReason::Syntax, "x")),
    ];
    let labels: Vec<&str> = outcomes.iter().map(Outcome::label).collect();
    for o in &outcomes {
        record(&f, svc, &timings(0.0, 0.0, 1.0), o);
    }
    let s = f.stats(svc, Window::M5);
    for l in labels {
        assert_eq!(s.outcomes.get(l), Some(&1), "outcome `{l}`");
    }
}

#[test]
fn an_idle_or_unknown_service_reports_zeros_not_nan() {
    let f = fixture();
    let s = f.stats(ServiceId::new(), Window::H24);
    assert_eq!(s, ServiceStats::default());
    assert_eq!((s.count, s.errors), (0, 0));
    assert_eq!(s.error_rate, 0.0, "0/0 must not become NaN");
    assert!(s.outcomes.is_empty());
    assert_eq!(s.exec, Percentiles::default());
}

#[test]
fn services_are_accounted_separately() {
    let f = fixture();
    let (a, b) = (ServiceId::new(), ServiceId::new());
    for _ in 0..30 {
        record(&f, a, &timings(0.0, 0.0, 10.0), &ok());
    }
    for _ in 0..7 {
        record(&f, b, &timings(0.0, 0.0, 400.0), &Outcome::GuestError);
    }
    let (sa, sb) = (f.stats(a, Window::M5), f.stats(b, Window::M5));
    assert_eq!((sa.count, sb.count), (30, 7));
    assert_eq!((sa.errors, sb.errors), (0, 7));
    within_5pct(sa.exec.p50_ms, 10.0, "a");
    within_5pct(sb.exec.p50_ms, 400.0, "b");
}

// ---------------------------------------------------------------------------------------------
// Thread-local shards
// ---------------------------------------------------------------------------------------------

#[test]
fn records_from_many_threads_are_merged_exactly_even_after_the_threads_exited() {
    let f = fixture();
    let svc = ServiceId::new();
    std::thread::scope(|s| {
        for t in 0..8 {
            let store = f.store.clone();
            s.spawn(move || {
                for i in 0..500 {
                    store.record(
                        svc,
                        &timings(0.0, 0.0, 1.0 + ((t * 500 + i) % 100) as f64),
                        &Outcome::Ok(Reply::ok()),
                    );
                }
            });
        }
    }); // all recording threads have ended before the merge
    let s = f.stats(svc, Window::M5);
    assert_eq!(s.count, 4_000, "shards of finished threads must not be lost");
    assert_eq!(s.outcomes.get("ok"), Some(&4_000));
    within_5pct(s.exec.p50_ms, 50.0, "p50 of 1..=100 ms");
    within_5pct(s.exec.p99_ms, 99.0, "p99 of 1..=100 ms");
}

#[test]
fn merging_twice_does_not_double_count() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..10 {
        record(&f, svc, &timings(0.0, 0.0, 5.0), &ok());
    }
    f.store.merge_shards();
    f.store.merge_shards();
    assert_eq!(f.store.window(svc, Window::M5).count, 10);
    for _ in 0..5 {
        record(&f, svc, &timings(0.0, 0.0, 5.0), &ok());
    }
    f.store.merge_shards();
    assert_eq!(
        f.store.window(svc, Window::M5).count,
        15,
        "later records are added by the next merge"
    );
}

// ---------------------------------------------------------------------------------------------
// Windows over a 24 h ring of 1-minute buckets
// ---------------------------------------------------------------------------------------------

#[test]
fn old_records_age_out_of_each_window_at_its_own_pace() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..100 {
        record(&f, svc, &timings(0.0, 0.0, 10.0), &ok());
    }
    assert_eq!(f.stats(svc, Window::M5).count, 100);

    f.advance(10 * MINUTE);
    assert_eq!(f.stats(svc, Window::M5).count, 0, "10 minutes old: outside 5m");
    assert_eq!(f.stats(svc, Window::H1).count, 100, "...but inside 1h");
    assert_eq!(f.stats(svc, Window::H24).count, 100);

    f.advance(2 * 60 * MINUTE);
    assert_eq!(f.stats(svc, Window::H1).count, 0, "2 h old: outside 1h");
    assert_eq!(f.stats(svc, Window::H24).count, 100, "...but inside 24h");

    f.advance(23 * 60 * MINUTE);
    assert_eq!(
        f.stats(svc, Window::H24).count,
        0,
        "25 h old: dropped from the 24 h ring"
    );
}

#[test]
fn a_window_aggregates_all_the_buckets_it_covers() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..50 {
        record(&f, svc, &timings(0.0, 0.0, 10.0), &ok());
    }
    f.advance(4 * MINUTE);
    for _ in 0..50 {
        record(&f, svc, &timings(0.0, 0.0, 10.0), &Outcome::GuestError);
    }
    let both = f.stats(svc, Window::M5);
    assert_eq!(
        both.count, 100,
        "minute 0 and minute 4 are both inside the last 5 minutes"
    );
    assert_eq!(both.errors, 50);
    assert!((both.error_rate - 0.5).abs() < 1e-9);

    f.advance(2 * MINUTE); // now minute 6: only the second batch is within 5 minutes
    let later = f.stats(svc, Window::M5);
    assert_eq!(later.count, 50);
    assert_eq!(later.errors, 50);
    assert_eq!(f.stats(svc, Window::H1).count, 100);
}

#[test]
fn percentiles_of_a_window_reflect_only_the_buckets_inside_it() {
    let f = fixture();
    let svc = ServiceId::new();
    for _ in 0..100 {
        record(&f, svc, &timings(0.0, 0.0, 1_000.0), &ok()); // slow, old
    }
    f.advance(30 * MINUTE);
    for _ in 0..100 {
        record(&f, svc, &timings(0.0, 0.0, 10.0), &ok()); // fast, recent
    }
    let recent = f.stats(svc, Window::M5);
    within_5pct(recent.exec.p99_ms, 10.0, "5m p99 ignores the old slow bucket");
    let hour = f.stats(svc, Window::H1);
    assert_eq!(hour.count, 200);
    within_5pct(hour.exec.p99_ms, 1_000.0, "1h p99 includes it");
}
