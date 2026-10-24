//! Clock hardening: SEC-22 (frozen clock, Deno half; the WASI clock half belongs to
//! `rt-exec-wasm`) and DNO-13 (`security.clock = "coarse"`).

use crate::common;

use std::time::{Duration, Instant};

use common::*;
use rt_config::ClockMode;

fn num(v: &serde_json::Value, key: &str) -> f64 {
    v[key]
        .as_f64()
        .unwrap_or_else(|| panic!("`{key}` missing or not a number in {v}"))
}

fn long_spec(path: &str) -> rt_types::TaskSpec {
    // The busy loop is iteration-based (a time-based loop would never end on a frozen clock) and
    // needs more than the default 1 s CPU in an unoptimized build.
    spec().cpu_limit(30_000).wall_limit(60_000).http_get(path).finish()
}

// ---------------------------------------------------------------------------------------------
// SEC-22
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_sec_22_a_50_ms_busy_loop_is_invisible_to_date_now_and_performance_now() {
    run_local(async {
        let mut h = Harness::new(cfg_with_clock(ClockMode::Frozen));
        h.warm_up().await;
        let p = h.prepare_fixture("clock_busy").await;

        let t0 = Instant::now();
        let ran = h.run(long_spec("/?mode=busy&iters=150000000"), &p).await; // JIT-compiled: ~30M iterations can finish in < 50 ms
        let got = read(ran.outcome).await;
        let real = t0.elapsed();

        assert_eq!(got.status, 200);
        assert!(
            real >= Duration::from_millis(50),
            "the loop must really burn >= 50 ms (took {real:?}); raise `iters`"
        );
        let v = got.json();
        assert_eq!(
            num(&v, "dateDelta"),
            0.0,
            "Date.now() must not advance during synchronous execution"
        );
        assert_eq!(
            num(&v, "perfDelta"),
            0.0,
            "performance.now() must not advance during synchronous execution"
        );
    });
}

#[test]
fn ac_sec_22_clocks_advance_at_timer_boundaries() {
    run_local(async {
        let mut h = Harness::new(cfg_with_clock(ClockMode::Frozen));
        h.warm_up().await;
        let p = h.prepare_fixture("clock_busy").await;

        let got = read(h.run(get("/?mode=timer&ms=10"), &p).await.outcome).await;
        let v = got.json();
        assert!(
            num(&v, "dateDelta") >= 10.0,
            "Date.now() advanced by {} after a 10 ms timer",
            num(&v, "dateDelta")
        );
        assert!(
            num(&v, "perfDelta") >= 10.0,
            "performance.now() advanced by {} after a 10 ms timer",
            num(&v, "perfDelta")
        );
        assert!(num(&v, "perfDelta") < 200.0, "but not wildly more than the timer");
    });
}

#[test]
fn sec_22_frozen_samples_never_change_within_one_synchronous_run() {
    run_local(async {
        let mut h = Harness::new(cfg_with_clock(ClockMode::Frozen));
        let p = h.prepare_fixture("clock_busy").await;
        let v = read(h.run(long_spec("/?mode=samples&n=1000"), &p).await.outcome)
            .await
            .json();
        let samples: Vec<f64> = v["samples"]
            .as_array()
            .expect("samples")
            .iter()
            .filter_map(|s| s.as_f64())
            .collect();
        assert_eq!(samples.len(), 1000);
        assert!(
            samples.windows(2).all(|w| w[0] == w[1]),
            "frozen clock: every sample is identical"
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-13
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_13_coarse_clock_deltas_are_multiples_of_1_ms() {
    run_local(async {
        let mut h = Harness::new(cfg_with_clock(ClockMode::Coarse));
        h.warm_up().await;
        let p = h.prepare_fixture("clock_busy").await;

        let v = read(h.run(long_spec("/?mode=samples&n=1000"), &p).await.outcome)
            .await
            .json();
        let samples: Vec<f64> = v["samples"]
            .as_array()
            .expect("samples")
            .iter()
            .filter_map(|s| s.as_f64())
            .collect();
        assert_eq!(samples.len(), 1000);

        let deltas: Vec<f64> = samples.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(deltas.iter().all(|d| *d >= 0.0), "monotonic");
        for d in &deltas {
            assert!((d - d.round()).abs() < 1e-9, "delta {d} is not a multiple of 1 ms");
        }
        assert!(
            deltas.iter().any(|d| *d >= 1.0),
            "coarse mode still ticks during synchronous execution (unlike frozen); 1000 samples took no time?"
        );
    });
}

#[test]
fn dno_13_coarse_clock_is_the_only_setting_that_changes_behaviour() {
    run_local(async {
        // Same fixture, same load, frozen: zero delta. Coarse: some elapsed time becomes visible.
        let mut frozen = Harness::new(cfg_with_clock(ClockMode::Frozen));
        let p = frozen.prepare_fixture("clock_busy").await;
        let f = read(frozen.run(long_spec("/?mode=busy&iters=20000000"), &p).await.outcome)
            .await
            .json();
        assert_eq!(num(&f, "perfDelta"), 0.0);

        let mut coarse = Harness::new(cfg_with_clock(ClockMode::Coarse));
        let p = coarse.prepare_fixture("clock_busy").await;
        let c = read(coarse.run(long_spec("/?mode=busy&iters=20000000"), &p).await.outcome)
            .await
            .json();
        assert!(
            num(&c, "perfDelta") >= 1.0,
            "coarse clock shows >= 1 ms for a 20M-iteration loop, got {}",
            num(&c, "perfDelta")
        );
        assert_eq!(num(&c, "perfDelta").fract(), 0.0);
    });
}
