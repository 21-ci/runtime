//! WSM-10 (perf, `#[ignore = "perf"]`): hello component, T1 hit, runtime overhead against SLO-3 for
//! WASM (p50 <= 50 us, p99 <= 250 us) and the SLO-1 ceiling.
//!
//! Run on the reference box in release mode:
//! `cargo test -p rt-exec-wasm --release --test it wsm_perf:: -- --ignored --nocapture`.
//! The measurement is `Executor::start` -> `Outcome` -> body read on one Machine thread, so it
//! includes the (small) harness cost and bounds the real overhead from above. SLO-1 is 50,000 RPS
//! on the 4-vCPU reference box: one Machine must sustain a quarter of that.

use crate::common;

use std::time::{Duration, Instant};

use common::{prepared, Req, Rig};

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::test]
#[ignore = "perf"]
async fn ac_wsm_10_hello_overhead_within_slo_3_and_ceiling_slo_1() {
    let mut rig = Rig::with(1024, Duration::ZERO);
    let hello = prepared("hello"); // T1 hit: compiled and linked once
    for _ in 0..500 {
        rig.run(&hello, Req::get("/")).await.reply();
    }

    const N: usize = 20_000;
    let mut latencies = Vec::with_capacity(N);
    let started = Instant::now();
    for _ in 0..N {
        let t = Instant::now();
        let done = rig.run(&hello, Req::get("/")).await;
        latencies.push(t.elapsed());
        assert_eq!(done.reply().status, 200);
    }
    let total = started.elapsed();
    latencies.sort();
    let (p50, p99) = (percentile(&latencies, 0.50), percentile(&latencies, 0.99));
    let rps = N as f64 / total.as_secs_f64();
    eprintln!("wsm-10: p50 = {p50:?}, p99 = {p99:?}, {rps:.0} req/s on one Machine");

    if cfg!(debug_assertions) {
        eprintln!("wsm-10: debug build, SLOs not asserted (use --release on the reference box)");
        return;
    }
    assert!(p50 <= Duration::from_micros(50), "SLO-3 p50 {p50:?} > 50 us");
    assert!(p99 <= Duration::from_micros(250), "SLO-3 p99 {p99:?} > 250 us");
    assert!(
        rps >= 50_000.0 / 4.0,
        "SLO-1: {rps:.0} req/s on one Machine, need >= 12,500 (50,000 on 4 vCPUs)"
    );
}
