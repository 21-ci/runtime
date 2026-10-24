//! DNO-14: runtime overhead of the Deno executor (SLO-3: p50 <= 1.5 ms, p99 <= 4 ms on the
//! reference box, T1 hit, empty handler, measured via `server-timing`).
//!
//! `cargo test -p rt-exec-deno --release --test it perf:: -- --ignored`

use crate::common;

use std::time::{Duration, Instant};

use common::*;
use rt_types::ServiceKnobs;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

#[test]
#[ignore = "perf"]
fn ac_dno_14_overhead_within_slo_3() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(1)); // balanced profile
        h.warm_up().await;
        let p = h.prepare_fixture("hello").await;

        let mut overhead = Vec::with_capacity(2000);
        for _ in 0..2000 {
            let ran = h.run(get("/"), &p).await;
            let got = read(ran.outcome).await;
            overhead.push(got.timings.total());
            // The P is idle between requests: refill the pool and dispose, as the Machine does.
            h.exec.on_idle(Duration::from_millis(2));
        }
        overhead.sort();
        let p50 = percentile(&overhead, 0.50);
        let p99 = percentile(&overhead, 0.99);
        eprintln!("DNO-14 overhead p50={p50:?} p99={p99:?} (SLO-3 Deno: 1.5 ms / 4 ms)");
        assert!(p50 <= Duration::from_micros(1500), "p50 {p50:?}");
        assert!(p99 <= Duration::from_millis(4), "p99 {p99:?}");
    });
}

/// D-A4 probe: sequential request latency with fresh isolates vs warm reuse, for an empty
/// handler and for a module whose top-level code does real work (like an npm SDK's init).
/// Prints p50/p99; a debug build gives relative numbers only. Run with `--ignored --nocapture`.
#[test]
#[ignore = "perf"]
fn d_a4_probe_reuse_vs_fresh_isolates() {
    const HELLO: &str = "export default () => new Response('hello');\n";
    const HEAVY: &str = "const table: Record<string, number> = {};
for (let i = 0; i < 50000; i++) table['k' + i] = i * 2;
const big = JSON.stringify(table);
export default () => new Response(String(big.length));
";
    run_local(async {
        for (name, src) in [("hello", HELLO), ("heavy top-level", HEAVY)] {
            for reuse in [false, true] {
                let mut h = Harness::new(cfg_with_pool(1));
                h.warm_up().await;
                let p = h.prepare(&ts_upload(src)).await;
                let knobs = ServiceKnobs {
                    reuse_isolates: reuse,
                    ..ServiceKnobs::default()
                };
                let mut times = Vec::with_capacity(300);
                for _ in 0..300 {
                    let t = Instant::now();
                    let ran = h.run(spec().knobs(knobs).http_get("/").finish(), &p).await;
                    read(ran.outcome).await;
                    times.push(t.elapsed());
                    // the driver parks or disposes right after the body ends; then the P's hook
                    tokio::task::yield_now().await;
                    tokio::task::yield_now().await;
                    h.exec.between_tasks(Duration::from_millis(2));
                }
                times.sort();
                eprintln!(
                    "D-A4 {name:16} reuse={reuse:5}: p50={:?} p99={:?} kept={}",
                    percentile(&times, 0.50),
                    percentile(&times, 0.99),
                    h.exec.warm_len()
                );
            }
        }
    });
}
