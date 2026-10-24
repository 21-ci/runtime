//! DNO-15: V8 must not compete with the Ps. Own test binary (= own process): V8's platform is
//! initialized once per process, so the thread count under test is exactly this file's config.
//!
//! Linux only (`/proc/self/task`); elsewhere the test is ignored.

mod common;

use std::time::{Duration, Instant};

use common::*;
use rt_exec_deno::DenoExecutorConfig;

/// `(comm, cpu ticks)` of every thread of this process.
fn threads() -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task").expect("/proc/self/task (Linux only)") {
        let dir = entry.expect("task entry").path();
        let comm = std::fs::read_to_string(dir.join("comm"))
            .unwrap_or_default()
            .trim()
            .to_string();
        let stat = std::fs::read_to_string(dir.join("stat")).unwrap_or_default();
        // Fields after the `(comm)` part: state is index 0, utime index 11, stime index 12.
        let ticks = stat
            .rsplit_once(')')
            .map(|(_, rest)| {
                let f: Vec<&str> = rest.split_whitespace().collect();
                let n = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
                n(11) + n(12)
            })
            .unwrap_or(0);
        out.push((comm, ticks));
    }
    out
}

fn v8_threads() -> Vec<(String, u64)> {
    threads().into_iter().filter(|(c, _)| c.starts_with("V8 ")).collect()
}

/// USER_HZ on Linux.
const CLK_TCK: f64 = 100.0;

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "needs /proc/self/task")]
fn ac_dno_15_v8_platform_threads_match_config_and_stay_idle_under_load() {
    run_local(async {
        let cfg = DenoExecutorConfig {
            v8_platform_threads: 1,
            isolate_pool_per_p: 1,
            ..DenoExecutorConfig::default()
        };
        let mut h = Harness::new(cfg.clone());
        let hello = h.prepare_fixture("hello").await;
        let counter = h.prepare_fixture("global_counter").await;
        let _ = h.get("/", &hello).await; // V8 fully up

        // Thread count: exactly `v8_platform_threads` worker threads (V8 also keeps one delayed-
        // task scheduler thread; nothing else may be called V8).
        let before = v8_threads();
        let workers = before.iter().filter(|(c, _)| c.starts_with("V8 DefaultWorke")).count();
        assert_eq!(
            workers, cfg.v8_platform_threads as usize,
            "V8 platform workers: {before:?}"
        );
        assert!(
            before.len() <= cfg.v8_platform_threads as usize + 1,
            "V8 background threads beyond workers + delayed scheduler: {before:?}"
        );

        // Load: ~2 s of steady Deno traffic with idle-time pool refills in between.
        let start = Instant::now();
        let mut n = 0u32;
        while start.elapsed() < Duration::from_secs(2) {
            let p = if n.is_multiple_of(2) { &hello } else { &counter };
            let (got, _) = h.get("/", p).await;
            assert_eq!(got.status, 200);
            h.exec.on_idle(Duration::from_millis(5));
            n += 1;
        }
        let wall = start.elapsed().as_secs_f64();
        let after = v8_threads();

        assert_eq!(
            after.iter().filter(|(c, _)| c.starts_with("V8 DefaultWorke")).count(),
            cfg.v8_platform_threads as usize,
            "thread count must not grow under load: {after:?}"
        );
        for (comm, ticks_after) in &after {
            let ticks_before = before.iter().find(|(c, _)| c == comm).map(|(_, t)| *t).unwrap_or(0);
            let share = (*ticks_after - ticks_before) as f64 / CLK_TCK / wall;
            assert!(
                share <= 0.05,
                "V8 thread `{comm}` used {:.1}% CPU under load (limit 5%)",
                share * 100.0
            );
        }
        assert!(n >= 20, "the load loop only managed {n} tasks");
    });
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "needs /proc/self/task")]
fn dno_15_gc_helper_threads_are_not_created() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let oom = h.prepare_fixture("oom").await;
        // Force several GCs: `--single-threaded-gc` means no concurrent-marking helper threads.
        let _ = h.run(spec().memory_mb(24).http_get("/").finish(), &oom).await;
        let names: Vec<String> = threads().into_iter().map(|(c, _)| c).collect();
        let v8_like = names.iter().filter(|c| c.starts_with("V8 ")).count();
        assert!(v8_like <= 2, "unexpected V8 threads: {names:?}");
    });
}
