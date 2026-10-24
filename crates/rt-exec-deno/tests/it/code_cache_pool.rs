//! Prepared bundles in the executor: V8 code cache accounting (DNO-10, BLD-13 execution half),
//! the pre-created isolate pool (DNO-11) and remote dependencies running from the bundle
//! (BLD-04 execution half: `200` on the first request after the build).

use crate::common;

use std::time::Duration;

use common::*;
use rt_build::CodeCache;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------------------------
// DNO-10 / BLD-13
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_dno_10_code_cache_hit_is_counted_when_a_task_starts() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg0(), metrics.clone());
        let p = h.prepare_fixture("hello").await;
        assert!(p.code_cache.is_some(), "the build produced a code cache");
        assert_eq!(code_cache_count(&metrics, "hit"), 0.0);

        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.text(), "hello");
        assert_eq!(
            code_cache_count(&metrics, "hit"),
            1.0,
            "rt_deno_code_cache_total{{result=\"hit\"}} increments"
        );
        assert_eq!(code_cache_count(&metrics, "rejected"), 0.0);
        assert_eq!(code_cache_count(&metrics, "miss"), 0.0);

        let _ = h.get("/", &p).await;
        assert_eq!(code_cache_count(&metrics, "hit"), 2.0);
    });
}

#[test]
fn ac_dno_10_rejected_code_cache_falls_back_to_compile_and_counts_rejected() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg0(), metrics.clone());
        let good = h.prepare_fixture("hello").await;

        // Same bundle, but every cached blob is corrupted: V8 must reject it.
        let mut bad = good.code_cache.clone().expect("cache");
        assert!(!bad.per_module.is_empty());
        for blob in bad.per_module.values_mut() {
            for b in blob.iter_mut() {
                *b ^= 0xFF;
            }
        }
        let p = with_code_cache(&good, Some(bad));
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.status, 200);
        assert_eq!(
            got.text(),
            "hello",
            "a rejected cache falls back to compiling from source"
        );
        assert_eq!(code_cache_count(&metrics, "rejected"), 1.0);
        assert_eq!(code_cache_count(&metrics, "hit"), 0.0);
    });
}

#[test]
fn dno_10_cache_from_another_engine_is_rejected() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg0(), metrics.clone());
        let good = h.prepare_fixture("hello").await;
        let mut other = good.code_cache.clone().expect("cache");
        other.engine_fp = other.engine_fp.wrapping_add(1);
        let (got, _) = h.get("/", &with_code_cache(&good, Some(other))).await;
        assert_eq!(got.text(), "hello");
        assert_eq!(
            code_cache_count(&metrics, "rejected"),
            1.0,
            "a different engine fingerprint is never loaded"
        );
        assert_eq!(code_cache_count(&metrics, "hit"), 0.0);
    });
}

#[test]
fn dno_10_missing_code_cache_counts_a_miss() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg0(), metrics.clone());
        let good = h.prepare_fixture("hello").await;
        let (got, _) = h.get("/", &with_code_cache(&good, None)).await;
        assert_eq!(got.text(), "hello");
        assert_eq!(code_cache_count(&metrics, "miss"), 1.0);
        assert_eq!(code_cache_count(&metrics, "hit"), 0.0);
    });
}

#[test]
fn ac_bld_13_code_cache_of_a_built_bundle_is_accepted_when_executed() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg0(), metrics.clone());
        // A multi-module bundle: every module needs its own accepted blob.
        let p = h.prepare_fixture("multi_file").await;

        let cc: &CodeCache = p
            .code_cache
            .as_ref()
            .expect("the build stored a code cache next to the bundle");
        assert_eq!(
            cc.engine_fp, h.engine_fp,
            "produced under the executor's engine fingerprint"
        );
        let mut want: Vec<&str> = p
            .bundle
            .modules
            .iter()
            .filter(|m| m.kind == rt_build::ModuleKind::Js)
            .map(|m| m.specifier.as_str())
            .collect();
        want.sort();
        let mut got: Vec<&str> = cc.per_module.keys().map(String::as_str).collect();
        got.sort();
        assert_eq!(got, want, "one blob per JS module");
        assert!(
            cc.per_module.values().all(|b| !b.is_empty()),
            "V8 produced real cache data"
        );

        let (r, _) = h.get("/?name=x", &p).await;
        assert_eq!(r.status, 200);
        assert!(
            code_cache_count(&metrics, "hit") >= 1.0,
            "code cache accepted (see DNO-10)"
        );
        assert_eq!(code_cache_count(&metrics, "rejected"), 0.0);
    });
}

// ---------------------------------------------------------------------------------------------
// BLD-04 execution half
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_bld_04_service_importing_npm_zod_answers_200_from_the_bundle() {
    run_local(async {
        let cdn = cdn_with_zod().await;
        let mut h = Harness::new(cfg0());
        let upload = fixture_upload("npm_zod");
        let p = h.prepare_with_cdn(&upload, &cdn).await;
        let hits_after_build = cdn.total_hits();
        assert_eq!(
            hits_after_build, 2,
            "entry + transitive module fetched once, at build time"
        );

        let ok = spec()
            .payload(post_payload("/", "application/json", r#"{"title":"t","done":true}"#))
            .finish();
        let got = read(h.run(ok, &p).await.outcome).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.json(), json!({"ok": true, "todo": {"title": "t", "done": true}}));

        let bad = spec()
            .payload(post_payload("/", "application/json", r#"{"title":1}"#))
            .finish();
        let got = read(h.run(bad, &p).await.outcome).await;
        assert_eq!(got.status, 400, "the bundled zod validates");

        assert_eq!(cdn.total_hits(), hits_after_build, "nothing is fetched at run time");
    });
}

#[test]
fn bld_04_import_map_flavour_runs_too() {
    run_local(async {
        let cdn = cdn_with_zod().await;
        let mut h = Harness::new(cfg0());
        let p = h.prepare_with_cdn(&fixture_upload("npm_zod_map"), &cdn).await;
        let (got, _) = h.get("/?name=ada", &p).await;
        assert_eq!(got.json(), json!({"ok": true}));
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-11 isolate pool
// ---------------------------------------------------------------------------------------------

fn names_of(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .expect(key)
        .iter()
        .filter_map(|s| s.as_str().map(String::from))
        .collect()
}

#[test]
fn ac_dno_11_pool_refills_on_idle_and_stays_at_its_cap() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(1));
        assert_eq!(
            h.exec.pool_len(),
            0,
            "nothing is pre-created before the Machine is idle"
        );
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.pool_len(), 1, "idle P refills the pool");
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.pool_len(), 1, "never above deno.isolate_pool_per_p");

        let p = h.prepare_fixture("hello").await;
        let _ = h.get("/", &p).await;
        assert_eq!(h.exec.pool_len(), 0, "pooled isolates are single-use");
        h.settle(Duration::from_millis(20)).await;
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.pool_len(), 1);
    });
}

#[test]
fn ac_dno_11_pooled_isolates_hold_only_base_snapshot_state_after_100_tenants() {
    run_local(async {
        // Reference: a fresh isolate straight from the base snapshot (no pool).
        let (names0, proto0) = {
            let mut fresh = Harness::new(cfg0());
            let dump = fresh.prepare_fixture("global_dump").await;
            let v = fresh.get("/", &dump).await.0.json();
            (names_of(&v, "names"), names_of(&v, "protoKeys"))
        };
        assert!(!names0.iter().any(|n| n.starts_with("__leak_")));

        let mut h = Harness::new(cfg_with_pool(1));
        let dump = h.prepare_fixture("global_dump").await;
        let counter = h.prepare_fixture("global_counter").await;

        for i in 0..100u32 {
            let tenant = u128::from(i % 7) + 1;
            h.exec.on_idle(Duration::from_millis(100)); // the P is idle: refill the pool
            assert_eq!(h.exec.pool_len(), 1, "iteration {i}");

            // Tenant `tenant` dirties the global scope and the Object prototype...
            let dirty = spec()
                .tenant(tenant)
                .service(tenant)
                .http_get(&format!("/?dirty={i}"))
                .finish();
            assert_eq!(read(h.run(dirty, &dump).await.outcome).await.text(), "dirtied");
            // ...and bumps a global counter.
            let bump = spec().tenant(tenant).service(tenant).http_get("/").finish();
            assert_eq!(read(h.run(bump, &counter).await.outcome).await.text(), "1");
            h.settle(Duration::from_millis(5)).await;

            if i % 10 == 9 {
                // Another tenant takes a pooled isolate and must see exactly the base snapshot.
                let other = tenant + 100;
                let probe = spec().tenant(other).service(other).http_get("/").finish();
                let v = read(h.run(probe, &dump).await.outcome).await.json();
                assert_eq!(
                    names_of(&v, "names"),
                    names0,
                    "iteration {i}: global property set differs from the base snapshot"
                );
                assert_eq!(
                    names_of(&v, "protoKeys"),
                    proto0,
                    "iteration {i}: Object.prototype was polluted"
                );
            }
        }
    });
}

#[test]
fn dno_11_pool_size_follows_the_profile() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(4));
        h.exec.on_idle(Duration::from_millis(500));
        assert!(h.exec.pool_len() <= 4);
        assert!(h.exec.pool_len() >= 1, "min_latency pre-creates isolates when idle");

        let mut off = Harness::new(cfg0());
        off.exec.on_idle(Duration::from_millis(200));
        assert_eq!(off.exec.pool_len(), 0, "max_density keeps no pool");
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-11 / B4.1: the pool refills between tasks (a loaded Machine never parks, so `on_idle`
// never runs), holds only the base heap, and gives way to memory pressure
// ---------------------------------------------------------------------------------------------

/// Tasks that had to create their isolate on their own path (no pooled isolate was waiting).
fn inline_creations(m: &rt_obs::Metrics) -> u64 {
    m.deno_isolate_create_seconds.get_sample_count()
}

#[test]
fn dno_11_back_to_back_tasks_are_served_from_a_pool_refilled_between_tasks() {
    run_local(async {
        let metrics = new_metrics();
        let mut h = Harness::with_metrics(cfg_with_pool(2), metrics.clone());
        let p = h.prepare_fixture("hello").await;
        let n = 20;
        for i in 0..n {
            let (got, _) = h.get("/", &p).await;
            assert_eq!(got.text(), "hello", "task {i}");
            h.settle(Duration::from_millis(5)).await; // the driver ends: its isolate waits for disposal
                                                      // What the scheduler does when the Machine finds no task (it never parks: no `on_idle`).
            h.exec.between_tasks(rt_sched::BETWEEN_TASKS_BUDGET);
        }
        let created = inline_creations(&metrics);
        assert!(
            created <= 1,
            "{created} of {n} back-to-back tasks created their isolate inline"
        );
        assert!(h.exec.pool_len() >= 1, "the pool is refilled after every task");
        // One unit per call goes to the pool; disposal waits for idle time, bounded by the backlog
        // (beyond it an isolate is disposed after its task, DNO-16).
        assert!(
            h.exec.dispose_backlog_len() <= h.cfg.dispose_backlog as usize,
            "disposal stays bounded: backlog {}",
            h.exec.dispose_backlog_len()
        );
    });
}

#[test]
fn dno_11_pooled_isolates_reserve_only_the_base_heap_and_grow_when_a_task_attaches() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(4));
        let base = u64::from(h.cfg.isolate_base_mb) * MIB;
        h.exec.on_idle(Duration::from_millis(5_000));
        assert_eq!(h.exec.pool_len(), 4);
        assert_eq!(
            h.gov.committed(),
            4 * base,
            "a pooled isolate reserves deno.isolate_base_mb, not the V8 heap floor"
        );

        // A task takes one: its reservation grows to the V8 heap floor at attach, and it is served.
        let p = h.prepare_fixture("hello").await;
        let ran = h.run(get("/"), &p).await;
        assert!(
            h.gov.committed() > 4 * base,
            "the attached isolate grew to its V8 heap limit"
        );
        assert_eq!(read(ran.outcome).await.text(), "hello");

        h.settle(Duration::from_millis(20)).await;
        h.exec.on_idle(Duration::from_millis(5_000));
        assert_eq!(h.exec.pool_len(), 4);
        assert_eq!(h.gov.committed(), 4 * base, "back to the pool's base reservation");
    });
}

#[test]
fn dno_11_the_pool_is_shed_under_memory_pressure_and_not_refilled() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(4));
        h.exec.on_idle(Duration::from_millis(5_000));
        assert_eq!(h.exec.pool_len(), 4);

        // >= 80% of the 512 MB test budget: `Pressure::High`.
        let hog = h
            .gov
            .reserve(rt_mem::MemCategory::Other, 420 * MIB, rt_mem::Owner::NONE)
            .expect("fill the budget");
        h.exec.between_tasks(rt_sched::BETWEEN_TASKS_BUDGET);
        assert_eq!(h.exec.pool_len(), 0, "idle pooled isolates are evictable");
        h.exec.on_idle(Duration::from_millis(200));
        assert_eq!(h.exec.pool_len(), 0, "no refill while the node is under pressure");
        assert_eq!(h.exec.dispose_backlog_len(), 0);

        drop(hog);
        h.exec.on_idle(Duration::from_millis(5_000));
        assert_eq!(h.exec.pool_len(), 4, "refilled once the pressure is gone");
    });
}

#[test]
fn dno_11_between_tasks_does_one_unit_per_round() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(2));
        let p = h.prepare_fixture("hello").await;
        for _ in 0..3 {
            let _ = h.get("/", &p).await;
        }
        h.settle(Duration::from_millis(30)).await;
        assert_eq!((h.exec.dispose_backlog_len(), h.exec.pool_len()), (3, 0));
        // A zero budget allows exactly one unit: the empty pool first (the next task's isolate),
        // then the queued disposals one by one, then the rest of the pool.
        let mut seen = Vec::new();
        for _ in 0..5 {
            h.exec.between_tasks(Duration::ZERO);
            seen.push((h.exec.dispose_backlog_len(), h.exec.pool_len()));
        }
        assert_eq!(seen, vec![(3, 1), (2, 1), (1, 1), (0, 1), (0, 2)]);
    });
}
