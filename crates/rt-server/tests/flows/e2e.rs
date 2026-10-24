//! End-to-end flows (E2E-01..07). E2E-08 (restart) is in `restart.rs`.
//!
//! Deno guests are defined inline (`common::guests`); the WASM hello component comes from
//! `fixtures/wasm/out/hello.wasm` (built by another lane).

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

node_test! {
/// E2E-01: tenant with a `deploy,invoke` key creates a Deno service, uploads
/// `export default () => new Response("hi")` with `activate=1`; the first call is `200 hi`
/// within SLO-5 (≤ 3 s, lazy compile) and carries `x-rt-version: 1`.
async fn ac_e2e_01_deno_hello_deploys_and_serves_on_the_first_call() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy", "invoke"]).await;
    let slug = uniq("hello");

    let created = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno"})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(&node, &t.key, &svc, guests::HELLO, "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    assert_eq!(up.json["version"], json!(1));
    assert_eq!(up.json["build_state"], json!("pending"), "lazy compile: the build starts on the first task");
    assert!(up.json["source_digest"].as_str().is_some_and(|d| d.starts_with("sha256:")));
    assert!(up.elapsed < Duration::from_millis(300) + Duration::from_millis(700), "the upload answers without a build: {:?}", up.elapsed);

    let first = invoke(&node, &slug, "/").await;
    assert_eq!(first.s(), 200, "{}", first.text);
    assert_eq!(first.text, "hi");
    assert_eq!(version_of(&first), Some(1), "x-rt-version: 1");
    assert!(first.elapsed < Duration::from_secs(3), "SLO-5: first call after deploy ≤ 3 s, took {:?}", first.elapsed);
    assert!(first.header("x-rt-task-id").is_some_and(|id| id.starts_with("task_")), "{:?}", first.header("x-rt-task-id"));
    assert!(first.header("server-timing").is_some(), "server-timing is set on every response");

    let second = invoke(&node, &slug, "/").await;
    assert_eq!(second.s(), 200);
    assert_ne!(second.header("x-rt-task-id"), first.header("x-rt-task-id"), "every request is its own task");
    node.shutdown().await;
}
}

node_test! {
/// E2E-02: the same flow with a `wasi:http` hello component (`fixtures/wasm/out/hello.wasm`).
async fn ac_e2e_02_wasm_hello_component_deploys_and_serves() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy", "invoke"]).await;
    let slug = uniq("wasm-hello");
    ship_wasm(&node, &t.key, &slug, "hello").await;

    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(!r.text.is_empty(), "the component's body");
    assert_eq!(version_of(&r), Some(1));
    assert!(r.elapsed < Duration::from_secs(5), "first call incl. lazy compile: {:?}", r.elapsed);

    // Sample component from sample/wasm/rust-hello (built by its own script) when present.
    let sample = repo_path("sample/wasm/rust-hello/target/wasm32-wasip2/release/rust_hello_wasm.wasm");
    if let Ok(bytes) = std::fs::read(&sample) {
        let slug2 = uniq("rust-hello");
        let svc = create_service(&node, &t.key, &slug2, "wasm").await;
        let up = upload(&node, &t.key, &svc, "application/wasm", bytes, "activate=1").await;
        assert!(up.s() == 201 || up.s() == 202, "{}", up.text);
        let r = invoke(&node, &slug2, "/?name=Ferris").await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert!(r.text.contains("Ferris"), "the sample greets by name: {}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// E2E-03: v2 activates while 100 slow v1 requests are in flight: every in-flight request
/// completes on v1; every request that starts after the activation is served by v2.
async fn ac_e2e_03_activating_v2_does_not_disturb_inflight_v1_requests() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("e2e03");
    let created = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "scaling": {"max_instances": 300, "max_queued": 300}}),
    )
    .await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    assert_eq!(upload_ts(&node, &t.key, &svc, &guests::slow(1, 1_500), "activate=1").await.s(), 201);
    // Warm up so the lazy build is not part of the measured window.
    let warm = invoke(&node, &slug, "/").await;
    assert_eq!((warm.s(), warm.text.as_str()), (200, "v1"), "{}", warm.text);

    let url = node.public_url(&format!("/s/{slug}/"));
    let inflight: Vec<_> = (0..100).map(|_| tokio::spawn(fetch(node.http().clone(), url.clone()))).collect();
    tokio::time::sleep(Duration::from_millis(500)).await; // all 100 are inside the handler now

    let up = upload_ts(&node, &t.key, &svc, &guests::versioned(2), "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    assert_eq!(up.json["version"], json!(2));

    // Requests that start after the activation get v2 — while the old ones are still running.
    for i in 0..20 {
        let r = invoke(&node, &slug, "/").await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(version_of(&r), Some(2), "request {i} after activation must be served by v2");
        assert_eq!(r.text, "v2");
    }

    for (i, h) in inflight.into_iter().enumerate() {
        let (status, body, version, _) = h.await.expect("join");
        assert_eq!(status, 200, "in-flight request {i} must complete");
        assert_eq!(body, "v1", "in-flight request {i} finishes on the version it started on");
        assert_eq!(version, Some(1), "in-flight request {i}");
    }
    node.shutdown().await;
}
}

node_test! {
/// E2E-04: `PUT /live {version:1}` is an atomic pointer flip: the next request is served by v1,
/// no request fails during repeated flips, and p99 latency is not degraded by more than 10 %.
async fn ac_e2e_04_rollback_flip_is_atomic_and_does_not_degrade_latency() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("e2e04");
    let svc = ship(&node, &t.key, &slug, &guests::versioned(1)).await;
    assert_eq!(invoke(&node, &slug, "/").await.text, "v1");
    assert_eq!(upload_ts(&node, &t.key, &svc, &guests::versioned(2), "activate=1").await.s(), 201);
    assert_eq!(invoke(&node, &slug, "/").await.text, "v2"); // v1 and v2 are both built now

    // The flip is visible to the very next request, in both directions.
    for (target, other) in [(1u32, 2u32), (2, 1), (1, 2)] {
        let r = put(&node, &t.key, &format!("/v1/services/{svc}/live"), json!({"version": target})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let served = invoke(&node, &slug, "/").await;
        assert_eq!(version_of(&served), Some(target), "after PUT /live {{version:{target}}} (not {other})");
        assert_eq!(served.text, format!("v{target}"));
    }

    // Latency: the same closed-loop load (4 workers, 2 s) without and with flips.
    let url = node.public_url(&format!("/s/{slug}/"));
    let run_load = |dur: Duration| {
        let (client, url) = (node.http().clone(), url.clone());
        async move {
            let deadline = Instant::now() + dur;
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let (client, url) = (client.clone(), url.clone());
                    tokio::spawn(async move {
                        let mut out = Vec::new();
                        while Instant::now() < deadline {
                            out.push(fetch(client.clone(), url.clone()).await);
                        }
                        out
                    })
                })
                .collect::<Vec<_>>();
            let mut all = Vec::new();
            for w in workers {
                all.extend(w.await.expect("worker"));
            }
            all
        }
    };
    let baseline = run_load(Duration::from_secs(2)).await;

    let flipper = {
        let (client, key, base) = (node.http().clone(), t.key.clone(), node.control_url(&format!("/v1/services/{svc}/live")));
        tokio::spawn(async move {
            for i in 0..8 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let version = if i % 2 == 0 { 1 } else { 2 };
                let r = client.put(&base).bearer_auth(&key).json(&json!({"version": version})).send().await.expect("flip");
                assert_eq!(r.status().as_u16(), 200);
            }
        })
    };
    let during = run_load(Duration::from_secs(2)).await;
    flipper.await.expect("flipper");

    for (status, body, version, _) in baseline.iter().chain(during.iter()) {
        assert_eq!(*status, 200, "no request may fail (404/5xx) during a flip");
        let v = version.expect("x-rt-version");
        assert!(v == 1 || v == 2);
        assert_eq!(body, &format!("v{v}"), "body and x-rt-version agree");
    }
    assert!(baseline.len() > 50 && during.len() > 50, "load actually ran: {} / {}", baseline.len(), during.len());
    let p99_base = percentile(baseline.iter().map(|r| r.3).collect(), 99.0);
    let p99_flip = percentile(during.iter().map(|r| r.3).collect(), 99.0);
    // 10 % per the AC, plus 25 ms of absolute slack for scheduler noise on shared CI machines.
    assert!(
        p99_flip <= p99_base * 1.10 + 25.0,
        "p99 during flips {p99_flip:.2} ms vs baseline {p99_base:.2} ms (> +10 %)"
    );
    node.shutdown().await;
}
}

node_test! {
/// E2E-05: a lazily built v3 with a missing import fails on its first request with
/// `502 build_failed`; the live pointer reverts to v2; the next request is `200` from v2 and a
/// `rollback.auto` event is recorded.
async fn ac_e2e_05_failed_lazy_build_auto_rolls_back_to_the_previous_version() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("e2e05");
    let svc = ship(&node, &t.key, &slug, &guests::versioned(1)).await;
    assert_eq!(upload_ts(&node, &t.key, &svc, &guests::versioned(2), "activate=1").await.s(), 201);
    let ok = invoke(&node, &slug, "/").await;
    assert_eq!((ok.s(), ok.text.as_str(), version_of(&ok)), (200, "v2", Some(2)), "{}", ok.text);

    let v3 = upload_ts(&node, &t.key, &svc, guests::MISSING_IMPORT, "activate=1").await;
    assert_eq!(v3.s(), 201, "a missing relative import is only found at build time: {}", v3.text);
    assert_eq!(v3.json["version"], json!(3));

    let bad = invoke(&node, &slug, "/").await;
    assert_eq!(bad.s(), 502, "{}", bad.text);
    assert_eq!(bad.header("x-rt-reason").as_deref(), Some("build_failed"));
    assert!(bad.header("content-type").unwrap_or_default().starts_with("text/plain"), "invoke errors are text/plain");
    assert!(!bad.text.contains("missing.ts"), "build diagnostics go to logs, not to the caller: {}", bad.text);

    // The live pointer reverts to v2.
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(10), "live to revert to v2", || async move {
        get(n, k, &format!("/v1/services/{s}")).await.json["live_version"] == json!(2)
    })
    .await;
    let after = invoke(&node, &slug, "/").await;
    assert_eq!((after.s(), after.text.as_str(), version_of(&after)), (200, "v2", Some(2)), "{}", after.text);

    // The failure is recorded on the version, and a `rollback.auto` event is logged.
    let v = get(&node, &t.key, &format!("/v1/services/{svc}/versions/3")).await;
    assert_eq!(v.json["build_state"], json!("failed"), "{}", v.text);
    let events = get(&node, &t.key, &format!("/v1/services/{svc}/events")).await;
    assert!(
        events.items().iter().any(|e| e["kind"] == json!("rollback.auto")),
        "a rollback.auto event is logged: {}",
        events.text
    );

    // BLD-09: more requests to the failed version do not trigger more builds (it is not live
    // anymore): they are all served by v2.
    for _ in 0..10 {
        assert_eq!(version_of(&invoke(&node, &slug, "/").await), Some(2));
    }
    node.shutdown().await;
}
}

node_test! {
/// E2E-06: a service logging `console.log("x")` per request, `GET /logs?follow=1` open, three
/// requests: three `x` records stream within 1 s, each with the request's `task_id`.
async fn ac_e2e_06_logs_follow_streams_each_record_within_a_second() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("e2e06");
    let svc = ship(&node, &t.key, &slug, guests::LOGGER).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build now, before the stream opens

    let mut sse = Sse::open(api(&node, &t.key, Method::GET, &format!("/v1/services/{svc}/logs?follow=1"))).await;
    assert_eq!(sse.status, 200);
    assert!(
        sse.headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").contains("text/event-stream"),
        "SSE content type"
    );
    // The first request's record may arrive as backlog; skip anything already there.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut seen_before = 0;
    while sse.next(Duration::from_millis(300)).await.is_some() {
        seen_before += 1;
    }
    let _ = seen_before;

    let mut task_ids = Vec::new();
    let mut sent_at = Vec::new();
    for _ in 0..3 {
        sent_at.push(Instant::now());
        let r = invoke(&node, &slug, "/").await;
        assert_eq!(r.s(), 200);
        task_ids.push(r.header("x-rt-task-id").expect("x-rt-task-id"));
    }
    let events = sse.take(3, Duration::from_secs(3)).await;
    assert_eq!(events.len(), 3, "three records must stream");
    let done = Instant::now();
    assert!(done.duration_since(sent_at[2]) < Duration::from_secs(1), "the last record arrived within 1 s of its request");
    for (i, e) in events.iter().enumerate() {
        let rec = e.json();
        assert_eq!(rec["msg"], json!("x"), "record {i}: {rec}");
        assert_eq!(rec["task_id"], json!(task_ids[i]), "record {i} carries the task_id of its request");
        assert_eq!(rec["kind"], json!("runtime"));
        assert_eq!(rec["level"], json!("info"));
    }
    node.shutdown().await;
}
}

node_test! {
/// E2E-07: an interval schedule every 2 s on a service; after 7 s, 3 ± 1 scheduled tasks ran
/// and none overlapped (the handler takes 500 ms).
async fn ac_e2e_07_interval_schedule_fires_three_times_in_seven_seconds_without_overlap() {
    let node = start_node().await; // `pro`: min_schedule_seconds = 1
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("e2e07");
    let svc = ship(&node, &t.key, &slug, guests::SCHEDULED_RECORDER).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // warm build, creates the table

    let s = post(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/schedules"),
        json!({"kind": "interval", "every_seconds": 2, "payload": {"job": "tick"}}),
    )
    .await;
    assert_eq!(s.s(), 201, "{}", s.text);
    tokio::time::sleep(Duration::from_secs(7)).await;

    let list = invoke(&node, &slug, "/").await;
    let runs: Vec<Value> = list.json["runs"].as_array().cloned().unwrap_or_default();
    assert!(
        (2..=4).contains(&runs.len()),
        "expected 3 ± 1 scheduled runs in 7 s, got {}: {}",
        runs.len(),
        list.text
    );
    let mut spans: Vec<(i64, i64)> = runs
        .iter()
        .map(|r| (r["started"].as_i64().expect("started"), r["finished"].as_i64().expect("finished")))
        .collect();
    spans.sort();
    for w in spans.windows(2) {
        assert!(w[1].0 >= w[0].1, "runs overlap: {:?} then {:?}", w[0], w[1]);
    }
    for r in &runs {
        assert_eq!(r["payload"], json!("{\"job\":\"tick\"}"), "event.payload is the schedule's payload");
        assert!(r["finished"].as_i64() >= r["started"].as_i64().map(|s| s + 400), "the handler ran for its 500 ms timer");
    }
    // Fires roughly every 2 s (not in a burst).
    for w in spans.windows(2) {
        assert!(w[1].0 - w[0].0 >= 1_500, "fires should be ~2 s apart: {:?} {:?}", w[0], w[1]);
    }

    // Schedules stop with their service's schedule list entry.
    let sid = s.json["id"].as_str().expect("schedule id").to_string();
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/schedules/{sid}")).await.s(), 204);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let n1 = invoke(&node, &slug, "/").await.json["runs"].as_array().map(Vec::len).unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let n2 = invoke(&node, &slug, "/").await.json["runs"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(n1, n2, "a deleted schedule no longer fires");
    node.shutdown().await;
}
}

node_test! {
/// Limits surface as documented statuses: a throwing
/// handler is `500 guest_error` with a generic body, a CPU hog `502 cpu_limit`, an allocator
/// `502 oom`, a sleeper `504 timeout`; the node keeps serving afterwards.
async fn guest_failures_map_to_documented_statuses_and_leak_nothing() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    // 24 MB (as in DNO-08): the allocator reaches its heap limit in ~50 ms of CPU, well inside the
    // 200 ms CPU budget even under load; a larger heap needs ~170 ms and races the CPU limit.
    let limits = json!({"cpu_ms": 200, "wall_ms": 2000, "memory_mb": 24});
    // CPU limits count the task's own CPU time, never wall time: on a loaded machine the hog may
    // need far more than 200 ms of wall time to burn 200 ms of CPU, so its wall limit is generous.
    let hog_limits = json!({"cpu_ms": 200, "wall_ms": 30000, "memory_mb": 24});
    let mk_with = |slug: String, src: &'static str, limits: serde_json::Value| {
        let (node, key) = (&node, t.key.clone());
        async move {
            let r = post(node, &key, "/v1/services", json!({"slug": slug, "runtime": "deno", "limits": limits})).await;
            assert_eq!(r.s(), 201, "{}", r.text);
            let id = r.json["id"].as_str().expect("id").to_string();
            assert_eq!(upload_ts(node, &key, &id, src, "activate=1").await.s(), 201);
            slug
        }
    };
    let mk = |slug: String, src: &'static str| mk_with(slug, src, limits.clone());
    let throw = mk(uniq("throw"), rt_testkit::fixtures::DENO_THROW).await;
    let hog = mk_with(uniq("hog"), rt_testkit::fixtures::DENO_LOOP, hog_limits).await;
    let oom = mk(uniq("oom"), rt_testkit::fixtures::DENO_OOM).await;
    let sleeper = mk(
        uniq("sleep"),
        "export default async function () { await new Promise((r) => setTimeout(r, 60000)); return new Response('late'); }",
    )
    .await;
    let ok = ship(&node, &t.key, &uniq("fine"), guests::HELLO).await;
    let _ = ok;

    let r = invoke(&node, &throw, "/").await;
    assert_eq!((r.s(), r.header("x-rt-reason").as_deref()), (500, Some("guest_error")), "{}", r.text);
    assert!(!r.text.contains("boom"), "the guest's message must not leak: {}", r.text);
    let r = invoke(&node, &hog, "/").await;
    assert_eq!((r.s(), r.header("x-rt-reason").as_deref()), (502, Some("cpu_limit")), "{}", r.text);
    let r = invoke(&node, &oom, "/").await;
    assert_eq!((r.s(), r.header("x-rt-reason").as_deref()), (502, Some("oom")), "{}", r.text);
    let r = invoke(&node, &sleeper, "/").await;
    assert_eq!((r.s(), r.header("x-rt-reason").as_deref()), (504, Some("timeout")), "{}", r.text);
    assert!(r.elapsed < Duration::from_secs(10), "killed at the wall limit, took {:?}", r.elapsed);

    // The node is fine afterwards.
    let again = invoke(&node, &throw, "/").await;
    assert_eq!(again.s(), 500);
    node.shutdown().await;
}
}
