//! Observability: logs (OBS-01/02), metrics (OBS-06, 07), health probes (OBS-10) and
//! `server-timing` (OBS-11). OBS-05 (logs survive a restart) is in `restart.rs`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use reqwest::Method;
use rt_obs::NODE_METRIC_NAMES;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

const LEVELS: &str = r#"
export default function (): Response {
  console.debug("d-msg");
  console.log("l-msg");
  console.info("i-msg");
  console.warn("w-msg");
  console.error("e-msg");
  const long = "z".repeat(20000);
  console.log(long);
  return new Response("ok");
}"#;

async fn logs_of(node: &impl Node, key: &str, svc: &str, query: &str) -> Vec<Value> {
    let r = get(node, key, &format!("/v1/services/{svc}/logs?{query}")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    r.items()
}

node_test! {
/// OBS-01: guest logs per request: every record has all fields, `task_id` equals the response's
/// `x-rt-task-id`; levels, kind, truncation and the query filters work.
async fn ac_obs_01_log_records_carry_every_field_and_the_task_id() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("logs");
    let svc = ship(&node, &t.key, &slug, LEVELS).await;
    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let task_id = r.header("x-rt-task-id").expect("x-rt-task-id");

    // Logs are written asynchronously: wait for the records of this task to show up.
    let (n, k, s, tid) = (&node, &t.key, &svc, task_id.clone());
    eventually(Duration::from_secs(5), "the 6 log records of the request", || {
        let tid = tid.clone();
        async move { logs_of(n, k, s, &format!("task_id={tid}")).await.len() >= 6 }
    })
    .await;
    let recs = logs_of(&node, &t.key, &svc, &format!("task_id={task_id}")).await;
    assert_eq!(recs.len(), 6, "five short lines and one long line: {recs:?}");

    for rec in &recs {
        for f in ["ts", "level", "service_id", "version", "task_id", "trigger", "kind", "msg"] {
            assert!(rec.get(f).is_some_and(|v| !v.is_null()), "record lacks `{f}`: {rec}");
        }
        assert_eq!(rec["task_id"], json!(task_id));
        assert_eq!(rec["service_id"], json!(svc));
        assert_eq!(rec["version"], json!(1));
        assert_eq!(rec["trigger"], json!("http"));
        assert_eq!(rec["kind"], json!("runtime"), "guest records are kind=runtime");
    }
    let level_of = |msg: &str| recs.iter().find(|r| r["msg"] == json!(msg)).map(|r| r["level"].clone());
    assert_eq!(level_of("e-msg"), Some(json!("error")));
    assert_eq!(level_of("w-msg"), Some(json!("warn")));
    assert_eq!(level_of("i-msg"), Some(json!("info")));
    assert_eq!(level_of("l-msg"), Some(json!("info")), "console.log is info");
    assert_eq!(level_of("d-msg"), Some(json!("debug")));

    // Messages are cut at 10,000 characters and flagged.
    let long = recs.iter().find(|r| r["msg"].as_str().is_some_and(|m| m.starts_with("zzzz"))).expect("the long record");
    assert!(long["msg"].as_str().expect("msg").chars().count() <= 10_000);
    assert_eq!(long["truncated"], json!(true));

    // Filters.
    let errs = logs_of(&node, &t.key, &svc, "level=error").await;
    assert!(!errs.is_empty() && errs.iter().all(|r| r["level"] == json!("error")), "{errs:?}");
    let other = invoke(&node, &slug, "/").await;
    let other_id = other.header("x-rt-task-id").expect("task id");
    assert_ne!(other_id, task_id);
    let (n, k, s, oid) = (&node, &t.key, &svc, other_id.clone());
    eventually(Duration::from_secs(5), "the second request's logs", || {
        let oid = oid.clone();
        async move { logs_of(n, k, s, &format!("task_id={oid}")).await.len() >= 6 }
    })
    .await;
    let only_first = logs_of(&node, &t.key, &svc, &format!("task_id={task_id}")).await;
    assert!(only_first.iter().all(|r| r["task_id"] == json!(task_id)), "task_id filter");
    let runtime = logs_of(&node, &t.key, &svc, "kind=runtime").await;
    assert!(runtime.len() >= 12);
    let build = logs_of(&node, &t.key, &svc, "kind=build").await;
    assert!(build.iter().all(|r| r["kind"] == json!("build")));

    // Time range: now-1h..now+1h contains them; a window in the past does not.
    let inside = logs_of(&node, &t.key, &svc, &format!("since={}&until={}", rfc3339_in(-3600), rfc3339_in(3600))).await;
    assert!(inside.len() >= 12);
    let past = logs_of(&node, &t.key, &svc, &format!("since={}&until={}", rfc3339_in(-7200), rfc3339_in(-3600))).await;
    assert!(past.is_empty(), "nothing was logged in the past: {past:?}");

    // Another tenant's key cannot read them.
    let intruder = new_tenant(&node, "pro").await;
    assert_eq!(get(&node, &intruder.key, &format!("/v1/services/{svc}/logs")).await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// OBS-02: SSE tail: 3 requests, disconnect, 2 requests, reconnect with `Last-Event-ID`: all 5
/// records are delivered exactly once, each within 1 s.
async fn ac_obs_02_sse_tail_resumes_with_last_event_id_without_loss_or_duplicates() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("tail");
    let svc = ship(&node, &t.key, &slug, guests::LOGGER).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build + warm-up record
    tokio::time::sleep(Duration::from_millis(300)).await;

    let path = format!("/v1/services/{svc}/logs?follow=1");
    let mut first = Sse::open(api(&node, &t.key, Method::GET, &path)).await;
    assert_eq!(first.status, 200);
    // Drain any backlog (the warm-up record).
    while first.next(Duration::from_millis(400)).await.is_some() {}

    let mut task_ids = Vec::new();
    let mut latencies = Vec::new();
    let mut last_id = None;
    let mut got: Vec<Value> = Vec::new();
    for _ in 0..3 {
        let sent = Instant::now();
        let r = invoke(&node, &slug, "/").await;
        task_ids.push(r.header("x-rt-task-id").expect("task id"));
        let ev = first.next(Duration::from_secs(2)).await.expect("a record within 2 s");
        latencies.push(sent.elapsed());
        assert!(ev.id.is_some(), "every event carries an id for resumption: {ev:?}");
        last_id = ev.id.clone();
        got.push(ev.json());
    }
    drop(first); // disconnect

    for _ in 0..2 {
        let r = invoke(&node, &slug, "/").await;
        task_ids.push(r.header("x-rt-task-id").expect("task id"));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut second = Sse::open(
        api(&node, &t.key, Method::GET, &path).header("last-event-id", last_id.clone().expect("an event id")),
    )
    .await;
    assert_eq!(second.status, 200);
    let sent = Instant::now();
    let replay = second.take(2, Duration::from_secs(2)).await;
    assert_eq!(replay.len(), 2, "the two missed records are replayed");
    assert!(sent.elapsed() < Duration::from_secs(1), "replay is immediate: {:?}", sent.elapsed());
    got.extend(replay.iter().map(|e| e.json()));
    // Nothing else is replayed (no duplicates of the first three).
    assert!(second.next(Duration::from_millis(500)).await.is_none(), "no duplicate delivery after the resume");

    let delivered: Vec<String> = got.iter().map(|r| r["task_id"].as_str().expect("task_id").to_string()).collect();
    assert_eq!(delivered, task_ids, "all 5 records, in order, exactly once");
    let unique: BTreeSet<&String> = delivered.iter().collect();
    assert_eq!(unique.len(), 5);
    assert!(latencies.iter().all(|l| *l < Duration::from_secs(1)), "each live record within 1 s: {latencies:?}");

    // New requests still stream on the resumed connection.
    let r = invoke(&node, &slug, "/").await;
    let live = second.next(Duration::from_secs(2)).await.expect("a live record");
    assert_eq!(live.json()["task_id"], json!(r.header("x-rt-task-id").expect("task id")));
    node.shutdown().await;
}
}

fn has_metric(text: &str, name: &str) -> bool {
    text.lines().any(|l| {
        l.strip_prefix("# TYPE ")
            .is_some_and(|rest| rest.split_whitespace().next() == Some(name))
            || l.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
    })
}

node_test! {
/// OBS-06: scraping `/metrics` lists every metric of docs/deploy.md (the test enumerates the names).
async fn ac_obs_06_metrics_endpoint_exposes_every_documented_metric() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("metrics");
    ship(&node, &t.key, &slug, guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    assert_eq!(invoke(&node, "no-such-service", "/").await.s(), 404);

    let text = send(node.http().get(node.metrics_url("/metrics"))).await;
    assert_eq!(text.s(), 200);
    assert!(text.header("content-type").unwrap_or_default().starts_with("text/plain"), "{:?}", text.header("content-type"));
    let missing: Vec<&&str> = NODE_METRIC_NAMES.iter().filter(|n| !has_metric(&text.text, n)).collect();
    assert!(missing.is_empty(), "metrics missing from /metrics: {missing:?}");

    // The request we made is visible, with the documented labels.
    let tasks: f64 = metric_values(&text.text, "rt_tasks_total")
        .iter()
        .filter(|(series, _)| series.contains("trigger=\"http\""))
        .map(|(_, v)| *v)
        .sum();
    assert!(tasks >= 1.0, "rt_tasks_total{{trigger=\"http\"}} counts the request:\n{}", text.text);
    assert!(
        text.text.lines().any(|l| l.starts_with("rt_http_requests_total{") && l.contains("listener=\"public\"") && l.contains("status_class=\"2xx\"")),
        "rt_http_requests_total{{listener=public,status_class=2xx}}"
    );
    assert!(
        text.text.lines().any(|l| l.starts_with("rt_http_requests_total{") && l.contains("status_class=\"4xx\"")),
        "the 404 is counted as 4xx"
    );
    // Per-service series are opt-in.
    assert!(!has_metric(&text.text, "rt_service_requests_total"), "per-service metrics are off by default");
    // The metrics listener serves nothing else (no control API on it).
    assert_eq!(send(node.http().get(node.metrics_url("/v1/services"))).await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// OBS-07 (small): with `metrics.per_service = true` and `per_service_top_n = 1` only the busiest
/// service gets its own label; the rest aggregate into `_other`.
async fn ac_obs_07_per_service_series_are_capped_to_the_top_n() {
    let node = start_node_with(|c| {
        c.metrics.per_service = true;
        c.metrics.per_service_top_n = 1;
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let busy = uniq("busy");
    let quiet = uniq("quiet");
    let third = uniq("third");
    for s in [&busy, &quiet, &third] {
        ship(&node, &t.key, s, guests::HELLO).await;
    }
    for _ in 0..10 {
        invoke(&node, &busy, "/").await;
    }
    invoke(&node, &quiet, "/").await;
    invoke(&node, &third, "/").await;
    tokio::time::sleep(Duration::from_millis(1_500)).await; // stats are merged by sysmon every second

    let text = send(node.http().get(node.metrics_url("/metrics"))).await.text;
    let services: BTreeSet<String> = text
        .lines()
        .filter(|l| l.starts_with("rt_service_requests_total{"))
        .filter_map(|l| l.split("service=\"").nth(1).and_then(|r| r.split('"').next()).map(str::to_string))
        .collect();
    assert!(services.len() <= 2, "top_n=1 ⇒ one service + `_other`, got {services:?}");
    assert!(services.contains("_other"), "the rest aggregate into _other: {services:?}");
    assert!(!services.contains(&quiet) || !services.contains(&third), "at most one of the quiet services has its own series: {services:?}");
    node.shutdown().await;
}
}

node_test! {
/// OBS-10: a draining node answers `/readyz` with 503 while `/healthz` stays 200, and refuses new
/// invocations with `503 x-rt-reason: draining`.
async fn ac_obs_10_readyz_turns_503_when_draining() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("drain");
    ship(&node, &t.key, &slug, guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let probe = |path: &'static str| {
        let (client, url) = (node.http().clone(), node.metrics_url(path));
        async move { client.get(&url).send().await.expect("probe").status().as_u16() }
    };
    assert_eq!(probe("/readyz").await, 200);
    assert_eq!(probe("/healthz").await, 200);

    let r = call(root(&node, Method::POST, "/v1/node/drain"), None).await;
    assert_eq!(r.s(), 202, "{}", r.text);

    let (client, url) = (node.http().clone(), node.metrics_url("/readyz"));
    eventually(Duration::from_secs(3), "/readyz to turn 503 while draining", || {
        let (client, url) = (client.clone(), url.clone());
        async move { client.get(&url).send().await.map(|r| r.status().as_u16() == 503).unwrap_or(false) }
    })
    .await;
    assert_eq!(probe("/healthz").await, 200, "liveness is unaffected by draining");
    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 503, "{}", r.text);
    assert_eq!(r.header("x-rt-reason").as_deref(), Some("draining"));
    let st = call(root(&node, Method::GET, "/v1/node/status"), None).await;
    assert_eq!(st.json["mode"], json!("drain"), "{}", st.text);
    node.shutdown().await;
}
}

node_test! {
/// OBS-10 (liveness): with the sysmon heartbeat stalled (test hook) `/healthz` turns 503; it
/// recovers when the heartbeat resumes.
async fn ac_obs_10_healthz_turns_503_when_the_sysmon_heartbeat_stalls() {
    let handle = new_handle();
    let node = start_node_opts(rt_server::StartOptions::default(), Some(handle.clone()), |_| {}).await;
    let (client, url) = (node.http().clone(), node.metrics_url("/healthz"));
    let status = |client: reqwest::Client, url: String| async move {
        client.get(&url).send().await.map(|r| r.status().as_u16()).unwrap_or(0)
    };
    assert_eq!(status(client.clone(), url.clone()).await, 200);

    let stall = |on: bool| {
        let guard = handle.lock().expect("handle lock");
        guard.as_ref().expect("running server handle").stall_sysmon_for_test(on);
    };
    stall(true);
    let (c2, u2) = (client.clone(), url.clone());
    eventually(Duration::from_secs(5), "/healthz to turn 503 once the heartbeat is stale (> 1 s)", || {
        let (c, u) = (c2.clone(), u2.clone());
        async move { status(c, u).await == 503 }
    })
    .await;
    stall(false);
    let (c3, u3) = (client.clone(), url.clone());
    eventually(Duration::from_secs(5), "/healthz to recover", || {
        let (c, u) = (c3.clone(), u3.clone());
        async move { status(c, u).await == 200 }
    })
    .await;
    node.shutdown().await;
}
}

fn parse_server_timing(v: &str) -> Vec<(String, f64)> {
    v.split(',')
        .filter_map(|part| {
            let mut it = part.trim().split(';');
            let name = it.next()?.trim().to_string();
            let dur = it.find_map(|p| p.trim().strip_prefix("dur=").map(str::to_string))?;
            Some((name, dur.parse::<f64>().ok()?))
        })
        .collect()
}

node_test! {
/// OBS-11: `server-timing` lists the phases and they add up to the time the request took. The AC
/// compares against the node's own measurement (±1 ms); from outside only the client-observed
/// time-to-headers is available, so the tolerance here is wider (30 ms on loopback).
async fn ac_obs_11_server_timing_phases_sum_to_the_request_time() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slow = uniq("timing");
    ship(&node, &t.key, &slow, &guests::slow(1, 200)).await;
    assert_eq!(invoke(&node, &slow, "/").await.s(), 200); // first call compiles

    for (what, slug, min_exec) in [("200 ms handler", slow.clone(), 190.0)] {
        let r = invoke(&node, &slug, "/").await;
        assert_eq!(r.s(), 200);
        let timing = parse_server_timing(&r.header("server-timing").expect("server-timing header"));
        let names: Vec<&str> = timing.iter().map(|(n, _)| n.as_str()).collect();
        for phase in ["queue", "wait_runtime", "init", "exec"] {
            assert!(names.contains(&phase), "{what}: phase `{phase}` missing from {names:?}");
        }
        assert!(timing.iter().all(|(_, d)| *d >= 0.0), "{what}: negative duration in {timing:?}");
        let sum: f64 = timing.iter().map(|(_, d)| d).sum();
        let exec = timing.iter().find(|(n, _)| n == "exec").map(|(_, d)| *d).expect("exec");
        assert!(exec >= min_exec, "{what}: exec {exec} ms for a 200 ms timer");
        let total = r.elapsed.as_secs_f64() * 1000.0;
        assert!(sum <= total + 1.0, "{what}: phases ({sum:.2} ms) cannot exceed the observed time ({total:.2} ms)");
        assert!(total - sum <= 30.0, "{what}: phases {sum:.2} ms vs observed {total:.2} ms differ by more than the allowed overhead");
    }

    // A fast handler: still all four phases, tiny durations, sum ≤ observed.
    let fast = uniq("timing-fast");
    ship(&node, &t.key, &fast, guests::HELLO).await;
    invoke(&node, &fast, "/").await;
    let r = invoke(&node, &fast, "/").await;
    let timing = parse_server_timing(&r.header("server-timing").expect("server-timing"));
    let sum: f64 = timing.iter().map(|(_, d)| d).sum();
    assert!(sum <= r.elapsed.as_secs_f64() * 1000.0 + 1.0);
    node.shutdown().await;
}
}
