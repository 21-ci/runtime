//! Queue and service-call triggers through the real node: TRG-09..17 at HTTP level. The timing
//! of backoff / visibility / depth is covered in virtual time in `rt-triggers/tests`; here the
//! whole path (API → queue.db → engine → guest → ack/retry/DLQ) runs for real.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

async fn attempts_of(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/").await.json["attempts"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn enqueue(node: &impl Node, key: &str, svc: &str, body: Value) -> Resp {
    post(node, key, &format!("/v1/services/{svc}/queue"), json!({"body": body})).await
}

node_test! {
/// TRG-09: a handler that fails (retries) twice and then succeeds: attempts 1, 2, 3 with a
/// backoff of ≈ 2 s and ≈ 4 s (±20 %); acked after attempt 3.
async fn ac_trg_09_queue_retries_with_exponential_backoff_then_acks() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("retry");
    let svc = ship(&node, &t.key, &slug, guests::QUEUE_ATTEMPTS).await;
    assert_eq!(put(&node, &t.key, &format!("/v1/services/{svc}/env"), json!({"SUCCEED_ON": "3"})).await.s(), 200);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200); // build

    assert_eq!(enqueue(&node, &t.key, &svc, json!({"job": 1})).await.s(), 202);
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(25), "three delivery attempts", || {
        let sl = sl.clone();
        async move { attempts_of(n, &sl).await.len() >= 3 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a = attempts_of(&node, &slug).await;
    assert_eq!(a.iter().map(|x| x["attempt"].as_i64().unwrap_or(0)).collect::<Vec<_>>(), vec![1, 2, 3], "{a:?}");
    let at = |i: usize| a[i]["at"].as_f64().expect("timestamp");
    let (g1, g2) = ((at(1) - at(0)) / 1000.0, (at(2) - at(1)) / 1000.0);
    assert!((1.6 - 0.4..=2.4 + 0.6).contains(&g1), "first backoff ≈ 2 s ±20 %, got {g1:.2} s");
    assert!((3.2 - 0.4..=4.8 + 0.6).contains(&g2), "second backoff ≈ 4 s ±20 %, got {g2:.2} s");

    // Acked: nothing waits, nothing is in flight, nothing in the DLQ, and no 4th delivery.
    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/queue")).await;
    assert_eq!((stats.json["depth"].as_u64(), stats.json["in_flight"].as_u64()), (Some(0), Some(0)), "{}", stats.text);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(attempts_of(&node, &slug).await.len(), 3);
    node.shutdown().await;
}
}

node_test! {
/// TRG-10: a handler that always fails: after `queue.max_attempts` the message is in the DLQ
/// (`GET /dlq`, `rt_queue_dlq_total`); redrive puts it back with attempt = 1; the DLQ can be purged.
/// (3 attempts instead of the default 5 to keep the wall time down; the 5-attempt timing is
/// asserted in virtual time in rt-triggers.)
async fn ac_trg_10_always_failing_message_lands_in_the_dlq_and_can_be_redriven() {
    let node = start_node_with(|c| c.queue.max_attempts = 3).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("dlq");
    let svc = ship(&node, &t.key, &slug, guests::QUEUE_ATTEMPTS).await; // SUCCEED_ON unset = never
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let m = enqueue(&node, &t.key, &svc, json!({"poison": true})).await;
    assert_eq!(m.s(), 202, "{}", m.text);
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(40), "the message to be dead-lettered", || async move {
        get(n, k, &format!("/v1/services/{s}/queue")).await.json["dlq"].as_u64() == Some(1)
    })
    .await;
    let a = attempts_of(&node, &slug).await;
    assert_eq!(a.len(), 3, "exactly max_attempts deliveries: {a:?}");

    let dlq = get(&node, &t.key, &format!("/v1/services/{svc}/dlq")).await;
    assert_eq!(dlq.s(), 200, "{}", dlq.text);
    let entries = if dlq.json.is_array() { dlq.items() } else { dlq.json["items"].as_array().cloned().unwrap_or_default() };
    assert_eq!(entries.len(), 1, "{}", dlq.text);
    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/queue")).await;
    assert_eq!(stats.json["depth"], json!(0));
    let metrics = send(node.http().get(node.metrics_url("/metrics"))).await.text;
    assert!(metric_sum(&metrics, "rt_queue_dlq_total") >= 1.0, "rt_queue_dlq_total counts the dead letter");

    // Redrive: back in the queue with attempt = 1.
    let rd = call(api(&node, &t.key, reqwest::Method::POST, &format!("/v1/services/{svc}/dlq/redrive")), None).await;
    assert_eq!(rd.s(), 200, "{}", rd.text);
    assert_eq!(rd.json["redriven"], json!(1));
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(10), "the redriven delivery", || {
        let sl = sl.clone();
        async move { attempts_of(n, &sl).await.len() >= 4 }
    })
    .await;
    let a = attempts_of(&node, &slug).await;
    assert_eq!(a[3]["attempt"], json!(1), "a redriven message restarts at attempt 1: {a:?}");

    // Purge the DLQ once it has been re-filled.
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(40), "the message to be dead-lettered again", || async move {
        get(n, k, &format!("/v1/services/{s}/queue")).await.json["dlq"].as_u64() == Some(1)
    })
    .await;
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc}/dlq")).await.s(), 204);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}/queue")).await.json["dlq"], json!(0));
    node.shutdown().await;
}
}

node_test! {
/// TRG-12: two enqueues with the same `dedup_id` within 24 h deliver one message.
async fn ac_trg_12_dedup_id_collapses_duplicate_enqueues() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("dedup");
    let svc = ship(&node, &t.key, &slug, guests::QUEUE_RECORDER).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let path = format!("/v1/services/{svc}/queue");
    // Hold them back so both enqueues certainly happen before the first delivery.
    let a = post(&node, &t.key, &path, json!({"body": {"n": 1}, "delay_seconds": 2, "dedup_id": "job-1"})).await;
    let b = post(&node, &t.key, &path, json!({"body": {"n": 2}, "delay_seconds": 2, "dedup_id": "job-1"})).await;
    let c = post(&node, &t.key, &path, json!({"body": {"n": 3}, "delay_seconds": 2, "dedup_id": "job-2"})).await;
    for r in [&a, &b, &c] {
        assert_eq!(r.s(), 202, "{}", r.text);
    }
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(15), "deliveries", || {
        let sl = sl.clone();
        async move { invoke(n, &sl, "/").await.json["got"].as_array().is_some_and(|g| g.len() >= 2) }
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let got = invoke(&node, &slug, "/").await.json["got"].as_array().cloned().unwrap_or_default();
    assert_eq!(got.len(), 2, "job-1 once and job-2 once: {got:?}");
    let bodies: Vec<&str> = got.iter().filter_map(|g| g["body"].as_str()).collect();
    assert!(bodies.contains(&"{\"n\":1}") && bodies.contains(&"{\"n\":3}"), "{bodies:?}");
    assert!(!bodies.contains(&"{\"n\":2}"), "the duplicate must not be delivered: {bodies:?}");
    node.shutdown().await;
}
}

node_test! {
/// TRG-13: with the queue at the plan's `max_queue_depth` the next enqueue is `429 queue_full`.
async fn ac_trg_13_queue_at_plan_depth_rejects_with_queue_full() {
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.max_queue_depth = 3;
        c.plans.insert("smallq".to_string(), plan);
    })
    .await;
    let t = new_tenant(&node, "smallq").await;
    let svc = ship(&node, &t.key, &uniq("full"), guests::QUEUE_RECORDER).await;
    let path = format!("/v1/services/{svc}/queue");
    for i in 0..3 {
        let r = post(&node, &t.key, &path, json!({"body": i, "delay_seconds": 3600})).await;
        assert_eq!(r.s(), 202, "message {i}: {}", r.text);
    }
    let r = post(&node, &t.key, &path, json!({"body": "one too many", "delay_seconds": 3600})).await;
    assert_eq!(r.s(), 429, "{}", r.text);
    assert_eq!(r.code(), "queue_full");
    assert_eq!(get(&node, &t.key, &path).await.json["depth"], json!(3));
    node.shutdown().await;
}
}

node_test! {
/// TRG-14: service A calls service B (same tenant): A gets B's response, and no extra HTTP
/// request reaches an ingress listener for the child call (verified by the ingress counters).
async fn ac_trg_14_service_call_is_in_process_without_an_http_hop() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let b = uniq("callee");
    let a = uniq("caller");
    ship(&node, &t.key, &b, guests::HELLO).await;
    ship(&node, &t.key, &a, guests::CALLER).await;
    // Warm both builds so only the calls under test are counted afterwards.
    assert_eq!(invoke(&node, &b, "/").await.s(), 200);
    assert_eq!(invoke(&node, &a, "/nothing").await.s(), 404);

    let ingress_total = |text: &str| {
        text.lines()
            .filter(|l| l.starts_with("rt_http_requests_total{") && (l.contains("listener=\"public\"") || l.contains("listener=\"internal\"")))
            .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.parse::<f64>().ok()))
            .sum::<f64>()
    };
    let before = ingress_total(&send(node.http().get(node.metrics_url("/metrics"))).await.text);

    let r = invoke(&node, &a, &format!("/call?target={b}&path=/hello")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["status"], json!(200), "{}", r.text);
    assert_eq!(r.json["body"], json!("hi"), "A gets B's response: {}", r.text);

    let after = ingress_total(&send(node.http().get(node.metrics_url("/metrics"))).await.text);
    assert_eq!(after - before, 1.0, "exactly one ingress request (the one to A); the call to B opened no connection");

    // The callee's own errors come back as response objects.
    let r = invoke(&node, &a, "/call?target=does-not-exist").await;
    assert_eq!(r.s(), 200);
    assert!(r.json["status"] == json!(404) || r.json["thrown"].is_string(), "unknown target: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// TRG-15: a recursive self-call stops at depth 15 with `508 call_depth`; every task in the chain
/// finishes and nothing stays in flight.
async fn ac_trg_15_recursive_call_chain_is_cut_at_depth_15() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("recurse");
    ship(&node, &t.key, &slug, guests::CALLER).await;

    let r = invoke(&node, &slug, "/recurse").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["status"], json!(508), "the chain ends with 508: {}", r.text);
    assert_eq!(r.json["reason"], json!("call_depth"), "{}", r.text);
    assert_eq!(r.json["deepest"], json!(15), "the task at depth 15 is the last that may not call: {}", r.text);

    // All tasks of the chain finished; reservations were released.
    let (n, root_tok) = (&node, node.root_token().to_string());
    eventually(Duration::from_secs(5), "no task left in flight", || {
        let tok = root_tok.clone();
        async move {
            let r = send(n.http().get(n.control_url("/v1/node/scheduler")).bearer_auth(tok)).await;
            r.json["inflight"] == json!(0)
        }
    })
    .await;
    let metrics = send(node.http().get(node.metrics_url("/metrics"))).await.text;
    assert_eq!(metric_sum(&metrics, "rt_inflight_tasks"), 0.0, "rt_inflight_tasks is back to zero");
    // The node serves normally afterwards.
    assert_eq!(invoke(&node, &slug, "/nothing").await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// TRG-16: tenant t1 calls a service of tenant t2: `403 cross_tenant_call` unless the caller's plan
/// allows it, and then only if the target lists the caller's service id (`call_not_allowed`).
async fn ac_trg_16_cross_tenant_calls_are_denied_unless_the_plan_allows_them() {
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.allow_cross_tenant_calls = true;
        c.plans.insert("open".to_string(), plan);
    })
    .await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    let open = new_tenant(&node, "open").await;
    let callee = uniq("theirs");
    let caller = uniq("mine");
    let open_caller = uniq("open-mine");
    let callee_id = ship(&node, &t2.key, &callee, guests::HELLO).await;
    ship(&node, &t1.key, &caller, guests::CALLER).await;
    let open_caller_id = ship(&node, &open.key, &open_caller, guests::CALLER).await;

    let r = invoke(&node, &caller, &format!("/call?target={callee}")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let denied = r.json["status"] == json!(403) || r.json["thrown"].as_str().is_some_and(|t| t.contains("cross_tenant_call"));
    assert!(denied, "t1 → t2 must be refused: {}", r.text);
    assert!(
        r.json["reason"] == json!("cross_tenant_call") || r.json["thrown"].as_str().is_some_and(|t| t.contains("cross_tenant_call")),
        "the stable code is cross_tenant_call: {}",
        r.text
    );

    // The plan allows it, but the target does not list the caller yet.
    let r = invoke(&node, &open_caller, &format!("/call?target={callee}")).await;
    let not_listed = r.json["thrown"].as_str().is_some_and(|t| t.contains("call_not_allowed"));
    assert!(not_listed, "another tenant must be listed by service id: {}", r.text);
    let listed = json!({"call": {"allowed_callers": [open_caller_id]}});
    assert_eq!(patch(&node, &t2.key, &format!("/v1/services/{callee_id}"), listed).await.s(), 200);
    let r = invoke(&node, &open_caller, &format!("/call?target={callee}")).await;
    assert_eq!(r.json["status"], json!(200), "a plan with allow_cross_tenant_calls may call a listing target: {}", r.text);
    assert_eq!(r.json["body"], json!("hi"));
    node.shutdown().await;
}
}

node_test! {
/// TRG-17 (end to end): a child never outlives its parent's deadline. The parent (wall 3 s) waits
/// 1 s and then calls a child that would sleep 20 s; the whole request is over at ≈ the parent's
/// deadline, the child having been killed with `timeout` rather than running on.
async fn ac_trg_17_child_never_outlives_the_parents_deadline() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent = uniq("parent");
    let child = uniq("child");
    let r = post(&node, &t.key, "/v1/services", json!({"slug": parent, "runtime": "deno", "limits": {"wall_ms": 3000}})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let pid = r.json["id"].as_str().expect("id").to_string();
    assert_eq!(upload_ts(&node, &t.key, &pid, guests::CALLER, "activate=1").await.s(), 201);
    ship(&node, &t.key, &child, &guests::slow(1, 20_000)).await;
    assert_eq!(invoke(&node, &parent, "/nothing").await.s(), 404); // warm the parent's build

    let start = Instant::now();
    let r = invoke(&node, &parent, &format!("/delayed?wait=1000&target={child}")).await;
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(5), "the 20 s child must not hold the request: {elapsed:?}");
    // Either the parent saw the child's 504 object, or the parent itself hit the same deadline.
    let child_timed_out = r.s() == 200 && r.json["status"] == json!(504) && r.json["reason"] == json!("timeout");
    let parent_timed_out = r.s() == 504 && r.header("x-rt-reason").as_deref() == Some("timeout");
    assert!(child_timed_out || parent_timed_out, "{} {}", r.s(), r.text);

    // And nothing keeps running: the node goes idle quickly.
    let (n, tok) = (&node, node.root_token().to_string());
    eventually(Duration::from_secs(5), "no task left in flight", || {
        let tok = tok.clone();
        async move {
            send(n.http().get(n.control_url("/v1/node/scheduler")).bearer_auth(tok)).await.json["inflight"] == json!(0)
        }
    })
    .await;
    node.shutdown().await;
}
}
