//! Instances and autoscaling (decision D-B2, SCH-35, SCH-36 and SCH-38; SCH-31..SCH-34 and SCH-37 are in rt-sched): `scaling.max_instances`
//! caps concurrent copies of a service and queues the excess (D-A3), `scaling.min_instances` keeps
//! the service's warm target and admission state through the idle TTL.

use std::time::Duration;

use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

/// Create a Deno service with a `scaling` block, ship `src`, return its id.
async fn ship_scaled(node: &impl Node, key: &str, slug: &str, scaling: Value, src: &str) -> String {
    let created = post(
        node,
        key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "scaling": scaling}),
    )
    .await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(node, key, &svc, src, "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    svc
}

async fn admission(node: &impl Node, key: &str, svc: &str) -> Value {
    let v = get(node, key, &format!("/v1/services/{svc}")).await;
    assert_eq!(v.s(), 200, "{}", v.text);
    v.json["status"]["admission"].clone()
}

node_test! {
/// SCH-35: a service with `max_instances = 1` runs one request at a time; the second
/// concurrent request waits in the queue (it is neither rejected nor run beside the first) and is
/// answered after the first finishes. The service view shows the bound, the queue and the wait.
async fn a_service_with_max_instances_1_queues_the_second_concurrent_request() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("one");
    let svc = ship_scaled(&node, &t.key, &slug, json!({"max_instances": 1}), &guests::slow(1, 800)).await;
    // warm up: the lazy build is not part of the measured window
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let url = node.public_url(&format!("/s/{slug}/"));
    let both: Vec<_> = (0..2).map(|_| tokio::spawn(fetch(node.http().clone(), url.clone()))).collect();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let adm = admission(&node, &t.key, &svc).await;
    assert_eq!((adm["running"].as_u64(), adm["queued"].as_u64()), (Some(1), Some(1)), "one runs, one waits: {adm}");
    assert_eq!((adm["instances"].as_u64(), adm["max_instances"].as_u64(), adm["limit"].as_u64()), (Some(1), Some(1), Some(1)), "{adm}");

    let mut elapsed = Vec::new();
    for h in both {
        let (status, body, _, ms) = h.await.expect("join");
        assert_eq!((status, body.as_str()), (200, "v1"), "the queued request is served, not rejected");
        elapsed.push(ms);
    }
    elapsed.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    assert!(elapsed[0] < 1_400.0, "the first request ran at once: {elapsed:?}");
    assert!(elapsed[1] >= 1_500.0, "the second waited for the first (2 x 800 ms): {elapsed:?}");

    let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    assert_eq!(view.json["effective"]["max_instances"]["value"], json!(1), "{}", view.text);
    assert_eq!(view.json["effective"]["max_instances"]["source"], json!("service"));
    let adm = &view.json["status"]["admission"];
    assert!(adm["queue_wait_ms"].as_f64().is_some_and(|w| w > 0.0), "the wait is reported: {adm}");
    node.shutdown().await;
}
}

node_test! {
/// SCH-36: with the idle TTL at 1 s, a service without `min_instances` scales to zero
/// shortly after its last request (no warm target); one with `min_instances = 2` keeps at least 2
/// warm instances and its limit never drops below 2, long after its idle TTL.
async fn a_service_with_min_instances_keeps_its_warm_target_after_idle() {
    let node = start_node_with(|c| c.cache.idle_ttl = Some(Duration::from_secs(1))).await;
    let t = new_tenant(&node, "pro").await;
    let (warm_slug, cold_slug) = (uniq("warm"), uniq("cold"));
    let warm = ship_scaled(&node, &t.key, &warm_slug, json!({"min_instances": 2}), guests::HELLO).await;
    let cold = ship_scaled(&node, &t.key, &cold_slug, json!({}), guests::HELLO).await;
    for slug in [&warm_slug, &cold_slug] {
        for _ in 0..3 {
            assert_eq!(invoke(&node, slug, "/").await.s(), 200);
        }
    }

    // both get a warm target from their demand ...
    let (n, key, cold_id) = (&node, t.key.as_str(), cold.as_str());
    eventually(Duration::from_secs(10), "the cold service gets a demand-driven warm target", || async move {
        admission(n, key, cold_id).await["target_instances"].as_u64().is_some_and(|t| t > 0)
    })
    .await;
    // ... and once the cold service has been idle past its TTL its target is gone
    eventually(Duration::from_secs(20), "the cold service scales to zero", || async move {
        let a = admission(n, key, cold_id).await;
        a["target_instances"] == json!(0) && a["warm_per_proc"] == json!(0)
    })
    .await;
    // ... while the other keeps its floor
    let a = admission(&node, &t.key, &warm).await;
    assert!(a["target_instances"].as_u64().is_some_and(|n| n >= 2), "at least min_instances stay warm: {a}");
    assert!(a["warm_per_proc"].as_u64().is_some_and(|n| n >= 1), "{a}");
    assert!(a["limit"].as_u64().is_some_and(|l| l >= 2) && a["min_instances"] == json!(2), "{a}");
    assert!(a["idle_ms"].as_u64().is_some_and(|ms| ms >= 1_000), "it has been idle past the TTL: {a}");
    let view = get(&node, &t.key, &format!("/v1/services/{warm}")).await;
    assert_eq!(view.json["effective"]["min_instances"]["value"], json!(2), "{}", view.text);
    assert_eq!(view.json["scaling"]["min_instances"], json!(2));

    // and the node-wide view lists both with the same numbers
    let sched = call(root(&node, reqwest::Method::GET, "/v1/node/scheduler"), None).await;
    let entry = sched.json["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|e| e["service_id"] == json!(warm))
        .unwrap_or_else(|| panic!("{warm} in {}", sched.text));
    assert!(entry["target_instances"].as_u64().is_some_and(|n| n >= 2), "{entry}");
    assert_eq!(entry["min_instances"], json!(2));

    // dropping the floor from the config scales the idle service to zero without another request
    let unpinned = patch(&node, &t.key, &format!("/v1/services/{warm}"), json!({"scaling": {"max_instances": 8}})).await;
    assert_eq!(unpinned.s(), 200, "{}", unpinned.text);
    let warm_id = warm.as_str();
    eventually(Duration::from_secs(20), "the unpinned service scales to zero", || async move {
        let a = admission(n, key, warm_id).await;
        a.is_null() || (a["target_instances"] == json!(0) && a["min_instances"] == json!(0))
    })
    .await;
    node.shutdown().await;
}
}

node_test! {
/// D-B2 / CFG-10 end to end: the plan's `max_instances` clamps a service's bounds (the `free`
/// plan allows 4): the stored config keeps what the owner asked for, `effective` and the admission
/// view show the plan's value and its source.
async fn the_plan_caps_the_instance_bounds_of_a_service() {
    let node = start_node().await;
    let t = new_tenant(&node, "free").await;
    let slug = uniq("capped");
    let svc = ship_scaled(&node, &t.key, &slug, json!({"min_instances": 9, "max_instances": 50}), guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    let view = get(&node, &t.key, &format!("/v1/services/{svc}")).await;
    let eff = &view.json["effective"];
    assert_eq!((eff["max_instances"]["value"].as_u64(), eff["max_instances"]["source"].as_str()), (Some(4), Some("plan")), "{eff}");
    assert_eq!((eff["min_instances"]["value"].as_u64(), eff["min_instances"]["source"].as_str()), (Some(4), Some("plan")), "{eff}");
    let adm = &view.json["status"]["admission"];
    assert_eq!((adm["max_instances"].as_u64(), adm["min_instances"].as_u64()), (Some(4), Some(4)), "{adm}");
    assert_eq!(view.json["scaling"]["max_instances"], json!(50), "the stored config is what the owner asked for");
    node.shutdown().await;
}
}

/// The service view's `(status.tier, status.build_state)`.
async fn tier_and_build(node: &impl Node, key: &str, svc: &str) -> (String, String) {
    let v = get(node, key, &format!("/v1/services/{svc}")).await;
    let s = |p: &str| v.json["status"][p].as_str().unwrap_or_default().to_string();
    (s("tier"), s("build_state"))
}

node_test! {
/// SCH-38: a service with `min_instances` is warmed without waiting for its first request. When its
/// version is activated, the scheduler gets its floor (admission state and warm target) and the
/// artifact is loaded and built into T1 off the request path; activating the next version moves the
/// warm target and the load to it. A service without a floor stays cold.
async fn a_min_instances_service_is_warmed_on_activation_before_any_request() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (warm_slug, cold_slug) = (uniq("pre"), uniq("cold"));
    let warm = ship_scaled(&node, &t.key, &warm_slug, json!({"min_instances": 2}), &guests::versioned(1)).await;
    // different code: identical sources share one prepared artifact
    let cold = ship_scaled(&node, &t.key, &cold_slug, json!({}), &guests::versioned(7)).await;

    let (n, key, warm_id) = (&node, t.key.as_str(), warm.as_str());
    eventually(Duration::from_secs(20), "the floor is published and the artifact loaded, with no request", || async move {
        let a = admission(n, key, warm_id).await;
        a["target_instances"].as_u64().is_some_and(|t| t >= 2)
            && tier_and_build(n, key, warm_id).await == ("T1".to_string(), "ready".to_string())
    })
    .await;
    let a = admission(&node, &t.key, &warm).await;
    assert_eq!((a["min_instances"].as_u64(), a["running"].as_u64(), a["queued"].as_u64()), (Some(2), Some(0), Some(0)), "{a}");
    assert!(a["limit"].as_u64().is_some_and(|l| l >= 2), "{a}");
    assert_eq!(admission(&node, &t.key, &cold).await, Value::Null, "no floor, no request: no state");
    assert_ne!(tier_and_build(&node, &t.key, &cold).await.0, "T1", "and nothing is loaded for it");

    // a new version goes live: the next artifact is built and loaded the same way
    let up = upload_ts(&node, &t.key, &warm, &guests::versioned(2), "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    eventually(Duration::from_secs(20), "v2 is built and loaded before any request", || async move {
        tier_and_build(n, key, warm_id).await == ("T1".to_string(), "ready".to_string())
            && admission(n, key, warm_id).await["target_instances"].as_u64().is_some_and(|t| t >= 2)
    })
    .await;
    let view = get(&node, &t.key, &format!("/v1/services/{warm}")).await;
    assert_eq!(view.json["live_version"], json!(2), "{}", view.text);
    assert_eq!(invoke(&node, &warm_slug, "/").await.text, "v2");

    // giving up the floor scales the service down again
    let unpinned = patch(&node, &t.key, &format!("/v1/services/{warm}"), json!({"scaling": {}})).await;
    assert_eq!(unpinned.s(), 200, "{}", unpinned.text);
    assert_eq!(admission(&node, &t.key, &warm).await["min_instances"], json!(0));
    node.shutdown().await;
}
}

/// SCH-38 at node start: after a restart the service shows its floor before any request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_min_instances_service_is_warmed_when_the_node_starts() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("boot");
    let svc = ship_scaled(&node, &t.key, &slug, json!({"min_instances": 2}), guests::HELLO).await;
    node.restart().await;

    let (n, key, svc_id) = (&node, t.key.as_str(), svc.as_str());
    eventually(
        Duration::from_secs(20),
        "the floor is published at start",
        || async move {
            let a = admission(n, key, svc_id).await;
            a["target_instances"].as_u64().is_some_and(|t| t >= 2)
                && tier_and_build(n, key, svc_id).await == ("T1".to_string(), "ready".to_string())
        },
    )
    .await;
    let a = admission(&node, &t.key, &svc).await;
    assert_eq!(
        (a["min_instances"].as_u64(), a["running"].as_u64()),
        (Some(2), Some(0)),
        "{a}"
    );
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    node.stop().await;
}

node_test! {
/// D-B2: the plan's `max_warm_floor_total` holds on the node too. A tenant above it (its plan was
/// downgraded) keeps only the floors that fit, oldest service first; a request to a service whose
/// floor does not fit does not bring it back.
async fn a_tenant_above_its_warm_floor_cap_keeps_only_the_floors_that_fit() {
    let node = start_node_with(|c| {
        let mut capped = pro_plan();
        capped.max_warm_floor_total = 3;
        c.plans.insert("capped".to_string(), capped);
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let first = ship_scaled(&node, &t.key, &uniq("first"), json!({"min_instances": 2}), guests::HELLO).await;
    let second_slug = uniq("second");
    let second = ship_scaled(&node, &t.key, &second_slug, json!({"min_instances": 2}), guests::HELLO).await;
    let (n, key) = (&node, t.key.as_str());
    let (first_id, second_id) = (first.as_str(), second.as_str());
    eventually(Duration::from_secs(10), "both floors are held", || async move {
        admission(n, key, first_id).await["min_instances"] == json!(2) && admission(n, key, second_id).await["min_instances"] == json!(2)
    })
    .await;

    let r = call(root(&node, reqwest::Method::PATCH, &format!("/v1/tenants/{}", t.id)), Some(json!({"plan": "capped"}))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    eventually(Duration::from_secs(10), "the floor that does not fit is released", || async move {
        let a = admission(n, key, second_id).await;
        a.is_null() || a["min_instances"] == json!(0)
    })
    .await;
    assert_eq!(admission(&node, &t.key, &first).await["min_instances"], json!(2), "the oldest floor fits and is kept");
    assert_eq!(invoke(&node, &second_slug, "/").await.s(), 200);
    assert_eq!(admission(&node, &t.key, &second).await["min_instances"], json!(0), "a request does not bring it back");
    node.shutdown().await;
}
}

node_test! {
/// mrt_flow_01: with `min_run_time_ms = 0` and no work waiting, the isolate is gone once the answer
/// has been sent, so the next request on an idle service sees fresh module state. The control
/// service has the same source and `min_run_time_ms` unset: its isolate is kept and the module
/// counter carries on. Both services reuse isolates (`deno.reuse_isolates`), so only the keep-alive
/// setting differs.
async fn mrt_flow_01_zero_min_run_time_gives_fresh_module_state_when_idle() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let src = r#"
let n = 0;
export default function (): Response { n += 1; return new Response("n" + n); }
"#;
    let mut svcs = Vec::new();
    for (tag, scaling) in [("mrt01z", json!({"min_run_time_ms": 0})), ("mrt01k", json!({}))] {
        let slug = uniq(tag);
        let created = post(
            &node,
            &t.key,
            "/v1/services",
            json!({"slug": slug, "runtime": "deno", "deno": {"reuse_isolates": true}, "scaling": scaling}),
        )
        .await;
        assert_eq!(created.s(), 201, "{}", created.text);
        let svc = created.json["id"].as_str().expect("id").to_string();
        let up = upload_ts(&node, &t.key, &svc, src, "activate=1").await;
        assert_eq!(up.s(), 201, "{}", up.text);
        svcs.push(slug);
    }
    let (zero_slug, kept_slug) = (&svcs[0], &svcs[1]);

    for slug in [zero_slug, kept_slug] {
        let first = invoke(&node, slug, "/").await;
        assert_eq!((first.s(), first.text.as_str()), (200, "n1"), "{slug}: {}", first.text);
    }
    // wait for the disposal the keep-alive metric reports, not for a fixed delay
    let (client, metrics) = (node.http().clone(), node.metrics_url("/metrics"));
    eventually(Duration::from_secs(10), "the zero service is disposed after its answer", || {
        let (client, metrics) = (client.clone(), metrics.clone());
        async move {
            let text = send(client.get(metrics)).await.text;
            metric_values(&text, "rt_instance_keepalive_total")
                .iter()
                .any(|(series, v)| series.contains("kind=\"deno\",outcome=\"disposed\"") && *v >= 1.0)
        }
    })
    .await;

    let fresh = invoke(&node, zero_slug, "/").await;
    assert_eq!((fresh.s(), fresh.text.as_str()), (200, "n1"), "min_run_time 0 starts fresh when idle: {}", fresh.text);
    let carried = invoke(&node, kept_slug, "/").await;
    assert_eq!((carried.s(), carried.text.as_str()), (200, "n2"), "without the setting the isolate is kept: {}", carried.text);
    node.shutdown().await;
}
}
