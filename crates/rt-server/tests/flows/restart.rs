//! Restart behavior: E2E-08 / ARC-04 (state per the persistence table survives), OBS-05 (logs
//! survive and are queryable by time range) and TRG-08 (schedules resume without back-filling).
//!
//! The node is stopped gracefully and started again on the same data directory and secrets
//! (`Restartable`); the listeners come back on new random ports.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};

use crate::common::*;

const ENV_READER: &str = r#"
export default function (): Response {
  return Response.json({ mode: Deno.env.get("MODE") ?? null, secret_len: (Deno.env.get("CANARY_SECRET") ?? "").length });
}"#;

async fn runs_of(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/").await.json["runs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// E2E-08: after a restart, all services, versions, live pointers, schedules, queues, env and
/// secrets are intact; the first request per service works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_e2e_08_restart_keeps_services_versions_live_schedules_queues_env_and_secrets() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;

    // alpha: two versions, rolled back to v1 (live pointer ≠ latest version).
    let alpha = uniq("alpha");
    let alpha_id = ship(&node, &t.key, &alpha, &guests::versioned(1)).await;
    assert_eq!(
        upload_ts(&node, &t.key, &alpha_id, &guests::versioned(2), "activate=1")
            .await
            .s(),
        201
    );
    assert_eq!(invoke(&node, &alpha, "/").await.text, "v2");
    assert_eq!(
        put(
            &node,
            &t.key,
            &format!("/v1/services/{alpha_id}/live"),
            json!({"version": 1})
        )
        .await
        .s(),
        200
    );
    assert_eq!(invoke(&node, &alpha, "/").await.text, "v1");
    let alpha_view = get(&node, &t.key, &format!("/v1/services/{alpha_id}")).await;

    // beta: SQL data written before the restart; env + secret.
    let beta = uniq("beta");
    let beta_id = ship(&node, &t.key, &beta, guests::NOTES).await;
    assert_eq!(
        invoke_with(&node, &beta, Method::POST, "/", &[], "kept across restarts")
            .await
            .s(),
        201
    );
    let env_slug = uniq("envs");
    let env_id = ship(&node, &t.key, &env_slug, ENV_READER).await;
    assert_eq!(
        put(
            &node,
            &t.key,
            &format!("/v1/services/{env_id}/env"),
            json!({"MODE": "prod"})
        )
        .await
        .s(),
        200
    );
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{env_id}/secrets/CANARY_SECRET"),
        json!({"value": "s3cr3t-value"}),
    )
    .await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);
    let before = invoke(&node, &env_slug, "/").await;
    assert_eq!(
        before.json,
        json!({"mode": "prod", "secret_len": 12}),
        "{}",
        before.text
    );

    // gamma: an interval schedule (hourly, so it does not fire during the test) and a cron one.
    let gamma = uniq("gamma");
    let gamma_id = ship(&node, &t.key, &gamma, guests::SCHEDULED_RECORDER).await;
    let s1 = post(
        &node,
        &t.key,
        &format!("/v1/services/{gamma_id}/schedules"),
        json!({"kind": "interval", "every_seconds": 3600, "payload": {"n": 1}}),
    )
    .await;
    assert_eq!(s1.s(), 201, "{}", s1.text);
    let s2 = post(
        &node,
        &t.key,
        &format!("/v1/services/{gamma_id}/schedules"),
        json!({"kind": "cron", "cron": "0 3 * * *", "timezone": "Asia/Tashkent"}),
    )
    .await;
    assert_eq!(s2.s(), 201, "{}", s2.text);

    // delta: a queue with a delayed message that is still waiting, and one consumed already.
    let delta = uniq("delta");
    let delta_id = ship(&node, &t.key, &delta, guests::QUEUE_RECORDER).await;
    assert_eq!(invoke(&node, &delta, "/").await.s(), 200);
    let q1 = post(
        &node,
        &t.key,
        &format!("/v1/services/{delta_id}/queue"),
        json!({"body": {"id": "consumed"}}),
    )
    .await;
    assert_eq!(q1.s(), 202, "{}", q1.text);
    let q2 = post(
        &node,
        &t.key,
        &format!("/v1/services/{delta_id}/queue"),
        json!({"body": {"id": "waiting"}, "delay_seconds": 3600}),
    )
    .await;
    assert_eq!(q2.s(), 202, "{}", q2.text);
    let (n, k) = (&node, &t.key);
    let d = delta.clone();
    eventually(Duration::from_secs(15), "the immediate message to be consumed", || {
        let d = d.clone();
        async move {
            invoke(n, &d, "/").await.json["got"]
                .as_array()
                .is_some_and(|g| g.len() == 1)
        }
    })
    .await;
    let _ = k;

    let schedules_before = get(&node, &t.key, &format!("/v1/services/{gamma_id}/schedules"))
        .await
        .items();
    assert_eq!(schedules_before.len(), 2);
    let services_before = get(&node, &t.key, "/v1/services").await.items().len();
    assert_eq!(services_before, 5, "alpha, beta, envs, gamma, delta");

    // --------------------------------------------------------------------------- restart
    node.restart().await;
    // --------------------------------------------------------------------------- after

    // The tenant key (hashed in meta.db) still authenticates; services are all there.
    let services = get(&node, &t.key, "/v1/services").await;
    assert_eq!(services.s(), 200, "{}", services.text);
    assert_eq!(services.items().len(), services_before, "all services are intact");

    // Versions and the live pointer (rolled back to v1) survive; the first request is served by v1.
    let alpha_after = get(&node, &t.key, &format!("/v1/services/{alpha_id}")).await;
    assert_eq!(
        alpha_after.json["live_version"],
        json!(1),
        "the live pointer is persisted"
    );
    assert_eq!(
        alpha_after.json["etag"], alpha_view.json["etag"],
        "service metadata is byte-for-byte the same"
    );
    assert_eq!(
        get(&node, &t.key, &format!("/v1/services/{alpha_id}/versions"))
            .await
            .items()
            .len(),
        2
    );
    let r = invoke(&node, &alpha, "/").await;
    assert_eq!(
        (r.s(), r.text.as_str(), version_of(&r)),
        (200, "v1", Some(1)),
        "{}",
        r.text
    );
    assert_eq!(
        put(
            &node,
            &t.key,
            &format!("/v1/services/{alpha_id}/live"),
            json!({"version": 2})
        )
        .await
        .s(),
        200,
        "v2 is still retained"
    );

    // Service data.
    let notes = invoke(&node, &beta, "/").await;
    assert_eq!(
        notes.json["notes"][0]["body"],
        json!("kept across restarts"),
        "{}",
        notes.text
    );
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{beta_id}")).await.s(), 200);

    // Env and sealed secrets.
    let after = invoke(&node, &env_slug, "/").await;
    assert_eq!(
        after.json,
        json!({"mode": "prod", "secret_len": 12}),
        "env and secrets are intact: {}",
        after.text
    );
    assert_eq!(
        get(&node, &t.key, &format!("/v1/services/{env_id}/env")).await.json,
        json!({"MODE": "prod"})
    );
    let names = get(&node, &t.key, &format!("/v1/services/{env_id}/secrets")).await;
    assert!(names.items().iter().any(|s| s["name"] == json!("CANARY_SECRET")));

    // Schedules.
    let schedules_after = get(&node, &t.key, &format!("/v1/services/{gamma_id}/schedules"))
        .await
        .items();
    assert_eq!(schedules_after.len(), 2, "schedules are rehydrated");
    let ids = |v: &Vec<Value>| {
        let mut x: Vec<String> = v.iter().map(|s| s["id"].as_str().unwrap_or("").to_string()).collect();
        x.sort();
        x
    };
    assert_eq!(ids(&schedules_after), ids(&schedules_before));
    for s in &schedules_after {
        assert!(
            s["next_due"].as_i64().is_some_and(|d| d > 0),
            "a next fire is computed after the restart: {s}"
        );
    }

    // Queue: the delayed message is still waiting, the consumed one is not redelivered.
    let stats = get(&node, &t.key, &format!("/v1/services/{delta_id}/queue")).await;
    assert_eq!(
        stats.json["depth"],
        json!(1),
        "the delayed message survived: {}",
        stats.text
    );
    let got = invoke(&node, &delta, "/").await;
    assert_eq!(
        got.json["got"].as_array().map(Vec::len),
        Some(1),
        "no redelivery of the acked message: {}",
        got.text
    );
    node.stop().await;
}

/// ARC-04 / persistence: tenants, key hashes, usage counters, events, logs
/// and tenant overrides survive a restart, and requests and API calls work right away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_arc_04_state_per_the_persistence_table_is_intact_after_a_restart() {
    let mut node = Restartable::boot(|_| {}).await;
    let tenant_id = {
        let r = call(
            root(&node, Method::POST, "/v1/tenants"),
            Some(json!({"name": "persist", "plan": "pro", "overrides": {"max_services": 7}})),
        )
        .await;
        assert_eq!(r.s(), 201, "{}", r.text);
        r.json["id"].as_str().expect("id").to_string()
    };
    let (key_id, key) = make_key(&node, &tenant_id, &ALL_SCOPES, None).await;
    let slug = uniq("persist");
    let svc = ship(&node, &key, &slug, guests::LOGGER).await;
    for _ in 0..5 {
        assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    }
    // Usage counters are flushed every 5 s; wait for them.
    let (n, tid) = (&node, tenant_id.clone());
    eventually(Duration::from_secs(15), "usage counters to be flushed", || {
        let tid = tid.clone();
        async move {
            let r = call(root(n, Method::GET, &format!("/v1/tenants/{tid}/usage")), None).await;
            r.items()
                .iter()
                .map(|u| u["invocations"].as_u64().unwrap_or(0))
                .sum::<u64>()
                >= 5
        }
    })
    .await;
    let events_before = get(&node, &key, &format!("/v1/services/{svc}/events"))
        .await
        .items()
        .len();
    assert!(events_before >= 1);

    node.restart().await;

    // API calls work immediately, with the same key and the root token.
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 200);
    let tenant = call(root(&node, Method::GET, &format!("/v1/tenants/{tenant_id}")), None).await;
    assert_eq!(tenant.s(), 200, "{}", tenant.text);
    assert_eq!(tenant.json["name"], json!("persist"));
    assert_eq!(tenant.json["overrides"], json!({"max_services": 7}));
    let keys = call(root(&node, Method::GET, &format!("/v1/tenants/{tenant_id}/keys")), None).await;
    assert!(keys.items().iter().any(|k| k["id"] == json!(key_id)));
    // Requests too (the first one may be a T2 hit).
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    // Counters kept: usage after the restart includes the 5 earlier invocations.
    let usage = call(
        root(&node, Method::GET, &format!("/v1/tenants/{tenant_id}/usage")),
        None,
    )
    .await;
    let total: u64 = usage
        .items()
        .iter()
        .map(|u| u["invocations"].as_u64().unwrap_or(0))
        .sum();
    assert!(total >= 5, "usage counters are persisted: {}", usage.text);
    let events_after = get(&node, &key, &format!("/v1/services/{svc}/events"))
        .await
        .items()
        .len();
    assert!(events_after >= events_before, "events are persisted");
    // A revoked key stays revoked across a restart.
    assert_eq!(
        call(
            root(&node, Method::DELETE, &format!("/v1/tenants/{tenant_id}/keys/{key_id}")),
            None
        )
        .await
        .s(),
        204
    );
    node.restart().await;
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 401);
    node.stop().await;
}

/// OBS-05: logs written before a restart are queryable by time range afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obs_05_logs_survive_a_restart_and_are_queryable_by_time_range() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("logs");
    let svc = ship(&node, &t.key, &slug, guests::LOG_PARAM).await;
    let mut task_ids = Vec::new();
    for i in 0..5 {
        let r = invoke(&node, &slug, &format!("/?msg=before-restart-{i}")).await;
        assert_eq!(r.s(), 200);
        task_ids.push(r.header("x-rt-task-id").expect("task id"));
    }
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(5), "the 5 records to be written", || async move {
        let r = get(n, k, &format!("/v1/services/{s}/logs?kind=runtime")).await;
        r.items().len() >= 5
    })
    .await;

    node.restart().await;

    let all = get(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/logs?kind=runtime&limit=100"),
    )
    .await;
    assert_eq!(all.s(), 200, "{}", all.text);
    let msgs: Vec<String> = all
        .items()
        .iter()
        .filter_map(|r| r["msg"].as_str().map(str::to_string))
        .collect();
    for i in 0..5 {
        assert!(
            msgs.contains(&format!("before-restart-{i}")),
            "record {i} is lost after the restart: {msgs:?}"
        );
    }
    let inside = get(
        &node,
        &t.key,
        &format!(
            "/v1/services/{svc}/logs?since={}&until={}",
            rfc3339_in(-3600),
            rfc3339_in(3600)
        ),
    )
    .await;
    assert!(inside.items().len() >= 5, "{}", inside.text);
    let outside = get(
        &node,
        &t.key,
        &format!(
            "/v1/services/{svc}/logs?since={}&until={}",
            rfc3339_in(-7200),
            rfc3339_in(-3600)
        ),
    )
    .await;
    assert!(outside.items().is_empty(), "{}", outside.text);
    let one = get(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/logs?task_id={}", task_ids[2]),
    )
    .await;
    assert_eq!(one.items().len(), 1);
    assert_eq!(one.items()[0]["msg"], json!("before-restart-2"));

    // New records append after the old ones.
    let r = invoke(&node, &slug, "/?msg=after-restart").await;
    assert_eq!(r.s(), 200);
    let (n, k, s) = (&node, &t.key, &svc);
    eventually(Duration::from_secs(5), "the new record", || async move {
        get(n, k, &format!("/v1/services/{s}/logs?kind=runtime&limit=100"))
            .await
            .items()
            .iter()
            .any(|r| r["msg"] == json!("after-restart"))
    })
    .await;
    node.stop().await;
}

/// TRG-08: with schedules in place the node is stopped for a while and restarted: the schedules
/// resume, and the ticks missed while it was down are not back-filled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_trg_08_schedules_resume_after_a_restart_without_backfilling_missed_ticks() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("resume");
    let svc = ship(&node, &t.key, &slug, guests::SCHEDULED_RECORDER).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    let s = post(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/schedules"),
        json!({"kind": "interval", "every_seconds": 2, "payload": {}}),
    )
    .await;
    assert_eq!(s.s(), 201, "{}", s.text);
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(10), "the first scheduled run", || {
        let sl = sl.clone();
        async move { !runs_of(n, &sl).await.is_empty() }
    })
    .await;

    // Down for 8 s: four ticks are missed.
    node.stop().await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let before = {
        node.start().await;
        runs_of(&node, &slug).await.len()
    };
    let restarted_at = Instant::now();

    // No burst of catch-up runs right after the restart…
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let soon = runs_of(&node, &slug).await.len();
    assert!(
        soon - before <= 2,
        "ticks missed during the downtime must not be back-filled: {} new runs in {:?}",
        soon - before,
        restarted_at.elapsed()
    );

    // …but the schedule keeps running afterwards.
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(10), "scheduled runs to resume", || {
        let sl = sl.clone();
        async move { runs_of(n, &sl).await.len() >= soon + 2 }
    })
    .await;
    assert_eq!(
        get(&node, &t.key, &format!("/v1/services/{svc}/schedules"))
            .await
            .items()
            .len(),
        1
    );
    node.stop().await;
}
