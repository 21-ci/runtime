//! Node startup and its background jobs: key material, listener rules, the passes that keep the
//! gauges, caches and sweeps current.

use std::sync::Arc;
use std::time::Duration;

use rt_server::{start, start_with, ServerError, StartOptions};
use rt_testkit::NodeSetup;
use serde_json::json;

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

fn fresh() -> (tempfile::TempDir, NodeSetup) {
    let dir = tempfile::tempdir().expect("tempdir");
    let setup = NodeSetup::new(dir.path());
    (dir, setup)
}

async fn refused(setup: NodeSetup) -> String {
    match start(setup.config, setup.secrets).await {
        Ok(running) => {
            running.shutdown().await;
            panic!("the node started")
        }
        Err(e) => e.to_string(),
    }
}

node_test! {
async fn a_node_without_a_master_key_makes_one_in_its_data_dir_and_keeps_it() {
    let (dir, mut setup) = fresh();
    setup.secrets.master_key = None;
    let first = start(setup.config.clone(), setup.secrets.clone()).await.expect("starts");
    first.shutdown().await;
    let path = dir.path().join("master.key");
    let made = std::fs::read_to_string(&path).expect("master.key was written");
    assert_eq!(made.trim().len(), 64, "32 bytes in hex");
    assert_eq!(std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&path).expect("meta").permissions()) & 0o777, 0o600);

    let again = start(setup.config, setup.secrets).await.expect("starts again with the same file");
    again.shutdown().await;
    assert_eq!(std::fs::read_to_string(&path).expect("master.key"), made, "a second start reuses it");
}
}

node_test! {
async fn a_malformed_master_key_file_or_previous_key_stops_the_node_before_it_serves() {
    let (dir, mut setup) = fresh();
    setup.secrets.master_key = None;
    std::fs::write(dir.path().join("master.key"), "not hex").expect("write");
    assert!(refused(setup).await.contains("invalid master.key"));

    let (_dir, mut setup) = setup_with_previous("zz");
    setup.secrets.master_key_previous = Some(rt_config::Secret::new("zz".to_string()));
    assert!(refused(setup).await.contains("invalid RT_MASTER_KEY_PREVIOUS"));
}
}

fn setup_with_previous(previous: &str) -> (tempfile::TempDir, NodeSetup) {
    let (dir, mut setup) = fresh();
    setup.secrets.master_key_previous = Some(rt_config::Secret::new(previous.to_string()));
    (dir, setup)
}

node_test! {
async fn a_node_started_with_a_previous_master_key_serves() {
    let (_dir, setup) = setup_with_previous(&hex::encode([5u8; 32]));
    let running = start(setup.config, setup.secrets).await.expect("starts");
    running.shutdown().await;
}
}

node_test! {
async fn a_tcp_internal_listener_needs_the_opt_in_and_a_loopback_address() {
    let (_dir, mut setup) = fresh();
    setup.config.listeners.internal = "127.0.0.1:0".into();
    let e = refused(setup).await;
    assert!(e.contains("allow_tcp_internal"), "{e}");

    let (_dir, mut setup) = fresh();
    setup.config.listeners.internal = "0.0.0.0:0".into();
    setup.config.listeners.allow_tcp_internal = true;
    let e = refused(setup).await;
    assert!(e.contains("loopback"), "{e}");
}
}

node_test! {
async fn requiring_a_v8_sandbox_this_build_does_not_have_stops_the_node() {
    let (_dir, mut setup) = fresh();
    setup.config.security.require_v8_sandbox = true;
    match start(setup.config, setup.secrets).await {
        Err(ServerError::V8SandboxRequired) => {}
        other => panic!("expected V8SandboxRequired, got {:?}", other.map(|_| ())),
    }
}
}

node_test! {
async fn a_node_with_hardening_on_and_no_root_token_starts_with_its_control_api_closed() {
    let (_dir, mut setup) = fresh();
    setup.config.security.seccomp = true;
    setup.secrets.root_token = None;
    let running = start_with(setup.config, setup.secrets, StartOptions::default()).await.expect("starts");
    running.shutdown().await;
}
}

node_test! {
async fn one_pass_of_every_periodic_job_keeps_the_node_healthy_and_publishes_the_queue_depth() {
    let handle = new_handle();
    let node = start_node_opts(StartOptions::default(), Some(handle.clone()), |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("q"), guests::QUEUE_RECORDER).await;
    let path = format!("/v1/services/{svc}/queue");
    for i in 0..2 {
        let r = post(&node, &t.key, &path, json!({"body": i, "delay_seconds": 3600})).await;
        assert_eq!(r.s(), 202, "{}", r.text);
    }
    // A service that does not consume a queue is not counted, whatever sits in its rows.
    let off = ship(&node, &t.key, &uniq("noq"), guests::QUEUE_RECORDER).await;
    let r = post(&node, &t.key, &format!("/v1/services/{off}/queue"), json!({"body": 1, "delay_seconds": 3600})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let r = patch(&node, &t.key, &format!("/v1/services/{off}"), json!({"triggers": {"queue": false}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    // 36 000 is a multiple of every interval of the maintenance pass: all its steps run.
    server.run_periodic_for_test(36_000).await;
    eventually(Duration::from_secs(5), "the depth gauge", || {
        let node = &node;
        async move {
            let text = send(node.http().get(node.metrics_url("/metrics"))).await.text;
            metric_sum(&text, "rt_queue_depth") == 2.0
        }
    })
    .await;
    let health = send(node.http().get(node.metrics_url("/healthz"))).await;
    assert_eq!(health.s(), 200, "the heartbeat pass keeps the node healthy: {}", health.text);
    drop(server);
    node.shutdown().await;
}
}

node_test! {
async fn a_wasm_service_with_a_warm_floor_is_preloaded_and_loses_the_floor_when_it_is_taken_away() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let created = post(&node, &t.key, "/v1/services", json!({"slug": uniq("floor"), "runtime": "wasm", "scaling": {"min_instances": 1}})).await;
    assert_eq!(created.s(), 201, "{}", created.text);
    let svc = created.json["id"].as_str().expect("id").to_string();
    let up = upload(&node, &t.key, &svc, "application/wasm", wasm_fixture("hello"), "activate=1").await;
    assert!(up.s() == 201 || up.s() == 202, "{}", up.text);
    eventually(Duration::from_secs(10), "the floor is held", || {
        let (node, key, svc) = (&node, t.key.as_str(), svc.as_str());
        async move { get(node, key, &format!("/v1/services/{svc}")).await.json["status"]["admission"]["min_instances"] == json!(1) }
    })
    .await;
    let r = patch(&node, &t.key, &format!("/v1/services/{svc}"), json!({"scaling": {"min_instances": 0}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    eventually(Duration::from_secs(10), "the floor is released", || {
        let (node, key, svc) = (&node, t.key.as_str(), svc.as_str());
        async move { get(node, key, &format!("/v1/services/{svc}")).await.json["status"]["admission"]["min_instances"] == json!(0) }
    })
    .await;
    node.shutdown().await;
}
}

// ---- store failures inside the periodic jobs ----------------------------------------------------

node_test! {
async fn a_store_that_fails_during_the_periodic_jobs_is_logged_and_the_node_carries_on() {
    let logs = rt_testkit::capture_tracing_globally();
    let (meta, data, queue) = (rt_store::Faults::new(), rt_store::Faults::new(), rt_store::Faults::new());
    let opts = StartOptions {
        faults: rt_server::StartFaults { meta: Some(meta.clone()), data: Some(data.clone()), queue: Some(queue.clone()) },
        ..StartOptions::default()
    };
    let handle = new_handle();
    let node = start_node_opts(opts, Some(handle.clone()), |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    // A service whose database is open, so the KV sweep has something to sweep.
    let slug = uniq("kv");
    ship(&node, &t.key, &slug, guests::KV).await;
    assert_eq!(invoke_with(&node, &slug, reqwest::Method::PUT, "/k", &[], "v").await.s(), 200);

    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    meta.fail_next(1000, rt_store::Fault::Io);
    data.fail_next(1000, rt_store::Fault::Io);
    queue.fail_next(1000, rt_store::Fault::Io);
    server.run_periodic_for_test(36_000).await;
    meta.clear();
    data.clear();
    queue.clear();

    for want in ["cache frequencies not saved", "expired-KV sweep failed"] {
        assert!(logs.contains(want), "no `{want}` in the log");
    }
    assert_eq!(invoke(&node, &slug, "/k").await.s(), 200, "the node serves on");
    drop(server);
    node.shutdown().await;
}
}

node_test! {
async fn a_schedule_whose_next_due_time_cannot_be_saved_still_fires_and_says_so() {
    let logs = rt_testkit::capture_tracing_globally();
    let meta = rt_store::Faults::new();
    let opts = StartOptions {
        faults: rt_server::StartFaults { meta: Some(meta.clone()), ..Default::default() },
        ..StartOptions::default()
    };
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("persist");
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
    // The schedule fires after 2 s; the store refuses to save its next due time right then.
    meta.fail_next(1000, rt_store::Fault::Io);
    tokio::time::sleep(Duration::from_secs(3)).await;
    meta.clear();
    assert!(logs.contains("next due time not persisted"), "{}", logs.text());
    let list = invoke(&node, &slug, "/").await;
    let runs = list.json["runs"].as_array().map_or(0, Vec::len);
    assert!(runs >= 1, "the schedule still ran: {}", list.text);
    node.shutdown().await;
}
}

node_test! {
async fn an_unreferenced_artifact_last_written_over_half_an_hour_ago_is_swept_by_the_periodic_pass() {
    let logs = rt_testkit::capture_tracing_globally();
    let handle = new_handle();
    let node = start_node_opts(StartOptions::default(), Some(handle.clone()), |_| {}).await;
    // An artifact directory named by a digest nothing references; its blob was written two hours ago.
    let orphan = node.data_dir().join("artifacts").join("ab".repeat(32));
    std::fs::create_dir_all(&orphan).expect("orphan dir");
    let blob = orphan.join("blob");
    std::fs::write(&blob, b"module").expect("blob");
    let two_hours_ago = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
    std::fs::File::options().write(true).open(&blob).expect("open").set_modified(two_hours_ago).expect("mtime");

    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    server.run_periodic_for_test(36_000).await;
    drop(server);
    assert!(!orphan.exists(), "the idle orphan is deleted");
    assert!(logs.contains("orphaned artifacts swept"), "{}", logs.text());
    node.shutdown().await;
}
}

node_test! {
async fn an_artifact_store_that_disappears_is_logged_by_the_periodic_sweep_and_the_node_carries_on() {
    let logs = rt_testkit::capture_tracing_globally();
    let handle = new_handle();
    let node = start_node_opts(StartOptions::default(), Some(handle.clone()), |_| {}).await;
    std::fs::remove_dir_all(node.data_dir().join("artifacts")).expect("remove the artifact store");

    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    server.run_periodic_for_test(36_000).await;
    drop(server);
    assert!(logs.contains("orphaned-artifact sweep failed"), "{}", logs.text());
    node.shutdown().await;
}
}

// ---- signals, rebuilds, live config, plugins ----------------------------------------------------

node_test! {
async fn waiting_for_a_signal_ignores_a_reload_and_ends_on_a_stop() {
    let waiting = tokio::spawn(rt_server::RunningServer::wait_for_signal());
    tokio::time::sleep(Duration::from_millis(300)).await; // the handlers are installed
    // SAFETY: raising a signal in this process; the node's handlers (tokio's) take it.
    unsafe { libc::raise(libc::SIGHUP) };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished(), "a reload does not stop the wait");
    unsafe { libc::raise(libc::SIGTERM) };
    tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("a stop ends the wait").expect("join");
}
}

node_test! {
async fn a_wasm_version_can_be_rebuilt_and_the_node_config_takes_a_rate_limit() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship_wasm(&node, &t.key, &uniq("w"), "hello").await;
    let slug = get(&node, &t.key, &format!("/v1/services/{svc}")).await.json["slug"].as_str().expect("slug").to_string();
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    let r = post(&node, &t.key, &format!("/v1/services/{svc}/versions/1/rebuild"), json!({})).await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    eventually(Duration::from_secs(20), "the rebuilt version serves", || {
        let (node, slug) = (&node, slug.as_str());
        async move { invoke(node, slug, "/").await.s() == 200 }
    })
    .await;

    let r = call(root(&node, reqwest::Method::PUT, "/v1/node/config"), Some(json!({"rate_limit": {"rps": 100}}))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["pending_restart"].as_array().is_some_and(|p| p.contains(&json!("rate_limit"))), "{}", r.text);
    node.shutdown().await;
}
}

/// Every request is the operator's: stands in for a deployment's own authenticator.
struct AlwaysOperator;

impl rt_sec::Authenticator for AlwaysOperator {
    fn authenticate(
        &self,
        _parts: &http::request::Parts,
        _listener: rt_types::Listener,
    ) -> Result<rt_sec::Principal, rt_sec::AuthError> {
        Ok(rt_sec::Principal::Operator)
    }
}

node_test! {
async fn a_node_started_with_its_own_authenticator_uses_it_and_survives_route_refreshes() {
    struct Isolation;
    impl rt_sec::IsolationBackend for Isolation {
        fn domain_for(&self, tenant: rt_types::TenantId) -> rt_sec::IsolationDomain {
            rt_sec::LocalIsolation.domain_for(tenant)
        }
    }
    let plugins = rt_sec::Plugins {
        authenticator: Arc::new(AlwaysOperator),
        secrets: Arc::new(rt_sec::LocalSecrets::new(&rt_sec::MasterKey::from_bytes([9; 32]))),
        audit: Arc::new(rt_sec::TracingAuditSink),
        isolation: Arc::new(Isolation),
    };
    // Without a root token and with a deployment's own authenticator the control API is not "open".
    let (_dir, mut setup) = fresh();
    setup.secrets.root_token = None;
    let running = start_with(setup.config, setup.secrets, StartOptions { plugins: Some(plugins.clone()), ..StartOptions::default() })
        .await
        .expect("starts");
    running.shutdown().await;
    let node = start_node_opts(StartOptions { plugins: Some(plugins), ..StartOptions::default() }, None, |_| {}).await;
    // No token at all: the injected authenticator lets the request in.
    let status = send(node.http().get(node.control_url("/v1/node/status"))).await;
    assert_eq!(status.s(), 200, "{}", status.text);
    node.shutdown().await;
}
}

node_test! {
async fn a_reload_warns_about_keys_that_need_a_restart_and_reports_a_route_refresh_that_failed() {
    let logs = rt_testkit::capture_tracing_globally();
    let meta = rt_store::Faults::new();
    let handle = new_handle();
    let opts = StartOptions { faults: rt_server::StartFaults { meta: Some(meta.clone()), ..Default::default() }, ..StartOptions::default() };
    let node = start_node_opts(opts, Some(handle.clone()), |_| {}).await;
    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("rt.toml");
    // `io.threads` is not one of the keys a reload applies.
    std::fs::write(&file, "[io]\nthreads = 3\n").expect("config");
    let opts = rt_config::LoadOptions { file: Some(file), env: vec![] };

    server.reload_config(&opts).await.expect("the new file loads and the tables refresh");
    assert!(logs.contains("configuration keys other than [plans] and the live keys changed"), "no warning");

    server.reload().await.expect("a plain reload of the tables");
    meta.fail_next(1000, rt_store::Fault::Io);
    let e = server.reload_config(&opts).await.expect_err("the refresh fails");
    assert!(matches!(e, ServerError::ReloadPartial(_)), "{e:?}");
    let e = server.reload().await.expect_err("and so does a plain reload");
    meta.clear();
    assert!(matches!(e, ServerError::Startup(_)), "{e:?}");
    drop(server);
    node.shutdown().await;
}
}

// ---- restarts, caches and sweeps -----------------------------------------------------------------

node_test! {
async fn a_restarted_node_seeds_every_tenants_usage_and_preloads_the_services_that_ask_for_it() {
    let logs = rt_testkit::capture_tracing_globally();
    let mut node = Restartable::boot(|c| c.cache.preload = Some(true)).await;
    let t = new_tenant(&node, "pro").await;
    let (deno, wasm) = (uniq("pre-deno"), uniq("pre-wasm"));
    ship(&node, &t.key, &deno, guests::HELLO).await;
    ship_wasm(&node, &t.key, &wasm, "hello").await;
    for slug in [&deno, &wasm] {
        assert_eq!(invoke(&node, slug, "/").await.s(), 200);
    }
    // More tenants than one page of the usage seed.
    for _ in 0..205 {
        create_tenant(&node, "free").await;
    }
    node.restart().await;
    for slug in [&deno, &wasm] {
        assert_eq!(invoke(&node, slug, "/").await.s(), 200, "{slug} serves after the restart");
    }
    assert!(logs.contains("preloading hot services into T1"), "no preload line");
    node.stop().await;
}
}

node_test! {
async fn expired_kv_rows_are_swept_by_the_periodic_pass() {
    let logs = rt_testkit::capture_tracing_globally();
    let handle = new_handle();
    let node = start_node_opts(StartOptions::default(), Some(handle.clone()), |_| {}).await;
    let t = new_tenant(&node, "pro").await;
        let kv = uniq("ttl");
    ship(
        &node,
        &t.key,
        &kv,
        r#"import { kv } from "std:kv";
           export default async function (): Promise<Response> { await kv.set("k", "v", { ttlSeconds: 1 }); return new Response("ok"); }"#,
    )
    .await;
    assert_eq!(invoke(&node, &kv, "/").await.s(), 200);
    tokio::time::sleep(Duration::from_millis(1500)).await; // the row expires

    let server = handle.lock().expect("handle lock").clone().expect("the running server");
    server.run_periodic_for_test(36_000).await;
    assert!(logs.contains("expired KV rows swept"), "the expired row was not swept");
    drop(server);
    node.shutdown().await;
}
}
