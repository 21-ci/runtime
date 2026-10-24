//! Review handoff B5.2 / B5.3 and decision D-A1 end to end: service environments and
//! `call.allowed_callers` (API-15, TRG-19/20), plan `allowed_triggers` at route time (SEC-37),
//! key-auth failure limiting on the public listener (SEC-39), the control lockout behind a trusted
//! proxy (SEC-40), the open debug control API (SEC-42) and expiring host claims (SEC-43).

use std::time::Duration;

use reqwest::Method;
use rt_server::{start_with, StartOptions};
use rt_testkit::NodeSetup;
use rt_types::TriggerKind;
use serde_json::json;

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

fn raw_get(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").into_bytes()
}

// ---------------------------------------------------------------------------------------------
// API-15: environment + call block on the service resource
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_15_services_carry_an_environment_and_call_rules() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["staging", "dev"]).await;

    let plain = post(&node, &t.key, "/v1/services", json!({"slug": uniq("plain"), "runtime": "wasm"})).await;
    assert_eq!(plain.s(), 201, "{}", plain.text);
    assert_eq!(plain.json["environment"], json!("production"), "the default environment");
    assert_eq!(plain.json["call"], json!({"allowed_callers": []}));

    let body = json!({"slug": uniq("stg"), "runtime": "wasm", "environment": "staging",
                      "call": {"allowed_callers": ["env:dev", "web", plain.json["id"]]}});
    let staged = post(&node, &t.key, "/v1/services", body).await;
    assert_eq!(staged.s(), 201, "{}", staged.text);
    assert_eq!(staged.json["environment"], json!("staging"));
    assert_eq!(staged.json["call"]["allowed_callers"], json!(["env:dev", "web", plain.json["id"]]));

    for bad in [json!({"environment": "Bad Env"}), json!({"environment": ""}), json!({"call": {"allowed_callers": ["env:"]}}),
                json!({"call": {"allowed_callers": ["svc_not-an-id"]}}), json!({"call": {"callers": []}})] {
        let mut b = json!({"slug": uniq("bad"), "runtime": "wasm"});
        for (k, v) in bad.as_object().expect("object") {
            b[k] = v.clone();
        }
        let r = post(&node, &t.key, "/v1/services", b).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{bad}: {}", r.text);
    }

    let id = plain.json["id"].as_str().expect("id");
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"environment": "dev"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!((r.json["environment"].clone(), r.json["call"].clone()), (json!("dev"), json!({"allowed_callers": []})));
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"call": {"allowed_callers": ["env:staging"]}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let got = get(&node, &t.key, &format!("/v1/services/{id}")).await;
    assert_eq!(got.json["environment"], json!("dev"), "PATCH of `call` keeps the environment");
    assert_eq!(got.json["call"], json!({"allowed_callers": ["env:staging"]}));
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"environment": "NOPE"})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// TRG-19 / TRG-20 through a real Deno caller
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_trg_20_a_staging_service_cannot_call_production_until_listed() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["staging"]).await;
    let target = uniq("prod-api");
    let target_id = ship(&node, &t.key, &target, guests::HELLO).await;
    let (staging, same_env) = (uniq("stg-web"), uniq("prod-web"));
    let staging_id = ship(&node, &t.key, &staging, guests::CALLER).await;
    ship(&node, &t.key, &same_env, guests::CALLER).await;
    assert_eq!(patch(&node, &t.key, &format!("/v1/services/{staging_id}"), json!({"environment": "staging"})).await.s(), 200);

    // TRG-19: same environment, empty list.
    let r = invoke(&node, &same_env, &format!("/call?target={target}")).await;
    assert_eq!(r.json["status"], json!(200), "production → production: {}", r.text);

    // TRG-20: staging → production is refused…
    let r = invoke(&node, &staging, &format!("/call?target={target}")).await;
    let refused = r.json["thrown"].as_str().is_some_and(|t| t.contains("call_not_allowed")) || r.json["reason"] == json!("call_not_allowed");
    assert!(refused, "staging → production needs an env: entry: {}", r.text);

    // …until the target lists `env:staging` (which then replaces the same-environment default).
    let r = patch(&node, &t.key, &format!("/v1/services/{target_id}"), json!({"call": {"allowed_callers": ["env:staging"]}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let r = invoke(&node, &staging, &format!("/call?target={target}")).await;
    assert_eq!(r.json["status"], json!(200), "listed environment: {}", r.text);
    let r = invoke(&node, &same_env, &format!("/call?target={target}")).await;
    assert_eq!(r.json["status"], json!(0), "a non-empty list names every caller: {}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-37: plan `allowed_triggers` at route time
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_37_a_plan_without_http_refuses_invocations_on_every_listener() {
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.allowed_triggers = vec![TriggerKind::Queue, TriggerKind::Call];
        c.plans.insert("nohttp".into(), plan);
    })
    .await;
    let t = new_tenant(&node, "nohttp").await;
    let slug = uniq("nohttp");
    ship_wasm(&node, &t.key, &slug, "hello").await;

    let r = invoke(&node, &slug, "/").await;
    assert_eq!((r.s(), r.header("x-rt-reason").as_deref()), (403, Some("trigger_not_allowed")), "{}", r.text);
    let sock = node.addrs.internal.clone().expect("internal socket");
    let raw = raw_unix(&sock, &raw_get(&format!("/s/{slug}/")), Duration::from_secs(10)).await.expect("response");
    assert_eq!(raw.status, 403, "the internal listener applies the plan too");

    // The operator moves the tenant to a plan with `http`: requests route again.
    let r = call(root(&node, Method::PATCH, &format!("/v1/tenants/{}", t.id)), Some(json!({"plan": "pro"}))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-39 / SEC-40: failed-auth limiter on the public listener and behind trusted proxies
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_39_wrong_invoke_keys_are_rate_limited_on_the_public_listener() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("keyed");
    let id = ship_wasm(&node, &t.key, &slug, "hello").await;
    assert_eq!(patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"access": "key"})).await.s(), 200);

    let wrong = "rtk_01J0000000000000000000000Z_wrongwrongwrongwrongwrongwrongwr";
    let mut statuses = Vec::new();
    let mut last = None;
    for _ in 0..11 {
        let r = invoke_with(&node, &slug, Method::GET, "/", &[("x-rt-key", wrong)], "").await;
        statuses.push(r.s());
        last = Some(r);
    }
    assert_eq!(statuses[..10], [401u16; 10], "{statuses:?}");
    assert_eq!(statuses[10], 429, "the 11th wrong key within a minute: {statuses:?}");
    let last = last.expect("11 requests");
    assert_eq!(last.header("x-rt-reason").as_deref(), Some("rate_limited"));
    assert!(last.header("retry-after").is_some());
    // The control budget is separate: a wrong token is a plain 401, the root token works.
    let r = send(node.http().get(node.control_url("/v1/services")).bearer_auth("wrong")).await;
    assert_eq!(r.s(), 401, "{}", r.text);
    let r = send(node.http().get(node.control_url("/v1/services")).bearer_auth(node.root_token())).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_40_the_control_lockout_follows_the_client_behind_a_trusted_proxy() {
    let node = start_node_with(|c| c.ingress.trusted_proxies = vec!["127.0.0.1/32".into(), "::1/128".into()]).await;
    let from = |client: &str, token: &str| {
        node.http().get(node.control_url("/v1/services")).bearer_auth(token).header("x-forwarded-for", client)
    };
    let mut statuses = Vec::new();
    for _ in 0..11 {
        statuses.push(send(from("203.0.113.10", "wrong")).await.s());
    }
    assert_eq!(statuses[10], 429, "{statuses:?}");
    // Another client behind the same proxy is not locked out.
    assert_eq!(send(from("203.0.113.11", "wrong")).await.s(), 401);
    assert_eq!(send(from("203.0.113.11", node.root_token())).await.s(), 200);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-42: a debug build without a root token only binds the control API to loopback
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_42_an_open_control_api_refuses_a_non_loopback_address() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut setup = NodeSetup::new(dir.path());
    setup.secrets.root_token = None;
    setup.config.listeners.control = "0.0.0.0:0".into();
    let err = start_with(setup.config.clone(), setup.secrets.clone(), StartOptions::default()).await.err().expect("refused");
    assert!(err.to_string().contains("RT_ROOT_TOKEN"), "{err}");

    setup.config.listeners.control = "127.0.0.1:0".into();
    let running = start_with(setup.config, setup.secrets, StartOptions::default()).await.expect("loopback is allowed");
    let url = format!("http://{}/v1/node/status", running.addrs().control);
    let r = reqwest::get(&url).await.expect("status");
    assert_eq!(r.status(), 200, "debug build without a token: the operator, on loopback only");
    running.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-43: pending host claims expire
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_43_an_expired_pending_claim_no_longer_blocks_other_services() {
    let node = start_node_with(|c| {
        c.hosts.pending_ttl = Duration::from_millis(300);
        c.hosts.verify_dns = false;
    })
    .await;
    let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
    let a_svc = create_service(&node, &a.key, &uniq("ha"), "wasm").await;
    let b_svc = create_service(&node, &b.key, &uniq("hb"), "wasm").await;
    let host = format!("{}.bots.example.com", uniq("shop"));

    let r = send(api(&node, &a.key, Method::PUT, &format!("/v1/services/{a_svc}/hosts/{host}"))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let (created, expires) = (r.json["created_at"].as_i64().expect("created_at"), r.json["expires_at"].as_i64().expect("expires_at"));
    assert_eq!(expires - created, 300, "expires_at = created_at + hosts.pending_ttl");
    let token = r.json["verify_token"].clone();
    // A re-PUT of one's own pending claim keeps the token (a published TXT record stays valid).
    let again = send(api(&node, &a.key, Method::PUT, &format!("/v1/services/{a_svc}/hosts/{host}"))).await;
    assert_eq!(again.json["verify_token"], token, "{}", again.text);

    let r = send(api(&node, &b.key, Method::PUT, &format!("/v1/services/{b_svc}/hosts/{host}"))).await;
    assert_eq!(r.code(), "host_taken", "a live pending claim blocks: {}", r.text);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let r = send(api(&node, &b.key, Method::PUT, &format!("/v1/services/{b_svc}/hosts/{host}"))).await;
    assert_eq!(r.s(), 200, "an expired claim no longer blocks: {}", r.text);
    assert_eq!(r.json["status"], json!("pending_verification"));
    let listed = get(&node, &a.key, &format!("/v1/services/{a_svc}/hosts")).await;
    assert_eq!(listed.json, json!([]), "A's expired claim is gone: {}", listed.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// Review findings: environment changes need an unscoped key (SEC-47), canonical caller ids,
// the per-service Deno knob, the plan's `queue` on API enqueue, host claim cooldown and cap
// (SEC-46), and the TCP internal listener opt-in (SEC-45).
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Recorder {
    events: std::sync::Mutex<Vec<rt_sec::AuditEvent>>,
}

impl rt_sec::AuditSink for Recorder {
    fn emit(&self, ev: rt_sec::AuditEvent) {
        self.events.lock().expect("recorder lock").push(ev);
    }
}

node_test! {
async fn ac_sec_47_only_an_unrestricted_key_or_the_operator_may_move_a_service_between_environments() {
    let rec = std::sync::Arc::new(Recorder::default());
    let sink: std::sync::Arc<dyn rt_sec::AuditSink> = rec.clone();
    let node = start_node_opts(StartOptions { extra_audit: Some(sink), ..Default::default() }, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["sandbox"]).await;
    let id = post(&node, &t.key, "/v1/services", json!({"slug": uniq("scoped"), "runtime": "wasm", "environment": "sandbox"})).await;
    assert_eq!(id.s(), 201, "{}", id.text);
    let id = id.json["id"].as_str().expect("id").to_string();
    // A key limited to the sandbox environment: it may not move the service out of it.
    let (_, scoped) = make_key_in(&node, &t.id, &["services:read", "services:write"], None, Some(vec!["sandbox".into()])).await;

    let r = patch(&node, &scoped, &format!("/v1/services/{id}"), json!({"environment": "production"})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    // The same key may still change everything else, and "change" to the current value is no change.
    let r = patch(&node, &scoped, &format!("/v1/services/{id}"), json!({"environment": "sandbox", "name": "x"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(rec.events.lock().expect("lock").iter().all(|e| e.action != "service.environment_changed"));

    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"environment": "production"})).await;
    assert_eq!(r.s(), 200, "an unrestricted key: {}", r.text);
    let ev = rec.events.lock().expect("lock").iter().rev().find(|e| e.action == "service.environment_changed").cloned();
    let ev = ev.expect("audited");
    assert_eq!(ev.data, json!({"from": "sandbox", "to": "production"}));
    node.shutdown().await;
}
}

node_test! {
async fn caller_ids_are_stored_canonical_and_the_deno_reuse_knob_round_trips() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let other = post(&node, &t.key, "/v1/services", json!({"slug": uniq("o"), "runtime": "wasm"})).await;
    let other_id = other.json["id"].as_str().expect("id").to_string();
    let body = json!({"slug": uniq("d"), "runtime": "deno", "deno": {"reuse_isolates": true},
                      "call": {"allowed_callers": [other_id.to_ascii_lowercase()]}});
    let r = post(&node, &t.key, "/v1/services", body).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert_eq!(r.json["call"]["allowed_callers"], json!([other_id]), "stored in canonical form");
    assert_eq!(r.json["deno"], json!({"reuse_isolates": true}));
    assert_eq!(r.json["effective"]["reuse_isolates"]["value"], json!(true), "{}", r.json["effective"]);
    let id = r.json["id"].as_str().expect("id").to_string();
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"deno": {}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json.get("deno").is_none(), "an empty block is unset: {}", r.text);
    assert_eq!(r.json["effective"]["reuse_isolates"]["value"], json!(false), "balanced profile default");
    node.shutdown().await;
}
}

node_test! {
async fn the_api_enqueue_needs_queue_in_the_tenants_plan() {
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.allowed_triggers = vec![TriggerKind::Http, TriggerKind::Call];
        c.plans.insert("noqueue".into(), plan);
    })
    .await;
    let t = new_tenant(&node, "noqueue").await;
    let id = create_service(&node, &t.key, &uniq("q"), "wasm").await;
    let r = post(&node, &t.key, &format!("/v1/services/{id}/queue"), json!({"body": {"x": 1}})).await;
    assert_eq!((r.s(), r.code()), (403, "trigger_not_allowed".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_46_host_claims_cannot_restart_their_ttl_and_are_capped_per_tenant() {
    let node = start_node_with(|c| {
        c.hosts.pending_ttl = Duration::from_millis(400);
        c.hosts.verify_dns = false;
        c.hosts.max_pending_per_tenant = 2;
    })
    .await;
    let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
    let a_svc = create_service(&node, &a.key, &uniq("ha"), "wasm").await;
    let b_svc = create_service(&node, &b.key, &uniq("hb"), "wasm").await;
    let host = format!("{}.bots.example.com", uniq("shop"));
    let claim = |key: &str, svc: &str, host: &str| send(api(&node, key, Method::PUT, &format!("/v1/services/{svc}/hosts/{host}")));

    let first = claim(&a.key, &a_svc, &host).await;
    assert_eq!(first.s(), 200, "{}", first.text);
    // DELETE + PUT continues the claim: same token, same expiry.
    let del = send(api(&node, &a.key, Method::DELETE, &format!("/v1/services/{a_svc}/hosts/{host}"))).await;
    assert_eq!(del.s(), 204, "{}", del.text);
    let again = claim(&a.key, &a_svc, &host).await;
    assert_eq!(again.s(), 200, "{}", again.text);
    assert_eq!((again.json["verify_token"].clone(), again.json["expires_at"].clone()), (first.json["verify_token"].clone(), first.json["expires_at"].clone()));

    // Once it expired, the same tenant waits one more TTL; another tenant may claim at once.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = claim(&a.key, &a_svc, &host).await;
    assert_eq!((r.s(), r.code()), (409, "host_cooldown".to_string()), "{}", r.text);
    assert!(r.json["error"]["details"]["retry_at"].as_i64().is_some(), "{}", r.text);
    let r = claim(&b.key, &b_svc, &host).await;
    assert_eq!(r.s(), 200, "another tenant: {}", r.text);

    // At most `max_pending_per_tenant` (2) pending claims per tenant.
    for i in 0..2 {
        let r = claim(&a.key, &a_svc, &format!("{}-{i}.bots.example.com", uniq("cap"))).await;
        assert_eq!(r.s(), 200, "{}", r.text);
    }
    let r = claim(&a.key, &a_svc, &format!("{}.bots.example.com", uniq("cap"))).await;
    assert_eq!((r.s(), r.code()), (403, "quota_exceeded".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_45_a_tcp_internal_listener_needs_the_explicit_opt_in() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut setup = NodeSetup::new(dir.path());
    setup.config.listeners.internal = "127.0.0.1:0".into();
    let err = start_with(setup.config.clone(), setup.secrets.clone(), StartOptions::default()).await.err().expect("refused");
    assert!(err.to_string().contains("allow_tcp_internal"), "{err}");
    setup.config.listeners.allow_tcp_internal = true;
    let running = start_with(setup.config, setup.secrets, StartOptions::default()).await.expect("opted in");
    running.shutdown().await;
}
}
