//! Auth, scopes, tenancy and quotas at the control API: SEC-02..06, 09, 14, 20, 32, 34 and
//! TRG-07 (HTTP level). SEC-01 (release build without a root token) is in the `process` binary.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Method;
use rt_sec::{Actor, AuditEvent, AuditSink};
use rt_server::StartOptions;
use rt_types::{Plan, TenantId, TriggerKind};
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

// ---------------------------------------------------------------------------------------------
// SEC-02
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_02_failed_authentication_is_rate_limited_per_ip() {
    let node = start_node().await;
    let mut statuses = Vec::new();
    let mut last = None;
    for _ in 0..11 {
        let r = send(node.http().get(node.control_url("/v1/services")).bearer_auth("wrong-token")).await;
        statuses.push(r.s());
        last = Some(r);
    }
    assert_eq!(statuses[..10], [401u16; 10], "the first 10 wrong tokens are 401: {statuses:?}");
    assert_eq!(statuses[10], 429, "the 11th within a minute is rate limited: {statuses:?}");
    let last = last.expect("11 requests");
    assert_eq!(last.code(), "rate_limited");
    assert!(last.header("retry-after").is_some(), "429 carries retry-after");
    node.shutdown().await;
}
}

node_test! {
async fn auth_accepts_root_token_tenant_keys_and_the_x_rt_key_header() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    assert_eq!(call(root(&node, Method::GET, "/v1/tenants"), None).await.s(), 200);
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200);
    let via_header = send(node.http().get(node.control_url("/v1/services")).header("x-rt-key", &t.key)).await;
    assert_eq!(via_header.s(), 200, "{}", via_header.text);

    // Malformed credentials: wrong prefix, truncated key, valid id with a wrong secret.
    let bads: Vec<String> = vec![
        "rtk_garbage".into(),
        "rtk_".into(),
        "Bearer".into(),
        format!("{}x", t.key),
        t.key.replace("rtk_", "rtx_"),
    ];
    for bad in &bads {
        let r = get(&node, bad, "/v1/services").await;
        assert_eq!(r.s(), 401, "credential `{bad}` must be rejected: {}", r.text);
        assert_eq!(r.code(), "unauthorized");
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-03 + scope matrix
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_03_key_without_deploy_scope_cannot_upload_versions() {
    let node = start_node().await;
    let writer = new_tenant_scopes(&node, "pro", &["services:read", "services:write"]).await;
    let svc = create_service(&node, &writer.key, &uniq("sec03"), "deno").await;

    let r = upload_ts(&node, &writer.key, &svc, guests::HELLO, "activate=1").await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "insufficient_scope");
    assert_eq!(get(&node, &writer.key, &format!("/v1/services/{svc}/versions")).await.items().len(), 0, "nothing was stored");

    let deployer = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy"]).await;
    let svc2 = create_service(&node, &deployer.key, &uniq("sec03b"), "deno").await;
    assert_eq!(upload_ts(&node, &deployer.key, &svc2, guests::HELLO, "activate=1").await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
async fn scopes_gate_every_write_surface() {
    let node = start_node().await;
    let full = new_tenant(&node, "pro").await;
    let svc = ship(&node, &full.key, &uniq("scopes"), guests::HELLO).await;

    // A key that may only read services.
    let (_, ro) = make_key(&node, &full.id, &["services:read"], None).await;
    let writes: Vec<(Method, String, Option<Value>)> = vec![
        (Method::POST, "/v1/services".into(), Some(json!({"slug": uniq("ro"), "runtime": "deno"}))),
        (Method::PATCH, format!("/v1/services/{svc}"), Some(json!({"name": "x"}))),
        (Method::DELETE, format!("/v1/services/{svc}"), None),
        (Method::PUT, format!("/v1/services/{svc}/live"), Some(json!({"version": 1}))),
        (Method::PUT, format!("/v1/services/{svc}/env"), Some(json!({"A": "1"}))),
        (Method::PUT, format!("/v1/services/{svc}/secrets/S"), Some(json!({"value": "v"}))),
        (Method::DELETE, format!("/v1/services/{svc}/secrets/S"), None),
        (Method::POST, format!("/v1/services/{svc}/queue"), Some(json!({"body": 1}))),
        (Method::POST, format!("/v1/services/{svc}/schedules"), Some(json!({"kind": "interval", "every_seconds": 60}))),
        (Method::GET, format!("/v1/services/{svc}/logs"), None),
        (Method::GET, format!("/v1/services/{svc}/data/export"), None),
        // Operator-only routes.
        (Method::GET, "/v1/tenants".into(), None),
        (Method::POST, "/v1/tenants".into(), Some(json!({"name": "n", "plan": "pro"}))),
        (Method::GET, "/v1/node/status".into(), None),
        (Method::PUT, "/v1/node/config".into(), Some(json!({}))),
        (Method::POST, "/v1/node/drain".into(), None),
        (Method::GET, "/v1/node/cache".into(), None),
        (Method::POST, format!("/v1/tenants/{}/keys", full.id), Some(json!({"scopes": ["deploy"]}))),
    ];
    for (m, path, body) in writes {
        let r = call(api(&node, &ro, m.clone(), &path), body).await;
        assert_eq!(r.s(), 403, "{m} {path} with a read-only key: {}", r.text);
        assert_eq!(r.code(), "insufficient_scope", "{m} {path}");
    }
    // …while reading is fine.
    assert_eq!(get(&node, &ro, &format!("/v1/services/{svc}")).await.s(), 200);
    assert_eq!(get(&node, &ro, "/v1/services").await.s(), 200);

    // Specific scopes unlock exactly their surface.
    let (_, env_key) = make_key(&node, &full.id, &["services:read", "env:write"], None).await;
    assert_eq!(put(&node, &env_key, &format!("/v1/services/{svc}/env"), json!({"A": "1"})).await.s(), 200);
    assert_eq!(put(&node, &env_key, &format!("/v1/services/{svc}/secrets/S"), json!({"value": "v"})).await.s(), 403);
    let (_, logs_key) = make_key(&node, &full.id, &["services:read", "logs:read"], None).await;
    assert_eq!(get(&node, &logs_key, &format!("/v1/services/{svc}/logs")).await.s(), 200);
    assert_eq!(get(&node, &logs_key, &format!("/v1/services/{svc}/stats")).await.s(), 200);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-04 / SEC-05
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_04_key_limited_to_one_service_cannot_see_the_tenants_other_services() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    // x lives in its own project, y in the default one: a key limited to x's project sees only x.
    let px = create_project(&node, &t.key, &uniq("only"), &[]).await;
    let x = create_service_in(&node, &t.key, &px, "production", &uniq("only-x"), "deno").await;
    let r = upload_ts(&node, &t.key, &x, guests::HELLO, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "upload: {} {}", r.s(), r.text);
    let y = ship(&node, &t.key, &uniq("not-y"), guests::HELLO).await;
    let (_, restricted) = make_key(&node, &t.id, &["services:read", "services:write", "deploy", "logs:read"], Some(vec![px.clone()])).await;

    assert_eq!(get(&node, &restricted, &format!("/v1/services/{x}")).await.s(), 200);
    for path in [
        format!("/v1/services/{y}"),
        format!("/v1/services/{y}/versions"),
        format!("/v1/services/{y}/logs"),
        format!("/v1/services/{y}/events"),
    ] {
        let r = get(&node, &restricted, &path).await;
        assert_eq!(r.s(), 404, "GET {path} with a key limited to another service: {}", r.text);
        assert_eq!(r.code(), "not_found");
    }
    let r = upload_ts(&node, &restricted, &y, guests::HELLO, "activate=1").await;
    assert_eq!(r.s(), 404, "deploying to a service outside the key's list: {}", r.text);
    let r = patch(&node, &restricted, &format!("/v1/services/{y}"), json!({"name": "hijack"})).await;
    assert_eq!(r.s(), 404);

    let list = get(&node, &restricted, "/v1/services").await;
    let ids: Vec<String> = list.items().iter().map(|s| s["id"].as_str().unwrap_or("").to_string()).collect();
    assert!(ids.contains(&x), "its own service is listed: {ids:?}");
    assert!(!ids.contains(&y), "the other service must not be listed: {ids:?}");
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_05_cross_tenant_access_is_404_and_reveals_nothing() {
    let node = start_node().await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    let theirs = ship(&node, &t2.key, &uniq("t2svc"), guests::HELLO).await;
    let ghost = "svc_01J00000000000000000000000";

    let probes: Vec<(Method, &str, Option<Value>)> = vec![
        (Method::GET, "", None),
        (Method::PATCH, "", Some(json!({"name": "x"}))),
        (Method::DELETE, "", None),
        (Method::GET, "/versions", None),
        (Method::GET, "/versions/1", None),
        (Method::GET, "/versions/1/source", None),
        (Method::PUT, "/live", Some(json!({"version": 1}))),
        (Method::GET, "/env", None),
        (Method::PUT, "/env", Some(json!({"A": "1"}))),
        (Method::GET, "/secrets", None),
        (Method::PUT, "/secrets/S", Some(json!({"value": "v"}))),
        (Method::GET, "/schedules", None),
        (Method::POST, "/schedules", Some(json!({"kind": "interval", "every_seconds": 60}))),
        (Method::POST, "/queue", Some(json!({"body": 1}))),
        (Method::GET, "/queue", None),
        (Method::GET, "/dlq", None),
        (Method::GET, "/logs", None),
        (Method::GET, "/stats", None),
        (Method::GET, "/events", None),
        (Method::GET, "/data/export", None),
        (Method::GET, "/hosts", None),
    ];
    for (m, suffix, body) in probes {
        let real = call(api(&node, &t1.key, m.clone(), &format!("/v1/services/{theirs}{suffix}")), body.clone()).await;
        let none = call(api(&node, &t1.key, m.clone(), &format!("/v1/services/{ghost}{suffix}")), body).await;
        assert_eq!(real.s(), 404, "{m} {suffix}: another tenant's service must be 404, not 403: {}", real.text);
        assert_eq!(real.code(), "not_found");
        assert_eq!(real.json, none.json, "{m} {suffix}: the answer must not distinguish 'exists' from 'does not exist'");
    }

    // Tenant-level routes: operator only (403), and never the other tenant's data.
    let r = get(&node, &t1.key, &format!("/v1/tenants/{}", t2.id)).await;
    assert!(r.s() == 403 || r.s() == 404, "{}", r.text);
    let r = get(&node, &t1.key, &format!("/v1/tenants/{}/keys", t2.id)).await;
    assert!(r.s() == 403 || r.s() == 404, "{}", r.text);
    assert!(
        get(&node, &t1.key, "/v1/services").await.items().iter().all(|s| s["tenant_id"] == json!(t1.id)),
        "listings never include another tenant's services"
    );
    // The owner still sees its service untouched.
    assert_eq!(get(&node, &t2.key, &format!("/v1/services/{theirs}")).await.s(), 200);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-06
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_06_revoked_key_is_rejected_within_a_second() {
    let node = start_node().await;
    let tenant = create_tenant(&node, "pro").await;
    let (key_id, key) = make_key(&node, &tenant, &ALL_SCOPES, None).await;
    // Use it (so the node caches the key), then revoke it.
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 200);
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 200);

    let del = call(root(&node, Method::DELETE, &format!("/v1/tenants/{tenant}/keys/{key_id}")), None).await;
    assert_eq!(del.s(), 204, "{}", del.text);
    let start = std::time::Instant::now();
    let r = get(&node, &key, "/v1/services").await;
    assert_eq!(r.s(), 401, "{}", r.text);
    assert!(start.elapsed() < Duration::from_secs(1));

    let keys = call(root(&node, Method::GET, &format!("/v1/tenants/{tenant}/keys")), None).await;
    assert!(keys.items().is_empty(), "the revoked key is gone from the listing");
    // Deleting it again is a 404.
    let again = call(root(&node, Method::DELETE, &format!("/v1/tenants/{tenant}/keys/{key_id}")), None).await;
    assert_eq!(again.s(), 404);
    node.shutdown().await;
}
}

node_test! {
async fn expired_keys_are_rejected() {
    let node = start_node().await;
    let tenant = create_tenant(&node, "pro").await;
    let r = call(
        root(&node, Method::POST, &format!("/v1/tenants/{tenant}/keys")),
        Some(json!({"scopes": ["services:read"], "expires_at": rfc3339_in(2)})),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let key = r.json["secret"].as_str().expect("secret").to_string();
    assert_eq!(get(&node, &key, "/v1/services").await.s(), 200, "valid until it expires");
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    let r = get(&node, &key, "/v1/services").await;
    assert_eq!(r.s(), 401, "an expired key is unauthorized: {}", r.text);

    // An expiry in the past is refused at creation.
    let r = call(
        root(&node, Method::POST, &format!("/v1/tenants/{tenant}/keys")),
        Some(json!({"scopes": ["services:read"], "expires_at": rfc3339_in(-3600)})),
    )
    .await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-09 / TRG-07
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_09_plan_without_cron_rejects_cron_schedules() {
    let node = start_node().await; // `free`: no cron in allowed_triggers, min_schedule_seconds = 60
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("sec09"), "deno").await;

    let r = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "cron", "cron": "*/5 * * * *", "timezone": "UTC"})).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "trigger_not_allowed");
    let ok = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 60})).await;
    assert_eq!(ok.s(), 201, "interval is allowed on the free plan: {}", ok.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_trg_07_interval_below_the_plan_minimum_is_422_schedule_too_frequent() {
    let node = start_node_with(|c| {
        c.plans.insert(
            "ten".into(),
            Plan {
                min_schedule_seconds: 10,
                max_schedules: 3,
                allowed_triggers: TriggerKind::ALL.to_vec(),
                ..Plan::default()
            },
        );
    })
    .await;
    let t = new_tenant(&node, "ten").await;
    let svc = create_service(&node, &t.key, &uniq("trg07"), "deno").await;
    let path = format!("/v1/services/{svc}/schedules");

    let r = post(&node, &t.key, &path, json!({"kind": "interval", "every_seconds": 5})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    assert_eq!(r.code(), "schedule_too_frequent");
    assert!(get(&node, &t.key, &path).await.items().is_empty(), "nothing was created");

    for every in [10, 30, 60] {
        let r = post(&node, &t.key, &path, json!({"kind": "interval", "every_seconds": every})).await;
        assert_eq!(r.s(), 201, "every {every} s: {}", r.text);
    }
    // plan.max_schedules = 3: the fourth is a quota error.
    let r = post(&node, &t.key, &path, json!({"kind": "interval", "every_seconds": 120})).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "quota_exceeded");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-14 / SEC-34
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_14_eleventh_service_exceeds_the_free_plan() {
    let node = start_node().await; // free: max_services = 10
    let t = new_tenant(&node, "free").await;
    let mut ids = Vec::new();
    for i in 0..10 {
        ids.push(create_service(&node, &t.key, &format!("quota-{i}"), "deno").await);
    }
    let r = post(&node, &t.key, "/v1/services", json!({"slug": "quota-10", "runtime": "deno"})).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "quota_exceeded");

    // Deleting one frees a slot.
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{}", ids[0])).await.s(), 204);
    let r = post(&node, &t.key, "/v1/services", json!({"slug": "quota-10", "runtime": "deno"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);

    // The quota is per tenant.
    let other = new_tenant(&node, "free").await;
    assert_eq!(post(&node, &other.key, "/v1/services", json!({"slug": "quota-other", "runtime": "deno"})).await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_34_twenty_first_upload_in_an_hour_is_build_quota() {
    let node = start_node().await; // free: builds_per_hour = 20
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("sec34"), "deno").await;
    for i in 0..20 {
        let r = upload_ts(&node, &t.key, &svc, &guests::versioned(i), "activate=0").await;
        assert_eq!(r.s(), 201, "upload {i}: {}", r.text);
    }
    let versions = format!("/v1/services/{svc}/versions");
    let before = get(&node, &t.key, &versions).await.items().len();
    let r = upload_ts(&node, &t.key, &svc, &guests::versioned(99), "activate=0").await;
    assert_eq!(r.s(), 429, "{}", r.text);
    assert_eq!(r.code(), "build_quota");
    // `deploy.retain_versions` (10) keeps the list short; the rejected upload adds nothing to it.
    assert_eq!(before, 10, "only the newest `retain_versions` versions are kept");
    assert_eq!(get(&node, &t.key, &versions).await.items().len(), before, "no 21st version");

    // Another tenant has its own budget.
    let other = new_tenant(&node, "free").await;
    let svc2 = create_service(&node, &other.key, &uniq("sec34b"), "deno").await;
    assert_eq!(upload_ts(&node, &other.key, &svc2, guests::HELLO, "activate=0").await.s(), 201);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-20: audit
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<AuditEvent>>,
}

impl AuditSink for Recorder {
    fn emit(&self, ev: AuditEvent) {
        self.events.lock().expect("recorder lock").push(ev);
    }
}

impl Recorder {
    fn count(&self, action: &str) -> usize {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .filter(|e| e.action == action)
            .count()
    }

    fn last(&self, action: &str) -> Option<AuditEvent> {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .rev()
            .find(|e| e.action == action)
            .cloned()
    }
}

node_test! {
async fn ac_sec_20_security_relevant_actions_emit_exactly_one_audit_event_each() {
    let rec = Arc::new(Recorder::default());
    let sink: Arc<dyn AuditSink> = rec.clone();
    let node = start_node_opts(StartOptions { extra_audit: Some(sink), ..Default::default() }, None, |_| {}).await;
    let tenant_id = create_tenant(&node, "pro").await;
    let tenant: TenantId = tenant_id.parse().expect("tenant id");
    let (key_id, key) = make_key(&node, &tenant_id, &ALL_SCOPES, None).await;

    // Key creation (operator).
    assert_eq!(rec.count("key.created"), 1, "events: {:?}", rec.events.lock().expect("lock").iter().map(|e| e.action.clone()).collect::<Vec<_>>());
    let ev = rec.last("key.created").expect("event");
    assert_eq!(ev.actor, Actor::Operator);
    assert_eq!(ev.tenant, Some(tenant));

    // Deploy + rollback (tenant key).
    let slug = uniq("audit");
    let svc = ship(&node, &key, &slug, &guests::versioned(1)).await;
    assert_eq!(rec.count("deploy"), 1);
    let ev = rec.last("deploy").expect("deploy event");
    assert_eq!(ev.tenant, Some(tenant));
    assert_eq!(ev.service.map(|s| s.to_string()), Some(svc.clone()));
    match &ev.actor {
        Actor::Key { key_id: k, tenant: t } => {
            assert_eq!(*t, tenant);
            assert!(k.contains(key_id.trim_start_matches("key_")) || *k == key_id, "actor key {k} vs {key_id}");
        }
        other => panic!("a tenant-key deploy must be attributed to the key, got {other:?}"),
    }
    assert_eq!(upload_ts(&node, &key, &svc, &guests::versioned(2), "activate=1").await.s(), 201);
    assert_eq!(rec.count("deploy"), 2);
    assert_eq!(put(&node, &key, &format!("/v1/services/{svc}/live"), json!({"version": 1})).await.s(), 200);
    assert_eq!(rec.count("rollback"), 1);
    assert_eq!(rec.last("rollback").expect("rollback").service.map(|s| s.to_string()), Some(svc.clone()));

    // Secret write: audited, and the value never appears in the event.
    let secret_value = "audit-canary-7c1e";
    let r = put(&node, &key, &format!("/v1/services/{svc}/secrets/TOKEN"), json!({"value": secret_value})).await;
    assert!(r.s() == 204 || r.s() == 200, "{}", r.text);
    assert_eq!(rec.count("secret.written"), 1);
    let ev = rec.last("secret.written").expect("event");
    assert!(!serde_json::to_string(&ev).expect("json").contains(secret_value), "secret values never reach the audit log");

    // Tenant change and key deletion (operator).
    let r = call(root(&node, Method::PATCH, &format!("/v1/tenants/{tenant_id}")), Some(json!({"name": "audited"}))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(rec.count("tenant.updated"), 1);
    assert_eq!(rec.last("tenant.updated").expect("event").tenant, Some(tenant));
    let r = call(root(&node, Method::DELETE, &format!("/v1/tenants/{tenant_id}/keys/{key_id}")), None).await;
    assert_eq!(r.s(), 204);
    assert_eq!(rec.count("key.deleted"), 1);

    // Every event names its actor; none is anonymous.
    for e in rec.events.lock().expect("lock").iter() {
        assert!(matches!(e.actor, Actor::Operator | Actor::Key { .. } | Actor::System), "{e:?}");
    }

    // And the service's `events` endpoint has rows for the deploys / rollback (the `events` table).
    let (_, key2) = make_key(&node, &tenant_id, &ALL_SCOPES, None).await;
    let events = get(&node, &key2, &format!("/v1/services/{svc}/events")).await;
    assert_eq!(events.s(), 200, "{}", events.text);
    assert!(events.items().len() >= 3, "deploy, deploy, rollback (at least): {}", events.text);
    for e in events.items() {
        assert!(e["kind"].is_string() && e["ts"].is_number() && e["id"].is_string(), "event row shape: {e}");
    }
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-32
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_32_custom_hosts_need_an_allowed_suffix_and_verification() {
    let node = start_node_with(|c| {
        c.routing.base_domain = "fn.example.com".to_string();
        c.security.reserved_hosts = vec!["reserved.bots.example.com".to_string()];
        // `pro` allows `.bots.example.com`; `free` allows nothing.
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let other = new_tenant(&node, "pro").await;
    let slug = uniq("hosts");
    let svc = ship(&node, &t.key, &slug, guests::HELLO).await;
    let other_svc = ship(&node, &other.key, &uniq("hosts2"), guests::HELLO).await;
    let put_host = |tenant: &Tenant, svc: &str, host: &str| {
        let rb = api(&node, &tenant.key, Method::PUT, &format!("/v1/services/{svc}/hosts/{host}"));
        send(rb)
    };

    // Allowed suffix: accepted but pending until the TXT record matches.
    let r = put_host(&t, &svc, "a.bots.example.com").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["status"], json!("pending_verification"));
    assert!(r.json["verify_token"].as_str().is_some_and(|t| !t.is_empty()), "the TXT token is handed out: {}", r.text);

    // Outside the suffix, reserved, and under the routing base domain: 403 host_not_allowed.
    for bad in ["admin.example.com", "reserved.bots.example.com", "x.fn.example.com", "bots.example.com.evil.net"] {
        let r = put_host(&t, &svc, bad).await;
        assert_eq!(r.s(), 403, "host `{bad}`: {}", r.text);
        assert_eq!(r.code(), "host_not_allowed", "host `{bad}`");
    }

    // A host mapped to another service of another tenant is taken.
    let r = put_host(&other, &other_svc, "a.bots.example.com").await;
    assert_eq!(r.s(), 409, "{}", r.text);
    assert_eq!(r.code(), "host_taken");

    // A pending host does not route yet (TRG-02 needs a *verified* host); the path route still does.
    let via_host = send(node.http().get(node.public_url("/")).header("host", "a.bots.example.com")).await;
    assert_eq!(via_host.s(), 404, "a pending host must not route: {}", via_host.text);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let hosts = get(&node, &t.key, &format!("/v1/services/{svc}/hosts")).await;
    assert_eq!(hosts.items().len(), 1);
    node.shutdown().await;
}
}

node_test! {
async fn a_disabled_tenant_serves_nothing_and_its_keys_stop_working_until_re_enabled() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("disabled");
    ship(&node, &t.key, &slug, guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200);

    let disable = |on: bool| call(root(&node, Method::PATCH, &format!("/v1/tenants/{}", t.id)), Some(json!({"disabled": on})));
    assert_eq!(disable(true).await.s(), 200);
    let r = invoke(&node, &slug, "/").await;
    assert_eq!((r.s(), r.text.as_str()), (404, "not_found"), "a disabled tenant's services are not routed");
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 401, "its keys no longer authenticate");

    assert_eq!(disable(false).await.s(), 200);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200, "re-enabling restores routing");
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200, "and the keys");
    node.shutdown().await;
}
}

node_test! {
async fn free_plan_tenants_cannot_attach_custom_hosts() {
    let node = start_node().await; // free: custom_host_suffixes = [] (operator only)
    let t = new_tenant(&node, "free").await;
    let svc = create_service(&node, &t.key, &uniq("nohost"), "deno").await;
    let r = call(api(&node, &t.key, Method::PUT, &format!("/v1/services/{svc}/hosts/bot.example.com")), None).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "host_not_allowed");
    node.shutdown().await;
}
}
