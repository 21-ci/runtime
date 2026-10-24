//! WSM-20: operator bindings from WASM (`std:host/platform`, the equivalent of the Deno
//! `std:platform` `platform.fetch`). The request goes through the node's binding facility
//! (`rt_host::ConfigBindings`): plan, method and path checks, the operator's credentials injected
//! upstream and never visible to the guest, and every refusal a response with `x-rt-reason`
//! (contracts C-8), never an error. Bindings belong to the operator SDK services of the system
//! tenant: these tests run as the system tenant, on behalf of a calling tenant.

use crate::common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{prepared, Reply, Req, Rig};
use rt_config::{BindingConfig, BindingRateLimit};
use rt_host::ConfigBindings;

const SECRET: &str = "Bearer operator-secret";

fn binding(name: &str, base_url: &str) -> BindingConfig {
    BindingConfig {
        name: name.into(),
        base_url: base_url.into(),
        inject_headers: BTreeMap::from([("authorization".to_string(), SECRET.to_string())]),
        tenant_header: "x-tenant".into(),
        allowed_plans: Vec::new(),
        allowed_methods: vec!["POST".into()],
        allowed_paths: vec!["/audit/*".into()],
        rate_limit: BindingRateLimit::default(),
    }
}

fn rig() -> Rig {
    let rig = Rig::new();
    let mut gated = binding("pro-only", "https://ops.example.test/api");
    gated.allowed_plans = vec!["pro".into()];
    let mut limited = binding("limited", "https://ops.example.test/api");
    limited.rate_limit = BindingRateLimit { rps: 1, burst: 1 };
    let bindings = ConfigBindings::new(
        vec![
            binding("acme", "https://ops.example.test/api"),
            gated,
            limited,
            binding("broken", "not a url"),
        ],
        Arc::new(|_tenant| "free".to_string()),
        rig.egress.clone(),
    );
    rig.platform(Arc::new(bindings))
}

/// An SDK call of the system tenant's `platform` component on behalf of `common::caller_stamp()`.
fn sdk_call(path: &str) -> Req {
    let mut caller = common::caller_stamp();
    caller.sdk = Some(rt_types::SdkStamp {
        name: "acme".into(),
        version: "1.0.0".into(),
    });
    let mut req = Req::get(path).tenant(rt_types::sdk::SYSTEM_TENANT_U128);
    req.trigger = Some(rt_types::Trigger::Call {
        parent: rt_types::TaskId(1),
        depth: 1,
    });
    req.caller = Some(caller);
    req
}

async fn fetch(rig: &mut Rig, binding: &str, method: &str, path: &str) -> Reply {
    let p = prepared("platform");
    rig.run(
        &p,
        sdk_call(&format!("/fetch?binding={binding}&method={method}&path={path}")),
    )
    .await
    .reply()
}

#[tokio::test]
async fn ac_wsm_20_a_binding_call_carries_the_operator_credentials_upstream_only() {
    let mut rig = rig();
    let r = fetch(&mut rig, "acme", "POST", "/audit/log").await;
    assert_eq!(
        (r.kv("status").as_str(), r.kv("reason").as_str(), r.kv("body").as_str()),
        ("200", "-", "mock upstream")
    );
    assert!(
        !r.text().contains("operator-secret"),
        "credentials reached the guest: {}",
        r.text()
    );

    let urls = rig.egress.requests.lock().clone();
    assert_eq!(urls, vec!["https://ops.example.test/api/audit/log".to_string()]);
    let headers = rig.egress.headers.lock()[0].clone();
    assert_eq!(
        headers.get("authorization").and_then(|v| v.to_str().ok()),
        Some(SECRET),
        "the guest's own authorization is replaced"
    );
    assert_eq!(headers.get("x-from").and_then(|v| v.to_str().ok()), Some("platform"));
    // The calling tenant, not the system tenant that runs the SDK.
    let tenant = common::caller_stamp().tenant.to_string();
    assert_eq!(
        headers.get("x-tenant").and_then(|v| v.to_str().ok()),
        Some(tenant.as_str())
    );
}

#[tokio::test]
async fn ac_wsm_20_a_tenant_component_never_reaches_a_binding() {
    let mut rig = rig();
    let p = prepared("platform");
    let r = rig
        .run(&p, Req::get("/fetch?binding=acme&method=POST&path=/audit/log"))
        .await
        .reply();
    assert_eq!(
        (r.kv("status").as_str(), r.kv("reason").as_str()),
        ("403", "binding_not_allowed")
    );
    assert!(rig.egress.requests.lock().is_empty());
}

#[tokio::test]
async fn ac_wsm_20_refusals_are_responses_with_a_reason_never_errors() {
    let mut rig = rig();
    let reason = |r: &Reply| (r.kv("status"), r.kv("reason"));
    let cases = [
        ("acme", "GET", "/audit/log", "403", "binding_denied"), // method not allowed
        ("acme", "POST", "/admin/keys", "403", "binding_denied"), // path not allowed
        ("acme", "POST", "/audit/../admin", "403", "binding_denied"), // traversal
        ("nope", "POST", "/audit/log", "404", "binding_not_found"),
        ("pro-only", "POST", "/audit/log", "403", "binding_not_allowed"), // the tenant's plan
        ("broken", "POST", "/audit/log", "502", "binding_upstream"),
    ];
    for (b, m, path, status, code) in cases {
        let r = fetch(&mut rig, b, m, path).await;
        assert_eq!(
            reason(&r),
            (status.to_string(), code.to_string()),
            "{b} {m} {path}: {}",
            r.text()
        );
    }
    assert!(
        rig.egress.requests.lock().is_empty(),
        "a refused call reached the upstream"
    );

    // the per-tenant rate limit: one call per second
    assert_eq!(reason(&fetch(&mut rig, "limited", "POST", "/audit/a").await).0, "200");
    assert_eq!(
        reason(&fetch(&mut rig, "limited", "POST", "/audit/b").await),
        ("429".into(), "rate_limited".into())
    );
}
