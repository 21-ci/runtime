//! Invoke surface: routing (TRG-01/04), invoke access (SEC-07/08), header hygiene (SEC-27),
//! ingress rules (SEC-19/35), quotas (SEC-15/16), HTTP limits and DoS guards (SEC-18/28), bindings
//! (SEC-25), error sanitization (SEC-26) and tenant isolation of host-side work (SEC-29).
//!
//! TRG-02 (custom host routing) needs a *verified* host. DNS TXT verification is not implemented
//! and the operator-approval path has no API yet, so only the base-domain rule is exercised end
//! to end here; the routing table itself is covered in rt-triggers.

use std::time::{Duration, Instant};

use reqwest::Method;
use rt_config::BindingConfig;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

fn merge(mut base: Value, extra: Value) -> Value {
    if let (Some(b), Value::Object(e)) = (base.as_object_mut(), extra) {
        b.extend(e);
    }
    base
}

/// Create a deno service with extra create-body fields and upload `src` (activated).
async fn svc_with(node: &impl Node, key: &str, slug: &str, extra: Value, src: &str) -> String {
    let r = post(
        node,
        key,
        "/v1/services",
        merge(json!({"slug": slug, "runtime": "deno"}), extra),
    )
    .await;
    assert_eq!(r.s(), 201, "create `{slug}`: {}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(node, key, &id, src, "activate=1").await;
    assert_eq!(up.s(), 201, "upload `{slug}`: {}", up.text);
    id
}

fn raw_get(path: &str, headers: &[(&str, &str)]) -> Vec<u8> {
    let mut s = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        s.push_str(&format!("{k}: {v}\r\n"));
    }
    s.push_str("\r\n");
    s.into_bytes()
}

// ---------------------------------------------------------------------------------------------
// TRG-01 / TRG-04 / routing
// ---------------------------------------------------------------------------------------------

node_test! {
/// TRG-01: `GET /s/a/x?y=1` — the handler sees `/x?y=1` and `x-rt-base-path: /s/a`; the runtime
/// adds `x-rt-task-id`, `x-rt-trigger: http` and a `traceparent`.
async fn ac_trg_01_handler_sees_the_stripped_path_base_path_and_runtime_headers() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("a");
    ship(&node, &t.key, &slug, guests::ECHO).await;

    let r = invoke(&node, &slug, "/x?y=1").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["path"], json!("/x"));
    assert_eq!(r.json["search"], json!("?y=1"));
    assert_eq!(r.json["headers"]["x-rt-base-path"], json!(format!("/s/{slug}")));
    assert_eq!(r.json["headers"]["x-rt-trigger"], json!("http"));
    assert_eq!(r.json["trigger"], json!("http"));
    assert_eq!(
        r.json["headers"]["x-rt-task-id"].as_str(),
        r.header("x-rt-task-id").as_deref(),
        "the task id the guest sees is the one in the response header"
    );
    assert!(r.json["headers"]["traceparent"].as_str().is_some_and(|t| t.starts_with("00-")), "traceparent is added when absent");

    // A caller-supplied traceparent is kept (trace continuation).
    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let r = invoke_with(&node, &slug, Method::GET, "/", &[("traceparent", tp)], "").await;
    assert!(r.json["headers"]["traceparent"].as_str().is_some_and(|t| t.contains("4bf92f3577b34da6a3ce929d0e0e4736")), "{}", r.text);

    // The root of the service, and a deep path with encoded characters.
    let r = invoke(&node, &slug, "").await;
    assert_eq!(r.json["path"], json!("/"));
    let r = invoke(&node, &slug, "/a%20b/c?x=%C3%A9&y=").await;
    assert_eq!(r.json["path"], json!("/a%20b/c"));
    assert_eq!(r.json["search"], json!("?x=%C3%A9&y="));

    // Unknown slug and unknown route: 404 text/plain with x-rt-reason (never the control JSON).
    let r = invoke(&node, "no-such-service", "/").await;
    assert_eq!(r.s(), 404);
    assert_eq!(r.header("x-rt-reason").as_deref(), Some("not_found"));
    assert!(r.header("content-type").unwrap_or_default().starts_with("text/plain"));
    node.shutdown().await;
}
}

node_test! {
/// The `{slug}.{routing.base_domain}` rule (TRG-02's sibling): the path is passed through unchanged
/// and there is no base path.
async fn routing_base_domain_maps_a_subdomain_to_the_service() {
    let node = start_node_with(|c| c.routing.base_domain = "fn.example.com".to_string()).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("sub");
    ship(&node, &t.key, &slug, guests::ECHO).await;

    let r = send(node.http().get(node.public_url("/hook?z=1")).header("host", format!("{slug}.fn.example.com"))).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["path"], json!("/hook"));
    assert_eq!(r.json["search"], json!("?z=1"));
    assert!(
        r.json["headers"].get("x-rt-base-path").is_none() || r.json["headers"]["x-rt-base-path"] == json!(""),
        "host routes carry no base path: {}",
        r.json["headers"]
    );
    let r = send(node.http().get(node.public_url("/")).header("host", "ghost.fn.example.com")).await;
    assert_eq!(r.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// TRG-04: through the internal socket `x-rt-invoker: user-42` reaches the handler as
/// `ctx.invoker`; on the public listener the header is stripped.
async fn ac_trg_04_invoker_header_is_honored_on_the_internal_socket_only() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("invoker");
    ship(&node, &t.key, &slug, guests::ECHO).await;
    let sock = node.addrs.internal.clone().expect("the test config uses a unix socket for the internal listener");

    let internal = raw_unix(&sock, &raw_get(&format!("/s/{slug}/"), &[("x-rt-invoker", "user-42")]), Duration::from_secs(10))
        .await
        .expect("a response on the internal socket");
    assert_eq!(internal.status, 200, "{}", internal.text());
    assert_eq!(internal.json()["invoker"], json!("user-42"), "ctx.invoker on the internal listener: {}", internal.text());

    let public = invoke_with(&node, &slug, Method::GET, "/", &[("x-rt-invoker", "user-42")], "").await;
    assert_eq!(public.s(), 200);
    assert_eq!(public.json["invoker"], Value::Null, "ctx.invoker must be empty on the public listener");
    assert!(public.json["headers"].get("x-rt-invoker").is_none(), "the spoofed header is stripped: {}", public.json["headers"]);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-07 / SEC-08
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-07: `access=key` — no key → 401; an invoke key of the owning tenant → 200; another
/// tenant's key → 401.
async fn ac_sec_07_access_key_requires_an_invoke_key_of_the_owning_tenant() {
    let node = start_node().await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    let slug = uniq("keyed");
    let svc = svc_with(&node, &t1.key, &slug, json!({"access": "key"}), guests::ECHO).await;
    let (_, invoke_key) = make_key(&node, &t1.id, &["invoke"], None).await;
    let (_, no_invoke) = make_key(&node, &t1.id, &["services:read"], None).await;
    let (_, other_key) = make_key(&node, &t2.id, &["invoke"], None).await;
    let _ = svc;

    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 401, "{}", r.text);
    assert_eq!(r.header("x-rt-reason").as_deref(), Some("unauthorized"));
    assert!(r.header("content-type").unwrap_or_default().starts_with("text/plain"));

    let bearer = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", &format!("Bearer {invoke_key}"))], "").await;
    assert_eq!(bearer.s(), 200, "{}", bearer.text);
    let header = invoke_with(&node, &slug, Method::GET, "/", &[("x-rt-key", &invoke_key)], "").await;
    assert_eq!(header.s(), 200, "{}", header.text);

    let theirs = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", &format!("Bearer {other_key}"))], "").await;
    assert_eq!(theirs.s(), 401, "another tenant's key: {}", theirs.text);
    let weak = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", &format!("Bearer {no_invoke}"))], "").await;
    assert_eq!(weak.s(), 401, "a key without the invoke scope: {}", weak.text);
    let junk = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", "Bearer rtk_not_a_key")], "").await;
    assert_eq!(junk.s(), 401);
    node.shutdown().await;
}
}

node_test! {
/// SEC-08: `access=internal` — `404` on the public listener, `200` on the internal one.
async fn ac_sec_08_access_internal_is_404_publicly_and_200_on_the_internal_listener() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("internal");
    svc_with(&node, &t.key, &slug, json!({"access": "internal"}), guests::HELLO).await;
    let sock = node.addrs.internal.clone().expect("internal socket");

    let public = invoke(&node, &slug, "/").await;
    assert_eq!(public.s(), 404, "{}", public.text);
    let internal = raw_unix(&sock, &raw_get(&format!("/s/{slug}/"), &[]), Duration::from_secs(10)).await.expect("response");
    assert_eq!(internal.status, 200, "{}", internal.text());
    assert_eq!(internal.text(), "hi");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-27: header hygiene
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-27: spoofed `x-rt-*` request headers never reach the guest (the runtime's own values do),
/// the invoke credential is stripped, hop-by-hop headers are removed.
async fn ac_sec_27_public_request_headers_are_sanitized_before_dispatch() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("hygiene");
    svc_with(&node, &t.key, &slug, json!({"access": "key"}), guests::ECHO).await;
    let (_, key) = make_key(&node, &t.id, &["invoke"], None).await;

    let spoof = [
        ("x-rt-invoker", "admin"),
        ("x-rt-tenant", "ten_01J00000000000000000000000"),
        ("x-rt-task-id", "task_spoofed"),
        ("x-rt-trigger", "queue"),
        ("x-rt-base-path", "/evil"),
        ("x-rt-anything-else", "1"),
        ("x-rt-key", key.as_str()),
        ("keep-alive", "timeout=5"),
        ("proxy-authorization", "Basic abc"),
        ("te", "trailers"),
        ("x-custom", "kept"),
    ];
    let r = invoke_with(&node, &slug, Method::GET, "/", &spoof, "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let h = &r.json["headers"];
    assert!(h.get("x-rt-invoker").is_none(), "x-rt-invoker spoof: {h}");
    assert!(h.get("x-rt-tenant").is_none(), "x-rt-tenant spoof: {h}");
    assert!(h.get("x-rt-anything-else").is_none(), "unknown x-rt-* header: {h}");
    assert!(h.get("x-rt-key").is_none(), "the invoke credential must not reach the guest: {h}");
    assert_eq!(r.json["invoker"], Value::Null);
    assert_ne!(h["x-rt-task-id"], json!("task_spoofed"), "runtime-set values win");
    assert_eq!(h["x-rt-task-id"].as_str(), r.header("x-rt-task-id").as_deref());
    assert_eq!(h["x-rt-trigger"], json!("http"), "runtime-set value present, not the spoofed one");
    assert_eq!(h["x-rt-base-path"], json!(format!("/s/{slug}")));
    for hop in ["keep-alive", "proxy-authorization", "te", "connection", "upgrade", "transfer-encoding"] {
        assert!(h.get(hop).is_none(), "hop-by-hop header `{hop}` must be removed: {h}");
    }
    assert_eq!(h["x-custom"], json!("kept"), "ordinary headers pass through");

    // The `Authorization` bearer that matched an rtk_ key is stripped, too.
    let r = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", &format!("Bearer {key}"))], "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["headers"].get("authorization").is_none(), "matched credential stripped: {}", r.json["headers"]);

    // On a public service the caller's own (non-rtk) Authorization is the guest's business.
    let pub_slug = uniq("hygiene-pub");
    svc_with(&node, &t.key, &pub_slug, json!({}), guests::ECHO).await;
    let r = invoke_with(&node, &pub_slug, Method::GET, "/", &[("authorization", "Bearer end-user-token")], "").await;
    assert_eq!(r.json["headers"]["authorization"], json!("Bearer end-user-token"));
    node.shutdown().await;
}
}

node_test! {
/// SEC-56: `Authorization` belongs to the guest unless it carries a runtime credential. On a
/// `public` service any non-`rtk_` value reaches the guest unchanged, an `rtk_` bearer never; on
/// an `access = "key"` service the guest's own token sent next to `x-rt-key` passes, and an `rtk_`
/// bearer is removed even when `x-rt-key` carried the key that matched.
async fn ac_sec_56_authorization_reaches_the_guest_unless_it_is_a_runtime_key() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, key) = make_key(&node, &t.id, &["invoke"], None).await;
    let (_, other) = make_key(&node, &t.id, &["invoke"], None).await;
    let public = uniq("auth-pub");
    svc_with(&node, &t.key, &public, json!({}), guests::ECHO).await;
    let keyed = uniq("auth-key");
    svc_with(&node, &t.key, &keyed, json!({"access": "key"}), guests::ECHO).await;

    for value in ["Bearer admin-0123456789abcdef", "Basic dXNlcjpwYXNz", "Token rtk_abc"] {
        let r = invoke_with(&node, &public, Method::GET, "/", &[("authorization", value)], "").await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(r.json["headers"]["authorization"], json!(value), "public service, `{value}` passes unchanged");
    }
    let r = invoke_with(&node, &public, Method::GET, "/", &[("authorization", &format!("Bearer {key}"))], "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["headers"].get("authorization").is_none(), "an rtk_ key never reaches a guest: {}", r.json["headers"]);

    let own = [("x-rt-key", key.as_str()), ("authorization", "Bearer admin-0123456789abcdef")];
    let r = invoke_with(&node, &keyed, Method::GET, "/", &own, "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["headers"]["authorization"], json!("Bearer admin-0123456789abcdef"), "the guest's own token");
    assert!(r.json["headers"].get("x-rt-key").is_none());

    let both = [("x-rt-key", key.as_str()), ("authorization", &format!("Bearer {other}"))];
    let r = invoke_with(&node, &keyed, Method::GET, "/", &both, "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["headers"].get("authorization").is_none(), "a second rtk_ key is stripped too: {}", r.json["headers"]);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// TRG-22: the client address for guests
// ---------------------------------------------------------------------------------------------

/// What a Deno guest knows about its client.
const CLIENT_ECHO: &str = r#"
export default function (req: Request, ctx: any): Response {
  return Response.json({ client: ctx.client ?? null, header: req.headers.get("x-rt-client-ip") });
}
"#;

node_test! {
/// TRG-22: guests see the client address ingress resolved behind `ingress.trusted_proxies`
/// (Deno: `ctx.client.ip` and `x-rt-client-ip`; WASM: `x-rt-client-ip`). A client-sent
/// `x-rt-client-ip` and an untrusted `X-Forwarded-For` change nothing; behind a trusted proxy it
/// is the forwarded client; the internal Unix socket has no address, so there is none.
async fn ac_trg_22_guests_see_the_resolved_client_ip_and_cannot_spoof_it() {
    let spoof = [("x-rt-client-ip", "6.6.6.6"), ("x-forwarded-for", "6.6.6.6")];
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let deno = uniq("client");
    ship(&node, &t.key, &deno, CLIENT_ECHO).await;
    let wasm = uniq("client-wasm");
    ship_wasm(&node, &t.key, &wasm, "hello").await;

    let r = invoke_with(&node, &deno, Method::GET, "/", &spoof, "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json, json!({ "client": { "ip": "127.0.0.1" }, "header": "127.0.0.1" }));
    let r = invoke_with(&node, &wasm, Method::GET, "/header/x-rt-client-ip", &spoof, "").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "127.0.0.1"));

    let sock = node.addrs.internal.clone().expect("internal socket");
    let internal = raw_unix(&sock, &raw_get(&format!("/s/{deno}/"), &spoof), Duration::from_secs(10)).await.expect("response");
    assert_eq!(internal.status, 200, "{}", internal.text());
    assert_eq!(internal.json(), json!({ "client": null, "header": null }), "no peer address on the Unix socket");
    node.shutdown().await;

    let node = start_node_with(|c| c.ingress.trusted_proxies = vec!["127.0.0.0/8".to_string(), "::1/128".to_string()]).await;
    let t = new_tenant(&node, "pro").await;
    ship(&node, &t.key, &deno, CLIENT_ECHO).await;
    ship_wasm(&node, &t.key, &wasm, "hello").await;
    let forwarded = [("x-forwarded-for", "6.6.6.6, 198.51.100.4, 127.0.0.2"), ("x-rt-client-ip", "6.6.6.6")];
    let r = invoke_with(&node, &deno, Method::GET, "/", &forwarded, "").await;
    assert_eq!(r.json["client"], json!({ "ip": "198.51.100.4" }), "the first untrusted hop from the right: {}", r.text);
    let r = invoke_with(&node, &wasm, Method::GET, "/header/x-rt-client-ip", &[("x-forwarded-for", "2001:db8::5")], "").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "2001:db8::5"));
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-19 / SEC-35: ingress rules
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-19: methods → 405, paths → 403, ip_allow → 403, rate limit → 429 + `retry-after`.
async fn ac_sec_19_ingress_rules_methods_paths_ips_and_rate_limit() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // Methods.
    let slug = uniq("methods");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"allowed_methods": ["GET", "POST"]}}), guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    assert_eq!(invoke_with(&node, &slug, Method::POST, "/", &[], "x").await.s(), 200);
    let r = invoke_with(&node, &slug, Method::DELETE, "/", &[], "").await;
    assert_eq!(r.s(), 405, "{}", r.text);
    assert_eq!(invoke_with(&node, &slug, Method::PUT, "/", &[], "x").await.s(), 405);

    // Paths.
    let slug = uniq("paths");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"allow_paths": ["/api/*", "/ok"]}}), guests::HELLO).await;
    assert_eq!(invoke(&node, &slug, "/api/users").await.s(), 200);
    assert_eq!(invoke(&node, &slug, "/ok").await.s(), 200);
    let r = invoke(&node, &slug, "/admin").await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 403);

    // IP allow list: the test client is 127.0.0.1.
    let allowed = uniq("ip-ok");
    svc_with(&node, &t.key, &allowed, json!({"ingress": {"ip_allow": ["127.0.0.0/8", "::1/128"]}}), guests::HELLO).await;
    assert_eq!(invoke(&node, &allowed, "/").await.s(), 200);
    let blocked = uniq("ip-no");
    svc_with(&node, &t.key, &blocked, json!({"ingress": {"ip_allow": ["10.0.0.0/8", "2001:db8::/32"]}}), guests::HELLO).await;
    let r = invoke(&node, &blocked, "/").await;
    assert_eq!(r.s(), 403, "{}", r.text);

    // Rate limit: burst 5 then 429 with retry-after; it refills.
    let slug = uniq("rate");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"rate_limit": {"rps": 5, "burst": 5}}}), guests::HELLO).await;
    let mut statuses = Vec::new();
    let mut limited = None;
    for _ in 0..12 {
        let r = invoke(&node, &slug, "/").await;
        statuses.push(r.s());
        if r.s() == 429 && limited.is_none() {
            limited = Some(r);
        }
    }
    let ok = statuses.iter().filter(|s| **s == 200).count();
    assert!((5..=7).contains(&ok), "burst 5 (+ a little refill) then limited: {statuses:?}");
    let limited = limited.unwrap_or_else(|| panic!("no 429 in {statuses:?}"));
    assert!(limited.header("retry-after").and_then(|v| v.parse::<u64>().ok()).is_some_and(|s| s >= 1), "{:?}", limited.headers);
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200, "the bucket refills");
    node.shutdown().await;
}
}

node_test! {
/// SEC-35: `ip_allow = ["2001:db8::/32"]`: requests from `2001:db8::1` pass, from `2001:db9::1`
/// get 403 (client addresses come from `X-Forwarded-For`, trusted from `ingress.trusted_proxies`).
async fn ac_sec_35_ipv6_ip_allow_via_a_trusted_proxy() {
    let node = start_node_with(|c| c.ingress.trusted_proxies = vec!["127.0.0.0/8".to_string(), "::1/128".to_string()]).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("v6");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"ip_allow": ["2001:db8::/32"]}}), guests::HELLO).await;

    let inside = invoke_with(&node, &slug, Method::GET, "/", &[("x-forwarded-for", "2001:db8::1")], "").await;
    assert_eq!(inside.s(), 200, "{}", inside.text);
    let outside = invoke_with(&node, &slug, Method::GET, "/", &[("x-forwarded-for", "2001:db9::1")], "").await;
    assert_eq!(outside.s(), 403, "{}", outside.text);
    // Without X-Forwarded-For the peer address (127.0.0.1) is not in the list.
    assert_eq!(invoke(&node, &slug, "/").await.s(), 403);
    node.shutdown().await;
}
}

node_test! {
/// `X-Forwarded-For` is ignored when the peer is not a trusted proxy (the default).
async fn x_forwarded_for_is_not_trusted_by_default() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("xff");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"ip_allow": ["10.0.0.0/8"]}}), guests::HELLO).await;
    let r = invoke_with(&node, &slug, Method::GET, "/", &[("x-forwarded-for", "10.1.2.3")], "").await;
    assert_eq!(r.s(), 403, "spoofed X-Forwarded-For must not satisfy ip_allow: {}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-15 / SEC-16: quotas
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-15: `invocations_per_day = 100`: the 101st invoke is `429 quota_exceeded` with `retry-after`.
async fn ac_sec_15_daily_invocation_quota_is_enforced_with_retry_after() {
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.invocations_per_day = 100;
        c.plans.insert("daily".to_string(), plan);
    })
    .await;
    let t = new_tenant(&node, "daily").await;
    let slug = uniq("daily");
    ship(&node, &t.key, &slug, guests::HELLO).await;

    for i in 0..100 {
        let r = invoke(&node, &slug, "/").await;
        assert_eq!(r.s(), 200, "invocation {} of 100: {}", i + 1, r.text);
    }
    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 429, "{}", r.text);
    assert_eq!(r.header("x-rt-reason").as_deref(), Some("quota_exceeded"));
    let retry = r.header("retry-after").and_then(|v| v.parse::<u64>().ok()).expect("retry-after seconds");
    assert!((1..=86_400).contains(&retry), "retry-after runs until UTC midnight: {retry}");

    // Per tenant: another tenant on the same plan is unaffected.
    let other = new_tenant(&node, "daily").await;
    let slug2 = uniq("daily2");
    ship(&node, &other.key, &slug2, guests::HELLO).await;
    assert_eq!(invoke(&node, &slug2, "/").await.s(), 200);
    node.shutdown().await;
}
}

node_test! {
/// SEC-16: `egress_mb_per_day = 1`, a guest downloading 2 MB fails with `egress_quota` after
/// ≈ 1 MB. Needs an upstream the SSRF guard lets through: the guard denies loopback by design and
/// there is no test allowance in the config yet, so this runs only where a public HTTP host is
/// reachable and is otherwise ignored.
#[ignore = "needs a public 2 MB download target (SSRF guard denies loopback); run manually with RT_TEST_EGRESS_URL"]
async fn ac_sec_16_egress_quota_cuts_a_large_download() {
    let url = std::env::var("RT_TEST_EGRESS_URL").expect("RT_TEST_EGRESS_URL must point at a public URL serving ≥ 2 MB");
    let node = start_node_with(|c| {
        let mut plan = pro_plan();
        plan.egress_mb_per_day = 1;
        c.plans.insert("tiny-egress".to_string(), plan);
    })
    .await;
    let t = new_tenant(&node, "tiny-egress").await;
    let slug = uniq("egress");
    let src = format!(
        r#"export default async function (): Promise<Response> {{
  let total = 0;
  try {{
    const res = await fetch("{url}");
    const reader = res.body!.getReader();
    for (;;) {{ const {{ done, value }} = await reader.read(); if (done) break; total += value.length; }}
    return Response.json({{ total, error: null }});
  }} catch (e) {{
    return Response.json({{ total, error: String((e as any)?.code ?? e) }});
  }}
}}"#
    );
    ship(&node, &t.key, &slug, &src).await;
    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.json["error"], json!("egress_quota"), "{}", r.text);
    let total = r.json["total"].as_u64().unwrap_or(0);
    assert!((900_000..=1_300_000).contains(&total), "cut at ≈ 1 MB, got {total}");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-18 / SEC-28: HTTP limits and DoS guards
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-18: a request with a 20 KB header gets `431`; so does one with more than 100 headers.
async fn ac_sec_18_oversized_headers_are_431() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("big-header");
    ship(&node, &t.key, &slug, guests::HELLO).await;

    let big = "a".repeat(20 * 1024);
    let r = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/"), &[("x-big", &big)]), Duration::from_secs(10))
        .await
        .expect("a response (not a silent close)");
    assert_eq!(r.status, 431, "{}", r.text());

    let many: Vec<(String, String)> = (0..120).map(|i| (format!("x-h{i}"), "v".to_string())).collect();
    let refs: Vec<(&str, &str)> = many.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let r = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/"), &refs), Duration::from_secs(10)).await.expect("response");
    assert_eq!(r.status, 431, "more than 100 headers: {}", r.text());

    // A normal request right after is fine.
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    node.shutdown().await;
}
}

node_test! {
/// SEC-28 (slowloris): a client that never finishes its request headers is cut off at
/// `ingress.header_timeout`.
async fn ac_sec_28_slow_header_clients_are_disconnected() {
    let node = start_node_with(|c| c.ingress.header_timeout = Duration::from_secs(2)).await;
    let mut s = tokio::net::TcpStream::connect(node.addrs.public).await.expect("connect");
    s.write_all(b"GET /s/x/ HTTP/1.1\r\nHost: a\r\nX-Slow: 1\r\n").await.expect("partial request");
    let start = Instant::now();
    let mut buf = [0u8; 256];
    let res = tokio::time::timeout(Duration::from_secs(8), s.read(&mut buf)).await;
    let elapsed = start.elapsed();
    assert!(res.is_ok(), "the server must close (or answer 408) within the header timeout; still open after {elapsed:?}");
    assert!(elapsed < Duration::from_secs(6), "closed after {elapsed:?}, header_timeout is 2 s");
    node.shutdown().await;
}
}

node_test! {
/// SEC-28 (connection cap): connections beyond `ingress.max_conns_per_ip` are refused; capacity
/// returns when they close.
async fn ac_sec_28_connections_per_ip_are_capped() {
    let node = start_node_with(|c| {
        c.ingress.max_conns_per_ip = 16;
        c.ingress.header_timeout = Duration::from_secs(20);
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("conns");
    ship(&node, &t.key, &slug, guests::HELLO).await;

    let mut held = Vec::new();
    for _ in 0..16 {
        held.push(tokio::net::TcpStream::connect(node.addrs.public).await.expect("connect"));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The 17th connection gets no service: refused / closed / reset.
    let extra = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/"), &[]), Duration::from_secs(3)).await;
    assert!(extra.is_none_or(|r| r.status != 200), "connections beyond the per-IP cap must be refused");

    drop(held);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ok = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/"), &[]), Duration::from_secs(5)).await.expect("response");
    assert_eq!(ok.status, 200, "capacity is released when connections close");
    node.shutdown().await;
}
}

/// A cleartext HTTP/2 client (prior knowledge) that opens and immediately resets streams.
fn h2_rapid_reset_payload(streams: u32) -> Vec<u8> {
    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let mut f = vec![(len >> 16) as u8, (len >> 8) as u8, len as u8, kind, flags];
        f.extend_from_slice(&stream.to_be_bytes());
        f.extend_from_slice(payload);
        f
    }
    let mut out = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    out.extend(frame(0x4, 0, 0, &[])); // SETTINGS
                                       // HPACK: :method GET, :scheme http, :path /, :authority localhost
    let mut block = vec![0x82, 0x86, 0x84, 0x41, 0x09];
    block.extend_from_slice(b"localhost");
    for i in 0..streams {
        let id = 2 * i + 1;
        out.extend(frame(0x1, 0x5, id, &block)); // HEADERS | END_HEADERS | END_STREAM
        out.extend(frame(0x3, 0, id, &8u32.to_be_bytes())); // RST_STREAM(CANCEL)
    }
    out
}

node_test! {
/// SEC-28 (rapid reset): an HTTP/2 client opening and resetting streams at a high rate has its
/// connection closed with `GOAWAY(ENHANCE_YOUR_CALM)`.
async fn ac_sec_28_http2_rapid_reset_is_answered_with_enhance_your_calm() {
    let node = start_node().await;
    let mut s = tokio::net::TcpStream::connect(node.addrs.public).await.expect("connect");
    let payload = h2_rapid_reset_payload(3_000);
    // The server may close while we are still writing; that is fine, keep reading afterwards.
    let _ = s.write_all(&payload).await;
    let mut data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(8), async {
        let mut chunk = [0u8; 16 * 1024];
        loop {
            match s.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => data.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await;

    // Walk the frames the server sent and look for GOAWAY (type 7) with error code 0xb.
    let mut pos = 0;
    let mut goaway_code = None;
    while pos + 9 <= data.len() {
        let len = ((data[pos] as usize) << 16) | ((data[pos + 1] as usize) << 8) | data[pos + 2] as usize;
        let kind = data[pos + 3];
        if pos + 9 + len > data.len() {
            break;
        }
        if kind == 0x7 && len >= 8 {
            let p = &data[pos + 9..pos + 9 + len];
            goaway_code = Some(u32::from_be_bytes([p[4], p[5], p[6], p[7]]));
        }
        pos += 9 + len;
    }
    assert_eq!(goaway_code, Some(0xb), "expected GOAWAY ENHANCE_YOUR_CALM (0xb); frames received: {} bytes", data.len());
    node.shutdown().await;
}
}

node_test! {
/// SEC-28 (SSE cap): at most 10 SSE streams per key; the 11th is `429`.
async fn ac_sec_28_eleventh_sse_stream_per_key_is_429() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = ship(&node, &t.key, &uniq("sse"), guests::LOGGER).await;
    let path = format!("/v1/services/{svc}/logs?follow=1");

    let mut streams = Vec::new();
    for i in 0..10 {
        let s = Sse::open(api(&node, &t.key, Method::GET, &path)).await;
        assert_eq!(s.status, 200, "stream {}", i + 1);
        streams.push(s);
    }
    let eleventh = send(api(&node, &t.key, Method::GET, &path).header("accept", "text/event-stream")).await;
    assert_eq!(eleventh.s(), 429, "{}", eleventh.text);
    assert_eq!(eleventh.code(), "rate_limited");

    // Closing one frees a slot; another key has its own budget.
    streams.pop();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let again = Sse::open(api(&node, &t.key, Method::GET, &path)).await;
    assert_eq!(again.status, 200, "a closed stream releases its slot");
    let (_, other_key) = make_key(&node, &t.id, &ALL_SCOPES, None).await;
    let other = Sse::open(api(&node, &other_key, Method::GET, &path)).await;
    assert_eq!(other.status, 200, "the cap is per key");
    drop(streams);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-25: bindings
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-25: binding `voicelab` (`allowed_paths=["/v1/calls*"]`, `allowed_methods=["POST"]`) is reached
/// only by operator SDK services (system tenant): a tenant reaches it through an SDK. Traversal
/// attempts and disallowed methods are `403 binding_denied` as a `Response` (contracts C-8); the
/// SDK's own `Authorization` never reaches the upstream, the injected one does, and the tenant
/// header is the *calling* tenant, whose plan `allowed_plans` checks.
async fn ac_sec_25_binding_denies_traversal_and_never_forwards_guest_credentials() {
    let upstream = start_upstream().await;
    let base = format!("http://{}", upstream.addr);
    let node = start_node_with(move |c| {
        c.binding.push(BindingConfig {
            name: "voicelab".to_string(),
            base_url: base,
            inject_headers: [("authorization".to_string(), "Bearer injected-by-runtime".to_string())].into_iter().collect(),
            tenant_header: "x-rt-tenant".to_string(),
            allowed_plans: vec!["pro".to_string()],
            allowed_methods: vec!["POST".to_string()],
            allowed_paths: vec!["/v1/calls*".to_string()],
            ..BindingConfig::default()
        });
    })
    .await;
    assert_eq!(publish_sdk(&node, "voicelab", "1.0.0", guests::PLATFORM_SDK, None).await.s(), 201);
    assert_eq!(put_grant(&node, json!({"scope": "node", "sdk": "voicelab", "effect": "allow"})).await.s(), 200);
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("platform");
    ship(&node, &t.key, &slug, guests::PLATFORM_SDK_CALLER).await;

    for (path, method) in [("/v1/../admin", "POST"), ("/v1/%2e%2e/admin", "POST"), ("/v1/calls", "GET"), ("/admin", "POST")] {
        let r = invoke(&node, &slug, &format!("/?path={}&method={method}", path.replace('/', "%2F"))).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let denied = r.json["status"] == json!(403) && r.json["reason"] == json!("binding_denied");
        assert!(denied, "{method} {path} must be 403 binding_denied (not thrown, not forwarded): {}", r.text);
    }
    assert!(upstream.requests().is_empty(), "denied calls never reach the upstream: {:?}", upstream.requests());

    let r = invoke(&node, &slug, "/?path=%2Fv1%2Fcalls&method=POST").await;
    assert_eq!(r.json["status"], json!(200), "an allowed call goes through: {}", r.text);
    let seen = upstream.requests();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/v1/calls");
    assert_eq!(seen[0].headers.get("authorization").map(String::as_str), Some("Bearer injected-by-runtime"), "the injected credential replaces the guest's");
    assert!(
        !seen[0].headers.values().any(|v| v.contains("guest-supplied-token")),
        "the guest's own Authorization never reaches the upstream: {:?}",
        seen[0].headers
    );
    assert_eq!(seen[0].headers.get("x-rt-tenant").map(String::as_str), Some(t.id.as_str()), "the calling tenant, set by the runtime");

    // A calling tenant whose plan is not in `allowed_plans` gets `binding_not_allowed`.
    let free = new_tenant(&node, "free").await;
    let slug2 = uniq("platform2");
    ship(&node, &free.key, &slug2, guests::PLATFORM_SDK_CALLER).await;
    let r = invoke(&node, &slug2, "/?path=%2Fv1%2Fcalls&method=POST").await;
    assert_eq!((r.json["status"].clone(), r.json["reason"].clone()), (json!(403), json!("binding_not_allowed")), "{}", r.text);

    // A tenant service never reaches a binding directly.
    let direct = uniq("platform-direct");
    ship(&node, &t.key, &direct, guests::PLATFORM_CALLER).await;
    let r = invoke(&node, &direct, "/?path=%2Fv1%2Fcalls&method=POST").await;
    assert_eq!((r.json["status"].clone(), r.json["reason"].clone()), (json!(403), json!("binding_not_allowed")), "{}", r.text);
    assert_eq!(upstream.requests().len(), 1, "nothing more reached the upstream");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-26: error sanitization
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-26: errors raised by SQL, blob and egress are stable codes: no data-dir path, no loopback
/// address, no driver text reaches the guest.
async fn ac_sec_26_guest_visible_errors_do_not_leak_internals() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("probe");
    ship(&node, &t.key, &slug, guests::ERROR_PROBE).await;

    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let data_dir = node.data_dir().display().to_string();
    let tmp_name = node.data_dir().file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    for canary in [data_dir.as_str(), tmp_name.as_str(), ".db", "rusqlite", "SQLITE_", "sqlite3_", "/services/", "meta.db", "queue.db"] {
        assert!(!canary.is_empty());
        assert!(!r.text.contains(canary), "guest-visible error text leaked `{canary}`: {}", r.text);
    }
    assert_ne!(r.json["sql"], json!("no error"), "a missing table is an error: {}", r.text);
    assert_ne!(r.json["attach"], json!("no error"), "ATTACH is not authorized: {}", r.text);
    assert!(r.json["egress"].as_str().is_some_and(|e| e.contains("egress_denied")), "loopback egress: {}", r.text);
    assert!(r.json["egress_meta"].as_str().is_some_and(|e| e.contains("egress_denied")), "metadata IP: {}", r.text);
    assert!(!r.text.contains("127.0.0.1"), "the denied address is not echoed back: {}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-29: host-side work is charged and isolated
// ---------------------------------------------------------------------------------------------

node_test! {
/// SEC-29: tenant A's runaway SQL statement is aborted at `storage.sql_budget_ms` with
/// `sql_timeout`; tenant B's KV reads keep their latency (p99 ≤ 2× baseline).
async fn ac_sec_29_a_cross_join_does_not_hurt_another_tenants_kv_latency() {
    let node = start_node_with(|c| c.storage.sql_budget_ms = 300).await;
    let a = new_tenant(&node, "pro").await;
    let b = new_tenant(&node, "pro").await;
    let hog = uniq("hog");
    let reader = uniq("reader");
    ship(&node, &a.key, &hog, guests::SQL_CROSS_JOIN).await;
    ship(&node, &b.key, &reader, guests::KV).await;
    let seed = invoke_with(&node, &reader, Method::PUT, "/k", &[], "value").await;
    assert_eq!(seed.s(), 200, "{}", seed.text);
    assert_eq!(invoke(&node, &reader, "/k").await.text, "value");

    let read_url = node.public_url(&format!("/s/{reader}/k"));
    let read_load = |dur: Duration| {
        let (client, url) = (node.http().clone(), read_url.clone());
        async move {
            let deadline = Instant::now() + dur;
            let workers: Vec<_> = (0..8)
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
    let baseline = read_load(Duration::from_secs(2)).await;

    let hog_url = node.public_url(&format!("/s/{hog}/"));
    let hogs: Vec<_> = (0..3).map(|_| tokio::spawn(fetch(node.http().clone(), hog_url.clone()))).collect();
    let during = read_load(Duration::from_secs(2)).await;
    for h in hogs {
        let (status, body, _, ms) = h.await.expect("hog");
        assert_eq!(status, 500, "the runaway statement is aborted: {body}");
        assert!(body.contains("sql_timeout"), "aborted with sql_timeout: {body}");
        assert!(ms < 5_000.0, "aborted near the 300 ms budget, took {ms} ms");
    }

    assert!(during.iter().all(|r| r.0 == 200), "B's reads keep succeeding");
    let p99_base = percentile(baseline.iter().map(|r| r.3).collect(), 99.0);
    let p99_during = percentile(during.iter().map(|r| r.3).collect(), 99.0);
    assert!(
        p99_during <= p99_base * 2.0 + 20.0,
        "tenant B's KV p99 rose from {p99_base:.2} ms to {p99_during:.2} ms while tenant A ran a cross join (> 2× + 20 ms slack)"
    );
    node.shutdown().await;
}
}
