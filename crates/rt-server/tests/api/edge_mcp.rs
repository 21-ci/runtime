//! Edge cases of the MCP endpoint (scratchpad edge-cases.md §MCP): notifications never run tools,
//! JSON-RPC ids round-trip, version headers, path segments in tool arguments, concurrent env
//! merges, the failed-auth limiter and audit.

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

const V: &str = "2025-11-25";

async fn mcp(node: &impl Node, key: &str, body: &Value, version: Option<&str>) -> Resp {
    let mut rb = node
        .http()
        .post(node.control_url("/mcp"))
        .header("accept", "application/json, text/event-stream")
        .bearer_auth(key)
        .json(body);
    if let Some(v) = version {
        rb = rb.header("mcp-protocol-version", v);
    }
    send(rb).await
}

async fn tool(node: &impl Node, key: &str, name: &str, args: Value) -> Value {
    let r = mcp(
        node,
        key,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}}),
        Some(V),
    )
    .await;
    assert_eq!(r.s(), 200, "{name}: {}", r.text);
    r.json["result"].clone()
}

node_test! {
/// MCP-005: a `tools/call` without an `id` is a notification: `202`, and the tool does not run.
async fn mcp_005_a_tools_call_notification_runs_nothing() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("m005"), "deno").await;
    let note = json!({"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "delete_service", "arguments": {"service": svc}}});
    let r = mcp(&node, &t.key, &note, Some(V)).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}")).await.s(), 200, "the service still exists");
    node.shutdown().await;
}
}

node_test! {
/// MCP-006: request ids of every JSON shape come back exactly as sent.
async fn mcp_006_ids_round_trip_exactly() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for id in [json!(-7), json!(1.5), json!(""), json!(u64::MAX), json!(i64::MIN), json!("abc")] {
        let r = mcp(&node, &t.key, &json!({"jsonrpc": "2.0", "id": id, "method": "tools/list"}), Some(V)).await;
        assert_eq!(r.s(), 200, "{id}: {}", r.text);
        assert_eq!(r.json["id"], id, "{}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// MCP-008 (docs/mcp.md): an unknown `MCP-Protocol-Version` is `400` on any request after
/// `initialize`; `initialize` itself ignores the header and negotiates in its body.
async fn mcp_008_unknown_version_headers() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let r = mcp(&node, &t.key, &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}), Some("1999-01-01")).await;
    assert_eq!(r.s(), 400, "{}", r.text);
    let init = json!({"jsonrpc": "2.0", "id": 2, "method": "initialize",
        "params": {"protocolVersion": V, "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}});
    let r = mcp(&node, &t.key, &init, Some("1999-01-01")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["result"]["protocolVersion"], json!(V));
    node.shutdown().await;
}
}

node_test! {
/// MCP-011: dot segments and slashes in tool arguments that become path segments never reach
/// another route: the call fails as not found / invalid and nothing else changes.
async fn mcp_011_dot_segments_in_arguments_stay_literal() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("m011"), "deno").await;
    for bad in ["..", ".", "%2e%2e", "a/b", "../../v1/tenants"] {
        let r = tool(&node, &t.key, "close_socket", json!({"service": svc, "object": bad, "socket": "ws_1"})).await;
        assert_eq!(r["isError"], json!(true), "object {bad}: {r}");
        let r = tool(&node, &t.key, "set_secret", json!({"service": svc, "name": bad, "value": "v"})).await;
        assert_eq!(r["isError"], json!(true), "secret {bad}: {r}");
        let r = tool(&node, &t.key, "revoke_token", json!({"key": bad})).await;
        assert_eq!(r["isError"], json!(true), "key {bad}: {r}");
    }
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}")).await.s(), 200);
    let secrets = get(&node, &t.key, &format!("/v1/services/{svc}/secrets")).await;
    assert_eq!(secrets.json, json!([]), "{}", secrets.text);
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200, "the caller's own key still works");
    node.shutdown().await;
}
}

node_test! {
/// MCP-014: `set_env` merges; two calls with disjoint `vars` at once both land.
async fn mcp_014_concurrent_set_env_calls_both_land() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("m014"), "deno").await;
    for round in 0..10 {
        let a = format!("A{round}");
        let b = format!("B{round}");
        let (ra, rb) = tokio::join!(
            tool(&node, &t.key, "set_env", json!({"service": svc, "vars": {a.clone(): "1"}})),
            tool(&node, &t.key, "set_env", json!({"service": svc, "vars": {b.clone(): "2"}})),
        );
        assert_eq!((ra["isError"].clone(), rb["isError"].clone()), (json!(false), json!(false)), "{ra} {rb}");
        let env = get(&node, &t.key, &format!("/v1/services/{svc}/env")).await;
        assert!(env.json[&a] == json!("1") && env.json[&b] == json!("2"), "round {round}: {}", env.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// MCP-019: wrong keys on `/mcp` count against the client's failed-auth budget like `/v1`.
async fn mcp_019_the_failed_auth_limiter_covers_mcp() {
    let node = start_node().await;
    let wrong = "rtk_01ARZ3NDEKTSV4RRFFQ69G5FAV_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut last = 0;
    for _ in 0..12 {
        last = mcp(&node, wrong, &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}), Some(V)).await.s();
    }
    assert_eq!(last, 429, "blocked after ten failures");
    node.shutdown().await;
}
}

node_test! {
/// MCP-020: tool calls are audited with the calling key as the actor.
async fn mcp_020_tool_calls_are_audited_with_the_key() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("m020"), "deno").await;
    let r = tool(&node, &t.key, "set_secret", json!({"service": svc, "name": "TOKEN", "value": "s3cret-value"})).await;
    assert_eq!(r["isError"], json!(false), "{r}");
    let events = call(root_for(&node, &t.id, Method::GET, &format!("/v1/services/{svc}/events")), None).await;
    assert_eq!(events.s(), 200, "{}", events.text);
    let secret = events.items().into_iter().find(|e| e["action"] == json!("secret.written") || e["kind"] == json!("secret.written"));
    let secret = secret.unwrap_or_else(|| panic!("secret.written audited: {}", events.text));
    assert!(secret.to_string().contains(&t.key_id), "the key is the actor: {secret}");
    assert!(!events.text.contains("s3cret-value"), "the value is never audited");
    node.shutdown().await;
}
}
