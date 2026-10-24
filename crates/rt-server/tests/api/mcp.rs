//! Decision D-B5 end to end: the MCP server on the control listener (`POST /mcp`, Streamable
//! HTTP). API-25 the handshake, `tools/list` and `tools/call` of the 2025-11-25 era, API-26 code
//! deploys and the rest of the tool set through a real node, API-27 transport rules (auth, Origin,
//! methods, versions, the stateless 2026-07-28 era), SEC-61 scopes and tenancy through tools.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use reqwest::Method;
use rt_store::{DelayedStore, NewDelayed, SqliteQueueStore};
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

static IDS: AtomicU64 = AtomicU64::new(1);

const ACCEPT: &str = "application/json, text/event-stream";

/// A raw JSON-RPC POST to `/mcp`.
async fn post_mcp(node: &impl Node, key: Option<&str>, body: &Value, headers: &[(&str, &str)]) -> Resp {
    let mut rb = node
        .http()
        .post(node.control_url("/mcp"))
        .header("accept", ACCEPT)
        .json(body);
    if let Some(k) = key {
        rb = rb.bearer_auth(k);
    }
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    send(rb).await
}

fn request(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": IDS.fetch_add(1, Ordering::Relaxed), "method": method, "params": params})
}

/// A client of the legacy (`initialize`) era, bound to one key.
struct Mcp<'a, N: Node> {
    node: &'a N,
    key: String,
}

impl<'a, N: Node> Mcp<'a, N> {
    async fn start(node: &'a N, key: &str) -> Self {
        let c = Mcp {
            node,
            key: key.to_string(),
        };
        let init = request(
            "initialize",
            json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}),
        );
        let r = post_mcp(node, Some(key), &init, &[]).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let r = post_mcp(
            node,
            Some(key),
            &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            &[("mcp-protocol-version", "2025-11-25")],
        )
        .await;
        assert_eq!(r.s(), 202, "{}", r.text);
        c
    }

    async fn rpc(&self, method: &str, params: Value) -> Resp {
        post_mcp(
            self.node,
            Some(&self.key),
            &request(method, params),
            &[("mcp-protocol-version", "2025-11-25")],
        )
        .await
    }

    /// `result` of `tools/call` (any outcome).
    async fn call(&self, name: &str, args: Value) -> Value {
        let r = self.rpc("tools/call", json!({"name": name, "arguments": args})).await;
        assert_eq!(r.s(), 200, "{name}: {}", r.text);
        assert!(r.json["error"].is_null(), "{name} is a protocol error: {}", r.text);
        r.json["result"].clone()
    }

    /// `structuredContent` of a call that must succeed; its text is the same JSON.
    async fn ok(&self, name: &str, args: Value) -> Value {
        let r = self.call(name, args.clone()).await;
        assert_eq!(r["isError"], json!(false), "{name} {args}: {r}");
        let text = r["content"][0]["text"].as_str().expect("text content");
        assert_eq!(
            serde_json::from_str::<Value>(text).expect("text is JSON"),
            r["structuredContent"],
            "{name}: text and structuredContent agree"
        );
        r["structuredContent"].clone()
    }

    /// `(code, message)` of a call that must fail as a tool error.
    async fn err(&self, name: &str, args: Value) -> (String, String) {
        let r = self.call(name, args.clone()).await;
        assert_eq!(r["isError"], json!(true), "{name} {args} should fail: {r}");
        let e = &r["structuredContent"]["error"];
        let (code, msg) = (
            e["code"].as_str().unwrap_or("").to_string(),
            e["message"].as_str().unwrap_or("").to_string(),
        );
        let text = r["content"][0]["text"].as_str().expect("text content");
        assert!(
            text.starts_with(&format!("{code}: ")),
            "the text starts with the code: {text}"
        );
        (code, msg)
    }

    async fn tool_names(&self) -> Vec<String> {
        let r = self.rpc("tools/list", json!({})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        r.json["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .map(|t| t["name"].as_str().expect("name").to_string())
            .collect()
    }
}

const ALL_TOOLS: [&str; 26] = [
    "whoami",
    "get_limits",
    "list_projects",
    "create_project",
    "list_environments",
    "list_sdks",
    "get_sdk_contract",
    "list_services",
    "get_service",
    "create_service",
    "update_service",
    "delete_service",
    "deploy_code",
    "list_versions",
    "list_schedules",
    "list_objects",
    "list_sockets",
    "close_socket",
    "rollback",
    "get_logs",
    "get_stats",
    "set_env",
    "set_secret",
    "list_tokens",
    "create_token",
    "revoke_token",
];

// ---------------------------------------------------------------------------------------------
// API-25: handshake and the basics
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_25_a_2025_11_25_client_initializes_lists_tools_and_calls_one() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy", "logs:read", "env:write", "secrets:write", "keys:write", "sockets:write"]).await;

    // initialize: the requested version is echoed, tools are the only capability, no session.
    let init = request("initialize", json!({"protocolVersion": "2025-11-25", "capabilities": {"roots": {}}, "clientInfo": {"name": "claude-code", "version": "2.1.0"}}));
    let r = post_mcp(&node, Some(&t.key), &init, &[]).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.header("content-type").as_deref(), Some("application/json"));
    assert!(r.header("mcp-session-id").is_none(), "stateless: no session id");
    assert_eq!(r.json["jsonrpc"], json!("2.0"));
    assert_eq!(r.json["id"], init["id"]);
    let result = &r.json["result"];
    assert_eq!(result["protocolVersion"], json!("2025-11-25"));
    assert_eq!(result["capabilities"], json!({"tools": {"listChanged": false}}));
    assert_eq!(result["serverInfo"]["name"], json!("21cloud-runtime"));
    assert!(result["serverInfo"]["version"].as_str().is_some_and(|v| !v.is_empty()));
    assert!(result["instructions"].as_str().is_some_and(|i| i.contains("deploy_code")));
    assert!(result.get("resultType").is_none(), "that is the stateless era");

    // Other versions: an older one we speak is echoed, an unknown one gets our newest.
    for (asked, answered) in [("2024-11-05", "2024-11-05"), ("2025-06-18", "2025-06-18"), ("2099-01-01", "2025-11-25"), ("", "2025-11-25")] {
        let r = post_mcp(&node, Some(&t.key), &request("initialize", json!({"protocolVersion": asked, "capabilities": {}, "clientInfo": {"name": "x", "version": "1"}})), &[]).await;
        assert_eq!(r.json["result"]["protocolVersion"], json!(answered), "asked {asked:?}: {}", r.text);
    }

    // initialized: a notification is accepted with an empty body.
    let r = post_mcp(&node, Some(&t.key), &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}), &[("mcp-protocol-version", "2025-11-25")]).await;
    assert_eq!((r.s(), r.text.as_str()), (202, ""));
    let r = post_mcp(&node, Some(&t.key), &request("ping", json!({})), &[("mcp-protocol-version", "2025-11-25")]).await;
    assert_eq!(r.json["result"], json!({}));

    // tools/list: every tool the key's scopes allow, with a schema and annotations.
    let mcp = Mcp::start(&node, &t.key).await;
    let r = mcp.rpc("tools/list", json!({})).await;
    let tools = r.json["result"]["tools"].as_array().expect("tools").clone();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ALL_TOOLS, "all of them, in a fixed order");
    for tool in &tools {
        let name = tool["name"].as_str().unwrap();
        assert!(tool["description"].as_str().is_some_and(|d| d.len() > 20), "{name} is described");
        assert_eq!(tool["inputSchema"]["type"], json!("object"), "{name}");
        assert!(tool["annotations"]["readOnlyHint"].is_boolean(), "{name}: {}", tool["annotations"]);
        assert_eq!(tool["annotations"]["openWorldHint"], json!(false));
    }
    let ann = |n: &str| tools.iter().find(|t| t["name"] == n).expect("tool")["annotations"].clone();
    assert_eq!(ann("list_services")["readOnlyHint"], json!(true));
    assert_eq!(ann("delete_service")["destructiveHint"], json!(true));
    assert_eq!(ann("revoke_token")["destructiveHint"], json!(true));
    assert_eq!(ann("create_service")["destructiveHint"], json!(false));
    assert_eq!(
        (ann("deploy_code")["idempotentHint"].clone(), ann("deploy_code")["destructiveHint"].clone()),
        (json!(false), json!(false)),
        "every deploy adds a version"
    );
    assert_eq!(ann("rollback")["idempotentHint"], json!(true));

    // tools/call of a read tool.
    let me = mcp.ok("whoami", json!({})).await;
    assert_eq!((me["principal"].clone(), me["tenant_id"].clone(), me["key_id"].clone()), (json!("tenant"), json!(t.id), json!(t.key_id)));
    assert!(me["scopes"].as_array().unwrap().contains(&json!("deploy")));
    let projects = mcp.ok("list_projects", json!({})).await;
    assert_eq!(projects["items"][0]["slug"], json!("default"));
    let limits = mcp.ok("get_limits", json!({})).await;
    assert_eq!(limits["services"]["used"], json!(0));
    assert!(limits["invocations"]["remaining"].as_u64().is_some());
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-26: deploys and the tool set
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_26_deploy_code_creates_a_live_version_that_serves_and_rolls_back() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy", "logs:read"]).await;
    let mcp = Mcp::start(&node, &t.key).await;
    let slug = uniq("agent");

    let svc = mcp.ok("create_service", json!({"slug": slug, "runtime": "deno", "name": "Agent made"})).await;
    let id = svc["id"].as_str().expect("id").to_string();
    assert_eq!((svc["project"].clone(), svc["environment"].clone(), svc["live_version"].clone()), (json!("default"), json!("production"), Value::Null));

    // One source file, by slug: version 1 is live and serves.
    let v1 = mcp.ok("deploy_code", json!({"service": slug, "source": guests::versioned(1)})).await;
    assert_eq!((v1["version"].clone(), v1["activated"].clone(), v1["service"].clone()), (json!(1), json!(true), json!(id)));
    assert!(v1["source_digest"].as_str().is_some_and(|d| d.starts_with("sha256:")), "{v1}");
    let r = invoke(&node, &slug, "/").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "v1"));

    // Several files: the second version imports a sibling.
    let v2 = mcp
        .ok("deploy_code", json!({"service": id, "files": {
            "main.ts": "import { word } from './word.ts';\nexport default function (): Response { return new Response('v2 ' + word); }",
            "word.ts": "export const word = 'multi';"}}))
        .await;
    assert_eq!(v2["version"], json!(2));
    let r = invoke(&node, &slug, "/").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "v2 multi"));

    // Not activated: version 3 exists, version 2 stays live.
    let v3 = mcp.ok("deploy_code", json!({"service": slug, "source": guests::versioned(3), "activate": false})).await;
    assert_eq!((v3["version"].clone(), v3["activated"].clone()), (json!(3), json!(false)));
    assert_eq!(invoke(&node, &slug, "/").await.text, "v2 multi");

    // list_versions shows all three and the live one.
    let versions = mcp.ok("list_versions", json!({"service": slug})).await;
    assert_eq!(versions["live_version"], json!(2));
    let numbers: Vec<_> = versions["items"].as_array().unwrap().iter().map(|v| v["version"].clone()).collect();
    assert_eq!(numbers, vec![json!(1), json!(2), json!(3)]);

    // rollback without a version: the newest older than live.
    let back = mcp.ok("rollback", json!({"service": slug})).await;
    assert_eq!((back["from"].clone(), back["to"].clone()), (json!(2), json!(1)));
    let (n, sl) = (&node, slug.as_str());
    eventually(std::time::Duration::from_secs(5), "version 1 serves again", || async move { invoke(n, sl, "/").await.text == "v1" }).await;
    // rollback to an explicit version; there is nothing older than 1.
    let fwd = mcp.ok("rollback", json!({"service": slug, "version": 3})).await;
    assert_eq!((fwd["to"].clone(), fwd["live_version"].clone()), (json!(3), json!(3)));
    let (code, _) = mcp.err("rollback", json!({"service": slug, "version": 99})).await;
    assert_eq!(code, "not_found");

    // A program that does not parse is a tool error with the diagnostics.
    let r = mcp.call("deploy_code", json!({"service": slug, "source": "export default function ( {"})).await;
    assert_eq!(r["isError"], json!(true), "{r}");
    assert_eq!(r["structuredContent"]["error"]["code"], json!("validation_failed"));
    assert!(r["structuredContent"]["error"]["details"]["diagnostics"].as_array().is_some_and(|d| !d.is_empty()), "{r}");
    assert_eq!(invoke(&node, &slug, "/").await.text, "v3", "a refused deploy changes nothing");

    // Bad arguments are tool errors too.
    for args in [
        json!({"service": slug}),
        json!({"service": slug, "source": "1", "files": {"a.ts": "1"}}),
        json!({"service": slug, "files": {"../x.ts": "1"}}),
        json!({"service": slug, "source": 7}),
        json!({"service": slug, "source": "export default () => new Response('x')", "compile": "sometime"}),
        json!({"service": slug, "source": "x", "surprise": true}),
        json!({"service": "no-such-service", "source": "x"}),
    ] {
        let r = mcp.call("deploy_code", args.clone()).await;
        assert_eq!(r["isError"], json!(true), "{args}: {r}");
    }

    // Logs, stats, then delete.
    invoke(&node, &slug, "/").await;
    let stats = mcp.ok("get_stats", json!({"service": slug, "window": "5m"})).await;
    assert!(stats.get("admission").is_some(), "{stats}");
    let logs = mcp.ok("get_logs", json!({"service": slug, "limit": 5})).await;
    assert!(logs["items"].is_array() && logs.get("next_cursor").is_some(), "{logs}");
    let (code, _) = mcp.err("get_logs", json!({"service": slug, "level": "loud"})).await;
    assert_eq!(code, "validation_failed");
    let gone = mcp.ok("delete_service", json!({"service": slug})).await;
    assert_eq!(gone["deleted"], json!(id));
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{id}")).await.s(), 404);
    let (code, _) = mcp.err("get_service", json!({"service": slug})).await;
    assert_eq!(code, "not_found");
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_26_services_projects_env_secrets_and_tokens_through_tools() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy", "env:write", "secrets:write", "keys:write", "logs:read"]).await;
    let mcp = Mcp::start(&node, &t.key).await;

    // Projects and environments.
    let web = mcp.ok("create_project", json!({"slug": "web", "environments": ["production", "staging"]})).await;
    assert_eq!(web["environments"], json!(["production", "staging"]));
    let (code, _) = mcp.err("create_project", json!({"slug": "web"})).await;
    assert_eq!(code, "project_slug_taken");
    let envs = mcp.ok("list_environments", json!({"project": "web"})).await;
    assert_eq!(envs, json!({"project": "web", "environments": ["production", "staging"]}));
    let (code, _) = mcp.err("list_environments", json!({"project": "nope"})).await;
    assert_eq!(code, "not_found");

    // Services in a project; the list is compact and filterable; get_service is whole.
    let a = uniq("front");
    let b = uniq("back");
    mcp.ok("create_service", json!({"slug": a, "runtime": "deno", "project": "web", "environment": "staging", "access": "key", "triggers": {"cron": false}})).await;
    mcp.ok("create_service", json!({"slug": b, "runtime": "deno"})).await;
    let (code, msg) = mcp.err("create_service", json!({"slug": uniq("x"), "runtime": "deno", "project": "web", "environment": "dev"})).await;
    assert_eq!(code, "validation_failed");
    assert!(msg.contains("dev"), "{msg}");
    let list = mcp.ok("list_services", json!({"project": "web"})).await;
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!((items[0]["slug"].clone(), items[0]["project"].clone(), items[0]["environment"].clone()), (json!(a), json!("web"), json!("staging")));
    assert!(items[0].get("effective").is_none(), "the list is compact: {}", items[0]);
    let all = mcp.ok("list_services", json!({})).await;
    assert_eq!(all["items"].as_array().unwrap().len(), 2);
    let staged = mcp.ok("list_services", json!({"environment": "staging"})).await;
    assert_eq!(staged["items"].as_array().unwrap().len(), 1);
    let full = mcp.ok("get_service", json!({"service": a})).await;
    assert_eq!((full["access"].clone(), full["triggers"]["cron"].clone()), (json!("key"), json!(false)));
    assert!(full["effective"].is_object() && full["etag"].is_string());

    // update_service: config blocks replace, etag guards.
    let etag = full["etag"].as_str().unwrap().to_string();
    let upd = mcp.ok("update_service", json!({"service": a, "etag": etag, "name": "Front", "limits": {"cpu_ms": 200}})).await;
    assert_eq!((upd["name"].clone(), upd["limits"]["cpu_ms"].clone()), (json!("Front"), json!(200)));
    let (code, _) = mcp.err("update_service", json!({"service": a, "etag": etag, "name": "Stale"})).await;
    assert_eq!(code, "precondition_failed", "the etag is out of date now");
    let (code, _) = mcp.err("update_service", json!({"service": a})).await;
    assert_eq!(code, "validation_failed", "nothing to change");
    let (code, _) = mcp.err("update_service", json!({"service": a, "limits": {"cpu_ms": 999_999_999u64}})).await;
    assert_eq!(code, "limit_exceeds_plan");

    // set_env merges, set_secret never echoes, a guest sees both.
    let src = r#"export default function (): Response {
        return new Response((Deno.env.get("GREETING") ?? "-") + "|" + (Deno.env.get("OTHER") ?? "-") + "|" + (Deno.env.get("TOKEN") ?? "-").length);
    }"#;
    mcp.ok("deploy_code", json!({"service": b, "source": src})).await;
    let env = mcp.ok("set_env", json!({"service": b, "vars": {"GREETING": "hello", "OTHER": "x"}})).await;
    assert_eq!(env["names"], json!(["GREETING", "OTHER"]));
    let env = mcp.ok("set_env", json!({"service": b, "vars": {"GREETING": "hi"}, "unset": ["OTHER"]})).await;
    assert_eq!((env["names"].clone(), env["set"].clone(), env["unset"].clone()), (json!(["GREETING"]), json!(["GREETING"]), json!(["OTHER"])));
    let (code, _) = mcp.err("set_env", json!({"service": b, "vars": {"lower": "x"}})).await;
    assert_eq!(code, "validation_failed");
    let (code, _) = mcp.err("set_env", json!({"service": b, "vars": {"N": 5}})).await;
    assert_eq!(code, "validation_failed");
    let sec = mcp.call("set_secret", json!({"service": b, "name": "TOKEN", "value": "s3cr3t-canary"})).await;
    assert_eq!(sec["isError"], json!(false));
    assert!(!sec.to_string().contains("s3cr3t-canary"), "the secret value is never echoed: {sec}");
    assert_eq!(sec["structuredContent"], json!({"service": get(&node, &t.key, &format!("/v1/services?slug={b}")).await.items()[0]["id"], "name": "TOKEN", "written": true}));
    let (code, _) = mcp.err("set_secret", json!({"service": b, "name": "RT_NOPE", "value": "x"})).await;
    assert_eq!(code, "validation_failed");
    let (n, bs) = (&node, b.as_str());
    eventually(std::time::Duration::from_secs(5), "the guest sees the env and the secret", || async move { invoke(n, bs, "/").await.text == "hi|-|13" }).await;

    // Tokens: create within the key's own scopes, use, list, revoke.
    let tok = mcp.ok("create_token", json!({"scopes": ["services:read"], "name": "reader"})).await;
    let (tok_id, secret) = (tok["id"].as_str().unwrap().to_string(), tok["secret"].as_str().expect("the secret is returned once").to_string());
    assert_eq!(get(&node, &secret, "/v1/services").await.s(), 200);
    let (code, _) = mcp.err("create_token", json!({"scopes": ["data:admin"]})).await;
    assert_eq!(code, "insufficient_scope", "more than the creating key holds");
    let listed = mcp.ok("list_tokens", json!({})).await;
    assert!(listed["items"].as_array().unwrap().iter().any(|k| k["id"] == json!(tok_id) && k.get("secret").is_none()), "{listed}");
    let revoked = mcp.ok("revoke_token", json!({"key_id": tok_id})).await;
    assert_eq!(revoked["revoked"], json!(tok_id));
    assert_eq!(get(&node, &secret, "/v1/services").await.s(), 401);
    let (code, _) = mcp.err("revoke_token", json!({"key_id": tok_id})).await;
    assert_eq!(code, "not_found");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// API-27: transport rules
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_api_27_the_endpoint_wants_a_tenant_key_and_a_trusted_origin() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let init = request("initialize", json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "x", "version": "1"}}));

    // No key / a wrong key / a revoked key: 401 with the Bearer challenge and nothing else in it.
    let (revoked_id, revoked) = make_key(&node, &t.id, &["services:read"], None).await;
    assert_eq!(call(root(&node, Method::DELETE, &format!("/v1/tenants/{}/keys/{revoked_id}", t.id)), None).await.s(), 204);
    for key in [None, Some("rtk_not-a-key"), Some("nonsense"), Some(revoked.as_str())] {
        let r = post_mcp(&node, key, &init, &[]).await;
        assert_eq!(r.s(), 401, "{key:?}: {}", r.text);
        assert_eq!(r.header("www-authenticate").as_deref(), Some("Bearer error=\"invalid_token\""));
        assert!(!r.header("www-authenticate").unwrap().contains("resource_metadata"), "there is no OAuth to start");
    }
    // x-rt-key works as on the rest of the control API; the well-known OAuth documents do not exist.
    let r = send(node.http().post(node.control_url("/mcp")).header("x-rt-key", &t.key).header("accept", ACCEPT).json(&init)).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    for path in ["/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-authorization-server"] {
        assert_eq!(send(node.http().get(node.control_url(path))).await.s(), 404, "{path}");
    }

    // The operator token manages nodes, not a tenant: refused, and told so.
    let r = post_mcp(&node, Some(node.root_token()), &init, &[]).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    assert!(r.header("www-authenticate").is_some_and(|h| h.contains("insufficient_scope")));

    // Origin: a browser page of another site is refused, a loopback one and a native client pass.
    for origin in ["https://evil.example", "http://localhost.evil.example", "null", "http://10.0.0.1:3000"] {
        let r = post_mcp(&node, Some(&t.key), &init, &[("origin", origin)]).await;
        assert_eq!(r.s(), 403, "{origin}: {}", r.text);
        assert_eq!(r.json["error"]["code"], json!(-32000));
        assert!(r.json["id"].is_null());
    }
    let r = post_mcp(&node, None, &init, &[("origin", "https://evil.example")]).await;
    assert_eq!(r.s(), 403, "the Origin is checked before the key: {}", r.text);
    for origin in ["http://localhost:6274", "http://127.0.0.1:8080", "http://[::1]:3000"] {
        let r = post_mcp(&node, Some(&t.key), &init, &[("origin", origin)]).await;
        assert_eq!(r.s(), 200, "{origin}: {}", r.text);
    }
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_27_only_post_is_served_one_json_message_at_a_time() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // No server-initiated stream and no sessions: GET and DELETE are 405 with Allow: POST.
    for m in [Method::GET, Method::DELETE, Method::PUT] {
        let r = send(node.http().request(m.clone(), node.control_url("/mcp")).bearer_auth(&t.key)).await;
        assert_eq!(r.s(), 405, "{m}");
        assert_eq!(r.header("allow").as_deref(), Some("POST"), "{m}");
    }

    // A body that is not JSON, not an object, or not JSON-RPC.
    let raw = |body: &'static str| {
        let rb = node.http().post(node.control_url("/mcp")).bearer_auth(&t.key).header("content-type", "application/json").body(body);
        async move { send(rb).await }
    };
    let r = raw("{nope").await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (400, json!(-32700)), "{}", r.text);
    let r = raw("[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}]").await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (400, json!(-32600)), "batching is gone since 2025-06-18: {}", r.text);
    let r = raw("{\"jsonrpc\":\"2.0\",\"id\":1}").await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (400, json!(-32600)), "{}", r.text);
    let r = raw("{\"jsonrpc\":\"1.0\",\"id\":1,\"method\":\"ping\"}").await;
    assert_eq!(r.s(), 400, "{}", r.text);
    let r = raw("{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"ping\"}").await;
    assert_eq!(r.s(), 400, "{}", r.text);
    // A response from the client (we send no requests) and a notification are accepted, empty.
    let r = raw("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}").await;
    assert_eq!((r.s(), r.text.as_str()), (202, ""));
    let r = raw("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}").await;
    assert_eq!((r.s(), r.text.as_str()), (202, ""));

    // A body over the limit.
    let big = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "deploy_code", "arguments": {"service": "x", "source": "x".repeat(4 * 1024 * 1024 + 1024)}}});
    let r = post_mcp(&node, Some(&t.key), &big, &[]).await;
    assert_eq!(r.s(), 413, "{}", &r.text[..r.text.len().min(200)]);

    // Unknown method and unknown tool are JSON-RPC errors, not tool results.
    let r = post_mcp(&node, Some(&t.key), &request("resources/list", json!({})), &[]).await;
    assert_eq!(r.json["error"]["code"], json!(-32601), "{}", r.text);
    let r = post_mcp(&node, Some(&t.key), &request("tools/call", json!({"name": "deploy_everything", "arguments": {}})), &[]).await;
    assert_eq!(r.json["error"]["code"], json!(-32602), "{}", r.text);
    assert!(r.json["error"]["message"].as_str().is_some_and(|m| m.contains("deploy_everything")));
    let r = post_mcp(&node, Some(&t.key), &request("tools/call", json!({"name": "whoami", "arguments": [1]})), &[]).await;
    assert_eq!(r.json["error"]["code"], json!(-32602), "{}", r.text);
    let r = post_mcp(&node, Some(&t.key), &request("tools/call", json!({})), &[]).await;
    assert_eq!(r.json["error"]["code"], json!(-32602), "{}", r.text);

    // MCP-Protocol-Version after the handshake: a version we speak passes, anything else is 400.
    for v in ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"] {
        let r = post_mcp(&node, Some(&t.key), &request("ping", json!({})), &[("mcp-protocol-version", v)]).await;
        assert_eq!(r.s(), 200, "{v}: {}", r.text);
    }
    let r = post_mcp(&node, Some(&t.key), &request("ping", json!({})), &[("mcp-protocol-version", "1999-01-01")]).await;
    assert_eq!(r.s(), 400, "{}", r.text);
    assert_eq!(r.json["error"]["data"]["requested"], json!("1999-01-01"));
    assert!(r.json["error"]["data"]["supported"].as_array().unwrap().contains(&json!("2025-11-25")));
    let r = post_mcp(&node, Some(&t.key), &request("ping", json!({})), &[]).await;
    assert_eq!(r.s(), 200, "no header: the client predates the header, 2025-03-26 is assumed: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_27_the_2026_07_28_era_needs_no_handshake_but_mirrors_its_headers() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let meta = json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": {}});
    let modern = |method: &str, mut params: Value| {
        params["_meta"] = meta.clone();
        request(method, params)
    };
    let hdr = |method: &'static str| vec![("mcp-protocol-version", "2026-07-28"), ("mcp-method", method)];

    // server/discover: versions, capabilities, identity, no handshake before it.
    let r = post_mcp(&node, Some(&t.key), &modern("server/discover", json!({})), &hdr("server/discover")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let d = &r.json["result"];
    assert_eq!(d["resultType"], json!("complete"));
    assert_eq!(d["supportedVersions"][0], json!("2026-07-28"));
    assert!(d["supportedVersions"].as_array().unwrap().contains(&json!("2025-11-25")));
    assert_eq!(d["capabilities"]["tools"], json!({}));
    assert_eq!(d["_meta"]["io.modelcontextprotocol/serverInfo"]["name"], json!("21cloud-runtime"));

    // tools/list and tools/call carry resultType and need Mcp-Method (and Mcp-Name for a call).
    let r = post_mcp(&node, Some(&t.key), &modern("tools/list", json!({})), &hdr("tools/list")).await;
    assert_eq!((r.s(), r.json["result"]["resultType"].clone()), (200, json!("complete")), "{}", r.text);
    assert!(r.json["result"]["tools"].as_array().is_some_and(|t| t.iter().any(|x| x["name"] == "whoami")));
    let call = modern("tools/call", json!({"name": "whoami", "arguments": {}}));
    let mut h = hdr("tools/call");
    h.push(("mcp-name", "whoami"));
    let r = post_mcp(&node, Some(&t.key), &call, &h).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!((r.json["result"]["resultType"].clone(), r.json["result"]["isError"].clone()), (json!("complete"), json!(false)));
    assert_eq!(r.json["result"]["structuredContent"]["tenant_id"], json!(t.id));

    // Header mismatches: 400 with HeaderMismatch (-32020).
    for (what, headers) in [
        ("no Mcp-Name", hdr("tools/call")),
        ("a different Mcp-Name", { let mut h = hdr("tools/call"); h.push(("mcp-name", "list_services")); h }),
        ("a different Mcp-Method", { let mut h = vec![("mcp-protocol-version", "2026-07-28"), ("mcp-method", "tools/list")]; h.push(("mcp-name", "whoami")); h }),
        ("no version header", vec![("mcp-method", "tools/call"), ("mcp-name", "whoami")]),
        ("another version header", vec![("mcp-protocol-version", "2025-11-25"), ("mcp-method", "tools/call"), ("mcp-name", "whoami")]),
    ] {
        let r = post_mcp(&node, Some(&t.key), &call, &headers).await;
        assert_eq!(r.s(), 400, "{what}: {}", r.text);
        assert_eq!(r.json["error"]["code"], json!(-32020), "{what}: {}", r.text);
    }

    // A version we do not speak: -32022 with the list we do.
    let mut old = request("tools/list", json!({"_meta": {"io.modelcontextprotocol/protocolVersion": "2031-01-01"}}));
    old["id"] = json!("u1");
    let r = post_mcp(&node, Some(&t.key), &old, &[("mcp-protocol-version", "2031-01-01"), ("mcp-method", "tools/list")]).await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (400, json!(-32022)), "{}", r.text);
    assert_eq!(r.json["id"], json!("u1"));
    assert_eq!(r.json["error"]["data"]["requested"], json!("2031-01-01"));
    assert_eq!(r.json["error"]["data"]["supported"][0], json!("2026-07-28"));

    // A modern header without the modern `_meta`, and an unknown method in the modern era.
    let r = post_mcp(&node, Some(&t.key), &request("tools/list", json!({})), &[("mcp-protocol-version", "2026-07-28"), ("mcp-method", "tools/list")]).await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (400, json!(-32020)), "{}", r.text);
    let r = post_mcp(&node, Some(&t.key), &modern("resources/list", json!({})), &hdr("resources/list")).await;
    assert_eq!((r.s(), r.json["error"]["code"].clone()), (404, json!(-32601)), "{}", r.text);
    // ping is gone in this era.
    let r = post_mcp(&node, Some(&t.key), &modern("ping", json!({})), &hdr("ping")).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// SEC-61: scopes, restrictions and tenancy through tools
// ---------------------------------------------------------------------------------------------

node_test! {
async fn ac_sec_61_tools_are_limited_by_the_scopes_of_the_key() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("target"), "deno").await;

    // A read-only key sees only the tools it can use.
    let (_, reader) = make_key(&node, &t.id, &["services:read"], None).await;
    let mcp = Mcp::start(&node, &reader).await;
    let names = mcp.tool_names().await;
    for allowed in ["whoami", "get_limits", "list_projects", "list_environments", "list_sdks", "get_sdk_contract", "list_services", "get_service", "list_versions", "list_schedules"] {
        assert!(names.iter().any(|n| n == allowed), "{allowed} in {names:?}");
    }
    for hidden in ["create_service", "update_service", "delete_service", "create_project", "deploy_code", "rollback", "get_logs", "get_stats", "set_env", "set_secret", "list_tokens", "create_token", "revoke_token"] {
        assert!(!names.iter().any(|n| n == hidden), "{hidden} must not be listed for services:read: {names:?}");
    }

    // Calling one anyway is refused as a tool error naming the missing scope; nothing changes.
    for (tool, args, scope) in [
        ("create_service", json!({"slug": uniq("nope"), "runtime": "deno"}), "services:write"),
        ("delete_service", json!({"service": svc}), "services:write"),
        ("deploy_code", json!({"service": svc, "source": "export default () => new Response('x')"}), "deploy"),
        ("get_logs", json!({"service": svc}), "logs:read"),
        ("set_secret", json!({"service": svc, "name": "A", "value": "b"}), "secrets:write"),
        ("create_token", json!({"scopes": ["services:read"]}), "keys:write"),
        ("list_tokens", json!({}), "keys:write"),
        ("create_project", json!({"slug": "nope"}), "services:write"),
    ] {
        let r = mcp.call(tool, args).await;
        assert_eq!(r["isError"], json!(true), "{tool}: {r}");
        assert_eq!(r["structuredContent"]["error"]["code"], json!("insufficient_scope"), "{tool}");
        assert_eq!(r["structuredContent"]["error"]["details"]["missing_scopes"], json!([scope]), "{tool}");
    }
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{svc}")).await.s(), 200, "the service survived");

    // set_env needs both env:write and services:read (it merges into the current set).
    let (_, writer) = make_key(&node, &t.id, &["env:write"], None).await;
    let names = Mcp::start(&node, &writer).await.tool_names().await;
    assert_eq!(names, ["whoami", "get_limits"], "env:write alone offers set_env nothing to merge into: {names:?}");
    let (_, both) = make_key(&node, &t.id, &["env:write", "services:read"], None).await;
    assert!(Mcp::start(&node, &both).await.tool_names().await.iter().any(|n| n == "set_env"));

    // keys:write unlocks the token tools and nothing else.
    let (_, keys_only) = make_key(&node, &t.id, &["keys:write"], None).await;
    let names = Mcp::start(&node, &keys_only).await.tool_names().await;
    assert_eq!(names, ["whoami", "get_limits", "list_tokens", "create_token", "revoke_token"]);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_61_tools_see_only_the_keys_tenant_and_the_keys_services() {
    let node = start_node().await;
    let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
    let (a_slug, b_slug) = (uniq("alpha"), uniq("beta"));
    create_service(&node, &a.key, &a_slug, "deno").await;
    create_service(&node, &b.key, &b_slug, "deno").await;
    let mcp = Mcp::start(&node, &a.key).await;

    // Another tenant's service does not exist for this key, by slug or by id.
    let (code, _) = mcp.err("get_service", json!({"service": b_slug})).await;
    assert_eq!(code, "not_found");
    let b_id = get(&node, &b.key, &format!("/v1/services?slug={b_slug}")).await.items()[0]["id"].as_str().unwrap().to_string();
    for tool in ["get_service", "delete_service", "list_versions"] {
        let (code, _) = mcp.err(tool, json!({"service": b_id})).await;
        assert_eq!(code, "not_found", "{tool}");
    }
    let (code, _) = mcp.err("deploy_code", json!({"service": b_id, "source": "export default () => new Response('x')"})).await;
    assert_eq!(code, "not_found");
    let listed = mcp.ok("list_services", json!({})).await;
    let slugs: Vec<_> = listed["items"].as_array().unwrap().iter().map(|s| s["slug"].clone()).collect();
    assert_eq!(slugs, vec![json!(a_slug)]);
    assert_eq!(get(&node, &b.key, &format!("/v1/services/{b_id}")).await.s(), 200, "the other tenant's service is untouched");

    // A key limited to one project sees and touches only that project's services.
    let other = create_service(&node, &a.key, &uniq("other"), "deno").await;
    let px = create_project(&node, &a.key, &uniq("mcp-only"), &[]).await;
    let a_svc = create_service_in(&node, &a.key, &px, "production", &uniq("in-px"), "deno").await;
    let (_, scoped) = make_key(&node, &a.id, &["services:read", "services:write", "deploy"], Some(vec![px.clone()])).await;
    let scoped = Mcp::start(&node, &scoped).await;
    let listed = scoped.ok("list_services", json!({})).await;
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    assert_eq!(listed["items"][0]["id"], json!(a_svc));
    let (code, _) = scoped.err("get_service", json!({"service": other})).await;
    assert_eq!(code, "not_found");
    let me = scoped.ok("whoami", json!({})).await;
    assert_eq!(me["project_ids"], json!([px]));
    scoped.ok("deploy_code", json!({"service": a_svc, "source": guests::versioned(1)})).await;
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_61_create_service_stays_inside_the_keys_projects_and_environments() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let pa = create_project(&node, &t.key, &uniq("mcp-a"), &["production", "dev"]).await;
    let pb = create_project(&node, &t.key, &uniq("mcp-b"), &["production", "dev"]).await;
    let (_, key) = make_key_in(&node, &t.id, &["services:read", "services:write"], Some(vec![pa.clone()]), Some(vec!["dev".into()])).await;
    let mcp = Mcp::start(&node, &key).await;
    mcp.ok("create_service", json!({"slug": uniq("ok"), "runtime": "wasm", "project": pa, "environment": "dev"})).await;
    for (project, env) in [(pb.as_str(), "dev"), (pa.as_str(), "production"), ("default", "production")] {
        let (code, _) = mcp.err("create_service", json!({"slug": uniq("no"), "runtime": "wasm", "project": project, "environment": env})).await;
        assert_eq!(code, "insufficient_scope", "{project}/{env}");
    }
    assert_eq!(get(&node, &t.key, "/v1/services").await.items().len(), 1);
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_61_a_service_id_cannot_reach_other_routes() {
    // Security review pass 2: the `service` argument went into the path unescaped, so a value
    // like `svc_…/data/export?` reached routes no tool offers (here with a read-only tool).
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("inj"), "deno").await;
    let mcp = Mcp::start(&node, &t.key).await;
    for (tool, suffix) in [("get_service", "/data/export?"), ("delete_service", "/dlq?"), ("get_service", "#x"), ("get_service", "%2Fdata")] {
        let (code, _) = mcp.err(tool, json!({"service": format!("{id}{suffix}")})).await;
        assert_eq!(code, "validation_failed", "{tool} {suffix}");
    }
    assert_eq!(mcp.ok("get_service", json!({"service": id})).await["id"], json!(id));
    node.shutdown().await;
}
}

node_test! {
async fn ac_sec_61_an_expired_or_revoked_key_loses_the_endpoint_at_once() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (key_id, key) = make_key(&node, &t.id, &["services:read"], None).await;
    let mcp = Mcp::start(&node, &key).await;
    mcp.ok("whoami", json!({})).await;
    assert_eq!(call(root(&node, Method::DELETE, &format!("/v1/tenants/{}/keys/{key_id}", t.id)), None).await.s(), 204);
    let r = post_mcp(&node, Some(&key), &request("tools/list", json!({})), &[]).await;
    assert_eq!(r.s(), 401, "{}", r.text);
    assert_eq!(r.header("www-authenticate").as_deref(), Some("Bearer error=\"invalid_token\""));

    // An expiring key: valid, then not.
    let r = call(
        root(&node, Method::POST, &format!("/v1/tenants/{}/keys", t.id)),
        Some(json!({"scopes": ["services:read"], "expires_at": rfc3339_in(2)})),
    )
    .await;
    let short = r.json["secret"].as_str().expect("secret").to_string();
    let ok = post_mcp(&node, Some(&short), &request("tools/list", json!({})), &[]).await;
    assert_eq!(ok.s(), 200, "{}", ok.text);
    tokio::time::sleep(std::time::Duration::from_millis(3_200)).await;
    let r = post_mcp(&node, Some(&short), &request("tools/list", json!({})), &[]).await;
    assert_eq!(r.s(), 401, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn list_schedules_returns_recurring_and_one_off_schedules() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("sched");
    let svc = create_service(&node, &t.key, &slug, "deno").await;
    let s = post(&node, &t.key, &format!("/v1/services/{svc}/schedules"), json!({"kind": "interval", "every_seconds": 3600})).await;
    assert_eq!(s.s(), 201, "{}", s.text);

    // a one-off schedule, as std:schedule stores it
    let db = SqliteQueueStore::open(&node.data_dir().join("queue.db"), 5).expect("open queue.db");
    let sid: rt_types::ServiceId = svc.parse().expect("service id");
    let put = db.put(sid, NewDelayed { id: "welcome".into(), run_at_ms: 1_900_000_000_000, payload: Bytes::from_static(b"{}") }).await;
    assert!(put.expect("put").is_some(), "stored");

    let mcp = Mcp::start(&node, &t.key).await;
    let listed = mcp.ok("list_schedules", json!({"service": slug})).await;
    assert_eq!(listed["service"], json!(svc), "{listed}");
    let items = listed["items"].as_array().expect("items");
    assert_eq!(items.len(), 2, "{listed}");
    assert_eq!(items[0]["kind"], json!("interval"));
    assert_eq!(items[0]["id"], s.json["id"]);
    assert_eq!(items[1], json!({"id": "welcome", "kind": "once", "run_at": 1_900_000_000_000i64}));
    node.shutdown().await;
}
}

node_test! {
/// The operator SDK reads: `list_sdks` is what the key's grants allow, `get_sdk_contract` its types.
async fn mcp_sdk_reads_show_the_granted_sdks_and_their_contract() {
    let node = start_node().await;
    assert_eq!(publish_sdk(&node, "acme", "1.0.0", &sdk_source("v1"), None).await.s(), 201);
    let t = new_tenant(&node, "pro").await;
    let (_, reader) = make_key(&node, &t.id, &["services:read"], None).await;
    let mcp = Mcp::start(&node, &reader).await;
    assert_eq!(mcp.ok("list_sdks", json!({})).await["items"], json!([]), "nothing granted yet");
    let (code, _) = mcp.err("get_sdk_contract", json!({"name": "acme"})).await;
    assert_eq!(code, "not_found");

    assert_eq!(put_grant(&node, json!({"scope": "node", "sdk": "acme", "effect": "allow"})).await.s(), 200);
    let listed = mcp.ok("list_sdks", json!({"environment": "production"})).await;
    assert_eq!(listed["items"][0]["name"], json!("acme"), "{listed}");
    assert_eq!(listed["items"][0]["version"], json!("1.0.0"), "{listed}");
    let r = mcp.call("get_sdk_contract", json!({"name": "acme"})).await;
    assert_eq!(r["isError"], json!(false), "{r}");
    assert!(r["structuredContent"]["value"].as_str().is_some_and(|t| t.contains("declare module \"sdk:acme\"")), "{r}");
    node.shutdown().await;
}
}

// ---------------------------------------------------------------------------------------------
// deploy_code / rollback with close_sockets
// ---------------------------------------------------------------------------------------------

/// An accepting echo service whose answer names its version.
fn ws_versioned(n: u32) -> String {
    format!(
        r#"import {{ sockets }} from "std:sockets";
export default {{
  fetch(req: Request) {{ return req.headers.get("upgrade") ? sockets.accept(req) : new Response("v{n}"); }},
  webSocketMessage(ws: any) {{ ws.send("v{n}"); }},
}};"#
    )
}

node_test! {
async fn deploy_code_and_rollback_close_sockets_with_1012_on_request() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let mcp = Mcp::start(&node, &t.key).await;
    let slug = uniq("agentws");
    mcp.ok("create_service", json!({"slug": slug, "runtime": "deno"})).await;
    mcp.ok("deploy_code", json!({"service": slug, "source": ws_versioned(1)})).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    // Without the flag the socket stays and the next event runs the new version.
    mcp.ok("deploy_code", json!({"service": slug, "source": ws_versioned(2)})).await;
    ws_send(&mut ws, "x").await;
    assert_eq!(ws_text(&mut ws).await, "v2");
    let r = mcp.ok("deploy_code", json!({"service": slug, "source": ws_versioned(3), "close_sockets": true})).await;
    assert_eq!(r["close_sockets"], json!(true), "{r}");
    assert_eq!(ws_close_code(&mut ws, std::time::Duration::from_secs(10)).await, Some(1012));
    let mut ws = ws_connect(&node, &slug, "/").await;
    let r = mcp.ok("rollback", json!({"service": slug, "close_sockets": true})).await;
    assert_eq!(r["to"], json!(2), "{r}");
    assert_eq!(ws_close_code(&mut ws, std::time::Duration::from_secs(10)).await, Some(1012));
    node.shutdown().await;
}
}

node_test! {
async fn list_objects_list_sockets_and_close_socket() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let mcp = Mcp::start(&node, &t.key).await;
    let slug = uniq("agentws");
    mcp.ok("create_service", json!({"slug": slug, "runtime": "deno"})).await;
    mcp.ok("deploy_code", json!({"service": slug, "source": ws_versioned(1)})).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    let objects = mcp.ok("list_objects", json!({"service": slug})).await;
    assert_eq!(objects["items"], json!([{"name": "default", "sockets": 1, "storage": false}]), "{objects}");
    let sockets = mcp.ok("list_sockets", json!({"service": slug, "object": "default"})).await;
    let id = sockets["items"][0]["id"].as_str().expect("socket id").to_string();
    assert!(id.starts_with("ws_"), "{sockets}");
    let r = mcp.ok("close_socket", json!({"service": slug, "object": "default", "socket": id, "code": 4000})).await;
    assert_eq!(r["closed"], json!(1), "{r}");
    assert_eq!(ws_close_code(&mut ws, std::time::Duration::from_secs(10)).await, Some(4000));
    // A key without sockets:write cannot close.
    let (_, reader) = make_key(&node, &t.id, &["services:read"], None).await;
    let ro = Mcp::start(&node, &reader).await;
    let (code, _) = ro.err("close_socket", json!({"service": slug, "socket": "ws_x"})).await;
    assert_eq!(code, "insufficient_scope");
    node.shutdown().await;
}
}
