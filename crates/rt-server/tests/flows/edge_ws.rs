//! Edge cases of WebSocket upgrades, the sockets API and runtime response headers (scratchpad
//! edge-cases.md §WebSockets, §outcomes).

use std::time::Duration;

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

/// Accepts an upgrade; on a plain request answers with headers only the runtime may set.
const GUEST: &str = r#"
import { sockets } from "std:sockets";
export default {
  fetch(req: Request): Response {
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") return sockets.accept(req);
    return new Response("body", { headers: { "x-rt-ws-ticket": "forged", "x-rt-task-id": "forged", "x-rt-version": "999" } });
  },
  webSocketMessage(ws: any, m: string): void { ws.send(m); },
};"#;

async fn ship_guest(node: &impl Node, key: &str, extra: Value) -> (String, String) {
    let slug = uniq("ews");
    let mut body = json!({"slug": slug, "runtime": "deno"});
    if let (Some(b), Value::Object(e)) = (body.as_object_mut(), extra) {
        b.extend(e);
    }
    let r = post(node, key, "/v1/services", body).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(node, key, &id, GUEST, "activate=1").await;
    assert_eq!(up.s(), 201, "{}", up.text);
    (slug, id)
}

node_test! {
/// OUTC-003 / OUTC-004: on an ordinary response a guest cannot forge the runtime's headers:
/// `x-rt-task-id` / `x-rt-version` are the runtime's, and a stray `x-rt-ws-ticket` neither
/// breaks the response nor leaks out.
async fn outc_003_004_runtime_headers_cannot_be_forged() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (slug, _) = ship_guest(&node, &t.key, json!({})).await;
    let r = invoke(&node, &slug, "/").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "body"));
    assert_eq!(r.header("x-rt-version").as_deref(), Some("1"));
    let task = r.header("x-rt-task-id").expect("task id");
    assert_ne!(task, "forged");
    assert_eq!(r.headers.get_all("x-rt-task-id").iter().count(), 1);
    assert!(r.header("x-rt-ws-ticket").is_none(), "{:?}", r.headers);
    node.shutdown().await;
}
}

node_test! {
/// WSUP-007 (decided): a request that asks for an upgrade without `Sec-WebSocket-Key` is not a
/// WebSocket handshake: it is served as a plain request (never `101`), so no socket is reserved.
async fn wsup_007_an_upgrade_without_a_key_is_a_plain_request() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (slug, _) = ship_guest(&node, &t.key, json!({})).await;
    let req = format!(
        "GET /s/{slug}/ HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    let r = raw_tcp(node.addrs.public, req.as_bytes(), Duration::from_secs(10)).await.expect("response");
    assert_eq!((r.status, r.text()), (200, "body".to_string()));
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    node.shutdown().await;
}
}

node_test! {
/// WSUP-012 / WSUP-014: close codes are 1000 or 3000-4999; a reason is at most 123 bytes (a
/// multibyte one at exactly 123 passes); object names are 1 to 256 bytes without control
/// characters, and `../x` is just a name.
async fn wsup_012_014_close_arguments_and_object_names() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, svc) = ship_guest(&node, &t.key, json!({})).await;
    let close = |name: &str, query: &str| {
        call(api(&node, &t.key, Method::DELETE, &format!("/v1/services/{svc}/objects/{name}/sockets?{query}")), None)
    };
    for code in [999, 1001, 1005, 2999, 5000] {
        let r = close("o", &format!("code={code}")).await;
        assert_eq!(r.s(), 422, "code {code}: {}", r.text);
    }
    for code in [1000, 3000, 4999] {
        assert_eq!(close("o", &format!("code={code}")).await.s(), 200, "code {code}");
    }
    // 41 three-byte characters = 123 bytes; one more ASCII byte is over.
    let ok = "%E2%82%AC".repeat(41);
    assert_eq!(close("o", &format!("reason={ok}")).await.s(), 200);
    assert_eq!(close("o", &format!("reason={ok}x")).await.s(), 422);
    assert_eq!(close(&"n".repeat(257), "").await.s(), 422, "257 bytes");
    assert_eq!(close(&"n".repeat(256), "").await.s(), 200, "256 bytes");
    assert_eq!(close("a%07b", "").await.s(), 422, "control character");
    assert_eq!(close("..%2Fx", "").await.s(), 200, "a name, not a path");
    node.shutdown().await;
}
}

node_test! {
/// WSUP-013: deleting an object of a service that is not durable, or one that does not exist,
/// is `404`.
async fn wsup_013_object_delete_of_nothing_is_404() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, svc) = ship_guest(&node, &t.key, json!({})).await;
    let r = delete(&node, &t.key, &format!("/v1/services/{svc}/objects/o1")).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    node.shutdown().await;
}
}
