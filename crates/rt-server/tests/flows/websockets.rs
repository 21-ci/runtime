//! WebSockets on a real node (docs/handlers.md "WebSockets"): the node owns the socket, a guest
//! accepts the upgrade from its `fetch` (`std:sockets`) and every event (message, close, error)
//! runs as one task that calls `webSocketMessage` / `webSocketClose` / `webSocketError`. No isolate
//! is alive between events; events of one object run one at a time, in order; a failed handler is
//! delivered again (at-least-once) and its sends are dropped.

use std::time::Duration;

use futures_util::SinkExt;
use serde_json::{json, Value};

use crate::common::*;

/// Echo: accepts every upgrade; `webSocketMessage` sends the message back. `/plain` is a normal
/// request; `/accept-plain` calls `accept` on a request that offered no upgrade.
const ECHO: &str = r#"
import { sockets } from "std:sockets";
export default {
  fetch(req: Request): Response {
    const u = new URL(req.url);
    if (u.pathname === "/accept-plain") {
      try { return sockets.accept(req); } catch (e: any) { return new Response(e.code ?? String(e), { status: 400 }); }
    }
    if (u.pathname === "/reject") return new Response("no", { status: 401 });
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      return sockets.accept(req, { protocol: req.headers.get("sec-websocket-protocol")?.split(",").map((s) => s.trim()).find((p) => p === "chat") });
    }
    return new Response("plain:" + (req.headers.get("sec-websocket-protocol") ?? ""));
  },
  webSocketMessage(ws: any, message: string | Uint8Array): void {
    if (message === "proto") { ws.send(String(ws.protocol ?? "")); return; }
    ws.send(message);
  },
};"#;

/// Records every event in std:kv (`/events` lists them) and checks that no two event tasks of the
/// object overlap (an `inflight` counter in std:kv, raised around an await).
const RECORDER: &str = r#"
import { sockets } from "std:sockets";
import { kv } from "std:kv";
async function record(e: unknown) {
  const n = await kv.incr("n", 1);
  await kv.set("e:" + String(n).padStart(6, "0"), e);
}
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    if (u.pathname === "/events") {
      const page = await kv.list({ prefix: "e:", limit: 1000 });
      return Response.json({ events: page.entries.map((x: any) => x.value), overlap: (await kv.get("overlap")) ?? 0 });
    }
    return sockets.accept(req);
  },
  async webSocketMessage(ws: any, message: string): Promise<void> {
    if ((await kv.incr("inflight", 1)) !== 1) await kv.incr("overlap", 1);
    await new Promise((r) => setTimeout(r, 2));
    await kv.incr("inflight", -1);
    if (message.startsWith("fail-once:")) {
      if ((await kv.incr("tries:" + message, 1)) === 1) { ws.send("dropped:" + message); throw new Error("first attempt fails"); }
    }
    if (message === "boom") { ws.send("dropped:boom"); throw new Error("always fails"); }
    ws.send(message);
  },
  async webSocketClose(ws: any, code: number, reason: string, wasClean: boolean): Promise<void> {
    await record({ kind: "close", id: ws.id, code, reason, wasClean });
  },
  async webSocketError(ws: any, error: any): Promise<void> {
    await record({ kind: "error", id: ws.id, code: error.code });
  },
};"#;

async fn tenant_node() -> (rt_testkit::NodeHarness, Tenant) {
    let node = start_node_with(|c| {
        c.plans.get_mut("pro").expect("pro").ws_max_message_bytes = 4096;
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    (node, t)
}

async fn events_of(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/events").await.json["events"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn inflight(node: &impl Node) -> u64 {
    call(root(node, reqwest::Method::GET, "/v1/node/status"), None)
        .await
        .json["inflight"]
        .as_u64()
        .unwrap_or(u64::MAX)
}

/// AC-WS-01: a message reaches `webSocketMessage` and the reply reaches the client; between
/// messages no task of the service is in flight (the socket is held by the node, not an isolate).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_echo() {
    let (node, t) = tenant_node().await;
    let slug = uniq("echo");
    ship(&node, &t.key, &slug, ECHO).await;
    assert_eq!(
        invoke(&node, &slug, "/plain").await.text,
        "plain:",
        "a plain request still works"
    );

    let mut ws = ws_connect(&node, &slug, "/").await;
    for msg in ["hi", "second", "third"] {
        ws_send(&mut ws, msg).await;
        assert_eq!(ws_text(&mut ws).await, msg);
        let n = &node;
        eventually(
            Duration::from_secs(5),
            "no task in flight between messages",
            || async move { inflight(n).await == 0 },
        )
        .await;
    }
    ws.send(WsMsg::binary(vec![1u8, 2, 3])).await.expect("send binary");
    match ws_next(&mut ws, Duration::from_secs(10)).await {
        Some(WsMsg::Binary(b)) => assert_eq!(b.as_ref(), &[1u8, 2, 3]),
        other => panic!("binary echo: {other:?}"),
    }
    node.shutdown().await;
}

/// A held socket is long-lived memory: while the node's memory pressure is `High` an upgrade is
/// `503` (`x-rt-reason: memory`), and once it eases upgrades work again. The pressure comes from
/// uploads that declare most of a 100 MiB budget and are charged before their bodies are read
/// (several, since the governor may evict caches to relieve the first one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_upgrade_refused_under_memory_pressure() {
    let node = start_node_with(|c| c.memory.budget_mb = 100).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("press");
    let id = ship(&node, &t.key, &slug, ECHO).await;
    drop(ws_connect(&node, &slug, "/").await);

    let n = &node;
    let memory =
        || async move { call(root(n, reqwest::Method::GET, "/v1/node/status"), None).await.json["memory"].clone() };
    let mut held = Vec::new();
    for _ in 0..rt_api::MAX_CONCURRENT_UPLOADS {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let m = memory().await;
        if m["pressure"] != json!("Normal") {
            break;
        }
        let (budget, committed) = (
            m["budget_bytes"].as_u64().unwrap(),
            m["committed_bytes"].as_u64().unwrap(),
        );
        let declared = (budget * 88 / 100)
            .saturating_sub(committed)
            .min(budget.saturating_sub(committed + (1 << 20)))
            .min(64 << 20);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
        tx.send(Ok(bytes::Bytes::from_static(b"// ")))
            .await
            .expect("first chunk");
        let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|c| (c, rx)) });
        let rb = api(
            &node,
            &t.key,
            reqwest::Method::POST,
            &format!("/v1/services/{id}/versions?activate=0"),
        )
        .header("content-type", "application/typescript")
        .header("content-length", declared.to_string())
        .body(reqwest::Body::wrap_stream(stream));
        held.push((tx, tokio::spawn(async move { send(rb).await })));
    }
    eventually(Duration::from_secs(5), "memory pressure is high", || async move {
        memory().await["pressure"] != json!("Normal")
    })
    .await;
    match ws_connect_with(&node, &slug, "/", &[]).await {
        Err(status) => assert_eq!(status, 503),
        Ok(_) => panic!("an upgrade under memory pressure must be refused"),
    }

    for (tx, task) in held {
        drop(tx);
        let _ = task.await;
    }
    let (n, sl) = (&node, &slug);
    eventually(Duration::from_secs(10), "an upgrade works again", || async move {
        ws_connect_with(n, sl, "/", &[]).await.is_ok()
    })
    .await;
    node.shutdown().await;
}

/// AC-WS-02: a `fetch` that answers without `accept` is an ordinary response: no upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_reject() {
    let (node, t) = tenant_node().await;
    let slug = uniq("echo");
    ship(&node, &t.key, &slug, ECHO).await;
    assert_eq!(ws_connect_with(&node, &slug, "/reject", &[]).await.err(), Some(401));
    // `accept` on a request without `Upgrade: websocket`.
    let r = invoke(&node, &slug, "/accept-plain").await;
    assert_eq!((r.s(), r.text.as_str()), (400, "not_upgradable"));
    node.shutdown().await;
}

/// AC-WS-03: events of one object never overlap, and each socket's messages arrive in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_serial() {
    let (node, t) = tenant_node().await;
    let slug = uniq("rec");
    ship(&node, &t.key, &slug, RECORDER).await;
    let mut a = ws_connect(&node, &slug, "/").await;
    let mut b = ws_connect(&node, &slug, "/").await;
    for i in 0..40 {
        ws_send(&mut a, &format!("a{i}")).await;
        ws_send(&mut b, &format!("b{i}")).await;
    }
    for i in 0..40 {
        assert_eq!(ws_text(&mut a).await, format!("a{i}"), "socket a in order");
        assert_eq!(ws_text(&mut b).await, format!("b{i}"), "socket b in order");
    }
    let r = invoke(&node, &slug, "/events").await;
    assert_eq!(
        r.json["overlap"],
        json!(0),
        "no two event tasks of one object overlapped"
    );
    node.shutdown().await;
}

/// AC-WS-04: a message over `plan.ws_max_message_bytes` closes the socket with 1009.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_limits() {
    let (node, t) = tenant_node().await;
    let slug = uniq("echo");
    ship(&node, &t.key, &slug, ECHO).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, &"x".repeat(4000)).await;
    assert_eq!(ws_text(&mut ws).await.len(), 4000, "under the cap");
    let _ = ws.send(WsMsg::text("y".repeat(5000))).await;
    assert_eq!(
        ws_close_code(&mut ws, Duration::from_secs(10)).await,
        Some(1009),
        "over the cap: 1009"
    );
    node.shutdown().await;
}

/// AC-WS-05: `access = "key"` refuses an upgrade without a key before any task runs; a key in
/// the subprotocol list (`rtk_…`, for browsers) is accepted and never reaches the guest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_auth() {
    let (node, t) = tenant_node().await;
    let slug = uniq("keyed");
    let id = ship(&node, &t.key, &slug, ECHO).await;
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"access": "key"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(
        ws_connect_with(&node, &slug, "/", &[]).await.err(),
        Some(401),
        "no key: 401"
    );
    let (_, invoke_key) = make_key(&node, &t.id, &["invoke"], None).await;
    let protocols = format!("{invoke_key}, chat");
    let mut ws = ws_connect_with(&node, &slug, "/", &[("sec-websocket-protocol", &protocols)])
        .await
        .expect("keyed upgrade");
    ws_send(&mut ws, "proto").await;
    assert_eq!(ws_text(&mut ws).await, "chat", "the guest saw (and chose) only `chat`");
    node.shutdown().await;
}

/// AC-WS-06: a client close reaches `webSocketClose` once, with its code, reason and wasClean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_close() {
    let (node, t) = tenant_node().await;
    let slug = uniq("rec");
    ship(&node, &t.key, &slug, RECORDER).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "x").await;
    assert_eq!(ws_text(&mut ws).await, "x");
    ws.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
        reason: "bye".into(),
    }))
    .await
    .expect("close");
    let (n, s) = (&node, slug.clone());
    eventually(Duration::from_secs(10), "webSocketClose", || {
        let s = s.clone();
        async move { !events_of(n, &s).await.is_empty() }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ev = events_of(&node, &slug).await;
    assert_eq!(ev.len(), 1, "one close event: {ev:?}");
    assert_eq!(
        (&ev[0]["kind"], &ev[0]["code"], &ev[0]["reason"], &ev[0]["wasClean"]),
        (&json!("close"), &json!(1000), &json!("bye"), &json!(true))
    );
    assert!(ev[0]["id"].as_str().is_some_and(|id| id.starts_with("ws_")), "{ev:?}");
    node.shutdown().await;
}

/// AC-WS-07: a handler that throws is delivered again (at-least-once) and the sends of the
/// failed attempt are dropped; one that keeps failing closes the socket with 1011.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_failure() {
    let (node, t) = tenant_node().await;
    let slug = uniq("rec");
    ship(&node, &t.key, &slug, RECORDER).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "fail-once:1").await;
    assert_eq!(
        ws_text(&mut ws).await,
        "fail-once:1",
        "the redelivery answered; the failed attempt's send was dropped"
    );
    ws_send(&mut ws, "after").await;
    assert_eq!(ws_text(&mut ws).await, "after", "the next message is processed");
    ws_send(&mut ws, "boom").await;
    assert_eq!(
        ws_close_code(&mut ws, Duration::from_secs(20)).await,
        Some(1011),
        "a poison message closes the socket 1011"
    );
    node.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// Phase 2: std:sockets CRUD, tags, attachments, auto-response, caps, the control API, deploys
// ---------------------------------------------------------------------------------------------

/// The full `std:sockets` surface. `fetch`: an upgrade is accepted with `?tags=a,b` and an
/// `?att=` JSON attachment (`?big=1`: 16 KiB + 1); plain requests drive the API from an HTTP task
/// (`/list`, `/count`, `/broadcast`, `/close-all`, `/get`, `/send`, `/auto`). Messages: `att`
/// answers the attachment, `set:<json>` replaces it, `tag:<t>` retags, `id` answers the socket id.
const FULL: &str = r#"
import { sockets } from "std:sockets";
import { kv } from "std:kv";
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const q = (k: string) => u.searchParams.get(k);
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      try {
        if (q("auto") !== null) sockets.setAutoResponse({ request: "ping", response: "pong" });
        const tags = q("tags") ? q("tags")!.split(",") : [];
        const attachment = q("big") !== null ? "x".repeat(16 * 1024 + 1) : q("att") !== null ? JSON.parse(q("att")!) : undefined;
        const res = sockets.accept(req, { tags, attachment });
        (res as any).socket.send("welcome:" + (res as any).socket.id);
        return res;
      } catch (e: any) {
        return new Response(e.code ?? String(e), { status: e.code === "too_large" ? 413 : 429 });
      }
    }
    const opt = { object: q("object") ?? undefined, tag: q("tag") ?? undefined };
    try {
      switch (u.pathname) {
        case "/list": return Response.json((await sockets.list(opt)).sockets.map((s: any) => ({ id: s.id, tags: s.tags, object: s.object, autoResponseAt: s.autoResponseAt ?? null })));
        case "/count": return Response.json(sockets.count(opt));
        case "/broadcast": return Response.json(sockets.broadcast(q("msg")!, { ...opt, object: q("object") ?? "default", except: q("except") ?? undefined }));
        case "/close-all": return Response.json(sockets.closeAll({ ...opt, object: q("object") ?? "default", code: 4002, reason: "all" }));
        case "/get": { const s = sockets.get(q("id")!); return Response.json(s === null ? null : { id: s.id, attachment: s.attachment ?? null }); }
        case "/send": sockets.send(q("id")!, q("msg")!); return new Response("sent");
        case "/events": return Response.json((await kv.get("events")) ?? 0);
        case "/version": return new Response("v1");
      }
    } catch (e: any) {
      return new Response(e.code ?? String(e), { status: 400 });
    }
    return new Response("?", { status: 404 });
  },
  async webSocketMessage(ws: any, message: string): Promise<void> {
    await kv.incr("events", 1);
    if (message === "att") ws.send(JSON.stringify(ws.attachment ?? null));
    else if (message.startsWith("set:")) { ws.serializeAttachment(JSON.parse(message.slice(4))); ws.send("ok"); }
    else if (message.startsWith("tag:")) { sockets.setTags(ws.id, [message.slice(4)]); ws.send("ok"); }
    else if (message === "id") ws.send(ws.id);
    else if (message === "version") ws.send("v1");
    else ws.send("echo:" + message);
  },
};"#;

async fn welcome(ws: &mut Ws) -> String {
    let w = ws_text(ws).await;
    w.strip_prefix("welcome:")
        .unwrap_or_else(|| panic!("welcome frame, got {w}"))
        .to_string()
}

/// AC-WS-08: tags select sockets (`list`, `count`, `broadcast`) and can change in an event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_tags() {
    let (node, t) = tenant_node().await;
    let slug = uniq("full");
    ship(&node, &t.key, &slug, FULL).await;
    let mut a = ws_connect(&node, &slug, "/?tags=room:1,admin").await;
    let mut b = ws_connect(&node, &slug, "/?tags=room:1").await;
    let mut c = ws_connect(&node, &slug, "/?tags=room:2").await;
    let (ida, idb, _idc) = (welcome(&mut a).await, welcome(&mut b).await, welcome(&mut c).await);
    assert_eq!(invoke(&node, &slug, "/count?tag=room:1").await.json, json!(2));
    assert_eq!(
        invoke(&node, &slug, "/count").await.json,
        json!(3),
        "no object, no tag: every socket of the service"
    );
    let listed = invoke(&node, &slug, "/list?tag=admin").await.json;
    assert_eq!(listed.as_array().map(|a| a.len()), Some(1));
    assert_eq!(listed[0]["id"], json!(ida));
    assert_eq!(listed[0]["tags"], json!(["room:1", "admin"]));
    let sent = invoke(&node, &slug, &format!("/broadcast?msg=hello&tag=room:1&except={idb}")).await;
    assert_eq!(sent.json["sent"], json!(1), "{}", sent.text);
    assert_eq!(ws_text(&mut a).await, "hello");
    ws_send(&mut b, "tag:room:2").await;
    assert_eq!(ws_text(&mut b).await, "ok");
    assert_eq!(
        invoke(&node, &slug, "/count?tag=room:2").await.json,
        json!(2),
        "retagged in an event"
    );
    let sent = invoke(&node, &slug, "/broadcast?msg=two&tag=room:2").await;
    assert_eq!(sent.json["sent"], json!(2));
    assert_eq!(ws_text(&mut b).await, "two");
    assert_eq!(ws_text(&mut c).await, "two");
    node.shutdown().await;
}

/// AC-WS-09: an attachment set at accept is read by a later event in a fresh isolate, can be
/// replaced, and is capped at 16 KiB serialized.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_attachment_survives() {
    let (node, t) = tenant_node().await;
    let slug = uniq("full");
    let id = ship(&node, &t.key, &slug, FULL).await;
    let r = patch(
        &node,
        &t.key,
        &format!("/v1/services/{id}"),
        json!({"ws": {"warm": false}}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let mut ws = ws_connect(&node, &slug, "/?att=%7B%22user%22%3A%22u1%22%2C%22n%22%3A1%7D").await;
    let sid = welcome(&mut ws).await;
    ws_send(&mut ws, "att").await;
    assert_eq!(
        serde_json::from_str::<Value>(&ws_text(&mut ws).await).expect("json"),
        json!({"user": "u1", "n": 1})
    );
    ws_send(&mut ws, r#"set:{"user":"u1","n":2}"#).await;
    assert_eq!(ws_text(&mut ws).await, "ok");
    ws_send(&mut ws, "att").await;
    assert_eq!(
        serde_json::from_str::<Value>(&ws_text(&mut ws).await).expect("json"),
        json!({"user": "u1", "n": 2})
    );
    let got = invoke(&node, &slug, &format!("/get?id={sid}")).await;
    assert_eq!(
        got.json["attachment"],
        json!({"user": "u1", "n": 2}),
        "an HTTP task reads it too: {}",
        got.text
    );
    assert_eq!(
        ws_connect_with(&node, &slug, "/?big=1", &[]).await.err(),
        Some(413),
        "16 KiB + 1: too_large"
    );
    node.shutdown().await;
}

/// AC-WS-10: the object's auto-response answers without an event task and stamps
/// `autoResponseAt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_autoresponse() {
    let (node, t) = tenant_node().await;
    let slug = uniq("full");
    ship(&node, &t.key, &slug, FULL).await;
    let mut ws = ws_connect(&node, &slug, "/?auto=1").await;
    welcome(&mut ws).await;
    for _ in 0..5 {
        ws_send(&mut ws, "ping").await;
        assert_eq!(ws_text(&mut ws).await, "pong");
    }
    assert_eq!(
        invoke(&node, &slug, "/events").await.json,
        json!(0),
        "no event task ran"
    );
    let listed = invoke(&node, &slug, "/list").await.json;
    assert!(
        listed[0]["autoResponseAt"].as_f64().is_some_and(|t| t > 0.0),
        "{listed}"
    );
    ws_send(&mut ws, "other").await;
    assert_eq!(ws_text(&mut ws).await, "echo:other", "anything else is an event");
    node.shutdown().await;
}

/// AC-WS-11: an HTTP task sends to and closes sockets of an object it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_broadcast_from_http() {
    let (node, t) = tenant_node().await;
    let slug = uniq("full");
    ship(&node, &t.key, &slug, FULL).await;
    let mut a = ws_connect(&node, &slug, "/").await;
    let mut b = ws_connect(&node, &slug, "/").await;
    let ida = welcome(&mut a).await;
    welcome(&mut b).await;
    assert_eq!(invoke(&node, &slug, "/broadcast?msg=all").await.json["sent"], json!(2));
    assert_eq!(ws_text(&mut a).await, "all");
    assert_eq!(ws_text(&mut b).await, "all");
    assert_eq!(
        invoke(&node, &slug, &format!("/send?id={ida}&msg=direct")).await.text,
        "sent"
    );
    assert_eq!(ws_text(&mut a).await, "direct");
    assert_eq!(invoke(&node, &slug, "/close-all").await.json["closed"], json!(2));
    assert_eq!(ws_close_code(&mut a, Duration::from_secs(10)).await, Some(4002));
    assert_eq!(ws_close_code(&mut b, Duration::from_secs(10)).await, Some(4002));
    node.shutdown().await;
}

/// AC-WS-12: another service never reaches a socket by its id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_cross_service_denied() {
    let (node, t) = tenant_node().await;
    let (owner, other) = (uniq("full"), uniq("full"));
    ship(&node, &t.key, &owner, FULL).await;
    ship(&node, &t.key, &other, FULL).await;
    let mut ws = ws_connect(&node, &owner, "/").await;
    let id = welcome(&mut ws).await;
    assert_eq!(invoke(&node, &other, &format!("/get?id={id}")).await.json, Value::Null);
    let r = invoke(&node, &other, &format!("/send?id={id}&msg=x")).await;
    assert_eq!((r.s(), r.text.as_str()), (400, "not_found"));
    assert_eq!(
        invoke(&node, &owner, &format!("/send?id={id}&msg=mine")).await.text,
        "sent"
    );
    assert_eq!(ws_text(&mut ws).await, "mine");
    node.shutdown().await;
}

/// AC-WS-13: socket caps: the plan's per tenant and per object, the service's own, the node's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_plan_caps() {
    let node = start_node_with(|c| {
        let mut tiny = c.plans.get("pro").expect("pro").clone();
        tiny.max_sockets = 3;
        tiny.max_sockets_per_object = 2;
        c.plans.insert("tiny".to_string(), tiny);
        c.ws.max_sockets = 5;
    })
    .await;
    let t = new_tenant(&node, "tiny").await;
    let (a, b) = (uniq("full"), uniq("full"));
    ship(&node, &t.key, &a, FULL).await;
    let bid = ship(&node, &t.key, &b, FULL).await;
    let _a1 = ws_connect(&node, &a, "/").await;
    let _a2 = ws_connect(&node, &a, "/").await;
    assert_eq!(
        ws_connect_with(&node, &a, "/", &[]).await.err(),
        Some(429),
        "per object: 2"
    );
    let r = patch(
        &node,
        &t.key,
        &format!("/v1/services/{bid}"),
        json!({"ws": {"max_sockets": 1}}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let _b1 = ws_connect(&node, &b, "/").await;
    assert_eq!(
        ws_connect_with(&node, &b, "/", &[]).await.err(),
        Some(429),
        "the service's own cap: 1"
    );
    let r = patch(&node, &t.key, &format!("/v1/services/{bid}"), json!({"ws": {}})).await;
    assert_eq!(r.s(), 200);
    assert_eq!(
        ws_connect_with(&node, &b, "/", &[]).await.err(),
        Some(429),
        "per tenant: 3"
    );
    // The node's cap: a second tenant fills the node (5).
    let t2 = new_tenant(&node, "pro").await;
    let c = uniq("full");
    ship(&node, &t2.key, &c, FULL).await;
    let _c1 = ws_connect(&node, &c, "/").await;
    let _c2 = ws_connect(&node, &c, "/").await;
    assert_eq!(
        ws_connect_with(&node, &c, "/", &[]).await.err(),
        Some(429),
        "the node holds 5"
    );
    let status = call(root(&node, reqwest::Method::GET, "/v1/node/status"), None).await;
    assert_eq!(status.json["sockets"]["open"], json!(5), "{}", status.text);
    node.shutdown().await;
}

/// AC-WS-14: the control API lists objects and sockets and closes one (audited) or several.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_api_list_close() {
    #[derive(Default)]
    struct Recorder(std::sync::Mutex<Vec<rt_sec::AuditEvent>>);
    impl rt_sec::AuditSink for Recorder {
        fn emit(&self, ev: rt_sec::AuditEvent) {
            self.0.lock().expect("lock").push(ev);
        }
    }
    let rec = std::sync::Arc::new(Recorder::default());
    let sink: std::sync::Arc<dyn rt_sec::AuditSink> = rec.clone();
    let node = start_node_opts(
        rt_server::StartOptions {
            extra_audit: Some(sink),
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("full");
    let id = ship(&node, &t.key, &slug, FULL).await;
    let mut a = ws_connect(&node, &slug, "/?tags=x").await;
    let mut b = ws_connect(&node, &slug, "/?tags=y").await;
    let ida = welcome(&mut a).await;
    welcome(&mut b).await;
    let objects = get(&node, &t.key, &format!("/v1/services/{id}/objects")).await;
    assert_eq!(objects.s(), 200, "{}", objects.text);
    assert_eq!(
        objects.items(),
        vec![json!({"name": "default", "sockets": 2, "storage": false})]
    );
    let listed = get(
        &node,
        &t.key,
        &format!("/v1/services/{id}/objects/default/sockets?tag=x"),
    )
    .await;
    assert_eq!(listed.items().len(), 1, "{}", listed.text);
    assert_eq!(listed.items()[0]["id"], json!(ida));
    let r = delete(
        &node,
        &t.key,
        &format!("/v1/services/{id}/objects/default/sockets/{ida}?code=4001&reason=kicked"),
    )
    .await;
    assert_eq!(r.s(), 204, "{}", r.text);
    assert_eq!(ws_close_code(&mut a, Duration::from_secs(10)).await, Some(4001));
    assert!(
        rec.0
            .lock()
            .expect("lock")
            .iter()
            .any(|e| e.action == "sockets.closed_by_api"),
        "audited"
    );
    let r = delete(
        &node,
        &t.key,
        &format!("/v1/services/{id}/objects/default/sockets?tag=y"),
    )
    .await;
    assert_eq!((r.s(), &r.json["closed"]), (200, &json!(1)), "{}", r.text);
    assert_eq!(ws_close_code(&mut b, Duration::from_secs(10)).await, Some(1000));
    // `sockets:write` is required to close.
    let (_, reader) = make_key(&node, &t.id, &["services:read"], None).await;
    let r = delete(
        &node,
        &reader,
        &format!("/v1/services/{id}/objects/default/sockets/{ida}"),
    )
    .await;
    assert_eq!(r.s(), 403, "{}", r.text);
    node.shutdown().await;
}

/// AC-WS-15: sockets survive a deploy (the next event runs the new code); a deploy or a rollback
/// with `close_sockets` closes them with 1012.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_deploy_keep_and_close() {
    let (node, t) = tenant_node().await;
    let slug = uniq("full");
    let id = ship(&node, &t.key, &slug, FULL).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    welcome(&mut ws).await;
    ws_send(&mut ws, "version").await;
    assert_eq!(ws_text(&mut ws).await, "v1");
    let v2 = FULL.replace("ws.send(\"v1\")", "ws.send(\"v2\")");
    let r = upload_ts(&node, &t.key, &id, &v2, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    ws_send(&mut ws, "version").await;
    assert_eq!(
        ws_text(&mut ws).await,
        "v2",
        "the socket survived; the next event ran v2"
    );
    let r = upload_ts(
        &node,
        &t.key,
        &id,
        &v2.replace("\"v2\"", "\"v3\""),
        "activate=1&close_sockets=1",
    )
    .await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    assert_eq!(
        ws_close_code(&mut ws, Duration::from_secs(10)).await,
        Some(1012),
        "close_sockets: 1012"
    );
    let mut ws = ws_connect(&node, &slug, "/").await;
    welcome(&mut ws).await;
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{id}/live"),
        json!({"version": 1, "close_sockets": true}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(
        ws_close_code(&mut ws, Duration::from_secs(10)).await,
        Some(1012),
        "rollback with close_sockets"
    );
    node.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// Phase 5: drain, service deletion, metrics, stats
// ---------------------------------------------------------------------------------------------

/// AC-WS-16: a draining node closes its sockets with 1001 and takes no new upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_drain_1001() {
    let handle = new_handle();
    let node = start_node_opts(rt_server::StartOptions::default(), Some(handle.clone()), |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("echo");
    ship(&node, &t.key, &slug, ECHO).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "x").await;
    assert_eq!(ws_text(&mut ws).await, "x");
    handle.lock().expect("lock").as_ref().expect("running").drain();
    assert_eq!(
        ws_close_code(&mut ws, Duration::from_secs(10)).await,
        Some(1001),
        "going away"
    );
    assert_eq!(
        ws_connect_with(&node, &slug, "/", &[]).await.err(),
        Some(503),
        "no upgrade while draining"
    );
    node.shutdown().await;
}

/// AC-WS-17: deleting a service closes its sockets with 1001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_service_delete_closes() {
    let (node, t) = tenant_node().await;
    let slug = uniq("echo");
    let id = ship(&node, &t.key, &slug, ECHO).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "x").await;
    assert_eq!(ws_text(&mut ws).await, "x");
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{id}")).await.s(), 204);
    assert_eq!(ws_close_code(&mut ws, Duration::from_secs(10)).await, Some(1001));
    node.shutdown().await;
}

/// AC-WS-18: socket metrics and the service's stats count sockets, upgrades, messages and events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_ws_metrics_and_stats() {
    let (node, t) = tenant_node().await;
    let slug = uniq("echo");
    let id = ship(&node, &t.key, &slug, ECHO).await;
    let mut ws = ws_connect(&node, &slug, "/").await;
    ws_send(&mut ws, "x").await;
    assert_eq!(ws_text(&mut ws).await, "x");
    let stats = get(&node, &t.key, &format!("/v1/services/{id}/stats")).await;
    assert_eq!(
        stats.json["sockets"],
        json!({"open": 1, "objects": 1}),
        "{}",
        stats.text
    );
    let text = node
        .http()
        .get(node.metrics_url("/metrics"))
        .send()
        .await
        .expect("metrics")
        .text()
        .await
        .expect("text");
    assert_eq!(metric_sum(&text, "rt_ws_sockets"), 1.0, "open sockets");
    for name in [
        "rt_ws_upgrades_total",
        "rt_ws_messages_total",
        "rt_ws_event_tasks_total",
    ] {
        assert!(metric_sum(&text, name) >= 1.0, "{name}: {text}");
    }
    ws.close(None).await.expect("close");
    let n = &node;
    eventually(
        Duration::from_secs(10),
        "the socket gone from the gauge",
        || async move {
            let text = n
                .http()
                .get(n.metrics_url("/metrics"))
                .send()
                .await
                .expect("metrics")
                .text()
                .await
                .expect("text");
            metric_sum(&text, "rt_ws_sockets") == 0.0 && metric_sum(&text, "rt_ws_closed_total") >= 1.0
        },
    )
    .await;
    node.shutdown().await;
}
