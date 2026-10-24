//! Durable objects on a real node: a durable service's class
//! has one instance per object, chosen in code (`durable.get(name)`); each object keeps its own
//! storage (rows of the service's data.db) and alarm; its requests and events run one at a time;
//! an event's writes commit with its handler, before its sends leave.

use std::time::{Duration, Instant};

use futures_util::SinkExt;
use serde_json::{json, Value};

use crate::common::*;

/// The durable class. `fetch`: an upgrade is accepted (tag `?user=`); `/hit` counts per object;
/// `/alarm?ms=` sets the alarm; `/fail-first` makes the next alarm throw once; `/state` reads the
/// object back. Messages: `who`, `count` (events seen by this instance), `sleep`,
/// `write-then-throw`; anything else is broadcast to the object.
const ROOMS: &str = r#"
import { sockets } from "std:sockets";
export default class Room {
  state: any; name: string; events: number;
  constructor(state: any, _env: any) { this.state = state; this.name = state.id.name; this.events = 0; }
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const s = this.state.storage;
    if (req.headers.get("upgrade")) {
      const res: any = sockets.accept(req, { tags: [u.searchParams.get("user") ?? "anon"] });
      if (u.searchParams.get("hello")) res.socket.send("hello " + this.name); // before it connects, behind the output gate
      return res;
    }
    if (u.pathname.endsWith("/hit")) { const n = ((await s.get("hits")) ?? 0) + 1; await s.put("hits", n); return Response.json({ object: this.name, hits: n }); }
    if (u.pathname.endsWith("/alarm")) { await s.setAlarm(Date.now() + Number(u.searchParams.get("ms") ?? "200")); return new Response("set"); }
    if (u.pathname.endsWith("/fail-first")) { await s.put("failFirst", true); return new Response("ok"); }
    if (u.pathname.endsWith("/sleep")) { await new Promise((r) => setTimeout(r, 500)); return new Response("slept"); }
    if (u.pathname.endsWith("/state")) {
      const m = await s.list({ prefix: "" });
      return Response.json({ object: this.name, fired: m.get("fired") ?? 0, x: m.get("x") ?? null, hits: m.get("hits") ?? 0, alarm: await s.getAlarm() });
    }
    return new Response("?", { status: 404 });
  }
  async webSocketMessage(ws: any, msg: string): Promise<void> {
    this.events++;
    if (msg === "who") { ws.send(this.name); return; }
    if (msg === "count") { ws.send(String(this.events)); return; }
    if (msg === "sleep") { await new Promise((r) => setTimeout(r, 300)); ws.send("slept"); return; }
    if (msg === "write-then-throw") { await this.state.storage.put("x", 1); ws.send("leaked"); throw new Error("boom"); }
    sockets.broadcast(this.name + ":" + msg);
  }
  async alarm(info: any): Promise<void> {
    const s = this.state.storage;
    const fired = ((await s.get("fired")) ?? 0) + 1;
    await s.put("fired", fired);
    if (info.retryCount === 0 && (await s.get("failFirst"))) throw new Error("retry me");
    sockets.broadcast("alarm:" + info.retryCount);
  }
}"#;

/// A stateless front: checks a token, then forwards to the room object `/room/<name>/...` of the
/// durable service `ROOMS_SLUG` (`/plain/<name>` targets a service that is not durable).
fn front(rooms: &str, plain: &str) -> String {
    format!(
        r#"
import {{ durable }} from "std:durable";
export default {{
  async fetch(req: Request): Promise<Response> {{
    const u = new URL(req.url);
    if (u.searchParams.get("token") !== "secret") return new Response("no", {{ status: 401 }});
    const [, kind, name] = u.pathname.split("/");
    try {{
      return await durable.get(name, {{ service: kind === "plain" ? "{plain}" : "{rooms}" }}).fetch(req);
    }} catch (e: any) {{
      return new Response(e.code ?? String(e), {{ status: 400 }});
    }}
  }},
}};"#
    )
}

struct Setup {
    node: rt_testkit::NodeHarness,
    t: Tenant,
    rooms: String,
    rooms_id: String,
    front: String,
}

async fn setup(warm: bool) -> Setup {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (rooms, front_slug, plain) = (uniq("rooms"), uniq("front"), uniq("plain"));
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": rooms, "runtime": "deno", "durable": true, "ws": {"warm": warm}}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let rooms_id = r.json["id"].as_str().expect("id").to_string();
    let r = upload_ts(&node, &t.key, &rooms_id, ROOMS, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    ship(
        &node,
        &t.key,
        &plain,
        "export default { fetch() { return new Response('plain'); } };",
    )
    .await;
    ship(&node, &t.key, &front_slug, &front(&rooms, &plain)).await;
    Setup {
        node,
        t,
        rooms,
        rooms_id,
        front: front_slug,
    }
}

async fn hit(s: &Setup, room: &str) -> Value {
    let r = invoke(&s.node, &s.front, &format!("/room/{room}/hit?token=secret")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    r.json
}

async fn state(s: &Setup, room: &str) -> Value {
    invoke(&s.node, &s.front, &format!("/room/{room}/state?token=secret"))
        .await
        .json
}

/// AC-OBJ-01: the same name reaches the same object's storage; other names are isolated; a
/// request straight to the durable service is its `default` object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_name_routing() {
    let s = setup(true).await;
    assert_eq!(hit(&s, "a").await, json!({"object": "a", "hits": 1}));
    assert_eq!(hit(&s, "a").await, json!({"object": "a", "hits": 2}));
    assert_eq!(hit(&s, "b").await, json!({"object": "b", "hits": 1}));
    let direct = invoke(&s.node, &s.rooms, "/hit").await;
    assert_eq!(direct.json, json!({"object": "default", "hits": 1}), "{}", direct.text);
    let r = invoke(&s.node, &s.front, "/plain/x/hit?token=secret").await;
    assert_eq!((r.s(), r.text.as_str()), (400, "not_durable"));
    let objects = get(&s.node, &s.t.key, &format!("/v1/services/{}/objects", s.rooms_id)).await;
    let names: Vec<Value> = objects
        .items()
        .iter()
        .map(|o| json!([o["name"], o["storage"]]))
        .collect();
    assert_eq!(
        names,
        vec![json!(["a", true]), json!(["b", true]), json!(["default", true])],
        "{}",
        objects.text
    );
    s.node.shutdown().await;
}

/// AC-OBJ-02: a front service authenticates and forwards the upgrade; the socket belongs to the
/// object it named, and its events run in that object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_front_forward() {
    let s = setup(true).await;
    assert_eq!(
        ws_connect_with(&s.node, &s.front, "/room/lobby/?token=nope", &[])
            .await
            .err(),
        Some(401)
    );
    let mut a = ws_connect(&s.node, &s.front, "/room/lobby/?token=secret&user=ann").await;
    let mut b = ws_connect(&s.node, &s.front, "/room/lobby/?token=secret&user=bob").await;
    let mut c = ws_connect(&s.node, &s.front, "/room/attic/?token=secret&user=cid&hello=1").await;
    assert_eq!(
        ws_text(&mut c).await,
        "hello attic",
        "a send at accept reaches the socket once it connects"
    );
    ws_send(&mut a, "who").await;
    assert_eq!(ws_text(&mut a).await, "lobby");
    ws_send(&mut c, "who").await;
    assert_eq!(ws_text(&mut c).await, "attic");
    ws_send(&mut b, "hello").await;
    assert_eq!(ws_text(&mut a).await, "lobby:hello", "broadcast stays in the object");
    assert_eq!(ws_text(&mut b).await, "lobby:hello");
    let sockets = get(
        &s.node,
        &s.t.key,
        &format!("/v1/services/{}/objects/lobby/sockets", s.rooms_id),
    )
    .await;
    let mut tags: Vec<Value> = sockets.items().iter().map(|i| i["tags"][0].clone()).collect();
    tags.sort_by_key(|t| t.to_string());
    assert_eq!(tags, vec![json!("ann"), json!("bob")], "{}", sockets.text);
    s.node.shutdown().await;
}

/// AC-OBJ-03: a handler that writes and then throws leaves no write behind and its message never
/// reaches the client (output gate); after its last attempt the socket closes 1011.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_atomic_event() {
    let s = setup(true).await;
    let mut ws = ws_connect(&s.node, &s.front, "/room/atom/?token=secret").await;
    ws_send(&mut ws, "write-then-throw").await;
    match ws_next(&mut ws, Duration::from_secs(20)).await {
        Some(WsMsg::Close(Some(f))) => assert_eq!(u16::from(f.code), 1011),
        other => panic!("expected close 1011 (and never `leaked`), got {other:?}"),
    }
    assert_eq!(
        state(&s, "atom").await["x"],
        Value::Null,
        "the failed handler's write was not committed"
    );
    s.node.shutdown().await;
}

/// AC-OBJ-04: two objects make progress at once; one object runs one request at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_parallel_objects_serial_object() {
    let s = setup(true).await;
    let sleep = |room: &'static str| {
        let (node, front) = (&s.node, s.front.clone());
        async move {
            invoke(node, &front, &format!("/room/{room}/sleep?token=secret"))
                .await
                .text
        }
    };
    hit(&s, "p0").await; // the first request builds the service
    let start = Instant::now();
    let (x, y) = tokio::join!(sleep("p1"), sleep("p2"));
    assert_eq!((x.as_str(), y.as_str()), ("slept", "slept"));
    let parallel = start.elapsed();
    let start = Instant::now();
    let (x, y) = tokio::join!(sleep("p3"), sleep("p3"));
    assert_eq!((x.as_str(), y.as_str()), ("slept", "slept"));
    let serial = start.elapsed();
    assert!(
        serial >= Duration::from_millis(1_000),
        "one object: one request at a time ({serial:?})"
    );
    assert!(
        parallel < Duration::from_millis(950),
        "two objects: at once ({parallel:?})"
    );
    s.node.shutdown().await;
}

/// AC-OBJ-05: an alarm fires once, as an event of its object (its sockets see the broadcast);
/// a failed `alarm()` is delivered again with `retryCount` 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_alarm() {
    let s = setup(true).await;
    let mut ws = ws_connect(&s.node, &s.front, "/room/clock/?token=secret").await;
    assert_eq!(
        invoke(&s.node, &s.front, "/room/clock/alarm?ms=300&token=secret")
            .await
            .text,
        "set"
    );
    assert!(
        state(&s, "clock").await["alarm"].as_f64().is_some(),
        "the alarm is stored"
    );
    assert_eq!(ws_text(&mut ws).await, "alarm:0");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let st = state(&s, "clock").await;
    assert_eq!(
        (&st["fired"], &st["alarm"]),
        (&json!(1), &Value::Null),
        "fired once, then gone: {st}"
    );
    // A failing alarm handler is retried.
    assert_eq!(
        invoke(&s.node, &s.front, "/room/clock/fail-first?token=secret")
            .await
            .text,
        "ok"
    );
    assert_eq!(
        invoke(&s.node, &s.front, "/room/clock/alarm?ms=100&token=secret")
            .await
            .text,
        "set"
    );
    assert_eq!(ws_text(&mut ws).await, "alarm:1", "the second delivery");
    assert_eq!(
        state(&s, "clock").await["fired"],
        json!(2),
        "the failed attempt wrote nothing"
    );
    s.node.shutdown().await;
}

/// AC-OBJ-06: a pending alarm survives a node restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_alarm_survives_restart() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let rooms = uniq("rooms");
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": rooms, "runtime": "deno", "durable": true}),
    )
    .await;
    let id = r.json["id"].as_str().expect("id").to_string();
    let r = upload_ts(&node, &t.key, &id, ROOMS, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    assert_eq!(invoke(&node, &rooms, "/alarm?ms=1500").await.text, "set");
    node.restart().await;
    let (n, sl) = (&node, rooms.clone());
    eventually(Duration::from_secs(15), "the alarm fires after the restart", || {
        let sl = sl.clone();
        async move { invoke(n, &sl, "/state").await.json["fired"] == json!(1) }
    })
    .await;
    node.stop().await;
}

/// AC-OBJ-07: with warm objects (default) an object's instance survives between events (while
/// the next event lands on the isolate that ran the previous one: an optimization, never a
/// guarantee); with `ws.warm = false` the constructor runs for every event (state lives in
/// storage only).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_hibernate_wake() {
    for warm in [true, false] {
        let s = setup(warm).await;
        let mut ws = ws_connect(&s.node, &s.front, "/room/w/?token=secret").await;
        let mut seen = Vec::new();
        for _ in 0..10 {
            ws_send(&mut ws, "count").await;
            seen.push(ws_text(&mut ws).await.parse::<u32>().expect("a count"));
        }
        if warm {
            assert!(
                seen.iter().any(|n| *n > 1),
                "a warm instance kept its state at least once: {seen:?}"
            );
        } else {
            assert!(seen.iter().all(|n| *n == 1), "a fresh instance per event: {seen:?}");
        }
        s.node.shutdown().await;
    }
}

/// AC-OBJ-08: the control API drops an object's storage (`data:admin`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_obj_api_delete_storage() {
    let s = setup(true).await;
    hit(&s, "gone").await;
    assert_eq!(hit(&s, "gone").await["hits"], json!(2));
    let (_, reader) = make_key(&s.node, &s.t.id, &["services:read"], None).await;
    let r = delete(&s.node, &reader, &format!("/v1/services/{}/objects/gone", s.rooms_id)).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    let r = delete(&s.node, &s.t.key, &format!("/v1/services/{}/objects/gone", s.rooms_id)).await;
    assert_eq!(r.s(), 204, "{}", r.text);
    assert_eq!(hit(&s, "gone").await["hits"], json!(1), "storage dropped");
    s.node.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// Phase 4: the same contract for a WASM component (fixture `ws`, world `socket-service`)
// ---------------------------------------------------------------------------------------------

/// A durable WASM service from `fixtures/wasm/out/<fixture>.wasm`: the upload's answer.
async fn ship_durable_wasm(node: &impl Node, key: &str, slug: &str, fixture: &str) -> (String, Resp) {
    let r = post(
        node,
        key,
        "/v1/services",
        json!({"slug": slug, "runtime": "wasm", "durable": true}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let r = upload(node, key, &id, "application/wasm", wasm_fixture(fixture), "activate=1").await;
    (id, r)
}

/// AC-WSM-WS-01: echo, attachment, serial order, close, a failing event (nothing written, nothing
/// sent, 1011), storage, alarms, all through the WIT interfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_wasm_sockets_and_durable_storage() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("wsm");
    let (_id, r) = ship_durable_wasm(&node, &t.key, &slug, "ws").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    assert_eq!(invoke(&node, &slug, "/accept").await.text, "not_upgradable");

    let mut a = ws_connect(&node, &slug, "/?user=ann").await;
    let mut b = ws_connect(&node, &slug, "/?user=bob").await;
    ws_send(&mut a, "who").await;
    assert_eq!(ws_text(&mut a).await, "default");
    ws_send(&mut b, "att").await;
    assert_eq!(ws_text(&mut b).await, "user:bob");
    for i in 0..30 {
        ws_send(&mut a, &format!("m{i}")).await;
    }
    for i in 0..30 {
        assert_eq!(ws_text(&mut a).await, format!("m{i}"), "in order");
    }
    a.send(WsMsg::binary(vec![9u8, 8])).await.expect("binary");
    match ws_next(&mut a, Duration::from_secs(10)).await {
        Some(WsMsg::Binary(d)) => assert_eq!(d.as_ref(), &[9u8, 8]),
        other => panic!("binary echo: {other:?}"),
    }
    assert_eq!(invoke(&node, &slug, "/hit").await.text, "hits=1");
    assert_eq!(invoke(&node, &slug, "/hit").await.text, "hits=2");

    ws_send(&mut b, "boom").await;
    match ws_next(&mut b, Duration::from_secs(20)).await {
        Some(WsMsg::Close(Some(f))) => assert_eq!(u16::from(f.code), 1011),
        other => panic!("expected close 1011 (never `leaked`), got {other:?}"),
    }
    assert!(
        invoke(&node, &slug, "/state").await.text.ends_with("x=-"),
        "the failed event wrote nothing"
    );

    assert_eq!(invoke(&node, &slug, "/alarm?ms=200").await.text, "set");
    assert_eq!(ws_text(&mut a).await, "alarm:0");
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(10), "the alarm counted", || {
        let sl = sl.clone();
        async move { invoke(n, &sl, "/state").await.text.contains("fired=1") }
    })
    .await;
    node.shutdown().await;
}

/// AC-WSM-WS-02: a durable WASM service must export `socket-handler` (`422 missing_export`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_wasm_durable_without_the_export_is_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_id, r) = ship_durable_wasm(&node, &t.key, &uniq("wsmx"), "hello").await;
    assert_eq!((r.s(), r.code()), (422, "missing_export".to_string()), "{}", r.text);
    node.shutdown().await;
}

/// The same check when `durable` is switched on later (PATCH) or a durable service's live version
/// is moved to one without the export (rollback).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac_wasm_durable_patch_and_rollback_check_the_export() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let plain = ship_wasm(&node, &t.key, &uniq("wsmp"), "hello").await;
    let r = patch(
        &node,
        &t.key,
        &format!("/v1/services/{plain}"),
        json!({"durable": true}),
    )
    .await;
    assert_eq!((r.s(), r.code()), (422, "missing_export".to_string()), "{}", r.text);
    assert_eq!(
        get(&node, &t.key, &format!("/v1/services/{plain}")).await.json["durable"],
        json!(false),
        "unchanged"
    );

    // A non-durable service with a version lacking the export, then one with it, then durable.
    let id = ship_wasm(&node, &t.key, &uniq("wsmr"), "hello").await;
    let r = upload(&node, &t.key, &id, "application/wasm", wasm_fixture("ws"), "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"durable": true})).await;
    assert_eq!(r.s(), 200, "the live version exports socket-handler: {}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{id}/live"), json!({"version": 1})).await;
    assert_eq!((r.s(), r.code()), (422, "missing_export".to_string()), "{}", r.text);
    node.shutdown().await;
}

/// A queue (or cron / interval) task of a durable class acts for no object: its `state.storage`
/// throws `no_object`, and it reaches an object (here the default one) through `durable.get`.
const QUEUED: &str = r#"
import { durable } from "std:durable";
import { kv } from "std:kv";
export default class Q {
  state: any;
  constructor(state: any) { this.state = state; }
  async queue(_batch: any): Promise<void> {
    let seen = "ok";
    try { await this.state.storage.get("k"); } catch (e: any) { seen = e.code ?? String(e); }
    await kv.set("seen", seen);
    const r = await durable.get("default").fetch(new Request("http://x/hit"));
    await kv.set("via", await r.text());
  }
  async fetch(req: Request): Promise<Response> {
    if (new URL(req.url).pathname.endsWith("/hit")) { await this.state.storage.put("h", 1); return new Response("hit:" + this.state.id.name); }
    return Response.json({ seen: (await kv.get("seen")) ?? null, via: (await kv.get("via")) ?? null, h: (await this.state.storage.get("h")) ?? 0 });
  }
}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_task_of_a_durable_class_acts_for_no_object() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("dq");
    let r = post(
        &node,
        &t.key,
        "/v1/services",
        json!({"slug": slug, "runtime": "deno", "durable": true}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let r = upload_ts(&node, &t.key, &id, QUEUED, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    let r = post(
        &node,
        &t.key,
        &format!("/v1/services/{id}/queue"),
        json!({"body": {"n": 1}}),
    )
    .await;
    assert!(r.s() < 300, "{}", r.text);
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(15), "the queue task ran", || {
        let sl = sl.clone();
        async move { invoke(n, &sl, "/").await.json["via"] != json!(null) }
    })
    .await;
    let v = invoke(&node, &slug, "/").await.json;
    assert_eq!(v, json!({"seen": "no_object", "via": "hit:default", "h": 1}), "{v}");
    node.shutdown().await;
}
