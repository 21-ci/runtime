//! `std:pubsub` through the real node (Q§6.6, plan-final §2.4): subscriptions declared in
//! `events.subscribe`, publish from a guest or `POST /v1/pubsub/publish`, fan-out and queue
//! groups, request/reply, namespace (tenant / project / environment) isolation, WASM components.

use std::time::Duration;

use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

/// Subscriber and publisher in one: `pubsub(msg)` records `(subject, body, replyable)` and replies
/// `{from: <slug>, subject, echo: body}`. `GET /got` lists deliveries, `GET /publish?s=&b=`
/// publishes `{tag: b}`, `GET /request?s=&t=` requests and reports `{reply}` or `{error}`.
const PEER: &str = r#"
import { sql } from "std:sql";
import { publish, request } from "std:pubsub";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS got (id INTEGER PRIMARY KEY AUTOINCREMENT, subject TEXT, body TEXT, replyable INTEGER)");
}
export default {
  async fetch(req: Request, _ctx: any): Promise<Response> {
    await ensure();
    const u = new URL(req.url);
    const s = u.searchParams.get("s") ?? "x";
    if (u.pathname === "/got") {
      const r = await sql.execute("SELECT subject, body, replyable FROM got ORDER BY id");
      return Response.json({ got: r.rows.map((x: any) => ({ subject: x[0], body: JSON.parse(x[1]), replyable: !!x[2] })) });
    }
    try {
      if (u.pathname === "/publish") return Response.json(await publish(s, { tag: u.searchParams.get("b") ?? "" }));
      if (u.pathname === "/request") {
        const reply = await request(s, { tag: "req" }, { timeoutMs: Number(u.searchParams.get("t") ?? "5000") });
        return Response.json({ reply });
      }
    } catch (e) {
      return Response.json({ error: (e as any)?.code ?? String(e) });
    }
    return new Response("ok");
  },
  async pubsub(msg: any, ctx: any): Promise<unknown> {
    await ensure();
    await sql.execute("INSERT INTO got (subject, body, replyable) VALUES (?, ?, ?)", [msg.subject, JSON.stringify(msg.body), msg.replyable ? 1 : 0]);
    return { from: ctx.service.slug, subject: msg.subject, echo: msg.body };
  },
};"#;

/// `PATCH events.subscribe = subs`.
async fn subscribe(node: &impl Node, key: &str, svc: &str, subs: Value) {
    let r = patch(
        node,
        key,
        &format!("/v1/services/{svc}"),
        json!({"events": {"subscribe": subs}}),
    )
    .await;
    assert_eq!(r.s(), 200, "subscribe: {}", r.text);
}

/// A PEER service subscribed to `subs`, built.
async fn peer(node: &impl Node, key: &str, slug: &str, subs: Value) -> String {
    let svc = ship(node, key, slug, PEER).await;
    subscribe(node, key, &svc, subs).await;
    assert_eq!(invoke(node, slug, "/got").await.s(), 200, "build `{slug}`");
    svc
}

/// A PEER service in `project` / `environment`, subscribed to `subs`.
async fn peer_in(node: &impl Node, key: &str, env: &str, slug: &str, subs: Value) -> String {
    let svc = create_service_in(node, key, "default", env, slug, "deno").await;
    let r = upload_ts(node, key, &svc, PEER, "activate=1").await;
    assert!(r.s() == 201 || r.s() == 202, "upload `{slug}`: {}", r.text);
    subscribe(node, key, &svc, subs).await;
    svc
}

async fn got(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/got").await.json["got"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Deliveries to `slug` whose body tag is `tag`.
async fn got_tag(node: &impl Node, slug: &str, tag: &str) -> usize {
    got(node, slug)
        .await
        .iter()
        .filter(|g| g["body"]["tag"] == json!(tag))
        .count()
}

async fn api_publish(node: &impl Node, key: &str, body: Value) -> Resp {
    post(node, key, "/v1/pubsub/publish", body).await
}

/// Publish probes on `subject` through the control API until `want` receivers are routed (the
/// subscription index follows the route refresh).
async fn wait_receivers(node: &impl Node, key: &str, subject: &str, want: u64) {
    let what = format!("{want} receivers on `{subject}`");
    eventually(Duration::from_secs(15), &what, || async move {
        let r = api_publish(node, key, json!({"subject": subject, "body": {"tag": "probe"}})).await;
        r.s() == 200 && r.json["receivers"].as_u64() == Some(want)
    })
    .await;
}

async fn wait_count(node: &impl Node, slugs: &[&str], tag: &str, want: usize) {
    let what = format!("{want} `{tag}` deliveries to {slugs:?}");
    eventually(Duration::from_secs(15), &what, || async move {
        let mut n = 0;
        for s in slugs {
            n += got_tag(node, s, tag).await;
        }
        n >= want
    })
    .await;
}

node_test! {
/// AC-PS-E2E-01: every matching service without a group gets a copy; services sharing a group
/// split the messages (one copy per group), wildcards match, and each copy is delivered once.
async fn ac_ps_e2e_01_publish_fans_out_and_groups_share() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (a, b, c) = (uniq("psall"), uniq("psg1"), uniq("psg2"));
    peer(&node, &t.key, &a, json!([{"subject": "orders.*.created"}, {"subject": "orders.>"}])).await;
    peer(&node, &t.key, &b, json!([{"subject": "orders.eu.created", "group": "billing"}])).await;
    peer(&node, &t.key, &c, json!([{"subject": "orders.>", "group": "billing"}])).await;
    let publisher = uniq("pspub");
    ship(&node, &t.key, &publisher, PEER).await;
    wait_receivers(&node, &t.key, "orders.eu.created", 2).await;

    for _ in 0..6 {
        let r = invoke(&node, &publisher, "/publish?s=orders.eu.created&b=real").await;
        assert_eq!(r.json["receivers"], json!(2), "one ungrouped copy + one per group: {}", r.text);
        assert_eq!(r.json["dropped"], json!(0));
    }
    wait_count(&node, &[&a], "real", 6).await;
    wait_count(&node, &[&b, &c], "real", 6).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(got_tag(&node, &a, "real").await, 6, "two matching patterns still give one copy");
    let (nb, nc) = (got_tag(&node, &b, "real").await, got_tag(&node, &c, "real").await);
    assert_eq!(nb + nc, 6, "the group gets each message once");
    assert!(nb > 0 && nc > 0, "the group members share the messages: {nb} / {nc}");
    let g = got(&node, &a).await;
    let real: Vec<_> = g.iter().filter(|x| x["body"]["tag"] == json!("real")).collect();
    assert!(real.iter().all(|x| x["subject"] == json!("orders.eu.created") && x["replyable"] == json!(false)), "{real:?}");
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-02: `request` gets the responder's return value as the reply; a subject nobody
/// subscribes to fails `no_responders` at once.
async fn ac_ps_e2e_02_request_reply_round_trip_and_no_responders() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (resp, req) = (uniq("psresp"), uniq("psreq"));
    peer(&node, &t.key, &resp, json!([{"subject": "pricing.quote"}])).await;
    ship(&node, &t.key, &req, PEER).await;
    wait_receivers(&node, &t.key, "pricing.quote", 1).await;

    let r = invoke(&node, &req, "/request?s=pricing.quote").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(
        r.json["reply"],
        json!({"from": resp, "subject": "pricing.quote", "echo": {"tag": "req"}}),
        "{}",
        r.text
    );
    assert!(got(&node, &resp).await.iter().any(|g| g["replyable"] == json!(true)));

    let started = std::time::Instant::now();
    let r = invoke(&node, &req, "/request?s=nobody.home").await;
    assert_eq!(r.json["error"], json!("no_responders"), "{}", r.text);
    assert!(started.elapsed() < Duration::from_secs(3), "no_responders is immediate");
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-03: a publish reaches only its own namespace: another tenant's and another
/// environment's subscribers of the same subject get nothing.
async fn ac_ps_e2e_03_environments_and_tenants_are_isolated() {
    let node = start_node().await;
    let t1 = new_tenant(&node, "pro").await;
    let t2 = new_tenant(&node, "pro").await;
    allow_environments(&node, &t1.key, &["staging"]).await;
    let (mine, other_env, other_tenant) = (uniq("psmine"), uniq("psstg"), uniq("psother"));
    peer(&node, &t1.key, &mine, json!([{"subject": "iso.x"}])).await;
    peer_in(&node, &t1.key, "staging", &other_env, json!([{"subject": "iso.x"}])).await;
    peer(&node, &t2.key, &other_tenant, json!([{"subject": "iso.x"}])).await;
    wait_receivers(&node, &t1.key, "iso.x", 1).await;
    wait_receivers(&node, &t2.key, "iso.x", 1).await;
    let publisher = uniq("psisopub");
    ship(&node, &t1.key, &publisher, PEER).await;

    for _ in 0..3 {
        let r = invoke(&node, &publisher, "/publish?s=iso.x&b=t1").await;
        assert_eq!(r.json["receivers"], json!(1), "only the same namespace: {}", r.text);
    }
    wait_count(&node, &[&mine], "t1", 3).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let stg = got(&node, &other_env).await;
    assert!(stg.iter().all(|g| g["body"]["tag"] == json!("probe")) && got_tag(&node, &other_env, "t1").await == 0);
    assert_eq!(got_tag(&node, &other_tenant, "t1").await, 0, "another tenant never sees it");
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-04: a WASM component publishes (`std:host/pubsub`) to a Deno subscriber, and
/// answers a Deno request through its `POST /__rt/pubsub` handler.
async fn ac_ps_e2e_04_wasm_component_publishes_and_answers() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (wasm, deno) = (uniq("pswasm"), uniq("psdeno"));
    let wsvc = ship_wasm(&node, &t.key, &wasm, "events").await;
    subscribe(&node, &t.key, &wsvc, json!([{"subject": "wasm.ask"}])).await;
    peer(&node, &t.key, &deno, json!([{"subject": "deno.hear"}])).await;
    wait_receivers(&node, &t.key, "wasm.ask", 1).await;
    wait_receivers(&node, &t.key, "deno.hear", 1).await;

    let r = invoke_with(&node, &wasm, reqwest::Method::POST, "/publish?subject=deno.hear", &[], r#"{"tag":"w"}"#).await;
    assert_eq!(r.json["ok"]["receivers"], json!(1), "{}", r.text);
    wait_count(&node, &[&deno], "w", 1).await;

    let r = invoke(&node, &deno, "/request?s=wasm.ask").await;
    assert_eq!(r.json["reply"], json!({"reply": "wasm.ask"}), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-05: `POST /v1/pubsub/publish` reaches the namespace's subscribers with the JSON body
/// and answers `{receivers, dropped}`.
async fn ac_ps_e2e_05_control_api_publish_reaches_subscribers() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let sub = uniq("psapi");
    peer(&node, &t.key, &sub, json!([{"subject": "api.in"}])).await;
    wait_receivers(&node, &t.key, "api.in", 1).await;
    let r = api_publish(&node, &t.key, json!({"subject": "api.in", "body": {"tag": "api", "n": 7}, "headers": {"h": "v"}})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json, json!({"receivers": 1, "dropped": 0}));
    wait_count(&node, &[&sub], "api", 1).await;
    let g = got(&node, &sub).await;
    assert!(g.iter().any(|x| x["body"] == json!({"tag": "api", "n": 7}) && x["subject"] == json!("api.in")), "{g:?}");
    let r = api_publish(&node, &t.key, json!({"subject": "api.*", "body": 1})).await;
    assert_eq!(r.s(), 422, "a wildcard is not a publish subject: {}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-06: a service at `max_instances: 1` that requests a subject it answers itself does
/// not deadlock: the reply delivery runs in the call class beside the waiting requester.
async fn ac_ps_e2e_06_self_request_with_max_instances_1_does_not_deadlock() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("psself");
    let r = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "deno", "scaling": {"max_instances": 1}})).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let svc = r.json["id"].as_str().expect("id").to_string();
    let u = upload_ts(&node, &t.key, &svc, PEER, "activate=1").await;
    assert!(u.s() == 201 || u.s() == 202, "{}", u.text);
    subscribe(&node, &t.key, &svc, json!([{"subject": "self.ping"}])).await;
    wait_receivers(&node, &t.key, "self.ping", 1).await;

    let started = std::time::Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(15), invoke(&node, &slug, "/request?s=self.ping&t=8000"))
        .await
        .expect("the self-request answers instead of hanging");
    assert_eq!(r.json["reply"]["from"], json!(slug), "{}", r.text);
    assert!(started.elapsed() < Duration::from_secs(7), "answered well before the timeout: {:?}", started.elapsed());
    node.shutdown().await;
}
}

node_test! {
/// AC-PS-E2E-07: a control API publish names its project / environment and never crosses into
/// another one: a staging publish reaches only staging subscribers, the default reaches only
/// production; a key restricted to production may not publish to staging.
async fn ac_ps_e2e_07_control_api_publish_never_crosses_environments() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["staging"]).await;
    let (prod, stg) = (uniq("psprod"), uniq("psstage"));
    peer(&node, &t.key, &prod, json!([{"subject": "env.x"}])).await;
    peer_in(&node, &t.key, "staging", &stg, json!([{"subject": "env.x"}])).await;
    wait_receivers(&node, &t.key, "env.x", 1).await;
    let (s, k) = (&node, &t.key);
    eventually(Duration::from_secs(15), "the staging subscriber to be routed", || async move {
        let r = api_publish(s, k, json!({"subject": "env.x", "body": {"tag": "probe"}, "environment": "staging"})).await;
        r.json["receivers"] == json!(1)
    })
    .await;

    for _ in 0..3 {
        let r = api_publish(&node, &t.key, json!({"subject": "env.x", "body": {"tag": "stg"}, "environment": "staging"})).await;
        assert_eq!(r.json["receivers"], json!(1), "{}", r.text);
    }
    let r = api_publish(&node, &t.key, json!({"subject": "env.x", "body": {"tag": "prd"}})).await;
    assert_eq!(r.json["receivers"], json!(1), "{}", r.text);
    wait_count(&node, &[&stg], "stg", 3).await;
    wait_count(&node, &[&prod], "prd", 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(got_tag(&node, &prod, "stg").await, 0, "staging never reaches production");
    assert_eq!(got_tag(&node, &stg, "prd").await, 0, "production never reaches staging");

    let (_, restricted) = make_key_in(&node, &t.id, &ALL_SCOPES, None, Some(vec!["production".to_string()])).await;
    let r = api_publish(&node, &restricted, json!({"subject": "env.x", "body": {"tag": "x"}, "environment": "staging"})).await;
    assert!(r.s() == 403 || r.s() == 404, "a production-only key cannot publish to staging: {} {}", r.s(), r.text);
    assert_eq!(got_tag(&node, &stg, "x").await, 0);
    let r = api_publish(&node, &t.key, json!({"subject": "env.x", "body": 1, "environment": "nope"})).await;
    assert!(r.s() == 404 || r.s() == 422 || r.s() == 409, "an unknown environment is refused: {} {}", r.s(), r.text);
    node.shutdown().await;
}
}
