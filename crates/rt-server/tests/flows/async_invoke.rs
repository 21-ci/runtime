//! Async invocation through the real node (Q§6.6, plan-final §2.3): `Prefer: respond-async` /
//! `X-Rt-Invocation-Type: Event` on the invoke listener → `202` + a sealed record in `queue.db` →
//! the async engine runs the same `fetch` handler later → the status decides the outcome, visible
//! in `GET /v1/services/{s}/invocations`. Async is opt-in (`async.enabled = true`).

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

/// Records every run (after the optional `?sleep=<ms>`): path, the runtime invocation headers,
/// `ctx.invocation`, the body. `?status=<n>` answers with that status. `GET /runs` lists them.
const RECORDER: &str = r#"
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT, inv TEXT, attempt TEXT, ctx_id TEXT, ctx_attempt INTEGER, idem TEXT, body TEXT, at INTEGER)");
}
export default async function (req: Request, ctx: any): Promise<Response> {
  await ensure();
  const u = new URL(req.url);
  if (u.pathname === "/runs") {
    const r = await sql.execute("SELECT path, inv, attempt, ctx_id, ctx_attempt, idem, body, at FROM runs ORDER BY id");
    return Response.json({ runs: r.rows.map((x: any) => ({ path: x[0], inv: x[1], attempt: x[2], ctx_id: x[3], ctx_attempt: x[4], idem: x[5], body: x[6], at: x[7] })) });
  }
  const ms = Number(u.searchParams.get("sleep") ?? "0");
  if (ms > 0) await new Promise((r) => setTimeout(r, ms));
  const body = await req.text();
  await sql.execute("INSERT INTO runs (path, inv, attempt, ctx_id, ctx_attempt, idem, body, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
    u.pathname + u.search, req.headers.get("x-rt-invocation-id"), req.headers.get("x-rt-invocation-attempt"),
    ctx.invocation?.id ?? null, ctx.invocation?.attempt ?? null, req.headers.get("idempotency-key"), body, Date.now(),
  ]);
  return new Response("done", { status: Number(u.searchParams.get("status") ?? "200") });
}"#;

/// A durable object that only answers.
const OBJECT: &str = r#"
export default class Obj {
  constructor(_state: any, _env: any) {}
  async fetch(_req: Request): Promise<Response> { return new Response("obj"); }
}"#;

/// A service created with `extra` fields (e.g. `scaling`), `src` uploaded and activated.
async fn ship_with(node: &impl Node, key: &str, slug: &str, extra: Value, src: &str) -> String {
    let mut body = json!({"slug": slug, "runtime": "deno"});
    if let (Some(b), Some(e)) = (body.as_object_mut(), extra.as_object()) {
        b.extend(e.clone());
    }
    let r = post(node, key, "/v1/services", body).await;
    assert_eq!(r.s(), 201, "create `{slug}`: {}", r.text);
    let id = r.json["id"].as_str().expect("service id").to_string();
    let u = upload_ts(node, key, &id, src, "activate=1").await;
    assert!(u.s() == 201 || u.s() == 202, "upload `{slug}`: {} {}", u.s(), u.text);
    id
}

/// `PATCH async = {enabled: true, ..extra}`.
async fn enable_async(node: &impl Node, key: &str, svc: &str, extra: Value) {
    let mut block = json!({"enabled": true});
    if let (Some(b), Some(e)) = (block.as_object_mut(), extra.as_object()) {
        b.extend(e.clone());
    }
    let r = patch(node, key, &format!("/v1/services/{svc}"), json!({"async": block})).await;
    assert_eq!(r.s(), 200, "enable async: {}", r.text);
}

async fn runs(node: &impl Node, slug: &str) -> Vec<Value> {
    invoke(node, slug, "/runs").await.json["runs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn invocations(node: &impl Node, key: &str, svc: &str) -> Resp {
    let r = get(node, key, &format!("/v1/services/{svc}/invocations")).await;
    assert_eq!(r.s(), 200, "list invocations: {}", r.text);
    r
}

async fn invocation(node: &impl Node, key: &str, svc: &str, id: &str) -> Value {
    let r = get(node, key, &format!("/v1/services/{svc}/invocations/{id}")).await;
    assert_eq!(r.s(), 200, "get invocation: {}", r.text);
    r.json
}

async fn wait_state(node: &impl Node, key: &str, svc: &str, id: &str, state: &str, timeout: Duration) -> Value {
    let what = format!("invocation {id} to be {state}");
    eventually(timeout, &what, || async move {
        invocation(node, key, svc, id).await["state"] == json!(state)
    })
    .await;
    invocation(node, key, svc, id).await
}

fn event(path_headers: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut h = vec![("x-rt-invocation-type", "Event")];
    h.extend_from_slice(path_headers);
    h
}

node_test! {
/// AC-ASYNC-E2E-01: `Prefer: respond-async` on a service with async on answers `202 {id, state:
/// pending}` with `x-rt-invocation-id` and `preference-applied`; the handler runs later with the
/// same method, path, query and body, the runtime invocation headers and `ctx.invocation`; the
/// record ends `succeeded` (no headers or body in the view).
async fn ac_async_e2e_01_prefer_respond_async_returns_202_and_runs() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("async");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;

    let r = invoke_with(&node, &slug, Method::POST, "/hook?x=1", &[("prefer", "respond-async")], "payload").await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    assert!(id.starts_with("inv_"), "{id}");
    assert_eq!(r.json["state"], json!("pending"));
    assert_eq!(r.header("x-rt-invocation-id").as_deref(), Some(id.as_str()));
    assert_eq!(r.header("preference-applied").as_deref(), Some("respond-async"));

    let v = wait_state(&node, &t.key, &svc, &id, "succeeded", Duration::from_secs(20)).await;
    assert_eq!((v["attempts"].clone(), v["status"].clone(), v["condition"].clone()), (json!(1), json!(200), json!("success")), "{v}");
    assert_eq!((v["method"].clone(), v["path"].clone(), v["has_query"].clone()), (json!("POST"), json!("/hook"), json!(true)), "{v}");
    assert_eq!(v["body_bytes"], json!(7));
    assert!(v.get("headers").is_none() && v.get("body").is_none(), "no headers or body in the view: {v}");

    let got = runs(&node, &slug).await;
    assert_eq!(got.len(), 1, "{got:?}");
    let run = &got[0];
    assert_eq!(run["path"], json!("/hook?x=1"));
    assert_eq!(run["body"], json!("payload"));
    assert_eq!((run["inv"].clone(), run["attempt"].clone()), (json!(id), json!("1")), "{run}");
    assert_eq!((run["ctx_id"].clone(), run["ctx_attempt"].clone()), (json!(id), json!(1)), "{run}");
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-02: a handler answering 5xx is retried (30 s backoff) until `async.max_attempts`,
/// then the record is `failed` / `retries_exhausted` / `status_5xx`; redrive puts it back to
/// pending with attempts reset and it runs again.
async fn ac_async_e2e_02_5xx_retries_then_failed_and_redrive_runs_again() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("async5xx");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({"max_attempts": 2})).await;

    let r = invoke_with(&node, &slug, Method::POST, "/fail?status=503", &event(&[]), "").await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let v = wait_state(&node, &t.key, &svc, &id, "failed", Duration::from_secs(60)).await;
    assert_eq!(v["attempts"], json!(2), "{v}");
    assert_eq!((v["condition"].clone(), v["error"].clone(), v["status"].clone()), (json!("retries_exhausted"), json!("status_5xx"), json!(503)), "{v}");
    let got = runs(&node, &slug).await;
    assert_eq!(got.iter().map(|r| r["attempt"].clone()).collect::<Vec<_>>(), vec![json!("1"), json!("2")], "{got:?}");
    let gap = got[1]["at"].as_f64().unwrap_or(0.0) - got[0]["at"].as_f64().unwrap_or(0.0);
    assert!((20_000.0..=45_000.0).contains(&gap), "≈ 30 s backoff, got {gap} ms");
    let list = invocations(&node, &t.key, &svc).await;
    assert_eq!(list.json["counts"]["failed"], json!(1), "{}", list.text);

    let rd = call(api(&node, &t.key, Method::POST, &format!("/v1/services/{svc}/invocations/redrive")), None).await;
    assert_eq!(rd.s(), 200, "{}", rd.text);
    assert_eq!(rd.json["redriven"], json!(1));
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(15), "the redriven run", || {
        let sl = sl.clone();
        async move { runs(n, &sl).await.len() >= 3 }
    })
    .await;
    assert_eq!(runs(&node, &slug).await[2]["attempt"], json!("1"), "a redriven invocation restarts at attempt 1");
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-03: deliveries the scheduler refuses (`max_instances: 1`, one queued, a slow
/// handler) are throttled, not attempts: with `max_attempts: 1` every invocation still succeeds
/// after exactly one run.
async fn ac_async_e2e_03_overload_is_not_an_attempt() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncbusy");
    let svc = ship_with(&node, &t.key, &slug, json!({"scaling": {"max_instances": 1, "max_queued": 1}}), RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({"max_attempts": 1})).await;
    assert_eq!(invoke(&node, &slug, "/runs").await.s(), 200); // build

    let mut ids = Vec::new();
    for i in 0..4 {
        let r = invoke_with(&node, &slug, Method::POST, &format!("/slow?sleep=700&n={i}"), &event(&[]), "").await;
        assert_eq!(r.s(), 202, "{}", r.text);
        ids.push(r.json["id"].as_str().expect("id").to_string());
    }
    for id in &ids {
        let v = wait_state(&node, &t.key, &svc, id, "succeeded", Duration::from_secs(40)).await;
        assert_eq!(v["attempts"], json!(1), "a refused delivery is not an attempt: {v}");
    }
    let got = runs(&node, &slug).await;
    assert_eq!(got.len(), 4, "each invocation ran once: {got:?}");
    assert!(got.iter().all(|r| r["attempt"] == json!("1")), "{got:?}");
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-04: an accepted invocation survives a restart (persisted before the `202`): a
/// slow one interrupted by the shutdown, or one not yet started, runs after the node is back.
async fn ac_async_e2e_04_pending_survives_restart() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncrst");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    assert_eq!(invoke(&node, &slug, "/runs").await.s(), 200);

    let r = invoke_with(&node, &slug, Method::POST, "/late?sleep=1500", &event(&[]), "keep").await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.restart().await;

    let v = wait_state(&node, &t.key, &svc, &id, "succeeded", Duration::from_secs(60)).await;
    assert_eq!(v["condition"], json!("success"), "{v}");
    let got = runs(&node, &slug).await;
    assert!(got.iter().any(|r| r["inv"] == json!(id) && r["body"] == json!("keep")), "{got:?}");
    node.stop().await;
}
}

node_test! {
/// AC-ASYNC-E2E-05: the same `Idempotency-Key` for the same request returns the same `202` and id
/// with `x-rt-idempotent-replay: true`; nothing more is enqueued (one run). The key also reaches
/// the guest unchanged.
async fn ac_async_e2e_05_idempotency_key_returns_the_same_id() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncidem");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;

    let h = event(&[("idempotency-key", "order-42")]);
    let a = invoke_with(&node, &slug, Method::POST, "/pay", &h, "{\"amount\":1}").await;
    assert_eq!(a.s(), 202, "{}", a.text);
    assert_eq!(a.header("x-rt-idempotent-replay"), None);
    let id = a.json["id"].as_str().expect("id").to_string();
    let b = invoke_with(&node, &slug, Method::POST, "/pay", &h, "{\"amount\":1}").await;
    assert_eq!(b.s(), 202, "{}", b.text);
    assert_eq!(b.json["id"], json!(id));
    assert_eq!(b.header("x-rt-idempotent-replay").as_deref(), Some("true"));

    wait_state(&node, &t.key, &svc, &id, "succeeded", Duration::from_secs(20)).await;
    let c = invoke_with(&node, &slug, Method::POST, "/pay", &h, "{\"amount\":1}").await;
    assert_eq!((c.s(), c.json["id"].clone(), c.json["state"].clone()), (202, json!(id), json!("succeeded")), "{}", c.text);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let got = runs(&node, &slug).await;
    assert_eq!(got.len(), 1, "one run for one key: {got:?}");
    assert_eq!(got[0]["idem"], json!("order-42"), "the key is forwarded to the guest");
    assert_eq!(invocations(&node, &t.key, &svc).await.items().len(), 1);
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-06: with async off (the default) `Prefer: respond-async` runs synchronously and
/// `X-Rt-Invocation-Type: Event` is `403 async_not_allowed`; a durable service refuses `Event`
/// the same way even with async on.
async fn ac_async_e2e_06_disabled_or_durable_runs_sync_for_prefer_and_403_for_event_type() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncoff");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;

    let r = invoke_with(&node, &slug, Method::POST, "/sync", &[("prefer", "respond-async")], "").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "done"));
    assert_eq!(r.header("preference-applied"), None);
    let r = invoke_with(&node, &slug, Method::POST, "/sync", &event(&[]), "").await;
    assert_eq!((r.s(), reason(&r)), (403, "async_not_allowed".to_string()), "{}", r.text);
    // Async off: an unknown invocation type is ignored, as before async existed (MINOR 7).
    let r = invoke_with(&node, &slug, Method::POST, "/sync", &[("x-rt-invocation-type", "DryRun")], "").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "done"), "{}", r.text);
    let r = invoke_with(&node, &slug, Method::POST, "/sync", &[("x-rt-invocation-type", "RequestResponse")], "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(invocations(&node, &t.key, &svc).await.items().len(), 0);

    let obj = uniq("asyncobj");
    let id = ship_with(&node, &t.key, &obj, json!({"durable": true}), OBJECT).await;
    let p = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"async": {"enabled": true}})).await;
    if p.s() == 200 {
        let r = invoke_with(&node, &obj, Method::POST, "/a/x", &event(&[]), "").await;
        assert_eq!((r.s(), reason(&r)), (403, "async_not_allowed".to_string()), "{}", r.text);
        let r = invoke_with(&node, &obj, Method::POST, "/a/x", &[("prefer", "respond-async")], "").await;
        assert_ne!(r.s(), 202, "a durable service never answers 202: {}", r.text);
        assert_eq!(invocations(&node, &t.key, &id).await.items().len(), 0);
    } else {
        assert_eq!(p.s(), 422, "a durable service refuses async at config time: {}", p.text);
    }
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-07: a body over 1 MiB with `X-Rt-Invocation-Type: Event` is `413 too_large`
/// (with `Prefer` it would run synchronously); nothing is stored.
async fn ac_async_e2e_07_body_over_1mib_is_413_for_event_type() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncbig");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    // Raw request: the node answers from the declared length without reading the body (a client
    // still writing a 1 MiB body may see the connection close instead of the answer).
    let req = format!(
        "POST /s/{slug}/big HTTP/1.1\r\nhost: localhost\r\nx-rt-invocation-type: Event\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        1024 * 1024 + 1
    );
    let r = raw_tcp(node.addrs.public, req.as_bytes(), Duration::from_secs(10)).await.expect("response");
    assert_eq!((r.status, r.header("x-rt-reason")), (413, Some("too_large")), "{}", r.text());
    let ok = invoke_with(&node, &slug, Method::POST, "/small", &event(&[]), "x").await;
    assert_eq!(ok.s(), 202, "{}", ok.text);
    assert_eq!(invocations(&node, &t.key, &svc).await.items().len(), 1, "only the small one is stored");
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-08: client-sent `x-rt-invocation-id` / `x-rt-invocation-attempt` are stripped; the
/// guest sees the runtime's values.
async fn ac_async_e2e_08_client_x_rt_invocation_headers_are_stripped() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asynchdr");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    let h = event(&[("x-rt-invocation-id", "inv_forged"), ("x-rt-invocation-attempt", "99")]);
    let r = invoke_with(&node, &slug, Method::POST, "/h", &h, "").await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    wait_state(&node, &t.key, &svc, &id, "succeeded", Duration::from_secs(20)).await;
    let got = runs(&node, &slug).await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!((got[0]["inv"].clone(), got[0]["attempt"].clone()), (json!(id), json!("1")), "{got:?}");

    // A synchronous request never sees forged values either.
    let s = invoke_with(&node, &slug, Method::POST, "/s", &[("x-rt-invocation-id", "inv_forged")], "").await;
    assert_eq!(s.s(), 200);
    let got = runs(&node, &slug).await;
    assert_eq!((got[1]["inv"].clone(), got[1]["ctx_id"].clone()), (Value::Null, Value::Null), "{got:?}");
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-09: at `plan.max_async_pending` pending invocations the next accept is
/// `429 async_backlog_full` with `Retry-After: 1`.
async fn ac_async_e2e_09_backlog_full_is_429() {
    let node = start_node_with(|c| {
        if let Some(p) = c.plans.get_mut("pro") {
            p.max_async_pending = 1;
        }
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncfull");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    let a = invoke_with(&node, &slug, Method::POST, "/slow?sleep=3000", &event(&[]), "").await;
    assert_eq!(a.s(), 202, "{}", a.text);
    let b = invoke_with(&node, &slug, Method::POST, "/slow?sleep=3000", &event(&[]), "").await;
    assert_eq!((b.s(), reason(&b)), (429, "async_backlog_full".to_string()), "{}", b.text);
    assert_eq!(b.header("retry-after").as_deref(), Some("1"));
    node.shutdown().await;
}
}

node_test! {
/// MINOR 7: with async on, an unknown `X-Rt-Invocation-Type` is `400 bad_request` and runs nothing.
async fn minor7_unknown_invocation_type_is_400_only_with_async_on() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncdry");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    let r = invoke_with(&node, &slug, Method::POST, "/x", &[("x-rt-invocation-type", "DryRun")], "").await;
    assert_eq!((r.s(), reason(&r)), (400, "bad_request".to_string()), "{}", r.text);
    assert!(runs(&node, &slug).await.is_empty());
    assert_eq!(invocations(&node, &t.key, &svc).await.items().len(), 0);
    node.shutdown().await;
}
}

node_test! {
/// B1: the tenant's pending + failed async bytes are capped by plan `storage_mb`, across its
/// services: past it the accept is `429 async_backlog_full` and nothing new is stored.
async fn b1_async_bytes_over_storage_mb_are_429_across_services() {
    let node = start_node_with(|c| {
        if let Some(p) = c.plans.get_mut("pro") {
            p.storage_mb = 1;
        }
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let a_slug = uniq("asyncbytesa");
    let a = ship(&node, &t.key, &a_slug, RECORDER).await;
    enable_async(&node, &t.key, &a, json!({})).await;
    let b_slug = uniq("asyncbytesb");
    let b = ship(&node, &t.key, &b_slug, RECORDER).await;
    enable_async(&node, &t.key, &b, json!({})).await;
    let body = "x".repeat(700 * 1024);
    let first = invoke_with(&node, &a_slug, Method::POST, "/slow?sleep=5000", &event(&[]), &body).await;
    assert_eq!(first.s(), 202, "{}", first.text);
    let second = invoke_with(&node, &b_slug, Method::POST, "/slow?sleep=5000", &event(&[]), &body).await;
    assert_eq!((second.s(), reason(&second)), (429, "async_backlog_full".to_string()), "{}", second.text);
    assert_eq!(invocations(&node, &t.key, &b).await.items().len(), 0, "nothing stored");
    let small = invoke_with(&node, &b_slug, Method::POST, "/small", &event(&[]), "x").await;
    assert_eq!(small.s(), 202, "a small one still fits: {}", small.text);
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-10: `Prefer: respond-async` on a service with async off runs synchronously and
/// stores nothing (no record, zero counts).
async fn ac_async_e2e_10_prefer_on_a_service_with_async_off_runs_sync_and_stores_nothing() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncnone");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    let r = invoke_with(&node, &slug, Method::POST, "/now", &[("prefer", "respond-async")], "b").await;
    assert_eq!((r.s(), r.text.as_str()), (200, "done"));
    assert_eq!(runs(&node, &slug).await.len(), 1, "it ran right away");
    let list = invocations(&node, &t.key, &svc).await;
    assert!(list.items().is_empty(), "{}", list.text);
    assert_eq!(list.json["counts"], json!({"pending": 0, "failed": 0}), "{}", list.text);
    node.shutdown().await;
}
}

node_test! {
/// AC-ASYNC-E2E-11: an `Idempotency-Key` reused for a different request (other body) is
/// `409 idempotency_mismatch`; the original stays the only record.
async fn ac_async_e2e_11_idempotency_key_reused_for_another_request_is_409() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("asyncmis");
    let svc = ship(&node, &t.key, &slug, RECORDER).await;
    enable_async(&node, &t.key, &svc, json!({})).await;
    let h = event(&[("idempotency-key", "k-1")]);
    let a = invoke_with(&node, &slug, Method::POST, "/pay", &h, "one").await;
    assert_eq!(a.s(), 202, "{}", a.text);
    let b = invoke_with(&node, &slug, Method::POST, "/pay", &h, "two").await;
    assert_eq!((b.s(), reason(&b)), (409, "idempotency_mismatch".to_string()), "{}", b.text);
    let c = invoke_with(&node, &slug, Method::POST, "/other", &h, "one").await;
    assert_eq!(c.s(), 409, "another path is another request: {}", c.text);
    assert_eq!(invocations(&node, &t.key, &svc).await.items().len(), 1);
    node.shutdown().await;
}
}

node_test! {
/// QENG-E2E-01: a queue message whose delivery the scheduler refuses (the service is busy at
/// `max_instances: 1`, one queued) is throttled, not dead-lettered, even with
/// `queue.max_attempts = 1`; it is delivered once the service is free.
async fn qeng_e2e_01_queue_message_under_overload_is_not_dead_lettered() {
    let node = start_node_with(|c| c.queue.max_attempts = 1).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("qbusy");
    let src = guests::QUEUE_RECORDER.replace(
        "async fetch(_req: Request): Promise<Response> {",
        "async fetch(req: Request): Promise<Response> {\n    if (new URL(req.url).pathname === \"/slow\") { await new Promise((r) => setTimeout(r, 1500)); return new Response(\"slow\"); }",
    );
    let svc = ship_with(&node, &t.key, &slug, json!({"scaling": {"max_instances": 1, "max_queued": 1}}), &src).await;
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let url = node.public_url(&format!("/s/{slug}/slow"));
    let busy: Vec<_> = (0..2).map(|_| tokio::spawn(fetch(node.http().clone(), url.clone()))).collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let m = post(&node, &t.key, &format!("/v1/services/{svc}/queue"), json!({"body": {"job": 1}})).await;
    assert_eq!(m.s(), 202, "{}", m.text);
    for b in busy {
        b.await.expect("join");
    }
    let (n, sl) = (&node, slug.clone());
    eventually(Duration::from_secs(30), "the queue message to be delivered", || {
        let sl = sl.clone();
        async move { invoke(n, &sl, "/").await.json["got"].as_array().is_some_and(|g| !g.is_empty()) }
    })
    .await;
    let stats = get(&node, &t.key, &format!("/v1/services/{svc}/queue")).await;
    assert_eq!(stats.json["dlq"], json!(0), "{}", stats.text);
    let got = invoke(&node, &slug, "/").await.json["got"].clone();
    assert_eq!(got[0]["attempt"], json!(1), "a throttle is not an attempt: {got}");
    node.shutdown().await;
}
}

/// `x-rt-reason` of an invoke-path error.
fn reason(r: &Resp) -> String {
    r.header("x-rt-reason").unwrap_or_default()
}
