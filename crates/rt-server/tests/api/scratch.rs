//! Previews (`docs/design/platform-plan.md` R5, `platform-enhancements.md`): `POST /v1/scratch` makes
//! a service with `config.scratch.kind = "preview"`, isolated by the runtime: no calls in or out, no
//! queue, topics, pub/sub, schedules or SDKs, egress denied unless listed, its own environment label.

use std::time::Duration;

use rt_server::StartOptions;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

/// Every capability a preview must not have, one path each. Answers `{ok}` or `{code, status}`.
const PROBE: &str = r#"
import { call } from "std:services";
import { enqueue } from "std:queue";
import { read } from "std:stream";
import { publish } from "std:pubsub";
import { schedule } from "std:schedule";
import { sql } from "std:sql";
async function attempt(f: () => Promise<any>): Promise<Response> {
  try {
    const out = await f();
    return Response.json({ ok: true, status: out?.status ?? null, reason: out?.headers?.get?.("x-rt-reason") ?? null });
  } catch (e) {
    return Response.json({ ok: false, code: (e as any)?.code ?? null, message: String((e as any)?.message ?? e) });
  }
}
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const target = u.searchParams.get("target") ?? "";
    switch (u.pathname) {
      case "/call": return attempt(() => call(target, new Request("http://svc/")));
      case "/enqueue": return attempt(() => enqueue(target, "m"));
      case "/read": return attempt(() => read(target, {}));
      case "/publish": return attempt(() => publish("orders.new", "m"));
      case "/schedule": return attempt(() => schedule.after(60, "m"));
      case "/fetch": return attempt(() => fetch("http://example.com/"));
      case "/put":
        await sql.execute("CREATE TABLE IF NOT EXISTS t (v TEXT)");
        await sql.execute("INSERT INTO t (v) VALUES (?)", [u.searchParams.get("v")]);
        return new Response("ok");
      case "/get": {
        await sql.execute("CREATE TABLE IF NOT EXISTS t (v TEXT)");
        const r = await sql.execute("SELECT v FROM t LIMIT 1");
        return new Response(String(r.rows[0]?.[0] ?? "none"));
      }
      default: return new Response("hello");
    }
  },
};"#;

async fn scratch(node: &impl Node, key: &str, body: Value) -> Value {
    let r = post(node, key, "/v1/scratch", body).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    r.json
}

async fn ship_to(node: &impl Node, key: &str, id: &str, src: &str) {
    let up = upload_ts(node, key, id, src, "activate=1&compile=eager").await;
    assert!(up.s() < 300, "{}", up.text);
    let path = format!("/v1/services/{id}/versions/{}", up.json["version"]);
    eventually(Duration::from_secs(60), "the version to build", || {
        let path = path.clone();
        async move { get(node, key, &path).await.json["build_state"] == json!("ready") }
    })
    .await;
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn id_of(v: &Value) -> String {
    v["id"].as_str().expect("id").to_string()
}

fn slug_of(v: &Value) -> String {
    v["slug"].as_str().expect("slug").to_string()
}

node_test! {
/// The forced policy, and the parts that mark a preview: its own label, a slug within 52 characters,
/// `expires_at` from the plan, hidden from the default list.
async fn a_preview_is_a_separate_isolated_service() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent_slug = format!("{}-{}", uniq("par"), "x".repeat(40));
    let parent = create_service(&node, &t.key, &parent_slug, "deno").await;

    let p = scratch(&node, &t.key, json!({"of": parent, "name": "pr-7"})).await;
    let slug = slug_of(&p);
    assert!(slug.len() <= 52 && slug.contains("-pr-7-") && !slug.contains("--"), "{slug}");
    assert_eq!(p["scratch"]["kind"], json!("preview"), "{p}");
    assert_eq!(p["scratch"]["of"], json!(parent), "{p}");
    assert_eq!(p["scratch"]["copied_data"], json!(false), "{p}");
    let env = p["environment"].as_str().expect("environment");
    assert!(env.starts_with("preview-") && env.len() == 16, "{env}");
    assert!(p["scratch"]["expires_at"].as_i64().expect("an expiry") > 0, "{p}");
    let triggers = &p["triggers"];
    assert_eq!(triggers["http"], json!(true), "{p}");
    for off in ["interval", "cron", "queue", "call", "websocket"] {
        assert_eq!(triggers[off], json!(false), "{off}: {p}");
    }
    assert_eq!(p["egress"]["allowed_hosts"], json!(["no-egress.invalid"]), "{p}");

    let other = scratch(&node, &t.key, json!({"of": parent, "name": "pr-7"})).await;
    assert_ne!(other["environment"], p["environment"], "each gets its own label");
    assert_ne!(other["slug"], p["slug"]);

    let listed = get(&node, &t.key, "/v1/services").await;
    assert_eq!(listed.items().len(), 1, "{}", listed.text);
    let listed = get(&node, &t.key, "/v1/services?include=scratch").await;
    assert_eq!(listed.items().len(), 3, "{}", listed.text);
    node.shutdown().await;
}
}

node_test! {
/// Without `of`: a standalone preview, runtime and project from the body.
async fn a_preview_without_a_parent_is_standalone() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let p = scratch(&node, &t.key, json!({"runtime": "deno"})).await;
    assert_eq!(p["scratch"]["of"], Value::Null, "{p}");
    assert_eq!(p["runtime"], json!("deno"), "{p}");
    ship_to(&node, &t.key, &id_of(&p), PROBE).await;
    let r = invoke(&node, &slug_of(&p), "/").await;
    assert_eq!(r.text, "hello");
    assert_eq!(r.header("x-robots-tag").as_deref(), Some("noindex"));
    // A parent settles the runtime: naming one with `of` is a contradiction.
    let parent = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let both = post(&node, &t.key, "/v1/scratch", json!({"of": parent, "runtime": "wasm"})).await;
    assert_eq!((both.s(), both.code().as_str()), (422, "validation_failed"), "{}", both.text);
    let neither = post(&node, &t.key, "/v1/scratch", json!({})).await;
    assert_eq!((neither.s(), neither.code().as_str()), (422, "validation_failed"), "{}", neither.text);
    node.shutdown().await;
}
}

node_test! {
/// Neither direction works, not even when the parent lists the preview by slug: isolation is the
/// runtime's rule, not a setting of the other side.
async fn a_preview_cannot_call_its_parent_or_be_called() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent_slug = uniq("par");
    let parent = ship(&node, &t.key, &parent_slug, PROBE).await;
    let p = scratch(&node, &t.key, json!({"of": parent})).await;
    let slug = slug_of(&p);
    ship_to(&node, &t.key, &id_of(&p), PROBE).await;
    let allow = patch(
        &node,
        &t.key,
        &format!("/v1/services/{parent}"),
        json!({"call": {"allowed_callers": [slug, format!("env:{}", p["environment"].as_str().expect("env"))]}}),
    )
    .await;
    assert_eq!(allow.s(), 200, "{}", allow.text);

    let out = invoke(&node, &slug, &format!("/call?target={parent_slug}")).await;
    assert_eq!(out.json["code"], json!("call_not_allowed"), "{}", out.text);

    let inbound = invoke(&node, &parent_slug, &format!("/call?target={slug}")).await;
    assert_eq!(inbound.json["code"], json!("call_not_allowed"), "{}", inbound.text);

    let queued = invoke(&node, &slug, &format!("/enqueue?target={parent_slug}")).await;
    assert_eq!(queued.json["ok"], json!(false), "{}", queued.text);
    let own = invoke(&node, &slug, &format!("/enqueue?target={slug}")).await;
    assert_eq!(own.json["ok"], json!(false), "no queue of its own either: {}", own.text);
    node.shutdown().await;
}
}

node_test! {
/// `std:stream`, `std:pubsub` and `std:schedule` answer `preview_isolated`; the parent's still work.
async fn a_preview_cannot_read_topics_publish_or_schedule() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent_slug = uniq("par");
    let parent = ship(&node, &t.key, &parent_slug, PROBE).await;
    let topic = post(&node, &t.key, "/v1/topics", json!({"name": "orders"})).await;
    assert_eq!(topic.s(), 201, "{}", topic.text);
    let p = scratch(&node, &t.key, json!({"of": parent})).await;
    let slug = slug_of(&p);
    ship_to(&node, &t.key, &id_of(&p), PROBE).await;

    for path in ["/read?target=orders", "/publish", "/schedule"] {
        let r = invoke(&node, &slug, path).await;
        assert_eq!(r.json["code"], json!("preview_isolated"), "{path}: {}", r.text);
    }
    let parent_reads = invoke(&node, &parent_slug, "/read?target=orders").await;
    assert_eq!(parent_reads.json["ok"], json!(true), "{}", parent_reads.text);
    node.shutdown().await;
}
}

node_test! {
/// Denied unless listed; a `PATCH` that empties the list does not open it.
async fn a_previews_egress_is_denied_unless_listed() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let p = scratch(&node, &t.key, json!({"runtime": "deno"})).await;
    let path = format!("/v1/services/{}", id_of(&p));
    ship_to(&node, &t.key, &id_of(&p), PROBE).await;
    let r = invoke(&node, &slug_of(&p), "/fetch").await;
    assert_eq!(r.json["ok"], json!(false), "{}", r.text);
    assert_eq!(r.json["message"], json!("egress_denied"), "{}", r.text);

    let listed = scratch(&node, &t.key, json!({"runtime": "deno", "allowed_hosts": ["api.example.com"]})).await;
    assert_eq!(listed["egress"]["allowed_hosts"], json!(["api.example.com"]), "{listed}");

    let emptied = patch(&node, &t.key, &path, json!({"egress": {"allowed_hosts": []}})).await;
    assert_eq!(emptied.s(), 200, "{}", emptied.text);
    assert_eq!(emptied.json["egress"]["allowed_hosts"], json!(["no-egress.invalid"]), "{}", emptied.text);
    node.shutdown().await;
}
}

node_test! {
/// What makes it a preview cannot be patched away, and the refusal changes nothing.
async fn a_preview_cannot_change_its_policy_with_patch() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let p = scratch(&node, &t.key, json!({"of": parent})).await;
    let path = format!("/v1/services/{}", id_of(&p));
    for body in [
        json!({"environment": "production"}),
        json!({"call": {"allowed_callers": ["env:production"]}}),
        json!({"triggers": {"http": true, "interval": false, "cron": false, "queue": true, "call": false, "websocket": false}}),
        json!({"triggers": {"http": true, "interval": false, "cron": false, "queue": false, "call": true, "websocket": false}}),
        json!({"events": {"subscribe": [{"subject": "orders.*"}]}}),
    ] {
        let r = patch(&node, &t.key, &path, body.clone()).await;
        assert_eq!((r.s(), r.code().as_str()), (422, "preview_policy"), "{body}: {}", r.text);
    }
    let after = get(&node, &t.key, &path).await;
    assert_eq!(after.json["environment"], p["environment"]);
    assert_eq!(after.json["triggers"], p["triggers"]);
    assert_eq!(after.json["scratch"], p["scratch"]);

    // A field that is not policy still goes through, and `scratch` is not writable at all.
    let named = patch(&node, &t.key, &path, json!({"name": "fine", "scratch": null})).await;
    assert_eq!(named.s(), 200, "{}", named.text);
    assert_eq!(named.json["scratch"], p["scratch"], "{}", named.text);
    assert_eq!(named.json["triggers"], p["triggers"]);
    node.shutdown().await;
}
}

node_test! {
/// A key limited to another project sees neither the parent nor the preview: `404`, not `403`.
async fn a_restricted_key_gets_404() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let parent = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let other = create_project(&node, &t.key, "other", &["production"]).await;
    let (_, limited) = make_key(&node, &t.id, &["services:read", "services:write"], Some(vec![other])).await;

    let r = post(&node, &limited, "/v1/scratch", json!({"of": parent})).await;
    assert_eq!((r.s(), r.code().as_str()), (404, "not_found"), "{}", r.text);
    let p = scratch(&node, &t.key, json!({"of": parent})).await;
    let r = get(&node, &limited, &format!("/v1/services/{}", id_of(&p))).await;
    assert_eq!(r.s(), 404, "{}", r.text);

    // Limited to one environment of the parent's project: a new label is out of its reach.
    let (_, one_env) = make_key_in(
        &node,
        &t.id,
        &["services:read", "services:write"],
        None,
        Some(vec!["production".to_string()]),
    )
    .await;
    let r = post(&node, &one_env, "/v1/scratch", json!({"of": parent})).await;
    assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{}", r.text);

    // `copy_data` needs `data:admin`.
    let r = post(&node, &limited, "/v1/scratch", json!({"of": parent, "copy_data": true})).await;
    assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// `max_scratch` caps previews only, `max_services` still counts them, `scratch_ttl_max` caps expiry.
async fn plan_keys_cap_previews() {
    let node = start_node_opts(StartOptions::default(), None, |c| {
        let pro = c.plans.get_mut("pro").expect("pro plan");
        pro.max_scratch = 1;
        pro.scratch_ttl_max = 3600;
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let parent = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let now = now_ms();

    let far = post(&node, &t.key, "/v1/scratch", json!({"of": parent, "expires_at": now + 7_200_000})).await;
    assert_eq!((far.s(), far.code().as_str()), (422, "scratch_ttl_too_long"), "{}", far.text);
    let far = post(&node, &t.key, "/v1/scratch", json!({"of": parent, "ttl_seconds": 7200})).await;
    assert_eq!((far.s(), far.code().as_str()), (422, "scratch_ttl_too_long"), "{}", far.text);
    let past = post(&node, &t.key, "/v1/scratch", json!({"of": parent, "expires_at": now - 1000})).await;
    assert_eq!((past.s(), past.code().as_str()), (422, "validation_failed"), "{}", past.text);
    let both = post(&node, &t.key, "/v1/scratch", json!({"of": parent, "expires_at": now + 1000, "ttl_seconds": 10})).await;
    assert_eq!((both.s(), both.code().as_str()), (422, "validation_failed"), "{}", both.text);

    let first = scratch(&node, &t.key, json!({"of": parent})).await;
    let at = first["scratch"]["expires_at"].as_i64().expect("expiry");
    assert!(at <= now_ms() + 3_600_000, "the default is the cap: {first}");

    let over = post(&node, &t.key, "/v1/scratch", json!({"of": parent})).await;
    assert_eq!((over.s(), over.code().as_str()), (403, "quota_exceeded"), "{}", over.text);
    assert!(over.text.contains("max_scratch"), "{}", over.text);
    let envs = get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone();
    assert_eq!(envs.as_array().expect("list").iter().filter(|e| e.as_str().is_some_and(|e| e.starts_with("preview-"))).count(), 1, "a refusal leaves no label: {envs}");

    // Branches are not previews.
    let br = post(&node, &t.key, &format!("/v1/services/{parent}/branches"), json!({"name": "ok"})).await;
    assert_eq!(br.s(), 201, "{}", br.text);

    // Gone, the room is back.
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{}", id_of(&first))).await.s(), 204);
    scratch(&node, &t.key, json!({"of": parent})).await;
    node.shutdown().await;
}
}

node_test! {
async fn previews_count_against_max_services() {
    let node = start_node_opts(StartOptions::default(), None, |c| {
        c.plans.get_mut("pro").expect("pro plan").max_services = 1;
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let parent = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let over = post(&node, &t.key, "/v1/scratch", json!({"of": parent})).await;
    assert_eq!((over.s(), over.code().as_str()), (403, "quota_exceeded"), "{}", over.text);
    node.shutdown().await;
}
}

node_test! {
/// The sweep takes the preview, its data and its label; `copy_data` copies and forces key access.
async fn the_sweep_deletes_an_expired_preview_with_its_data() {
    let handle = new_handle();
    let node = start_node_opts(StartOptions::default(), Some(handle.clone()), |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let parent_slug = uniq("par");
    let parent = ship(&node, &t.key, &parent_slug, PROBE).await;
    assert_eq!(invoke(&node, &parent_slug, "/put?v=prod").await.text, "ok");

    let plain = scratch(&node, &t.key, json!({"of": parent, "ttl_seconds": 1})).await;
    let copied = scratch(&node, &t.key, json!({"of": parent, "copy_data": true, "ttl_seconds": 1})).await;
    let kept = scratch(&node, &t.key, json!({"of": parent})).await;
    assert_eq!(copied["access"], json!("key"), "{copied}");
    assert_eq!(copied["scratch"]["copied_data"], json!(true), "{copied}");

    ship_to(&node, &t.key, &id_of(&plain), PROBE).await;
    let plain_slug = slug_of(&plain);
    assert_eq!(invoke(&node, &plain_slug, "/get").await.text, "none", "no production data");
    assert_eq!(invoke(&node, &plain_slug, "/put?v=mine").await.text, "ok");
    assert_eq!(invoke(&node, &plain_slug, "/get").await.text, "mine");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let server = handle.lock().expect("handle lock").clone().expect("running");
    server.run_periodic_for_test(600).await;
    drop(server);

    for gone in [&plain, &copied] {
        let r = get(&node, &t.key, &format!("/v1/services/{}", id_of(gone))).await;
        assert_eq!(r.s(), 404, "{}", r.text);
    }
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{}", id_of(&kept))).await.s(), 200);
    assert_eq!(invoke(&node, &plain_slug, "/get").await.s(), 404);
    let envs = get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone();
    assert!(!envs.as_array().expect("list").contains(&plain["environment"]), "{envs}");
    // The parent keeps its own data.
    assert_eq!(invoke(&node, &parent_slug, "/get").await.text, "prod");
    node.shutdown().await;
}
}
