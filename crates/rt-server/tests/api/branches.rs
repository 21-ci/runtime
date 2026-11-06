//! Branch services (`docs/design/branching-services-design.md` §3, §4, §6, §7): a branch is a service
//! with `config.scratch`: forced policy, hidden from default lists, its own environment label, empty
//! unless `copy_data` asked otherwise, and gone with its data on delete or expiry.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rt_server::StartOptions;
use rt_testkit::NodeHarness;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<rt_sec::AuditEvent>>,
}

impl rt_sec::AuditSink for Recorder {
    fn emit(&self, ev: rt_sec::AuditEvent) {
        self.events.lock().expect("recorder lock").push(ev);
    }
}

impl Recorder {
    fn of(&self, action: &str) -> Vec<rt_sec::AuditEvent> {
        let events = self.events.lock().expect("recorder lock");
        events.iter().filter(|e| e.action == action).cloned().collect()
    }
}

async fn audited(tweak: impl FnOnce(&mut rt_config::Config)) -> (NodeHarness, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    let extra_audit = Some(rec.clone() as Arc<dyn rt_sec::AuditSink>);
    let node = start_node_opts(
        StartOptions {
            extra_audit,
            ..Default::default()
        },
        None,
        tweak,
    )
    .await;
    (node, rec)
}

/// `/put?v=`: stores a value in KV-like SQL. `/get`: reads it back, or `none`.
const GUEST: &str = r#"
import { sql } from "std:sql";
import { blob } from "std:blob";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS t (v TEXT)");
}
export default {
  async fetch(req: Request): Promise<Response> {
    await ensure();
    const u = new URL(req.url);
    if (u.pathname === "/put") {
      await sql.execute("INSERT INTO t (v) VALUES (?)", [u.searchParams.get("v")]);
      await blob.put("k", "blob-" + u.searchParams.get("v"));
      return new Response("ok");
    }
    if (u.pathname === "/grow") {
      await sql.execute("INSERT INTO t (v) VALUES (?)", ["x".repeat(Number(u.searchParams.get("n")))]);
      return new Response("ok");
    }
    if (u.pathname === "/get") {
      const r = await sql.execute("SELECT v FROM t ORDER BY rowid DESC LIMIT 1");
      const b = await blob.get("k");
      return Response.json({ v: r.rows[0]?.[0] ?? "none", blob: b ? await new Response(b.body).text() : "none" });
    }
    return new Response("hello");
  },
};"#;

/// Counts its `scheduled` runs.
const TICKS: &str = r#"
import { sql } from "std:sql";
async function ensure() {
  await sql.execute("CREATE TABLE IF NOT EXISTS ticks (id INTEGER PRIMARY KEY AUTOINCREMENT)");
}
export default {
  async fetch(_req: Request): Promise<Response> {
    await ensure();
    const r = await sql.execute("SELECT count(*) FROM ticks");
    return new Response(String(r.rows[0][0]));
  },
  async scheduled(_event: any, _ctx: any): Promise<void> {
    await ensure();
    await sql.execute("INSERT INTO ticks DEFAULT VALUES");
  },
};"#;

/// A `scheduled` run that takes a long time.
const SLOW_TICK: &str = r#"
export default {
  async fetch(_req: Request): Promise<Response> { return new Response("slow"); },
  async scheduled(_event: any, _ctx: any): Promise<void> {
    await new Promise((r) => setTimeout(r, 20000));
  },
};"#;

const PUBSUB: &str = r#"
export default {
  async fetch(_req: Request): Promise<Response> { return new Response("ok"); },
  async pubsub(_msg: any, _ctx: any): Promise<unknown> { return {}; },
};"#;

fn branches(id: &str) -> String {
    format!("/v1/services/{id}/branches")
}

fn assert_refused(r: &Resp, status: u16, code: &str) {
    assert_eq!((r.s(), r.code().as_str()), (status, code), "{}", r.text);
}

async fn branch(node: &NodeHarness, key: &str, parent: &str, body: Value) -> Value {
    let r = post(node, key, &branches(parent), body).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    r.json
}

/// Ships to a branch and waits for it to serve.
async fn ship_to(node: &NodeHarness, key: &str, id: &str, src: &str) {
    let up = upload_ts(node, key, id, src, "activate=1&compile=eager").await;
    assert!(up.s() < 300, "{}", up.text);
    let v = up.json["version"].as_u64().expect("version");
    let path = format!("/v1/services/{id}/versions/{v}");
    eventually(Duration::from_secs(60), "the version to build", || {
        let path = path.clone();
        async move { get(node, key, &path).await.json["build_state"] == json!("ready") }
    })
    .await;
}

fn slugs(r: &Resp) -> Vec<String> {
    r.items()
        .iter()
        .map(|s| s["slug"].as_str().unwrap_or_default().to_string())
        .collect()
}

node_test! {
/// `code_of_parent`: the parent's live version, live on the branch from the first request, with
/// the digest of the parent's. Takes `deploy`; a parent with nothing live gives an empty branch.
async fn a_branch_can_start_from_the_parents_code() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    // A lazy live version has no finished build to adopt yet, and no branch is left behind.
    let lazy = post(&node, &t.key, &branches(&id), json!({"name": "code", "code_of_parent": true})).await;
    assert_refused(&lazy, 409, "version_not_ready");
    ship_to(&node, &t.key, &id, GUEST).await;
    let br = branch(&node, &t.key, &id, json!({"name": "code", "code_of_parent": true})).await;
    assert_eq!(br["live_version"], json!(1), "{br}");
    let bid = br["id"].as_str().expect("id");
    let of = |sid: String, v: u32| {
        let node = &node;
        let key = t.key.clone();
        async move {
            get(node, &key, &format!("/v1/services/{sid}/versions/{v}")).await.json["source_digest"].clone()
        }
    };
    assert_eq!(of(bid.to_string(), 1).await, of(id.clone(), 2).await);

    let without = branch(&node, &t.key, &id, json!({"name": "bare"})).await;
    assert_eq!(without["live_version"], Value::Null, "the default stays no code");

    let empty = create_service(&node, &t.key, &uniq("nocode"), "deno").await;
    let none = branch(&node, &t.key, &empty, json!({"name": "e", "code_of_parent": true})).await;
    assert_eq!(none["live_version"], Value::Null);
}
}

node_test! {
/// `<parent-slug>-br-<name>`: names match `^[a-z][a-z0-9-]{0,23}$` without `--`, and the whole slug
/// must still be a slug. A taken slug is `branch_exists`, whoever took it.
async fn branch_slug_grammar() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = create_service(&node, &t.key, &slug, "deno").await;

    let ok = branch(&node, &t.key, &id, json!({"name": "staging"})).await;
    assert_eq!(ok["slug"], json!(format!("{slug}-br-staging")));
    let max = "a".repeat(24);
    let ok = branch(&node, &t.key, &id, json!({"name": max})).await;
    assert_eq!(ok["slug"], json!(format!("{slug}-br-{max}")));

    let long = "a".repeat(25);
    for bad in ["", "Bad", "1a", "a_b", "a--b", "-a", long.as_str()] {
        let r = post(&node, &t.key, &branches(&id), json!({"name": bad})).await;
        assert_refused(&r, 422, "branch_name_invalid");
    }
    let again = post(&node, &t.key, &branches(&id), json!({"name": "staging"})).await;
    assert_refused(&again, 409, "branch_exists");

    let squat = create_service(&node, &t.key, &format!("{slug}-br-taken"), "deno").await;
    assert!(!squat.is_empty());
    let taken = post(&node, &t.key, &branches(&id), json!({"name": "taken"})).await;
    assert_refused(&taken, 409, "branch_exists");

    // `<parent>-br-<name>` over the 63 characters of a slug.
    let long_parent = format!("{slug}-{}", "p".repeat(58 - slug.len().min(50)));
    let lp = create_service(&node, &t.key, &long_parent, "deno").await;
    let r = post(&node, &t.key, &branches(&lp), json!({"name": "a".repeat(24)})).await;
    assert_refused(&r, 422, "branch_name_invalid");

    // A branch has no branches of its own.
    let child = ok["id"].as_str().expect("id").to_string();
    let r = post(&node, &t.key, &branches(&child), json!({"name": "deep"})).await;
    assert_refused(&r, 422, "validation_failed");
    node.shutdown().await;
}
}

node_test! {
/// Limits, egress and the switches of the parent are mirrored; `scaling.min_instances` is 0 and the
/// timed triggers are off, at create and again after a `PATCH` that tries to change them.
async fn policy_is_forced_at_create_and_on_patch() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let p = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({
        "scaling": {"min_instances": 1},
        "egress": {"allowed_hosts": ["api.example.com"]},
        "triggers": {"queue": false},
        "limits": {"wall_ms": 1234},
    })).await;
    assert_eq!(p.s(), 200, "{}", p.text);

    let b = branch(&node, &t.key, &id, json!({"name": "one"})).await;
    let bid = b["id"].as_str().expect("id").to_string();
    assert_eq!(b["scaling"]["min_instances"], json!(0), "{b}");
    assert_eq!(b["triggers"]["cron"], json!(false), "{b}");
    assert_eq!(b["triggers"]["interval"], json!(false), "{b}");
    assert_eq!(b["triggers"]["queue"], json!(false), "the parent's switch: {b}");
    assert_eq!(b["triggers"]["http"], json!(true), "{b}");
    assert_eq!(b["egress"]["allowed_hosts"], json!(["api.example.com"]), "{b}");
    assert_eq!(b["limits"]["wall_ms"], json!(1234), "{b}");
    assert_eq!(b["scratch"]["kind"], json!("branch"), "{b}");
    assert_eq!(b["scratch"]["of"], json!(id), "{b}");
    assert_eq!(b["scratch"]["name"], json!("one"), "{b}");

    let patched = patch(&node, &t.key, &format!("/v1/services/{bid}"), json!({
        "scaling": {"min_instances": 1},
        "triggers": {"cron": true, "interval": true, "http": false},
    })).await;
    assert_eq!(patched.s(), 200, "{}", patched.text);
    assert_eq!(patched.json["scaling"]["min_instances"], json!(0), "{}", patched.text);
    assert_eq!(patched.json["triggers"]["cron"], json!(false), "{}", patched.text);
    assert_eq!(patched.json["triggers"]["interval"], json!(false), "{}", patched.text);
    // What the policy does not force stays the caller's.
    assert_eq!(patched.json["triggers"]["http"], json!(false), "{}", patched.text);
    assert_eq!(patched.json["scratch"]["name"], json!("one"), "{}", patched.text);

    // The object is not the caller's to write: the routes do not read it.
    let r = patch(&node, &t.key, &format!("/v1/services/{bid}"), json!({"scratch": null})).await;
    assert_eq!(r.json["scratch"]["name"], json!("one"), "{}", r.text);
    let fake = uniq("fake");
    let r = post(&node, &t.key, "/v1/services", json!({
        "slug": fake, "runtime": "deno", "scratch": {"of": id, "kind": "branch", "name": "x"},
    })).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    assert!(r.json.get("scratch").is_none(), "{}", r.text);

    // `egress_of_parent: false` opens nothing: an allowlist no host can match.
    let denied = branch(&node, &t.key, &id, json!({"name": "closed", "egress_of_parent": false})).await;
    assert_ne!(denied["egress"]["allowed_hosts"], json!(["api.example.com"]), "{denied}");
    assert_eq!(denied["egress"]["allowed_hosts"].as_array().map(Vec::len), Some(1), "{denied}");
    assert!(denied["egress"]["allowed_hosts"][0].as_str().expect("host").ends_with(".invalid"), "{denied}");
    node.shutdown().await;
}
}

node_test! {
/// A default list has no branches; `?include=scratch`, `?slug=` and the parent's own list do.
async fn branches_are_hidden_from_default_lists() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = create_service(&node, &t.key, &slug, "deno").await;
    let other = create_service(&node, &t.key, &uniq("oth"), "deno").await;
    branch(&node, &t.key, &id, json!({"name": "a"})).await;
    branch(&node, &t.key, &other, json!({"name": "b"})).await;
    let name = format!("{slug}-br-a");

    let all = get(&node, &t.key, "/v1/services").await;
    assert_eq!(all.items().len(), 2, "{}", all.text);
    assert!(!slugs(&all).contains(&name), "{}", all.text);
    let incl = get(&node, &t.key, "/v1/services?include=scratch").await;
    assert_eq!(incl.items().len(), 4, "{}", incl.text);
    let by = get(&node, &t.key, &format!("/v1/services?slug={name}")).await;
    assert_eq!(slugs(&by), std::slice::from_ref(&name), "{}", by.text);
    let of = get(&node, &t.key, &branches(&id)).await;
    assert_eq!(of.s(), 200, "{}", of.text);
    assert_eq!(slugs(&of), [name], "{}", of.text);
    node.shutdown().await;
}
}

node_test! {
/// `{branch}` resolves through the hidden slug; a service merely named like a branch is not one.
async fn switch_from_a_branch_finds_it_and_refuses_a_lookalike() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    let b = branch(&node, &t.key, &id, json!({"name": "real"})).await;
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), GUEST).await;
    let path = format!("/v1/services/{id}/switch");
    let ok = post(&node, &t.key, &path, json!({"from": {"branch": "real"}})).await;
    assert_eq!(ok.s(), 202, "{}", ok.text);

    let fake = create_service(&node, &t.key, &format!("{slug}-br-fake"), "deno").await;
    ship_to(&node, &t.key, &fake, GUEST).await;
    let r = post(&node, &t.key, &path, json!({"from": {"branch": "fake"}})).await;
    assert_refused(&r, 409, "not_a_branch");
    let d = delete(&node, &t.key, &format!("{}/fake", branches(&id))).await;
    assert_refused(&d, 409, "not_a_branch");
    let r = post(&node, &t.key, &path, json!({"from": {"branch": "absent"}})).await;
    assert_refused(&r, 404, "not_found");
    let r = post(&node, &t.key, &path, json!({"from": {"branch": "Bad"}})).await;
    assert_refused(&r, 422, "branch_name_invalid");

    // Another parent's branch, reached through a slug that happens to fit, is not this one's.
    let other_slug = format!("{slug}-br-real");
    let other = get(&node, &t.key, &format!("/v1/services?slug={other_slug}")).await;
    assert_eq!(other.items().len(), 1, "{}", other.text);
    let second = create_service(&node, &t.key, &uniq("sec"), "deno").await;
    let r = delete(&node, &t.key, &format!("{}/real", branches(&second))).await;
    assert_refused(&r, 404, "not_found");
    node.shutdown().await;
}
}

node_test! {
/// A branch gets `br-<name>`, added to the project and removed again with it. A named environment
/// must exist and be inside the key's scope.
async fn environment_label_is_auto_created_and_removed() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["staging"]).await;
    let id = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let envs = || async { get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone() };

    let b = branch(&node, &t.key, &id, json!({"name": "lbl"})).await;
    assert_eq!(b["environment"], json!("br-lbl"), "{b}");
    assert!(envs().await.as_array().expect("list").contains(&json!("br-lbl")));

    let named = branch(&node, &t.key, &id, json!({"name": "st", "environment": "staging"})).await;
    assert_eq!(named["environment"], json!("staging"), "{named}");
    let none = post(&node, &t.key, &branches(&id), json!({"name": "no", "environment": "nosuch"})).await;
    assert_refused(&none, 422, "validation_failed");
    assert_eq!(get(&node, &t.key, &branches(&id)).await.items().len(), 2);

    assert_eq!(delete(&node, &t.key, &format!("{}/lbl", branches(&id))).await.s(), 204);
    assert!(!envs().await.as_array().expect("list").contains(&json!("br-lbl")));
    // A label the branch did not create stays.
    assert_eq!(delete(&node, &t.key, &format!("{}/st", branches(&id))).await.s(), 204);
    assert!(envs().await.as_array().expect("list").contains(&json!("staging")));
    node.shutdown().await;
}
}

node_test! {
/// Scopes and tenancy: `services:write` to create, others see `404`, and a key held to some
/// environments neither creates outside them nor adds a label for it.
async fn create_is_scoped_and_tenanted() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("par"), "deno").await;

    let ro = make_key(&node, &t.id, &["services:read"], None).await.1;
    let r = post(&node, &ro, &branches(&id), json!({"name": "x"})).await;
    assert_refused(&r, 403, "insufficient_scope");
    let list = make_key(&node, &t.id, &["deploy"], None).await.1;
    let r = get(&node, &list, &branches(&id)).await;
    assert_refused(&r, 403, "insufficient_scope");

    let other = new_tenant(&node, "pro").await;
    let r = post(&node, &other.key, &branches(&id), json!({"name": "x"})).await;
    assert_refused(&r, 404, "not_found");
    let r = get(&node, &other.key, &branches(&id)).await;
    assert_refused(&r, 404, "not_found");

    allow_environments(&node, &t.key, &["staging"]).await;
    // Parent in `staging`, the key limited to `staging`: the default label is outside its scope.
    let st = create_service_in(&node, &t.key, "default", "staging", &uniq("stg"), "deno").await;
    let limited = make_key_in(&node, &t.id, &["services:write", "services:read"], None, Some(vec!["staging".to_string()])).await.1;
    let r = post(&node, &limited, &branches(&st), json!({"name": "x"})).await;
    assert_refused(&r, 403, "insufficient_scope");
    let envs = get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone();
    assert!(!envs.as_array().expect("list").contains(&json!("br-x")), "{envs}");
    assert_eq!(get(&node, &t.key, &branches(&st)).await.items().len(), 0);
    let ok = post(&node, &limited, &branches(&st), json!({"name": "x", "environment": "staging"})).await;
    assert_eq!(ok.s(), 201, "{}", ok.text);
    let hidden = post(&node, &limited, &branches(&id), json!({"name": "y", "environment": "staging"})).await;
    assert_refused(&hidden, 404, "not_found");

    // Past the plan's services, like any service (decision 16).
    let quota = |i: u32| json!({"name": format!("q{i}"), "environment": "production"});
    let mut last = post(&node, &t.key, &branches(&id), quota(0)).await;
    for i in 1..2000 {
        if last.s() != 201 {
            break;
        }
        last = post(&node, &t.key, &branches(&id), quota(i)).await;
    }
    assert_eq!(last.s(), 403, "{}", last.text);
    assert_eq!(last.code(), "quota_exceeded", "{}", last.text);
    node.shutdown().await;
}
}

node_test! {
/// `Idempotency-Key`: the same body replays, another body is refused.
async fn create_is_idempotent() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let send = |name: &'static str| {
        call(
            api(&node, &t.key, reqwest::Method::POST, &branches(&id)).header("idempotency-key", "k1"),
            Some(json!({"name": name})),
        )
    };
    let a = send("idem").await;
    assert_eq!(a.s(), 201, "{}", a.text);
    let b = send("idem").await;
    assert_eq!((b.s(), &b.json["id"]), (201, &a.json["id"]), "{}", b.text);
    let c = send("other").await;
    assert_refused(&c, 422, "idempotency_mismatch");
    node.shutdown().await;
}
}

node_test! {
/// Empty by default: no row of the parent is readable at the branch, and neither are env values.
async fn data_is_empty_by_default() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    assert_eq!(invoke(&node, &slug, "/put?v=prod").await.s(), 200);
    let b = branch(&node, &t.key, &id, json!({"name": "em"})).await;
    assert_eq!(b["scratch"]["copied_data"], json!(false), "{b}");
    assert_eq!(b["access"], json!("public"), "the parent's: {b}");
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), GUEST).await;
    let got = invoke(&node, &format!("{slug}-br-em"), "/get").await;
    assert_eq!(got.json["v"], json!("none"), "{}", got.text);
    assert_eq!(got.json["blob"], json!("none"), "{}", got.text);
    node.shutdown().await;
}
}

node_test! {
/// `copy_data`: `data:admin` on top of `services:write`, audited, the data and blobs arrive, and the
/// branch asks for a key however public the parent is, also after a `PATCH`.
async fn copy_data_requires_data_admin_and_forces_key_access() {
    let (node, rec) = audited(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    assert_eq!(invoke(&node, &slug, "/put?v=prod").await.s(), 200);

    let weak = make_key(&node, &t.id, &["services:write", "services:read"], None).await.1;
    let r = post(&node, &weak, &branches(&id), json!({"name": "cp", "copy_data": true})).await;
    assert_refused(&r, 403, "insufficient_scope");
    assert_eq!(get(&node, &t.key, &branches(&id)).await.items().len(), 0, "nothing was created");
    assert!(rec.of("scratch.copied_data").is_empty());

    let b = branch(&node, &t.key, &id, json!({"name": "cp", "copy_data": true})).await;
    let bid = b["id"].as_str().expect("id").to_string();
    assert_eq!(b["access"], json!("key"), "{b}");
    assert_eq!(b["scratch"]["copied_data"], json!(true), "{b}");
    let ev = rec.of("scratch.copied_data");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].data["source"], json!(id), "{:?}", ev[0]);
    assert!(ev[0].data["bytes"].as_u64().expect("bytes") > 0, "{:?}", ev[0]);
    assert_eq!(rec.of("scratch.created").len(), 1);

    ship_to(&node, &t.key, &bid, GUEST).await;
    let bslug = format!("{slug}-br-cp");
    let anon = invoke(&node, &bslug, "/get").await;
    assert_eq!(anon.s(), 401, "a copied branch is not public: {}", anon.text);
    let p = patch(&node, &t.key, &format!("/v1/services/{bid}"), json!({"access": "public"})).await;
    assert_eq!((p.s(), &p.json["access"]), (200, &json!("key")), "{}", p.text);
    let anon = invoke(&node, &bslug, "/get").await;
    assert_eq!(anon.s(), 401, "{}", anon.text);

    let seen = invoke_with(&node, &bslug, reqwest::Method::GET, "/get", &[("authorization", &format!("Bearer {}", t.key))], "").await;
    assert_eq!(seen.s(), 200, "{}", seen.text);
    assert_eq!(seen.json["v"], json!("prod"), "{}", seen.text);
    assert_eq!(seen.json["blob"], json!("blob-prod"), "{}", seen.text);

    // Two histories from here on: the parent's next row does not show up at the branch.
    assert_eq!(invoke(&node, &slug, "/put?v=later").await.s(), 200);
    let seen = invoke_with(&node, &bslug, reqwest::Method::GET, "/get", &[("authorization", &format!("Bearer {}", t.key))], "").await;
    assert_eq!(seen.json["v"], json!("prod"), "{}", seen.text);
    node.shutdown().await;
}
}

node_test! {
/// Over `deploy.dry_run_max_mb` the copy is refused, and no half-made branch stays behind.
async fn copy_data_over_the_cap_is_413_and_leaves_nothing() {
    let (node, rec) = audited(|c| c.deploy.dry_run_max_mb = 1).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    for _ in 0..3 {
        assert_eq!(invoke(&node, &slug, "/grow?n=500000").await.s(), 200);
    }
    let r = post(&node, &t.key, &branches(&id), json!({"name": "big", "copy_data": true})).await;
    assert_refused(&r, 413, "copy_data_too_large");
    let incl = get(&node, &t.key, &format!("/v1/services?slug={slug}-br-big")).await;
    assert_eq!(incl.items().len(), 0, "{}", incl.text);
    let envs = get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone();
    assert!(!envs.as_array().expect("list").contains(&json!("br-big")), "{envs}");
    assert!(rec.of("scratch.copied_data").is_empty());
    // Under the cap the same parent branches fine without a copy.
    branch(&node, &t.key, &id, json!({"name": "big"})).await;
    node.shutdown().await;
}
}

node_test! {
/// System services: `scratch_source_reserved` when their data was asked for, `reserved` otherwise.
async fn system_services_have_no_branches() {
    use rt_store::{MetaStore, TenantScope};
    use std::sync::OnceLock;
    let captured: Arc<OnceLock<Arc<dyn MetaStore>>> = Arc::new(OnceLock::new());
    let slot = captured.clone();
    let node = start_node_opts(
        StartOptions {
            meta_layer: Some(Box::new(move |m| {
                let _ = slot.set(m.clone());
                m
            })),
            ..Default::default()
        },
        None,
        |_| {},
    )
    .await;
    let t = new_tenant(&node, "pro").await;
    let tenant: rt_types::TenantId = t.id.parse().expect("tenant");
    let sys = captured
        .get()
        .expect("meta")
        .create_service(
            TenantScope::Tenant(tenant),
            tenant,
            rt_types::NewService {
                slug: uniq("sys-x"),
                runtime: rt_types::RuntimeKind::Deno,
                name: None,
                profile: None,
                environment: "production".into(),
                project: "default".into(),
                config: rt_types::ServiceConfig::default(),
            },
        )
        .await
        .expect("create")
        .id
        .to_string();
    let r = post(&node, &t.key, &branches(&sys), json!({"name": "x", "copy_data": true})).await;
    assert_refused(&r, 403, "scratch_source_reserved");
    let r = post(&node, &t.key, &branches(&sys), json!({"name": "x"})).await;
    assert_refused(&r, 403, "reserved");
    node.shutdown().await;
}
}

node_test! {
/// `env_names_from_parent`: names arrive with empty values; a secret's value never does.
async fn env_names_are_copied_never_values() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("par"), "deno").await;
    let base = format!("/v1/services/{id}");
    assert_eq!(put(&node, &t.key, &format!("{base}/env"), json!({"PLAIN": "plain-value"})).await.s(), 200);
    assert_eq!(put(&node, &t.key, &format!("{base}/secrets/TOKEN"), json!({"value": "secret-value"})).await.s(), 204);

    let b = branch(&node, &t.key, &id, json!({"name": "en", "env_names_from_parent": true})).await;
    let env = get(&node, &t.key, &format!("/v1/services/{}/env", b["id"].as_str().expect("id"))).await;
    assert_eq!(env.json["PLAIN"], json!(""), "{}", env.text);
    assert_eq!(env.json["TOKEN"], json!(""), "{}", env.text);
    assert!(!env.text.contains("-value"), "{}", env.text);

    let bare = branch(&node, &t.key, &id, json!({"name": "bare"})).await;
    let env = get(&node, &t.key, &format!("/v1/services/{}/env", bare["id"].as_str().expect("id"))).await;
    assert_eq!(env.json.as_object().map(serde_json::Map::len), Some(0), "{}", env.text);
    node.shutdown().await;
}
}

node_test! {
/// Deleting a branch removes its data for good: a new one under the same name starts empty, and the
/// parent keeps its own. The parent cannot be deleted from under its branches.
async fn delete_removes_the_branch_and_its_data() {
    let (node, rec) = audited(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    assert_eq!(invoke(&node, &slug, "/put?v=prod").await.s(), 200);
    let b = branch(&node, &t.key, &id, json!({"name": "del"})).await;
    let bid = b["id"].as_str().expect("id").to_string();
    ship_to(&node, &t.key, &bid, GUEST).await;
    let bslug = format!("{slug}-br-del");
    assert_eq!(invoke(&node, &bslug, "/put?v=branch").await.s(), 200);
    assert_eq!(invoke(&node, &bslug, "/get").await.json["v"], json!("branch"));

    let parent_del = delete(&node, &t.key, &format!("/v1/services/{id}")).await;
    assert_refused(&parent_del, 409, "branch_exists");

    let ro = make_key(&node, &t.id, &["services:read"], None).await.1;
    let r = delete(&node, &ro, &format!("{}/del", branches(&id))).await;
    assert_refused(&r, 403, "insufficient_scope");

    let d = delete(&node, &t.key, &format!("{}/del", branches(&id))).await;
    assert_eq!(d.s(), 204, "{}", d.text);
    assert_eq!(rec.of("scratch.deleted").len(), 1);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{bid}")).await.s(), 404);
    assert_eq!(invoke(&node, &bslug, "/get").await.s(), 404);
    let gone = delete(&node, &t.key, &format!("{}/del", branches(&id))).await;
    assert_refused(&gone, 404, "not_found");

    let again = branch(&node, &t.key, &id, json!({"name": "del"})).await;
    ship_to(&node, &t.key, again["id"].as_str().expect("id"), GUEST).await;
    assert_eq!(invoke(&node, &bslug, "/get").await.json["v"], json!("none"));
    assert_eq!(invoke(&node, &slug, "/get").await.json["v"], json!("prod"));

    // With no branch left, the parent goes as before.
    assert_eq!(delete(&node, &t.key, &format!("{}/del", branches(&id))).await.s(), 204);
    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{id}")).await.s(), 204);
    node.shutdown().await;
}
}

node_test! {
/// Timed triggers stay off unless `schedules: true`: a schedule on a default branch never runs.
async fn branch_has_no_timed_triggers_by_default() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, TICKS).await;

    let quiet = branch(&node, &t.key, &id, json!({"name": "quiet"})).await;
    let loud = branch(&node, &t.key, &id, json!({"name": "loud", "schedules": true})).await;
    assert_eq!(quiet["triggers"]["interval"], json!(false), "{quiet}");
    assert_eq!(loud["triggers"]["interval"], json!(true), "{loud}");
    let schedule = json!({"kind": "interval", "every_seconds": 1, "payload": {}});
    let qid = quiet["id"].as_str().expect("id");
    ship_to(&node, &t.key, qid, TICKS).await;
    let r = post(&node, &t.key, &format!("/v1/services/{qid}/schedules"), schedule.clone()).await;
    assert_refused(&r, 403, "trigger_not_allowed");
    let lid = loud["id"].as_str().expect("id");
    ship_to(&node, &t.key, lid, TICKS).await;
    let r = post(&node, &t.key, &format!("/v1/services/{lid}/schedules"), schedule).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    let loud_slug = format!("{slug}-br-loud");
    let ls = &loud_slug;
    let node_ref = &node;
    eventually(Duration::from_secs(20), "the opted-in branch to tick", || async move {
        invoke(node_ref, ls, "/").await.text != "0"
    })
    .await;
    assert_eq!(invoke(&node, &format!("{slug}-br-quiet"), "/").await.text, "0");
    // `schedules: true` survives a PATCH of the switches.
    let p = patch(
        &node,
        &t.key,
        &format!("/v1/services/{}", loud["id"].as_str().expect("id")),
        json!({"triggers": {"interval": true}}),
    )
    .await;
    assert_eq!(p.json["triggers"]["interval"], json!(true), "{}", p.text);
    node.shutdown().await;
}
}

node_test! {
/// M8: a branch is not a second member of the parent's subscriptions and consumers unless asked,
/// the asking takes `deploy`, and a `PATCH` cannot add them later.
async fn branch_does_not_inherit_subscriptions_by_default() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, PUBSUB).await;
    let p = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"events": {"subscribe": [{"subject": "orders.*"}]}})).await;
    assert_eq!(p.s(), 200, "{}", p.text);

    let write_only = make_key(&node, &t.id, &["services:write", "services:read"], None).await.1;
    let b = branch(&node, &write_only, &id, json!({"name": "plain", "environment": "production"})).await;
    assert!(b["events"]["subscribe"].as_array().is_none_or(Vec::is_empty), "{b}");
    let path = format!("/v1/services/{}", b["id"].as_str().expect("id"));
    let p = patch(&node, &t.key, &path, json!({"events": {"subscribe": [{"subject": "orders.*"}]}})).await;
    assert_eq!(p.s(), 200, "{}", p.text);
    assert!(p.json["events"]["subscribe"].as_array().is_none_or(Vec::is_empty), "forced after PATCH: {}", p.text);

    let r = post(&node, &write_only, &branches(&id), json!({"name": "ask", "events_of_parent": true})).await;
    assert_refused(&r, 403, "insufficient_scope");
    let kept = branch(&node, &t.key, &id, json!({"name": "ask", "events_of_parent": true})).await;
    assert_eq!(kept["events"]["subscribe"][0]["subject"], json!("orders.*"), "{kept}");
    node.shutdown().await;
}
}

node_test! {
/// Pub/sub is scoped by environment: a branch that asked to mirror the parent's subscription still
/// gets nothing published to the parent's environment. Sharing a label is a choice, and shares.
async fn branch_cannot_read_parent_topic() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, PUBSUB).await;
    let p = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"events": {"subscribe": [{"subject": "orders.*"}]}})).await;
    assert_eq!(p.s(), 200, "{}", p.text);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);

    let b = branch(&node, &t.key, &id, json!({"name": "iso", "events_of_parent": true})).await;
    assert_eq!(b["events"]["subscribe"][0]["subject"], json!("orders.*"), "mirrored: {b}");
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), PUBSUB).await;
    assert_eq!(invoke(&node, &format!("{slug}-br-iso"), "/").await.s(), 200);

    let publish = |env: &'static str| {
        let (node, key) = (&node, &t.key);
        async move {
            let r = post(node, key, "/v1/pubsub/publish", json!({"subject": "orders.new", "body": {}, "environment": env})).await;
            assert_eq!(r.s(), 200, "{}", r.text);
            r.json["receivers"].as_u64().expect("receivers")
        }
    };
    assert_eq!(publish("production").await, 1, "the parent, not the branch");
    assert_eq!(publish("br-iso").await, 1, "the branch alone");

    // A label the parent's environment shares on request.
    let shared = branch(&node, &t.key, &id, json!({"name": "shr", "environment": "production", "events_of_parent": true})).await;
    ship_to(&node, &t.key, shared["id"].as_str().expect("id"), PUBSUB).await;
    assert_eq!(invoke(&node, &format!("{slug}-br-shr"), "/").await.s(), 200);
    assert_eq!(publish("production").await, 2);
    node.shutdown().await;
}
}

node_test! {
/// The sweep removes a branch past `expires_at` with its data, and nothing else. A past
/// `expires_at` is refused.
async fn expired_branches_are_swept() {
    let rec = Arc::new(Recorder::default());
    let extra_audit = Some(rec.clone() as Arc<dyn rt_sec::AuditSink>);
    let handle = new_handle();
    let node = start_node_opts(
        StartOptions {
            extra_audit,
            ..Default::default()
        },
        Some(handle.clone()),
        |_| {},
    )
    .await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = create_service(&node, &t.key, &slug, "deno").await;
    let now = chrono_ms();
    let past = post(&node, &t.key, &branches(&id), json!({"name": "pst", "expires_at": now - 1000})).await;
    assert_refused(&past, 422, "validation_failed");

    let soon = branch(&node, &t.key, &id, json!({"name": "soon", "expires_at": now + 1500})).await;
    let later = branch(&node, &t.key, &id, json!({"name": "later", "expires_at": now + 3_600_000})).await;
    let forever = branch(&node, &t.key, &id, json!({"name": "ever"})).await;
    assert_eq!(soon["scratch"]["expires_at"], json!(now + 1500), "{soon}");
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let server = handle.lock().expect("handle lock").clone().expect("running");
    server.run_periodic_for_test(600).await;
    drop(server);

    let left = slugs(&get(&node, &t.key, &branches(&id)).await);
    assert_eq!(left.len(), 2, "{left:?}");
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{}", soon["id"].as_str().expect("id"))).await.s(), 404);
    for kept in [&later, &forever] {
        assert_eq!(get(&node, &t.key, &format!("/v1/services/{}", kept["id"].as_str().expect("id"))).await.s(), 200);
    }
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{id}")).await.s(), 200);
    let ev = rec.of("scratch.deleted");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].actor, rt_sec::Actor::System);
    assert_eq!(ev[0].data["reason"], json!("expired"), "{:?}", ev[0]);
    let envs = get(&node, &t.key, "/v1/projects/default").await.json["environments"].clone();
    assert!(!envs.as_array().expect("list").contains(&json!("br-soon")), "{envs}");
    node.shutdown().await;
}
}

node_test! {
/// A branch does not get indexed; a service that is not one is untouched.
async fn branches_answer_noindex() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    let b = branch(&node, &t.key, &id, json!({"name": "web"})).await;
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), GUEST).await;
    let r = invoke(&node, &format!("{slug}-br-web"), "/").await;
    assert_eq!(r.header("x-robots-tag").as_deref(), Some("noindex"), "{}", r.text);
    let r = invoke(&node, &slug, "/").await;
    assert_eq!(r.header("x-robots-tag"), None);
    node.shutdown().await;
}
}

node_test! {
/// A schedule's run still in progress at the flip costs the new version a tick: the switch says so.
async fn a_running_schedule_warns_of_a_skipped_tick() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, SLOW_TICK).await;
    let b = branch(&node, &t.key, &id, json!({"name": "new"})).await;
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), GUEST).await;
    let path = format!("/v1/services/{id}/switch");
    let has_tick = |v: &Value| v["warnings"].as_array().is_some_and(|w| w.iter().any(|x| x["code"] == json!("schedule_tick_skipped")));

    let idle = post(&node, &t.key, &path, json!({"from": {"branch": "new"}, "dry_run": true})).await;
    assert_eq!(idle.s(), 200, "{}", idle.text);
    assert!(!has_tick(&idle.json), "nothing runs yet: {}", idle.text);

    let s = post(&node, &t.key, &format!("/v1/services/{id}/schedules"), json!({"kind": "interval", "every_seconds": 1, "payload": {}})).await;
    assert_eq!(s.s(), 201, "{}", s.text);
    let (node_ref, key, p) = (&node, &t.key, &path);
    eventually(Duration::from_secs(20), "the dry run to see the running tick", || async move {
        let r = post(node_ref, key, p, json!({"from": {"branch": "new"}, "dry_run": true})).await;
        r.json["warnings"].as_array().is_some_and(|w| w.iter().any(|x| x["code"] == json!("schedule_tick_skipped")))
    })
    .await;
    let done = post(&node, &t.key, &path, json!({"from": {"branch": "new"}})).await;
    assert_eq!(done.s(), 202, "{}", done.text);
    assert!(has_tick(&done.json), "{}", done.text);
    node.shutdown().await;
}
}

/// After a restart T1 holds nothing of the branch: only the switch's preload can bring its artifact
/// in, before the first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_switch_preloads_the_prepared_artifact() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    let bid = post(&node, &t.key, &branches(&id), json!({"name": "pl"})).await.json["id"]
        .as_str()
        .expect("id")
        .to_string();
    let up = upload_ts(
        &node,
        &t.key,
        &bid,
        "export default { fetch() { return new Response('pl'); } };",
        "activate=1&compile=eager",
    )
    .await;
    assert!(up.s() < 300, "{}", up.text);
    let vpath = format!("/v1/services/{bid}/versions/{}", up.json["version"]);
    for _ in 0..600 {
        if get(&node, &t.key, &vpath).await.json["build_state"] == json!("ready") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    node.restart().await;
    let view = get(&node, &t.key, &format!("/v1/services/{bid}")).await;
    assert_eq!(
        view.json["status"]["tier"].as_str().map(|t| t != "T1"),
        Some(true),
        "cold after the restart: {}",
        view.text
    );

    let r = post(
        &node,
        &t.key,
        &format!("/v1/services/{id}/switch"),
        json!({"from": {"branch": "pl"}}),
    )
    .await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let mut tier = Value::Null;
    for _ in 0..100 {
        tier = get(&node, &t.key, &format!("/v1/services/{id}")).await.json["status"]["tier"].clone();
        if tier == json!("T1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(tier, json!("T1"), "no request has run on it");
    node.stop().await;
}

fn chrono_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

node_test! {
/// A branch has timed triggers off by policy. That only matters once a schedule exists on either side.
async fn timed_triggers_differ_only_with_a_schedule() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("par");
    let id = ship(&node, &t.key, &slug, TICKS).await;
    let b = branch(&node, &t.key, &id, json!({"name": "new"})).await;
    ship_to(&node, &t.key, b["id"].as_str().expect("id"), TICKS).await;
    let path = format!("/v1/services/{id}/switch");
    let differ = |v: &Value| -> Option<Value> {
        let w = v["warnings"].as_array()?;
        w.iter().find(|x| x["code"] == json!("triggers_differ")).map(|x| x["names"].clone())
    };

    let dry = post(&node, &t.key, &path, json!({"from": {"branch": "new"}, "dry_run": true})).await;
    assert_eq!(dry.s(), 200, "{}", dry.text);
    assert_eq!(differ(&dry.json), None, "no schedules anywhere: {}", dry.text);

    let s = post(&node, &t.key, &format!("/v1/services/{id}/schedules"), json!({"kind": "interval", "every_seconds": 3600, "payload": {}})).await;
    assert_eq!(s.s(), 201, "{}", s.text);
    let dry = post(&node, &t.key, &path, json!({"from": {"branch": "new"}, "dry_run": true})).await;
    assert_eq!(differ(&dry.json), Some(json!(["interval"])), "{}", dry.text);
    node.shutdown().await;
}
}
