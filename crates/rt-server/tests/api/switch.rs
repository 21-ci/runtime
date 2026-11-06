//! `POST /v1/services/{s}/switch` and the switch routes (`docs/design/branching-services-design.md`
//! §4, §5, §7): a version moves between services by digest, only through keys that may touch both,
//! with stable codes, an audit trail, `dry_run`, `If-Match` and idempotent replay.
//!
//! Branch policy (`copy_data`, timed triggers, topics) comes with the branch primitive (T5).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Method;
use rt_api::promote::RouteRefresh;
use rt_api::ApiError;
use rt_server::StartOptions;
use rt_store::{MetaStore, PageReq, TenantScope};
use rt_testkit::NodeHarness;
use rt_types::{
    NewService, NewSwitch, RuntimeKind, ServiceConfig, ServiceId, Switch, SwitchKind, SwitchState, TenantId, VersionNo,
};
use serde_json::{json, Value};
use tokio::sync::watch;

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

/// Parks `promote()`'s route refresh while the gate is shut: a switch that gets there still holds
/// the service's flip lock, as it does from before adoption until it returns.
struct GatedRefresh {
    inner: Arc<dyn RouteRefresh>,
    open: watch::Receiver<bool>,
    parked: Arc<AtomicBool>,
}

#[async_trait]
impl RouteRefresh for GatedRefresh {
    async fn refresh(&self) -> Result<(), ApiError> {
        let mut open = self.open.clone();
        if !*open.borrow() {
            self.parked.store(true, Ordering::SeqCst);
            let _ = open.wait_for(|o| *o).await;
        }
        self.inner.refresh().await
    }
}

struct Rig {
    node: NodeHarness,
    t: Tenant,
    /// The service being switched, `production`, serving `parent`.
    parent: String,
    parent_slug: String,
    /// The tested service, `staging`, its version 1 built and live, serving `branch`.
    source: String,
    meta: Arc<dyn MetaStore>,
    audit: Arc<Recorder>,
    gate: watch::Sender<bool>,
    parked: Arc<AtomicBool>,
}

/// `/`: the tag. `/hold?ms=`: an old task; `/started` says it began.
fn guest(tag: &str) -> String {
    format!(
        r#"import {{ sql }} from "std:sql";
export default {{
  async fetch(req: Request): Promise<Response> {{
    const u = new URL(req.url);
    await sql.execute("CREATE TABLE IF NOT EXISTS h (id INTEGER PRIMARY KEY)");
    if (u.pathname === "/hold") {{
      await sql.execute("INSERT INTO h DEFAULT VALUES");
      await new Promise((r) => setTimeout(r, Number(u.searchParams.get("ms"))));
      return new Response("{tag}");
    }}
    if (u.pathname === "/started") {{
      const r = await sql.execute("SELECT count(*) FROM h");
      return new Response(String(r.rows[0][0]));
    }}
    return new Response("{tag}");
  }},
}};"#
    )
}

/// A built version: only `ready` may be switched to.
async fn wait_ready(node: &NodeHarness, key: &str, id: &str, v: u32) {
    let path = format!("/v1/services/{id}/versions/{v}");
    eventually(Duration::from_secs(60), "the version to build", || {
        let path = path.clone();
        async move { get(node, key, &path).await.json["build_state"] == json!("ready") }
    })
    .await;
}

async fn create_in(node: &NodeHarness, key: &str, slug: &str, env: &str, runtime: &str) -> String {
    let r = post(
        node,
        key,
        "/v1/services",
        json!({"slug": slug, "runtime": runtime, "environment": env}),
    )
    .await;
    assert_eq!(r.s(), 201, "{}", r.text);
    r.json["id"].as_str().expect("id").to_string()
}

async fn built(node: &NodeHarness, key: &str, id: &str, src: &str) {
    let up = upload_ts(node, key, id, src, "activate=1&compile=eager").await;
    assert!(up.s() < 300, "{}", up.text);
    wait_ready(node, key, id, up.json["version"].as_u64().expect("version") as u32).await;
}

async fn rig() -> Rig {
    let captured: Arc<OnceLock<Arc<dyn MetaStore>>> = Arc::new(OnceLock::new());
    let slot = captured.clone();
    let audit = Arc::new(Recorder::default());
    let sink: Arc<dyn rt_sec::AuditSink> = audit.clone();
    let (gate, open) = watch::channel(true);
    let parked = Arc::new(AtomicBool::new(false));
    let seen = parked.clone();
    let opts = StartOptions {
        meta_layer: Some(Box::new(move |m| {
            let _ = slot.set(m.clone());
            m
        })),
        extra_audit: Some(sink),
        route_refresh_layer: Some(Box::new(move |inner| {
            Arc::new(GatedRefresh {
                inner,
                open,
                parked: seen,
            }) as Arc<dyn RouteRefresh>
        })),
        ..StartOptions::default()
    };
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    allow_environments(&node, &t.key, &["staging"]).await;
    let parent_slug = uniq("par");
    let parent = create_in(&node, &t.key, &parent_slug, "production", "deno").await;
    built(&node, &t.key, &parent, &guest("parent")).await;
    let source = create_in(&node, &t.key, &uniq("src"), "staging", "deno").await;
    built(&node, &t.key, &source, &guest("branch")).await;
    let meta = captured.get().expect("the meta layer ran").clone();
    Rig {
        node,
        t,
        parent,
        parent_slug,
        source,
        meta,
        audit,
        gate,
        parked,
    }
}

impl Rig {
    fn path(&self) -> String {
        format!("/v1/services/{}/switch", self.parent)
    }

    fn source_body(&self) -> Value {
        json!({"from": {"service": self.source}})
    }

    async fn switch(&self, key: &str, body: Value) -> Resp {
        post(&self.node, key, &self.path(), body).await
    }

    async fn serves(&self) -> String {
        invoke(&self.node, &self.parent_slug, "/").await.text
    }

    fn svc(&self) -> ServiceId {
        self.parent.parse().expect("id")
    }

    async fn rows(&self) -> Vec<Switch> {
        let page = PageReq {
            limit: 100,
            cursor: None,
        };
        self.meta.list_switches(self.svc(), page).await.expect("rows").items
    }

    async fn version_count(&self) -> usize {
        self.meta.list_versions(self.svc()).await.expect("versions").len()
    }

    fn audited(&self, action: &str) -> Vec<rt_sec::AuditEvent> {
        let events = self.audit.events.lock().expect("lock");
        events.iter().filter(|e| e.action == action).cloned().collect()
    }

    async fn digest_of(&self, id: &str, v: u32) -> Value {
        let r = get(&self.node, &self.t.key, &format!("/v1/services/{id}/versions/{v}")).await;
        r.json["source_digest"].clone()
    }
}

fn assert_refused(r: &Resp, status: u16, code: &str) {
    assert_eq!((r.s(), r.code().as_str()), (status, code), "{}", r.text);
}

node_test! {
/// The whole path: the parent runs the bytes that were tested, the row says so, and the audit
/// names digest and actor. The digest in the answer comes from the source row.
async fn a_switch_adopts_by_digest_flips_and_is_tracked_and_audited() {
    let r = rig().await;
    let resp = r.switch(&r.t.key, r.source_body()).await;
    assert_eq!(resp.s(), 202, "{}", resp.text);
    let digest = r.digest_of(&r.source, 1).await;
    assert_eq!(resp.json["kind"], json!("switch"));
    assert_eq!(resp.json["from_version"], json!(1));
    assert_eq!(resp.json["to_version"], json!(2));
    assert_eq!(resp.json["source"], json!({"service": r.source, "version": 1, "digest": digest}));
    assert_eq!(r.digest_of(&r.parent, 2).await, digest, "same bytes");
    assert_eq!(r.serves().await, "branch");

    let id = resp.json["id"].as_str().expect("id");
    let one = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{id}", r.parent)).await;
    assert_eq!(one.s(), 200, "{}", one.text);
    assert_eq!(one.json["source"]["digest"], digest);
    let list = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches?limit=1", r.parent)).await;
    assert_eq!(list.s(), 200, "{}", list.text);
    assert_eq!(list.items()[0]["id"], json!(id), "newest first");
    assert!(list.json["next_cursor"].is_string(), "the upload's flip is on the next page");

    let events = r.audited("switch");
    let ev = events.last().expect("audited");
    assert_eq!(ev.data["digest"], digest);
    assert_eq!(ev.data["source_service"], json!(r.source));
    assert_eq!(ev.data["to"], json!(2));
    assert!(ev.data.get("git_sha").is_some(), "{}", ev.data);
    let actor = serde_json::to_string(&ev.actor).expect("actor");
    assert!(actor.contains(&r.t.key_id), "{actor}");
    r.node.shutdown().await;
}
}

node_test! {
async fn switching_the_same_source_version_twice_reuses_the_adopted_row() {
    let r = rig().await;
    assert_eq!(r.switch(&r.t.key, r.source_body()).await.json["to_version"], json!(2));
    let back = put(&r.node, &r.t.key, &format!("/v1/services/{}/live", r.parent), json!({"version": 1})).await;
    assert_eq!(back.s(), 200, "{}", back.text);
    let again = r.switch(&r.t.key, r.source_body()).await;
    assert_eq!((again.s(), again.json["to_version"].clone()), (202, json!(2)), "{}", again.text);
    assert_eq!(r.version_count().await, 2);
    r.node.shutdown().await;
}
}

node_test! {
/// `{version}` promotes one of the parent's own versions (no adoption), `{service, version}` names
/// a source version, and without a version the source's live one is meant.
async fn every_form_of_from_works() {
    let r = rig().await;
    let up = upload_ts(&r.node, &r.t.key, &r.source, &guest("branch2"), "activate=1&compile=eager").await;
    assert!(up.s() < 300, "{}", up.text);
    wait_ready(&r.node, &r.t.key, &r.source, 2).await;

    let old = r.switch(&r.t.key, json!({"from": {"service": r.source, "version": 1}})).await;
    assert_eq!(old.s(), 202, "{}", old.text);
    assert_eq!(old.json["source"]["version"], json!(1));
    assert_eq!(r.serves().await, "branch");
    let live = r.switch(&r.t.key, r.source_body()).await;
    assert_eq!(live.json["source"]["version"], json!(2), "{}", live.text);
    assert_eq!(r.serves().await, "branch2");

    let own = r.switch(&r.t.key, json!({"from": {"version": 1}})).await;
    assert_eq!(own.s(), 202, "{}", own.text);
    assert!(own.json.get("source").is_none(), "{}", own.text);
    assert_eq!(r.serves().await, "parent");
    // The service itself named as the source is its own version too.
    let own = r.switch(&r.t.key, json!({"from": {"service": r.parent, "version": 2}})).await;
    assert_eq!(own.s(), 202, "{}", own.text);
    assert!(own.json.get("source").is_none(), "{}", own.text);

    // `{branch}` names `<parent-slug>-br-<name>`.
    let made = post(&r.node, &r.t.key, &format!("/v1/services/{}/branches", r.parent), json!({"name": "staging"})).await;
    assert_eq!(made.s(), 201, "{}", made.text);
    let branch = made.json["id"].as_str().expect("id").to_string();
    built(&r.node, &r.t.key, &branch, &guest("named")).await;
    let named = r.switch(&r.t.key, json!({"from": {"branch": "staging"}})).await;
    assert_eq!(named.json["source"]["service"], json!(branch), "{}", named.text);
    assert_eq!(r.serves().await, "named");
    let none = r.switch(&r.t.key, json!({"from": {"branch": "nope"}})).await;
    assert_refused(&none, 404, "not_found");
    r.node.shutdown().await;
}
}

/// A deadline is at most `deploy.drain_max` away, whatever the switch asked for or the service's
/// longest task would need.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_drain_deadline_is_capped_by_deploy_drain_max() {
    let node = start_node_with(|c| c.deploy.drain_max = Duration::from_secs(2)).await;
    let t = new_tenant(&node, "pro").await;
    let id = create_service(&node, &t.key, &uniq("cap"), "deno").await;
    built(&node, &t.key, &id, &guest("v1")).await;
    built(&node, &t.key, &id, &guest("v2")).await;
    let first = get(&node, &t.key, &format!("/v1/services/{id}")).await;
    let live = first.json["live_version"].as_u64().expect("live");
    let path = format!("/v1/services/{id}/switch");
    for (asked, version) in [(json!("1h"), live - 1), (Value::Null, live)] {
        let mut body = json!({"from": {"version": version}});
        if !asked.is_null() {
            body["drain_timeout"] = asked;
        }
        let up = post(&node, &t.key, &path, body).await;
        assert_eq!(up.s(), 202, "{}", up.text);
        let room =
            up.json["drain_deadline"].as_i64().expect("deadline") - up.json["updated_at"].as_i64().expect("time");
        assert!((0..=2_000).contains(&room), "{room}: {}", up.text);
    }
    // Under the cap the request is honoured.
    let up = post(
        &node,
        &t.key,
        &path,
        json!({"from": {"version": live - 1}, "drain_timeout": "1s"}),
    )
    .await;
    assert_eq!(up.s(), 202, "{}", up.text);
    let room = up.json["drain_deadline"].as_i64().expect("deadline") - up.json["updated_at"].as_i64().expect("time");
    assert!((0..=1_000).contains(&room), "{room}: {}", up.text);
    node.shutdown().await;
}

node_test! {
/// `dry_run` reports what a switch would do and writes nothing at all: no version, no row, no flip.
/// Names only: a value of an env variable or secret never appears.
async fn dry_run_reports_the_diff_and_changes_nothing() {
    let r = rig().await;
    let env = put(&r.node, &r.t.key, &format!("/v1/services/{}/env", r.source), json!({"ONLY_ON_BRANCH": "env-value"})).await;
    assert_eq!(env.s(), 200, "{}", env.text);
    let sec = put(&r.node, &r.t.key, &format!("/v1/services/{}/secrets/BRANCH_KEY", r.source), json!({"value": "secret-value"})).await;
    assert_eq!(sec.s(), 204, "{}", sec.text);
    let sch = post(&r.node, &r.t.key, &format!("/v1/services/{}/schedules", r.source), json!({"kind": "cron", "cron": "0 0 1 1 *", "payload": {}})).await;
    assert_eq!(sch.s(), 201, "{}", sch.text);
    let pat = patch(&r.node, &r.t.key, &format!("/v1/services/{}", r.source), json!({"triggers": {"cron": false}})).await;
    assert_eq!(pat.s(), 200, "{}", pat.text);
    let rows = r.rows().await.len();

    let mut body = r.source_body();
    body["dry_run"] = json!(true);
    let resp = r.switch(&r.t.key, body).await;
    assert_eq!(resp.s(), 200, "{}", resp.text);
    assert_eq!(resp.json["dry_run"], json!(true));
    assert!(resp.json.get("id").is_none(), "{}", resp.text);
    assert!(resp.json.get("to_version").is_none(), "not adopted yet: {}", resp.text);
    assert_eq!(resp.json["from_version"], json!(1));
    assert_eq!(resp.json["source"]["digest"], r.digest_of(&r.source, 1).await);
    assert_eq!(
        resp.json["warnings"],
        json!([
            {"code": "env_missing_on_parent", "names": ["BRANCH_KEY", "ONLY_ON_BRANCH"]},
            {"code": "triggers_differ", "names": ["cron"]},
        ]),
        "{}",
        resp.text
    );
    assert!(!resp.text.contains("env-value") && !resp.text.contains("secret-value"));
    assert_eq!(r.rows().await.len(), rows);
    assert_eq!(r.version_count().await, 1);
    assert_eq!(r.serves().await, "parent");
    assert!(r.audited("switch").is_empty());

    // Once adopted, a dry run can name the number; the real switch carries the warnings too.
    let real = r.switch(&r.t.key, r.source_body()).await;
    assert_eq!(real.json["warnings"][0]["code"], json!("env_missing_on_parent"), "{}", real.text);
    let mut body = r.source_body();
    body["dry_run"] = json!(true);
    assert_eq!(r.switch(&r.t.key, body).await.json["to_version"], json!(2));
    r.node.shutdown().await;
}
}

node_test! {
/// A stale `If-Match` refuses before anything is written; a fresh one passes; dry runs honor it.
async fn if_match_guards_the_switch() {
    let r = rig().await;
    let rows = r.rows().await.len();
    for body in [r.source_body(), json!({"from": {"service": r.source}, "dry_run": true})] {
        let stale = send(
            api(&r.node, &r.t.key, Method::POST, &r.path()).header("if-match", "\"999999\"").json(&body),
        )
        .await;
        assert_refused(&stale, 412, "precondition_failed");
    }
    assert_eq!((r.rows().await.len(), r.version_count().await), (rows, 1));
    assert_eq!(r.serves().await, "parent");

    let malformed = send(
        api(&r.node, &r.t.key, Method::POST, &r.path()).header("if-match", "nonsense").json(&r.source_body()),
    )
    .await;
    assert_refused(&malformed, 422, "validation_failed");

    let cur = get(&r.node, &r.t.key, &format!("/v1/services/{}", r.parent)).await;
    let etag = cur.json["etag"].as_str().expect("etag").to_string();
    let fresh = send(
        api(&r.node, &r.t.key, Method::POST, &r.path()).header("if-match", etag).json(&r.source_body()),
    )
    .await;
    assert_eq!(fresh.s(), 202, "{}", fresh.text);
    r.node.shutdown().await;
}
}

node_test! {
async fn an_idempotency_key_replays_the_switch_and_refuses_another_body() {
    let r = rig().await;
    let keyed = |body: &Value| {
        api(&r.node, &r.t.key, Method::POST, &r.path()).header("idempotency-key", "sw-1").json(body)
    };
    let first = send(keyed(&r.source_body())).await;
    assert_eq!(first.s(), 202, "{}", first.text);
    let rows = r.rows().await.len();
    let replay = send(keyed(&r.source_body())).await;
    assert_eq!(replay.s(), 202, "{}", replay.text);
    assert_eq!(replay.json["id"], first.json["id"]);
    assert_eq!(r.rows().await.len(), rows, "no second flip");
    assert_eq!(r.audited("switch").len(), 1);

    let other = send(keyed(&json!({"from": {"version": 1}}))).await;
    assert_refused(&other, 422, "idempotency_mismatch");
    r.node.shutdown().await;
}
}

node_test! {
/// §7: switching is decided on the parent. A key needs `deploy`, needs to see the source, and a
/// key that may only touch production does not see a staging source.
async fn switch_needs_deploy_on_parent_not_on_branch() {
    let r = rig().await;
    let (_, read_only) = make_key(&r.node, &r.t.id, &["services:read"], None).await;
    assert_refused(&r.switch(&read_only, r.source_body()).await, 403, "insufficient_scope");
    let (_, deploy_only) = make_key(&r.node, &r.t.id, &["deploy"], None).await;
    assert_refused(&r.switch(&deploy_only, r.source_body()).await, 403, "insufficient_scope");

    let (_, prod) = make_key_in(&r.node, &r.t.id, &["deploy", "services:read"], None, Some(vec!["production".into()])).await;
    assert_refused(&r.switch(&prod, r.source_body()).await, 404, "not_found");
    // The parent's own version needs no read of anything else.
    assert_eq!(r.switch(&deploy_only, json!({"from": {"version": 1}})).await.s(), 202);

    let (_, both) = make_key(&r.node, &r.t.id, &["deploy", "services:read"], None).await;
    assert_eq!(r.switch(&both, r.source_body()).await.s(), 202);
    r.node.shutdown().await;
}
}

node_test! {
async fn staging_scoped_key_gets_404_on_parent_switch() {
    let r = rig().await;
    let (_, staging) = make_key_in(&r.node, &r.t.id, &["deploy", "services:read"], None, Some(vec!["staging".into()])).await;
    assert_refused(&r.switch(&staging, r.source_body()).await, 404, "not_found");
    let rollback = post(&r.node, &staging, &format!("/v1/services/{}/switches/swt_00000000000000000000000000/rollback", r.parent), json!({})).await;
    assert_refused(&rollback, 404, "not_found");
    let list = get(&r.node, &staging, &format!("/v1/services/{}/switches", r.parent)).await;
    assert_refused(&list, 404, "not_found");
    assert_eq!(r.serves().await, "parent");
    r.node.shutdown().await;
}
}

node_test! {
/// A source of another tenant or project answers exactly like a source that does not exist.
async fn switch_from_other_tenant_service_is_404() {
    let r = rig().await;
    let other = new_tenant(&r.node, "pro").await;
    let theirs = ship(&r.node, &other.key, &uniq("oth"), &guest("theirs")).await;
    let cross = r.switch(&r.t.key, json!({"from": {"service": theirs}})).await;
    let absent = r.switch(&r.t.key, json!({"from": {"service": ServiceId::new().to_string()}})).await;
    assert_refused(&cross, 404, "not_found");
    assert_eq!(cross.text, absent.text, "existence is not revealed");
    // The other way round: they do not see the parent.
    let theirs_key = r.switch(&other.key, json!({"from": {"version": 1}})).await;
    assert_refused(&theirs_key, 404, "not_found");

    let p = post(&r.node, &r.t.key, "/v1/projects", json!({"slug": uniq("prj"), "name": "Other"})).await;
    assert_eq!(p.s(), 201, "{}", p.text);
    let there = post(&r.node, &r.t.key, "/v1/services", json!({"slug": uniq("inp"), "runtime": "deno", "project": p.json["slug"]})).await;
    assert_eq!(there.s(), 201, "{}", there.text);
    let id = there.json["id"].as_str().expect("id").to_string();
    built(&r.node, &r.t.key, &id, &guest("there")).await;
    assert_refused(&r.switch(&r.t.key, json!({"from": {"service": id}})).await, 404, "not_found");
    assert_eq!(r.version_count().await, 1);
    r.node.shutdown().await;
}
}

node_test! {
async fn switch_body_never_accepts_a_digest() {
    let r = rig().await;
    let digest = r.digest_of(&r.source, 1).await;
    for body in [
        json!({"from": {"service": r.source, "digest": digest}}),
        json!({"from": {"service": r.source}, "digest": digest}),
        json!({"from": {"digest": digest}}),
        json!({"from": {"branch": "x", "service": r.source}}),
        json!({}),
    ] {
        assert_refused(&r.switch(&r.t.key, body).await, 422, "validation_failed");
    }
    assert_eq!(r.version_count().await, 1);
    r.node.shutdown().await;
}
}

node_test! {
/// `sys-` services take part in no switch, and the system tenant is refused before that.
async fn switch_refuses_system_services() {
    let r = rig().await;
    let tenant: TenantId = r.t.id.parse().expect("tenant");
    let new = |slug: String| NewService {
        slug,
        runtime: RuntimeKind::Deno,
        name: None,
        profile: None,
        environment: "production".into(),
        project: "default".into(),
        config: ServiceConfig::default(),
    };
    // No route creates these in a tenant; only the rule is under test.
    let sys = r
        .meta
        .create_service(TenantScope::Tenant(tenant), tenant, new(uniq("sys-x")))
        .await
        .expect("create")
        .id
        .to_string();
    let as_source = r.switch(&r.t.key, json!({"from": {"service": sys, "version": 1}})).await;
    assert_refused(&as_source, 403, "scratch_source_reserved");
    let as_parent = post(&r.node, &r.t.key, &format!("/v1/services/{sys}/switch"), r.source_body()).await;
    assert_refused(&as_parent, 403, "reserved");

    let tenant = rt_types::system_tenant().to_string();
    let system = call(root_for(&r.node, &tenant, Method::POST, &r.path()), Some(r.source_body())).await;
    assert_refused(&system, 403, "reserved");
    r.node.shutdown().await;
}
}

node_test! {
/// A live switch holds the service from before adoption until it returns, so a second one answers
/// at once, whichever step the first is at. The first is parked in its route refresh: that is the
/// only point of the critical section a test can hold without a hook in production code.
async fn second_switch_during_preparing_is_409() {
    let r = rig().await;
    r.gate.send(false).expect("gate");
    let first = r.switch(&r.t.key, r.source_body());
    let second = async {
        eventually(Duration::from_secs(10), "the first switch to reach its refresh", || async {
            r.parked.load(Ordering::SeqCst)
        })
        .await;
        let rows = r.rows().await.len();
        let resp = tokio::time::timeout(Duration::from_secs(5), r.switch(&r.t.key, json!({"from": {"version": 1}})))
            .await
            .expect("a live switch answers 409 at once, it does not wait");
        assert_refused(&resp, 409, "switch_in_progress");
        let list = format!("/v1/services/{}/switches/{}/rollback", r.parent, r.rows().await[0].id);
        let back = tokio::time::timeout(Duration::from_secs(5), post(&r.node, &r.t.key, &list, json!({})))
            .await
            .expect("so does a rollback");
        assert_refused(&back, 409, "switch_in_progress");
        assert_eq!(r.rows().await.len(), rows, "the refused ones wrote nothing");
        r.gate.send(true).expect("gate");
    };
    let (first, ()) = tokio::join!(first, second);
    assert_eq!(first.s(), 202, "{}", first.text);
    assert_eq!(r.serves().await, "branch");
    assert_eq!(r.switch(&r.t.key, json!({"from": {"version": 1}})).await.s(), 202, "free again");
    r.node.shutdown().await;
}
}

node_test! {
/// The other side of the same rule: with no switch running, an open row is a leftover of a failed
/// store write. It must not fake a conflict.
async fn a_stale_open_row_is_closed_and_does_not_block() {
    let r = rig().await;
    let stale = r
        .meta
        .create_switch(
            r.svc(),
            NewSwitch {
                kind: SwitchKind::Promote,
                source: None,
                from_version: None,
                to_version: VersionNo(1),
                created_by: None,
            },
        )
        .await
        .expect("open row");
    let resp = r.switch(&r.t.key, r.source_body()).await;
    assert_eq!(resp.s(), 202, "{}", resp.text);
    let row = r.meta.get_switch(r.svc(), stale.id).await.expect("get").expect("row");
    assert_eq!((row.state, row.error.as_deref()), (SwitchState::Failed, Some("interrupted")));
    r.node.shutdown().await;
}
}

node_test! {
/// A drain is no conflict: old tasks must not hold up a rollback, up to `drain_max`.
async fn a_rollback_does_not_wait_for_a_drain() {
    let r = rig().await;
    let hold = invoke(&r.node, &r.parent_slug, "/hold?ms=3000");
    let body = async {
        let (n, s) = (&r.node, &r.parent_slug);
        eventually(Duration::from_secs(20), "the hold to start", || async move {
            invoke(n, s, "/started").await.text == "1"
        })
        .await;
        let sw = r.switch(&r.t.key, r.source_body()).await;
        assert_eq!((sw.s(), sw.json["state"].clone()), (202, json!("draining")), "{}", sw.text);
        assert_eq!(sw.json["inflight_old"], json!(1), "{}", sw.text);
        let id = sw.json["id"].as_str().expect("id");
        let back = post(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{id}/rollback", r.parent), json!({})).await;
        assert_eq!(back.s(), 202, "{}", back.text);
        assert_eq!(r.serves().await, "parent");
    };
    let (held, ()) = tokio::join!(hold, body);
    assert_eq!(held.text, "parent");
    r.node.shutdown().await;
}
}

node_test! {
async fn rollback_returns_to_the_previous_version_as_a_new_switch() {
    let r = rig().await;
    let sw = r.switch(&r.t.key, r.source_body()).await;
    let id = sw.json["id"].as_str().expect("id").to_string();
    let path = format!("/v1/services/{}/switches/{id}/rollback", r.parent);

    let (_, reader) = make_key(&r.node, &r.t.id, &["services:read"], None).await;
    assert_refused(&post(&r.node, &reader, &path, json!({})).await, 403, "insufficient_scope");
    let other = new_tenant(&r.node, "pro").await;
    assert_refused(&post(&r.node, &other.key, &path, json!({})).await, 404, "not_found");

    let back = post(&r.node, &r.t.key, &path, json!({})).await;
    assert_eq!(back.s(), 202, "{}", back.text);
    assert_eq!(back.json["kind"], json!("rollback"));
    assert_eq!((back.json["from_version"].clone(), back.json["to_version"].clone()), (json!(2), json!(1)));
    assert_ne!(back.json["id"], sw.json["id"]);
    assert_eq!(r.serves().await, "parent");
    let ev = r.audited("rollback");
    let ev = ev.last().expect("audited");
    assert_eq!(ev.data["switch"], json!(id));
    assert_eq!(ev.data["digest"], r.digest_of(&r.parent, 1).await);

    // The first flip had nothing before it.
    let first = r.rows().await.into_iter().find(|x| x.from_version.is_none()).expect("first");
    let gone = post(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{}/rollback", r.parent, first.id), json!({})).await;
    assert_refused(&gone, 409, "rollback_target_gone");
    let bad = post(&r.node, &r.t.key, &format!("/v1/services/{}/switches/nonsense/rollback", r.parent), json!({})).await;
    assert_refused(&bad, 404, "not_found");
    let one = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches/nonsense", r.parent)).await;
    assert_refused(&one, 404, "not_found");
    let absent = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{}", r.source, first.id)).await;
    assert_refused(&absent, 404, "not_found");

    // Replay of the same rollback.
    let keyed = || api(&r.node, &r.t.key, Method::POST, &path).header("idempotency-key", "rb-1");
    let a = send(keyed()).await;
    let b = send(keyed()).await;
    assert_eq!(a.json["id"], b.json["id"], "{}", a.text);
    r.node.shutdown().await;
}
}

node_test! {
/// Every refusal is a code from the documented table, and no text from the machine.
async fn switch_errors_are_stable_codes() {
    let r = rig().await;
    let wasm = create_in(&r.node, &r.t.key, &uniq("wsm"), "production", "wasm").await;
    let empty = create_in(&r.node, &r.t.key, &uniq("emp"), "production", "deno").await;
    let pending = upload_ts(&r.node, &r.t.key, &r.source, &guest("pending"), "").await;
    assert!(pending.s() < 300, "{}", pending.text);

    let cases: Vec<(Value, u16, &str)> = vec![
        (json!({"from": {"service": wasm, "version": 1}}), 422, "runtime_mismatch"),
        (json!({"from": {"service": empty}}), 409, "version_not_ready"),
        (json!({"from": {"service": r.source, "version": 2}}), 409, "version_not_ready"),
        (json!({"from": {"service": r.source, "version": 99}}), 404, "not_found"),
        (json!({"from": {"version": 99}}), 404, "not_found"),
        (json!({"from": {"branch": "Not Valid"}}), 422, "branch_name_invalid"),
        (json!({"from": {"service": "../../etc/passwd"}}), 404, "not_found"),
        (json!({"from": {"service": r.source}, "drain_timeout": "soon"}), 422, "validation_failed"),
        (json!({"from": {"service": r.source}, "drain_timeout": "0s"}), 422, "validation_failed"),
    ];
    for (body, status, code) in cases {
        let resp = r.switch(&r.t.key, body.clone()).await;
        assert_refused(&resp, status, code);
        assert!(!resp.text.contains("/Users") && !resp.text.contains("SQLITE") && !resp.text.contains(".db"), "{}", resp.text);
    }
    assert_eq!(r.version_count().await, 1);
    r.node.shutdown().await;
}
}

node_test! {
/// `drain_timeout` shortens the wait, and is capped.
async fn drain_timeout_sets_the_deadline() {
    let r = rig().await;
    let mut body = r.source_body();
    body["drain_timeout"] = json!("5m");
    let hold = invoke(&r.node, &r.parent_slug, "/hold?ms=1500");
    let run = async {
        let (n, s) = (&r.node, &r.parent_slug);
        eventually(Duration::from_secs(20), "the hold to start", || async move {
            invoke(n, s, "/started").await.text == "1"
        })
        .await;
        let sw = r.switch(&r.t.key, body).await;
        assert_eq!(sw.s(), 202, "{}", sw.text);
        let wait = sw.json["drain_deadline"].as_i64().expect("deadline") - sw.json["created_at"].as_i64().expect("at");
        assert!((290_000..=310_000).contains(&wait), "{wait}");
        let back = r.switch(&r.t.key, json!({"from": {"version": 1}, "drain_timeout": "100h"})).await;
        let wait = back.json["drain_deadline"].as_i64().expect("deadline") - back.json["created_at"].as_i64().expect("at");
        assert!(wait <= 15 * 60 * 1000 + 10_000, "{wait}");
    };
    let _ = tokio::join!(hold, run);
    r.node.shutdown().await;
}
}

node_test! {
/// `close_sockets` is the same switch as on `PUT /live`, and is audited as a count.
async fn close_sockets_is_reported_in_the_audit() {
    let r = rig().await;
    let mut body = r.source_body();
    body["close_sockets"] = json!(true);
    assert_eq!(r.switch(&r.t.key, body).await.s(), 202);
    let ev = r.audited("switch");
    assert_eq!(ev.last().expect("audited").data["sockets_closed"], json!(0));
    r.node.shutdown().await;
}
}

node_test! {
/// The answers match the published schemas.
async fn switch_answers_validate_against_the_openapi_document() {
    use crate::common::openapi::{response_schema, validate};
    let r = rig().await;
    let doc = get(&r.node, &r.t.key, "/v1/openapi.json").await.json;
    let check = |route: &str, method: &str, status: u16, v: &Value| {
        let op = &doc["paths"][route][method];
        let schema = response_schema(op, status).flatten().expect("a schema");
        let mut errs = Vec::new();
        validate(&doc, schema, v, route, &mut errs);
        assert!(errs.is_empty(), "{route}: {errs:?} in {v}");
    };
    let mut dry = r.source_body();
    dry["dry_run"] = json!(true);
    let resp = r.switch(&r.t.key, dry).await;
    check("/v1/services/{s}/switch", "post", 200, &resp.json);
    let resp = r.switch(&r.t.key, r.source_body()).await;
    check("/v1/services/{s}/switch", "post", 202, &resp.json);
    let id = resp.json["id"].as_str().expect("id");
    let one = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{id}", r.parent)).await;
    check("/v1/services/{s}/switches/{id}", "get", 200, &one.json);
    let list = get(&r.node, &r.t.key, &format!("/v1/services/{}/switches", r.parent)).await;
    check("/v1/services/{s}/switches", "get", 200, &list.json);
    let back = post(&r.node, &r.t.key, &format!("/v1/services/{}/switches/{id}/rollback", r.parent), json!({})).await;
    check("/v1/services/{s}/switches/{id}/rollback", "post", 202, &back.json);
    r.node.shutdown().await;
}
}
