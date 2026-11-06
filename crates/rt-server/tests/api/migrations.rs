//! SQL migrations in `promote()` (R6): they run in `migrating`, before the flip; failure leaves the
//! live version and the schema alone.

use rt_types::{SwitchState, Version};
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

const CODE: &str = r#"import { sql } from "std:sql";
export default {
  async fetch(req: Request): Promise<Response> {
    const q = new URL(req.url).searchParams.get("q") ?? "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_rt_%' ORDER BY name";
    const r = await sql.execute(q);
    return Response.json(r.rows.map((x: unknown[]) => x[0]));
  },
};"#;

/// A JSON deploy payload: the code and `migrations/<name>`.
fn payload(tag: &str, migrations: &[(&str, &str)]) -> Vec<u8> {
    let mut files = serde_json::Map::new();
    files.insert("main.ts".into(), json!(format!("{CODE}\n// {tag}")));
    for (name, sql) in migrations {
        files.insert(format!("migrations/{name}"), json!(sql));
    }
    serde_json::to_vec(&json!({"files": files})).expect("json")
}

async fn add(node: &impl Node, key: &str, id: &str, tag: &str, migrations: &[(&str, &str)]) -> Resp {
    upload(node, key, id, "application/json", payload(tag, migrations), "").await
}

async fn add_ok(node: &impl Node, key: &str, id: &str, tag: &str, migrations: &[(&str, &str)]) -> u32 {
    let r = add(node, key, id, tag, migrations).await;
    assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
    r.json["version"].as_u64().expect("version") as u32
}

async fn live(node: &impl Node, key: &str, id: &str, body: Value) -> Resp {
    put(node, key, &format!("/v1/services/{id}/live"), body).await
}

async fn status(node: &impl Node, key: &str, id: &str) -> Value {
    let r = get(node, key, &format!("/v1/services/{id}/migrations")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    r.json
}

fn names(v: &Value) -> Vec<String> {
    v.as_array()
        .expect("array")
        .iter()
        .map(|x| x["name"].as_str().or(x.as_str()).expect("name").to_string())
        .collect()
}

async fn tables(node: &impl Node, slug: &str) -> Vec<String> {
    let r = invoke(node, slug, "/").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    names(&r.json)
}

async fn setup(node: &impl Node, key: &str, prefix: &str) -> (String, String) {
    let slug = uniq(prefix);
    let id = ship(node, key, &slug, CODE).await;
    (id, slug)
}

node_test! {
    async fn a_promote_runs_pending_migrations_before_the_flip() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mg").await;
        let v = add_ok(&node, &t.key, &id, "v2", &[("0001_a.sql", "CREATE TABLE a (x);"), ("0002_b.sql", "CREATE TABLE b (x);")]).await;
        let got = get(&node, &t.key, &format!("/v1/services/{id}/versions/{v}")).await;
        assert_eq!(names(&got.json["migrations"]), ["0001_a", "0002_b"]);
        assert_eq!(names(&status(&node, &t.key, &id).await["pending_for_latest"]), ["0001_a", "0002_b"]);

        let r = live(&node, &t.key, &id, json!({"version": v})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(names(&r.json["migrations_applied"]), ["0001_a", "0002_b"]);
        assert_eq!(r.json["live_version"], json!(v));
        assert_eq!(tables(&node, &slug).await, ["a", "b"]);
        let st = status(&node, &t.key, &id).await;
        assert_eq!(names(&st["applied"]), ["0001_a", "0002_b"]);
        assert_eq!(st["applied"][0]["version"], json!(v));
        assert!(names(&st["pending_for_latest"]).is_empty());

        // Promoting again runs nothing.
        let r = live(&node, &t.key, &id, json!({"version": v})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert!(r.json.get("migrations_applied").is_none());
        node.shutdown().await;
    }
}

node_test! {
    /// Failure in file 2 of 3: schema, `_rt_migrations` and live are unchanged, the error names the
    /// spot, the switch row is `failed`, and no driver path appears.
    async fn a_failed_migration_changes_nothing_and_names_the_statement() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mgf").await;
        let before = get(&node, &t.key, &format!("/v1/services/{id}")).await.json["live_version"].clone();
        let v = add_ok(&node, &t.key, &id, "v2", &[
            ("0001_a.sql", "CREATE TABLE a (x);"),
            ("0002_b.sql", "CREATE TABLE b (x);\nINSERT INTO nope VALUES (1);"),
            ("0003_c.sql", "CREATE TABLE c (x);"),
        ]).await;
        let r = live(&node, &t.key, &id, json!({"version": v})).await;
        assert_eq!(r.s(), 409, "{}", r.text);
        assert_eq!(r.json["error"]["code"], "migration_failed");
        let d = &r.json["error"]["details"];
        assert_eq!(d["file"], "0002_b.sql");
        assert_eq!(d["statement_index"], 1);
        assert_eq!(d["line"], 2);
        assert_eq!(d["sqlite_code"], 1);
        assert!(d["message"].as_str().expect("message").contains("nope"));
        assert!(!r.text.contains("data.db"), "{}", r.text);

        assert_eq!(get(&node, &t.key, &format!("/v1/services/{id}")).await.json["live_version"], before);
        assert!(tables(&node, &slug).await.is_empty());
        assert!(names(&status(&node, &t.key, &id).await["applied"]).is_empty());
        let sw = get(&node, &t.key, &format!("/v1/services/{id}/switches")).await;
        assert_eq!(sw.json["items"][0]["state"], json!("failed"));
        assert_eq!(sw.json["items"][0]["error"], json!("migration_failed"));
        node.shutdown().await;
    }
}

node_test! {
    async fn hostile_migrations_are_refused_through_the_guest_authorizer() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mgh").await;
        for (i, sql) in ["SELECT * FROM _rt_migrations;", "PRAGMA writable_schema=1;", "ATTACH '/etc/passwd' AS p;"].iter().enumerate() {
            let tag = format!("h{i}");
            let v = add_ok(&node, &t.key, &id, &tag, &[("0001_h.sql", sql)]).await;
            let r = live(&node, &t.key, &id, json!({"version": v})).await;
            assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migration_failed")), "{}", r.text);
            assert!(!r.json["error"]["details"]["message"].as_str().expect("m").contains('/'), "{}", r.text);
        }
        let v = add_ok(&node, &t.key, &id, "tx", &[("0001_h.sql", "BEGIN;")]).await;
        let r = live(&node, &t.key, &id, json!({"version": v})).await;
        assert_eq!(r.json["error"]["code"], "transaction_control_forbidden", "{}", r.text);
        node.shutdown().await;
    }
}

node_test! {
    async fn drift_and_order_are_conflicts_and_an_older_version_can_go_back() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mgd").await;
        let old = add_ok(&node, &t.key, &id, "old", &[("0002_b.sql", "CREATE TABLE b (x);")]).await;
        let r = live(&node, &t.key, &id, json!({"version": old})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let newer = add_ok(&node, &t.key, &id, "new", &[("0002_b.sql", "CREATE TABLE b (x);"), ("0003_c.sql", "CREATE TABLE c (x);")]).await;
        assert_eq!(live(&node, &t.key, &id, json!({"version": newer})).await.s(), 200);

        let drift = add_ok(&node, &t.key, &id, "drift", &[("0002_b.sql", "CREATE TABLE other (x);")]).await;
        let r = live(&node, &t.key, &id, json!({"version": drift})).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migration_drift")), "{}", r.text);
        let early = add_ok(&node, &t.key, &id, "early", &[("0001_a.sql", "CREATE TABLE a (x);")]).await;
        let r = live(&node, &t.key, &id, json!({"version": early})).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migration_out_of_order")), "{}", r.text);
        assert_eq!(tables(&node, &slug).await, ["b", "c"]);

        // Back to the older version: allowed, nothing re-runs, the schema stays.
        let r = live(&node, &t.key, &id, json!({"version": old})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(tables(&node, &slug).await, ["b", "c"]);
        node.shutdown().await;
    }
}

node_test! {
    async fn a_rollback_route_runs_no_migrations() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mgr").await;
        let a = add_ok(&node, &t.key, &id, "a", &[("0001_a.sql", "CREATE TABLE a (x);")]).await;
        // Applied by a flip whose source then loses it: set directly through a second service is
        // not possible, so the proof is that a target with pending files stays unmigrated.
        let plain = add_ok(&node, &t.key, &id, "p", &[]).await;
        assert_eq!(live(&node, &t.key, &id, json!({"version": plain})).await.s(), 200);
        assert_eq!(live(&node, &t.key, &id, json!({"version": a, "migrate": false})).await.s(), 200);
        assert!(tables(&node, &slug).await.is_empty(), "migrate:false ran nothing");
        let flip = live(&node, &t.key, &id, json!({"version": plain})).await;
        let sw = flip.json["switch_id"].as_str().expect("switch").to_string();
        let r = post(&node, &t.key, &format!("/v1/services/{id}/switches/{sw}/rollback"), json!({})).await;
        assert_eq!(r.s(), 202, "{}", r.text);
        assert_eq!(r.json["to_version"], json!(a));
        assert!(tables(&node, &slug).await.is_empty(), "a rollback to `a` runs none of its files");
        node.shutdown().await;
    }
}

node_test! {
    async fn migrate_false_needs_data_admin() {
        let node = start_node().await;
        let t = new_tenant_scopes(&node, "pro", &["services:read", "services:write", "deploy"]).await;
        let (id, _slug) = setup(&node, &t.key, "mgs").await;
        let v = add_ok(&node, &t.key, &id, "v", &[("0001_a.sql", "CREATE TABLE a (x);")]).await;
        let r = live(&node, &t.key, &id, json!({"version": v, "migrate": false})).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (403, Some("insufficient_scope")), "{}", r.text);
        node.shutdown().await;
    }
}

node_test! {
    /// A failed build must not leave an applied schema behind: refused before anything is stored.
    async fn activate_with_migrations_is_refused() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mga").await;
        let body = payload("v", &[("0001_a.sql", "CREATE TABLE a (x);")]);
        let before = get(&node, &t.key, &format!("/v1/services/{id}/versions")).await.json.as_array().expect("a").len();
        let r = upload(&node, &t.key, &id, "application/json", body, "activate=1").await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("activate_with_migrations_needs_build")), "{}", r.text);
        assert_eq!(get(&node, &t.key, &format!("/v1/services/{id}/versions")).await.json.as_array().expect("a").len(), before);
        node.shutdown().await;
    }
}

node_test! {
    async fn bad_migration_files_fail_validation_at_upload() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mgv").await;
        for files in [
            vec![("1_bad.sql", "SELECT 1;")],
            vec![("0001_a.sql", "SELECT 1;"), ("0001_b.sql", "SELECT 1;")],
            vec![("sub/0001_a.sql", "SELECT 1;")],
        ] {
            let r = add(&node, &t.key, &id, "bad", &files).await;
            assert_eq!((r.s(), r.json["error"]["code"].as_str()), (422, Some("validation_failed")), "{}", r.text);
        }
        // Other files in the folder are not migrations.
        let v = add_ok(&node, &t.key, &id, "doc", &[("README.md", "hello")]).await;
        assert!(get(&node, &t.key, &format!("/v1/services/{id}/versions/{v}")).await.json.get("migrations").is_none());
        node.shutdown().await;
    }
}

node_test! {
    /// `versions.migrations` has no client write path: `meta` and unknown upload fields do nothing.
    async fn a_client_cannot_forge_the_list() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mgc").await;
        let forged = json!({"name": "0001_x", "sha256": "sha256:00", "sql": "CREATE TABLE x (x);"});
        let body = serde_json::to_vec(&json!({"files": {"main.ts": CODE}, "migrations": [forged]})).expect("json");
        let r = send(
            api(&node, &t.key, reqwest::Method::POST, &format!("/v1/services/{id}/versions"))
                .header("content-type", "application/json")
                .header("x-rt-version-meta", r#"{"migrations":[{"name":"0001_x","sha256":"sha256:00"}]}"#)
                .body(body),
        )
        .await;
        if r.s() == 201 || r.s() == 202 {
            assert!(r.json.get("migrations").is_none(), "{}", r.text);
        } else {
            assert_eq!(r.s(), 422, "{}", r.text);
        }
        node.shutdown().await;
    }
}

node_test! {
    async fn the_live_dry_run_reports_real_errors_and_writes_nothing() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mgy").await;
        let before = get(&node, &t.key, &format!("/v1/services/{id}")).await.json["live_version"].clone();
        let good = add_ok(&node, &t.key, &id, "g", &[("0001_a.sql", "CREATE TABLE a (x);")]).await;
        let r = live(&node, &t.key, &id, json!({"version": good, "dry_run": true})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(r.json["dry_run"], json!(true));
        assert_eq!(names(&r.json["pending_migrations"]), ["0001_a"]);
        assert_eq!(r.json["live_version"], before);
        assert!(tables(&node, &slug).await.is_empty());
        assert!(get(&node, &t.key, &format!("/v1/services/{id}/switches")).await.json["items"].as_array().expect("a").iter().all(|s| s["to_version"] != json!(good)));

        let bad = add_ok(&node, &t.key, &id, "b", &[("0001_a.sql", "CREATE TABLE a (x);"), ("0002_b.sql", "DROP TABLE nope;")]).await;
        let r = live(&node, &t.key, &id, json!({"version": bad, "dry_run": true})).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migration_failed")), "{}", r.text);
        assert_eq!(r.json["error"]["details"]["file"], "0002_b.sql");
        node.shutdown().await;
    }
}

node_test! {
    async fn wasm_migrations_are_write_once_and_deno_refuses_the_route() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mgw").await;
        let r = post(&node, &t.key, &format!("/v1/services/{id}/versions/1/migrations"),
            json!({"files": [{"name": "0001_a.sql", "sql": "SELECT 1;"}]})).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migrations_from_source")), "{}", r.text);

        let wasm = create_service(&node, &t.key, &uniq("mgw"), "wasm").await;
        let r = upload(&node, &t.key, &wasm, "application/wasm", wasm_fixture("hello"), "").await;
        assert!(r.s() == 201 || r.s() == 202, "{}", r.text);
        let path = format!("/v1/services/{wasm}/versions/1/migrations");
        let body = json!({"files": [{"name": "0001_a.sql", "sql": "CREATE TABLE a (x);"}]});
        let r = post(&node, &t.key, &path, body.clone()).await;
        assert_eq!(r.s(), 201, "{}", r.text);
        assert_eq!(names(&r.json["migrations"]), ["0001_a"]);
        let r = post(&node, &t.key, &path, body).await;
        assert_eq!((r.s(), r.json["error"]["code"].as_str()), (409, Some("migrations_already_set")), "{}", r.text);
        let r = post(&node, &t.key, &path, json!({"files": [{"name": "../x", "sql": ""}]})).await;
        assert_eq!(r.s(), 422, "{}", r.text);

        let r = live(&node, &t.key, &wasm, json!({"version": 1})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(names(&status(&node, &t.key, &wasm).await["applied"]), ["0001_a"]);
        node.shutdown().await;
    }
}

node_test! {
    async fn another_tenant_sees_not_found_and_a_reader_cannot_post() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let other = new_tenant(&node, "pro").await;
        let (id, _slug) = setup(&node, &t.key, "mgt").await;
        assert_eq!(get(&node, &other.key, &format!("/v1/services/{id}/migrations")).await.s(), 404);
        let reader = new_tenant_scopes(&node, "pro", &["services:read"]).await;
        let (rid, _s) = (id.clone(), ());
        let r = post(&node, &reader.key, &format!("/v1/services/{rid}/versions/1/migrations"), json!({"files": []})).await;
        assert!(r.s() == 403 || r.s() == 404, "{}", r.text);
        node.shutdown().await;
    }
}

node_test! {
    /// The console reads `pending_migrations` from a switch dry run; adoption carries the list, and
    /// the switch itself runs it in `migrating`.
    async fn a_switch_dry_run_lists_pending_migrations_and_the_switch_runs_them() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let (id, slug) = setup(&node, &t.key, "mgb").await;
        let r = post(&node, &t.key, &format!("/v1/services/{id}/branches"), json!({"name": "dev"})).await;
        assert_eq!(r.s(), 201, "{}", r.text);
        let branch = r.json["id"].as_str().expect("id").to_string();
        let v = add_ok(&node, &t.key, &branch, "b", &[("0007_add_index.sql", "CREATE TABLE a (x);")]).await;
        let r = live(&node, &t.key, &branch, json!({"version": v})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        let rb = post(&node, &t.key, &format!("/v1/services/{branch}/versions/{v}/rebuild"), json!({})).await;
        assert!(rb.s() < 300, "{}", rb.text);
        let path = format!("/v1/services/{branch}/versions/{v}");
        let (n, k, p) = (&node, &t.key, &path);
        eventually(std::time::Duration::from_secs(30), "the build", || async move {
            get(n, k, p).await.json["build_state"] == json!("ready")
        }).await;

        let from = json!({"branch": "dev"});
        let r = post(&node, &t.key, &format!("/v1/services/{id}/switch"), json!({"from": from, "dry_run": true})).await;
        assert_eq!(r.s(), 200, "{}", r.text);
        assert_eq!(r.json["pending_migrations"], json!(["0007_add_index"]));
        assert!(tables(&node, &slug).await.is_empty());

        let etag = get(&node, &t.key, &format!("/v1/services/{id}")).await.json["etag"].as_str().expect("etag").to_string();
        let r = send(api(&node, &t.key, reqwest::Method::POST, &format!("/v1/services/{id}/switch"))
            .header("if-match", etag).json(&json!({"from": from}))).await;
        assert_eq!(r.s(), 202, "{}", r.text);
        assert_ne!(r.json["state"], json!("failed"));
        assert_eq!(tables(&node, &slug).await, ["a"]);
        let r = post(&node, &t.key, &format!("/v1/services/{id}/switch"), json!({"from": from, "dry_run": true})).await;
        assert_eq!(r.json["pending_migrations"], json!([]), "{}", r.text);
        node.shutdown().await;
    }
}

/// A row left `migrating` by a crash closes as `failed`, live stays (§5 crash recovery), and the next
/// promote sees what was applied and does not run it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_while_migrating_closes_the_row_and_the_next_promote_goes_on() {
    use rt_store::{MetaStore, SqliteMetaStore};
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("mgz");
    let id = ship(&node, &t.key, &slug, CODE).await;
    let v = add_ok(&node, &t.key, &id, "v", &[("0001_a.sql", "CREATE TABLE a (x);")]).await;
    node.stop().await;
    let svc: rt_types::ServiceId = id.parse().expect("id");
    let path = node.dir.path().join("meta.db");
    {
        let m = SqliteMetaStore::open(&path).expect("meta");
        let row = m
            .create_switch(
                svc,
                rt_types::NewSwitch {
                    kind: rt_types::SwitchKind::Promote,
                    source: None,
                    from_version: None,
                    to_version: rt_types::VersionNo(v),
                    created_by: None,
                },
            )
            .await
            .expect("row");
        m.update_switch(
            svc,
            row.id,
            rt_types::SwitchPatch {
                state: Some(SwitchState::Migrating),
                ..Default::default()
            },
        )
        .await
        .expect("migrating");
        let got: Option<Version> = m.get_version(svc, rt_types::VersionNo(v)).await.expect("get");
        assert_eq!(got.expect("row").migrations.len(), 1);
    }
    node.start().await;
    let r = live(&node, &t.key, &id, json!({"version": v})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(tables(&node, &slug).await, ["a"]);
    let items = get(&node, &t.key, &format!("/v1/services/{id}/switches")).await;
    assert!(items.json["items"]
        .as_array()
        .expect("a")
        .iter()
        .any(|s| s["error"] == json!("interrupted")));
    node.stop().await;
}
