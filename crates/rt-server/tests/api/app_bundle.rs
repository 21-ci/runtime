//! App export, import and fork (R12) through the node: the round trip, and hostile bundles at the
//! door. The bundle-level attack table is in `rt-backup`'s `tests/it/app.rs`; here the point is that
//! a refusal leaves no service, and that the checks are the API's own.

use std::time::Duration;

use reqwest::Method;
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

const GUEST: &str = r#"
import { sql } from "std:sql";
import { blob } from "std:blob";
export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    await sql.execute("CREATE TABLE IF NOT EXISTS t (v TEXT)");
    if (u.pathname.endsWith("/put")) {
      await sql.execute("INSERT INTO t VALUES ('kept')");
      await blob.put("k", "blob-bytes");
      return new Response("put");
    }
    const r = await sql.execute("SELECT v FROM t");
    const b = await blob.get("k");
    return Response.json({ rows: r.rows.length, blob: b ? await new Response(b.body).text() : null });
  },
};"#;

async fn export(node: &NodeHarness, key: &str, id: &str) -> Vec<u8> {
    let r = post(node, key, &format!("/v1/services/{id}/bundle"), json!({})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let path = format!("/v1/services/{id}/bundle/{}", r.json["id"].as_str().expect("id"));
    for _ in 0..600 {
        let resp = api(node, key, Method::GET, &path).send().await.expect("get");
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if ct == "application/zstd" {
            let disposition = resp.headers()["content-disposition"]
                .to_str()
                .expect("name")
                .to_string();
            assert!(disposition.contains("filename=\"app-"), "{disposition}");
            assert!(disposition.contains(".tar.zst"), "{disposition}");
            return resp.bytes().await.expect("bytes").to_vec();
        }
        let v: Value = resp.json().await.expect("json");
        assert_ne!(v["state"], "failed", "{v}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the export did not finish");
}

async fn import(node: &NodeHarness, key: &str, query: &str, body: Vec<u8>) -> Resp {
    let rb = api(node, key, Method::POST, &format!("/v1/services/import?{query}"))
        .header("content-type", "application/zstd")
        .body(body);
    send(rb).await
}

async fn exists(node: &NodeHarness, key: &str, slug: &str) -> bool {
    !get(node, key, &format!("/v1/services?slug={slug}"))
        .await
        .items()
        .is_empty()
}

/// Rewrite a bundle: `f` may change the manifest and the database, then digests are made right, as
/// an attacker's tool would.
fn tamper(bundle: &[u8], f: impl FnOnce(&mut Value, &rusqlite::Connection)) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let d = tempfile::tempdir().expect("dir");
    let mut a = tar::Archive::new(zstd::stream::read::Decoder::new(bundle).expect("dec"));
    let mut manifest = Value::Null;
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for e in a.entries().expect("entries") {
        let mut e = e.expect("entry");
        let path = String::from_utf8(e.path_bytes().to_vec()).expect("utf8");
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut e, &mut buf).expect("read");
        if path == "manifest.json" {
            manifest = serde_json::from_slice(&buf).expect("json");
        } else {
            files.push((path, buf));
        }
    }
    let db_path = d.path().join("data.db");
    let db = files.iter_mut().find(|(p, _)| p == "data.db").expect("db");
    std::fs::write(&db_path, &db.1).expect("write");
    let c = rusqlite::Connection::open(&db_path).expect("open");
    f(&mut manifest, &c);
    drop(c);
    db.1 = std::fs::read(&db_path).expect("read");
    manifest["data"]["size"] = json!(db.1.len());
    manifest["data"]["sha256"] = json!(Sha256::digest(&db.1)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect::<String>());
    let mut out = Vec::new();
    {
        let enc = zstd::stream::write::Encoder::new(&mut out, 1)
            .expect("enc")
            .auto_finish();
        let mut b = tar::Builder::new(enc);
        let json = serde_json::to_vec(&manifest).expect("json");
        let mut add = |p: &str, data: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o600);
            b.append_data(&mut h, p, data).expect("append");
        };
        add("manifest.json", &json);
        for (p, data) in &files {
            add(p, data);
        }
        b.finish().expect("finish");
    }
    out
}

async fn source(node: &NodeHarness, t: &Tenant) -> (String, Vec<u8>) {
    let slug = uniq("src");
    let id = ship(node, &t.key, &slug, GUEST).await;
    let r = call_service(node, &t.key, &slug, "/put").await;
    assert_eq!(r, "put");
    let bytes = export(node, &t.key, &id).await;
    (id, bytes)
}

async fn call_service(node: &NodeHarness, _key: &str, slug: &str, path: &str) -> String {
    let mut last = String::new();
    for _ in 0..200 {
        let r = node.invoke(slug, Method::GET, path, "").await;
        let ok = r.status().is_success();
        last = r.text().await.expect("text");
        if ok {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("/s/{slug}{path}: {last}");
}

node_test! {
/// Same data, blobs, env, schedules, live version; secrets named and not carried; another tenant.
async fn a_service_moves_to_another_tenant_with_its_data() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("src");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    assert_eq!(call_service(&node, &t.key, &slug, "/put").await, "put");
    let r = put(&node, &t.key, &format!("/v1/services/{id}/env"), json!({"MODE": "prod"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let r = put(&node, &t.key, &format!("/v1/services/{id}/secrets/API_KEY"), json!({"value": "s3cret-value"})).await;
    assert!(r.s() < 300, "{}", r.text);
    let r = post(&node, &t.key, &format!("/v1/services/{id}/schedules"),
        json!({"kind": "interval", "every_seconds": 3600})).await;
    assert_eq!(r.s(), 201, "{}", r.text);

    let bundle = export(&node, &t.key, &id).await;
    let plain = zstd::decode_all(&bundle[..]).expect("zstd");
    assert!(!plain.windows(12).any(|w| w == b"s3cret-value"), "a sealed secret stays home");

    let other = new_tenant(&node, "pro").await;
    let new_slug = uniq("imp");
    let r = import(&node, &other.key, &format!("slug={new_slug}&activate=1"), bundle).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    assert_eq!(r.json["warnings"], json!(["missing_secrets"]));
    assert_eq!(r.json["missing_secrets"], json!(["API_KEY"]));
    let nid = r.json["service_id"].as_str().expect("id").to_string();

    let out = call_service(&node, &other.key, &new_slug, "/get").await;
    let v: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(v, json!({"rows": 1, "blob": "blob-bytes"}));
    let env = get(&node, &other.key, &format!("/v1/services/{nid}/env")).await;
    assert_eq!(env.json["MODE"], "prod");
    let secrets = get(&node, &other.key, &format!("/v1/services/{nid}/secrets")).await;
    assert!(!secrets.text.contains("API_KEY"), "{}", secrets.text);
    let sch = get(&node, &other.key, &format!("/v1/services/{nid}/schedules")).await;
    assert_eq!(sch.text.matches("every_seconds").count(), 1, "{}", sch.text);
    // The source is untouched and the tenants stay apart.
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{nid}")).await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
/// M2: a tenant out of builds is refused before a service exists (the taken slug would answer `409`
/// otherwise), and an import charges one build per version, not more.
async fn an_import_charges_builds_up_front_and_once() {
    let node = start_node_with(|c| {
        if let Some(p) = c.plans.get_mut("pro") {
            p.builds_per_hour = 3;
        }
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await; // build 1
    let slug = uniq("imp");
    let r = import(&node, &t.key, &format!("slug={slug}"), bundle.clone()).await; // build 2
    assert_eq!(r.s(), 202, "{}", r.text);
    let svc = create_service(&node, &t.key, &uniq("up"), "deno").await;
    let r = upload_ts(&node, &t.key, &svc, GUEST, "activate=0").await; // build 3
    assert_eq!(r.s(), 201, "one version, one build: {}", r.text);
    let r = import(&node, &t.key, &format!("slug={slug}"), bundle).await;
    assert_eq!((r.s(), r.code()), (429, "build_quota".into()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// M3: a bundle's env is a write of its own: the scope of the normal route applies. Schedules need
/// `services:write` on their own route, which an import already asks for.
async fn an_import_needs_the_scopes_of_what_it_writes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("src");
    let id = ship(&node, &t.key, &slug, GUEST).await;
    let plain = export(&node, &t.key, &id).await;
    let r = put(&node, &t.key, &format!("/v1/services/{id}/env"), json!({"MODE": "prod"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let with_env = export(&node, &t.key, &id).await;

    let base = ["services:write", "services:read", "deploy", "data:admin"];
    let narrow = make_key(&node, &t.id, &base, None).await.1;
    let refused = uniq("imp");
    let r = import(&node, &narrow, &format!("slug={refused}"), with_env.clone()).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".into()), "{}", r.text);
    assert!(!exists(&node, &t.key, &refused).await);

    // Nothing of the kind in the bundle, nothing more to ask for.
    let r = import(&node, &narrow, &format!("slug={}", uniq("imp")), plain).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let env_key = make_key(&node, &t.id, &[&base[..], &["env:write"]].concat(), None).await.1;
    let r = import(&node, &env_key, &format!("slug={}", uniq("imp")), with_env).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// M9: finished exports wait on disk for an hour; a tenant holds at most two of them.
async fn a_tenant_holds_a_bounded_number_of_exports() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (id, _) = source(&node, &t).await; // the first
    export(&node, &t.key, &id).await;
    let r = post(&node, &t.key, &format!("/v1/services/{id}/bundle"), json!({})).await;
    assert_eq!((r.s(), r.code()), (429, "heavy_job_busy".into()), "{}", r.text);
    let other = new_tenant(&node, "pro").await;
    let (_, _) = source(&node, &other).await;
    node.shutdown().await;
}
}

node_test! {
/// Without `activate` nothing goes live: the guest code of a bundle does not run because it arrived.
async fn an_import_goes_live_only_when_asked() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    let slug = uniq("imp");
    let r = import(&node, &t.key, &format!("slug={slug}"), bundle).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let view = get(&node, &t.key, &format!("/v1/services/{}", r.json["service_id"].as_str().expect("id"))).await;
    assert_eq!(view.json["live_version"], Value::Null, "{}", view.text);
    node.shutdown().await;
}
}

node_test! {
/// The critical row: `std:blob.get` would read the master key. Refused, and no service appears.
async fn a_blob_row_at_the_master_key_is_refused_and_leaves_no_service() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    for file in ["/data/master.key", "../../../master.key"] {
        let evil = tamper(&bundle, |_, c| {
            c.execute("INSERT INTO _rt_blobs VALUES (X'99', 1, NULL, ?1)", [file]).expect("row");
        });
        let slug = uniq("evil");
        let r = import(&node, &t.key, &format!("slug={slug}"), evil).await;
        assert_eq!((r.s(), r.code().as_str()), (422, "bundle_invalid"), "{}", r.text);
        assert_eq!(r.json["error"]["details"]["reason"], "unsafe_entry", "{}", r.text);
        assert!(!r.text.contains("master.key"), "{}", r.text);
        assert!(!exists(&node, &t.key, &slug).await);
    }
    node.shutdown().await;
}
}

node_test! {
async fn malicious_schema_in_data_db_is_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    for sql in [
        "CREATE TRIGGER x AFTER INSERT ON t BEGIN DELETE FROM _rt_blobs; END;",
        "CREATE VIEW spy AS SELECT * FROM _rt_kv;",
        "CREATE TRIGGER y AFTER INSERT ON t BEGIN SELECT load_extension('/tmp/evil'); END;",
        "DROP TABLE _rt_kv; CREATE TABLE _rt_kv (key BLOB PRIMARY KEY, value BLOB) WITHOUT ROWID;",
    ] {
        let evil = tamper(&bundle, |_, c| c.execute_batch(sql).expect("sql"));
        let slug = uniq("evil");
        let r = import(&node, &t.key, &format!("slug={slug}"), evil).await;
        assert_eq!((r.s(), r.code().as_str()), (422, "bundle_invalid"), "{sql}: {}", r.text);
        assert!(!exists(&node, &t.key, &slug).await, "{sql}");
    }
    node.shutdown().await;
}
}

node_test! {
/// The manifest is config from a stranger: the allowlist, then the normal create validation.
async fn a_hostile_manifest_is_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    let keys: [fn(&mut Value); 4] = [
        |m| m["tenant"] = json!("ten_x"),
        |m| m["service"]["project"] = json!("x"),
        |m| m["service"]["scratch"] = json!({"kind": "preview"}),
        |m| m["preview"] = json!(true),
    ];
    for f in keys {
        let evil = tamper(&bundle, |m, _| f(m));
        let slug = uniq("evil");
        let r = import(&node, &t.key, &format!("slug={slug}"), evil).await;
        assert_eq!((r.s(), r.code().as_str()), (422, "bundle_invalid"), "{}", r.text);
        assert!(!exists(&node, &t.key, &slug).await);
    }
    // Past the plan: the create route's own refusal.
    let evil = tamper(&bundle, |m, _| m["service"]["limits"] = json!({"memory_mb": 1_000_000}));
    let slug = uniq("evil");
    let r = import(&node, &t.key, &format!("slug={slug}"), evil).await;
    assert_eq!((r.s(), r.code().as_str()), (422, "limit_exceeds_plan"), "{}", r.text);
    // A source that does not validate takes the service back out.
    let evil = tamper(&bundle, |m, _| m["versions"][0]["content_type"] = json!("application/wasm"));
    let slug = uniq("evil");
    let r = import(&node, &t.key, &format!("slug={slug}"), evil).await;
    assert!(r.s() >= 400, "{}", r.text);
    assert!(!exists(&node, &t.key, &slug).await);
    // `sys-` names belong to the node.
    let r = import(&node, &t.key, "slug=sys-mine", bundle.clone()).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn truncated_and_garbage_bundles_are_refused() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    for body in [Vec::new(), b"not a bundle".to_vec(), bundle[..bundle.len() / 2].to_vec()] {
        let slug = uniq("evil");
        let r = import(&node, &t.key, &format!("slug={slug}"), body).await;
        assert_eq!((r.s(), r.code().as_str()), (422, "bundle_invalid"), "{}", r.text);
        assert!(!exists(&node, &t.key, &slug).await);
    }
    node.shutdown().await;
}
}

node_test! {
async fn a_bundle_over_the_limit_is_413_counted_or_declared() {
    let node = start_node_with(|c| c.import.max_bundle_mb = 1).await;
    let t = new_tenant(&node, "pro").await;
    let big = vec![0u8; (1 << 20) + 1];
    let r = import(&node, &t.key, "slug=toobig", big.clone()).await;
    assert_eq!((r.s(), r.code().as_str()), (413, "too_large"), "{}", r.text);
    // Chunked: no Content-Length to believe.
    let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(big)]);
    let rb = api(&node, &t.key, Method::POST, "/v1/services/import?slug=toobig")
        .body(reqwest::Body::wrap_stream(stream));
    let r = send(rb).await;
    assert_eq!((r.s(), r.code().as_str()), (413, "too_large"), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn a_taken_slug_is_409() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (_, bundle) = source(&node, &t).await;
    let slug = uniq("taken");
    create_service(&node, &t.key, &slug, "deno").await;
    let r = import(&node, &t.key, &format!("slug={slug}"), bundle).await;
    assert_eq!((r.s(), r.code().as_str()), (409, "slug_taken"), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// `?from=`: a fork. Another tenant's service is `404` to a key, whatever the id.
async fn a_fork_copies_within_a_tenant_and_stops_at_its_edge() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (id, _) = source(&node, &t).await;
    let slug = uniq("fork");
    let r = send(api(&node, &t.key, Method::POST, &format!("/v1/services/import?slug={slug}&activate=1&from={id}"))).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let v: Value = serde_json::from_str(&call_service(&node, &t.key, &slug, "/get").await).expect("json");
    assert_eq!(v, json!({"rows": 1, "blob": "blob-bytes"}));

    let other = new_tenant(&node, "pro").await;
    let stolen = uniq("stolen");
    let r = send(api(&node, &other.key, Method::POST, &format!("/v1/services/import?slug={stolen}&from={id}"))).await;
    assert_eq!((r.s(), r.code().as_str()), (404, "not_found"), "{}", r.text);
    assert!(!exists(&node, &other.key, &stolen).await);

    // The operator names a tenant and may take from another.
    let moved = uniq("moved");
    let r = send(root_for(&node, &other.id, Method::POST, &format!("/v1/services/import?slug={moved}&from={id}"))).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn export_and_import_need_their_scopes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (id, bundle) = source(&node, &t).await;
    let (_, weak) = make_key(&node, &t.id, &["services:read", "services:write", "deploy"], None).await;
    let r = post(&node, &weak, &format!("/v1/services/{id}/bundle"), json!({})).await;
    assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{}", r.text);
    let r = import(&node, &weak, "slug=nope", bundle).await;
    assert_eq!((r.s(), r.code().as_str()), (403, "insufficient_scope"), "{}", r.text);

    // Another tenant cannot export it, nor fetch a bundle by its id.
    let other = new_tenant(&node, "pro").await;
    let r = post(&node, &other.key, &format!("/v1/services/{id}/bundle"), json!({})).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    let started = post(&node, &t.key, &format!("/v1/services/{id}/bundle"), json!({})).await;
    let path = format!("/v1/services/{id}/bundle/{}", started.json["id"].as_str().expect("id"));
    assert_eq!(get(&node, &other.key, &path).await.s(), 404);
    // Nor is an id good for another service of the same tenant.
    let sibling = create_service(&node, &t.key, &uniq("sib"), "deno").await;
    let cross = format!("/v1/services/{sibling}/bundle/{}", started.json["id"].as_str().expect("id"));
    assert_eq!(get(&node, &t.key, &cross).await.s(), 404);
    node.shutdown().await;
}
}

node_test! {
async fn a_bundle_downloads_with_range() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (id, _) = source(&node, &t).await;
    let started = post(&node, &t.key, &format!("/v1/services/{id}/bundle"), json!({})).await;
    assert_eq!(started.s(), 202, "{}", started.text);
    let path = format!("/v1/services/{id}/bundle/{}", started.json["id"].as_str().expect("id"));
    for _ in 0..600 {
        let r = api(&node, &t.key, Method::GET, &path).header("range", "bytes=0-9").send().await.expect("get");
        if r.status() == 206 {
            assert_eq!(r.bytes().await.expect("bytes").len(), 10);
            node.shutdown().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no range answer");
}
}
