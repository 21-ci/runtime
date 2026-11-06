//! The data browser over HTTP (R9, SECURITY.md "Data browser"): scope, tenancy, audit, limits. The
//! attack table itself runs against the store (`rt-store` `browse::`).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rt_server::StartOptions;
use rt_store::{
    BatchMode, DataConfig, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, SqlStatement, ValueTag,
};
use serde_json::{json, Value};

use crate::common::*;

async fn seed(node: &rt_testkit::NodeHarness, svc: &str) {
    let store = LocalServiceData::open(DataConfig::new(node.data_dir())).expect("store");
    let id = svc.parse().expect("service id");
    let st = |s: &str| SqlStatement {
        sql: s.into(),
        args: vec![],
    };
    store
        .sql(id)
        .batch(
            vec![
                st("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, big INTEGER, photo BLOB)"),
                st("INSERT INTO users VALUES (1, 'ann', 9007199254740993, x'ab01')"),
            ],
            BatchMode::Write,
        )
        .await
        .expect("seed sql");
    store
        .kv(id)
        .set(
            b"user:1",
            KvValue {
                tag: ValueTag::Json,
                data: bytes::Bytes::from_static(b"\"secret\""),
            },
            KvSetOptions::default(),
        )
        .await
        .expect("seed kv");
}

fn query(path_svc: &str) -> String {
    format!("/v1/services/{path_svc}/data/query")
}

const READER: &[&str] = &["data:read", "services:read"];

async fn events(node: &impl Node, key: &str, svc: &str) -> Vec<Value> {
    let r = get(node, key, &format!("/v1/services/{svc}/events")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    r.json["items"].as_array().cloned().unwrap_or_default()
}

/// The audit writer of other events is asynchronous; this one is not: the event is there when the
/// read answers, with a hash and never the statement, its args or a value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_answers_rows_and_leaves_its_audit_event() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["data:read", "services:read", "services:write"]).await;
    let svc = create_service(&node, &t.key, "browse-ok", "deno").await;
    seed(&node, &svc).await;
    let r = post(
        &node,
        &t.key,
        &query(&svc),
        json!({"sql": "SELECT id, name, big, photo FROM users WHERE name = ?1", "args": ["ann"]}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["columns"], json!(["id", "name", "big", "photo"]));
    assert_eq!(
        r.json["rows"],
        json!([[1, "ann", "9007199254740993", {"$blob": 2, "hex": "ab01"}]])
    );
    assert_eq!(r.json["truncated"], json!(false));

    let evs = events(&node, &t.key, &svc).await;
    let ev = evs.iter().find(|e| e["kind"] == "data.queried").expect("audit event");
    let text = ev.to_string();
    assert!(text.contains("sql_sha256"), "{text}");
    assert!(!text.contains("SELECT") && !text.contains("ann"), "{text}");

    let tables = get(&node, &t.key, &format!("/v1/services/{svc}/data/tables")).await;
    assert_eq!(tables.s(), 200, "{}", tables.text);
    let items = tables.json["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "no _rt_ table is listed: {}", tables.text);
    assert_eq!(items[0]["name"], "users");
    assert_eq!(items[0]["rows_estimate"], json!(1));

    let kv = get(&node, &t.key, &format!("/v1/services/{svc}/data/kv?prefix=user")).await;
    assert_eq!(kv.s(), 200, "{}", kv.text);
    assert_eq!(kv.json["items"][0]["key"], "user:1");
    let k = URL_SAFE_NO_PAD.encode("user:1");
    let v = get(&node, &t.key, &format!("/v1/services/{svc}/data/kv/{k}")).await;
    assert_eq!(v.s(), 200, "{}", v.text);
    assert_eq!(v.json["value"], "\"secret\"");
    let gone = URL_SAFE_NO_PAD.encode("nope");
    assert_eq!(
        get(&node, &t.key, &format!("/v1/services/{svc}/data/kv/{gone}"))
            .await
            .s(),
        404
    );
    let evs = events(&node, &t.key, &svc).await;
    let ev = evs
        .iter()
        .find(|e| e["kind"] == "data.kv_read")
        .expect("kv audit event");
    assert!(!ev.to_string().contains("user:1") && !ev.to_string().contains("secret"));

    let blobs = get(&node, &t.key, &format!("/v1/services/{svc}/data/blobs")).await;
    assert_eq!(blobs.s(), 200, "{}", blobs.text);
}

/// `data:read` is asked for by name: other scopes, even `data:admin`, do not include it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_without_data_read_is_refused_and_another_tenant_gets_404() {
    let node = start_node().await;
    let owner = new_tenant_scopes(&node, "pro", &["services:write", "data:admin", "services:read"]).await;
    let svc = create_service(&node, &owner.key, "browse-sc", "deno").await;
    for path in ["tables", "kv", "blobs"] {
        let r = get(&node, &owner.key, &format!("/v1/services/{svc}/data/{path}")).await;
        assert_eq!(
            (r.s(), r.json["error"]["code"].as_str()),
            (403, Some("insufficient_scope")),
            "{path}"
        );
    }
    let r = post(&node, &owner.key, &query(&svc), json!({"sql": "SELECT 1"})).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].as_str()),
        (403, Some("insufficient_scope"))
    );

    let other = new_tenant_scopes(&node, "pro", READER).await;
    let r = post(&node, &other.key, &query(&svc), json!({"sql": "SELECT 1"})).await;
    assert_eq!(r.s(), 404, "{}", r.text);
    assert_eq!(
        get(&node, &other.key, &format!("/v1/services/{svc}/data/tables"))
            .await
            .s(),
        404
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_have_their_stable_codes_and_are_audited() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["data:read", "services:read", "services:write"]).await;
    let svc = create_service(&node, &t.key, "browse-no", "deno").await;
    seed(&node, &svc).await;
    for sql in [
        "WITH x AS (SELECT 1) DELETE FROM users",
        "ATTACH DATABASE 'x' AS y",
        "SELECT load_extension('x')",
        "SELECT * FROM _rt_kv",
        "SELECT sql FROM sqlite_master",
    ] {
        let r = post(&node, &t.key, &query(&svc), json!({"sql": sql})).await;
        assert_eq!(
            (r.s(), r.json["error"]["code"].as_str()),
            (422, Some("sql_not_readonly")),
            "{sql}: {}",
            r.text
        );
    }
    let r = post(&node, &t.key, &query(&svc), json!({"sql": "SELEC 1"})).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].as_str()),
        (422, Some("sql_error")),
        "{}",
        r.text
    );
    assert!(r.json["error"]["details"]["offset"].is_number(), "{}", r.text);
    let r = post(&node, &t.key, &query(&svc), json!({"sql": "SELECT 1", "limit": 501})).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].as_str()),
        (422, Some("validation_failed"))
    );
    let r = post(
        &node,
        &t.key,
        &query(&svc),
        json!({"sql": "SELECT 'a long text'", "max_bytes": 1}),
    )
    .await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].as_str()),
        (413, Some("result_too_large")),
        "{}",
        r.text
    );
    let evs = events(&node, &t.key, &svc).await;
    assert!(
        evs.iter().any(|e| e.to_string().contains("sql_not_readonly")),
        "a refused try is on record"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_are_limited_per_service() {
    let node = start_node().await;
    let t = new_tenant_scopes(&node, "pro", &["data:read", "services:write"]).await;
    let svc = create_service(&node, &t.key, "browse-rl", "deno").await;
    for _ in 0..10 {
        assert_eq!(
            post(&node, &t.key, &query(&svc), json!({"sql": "SELECT 1"})).await.s(),
            200
        );
    }
    let r = post(&node, &t.key, &query(&svc), json!({"sql": "SELECT 1"})).await;
    assert_eq!(
        (r.s(), r.json["error"]["code"].as_str()),
        (429, Some("rate_limited")),
        "{}",
        r.text
    );
}

/// A read that cannot be audited is not served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_audit_write_fails_the_read() {
    let faults = rt_store::Faults::new();
    let mut opts = StartOptions::default();
    opts.faults.meta = Some(faults.clone());
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant_scopes(&node, "pro", &["data:read", "services:write"]).await;
    let svc = create_service(&node, &t.key, "browse-au", "deno").await;
    seed(&node, &svc).await;
    // Pass what loading the key and the service read, then fail the event's append: try growing
    // numbers of passes until the service loads and the append is the call that fails.
    let mut refused = None;
    for pass in 0..8 {
        faults.clear();
        faults.pass_next(pass);
        faults.fail_next(1, rt_store::Fault::Io);
        let r = post(&node, &t.key, &query(&svc), json!({"sql": "SELECT name FROM users"})).await;
        faults.clear();
        if r.s() == 503 && r.json["error"]["code"] == "unavailable" {
            refused = Some(r);
            break;
        }
    }
    let r = refused.expect("a failing append refuses the read");
    assert!(!r.text.contains("ann"), "{}", r.text);
}
