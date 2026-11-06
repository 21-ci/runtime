//! `…/data-events` (R14) and `…/secrets/{name}/ref` (R15), on a whole node; Vault is a local mock.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};

use crate::common::*;

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("never happened: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_events_need_data_read_and_a_topic_of_the_namespace() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("evs"), "deno").await;
    let path = format!("/v1/services/{svc}/data-events");
    let cfg = json!({"topic": "orders-changes", "tables": [{"name": "orders"}]});

    let r = get(&node, &t.key, &path).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["state"], "disabled");

    let r = put(&node, &t.key, &path, cfg.clone()).await;
    assert_eq!(r.s(), 422, "no such topic: {}", r.text);
    let r = post(&node, &t.key, "/v1/topics", json!({"name": "orders-changes"})).await;
    assert_eq!(r.s(), 201, "{}", r.text);

    let (_, narrow) = make_key(&node, &t.id, &["services:write", "services:read"], None).await;
    let r = put(&node, &narrow, &path, cfg.clone()).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    assert_eq!(r.code(), "insufficient_scope");
    let r = patch(
        &node,
        &narrow,
        &format!("/v1/services/{svc}"),
        json!({"events": {"data_events": cfg}}),
    )
    .await;
    assert_eq!(r.s(), 403, "PATCH is no way around the rule: {}", r.text);

    let r = put(&node, &t.key, &path, cfg.clone()).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["topic"], "orders-changes");
    assert_eq!(r.json["kv_prefixes"], json!([]));
    let with_kv = json!({"topic": "orders-changes", "kv_prefixes": [{"prefix": "cart:"}]});
    let r = put(&node, &t.key, &path, with_kv).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["kv_prefixes"][0]["prefix"], "cart:", "{}", r.text);
    let r = put(&node, &t.key, &path, cfg.clone()).await;
    assert_eq!(r.s(), 200, "{}", r.text);

    for bad in [
        json!({"topic": "orders-changes"}),
        json!({"topic": "orders-changes", "tables": [{"name": "_rt_changes"}]}),
        json!({"topic": "orders-changes", "tables": [{"name": "t", "payload": "all"}]}),
        json!({"topic": "BAD", "tables": [{"name": "t"}]}),
    ] {
        assert_eq!(put(&node, &t.key, &path, bad.clone()).await.s(), 422, "{bad}");
    }

    let other = new_tenant(&node, "pro").await;
    assert_eq!(get(&node, &other.key, &path).await.s(), 404);

    let r = delete(&node, &t.key, &path).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["state"], "disabled");
}

#[derive(Default)]
struct Vault {
    value: Mutex<Option<String>>,
    paths: Mutex<Vec<String>>,
}

async fn mock_vault(v: Arc<Vault>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = l.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.expect("accept");
            let v = v.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = text.split_whitespace().nth(1).unwrap_or("").to_string();
                v.paths.lock().push(path);
                let answer = v.value.lock().clone();
                let (status, body) = match answer {
                    Some(val) => (
                        200,
                        json!({"data": {"data": {"password": val}, "metadata": {"version": 1}}}).to_string(),
                    ),
                    None => (500, r#"{"errors":["vault internal detail"]}"#.to_string()),
                };
                let out = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(out.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

async fn secret_row(node: &impl Node, key: &str, svc: &str, name: &str) -> Value {
    let r = get(node, key, &format!("/v1/services/{svc}/secrets")).await;
    r.json
        .as_array()
        .and_then(|a| a.iter().find(|s| s["name"] == name).cloned())
        .unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ref_syncs_rotates_survives_an_outage_and_cannot_leave_its_prefix() {
    let vault = Arc::new(Vault::default());
    *vault.value.lock() = Some("first".into());
    let address = mock_vault(vault.clone()).await;
    let dir = tempfile::tempdir().expect("dir");
    let token = dir.path().join("token");
    std::fs::write(&token, "s.node").expect("write");
    let token_file = token.to_string_lossy().to_string();
    let node = start_node_with(move |c| {
        c.secrets.vault.address = address;
        c.secrets.vault.token_file = token_file;
        c.secrets.vault.poll = Duration::from_secs(10);
    })
    .await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("refs");
    let svc = ship(
        &node,
        &t.key,
        &slug,
        r#"export default { fetch() { return new Response(Deno.env.get("DB") ?? "unset"); } };"#,
    )
    .await;
    let path = format!("/v1/services/{svc}/secrets/DB/ref");

    for bad in ["../other/db", "a//b", "a%2fb", "/abs", "x/../../y"] {
        let r = put(&node, &t.key, &path, json!({"path": bad, "field": "password"})).await;
        assert_eq!(r.s(), 422, "{bad}");
        assert_eq!(r.code(), "ref_outside_prefix");
    }
    assert!(vault.paths.lock().is_empty(), "a refused path reaches no source");

    let r = put(&node, &t.key, &path, json!({"path": "app/db", "field": "password"})).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["source"], "vault");
    assert!(!r.text.contains("first"), "a value is never returned");
    let seen = vault.paths.lock()[0].clone();
    assert!(
        seen.starts_with("/v1/secret/data/rt/ten") || seen.contains(&t.id),
        "{seen}"
    );
    assert!(seen.contains(&t.id), "the tenant comes from the credential: {seen}");
    assert!(
        seen.ends_with("/production/app/db") || seen.contains("/app/db"),
        "{seen}"
    );

    let row = secret_row(&node, &t.key, &svc, "DB").await;
    assert_eq!(row["source"], "vault");
    assert!(row.get("value").is_none());

    assert_eq!(invoke(&node, &slug, "/").await.text, "first");

    // A write to a ref is refused.
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/DB"),
        json!({"value": "x"}),
    )
    .await;
    assert_eq!(r.s(), 409, "{}", r.text);
    assert_eq!(r.code(), "secret_is_ref");

    // Outage: the error is a code, the source's text stays out, and the value stays.
    *vault.value.lock() = None;
    let r = post(&node, &t.key, &format!("/v1/services/{svc}/secrets/sync"), json!({})).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    let (k, s) = (t.key.clone(), svc.clone());
    eventually("the outage is recorded", || {
        let (k, s, node) = (k.clone(), s.clone(), &node);
        async move { secret_row(node, &k, &s, "DB").await["last_error"] == "secret_source_unavailable" }
    })
    .await;
    let list = get(&node, &t.key, &format!("/v1/services/{svc}/secrets")).await;
    assert!(!list.text.contains("internal detail"));
    assert_eq!(
        invoke(&node, &slug, "/").await.text,
        "first",
        "an outage keeps the last value"
    );

    // Rotation: seen after the next sync, error cleared.
    *vault.value.lock() = Some("second".into());
    post(&node, &t.key, &format!("/v1/services/{svc}/secrets/sync"), json!({})).await;
    eventually("the error clears", || {
        let (k, s, node) = (k.clone(), s.clone(), &node);
        async move { secret_row(node, &k, &s, "DB").await["last_error"].is_null() }
    })
    .await;

    assert_eq!(
        invoke(&node, &slug, "/").await.text,
        "second",
        "the next task sees the rotation"
    );

    // The limit: 6 a minute (2 used).
    let mut codes = Vec::new();
    for _ in 0..6 {
        codes.push(
            post(&node, &t.key, &format!("/v1/services/{svc}/secrets/sync"), json!({}))
                .await
                .s(),
        );
    }
    assert_eq!(codes.iter().filter(|c| **c == 202).count(), 4, "{codes:?}");
    assert!(codes.contains(&429));

    // Deleting the ref leaves a plain secret.
    assert_eq!(delete(&node, &t.key, &path).await.s(), 204);
    assert_eq!(secret_row(&node, &t.key, &svc, "DB").await["source"], "plain");
    assert_eq!(delete(&node, &t.key, &path).await.s(), 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_vault_says_so() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("norefs"), "deno").await;
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/DB/ref"),
        json!({"path": "app/db", "field": "f"}),
    )
    .await;
    assert_eq!(r.s(), 422);
    assert_eq!(r.code(), "validation_failed");
    let r = post(&node, &t.key, &format!("/v1/services/{svc}/secrets/sync"), json!({})).await;
    assert_eq!(r.s(), 422);
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/secrets/P"),
        json!({"value": "v"}),
    )
    .await;
    assert!(r.s() < 300, "{}", r.text);
    assert_eq!(secret_row(&node, &t.key, &svc, "P").await["source"], "plain");
}

const WRITER: &str = r#"
import { sql } from "std:sql";
export default {
  async fetch(req) {
    await sql.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY)");
    const id = new URL(req.url).searchParams.get("id");
    await sql.execute("INSERT INTO orders VALUES (?)", [Number(id)]);
    return new Response("ok");
  },
};
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_write_reaches_the_topic_and_a_branch_stays_out() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("pump");
    let svc = ship(&node, &t.key, &slug, WRITER).await;
    assert_eq!(invoke(&node, &slug, "?id=1").await.s(), 200);
    let topic = post(
        &node,
        &t.key,
        "/v1/topics",
        json!({"name": "orders-changes", "partitions": 1}),
    )
    .await;
    assert_eq!(topic.s(), 201, "{}", topic.text);
    let tid = topic.json["id"].as_str().expect("id").to_string();
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/data-events"),
        json!({"topic": "orders-changes", "tables": [{"name": "orders", "payload": "row"}]}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let (k, s) = (t.key.clone(), svc.clone());
    eventually("the pump is active", || {
        let (k, s, node) = (k.clone(), s.clone(), &node);
        async move { get(node, &k, &format!("/v1/services/{s}/data-events")).await.json["state"] == "active" }
    })
    .await;

    assert_eq!(invoke(&node, &slug, "?id=2").await.s(), 200);
    let path = format!("/v1/topics/{tid}/messages?from=earliest");
    let (k2, p2) = (t.key.clone(), path.clone());
    eventually("the event arrives", || {
        let (k, p, node) = (k2.clone(), p2.clone(), &node);
        async move { get(node, &k, &p).await.text.contains("sql:orders") }
    })
    .await;
    let read = get(&node, &t.key, &path).await;
    assert_eq!(read.items().len(), 1, "only the write after subscribing: {}", read.text);

    // A branch with the data copied: its config has no data events and its writes stay home.
    let b = post(
        &node,
        &t.key,
        &format!("/v1/services/{svc}/branches"),
        json!({"name": "pr-1", "copy_data": true}),
    )
    .await;
    assert!(b.s() < 300, "{}", b.text);
    let bid = b.json["id"]
        .as_str()
        .or(b.json["service"]["id"].as_str())
        .expect("id")
        .to_string();
    let de = get(&node, &t.key, &format!("/v1/services/{bid}/data-events")).await;
    assert_eq!(de.json["state"], "disabled", "{}", de.text);
}
