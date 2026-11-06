//! `GET /v1/telemetry/preview`, `PUT /v1/telemetry/console`: operator-only, write only what they say.

use reqwest::Method;
use serde_json::json;

use crate::common::*;

const ZEROS: &str = "00000000000000000000000000000000";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_key_reaches_neither_route() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let r = get(&node, &t.key, "/v1/telemetry/preview").await;
    assert_eq!(r.s(), 403, "{}", r.text);
    let r = put(&node, &t.key, "/v1/telemetry/console", json!({"users": 1})).await;
    assert_eq!(r.s(), 403, "{}", r.text);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_preview_counts_without_the_system_tenant_and_creates_no_id() {
    let node = start_node_with(|c| c.telemetry.url = String::new()).await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("tele");
    ship(
        &node,
        &t.key,
        &slug,
        "export default { fetch() { return new Response('x'); } };",
    )
    .await;
    let r = send(root(&node, Method::GET, "/v1/telemetry/preview")).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert_eq!(r.json["sending"], json!(false));
    assert_eq!(r.json["url"], json!(null));
    let report = &r.json["report"];
    assert_eq!(report["install_id"], json!(ZEROS));
    assert_eq!(report["organizations"], json!(1), "{}", r.text);
    assert_eq!(report["services"], json!(1), "{}", r.text);
    assert_eq!(report["users"], json!(null));
    assert_eq!(report.as_object().unwrap().len(), 7);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_console_count_shows_in_the_report_and_is_bounded() {
    let node = start_node().await;
    let set = |users: serde_json::Value, extra: bool| {
        let mut body = json!({"users": users});
        if extra {
            body["email"] = json!("a@example.com");
        }
        send(root(&node, Method::PUT, "/v1/telemetry/console").json(&body))
    };
    assert_eq!(set(json!(1_000_000_001u64), false).await.s(), 422);
    assert_eq!(set(json!(-1), false).await.s(), 422);
    assert_eq!(set(json!(3), true).await.s(), 422, "unknown keys are refused");
    assert_eq!(set(json!(3), false).await.s(), 204);
    let r = send(root(&node, Method::GET, "/v1/telemetry/preview")).await;
    assert_eq!(r.json["report"]["users"], json!(3), "{}", r.text);
    node.shutdown().await;
}
