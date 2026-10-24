//! Upload size and memory edge cases (scratchpad edge-cases.md CTRL-003, CTRL-005, CTRL-006,
//! RES-002). The ones that move 64 MiB are `#[ignore = "disk"]` and named `disk_…`; CI runs them
//! on the Linux runner with `-- --ignored disk_`.

use std::time::Duration;

use bytes::Bytes;
use reqwest::Method;

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

/// A request whose body is still being sent: the upload holds its slot and its memory charge.
fn held_upload(
    node: &impl Node,
    key: &str,
    svc: &str,
    declared: Option<u64>,
) -> (
    tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    tokio::task::JoinHandle<Resp>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|c| (c, rx)) });
    let mut rb = api(
        node,
        key,
        Method::POST,
        &format!("/v1/services/{svc}/versions?activate=0"),
    )
    .header("content-type", "application/wasm")
    .body(reqwest::Body::wrap_stream(stream));
    if let Some(n) = declared {
        rb = rb.header("content-length", n.to_string());
    }
    (tx, tokio::spawn(async move { send(rb).await }))
}

node_test! {
/// CTRL-006: when the memory governor cannot charge an upload's buffer the answer is `429
/// rate_limited` (not 500), and the slot is free again for the next upload.
async fn ctrl_006_an_upload_the_governor_refuses_is_429() {
    let node = start_node_with(|c| c.memory.budget_mb = 48).await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("u006"), "deno").await;
    // No Content-Length: the upload is charged the 64 MiB maximum, more than the budget.
    let (tx, task) = held_upload(&node, &t.key, &svc, None);
    tx.send(Ok(Bytes::from_static(b"x"))).await.ok();
    drop(tx);
    let r = task.await.expect("join");
    assert_eq!((r.s(), r.code()), (429, "rate_limited".to_string()), "{}", r.text);
    let ok = upload_ts(&node, &t.key, &svc, "export default () => new Response('x');", "activate=0").await;
    assert!(ok.s() < 300, "a small upload with its length passes: {}", ok.text);
    node.shutdown().await;
}
}

node_test! {
/// RES-002: uploads declaring 64 MiB each are charged before their bodies arrive; once the 200
/// MiB budget is spent the next is `429 rate_limited`, and when the held ones end nothing stays
/// charged.
async fn res_002_concurrent_large_uploads_are_bounded_by_the_budget() {
    let node = start_node_with(|c| c.memory.budget_mb = 200).await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("r002"), "deno").await;
    let mut held = Vec::new();
    let mut refused = None;
    for _ in 0..rt_api::MAX_CONCURRENT_UPLOADS {
        let (tx, task) = held_upload(&node, &t.key, &svc, Some(64 * 1024 * 1024));
        tokio::time::sleep(Duration::from_millis(300)).await;
        if task.is_finished() {
            refused = Some(task.await.expect("join"));
            break;
        }
        held.push((tx, task));
    }
    let refused = refused.expect("the budget refuses one of the 64 MiB uploads");
    assert_eq!((refused.s(), refused.code()), (429, "rate_limited".to_string()), "{}", refused.text);
    assert!(!held.is_empty(), "at least one 64 MiB upload fits a 200 MiB budget");
    for (tx, task) in held {
        drop(tx);
        let _ = task.await;
    }
    let (tx, task) = held_upload(&node, &t.key, &svc, Some(64 * 1024 * 1024));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!task.is_finished(), "the charges were released: a new 64 MiB upload is accepted and waits for its body");
    drop(tx);
    let _ = task.await;
    node.shutdown().await;
}
}

fn payload(n: usize) -> Vec<u8> {
    vec![0u8; n]
}

node_test! {
#[ignore = "disk"]
/// CTRL-003: exactly 64 MiB is read (the raw body is not under the 1 MiB JSON limit) and judged
/// on its content; one byte more is `413 too_large`.
async fn disk_ctrl_003_the_upload_limit_is_exactly_64_mib() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("c003"), "wasm").await;
    let r = upload(&node, &t.key, &svc, "application/wasm", payload(rt_api::MAX_UPLOAD_BODY), "activate=0").await;
    // Read whole and judged by the component validator (its own 50 MB cap), not cut by transport.
    assert!(!r.text.contains("uploads are limited"), "exactly the limit is read: {}", &r.text[..r.text.len().min(200)]);
    let r = upload(&node, &t.key, &svc, "application/wasm", payload(rt_api::MAX_UPLOAD_BODY + 1), "activate=0").await;
    assert_eq!((r.s(), r.code()), (413, "too_large".to_string()));
    assert!(r.text.contains("uploads are limited"), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// CTRL-005: a `Content-Length` smaller than what is sent: only the declared bytes are the body
/// (and the charge); the rest is never buffered as part of the upload, and the node keeps serving.
/// `RT_TEST_EXTRA_MB` (default 1) sizes the extra bytes.
async fn ctrl_005_a_lying_content_length_reads_only_the_declared_bytes() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let svc = create_service(&node, &t.key, &uniq("c005"), "wasm").await;
    let mut req = format!(
        "POST /v1/services/{svc}/versions?activate=0 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/wasm\r\nContent-Length: 10\r\n\r\n",
        t.key
    )
    .into_bytes();
    let extra_mb: usize = std::env::var("RT_TEST_EXTRA_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    req.extend(payload(extra_mb * 1024 * 1024));
    let r = raw_tcp(node.addrs.control, &req, Duration::from_secs(30)).await.expect("response");
    assert!(r.status == 422 || r.status == 400, "judged on the 10 declared bytes: {} {}", r.status, r.text());
    assert_eq!(get(&node, &t.key, "/v1/services").await.s(), 200, "the node keeps serving");
    node.shutdown().await;
}
}
