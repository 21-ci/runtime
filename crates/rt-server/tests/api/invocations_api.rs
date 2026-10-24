//! Control API tests for async invocations (`/v1/services/{s}/invocations…`, plan-final T32,
//! Q§6.6): list with counts, get, redrive, purge, scopes, tenancy, and views that never carry the
//! request headers, body or query.

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

/// `/fail…` answers 500, everything else 200.
const GUEST: &str = r#"
export default function (req: Request): Response {
  const u = new URL(req.url);
  if (u.pathname.startsWith("/fail")) return new Response("boom", { status: 500 });
  return new Response("ok");
}"#;

/// A deno service with async invocation on and one attempt; `(id, slug)`.
async fn async_service(node: &impl Node, key: &str) -> (String, String) {
    let slug = uniq("asy");
    let id = ship(node, key, &slug, GUEST).await;
    let r = patch(
        node,
        key,
        &format!("/v1/services/{id}"),
        json!({"async": {"enabled": true, "max_attempts": 1}}),
    )
    .await;
    assert_eq!(r.s(), 200, "{}", r.text);
    (id, slug)
}

/// `Prefer: respond-async` request; the invocation id.
async fn accept(node: &impl Node, slug: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    let mut h = vec![("prefer", "respond-async")];
    h.extend_from_slice(headers);
    let r = invoke_with(node, slug, Method::POST, path, &h, body).await;
    assert_eq!(r.s(), 202, "{}", r.text);
    r.json["id"].as_str().expect("invocation id").to_string()
}

async fn wait_state(node: &impl Node, key: &str, svc: &str, id: &str, state: &str) -> Value {
    let path = format!("/v1/services/{svc}/invocations/{id}");
    eventually(Duration::from_secs(20), &format!("invocation {id} {state}"), || {
        let p = path.clone();
        async move {
            let r = get(node, key, &p).await;
            r.json["state"] == json!(state)
        }
    })
    .await;
    get(node, key, &path).await.json
}

node_test! {
/// A view shows the path without the query and `has_query`, and never the request headers, the
/// body or the query string (they may hold the end user's credentials).
async fn invocation_view_has_no_query_headers_or_body() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (svc, slug) = async_service(&node, &t.key).await;
    let id = accept(&node, &slug, "/hook?token=s3cr3t-q", &[("authorization", "Bearer s3cr3t-h"), ("x-custom", "s3cr3t-c")], "s3cr3t-body").await;
    let v = wait_state(&node, &t.key, &svc, &id, "succeeded").await;
    assert_eq!((v["id"].clone(), v["method"].clone(), v["path"].clone(), v["has_query"].clone()), (json!(id), json!("POST"), json!("/hook"), json!(true)), "{v}");
    assert_eq!((v["status"].clone(), v["condition"].clone(), v["attempts"].clone()), (json!(200), json!("success"), json!(1)), "{v}");
    assert_eq!(v["body_bytes"], json!("s3cr3t-body".len()));
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    for k in ["headers", "body", "uri", "query"] {
        assert!(!keys.contains(&k), "`{k}` in the view: {v}");
    }
    let list = get(&node, &t.key, &format!("/v1/services/{svc}/invocations")).await;
    for text in [v.to_string(), list.text.clone()] {
        assert!(!text.contains("s3cr3t"), "a secret leaks into a view: {text}");
    }
    // Without a query.
    let plain = accept(&node, &slug, "/plain", &[], "").await;
    let v = wait_state(&node, &t.key, &svc, &plain, "succeeded").await;
    assert_eq!((v["path"].clone(), v["has_query"].clone()), (json!("/plain"), json!(false)));
    node.shutdown().await;
}
}

node_test! {
/// The list reports `counts: {pending, failed}` for the service, filters by `state`, pages with
/// `next_cursor`; redrive puts failed records back to pending and purge (`state=failed` only)
/// deletes them.
async fn invocations_list_reports_counts() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (svc, slug) = async_service(&node, &t.key).await;
    let ok = accept(&node, &slug, "/ok", &[], "").await;
    let f1 = accept(&node, &slug, "/fail/1", &[], "").await;
    let f2 = accept(&node, &slug, "/fail/2", &[], "").await;
    wait_state(&node, &t.key, &svc, &ok, "succeeded").await;
    let v = wait_state(&node, &t.key, &svc, &f1, "failed").await;
    assert_eq!((v["condition"].clone(), v["error"].clone(), v["status"].clone()), (json!("retries_exhausted"), json!("status_5xx"), json!(500)), "{v}");
    wait_state(&node, &t.key, &svc, &f2, "failed").await;

    let base = format!("/v1/services/{svc}/invocations");
    let all = get(&node, &t.key, &base).await;
    assert_eq!(all.s(), 200, "{}", all.text);
    assert_eq!(all.items().len(), 3);
    assert_eq!(all.json["counts"], json!({"pending": 0, "failed": 2}), "{}", all.text);
    assert!(all.json["next_cursor"].is_null());
    let failed = get(&node, &t.key, &format!("{base}?state=failed")).await;
    let mut ids: Vec<String> = failed.items().iter().map(|x| x["id"].as_str().unwrap().to_string()).collect();
    ids.sort();
    let mut want = vec![f1.clone(), f2.clone()];
    want.sort();
    assert_eq!(ids, want);
    assert_eq!(get(&node, &t.key, &format!("{base}?state=bogus")).await.s(), 422);

    // Paging.
    let p1 = get(&node, &t.key, &format!("{base}?limit=2")).await;
    assert_eq!(p1.items().len(), 2);
    let cur = p1.json["next_cursor"].as_str().expect("next page").to_string();
    let p2 = get(&node, &t.key, &format!("{base}?limit=2&cursor={cur}")).await;
    assert_eq!((p2.items().len(), p2.json["next_cursor"].is_null()), (1, true), "{}", p2.text);
    let mut seen: Vec<Value> = p1.items().iter().chain(p2.items().iter()).map(|x| x["id"].clone()).collect();
    seen.dedup();
    assert_eq!(seen.len(), 3);

    // Get: unknown and malformed ids are 404.
    assert_eq!(get(&node, &t.key, &format!("{base}/inv_01ARZ3NDEKTSV4RRFFQ69G5FAV")).await.s(), 404);
    assert_eq!(get(&node, &t.key, &format!("{base}/nope")).await.s(), 404);

    // Redrive: the failed ones run again (and fail again).
    let r = post(&node, &t.key, &format!("{base}/redrive"), json!({})).await;
    assert_eq!((r.s(), r.json["redriven"].clone()), (200, json!(2)), "{}", r.text);
    let v = wait_state(&node, &t.key, &svc, &f1, "failed").await;
    assert_eq!(v["attempts"], json!(1), "redrive resets attempts: {v}");

    // Purge needs state=failed.
    for q in ["", "?state=pending", "?state=succeeded"] {
        let r = delete(&node, &t.key, &format!("{base}{q}")).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{q}: {}", r.text);
    }
    wait_state(&node, &t.key, &svc, &f2, "failed").await;
    assert_eq!(delete(&node, &t.key, &format!("{base}?state=failed")).await.s(), 204);
    let after = get(&node, &t.key, &base).await;
    assert_eq!(after.json["counts"], json!({"pending": 0, "failed": 0}), "{}", after.text);
    assert_eq!(after.items().iter().map(|x| x["id"].clone()).collect::<Vec<_>>(), [json!(ok)]);
    node.shutdown().await;
}
}

node_test! {
/// Reads need `services:read`, redrive and purge `queue:write`; another tenant (or a key
/// restricted to another project) gets `404` for the service and its invocations.
async fn ac_invocations_api_scopes_and_tenancy() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let (svc, slug) = async_service(&node, &t.key).await;
    let id = accept(&node, &slug, "/ok", &[], "").await;
    wait_state(&node, &t.key, &svc, &id, "succeeded").await;
    let base = format!("/v1/services/{svc}/invocations");
    let one = format!("{base}/{id}");

    let (_, invoke_only) = make_key(&node, &t.id, &["invoke"], None).await;
    let (_, read_only) = make_key(&node, &t.id, &["services:read"], None).await;
    let (_, qw) = make_key(&node, &t.id, &["services:read", "queue:write"], None).await;
    for (m, p) in [(Method::GET, base.clone()), (Method::GET, one.clone())] {
        assert_eq!(call(api(&node, &invoke_only, m.clone(), &p), None).await.code(), "insufficient_scope", "{m} {p}");
        assert_eq!(call(api(&node, &read_only, m, &p), None).await.s(), 200);
    }
    let writes = [(Method::POST, format!("{base}/redrive")), (Method::DELETE, format!("{base}?state=failed"))];
    for (m, p) in &writes {
        let r = call(api(&node, &read_only, m.clone(), p), Some(json!({}))).await;
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{m} {p}: {}", r.text);
        let r = call(api(&node, &qw, m.clone(), p), Some(json!({}))).await;
        assert!(r.s() == 200 || r.s() == 204, "{m} {p}: {}", r.text);
    }

    let other = new_tenant(&node, "pro").await;
    let prj = create_project(&node, &t.key, "elsewhere", &[]).await;
    let (_, restricted) = make_key(&node, &t.id, &ALL_SCOPES, Some(vec![prj])).await;
    for key in [&other.key, &restricted] {
        assert_eq!(get(&node, key, &base).await.s(), 404);
        assert_eq!(get(&node, key, &one).await.s(), 404);
        assert_eq!(post(&node, key, &format!("{base}/redrive"), json!({})).await.s(), 404);
        assert_eq!(delete(&node, key, &format!("{base}?state=failed")).await.s(), 404);
    }
    // An invocation id under another tenant's service is not found either.
    let (svc2, _) = async_service(&node, &other.key).await;
    assert_eq!(get(&node, &other.key, &format!("/v1/services/{svc2}/invocations/{id}")).await.s(), 404);
    node.shutdown().await;
}
}
