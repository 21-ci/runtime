//! Edge cases of the invoke path found by the edge-case catalogue (scratchpad edge-cases.md,
//! section 2): runtime credentials and header hygiene in both directions, path allow-lists.

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

/// Create a deno service with extra create-body fields and upload `src` (activated).
async fn svc_with(node: &impl Node, key: &str, slug: &str, extra: Value, src: &str) -> String {
    let mut body = json!({"slug": slug, "runtime": "deno"});
    if let (Some(b), Value::Object(e)) = (body.as_object_mut(), extra) {
        b.extend(e);
    }
    let r = post(node, key, "/v1/services", body).await;
    assert_eq!(r.s(), 201, "create `{slug}`: {}", r.text);
    let id = r.json["id"].as_str().expect("id").to_string();
    let up = upload_ts(node, key, &id, src, "activate=1").await;
    assert_eq!(up.s(), 201, "upload `{slug}`: {}", up.text);
    id
}

fn raw_get(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").into_bytes()
}

node_test! {
/// INV-009: the root token in `Authorization` never reaches a guest, on a public service too
/// (SECURITY.md "Request path": `Authorization` carrying an `rtk_` key or the root token is
/// removed). The guest's own bearer tokens still pass.
async fn inv_009_the_root_token_never_reaches_a_guest() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv009");
    svc_with(&node, &t.key, &slug, json!({}), guests::ECHO).await;
    let bearer = format!("Bearer {}", node.root_token());
    let r = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", bearer.as_str())], "").await;
    assert_eq!(r.s(), 200, "{}", r.text);
    assert!(r.json["headers"].get("authorization").is_none(), "root token leaked to the guest: {}", r.json["headers"]);
    assert!(!r.text.contains(node.root_token()), "{}", r.text);
    let own = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", "Bearer app-token-1")], "").await;
    assert_eq!(own.json["headers"]["authorization"], json!("Bearer app-token-1"));
    node.shutdown().await;
}
}

node_test! {
/// INV-021: `ingress.allow_paths` is matched on the path the guest will see, so dot segments
/// (raw or percent-encoded) cannot climb out of an allowed prefix.
async fn inv_021_allow_paths_cannot_be_bypassed_with_dot_segments() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv021");
    svc_with(&node, &t.key, &slug, json!({"ingress": {"allow_paths": ["/public/*"]}}), guests::ECHO).await;
    let ok = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/public/a")), Duration::from_secs(10)).await.expect("response");
    assert_eq!(ok.status, 200, "{}", ok.text());
    for evil in ["/public/../admin", "/public/%2e%2e/admin", "/public/%2E./admin", "/public/.%2e/admin", "/public/./../admin"] {
        let r = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}{evil}")), Duration::from_secs(10)).await.expect("response");
        assert_eq!(r.status, 403, "`{evil}` must not reach the guest: {}", r.text());
    }
    // A dot segment that stays inside the prefix is fine and the guest sees the resolved path.
    let inside = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/public/x/../y")), Duration::from_secs(10)).await.expect("response");
    assert_eq!(inside.status, 200, "{}", inside.text());
    assert_eq!(inside.json()["path"], json!("/public/y"), "{}", inside.text());
    node.shutdown().await;
}
}

/// A guest answering with hop-by-hop and framing headers of its own.
const FRAMING: &str = r#"
export default function (): Response {
  return new Response("0123456789", { headers: {
    "content-length": "3", "connection": "upgrade", "upgrade": "h2c", "keep-alive": "timeout=1",
    "proxy-connection": "keep-alive", "x-own": "kept",
  } });
}"#;

node_test! {
/// DREQ-004: the response framing is the runtime's: a guest's `content-length` that disagrees with
/// its body, and hop-by-hop headers (`connection`, `upgrade`, `keep-alive`, …), never reach the
/// client; its other headers do.
async fn dreq_004_guest_hop_by_hop_and_framing_headers_are_not_forwarded() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("dreq004");
    svc_with(&node, &t.key, &slug, json!({}), FRAMING).await;
    let r = raw_tcp(node.addrs.public, &raw_get(&format!("/s/{slug}/")), Duration::from_secs(10)).await.expect("response");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.text(), "0123456789", "the whole body arrives");
    assert_eq!(r.header("x-own"), Some("kept"));
    for h in ["upgrade", "keep-alive", "proxy-connection"] {
        assert_eq!(r.header(h), None, "`{h}` must not be forwarded");
    }
    assert_ne!(r.header("content-length"), Some("3"));
    assert_ne!(r.header("connection").map(str::to_ascii_lowercase).as_deref(), Some("upgrade"));
    node.shutdown().await;
}
}

/// Reads the whole body; answers its length, or the error message when reading failed.
const BODY_LEN: &str = r#"
export default async function (req: Request): Promise<Response> {
  try {
    const b = await req.arrayBuffer();
    return Response.json({ len: b.byteLength });
  } catch (e) {
    return Response.json({ error: String((e as Error).message ?? e) });
  }
}"#;

node_test! {
/// INV-002: malformed or unknown slugs are `404 not_found`, plain text with `x-rt-reason`.
async fn inv_002_bad_or_unknown_slugs_are_plain_404s() {
    let node = start_node().await;
    for path in ["/s/", "/s//x", "/s/%2F/x", "/s/UNKNOWN", "/s/no-such-service/x"] {
        let r = raw_tcp(node.addrs.public, &raw_get(path), Duration::from_secs(5)).await.expect("response");
        assert_eq!(r.status, 404, "{path}: {}", r.text());
        assert_eq!(r.header("x-rt-reason"), Some("not_found"), "{path}");
        assert!(r.header("content-type").is_some_and(|c| c.starts_with("text/plain")), "{path}");
    }
    node.shutdown().await;
}
}

node_test! {
/// INV-008: the root token is not an invoke credential: an `access: key` service answers `401`.
async fn inv_008_the_root_token_does_not_open_an_access_key_service() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv008");
    svc_with(&node, &t.key, &slug, json!({"access": "key"}), guests::ECHO).await;
    let bearer = format!("Bearer {}", node.root_token());
    let r = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", bearer.as_str())], "").await;
    assert_eq!(r.s(), 401, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
/// INV-012: on a public service a wrong `rtk_` key is never checked, so it never counts as a
/// failure: the twelfth request still runs.
async fn inv_012_wrong_keys_on_a_public_service_are_not_rate_limited() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv012");
    svc_with(&node, &t.key, &slug, json!({}), guests::ECHO).await;
    let wrong = "Bearer rtk_01J0000000000000000000000Z_wrongwrongwrongwrongwrongwrongwr";
    for i in 0..12 {
        let r = invoke_with(&node, &slug, Method::GET, "/", &[("authorization", wrong)], "").await;
        assert_eq!(r.s(), 200, "request {i}: {}", r.text);
        assert!(r.json["headers"].get("authorization").is_none(), "the key-shaped bearer is still removed");
    }
    node.shutdown().await;
}
}

node_test! {
/// INV-024 / INV-025: a burst below `rps` is raised to `rps`; the limit answers `429` with
/// `Retry-After` ≥ 1; requests the ingress rules refuse take no token.
async fn inv_024_025_service_rate_limit_burst_and_refusals() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv024");
    svc_with(
        &node,
        &t.key,
        &slug,
        json!({"ingress": {"rate_limit": {"rps": 2, "burst": 1}, "allowed_methods": ["GET"]}}),
        guests::ECHO,
    )
    .await;
    for _ in 0..5 {
        let r = invoke_with(&node, &slug, Method::POST, "/", &[], "").await;
        assert_eq!(r.s(), 405, "{}", r.text);
    }
    let a = invoke(&node, &slug, "/").await;
    let b = invoke(&node, &slug, "/").await;
    assert_eq!((a.s(), b.s()), (200, 200), "burst raised to rps=2; refused requests took no token");
    let c = invoke(&node, &slug, "/").await;
    assert_eq!(c.s(), 429, "{}", c.text);
    let retry: u64 = c.header("retry-after").and_then(|v| v.parse().ok()).expect("retry-after");
    assert!(retry >= 1);

    let off = uniq("inv024off");
    svc_with(&node, &t.key, &off, json!({"ingress": {"rate_limit": {"rps": 0, "burst": 5}}}), guests::ECHO).await;
    for i in 0..20 {
        assert_eq!(invoke(&node, &off, "/").await.s(), 200, "rps 0 is no limit (request {i})");
    }
    node.shutdown().await;
}
}

node_test! {
/// INV-026: a streamed body of exactly `limits.max_body_bytes` reaches the guest whole; one byte
/// more fails the guest's read (`request_too_large`) and a declared length over the limit is
/// `413` at ingress.
async fn inv_026_the_body_limit_is_exact() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("inv026");
    svc_with(&node, &t.key, &slug, json!({"limits": {"max_body_bytes": 4096}}), BODY_LEN).await;
    let url = node.public_url(&format!("/s/{slug}/"));
    let stream = |n: usize| {
        let chunks: Vec<Result<Vec<u8>, std::io::Error>> = vec![Ok(vec![b'a'; n / 2]), Ok(vec![b'b'; n - n / 2])];
        reqwest::Body::wrap_stream(futures_util::stream::iter(chunks))
    };
    let r = send(node.http().post(&url).body(stream(4096))).await;
    assert_eq!(r.json, json!({"len": 4096}), "{}", r.text);
    let r = send(node.http().post(&url).body(stream(4097))).await;
    assert!(r.json["error"].as_str().is_some_and(|e| e.contains("request_too_large")), "{}", r.text);
    let r = send(node.http().post(&url).body(vec![0u8; 4097])).await;
    assert_eq!(r.s(), 413, "{}", r.text);
    node.shutdown().await;
}
}
