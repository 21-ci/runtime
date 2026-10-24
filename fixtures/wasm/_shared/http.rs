// Shared helpers for the WASM guest fixtures. Included by each fixture crate with
//
//     #[allow(dead_code)]
//     mod http { include!("../../_shared/http.rs"); }
//
// so every fixture stays a standalone crate (own `[workspace]`, own Cargo.lock).
//
// Fixtures are built with `std` on wasm32-wasip2. That component imports `wasi:cli/*`,
// `wasi:filesystem/*`, `wasi:sockets/*`, `wasi:clocks/*`, `wasi:random/*`, `wasi:io/*`
// - all of which are linkable under contracts C-7 (filesystem has no preopens, sockets deny
// every address). The sample `rust-hello` is the `no_std` variant with a minimal import set.

use std::io::Read as _;

use wasip2::http::types::{Fields, IncomingBody, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};

/// `/path?query` as the guest sees it (the runtime strips `/s/{slug}`).
pub fn path_and_query(req: &IncomingRequest) -> String {
    req.path_with_query().unwrap_or_else(|| "/".to_string())
}

/// Split `/path?query` into `(path, query)`.
pub fn split_query(pq: &str) -> (&str, &str) {
    pq.split_once('?').unwrap_or((pq, ""))
}

/// First `key=value` of `query` (no percent-decoding: fixtures only use plain tokens).
pub fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then(|| v.to_string())
    })
}

/// Drain the request body.
pub fn read_body(req: IncomingRequest) -> Vec<u8> {
    let body = req.consume().expect("request body");
    let mut stream = body.stream().expect("request body stream");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    drop(stream);
    drop(IncomingBody::finish(body));
    buf
}

/// Send a complete response.
pub fn respond(response_out: ResponseOutparam, status: u16, content_type: &str, body: &[u8]) {
    let headers = Fields::new();
    headers.set("content-type", &[content_type.as_bytes().to_vec()]).expect("set content-type");

    let response = OutgoingResponse::new(headers);
    response.set_status_code(status).expect("set status");
    let out_body = response.body().expect("response body");

    // Hand the head to the host first, then stream the body.
    ResponseOutparam::set(response_out, Ok(response));

    let stream = out_body.write().expect("body stream");
    // `blocking-write-and-flush` accepts at most 4096 bytes per call.
    for chunk in body.chunks(4096) {
        stream.blocking_write_and_flush(chunk).expect("write body");
    }
    drop(stream);
    OutgoingBody::finish(out_body, None).expect("finish body");
}

pub fn respond_text(response_out: ResponseOutparam, status: u16, body: &str) {
    respond(response_out, status, "text/plain", body.as_bytes());
}
