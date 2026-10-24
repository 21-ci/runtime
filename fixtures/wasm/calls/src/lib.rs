//! `calls` fixture (WSM-15): `std:host/call` and `std:host/queue` from `/wit/rt-host.wit`.
//!
//! - `GET /call?slug=S&path=/p` -> `call(S, POST /p, x-from: calls, body "ping")`; answers `200`
//!   with `status=<n>`, `reason=<x-rt-reason or ->`, `body=<text>` lines, or `error=<code>`
//! - `GET /enqueue?slug=S&body=B&delay=N&dedup=D` -> `enqueue(S, B, {delay N s, dedup D})`;
//!   answers `200` with `id=<message id>` or `error=<code>`

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

// `std` is also the Rust crate name: the generated `std` module is reached through `self`.
use self::std::host::{call, queue};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

fn do_call(slug: &str, path: &str) -> String {
    let req = call::Request {
        method: "POST".to_string(),
        path_with_query: path.to_string(),
        headers: vec![("x-from".to_string(), b"calls".to_vec())],
        body: b"ping".to_vec(),
    };
    match call::call(slug, &req) {
        Ok(resp) => {
            let reason = resp
                .headers
                .iter()
                .find(|(k, _)| k == "x-rt-reason")
                .map_or("-".to_string(), |(_, v)| String::from_utf8_lossy(v).into_owned());
            format!("status={}\nreason={reason}\nbody={}\n", resp.status, String::from_utf8_lossy(&resp.body))
        }
        Err(e) => format!("error={}\n", e.code),
    }
}

fn do_enqueue(slug: &str, body: &str, delay: Option<u64>, dedup: Option<String>) -> String {
    let opts = queue::SendOptions { delay_seconds: delay, dedup_id: dedup };
    match queue::enqueue(slug, body.as_bytes(), &opts) {
        Ok(id) => format!("id={id}\n"),
        Err(e) => format!("error={}\n", e.code),
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let slug = http::query_param(query, "slug").unwrap_or_default();
        let text = match path {
            "/call" => do_call(&slug, &http::query_param(query, "path").unwrap_or_else(|| "/".to_string())),
            "/enqueue" => do_enqueue(
                &slug,
                &http::query_param(query, "body").unwrap_or_default(),
                http::query_param(query, "delay").and_then(|v| v.parse().ok()),
                http::query_param(query, "dedup"),
            ),
            _ => "error=unknown_route\n".to_string(),
        };
        http::respond_text(response_out, 200, &text);
    }
}
