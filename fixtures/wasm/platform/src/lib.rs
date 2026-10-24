//! `platform` fixture (WSM-20): `std:host/platform` (operator bindings) from `/wit/rt-host.wit`.
//!
//! `GET /fetch?binding=B&method=M&path=/p` -> `platform.fetch(B, M /p)` with the guest's own
//! `authorization: Bearer guest-token` and `x-from: platform` headers and body `ping`; answers
//! `200` with `status=<n>`, `reason=<x-rt-reason or ->`, `body=<text>` lines, or `error=<code>`.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

// `std` is also the Rust crate name: the generated `std` module is reached through `self`.
use self::std::host::{call, platform};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

fn do_fetch(binding: &str, method: &str, path: &str) -> String {
    let req = call::Request {
        method: method.to_string(),
        path_with_query: path.to_string(),
        headers: vec![
            ("authorization".to_string(), b"Bearer guest-token".to_vec()),
            ("x-from".to_string(), b"platform".to_vec()),
        ],
        body: b"ping".to_vec(),
    };
    match platform::fetch(binding, &req) {
        Ok(resp) => {
            let reason = resp
                .headers
                .iter()
                .find(|(k, _)| k == "x-rt-reason")
                .map_or("-".to_string(), |(_, v)| String::from_utf8_lossy(v).into_owned());
            let headers: Vec<&str> = resp.headers.iter().map(|(k, _)| k.as_str()).collect();
            format!(
                "status={}\nreason={reason}\nbody={}\nheaders={}\n",
                resp.status,
                String::from_utf8_lossy(&resp.body),
                headers.join(",")
            )
        }
        Err(e) => format!("error={}\n", e.code),
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let text = match path {
            "/fetch" => do_fetch(
                &http::query_param(query, "binding").unwrap_or_default(),
                &http::query_param(query, "method").unwrap_or_else(|| "GET".to_string()),
                &http::query_param(query, "path").unwrap_or_else(|| "/".to_string()),
            ),
            _ => "error=unknown_route\n".to_string(),
        };
        http::respond_text(response_out, 200, &text);
    }
}
