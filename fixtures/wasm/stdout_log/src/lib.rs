//! `stdout_log` fixture (WSM-07): `wasi:cli` stdout/stderr lines become service logs.
//!
//! `GET /?lines=N` (default 1) writes `N` stdout lines `fixture stdout line <i>` and one
//! stderr line `fixture stderr line`, then answers `200 logged`.
//!
//! `GET /fields?level=L&msg=M&n=N` (WSM-31) calls `log.log-fields(L, M, {"k0":0,…,"k<N-1>":N-1})`
//! (`msg` default `fields`, `level` default `info`), `200 logged`; a refused call answers
//! `500 log_error:<code>`.

use ::std::io::Write as _;

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

// Imports of the `std:host` world; only `log` is used.
wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

use self::std::host::log;
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        if path == "/fields" {
            let level = match http::query_param(query, "level").as_deref() {
                Some("debug") => log::Level::Debug,
                Some("warn") => log::Level::Warn,
                Some("error") => log::Level::Error,
                _ => log::Level::Info,
            };
            let msg = http::query_param(query, "msg").unwrap_or_else(|| "fields".to_string());
            let n: u32 = http::query_param(query, "n").and_then(|v| v.parse().ok()).unwrap_or(0);
            let pairs: Vec<String> = (0..n).map(|i| format!("\"k{i}\":{i}")).collect();
            let fields = format!("{{{}}}", pairs.join(","));
            return match log::log_fields(level, &msg, &fields) {
                Ok(()) => http::respond_text(response_out, 200, "logged"),
                Err(e) => http::respond_text(response_out, 500, &format!("log_error:{}", e.code)),
            };
        }
        let lines: u32 = http::query_param(query, "lines").and_then(|v| v.parse().ok()).unwrap_or(1);

        let mut out = ::std::io::stdout();
        for i in 1..=lines {
            let _ = writeln!(out, "fixture stdout line {i}");
        }
        let _ = out.flush();
        eprintln!("fixture stderr line");

        http::respond_text(response_out, 200, "logged");
    }
}
