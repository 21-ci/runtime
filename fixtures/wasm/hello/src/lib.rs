//! `hello` fixture (WSM-01).
//!
//! - `GET /...`      -> `200`, body `hello wasm`
//! - `GET /trap`     -> the guest traps (`panic = "abort"` => `unreachable`): outcome `guest_error`
//! - `GET /status/N` -> responds with status `N` (any body)
//! - `POST /echo`    -> echoes the request body
//! - `GET /header/H` -> `200` with the request header `H` (all values, `, `-joined), `404 absent`
//! - `GET /secret?name=N` (WSM-33) -> `200` with `secrets.get(N)`, `none` when unset, or
//!   `error:<code>` when refused
//! - `GET /schedule?op=after&seconds=S&id=I` / `op=cancel&id=I` / `op=list` (WSM-35) -> the schedule's
//!   `id|runAt`, `true`/`false`, or the pending ids; `error:<code>` when refused
//! - `GET /lock?key=K&wait=MS&hold=MS&op=hold|leak` (WSM-34) -> `held` after holding K for `hold`
//!   ms, `leaked` when `op=leak` (the lease is never released), or `error:<code>` when refused

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

// Imports of the `std:host` world; only `secrets` is used.
wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

use self::std::host::{lock, schedule, secrets};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

/// Waits `ms` on the guest's monotonic clock (I/O, so the host's time moves on meanwhile).
fn sleep_ms(ms: u64) {
    if ms > 0 {
        wasip2::clocks::monotonic_clock::subscribe_duration(ms * 1_000_000).block();
    }
}

struct Component;

wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        if path == "/trap" {
            panic!("hello fixture: trap requested");
        }
        if path == "/secret" {
            let name = http::query_param(query, "name").unwrap_or_default();
            match secrets::get(&name) {
                Ok(Some(value)) => http::respond_text(response_out, 200, &value),
                Ok(None) => http::respond_text(response_out, 200, "none"),
                Err(e) => http::respond_text(response_out, 200, &format!("error:{}", e.code)),
            }
            return;
        }
        if path == "/schedule" {
            // `op=after|cancel|list`; `after` answers `id|runAt`, `list` the pending ids.
            let op = http::query_param(query, "op").unwrap_or_default();
            let id = http::query_param(query, "id");
            let text = match op.as_str() {
                "after" => {
                    let seconds = http::query_param(query, "seconds").and_then(|v| v.parse().ok()).unwrap_or(60);
                    match schedule::after(seconds, "{\"n\":1}", id.as_deref()) {
                        Ok(entry) => format!("{}|{}", entry.id, entry.run_at_ms),
                        Err(e) => format!("error:{}", e.code),
                    }
                }
                "cancel" => match schedule::cancel(&id.unwrap_or_default()) {
                    Ok(removed) => removed.to_string(),
                    Err(e) => format!("error:{}", e.code),
                },
                _ => match schedule::list(None, 10) {
                    Ok(page) => page.entries.iter().map(|e| e.id.as_str()).collect::<Vec<_>>().join(","),
                    Err(e) => format!("error:{}", e.code),
                },
            };
            http::respond_text(response_out, 200, &text);
            return;
        }
        if path == "/lock" {
            // `op=leak` keeps the lease without releasing it (the task ends holding it).
            let key = http::query_param(query, "key").unwrap_or_default();
            let number = |name: &str, default: u64| http::query_param(query, name).and_then(|v| v.parse().ok()).unwrap_or(default);
            let op = http::query_param(query, "op").unwrap_or_default();
            match lock::acquire(&key, number("wait", 10_000) as u32, 30_000) {
                Err(e) => http::respond_text(response_out, 200, &format!("error:{}", e.code)),
                Ok(lease) if op == "leak" => {
                    core::mem::forget(lease);
                    http::respond_text(response_out, 200, "leaked");
                }
                Ok(lease) => {
                    sleep_ms(number("hold", 0));
                    lease.release();
                    http::respond_text(response_out, 200, "held");
                }
            }
            return;
        }
        if let Some(code) = path.strip_prefix("/status/") {
            let status: u16 = code.parse().unwrap_or(200);
            http::respond_text(response_out, status, "status");
            return;
        }
        if let Some(name) = path.strip_prefix("/header/") {
            let values: Vec<String> =
                request.headers().get(name).iter().map(|v| String::from_utf8_lossy(v).into_owned()).collect();
            if values.is_empty() {
                http::respond_text(response_out, 404, "absent");
            } else {
                http::respond_text(response_out, 200, &values.join(", "));
            }
            return;
        }
        if path == "/echo" {
            let body = http::read_body(request);
            http::respond(response_out, 200, "application/octet-stream", &body);
            return;
        }
        http::respond_text(response_out, 200, "hello wasm");
    }
}
