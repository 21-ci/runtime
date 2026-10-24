//! `kv` fixture (WSM-06): uses `std:host/kv` from `/wit/rt-host.wit`.
//!
//! - `GET  /set?key=K&value=V`        -> `kv.set(K, bytes(V))`, `200 ok`
//! - `PUT  /set?key=K` (body = value) -> `kv.set(K, bytes(body))`, `200 ok`
//! - `GET  /get?key=K`                -> `200 <value>` or `404 missing`
//! - `GET  /roundtrip?key=K&value=V`  -> set then get in one request, `200 <value read back>`
//! - `GET  /delete?key=K`             -> `200 deleted` / `200 absent`
//! - `GET  /incr?key=K&delta=D&ttl=S&n=N` -> `kv.incr(K, D (default 1), {ttl-seconds: S})` N times
//!   (default 1), `200 <last value>`
//! - `GET  /cas?key=K&expected=E&next=N&ttl=S` -> `kv.cas` (absent param = null), `200 true|false`
//! - `GET  /expire?key=K&ttl=S` -> `kv.expire` (absent `ttl` clears), `200 true|false`
//! - `GET  /ttl?key=K` -> `kv.ttl`, `200 <seconds left>` or `200 none`
//!
//! Host errors (`kv.error { code, message }`) become `500 kv_error:<code>`.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

// Imports of the `std:host` world (kv, sql, blob, log); only `kv` is used, so only `kv` ends up
// in the component's import list.
wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

// `std` is also the Rust crate name: the generated `std` module is reached through `self`.
use self::std::host::kv;
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, Method, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

fn value_bytes(v: kv::Value) -> Vec<u8> {
    match v {
        kv::Value::Json(s) => s.into_bytes(),
        kv::Value::Bytes(b) => b,
    }
}

fn kv_error(response_out: ResponseOutparam, e: kv::Error) {
    http::respond_text(response_out, 500, &format!("kv_error:{}", e.code));
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let key = http::query_param(query, "key").unwrap_or_default();
        let value_param = http::query_param(query, "value");
        let method = request.method();
        let opts = kv::SetOptions { ttl_seconds: None };

        match path {
            "/set" => {
                let data = match (&method, value_param) {
                    (Method::Put | Method::Post, _) => http::read_body(request),
                    (_, Some(v)) => v.into_bytes(),
                    (_, None) => Vec::new(),
                };
                match kv::set(key.as_bytes(), &kv::Value::Bytes(data), opts) {
                    Ok(()) => http::respond_text(response_out, 200, "ok"),
                    Err(e) => kv_error(response_out, e),
                }
            }
            "/get" => match kv::get(key.as_bytes()) {
                Ok(Some(v)) => http::respond(response_out, 200, "application/octet-stream", &value_bytes(v)),
                Ok(None) => http::respond_text(response_out, 404, "missing"),
                Err(e) => kv_error(response_out, e),
            },
            "/roundtrip" => {
                let data = value_param.unwrap_or_default().into_bytes();
                if let Err(e) = kv::set(key.as_bytes(), &kv::Value::Bytes(data), opts) {
                    return kv_error(response_out, e);
                }
                match kv::get(key.as_bytes()) {
                    Ok(Some(v)) => http::respond(response_out, 200, "application/octet-stream", &value_bytes(v)),
                    Ok(None) => http::respond_text(response_out, 404, "missing"),
                    Err(e) => kv_error(response_out, e),
                }
            }
            "/delete" => match kv::delete(key.as_bytes()) {
                Ok(true) => http::respond_text(response_out, 200, "deleted"),
                Ok(false) => http::respond_text(response_out, 200, "absent"),
                Err(e) => kv_error(response_out, e),
            },
            "/incr" => {
                let num = |name: &str| http::query_param(query, name).and_then(|v| v.parse::<i64>().ok());
                let delta = num("delta").unwrap_or(1);
                let ttl = num("ttl").map(|s| s as u64);
                let mut last = 0;
                for _ in 0..num("n").unwrap_or(1) {
                    match kv::incr(key.as_bytes(), delta, kv::SetOptions { ttl_seconds: ttl }) {
                        Ok(v) => last = v,
                        Err(e) => return kv_error(response_out, e),
                    }
                }
                http::respond_text(response_out, 200, &last.to_string())
            }
            // `expected` / `next` absent in the query: `none` (absent / delete). `ttl` as for `/incr`.
            "/cas" => {
                let bytes = |name: &str| http::query_param(query, name).map(|v| kv::Value::Bytes(v.into_bytes()));
                let ttl = http::query_param(query, "ttl").and_then(|v| v.parse::<u64>().ok());
                let (expected, next) = (bytes("expected"), bytes("next"));
                match kv::cas(key.as_bytes(), expected.as_ref(), next.as_ref(), kv::SetOptions { ttl_seconds: ttl }) {
                    Ok(swapped) => http::respond_text(response_out, 200, &swapped.to_string()),
                    Err(e) => kv_error(response_out, e),
                }
            }
            // `ttl` absent: clears the expiry.
            "/expire" => {
                let ttl = http::query_param(query, "ttl").and_then(|v| v.parse::<u64>().ok());
                match kv::expire(key.as_bytes(), kv::SetOptions { ttl_seconds: ttl }) {
                    Ok(found) => http::respond_text(response_out, 200, &found.to_string()),
                    Err(e) => kv_error(response_out, e),
                }
            }
            "/ttl" => match kv::ttl(key.as_bytes()) {
                Ok(Some(left)) => http::respond_text(response_out, 200, &left.to_string()),
                Ok(None) => http::respond_text(response_out, 200, "none"),
                Err(e) => kv_error(response_out, e),
            },
            _ => http::respond_text(response_out, 404, "unknown route"),
        }
    }
}
