//! `rpc` fixture: both sides of an RPC (`/wit/rt-host.wit`, world `rpc-service`).
//!
//! Callee (`std:host/rpc` export `handle`):
//! - `echo` answers its argument; `nested.add` answers `a + b` of `{"a", "b"}`
//! - `whoami` answers the caller record as JSON (`null` without one)
//! - `fail` answers the error `{code: "out_of_stock", message: "no stock left"}`
//! - `big` answers a 300 000-character string (past the 256 KiB cap)
//! - anything else: `method_not_found`
//!
//! Caller (HTTP): `PUT /rpc?slug=S&method=M` with the JSON argument as the body ->
//! `call-rpc(S, M, body)`; `PUT /sdk?name=N&method=M` -> `sdk.invoke(N, M, body)` (an operator SDK).
//! Both answer `200` with `ok=<json>` or `error=<code>` and `message=<text>`.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "rpc-service",
});

// `std` is also the Rust crate name: the generated `std` module is reached through `self`.
use self::exports::std::host::rpc;
use self::std::host::{call, sdk};
use wasip2::exports::http::incoming_handler::Guest as HttpGuest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);
export!(Component);

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn opt_json(s: &Option<String>) -> String {
    s.as_deref().map_or_else(|| "null".to_string(), json_str)
}

/// The integer after `"key":` in a flat JSON object (fixture-grade parsing).
fn int_field(json: &str, key: &str) -> Option<i64> {
    let at = json.find(&format!("\"{key}\""))?;
    let rest = json[at + key.len() + 2..].trim_start().strip_prefix(':')?.trim_start();
    let end = rest.find(|c: char| !(c.is_ascii_digit() || c == '-')).unwrap_or(rest.len());
    rest[..end].parse().ok()
}

impl rpc::Guest for Component {
    fn handle(method: String, args_json: String, caller: Option<rpc::Caller>) -> Result<String, rpc::Error> {
        match method.as_str() {
            "echo" => Ok(args_json),
            "nested.add" => {
                let a = int_field(&args_json, "a").unwrap_or(0);
                let b = int_field(&args_json, "b").unwrap_or(0);
                Ok((a + b).to_string())
            }
            "whoami" => Ok(match caller {
                None => "null".to_string(),
                Some(c) => format!(
                    "{{\"service\":{},\"slug\":{},\"project\":{},\"environment\":{},\"tenant\":{},\"plan\":{}}}",
                    json_str(&c.service),
                    opt_json(&c.slug),
                    opt_json(&c.project),
                    opt_json(&c.environment),
                    json_str(&c.tenant),
                    opt_json(&c.plan)
                ),
            }),
            "fail" => Err(rpc::Error { code: "out_of_stock".to_string(), message: "no stock left".to_string() }),
            "big" => Ok(json_str(&"x".repeat(300_000))),
            other => Err(rpc::Error { code: "method_not_found".to_string(), message: format!("no RPC method {other}") }),
        }
    }
}

impl HttpGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let slug = http::query_param(query, "slug").unwrap_or_default();
        let method = http::query_param(query, "method").unwrap_or_default();
        let text = match path {
            "/rpc" => {
                let body = http::read_body(request);
                let args = String::from_utf8_lossy(&body).into_owned();
                match call::call_rpc(&slug, &method, &args) {
                    Ok(value) => format!("ok={value}\n"),
                    Err(e) => format!("error={}\nmessage={}\n", e.code, e.message),
                }
            }
            "/sdk" => {
                let body = http::read_body(request);
                let args = String::from_utf8_lossy(&body).into_owned();
                let name = http::query_param(query, "name").unwrap_or_default();
                match sdk::invoke(&name, &method, &args) {
                    Ok(value) => format!("ok={value}\n"),
                    Err(e) => format!("error={}\nmessage={}\n", e.code, e.message),
                }
            }
            _ => "error=unknown_route\n".to_string(),
        };
        http::respond_text(response_out, 200, &text);
    }
}
