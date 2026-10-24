//! `events` fixture: `std:host/pubsub` and `std:host/streams` from `/wit/rt-host.wit`, and the
//! synthetic deliveries a subscriber / consumer receives.
//!
//! Every call route answers `200` JSON `{"ok": …}` or `{"error": {"code", "message"}}`:
//! - `GET /publish?subject=S&body=B&h=K:V` (or `POST` with the body) -> `{"ok":{"receivers","dropped"}}`
//! - `GET /request?subject=S&body=B&timeout_ms=T` -> `{"ok":{"content_type","body"}}` (body as text)
//! - `GET /append?topic=T&key=K&body=B&dedup=D&n=N` (or `POST` with the body; `n` copies, default 1,
//!   `0` sends none) -> `{"ok":[{"partition","offset","duplicate"}]}`
//! - `GET /read?topic=T&partition=P&from=earliest|latest|<offset>|t<ms>&limit=L` ->
//!   `{"ok":{"entries":[{"partition","offset","key","body","timestamp"}],"next","end"}}`
//! - `GET /info?topic=T` -> `{"ok":{"name","partitions":[[p,first,next]],"retention_hours","max_mb","bytes"}}`
//!
//! Deliveries:
//! - `POST /__rt/pubsub` -> `200 {"reply":"<subject>"}`
//! - `POST /__rt/stream` -> `200 ok`, with `x-rt-stream-fail-at: N` when the first message body has
//!   a `"fail_at": N` field.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

use self::std::host::{pubsub, streams};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
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

fn opt(s: &Option<String>) -> String {
    s.as_deref().map(esc).unwrap_or_else(|| "null".to_string())
}

fn text(b: &[u8]) -> String {
    esc(&String::from_utf8_lossy(b))
}

fn error_json(code: &str, message: &str) -> String {
    format!("{{\"error\":{{\"code\":{},\"message\":{}}}}}", esc(code), esc(message))
}

fn respond_json(out: ResponseOutparam, json: String) {
    http::respond(out, 200, "application/json", json.as_bytes());
}

fn respond_with_header(out: ResponseOutparam, name: &str, value: &str) {
    let headers = Fields::new();
    headers.set("content-type", &[b"text/plain".to_vec()]).expect("content-type");
    headers.set(name, &[value.as_bytes().to_vec()]).expect("header");
    let response = OutgoingResponse::new(headers);
    response.set_status_code(200).expect("status");
    let body = response.body().expect("body");
    ResponseOutparam::set(out, Ok(response));
    let stream = body.write().expect("stream");
    stream.blocking_write_and_flush(b"ok").expect("write");
    drop(stream);
    OutgoingBody::finish(body, None).expect("finish");
}

/// `"field":"value"` or `"field": N` in a flat JSON text (fixture-grade parsing).
fn json_field<'a>(json: &'a str, field: &str) -> Option<&'a str> {
    let at = json.find(&format!("\"{field}\""))?;
    let rest = json[at + field.len() + 2..].trim_start().strip_prefix(':')?.trim_start();
    if let Some(s) = rest.strip_prefix('"') {
        return s.split('"').next();
    }
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    Some(&rest[..end])
}

fn headers_param(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter_map(|p| p.strip_prefix("h="))
        .filter_map(|kv| kv.split_once(':'))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn position(from: &str) -> streams::Position {
    match from {
        "" | "earliest" => streams::Position::Earliest,
        "latest" => streams::Position::Latest,
        t if t.starts_with('t') => streams::Position::TimeMs(t[1..].parse().unwrap_or(-1)),
        o => streams::Position::Offset(o.parse().unwrap_or(0)),
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let q = |k: &str| http::query_param(query, k);
        let method = request.method();
        let path = path.to_string();
        let body = match method {
            Method::Post | Method::Put => http::read_body(request),
            _ => q("body").unwrap_or_default().into_bytes(),
        };

        match path.as_str() {
            "/publish" => {
                let subject = q("subject").unwrap_or_default();
                let json = match pubsub::publish(&subject, &body, &headers_param(query)) {
                    Ok(r) => format!("{{\"ok\":{{\"receivers\":{},\"dropped\":{}}}}}", r.receivers, r.dropped),
                    Err(e) => error_json(&e.code, &e.message),
                };
                respond_json(out, json);
            }
            "/request" => {
                let subject = q("subject").unwrap_or_default();
                let timeout = q("timeout_ms").and_then(|t| t.parse().ok());
                let json = match pubsub::request(&subject, &body, &headers_param(query), timeout) {
                    Ok(r) => format!(
                        "{{\"ok\":{{\"content_type\":{},\"body\":{}}}}}",
                        opt(&r.content_type),
                        text(&r.body)
                    ),
                    Err(e) => error_json(&e.code, &e.message),
                };
                respond_json(out, json);
            }
            "/append" => {
                let topic = q("topic").unwrap_or_default();
                let n: usize = q("n").and_then(|n| n.parse().ok()).unwrap_or(1);
                let entry = streams::NewEntry {
                    body,
                    key: q("key"),
                    headers: headers_param(query),
                    dedup_id: q("dedup"),
                };
                let entries = vec![entry; n];
                let json = match streams::append(&topic, &entries) {
                    Ok(list) => {
                        let items: Vec<String> = list
                            .iter()
                            .map(|a| {
                                format!(
                                    "{{\"partition\":{},\"offset\":{},\"duplicate\":{}}}",
                                    a.partition, a.offset, a.duplicate
                                )
                            })
                            .collect();
                        format!("{{\"ok\":[{}]}}", items.join(","))
                    }
                    Err(e) => error_json(&e.code, &e.message),
                };
                respond_json(out, json);
            }
            "/read" => {
                let topic = q("topic").unwrap_or_default();
                let opts = streams::ReadOptions {
                    partition: q("partition").and_then(|p| p.parse().ok()).unwrap_or(0),
                    start: position(&q("from").unwrap_or_default()),
                    limit: q("limit").and_then(|l| l.parse().ok()).unwrap_or(100),
                };
                let json = match streams::read(&topic, opts) {
                    Ok(page) => {
                        let items: Vec<String> = page
                            .entries
                            .iter()
                            .map(|e| {
                                format!(
                                    "{{\"partition\":{},\"offset\":{},\"key\":{},\"body\":{},\"timestamp\":{}}}",
                                    e.partition,
                                    e.offset,
                                    opt(&e.key),
                                    text(&e.body),
                                    e.timestamp_ms
                                )
                            })
                            .collect();
                        format!(
                            "{{\"ok\":{{\"entries\":[{}],\"next\":{},\"end\":{}}}}}",
                            items.join(","),
                            page.next,
                            page.end
                        )
                    }
                    Err(e) => error_json(&e.code, &e.message),
                };
                respond_json(out, json);
            }
            "/info" => {
                let topic = q("topic").unwrap_or_default();
                let json = match streams::info(&topic) {
                    Ok(i) => {
                        let parts: Vec<String> = i
                            .partitions
                            .iter()
                            .map(|p| format!("[{},{},{}]", p.partition, p.first, p.next))
                            .collect();
                        format!(
                            "{{\"ok\":{{\"name\":{},\"partitions\":[{}],\"retention_hours\":{},\"max_mb\":{},\"bytes\":{}}}}}",
                            esc(&i.name),
                            parts.join(","),
                            i.retention_hours,
                            i.max_mb,
                            i.bytes
                        )
                    }
                    Err(e) => error_json(&e.code, &e.message),
                };
                respond_json(out, json);
            }
            "/__rt/pubsub" => {
                let json = String::from_utf8_lossy(&body);
                let subject = json_field(&json, "subject").unwrap_or_default();
                respond_json(out, format!("{{\"reply\":{}}}", esc(subject)));
            }
            "/__rt/stream" => {
                let json = String::from_utf8_lossy(&body);
                let first = json.find("\"messages\"").map(|at| &json[at..]).unwrap_or_default();
                match json_field(first, "fail_at") {
                    Some(n) if !n.is_empty() => respond_with_header(out, "x-rt-stream-fail-at", n),
                    _ => http::respond_text(out, 200, "ok"),
                }
            }
            _ => http::respond_text(out, 404, "not found"),
        }
    }
}
