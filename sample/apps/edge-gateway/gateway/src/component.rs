//! The WASI side: bindings, the `wasi:http/incoming-handler` export and the request pipeline.
//! Compiled only for wasm32; the pure modules it uses are tested on the host.

use crate::{audit, config, ratelimit, request, token};

// `std` is also the Rust crate name, so the generated `std:host` module lives in its own scope.
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "guest",
    });
}

use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use bindings::std::host::{call as rtcall, kv, log, queue, sdk};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{
    Fields, IncomingBody, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};
use wasip2::io::streams::StreamError;

use config::Config;
use request::{Headers, HttpMethod, Route, Target};

struct Gateway;

wasip2::http::proxy::export!(Gateway);

/// The response the gateway sends, plus what the audit trail records about it.
struct Reply {
    status: u16,
    headers: Headers,
    body: Vec<u8>,
    outcome: &'static str,
    reason: Option<String>,
}

impl Reply {
    fn json(status: u16, outcome: &'static str, body: serde_json::Value) -> Self {
        Reply {
            status,
            headers: vec![("content-type".into(), b"application/json".to_vec())],
            body: body.to_string().into_bytes(),
            outcome,
            reason: None,
        }
    }

    fn error(status: u16, outcome: &'static str, code: &str, message: &str) -> Self {
        let mut r = Self::json(
            status,
            outcome,
            serde_json::json!({ "error": { "code": code, "message": message } }),
        );
        r.reason = Some(code.to_string());
        r
    }

    fn header(mut self, name: &str, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// What the gateway knows about a request, for the audit event.
#[derive(Default)]
struct Seen {
    sub: Option<String>,
    ip: Option<IpAddr>,
    route: Option<&'static str>,
    reason: Option<String>,
}

impl Guest for Gateway {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        let headers: Headers = req.headers().entries();
        let request_id = request::header(&headers, "x-rt-task-id")
            .and_then(|v| std::str::from_utf8(v).ok())
            .unwrap_or("-")
            .to_string();
        let method = method_name(&req.method());
        let pwq = req.path_with_query().unwrap_or_else(|| "/".into());
        let (path, query) = pwq.split_once('?').unwrap_or((pwq.as_str(), ""));

        let cfg = config::from_env(std::env::vars());
        let mut seen = Seen {
            ip: request::client_ip(&headers),
            ..Seen::default()
        };
        let mut reply = match &cfg {
            Ok(cfg) => handle(cfg, &req, &headers, &method, path, query, &request_id, &mut seen),
            Err(e) if path != "/healthz" => {
                // Misconfiguration fails closed: nothing is forwarded without verified tokens.
                log::log(log::Level::Error, &format!("gateway misconfigured: {e}"));
                Reply::error(
                    503,
                    "misconfigured",
                    "gateway_misconfigured",
                    "the gateway is not configured",
                )
            }
            Err(_) => Reply::json(503, "misconfigured", serde_json::json!({ "ok": false })),
        };
        if seen.reason.is_none() {
            seen.reason = reply.reason.take();
        }

        log::log(
            if reply.status >= 500 {
                log::Level::Warn
            } else {
                log::Level::Info
            },
            &format!(
                "{method} {path} -> {} sub={} {}",
                reply.status,
                seen.sub.as_deref().unwrap_or("-"),
                seen.reason.as_deref().unwrap_or(reply.outcome)
            ),
        );
        let event = match &cfg {
            Ok(cfg) if audit::records(cfg.audit, &method, reply.status, path) => Some(
                audit::Event {
                    id: &request_id,
                    at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    sub: seen.sub.as_deref(),
                    ip: seen.ip,
                    method: &method,
                    path,
                    route: seen.route,
                    status: reply.status,
                    outcome: reply.outcome,
                    reason: seen.reason.as_deref(),
                }
                .to_json(),
            ),
            _ => None,
        };
        reply
            .headers
            .push(("x-request-id".into(), request_id.as_bytes().to_vec()));
        // The client has the whole response now (buffered, with content-length); the audit work
        // runs after it, while this instance is still held (bounded by the wall limit).
        send(out, reply);
        if let (Ok(cfg), Some(event)) = (&cfg, event) {
            deliver(cfg, &request_id, event);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle(
    cfg: &Config,
    req: &IncomingRequest,
    headers: &Headers,
    method: &str,
    path: &str,
    query: &str,
    request_id: &str,
    seen: &mut Seen,
) -> Reply {
    // 1. Shape of the request.
    if let Err(e) = request::validate_path(path).and_then(|_| request::validate_query(query)) {
        return Reply::error(400, "invalid", "invalid_request", e);
    }
    if path == "/healthz" {
        return Reply::json(200, "ok", serde_json::json!({ "ok": true }));
    }
    let Some(route) = request::match_route(path) else {
        return Reply::error(404, "invalid", "not_found", "no such route");
    };
    seen.route = Some(route.target.name());
    let Some(m) = HttpMethod::parse(method).filter(|m| request::method_allowed(route.target, *m)) else {
        return Reply::error(405, "invalid", "method_not_allowed", "method not allowed on this route");
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // 2. How often from this address? Before the token is looked at, so a flood of bad tokens
    //    costs one counter increment per request and never reaches a backend.
    if let Some(ip) = seen.ip {
        let limit = rate_limit(cfg, cfg.ip_rate_limit, |w| ratelimit::ip_key(ip, w), now);
        if let Some(d) = limit.filter(|d| !d.allowed) {
            return too_many(d, "ip_rate_limited");
        }
    }

    // 3. Who is calling.
    let unauthorized = |reason: &str| {
        let mut r = Reply::error(401, "denied", "invalid_token", "a valid bearer token is required")
            .header("www-authenticate", "Bearer realm=\"edge\", error=\"invalid_token\"");
        r.reason = Some(reason.to_string());
        r
    };
    let token = match request::bearer(headers) {
        Ok(t) => t,
        Err(_) => return unauthorized("missing_token"),
    };
    let policy = token::Policy {
        keys: &cfg.keys,
        audience: &cfg.audience,
        issuer: cfg.issuer.as_deref(),
        max_ttl: cfg.max_ttl,
        leeway: cfg.leeway,
    };
    let claims = match token::verify(token, &policy, now) {
        Ok(c) => c,
        Err(e) => return unauthorized(e.code()),
    };
    seen.sub = Some(claims.sub.clone());

    // 4. May they do this?
    let scope = request::required_scope(route.target, m);
    if !claims.has_scope(scope) {
        return Reply::error(
            403,
            "denied",
            "insufficient_scope",
            &format!("this request needs the {scope} scope"),
        );
    }

    // 5. How often from this subject?
    let limit = rate_limit(cfg, cfg.rate_limit, |w| ratelimit::sub_key(&claims.sub, w), now);
    if let Some(d) = limit.filter(|d| !d.allowed) {
        return too_many(d, "rate_limited");
    }

    // 6. Body.
    let body = match read_body(req, headers, m, cfg.max_body) {
        Ok(b) => b,
        Err(r) => return with_limit(r, limit),
    };

    // 7. Forward (or answer from the operator SDK).
    if route.target == Target::Platform {
        return with_limit(platform_info(cfg), limit);
    }
    let r = forward(cfg, &route, m, query, headers, body, &claims, request_id, seen.ip);
    with_limit(r, limit)
}

/// Counts this request in the current window (`kv::incr`: atomic, so the limit is exact) and
/// decides on the new count. None when the limit is off or the store failed: availability over
/// strictness, a KV failure does not lock everyone out (it is logged).
fn rate_limit(
    cfg: &Config,
    limit: u64,
    key: impl FnOnce(ratelimit::Window) -> Vec<u8>,
    now: u64,
) -> Option<ratelimit::Decision> {
    if limit == 0 {
        return None;
    }
    let w = ratelimit::window(now, cfg.rate_window);
    // The TTL applies when the increment creates the key: each window's counter expires on its own.
    let opts = kv::SetOptions {
        ttl_seconds: Some(cfg.rate_window + 5),
    };
    match kv::incr(&key(w), 1, opts) {
        Ok(count) => Some(ratelimit::decide(u64::try_from(count).unwrap_or(0), limit, w)),
        Err(e) => {
            log::log(log::Level::Warn, &format!("rate limit store: {}", e.code));
            None
        }
    }
}

fn with_limit(r: Reply, limit: Option<ratelimit::Decision>) -> Reply {
    match limit {
        Some(d) => r
            .header("ratelimit-limit", d.limit.to_string())
            .header("ratelimit-remaining", d.remaining.to_string())
            .header("ratelimit-reset", d.reset_in.to_string()),
        None => r,
    }
}

fn too_many(d: ratelimit::Decision, reason: &str) -> Reply {
    let mut r = with_limit(
        Reply::error(429, "limited", "rate_limited", "too many requests").header("retry-after", d.reset_in.to_string()),
        Some(d),
    );
    r.reason = Some(reason.to_string());
    r
}

fn read_body(req: &IncomingRequest, headers: &Headers, m: HttpMethod, max: usize) -> Result<Vec<u8>, Reply> {
    let declared = request::content_length(headers);
    if !m.takes_body() {
        // GET / DELETE carry no body here; a declared one is refused rather than ignored.
        if declared.unwrap_or(0) > 0 || request::header(headers, "transfer-encoding").is_some() {
            return Err(Reply::error(
                400,
                "invalid",
                "unexpected_body",
                "this method takes no body",
            ));
        }
        return Ok(Vec::new());
    }
    if !request::is_json(request::header(headers, "content-type")) {
        return Err(Reply::error(
            415,
            "invalid",
            "unsupported_media_type",
            "send Content-Type: application/json",
        ));
    }
    if declared.is_some_and(|n| n > max as u64) {
        return Err(Reply::error(413, "invalid", "too_large", "request body too large"));
    }
    let too_large = || Reply::error(413, "invalid", "too_large", "request body too large");
    let incoming = req
        .consume()
        .map_err(|_| Reply::error(400, "invalid", "invalid_body", "cannot read the body"))?;
    let stream = incoming
        .stream()
        .map_err(|_| Reply::error(400, "invalid", "invalid_body", "cannot read the body"))?;
    let mut buf = Vec::new();
    loop {
        match stream.blocking_read(16 * 1024) {
            Ok(chunk) => {
                if buf.len() + chunk.len() > max {
                    return Err(too_large());
                }
                buf.extend_from_slice(&chunk);
            }
            Err(StreamError::Closed) => break,
            Err(_) => {
                return Err(Reply::error(
                    400,
                    "invalid",
                    "invalid_body",
                    "the body could not be read",
                ))
            }
        }
    }
    drop(stream);
    let _ = IncomingBody::finish(incoming);
    if buf.is_empty() {
        return Err(Reply::error(400, "invalid", "invalid_body", "a JSON body is required"));
    }
    if serde_json::from_slice::<serde_json::Value>(&buf).is_err() {
        return Err(Reply::error(
            400,
            "invalid",
            "invalid_json",
            "the body is not valid JSON",
        ));
    }
    Ok(buf)
}

#[allow(clippy::too_many_arguments)]
fn forward(
    cfg: &Config,
    route: &Route,
    m: HttpMethod,
    query: &str,
    headers: &Headers,
    body: Vec<u8>,
    claims: &token::Claims,
    request_id: &str,
    client_ip: Option<IpAddr>,
) -> Reply {
    let slug = match route.target {
        Target::Notes => &cfg.notes_service,
        Target::Audit | Target::Platform => &cfg.audit_service,
    };
    let mut fwd = request::filter(headers, request::FORWARD_REQUEST);
    fwd.push(("x-gw-sub".into(), claims.sub.clone().into_bytes()));
    fwd.push(("x-gw-scopes".into(), claims.scopes.join(" ").into_bytes()));
    fwd.push(("x-gw-request-id".into(), request_id.as_bytes().to_vec()));
    if let Some(ip) = client_ip {
        fwd.push(("x-gw-client-ip".into(), ip.to_string().into_bytes()));
    }
    let path_with_query = if query.is_empty() {
        route.rest.clone()
    } else {
        format!("{}?{query}", route.rest)
    };
    let call_req = rtcall::Request {
        method: m.as_str().into(),
        path_with_query,
        headers: fwd,
        body,
    };

    match rtcall::call(slug, &call_req) {
        Ok(res) => {
            let mut headers = request::filter(&res.headers, request::PASS_RESPONSE);
            if !headers.iter().any(|(k, _)| k == "cache-control") {
                headers.push(("cache-control".into(), b"no-store".to_vec()));
            }
            let outcome = if res.status >= 500 { "upstream_error" } else { "ok" };
            let mut r = Reply {
                status: res.status,
                headers,
                body: res.body,
                outcome,
                reason: None,
            };
            if res.status >= 500 {
                // Do not leak a backend's error page; keep its status.
                r = Reply::error(
                    res.status.clamp(502, 504),
                    "upstream_error",
                    "upstream_error",
                    "the backend failed",
                );
                r.reason = Some(format!("backend {}", res.status));
            }
            r
        }
        Err(e) => {
            // not_found, call_not_allowed (allowed_callers / environment), forbidden, call_failed …
            log::log(log::Level::Error, &format!("call {slug}: {} ({})", e.code, e.message));
            let mut r = Reply::error(
                502,
                "upstream_error",
                "upstream_unavailable",
                "the backend is not reachable",
            );
            r.reason = Some(e.code);
            r
        }
    }
}

/// `GET /v1/platform`: the operator SDK's `getPlatformInfo`, answered by whichever SDK release the
/// gateway's grants select now (an SDK update reaches it without a redeploy). The SDK's refusal
/// (sdk_unavailable after a removal, sdk_not_granted, ...) is a 502 with that code.
fn platform_info(cfg: &Config) -> Reply {
    let Some(name) = cfg.sdk.as_deref() else {
        return Reply::error(404, "invalid", "not_found", "no operator SDK is configured (GATEWAY_SDK=off)");
    };
    match sdk::invoke(name, "getPlatformInfo", "null") {
        Ok(json) => match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(v) => Reply::json(200, "ok", v).header("cache-control", "no-store"),
            Err(_) => Reply::error(502, "upstream_error", "invalid_response", "the SDK answered no JSON"),
        },
        Err(e) => {
            log::log(log::Level::Warn, &format!("sdk {name}: {} ({})", e.code, e.message));
            Reply::error(502, "upstream_error", &e.code, "the operator SDK is not available")
        }
    }
}

/// Runs after the response: ships the event to the operator's audit log through the SDK, then
/// enqueues it with the result to the audit service (the local trail; it retries what was not
/// shipped). The dedup id (the task id) makes a retried enqueue harmless. The request has been
/// answered already, so nothing here can fail it: failures are logged.
fn deliver(cfg: &Config, request_id: &str, event: serde_json::Value) {
    let shipped = cfg.sdk.as_deref().map(|name| ship(name, &event));
    if let Some(Err(e)) = &shipped {
        log::log(
            log::Level::Warn,
            &format!("audit shipping: {e} (the audit service retries)"),
        );
    }
    let message = audit::queue_message(event, shipped.as_ref());
    let opts = queue::SendOptions {
        delay_seconds: None,
        dedup_id: (request_id != "-").then(|| format!("gw:{request_id}")),
    };
    if let Err(e) = queue::enqueue(&cfg.audit_service, message.to_string().as_bytes(), &opts) {
        log::log(
            log::Level::Warn,
            &format!("audit enqueue to {}: {}", cfg.audit_service, e.code),
        );
    }
}

/// `audit.ship({"source", "events": [event]})` of the operator SDK. The SDK holds the operator
/// binding (credentials injected by the node, the gateway's tenant forwarded); no SDK code is in
/// this component.
fn ship(name: &str, event: &serde_json::Value) -> Result<(), String> {
    let args = String::from_utf8(audit::shipment(event)).unwrap_or_default();
    match sdk::invoke(name, "audit.ship", &args) {
        Ok(_) => Ok(()),
        Err(e) => Err(audit::ship_error(name, &e.code)),
    }
}

fn method_name(m: &Method) -> String {
    match m {
        Method::Get => "GET".into(),
        Method::Head => "HEAD".into(),
        Method::Post => "POST".into(),
        Method::Put => "PUT".into(),
        Method::Delete => "DELETE".into(),
        Method::Connect => "CONNECT".into(),
        Method::Options => "OPTIONS".into(),
        Method::Trace => "TRACE".into(),
        Method::Patch => "PATCH".into(),
        Method::Other(o) => o.clone(),
    }
}

fn send(out: ResponseOutparam, mut reply: Reply) {
    reply
        .headers
        .push(("x-content-type-options".into(), b"nosniff".to_vec()));
    reply.headers.push(("referrer-policy".into(), b"no-referrer".to_vec()));
    // The runtime ends the response body only when `handle` returns, after the audit work: with a
    // length the client has the whole (buffered) response as soon as it is written.
    let length = ("content-length".to_string(), reply.body.len().to_string().into_bytes());
    reply.headers.push(length.clone());
    // A header the host refuses (forbidden or invalid) must not cost the response: drop the extras.
    let fields = Fields::from_list(&reply.headers).unwrap_or_else(|_| {
        Fields::from_list(&[("content-type".to_string(), b"application/json".to_vec()), length]).expect("static header")
    });
    let response = OutgoingResponse::new(fields);
    let _ = response.set_status_code(reply.status);
    let Ok(body) = response.body() else { return };
    ResponseOutparam::set(out, Ok(response));
    if let Ok(stream) = body.write() {
        for chunk in reply.body.chunks(4096) {
            if stream.blocking_write_and_flush(chunk).is_err() {
                break;
            }
        }
        drop(stream);
    }
    let _ = OutgoingBody::finish(body, None);
}
