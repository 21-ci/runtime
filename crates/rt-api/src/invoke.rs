//! The invoke surface (docs/api.md): `ANY /s/{slug}/{path…}`, or a custom
//! `Host` / `{slug}.{base_domain}`.
//!
//! Per request (in order): resolve the route → the plan allows `http` (`403 trigger_not_allowed`)
//! → invoke access (`public` / `key` / `internal`; failed keys are limited per client, SEC-39) →
//! ingress rules (methods, paths, IP allow, rate limit) → quotas → header hygiene → build the
//! `TaskSpec` (config resolution) → `Scheduler::submit` → await the outcome → response
//! (`x-rt-task-id`, `x-rt-version`, `server-timing`; failures are `text/plain` + `x-rt-reason`).
//!
//! Ingress DoS limits (header ≤ 16 KB / 100 headers, header timeout, per-IP connection cap, h2
//! stream caps) are enforced where the listener is built (`serve`), not per request.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::body::Body as AxBody;
use axum::extract::{ConnectInfo, Extension, Request as AxRequest, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use futures_util::StreamExt;
use rt_obs::Metrics;
use rt_sec::{Principal, QuotaError, QuotaOp};
use rt_triggers::RouteResolver;
use rt_types::{
    Access, Body, BodyError, HttpRequest, Listener, Outcome, Scope, TaskId, TraceCtx, TriggerKind, UsageDelta,
    VersionNo,
};

use crate::state::ApiState;

/// `/s/{slug}` and `/s/{slug}/{*rest}` plus a host-based fallback. `listener` is `Public` or
/// `Internal` (the internal listener trusts the peer and honors `x-rt-invoker`).
pub fn invoke_router(state: ApiState, listener: Listener) -> Router {
    let metrics = state.metrics.clone();
    let label = if listener == Listener::Internal {
        "internal"
    } else {
        "public"
    };
    let router = Router::new()
        .route("/s/{slug}", any(invoke))
        .route("/s/{slug}/{*rest}", any(invoke))
        .fallback(invoke)
        .layer(Extension(listener))
        .with_state(state);
    with_http_metrics(router, metrics, label)
}

/// Count every response in `rt_http_requests_total{listener,status_class}`.
pub fn with_http_metrics(router: Router, metrics: Arc<Metrics>, listener: &'static str) -> Router {
    router.layer(axum::middleware::from_fn_with_state(
        (metrics, listener),
        count_response,
    ))
}

async fn count_response(
    State((m, listener)): State<(Arc<Metrics>, &'static str)>,
    req: AxRequest,
    next: Next,
) -> Response {
    let resp = next.run(req).await;
    let class = format!("{}xx", resp.status().as_u16() / 100);
    m.http_requests_total
        .with_label_values(&[listener, class.as_str()])
        .inc();
    resp
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Remove hop-by-hop headers (RFC 9110 §7.6.1), including the ones the `Connection` header names.
fn strip_hop_by_hop(h: &mut HeaderMap) {
    let named: Vec<String> = h
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    for n in HOP_BY_HOP {
        h.remove(*n);
    }
    for n in named {
        if let Ok(name) = HeaderName::from_bytes(n.as_bytes()) {
            h.remove(name);
        }
    }
}

/// Every `x-rt-*` request header is removed (only the runtime sets them); on the internal
/// listener `x-rt-invoker` survives.
fn strip_runtime_headers(h: &mut HeaderMap, keep_invoker: bool) {
    let names: Vec<HeaderName> = h
        .keys()
        .filter(|k| k.as_str().starts_with("x-rt-") && !(keep_invoker && k.as_str() == "x-rt-invoker"))
        .cloned()
        .collect();
    for n in names {
        h.remove(n);
    }
}

/// The client address: the peer, or the `X-Forwarded-For` hop in front of the trusted proxies
/// (`rt_sec::TrustedProxies`: hops may carry a port or brackets; the walk stops at the first
/// untrusted hop, or at one it cannot parse, so it never reaches entries the client wrote itself).
fn client_ip(peer: Option<IpAddr>, h: &HeaderMap, proxies: &rt_sec::TrustedProxies) -> Option<IpAddr> {
    let peer = peer?;
    let forwarded: Vec<&str> = h
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    Some(proxies.client_ip(peer, &forwarded))
}

/// `Authorization: Bearer rtk_…`: a tenant key, an invoke credential rather than the guest's own.
fn is_key_bearer(v: &HeaderValue) -> bool {
    v.to_str()
        .ok()
        .and_then(|v| v.trim().split_once(char::is_whitespace))
        .is_some_and(|(scheme, token)| {
            scheme.eq_ignore_ascii_case("bearer")
                && token
                    .trim()
                    .strip_prefix(rt_sec::keys::KEY_PREFIX)
                    .is_some_and(|rest| rest.starts_with('_'))
        })
}

/// `Authorization: Bearer <root token>`: the operator's credential, never the guest's business
/// (SECURITY.md "Request path": it is removed like an `rtk_` key, INV-009).
fn is_root_bearer(v: &HeaderValue, auth: &dyn rt_sec::Authenticator) -> bool {
    v.to_str()
        .ok()
        .and_then(|v| v.trim().split_once(char::is_whitespace))
        .is_some_and(|(scheme, token)| scheme.eq_ignore_ascii_case("bearer") && auth.is_root_token(token.trim()))
}

/// The request headers the guest gets (SECURITY.md, docs/handlers.md): the client's
/// minus hop-by-hop headers, `Host` and every `x-rt-*` (on the internal listener `x-rt-invoker`
/// survives), minus `Authorization` when it carries a runtime credential (`strip_authorization`:
/// an `rtk_` bearer, or the root token that passed `access = "key"`); plus the runtime's
/// `x-rt-base-path`, `x-rt-client-ip` (the address behind `ingress.trusted_proxies`, when there is
/// one) and `x-rt-trigger: http`. A client-sent `x-rt-client-ip` never survives.
fn guest_headers(
    client: &HeaderMap,
    internal: bool,
    strip_authorization: bool,
    base_path: &str,
    client_ip: Option<IpAddr>,
) -> HeaderMap {
    let mut headers = client.clone();
    strip_hop_by_hop(&mut headers);
    strip_runtime_headers(&mut headers, internal);
    headers.remove(header::HOST);
    if strip_authorization {
        headers.remove(header::AUTHORIZATION);
    }
    if !base_path.is_empty() {
        if let Ok(v) = HeaderValue::from_str(base_path) {
            headers.insert(HeaderName::from_static("x-rt-base-path"), v);
        }
    }
    if let Some(ip) = client_ip {
        if let Ok(v) = HeaderValue::from_str(&ip.to_string()) {
            headers.insert(HeaderName::from_static("x-rt-client-ip"), v);
        }
    }
    headers.insert(
        HeaderName::from_static("x-rt-trigger"),
        HeaderValue::from_static("http"),
    );
    headers
}

/// Drop the `Authorization` values `is_credential` marks (runtime credentials), keeping the
/// guest's own ones in order (INV-011).
fn keep_guest_authorization(h: &mut HeaderMap, is_credential: impl Fn(&HeaderValue) -> bool) {
    let all: Vec<HeaderValue> = h.get_all(header::AUTHORIZATION).iter().cloned().collect();
    if !all.iter().any(&is_credential) {
        return;
    }
    h.remove(header::AUTHORIZATION);
    for v in all.into_iter().filter(|v| !is_credential(v)) {
        h.append(header::AUTHORIZATION, v);
    }
}

/// `ingress.allowed_methods`: empty admits every method; names compare case-insensitively
/// (INV-023).
fn method_allowed(list: &[String], method: &http::Method) -> bool {
    list.is_empty() || list.iter().any(|m| m.eq_ignore_ascii_case(method.as_str()))
}

/// A W3C `traceparent` of version `00`: `00-<32 lower hex, not all 0>-<16 lower hex, not all
/// 0>-<2 lower hex>` (INV-032). Anything else is replaced by a fresh one.
fn valid_traceparent(t: &str) -> bool {
    let hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    let parts: Vec<&str> = t.split('-').collect();
    matches!(parts.as_slice(), [v, trace, span, flags]
        if *v == "00" && hex(trace, 32) && hex(span, 16) && hex(flags, 2)
            && trace.bytes().any(|b| b != b'0') && span.bytes().any(|b| b != b'0'))
}

/// The URI the guest sees: the path tail after the route prefix and the query, verbatim. `None`
/// when they do not form a URI (the request is refused, never rewritten to `/`, INV-034).
fn guest_uri(path: &str, query: Option<&str>) -> Option<http::Uri> {
    let pq = match query {
        Some(q) => format!("{path}?{q}"),
        None => path.to_string(),
    };
    pq.parse::<http::Uri>().ok()
}

/// `.` / `..` path segments as a WHATWG URL parser (the guest's `new URL(req.url)`) sees them:
/// also percent-encoded (`%2e`, any case).
fn dot_segment(seg: &str) -> Option<u8> {
    match seg.to_ascii_lowercase().replace("%2e", ".").as_str() {
        "." => Some(1),
        ".." => Some(2),
        _ => None,
    }
}

/// The path the guest resolves `path` to: dot segments (raw or percent-encoded, also after a
/// backslash) removed as RFC 3986 §5.2.4 does.
fn resolved_path(path: &str) -> String {
    let segs: Vec<&str> = path.split(['/', '\\']).collect();
    let last = segs.len().saturating_sub(1);
    let mut out: Vec<&str> = Vec::new();
    for (i, seg) in segs.iter().enumerate().skip(1) {
        match dot_segment(seg) {
            Some(n) => {
                if n == 2 {
                    out.pop();
                }
                if i == last {
                    out.push("");
                }
            }
            None => out.push(seg),
        }
    }
    format!("/{}", out.join("/"))
}

/// `allow_paths` admits a request only when both the raw path and the path the guest resolves it
/// to match, so dot segments cannot climb out of an allowed prefix (INV-021).
fn request_path_allowed(patterns: &[String], path: &str) -> bool {
    path_allowed(patterns, path) && path_allowed(patterns, &resolved_path(path))
}

fn path_allowed(patterns: &[String], path: &str) -> bool {
    patterns.is_empty()
        || patterns.iter().any(|p| match p.strip_suffix('*') {
            Some(prefix) => path.starts_with(prefix),
            None => p == path,
        })
}

fn retry_after_secs(d: std::time::Duration) -> u64 {
    d.as_secs() + u64::from(d.subsec_nanos() > 0).max(u64::from(d.as_secs() == 0))
}

fn limited(status: u16, reason: &'static str, retry_after: std::time::Duration) -> Response {
    let mut resp = plain_error(status, reason);
    if let Ok(v) = HeaderValue::from_str(&retry_after_secs(retry_after).max(1).to_string()) {
        resp.headers_mut().insert(header::RETRY_AFTER, v);
    }
    resp
}

fn quota_response(e: &QuotaError) -> Response {
    match e {
        QuotaError::RateLimited { retry_after } => limited(429, "rate_limited", *retry_after),
        QuotaError::InvocationsPerDay { retry_after } => limited(429, "quota_exceeded", *retry_after),
        QuotaError::Exceeded { .. } => plain_error(429, "quota_exceeded"),
        _ => plain_error(429, "rate_limited"),
    }
}

fn thread_shard() -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    h.finish() as usize
}

fn limited_body(body: AxBody, limit: u64) -> Body {
    let mut seen = 0u64;
    let stream = body.into_data_stream().map(move |r| match r {
        Ok(b) => {
            seen += b.len() as u64;
            if seen > limit {
                Err(BodyError::TooLarge { limit })
            } else {
                Ok(b)
            }
        }
        Err(e) => Err(BodyError::Io(e.to_string())),
    });
    Body::from_stream(stream)
}

/// `Sec-WebSocket-Protocol` values of a request, in order.
fn ws_protocols(h: &HeaderMap) -> Vec<String> {
    h.get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// A browser cannot set `Authorization` on a WebSocket: an invoke key may come as a subprotocol
/// (`new WebSocket(url, ["rtk_…", "chat"])`). It is taken out of the list (the guest never sees
/// it) and returned.
fn take_key_protocol(h: &mut HeaderMap) -> Option<String> {
    let all = ws_protocols(h);
    let key = all
        .iter()
        .find(|p| p.starts_with(rt_sec::keys::KEY_PREFIX) && p[rt_sec::keys::KEY_PREFIX.len()..].starts_with('_'))
        .cloned();
    let rest: Vec<&str> = all
        .iter()
        .filter(|p| !p.starts_with("rtk_"))
        .map(String::as_str)
        .collect();
    h.remove(header::SEC_WEBSOCKET_PROTOCOL);
    if !rest.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&rest.join(", ")) {
            h.insert(header::SEC_WEBSOCKET_PROTOCOL, v);
        }
    }
    key
}

/// The `101 Switching Protocols` answer to an accepted upgrade.
fn switching(key: &HeaderValue, protocol: Option<&str>) -> Response {
    let mut resp = Response::new(AxBody::empty());
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let h = resp.headers_mut();
    h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    h.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    if let Ok(v) = HeaderValue::from_str(&rt_triggers::accept_key(key.as_bytes())) {
        h.insert(header::SEC_WEBSOCKET_ACCEPT, v);
    }
    if let Some(v) = protocol.and_then(|p| HeaderValue::from_str(p).ok()) {
        h.insert(header::SEC_WEBSOCKET_PROTOCOL, v);
    }
    resp
}

async fn invoke(State(s): State<ApiState>, Extension(listener): Extension<Listener>, req: Request<AxBody>) -> Response {
    let (mut parts, body) = req.into_parts();
    crate::auth::seed_peer(&mut parts, &s);
    let internal = listener == Listener::Internal;
    let upgrade = rt_triggers::is_upgrade_request(&parts.method, &parts.headers);

    // 1. route
    let host = parts
        .headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| parts.uri.authority().map(|a| a.to_string()))
        .unwrap_or_default();
    let Some(route) = s.routes.resolve(listener, &host, parts.uri.path()) else {
        return plain_error(404, "not_found");
    };
    let entry = route.entry.clone();
    if !entry.triggers.http {
        return plain_error(404, "not_found");
    }
    // The tenant's plan may not allow HTTP at all (B5.3, SEC-37); the plan cache is in memory.
    if !s.plans.plan_arc(entry.tenant).allows_trigger(TriggerKind::Http) {
        return plain_error(403, "trigger_not_allowed");
    }
    // A WebSocket upgrade (docs/handlers.md "WebSockets"): the service and the plan must allow
    // it, the node must take sockets, and the `Origin` must be one the service allows (checked
    // before any task runs: cross-site WebSocket hijacking).
    let mut key_protocol = None;
    if upgrade {
        if !entry.triggers.websocket || !s.plans.plan_arc(entry.tenant).allows_trigger(TriggerKind::Websocket) {
            return plain_error(403, "trigger_not_allowed");
        }
        if s.sockets.is_draining() {
            return plain_error(503, "draining");
        }
        // A held socket is long-lived memory: none is taken on while the node is short of it.
        if s.mem.pressure() >= rt_mem::Pressure::High {
            return limited(503, "memory", std::time::Duration::from_secs(1));
        }
        let origin = parts.headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
        let allowed = s
            .service_table
            .load()
            .get(&entry.service)
            .is_none_or(|svc| svc.config.ws.origin_allowed(origin));
        if !allowed {
            return plain_error(403, "origin_not_allowed");
        }
        key_protocol = take_key_protocol(&mut parts.headers);
    }

    // 2. invoke access
    // An `rtk_` key or the root token in `Authorization` is a runtime credential, never the
    // guest's business: those values are removed on every invoke (public services too), whether
    // or not one matched (`keep_guest_authorization` below). Any other `Authorization` value is
    // the guest's own and reaches it unchanged (INV-011).
    let mut strip_authorization = false;
    if let Some(k) = key_protocol
        .as_deref()
        .filter(|_| !parts.headers.contains_key(header::AUTHORIZATION))
    {
        // The key a browser sent as a subprotocol authenticates like a bearer, then goes away.
        if let Ok(v) = HeaderValue::from_str(&format!("Bearer {k}")) {
            parts.headers.insert(header::AUTHORIZATION, v);
            strip_authorization = true;
        }
    }
    if entry.access == Access::Key && !internal {
        match s.auth.authenticate(&parts, listener) {
            // The root token (a bearer) never reaches the guest either.
            Ok(Principal::Operator) => {}
            Ok(Principal::Tenant {
                tenant,
                scopes,
                project_ids,
                environments,
                ..
            }) if tenant == entry.tenant
                && scopes.contains(Scope::Invoke)
                && rt_types::scope_admits(
                    project_ids.as_deref(),
                    environments.as_deref(),
                    entry.project,
                    &entry.environment,
                ) => {}
            Err(rt_sec::AuthError::RateLimited { retry_after }) => return limited(429, "rate_limited", retry_after),
            _ => return plain_error(401, "unauthorized"),
        }
    }

    // 3. ingress rules (public listener only: the internal listener is trusted)
    let cfg = s.config.load();
    let peer = parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|c| c.0);
    // The client address behind `ingress.trusted_proxies`: what `ip_allow` matches and what the
    // guest gets as `x-rt-client-ip` / `ctx.client.ip`. The proxy list is parsed once per config
    // generation, not per request. None without a peer address (the internal Unix socket).
    let client = client_ip(peer.map(|p| p.ip()), &parts.headers, &s.ingress.trusted_proxies(&cfg));
    if !internal {
        let rules = &entry.ingress;
        if !method_allowed(&rules.allowed_methods, &parts.method) {
            return plain_error(405, "method_not_allowed");
        }
        if !request_path_allowed(&rules.allow_paths, &route.path) {
            return plain_error(403, "forbidden");
        }
        if !rules.ip_allow.is_empty() {
            // The rule list is parsed once per rule list, not per request. IPv4-mapped IPv6 peers
            // are matched as IPv4; malformed rules never match.
            let allow = s.ingress.ip_allow(entry.service, &rules.ip_allow);
            let ok = client.is_some_and(|ip| allow.allows(ip));
            if !ok {
                return plain_error(403, "forbidden");
            }
        }
        if rules.rate_limit.rps > 0 {
            let bucket = s.buckets.bucket(
                &s.clock,
                entry.service,
                rules.rate_limit.rps,
                rules.rate_limit.burst.max(rules.rate_limit.rps),
            );
            if !bucket.try_acquire(thread_shard(), 1) {
                return limited(429, "rate_limited", bucket.retry_after());
            }
        }
    }

    // 4. quotas (tenant RPS bucket, invocations per day)
    if let Err(e) = s.quotas.check(entry.tenant, QuotaOp::Invoke) {
        return quota_response(&e);
    }

    // 5. header hygiene
    let mut headers = guest_headers(&parts.headers, internal, strip_authorization, &route.base_path, client);
    keep_guest_authorization(&mut headers, |v| is_key_bearer(v) || is_root_bearer(v, &*s.auth));
    if upgrade {
        // The guest decides on the upgrade: it must see that one was offered.
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    }
    let on_upgrade = if upgrade {
        parts.extensions.remove::<hyper::upgrade::OnUpgrade>()
    } else {
        None
    };
    let hold = parts.extensions.remove::<crate::serve::ConnHold>();
    let traceparent = match headers.get("traceparent").and_then(|v| v.to_str().ok()) {
        Some(t) if valid_traceparent(t) => t.to_string(),
        _ => {
            let t = format!("00-{:032x}-{:016x}-01", rand::random::<u128>(), rand::random::<u64>());
            if let Ok(v) = HeaderValue::from_str(&t) {
                headers.insert(HeaderName::from_static("traceparent"), v);
            }
            t
        }
    };
    let sampled = traceparent.ends_with("-01");

    // 6. task
    let Some(uri) = guest_uri(&route.path, parts.uri.query()) else {
        return plain_error(400, "bad_request");
    };
    let http_req = HttpRequest {
        method: parts.method.clone(),
        uri,
        headers,
        remote_addr: peer,
        body: Body::empty(),
    };
    let Some(mut spec) = s
        .node
        .http_task(&entry, http_req, Some(TraceCtx { traceparent, sampled }))
    else {
        return route_stale();
    };
    let limit = spec.limits.max_request_body_bytes;
    if let Some(len) = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        if len > limit {
            return plain_error(413, "too_large");
        }
    }
    if let rt_types::Payload::Http(r) = &mut spec.payload {
        r.body = limited_body(body, limit);
    }
    // A durable object runs one request or event at a time: wait for its turn (held until the
    // response head), at most the service's queue timeout.
    let _turn = match spec.object.as_ref().map(|o| o.name.clone()) {
        Some(name) => match tokio::time::timeout(
            spec.knobs.queue_timeout,
            s.sockets.turn(entry.tenant, entry.service, &name),
        )
        .await
        {
            Ok(Ok(turn)) => {
                spec.object = Some(turn.object.clone());
                Some(turn)
            }
            Ok(Err(rt_triggers::TurnError::Draining)) => return plain_error(503, "draining"),
            Ok(Err(rt_triggers::TurnError::Busy)) => {
                return limited(503, "overloaded", std::time::Duration::from_secs(1))
            }
            Err(_) => return limited(503, "queue_timeout", std::time::Duration::from_secs(1)),
        },
        None => None,
    };

    // 7. submit and wait
    let mut handle = match s.scheduler.submit(spec) {
        Ok(h) => h,
        // Capacity refusals (`503 overloaded`, ...) carry `Retry-After` from the queue state (D-A3).
        Err(reason) => {
            return match reason.retry_after() {
                Some(_) => limited(reason.http_status(), reason.reason(), s.scheduler.retry_after_hint()),
                None => plain_error(reason.http_status(), reason.reason()),
            };
        }
    };
    let task = handle.id;
    // A client that goes away while its request still waits for a slot frees the place at once
    // (the handler future is dropped with this guard); a started task runs on.
    let _abandoned = CancelIfWaiting(handle.canceller());
    let outcome = (&mut handle).await;
    let timings = handle.timings();
    if outcome.is_ok() || matches!(outcome, Outcome::GuestError | Outcome::Killed(_)) {
        s.quotas.record(
            entry.tenant,
            entry.service,
            UsageDelta {
                invocations: 1,
                cpu_ms: timings.cpu.as_millis() as u64,
                egress_bytes: 0,
                storage_bytes: 0,
            },
        );
    }
    let capacity = matches!(&outcome, Outcome::Rejected(r) if r.retry_after().is_some());
    let mut outcome = outcome;
    // An accept ticket travels in the response head; it never reaches the client.
    let ticket = match &mut outcome {
        Outcome::Ok(reply) => reply
            .headers
            .remove(rt_types::TICKET_HEADER)
            .and_then(|v| v.to_str().ok().map(str::to_string)),
        _ => None,
    };
    let switching_requested = matches!(&outcome, Outcome::Ok(reply) if reply.status == StatusCode::SWITCHING_PROTOCOLS);
    if switching_requested || ticket.is_some() {
        return finish_upgrade(
            &s,
            &entry,
            &parts.headers,
            outcome,
            ticket,
            on_upgrade,
            hold,
            client,
            task,
        );
    }
    let mut resp = outcome_response(outcome, task, entry.version);
    if capacity {
        if let Ok(v) = HeaderValue::from_str(&retry_after_secs(s.scheduler.retry_after_hint()).max(1).to_string()) {
            resp.headers_mut().insert(header::RETRY_AFTER, v);
        }
    }
    resp
}

/// The guest answered an upgrade request: bind the socket it accepted (`101`), or refuse.
#[allow(clippy::too_many_arguments)]
fn finish_upgrade(
    s: &ApiState,
    entry: &rt_types::RouteEntry,
    request: &HeaderMap,
    outcome: Outcome,
    ticket: Option<String>,
    on_upgrade: Option<hyper::upgrade::OnUpgrade>,
    hold: Option<crate::serve::ConnHold>,
    client: Option<IpAddr>,
    task: TaskId,
) -> Response {
    let is_101 = matches!(&outcome, Outcome::Ok(reply) if reply.status == StatusCode::SWITCHING_PROTOCOLS);
    let (Some(ticket), true) = (ticket.clone(), is_101) else {
        // A ticket on an ordinary response (the guest accepted, then answered something else):
        // the reserved socket is released and the response goes out as it is.
        if let Some(t) = ticket {
            s.sockets.cancel_ticket(&t);
        }
        if is_101 {
            return plain_error(502, "bad_upgrade");
        }
        return outcome_response(outcome, task, entry.version);
    };
    let Some(claim) = s.sockets.claim(&ticket, entry.tenant) else {
        return plain_error(502, "bad_upgrade");
    };
    let (Some(on_upgrade), Some(key)) = (on_upgrade, request.get(header::SEC_WEBSOCKET_KEY)) else {
        return plain_error(400, "not_upgradable");
    };
    let mut resp = switching(key, claim.protocol());
    if let Ok(v) = HeaderValue::from_str(&task.to_string()) {
        resp.headers_mut().insert(HeaderName::from_static("x-rt-task-id"), v);
    }
    let hub = s.sockets.clone();
    let remote = client.map(|ip| ip.to_string());
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => hub.bind_io(claim, hyper_util::rt::TokioIo::new(upgraded), remote, Box::new(hold)),
            Err(e) => tracing::debug!(error = %e, "websocket upgrade failed before the socket connected"),
        }
    });
    resp
}

/// Cancels a still-waiting task when dropped (see `invoke`).
struct CancelIfWaiting(rt_sched::TaskCanceller);

impl Drop for CancelIfWaiting {
    fn drop(&mut self) {
        self.0.cancel_if_waiting();
    }
}

/// The route table names a service the service table does not hold yet (a control change is
/// being applied): not overload, and gone on the next refresh, so the client may retry at once.
fn route_stale() -> Response {
    limited(503, "route_stale", std::time::Duration::from_secs(1))
}

/// A guest-error style response: `text/plain` body (the reason) + `x-rt-reason`.
/// Never JSON, so guest failures cannot be mistaken for control API errors.
pub fn plain_error(status: u16, reason: &'static str) -> Response {
    let mut resp = Response::new(AxBody::from(reason));
    *resp.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp.headers_mut()
        .insert(HeaderName::from_static("x-rt-reason"), HeaderValue::from_static(reason));
    resp
}

/// Map a task `Outcome` to an HTTP response (docs/api.md).
///
/// `Ok` replies keep their status/headers/body and gain `x-rt-task-id`, `x-rt-version` and
/// `server-timing`; every other outcome becomes [`plain_error`].
pub fn outcome_response(outcome: Outcome, task: TaskId, version: VersionNo) -> Response {
    let mut resp = match outcome {
        Outcome::Ok(reply) => {
            // The framing is the runtime's (DREQ-004): hop-by-hop headers of the guest never reach
            // the client, and neither does a `content-length` its body does not have.
            let mut headers = reply.headers;
            strip_hop_by_hop(&mut headers);
            let declared = headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok());
            if declared.is_none_or(|n| reply.body.exact_len() != Some(n)) {
                headers.remove(header::CONTENT_LENGTH);
            }
            let mut r = Response::new(AxBody::from_stream(reply.body));
            *r.status_mut() = reply.status;
            *r.headers_mut() = headers;
            if let Ok(v) = HeaderValue::from_str(&reply.timings.server_timing()) {
                r.headers_mut().insert(HeaderName::from_static("server-timing"), v);
            }
            r
        }
        // A wait that ended without a slot (`503 queue_timeout`) carries `Retry-After` (D-A3).
        Outcome::Rejected(r) => match r.retry_after() {
            Some(after) => limited(r.http_status(), r.reason(), after),
            None => plain_error(r.http_status(), r.reason()),
        },
        other => {
            let reason = other.reason().unwrap_or("guest_error");
            plain_error(other.http_status(), reason)
        }
    };
    if let Ok(v) = HeaderValue::from_str(&task.to_string()) {
        resp.headers_mut().insert(HeaderName::from_static("x-rt-task-id"), v);
    }
    if let Ok(v) = HeaderValue::from_str(&version.to_string()) {
        resp.headers_mut().insert(HeaderName::from_static("x-rt-version"), v);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::RejectReason;

    /// Review finding 9: a route/service-table desync is a retryable `503 route_stale`.
    #[test]
    fn a_stale_route_is_a_retryable_503_not_overload() {
        let r = route_stale();
        assert_eq!(r.status(), 503);
        assert_eq!(
            r.headers().get("x-rt-reason").and_then(|v| v.to_str().ok()),
            Some("route_stale")
        );
        assert_eq!(
            r.headers().get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    fn client_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                HeaderName::from_bytes(k.as_bytes()).expect("name"),
                HeaderValue::from_str(v).expect("value"),
            );
        }
        h
    }

    fn get<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
        h.get(name).and_then(|v| v.to_str().ok())
    }

    /// TRG-22, SEC-56: `x-rt-client-ip` is the runtime's (a client's own never survives) and is
    /// absent without an address; the other runtime headers are set, hop-by-hop ones dropped.
    #[test]
    fn ac_trg_22_the_guest_gets_the_resolved_client_ip_and_never_a_spoofed_one() {
        let client = client_headers(&[
            ("x-rt-client-ip", "6.6.6.6"),
            ("x-rt-anything", "x"),
            ("x-forwarded-for", "6.6.6.6"),
            ("connection", "keep-alive, x-drop"),
            ("x-drop", "1"),
            ("host", "example.com"),
            ("accept", "text/plain"),
        ]);
        let ip: IpAddr = "203.0.113.9".parse().expect("ip");
        let h = guest_headers(&client, false, false, "/s/app", Some(ip));
        assert_eq!(get(&h, "x-rt-client-ip"), Some("203.0.113.9"));
        assert_eq!(h.get_all("x-rt-client-ip").iter().count(), 1);
        assert_eq!(
            (get(&h, "x-rt-base-path"), get(&h, "x-rt-trigger")),
            (Some("/s/app"), Some("http"))
        );
        assert_eq!(get(&h, "accept"), Some("text/plain"));
        assert_eq!(
            get(&h, "x-forwarded-for"),
            Some("6.6.6.6"),
            "the raw chain is passed on as it came"
        );
        for gone in ["x-rt-anything", "connection", "x-drop", "host"] {
            assert!(h.get(gone).is_none(), "{gone}");
        }
        let h = guest_headers(&client, true, false, "", None);
        assert!(
            h.get("x-rt-client-ip").is_none(),
            "no address (Unix socket): no header, not the client's"
        );
        assert!(h.get("x-rt-base-path").is_none());

        // Behind a trusted proxy the address comes from X-Forwarded-For; otherwise from the peer.
        let proxies = rt_sec::TrustedProxies::parse(&["10.0.0.0/8"]).expect("cidr");
        let xff = client_headers(&[("x-forwarded-for", "6.6.6.6, 198.51.100.4")]);
        let via_proxy = client_ip(Some("10.1.2.3".parse().expect("ip")), &xff, &proxies);
        assert_eq!(via_proxy, Some("198.51.100.4".parse().expect("ip")));
        let direct = client_ip(Some("192.0.2.7".parse().expect("ip")), &xff, &proxies);
        assert_eq!(
            direct,
            Some("192.0.2.7".parse().expect("ip")),
            "an untrusted peer's X-Forwarded-For is ignored"
        );
        let mapped = client_ip(
            Some("::ffff:192.0.2.7".parse().expect("ip")),
            &HeaderMap::new(),
            &proxies,
        );
        assert_eq!(
            mapped.map(|ip| ip.to_string()).as_deref(),
            Some("192.0.2.7"),
            "IPv4-mapped peers read as IPv4"
        );
    }

    /// SEC-56: a non-`rtk_` `Authorization` reaches the guest unchanged; an `rtk_` bearer (a
    /// runtime credential, on any service) is removed.
    #[test]
    fn ac_sec_56_only_the_invoke_key_is_removed_from_authorization() {
        for (value, key) in [
            ("Bearer rtk_abc_def", true),
            ("bearer   rtk_abc_def ", true),
            ("Bearer app-token", false),
            ("Bearer rtkabc", false),
            ("Basic dXNlcjpwYXNz", false),
            ("rtk_abc_def", false),
        ] {
            assert_eq!(is_key_bearer(&HeaderValue::from_static(value)), key, "{value}");
        }
        let app = client_headers(&[("authorization", "Bearer app-token"), ("x-rt-key", "rtk_abc_def")]);
        let h = guest_headers(&app, false, false, "", None);
        assert_eq!(
            get(&h, "authorization"),
            Some("Bearer app-token"),
            "the guest's own token passes unchanged"
        );
        assert!(h.get("x-rt-key").is_none(), "x-rt-key is a runtime header");
        let keyed = client_headers(&[("authorization", "Bearer rtk_abc_def")]);
        assert!(guest_headers(&keyed, false, true, "", None)
            .get("authorization")
            .is_none());
    }

    /// INV-021: dot segments (raw, percent-encoded, after a backslash) resolve like the guest's
    /// URL parser resolves them, and `allow_paths` needs both forms to match.
    #[test]
    fn inv_021_paths_resolve_like_the_guests_url_parser() {
        for (raw, resolved) in [
            ("/", "/"),
            ("/a/b", "/a/b"),
            ("/a/b/", "/a/b/"),
            ("/public/../admin", "/admin"),
            ("/public/%2e%2e/admin", "/admin"),
            ("/public/%2E./admin", "/admin"),
            ("/public/.%2e/x", "/x"),
            ("/public/./a", "/public/a"),
            ("/public/x/../y", "/public/y"),
            ("/a/..", "/"),
            ("/a/.", "/a/"),
            ("/../../etc", "/etc"),
            ("/public\\..\\admin", "/admin"),
            ("/a/%2e%2e%2e/b", "/a/%2e%2e%2e/b"),
        ] {
            assert_eq!(resolved_path(raw), resolved, "{raw}");
        }
        let allow = vec!["/public/*".to_string(), "/ok".to_string()];
        assert!(request_path_allowed(&allow, "/public/a"));
        assert!(request_path_allowed(&allow, "/public/x/../y"));
        assert!(!request_path_allowed(&allow, "/public/../admin"));
        assert!(!request_path_allowed(&allow, "/public/%2e%2e/admin"));
        assert!(
            !request_path_allowed(&allow, "/admin/../public/a"),
            "the raw path must match too"
        );
        assert!(request_path_allowed(&allow, "/ok"));
        assert!(!request_path_allowed(&allow, "/ok/"));
        assert!(request_path_allowed(&[], "/anything/../at/all"), "no rules: everything");
    }

    /// D-A3: capacity refusals carry `Retry-After`; a guest-visible error does not.
    #[test]
    fn capacity_rejections_carry_retry_after() {
        for r in [RejectReason::QueueTimeout, RejectReason::Overloaded] {
            let resp = outcome_response(Outcome::Rejected(r), TaskId(1), VersionNo(1));
            assert_eq!(resp.status().as_u16(), 503);
            assert_eq!(
                resp.headers().get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
                Some("1"),
                "{r:?}"
            );
            assert_eq!(
                resp.headers().get("x-rt-reason").and_then(|v| v.to_str().ok()),
                Some(r.reason())
            );
        }
        let resp = outcome_response(Outcome::Rejected(RejectReason::Draining), TaskId(1), VersionNo(1));
        assert!(
            resp.headers().get(header::RETRY_AFTER).is_none(),
            "draining: retry elsewhere"
        );
        let resp = outcome_response(Outcome::GuestError, TaskId(1), VersionNo(1));
        assert!(resp.headers().get(header::RETRY_AFTER).is_none());
    }

    /// INV-011: next to an `rtk_` bearer, the guest's own `Authorization` values survive: only the
    /// runtime credentials are removed.
    #[test]
    fn inv_011_only_credential_authorization_values_are_removed() {
        let mut h = client_headers(&[
            ("authorization", "Bearer app"),
            ("authorization", "Bearer rtk_abc_def"),
            ("authorization", "Basic dXNlcjpw"),
        ]);
        keep_guest_authorization(&mut h, is_key_bearer);
        let left: Vec<&str> = h
            .get_all("authorization")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        assert_eq!(left, ["Bearer app", "Basic dXNlcjpw"]);
        let mut only = client_headers(&[("authorization", "Bearer rtk_abc_def")]);
        keep_guest_authorization(&mut only, is_key_bearer);
        assert!(only.get("authorization").is_none());
    }

    /// INV-022: an `allow_paths` entry without `*` is exact: no trailing slash, doubled slash,
    /// other case or `;` parameter variants.
    #[test]
    fn inv_022_exact_allow_paths_match_only_themselves() {
        let allow = vec!["/admin".to_string()];
        assert!(request_path_allowed(&allow, "/admin"));
        for p in ["/admin/", "//admin", "/ADMIN", "/admin;x", "/admin%2f", "/x/../admin/"] {
            assert!(!request_path_allowed(&allow, p), "{p}");
        }
    }

    /// INV-023: `allowed_methods` ignores case and admits custom methods it lists.
    #[test]
    fn inv_023_allowed_methods_ignore_case_and_take_custom_methods() {
        let list = vec!["get".to_string(), "PURGE".to_string()];
        assert!(method_allowed(&list, &http::Method::GET));
        assert!(method_allowed(&list, &http::Method::from_bytes(b"PURGE").unwrap()));
        assert!(method_allowed(&list, &http::Method::from_bytes(b"purge").unwrap()));
        assert!(!method_allowed(&list, &http::Method::POST));
        assert!(method_allowed(&[], &http::Method::DELETE), "no list: every method");
    }

    /// INV-031: headers the client names in `Connection` are dropped, also `x-rt-invoker` on the
    /// internal listener (a hop-by-hop header is never forwarded).
    #[test]
    fn inv_031_connection_named_headers_are_dropped() {
        let c = client_headers(&[
            ("connection", "keep-alive, X-Secret"),
            ("x-secret", "s"),
            ("x-keep", "k"),
        ]);
        let h = guest_headers(&c, false, false, "", None);
        assert!(h.get("x-secret").is_none());
        assert_eq!(get(&h, "x-keep"), Some("k"));
        let c = client_headers(&[("connection", "x-rt-invoker"), ("x-rt-invoker", "platform")]);
        assert!(guest_headers(&c, true, false, "", None).get("x-rt-invoker").is_none());
        let c = client_headers(&[("x-rt-invoker", "platform")]);
        assert_eq!(
            get(&guest_headers(&c, true, false, "", None), "x-rt-invoker"),
            Some("platform")
        );
    }

    /// INV-032: only a well-formed W3C `traceparent` is propagated.
    #[test]
    fn inv_032_only_well_formed_traceparents_are_kept() {
        let good = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert!(valid_traceparent(good));
        assert!(valid_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        ));
        for bad in [
            "00-garbage",
            "00-zz",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-4bf92f3577b34da6a3ce929d0e0e473-600f067aa0ba902b7-01",
        ] {
            assert!(!valid_traceparent(bad), "{bad}");
        }
    }

    /// INV-034: the guest's URI keeps the path tail and query byte for byte (percent-encoding
    /// untouched); a tail that is no URI is refused rather than replaced by `/`.
    #[test]
    fn inv_034_the_guest_uri_keeps_the_tail_and_query_verbatim() {
        let u = guest_uri("/a%2Fb/%E2%9C%93", Some("q=%20x&y")).expect("uri");
        assert_eq!(u.path(), "/a%2Fb/%E2%9C%93");
        assert_eq!(u.query(), Some("q=%20x&y"));
        assert_eq!(guest_uri("/", None).expect("uri").to_string(), "/");
        assert!(guest_uri("/a b", None).is_none());
    }

    /// WSUP-006: of several `rtk_` subprotocols the first is the key and every one is removed
    /// (the guest never sees a key); other protocols pass in order; a malformed `rtk_x` is still
    /// taken as the (failing) key, never left for the guest.
    #[test]
    fn wsup_006_key_subprotocols_are_all_stripped() {
        let mut h = client_headers(&[("sec-websocket-protocol", "chat, rtk_AAA_one, rtk_BBB_two, v2")]);
        assert_eq!(take_key_protocol(&mut h).as_deref(), Some("rtk_AAA_one"));
        assert_eq!(get(&h, "sec-websocket-protocol"), Some("chat, v2"));
        let mut h = client_headers(&[("sec-websocket-protocol", "rtk_x")]);
        assert_eq!(
            take_key_protocol(&mut h).as_deref(),
            Some("rtk_x"),
            "taken as the key, which then fails authentication"
        );
        assert!(
            h.get("sec-websocket-protocol").is_none(),
            "still never reaches the guest"
        );
    }
}
