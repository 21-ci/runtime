//! Service-to-service calls and operator platform bindings.
//!
//! [`Calls`] is implemented by `rt-triggers` (it owns routes + the scheduler); [`PlatformBindings`]
//! by [`ConfigBindings`] below. Both are injected into `HostServices`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, HeaderValue};
use parking_lot::Mutex;
use rt_types::{Body, Limits, MsgId, RejectReason, ServiceId, TaskId, TenantId, TraceCtx};
use thiserror::Error;
use url::Url;

use crate::egress::{EgressBudget, EgressGuard, EgressPolicy, Request, Response};
use crate::rpc::RpcError;

/// Identity and deadline of the task making a host call.
#[derive(Clone, Debug)]
pub struct TaskCtx {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub task: TaskId,
    /// Call depth of this task (0 for non-call triggers).
    pub depth: u8,
    pub deadline: Instant,
    pub trace: Option<TraceCtx>,
    /// A task of the system tenant run for an operator SDK call: the calling tenant. Operator
    /// bindings forward it (the tenant header) and apply its plan and rate limit.
    pub on_behalf_of: Option<TenantId>,
    /// The object this task acts for (a socket event, an alarm, a request to a durable object).
    pub object: Option<std::sync::Arc<str>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnqueueOpts {
    pub delay: Duration,
    pub dedup_id: Option<String>,
}

/// Failures of `std:services` calls / `std:queue` enqueues. Codes are stable.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CallError {
    /// Unknown slug (or another tenant's, when cross-tenant calls are off ⇒ `NotFound` to avoid
    /// revealing existence — except the explicit `CrossTenant` case below).
    #[error("not_found")]
    NotFound,
    /// `403 cross_tenant_call` unless the plan allows it (TRG-16).
    #[error("cross_tenant_call")]
    CrossTenant,
    /// `508 call_depth` (TRG-15).
    #[error("call_depth")]
    Depth,
    /// Target has `call` disabled or access `internal`/`key` forbids the caller.
    #[error("forbidden")]
    Forbidden,
    /// `403 call_not_allowed`: same tenant, but the target's `call.allowed_callers` (or, when
    /// empty, its environment) does not admit the caller (D-A1).
    #[error("call_not_allowed")]
    NotAllowed,
    /// `403 trigger_not_allowed`: the target tenant's plan does not allow the trigger.
    #[error("trigger_not_allowed")]
    TriggerNotAllowed,
    /// Admission refused the child task.
    #[error("rejected: {0:?}")]
    Rejected(RejectReason),
    /// `queue_full`.
    #[error("queue_full")]
    QueueFull,
    /// Child killed (timeout, cpu, oom…) — the parent sees a 5xx response object instead where
    /// possible; this is for transport-level failures.
    #[error("call_failed")]
    Failed,
    /// `durable.get(name)` of a service that is not durable.
    #[error("not_durable")]
    NotDurable,
}

impl CallError {
    pub fn code(&self) -> &'static str {
        match self {
            CallError::NotFound => "not_found",
            CallError::CrossTenant => "cross_tenant_call",
            CallError::Depth => "call_depth",
            CallError::Forbidden => "forbidden",
            CallError::NotAllowed => "call_not_allowed",
            CallError::TriggerNotAllowed => "trigger_not_allowed",
            CallError::Rejected(r) => r.reason(),
            CallError::QueueFull => "queue_full",
            CallError::Failed => "call_failed",
            CallError::NotDurable => "not_durable",
        }
    }
}

#[async_trait]
pub trait Calls: Send + Sync {
    /// In-process child task (`trigger = Call{parent, depth+1}`); no HTTP hop. `slug` resolves
    /// within the caller's tenant.
    async fn call(&self, from: &TaskCtx, slug: &str, req: Request) -> Result<Response, CallError>;

    /// `std:queue` enqueue to a service of the same tenant.
    async fn enqueue(&self, from: &TaskCtx, slug: &str, body: Bytes, o: EnqueueOpts) -> Result<MsgId, CallError>;

    /// `std:durable` `durable.get(name, { service }).fetch(req)`: a request to the object `object`
    /// of the durable service `slug` (`None`: the caller's own service). It waits for the object's
    /// turn: an object runs one request or event at a time. `not_durable` (as `Forbidden`) when the
    /// target is not a durable service.
    async fn call_object(
        &self,
        from: &TaskCtx,
        slug: Option<&str>,
        object: &str,
        req: Request,
    ) -> Result<Response, CallError> {
        let _ = (from, slug, object, req);
        Err(CallError::NotFound)
    }

    /// `std:sdk` / WIT `sdk.invoke`: `method` of the operator SDK `sdk` with the JSON argument,
    /// bound to the release the caller's grants select. The JSON result,
    /// or the SDK's (or the runtime's) error.
    async fn sdk_invoke(&self, from: &TaskCtx, sdk: &str, method: &str, args_json: &str) -> Result<String, RpcError> {
        let _ = (from, method, args_json);
        Err(RpcError::new("sdk_not_found", format!("there is no SDK `{sdk}`")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BindingError {
    #[error("binding_not_found")]
    NotFound,
    /// Tenant's plan not in `allowed_plans` (`403 binding_not_allowed`).
    #[error("binding_not_allowed")]
    NotAllowed,
    /// Method/path not allowed or traversal attempt (`403 binding_denied`, SEC-25).
    /// Executors surface it to the guest as a normal `Response` with status 403 and header
    /// `x-rt-reason: binding_denied` — `platform.fetch` does **not** throw (contracts C-8).
    #[error("binding_denied")]
    Denied,
    #[error("rate_limited")]
    RateLimited,
    #[error("binding_upstream")]
    Upstream,
}

impl BindingError {
    /// HTTP status executors use when they surface the error as a response (`403` for
    /// `binding_denied` / `binding_not_allowed`, contracts C-8).
    pub fn http_status(&self) -> u16 {
        match self {
            BindingError::Denied | BindingError::NotAllowed => 403,
            BindingError::NotFound => 404,
            BindingError::RateLimited => 429,
            BindingError::Upstream => 502,
        }
    }

    /// The error as a guest-visible response: status per [`BindingError::http_status`], header
    /// `x-rt-reason: <code>`, generic text body (no upstream detail, SEC-26).
    pub fn to_response(&self) -> Response {
        let code = self.code();
        let mut resp = http::Response::new(Body::from(code));
        *resp.status_mut() = http::StatusCode::from_u16(self.http_status()).unwrap_or(http::StatusCode::FORBIDDEN);
        resp.headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        resp.headers_mut().insert("x-rt-reason", HeaderValue::from_static(code));
        resp
    }

    pub fn code(&self) -> &'static str {
        match self {
            BindingError::NotFound => "binding_not_found",
            BindingError::NotAllowed => "binding_not_allowed",
            BindingError::Denied => "binding_denied",
            BindingError::RateLimited => "rate_limited",
            BindingError::Upstream => "binding_upstream",
        }
    }
}

#[async_trait]
pub trait PlatformBindings: Send + Sync {
    async fn fetch(&self, from: &TaskCtx, binding: &str, req: Request) -> Result<Response, BindingError>;
}

/// Bindings from `[[binding]]` config.
///
/// Confused-deputy guard (RS-06): the path is percent-decoded and normalized and any `..`
/// segment is rejected; `allowed_methods`/`allowed_paths` (glob) are matched afterwards; guest
/// `Authorization`, `Cookie` and `x-rt-*` headers are stripped before the configured headers are
/// injected; upstream `Set-Cookie` is dropped; injected header values never reach guest memory;
/// per-tenant rate limit applies.
///
/// Fail closed: an empty `allowed_methods` or `allowed_paths` allows nothing.
pub struct ConfigBindings {
    bindings: HashMap<String, ResolvedBinding>,
    plan_of: Arc<dyn Fn(TenantId) -> String + Send + Sync>,
    egress: Arc<dyn EgressGuard>,
    buckets: Mutex<HashMap<(String, TenantId), Bucket>>,
}

/// A `[[binding]]` with its operator-side values resolved once at startup.
struct ResolvedBinding {
    cfg: rt_config::BindingConfig,
    /// `None`: `base_url` did not parse; every call fails with `binding_upstream`.
    base: Option<Url>,
    inject: Vec<(HeaderName, HeaderValue)>,
    tenant_header: Option<HeaderName>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl ConfigBindings {
    /// `plan_of` resolves a tenant's plan name for `allowed_plans`. `egress` carries the upstream
    /// requests: it must be a guard for operator-trusted endpoints
    /// ([`crate::GuardedEgress::for_trusted_endpoints`]) when `base_url`s are internal.
    pub fn new(
        bindings: Vec<rt_config::BindingConfig>,
        plan_of: Arc<dyn Fn(TenantId) -> String + Send + Sync>,
        egress: Arc<dyn crate::egress::EgressGuard>,
    ) -> Self {
        let mut map = HashMap::new();
        for cfg in bindings {
            let base = match Url::parse(&cfg.base_url) {
                Ok(u) if matches!(u.scheme(), "http" | "https") && u.host_str().is_some() => Some(u),
                _ => {
                    tracing::warn!(binding = %cfg.name, "binding base_url is not a valid http(s) URL; binding disabled");
                    None
                }
            };
            let mut inject = Vec::new();
            for (name, value) in &cfg.inject_headers {
                let resolved = match value.strip_prefix("env:") {
                    Some(var) => match std::env::var(var) {
                        Ok(v) => v,
                        Err(_) => {
                            tracing::warn!(binding = %cfg.name, header = %name, "binding header env var is not set; header skipped");
                            continue;
                        }
                    },
                    None => value.clone(),
                };
                match (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(&resolved),
                ) {
                    (Ok(n), Ok(mut v)) => {
                        v.set_sensitive(true);
                        inject.push((n, v));
                    }
                    _ => tracing::warn!(binding = %cfg.name, header = %name, "invalid binding header; skipped"),
                }
            }
            let tenant_header = (!cfg.tenant_header.is_empty())
                .then(|| HeaderName::from_bytes(cfg.tenant_header.as_bytes()).ok())
                .flatten();
            map.insert(
                cfg.name.clone(),
                ResolvedBinding {
                    cfg,
                    base,
                    inject,
                    tenant_header,
                },
            );
        }
        Self {
            bindings: map,
            plan_of,
            egress,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Percent-decode and normalize `path`; `Err` if it contains `..`, NUL, or fails to decode.
    ///
    /// The returned path is decoded once, with `.` and empty segments removed. Traversal is
    /// checked on every decoding round (up to three) so `%252e%252e` cannot slip through a
    /// double-decoding upstream; `\` is treated as a separator and refused.
    pub fn normalize_path(path: &str) -> Result<String, BindingError> {
        let mut current = path.to_string();
        let mut first: Option<String> = None;
        for round in 0..3 {
            let decoded = percent_decode(&current, round == 0)?;
            check_segments(&decoded)?;
            let changed = decoded != current;
            if first.is_none() {
                first = Some(decoded.clone());
            }
            if !changed {
                break;
            }
            current = decoded;
        }
        let decoded = first.ok_or(BindingError::Denied)?;
        let mut out = String::with_capacity(decoded.len() + 1);
        for seg in decoded.split('/') {
            if seg.is_empty() || seg == "." {
                continue;
            }
            out.push('/');
            out.push_str(seg);
        }
        if out.is_empty() || decoded.ends_with('/') {
            out.push('/');
        }
        Ok(out)
    }

    /// Does `path` match one of the glob patterns (`/v1/calls*`)? `*` matches any run of
    /// characters (including `/`), `?` one character.
    pub fn path_allowed(patterns: &[String], path: &str) -> bool {
        patterns.iter().any(|p| glob_match(p, path))
    }

    fn take_token(&self, binding: &ResolvedBinding, tenant: TenantId) -> bool {
        let rps = binding.cfg.rate_limit.rps;
        if rps == 0 {
            return true;
        }
        let burst = f64::from(binding.cfg.rate_limit.burst.max(1));
        let now = Instant::now();
        let mut g = self.buckets.lock();
        let b = g.entry((binding.cfg.name.clone(), tenant)).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * f64::from(rps)).min(burst);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[async_trait]
impl PlatformBindings for ConfigBindings {
    async fn fetch(&self, from: &TaskCtx, binding: &str, req: Request) -> Result<Response, BindingError> {
        let b = self.bindings.get(binding).ok_or(BindingError::NotFound)?;
        // Operator bindings belong to the operator SDK services of the system tenant; a tenant
        // reaches the operator API through an SDK, never directly.
        if !rt_types::is_system_tenant(from.tenant) {
            return Err(BindingError::NotAllowed);
        }
        // The tenant the call is for: an SDK call's caller (forwarded upstream, its plan and rate
        // limit applied), else the system tenant itself.
        let tenant = from.on_behalf_of.unwrap_or(from.tenant);
        if !b.cfg.allowed_plans.is_empty() {
            let plan = (self.plan_of)(tenant);
            if !b.cfg.allowed_plans.contains(&plan) {
                return Err(BindingError::NotAllowed);
            }
        }
        let method = req.method().clone();
        if !b
            .cfg
            .allowed_methods
            .iter()
            .any(|m| m.eq_ignore_ascii_case(method.as_str()))
        {
            return Err(BindingError::Denied);
        }
        let path = Self::normalize_path(req.uri().path())?;
        if !Self::path_allowed(&b.cfg.allowed_paths, &path) {
            return Err(BindingError::Denied);
        }
        let base = b.base.as_ref().ok_or(BindingError::Upstream)?;
        if !self.take_token(b, tenant) {
            return Err(BindingError::RateLimited);
        }

        // Upstream URL: base_url's path prefix + the normalized, re-encoded guest path.
        let mut url = base.clone();
        let prefix = base.path().trim_end_matches('/');
        url.set_path(&format!("{prefix}{}", encode_path(&path)));
        url.set_query(req.uri().query());
        url.set_fragment(None);

        let (parts, body) = req.into_parts();
        let headers = binding_request_headers(parts.headers, b);
        let mut upstream = http::Request::new(body);
        *upstream.method_mut() = method;
        *upstream.uri_mut() = url.as_str().parse().map_err(|_| BindingError::Upstream)?;
        *upstream.headers_mut() = headers;
        if let Some(name) = &b.tenant_header {
            if let Ok(v) = HeaderValue::from_str(&tenant.to_string()) {
                upstream.headers_mut().insert(name.clone(), v);
            }
        }

        let policy = EgressPolicy::for_build(vec![base.host_str().unwrap_or_default().to_string()]);
        let limits = Limits {
            max_fetches: 1,
            max_concurrent_fetches: 1,
            ..Limits::default()
        };
        // Upstream connections are pooled per tenant like guest fetches (SEC-52).
        let mut budget = EgressBudget::new(&limits, from.deadline, None).for_tenant(tenant);
        let mut resp = self.egress.send(upstream, &policy, &mut budget).await.map_err(|e| {
            tracing::warn!(binding, code = e.code(), "binding upstream call failed");
            BindingError::Upstream
        })?;
        resp.headers_mut().remove(header::SET_COOKIE);
        Ok(resp)
    }
}

/// Guest headers as forwarded to a binding upstream: no credentials, no `x-rt-*`, no hop-by-hop;
/// anything the operator injects (or the tenant header) replaces a guest header of the same name.
fn binding_request_headers(guest: HeaderMap, b: &ResolvedBinding) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in &guest {
        let n = name.as_str();
        let drop = n.starts_with("x-rt-")
            || matches!(
                n,
                "authorization"
                    | "cookie"
                    | "proxy-authorization"
                    | "host"
                    | "content-length"
                    | "connection"
                    | "keep-alive"
                    | "proxy-connection"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
            )
            || b.inject.iter().any(|(h, _)| h == name)
            || b.tenant_header.as_ref().is_some_and(|h| h == name);
        if !drop {
            out.append(name.clone(), value.clone());
        }
    }
    for (name, value) in &b.inject {
        out.insert(name.clone(), value.clone());
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Percent-decode; `strict` refuses malformed escapes and non-UTF-8 results, otherwise malformed
/// escapes are left as they are (later decoding rounds only look for traversal).
fn percent_decode(s: &str, strict: bool) -> Result<String, BindingError> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = bytes.get(i + 1).copied().and_then(hex_val);
            let lo = bytes.get(i + 2).copied().and_then(hex_val);
            match (hi, lo) {
                (Some(h), Some(l)) => {
                    out.push(h * 16 + l);
                    i += 3;
                    continue;
                }
                _ if strict => return Err(BindingError::Denied),
                _ => {}
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    match String::from_utf8(out) {
        Ok(s) => Ok(s),
        Err(_) if strict => Err(BindingError::Denied),
        Err(e) => Ok(String::from_utf8_lossy(e.as_bytes()).into_owned()),
    }
}

/// Reject traversal (`..`, also behind `\`), NUL and control characters.
fn check_segments(decoded: &str) -> Result<(), BindingError> {
    if decoded.chars().any(|c| c.is_control()) {
        return Err(BindingError::Denied);
    }
    if decoded.split(['/', '\\']).any(|seg| seg == "..") {
        return Err(BindingError::Denied);
    }
    if decoded.contains('\\') {
        return Err(BindingError::Denied);
    }
    Ok(())
}

/// Percent-encode everything but unreserved characters and the path delimiters.
fn encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for &b in path.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-._~/!$&'()*+,;=:@".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `*` (any run, incl. `/`) and `?` (one char) glob matching.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) && p[pi] != '*' {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Calls` that overrides only the two required methods: the optional ones keep their defaults.
    struct RequiredOnly;

    #[async_trait]
    impl Calls for RequiredOnly {
        async fn call(&self, _from: &TaskCtx, _slug: &str, _req: Request) -> Result<Response, CallError> {
            Err(CallError::NotFound)
        }

        async fn enqueue(
            &self,
            _from: &TaskCtx,
            _slug: &str,
            _body: Bytes,
            _o: EnqueueOpts,
        ) -> Result<MsgId, CallError> {
            Err(CallError::NotFound)
        }
    }

    fn ctx() -> TaskCtx {
        TaskCtx {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            task: TaskId(3),
            depth: 0,
            deadline: Instant::now() + Duration::from_secs(5),
            trace: None,
            on_behalf_of: None,
            object: None,
        }
    }

    #[test]
    fn call_errors_map_to_their_stable_wire_codes() {
        assert_eq!(CallError::NotFound.code(), "not_found");
        assert_eq!(CallError::CrossTenant.code(), "cross_tenant_call");
        assert_eq!(CallError::Depth.code(), "call_depth");
        assert_eq!(CallError::Forbidden.code(), "forbidden");
        assert_eq!(CallError::NotAllowed.code(), "call_not_allowed");
        assert_eq!(CallError::TriggerNotAllowed.code(), "trigger_not_allowed");
        assert_eq!(CallError::QueueFull.code(), "queue_full");
        assert_eq!(CallError::Failed.code(), "call_failed");
        assert_eq!(CallError::NotDurable.code(), "not_durable");
    }

    #[tokio::test]
    async fn a_calls_implementation_that_skips_the_optional_methods_refuses_them() {
        let req = || {
            http::Request::builder()
                .uri("http://svc/")
                .body(Body::empty())
                .expect("request")
        };
        let from = ctx();
        assert_eq!(
            RequiredOnly
                .call_object(&from, None, "obj", req())
                .await
                .expect_err("no durable objects"),
            CallError::NotFound
        );
        let err = RequiredOnly
            .sdk_invoke(&from, "ops", "list", "{}")
            .await
            .expect_err("no SDKs");
        assert_eq!(err.code, "sdk_not_found");
        assert!(err.message.contains("ops"), "the error names the SDK: {}", err.message);
    }

    #[test]
    fn percent_decoding_is_strict_for_routing_and_lenient_for_later_rounds() {
        assert_eq!(percent_decode("a%41b", true), Ok("aAb".to_string()));
        assert_eq!(
            percent_decode("a%zz", true),
            Err(BindingError::Denied),
            "a malformed escape is refused"
        );
        assert_eq!(
            percent_decode("a%zz", false),
            Ok("a%zz".to_string()),
            "kept as written after round one"
        );
        assert_eq!(
            percent_decode("%FF", true),
            Err(BindingError::Denied),
            "bytes that are not UTF-8 are refused"
        );
        assert_eq!(percent_decode("%FF", false), Ok("\u{FFFD}".to_string()));
    }

    #[test]
    fn segments_refuse_traversal_backslashes_and_control_characters() {
        assert_eq!(check_segments("a/b"), Ok(()));
        assert_eq!(check_segments("a/../b"), Err(BindingError::Denied));
        assert_eq!(check_segments(".."), Err(BindingError::Denied));
        assert_eq!(check_segments("a\\b"), Err(BindingError::Denied));
        assert_eq!(check_segments("a\u{1}b"), Err(BindingError::Denied));
        assert_eq!(check_segments("a\0b"), Err(BindingError::Denied));
    }

    #[test]
    fn encode_path_keeps_delimiters_and_escapes_everything_else() {
        assert_eq!(encode_path("/a b/\u{e9}"), "/a%20b/%C3%A9");
        assert_eq!(encode_path("/v1/x-y_z.~"), "/v1/x-y_z.~");
    }

    #[test]
    fn normalize_path_drops_dot_and_empty_segments_and_keeps_a_trailing_slash() {
        assert_eq!(ConfigBindings::normalize_path("/a/./b//c"), Ok("/a/b/c".to_string()));
        assert_eq!(ConfigBindings::normalize_path("/x/"), Ok("/x/".to_string()));
        assert_eq!(ConfigBindings::normalize_path("/%41%42"), Ok("/AB".to_string()));
        assert_eq!(ConfigBindings::normalize_path(""), Ok("/".to_string()));
    }

    #[test]
    fn normalize_path_refuses_traversal_however_it_is_encoded() {
        assert_eq!(ConfigBindings::normalize_path("/a/%2e%2e/b"), Err(BindingError::Denied));
        // Double-encoded: the second decoding round exposes `..`.
        assert_eq!(
            ConfigBindings::normalize_path("/%252e%252e/x"),
            Err(BindingError::Denied)
        );
        // Triple-encoded: the third round does.
        assert_eq!(
            ConfigBindings::normalize_path("/%25252e%25252e/x"),
            Err(BindingError::Denied)
        );
        assert_eq!(ConfigBindings::normalize_path("/a%5cb"), Err(BindingError::Denied));
        assert_eq!(ConfigBindings::normalize_path("/a%00b"), Err(BindingError::Denied));
        assert_eq!(ConfigBindings::normalize_path("/%zz"), Err(BindingError::Denied));
    }

    #[test]
    fn path_patterns_are_globs_over_the_normalized_path() {
        let pats = vec!["/v1/calls*".to_string()];
        assert!(ConfigBindings::path_allowed(&pats, "/v1/calls/x/y"));
        assert!(!ConfigBindings::path_allowed(&pats, "/v2/calls"));
        assert!(
            !ConfigBindings::path_allowed(&[], "/v1/calls"),
            "no pattern admits nothing"
        );
    }
}
