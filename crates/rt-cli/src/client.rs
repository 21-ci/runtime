//! Blocking HTTP client for the control API: auth headers, error envelope parsing, network
//! error mapping, `Retry-After` handling (`--retry`).

use std::error::Error as _;
use std::time::Duration;

use reqwest::blocking::{Client as Http, RequestBuilder, Response};
use reqwest::header::{HeaderMap, ACCEPT, RETRY_AFTER};
use reqwest::Method;
use serde_json::{json, Value};

use crate::config::Settings;
use crate::error::{human_secs, CliError, Result};
use crate::fmt;

pub const USER_AGENT: &str = concat!("21ci/", env!("CARGO_PKG_VERSION"));

/// `--retry` never sleeps longer than this for one attempt; a longer `Retry-After` (a daily
/// quota, say) is reported at once instead.
pub const MAX_RETRY_WAIT_SECS: u64 = 60;

pub struct Client {
    http: Http,
    /// No overall timeout: SSE streams live as long as the server keeps them.
    stream: Http,
    pub api: String,
    token: Option<String>,
    tenant: Option<String>,
    /// `--retry N`: how many times a `429`/`503` with `Retry-After` is retried.
    retries: u32,
}

/// A successful response with its JSON body (`Null` for an empty body).
pub struct Reply {
    pub headers: HeaderMap,
    pub body: Value,
}

pub fn http_client(timeout: Option<Duration>) -> Result<Http> {
    Http::builder()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(15))
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| CliError::failed(format!("cannot build the HTTP client: {e}")))
}

/// `http://` to a host that is not loopback: a credential sent there crosses the network in the
/// clear (CLINET-001).
fn plain_http_beyond_loopback(api: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(api) else {
        return false;
    };
    if url.scheme() != "http" {
        return false;
    }
    let Some(host) = url.host_str() else { return false };
    let host = host.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => !ip.is_loopback(),
        Err(_) => host != "localhost" && !host.ends_with(".localhost"),
    }
}

/// Map a transport error to exit code 3 (unreachable / timeout) or a plain failure.
pub fn net_error(base: &str, e: &reqwest::Error) -> CliError {
    if e.is_timeout() {
        return CliError::Network(format!("request to {base} timed out"));
    }
    if e.is_builder() {
        return CliError::usage(format!("invalid URL {base}: {e}"));
    }
    if e.is_connect() || e.is_request() || e.is_body() {
        let mut deepest = e.to_string();
        let mut src = e.source();
        while let Some(s) = src {
            deepest = s.to_string();
            src = s.source();
        }
        return CliError::Network(format!("cannot reach {base}: {deepest}"));
        // LCOV_EXCL_START: reqwest reports send and body failures as request or body errors, and redirects are not followed
    }
    CliError::failed(format!("request to {base} failed: {e}"))
    // LCOV_EXCL_STOP
}

/// `Retry-After` as delta-seconds (the runtime never sends the HTTP-date form).
pub fn retry_after(h: &HeaderMap) -> Option<u64> {
    h.get(RETRY_AFTER)?.to_str().ok()?.trim().parse().ok()
}

/// `x-rt-reason` (invoke listener failures carry it).
pub fn rt_reason(h: &HeaderMap) -> Option<String> {
    h.get("x-rt-reason").and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// Send, and while `retries` allow, repeat a `429`/`503` that carries a `Retry-After` of at most
/// [`MAX_RETRY_WAIT_SECS`] after sleeping that long. Returns the last response whatever its
/// status. A request whose body cannot be cloned is sent once.
pub fn send_retrying(rb: RequestBuilder, retries: u32, base: &str) -> Result<Response> {
    let mut rb = rb;
    let mut attempt = 0u32;
    loop {
        let next = if attempt < retries { rb.try_clone() } else { None };
        let resp = rb.send().map_err(|e| net_error(base, &e))?;
        let status = resp.status().as_u16();
        let wait = retry_after(resp.headers()).filter(|s| *s <= MAX_RETRY_WAIT_SECS);
        match (next, wait) {
            (Some(again), Some(secs)) if status == 429 || status == 503 => {
                attempt += 1;
                let reason = rt_reason(resp.headers());
                let code = resp
                    .text()
                    .ok()
                    .and_then(|t| serde_json::from_str::<Value>(&t).ok())
                    .and_then(|v| v.pointer("/error/code").and_then(Value::as_str).map(str::to_string))
                    .or(reason)
                    .map(|c| format!(" {c}"))
                    .unwrap_or_default();
                fmt::note(&format!(
                    "HTTP {status}{code}: retrying in {} (attempt {attempt} of {retries})",
                    human_secs(secs)
                ));
                std::thread::sleep(Duration::from_secs(secs));
                rb = again;
            }
            _ => return Ok(resp),
        }
    }
}

impl Client {
    pub fn new(s: &Settings, retries: u32) -> Result<Client> {
        if s.token.is_some() && plain_http_beyond_loopback(&s.api) {
            fmt::note(&format!(
                "warning: the credential goes over plain http to {}: use https, a loopback address or a VPN (SECURITY.md)",
                s.api
            ));
        }
        Ok(Client {
            http: http_client(Some(Duration::from_secs(300)))?,
            stream: http_client(None)?,
            api: s.api.clone(),
            token: s.token.clone(),
            tenant: s.tenant.clone(),
            retries,
        })
    }

    fn builder(&self, http: &Http, method: Method, path: &str) -> RequestBuilder {
        let mut rb = http
            .request(method, format!("{}{}", self.api, path))
            .header(ACCEPT, "application/json");
        if let Some(t) = &self.token {
            rb = rb.bearer_auth(t);
        }
        if let Some(t) = &self.tenant {
            rb = rb.header("x-rt-tenant", t);
        }
        rb
    }

    pub fn req(&self, method: Method, path: &str) -> RequestBuilder {
        self.builder(&self.http, method, path)
    }

    /// Request on the no-timeout client (SSE).
    pub fn stream_req(&self, path: &str) -> RequestBuilder {
        self.builder(&self.stream, Method::GET, path)
            .header(ACCEPT, "text/event-stream")
    }

    /// Request on the no-timeout client for a raw (binary) download.
    pub fn download_req(&self, path: &str) -> RequestBuilder {
        self.builder(&self.stream, Method::GET, path).header(ACCEPT, "*/*")
    }

    /// Send (with `--retry`); a non-2xx answer becomes [`CliError::Api`].
    pub fn send(&self, rb: RequestBuilder) -> Result<Response> {
        let resp = send_retrying(rb, self.retries, &self.api)?;
        if resp.status().is_success() {
            Ok(resp)
        } else {
            Err(self.api_error(resp))
        }
    }

    /// Send and parse the JSON body.
    pub fn send_json(&self, rb: RequestBuilder) -> Result<Reply> {
        let resp = self.send(rb)?;
        let headers = resp.headers().clone();
        let bytes = resp.bytes().map_err(|e| net_error(&self.api, &e))?;
        let body = if bytes.iter().all(u8::is_ascii_whitespace) {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|e| CliError::failed(format!("the server sent invalid JSON: {e}")))?
        };
        Ok(Reply { headers, body })
    }

    pub fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        Ok(self.send_json(self.req(Method::GET, path).query(query))?.body)
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Value> {
        Ok(self.send_json(self.req(Method::POST, path).json(body))?.body)
    }

    pub fn put(&self, path: &str, body: &Value) -> Result<Value> {
        Ok(self.send_json(self.req(Method::PUT, path).json(body))?.body)
    }

    pub fn patch(&self, path: &str, body: &Value) -> Result<Value> {
        Ok(self.send_json(self.req(Method::PATCH, path).json(body))?.body)
    }

    pub fn delete(&self, path: &str, query: &[(&str, String)]) -> Result<()> {
        self.send(self.req(Method::DELETE, path).query(query)).map(drop)
    }

    fn api_error(&self, resp: Response) -> CliError {
        let status = resp.status();
        let headers = resp.headers().clone();
        let text = resp.text().unwrap_or_default();
        let mut e = parse_api_error(status.as_u16(), status.canonical_reason(), &text);
        if let CliError::Api {
            code, retry_after: ra, ..
        } = &mut e
        {
            *ra = retry_after(&headers);
            // a non-envelope body (a proxy, the invoke listener) may still name its reason
            if code.starts_with("http_") {
                if let Some(r) = rt_reason(&headers) {
                    *code = r;
                }
            }
        } // LCOV_EXCL_LINE: parse_api_error always returns CliError::Api, so the pattern always matches
        e
    }
}

/// `{"error": {code, message, details}}`, or whatever else the server (or a proxy) sent.
pub fn parse_api_error(status: u16, reason: Option<&str>, body: &str) -> CliError {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if let Some(e) = v.get("error").filter(|e| e.is_object()) {
            let code = e.get("code").and_then(Value::as_str).unwrap_or("error").to_string();
            let message = e.get("message").and_then(Value::as_str).unwrap_or("").to_string();
            let details = e.get("details").cloned().unwrap_or_else(|| json!({}));
            return CliError::Api {
                status: Some(status),
                code,
                message,
                details: Box::new(details),
                retry_after: None,
            };
        }
    }
    let text = body.trim();
    let message = if text.is_empty() {
        reason.unwrap_or("request failed").to_string()
    } else {
        let mut t: String = text.chars().take(300).collect();
        if text.chars().count() > 300 {
            t.push('…');
        }
        t
    };
    CliError::Api {
        status: Some(status),
        code: format!("http_{status}"),
        message,
        details: Box::new(json!({})),
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn envelope_is_parsed() {
        let e = parse_api_error(
            409,
            Some("Conflict"),
            r#"{"error":{"code":"slug_taken","message":"in use","details":{"slug":"x"}}}"#,
        );
        match e {
            CliError::Api {
                status,
                code,
                message,
                details,
                retry_after,
            } => {
                assert_eq!(
                    (status, code.as_str(), message.as_str()),
                    (Some(409), "slug_taken", "in use")
                );
                assert_eq!(*details, json!({"slug": "x"}));
                assert_eq!(retry_after, None);
            }
            other => panic!("{other:?}"), // LCOV_EXCL_LINE: test-only arm; parse_api_error never returns another variant
        }
    }

    #[test]
    fn foreign_bodies_become_http_errors() {
        let e = parse_api_error(502, Some("Bad Gateway"), "<html>oops</html>");
        assert_eq!(
            e.render_text().unwrap(),
            "error: http_502: <html>oops</html> (HTTP 502)"
        );
        let e = parse_api_error(503, Some("Service Unavailable"), "");
        assert_eq!(
            e.render_text().unwrap(),
            "error: http_503: Service Unavailable (HTTP 503)"
        );
    }

    #[test]
    fn retry_after_takes_delta_seconds_only() {
        let mut h = HeaderMap::new();
        assert_eq!(retry_after(&h), None);
        h.insert(RETRY_AFTER, HeaderValue::from_static(" 3 "));
        assert_eq!(retry_after(&h), Some(3));
        h.insert(RETRY_AFTER, HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT"));
        assert_eq!(retry_after(&h), None);
    }

    #[test]
    fn a_request_past_its_timeout_is_a_network_error() {
        // the listener accepts the connection and never answers, so the request runs out of time;
        // the CLI has no timeout flag, so the mapping is checked on a real reqwest timeout here
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let http = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let e = http.get(&url).send().unwrap_err();
        assert!(matches!(net_error(&url, &e), CliError::Network(m) if m.contains("timed out")));
        drop(listener);
    }
}
