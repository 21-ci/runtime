//! The wire format of `std:services` RPC, shared by the WASM host
//! (`call-rpc`, the `rpc` export) and the operator SDK dispatcher. The Deno guest speaks the same
//! format in `js/bootstrap.js`.
//!
//! An RPC is `POST /__rt/rpc/<method>` on the in-process call path with the JSON argument as the
//! body. The callee answers `200` with `{"ok":true,"value":VALUE}` or
//! `{"ok":false,"error":{"code","message"}}`; any other status is a host failure whose
//! `x-rt-reason` is the code. The argument and the result are at most 256 KiB of JSON each way.

use http::{header, HeaderValue, Method};
use rt_types::Body;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::calls::CallError;
use crate::egress::{Request, Response};

/// Paths under it belong to the runtime: a plain `services.fetch` cannot reach them.
pub const RPC_PREFIX_ROOT: &str = "/__rt/";
/// The path prefix of an RPC; the method follows.
pub const RPC_PREFIX: &str = "/__rt/rpc/";
/// The argument and the result: at most this many bytes of JSON text each way.
pub const RPC_JSON_MAX_BYTES: usize = 262_144;
/// A method name: at most this many bytes.
pub const RPC_METHOD_MAX_BYTES: usize = 256;
/// An error message the caller receives: at most this many characters.
const RPC_MESSAGE_MAX_CHARS: usize = 1024;
/// What the envelope adds around a result at most (`{"ok":true,"value":}` and an error's fields).
const ENVELOPE_OVERHEAD: usize = 2048;

/// A failed RPC as the caller receives it: the callee's code and message, or the host's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

impl RpcError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// A refused or failed call, by its stable code.
    pub fn from_call(e: &CallError) -> Self {
        Self::new(e.code(), e.code())
    }

    /// The error as a callee may answer it: a code of `[A-Za-z][A-Za-z0-9_.-]{0,63}` (anything
    /// else is `internal_error`) and a message of at most 1024 characters.
    pub fn sanitized(self) -> Self {
        let code = if valid_code(&self.code) {
            self.code
        } else {
            "internal_error".to_string()
        };
        let message = match self.message.char_indices().nth(RPC_MESSAGE_MAX_CHARS) {
            Some((at, _)) => self.message[..at].to_string(),
            None => self.message,
        };
        Self { code, message }
    }
}

fn valid_code(code: &str) -> bool {
    let b = code.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b[0].is_ascii_alphabetic()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

fn ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c == b'$'
}

/// A method is a dot-separated path of identifiers (`createInvoice`, `invoices.create`), 256 bytes
/// at most.
pub fn valid_method(method: &str) -> bool {
    method.len() <= RPC_METHOD_MAX_BYTES
        && method.split('.').all(|part| {
            let b = part.as_bytes();
            !b.is_empty() && ident_start(b[0]) && b.iter().all(|c| ident_start(*c) || c.is_ascii_digit())
        })
}

/// The argument as the JSON text sent: empty is `null`; anything that is not one JSON value, or
/// is larger than 256 KiB, is `validation_failed`.
pub fn check_args(args_json: &str) -> Result<&str, RpcError> {
    let text = if args_json.trim().is_empty() { "null" } else { args_json };
    if text.len() > RPC_JSON_MAX_BYTES {
        return Err(RpcError::new(
            "validation_failed",
            "the argument is larger than 256 KiB",
        ));
    }
    if serde_json::from_str::<serde::de::IgnoredAny>(text).is_err() {
        return Err(RpcError::new("validation_failed", "the argument is not JSON"));
    }
    Ok(text)
}

/// The request of an RPC to `method` with `args_json`; validated first.
pub fn rpc_request(method: &str, args_json: &str) -> Result<Request, RpcError> {
    if !valid_method(method) {
        return Err(RpcError::new(
            "validation_failed",
            "the method is a dot-separated path of identifiers",
        ));
    }
    let args = check_args(args_json)?;
    http::Request::builder()
        .method(Method::POST)
        .uri(format!("{RPC_PREFIX}{method}"))
        .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Body::from(args.to_string()))
        .map_err(|_| RpcError::new("validation_failed", "invalid method"))
}

#[derive(Deserialize)]
struct Envelope<'a> {
    ok: bool,
    #[serde(borrow, default)]
    value: Option<&'a RawValue>,
    #[serde(default)]
    error: Option<EnvelopeError>,
}

#[derive(Deserialize)]
struct EnvelopeError {
    code: String,
    #[serde(default)]
    message: Option<String>,
}

/// The result of an RPC from the callee's answer: the JSON text of the value, or the error.
pub async fn rpc_result(resp: Response) -> Result<String, RpcError> {
    let status = resp.status().as_u16();
    if status != 200 {
        let reason = resp
            .headers()
            .get("x-rt-reason")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("call_failed")
            .to_string();
        return Err(RpcError::new(reason, format!("the call answered {status}")));
    }
    let body = resp
        .into_body()
        .collect((RPC_JSON_MAX_BYTES + ENVELOPE_OVERHEAD) as u64)
        .await
        .map_err(|_| RpcError::new("response_too_large", "the result is larger than 256 KiB"))?;
    parse_envelope(&body)
}

/// [`rpc_result`] on a buffered answer.
pub fn parse_envelope(body: &[u8]) -> Result<String, RpcError> {
    let invalid = || RpcError::new("invalid_response", "the callee did not answer an RPC");
    let env: Envelope<'_> = serde_json::from_slice(body).map_err(|_| invalid())?;
    match (env.ok, env.value, env.error) {
        (true, Some(v), _) => Ok(v.get().to_string()),
        (true, None, _) => Ok("null".to_string()),
        (false, _, Some(e)) => Err(RpcError::new(e.code, e.message.unwrap_or_default())),
        (false, _, None) => Err(invalid()),
    }
}

/// The method an RPC request names (percent-decoded), or `None` when `path` is not an RPC.
pub fn rpc_method_of(path: &str) -> Option<String> {
    let raw = path.strip_prefix(RPC_PREFIX)?;
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The callee's answer to a successful call: `value_json` must be one JSON value of at most
/// 256 KiB, else the answer is the `response_too_large` / `internal_error` failure.
pub fn envelope_ok(value_json: &str) -> String {
    if value_json.len() > RPC_JSON_MAX_BYTES {
        return envelope_err(&RpcError::new(
            "response_too_large",
            "the result is larger than 256 KiB",
        ));
    }
    if serde_json::from_str::<serde::de::IgnoredAny>(value_json).is_err() {
        return envelope_err(&RpcError::new("internal_error", "the result is not JSON"));
    }
    format!("{{\"ok\":true,\"value\":{value_json}}}")
}

/// The callee's answer to a failed call (sanitized: [`RpcError::sanitized`]).
pub fn envelope_err(e: &RpcError) -> String {
    let e = e.clone().sanitized();
    serde_json::json!({"ok": false, "error": {"code": e.code, "message": e.message}}).to_string()
}

/// The envelope as the `200` response a callee answers.
pub fn envelope_response(text: String) -> Response {
    let mut resp = http::Response::new(Body::from(text));
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_are_dotted_identifiers() {
        for ok in ["add", "invoices.create", "_x.$y1", "a.b.c"] {
            assert!(valid_method(ok), "{ok}");
        }
        for bad in ["", ".", "a.", ".a", "1a", "a-b", "a b", "a/b", &"a".repeat(257)] {
            assert!(!valid_method(bad), "{bad}");
        }
    }

    #[test]
    fn arguments_are_one_json_value_within_the_cap() {
        assert_eq!(check_args("").unwrap(), "null");
        assert_eq!(check_args(r#"{"a":1}"#).unwrap(), r#"{"a":1}"#);
        assert_eq!(check_args("{nope").unwrap_err().code, "validation_failed");
        assert_eq!(check_args("1 2").unwrap_err().code, "validation_failed");
        let big = format!("\"{}\"", "x".repeat(RPC_JSON_MAX_BYTES));
        assert_eq!(check_args(&big).unwrap_err().code, "validation_failed");
    }

    #[test]
    fn the_request_posts_the_method_with_the_argument() {
        let req = rpc_request("invoices.create", r#"{"n":1}"#).unwrap();
        assert_eq!(
            (req.method().as_str(), req.uri().path()),
            ("POST", "/__rt/rpc/invoices.create")
        );
        assert_eq!(rpc_request("no-pe", "1").unwrap_err().code, "validation_failed");
    }

    #[test]
    fn envelopes_round_trip() {
        assert_eq!(
            parse_envelope(envelope_ok(r#"{"a":[1]}"#).as_bytes()).unwrap(),
            r#"{"a":[1]}"#
        );
        assert_eq!(parse_envelope(br#"{"ok":true}"#).unwrap(), "null");
        let e = parse_envelope(envelope_err(&RpcError::new("out_of_stock", "none")).as_bytes()).unwrap_err();
        assert_eq!(e, RpcError::new("out_of_stock", "none"));
        assert_eq!(parse_envelope(b"not json").unwrap_err().code, "invalid_response");
        assert_eq!(parse_envelope(br#"{"ok":false}"#).unwrap_err().code, "invalid_response");
        let big = format!("\"{}\"", "x".repeat(RPC_JSON_MAX_BYTES));
        assert_eq!(
            parse_envelope(envelope_ok(&big).as_bytes()).unwrap_err().code,
            "response_too_large"
        );
        assert_eq!(
            parse_envelope(envelope_ok("{bad").as_bytes()).unwrap_err().code,
            "internal_error"
        );
    }

    #[test]
    fn a_callee_error_is_sanitized() {
        let e = RpcError::new("bad code!", "m".repeat(2000)).sanitized();
        assert_eq!((e.code.as_str(), e.message.chars().count()), ("internal_error", 1024));
        assert_eq!(RpcError::new("out_of_stock", "x").sanitized().code, "out_of_stock");
    }

    #[test]
    fn the_method_of_a_path() {
        assert_eq!(rpc_method_of("/__rt/rpc/a.b").as_deref(), Some("a.b"));
        assert_eq!(rpc_method_of("/__rt/rpc/a%2Eb").as_deref(), Some("a.b"));
        assert_eq!(rpc_method_of("/x"), None);
        assert_eq!(rpc_method_of("/__rt/rpc/%zz"), None);
    }

    #[tokio::test]
    async fn a_failed_status_is_its_reason() {
        let mut resp = http::Response::new(Body::empty());
        *resp.status_mut() = http::StatusCode::BAD_GATEWAY;
        resp.headers_mut()
            .insert("x-rt-reason", HeaderValue::from_static("guest_error"));
        assert_eq!(rpc_result(resp).await.unwrap_err().code, "guest_error");
        let ok = envelope_response(envelope_ok("7"));
        assert_eq!(rpc_result(ok).await.unwrap(), "7");
    }
}
