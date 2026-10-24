//! MCP server (decision D-B5, docs/mcp.md): a Model Context Protocol endpoint on the control
//! listener (`POST /mcp`, Streamable HTTP transport) that lets an AI client manage the tenant of
//! the API key it presents.
//!
//! - **Auth**: `Authorization: Bearer rtk_…` (a tenant key; the operator token is refused). A missing
//!   or wrong key is `401` with `WWW-Authenticate: Bearer error="invalid_token"` (no
//!   `resource_metadata`: there is no OAuth to start, the client just needs a token).
//! - **Origin**: a request that carries an `Origin` header must come from a loopback origin
//!   (`403` otherwise, DNS-rebinding guard); native clients send none.
//! - **Transport**: one JSON-RPC message per POST, answered with `application/json` (never SSE);
//!   notifications get `202`; `GET`/`DELETE` are `405` (no server-initiated stream, no sessions).
//! - **Protocol eras**: the `initialize` handshake of 2024-11-05 … 2025-11-25 (what Claude Code,
//!   Claude Desktop, Cursor and the TypeScript SDK speak today) and the stateless 2026-07-28 era
//!   (per-request `_meta`, `server/discover`, `Mcp-Method`/`Mcp-Name` headers).
//! - **Tools**: [`tools`]. Every tool is one or two calls to the control API, dispatched through
//!   the `/v1` router in-process with the caller's own credentials, so scopes, quotas, validation,
//!   ETags and audit are exactly the control API's, with nothing re-implemented here.

pub mod tools;

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use rt_sec::{AuthError, Principal};
use rt_types::Listener;
use serde_json::{json, Map, Value};

use crate::state::ApiState;

/// `serverInfo.name`.
pub const SERVER_NAME: &str = "21cloud-runtime";
/// Protocol versions of the `initialize` era, newest first.
pub const LEGACY_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
/// The stateless era: no handshake, the version and capabilities travel in every request.
pub const MODERN_VERSION: &str = "2026-07-28";
/// Everything `initialize` or `server/discover` may negotiate, newest first.
pub const SUPPORTED_VERSIONS: [&str; 5] = [MODERN_VERSION, "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
/// `params._meta` key that carries the version of a modern request.
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
/// Largest request body (a code deploy is the biggest message).
pub const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

const INSTRUCTIONS: &str = "Manage the 21 Cloud Functions Runtime tenant of the API key. Services are named by slug \
or svc_ id and live in a project (default: `default`) and one of its environments. Start with `whoami` and \
`list_services`; `deploy_code` uploads Deno TypeScript/JavaScript and makes it live. Only tools the key's scopes allow \
are listed. Secrets are write-only.";

// JSON-RPC and MCP error codes.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
/// `HeaderMismatch` (2026-07-28): a required header is missing or disagrees with the body.
const HEADER_MISMATCH: i64 = -32020;
/// `UnsupportedProtocolVersionError` (2026-07-28).
const UNSUPPORTED_VERSION: i64 = -32022;
/// Origin refused (no standard code: implementation defined).
const ORIGIN_REFUSED: i64 = -32000;

#[derive(Clone)]
struct McpState {
    api: ApiState,
    tools: Arc<tools::Tools>,
}

/// `POST /mcp`. `v1` is the control API router (`/v1/…`) the tools call in-process.
pub fn mcp_router(api: ApiState, v1: Router) -> Router {
    let state = McpState {
        api,
        tools: Arc::new(tools::Tools::new(v1)),
    };
    Router::new()
        .route("/mcp", post(mcp_post).fallback(method_not_allowed))
        .with_state(state)
}

// ---- responses ---------------------------------------------------------------------------------

fn json_response(status: StatusCode, v: &Value) -> Response {
    let mut r = Response::new(Body::from(serde_json::to_vec(v).unwrap_or_default()));
    *r.status_mut() = status;
    r.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn rpc_error_body(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut e = json!({"code": code, "message": message});
    if let Some(d) = data {
        e["data"] = d;
    }
    json!({"jsonrpc": "2.0", "id": id, "error": e})
}

/// An HTTP error status with a JSON-RPC error body (`id: null` when the request had none).
fn http_rpc_error(status: StatusCode, id: Value, code: i64, message: &str, data: Option<Value>) -> Response {
    json_response(status, &rpc_error_body(id, code, message, data))
}

fn accepted() -> Response {
    StatusCode::ACCEPTED.into_response()
}

async fn method_not_allowed() -> Response {
    let mut r = http_rpc_error(
        StatusCode::METHOD_NOT_ALLOWED,
        Value::Null,
        METHOD_NOT_FOUND,
        "this endpoint takes POST only: it has no server-initiated stream and no sessions",
        None,
    );
    r.headers_mut().insert(header::ALLOW, HeaderValue::from_static("POST"));
    r
}

/// `401` for credentials that are missing or wrong, `429`/`503` as the control API answers them.
fn auth_failure(e: AuthError) -> Response {
    let status = e.http_status();
    let mut resp = crate::error::ApiError::from(e).into_response();
    if status == 401 {
        resp.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer error=\"invalid_token\""),
        );
    }
    resp
}

// ---- origin ------------------------------------------------------------------------------------

/// `Origin: <scheme>://<host>[:port]` whose host is loopback (`localhost`, `127.0.0.0/8`, `::1`).
fn loopback_origin(origin: &str) -> bool {
    let Some((scheme, rest)) = origin.split_once("://") else {
        return false;
    };
    if scheme != "http" && scheme != "https" {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        // `[v6]` or `[v6]:port`; an unterminated bracket is not an origin (MCP-003).
        Some(v6) => match v6.split_once(']') {
            Some((h, "")) => h,
            Some((h, port))
                if port
                    .strip_prefix(':')
                    .is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit())) =>
            {
                h
            }
            _ => return false,
        },
        None => authority.rsplit_once(':').map_or(authority, |(h, port)| {
            if port.bytes().all(|b| b.is_ascii_digit()) {
                h
            } else {
                authority
            }
        }),
    };
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Servers MUST validate `Origin` (DNS rebinding): absent passes (native clients), a loopback
/// origin passes, anything else is `403`.
fn check_origin(headers: &HeaderMap) -> Result<(), Response> {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return Ok(());
    };
    if origin.to_str().is_ok_and(loopback_origin) {
        return Ok(());
    }
    Err(http_rpc_error(
        StatusCode::FORBIDDEN,
        Value::Null,
        ORIGIN_REFUSED,
        "this Origin is not allowed: browsers may only reach the MCP endpoint from a loopback origin",
        None,
    ))
}

// ---- the endpoint ------------------------------------------------------------------------------

async fn mcp_post(State(m): State<McpState>, req: Request) -> Response {
    let (mut parts, body) = req.into_parts();
    if let Err(r) = check_origin(&parts.headers) {
        return r;
    }
    crate::auth::seed_peer(&mut parts, &m.api);
    let principal = match m.api.auth.authenticate(&parts, Listener::Control) {
        Ok(p) => p,
        Err(e) => return auth_failure(e),
    };
    if !matches!(principal, Principal::Tenant { .. }) {
        // The operator token manages nodes and tenants, not one tenant's services: an agent must
        // not be handed it. Without any credential (an open debug node) the answer is still 401.
        if !parts.headers.contains_key(header::AUTHORIZATION) && !parts.headers.contains_key("x-rt-key") {
            return auth_failure(AuthError::Missing);
        }
        let mut r = crate::error::ApiError::new(
            "insufficient_scope",
            "the MCP endpoint takes a tenant key (rtk_…), not the operator token",
        )
        .into_response();
        r.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer error=\"insufficient_scope\""),
        );
        return r;
    }
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return http_rpc_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                Value::Null,
                INVALID_REQUEST,
                &format!("request body exceeds {MAX_BODY_BYTES} bytes"),
                None,
            )
        }
    };
    let msg: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            return http_rpc_error(
                StatusCode::BAD_REQUEST,
                Value::Null,
                PARSE_ERROR,
                &format!("parse error: {e}"),
                None,
            )
        }
    };
    let Value::Object(msg) = msg else {
        return http_rpc_error(
            StatusCode::BAD_REQUEST,
            Value::Null,
            INVALID_REQUEST,
            "a request body is one JSON-RPC message object (batches are not supported since 2025-06-18)",
            None,
        );
    };
    handle(&m, principal, &parts, msg).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Era {
    Legacy,
    Modern,
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim)
}

fn mismatch(id: &Value, message: String) -> Response {
    http_rpc_error(StatusCode::BAD_REQUEST, id.clone(), HEADER_MISMATCH, &message, None)
}

/// Which era a request is in, and `Err` with the HTTP answer when its version or its mirrored
/// headers are wrong.
fn classify(headers: &HeaderMap, msg: &Map<String, Value>, method: &str, id: &Value) -> Result<Era, Response> {
    let is_request = msg.contains_key("id");
    let meta_version = msg
        .get("params")
        .and_then(|p| p.get("_meta"))
        .and_then(|m| m.get(META_VERSION));
    let header_version = header_str(headers, "mcp-protocol-version");
    match (method, meta_version) {
        ("initialize", _) => Ok(Era::Legacy),
        ("server/discover", None) => Ok(Era::Modern),
        (_, Some(v)) => {
            let Some(version) = v.as_str() else {
                return Err(http_rpc_error(
                    StatusCode::BAD_REQUEST,
                    id.clone(),
                    INVALID_PARAMS,
                    &format!("params._meta[\"{META_VERSION}\"] must be a version string"),
                    None,
                ));
            };
            if version != MODERN_VERSION {
                return Err(http_rpc_error(
                    StatusCode::BAD_REQUEST,
                    id.clone(),
                    UNSUPPORTED_VERSION,
                    "Unsupported protocol version",
                    Some(json!({"supported": SUPPORTED_VERSIONS, "requested": version})),
                ));
            }
            if is_request {
                if header_version != Some(version) {
                    return Err(mismatch(
                        id,
                        format!(
                            "the MCP-Protocol-Version header must be {version}, like params._meta[\"{META_VERSION}\"]"
                        ),
                    ));
                }
                if header_str(headers, "mcp-method") != Some(method) {
                    return Err(mismatch(id, format!("the Mcp-Method header must be {method}")));
                }
                if method == "tools/call" {
                    let name = msg.get("params").and_then(|p| p.get("name")).and_then(Value::as_str);
                    if name.is_none() || header_str(headers, "mcp-name") != name {
                        return Err(mismatch(
                            id,
                            "the Mcp-Name header must be the name of the tool".to_string(),
                        ));
                    }
                }
            }
            Ok(Era::Modern)
        }
        (_, None) => {
            // A request after the `initialize` handshake: the header, when present, names the
            // negotiated legacy version.
            match header_version {
                None => Ok(Era::Legacy),
                Some(v) if LEGACY_VERSIONS.contains(&v) => Ok(Era::Legacy),
                Some(v) if v == MODERN_VERSION => Err(mismatch(
                    id,
                    format!("a {MODERN_VERSION} request carries params._meta[\"{META_VERSION}\"]"),
                )),
                Some(v) => Err(http_rpc_error(
                    StatusCode::BAD_REQUEST,
                    id.clone(),
                    UNSUPPORTED_VERSION,
                    "Unsupported protocol version",
                    Some(json!({"supported": SUPPORTED_VERSIONS, "requested": v})),
                )),
            }
        }
    }
}

fn result_body(id: Value, mut result: Value, era: Era) -> Response {
    if era == Era::Modern {
        if let Some(o) = result.as_object_mut() {
            o.insert("resultType".into(), json!("complete"));
        }
    }
    json_response(StatusCode::OK, &json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn server_info(m: &McpState) -> Value {
    json!({"name": SERVER_NAME, "title": "21 Cloud Functions Runtime", "version": m.api.node.version()})
}

async fn handle(
    m: &McpState,
    principal: Principal,
    parts: &axum::http::request::Parts,
    msg: Map<String, Value>,
) -> Response {
    // A response to a request of ours: the server never sends requests, so there is nothing to match.
    if !msg.contains_key("method") && (msg.contains_key("result") || msg.contains_key("error")) {
        return accepted();
    }
    let id_value = msg.get("id").cloned();
    let id = id_value.clone().unwrap_or(Value::Null);
    let Some(method) = msg.get("method").and_then(Value::as_str) else {
        return http_rpc_error(
            StatusCode::BAD_REQUEST,
            id,
            INVALID_REQUEST,
            "the message has no `method`",
            None,
        );
    };
    if msg.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return http_rpc_error(
            StatusCode::BAD_REQUEST,
            id,
            INVALID_REQUEST,
            "`jsonrpc` must be \"2.0\"",
            None,
        );
    }
    if matches!(id_value, Some(Value::Null))
        || matches!(id_value, Some(Value::Array(_) | Value::Object(_) | Value::Bool(_)))
    {
        return http_rpc_error(
            StatusCode::BAD_REQUEST,
            Value::Null,
            INVALID_REQUEST,
            "`id` must be a string or a number",
            None,
        );
    }
    let era = match classify(&parts.headers, &msg, method, &id) {
        Ok(e) => e,
        Err(r) => return r,
    };
    if id_value.is_none() {
        // A notification (`notifications/initialized`, `notifications/cancelled`, …): accepted, nothing to answer.
        return accepted();
    }
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    match method {
        "initialize" => {
            let asked = params.get("protocolVersion").and_then(Value::as_str).unwrap_or("");
            // The version the client asked for when we speak it, else our newest handshake version.
            let negotiated = LEGACY_VERSIONS
                .iter()
                .copied()
                .find(|v| *v == asked)
                .unwrap_or(LEGACY_VERSIONS[0]);
            let result = json!({
                "protocolVersion": negotiated,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": server_info(m),
                "instructions": INSTRUCTIONS,
            });
            result_body(id, result, Era::Legacy)
        }
        "ping" if era == Era::Legacy => result_body(id, json!({}), era),
        "server/discover" => {
            let result = json!({
                "supportedVersions": SUPPORTED_VERSIONS,
                "capabilities": {"tools": {}},
                "_meta": {"io.modelcontextprotocol/serverInfo": server_info(m)},
                "instructions": INSTRUCTIONS,
                "ttlMs": 60_000,
                "cacheScope": "public",
            });
            result_body(id, result, Era::Modern)
        }
        "tools/list" => result_body(id, json!({"tools": tools::list(&principal)}), era),
        "tools/call" => {
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return http_rpc_error(
                    StatusCode::OK,
                    id,
                    INVALID_PARAMS,
                    "`params.name` must name a tool",
                    None,
                );
            };
            let args = match params.get("arguments") {
                None | Some(Value::Null) => Map::new(),
                Some(Value::Object(o)) => o.clone(),
                Some(_) => {
                    return json_response(
                        StatusCode::OK,
                        &rpc_error_body(id, INVALID_PARAMS, "`params.arguments` must be an object", None),
                    )
                }
            };
            let caller = tools::Caller::new(principal, parts);
            match m.tools.call(&caller, name, args).await {
                Some(result) => result_body(id, result, era),
                None => json_response(
                    StatusCode::OK,
                    &rpc_error_body(id, INVALID_PARAMS, &format!("Unknown tool: {name}"), None),
                ),
            }
        }
        other => {
            let status = if era == Era::Modern {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            };
            json_response(
                status,
                &rpc_error_body(id, METHOD_NOT_FOUND, &format!("method not found: {other}"), None),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_003_origin_edge_cases_are_refused_unless_truly_loopback() {
        for bad in [
            "http://localhost.:80",
            "http://127.0.0.1:80@evil.example",
            "HTTP://LOCALHOST",
            "http://[::ffff:127.0.0.1]",
            "http://0.0.0.0",
            "http://2130706433",
            "http://localhost:80a",
            "http://[::1",
        ] {
            assert!(!loopback_origin(bad), "{bad}");
        }
        assert!(
            loopback_origin("http://localhost/with/path"),
            "a path does not change the host"
        );
        assert!(
            loopback_origin("http://LocalHost:3000"),
            "the host compares case-insensitively"
        );
    }

    #[test]
    fn only_loopback_origins_pass() {
        for ok in [
            "http://localhost:6274",
            "https://localhost",
            "http://127.0.0.1:3000",
            "http://127.1.2.3",
            "http://[::1]:8080",
        ] {
            assert!(loopback_origin(ok), "{ok}");
        }
        for bad in [
            "https://evil.example",
            "http://localhost.evil.example",
            "http://127.0.0.1.evil.example",
            "http://evil.example:80",
            "null",
            "",
            "file://localhost",
            "http://10.0.0.1",
            "http://localhost@evil.example",
            "http://[2001:db8::1]",
        ] {
            assert!(!loopback_origin(bad), "{bad}");
        }
    }

    #[test]
    fn the_supported_versions_list_the_modern_era_first() {
        assert_eq!(SUPPORTED_VERSIONS[0], MODERN_VERSION);
        assert_eq!(&SUPPORTED_VERSIONS[1..], &LEGACY_VERSIONS);
    }
}
