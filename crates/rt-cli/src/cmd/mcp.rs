//! `21ci mcp`: a stdio bridge to the MCP endpoint of the control API (decision D-B5, docs/mcp.md).
//!
//! MCP clients that cannot speak Streamable HTTP with a custom `Authorization` header (Claude
//! Desktop, many editors) start `21ci mcp` as a stdio server instead. The bridge forwards each
//! JSON-RPC message, one per line, as a `POST <api>/mcp` with the tenant key from the current
//! context as a Bearer token, and prints each answer as one line. Nothing else ever goes to stdout
//! (clients parse it); diagnostics go to stderr. The key comes from `RT_KEY` or the config file,
//! never from a command-line argument.
//!
//! Messages are handled in order. A request the server cannot answer (no connection, `401`, an
//! error status without a JSON-RPC body) is answered here with a JSON-RPC error carrying the
//! request's `id`, so the client never waits for a reply that will not come.

use std::io::{BufRead, BufReader, Read};
use std::time::Duration;

use reqwest::blocking::{Client as Http, Response};
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::{json, Map, Value};

use super::App;
use crate::client::{http_client, net_error, parse_api_error};
use crate::error::{CliError, Result};
use crate::fmt;
use crate::sse;

/// JSON-RPC: the message could not be parsed.
const PARSE_ERROR: i64 = -32700;
/// JSON-RPC: not a request, notification or response.
const INVALID_REQUEST: i64 = -32600;
/// A failure of the bridge or of the HTTP exchange (implementation-defined range).
const BRIDGE_ERROR: i64 = -32000;
/// `params._meta` key a 2026-07-28 request carries its protocol version under.
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";

struct Bridge {
    http: Http,
    url: String,
    token: String,
    /// The version `initialize` negotiated; sent as `MCP-Protocol-Version` on every later request.
    negotiated: Option<String>,
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn emit_value(v: &Value) {
    // `to_string` is compact: a message must not contain a newline.
    fmt::emitln(&v.to_string());
}

impl Bridge {
    /// Handle one input line.
    fn line(&mut self, line: &str) {
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => return emit_value(&rpc_error(Value::Null, PARSE_ERROR, &format!("parse error: {e}"))),
        };
        let Value::Object(obj) = &msg else {
            return emit_value(&rpc_error(
                Value::Null,
                INVALID_REQUEST,
                "expected one JSON-RPC message object per line (batches are not supported)",
            ));
        };
        let is_response = !obj.contains_key("method") && (obj.contains_key("result") || obj.contains_key("error"));
        if !obj.contains_key("method") && !is_response {
            return emit_value(&rpc_error(
                obj.get("id").cloned().unwrap_or(Value::Null),
                INVALID_REQUEST,
                "the message has no `method`",
            ));
        }
        // A request has an `id` and a `method`; everything else is answered with `202` and no body.
        let request_id = obj.get("method").and(obj.get("id")).cloned();
        let method = obj.get("method").and_then(Value::as_str).map(str::to_string);
        match self.post(&msg, obj) {
            Err(e) => self.fail(request_id, &e.to_string()),
            Ok(resp) => self.answer(resp, request_id, method.as_deref()),
        }
    }

    fn post(&self, msg: &Value, obj: &Map<String, Value>) -> std::result::Result<Response, CliError> {
        let mut rb = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .body(msg.to_string());
        let method = obj.get("method").and_then(Value::as_str);
        let meta_version = obj
            .get("params")
            .and_then(|p| p.get("_meta"))
            .and_then(|m| m.get(META_VERSION))
            .and_then(Value::as_str);
        if let (Some(v), Some(m)) = (meta_version, method) {
            // 2026-07-28: the transport mirrors the version, method and tool name in headers.
            rb = rb.header("mcp-protocol-version", v).header("mcp-method", m);
            if m == "tools/call" {
                if let Some(n) = obj.get("params").and_then(|p| p.get("name")).and_then(Value::as_str) {
                    rb = rb.header("mcp-name", n);
                }
            }
        } else if method != Some("initialize") {
            if let Some(v) = &self.negotiated {
                rb = rb.header("mcp-protocol-version", v);
            }
        }
        rb.send().map_err(|e| net_error(&self.url, &e))
    }

    /// Print what the server answered.
    fn answer(&mut self, resp: Response, id: Option<Value>, method: Option<&str>) {
        let status = resp.status();
        if status.as_u16() == 202 {
            return;
        }
        let is_stream = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|c| c.starts_with("text/event-stream"));
        if status.is_success() && is_stream {
            let r = sse::read_events(BufReader::new(resp), |ev| -> std::result::Result<(), ()> {
                match serde_json::from_str::<Value>(&ev.data) {
                    Ok(v) => emit_value(&v),
                    Err(_) => eprintln!("21ci mcp: ignored a non-JSON event from the server"),
                }
                Ok(())
            });
            if let Err(sse::ReadError::Io(e)) = r {
                self.fail(id, &format!("the event stream broke: {e}"));
            }
            return;
        }
        let mut text = String::new();
        if let Err(e) = BufReader::new(resp.take(8 * 1024 * 1024)).read_to_string(&mut text) {
            return self.fail(id, &format!("cannot read the answer: {e}"));
        }
        let body: Option<Value> = serde_json::from_str(&text).ok();
        if status.is_success() {
            match body {
                Some(v) => {
                    if method == Some("initialize") {
                        self.negotiated = v
                            .pointer("/result/protocolVersion")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                    }
                    emit_value(&v);
                }
                None if id.is_some() => self.fail(id, "the server sent a body that is not JSON"),
                None => {}
            }
            return;
        }
        // An error status: the server's own JSON-RPC error is the answer when it sent one.
        if let Some(v) = body
            .as_ref()
            .filter(|v| v.get("jsonrpc").is_some() && v.get("error").is_some())
        {
            if id.is_some() {
                emit_value(v);
            } else {
                eprintln!(
                    "21ci mcp: HTTP {}: {}",
                    status.as_u16(),
                    v["error"]["message"].as_str().unwrap_or("")
                );
            }
            return;
        }
        let e = parse_api_error(status.as_u16(), status.canonical_reason(), &text);
        let message = match &e {
            CliError::Api { code, message, .. } => format!("HTTP {}: {code}: {message}", status.as_u16()),
            other => other.to_string(), // LCOV_EXCL_LINE: parse_api_error always returns CliError::Api
        };
        self.fail(id, &message);
    }

    /// Answer a request with an error (a notification only gets a line on stderr).
    fn fail(&self, id: Option<Value>, message: &str) {
        match id {
            Some(id) => emit_value(&rpc_error(id, BRIDGE_ERROR, message)),
            None => eprintln!("21ci mcp: {message}"),
        }
    }
}

pub fn run(app: &App) -> Result<()> {
    let token = app.settings.token.clone().ok_or_else(|| {
        CliError::usage(
            "21ci mcp needs a tenant key (rtk_…): set RT_KEY, or store it with `21ci config set-context <name> --token …`",
        )
    })?;
    let mut bridge = Bridge {
        // No overall timeout: a tool call may run as long as the client allows.
        http: http_client(None::<Duration>)?,
        url: format!("{}/mcp", app.settings.api.trim_end_matches('/')),
        token,
        negotiated: None,
    };
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match input.read_until(b'\n', &mut buf) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                let line = String::from_utf8_lossy(&buf);
                let line = line.trim();
                if !line.is_empty() {
                    bridge.line(line);
                }
            }
            Err(e) => return Err(CliError::failed(format!("cannot read stdin: {e}"))),
        }
    }
}
