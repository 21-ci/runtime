//! `21ci invoke`: `ANY /s/{slug}/{path}` on the invoke listener. The control token is never sent
//! there; only `--key` / `RT_INVOKE_KEY` is.

use std::io::Write;

use reqwest::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use reqwest::Method;
use serde_json::{json, Map, Value};

use super::{resolve, App};
use crate::cli::InvokeArgs;
use crate::client::{http_client, net_error, retry_after, rt_reason, send_retrying};
use crate::error::{human_secs, is_busy, CliError, Result, EXIT_BUSY, EXIT_FAILURE};
use crate::fmt;

/// The request body and its default Content-Type (same guess as sample/scripts/invoke.sh).
fn read_data(data: &str) -> Result<(Vec<u8>, &'static str)> {
    if let Some(path) = data.strip_prefix('@') {
        let bytes = std::fs::read(path).map_err(|e| CliError::usage(format!("-d {path}: {e}")))?;
        return Ok((bytes, "application/octet-stream"));
    }
    let ct = if data.starts_with(['{', '[', '"']) {
        "application/json"
    } else {
        "text/plain"
    };
    Ok((data.as_bytes().to_vec(), ct))
}

fn parse_header(h: &str) -> Result<(HeaderName, HeaderValue)> {
    let (k, v) = h
        .split_once(':')
        .ok_or_else(|| CliError::usage(format!("invalid header `{h}`: expected \"Name: value\"")))?;
    let name = HeaderName::from_bytes(k.trim().as_bytes())
        .map_err(|_| CliError::usage(format!("invalid header name `{}`", k.trim())))?;
    let value = HeaderValue::from_str(v.trim())
        .map_err(|_| CliError::usage(format!("invalid value for header `{}`", k.trim())))?;
    Ok((name, value))
}

/// Response headers worth showing: the runtime's `x-rt-*` plus a few standard ones.
fn shown(name: &str) -> bool {
    name.starts_with("x-rt-") || matches!(name, "content-type" | "server-timing" | "retry-after")
}

pub fn run(app: &App, a: &InvokeArgs) -> Result<()> {
    // arguments first: no request for bad input
    let headers = a.headers.iter().map(|h| parse_header(h)).collect::<Result<Vec<_>>>()?;
    let data = a.data.as_deref().map(read_data).transpose()?;
    let method = match &a.method {
        Some(m) => Method::from_bytes(m.to_ascii_uppercase().as_bytes())
            .map_err(|_| CliError::usage(format!("invalid method `{m}`")))?,
        None if data.is_some() => Method::POST,
        None => Method::GET,
    };
    let mut path = a.path.clone().unwrap_or_else(|| "/".into());
    if !path.starts_with('/') {
        path.insert(0, '/');
    }

    let base = if a.svc.starts_with("http://") || a.svc.starts_with("https://") {
        a.svc.trim_end_matches('/').to_string()
    } else {
        let slug = if a.svc.starts_with("svc_") {
            let c = app.client()?;
            let mut svc = resolve(&c, &a.svc)?;
            svc.view(&c)?;
            svc.slug
                .ok_or_else(|| CliError::failed("the server returned a service without a slug"))?
        } else {
            a.svc.clone()
        };
        format!("{}/s/{slug}", app.settings.invoke)
    };
    let url = format!("{base}{path}");

    let http = http_client(Some(std::time::Duration::from_secs(300)))?;
    let mut rb = http.request(method.clone(), &url);
    if let Some(k) = a.key.as_deref().filter(|k| !k.is_empty()) {
        rb = rb.bearer_auth(k);
    }
    let user_ct = headers.iter().any(|(n, _)| n == CONTENT_TYPE);
    for (n, v) in headers {
        rb = rb.header(n, v);
    }
    if let Some((bytes, ct)) = data {
        if !user_ct {
            rb = rb.header(CONTENT_TYPE, ct);
        }
        rb = rb.body(bytes);
    }
    let resp = send_retrying(rb, app.retry, &base)?;
    let status = resp.status();
    let reason = rt_reason(resp.headers());
    let wait = retry_after(resp.headers());
    let resp_headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body = resp.bytes().map_err(|e| net_error(&base, &e))?;
    let ctype = resp_headers
        .iter()
        .find(|(k, _)| k == "content-type")
        .map(|(_, v)| v.to_ascii_lowercase())
        .unwrap_or_default();

    if a.body_only {
        let mut out = std::io::stdout().lock();
        if out.write_all(&body).and_then(|()| out.flush()).is_err() {
            std::process::exit(0);
        }
    } else if app.json {
        let mut hm = Map::new();
        for (k, v) in &resp_headers {
            hm.insert(k.clone(), json!(v));
        }
        let mut doc = Map::new();
        doc.insert("status".into(), json!(status.as_u16()));
        doc.insert("headers".into(), Value::Object(hm));
        match std::str::from_utf8(&body) {
            Ok(text) => {
                let parsed = if ctype.contains("json") {
                    serde_json::from_str::<Value>(text).ok()
                } else {
                    None
                };
                doc.insert("body".into(), parsed.unwrap_or_else(|| json!(text)));
            }
            Err(_) => {
                doc.insert("body".into(), Value::Null);
                doc.insert("binary".into(), json!(true));
                doc.insert("size".into(), json!(body.len()));
            }
        }
        fmt::print_json(&Value::Object(doc));
    } else {
        fmt::emitln(&format!("-> {method} {url}"));
        fmt::emitln(&format!("<- HTTP {}", status.as_u16()));
        let mut hs: Vec<_> = resp_headers.iter().filter(|(k, _)| shown(k)).collect();
        hs.sort();
        for (k, v) in hs {
            fmt::emitln(&format!("   {k}: {v}"));
        }
        if !body.is_empty() {
            fmt::emitln("");
            match std::str::from_utf8(&body) {
                Ok(text) => {
                    let pretty = if ctype.contains("json") {
                        serde_json::from_str::<Value>(text).ok().map(|v| fmt::pretty(&v))
                    } else {
                        None
                    };
                    let out = pretty.unwrap_or_else(|| text.to_string());
                    fmt::emit(&out);
                    if !out.ends_with('\n') {
                        fmt::emit("\n");
                    }
                }
                Err(_) => fmt::emitln(&format!(
                    "(binary body, {} bytes; use --body-only to get the raw bytes)",
                    body.len()
                )),
            }
        }
    }

    if a.fail && status.as_u16() >= 400 {
        let st = status.as_u16();
        let mut msg = format!("error: HTTP {st}");
        if let Some(r) = &reason {
            msg.push_str(&format!(" {r}"));
        }
        if let Some(w) = wait {
            msg.push_str(&format!(" (retry after {})", human_secs(w)));
        }
        fmt::note(&msg);
        return Err(CliError::Silent(if is_busy(Some(st), wait) {
            EXIT_BUSY
        } else {
            EXIT_FAILURE
        }));
    }
    Ok(())
}
