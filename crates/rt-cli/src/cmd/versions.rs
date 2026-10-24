//! `21ci versions`, `rollback`, `rebuild`, `build-logs`, `source`.

use std::io::BufReader;

use reqwest::header::IF_MATCH;
use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, resolve, save_download, App};
use crate::cli::{RebuildArgs, RollbackArgs, SourceArgs, VersionsArgs};
use crate::error::{CliError, Result};
use crate::fmt;
use crate::sse::{self, ReadError};

fn number(v: &Value) -> u64 {
    v.get("version").and_then(Value::as_u64).unwrap_or(0)
}

fn live_version(view: &Value) -> Option<u64> {
    view.get("live_version").and_then(Value::as_u64)
}

/// `sha256:` + the first 12 hex digits.
fn short_digest(d: &str) -> String {
    match d.strip_prefix("sha256:") {
        Some(hex) => format!("sha256:{}", &hex[..hex.len().min(12)]),
        None => d.chars().take(19).collect(),
    }
}

fn versions_of(body: &Value) -> Vec<Value> {
    let mut v = body.as_array().cloned().unwrap_or_default();
    v.sort_by_key(|x| std::cmp::Reverse(number(x)));
    v
}

pub fn list(app: &App, a: &VersionsArgs) -> Result<()> {
    let c = app.client()?;
    let mut svc = resolve(&c, &a.svc)?;
    let live = live_version(svc.view(&c)?);
    if let Some(n) = a.number {
        let v = c.get(&svc.path(&format!("/versions/{n}")), &[])?;
        if app.json {
            fmt::print_json(&v);
            return Ok(());
        }
        let mut rows = vec![
            ("Version", number(&v).to_string()),
            ("Build", cell(&v, "build_state")),
            ("Live", if live == Some(number(&v)) { "yes" } else { "no" }.to_string()),
            ("Source digest", cell(&v, "source_digest")),
            ("Lock digest", cell(&v, "lock_digest")),
            ("Created", fmt::ts_field(&v, "created_at")),
        ];
        if let Some(f) = v.get("failure").filter(|f| !f.is_null()) {
            rows.push(("Failure", fmt::compact(f)));
        }
        fmt::emit(&fmt::kv(&rows));
        return Ok(());
    }
    let body = c.get(&svc.path("/versions"), &[])?;
    if app.json {
        fmt::print_json(&body);
        return Ok(());
    }
    let items = versions_of(&body);
    if items.is_empty() {
        fmt::note("No versions yet. Upload one with: 21ci push <path>");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|v| {
            vec![
                number(v).to_string(),
                cell(v, "build_state"),
                if live == Some(number(v)) {
                    "*".into()
                } else {
                    "-".into()
                },
                short_digest(&cell(v, "source_digest")),
                fmt::ts_field(v, "created_at"),
            ]
        })
        .collect();
    fmt::emit(&fmt::table(&["VERSION", "BUILD", "LIVE", "DIGEST", "CREATED"], &rows));
    Ok(())
}

pub fn rollback(app: &App, a: &RollbackArgs) -> Result<()> {
    let c = app.client()?;
    let mut svc = resolve(&c, &a.svc)?;
    let live = live_version(svc.view(&c)?);
    let target = match a.to {
        Some(n) => u64::from(n),
        None => {
            let live = live.ok_or_else(|| CliError::failed("the service has no live version; pass --to <version>"))?;
            let body = c.get(&svc.path("/versions"), &[])?;
            versions_of(&body)
                .iter()
                .filter(|v| number(v) < live && v.get("build_state").and_then(Value::as_str) != Some("failed"))
                .map(number)
                .max()
                .ok_or_else(|| {
                    CliError::failed(format!("nothing to roll back to: no usable version older than {live}"))
                })?
        }
    };
    let body = if a.close_sockets {
        json!({"version": target, "close_sockets": true})
    } else {
        json!({"version": target})
    };
    let mut rb = c.req(Method::PUT, &svc.path("/live")).json(&body);
    if let Some(etag) = &svc.etag {
        rb = rb.header(IF_MATCH, etag);
    }
    let updated = c.send_json(rb)?.body;
    if app.json {
        fmt::print_json(&updated);
    } else {
        let was = live.map(|l| format!(" (was {l})")).unwrap_or_default();
        fmt::emitln(&format!("Service {} is now live at version {target}{was}.", svc.label));
    }
    Ok(())
}

pub fn rebuild(app: &App, a: &RebuildArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let v = c
        .send_json(c.req(Method::POST, &svc.path(&format!("/versions/{}/rebuild", a.number))))?
        .body;
    if app.json {
        fmt::print_json(&v);
    } else {
        fmt::emitln(&format!(
            "Rebuild of {} version {} started (build_state: {}).",
            svc.label,
            a.number,
            v.get("build_state").and_then(Value::as_str).unwrap_or("?")
        ));
    }
    Ok(())
}

/// `GET …/versions/{n}/build/logs`: Server-Sent Events until the build is `ready` or `failed`.
pub fn build_logs(app: &App, a: &RebuildArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let resp = c.send(c.stream_req(&svc.path(&format!("/versions/{}/build/logs", a.number))))?;
    let result = sse::read_events(BufReader::new(resp), |ev| -> std::result::Result<(), CliError> {
        if ev.event == "log" {
            match serde_json::from_str::<Value>(&ev.data) {
                Ok(r) if app.json => fmt::emitln(&fmt::compact(&r)),
                Ok(r) => fmt::emitln(&super::logs::format_line(&r)),
                Err(e) => fmt::note(&format!("skipping an unreadable log event: {e}")),
            }
        }
        Ok(())
    });
    match result {
        Ok(()) => Ok(()),
        Err(ReadError::Handler(e)) => Err(e),
        Err(ReadError::Io(e)) => Err(CliError::Network(format!("build log stream interrupted: {e}"))),
    }
}

/// `GET …/versions/{n}/source`: the uploaded bytes (a file, an archive or a JSON bundle).
pub fn source(app: &App, a: &SourceArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let resp = c.send(c.download_req(&svc.path(&format!("/versions/{}/source", a.number))))?;
    let bytes = resp.bytes().map_err(|e| crate::client::net_error(&c.api, &e))?;
    save_download(
        app,
        &bytes,
        a.out.as_deref(),
        &format!("the source of version {}", a.number),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_are_shortened() {
        assert_eq!(
            short_digest(&format!("sha256:{}", "ab".repeat(32))),
            "sha256:abababababab"
        );
        assert_eq!(short_digest("sha256:abc"), "sha256:abc");
        assert_eq!(short_digest("-"), "-");
    }

    #[test]
    fn versions_sort_newest_first() {
        let v = versions_of(&json!([{"version": 1}, {"version": 3}, {"version": 2}]));
        assert_eq!(v.iter().map(number).collect::<Vec<_>>(), vec![3, 2, 1]);
    }
}
