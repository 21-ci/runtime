//! `21ci openapi | docs | version | completion`.

use std::path::Path;

use clap::CommandFactory;
use reqwest::Method;
use serde_json::{json, Value};

use super::App;
use crate::cli::{Cli, CompletionArgs, OpenapiArgs, VersionArgs};
use crate::error::{CliError, Result};
use crate::fmt;

pub fn openapi(app: &App, a: &OpenapiArgs) -> Result<()> {
    let c = app.client()?;
    let text = c
        .send(c.req(Method::GET, "/v1/openapi.json"))?
        .text()
        .map_err(|e| CliError::failed(format!("cannot read the response: {e}")))?;
    let rendered = match serde_json::from_str::<Value>(&text) {
        Ok(v) => fmt::pretty(&v),
        Err(_) => text,
    };
    match &a.out {
        None => fmt::emitln(&rendered),
        Some(path) => {
            write_file(path, &format!("{rendered}\n"))?;
            let bytes = rendered.len() + 1;
            if app.json {
                fmt::print_json(&json!({"path": path.display().to_string(), "bytes": bytes}));
            } else {
                fmt::emitln(&format!("Wrote {bytes} bytes to {}", path.display()));
            }
        }
    }
    Ok(())
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    std::fs::write(path, content).map_err(|e| CliError::failed(format!("cannot write {}: {e}", path.display())))
}

pub fn docs(app: &App) -> Result<()> {
    let url = format!("{}/docs", app.settings.api);
    if app.json {
        fmt::print_json(&json!({"url": url}));
    } else {
        fmt::emitln(&url);
    }
    Ok(())
}

pub fn version(app: &App, a: &VersionArgs) -> Result<()> {
    let client = env!("CARGO_PKG_VERSION");
    if !app.json {
        fmt::emitln(&format!("21ci {client}"));
    }
    let mut server = None;
    if a.server {
        // `GET /v1/version` takes any valid token; a node that predates it answers 404, then
        // `node/status` (operator) is asked instead
        let c = app.client()?;
        let s = match c.get("/v1/version", &[]) {
            Err(CliError::Api { status: Some(404), .. }) => c.get("/v1/node/status", &[])?,
            other => other?,
        };
        server = Some(
            s.get("version")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        );
    }
    if app.json {
        let mut v = json!({"client": client});
        if let Some(s) = &server {
            v["server"] = json!(s);
        }
        fmt::print_json(&v);
    } else if let Some(s) = &server {
        fmt::emitln(&format!("server {s}"));
    }
    Ok(())
}

pub fn completion(a: &CompletionArgs) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    clap_complete::generate(a.shell, &mut Cli::command(), "21ci", &mut buf);
    fmt::emit(&String::from_utf8_lossy(&buf));
    Ok(())
}
