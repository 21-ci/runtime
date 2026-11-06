//! `21ci app export | import | fork`: move a service as a bundle (scope `data:admin` and, to import,
//! `services:write` and `deploy`). The bundle holds env values: it is written mode 0600.

use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, resolve, App};
use crate::cli::AppCmd;
use crate::client::{net_error, Client};
use crate::config::{ConfigFile, Settings};
use crate::error::{CliError, Result};
use crate::fmt;

const POLL: Duration = Duration::from_millis(500);

fn bundle_id(v: &Value) -> Result<String> {
    let id = cell(v, "id");
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(CliError::failed("the node answered with a bad bundle id"));
    }
    Ok(id)
}

/// Follow the export until the node streams the bundle; it lands in `FILE.part` first.
fn download(c: &Client, path: &str, out: &Path) -> Result<u64> {
    let mut name = out.as_os_str().to_os_string();
    name.push(".part");
    let part = PathBuf::from(name);
    loop {
        let mut resp = c.send(c.download_req(path))?;
        let is_bundle = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|t| t.starts_with("application/zstd"));
        if !is_bundle {
            let text = resp.text().map_err(|e| net_error(&c.api, &e))?;
            let state: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            match state.get("state").and_then(Value::as_str) {
                Some("running") => {
                    std::thread::sleep(POLL);
                    continue;
                }
                Some("failed") => {
                    let why = state.get("failure").map_or_else(|| "-".to_string(), fmt::compact);
                    return Err(CliError::failed(format!("the export failed: {why}")));
                }
                _ => return Err(CliError::failed("the node answered with neither a bundle nor a state")),
            }
        }
        let fail = |e: std::io::Error| CliError::failed(format!("cannot write {}: {e}", part.display()));
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&part)
            .map_err(fail)?;
        let bytes = match resp.copy_to(&mut f) {
            Ok(n) => n,
            Err(e) => {
                let _ = std::fs::remove_file(&part);
                return Err(net_error(&c.api, &e));
            }
        };
        f.sync_all().map_err(fail)?;
        std::fs::rename(&part, out).map_err(fail)?;
        return Ok(bytes);
    }
}

fn import_query(slug: &str, project: Option<&str>, env: Option<&str>, activate: bool, from: Option<&str>) -> String {
    let mut pairs = vec![("slug", slug)];
    pairs.extend(project.map(|p| ("project", p)));
    pairs.extend(env.map(|e| ("environment", e)));
    if activate {
        pairs.push(("activate", "1"));
    }
    pairs.extend(from.map(|f| ("from", f)));
    reqwest::Url::parse_with_params("http://x/", &pairs)
        .ok()
        .and_then(|u| u.query().map(str::to_string))
        .unwrap_or_default()
}

fn show_import(app: &App, r: &Value, slug: &str) {
    if app.json {
        fmt::print_json(r);
        return;
    }
    fmt::emitln(&format!("Imported {slug} ({}).", cell(r, "service_id")));
    let missing: Vec<&str> = r
        .get("missing_secrets")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if !missing.is_empty() {
        fmt::emitln(&format!(
            "Secrets were not carried. Enter them again: {}",
            missing.join(", ")
        ));
    }
}

/// Export through a private directory, import on the other node, and always remove the bundle: it
/// holds env values.
fn move_service(app: &App, c: &Client, svc_id: &str, target: &Settings, query: &str, slug: &str) -> Result<()> {
    let dir = std::env::temp_dir().join(format!("21ci-move-{}-{svc_id}", std::process::id()));
    let fail = |e: std::io::Error| CliError::failed(format!("cannot write {}: {e}", dir.display()));
    std::fs::DirBuilder::new().mode(0o700).create(&dir).map_err(fail)?;
    let moved = (|| {
        let file = dir.join("bundle.tar.zst");
        let base = format!("/v1/services/{svc_id}/bundle");
        let started = c.send_json(c.req(Method::POST, &base))?.body;
        download(c, &format!("{base}/{}", bundle_id(&started)?), &file)?;
        let to = Client::new(target, app.retry)?;
        let f = std::fs::File::open(&file).map_err(fail)?;
        let rb = to
            .upload_req(&format!("/v1/services/import?{query}"))
            .header(reqwest::header::CONTENT_TYPE, "application/zstd")
            .body(f);
        show_import(app, &to.send_json(rb)?.body, slug);
        Ok(())
    })();
    let _ = std::fs::remove_dir_all(&dir);
    moved
}

pub fn run(app: &App, cmd: &AppCmd, config: &ConfigFile) -> Result<()> {
    let c = app.client()?;
    match cmd {
        AppCmd::Move {
            service,
            to,
            slug,
            project,
            env,
            activate,
        } => {
            // The named context alone: `RT_API` and friends describe the source.
            let target = config.resolve(Some(to), None, &|_| None)?;
            let svc = resolve(&c, service)?;
            let slug = match slug {
                Some(s) => s.clone(),
                None => cell(&c.get(&svc.path(""), &[])?, "slug"),
            };
            let q = import_query(&slug, project.as_deref(), env.as_deref(), *activate, None);
            move_service(app, &c, &svc.id, &target, &q, &slug)?;
        }
        AppCmd::Export { service, out } => {
            let svc = resolve(&c, service)?;
            let base = format!("/v1/services/{}/bundle", svc.id);
            let started = c.send_json(c.req(Method::POST, &base))?.body;
            let bytes = download(&c, &format!("{base}/{}", bundle_id(&started)?), out)?;
            if app.json {
                fmt::print_json(&json!({"path": out.display().to_string(), "bytes": bytes}));
            } else {
                fmt::emitln(&format!("Wrote {} ({bytes} bytes).", out.display()));
                fmt::emitln("The bundle holds env values and data: keep it private.");
            }
        }
        AppCmd::Import {
            file,
            slug,
            project,
            env,
            activate,
        } => {
            let q = import_query(slug, project.as_deref(), env.as_deref(), *activate, None);
            let f = std::fs::File::open(file)
                .map_err(|e| CliError::failed(format!("cannot read {}: {e}", file.display())))?;
            let rb = c
                .upload_req(&format!("/v1/services/import?{q}"))
                .header(reqwest::header::CONTENT_TYPE, "application/zstd")
                .body(f);
            show_import(app, &c.send_json(rb)?.body, slug);
        }
        AppCmd::Fork {
            service,
            slug,
            project,
            env,
            activate,
        } => {
            let svc = resolve(&c, service)?;
            let q = import_query(slug, project.as_deref(), env.as_deref(), *activate, Some(&svc.id));
            let rb = c.upload_req(&format!("/v1/services/import?{q}"));
            show_import(app, &c.send_json(rb)?.body, slug);
        }
    }
    Ok(())
}
