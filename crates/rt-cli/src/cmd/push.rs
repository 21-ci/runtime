//! `21ci push`: find or create the service, upload a version, optionally wait for the build.
//! Same flow as `sample/scripts/deploy.sh`.

use std::time::{Duration, Instant};

use reqwest::header::CONTENT_TYPE;
use reqwest::Method;
use serde_json::{json, Value};

use super::{lookup_slug, App};
use crate::artifact::{self, valid_slug};
use crate::cli::{CompileArg, PushArgs};
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// Progress line (`Label:   text`), table mode only.
fn step(app: &App, label: &str, text: &str) {
    if !app.json {
        fmt::emitln(&format!("{:<10}{text}", format!("{label}:")));
    }
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("?")
}

pub fn run(app: &App, a: &PushArgs) -> Result<()> {
    // everything that can be wrong with the arguments is checked before the first request
    let art = artifact::build(&a.path)?;
    if let Some(r) = a.runtime {
        let want = super::service::runtime_str(r);
        if want != art.runtime.as_str() {
            return Err(CliError::usage(format!(
                "--runtime {want} does not match the artifact ({} -> {})",
                art.content_type,
                art.runtime.as_str()
            )));
        }
    }
    if a.wait && a.compile == Some(CompileArg::Lazy) {
        return Err(CliError::usage("--wait needs an eager build: drop --compile lazy"));
    }
    let slug = a.service.clone().unwrap_or_else(|| art.default_slug.clone());
    if !valid_slug(&slug) {
        return Err(CliError::usage(format!(
            "invalid service slug \"{slug}\" (must match ^[a-z][a-z0-9-]{{1,62}}$); pass one with --service <slug>"
        )));
    }
    if let Some(e) = &a.environment {
        if !super::project::valid_environment(e) {
            return Err(CliError::usage(format!(
                "invalid environment \"{e}\": must match ^[a-z][a-z0-9-]{{0,31}}$"
            )));
        }
    }
    let compile = match (a.compile, a.wait) {
        (Some(c), _) => Some(c.as_str()),
        (None, true) => Some("eager"),
        (None, false) => None,
    };

    let c = app.client()?;

    // 1. find or create the service
    let place = Placement {
        project: a.project.as_deref(),
        environment: a.environment.as_deref(),
    };
    let (id, created) = find_or_create(app, &c, &slug, art.runtime.as_str(), a.create, &place)?;

    // 2. upload (+ activate)
    let activate = !a.no_activate;
    let mut query: Vec<(&str, String)> = Vec::new();
    if activate {
        query.push(("activate", "1".into()));
        if a.close_sockets {
            query.push(("close_sockets", "1".into()));
        }
    }
    if let Some(m) = compile {
        query.push(("compile", m.into()));
    }
    step(
        app,
        "Upload",
        &format!("{}, {} bytes", art.content_type, art.bytes.len()),
    );
    let reply = c.send_json(
        c.req(Method::POST, &format!("/v1/services/{id}/versions"))
            .query(&query)
            .header(CONTENT_TYPE, art.content_type)
            .body(art.bytes),
    )?;
    let mut version = reply.body;
    let n = version.get("version").and_then(Value::as_u64).unwrap_or(0);
    let digest = str_of(&version, "source_digest").to_string();
    step(
        app,
        "Version",
        &format!("{n} (build_state: {}, {digest})", str_of(&version, "build_state")),
    );

    // 3. wait for the build
    if a.wait {
        let state = str_of(&version, "build_state").to_string();
        if state != "ready" && state != "failed" {
            version = wait_for_build(&c, &id, n, a.wait_timeout, a.wait_interval)?;
        }
        let state = str_of(&version, "build_state").to_string();
        step(app, "Building", &state);
        if state == "failed" {
            let why = version
                .get("failure")
                .filter(|f| !f.is_null())
                .map(fmt::compact)
                .unwrap_or_else(|| "no details".into());
            return Err(CliError::failed(format!("version {n} failed to build: {why}")));
        }
    }

    // 4. report
    let invoke_url = format!("{}/s/{slug}/", app.settings.invoke);
    if activate {
        step(app, "Live", "yes");
        step(app, "Invoke", &invoke_url);
    } else {
        step(
            app,
            "Live",
            &format!("no (not activated; make it live with: 21ci rollback {slug} --to {n})"),
        );
    }
    if app.json {
        fmt::print_json(&json!({
            "service": {"id": id, "slug": slug, "runtime": art.runtime.as_str(), "created": created},
            "version": version,
            "activated": activate,
            "invoke_url": if activate { json!(invoke_url) } else { Value::Null },
        }));
    }
    Ok(())
}

/// Where a created service goes (`--project`, `--env`).
struct Placement<'a> {
    project: Option<&'a str>,
    environment: Option<&'a str>,
}

fn find_or_create(
    app: &App,
    c: &Client,
    slug: &str,
    runtime: &str,
    create: bool,
    place: &Placement<'_>,
) -> Result<(String, bool)> {
    if let Some(v) = lookup_slug(c, slug)? {
        let id = str_of(&v, "id").to_string();
        step(app, "Service", &format!("{id} (slug: {slug}, existing)"));
        return Ok((id, false));
    }
    if !create {
        return Err(CliError::not_found(format!(
            "service \"{slug}\" not found (use --create to create it)"
        )));
    }
    let mut body = json!({"slug": slug, "runtime": runtime, "name": slug});
    if let Some(p) = place.project {
        body["project"] = json!(p);
    }
    if let Some(e) = place.environment {
        body["environment"] = json!(e);
    }
    match c.post("/v1/services", &body) {
        Ok(v) => {
            let id = str_of(&v, "id").to_string();
            step(
                app,
                "Service",
                &format!("{id} (slug: {slug}, runtime: {runtime}, created)"),
            );
            Ok((id, true))
        }
        // somebody created it between our lookup and the POST
        Err(CliError::Api { code, .. }) if code == "slug_taken" => match lookup_slug(c, slug)? {
            Some(v) => {
                let id = str_of(&v, "id").to_string();
                step(app, "Service", &format!("{id} (slug: {slug}, existing)"));
                Ok((id, false))
            }
            None => Err(CliError::Api {
                status: Some(409),
                code: "slug_taken".into(),
                message: format!("slug \"{slug}\" is taken by a service this key cannot see"),
                details: Box::new(json!({})),
                retry_after: None,
            }),
        },
        Err(e) => Err(e),
    }
}

/// Poll `GET …/versions/{n}` until the build is `ready` or `failed`.
fn wait_for_build(c: &Client, id: &str, n: u64, timeout: Duration, interval: Duration) -> Result<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let v = c.get(&format!("/v1/services/{id}/versions/{n}"), &[])?;
        let state = str_of(&v, "build_state").to_string();
        if state == "ready" || state == "failed" {
            return Ok(v);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(CliError::failed(format!(
                "version {n} still {state} after {} (follow it with: 21ci build-logs {id} {n})",
                humantime::format_duration(timeout)
            )));
        }
        std::thread::sleep(interval.min(deadline - now));
    }
}
