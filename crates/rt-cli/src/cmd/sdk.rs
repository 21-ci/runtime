//! `21ci types <service>` (`GET /v1/services/{s}/types`): the TypeScript declarations of a
//! service's RPC surface. `21ci sdk …` (`/v1/sdks`, `/v1/sdk-grants`): operator SDKs, published,
//! promoted and granted by the operator; a tenant key lists the SDKs its grants allow and
//! downloads their contracts.

use std::path::Path;

use base64::Engine as _;
use serde_json::{json, Value};

use super::{cell, resolve, App};
use crate::cli::{SdkCmd, SdkGrantsCmd, SdkLsArgs, SdkPublishArgs, SdkTypesArgs, TypesArgs};
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// GET `path` as text.
fn get_text(c: &Client, path: &str) -> Result<String> {
    let resp = c.send(c.download_req(path))?;
    resp.text().map_err(|e| crate::client::net_error(&c.api, &e))
}

/// Declarations to `<dir>/<file>` (the directory is created), or to stdout.
fn save_types(app: &App, text: &str, dir: Option<&Path>, file: &str) -> Result<()> {
    let Some(dir) = dir else {
        print!("{text}");
        return Ok(());
    };
    std::fs::create_dir_all(dir).map_err(|e| CliError::failed(format!("cannot create {}: {e}", dir.display())))?;
    let path = dir.join(file);
    std::fs::write(&path, text).map_err(|e| CliError::failed(format!("cannot write {}: {e}", path.display())))?;
    if app.json {
        fmt::print_json(&serde_json::json!({"path": path.display().to_string(), "bytes": text.len()}));
    } else {
        fmt::emitln(&format!("Wrote {} ({} bytes).", path.display(), text.len()));
    }
    Ok(())
}

/// `21ci types <service> [VERSION] [-d DIR]`.
pub fn types(app: &App, a: &TypesArgs) -> Result<()> {
    let c = app.client()?;
    let mut svc = resolve(&c, &a.svc)?;
    let mut path = svc.path("/types");
    if let Some(v) = a.number {
        path.push_str(&format!("?version={v}"));
    }
    let text = get_text(&c, &path)?;
    let slug = match &svc.slug {
        Some(s) => s.clone(),
        None if a.out.is_some() => {
            let view = svc.view(&c)?;
            view.get("slug").and_then(|v| v.as_str()).unwrap_or(&a.svc).to_string()
        }
        None => a.svc.clone(),
    };
    save_types(app, &text, a.out.as_deref(), &format!("{slug}.d.ts"))
}

pub fn run(app: &App, cmd: &SdkCmd) -> Result<()> {
    match cmd {
        SdkCmd::Publish(a) => publish(app, a),
        SdkCmd::Ls(a) => ls(app, a),
        SdkCmd::Releases { name } => releases(app, name),
        SdkCmd::Promote { name, release, channel } => promote(app, name, release, channel),
        SdkCmd::Rollback { name, channel } => rollback(app, name, channel),
        SdkCmd::Rm { name } => {
            app.client()?.delete(&format!("/v1/sdks/{name}"), &[])?;
            fmt::emitln(&format!(
                "Removed the SDK {name}: calls answer sdk_unavailable until it is published again."
            ));
            Ok(())
        }
        SdkCmd::Grants { cmd } => grants(app, cmd),
        SdkCmd::Types(a) => sdk_types(app, a),
    }
}

fn list(v: &Value, key: &str) -> String {
    match v.get(key).and_then(Value::as_array) {
        Some(a) if !a.is_empty() => a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","),
        _ => "-".into(),
    }
}

/// `21ci sdk publish NAME VERSION PATH`.
fn publish(app: &App, a: &SdkPublishArgs) -> Result<()> {
    let art = crate::artifact::build(&a.path)?;
    let mut body = json!({
        "name": a.name,
        "version": a.release,
        "runtime": art.runtime.as_str(),
        "content_type": art.content_type,
        "source": base64::engine::general_purpose::STANDARD.encode(&art.bytes),
    });
    if let Some(c) = &a.channel {
        body["channel"] = json!(c);
    }
    if let Some(d) = &a.description {
        body["description"] = json!(d);
    }
    if let Some(path) = &a.contract {
        let text = std::fs::read_to_string(path)
            .map_err(|e| CliError::usage(format!("cannot read {}: {e}", path.display())))?;
        body["contract"] = json!(text);
    }
    if !a.members.is_empty() {
        body["members"] = json!(a.members);
    }
    let r = app.client()?.post("/v1/sdks", &body)?;
    if app.json {
        fmt::print_json(&r);
        return Ok(());
    }
    fmt::emitln(&format!(
        "Published {} {} (service version {}).",
        cell(&r, "sdk"),
        cell(&r, "version"),
        cell(&r, "version_no")
    ));
    fmt::emit(&fmt::kv(&[
        ("Members", list(&r, "members")),
        ("Channels", list(&r, "channels")),
    ]));
    Ok(())
}

/// Operator: `GET /v1/sdks`; a tenant key: `GET /v1/sdks/effective`.
fn ls(app: &App, a: &SdkLsArgs) -> Result<()> {
    let c = app.client()?;
    let me = c.get("/v1/me", &[])?;
    if me.get("principal").and_then(Value::as_str) == Some("operator") && a.project.is_none() && a.environment.is_none()
    {
        let v = c.get("/v1/sdks", &[])?;
        if app.json {
            fmt::print_json(&v);
            return Ok(());
        }
        let rows: Vec<Vec<String>> = v
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|s| {
                let channels = s
                    .get("channels")
                    .and_then(Value::as_object)
                    .map(|m| {
                        m.iter()
                            .map(|(k, c)| format!("{k}={}", cell(c, "version")))
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .filter(|x| !x.is_empty())
                    .unwrap_or_else(|| "-".into());
                vec![
                    cell(s, "name"),
                    cell(s, "state"),
                    channels,
                    cell(s, "releases"),
                    cell(s, "latest"),
                    cell(s, "service_id"),
                ]
            })
            .collect();
        if rows.is_empty() {
            fmt::note("No SDKs published. Publish one with: 21ci sdk publish <path> --name <name> --version 1.0.0");
            return Ok(());
        }
        fmt::emit(&fmt::table(
            &["NAME", "STATE", "CHANNELS", "RELEASES", "LATEST", "SERVICE"],
            &rows,
        ));
        return Ok(());
    }
    let mut q = Vec::new();
    if let Some(p) = &a.project {
        q.push(("project", p.clone()));
    }
    if let Some(e) = &a.environment {
        q.push(("environment", e.clone()));
    }
    let v = c.get("/v1/sdks/effective", &q)?;
    if app.json {
        fmt::print_json(&v);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = v
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|s| {
            vec![
                cell(s, "name"),
                cell(s, "version"),
                cell(s, "channel"),
                list(s, "version_reqs"),
                list(s, "members"),
                cell(s, "reason"),
            ]
        })
        .collect();
    if rows.is_empty() {
        fmt::note("No SDK is granted to this project and environment.");
        return Ok(());
    }
    fmt::emit(&fmt::table(
        &["NAME", "VERSION", "CHANNEL", "REQUIRES", "MEMBERS", "REASON"],
        &rows,
    ));
    Ok(())
}

fn releases(app: &App, name: &str) -> Result<()> {
    let v = app.client()?.get(&format!("/v1/sdks/{name}/releases"), &[])?;
    if app.json {
        fmt::print_json(&v);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = v
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|r| {
            vec![
                cell(r, "version"),
                list(r, "channels"),
                cell(r, "version_no"),
                list(r, "members"),
                fmt::str_field(r, "created_at"),
            ]
        })
        .collect();
    fmt::emit(&fmt::table(
        &["VERSION", "CHANNELS", "SERVICE_VERSION", "MEMBERS", "CREATED"],
        &rows,
    ));
    Ok(())
}

fn print_channel(app: &App, name: &str, v: &Value) {
    if app.json {
        fmt::print_json(v);
    } else {
        fmt::emitln(&format!(
            "{name}: {} -> {} (was {}). Calls bound to it use the new release now; no tenant service is redeployed.",
            cell(v, "channel"),
            cell(v, "version"),
            cell(v, "previous")
        ));
    }
}

fn promote(app: &App, name: &str, version: &str, channel: &str) -> Result<()> {
    let v = app.client()?.put(
        &format!("/v1/sdks/{name}/channels/{channel}"),
        &json!({"version": version}),
    )?;
    print_channel(app, name, &v);
    Ok(())
}

fn rollback(app: &App, name: &str, channel: &str) -> Result<()> {
    let c = app.client()?;
    let sdks = c.get("/v1/sdks", &[])?;
    let sdk = sdks
        .as_array()
        .and_then(|a| a.iter().find(|s| s.get("name").and_then(Value::as_str) == Some(name)))
        .ok_or_else(|| CliError::not_found(format!("no SDK \"{name}\"")))?;
    let previous = sdk
        .pointer(&format!("/channels/{channel}/previous"))
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::failed(format!("the channel {channel} of {name} has no previous release")))?;
    let v = c.put(
        &format!("/v1/sdks/{name}/channels/{channel}"),
        &json!({"version": previous}),
    )?;
    print_channel(app, name, &v);
    Ok(())
}

fn grants(app: &App, cmd: &SdkGrantsCmd) -> Result<()> {
    let c = app.client()?;
    match cmd {
        SdkGrantsCmd::Get { sdk } => {
            let v = c.get("/v1/sdk-grants", &[])?;
            let items: Vec<Value> = v
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|g| {
                    sdk.as_deref()
                        .is_none_or(|s| g["sdk"] == json!(s) || g["sdk"] == json!("*"))
                })
                .collect();
            if app.json {
                fmt::print_json(&Value::Array(items));
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|g| {
                    vec![
                        cell(g, "scope"),
                        cell(g, "sdk"),
                        cell(g, "effect"),
                        cell(g, "version_req"),
                        cell(g, "channel"),
                        cell(g, "created_by"),
                    ]
                })
                .collect();
            if rows.is_empty() {
                fmt::note("No grants: nothing is granted to any tenant.");
                return Ok(());
            }
            fmt::emit(&fmt::table(
                &["SCOPE", "SDK", "EFFECT", "VERSION_REQ", "CHANNEL", "BY"],
                &rows,
            ));
        }
        SdkGrantsCmd::Set {
            scope,
            sdk,
            allow,
            deny,
            version_req,
            channel,
        } => {
            let effect = if *allow {
                json!("allow")
            } else if *deny {
                json!("deny")
            } else {
                Value::Null
            };
            let body =
                json!({"scope": scope, "sdk": sdk, "effect": effect, "version_req": version_req, "channel": channel});
            let stored = c.put("/v1/sdk-grants", &body)?;
            if app.json {
                fmt::print_json(&stored);
            } else if stored.is_null() {
                fmt::emitln(&format!("Deleted the grant of {sdk} at {scope}."));
            } else {
                fmt::emitln(&format!("Granted at {scope}:"));
                fmt::emit(&fmt::flat_table(&stored));
            }
        }
    }
    Ok(())
}

/// `21ci sdk types NAME [-d DIR]`: the contract of the release this key's grants select.
fn sdk_types(app: &App, a: &SdkTypesArgs) -> Result<()> {
    let c = app.client()?;
    let mut q = Vec::new();
    if let Some(p) = &a.project {
        q.push(format!("project={p}"));
    }
    if let Some(e) = &a.environment {
        q.push(format!("environment={e}"));
    }
    if let Some(v) = &a.release {
        q.push(format!("version={v}"));
    }
    let mut path = format!("/v1/sdks/{}/contract", a.name);
    if !q.is_empty() {
        path.push('?');
        path.push_str(&q.join("&"));
    }
    let text = get_text(&c, &path)?;
    save_types(app, &text, a.out.as_deref(), &format!("{}.d.ts", a.name))
}
