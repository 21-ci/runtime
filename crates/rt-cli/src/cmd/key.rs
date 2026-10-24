//! `21ci key create | list | revoke` (the operator's keys of a tenant). `21ci token …` shows the same
//! key views ([`show_created`], [`show_list`]).

use serde_json::{json, Value};

use super::{cell, App};
use crate::cli::KeyCmd;
use crate::error::Result;
use crate::fmt;

fn scopes(k: &Value) -> String {
    k.get("scopes")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","))
        .unwrap_or_default()
}

/// A key's list field (`project_ids`, `environments`): `all` when unrestricted (`null`).
fn limit(k: &Value, field: &str) -> String {
    match k.get(field).and_then(Value::as_array) {
        Some(a) if !a.is_empty() => a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","),
        Some(_) => "none".into(),
        None => "all".into(),
    }
}

fn expires(k: &Value) -> String {
    k.get("expires_at")
        .and_then(Value::as_i64)
        .map(fmt::ts_s)
        .unwrap_or_else(|| "never".into())
}

pub fn run(app: &App, cmd: &KeyCmd) -> Result<()> {
    match cmd {
        KeyCmd::Create {
            tenant,
            scopes: wanted,
            name,
            projects,
            environments,
            expires_at,
        } => {
            let mut body = json!({"scopes": wanted});
            if let Some(n) = name {
                body["name"] = json!(n);
            }
            if !projects.is_empty() {
                body["project_ids"] = json!(projects);
            }
            if !environments.is_empty() {
                body["environments"] = json!(environments);
            }
            if let Some(e) = expires_at {
                body["expires_at"] = json!(e);
            }
            let k = app
                .client()?
                .post(&format!("/v1/tenants/{}/keys", fmt::enc(tenant)), &body)?;
            show_created(app, &k);
        }
        KeyCmd::List { tenant } => {
            let keys = app
                .client()?
                .get(&format!("/v1/tenants/{}/keys", fmt::enc(tenant)), &[])?;
            show_list(app, &keys);
        }
        KeyCmd::Revoke { key, tenant } => {
            app.client()?
                .delete(&format!("/v1/tenants/{}/keys/{}", fmt::enc(tenant), fmt::enc(key)), &[])?;
            show_revoked(app, key);
        }
    }
    Ok(())
}

/// A key as `POST …/keys` returns it: the fields, and the secret (shown once).
pub(super) fn show_created(app: &App, k: &Value) {
    if app.json {
        fmt::print_json(k);
    } else {
        fmt::emit(&fmt::kv(&[
            ("ID", cell(k, "id")),
            ("Name", cell(k, "name")),
            ("Scopes", scopes(k)),
            ("Projects", limit(k, "project_ids")),
            ("Environments", limit(k, "environments")),
            ("Expires", expires(k)),
            ("Created", fmt::ts_field(k, "created_at")),
            ("Secret", cell(k, "secret")),
        ]));
        fmt::note("The secret is shown only once: store it now.");
    }
}

/// The keys of a tenant (an array), as a table.
pub(super) fn show_list(app: &App, keys: &Value) {
    if app.json {
        fmt::print_json(keys);
        return;
    }
    let items = keys.as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        fmt::note("No keys found.");
        return;
    }
    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|k| {
            vec![
                cell(k, "id"),
                cell(k, "name"),
                scopes(k),
                limit(k, "project_ids"),
                limit(k, "environments"),
                expires(k),
                fmt::ts_field(k, "last_used_at"),
            ]
        })
        .collect();
    fmt::emit(&fmt::table(
        &[
            "ID",
            "NAME",
            "SCOPES",
            "PROJECTS",
            "ENVIRONMENTS",
            "EXPIRES",
            "LAST_USED",
        ],
        &rows,
    ));
}

pub(super) fn show_revoked(app: &App, key: &str) {
    if app.json {
        fmt::print_json(&json!({"revoked": key}));
    } else {
        fmt::emitln(&format!("Key {key} revoked."));
    }
}
