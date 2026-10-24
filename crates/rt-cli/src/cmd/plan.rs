//! `21ci plan list | get` (operator, `GET /v1/plans`) and `21ci whoami` (`GET /v1/me`).

use serde_json::Value;

use super::{cell, App};
use crate::cli::PlanCmd;
use crate::error::{CliError, Result};
use crate::fmt;

fn setting(p: &Value, key: &str) -> String {
    fmt::str_field(p.get("settings").unwrap_or(&Value::Null), key)
}

/// `0` means unlimited for the quota fields.
fn quota(p: &Value, key: &str) -> String {
    match p.pointer(&format!("/settings/{key}")).and_then(Value::as_u64) {
        Some(0) => "unlimited".into(),
        Some(n) => n.to_string(),
        None => "-".into(),
    }
}

fn suffixes(p: &Value) -> String {
    match p.pointer("/settings/custom_host_suffixes").and_then(Value::as_array) {
        Some(a) if !a.is_empty() => a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","),
        _ => "-".into(),
    }
}

pub fn run(app: &App, cmd: &PlanCmd) -> Result<()> {
    let plans = app.client()?.get("/v1/plans", &[])?;
    let items = plans.as_array().cloned().unwrap_or_default();
    match cmd {
        PlanCmd::List => {
            if app.json {
                fmt::print_json(&plans);
                return Ok(());
            }
            if items.is_empty() {
                fmt::note("No plans configured.");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|p| {
                    vec![
                        cell(p, "name"),
                        setting(p, "max_services"),
                        quota(p, "max_concurrency"),
                        quota(p, "rps"),
                        quota(p, "invocations_per_day"),
                        setting(p, "storage_mb"),
                        suffixes(p),
                    ]
                })
                .collect();
            fmt::emit(&fmt::table(
                &[
                    "NAME",
                    "SERVICES",
                    "CONCURRENCY",
                    "RPS",
                    "INVOCATIONS/DAY",
                    "STORAGE_MB",
                    "HOST_SUFFIXES",
                ],
                &rows,
            ));
        }
        PlanCmd::Get { name } => {
            let p = items
                .into_iter()
                .find(|p| p.get("name").and_then(Value::as_str) == Some(name.as_str()))
                .ok_or_else(|| CliError::not_found(format!("plan \"{name}\" is not configured")))?;
            if app.json {
                fmt::print_json(&p);
                return Ok(());
            }
            fmt::emitln(&format!("Plan: {name}"));
            fmt::emit(&fmt::flat_table(p.get("settings").unwrap_or(&Value::Null)));
        }
    }
    Ok(())
}

pub fn whoami(app: &App) -> Result<()> {
    let me = app.client()?.get("/v1/me", &[])?;
    if app.json {
        fmt::print_json(&me);
        return Ok(());
    }
    let list = |key: &str, none: &str| match me.get(key).and_then(Value::as_array) {
        Some(a) if !a.is_empty() => a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","),
        _ => none.to_string(),
    };
    let mut rows = vec![
        ("Principal", cell(&me, "principal")),
        ("Tenant", cell(&me, "tenant_id")),
        ("Key", cell(&me, "key_id")),
        ("Scopes", list("scopes", "-")),
        ("Projects", list("project_ids", "all")),
        ("Environments", list("environments", "all")),
    ];
    if let Some(p) = me.get("plan").filter(|p| !p.is_null()) {
        rows.push(("Plan", cell(p, "name")));
    }
    rows.push(("Node version", cell(&me, "version")));
    fmt::emit(&fmt::kv(&rows));
    Ok(())
}
