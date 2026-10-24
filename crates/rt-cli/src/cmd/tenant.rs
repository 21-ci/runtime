//! `21ci tenant create | list | get | update | delete | usage` (operator; `usage` also for a
//! tenant key on its own tenant).

use std::time::SystemTime;

use serde_json::{json, Value};

use super::{cell, fetch_page, parse_json_arg, print_page, App};
use crate::cli::TenantCmd;
use crate::error::{CliError, Result};
use crate::fmt;

fn n(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// `1536` -> `1.5 KiB`.
pub(super) fn bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// Rows per (day, service), then a total of the counters (storage is a level, not summed).
fn usage_table(body: &Value) {
    let items = body.as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        fmt::note("No usage recorded in that range.");
        return;
    }
    let mut rows: Vec<Vec<String>> = items
        .iter()
        .map(|u| {
            vec![
                cell(u, "day"),
                cell(u, "service_id"),
                n(u, "invocations").to_string(),
                n(u, "cpu_ms").to_string(),
                bytes(n(u, "egress_bytes")),
                bytes(u.get("storage_bytes").and_then(Value::as_i64).unwrap_or(0).max(0) as u64),
            ]
        })
        .collect();
    let sum = |k: &str| items.iter().map(|u| n(u, k)).sum::<u64>();
    rows.push(vec![
        "TOTAL".into(),
        "-".into(),
        sum("invocations").to_string(),
        sum("cpu_ms").to_string(),
        bytes(sum("egress_bytes")),
        "-".into(),
    ]);
    fmt::emit(&fmt::table(
        &["DAY", "SERVICE", "INVOCATIONS", "CPU_MS", "EGRESS", "STORAGE"],
        &rows,
    ));
}

pub fn run(app: &App, cmd: &TenantCmd) -> Result<()> {
    match cmd {
        TenantCmd::Create { name, plan, overrides } => {
            let mut body = json!({"name": name, "plan": plan});
            if let Some(o) = overrides {
                body["overrides"] = parse_json_arg("--overrides", o)?;
            }
            let v = app.client()?.post("/v1/tenants", &body)?;
            show(app, &v);
        }
        TenantCmd::List(p) => {
            let page = fetch_page(&app.client()?, "/v1/tenants", p)?;
            print_page(
                app,
                &page,
                "tenants",
                &["ID", "NAME", "PLAN", "DISABLED", "CREATED"],
                |t| {
                    vec![
                        cell(t, "id"),
                        cell(t, "name"),
                        cell(t, "plan"),
                        cell(t, "disabled"),
                        fmt::ts_field(t, "created_at"),
                    ]
                },
            );
        }
        TenantCmd::Get { id } => {
            let v = app.client()?.get(&format!("/v1/tenants/{}", fmt::enc(id)), &[])?;
            show(app, &v);
        }
        TenantCmd::Update {
            id,
            name,
            plan,
            overrides,
            disable,
            enable,
        } => {
            let mut body = json!({});
            if let Some(n) = name {
                body["name"] = json!(n);
            }
            if let Some(p) = plan {
                body["plan"] = json!(p);
            }
            if let Some(o) = overrides {
                let v = parse_json_arg("--overrides", o)?;
                if !v.is_object() {
                    return Err(CliError::usage("--overrides must be a JSON object"));
                }
                body["overrides"] = v;
            }
            if *disable || *enable {
                body["disabled"] = json!(*disable);
            }
            if body.as_object().is_some_and(|o| o.is_empty()) {
                return Err(CliError::usage(
                    "nothing to change: give --name, --plan, --overrides, --disable or --enable",
                ));
            }
            let v = app.client()?.patch(&format!("/v1/tenants/{}", fmt::enc(id)), &body)?;
            show(app, &v);
        }
        TenantCmd::Usage { id, from, to } => {
            let now = SystemTime::now();
            let mut q: Vec<(&str, String)> = Vec::new();
            if let Some(f) = from {
                q.push(("from", fmt::parse_day_arg(f, "--from", now)?));
            }
            if let Some(t) = to {
                q.push(("to", fmt::parse_day_arg(t, "--to", now)?));
            }
            let c = app.client()?;
            let id = match id {
                Some(id) => id.clone(),
                // a tenant key (or the operator with --as-tenant): the tenant of the credentials
                None => c
                    .get("/v1/me", &[])?
                    .get("tenant_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        CliError::usage("pass the tenant id: the operator token has no tenant (or use --as-tenant)")
                    })?,
            };
            let body = c.get(&format!("/v1/tenants/{}/usage", fmt::enc(&id)), &q)?;
            if app.json {
                fmt::print_json(&body);
                return Ok(());
            }
            usage_table(&body);
        }
        TenantCmd::Delete { id, purge } => {
            let query: Vec<(&str, String)> = if *purge {
                vec![("purge", "1".to_string())]
            } else {
                vec![]
            };
            app.client()?.delete(&format!("/v1/tenants/{}", fmt::enc(id)), &query)?;
            if app.json {
                fmt::print_json(&json!({"deleted": id}));
            } else {
                fmt::emitln(&format!("Tenant {id} deleted."));
            }
        }
    }
    Ok(())
}

fn show(app: &App, t: &Value) {
    if app.json {
        fmt::print_json(t);
        return;
    }
    let mut rows = vec![
        ("ID", cell(t, "id")),
        ("Name", cell(t, "name")),
        ("Plan", cell(t, "plan")),
        ("Disabled", cell(t, "disabled")),
        ("Created", fmt::ts_field(t, "created_at")),
    ];
    if let Some(o) = t
        .get("overrides")
        .filter(|o| o.as_object().is_some_and(|m| !m.is_empty()))
    {
        rows.push(("Overrides", fmt::compact(o)));
    }
    fmt::emit(&fmt::kv(&rows));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1023), "1023 B");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(5 * 1024 * 1024 * 1024), "5.0 GiB");
    }
}
