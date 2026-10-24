//! `21ci events`.

use serde_json::Value;

use super::{cell, resolve, App};
use crate::cli::EventsArgs;
use crate::error::Result;
use crate::fmt;

/// `key_…` for a key, the kind (`operator`, `system`) otherwise, `-` when unknown.
fn actor(e: &Value) -> String {
    match e.pointer("/data/actor") {
        Some(a) => match a.get("kind").and_then(Value::as_str) {
            Some("key") => a.get("key_id").and_then(Value::as_str).unwrap_or("key").to_string(),
            Some(kind) => kind.to_string(),
            None => "-".into(),
        },
        None => "-".into(),
    }
}

/// The event payload; audit records carry it one level down.
fn payload(e: &Value) -> String {
    match e.get("data") {
        Some(d) if d.get("actor").is_some() => d.get("data").map(fmt::compact).unwrap_or_else(|| "{}".into()),
        Some(d) => fmt::compact(d),
        None => "{}".into(),
    }
}

pub fn run(app: &App, a: &EventsArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let mut q: Vec<(&str, String)> = Vec::new();
    if let Some(l) = a.limit {
        q.push(("limit", l.to_string()));
    }
    if let Some(cur) = &a.cursor {
        q.push(("cursor", cur.clone()));
    }
    let body = c.get(&svc.path("/events"), &q)?;
    if app.json {
        fmt::print_json(&body);
        return Ok(());
    }
    let items = body.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
    if items.is_empty() {
        fmt::note("No events found.");
    } else {
        // control-plane events are audit records: {"actor": {...}, "data": {...}}
        let audited = items.iter().any(|e| e.pointer("/data/actor").is_some());
        let rows: Vec<Vec<String>> = items
            .iter()
            .map(|e| {
                let mut row = vec![fmt::ts_field(e, "ts"), cell(e, "kind")];
                if audited {
                    row.push(actor(e));
                }
                row.push(payload(e));
                row
            })
            .collect();
        let headers: &[&str] = if audited {
            &["TIME", "KIND", "ACTOR", "DATA"]
        } else {
            &["TIME", "KIND", "DATA"]
        };
        fmt::emit(&fmt::table(headers, &rows));
    }
    if let Some(n) = body.get("next_cursor").and_then(Value::as_str) {
        fmt::note(&format!("(more results: --cursor {n})"));
    }
    Ok(())
}
