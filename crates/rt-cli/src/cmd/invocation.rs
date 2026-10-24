//! `21ci invocation list | get | redrive | purge`: the async invocations of a service
//! (accepted by `21ci invoke --async` or any `Prefer: respond-async` request).

use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, fetch_page_with, print_page, resolve, App};
use crate::cli::InvocationCmd;
use crate::error::Result;
use crate::fmt;

/// `pending 3, failed 1` from the list's `counts`.
fn counts(raw: &Value) -> Option<String> {
    let c = raw.get("counts")?;
    Some(format!(
        "pending {}, failed {}",
        c.get("pending").and_then(Value::as_u64).unwrap_or(0),
        c.get("failed").and_then(Value::as_u64).unwrap_or(0)
    ))
}

/// `/hook?…` when the request had a query (never shown: it may hold credentials).
fn path(v: &Value) -> String {
    let p = cell(v, "path");
    if v.get("has_query").and_then(Value::as_bool) == Some(true) {
        format!("{p}?…")
    } else {
        p
    }
}

fn show(app: &App, v: &Value) {
    if app.json {
        fmt::print_json(v);
        return;
    }
    fmt::emit(&fmt::kv(&[
        ("ID", cell(v, "id")),
        ("State", cell(v, "state")),
        ("Request", format!("{} {}", cell(v, "method"), path(v))),
        ("Attempts", cell(v, "attempts")),
        ("Status", cell(v, "status")),
        ("Condition", cell(v, "condition")),
        ("Error", cell(v, "error")),
        ("Body bytes", cell(v, "body_bytes")),
        ("Created", fmt::ts_field(v, "created_at")),
        ("Updated", fmt::ts_field(v, "updated_at")),
        ("Expires", fmt::ts_field(v, "expires_at")),
    ]));
}

pub fn run(app: &App, cmd: &InvocationCmd) -> Result<()> {
    match cmd {
        InvocationCmd::List { svc, state, page } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let filter: Vec<(&str, String)> = state.iter().map(|s| ("state", s.clone())).collect();
            let p = fetch_page_with(&c, &svc.path("/invocations"), page, &filter)?;
            if !app.json {
                if let Some(text) = counts(&p.raw) {
                    fmt::emitln(&text);
                }
            }
            print_page(
                app,
                &p,
                "invocations",
                &["ID", "STATE", "REQUEST", "ATTEMPTS", "STATUS", "ERROR", "CREATED"],
                |v| {
                    vec![
                        cell(v, "id"),
                        cell(v, "state"),
                        format!("{} {}", cell(v, "method"), path(v)),
                        cell(v, "attempts"),
                        cell(v, "status"),
                        cell(v, "error"),
                        fmt::ts_field(v, "created_at"),
                    ]
                },
            );
        }
        InvocationCmd::Get { svc, id } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let v = c.get(&svc.path(&format!("/invocations/{}", fmt::enc(id))), &[])?;
            show(app, &v);
        }
        InvocationCmd::Redrive(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let r = c
                .send_json(c.req(Method::POST, &svc.path("/invocations/redrive")))?
                .body;
            if app.json {
                fmt::print_json(&r);
            } else {
                let n = r.get("redriven").and_then(Value::as_u64).unwrap_or(0) as usize;
                fmt::emitln(&format!(
                    "Redrove {} of {}.",
                    fmt::plural(n, "failed invocation", "failed invocations"),
                    svc.label
                ));
            }
        }
        InvocationCmd::Purge(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            c.delete(&svc.path("/invocations"), &[("state", "failed".to_string())])?;
            if app.json {
                fmt::print_json(&json!({"purged": true}));
            } else {
                fmt::emitln(&format!("Purged the failed invocations of {}.", svc.label));
            }
        }
    }
    Ok(())
}
