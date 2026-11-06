//! `21ci queue send | stats | dlq`.

use std::io::Read;

use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, parse_json_arg, resolve, App};
use crate::cli::QueueCmd;
use crate::error::{CliError, Result};
use crate::fmt;

/// `--body`: JSON text, `@file`, or `-` (stdin).
pub(crate) fn read_body(arg: &str) -> Result<Value> {
    let text = if arg == "-" {
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| CliError::usage(format!("cannot read stdin: {e}")))?;
        s
    } else if let Some(path) = arg.strip_prefix('@') {
        std::fs::read_to_string(path).map_err(|e| CliError::usage(format!("--body {path}: {e}")))?
    } else {
        arg.to_string()
    };
    parse_json_arg("--body", &text)
}

pub fn run(app: &App, cmd: &QueueCmd) -> Result<()> {
    match cmd {
        QueueCmd::Send {
            svc,
            body,
            delay,
            dedup,
        } => {
            let mut b = json!({"body": read_body(body)?});
            if let Some(d) = delay {
                b["delay_seconds"] = json!(d.as_secs());
            }
            if let Some(d) = dedup {
                b["dedup_id"] = json!(d);
            }
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let r = c.post(&svc.path("/queue"), &b)?;
            if app.json {
                fmt::print_json(&r);
            } else {
                fmt::emitln(&format!("Enqueued {} on {}.", cell(&r, "id"), svc.label));
            }
        }
        QueueCmd::Stats(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let s = c.get(&svc.path("/queue"), &[])?;
            if app.json {
                fmt::print_json(&s);
            } else {
                fmt::emit(&fmt::kv(&[
                    ("Depth", cell(&s, "depth")),
                    ("In flight", cell(&s, "in_flight")),
                    ("Oldest age ms", cell(&s, "oldest_age_ms")),
                    ("DLQ", cell(&s, "dlq")),
                ]));
            }
        }
        QueueCmd::Dlq { svc, redrive, purge } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            if *redrive {
                let r = c.send_json(c.req(Method::POST, &svc.path("/dlq/redrive")))?.body;
                if app.json {
                    fmt::print_json(&r);
                } else {
                    let n = r.get("redriven").and_then(Value::as_u64).unwrap_or(0) as usize;
                    fmt::emitln(&format!(
                        "Redrove {} from the DLQ of {}.",
                        fmt::plural(n, "message", "messages"),
                        svc.label
                    ));
                }
            } else if *purge {
                c.delete(&svc.path("/dlq"), &[])?;
                if app.json {
                    fmt::print_json(&json!({"purged": true}));
                } else {
                    fmt::emitln(&format!("Purged the DLQ of {}.", svc.label));
                }
            } else {
                let r = c.get(&svc.path("/dlq"), &[])?;
                if app.json {
                    fmt::print_json(&r);
                    return Ok(());
                }
                let items = r.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
                if items.is_empty() {
                    fmt::note("The DLQ is empty.");
                    return Ok(());
                }
                let rows: Vec<Vec<String>> = items
                    .iter()
                    .map(|m| {
                        vec![
                            cell(m, "id"),
                            cell(m, "attempts"),
                            fmt::ts_field(m, "failed_at"),
                            m.get("body").map(fmt::compact).unwrap_or_else(|| "-".into()),
                        ]
                    })
                    .collect();
                fmt::emit(&fmt::table(&["ID", "ATTEMPTS", "FAILED_AT", "BODY"], &rows));
            }
        }
    }
    Ok(())
}
