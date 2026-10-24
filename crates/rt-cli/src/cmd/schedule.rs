//! `21ci schedule add | list | update | rm`.

use serde_json::{json, Value};

use super::{cell, parse_json_arg, resolve, App};
use crate::cli::{ScheduleAddArgs, ScheduleCmd, ScheduleUpdateArgs};
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// `every 60s` for an interval, the expression for cron.
fn when(s: &Value) -> String {
    match s.get("kind").and_then(Value::as_str) {
        Some("interval") => s
            .get("every_seconds")
            .and_then(Value::as_u64)
            .map(|n| format!("every {n}s"))
            .unwrap_or_else(|| "-".into()),
        _ => cell(s, "cron"),
    }
}

fn is_once(s: &Value) -> bool {
    s.get("kind").and_then(Value::as_str) == Some("once")
}

/// A one-off schedule is due at its run time.
fn next_due(s: &Value) -> String {
    fmt::ts_field(s, if is_once(s) { "run_at" } else { "next_due" })
}

/// A one-off schedule's payload is not listed.
fn payload(s: &Value) -> String {
    if is_once(s) {
        return "-".into();
    }
    s.get("payload").map(fmt::compact).unwrap_or_else(|| "{}".into())
}

/// Every page of the one-off schedules of a service (`GET …/schedules/once`).
fn once_schedules(c: &Client, path: &str) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut q = vec![("limit", "200".to_string())];
        if let Some(cur) = &cursor {
            q.push(("cursor", cur.clone()));
        }
        let page = c.get(path, &q)?;
        out.extend(page.get("items").and_then(Value::as_array).cloned().unwrap_or_default());
        cursor = page.get("next_cursor").and_then(Value::as_str).map(str::to_string);
        if cursor.is_none() {
            return Ok(out);
        }
    }
}

fn body(a: &ScheduleAddArgs) -> Result<Value> {
    let payload = match &a.payload {
        Some(p) => parse_json_arg("--payload", p)?,
        None => json!({}),
    };
    let mut b = match (&a.every, &a.cron) {
        (Some(every), None) => {
            json!({"kind": "interval", "every_seconds": every_seconds(every)?, "payload": payload})
        }
        (None, Some(cron)) => {
            let mut v = json!({"kind": "cron", "cron": cron, "payload": payload});
            if let Some(tz) = &a.tz {
                v["timezone"] = json!(tz);
            }
            v
        }
        _ => {
            return Err(CliError::usage(
                "pass exactly one of --every <duration> or --cron <expression>",
            ))
        }
    };
    if a.disabled {
        b["enabled"] = json!(false);
    }
    Ok(b)
}

fn every_seconds(every: &str) -> Result<u64> {
    let d = fmt::parse_duration(every).map_err(|e| CliError::usage(format!("--every: {e}")))?;
    if d.as_secs() == 0 || d.subsec_nanos() != 0 {
        return Err(CliError::usage(format!(
            "--every must be a whole number of seconds, at least 1s (got `{every}`)"
        )));
    }
    Ok(d.as_secs())
}

/// The `PATCH` body: `kind` is required by the API, so without `--every`/`--cron` it is the
/// schedule's current kind (`current_kind`, looked up only when needed).
fn update_body(a: &ScheduleUpdateArgs, current_kind: impl FnOnce() -> Result<String>) -> Result<Value> {
    let payload = a
        .payload
        .as_deref()
        .map(|p| parse_json_arg("--payload", p))
        .transpose()?;
    if a.every.is_none() && a.cron.is_none() && a.tz.is_none() && payload.is_none() && !a.enable && !a.disable {
        return Err(CliError::usage(
            "nothing to change: give --every, --cron, --tz, --payload, --enable or --disable",
        ));
    }
    let mut b = match (&a.every, &a.cron) {
        (Some(every), _) => json!({"kind": "interval", "every_seconds": every_seconds(every)?}),
        (None, Some(cron)) => json!({"kind": "cron", "cron": cron}),
        (None, None) => json!({"kind": current_kind()?}),
    };
    if let Some(tz) = &a.tz {
        if b["kind"] != "cron" {
            return Err(CliError::usage("--tz applies to cron schedules only"));
        }
        b["timezone"] = json!(tz);
    }
    if let Some(p) = payload {
        b["payload"] = p;
    }
    if a.enable || a.disable {
        b["enabled"] = json!(a.enable);
    }
    Ok(b)
}

fn show_one(app: &App, s: &Value) {
    if app.json {
        fmt::print_json(s);
        return;
    }
    let mut rows = vec![("ID", cell(s, "id")), ("Kind", cell(s, "kind")), ("Schedule", when(s))];
    if s.get("timezone").is_some_and(|t| !t.is_null()) {
        rows.push(("Timezone", cell(s, "timezone")));
    }
    rows.push(("Enabled", cell(s, "enabled")));
    rows.push(("Next due", next_due(s)));
    rows.push((
        "Payload",
        s.get("payload").map(fmt::compact).unwrap_or_else(|| "{}".into()),
    ));
    fmt::emit(&fmt::kv(&rows));
}

pub fn run(app: &App, cmd: &ScheduleCmd) -> Result<()> {
    match cmd {
        ScheduleCmd::Add(a) => {
            let b = body(a)?;
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let s = c.post(&svc.path("/schedules"), &b)?;
            show_one(app, &s);
        }
        ScheduleCmd::Update(a) => {
            // flags first, so a usage error costs no request (a stand-in kind; the real one is
            // checked below)
            update_body(a, || Ok("cron".to_string()))?;
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let b = update_body(a, || {
                let list = c.get(&svc.path("/schedules"), &[])?;
                list.as_array()
                    .and_then(|l| {
                        l.iter()
                            .find(|s| s.get("id").and_then(Value::as_str) == Some(a.id.as_str()))
                    })
                    .and_then(|s| s.get("kind").and_then(Value::as_str))
                    .map(str::to_string)
                    .ok_or_else(|| CliError::not_found(format!("schedule {} not found on {}", a.id, svc.label)))
            })?;
            let s = c.patch(&svc.path(&format!("/schedules/{}", fmt::enc(&a.id))), &b)?;
            show_one(app, &s);
        }
        ScheduleCmd::List(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            // the recurring schedules, then the pending one-off ones
            let mut items = c
                .get(&svc.path("/schedules"), &[])?
                .as_array()
                .cloned()
                .unwrap_or_default();
            items.extend(once_schedules(&c, &svc.path("/schedules/once"))?);
            if app.json {
                fmt::print_json(&Value::Array(items));
                return Ok(());
            }
            if items.is_empty() {
                fmt::note("No schedules found.");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|s| {
                    vec![
                        cell(s, "id"),
                        cell(s, "kind"),
                        when(s),
                        cell(s, "timezone"),
                        cell(s, "enabled"),
                        next_due(s),
                        payload(s),
                    ]
                })
                .collect();
            fmt::emit(&fmt::table(
                &["ID", "KIND", "SCHEDULE", "TZ", "ENABLED", "NEXT_DUE", "PAYLOAD"],
                &rows,
            ));
        }
        ScheduleCmd::Rm { svc, id } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            c.delete(&svc.path(&format!("/schedules/{}", fmt::enc(id))), &[])?;
            if app.json {
                fmt::print_json(&json!({"deleted": id}));
            } else {
                fmt::emitln(&format!("Schedule {id} removed from {}.", svc.label));
            }
        }
    }
    Ok(())
}
