//! `21ci logs [-f]`: a page of records, or a Server-Sent Events stream (`?follow=1`).

use std::io::BufReader;
use std::time::SystemTime;

use serde_json::Value;

use super::{resolve, App, Svc};
use crate::cli::LogsArgs;
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;
use crate::sse::{self, ReadError};

/// `2026-01-01T00:00:00.123Z info  v3 task_1 message`, and for a `std:log` record with fields the
/// fields as compact JSON after the message. `--json` prints the record as it is.
pub fn format_line(r: &Value) -> String {
    let ts = fmt::ts_ms(r.get("ts").and_then(Value::as_i64).unwrap_or(0));
    let level = r.get("level").and_then(Value::as_str).unwrap_or("?");
    let version = r.get("version").and_then(Value::as_u64).unwrap_or(0);
    let task = r.get("task_id").and_then(Value::as_str).unwrap_or("-");
    let msg = r.get("msg").and_then(Value::as_str).unwrap_or("");
    let line = format!("{ts} {level:<5} v{version} {task} {msg}");
    match r.get("fields").filter(|f| f.as_object().is_some_and(|o| !o.is_empty())) {
        Some(fields) => format!("{line} {}", fmt::compact(fields)),
        None => line,
    }
}

fn filters(a: &LogsArgs, since: Option<&str>, until: Option<&str>) -> Vec<(&'static str, String)> {
    let mut q = Vec::new();
    if let Some(s) = since {
        q.push(("since", s.to_string()));
    }
    if let Some(u) = until {
        q.push(("until", u.to_string()));
    }
    for (k, v) in [("level", &a.level), ("task_id", &a.task), ("kind", &a.kind)] {
        if let Some(v) = v {
            q.push((k, v.clone()));
        }
    }
    q
}

pub fn run(app: &App, a: &LogsArgs) -> Result<()> {
    // parse the time arguments first: bad input must not cost a request
    let now = SystemTime::now();
    let since = a
        .since
        .as_deref()
        .map(|s| fmt::parse_time_arg(s, "--since", now))
        .transpose()?;
    let until = a
        .until
        .as_deref()
        .map(|s| fmt::parse_time_arg(s, "--until", now))
        .transpose()?;
    if a.follow && until.is_some() {
        return Err(CliError::usage("--until cannot be combined with --follow"));
    }
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    if a.follow {
        follow(app, &c, &svc, a, since.as_deref())
    } else {
        page(app, &c, &svc, a, since.as_deref(), until.as_deref())
    }
}

fn page(app: &App, c: &Client, svc: &Svc, a: &LogsArgs, since: Option<&str>, until: Option<&str>) -> Result<()> {
    let mut q = filters(a, since, until);
    if let Some(l) = a.limit {
        q.push(("limit", l.to_string()));
    }
    if let Some(cur) = &a.cursor {
        q.push(("cursor", cur.clone()));
    }
    let body = c.get(&svc.path("/logs"), &q)?;
    if app.json {
        fmt::print_json(&body);
        return Ok(());
    }
    let items = body.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
    if items.is_empty() {
        fmt::note("No log records found.");
    }
    for r in &items {
        fmt::emitln(&format_line(r));
    }
    if let Some(n) = body.get("next_cursor").and_then(Value::as_str) {
        fmt::note(&format!("(more results: --cursor {n})"));
    }
    Ok(())
}

fn print_record(app: &App, r: &Value) {
    if app.json {
        fmt::emitln(&fmt::compact(r));
    } else {
        fmt::emitln(&format_line(r));
    }
}

fn follow(app: &App, c: &Client, svc: &Svc, a: &LogsArgs, since: Option<&str>) -> Result<()> {
    // `--since`: print the backlog first, then stream from the last record we printed
    let mut last_seq: Option<u64> = None;
    if since.is_some() {
        let mut cursor: Option<String> = None;
        loop {
            let mut q = filters(a, since, None);
            q.push(("limit", "1000".into()));
            if let Some(cur) = &cursor {
                q.push(("cursor", cur.clone()));
            }
            let body = c.get(&svc.path("/logs"), &q)?;
            for r in body
                .get("items")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                print_record(app, r);
                if let Some(s) = r.get("seq").and_then(Value::as_u64) {
                    last_seq = Some(s);
                }
            }
            match body.get("next_cursor").and_then(Value::as_str) {
                Some(n) => cursor = Some(n.to_string()),
                None => break,
            }
        }
    }

    let mut q = filters(a, None, None);
    q.push(("follow", "1".into()));
    let mut rb = c.stream_req(&svc.path("/logs")).query(&q);
    if let Some(s) = last_seq {
        rb = rb.header("last-event-id", s.to_string());
    }
    let resp = c.send(rb)?;
    let result = sse::read_events(BufReader::new(resp), |ev| -> std::result::Result<(), CliError> {
        if ev.event == "log" {
            match serde_json::from_str::<Value>(&ev.data) {
                Ok(r) => print_record(app, &r),
                Err(e) => fmt::note(&format!("skipping an unreadable log event: {e}")),
            }
        }
        Ok(())
    });
    match result {
        Ok(()) => Ok(()),
        Err(ReadError::Handler(e)) => Err(e), // LCOV_EXCL_LINE: the handler above never fails, so no Handler error is built
        Err(ReadError::Io(e)) => Err(CliError::Network(format!("log stream interrupted: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::format_line;
    use crate::fmt;
    use serde_json::json;

    #[test]
    fn a_record_without_fields_keeps_the_plain_line() {
        let r = json!({"ts": 0, "level": "info", "version": 3, "task_id": "t1", "msg": "hi"});
        assert_eq!(format_line(&r), format!("{} info  v3 t1 hi", fmt::ts_ms(0)));
    }

    #[test]
    fn fields_follow_the_message_as_compact_json() {
        let r = json!({"ts": 0, "level": "warn", "version": 3, "task_id": "t1", "msg": "hi",
                       "fields": {"chat": 42, "ok": true}});
        assert_eq!(
            format_line(&r),
            format!("{} warn  v3 t1 hi {{\"chat\":42,\"ok\":true}}", fmt::ts_ms(0))
        );

        let empty = json!({"ts": 0, "level": "info", "version": 3, "task_id": null, "msg": "hi", "fields": {}});
        assert_eq!(format_line(&empty), format!("{} info  v3 - hi", fmt::ts_ms(0)));
    }
}
