//! `21ci switch`, `switch status`: make a branch's code live on its parent and follow the drain.

use std::time::{Duration, Instant};

use reqwest::header::IF_MATCH;
use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, fetch_page, print_page, resolve, App, Svc};
use crate::cli::{SwitchArgs, SwitchCmd};
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// A switch id goes into a request path: letters, digits and `_` only.
pub fn check_id(id: &str) -> Result<()> {
    if !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Ok(());
    }
    Err(CliError::usage(format!("invalid switch id \"{id}\"")))
}

fn state_of(v: &Value) -> &str {
    v.get("state").and_then(Value::as_str).unwrap_or("?")
}

fn warnings(v: &Value) -> Vec<String> {
    let items = v.get("warnings").and_then(Value::as_array);
    items
        .into_iter()
        .flatten()
        .map(|w| {
            let code = cell(w, "code");
            let names: Vec<&str> = w
                .get("names")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            if names.is_empty() {
                code
            } else {
                format!("{code}: {}", names.join(", "))
            }
        })
        .collect()
}

/// One switch, as JSON or as `Key: value` rows.
pub fn print_view(app: &App, v: &Value) {
    if app.json {
        fmt::print_json(v);
        return;
    }
    let mut rows = vec![
        ("Switch", cell(v, "id")),
        ("Kind", cell(v, "kind")),
        ("State", cell(v, "state")),
        ("From version", cell(v, "from_version")),
        ("To version", cell(v, "to_version")),
    ];
    if let Some(s) = v.get("source").filter(|s| !s.is_null()) {
        rows.push((
            "Source",
            format!(
                "{} version {} ({})",
                cell(s, "service"),
                cell(s, "version"),
                cell(s, "digest")
            ),
        ));
    }
    rows.push(("Old tasks running", cell(v, "inflight_old")));
    if v.get("drain_deadline").is_some_and(|d| !d.is_null()) {
        rows.push(("Drain deadline", fmt::ts_field(v, "drain_deadline")));
    }
    if v.get("interrupted").and_then(Value::as_bool) == Some(true) {
        rows.push(("Interrupted", "yes (the node restarted during the drain)".to_string()));
    }
    if v.get("error").is_some_and(|e| !e.is_null()) {
        rows.push(("Error", cell(v, "error")));
    }
    for w in warnings(v) {
        rows.push(("Warning", w));
    }
    fmt::emit(&fmt::kv(&rows));
}

/// `POST …` of a switch or rollback with `If-Match` of the service as read a moment before.
pub fn post_guarded(c: &Client, svc: &mut Svc, suffix: &str, body: &Value) -> Result<Value> {
    svc.view(c)?;
    let mut rb = c.req(Method::POST, &svc.path(suffix)).json(body);
    if let Some(etag) = &svc.etag {
        rb = rb.header(IF_MATCH, etag);
    }
    Ok(c.send_json(rb)?.body)
}

pub fn run(app: &App, a: &SwitchArgs) -> Result<()> {
    if let Some(SwitchCmd::Status { svc, id, page }) = &a.cmd {
        if let Some(id) = id {
            check_id(id)?;
        }
        let c = app.client()?;
        let s = resolve(&c, svc)?;
        if let Some(id) = id {
            print_view(app, &c.get(&s.path(&format!("/switches/{id}")), &[])?);
            return Ok(());
        }
        let p = fetch_page(&c, &s.path("/switches"), page)?;
        print_page(
            app,
            &p,
            "switches",
            &["ID", "KIND", "STATE", "FROM", "TO", "CREATED"],
            |v| {
                vec![
                    cell(v, "id"),
                    cell(v, "kind"),
                    cell(v, "state"),
                    cell(v, "from_version"),
                    cell(v, "to_version"),
                    fmt::ts_field(v, "created_at"),
                ]
            },
        );
        return Ok(());
    }
    // clap requires both unless a subcommand was given
    let (Some(svc), Some(from)) = (&a.svc, &a.from) else {
        return Err(CliError::usage("usage: 21ci switch <svc> --from <branch>"));
    };
    super::branch::check_name(from)?;
    let mut body = json!({"from": {"branch": from}});
    if a.dry_run {
        body["dry_run"] = json!(true);
    }
    if a.close_sockets {
        body["close_sockets"] = json!(true);
    }
    if let Some(d) = &a.drain_timeout {
        body["drain_timeout"] = json!(d);
    }
    let c = app.client()?;
    let mut s = resolve(&c, svc)?;
    let started = post_guarded(&c, &mut s, "/switch", &body)?;
    if a.dry_run {
        print_view(app, &started);
        fmt::note("Dry run: nothing was written.");
        return Ok(());
    }
    if !a.wait {
        print_view(app, &started);
        if let Some(id) = started.get("id").and_then(Value::as_str) {
            fmt::note(&format!("Follow it with: 21ci switch status {svc} {id}"));
        }
        return Ok(());
    }
    let id = started
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::failed("the server returned a switch without an id"))?;
    let mut last = wait(&c, &s, id, svc, a.wait_timeout, a.wait_interval)?;
    // Warnings of the start are in its answer; a later read may not repeat them.
    if warnings(&last).is_empty() {
        last["warnings"] = started.get("warnings").cloned().unwrap_or_else(|| json!([]));
    }
    print_view(app, &last);
    match state_of(&last) {
        "failed" => Err(CliError::failed(format!(
            "switch {id} failed: {}; the live version did not change",
            cell(&last, "error")
        ))),
        "drain_timed_out" => {
            let why = match last.get("error").and_then(Value::as_str) {
                Some(code) => format!(" ({code})"),
                None => String::new(),
            };
            fmt::note(&format!(
                "The new version is live, but the previous version's tasks were not seen to finish{why}."
            ));
            Ok(())
        }
        _ => Ok(()),
    }
}

fn wait(c: &Client, s: &Svc, id: &str, svc: &str, timeout: Duration, interval: Duration) -> Result<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let v = c.get(&s.path(&format!("/switches/{id}")), &[])?;
        if matches!(state_of(&v), "completed" | "drain_timed_out" | "failed") {
            return Ok(v);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(CliError::failed(format!(
                "switch {id} still {} after {}; it goes on without this command (follow it with: 21ci switch status {svc} {id})",
                state_of(&v),
                humantime::format_duration(timeout)
            )));
        }
        std::thread::sleep(interval.min(deadline - now));
    }
}
