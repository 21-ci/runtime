//! `21ci topic …` (stream topics), `21ci consumer …` (a service's stream consumers) and
//! `21ci pubsub publish`.
//!
//! A topic is named by its `tpc_…` id or by its name; a name resolves in the namespace given by
//! `--project` / `--env` (query on GET / PATCH / DELETE, body fields on POST).

use std::io::{BufRead, IsTerminal, Write};
use std::time::Duration;

use serde_json::{json, Map, Value};

use super::queue::read_body;
use super::{cell, resolve, split_pair, App};
use crate::cli::{ConsumerCmd, ConsumerDlqCmd, NsArgs, PubsubCmd, TopicCmd};
use crate::error::{CliError, Result};
use crate::fmt;

/// `--project` / `--env` as query parameters.
fn ns_query(ns: &NsArgs) -> Vec<(&'static str, String)> {
    let mut q = Vec::new();
    if let Some(p) = &ns.project {
        q.push(("project", p.clone()));
    }
    if let Some(e) = &ns.environment {
        q.push(("environment", e.clone()));
    }
    q
}

/// `--project` / `--env` as body fields.
fn ns_body(ns: &NsArgs, body: &mut Value) {
    for (k, v) in ns_query(ns) {
        body[k] = json!(v);
    }
}

fn topic_path(topic: &str, suffix: &str) -> String {
    format!("/v1/topics/{}{suffix}", fmt::enc(topic))
}

/// `--retention`: whole hours only.
fn retention_hours(d: Duration) -> Result<u64> {
    let secs = d.as_secs();
    if d.subsec_nanos() != 0 || secs == 0 || !secs.is_multiple_of(3600) {
        return Err(CliError::usage("retention must be a whole number of hours"));
    }
    Ok(secs / 3600)
}

/// Repeated `--header NAME=VALUE` as a JSON object (`None` when there is none).
fn headers_obj(headers: &[String]) -> Result<Option<Value>> {
    if headers.is_empty() {
        return Ok(None);
    }
    let mut m = Map::new();
    for h in headers {
        let (k, v) = split_pair(h)?;
        m.insert(k.to_string(), json!(v));
    }
    Ok(Some(Value::Object(m)))
}

fn body_cell(v: &Value) -> String {
    v.get("body").map(fmt::compact).unwrap_or_else(|| "-".into())
}

fn heads(t: &Value) -> String {
    match t.get("heads").and_then(Value::as_array) {
        Some(h) if !h.is_empty() => h
            .iter()
            .map(|p| format!("{}: {}..{}", cell(p, "partition"), cell(p, "first"), cell(p, "next")))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "-".into(),
    }
}

fn show_topic(app: &App, t: &Value) {
    if app.json {
        fmt::print_json(t);
        return;
    }
    let mut rows = vec![
        ("ID", cell(t, "id")),
        ("Name", cell(t, "name")),
        ("Project", cell(t, "project")),
        ("Environment", cell(t, "environment")),
        ("Partitions", cell(t, "partitions")),
        ("Retention", format!("{}h", cell(t, "retention_hours"))),
        ("Max MB", cell(t, "max_mb")),
        ("Bytes", cell(t, "bytes")),
        ("Heads", heads(t)),
        ("Created", fmt::ts_field(t, "created_at")),
    ];
    if let Some(cs) = t.get("consumers").and_then(Value::as_array) {
        let text = if cs.is_empty() {
            "none".to_string()
        } else {
            cs.iter()
                .map(|c| {
                    let who = c
                        .get("slug")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| cell(c, "service"));
                    format!("{who}/{} (lag {})", cell(c, "group"), cell(c, "lag"))
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        rows.push(("Consumers", text));
    }
    fmt::emit(&fmt::kv(&rows));
}

/// Ask on a terminal; refuse without `--yes` otherwise.
fn confirm(question: &str) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        return Err(CliError::usage(
            "refusing to delete without --yes (stdin is not a terminal)",
        ));
    }
    // LCOV_EXCL_START: needs an interactive terminal
    eprint!("{question} [y/N] ");
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    let _ = std::io::stdin().lock().read_line(&mut line);
    Ok(matches!(line.trim(), "y" | "Y" | "yes"))
    // LCOV_EXCL_STOP
}

pub fn run(app: &App, cmd: &TopicCmd) -> Result<()> {
    match cmd {
        TopicCmd::Create {
            name,
            ns,
            partitions,
            retention,
            max_mb,
        } => {
            let mut body = json!({"name": name});
            ns_body(ns, &mut body);
            if let Some(p) = partitions {
                body["partitions"] = json!(p);
            }
            if let Some(r) = retention {
                body["retention_hours"] = json!(retention_hours(*r)?);
            }
            if let Some(m) = max_mb {
                body["max_mb"] = json!(m);
            }
            let t = app.client()?.post("/v1/topics", &body)?;
            show_topic(app, &t);
        }
        TopicCmd::List { ns } => {
            let r = app.client()?.get("/v1/topics", &ns_query(ns))?;
            if app.json {
                fmt::print_json(&r);
                return Ok(());
            }
            let items = r.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
            if items.is_empty() {
                fmt::note("No topics found.");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|t| {
                    vec![
                        cell(t, "id"),
                        cell(t, "name"),
                        cell(t, "project"),
                        cell(t, "environment"),
                        cell(t, "partitions"),
                        format!("{}h", cell(t, "retention_hours")),
                        cell(t, "bytes"),
                    ]
                })
                .collect();
            fmt::emit(&fmt::table(
                &["ID", "NAME", "PROJECT", "ENV", "PARTITIONS", "RETENTION", "BYTES"],
                &rows,
            ));
        }
        TopicCmd::Get { topic, ns } => {
            let t = app.client()?.get(&topic_path(topic, ""), &ns_query(ns))?;
            show_topic(app, &t);
        }
        TopicCmd::Update {
            topic,
            ns,
            retention,
            max_mb,
        } => {
            let mut body = json!({});
            if let Some(r) = retention {
                body["retention_hours"] = json!(retention_hours(*r)?);
            }
            if let Some(m) = max_mb {
                body["max_mb"] = json!(m);
            }
            if body.as_object().is_some_and(Map::is_empty) {
                return Err(CliError::usage("nothing to change: give --retention or --max-mb"));
            }
            let c = app.client()?;
            let rb = c
                .req(reqwest::Method::PATCH, &topic_path(topic, ""))
                .query(&ns_query(ns))
                .json(&body);
            let t = c.send_json(rb)?.body;
            show_topic(app, &t);
        }
        TopicCmd::Delete { topic, ns, yes } => {
            if !*yes
                && !confirm(&format!(
                    "Delete topic {topic} with its records, cursors and dead letters?"
                ))?
            {
                return Err(CliError::failed("not deleted")); // LCOV_EXCL_LINE: needs an interactive terminal
            }
            app.client()?.delete(&topic_path(topic, ""), &ns_query(ns))?;
            if app.json {
                fmt::print_json(&json!({"deleted": topic}));
            } else {
                fmt::emitln(&format!("Topic {topic} deleted."));
            }
        }
        TopicCmd::Append {
            topic,
            ns,
            body,
            key,
            headers,
            dedup,
        } => {
            let mut rec = json!({"body": read_body(body)?});
            if let Some(k) = key {
                rec["key"] = json!(k);
            }
            if let Some(h) = headers_obj(headers)? {
                rec["headers"] = h;
            }
            if let Some(d) = dedup {
                rec["dedup_id"] = json!(d);
            }
            let mut b = json!({"records": [rec]});
            ns_body(ns, &mut b);
            let r = app.client()?.post(&topic_path(topic, "/messages"), &b)?;
            if app.json {
                fmt::print_json(&r);
                return Ok(());
            }
            for it in r.get("items").and_then(Value::as_array).into_iter().flatten() {
                let dup = if it.get("duplicate").and_then(Value::as_bool) == Some(true) {
                    " (duplicate: already appended)"
                } else {
                    ""
                };
                fmt::emitln(&format!(
                    "Appended to {topic}: partition {}, offset {}{dup}.",
                    cell(it, "partition"),
                    cell(it, "offset")
                ));
            }
        }
        TopicCmd::Read {
            topic,
            ns,
            partition,
            from,
            from_time,
            limit,
        } => {
            let mut q = ns_query(ns);
            if let Some(p) = partition {
                q.push(("partition", p.to_string()));
            }
            if let Some(f) = from {
                q.push(("from", f.clone()));
            }
            if let Some(t) = from_time {
                q.push(("from_time", t.clone()));
            }
            if let Some(l) = limit {
                q.push(("limit", l.to_string()));
            }
            let r = app.client()?.get(&topic_path(topic, "/messages"), &q)?;
            if app.json {
                fmt::print_json(&r);
                return Ok(());
            }
            let items = r.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
            if items.is_empty() {
                fmt::note("No records.");
            } else {
                let rows: Vec<Vec<String>> = items
                    .iter()
                    .map(|m| {
                        vec![
                            cell(m, "partition"),
                            cell(m, "offset"),
                            cell(m, "key"),
                            fmt::ts_field(m, "timestamp"),
                            body_cell(m),
                        ]
                    })
                    .collect();
                fmt::emit(&fmt::table(&["PARTITION", "OFFSET", "KEY", "TIMESTAMP", "BODY"], &rows));
            }
            if r.get("end").and_then(Value::as_bool) != Some(true) {
                if let Some(n) = r.get("next").filter(|n| !n.is_null()) {
                    fmt::note(&format!("(more records: --from {n})"));
                }
            }
        }
    }
    Ok(())
}

/// `earliest` | `latest` | an offset (sent as a number) | an RFC 3339 time.
fn reset_target(to: &str) -> Result<Value> {
    let t = to.trim();
    if t == "earliest" || t == "latest" {
        return Ok(json!(t));
    }
    if let Ok(n) = t.parse::<u64>() {
        return Ok(json!(n));
    }
    if humantime::parse_rfc3339(t).is_ok() {
        return Ok(json!(t));
    }
    Err(CliError::usage(format!(
        "invalid --to `{to}`: use earliest, latest, an offset or an RFC 3339 time"
    )))
}

fn consumer_path(topic: &str, group: &str, suffix: &str) -> String {
    format!("/consumers/{}/{}{suffix}", fmt::enc(topic), fmt::enc(group))
}

/// One row per partition of a consumer view.
fn consumer_rows(c: &Value) -> Vec<Vec<String>> {
    let parts = c
        .get("partitions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if parts.is_empty() {
        let note = if c.get("topic_exists").and_then(Value::as_bool) == Some(false) {
            "(no topic)"
        } else {
            "-"
        };
        return vec![vec![
            cell(c, "topic"),
            cell(c, "group"),
            note.into(),
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
            cell(c, "dead"),
        ]];
    }
    parts
        .iter()
        .map(|p| {
            vec![
                cell(c, "topic"),
                cell(c, "group"),
                cell(p, "partition"),
                cell(p, "committed"),
                cell(p, "next"),
                cell(p, "lag"),
                cell(p, "state"),
                cell(c, "dead"),
            ]
        })
        .collect()
}

const CONSUMER_HEADERS: [&str; 8] = [
    "TOPIC",
    "GROUP",
    "PARTITION",
    "COMMITTED",
    "NEXT",
    "LAG",
    "STATE",
    "DEAD",
];

pub fn consumer(app: &App, cmd: &ConsumerCmd) -> Result<()> {
    match cmd {
        ConsumerCmd::List(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let r = c.get(&svc.path("/consumers"), &[])?;
            if app.json {
                fmt::print_json(&r);
                return Ok(());
            }
            let items = r.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
            if items.is_empty() {
                fmt::note(&format!("{} has no stream consumers.", svc.label));
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items.iter().flat_map(consumer_rows).collect();
            fmt::emit(&fmt::table(&CONSUMER_HEADERS, &rows));
        }
        ConsumerCmd::Reset {
            svc,
            topic,
            group,
            to,
            partition,
        } => {
            let mut body = json!({"to": reset_target(to)?});
            if let Some(p) = partition {
                body["partition"] = json!(p);
            }
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let r = c.post(&svc.path(&consumer_path(topic, group, "/reset")), &body)?;
            if app.json {
                fmt::print_json(&r);
            } else {
                fmt::emitln(&format!("Reset {topic}/{group} of {} to {to}.", svc.label));
                fmt::emit(&fmt::table(&CONSUMER_HEADERS, &consumer_rows(&r)));
            }
        }
        ConsumerCmd::Dlq { cmd } => match cmd {
            ConsumerDlqCmd::List {
                svc,
                topic,
                group,
                limit,
            } => {
                let c = app.client()?;
                let svc = resolve(&c, svc)?;
                let q: Vec<(&str, String)> = limit.iter().map(|l| ("limit", l.to_string())).collect();
                let r = c.get(&svc.path(&consumer_path(topic, group, "/dlq")), &q)?;
                if app.json {
                    fmt::print_json(&r);
                    return Ok(());
                }
                let items = r.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
                if items.is_empty() {
                    fmt::note("No dead letters.");
                    return Ok(());
                }
                let rows: Vec<Vec<String>> = items
                    .iter()
                    .map(|d| {
                        vec![
                            cell(d, "partition"),
                            cell(d, "offset"),
                            cell(d, "attempts"),
                            cell(d, "error"),
                            fmt::ts_field(d, "failed_at"),
                            body_cell(d),
                        ]
                    })
                    .collect();
                fmt::emit(&fmt::table(
                    &["PARTITION", "OFFSET", "ATTEMPTS", "ERROR", "FAILED_AT", "BODY"],
                    &rows,
                ));
            }
            ConsumerDlqCmd::Purge { svc, topic, group } => {
                let c = app.client()?;
                let svc = resolve(&c, svc)?;
                c.delete(&svc.path(&consumer_path(topic, group, "/dlq")), &[])?;
                if app.json {
                    fmt::print_json(&json!({"purged": true}));
                } else {
                    fmt::emitln(&format!("Purged the dead letters of {topic}/{group} of {}.", svc.label));
                }
            }
        },
    }
    Ok(())
}

pub fn pubsub(app: &App, cmd: &PubsubCmd) -> Result<()> {
    match cmd {
        PubsubCmd::Publish {
            subject,
            body,
            headers,
            ns,
        } => {
            let mut b = json!({"subject": subject, "body": read_body(body)?});
            if let Some(h) = headers_obj(headers)? {
                b["headers"] = h;
            }
            ns_body(ns, &mut b);
            let r = app.client()?.post("/v1/pubsub/publish", &b)?;
            if app.json {
                fmt::print_json(&r);
            } else {
                fmt::emitln(&format!(
                    "receivers {}, dropped {}",
                    r.get("receivers").and_then(Value::as_u64).unwrap_or(0),
                    r.get("dropped").and_then(Value::as_u64).unwrap_or(0)
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_is_whole_hours() {
        assert_eq!(retention_hours(Duration::from_secs(7200)).unwrap(), 2);
        assert_eq!(retention_hours(Duration::from_secs(7 * 86_400)).unwrap(), 168);
        assert!(retention_hours(Duration::from_secs(90 * 60)).is_err());
        assert!(retention_hours(Duration::ZERO).is_err());
        assert!(retention_hours(Duration::from_millis(3_600_500)).is_err());
    }

    #[test]
    fn reset_targets() {
        assert_eq!(reset_target("earliest").unwrap(), json!("earliest"));
        assert_eq!(reset_target("42").unwrap(), json!(42));
        assert_eq!(
            reset_target("2026-01-01T00:00:00Z").unwrap(),
            json!("2026-01-01T00:00:00Z")
        );
        assert_eq!(reset_target("soon").unwrap_err().exit_code(), 2);
    }
}
