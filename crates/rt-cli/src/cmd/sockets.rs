//! `21ci sockets ls | close` and `21ci objects ls | rm`: a service's open WebSockets
//! and its objects (`/v1/services/{s}/objects…`).

use serde_json::Value;

use super::{cell, resolve, App};
use crate::cli::{ObjectsCmd, SocketsCmd};
use crate::client::Client;
use crate::error::Result;
use crate::fmt;

/// Every page of a listing (`items` + `next_cursor`).
fn all_pages(c: &Client, path: &str, mut q: Vec<(&str, String)>) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    loop {
        let page = c.get(path, &q)?;
        out.extend(page.get("items").and_then(Value::as_array).cloned().unwrap_or_default());
        match page.get("next_cursor").and_then(Value::as_str) {
            Some(cur) => {
                q.retain(|(k, _)| *k != "cursor");
                q.push(("cursor", cur.to_string()));
            }
            None => return Ok(out),
        }
    }
}

fn tags(v: &Value) -> String {
    let t: Vec<&str> = v
        .get("tags")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if t.is_empty() {
        "-".into()
    } else {
        t.join(",")
    }
}

pub fn run(app: &App, cmd: &SocketsCmd) -> Result<()> {
    let c = app.client()?;
    match cmd {
        SocketsCmd::Ls(a) => {
            let svc = resolve(&c, &a.svc)?;
            let mut q = Vec::new();
            if let Some(t) = &a.tag {
                q.push(("tag", t.clone()));
            }
            let items = all_pages(&c, &svc.path(&format!("/objects/{}/sockets", fmt::enc(&a.object))), q)?;
            if app.json {
                fmt::print_json(&Value::Array(items));
                return Ok(());
            }
            if items.is_empty() {
                fmt::note(&format!("No open sockets on object {} of {}.", a.object, svc.label));
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|s| {
                    vec![
                        cell(s, "id"),
                        tags(s),
                        cell(s, "protocol"),
                        fmt::ts_field(s, "created_at"),
                        fmt::ts_field(s, "last_message_at"),
                        cell(s, "buffered_amount"),
                        cell(s, "remote_ip"),
                    ]
                })
                .collect();
            fmt::emit(&fmt::table(
                &[
                    "ID",
                    "TAGS",
                    "PROTOCOL",
                    "CREATED",
                    "LAST MESSAGE",
                    "BUFFERED",
                    "CLIENT",
                ],
                &rows,
            ));
        }
        SocketsCmd::Close(a) => {
            let svc = resolve(&c, &a.svc)?;
            let mut q = Vec::new();
            if let Some(code) = a.code {
                q.push(("code", code.to_string()));
            }
            if let Some(r) = &a.reason {
                q.push(("reason", r.clone()));
            }
            let base = svc.path(&format!("/objects/{}/sockets", fmt::enc(&a.object)));
            match (&a.socket, &a.tag) {
                (Some(ws), _) => {
                    c.delete(&format!("{base}/{}", fmt::enc(ws)), &q)?;
                    fmt::emitln(&format!("Closed socket {ws}."));
                }
                (None, Some(tag)) => {
                    q.push(("tag", tag.clone()));
                    let r = c.send_json(c.req(reqwest::Method::DELETE, &base).query(&q))?.body;
                    if app.json {
                        fmt::print_json(&r);
                    } else {
                        let n = r.get("closed").and_then(Value::as_u64).unwrap_or(0);
                        fmt::emitln(&format!("Closed {n} sockets of object {} with tag {tag}.", a.object));
                    }
                }
                (None, None) => unreachable!("clap requires a socket or --tag"), // LCOV_EXCL_LINE: clap rejects the call before this match
            }
        }
    }
    Ok(())
}

pub fn objects(app: &App, cmd: &ObjectsCmd) -> Result<()> {
    let c = app.client()?;
    match cmd {
        ObjectsCmd::Ls(a) => {
            let svc = resolve(&c, &a.svc)?;
            let mut q = Vec::new();
            if let Some(p) = &a.prefix {
                q.push(("prefix", p.clone()));
            }
            let items = all_pages(&c, &svc.path("/objects"), q)?;
            if app.json {
                fmt::print_json(&Value::Array(items));
                return Ok(());
            }
            if items.is_empty() {
                fmt::note(&format!("No objects with sockets or storage on {}.", svc.label));
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|o| {
                    let storage = if o.get("storage").and_then(Value::as_bool) == Some(true) {
                        "yes"
                    } else {
                        "no"
                    };
                    vec![cell(o, "name"), cell(o, "sockets"), storage.to_string()]
                })
                .collect();
            fmt::emit(&fmt::table(&["NAME", "SOCKETS", "STORAGE"], &rows));
        }
        ObjectsCmd::Rm { svc, name } => {
            let svc = resolve(&c, svc)?;
            c.delete(&svc.path(&format!("/objects/{}", fmt::enc(name))), &[])?;
            fmt::emitln(&format!("Dropped the storage of object {name} of {}.", svc.label));
        }
    }
    Ok(())
}
