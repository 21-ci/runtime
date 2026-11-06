//! `21ci data`: `export`, a consistent SQLite snapshot (scope `data:admin`); `tables`, `query`, `kv`
//! and `blobs`, the read-only data browser (scope `data:read`).

use std::io::IsTerminal;

use super::{resolve, save_download, App};
use crate::cli::DataCmd;
use crate::client::net_error;
use crate::error::{CliError, Result};

pub fn run(app: &App, cmd: &DataCmd) -> Result<()> {
    match cmd {
        DataCmd::Events { svc, set, off } => {
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let path = s.path("/data-events");
            let body = if let Some(file) = set {
                let text =
                    std::fs::read_to_string(file).map_err(|e| CliError::usage(format!("{}: {e}", file.display())))?;
                let v: serde_json::Value =
                    serde_json::from_str(&text).map_err(|e| CliError::usage(format!("{}: {e}", file.display())))?;
                c.put(&path, &v)?
            } else if *off {
                c.delete(&path, &[])?;
                c.get(&path, &[])?
            } else {
                c.get(&path, &[])?
            };
            crate::fmt::print_json(&body);
            Ok(())
        }
        DataCmd::Tables { svc } => {
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            crate::fmt::print_json(&c.get(&s.path("/data/tables"), &[])?);
            Ok(())
        }
        DataCmd::Query { svc, sql, args, limit } => {
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let mut body = serde_json::json!({"sql": sql});
            if let Some(a) = args {
                body["args"] = serde_json::from_str(a).map_err(|_| CliError::usage("--args is a JSON array"))?;
            }
            if let Some(l) = limit {
                body["limit"] = (*l).into();
            }
            crate::fmt::print_json(&c.post(&s.path("/data/query"), &body)?);
            Ok(())
        }
        DataCmd::Kv {
            svc, key: Some(key), ..
        } => {
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            crate::fmt::print_json(&c.get(&s.path(&format!("/data/kv/{key}")), &[])?);
            Ok(())
        }
        DataCmd::Kv {
            svc,
            prefix,
            cursor,
            limit,
            key: None,
        } => page(app, svc, "/data/kv", prefix, cursor, limit),
        DataCmd::Blobs {
            svc,
            prefix,
            cursor,
            limit,
        } => page(app, svc, "/data/blobs", prefix, cursor, limit),
        DataCmd::Export { svc, out } => {
            // a database on a terminal is never what anybody wants: refuse before the request
            // LCOV_EXCL_START: the test harness always pipes stdout, so a terminal cannot be attached
            if out.is_none() && std::io::stdout().is_terminal() {
                return Err(CliError::usage(
                    "the export is a binary SQLite file: pass --out <file> or redirect stdout",
                ));
            }
            // LCOV_EXCL_STOP
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let resp = c.send(c.download_req(&s.path("/data/export")))?;
            let bytes = resp.bytes().map_err(|e| net_error(&c.api, &e))?;
            save_download(app, &bytes, out.as_deref(), &format!("the database of {}", s.label))
        }
    }
}

fn page(
    app: &App,
    svc: &str,
    suffix: &str,
    prefix: &Option<String>,
    cursor: &Option<String>,
    limit: &Option<u32>,
) -> Result<()> {
    let c = app.client()?;
    let s = resolve(&c, svc)?;
    let mut q: Vec<(&str, String)> = Vec::new();
    if let Some(p) = prefix {
        q.push(("prefix", p.clone()));
    }
    if let Some(p) = cursor {
        q.push(("cursor", p.clone()));
    }
    if let Some(l) = limit {
        q.push(("limit", l.to_string()));
    }
    crate::fmt::print_json(&c.get(&s.path(suffix), &q)?);
    Ok(())
}
