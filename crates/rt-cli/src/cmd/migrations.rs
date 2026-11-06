//! `21ci migrations status|dry-run`, `21ci promote`: SQL migrations of a service.

use std::path::Path;

use serde_json::{json, Value};

use super::{cell, resolve, App};
use crate::cli::{MigrationsCmd, PromoteArgs};
use crate::error::{CliError, Result};
use crate::fmt;

fn names(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|m| {
            m.get("name")
                .and_then(Value::as_str)
                .or(m.as_str())
                .unwrap_or("?")
                .to_string()
        })
        .collect()
}

fn print_names(label: &str, list: &[String]) {
    let text = if list.is_empty() {
        "none".to_string()
    } else {
        list.join(", ")
    };
    fmt::emitln(&format!("{label}: {text}"));
}

pub fn run(app: &App, cmd: &MigrationsCmd) -> Result<()> {
    let c = app.client()?;
    match cmd {
        MigrationsCmd::Status { svc } => {
            let svc = resolve(&c, svc)?;
            let v = c.get(&svc.path("/migrations"), &[])?;
            if app.json {
                fmt::print_json(&v);
                return Ok(());
            }
            let rows: Vec<Vec<String>> = v
                .get("applied")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|m| vec![cell(m, "name"), cell(m, "version"), cell(m, "sha256")])
                .collect();
            fmt::emitln(&fmt::table(&["APPLIED", "VERSION", "SHA256"], &rows));
            print_names("Pending for the newest version", &names(v.get("pending_for_latest")));
            Ok(())
        }
    }
}

/// `PUT …/live`: migrate and flip, or with `--dry-run` try the migrations on a copy of the data.
pub fn promote(app: &App, a: &PromoteArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let mut body = json!({"version": a.number, "close_sockets": a.close_sockets});
    if a.dry_run {
        body["dry_run"] = json!(true);
    }
    if a.no_migrate {
        body["migrate"] = json!(false);
    }
    let v = c.put(&svc.path("/live"), &body)?;
    if app.json {
        fmt::print_json(&v);
    } else if a.dry_run {
        print_names("Would run", &names(v.get("pending_migrations")));
    } else {
        print_names("Migrations applied", &names(v.get("migrations_applied")));
        fmt::emitln(&format!("Live: {}", a.number));
    }
    Ok(())
}

/// The `.sql` files of `dir` as the body of `POST …/versions/{v}/migrations`.
pub fn read_dir(dir: &Path) -> Result<Value> {
    let bad = |e: std::io::Error| CliError::usage(format!("{}: {e}", dir.display()));
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(bad)? {
        let path = entry.map_err(bad)?.path();
        let Some(name) = path
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| n.ends_with(".sql"))
        else {
            continue;
        };
        let sql = std::fs::read_to_string(&path).map_err(bad)?;
        files.push(json!({"name": name, "sql": sql}));
    }
    if files.is_empty() {
        return Err(CliError::usage(format!("no .sql files in {}", dir.display())));
    }
    Ok(json!({"files": files}))
}
