//! `21ci secret set | list | delete`. Values are write-only: never printed, never listed.

use std::io::{IsTerminal, Read};
use std::path::Path;

use reqwest::Method;
use serde_json::json;

use super::{cell, resolve, App};
use crate::cli::SecretCmd;
use crate::error::{CliError, Result};
use crate::fmt;

/// `--from-file`: the file verbatim. Otherwise stdin, minus one trailing newline.
fn read_value(from_file: Option<&Path>) -> Result<String> {
    let value = match from_file {
        Some(p) => std::fs::read_to_string(p).map_err(|e| CliError::usage(format!("{}: {e}", p.display())))?,
        None => {
            let mut stdin = std::io::stdin();
            if stdin.is_terminal() {
                return Err(CliError::usage(
                    "no secret value: pipe it on stdin or use --from-file <file>",
                ));
            }
            let mut s = String::new();
            stdin
                .read_to_string(&mut s)
                .map_err(|e| CliError::usage(format!("cannot read stdin: {e}")))?;
            if let Some(t) = s.strip_suffix('\n') {
                s = t.strip_suffix('\r').unwrap_or(t).to_string();
            }
            s
        }
    };
    if value.is_empty() {
        return Err(CliError::usage(
            "the secret value is empty: pipe it on stdin or use --from-file <file>",
        ));
    }
    Ok(value)
}

pub fn run(app: &App, cmd: &SecretCmd) -> Result<()> {
    match cmd {
        SecretCmd::Set { svc, name, from_file } => {
            let value = read_value(from_file.as_deref())?;
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            let path = svc.path(&format!("/secrets/{}", fmt::enc(name)));
            c.send(c.req(Method::PUT, &path).json(&json!({"value": value})))?;
            if app.json {
                fmt::print_json(&json!({"service": svc.id, "secret": name, "set": true}));
            } else {
                fmt::emitln(&format!("Secret {name} set on {}.", svc.label));
            }
        }
        SecretCmd::List(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let body = c.get(&svc.path("/secrets"), &[])?;
            if app.json {
                fmt::print_json(&body);
                return Ok(());
            }
            let items = body.as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                fmt::note("No secrets set.");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|s| vec![cell(s, "name"), fmt::ts_field(s, "updated_at")])
                .collect();
            fmt::emit(&fmt::table(&["NAME", "UPDATED"], &rows));
        }
        SecretCmd::Delete { svc, name } => {
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            c.delete(&svc.path(&format!("/secrets/{}", fmt::enc(name))), &[])?;
            if app.json {
                fmt::print_json(&json!({"deleted": name}));
            } else {
                fmt::emitln(&format!("Secret {name} deleted from {}.", svc.label));
            }
        }
    }
    Ok(())
}
