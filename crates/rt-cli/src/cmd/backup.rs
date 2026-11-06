//! `21ci backup create | list | status | get | download | delete` (operator token).

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};

use super::tenant::bytes;
use super::{cell, fetch_page, print_page, App};
use crate::cli::BackupCmd;
use crate::client::net_error;
use crate::error::{CliError, Result};
use crate::fmt;

const POLL: Duration = Duration::from_millis(1000);

fn path(id: &str) -> Result<String> {
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(CliError::usage(format!("not a backup id: {id:?}")));
    }
    Ok(format!("/v1/node/backups/{id}"))
}

/// `bytes` for people; `-` while a backup has none yet. `--json` keeps the raw number.
fn size(b: &Value) -> String {
    b.get("bytes").and_then(Value::as_u64).map_or_else(|| "-".into(), bytes)
}

fn list_row(b: &Value) -> Vec<String> {
    vec![
        cell(b, "id"),
        cell(b, "state"),
        cell(b, "trigger"),
        size(b),
        fmt::ts_field(b, "started_at"),
    ]
}

fn show(app: &App, b: &Value) {
    if app.json {
        fmt::print_json(b);
        return;
    }
    let failure = b.get("failure").filter(|f| !f.is_null());
    let mut rows = vec![
        ("ID", cell(b, "id")),
        ("State", cell(b, "state")),
        ("Trigger", cell(b, "trigger")),
        ("Size", size(b)),
        ("Started", fmt::ts_field(b, "started_at")),
        ("Finished", fmt::ts_field(b, "finished_at")),
        ("SHA-256", cell(b, "sha256")),
    ];
    if let Some(f) = failure {
        rows.push(("Failure", fmt::compact(f)));
    }
    fmt::emit(&fmt::kv(&rows));
}

pub fn run(app: &App, cmd: &BackupCmd) -> Result<()> {
    let c = app.client()?;
    match cmd {
        BackupCmd::Create { wait } => {
            let started = c.send_json(c.req(Method::POST, "/v1/node/backups"))?.body;
            if !wait {
                if app.json {
                    fmt::print_json(&started);
                } else {
                    fmt::emitln(&format!("Backup {} started.", cell(&started, "id")));
                }
                return Ok(());
            }
            let p = path(&cell(&started, "id"))?;
            loop {
                let b = c.get(&p, &[])?;
                match b.get("state").and_then(Value::as_str) {
                    Some("ok") => {
                        show(app, &b);
                        return Ok(());
                    }
                    Some("failed") => {
                        show(app, &b);
                        return Err(CliError::failed("the backup failed"));
                    }
                    _ => std::thread::sleep(POLL),
                }
            }
        }
        BackupCmd::List(args) => {
            let page = fetch_page(&c, "/v1/node/backups", args)?;
            print_page(
                app,
                &page,
                "backups",
                &["ID", "STATE", "TRIGGER", "SIZE", "STARTED"],
                list_row,
            );
        }
        BackupCmd::Status => {
            let s = c.get("/v1/node/backups/status", &[])?;
            if app.json {
                fmt::print_json(&s);
            } else {
                fmt::emit(&fmt::kv(&[
                    ("Enabled", cell(&s, "enabled")),
                    ("Schedule", cell(&s, "schedule")),
                    ("Last good", cell(&s, "last_ok_id")),
                    ("Last good at", fmt::ts_field(&s, "last_ok_at")),
                    (
                        "Last failure",
                        s.get("last_failure")
                            .filter(|f| !f.is_null())
                            .map_or_else(|| "-".into(), fmt::compact),
                    ),
                    ("Next at", fmt::ts_field(&s, "next_at")),
                    ("Running", cell(&s, "running_id")),
                ]));
            }
        }
        BackupCmd::Get { id } => show(app, &c.get(&path(id)?, &[])?),
        BackupCmd::Download { id, out } => {
            let mut resp = c.send(c.download_req(&format!("{}/archive", path(id)?)))?;
            // Streamed to the side; the file only appears once it is whole.
            let mut name = out.as_os_str().to_os_string();
            name.push(".part");
            let part = std::path::PathBuf::from(name);
            let fail = |e: std::io::Error| CliError::failed(format!("cannot write {}: {e}", part.display()));
            let mut f = {
                use std::os::unix::fs::OpenOptionsExt;
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&part)
                    .map_err(fail)?
            };
            let bytes = match resp.copy_to(&mut f) {
                Ok(n) => n,
                Err(e) => {
                    let _ = std::fs::remove_file(&part);
                    return Err(net_error(&c.api, &e));
                }
            };
            f.sync_all().map_err(fail)?;
            std::fs::rename(&part, out).map_err(fail)?;
            if app.json {
                fmt::print_json(&json!({"path": out.display().to_string(), "bytes": bytes}));
            } else {
                fmt::emitln(&format!("Wrote backup {id} to {} ({bytes} bytes).", out.display()));
            }
        }
        BackupCmd::Delete { id } => {
            c.delete(&path(id)?, &[])?;
            if app.json {
                fmt::print_json(&json!({"deleted": id}));
            } else {
                fmt::emitln(&format!("Deleted backup {id}."));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listed_backup_reads_as_a_date_and_a_size() {
        let b = json!({"id": "bkp_1", "state": "ok", "trigger": "manual", "bytes": 13_201_920, "started_at": 1_793_950_000_000_i64});
        let row = list_row(&b);
        assert_eq!(row[3], "12.6 MiB");
        assert_eq!(row[4], fmt::ts_s(1_793_950_000_000));
    }

    #[test]
    fn a_running_backup_has_no_size_yet() {
        let row = list_row(
            &json!({"id": "bkp_1", "state": "running", "trigger": "manual", "bytes": null, "started_at": null}),
        );
        assert_eq!((row[3].as_str(), row[4].as_str()), ("-", "-"));
    }
}
