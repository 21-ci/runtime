//! `21ci data export`: a consistent SQLite snapshot of a service's database (scope `data:admin`).

use std::io::IsTerminal;

use super::{resolve, save_download, App};
use crate::cli::DataCmd;
use crate::client::net_error;
use crate::error::{CliError, Result};

pub fn run(app: &App, cmd: &DataCmd) -> Result<()> {
    match cmd {
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
