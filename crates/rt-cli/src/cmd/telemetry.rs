//! `21ci telemetry show`: the node's next anonymous report (`GET /v1/telemetry/preview`).

use serde_json::Value;

use super::App;
use crate::cli::TelemetryCmd;
use crate::error::Result;
use crate::fmt;

pub fn run(app: &App, cmd: &TelemetryCmd) -> Result<()> {
    match cmd {
        TelemetryCmd::Show => {
            let p = app.client()?.get("/v1/telemetry/preview", &[])?;
            if app.json {
                fmt::print_json(&p);
                return Ok(());
            }
            let sending = p.get("sending").and_then(Value::as_bool).unwrap_or(false);
            let url = p.get("url").and_then(Value::as_str).unwrap_or("-");
            fmt::emitln(&format!("Sending: {}", if sending { "on" } else { "off" }));
            fmt::emitln(&format!("URL: {url}"));
            fmt::print_json(p.get("report").unwrap_or(&Value::Null));
            Ok(())
        }
    }
}
