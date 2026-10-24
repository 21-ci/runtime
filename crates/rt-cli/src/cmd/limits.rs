//! `21ci limits`: the plan limits of the caller's tenant and what is left of them today
//! (`GET /v1/limits`).

use serde_json::Value;

use super::tenant::bytes;
use super::App;
use crate::error::Result;
use crate::fmt;

/// A quota's number: `unlimited` for a `null` limit, plain text otherwise (`as_size`: bytes).
fn num(v: &Value, as_size: bool) -> String {
    match v.as_u64() {
        None => "unlimited".into(),
        Some(n) if as_size => bytes(n),
        Some(n) => n.to_string(),
    }
}

pub fn run(app: &App) -> Result<()> {
    let l = app.client()?.get("/v1/limits", &[])?;
    if app.json {
        fmt::print_json(&l);
        return Ok(());
    }
    let resets = l
        .get("resets_at")
        .and_then(Value::as_i64)
        .map_or_else(|| "-".to_string(), fmt::ts_s);
    fmt::emit(&fmt::kv(&[
        ("Plan", fmt::str_field(&l, "plan")),
        (
            "Day",
            format!("{} (daily counters reset {resets})", fmt::str_field(&l, "day")),
        ),
    ]));
    let rows: Vec<Vec<String>> = [
        ("invocations", "invocations", false),
        ("egress", "egress_bytes", true),
        ("storage", "storage_bytes", true),
        ("services", "services", false),
        ("schedules", "schedules", false),
    ]
    .iter()
    .map(|(label, key, size)| {
        let q = l.get(*key).unwrap_or(&Value::Null);
        let used = q
            .get("used")
            .and_then(Value::as_u64)
            .map_or_else(|| "-".to_string(), |u| num(&Value::from(u), *size));
        vec![
            (*label).to_string(),
            num(q.get("limit").unwrap_or(&Value::Null), *size),
            used,
            num(q.get("remaining").unwrap_or(&Value::Null), *size),
            fmt::str_field(q, "period"),
        ]
    })
    .collect();
    fmt::emitln("");
    fmt::emit(&fmt::table(&["QUOTA", "LIMIT", "USED", "LEFT", "PERIOD"], &rows));
    Ok(())
}
