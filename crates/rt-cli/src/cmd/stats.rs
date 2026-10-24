//! `21ci stats` / `21ci service stats`: request statistics plus the queueing / autoscaler state.

use serde_json::Value;

use super::{resolve, App};
use crate::cli::StatsArgs;
use crate::error::Result;
use crate::fmt;

fn n(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn ms(p: &Value, key: &str) -> String {
    p.get(key)
        .and_then(Value::as_f64)
        .map(|f| format!("{f:.1}"))
        .unwrap_or_else(|| "-".into())
}

fn num(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::Number(n)) => n.to_string(),
        _ => "-".into(),
    }
}

/// `limit 8 (1..64), running 3, queued 0 (max 256)`: the adaptive limit and what runs and waits.
pub fn concurrency(adm: &Value) -> String {
    if adm.is_null() {
        return "- (no admission state: the service has not run recently)".into();
    }
    format!(
        "limit {} ({}..{}), running {}, queued {} (max {})",
        num(adm, "limit"),
        num(adm, "min"),
        num(adm, "max"),
        num(adm, "running"),
        num(adm, "queued"),
        num(adm, "max_queued")
    )
}

/// The `admission` rows of `stats` (nothing when the server sends none).
fn admission_rows(s: &Value) -> Vec<(&'static str, String)> {
    let Some(adm) = s.get("admission") else {
        return Vec::new();
    };
    let mut rows = vec![("Concurrency", concurrency(adm))];
    if !adm.is_null() {
        let baseline = adm
            .get("latency_baseline_ms")
            .and_then(Value::as_f64)
            .map(|b| format!("{b:.1} ms"))
            .unwrap_or_else(|| "-".into());
        if let Some(instances) = instances(adm) {
            rows.push(("Instances", instances));
        }
        let mut wait = format!(
            "{:.1} ms (timeouts {})",
            adm.get("queue_wait_ms").and_then(Value::as_f64).unwrap_or(0.0),
            num(adm, "queue_timeouts")
        );
        if let Some(p) = adm.get("pred_ms").and_then(Value::as_f64) {
            wait.push_str(&format!(", predicted run {}", run_time(p)));
            if let Some(b) = adm.get("pred_background_ms").and_then(Value::as_f64) {
                wait.push_str(&format!(" (background {})", run_time(b)));
            }
        }
        rows.push(("Queue wait", wait));
        let rate = adm
            .get("rps")
            .and_then(Value::as_f64)
            .map(|r| format!("{r:.1} req/s, "))
            .unwrap_or_default();
        rows.push((
            "Autoscaler",
            format!(
                "demand {:.1}, latency baseline {baseline}, warm {}/proc, {rate}idle {}",
                adm.get("demand").and_then(Value::as_f64).unwrap_or(0.0),
                num(adm, "warm_per_proc"),
                crate::error::human_secs(adm.get("idle_ms").and_then(Value::as_u64).unwrap_or(0) / 1000)
            ),
        ));
        if let Some(change) = last_change(adm) {
            rows.push(("Last change", change));
        }
    }
    rows
}

/// A predicted run time: `4.3 ms` below a second, `2.1 s` from there.
fn run_time(ms: f64) -> String {
    if ms < 1000.0 {
        format!("{ms:.1} ms")
    } else {
        format!("{:.1} s", ms / 1000.0)
    }
}

/// `3 running, min 2, max 64, warm target 4` (nothing when the server sends no instance fields).
fn instances(adm: &Value) -> Option<String> {
    adm.get("min_instances")?;
    Some(format!(
        "{} running, min {}, max {}, warm target {}",
        num(adm, "instances"),
        num(adm, "min_instances"),
        num(adm, "max_instances"),
        num(adm, "target_instances")
    ))
}

/// `trend, limit 4 -> 8, 3s ago`, or `none` when the limit never moved (nothing when the server
/// does not report it).
fn last_change(adm: &Value) -> Option<String> {
    let c = adm.get("last_change")?;
    if c.is_null() {
        return Some("none".into());
    }
    Some(format!(
        "{}, limit {} -> {}, {} ago",
        c.get("reason").and_then(Value::as_str).unwrap_or("-"),
        num(c, "from"),
        num(c, "to"),
        crate::error::human_secs(c.get("ago_ms").and_then(Value::as_u64).unwrap_or(0) / 1000)
    ))
}

pub fn run(app: &App, a: &StatsArgs) -> Result<()> {
    let c = app.client()?;
    let svc = resolve(&c, &a.svc)?;
    let query: Vec<(&str, String)> = a.window.iter().map(|w| ("window", w.clone())).collect();
    let s = c.get(&svc.path("/stats"), &query)?;
    if app.json {
        fmt::print_json(&s);
        return Ok(());
    }
    let (count, errors) = (n(&s, "count"), n(&s, "errors"));
    let rate = s.get("error_rate").and_then(Value::as_f64).unwrap_or(if count == 0 {
        0.0
    } else {
        errors as f64 / count as f64
    });
    let outcomes = s
        .get("outcomes")
        .and_then(Value::as_object)
        .filter(|o| !o.is_empty())
        .map(|o| o.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|| "-".into());
    let mut rows = vec![
        ("Window", a.window.clone().unwrap_or_else(|| "5m".into())),
        ("Requests", count.to_string()),
        ("Errors", format!("{errors} ({:.2}%)", rate * 100.0)),
        ("Outcomes", outcomes),
    ];
    rows.extend(admission_rows(&s));
    fmt::emit(&fmt::kv(&rows));
    let rows: Vec<Vec<String>> = ["queue", "init", "exec", "total"]
        .iter()
        .filter_map(|phase| {
            s.get(*phase)
                .filter(|p| p.is_object())
                .map(|p| vec![(*phase).to_string(), ms(p, "p50_ms"), ms(p, "p95_ms"), ms(p, "p99_ms")])
        })
        .collect();
    if !rows.is_empty() {
        fmt::emitln("");
        fmt::emit(&fmt::table(&["PHASE", "P50_MS", "P95_MS", "P99_MS"], &rows));
    }
    Ok(())
}
