//! `21ci node status | scheduler | cache | drain | config get | config set` (operator token).

use reqwest::Method;
use serde_json::{json, Map, Value};

use super::{cell, parse_json_arg, split_pair, App};
use crate::cli::{NodeCmd, NodeConfigCmd};
use crate::error::{CliError, Result};
use crate::fmt;

pub fn run(app: &App, cmd: &NodeCmd) -> Result<()> {
    match cmd {
        NodeCmd::Status => {
            let s = app.client()?.get("/v1/node/status", &[])?;
            if app.json {
                fmt::print_json(&s);
                return Ok(());
            }
            let kinds = s
                .get("kinds")
                .and_then(Value::as_object)
                .map(|o| o.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" "))
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| "-".into());
            fmt::emit(&fmt::kv(&[
                ("Version", cell(&s, "version")),
                ("Mode", cell(&s, "mode")),
                ("Default profile", cell(&s, "default_profile")),
                ("V8 sandbox", cell(&s, "v8_sandbox")),
                ("Processors", cell(&s, "processors")),
                ("Kinds", kinds),
                ("In flight", cell(&s, "inflight")),
                (
                    "Memory",
                    s.get("memory").map(fmt::compact).unwrap_or_else(|| "-".into()),
                ),
            ]));
        }
        NodeCmd::Scheduler => {
            let s = app.client()?.get("/v1/node/scheduler", &[])?;
            if app.json {
                fmt::print_json(&s);
            } else {
                scheduler(&s);
            }
        }
        // the cache snapshot has no fixed shape: JSON in both modes
        NodeCmd::Cache => fmt::print_json(&app.client()?.get("/v1/node/cache", &[])?),
        NodeCmd::Drain => {
            let c = app.client()?;
            c.send(c.req(Method::POST, "/v1/node/drain"))?;
            if app.json {
                fmt::print_json(&json!({"draining": true}));
            } else {
                fmt::emitln("Node is draining.");
            }
        }
        NodeCmd::Config {
            cmd: NodeConfigCmd::Get { key },
        } => {
            let cfg = app.client()?.get("/v1/node/config", &[])?;
            let picked = match key {
                None => cfg,
                Some(k) => k
                    .split('.')
                    .try_fold(&cfg, |v, part| v.get(part))
                    .cloned()
                    .ok_or_else(|| CliError::not_found(format!("no config key `{k}`")))?,
            };
            if app.json {
                fmt::print_json(&picked);
            } else if picked.is_object() {
                fmt::emit(&fmt::flat_table(&picked));
            } else {
                // one leaf: just its value
                fmt::emitln(&fmt::flatten(&picked).into_iter().map(|(_, v)| v).collect::<String>());
            }
        }
        NodeCmd::Config {
            cmd: NodeConfigCmd::Set { pairs },
        } => {
            let body = config_body(pairs)?;
            let r = app.client()?.put("/v1/node/config", &body)?;
            if app.json {
                fmt::print_json(&r);
                return Ok(());
            }
            let list = |key: &str| -> Vec<String> {
                r.get(key)
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                    .unwrap_or_default()
            };
            let (applied, pending) = (list("applied"), list("pending_restart"));
            if !applied.is_empty() {
                fmt::emitln(&format!("Applied: {}", applied.join(", ")));
            }
            if !pending.is_empty() {
                fmt::emitln(&format!("Pending restart: {}", pending.join(", ")));
            }
            if applied.is_empty() && pending.is_empty() {
                fmt::emitln("Nothing to apply.");
            }
        }
    }
    Ok(())
}

fn num(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// `GET /v1/node/scheduler` (the schedtrace snapshot) as a summary plus one row per processor.
fn scheduler(s: &Value) {
    let empty = Vec::new();
    let procs = s.get("procs").and_then(Value::as_array).unwrap_or(&empty);
    let machines = s.get("machines").and_then(Value::as_array).unwrap_or(&empty);
    let grq = s
        .get("grq")
        .and_then(Value::as_object)
        .map(|o| o.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" "))
        .filter(|g| !g.is_empty())
        .unwrap_or_else(|| "-".into());
    let lrq: u64 = procs.iter().map(|p| num(p, "lrq_len")).sum();
    let count = |key: &str| {
        machines
            .iter()
            .filter(|m| m.get(key).and_then(Value::as_bool) == Some(true))
            .count()
    };
    let bound: u64 = machines.iter().map(|m| num(m, "bound_count")).sum();
    let mut summary = vec![
        ("Uptime", crate::error::human_secs(num(s, "uptime_ms") / 1000)),
        ("Mode", cell(s, "mode")),
        ("In flight", num(s, "inflight").to_string()),
        (
            "Tasks",
            format!(
                "{} submitted, {} completed",
                num(s, "tasks_submitted"),
                num(s, "tasks_completed")
            ),
        ),
        ("Global queues", grq),
        ("Local queues", lrq.to_string()),
        (
            "Waiting",
            format!(
                "runtime {}, memory {}",
                num(s, "waiting_runtime"),
                num(s, "waiting_memory")
            ),
        ),
        (
            "Processors",
            format!(
                "{} (idle {}, spinning {})",
                procs.len(),
                num(s, "idle"),
                num(s, "spinning")
            ),
        ),
        (
            "Machines",
            format!(
                "{} (parked {}, spinning {}, bound tasks {bound})",
                machines.len(),
                count("parked"),
                count("spinning")
            ),
        ),
    ];
    if let Some(a) = s.get("admission").filter(|a| a.is_object()) {
        summary.push((
            "Admission",
            format!(
                "queued {} (max {}), max in flight {}",
                num(a, "queued"),
                num(a, "max_queued"),
                num(a, "max_inflight")
            ),
        ));
    }
    fmt::emit(&fmt::kv(&summary));
    if !procs.is_empty() {
        procs_table(procs);
    }
    if let Some(services) = s.get("services").and_then(Value::as_array).filter(|a| !a.is_empty()) {
        services_table(services);
    }
}

/// Per-service queueing and autoscaler state (`services` of `GET /v1/node/scheduler`).
fn services_table(services: &[Value]) {
    let rows: Vec<Vec<String>> = services
        .iter()
        .map(|e| {
            let name = e
                .get("slug")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| cell(e, "service_id"));
            vec![
                name,
                cell(e, "kind"),
                format!("{} ({}..{})", num(e, "limit"), num(e, "min"), num(e, "max")),
                num(e, "running").to_string(),
                num(e, "queued").to_string(),
                format!("{:.1}", e.get("queue_wait_ms").and_then(Value::as_f64).unwrap_or(0.0)),
                num(e, "queue_timeouts").to_string(),
                num(e, "warm_per_proc").to_string(),
            ]
        })
        .collect();
    fmt::emitln("");
    fmt::emit(&fmt::table(
        &[
            "SERVICE", "KIND", "LIMIT", "RUNNING", "QUEUED", "WAIT_MS", "TIMEOUTS", "WARM/P",
        ],
        &rows,
    ));
}

fn procs_table(procs: &[Value]) {
    let rows: Vec<Vec<String>> = procs
        .iter()
        .map(|p| {
            vec![
                cell(p, "id"),
                cell(p, "kind"),
                cell(p, "status"),
                num(p, "lrq_len").to_string(),
                cell(p, "machine"),
                (num(p, "busy_ns") / 1_000_000).to_string(),
                num(p, "steals_in").to_string(),
                num(p, "steals_out").to_string(),
                num(p, "preemptions").to_string(),
            ]
        })
        .collect();
    fmt::emitln("");
    fmt::emit(&fmt::table(
        &[
            "P",
            "KIND",
            "STATUS",
            "LRQ",
            "M",
            "BUSY_MS",
            "STEALS_IN",
            "STEALS_OUT",
            "PREEMPTIONS",
        ],
        &rows,
    ));
}

const KEYS: &str = "degradation.mode, default_profile, rate_limit.rps, rate_limit.burst, rate_limit";

/// `degradation.mode=shed rate_limit.rps=100` -> `{"degradation_mode": "shed", "rate_limit": {"rps": 100}}`
pub fn config_body(pairs: &[String]) -> Result<Value> {
    let mut body = Map::new();
    for p in pairs {
        let (key, value) = split_pair(p)?;
        match key {
            "degradation.mode" | "degradation_mode" => {
                body.insert("degradation_mode".into(), json!(value));
            }
            "default_profile" => {
                body.insert("default_profile".into(), json!(value));
            }
            "rate_limit" => {
                let v = parse_json_arg("rate_limit", value)?;
                if !v.is_object() {
                    return Err(CliError::usage(
                        "rate_limit must be a JSON object, e.g. rate_limit={\"rps\":100,\"burst\":200}",
                    ));
                }
                body.insert("rate_limit".into(), v);
            }
            "rate_limit.rps" | "rate_limit.burst" => {
                let n: u64 = value
                    .parse()
                    .map_err(|_| CliError::usage(format!("{key} must be a non-negative integer, got `{value}`")))?;
                let field = key.trim_start_matches("rate_limit.");
                let entry = body.entry("rate_limit").or_insert_with(|| json!({}));
                if let Some(o) = entry.as_object_mut() {
                    o.insert(field.into(), json!(n));
                }
            }
            other => {
                return Err(CliError::usage(format!(
                    "unknown node config key `{other}`; supported: {KEYS}"
                )))
            }
        }
    }
    Ok(Value::Object(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_keys_nest() {
        let b = config_body(&[
            "degradation.mode=shed".into(),
            "rate_limit.rps=5".into(),
            "rate_limit.burst=9".into(),
        ])
        .unwrap();
        assert_eq!(
            b,
            json!({"degradation_mode": "shed", "rate_limit": {"rps": 5, "burst": 9}})
        );
    }

    #[test]
    fn bad_input_is_a_usage_error() {
        assert!(config_body(&["nope".into()]).is_err());
        assert!(config_body(&["mystery=1".into()]).is_err());
        assert!(config_body(&["rate_limit.rps=fast".into()]).is_err());
        assert!(config_body(&["rate_limit=3".into()]).is_err());
    }
}
