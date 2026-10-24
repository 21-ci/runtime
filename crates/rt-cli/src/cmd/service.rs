//! `21ci service create | update | list | get | stats | delete`.
//!
//! Config blocks (`limits`, `scaling`, `triggers`, `ingress`, `egress`, `deno`, `wasm`, `ws`) are set
//! field by field with `--set block.key=value`. The API replaces a whole block on `PATCH`, so
//! `update` merges the given fields into the block as it is now and sends it with `If-Match`.

use reqwest::header::IF_MATCH;
use reqwest::Method;
use serde_json::{json, Map, Value};

use super::{cell, fetch_page_with, print_page, resolve, App};
use crate::artifact::valid_slug;
use crate::cli::{RuntimeArg, ServiceCmd};
use crate::error::{CliError, Result};
use crate::fmt;

pub fn runtime_str(r: RuntimeArg) -> &'static str {
    match r {
        RuntimeArg::Deno => "deno",
        RuntimeArg::Wasm => "wasm",
    }
}

/// Blocks `--set` / `--unset` may touch (`call` has its own flags).
const BLOCKS: [&str; 10] = [
    "limits", "scaling", "triggers", "ingress", "egress", "deno", "wasm", "ws", "events", "async",
];

/// One `--set` / `--unset` edit: `block`, the path inside it, the value (`None` = remove).
#[derive(Debug, Clone, PartialEq)]
struct Edit {
    block: String,
    path: Vec<String>,
    value: Option<Value>,
}

fn split_path(spec: &str, flag: &str) -> Result<(String, Vec<String>)> {
    let mut parts = spec.split('.');
    let block = parts.next().unwrap_or_default().to_string();
    let mut path: Vec<String> = parts.map(str::to_string).collect();
    // `scaling.max_concurrency` is the old name of `scaling.max_instances` (D-B2): the server
    // refuses a block that holds both, and it only ever sends the new one.
    if block == "scaling" && path == ["max_concurrency"] {
        path = vec!["max_instances".into()];
    }
    if !BLOCKS.contains(&block.as_str()) {
        return Err(CliError::usage(format!(
            "{flag} `{spec}`: unknown block `{block}`; use one of {}",
            BLOCKS.join(", ")
        )));
    }
    if path.is_empty() || path.iter().any(String::is_empty) {
        return Err(CliError::usage(format!(
            "{flag} `{spec}`: expected BLOCK.KEY (for example scaling.max_instances)"
        )));
    }
    Ok((block, path))
}

/// `--set limits.cpu_ms=500`: the value is JSON when it parses (numbers, booleans, arrays,
/// objects, quoted strings), else taken as a string.
fn parse_set(s: &str) -> Result<Edit> {
    let (spec, raw) = s
        .split_once('=')
        .ok_or_else(|| CliError::usage(format!("--set `{s}`: expected BLOCK.KEY=VALUE")))?;
    let (block, path) = split_path(spec, "--set")?;
    let value = serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_string()));
    Ok(Edit {
        block,
        path,
        value: Some(value),
    })
}

fn parse_unset(s: &str) -> Result<Edit> {
    let (block, path) = split_path(s, "--unset")?;
    Ok(Edit {
        block,
        path,
        value: None,
    })
}

/// Apply `edits` of one block to `base` (an object; anything else counts as empty).
fn apply(base: Option<&Value>, edits: &[&Edit]) -> Value {
    let mut root = base.filter(|v| v.is_object()).cloned().unwrap_or_else(|| json!({}));
    for e in edits {
        let mut cur = &mut root;
        for (i, key) in e.path.iter().enumerate() {
            let obj = match cur {
                Value::Object(o) => o,
                other => {
                    *other = Value::Object(Map::new());
                    other.as_object_mut().expect("just made an object")
                }
            };
            if i + 1 == e.path.len() {
                match &e.value {
                    Some(v) => {
                        obj.insert(key.clone(), v.clone());
                    }
                    None => {
                        obj.remove(key);
                    }
                }
                break;
            }
            cur = obj.entry(key.clone()).or_insert_with(|| json!({}));
        }
    }
    root
}

/// The `--min-instances` / `--max-instances` / `--min-run-time` flags.
#[derive(Clone, Copy, Debug, Default)]
struct Instances {
    min: Option<u32>,
    max: Option<u32>,
    min_run_time: Option<std::time::Duration>,
}

impl Instances {
    /// Both bounds given and crossed: refused before any request.
    fn check(self) -> Result<()> {
        match (self.min, self.max) {
            (Some(min), Some(max)) if min > max => Err(CliError::usage(format!(
                "--min-instances {min} is greater than --max-instances {max}"
            ))),
            _ => Ok(()),
        }
    }
}

/// The edits for `--reuse-isolates` / `--reuse-instances`, `--min-instances` /
/// `--max-instances` plus `--set` / `--unset` (flags first, so an explicit `--set` wins).
fn edits(
    reuse_isolates: Option<bool>,
    reuse_instances: Option<bool>,
    instances: Instances,
    set: &[String],
    unset: &[String],
) -> Result<Vec<Edit>> {
    instances.check()?;
    let mut out = Vec::new();
    for (key, v) in [("min_instances", instances.min), ("max_instances", instances.max)] {
        if let Some(v) = v {
            out.push(Edit {
                block: "scaling".into(),
                path: vec![key.into()],
                value: Some(json!(v)),
            });
        }
    }
    if let Some(d) = instances.min_run_time {
        out.push(Edit {
            block: "scaling".into(),
            path: vec!["min_run_time_ms".into()],
            value: Some(json!(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))),
        });
    }
    if let Some(r) = reuse_isolates {
        out.push(Edit {
            block: "deno".into(),
            path: vec!["reuse_isolates".into()],
            value: Some(json!(r)),
        });
    }
    if let Some(r) = reuse_instances {
        out.push(Edit {
            block: "wasm".into(),
            path: vec!["reuse_instances".into()],
            value: Some(json!(r)),
        });
    }
    for s in set {
        out.push(parse_set(s)?);
    }
    for u in unset {
        out.push(parse_unset(u)?);
    }
    Ok(out)
}

/// Each touched block, merged onto `current` (the service view, or nothing on create).
fn merged_blocks(current: Option<&Value>, edits: &[Edit], body: &mut Value) {
    for block in BLOCKS {
        let mine: Vec<&Edit> = edits.iter().filter(|e| e.block == block).collect();
        if !mine.is_empty() {
            body[block] = apply(current.and_then(|c| c.get(block)), &mine);
        }
    }
}

pub fn run(app: &App, cmd: &ServiceCmd) -> Result<()> {
    match cmd {
        ServiceCmd::Create {
            slug,
            runtime,
            name,
            profile,
            access,
            environment,
            allowed_callers,
            reuse_isolates,
            reuse_instances,
            min_instances,
            max_instances,
            durable,
            min_run_time,
            set,
            project,
        } => {
            if !valid_slug(slug) {
                return Err(CliError::usage(format!(
                    "invalid slug \"{slug}\": must match ^[a-z][a-z0-9-]{{1,62}}$"
                )));
            }
            check_environment(environment.as_deref())?;
            let allowed_callers = canonical_callers(allowed_callers)?;
            let instances = Instances {
                min: *min_instances,
                max: *max_instances,
                min_run_time: *min_run_time,
            };
            let edits = edits(*reuse_isolates, *reuse_instances, instances, set, &[])?;
            let mut body = json!({"slug": slug, "runtime": runtime_str(*runtime)});
            for (k, v) in [
                ("name", name),
                ("profile", profile),
                ("access", access),
                ("project", project),
                ("environment", environment),
            ] {
                if let Some(v) = v {
                    body[k] = json!(v);
                }
            }
            if !allowed_callers.is_empty() {
                body["call"] = json!({"allowed_callers": allowed_callers});
            }
            if *durable {
                body["durable"] = json!(true);
            }
            merged_blocks(None, &edits, &mut body);
            let v = app.client()?.post("/v1/services", &body)?;
            show(app, &v);
        }
        ServiceCmd::Update {
            svc,
            name,
            profile,
            access,
            environment,
            allowed_callers,
            no_allowed_callers,
            reuse_isolates,
            reuse_instances,
            min_instances,
            max_instances,
            durable,
            min_run_time,
            set,
            unset,
        } => {
            check_environment(environment.as_deref())?;
            let allowed_callers = canonical_callers(allowed_callers)?;
            let instances = Instances {
                min: *min_instances,
                max: *max_instances,
                min_run_time: *min_run_time,
            };
            let edits = edits(*reuse_isolates, *reuse_instances, instances, set, unset)?;
            let mut body = json!({});
            for (k, v) in [
                ("name", name),
                ("profile", profile),
                ("access", access),
                ("environment", environment),
            ] {
                if let Some(v) = v {
                    body[k] = json!(v);
                }
            }
            if !allowed_callers.is_empty() || *no_allowed_callers {
                body["call"] = json!({"allowed_callers": allowed_callers});
            }
            if let Some(d) = durable {
                body["durable"] = json!(d);
            }
            if body.as_object().is_some_and(|o| o.is_empty()) && edits.is_empty() {
                return Err(CliError::usage(
                    "nothing to change: give --name, --profile, --access, --env, --allowed-caller, \
                     --no-allowed-callers, --reuse-isolates, --reuse-instances, --min-instances, \
                     --max-instances, --min-run-time, --durable, --set or --unset",
                ));
            }
            let c = app.client()?;
            let mut s = resolve(&c, svc)?;
            if !edits.is_empty() {
                let view = s.view(&c)?.clone();
                merged_blocks(Some(&view), &edits, &mut body);
            }
            let mut rb = c.req(Method::PATCH, &s.path("")).json(&body);
            if let Some(etag) = &s.etag {
                rb = rb.header(IF_MATCH, etag);
            }
            let v = c.send_json(rb)?.body;
            show(app, &v);
        }
        ServiceCmd::List {
            page: p,
            project,
            environment,
        } => {
            let mut filter: Vec<(&str, String)> = Vec::new();
            if let Some(v) = project {
                filter.push(("project", v.clone()));
            }
            if let Some(v) = environment {
                filter.push(("environment", v.clone()));
            }
            let page = fetch_page_with(&app.client()?, "/v1/services", p, &filter)?;
            print_page(
                app,
                &page,
                "services",
                &["ID", "SLUG", "RUNTIME", "PROJECT", "ENV", "LIVE", "BUILD", "ACCESS"],
                |s| {
                    vec![
                        cell(s, "id"),
                        cell(s, "slug"),
                        cell(s, "runtime"),
                        cell(s, "project"),
                        cell(s, "environment"),
                        cell(s, "live_version"),
                        build_state(s),
                        cell(s, "access"),
                    ]
                },
            );
        }
        ServiceCmd::Get(a) => {
            let c = app.client()?;
            let mut svc = resolve(&c, &a.svc)?;
            let v = svc.view(&c)?.clone();
            show(app, &v);
        }
        ServiceCmd::Stats(a) => super::stats::run(app, a)?,
        ServiceCmd::Delete { svc, keep_data } => {
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let query: Vec<(&str, String)> = if *keep_data {
                vec![("keep_data", "1".to_string())]
            } else {
                vec![]
            };
            c.delete(&s.path(""), &query)?;
            if app.json {
                fmt::print_json(&json!({"deleted": s.id}));
            } else {
                fmt::emitln(&format!("Service {} deleted.", s.label));
            }
        }
    }
    Ok(())
}

/// `^[a-z][a-z0-9-]{0,31}$`, as the server checks it.
fn valid_environment(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn check_environment(env: Option<&str>) -> Result<()> {
    match env {
        Some(e) if !valid_environment(e) => Err(CliError::usage(format!(
            "invalid environment \"{e}\": must match ^[a-z][a-z0-9-]{{0,31}}$"
        ))),
        _ => Ok(()),
    }
}

/// Each `--allowed-caller` is a slug, a `svc_…` id or `env:<environment>`. Ids are sent in
/// their canonical form (uppercase ULID), as the server stores them.
fn canonical_callers(callers: &[String]) -> Result<Vec<String>> {
    callers
        .iter()
        .map(|c| {
            let canonical = match c.strip_prefix("env:") {
                Some(env) => valid_environment(env).then(|| c.clone()),
                None => match c.strip_prefix("svc_") {
                    Some(id) if id.len() == 26 && id.bytes().all(|b| b.is_ascii_alphanumeric()) => {
                        Some(format!("svc_{}", id.to_ascii_uppercase()))
                    }
                    Some(_) => None,
                    None => valid_slug(c).then(|| c.clone()),
                },
            };
            canonical.ok_or_else(|| {
                CliError::usage(format!(
                    "invalid allowed_callers entry \"{c}\": expected a service slug, a svc_… id or env:<environment>"
                ))
            })
        })
        .collect()
}

fn build_state(s: &Value) -> String {
    s.pointer("/status/build_state")
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string()
}

/// `a, env:b`, or what an empty list means.
fn callers(s: &Value) -> String {
    match s.pointer("/call/allowed_callers").and_then(Value::as_array) {
        Some(a) if !a.is_empty() => a
            .iter()
            .map(|c| c.as_str().map(str::to_string).unwrap_or_else(|| c.to_string()))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "(services in the same environment)".into(),
    }
}

/// The runtime's reuse knob: its value, or where the default comes from.
fn reuse(s: &Value) -> (&'static str, String) {
    let (label, ptr, default) = match s.get("runtime").and_then(Value::as_str) {
        Some("wasm") => ("Reuse instances", "/wasm/reuse_instances", "default (profile)"),
        _ => (
            "Reuse isolates",
            "/deno/reuse_isolates",
            "default (node config, then profile)",
        ),
    };
    let v = s
        .pointer(ptr)
        .filter(|v| !v.is_null())
        .map(Value::to_string)
        .unwrap_or_else(|| default.to_string());
    (label, v)
}

/// `min 2 (service), max 4 (plan)`: the effective instance bounds and where each comes from
/// (nothing when the server sends no `effective` block).
fn instances(s: &Value) -> Option<String> {
    let part = |key: &str| {
        let e = s.pointer(&format!("/effective/{key}"))?;
        Some(format!(
            "{} ({})",
            e.get("value").map(Value::to_string)?,
            e.get("source").and_then(Value::as_str).unwrap_or("-")
        ))
    };
    Some(format!(
        "min {}, max {}",
        part("min_instances")?,
        part("max_instances")?
    ))
}

/// `scaling.min_run_time_ms` in words: the effective value and its source when the server sends
/// one, else the service's own setting.
fn keep_alive(s: &Value) -> String {
    let (value, source) = match s.pointer("/effective/min_run_time_ms") {
        Some(e) if e.is_object() => (
            e.get("value").and_then(Value::as_u64),
            e.get("source").and_then(Value::as_str).unwrap_or("service").to_string(),
        ),
        _ => (
            s.pointer("/scaling/min_run_time_ms").and_then(Value::as_u64),
            "service".to_string(),
        ),
    };
    match value {
        None => "inherit (node idle TTL)".into(),
        Some(0) => "0 ms (disposed after the answer unless work waits)".into(),
        Some(n) => format!("{n} ms ({source})"),
    }
}

/// The triggers that are off; `all on` when none is.
fn triggers(s: &Value) -> String {
    let off: Vec<&str> = s
        .get("triggers")
        .and_then(Value::as_object)
        .map(|o| {
            o.iter()
                .filter(|(_, v)| v.as_bool() == Some(false))
                .map(|(k, _)| k.as_str())
                .collect()
        })
        .unwrap_or_default();
    if off.is_empty() {
        "all on".into()
    } else {
        format!("off: {}", off.join(", "))
    }
}

fn show(app: &App, s: &Value) {
    if app.json {
        fmt::print_json(s);
        return;
    }
    let mut rows = vec![
        ("ID", cell(s, "id")),
        ("Slug", cell(s, "slug")),
        ("Runtime", cell(s, "runtime")),
        ("Name", cell(s, "name")),
        ("Profile", cell(s, "profile")),
        ("Project", cell(s, "project")),
        ("Environment", cell(s, "environment")),
        ("Allowed callers", callers(s)),
        ("Live", cell(s, "live_version")),
        ("Access", cell(s, "access")),
        reuse(s),
        ("Limits", fmt::pairs(s.get("limits"))),
        ("Scaling", fmt::pairs(s.get("scaling"))),
    ];
    if let Some(instances) = instances(s) {
        rows.push(("Instances", instances));
    }
    rows.push(("Keep-alive", keep_alive(s)));
    rows.push(("Triggers", triggers(s)));
    for (label, key) in [
        ("Ingress", "ingress"),
        ("Egress", "egress"),
        ("Events", "events"),
        ("Async", "async"),
    ] {
        let p = fmt::pairs(s.get(key));
        if p != "-" {
            rows.push((label, p));
        }
    }
    rows.extend([
        ("Build", build_state(s)),
        (
            "Tier",
            s.pointer("/status/tier")
                .and_then(Value::as_str)
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "In flight",
            s.pointer("/status/inflight")
                .map(Value::to_string)
                .unwrap_or_else(|| "-".into()),
        ),
    ]);
    if let Some(adm) = s.pointer("/status/admission") {
        rows.push(("Concurrency", super::stats::concurrency(adm)));
    }
    if let Some(e) = s.pointer("/status/last_error").and_then(Value::as_str) {
        rows.push(("Last error", e.to_string()));
    }
    rows.push(("ETag", cell(s, "etag")));
    rows.push(("Tenant", cell(s, "tenant_id")));
    fmt::emit(&fmt::kv(&rows));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_values_are_json_or_strings() {
        assert_eq!(parse_set("limits.cpu_ms=500").unwrap().value, Some(json!(500)));
        assert_eq!(parse_set("triggers.cron=false").unwrap().value, Some(json!(false)));
        assert_eq!(
            parse_set(r#"ingress.allowed_methods=["GET"]"#).unwrap().value,
            Some(json!(["GET"]))
        );
        assert_eq!(parse_set("x.y=1").map(drop).unwrap_err().exit_code(), 2);
        assert_eq!(parse_set("limits=1").map(drop).unwrap_err().exit_code(), 2);
        assert_eq!(parse_set("limits.cpu_ms").map(drop).unwrap_err().exit_code(), 2);
        assert_eq!(
            parse_set("egress.allowed_hosts=api.example.com").unwrap().value,
            Some(json!("api.example.com"))
        );
    }

    #[test]
    fn edits_merge_into_the_current_block() {
        let current = json!({"scaling": {"max_instances": 4, "max_queued": 10}, "ingress": {"rate_limit": {"rps": 1, "burst": 2}}});
        let e = edits(
            None,
            None,
            Instances::default(),
            &["scaling.max_queued=50".into(), "ingress.rate_limit.rps=9".into()],
            &["scaling.max_instances".into()],
        )
        .unwrap();
        let mut body = json!({});
        merged_blocks(Some(&current), &e, &mut body);
        assert_eq!(
            body,
            json!({"scaling": {"max_queued": 50}, "ingress": {"rate_limit": {"rps": 9, "burst": 2}}})
        );
    }
}
