//! `21ci doctor`: configuration, connectivity, credentials and versions, one check per line.
//! Exits 1 when a check fails; warnings do not fail.

use std::path::Path;
use std::time::Instant;

use reqwest::Method;
use serde_json::{json, Value};

use super::App;
use crate::client::{http_client, net_error};
use crate::error::{CliError, Result};
use crate::fmt;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Level {
    Ok,
    Warn,
    Fail,
    Skip,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Ok => "ok",
            Level::Warn => "warn",
            Level::Fail => "fail",
            Level::Skip => "skip",
        }
    }
}

struct Report(Vec<(&'static str, Level, String)>);

impl Report {
    fn add(&mut self, check: &'static str, level: Level, detail: impl Into<String>) {
        self.0.push((check, level, detail.into()));
    }
}

/// `major.minor` of a `x.y.z[-pre]` version.
fn major_minor(v: &str) -> Option<(u64, u64)> {
    let mut it = v.trim().trim_start_matches('v').split(['.', '-', '+']);
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

/// What kind of credential is configured (never its secret part).
fn credential(token: Option<&str>) -> String {
    match token {
        None => "none".into(),
        Some(t) if t.starts_with("rtk_") => "tenant key (rtk_…)".into(),
        Some(_) => "root token (operator)".into(),
    }
}

/// Client vs node version: `major.minor` must match.
fn version_check(r: &mut Report, node: &str) {
    let client = env!("CARGO_PKG_VERSION");
    let level = match (major_minor(client), major_minor(node)) {
        (Some(a), Some(b)) if a == b => Level::Ok,
        _ => Level::Warn,
    };
    let detail = if level == Level::Ok {
        format!("client {client}, node {node}")
    } else {
        format!("client {client}, node {node}: versions differ, some commands may not match the API")
    };
    r.add("version", level, detail);
}

pub fn run(app: &App, config_path: &Path) -> Result<()> {
    let s = &app.settings;
    let mut r = Report(Vec::new());

    // 1. configuration
    let ctx = s
        .context
        .clone()
        .map(|c| format!("context {c}"))
        .unwrap_or_else(|| "no context (environment and defaults)".into());
    let file = if config_path.exists() {
        config_path.display().to_string()
    } else {
        format!("{} (absent)", config_path.display())
    };
    r.add("config", Level::Ok, format!("{ctx}; {file}"));
    let mut cred = credential(s.token.as_deref());
    if let Some(t) = &s.tenant {
        cred.push_str(&format!(", acting for {t}"));
    }
    r.add(
        "credentials",
        if s.token.is_some() { Level::Ok } else { Level::Warn },
        if s.token.is_some() {
            cred
        } else {
            "no token: set RT_KEY or `21ci config set-context --token` (only a debug node without a root token accepts none)".into()
        },
    );

    // 2. the control API answers (openapi.json needs no auth)
    let c = app.client()?;
    let started = Instant::now();
    let reachable = match c.send(c.req(Method::GET, "/v1/openapi.json")) {
        Ok(resp) => {
            let ms = started.elapsed().as_millis();
            let api = resp
                .json::<Value>()
                .ok()
                .and_then(|v| v.pointer("/info/version").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_else(|| "?".into());
            r.add(
                "control api",
                Level::Ok,
                format!("{} answers in {ms} ms (API {api})", s.api),
            );
            true
        }
        Err(CliError::Network(m)) => {
            r.add("control api", Level::Fail, m);
            false
        }
        Err(e) => {
            r.add(
                "control api",
                Level::Fail,
                e.render_text()
                    .unwrap_or_default()
                    .trim_start_matches("error: ")
                    .to_string(),
            );
            false
        }
    };

    // 3. what the credentials can do, and the node's version
    if reachable {
        match c.get("/v1/node/status", &[]) {
            Ok(st) => {
                r.add("auth", Level::Ok, "operator: node routes allowed");
                version_check(&mut r, &fmt::str_field(&st, "version"));
                let mode = fmt::str_field(&st, "mode");
                r.add(
                    "node mode",
                    if mode == "normal" { Level::Ok } else { Level::Warn },
                    match mode.as_str() {
                        "normal" => "normal".to_string(),
                        "shed" => "shed: only warm services are admitted".to_string(),
                        "drain" => "drain: the node takes no new work".to_string(),
                        other => other.to_string(),
                    },
                );
            }
            Err(CliError::Api { status: Some(403), .. }) => {
                // a tenant key: check it can read services
                match c.get("/v1/services", &[("limit", "1".to_string())]) {
                    Ok(_) => r.add("auth", Level::Ok, "tenant key: services:read allowed"),
                    Err(CliError::Api {
                        status: Some(403),
                        code,
                        ..
                    }) => r.add(
                        "auth",
                        Level::Warn,
                        format!("tenant key without services:read ({code})"),
                    ),
                    Err(e) => r.add(
                        "auth",
                        Level::Fail,
                        e.render_text()
                            .unwrap_or_default()
                            .trim_start_matches("error: ")
                            .to_string(),
                    ),
                }
                // any valid credential may read the version (GET /v1/version)
                match c.get("/v1/version", &[]) {
                    Ok(v) => version_check(&mut r, &fmt::str_field(&v, "version")),
                    Err(CliError::Api { status: Some(404), .. }) => {
                        r.add("version", Level::Skip, "the node predates GET /v1/version")
                    }
                    Err(e) => r.add(
                        "version",
                        Level::Warn,
                        e.render_text()
                            .unwrap_or_default()
                            .trim_start_matches("error: ")
                            .to_string(),
                    ),
                }
            }
            Err(CliError::Api { status: Some(401), .. }) => {
                r.add("auth", Level::Fail, "the token was rejected (401 unauthorized)");
                r.add("version", Level::Skip, "needs a valid token");
            }
            Err(e) => {
                r.add(
                    "auth",
                    Level::Fail,
                    e.render_text()
                        .unwrap_or_default()
                        .trim_start_matches("error: ")
                        .to_string(),
                );
                r.add("version", Level::Skip, "needs a working control API");
            }
        }
    } else {
        r.add("auth", Level::Skip, "the control API is not reachable");
        r.add("version", Level::Skip, "the control API is not reachable");
    }

    // 4. the invoke listener answers at all (any HTTP status will do)
    let http = http_client(Some(std::time::Duration::from_secs(10)))?;
    let started = Instant::now();
    match http.get(format!("{}/", s.invoke)).send() {
        Ok(resp) => r.add(
            "invoke listener",
            Level::Ok,
            format!(
                "{} answers in {} ms (HTTP {})",
                s.invoke,
                started.elapsed().as_millis(),
                resp.status().as_u16()
            ),
        ),
        Err(e) => {
            let m = match net_error(&s.invoke, &e) {
                CliError::Network(m) | CliError::Failed(m) | CliError::Usage(m) => m,
                other => other.to_string(), // LCOV_EXCL_LINE: net_error only returns Network, Failed or Usage
            };
            r.add("invoke listener", Level::Warn, m);
        }
    }

    let failed = r.0.iter().any(|(_, l, _)| *l == Level::Fail);
    if app.json {
        let checks: Vec<Value> =
            r.0.iter()
                .map(|(c, l, d)| json!({"check": c, "status": l.as_str(), "detail": d}))
                .collect();
        fmt::print_json(&json!({"ok": !failed, "checks": checks}));
    } else {
        let rows: Vec<Vec<String>> =
            r.0.iter()
                .map(|(c, l, d)| vec![(*c).to_string(), l.as_str().to_string(), d.clone()])
                .collect();
        fmt::emit(&fmt::table(&["CHECK", "STATUS", "DETAIL"], &rows));
    }
    if failed {
        return Err(CliError::Silent(crate::error::EXIT_FAILURE));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_by_major_minor() {
        assert_eq!(major_minor("0.1.0"), Some((0, 1)));
        assert_eq!(major_minor("v1.2.3-rc.1"), Some((1, 2)));
        assert_eq!(major_minor("dev"), None);
    }

    #[test]
    fn credentials_never_show_the_secret() {
        assert_eq!(credential(Some("rtk_01ABC_secret")), "tenant key (rtk_…)");
        assert_eq!(credential(Some("dev-root-token")), "root token (operator)");
        assert_eq!(credential(None), "none");
    }
}
