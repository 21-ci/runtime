//! `21ci host attach | list | get | detach`: custom hosts of a service.
//!
//! An operator's mapping is active at once. A tenant's claim is `pending_verification` until the
//! node finds the claim's token in the TXT record `_rt-verify.<host>` (it looks every
//! `hosts.verify_interval`) or the operator approves it; it expires after `hosts.pending_ttl`.

use std::time::{Duration, Instant, SystemTime};

use reqwest::Method;
use serde_json::{json, Value};

use super::{cell, resolve, App, Svc};
use crate::cli::HostCmd;
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::fmt;

/// The label in front of the host whose TXT record carries the verify token.
const VERIFY_LABEL: &str = "_rt-verify";

/// Lowercase, no trailing dot, `[a-z0-9.-]`, at most 253 bytes: what the server accepts.
fn normalize_host(h: &str) -> Result<String> {
    let host = h.trim().trim_end_matches('.').to_ascii_lowercase();
    let ok = !host.is_empty()
        && host.len() <= 253
        && !host.starts_with('.')
        && !host.contains("..")
        && host
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'.' || c == b'-');
    if ok {
        Ok(host)
    } else {
        Err(CliError::usage(format!(
            "invalid host \"{h}\": expected a DNS name such as shop.example.com"
        )))
    }
}

fn pending(h: &Value) -> bool {
    h.get("status").and_then(Value::as_str) == Some("pending_verification")
}

/// `{name, type, value}` of the TXT record that verifies a pending claim.
fn dns_record(h: &Value) -> Option<Value> {
    let token = h
        .get("verify_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())?;
    let host = h.get("host").and_then(Value::as_str)?;
    Some(json!({"name": format!("{VERIFY_LABEL}.{host}"), "type": "TXT", "value": token}))
}

/// The API's host view plus `dns_record` while the claim is pending.
fn with_record(h: &Value) -> Value {
    let mut v = h.clone();
    if let (Some(r), Some(o)) = (dns_record(h).filter(|_| pending(h)), v.as_object_mut()) {
        o.insert("dns_record".into(), r);
    }
    v
}

fn expires(h: &Value, now_ms: i64) -> String {
    match h.get("expires_at").and_then(Value::as_i64) {
        Some(ms) => format!("{} ({})", fmt::ts_s(ms), fmt::relative(ms, now_ms)),
        None => "-".into(),
    }
}

fn show(app: &App, h: &Value, svc_arg: &str) {
    if app.json {
        fmt::print_json(&with_record(h));
        return;
    }
    let now = fmt::epoch_ms(SystemTime::now());
    let mut rows = vec![
        ("Host", cell(h, "host")),
        ("Status", cell(h, "status")),
        ("Created", fmt::ts_field(h, "created_at")),
    ];
    if pending(h) {
        rows.push(("Expires", expires(h, now)));
        if let Some(r) = dns_record(h) {
            rows.push((
                "DNS record",
                format!(
                    "{}. IN TXT \"{}\"",
                    fmt::str_field(&r, "name"),
                    fmt::str_field(&r, "value")
                ),
            ));
        }
    }
    fmt::emit(&fmt::kv(&rows));
    if pending(h) {
        let host = fmt::str_field(h, "host");
        fmt::note(&format!(
            "Publish the TXT record above; the node checks pending claims every hosts.verify_interval and \
             activates {host} when the token matches (or the operator approves it). Also point {host} at the \
             node's public listener (A/AAAA or CNAME). Watch it with: 21ci host get {svc_arg} {host} --wait"
        ));
    }
}

fn list(c: &Client, svc: &Svc) -> Result<Vec<Value>> {
    Ok(c.get(&svc.path("/hosts"), &[])?.as_array().cloned().unwrap_or_default())
}

/// `GET …/hosts/{host}`; `None` when the host is not mapped to the service (`404`).
fn get_one(c: &Client, svc: &Svc, host: &str) -> Result<Option<Value>> {
    match c.get(&svc.path(&format!("/hosts/{}", fmt::enc(host))), &[]) {
        Ok(h) => Ok(Some(h)),
        Err(CliError::Api { status: Some(404), .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

fn find(c: &Client, svc: &Svc, host: &str) -> Result<Value> {
    get_one(c, svc, host)?.ok_or_else(|| CliError::not_found(format!("host {host} is not attached to {}", svc.label)))
}

pub fn run(app: &App, cmd: &HostCmd) -> Result<()> {
    match cmd {
        HostCmd::Attach { svc, host } => {
            let host = normalize_host(host)?;
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let r = c.send_json(c.req(Method::PUT, &s.path(&format!("/hosts/{}", fmt::enc(&host)))));
            match r {
                Ok(reply) => show(app, &reply.body, svc),
                Err(CliError::Api {
                    status,
                    code,
                    message,
                    details,
                    retry_after,
                }) if code == "host_cooldown" => {
                    if let (Some(at), false) = (details.get("retry_at").and_then(Value::as_i64), app.json) {
                        fmt::note(&format!(
                            "{host} can be claimed again by this tenant from {}.",
                            fmt::ts_s(at)
                        ));
                    }
                    return Err(CliError::Api {
                        status,
                        code,
                        message,
                        details,
                        retry_after,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        HostCmd::List(a) => {
            let c = app.client()?;
            let svc = resolve(&c, &a.svc)?;
            let items = list(&c, &svc)?;
            if app.json {
                fmt::print_json(&Value::Array(items.iter().map(with_record).collect()));
                return Ok(());
            }
            if items.is_empty() {
                fmt::note("No hosts attached.");
                return Ok(());
            }
            let now = fmt::epoch_ms(SystemTime::now());
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|h| {
                    vec![
                        cell(h, "host"),
                        cell(h, "status"),
                        fmt::ts_field(h, "created_at"),
                        if pending(h) { expires(h, now) } else { "-".into() },
                    ]
                })
                .collect();
            fmt::emit(&fmt::table(&["HOST", "STATUS", "CREATED", "EXPIRES"], &rows));
            if items.iter().any(pending) {
                fmt::note("Pending claims need a DNS TXT record: 21ci host get <svc> <host> shows it.");
            }
        }
        HostCmd::Get {
            svc,
            host,
            wait,
            wait_timeout,
            wait_interval,
        } => {
            let host = normalize_host(host)?;
            let c = app.client()?;
            let s = resolve(&c, svc)?;
            let mut h = find(&c, &s, &host)?;
            if *wait && pending(&h) {
                h = wait_active(app, &c, &s, &host, h, *wait_timeout, *wait_interval)?;
            }
            show(app, &h, svc);
        }
        HostCmd::Detach { svc, host } => {
            let host = normalize_host(host)?;
            let c = app.client()?;
            let svc = resolve(&c, svc)?;
            c.delete(&svc.path(&format!("/hosts/{}", fmt::enc(&host))), &[])?;
            if app.json {
                fmt::print_json(&json!({"detached": host}));
            } else {
                fmt::emitln(&format!("Host {host} detached from {}.", svc.label));
            }
        }
    }
    Ok(())
}

/// Poll the host list until `host` is active; fails when the claim disappears (expired or
/// released) or the timeout passes.
fn wait_active(
    app: &App,
    c: &Client,
    svc: &Svc,
    host: &str,
    first: Value,
    timeout: Duration,
    interval: Duration,
) -> Result<Value> {
    if !app.json {
        if let Some(r) = dns_record(&first) {
            fmt::note(&format!(
                "Waiting for {host} to be verified ({}. IN TXT \"{}\")…",
                fmt::str_field(&r, "name"),
                fmt::str_field(&r, "value")
            ));
        }
    }
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(CliError::failed(format!(
                "host {host} still pending_verification after {}",
                humantime::format_duration(timeout)
            )));
        }
        std::thread::sleep(interval.min(deadline - now));
        let h = get_one(c, svc, host)?.ok_or_else(|| {
            CliError::failed(format!(
                "the claim of {host} is gone (expired unverified, or released) while waiting"
            ))
        })?;
        if !pending(&h) {
            return Ok(h);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_are_normalized_like_the_server_does() {
        assert_eq!(normalize_host(" Shop.Example.COM. ").unwrap(), "shop.example.com");
        assert!(normalize_host("").is_err());
        assert!(normalize_host("a b.com").is_err());
        assert!(normalize_host(".x.com").is_err());
        assert!(normalize_host("a..b").is_err());
        assert!(normalize_host("under_score.com").is_err());
    }

    #[test]
    fn pending_claims_get_their_dns_record() {
        let h = json!({"host": "shop.example.com", "status": "pending_verification", "verify_token": "tok", "created_at": 1, "expires_at": 2});
        assert_eq!(
            with_record(&h)["dns_record"],
            json!({"name": "_rt-verify.shop.example.com", "type": "TXT", "value": "tok"})
        );
        let active =
            json!({"host": "a.com", "status": "active", "verify_token": null, "created_at": 1, "expires_at": null});
        assert_eq!(with_record(&active), active);
    }
}
