//! The audit trail: which requests are recorded, the event, and how it travels.
//!
//! After the response is sent, the gateway ships the event to the operator's audit log through the
//! operator SDK (`std:host/sdk`, method `audit.ship`; the SDK holds the operator binding), then enqueues it to the
//! audit service with the result. The audit service keeps the queryable trail behind
//! `/v1/audit/recent` and ships, in batches, what the gateway could not. The event id (the
//! gateway's task id) is the dedup key everywhere: queue, table and operator API.

use std::net::IpAddr;

use serde_json::{json, Value};

use crate::config::AuditMode;

pub fn records(mode: AuditMode, method: &str, status: u16, path: &str) -> bool {
    if path == "/healthz" {
        return false;
    }
    match mode {
        AuditMode::Off => false,
        AuditMode::All => true,
        AuditMode::Mutations => method != "GET" || status >= 400,
    }
}

/// What the gateway knows about one answered request.
pub struct Event<'a> {
    pub id: &'a str,
    /// epoch ms
    pub at: u64,
    pub sub: Option<&'a str>,
    pub ip: Option<IpAddr>,
    pub method: &'a str,
    pub path: &'a str,
    pub route: Option<&'a str>,
    pub status: u16,
    pub outcome: &'a str,
    pub reason: Option<&'a str>,
}

impl Event<'_> {
    /// The record the operator API and the audit service's table get (audit/src/events.ts).
    pub fn to_json(&self) -> Value {
        // The audit service accepts paths up to 512 characters; a longer (already rejected) one is cut.
        let path: String = self.path.chars().take(512).collect();
        json!({
            "id": self.id,
            "at": self.at,
            "sub": self.sub,
            "ip": self.ip.map(|ip| ip.to_string()),
            "method": self.method,
            "path": path,
            "route": self.route,
            "status": self.status,
            "outcome": self.outcome,
            "reason": self.reason,
        })
    }
}

/// The argument of the SDK's `audit.ship`: the shape the audit service ships its batches in.
pub fn shipment(event: &Value) -> Vec<u8> {
    json!({ "source": "edge-gateway", "events": [event] })
        .to_string()
        .into_bytes()
}

/// A failed `audit.ship` as `<sdk>: <code>` (sdk_not_granted, sdk_unavailable, the SDK's own
/// code such as binding_not_allowed): printable ASCII, at most 120 characters (what the audit
/// service stores).
pub fn ship_error(sdk: &str, why: &str) -> String {
    format!("{sdk}: {why}")
        .chars()
        .filter(|c| c.is_ascii_graphic() || *c == ' ')
        .take(120)
        .collect()
}

/// The audit service's queue message: the event, a version, and the shipping result.
///   `shipped: true`                       the operator API has it: stored as shipped, never sent again
///   `shipped: false, ship_error: "…"`     the gateway's attempt failed: the audit service retries
///   `shipped: false, ship_error: null`    the gateway does not ship (`GATEWAY_SDK=off`)
pub fn queue_message(mut event: Value, shipped: Option<&Result<(), String>>) -> Value {
    event["v"] = 1.into();
    event["shipped"] = matches!(shipped, Some(Ok(()))).into();
    event["ship_error"] = match shipped {
        Some(Err(e)) => e.as_str().into(),
        _ => Value::Null,
    };
    event
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Value {
        Event {
            id: "task_0000000000000002",
            at: 1_800_000_000_000,
            sub: Some("alice"),
            ip: "203.0.113.7".parse().ok(),
            method: "POST",
            path: "/v1/notes",
            route: Some("notes"),
            status: 201,
            outcome: "ok",
            reason: None,
        }
        .to_json()
    }

    #[test]
    fn what_is_recorded() {
        use AuditMode::*;
        assert!(records(Mutations, "POST", 201, "/v1/notes"));
        assert!(records(Mutations, "GET", 401, "/v1/notes"), "every denial");
        assert!(!records(Mutations, "GET", 200, "/v1/notes"));
        assert!(records(All, "GET", 200, "/v1/notes"));
        assert!(!records(Off, "POST", 500, "/v1/notes"));
        assert!(!records(All, "GET", 200, "/healthz"));
    }

    #[test]
    fn event_shape() {
        let e = event();
        assert_eq!(e["ip"], "203.0.113.7");
        assert_eq!(e["sub"], "alice");
        assert_eq!(e["reason"], Value::Null);
        assert!(
            e.get("v").is_none() && e.get("shipped").is_none(),
            "the operator gets the bare record"
        );
        let long = "/a".repeat(400);
        let cut = Event {
            id: "t",
            at: 1,
            sub: None,
            ip: None,
            method: "GET",
            path: &long,
            route: None,
            status: 400,
            outcome: "invalid",
            reason: Some("invalid_request"),
        }
        .to_json();
        assert_eq!(cut["path"].as_str().unwrap().len(), 512);
        assert_eq!(cut["ip"], Value::Null);
        let shipped: Value = serde_json::from_slice(&shipment(&e)).unwrap();
        assert_eq!(shipped["source"], "edge-gateway");
        assert_eq!(shipped["events"][0], e);
    }

    #[test]
    fn shipping_errors() {
        assert_eq!(ship_error("acme", "sdk_unavailable"), "acme: sdk_unavailable");
        let noisy = ship_error("acme", &format!("bad\nreason{}", "x".repeat(200)));
        assert_eq!(noisy.len(), 120);
        assert!(noisy.starts_with("acme: badreason"));
    }

    #[test]
    fn queue_messages() {
        let ok = queue_message(event(), Some(&Ok(())));
        assert_eq!(ok["v"], 1);
        assert_eq!(ok["shipped"], true);
        assert_eq!(ok["ship_error"], Value::Null);
        assert_eq!(ok["id"], "task_0000000000000002");
        let failed = queue_message(event(), Some(&Err("acme: rate_limited".into())));
        assert_eq!(failed["shipped"], false);
        assert_eq!(failed["ship_error"], "acme: rate_limited");
        let off = queue_message(event(), None);
        assert_eq!(off["shipped"], false);
        assert_eq!(off["ship_error"], Value::Null);
    }
}
