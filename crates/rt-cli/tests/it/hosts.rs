//! `21ci host attach | list | get | detach` and `21ci data export`.
use crate::common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::*;
use httpmock::prelude::*;
use httpmock::HttpMockResponse;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

/// 2100-01-01T00:00:00Z: an expiry that stays in the future.
const FAR_MS: i64 = 4_102_444_800_000;

fn pending(host: &str) -> Value {
    json!({"host": host, "status": "pending_verification", "verify_token": "tok123",
           "created_at": T0_MS, "expires_at": FAR_MS})
}

fn active(host: &str) -> Value {
    json!({"host": host, "status": "active", "verify_token": null, "created_at": T0_MS, "expires_at": null})
}

#[test]
fn attach_normalises_the_host_and_prints_the_dns_record_to_create() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(PUT)
            .path("/v1/services/svc_1/hosts/shop.example.com")
            .header("authorization", auth());
        then.status(200).json_body(pending("shop.example.com"));
    });
    let out = ctx.run(&["host", "attach", "hello", "Shop.Example.com."]).ok();
    m.assert();
    let lines = out.lines();
    assert_eq!(lines[0], "Host: shop.example.com");
    assert_eq!(lines[1], "Status: pending_verification");
    assert_eq!(lines[2], "Created: 2026-01-01T00:00:00Z");
    assert!(lines[3].starts_with("Expires: 2100-01-01T00:00:00Z (in "), "{lines:?}");
    assert_eq!(lines[4], r#"DNS record: _rt-verify.shop.example.com. IN TXT "tok123""#);
    assert!(out.stderr.contains("Publish the TXT record"), "{}", out.stderr);
    assert!(
        out.stderr.contains("21ci host get hello shop.example.com --wait"),
        "{}",
        out.stderr
    );
}

#[test]
fn attach_json_is_the_host_view_plus_the_dns_record() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/hosts/shop.example.com");
        then.status(200).json_body(pending("shop.example.com"));
    });
    let out = ctx.run(&["--json", "host", "add", "svc_1", "shop.example.com"]).ok();
    let mut want = pending("shop.example.com");
    want["dns_record"] = json!({"name": "_rt-verify.shop.example.com", "type": "TXT", "value": "tok123"});
    assert_eq!(out.json(), want);
}

#[test]
fn an_operator_mapping_is_active_at_once() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/hosts/api.example.com");
        then.status(200).json_body(active("api.example.com"));
    });
    let out = ctx.run(&["host", "attach", "svc_1", "api.example.com"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "Host: api.example.com",
            "Status: active",
            "Created: 2026-01-01T00:00:00Z"
        ]
    );
    assert_eq!(out.stderr, "");
}

#[test]
fn attach_cooldown_says_when_the_host_can_be_claimed_again() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/hosts/shop.example.com");
        then.status(409).json_body(json!({"error": {
            "code": "host_cooldown",
            "message": "this tenant's claim of the host expired unverified; it can be claimed again later",
            "details": {"retry_at": T0_MS}
        }}));
    });
    let out = ctx.run(&["host", "attach", "svc_1", "shop.example.com"]).fails_with(1);
    assert!(
        out.stderr
            .contains("shop.example.com can be claimed again by this tenant from 2026-01-01T00:00:00Z."),
        "{}",
        out.stderr
    );
    assert!(out.stderr.contains("error: host_cooldown:"), "{}", out.stderr);
}

#[test]
fn attach_rejects_an_invalid_host_without_a_request() {
    let ctx = Ctx::new();
    let any = ctx.server.mock(|when, then| {
        when.any_request();
        then.status(200).json_body(json!({}));
    });
    let out = ctx.run(&["host", "attach", "svc_1", "not a host"]).fails_with(2);
    assert!(out.stderr.contains("invalid host"), "{}", out.stderr);
    any.assert_calls(0);
}

#[test]
fn list_renders_a_table_and_hints_at_pending_claims() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/hosts")
            .header("authorization", auth());
        then.status(200)
            .json_body(json!([active("api.example.com"), pending("shop.example.com")]));
    });
    let out = ctx.run(&["host", "list", "hello"]).ok();
    let lines = out.lines();
    assert_eq!(lines[0], "HOST STATUS CREATED EXPIRES");
    assert_eq!(lines[1], "api.example.com active 2026-01-01T00:00:00Z -");
    assert!(
        lines[2].starts_with("shop.example.com pending_verification 2026-01-01T00:00:00Z 2100-01-01T00:00:00Z (in"),
        "{lines:?}"
    );
    assert!(out.stderr.contains("21ci host get"), "{}", out.stderr);

    let json = ctx.run(&["-o", "json", "host", "list", "hello"]).ok().json();
    assert_eq!(json[0], active("api.example.com"));
    assert_eq!(json[1]["dns_record"]["name"], "_rt-verify.shop.example.com");
}

#[test]
fn list_empty() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/hosts");
        then.status(200).json_body(json!([]));
    });
    let out = ctx.run(&["host", "list", "svc_1"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No hosts attached."), "{}", out.stderr);
}

#[test]
fn get_shows_one_host_or_not_found() {
    let ctx = Ctx::new();
    let one = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/hosts/shop.example.com")
            .header("authorization", auth());
        then.status(200).json_body(pending("shop.example.com"));
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/hosts/other.example.com");
        then.status(404).json_body(err_json("not_found", "not found"));
    });
    let out = ctx.run(&["host", "get", "svc_1", "Shop.Example.com"]).ok();
    one.assert();
    assert!(
        out.lines()
            .contains(&r#"DNS record: _rt-verify.shop.example.com. IN TXT "tok123""#.to_string()),
        "{:?}",
        out.lines()
    );
    // `status` is an alias of `get`
    let out = ctx.run(&["host", "status", "svc_1", "other.example.com"]).fails_with(1);
    assert!(
        out.stderr
            .contains("not_found: host other.example.com is not attached to svc_1"),
        "{}",
        out.stderr
    );
}

/// `GET …/hosts/shop.example.com` answering pending for the first `pending_for` calls, then
/// `last` (`None`: `404`, the claim is gone).
fn host_sequence(ctx: &Ctx, pending_for: usize, last: Option<Value>) -> httpmock::Mock<'_> {
    let n = Arc::new(AtomicUsize::new(0));
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/hosts/shop.example.com");
        then.respond_with(move |_| {
            let i = n.fetch_add(1, Ordering::SeqCst);
            let (status, body) = match (i < pending_for, &last) {
                (true, _) => (200, pending("shop.example.com")),
                (false, Some(v)) => (200, v.clone()),
                (false, None) => (404, err_json("not_found", "not found")),
            };
            HttpMockResponse::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(body.to_string())
                .build()
        });
    })
}

#[test]
fn status_wait_polls_until_the_host_is_active() {
    let ctx = Ctx::new();
    let m = host_sequence(&ctx, 3, Some(active("shop.example.com")));
    let out = ctx
        .run(&[
            "host",
            "get",
            "svc_1",
            "shop.example.com",
            "--wait",
            "--wait-interval",
            "10ms",
        ])
        .ok();
    m.assert_calls(4);
    assert!(out.lines().contains(&"Status: active".to_string()), "{:?}", out.lines());
    assert!(out.stderr.contains("Waiting for shop.example.com"), "{}", out.stderr);
}

#[test]
fn status_wait_fails_when_the_claim_goes_away_or_time_runs_out() {
    let ctx = Ctx::new();
    host_sequence(&ctx, 1, None);
    let out = ctx
        .run(&[
            "host",
            "get",
            "svc_1",
            "shop.example.com",
            "--wait",
            "--wait-interval",
            "10ms",
        ])
        .fails_with(1);
    assert!(out.stderr.contains("is gone"), "{}", out.stderr);

    let ctx = Ctx::new();
    host_sequence(&ctx, usize::MAX, None);
    let out = ctx
        .run(&[
            "host",
            "get",
            "svc_1",
            "shop.example.com",
            "--wait",
            "--wait-interval",
            "10ms",
            "--wait-timeout",
            "50ms",
        ])
        .fails_with(1);
    assert!(
        out.stderr.contains("still pending_verification after"),
        "{}",
        out.stderr
    );
}

#[test]
fn status_wait_in_json_mode_prints_only_the_final_view() {
    let ctx = Ctx::new();
    let m = host_sequence(&ctx, 2, Some(active("shop.example.com")));
    let out = ctx
        .run(&[
            "--json",
            "host",
            "get",
            "svc_1",
            "shop.example.com",
            "--wait",
            "--wait-interval",
            "10ms",
        ])
        .ok();
    m.assert_calls(3);
    assert_eq!(out.json(), active("shop.example.com"));
}

#[test]
fn a_pending_claim_without_an_expiry_shows_a_dash() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/hosts/shop.example.com");
        then.status(200).json_body(json!({
            "host": "shop.example.com", "status": "pending_verification", "verify_token": "tok123",
            "created_at": T0_MS, "expires_at": null
        }));
    });
    let out = ctx.run(&["host", "get", "svc_1", "shop.example.com"]).ok();
    assert!(out.lines().contains(&"Expires: -".to_string()), "{:?}", out.lines());
}

#[test]
fn get_reports_other_failures_as_they_are() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/hosts/shop.example.com");
        then.status(500)
            .json_body(err_json("internal", "the host table is unavailable"));
    });
    let out = ctx.run(&["host", "get", "svc_1", "shop.example.com"]).fails_with(1);
    assert!(out.stderr.contains("the host table is unavailable"), "{}", out.stderr);
    assert!(!out.stderr.contains("is not attached"), "{}", out.stderr);
}

#[test]
fn attach_refusals_other_than_a_cooldown_are_api_errors() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(PUT).path("/v1/services/svc_1/hosts/shop.example.com");
        then.status(403)
            .json_body(err_json("insufficient_scope", "the key lacks hosts:write"));
    });
    let out = ctx.run(&["host", "attach", "svc_1", "shop.example.com"]).fails_with(1);
    assert!(out.stderr.contains("insufficient_scope"), "{}", out.stderr);
    assert!(!out.stderr.contains("can be claimed again"), "{}", out.stderr);
}

#[test]
fn detach_deletes_the_mapping() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/hosts/shop.example.com")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["host", "detach", "hello", "shop.example.com"]).ok();
    m.assert();
    assert_eq!(out.stdout, "Host shop.example.com detached from hello (svc_1).\n");
    let out = ctx.run(&["-o", "json", "host", "rm", "hello", "shop.example.com"]).ok();
    assert_eq!(out.json(), json!({"detached": "shop.example.com"}));
}

// ---- data export -----------------------------------------------------------------------------

#[test]
fn data_export_writes_the_database_to_a_file() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let db = b"SQLite format 3\0\x10\x00\x01\x01".to_vec();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/data/export")
            .header("authorization", auth());
        then.status(200)
            .header("content-type", "application/vnd.sqlite3")
            .body(db.clone());
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hello.db");
    let out = ctx
        .run(&["data", "export", "hello", "--out", path.to_str().unwrap()])
        .ok();
    m.assert();
    assert_eq!(std::fs::read(&path).unwrap(), db);
    assert_eq!(
        out.stdout,
        format!(
            "Wrote the database of hello (svc_1) to {} (20 bytes).\n",
            path.display()
        )
    );

    // stdout that is not a terminal gets the raw bytes
    let out = ctx.cmd().args(["data", "export", "svc_1"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(out.stdout, db);
}

#[test]
fn data_export_needs_data_admin() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/data/export");
        then.status(403)
            .json_body(err_json("insufficient_scope", "needs data:admin"));
    });
    let out = ctx
        .run(&["data", "export", "svc_1", "--out", "/dev/null"])
        .fails_with(1);
    assert!(out.stderr.contains("insufficient_scope"), "{}", out.stderr);
}
