//! `21ci telemetry show`.

use crate::common;
use common::*;
use httpmock::prelude::*;
use serde_json::json;

#[test]
fn show_prints_the_state_and_the_report() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/telemetry/preview");
        then.status(200).json_body(json!({
            "sending": true,
            "url": "https://t.example",
            "report": {"install_id": "0", "version": "0.1.1", "requests": 4, "projects": 1,
                       "services": 2, "organizations": 1, "users": null}
        }));
    });
    let out = ctx.run(&["telemetry", "show"]).ok();
    m.assert();
    assert!(out.stdout.contains("Sending: on"), "{}", out.stdout);
    assert!(out.stdout.contains("https://t.example"), "{}", out.stdout);
    assert!(out.stdout.contains("\"requests\": 4"), "{}", out.stdout);
}

#[test]
fn show_says_off_when_nothing_is_sent() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/telemetry/preview");
        then.status(200)
            .json_body(json!({"sending": false, "url": null, "report": {}}));
    });
    let out = ctx.run(&["telemetry", "show"]).ok();
    assert!(out.stdout.contains("Sending: off"), "{}", out.stdout);
}
