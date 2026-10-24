//! `21ci schedule …` and `21ci queue …`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn schedule(id: &str, kind: &str) -> Value {
    if kind == "interval" {
        json!({"id": id, "kind": "interval", "every_seconds": 60, "cron": null, "timezone": null,
               "payload": {"a": 1}, "enabled": true, "next_due": T0_MS})
    } else {
        json!({"id": id, "kind": "cron", "every_seconds": null, "cron": "*/5 * * * *", "timezone": "Europe/Berlin",
               "payload": {}, "enabled": false, "next_due": null})
    }
}

// ---- schedules -------------------------------------------------------------------------------

#[test]
fn schedule_add_every_posts_an_interval() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/schedules")
            .header("authorization", auth())
            .json_body(json!({"kind": "interval", "every_seconds": 60, "payload": {"a": 1}}));
        then.status(201).json_body(schedule("sch_1", "interval"));
    });
    let out = ctx
        .run(&["schedule", "add", "hello", "--every", "60s", "--payload", r#"{"a":1}"#])
        .ok();
    m.assert();
    let lines = out.lines();
    assert_eq!(lines[0], "ID: sch_1");
    assert!(lines.contains(&"Kind: interval".to_string()), "{lines:?}");
    assert!(lines.contains(&"Schedule: every 60s".to_string()), "{lines:?}");
    assert!(lines.contains(&"Enabled: true".to_string()), "{lines:?}");
    assert!(lines.contains(&format!("Next due: {T0}")), "{lines:?}");
}

#[test]
fn schedule_every_accepts_humantime_and_bare_seconds() {
    for (arg, secs) in [("5m", 300), ("1h", 3600), ("90", 90), ("1m 30s", 90)] {
        let ctx = Ctx::new();
        ctx.lookup("hello", "svc_1");
        let m = ctx.server.mock(|when, then| {
            when.method(POST)
                .path("/v1/services/svc_1/schedules")
                .json_body(json!({"kind": "interval", "every_seconds": secs, "payload": {}}));
            then.status(201).json_body(schedule("sch_1", "interval"));
        });
        ctx.run(&["schedule", "add", "hello", "--every", arg]).ok();
        m.assert();
    }
}

#[test]
fn schedule_add_cron_with_timezone() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/schedules").json_body(json!({
            "kind": "cron", "cron": "*/5 * * * *", "timezone": "Europe/Berlin", "payload": {}, "enabled": false
        }));
        then.status(201).json_body(schedule("sch_2", "cron"));
    });
    ctx.run(&[
        "schedule",
        "add",
        "hello",
        "--cron",
        "*/5 * * * *",
        "--tz",
        "Europe/Berlin",
        "--disabled",
    ])
    .ok();
    m.assert();
}

#[test]
fn schedule_add_needs_exactly_one_of_every_and_cron_and_valid_payload() {
    let ctx = Ctx::new();
    ctx.run(&["schedule", "add", "hello"]).fails_with(2);
    ctx.run(&["schedule", "add", "hello", "--every", "1m", "--cron", "* * * * *"])
        .fails_with(2);
    ctx.run(&["schedule", "add", "hello", "--every", "soon"]).fails_with(2);
    ctx.run(&["schedule", "add", "hello", "--every", "1m", "--payload", "{nope"])
        .fails_with(2);
    ctx.run(&["schedule", "add", "hello", "--every", "0s"]).fails_with(2);
}

#[test]
fn schedule_too_frequent_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/schedules");
        then.status(422)
            .json_body(err_json("schedule_too_frequent", "minimum interval is 10s"));
    });
    let out = ctx.run(&["schedule", "add", "hello", "--every", "1s"]).fails_with(1);
    assert!(
        out.stderr
            .contains("schedule_too_frequent: minimum interval is 10s (HTTP 422)"),
        "{}",
        out.stderr
    );
}

#[test]
fn schedule_list_renders_a_table() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules")
            .header("authorization", auth());
        then.status(200)
            .json_body(json!([schedule("sch_1", "interval"), schedule("sch_2", "cron")]));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules/once")
            .header("authorization", auth());
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let out = ctx.run(&["schedule", "list", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID KIND SCHEDULE TZ ENABLED NEXT_DUE PAYLOAD",
            &format!("sch_1 interval every 60s - true {T0} {{\"a\":1}}"),
            "sch_2 cron */5 * * * * Europe/Berlin false - {}",
        ]
    );
}

fn one_off(id: &str, run_at_ms: i64) -> Value {
    json!({"id": id, "kind": "once", "run_at": run_at_ms})
}

#[test]
fn schedule_list_shows_one_off_schedules_after_the_recurring_ones_across_pages() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules")
            .header("authorization", auth());
        then.status(200).json_body(json!([schedule("sch_1", "interval")]));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules/once")
            .header("authorization", auth())
            .query_param_missing("cursor");
        then.status(200)
            .json_body(json!({"items": [one_off("welcome", T0_MS)], "next_cursor": "welcome"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules/once")
            .header("authorization", auth())
            .query_param("cursor", "welcome");
        then.status(200)
            .json_body(json!({"items": [one_off("later", T0_MS + 60_000)], "next_cursor": null}));
    });
    let out = ctx.run(&["schedule", "list", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "ID KIND SCHEDULE TZ ENABLED NEXT_DUE PAYLOAD",
            &format!("sch_1 interval every 60s - true {T0} {{\"a\":1}}"),
            &format!("welcome once - - - {T0} -"),
            "later once - - - 2026-01-01T00:01:00Z -",
        ]
    );
}

#[test]
fn schedule_list_json_appends_one_off_schedules_to_the_recurring_ones() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules")
            .header("authorization", auth());
        then.status(200).json_body(json!([schedule("sch_1", "interval")]));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/schedules/once")
            .header("authorization", auth());
        then.status(200)
            .json_body(json!({"items": [one_off("welcome", T0_MS)], "next_cursor": null}));
    });
    let out = ctx.run(&["-o", "json", "schedule", "list", "hello"]).ok();
    assert_eq!(
        out.json(),
        json!([schedule("sch_1", "interval"), one_off("welcome", T0_MS)])
    );
}

#[test]
fn schedule_rm_deletes() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/schedules/sch_1")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["schedule", "rm", "hello", "sch_1"]).ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Schedule sch_1 removed from hello (svc_1)."]);
    let out = ctx.run(&["-o", "json", "schedule", "rm", "hello", "sch_1"]).ok();
    assert_eq!(out.json(), json!({"deleted": "sch_1"}));
}

// ---- queue -----------------------------------------------------------------------------------

#[test]
fn queue_send_posts_body_delay_and_dedup() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/queue")
            .header("authorization", auth())
            .json_body(json!({"body": {"hello": "world"}, "delay_seconds": 30, "dedup_id": "d1"}));
        then.status(202).json_body(json!({"id": "msg_1"}));
    });
    let out = ctx
        .run(&[
            "queue",
            "send",
            "hello",
            "--body",
            r#"{"hello":"world"}"#,
            "--delay",
            "30s",
            "--dedup",
            "d1",
        ])
        .ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Enqueued msg_1 on hello (svc_1)."]);
}

#[test]
fn queue_send_minimal_and_json_output() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/queue")
            .json_body(json!({"body": [1, 2, 3]}));
        then.status(202).json_body(json!({"id": "msg_2"}));
    });
    let out = ctx
        .run(&["-o", "json", "queue", "send", "hello", "--body", "[1,2,3]"])
        .ok();
    m.assert();
    assert_eq!(out.json(), json!({"id": "msg_2"}));
}

#[test]
fn queue_send_body_from_file_and_stdin() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("msg.json");
    std::fs::write(&f, "{\"from\":\"file\"}\n").unwrap();
    let from_file = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/queue")
            .json_body(json!({"body": {"from": "file"}}));
        then.status(202).json_body(json!({"id": "msg_3"}));
    });
    ctx.run(&["queue", "send", "hello", "--body", &format!("@{}", f.display())])
        .ok();
    from_file.assert();

    let from_stdin = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/queue")
            .json_body(json!({"body": {"from": "stdin"}}));
        then.status(202).json_body(json!({"id": "msg_4"}));
    });
    ctx.run_stdin(&["queue", "send", "hello", "--body", "-"], "{\"from\":\"stdin\"}")
        .ok();
    from_stdin.assert();
}

#[test]
fn queue_send_rejects_non_json_bodies() {
    let ctx = Ctx::new();
    let out = ctx.run(&["queue", "send", "hello", "--body", "not json"]).fails_with(2);
    assert!(out.stderr.contains("--body"), "{}", out.stderr);
    ctx.run(&["queue", "send", "hello"]).fails_with(2);
}

#[test]
fn queue_full_is_an_api_error() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/queue");
        then.status(429).json_body(err_json("queue_full", "queue is full"));
    });
    ctx.run(&["queue", "send", "hello", "--body", "{}"]).fails_with(1);
}

#[test]
fn queue_stats() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let stats = json!({"depth": 3, "in_flight": 1, "oldest_age_ms": 1500, "dlq": 2});
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/queue")
            .header("authorization", auth());
        then.status(200).json_body(stats.clone());
    });
    let out = ctx.run(&["queue", "stats", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec!["Depth: 3", "In flight: 1", "Oldest age ms: 1500", "DLQ: 2"]
    );
    let out = ctx.run(&["-o", "json", "queue", "stats", "hello"]).ok();
    assert_eq!(out.json(), stats);
}

#[test]
fn queue_dlq_lists_dead_letters() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/dlq")
            .header("authorization", auth());
        then.status(200).json_body(json!({"items": [
            {"id": "msg_1", "body": {"x": 1}, "attempts": 5, "failed_at": T0_MS}
        ]}));
    });
    let out = ctx.run(&["queue", "dlq", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec!["ID ATTEMPTS FAILED_AT BODY", &format!("msg_1 5 {T0} {{\"x\":1}}")]
    );
}

#[test]
fn queue_dlq_redrive_and_purge() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let redrive = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/dlq/redrive")
            .header("authorization", auth());
        then.status(200).json_body(json!({"redriven": 2}));
    });
    let out = ctx.run(&["queue", "dlq", "hello", "--redrive"]).ok();
    redrive.assert();
    assert_eq!(out.lines(), vec!["Redrove 2 messages from the DLQ of hello (svc_1)."]);
    let out = ctx.run(&["-o", "json", "queue", "dlq", "hello", "--redrive"]).ok();
    assert_eq!(out.json(), json!({"redriven": 2}));

    let purge = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/dlq")
            .header("authorization", auth());
        then.status(204);
    });
    let out = ctx.run(&["queue", "dlq", "hello", "--purge"]).ok();
    purge.assert();
    assert_eq!(out.lines(), vec!["Purged the DLQ of hello (svc_1)."]);
}

#[test]
fn queue_dlq_redrive_and_purge_conflict() {
    Ctx::new()
        .run(&["queue", "dlq", "hello", "--redrive", "--purge"])
        .fails_with(2);
}

// ---- schedule update -------------------------------------------------------------------------

#[test]
fn schedule_update_with_a_new_interval_needs_no_lookup_of_the_kind() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let list = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/schedules");
        then.status(200).json_body(json!([]));
    });
    let mut updated = schedule("sch_1", "interval");
    updated["every_seconds"] = json!(600);
    updated["enabled"] = json!(false);
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1/schedules/sch_1")
            .header("authorization", auth())
            .json_body(json!({"kind": "interval", "every_seconds": 600, "enabled": false}));
        then.status(200).json_body(updated);
    });
    let out = ctx
        .run(&["schedule", "update", "hello", "sch_1", "--every", "10m", "--disable"])
        .ok();
    m.assert();
    list.assert_calls(0);
    assert!(out.lines().contains(&"Enabled: false".to_string()), "{:?}", out.lines());
}

#[test]
fn schedule_update_keeps_the_current_kind() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/schedules");
        then.status(200)
            .json_body(json!([schedule("sch_0", "interval"), schedule("sch_1", "cron")]));
    });
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/services/svc_1/schedules/sch_1")
            .json_body(json!({"kind": "cron", "timezone": "Europe/Berlin", "payload": {"x": 2}, "enabled": true}));
        then.status(200).json_body(schedule("sch_1", "cron"));
    });
    ctx.run(&[
        "schedule",
        "update",
        "svc_1",
        "sch_1",
        "--tz",
        "Europe/Berlin",
        "--payload",
        r#"{"x":2}"#,
        "--enable",
    ])
    .ok();
    m.assert();

    // --tz on an interval schedule is refused once its kind is known
    let out = ctx
        .run(&["schedule", "update", "svc_1", "sch_0", "--tz", "UTC"])
        .fails_with(2);
    assert!(
        out.stderr.contains("--tz applies to cron schedules only"),
        "{}",
        out.stderr
    );
    // an unknown schedule
    let out = ctx
        .run(&["schedule", "update", "svc_1", "sch_9", "--enable"])
        .fails_with(1);
    assert!(out.stderr.contains("schedule sch_9 not found"), "{}", out.stderr);
}

#[test]
fn schedule_update_validates_flags_before_any_request() {
    let ctx = Ctx::new();
    let any = ctx.server.mock(|when, then| {
        when.any_request();
        then.status(200).json_body(json!([]));
    });
    ctx.run(&["schedule", "update", "svc_1", "sch_1"]).fails_with(2);
    ctx.run(&[
        "schedule",
        "update",
        "svc_1",
        "sch_1",
        "--every",
        "1m",
        "--cron",
        "* * * * *",
    ])
    .fails_with(2);
    ctx.run(&["schedule", "update", "svc_1", "sch_1", "--enable", "--disable"])
        .fails_with(2);
    ctx.run(&["schedule", "update", "svc_1", "sch_1", "--every", "1m", "--tz", "UTC"])
        .fails_with(2);
    ctx.run(&["schedule", "update", "svc_1", "sch_1", "--payload", "nope"])
        .fails_with(2);
    ctx.run(&["schedule", "update", "svc_1", "sch_1", "--every", "0s"])
        .fails_with(2);
    any.assert_calls(0);
}
