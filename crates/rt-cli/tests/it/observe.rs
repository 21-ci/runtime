//! `21ci logs`, `stats`, `events`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn log(seq: u64, level: &str, task: Option<&str>, msg: &str) -> Value {
    json!({"seq": seq, "ts": T0_MS + 123, "level": level, "service_id": "svc_1", "version": 3, "task_id": task,
           "trigger": "http", "kind": "runtime", "msg": msg, "truncated": false})
}

fn sse(events: &[Value]) -> String {
    let mut s = String::from(": keep-alive\n\n");
    for e in events {
        s.push_str(&format!("id: {}\nevent: log\ndata: {}\n\n", e["seq"], e));
    }
    s
}

#[test]
fn logs_prints_one_line_per_record() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .header("authorization", auth())
            .query_param_missing("follow")
            .query_param_missing("level");
        then.status(200).json_body(json!({
            "items": [log(1, "info", Some("task_1"), "hello world"), log(2, "error", None, "boom")],
            "next_cursor": null
        }));
    });
    let out = ctx.run(&["logs", "hello"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "2026-01-01T00:00:00.123Z info v3 task_1 hello world",
            "2026-01-01T00:00:00.123Z error v3 - boom",
        ]
    );
}

#[test]
fn logs_filters_are_forwarded() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("since", "2026-01-01T00:00:00Z")
            .query_param("until", "2026-01-02T00:00:00Z")
            .query_param("level", "warn")
            .query_param("task_id", "task_1")
            .query_param("kind", "build")
            .query_param("limit", "5")
            .query_param("cursor", "9");
        then.status(200).json_body(json!({"items": [], "next_cursor": "10"}));
    });
    let out = ctx
        .run(&[
            "logs",
            "hello",
            "--since",
            "2026-01-01T00:00:00Z",
            "--until",
            "2026-01-02T00:00:00Z",
            "--level",
            "warn",
            "--task",
            "task_1",
            "--kind",
            "build",
            "--limit",
            "5",
            "--cursor",
            "9",
        ])
        .ok();
    m.assert();
    assert!(out.stderr.contains("--cursor 10"), "{}", out.stderr);
}

#[test]
fn logs_relative_since_becomes_an_rfc3339_timestamp() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param_matches("since", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    ctx.run(&["logs", "hello", "--since", "15m"]).ok();
    m.assert();
}

#[test]
fn logs_rejects_a_bad_level_and_a_bad_since() {
    let ctx = Ctx::new();
    ctx.run(&["logs", "hello", "--level", "loud"]).fails_with(2);
    ctx.run(&["logs", "hello", "--since", "yesterday-ish"]).fails_with(2);
}

#[test]
fn logs_json_prints_the_page() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let page = json!({"items": [log(1, "info", Some("task_1"), "hi")], "next_cursor": null});
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/logs");
        then.status(200).json_body(page.clone());
    });
    let out = ctx.run(&["-o", "json", "logs", "hello"]).ok();
    assert_eq!(out.json(), page);
}

#[test]
fn logs_follow_streams_server_sent_events() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .header("authorization", auth())
            .header("accept", "text/event-stream")
            .query_param("follow", "1")
            .query_param("level", "info")
            .query_param("task_id", "task_1")
            .header_missing("last-event-id");
        then.status(200).header("content-type", "text/event-stream").body(sse(&[
            log(1, "info", Some("task_1"), "first"),
            log(2, "warn", Some("task_1"), "second"),
        ]));
    });
    let out = ctx
        .run(&["logs", "hello", "-f", "--level", "info", "--task", "task_1"])
        .ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "2026-01-01T00:00:00.123Z info v3 task_1 first",
            "2026-01-01T00:00:00.123Z warn v3 task_1 second",
        ]
    );
}

#[test]
fn logs_follow_json_is_ndjson() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let (a, b) = (log(1, "info", None, "a"), log(2, "info", None, "b"));
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(sse(&[a.clone(), b.clone()]));
    });
    let out = ctx.run(&["-o", "json", "logs", "hello", "--follow"]).ok();
    let parsed: Vec<Value> = out
        .stdout
        .lines()
        .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
        .collect();
    assert_eq!(parsed, vec![a, b]);
}

#[test]
fn logs_follow_ignores_comments_and_unknown_events_and_handles_split_data() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let body = format!(
        ": hi\n\nevent: ping\ndata: ignore me\n\nid: 7\nevent: log\ndata: {}\r\n\r\n",
        log(7, "debug", None, "after-ping")
    );
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(body.clone());
    });
    let out = ctx.run(&["logs", "hello", "-f"]).ok();
    assert_eq!(out.lines(), vec!["2026-01-01T00:00:00.123Z debug v3 - after-ping"]);
}

#[test]
fn logs_follow_with_since_prints_the_backlog_then_resumes_after_its_last_seq() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let backlog = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("since", "2026-01-01T00:00:00Z")
            .query_param_missing("follow");
        then.status(200).json_body(
            json!({"items": [log(5, "info", None, "old-1"), log(6, "info", None, "old-2")], "next_cursor": null}),
        );
    });
    let live = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1")
            .header("last-event-id", "6");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(sse(&[log(7, "info", None, "new-1")]));
    });
    let out = ctx
        .run(&["logs", "hello", "-f", "--since", "2026-01-01T00:00:00Z"])
        .ok();
    backlog.assert();
    live.assert();
    assert_eq!(
        out.lines()
            .iter()
            .map(|l| l.rsplit(' ').next().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["old-1", "old-2", "new-1"]
    );
}

#[test]
fn logs_follow_api_error_exits_1() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1");
        then.status(429)
            .json_body(err_json("rate_limited", "too many concurrent streams for this key"));
    });
    let out = ctx.run(&["logs", "hello", "-f"]).fails_with(1);
    assert!(out.stderr.contains("rate_limited"), "{}", out.stderr);
}

#[test]
fn logs_until_cannot_be_combined_with_follow() {
    let ctx = Ctx::new();
    let out = ctx
        .run(&["logs", "hello", "--follow", "--until", "2026-01-02T00:00:00Z"])
        .fails_with(2);
    assert!(
        out.stderr.contains("--until cannot be combined with --follow"),
        "{}",
        out.stderr
    );
}

#[test]
fn logs_follow_pages_through_the_backlog_before_streaming() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let first = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("since", "2026-01-01T00:00:00Z")
            .query_param("limit", "1000")
            .query_param_missing("cursor")
            .query_param_missing("follow");
        then.status(200)
            .json_body(json!({"items": [log(5, "info", None, "page-1")], "next_cursor": "c1"}));
    });
    let second = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("cursor", "c1")
            .query_param_missing("follow");
        then.status(200)
            .json_body(json!({"items": [log(6, "info", None, "page-2")], "next_cursor": null}));
    });
    let live = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1")
            .header("last-event-id", "6");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(sse(&[log(7, "info", None, "live")]));
    });
    let out = ctx
        .run(&["logs", "hello", "-f", "--since", "2026-01-01T00:00:00Z"])
        .ok();
    first.assert();
    second.assert();
    live.assert();
    let msgs: Vec<&str> = out.stdout.lines().map(|l| l.rsplit(' ').next().unwrap()).collect();
    assert_eq!(msgs, vec!["page-1", "page-2", "live"]);
}

#[test]
fn logs_follow_skips_an_event_whose_data_is_not_json() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let body = format!(
        "event: log\ndata: {{not json\n\n{}",
        sse(&[log(1, "info", None, "kept")])
    );
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1");
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(body.clone());
    });
    let out = ctx.run(&["logs", "hello", "-f"]).ok();
    assert_eq!(out.lines(), vec!["2026-01-01T00:00:00.123Z info v3 - kept"]);
    assert!(
        out.stderr.contains("skipping an unreadable log event"),
        "{}",
        out.stderr
    );
}

#[test]
fn logs_follow_stream_that_breaks_exits_3() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/logs")
            .query_param("follow", "1");
        // not UTF-8: the line reader fails, and the stream is reported as interrupted
        then.status(200)
            .header("content-type", "text/event-stream")
            .body(b"event: log\ndata: \xff\n\n");
    });
    let out = ctx.run(&["logs", "hello", "-f"]).fails_with(3);
    assert!(out.stderr.contains("log stream interrupted"), "{}", out.stderr);
}

// ---- stats -----------------------------------------------------------------------------------

fn stats_body() -> Value {
    json!({
        "count": 100, "errors": 2, "error_rate": 0.02,
        "outcomes": {"guest_error": 2, "ok": 98},
        "queue": {"p50_ms": 0.1, "p95_ms": 0.2, "p99_ms": 0.3},
        "init": {"p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0},
        "exec": {"p50_ms": 10.0, "p95_ms": 20.5, "p99_ms": 30.25},
        "total": {"p50_ms": 11.1, "p95_ms": 22.7, "p99_ms": 33.5}
    })
}

#[test]
fn stats_summarises_counts_outcomes_and_latency() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/stats")
            .header("authorization", auth())
            .query_param("window", "1h");
        then.status(200).json_body(stats_body());
    });
    let out = ctx.run(&["stats", "hello", "--window", "1h"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "Window: 1h",
            "Requests: 100",
            "Errors: 2 (2.00%)",
            "Outcomes: guest_error=2 ok=98",
            "PHASE P50_MS P95_MS P99_MS",
            "queue 0.1 0.2 0.3",
            "init 1.0 2.0 3.0",
            "exec 10.0 20.5 30.2",
            "total 11.1 22.7 33.5",
        ]
    );
}

#[test]
fn stats_default_window_is_left_to_the_server() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/stats")
            .query_param_missing("window");
        then.status(200).json_body(stats_body());
    });
    let out = ctx.run(&["stats", "hello"]).ok();
    m.assert();
    assert_eq!(out.lines()[0], "Window: 5m");
    let out = ctx.run(&["-o", "json", "stats", "hello"]).ok();
    assert_eq!(out.json(), stats_body());
}

#[test]
fn stats_rejects_unknown_windows() {
    Ctx::new().run(&["stats", "hello", "--window", "2d"]).fails_with(2);
}

// ---- events ----------------------------------------------------------------------------------

#[test]
fn events_renders_a_table_and_paginates() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/events")
            .header("authorization", auth())
            .query_param("limit", "2")
            .query_param("cursor", "c0");
        then.status(200).json_body(json!({
            "items": [
                {"id": "evt_1", "ts": T0_MS, "kind": "deploy", "data": {"version": 3}},
                {"id": "evt_2", "ts": T0_MS, "kind": "live.changed", "data": {}}
            ],
            "next_cursor": "c1"
        }));
    });
    let out = ctx.run(&["events", "hello", "--limit", "2", "--cursor", "c0"]).ok();
    m.assert();
    assert_eq!(
        out.lines(),
        vec![
            "TIME KIND DATA",
            &format!("{T0} deploy {{\"version\":3}}"),
            &format!("{T0} live.changed {{}}"),
        ]
    );
    assert!(out.stderr.contains("--cursor c1"), "{}", out.stderr);
}

#[test]
fn events_with_audit_actors_get_an_actor_column_and_show_the_inner_data() {
    // the real server wraps control-plane events as {"actor": {...}, "data": {...}}
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/events");
        then.status(200).json_body(json!({
            "items": [
                {"id": "evt_1", "ts": T0_MS, "kind": "deploy",
                 "data": {"actor": {"kind": "key", "key_id": "key_9", "tenant": "ten_1"}, "data": {"version": 3}}},
                {"id": "evt_2", "ts": T0_MS, "kind": "rollback",
                 "data": {"actor": {"kind": "operator"}, "data": {"from": 3, "to": 2}}}
            ],
            "next_cursor": null
        }));
    });
    let out = ctx.run(&["events", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "TIME KIND ACTOR DATA",
            &format!("{T0} deploy key_9 {{\"version\":3}}"),
            &format!("{T0} rollback operator {{\"from\":3,\"to\":2}}"),
        ]
    );
}

#[test]
fn audit_rows_without_a_kind_or_an_actor_show_a_dash() {
    // one audited list holds an actor without a kind, a record without an actor, and a record
    // without any data: the ACTOR column shows `-` for the last two, `{}` for the empty payloads
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/events");
        then.status(200).json_body(json!({
            "items": [
                {"id": "evt_1", "ts": T0_MS, "kind": "deploy",
                 "data": {"actor": {}, "data": {"x": 1}}},
                {"id": "evt_2", "ts": T0_MS, "kind": "live.changed", "data": {}},
                {"id": "evt_3", "ts": T0_MS, "kind": "misc"}
            ],
            "next_cursor": null
        }));
    });
    let out = ctx.run(&["events", "hello"]).ok();
    assert_eq!(
        out.lines(),
        vec![
            "TIME KIND ACTOR DATA",
            &format!("{T0} deploy - {{\"x\":1}}"),
            &format!("{T0} live.changed - {{}}"),
            &format!("{T0} misc - {{}}"),
        ]
    );
}

#[test]
fn events_json_prints_the_page_as_returned() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let page = json!({
        "items": [{"id": "evt_1", "ts": T0_MS, "kind": "deploy", "data": {"version": 3}}],
        "next_cursor": null
    });
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/events");
        then.status(200).json_body(page.clone());
    });
    let out = ctx.run(&["--json", "events", "hello"]).ok();
    assert_eq!(out.json(), page);
}

#[test]
fn events_with_nothing_to_show_say_so() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/events");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let out = ctx.run(&["events", "hello"]).ok();
    assert_eq!(out.stdout, "");
    assert!(out.stderr.contains("No events found."), "{}", out.stderr);
}
