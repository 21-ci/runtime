//! `21ci topic …`, `21ci consumer …`, `21ci pubsub publish`, `21ci invocation …`.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::{json, Value};

fn auth() -> String {
    format!("Bearer {TOKEN}")
}

fn topic(name: &str) -> Value {
    json!({"id": "tpc_1", "name": name, "project": "shop", "project_id": "prj_1", "environment": "staging",
           "partitions": 4, "retention_hours": 48, "max_mb": 10, "bytes": 1234, "created_at": T0_MS,
           "heads": [{"partition": 0, "first": 0, "next": 12}, {"partition": 1, "first": 3, "next": 5}]})
}

fn consumer_view() -> Value {
    json!({"topic": "orders", "group": "default", "declared": true, "topic_exists": true, "dead": 2,
           "partitions": [
               {"partition": 0, "committed": 10, "next": 12, "lag": 2, "attempt": 0, "state": "active",
                "last_error": null, "next_at": 0},
               {"partition": 1, "committed": 4, "next": 5, "lag": 1, "attempt": 3, "state": "blocked",
                "last_error": "guest_error", "next_at": 0}]})
}

fn invocation(id: &str, state: &str) -> Value {
    json!({"id": id, "state": state, "method": "POST", "path": "/hook", "has_query": true, "attempts": 3,
           "status": 500, "condition": "retries_exhausted", "error": "status_5xx", "body_bytes": 12,
           "created_at": T0_MS, "updated_at": T0_MS, "expires_at": T0_MS})
}

// ---- topics ------------------------------------------------------------------------------------

/// `topic create` posts the name, the namespace, the partitions, the retention in hours and the cap.
#[test]
fn topic_create_posts_name_partitions_retention_and_namespace() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/topics")
            .header("authorization", auth())
            .json_body(json!({"name": "orders", "project": "shop", "environment": "staging",
                              "partitions": 4, "retention_hours": 48, "max_mb": 10}));
        then.status(201).json_body(topic("orders"));
    });
    let lines = ctx
        .run(&[
            "topic",
            "create",
            "orders",
            "--project",
            "shop",
            "--env",
            "staging",
            "--partitions",
            "4",
            "--retention",
            "2d",
            "--max-mb",
            "10",
        ])
        .ok()
        .lines();
    m.assert();
    for want in [
        "ID: tpc_1",
        "Name: orders",
        "Partitions: 4",
        "Retention: 48h",
        "Heads: 0: 0..12, 1: 3..5",
    ] {
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
    assert!(!lines.iter().any(|l| l.starts_with("Consumers:")), "{lines:?}");
}

/// `--retention` must be whole hours: refused before any request, exit 2.
#[test]
fn retention_must_be_whole_hours() {
    let ctx = Ctx::new();
    for args in [
        vec!["topic", "create", "orders", "--retention", "90m"],
        vec!["topic", "update", "orders", "--retention", "1h 30m"],
        vec!["topic", "create", "orders", "--retention", "0"],
    ] {
        let out = ctx.run(&args).fails_with(2);
        assert!(
            out.stderr.contains("retention must be a whole number of hours"),
            "{}",
            out.stderr
        );
    }
}

#[test]
fn topic_list_renders_a_table_or_the_empty_notice() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/topics").query_param("project", "shop");
        then.status(200).json_body(json!({"items": [topic("orders")]}));
    });
    let lines = ctx.run(&["topic", "list", "--project", "shop"]).ok().lines();
    m.assert();
    assert_eq!(lines[0], "ID NAME PROJECT ENV PARTITIONS RETENTION BYTES");
    assert_eq!(lines[1], "tpc_1 orders shop staging 4 48h 1234");

    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/topics").query_param_missing("project");
        then.status(200).json_body(json!({"items": []}));
    });
    let out = ctx.run(&["topic", "list"]).ok();
    assert!(out.stderr.contains("No topics found."), "{}", out.stderr);
    let out = ctx.run(&["topic", "list", "--json"]).ok();
    assert_eq!(out.json(), json!({"items": []}));
}

#[test]
fn topic_get_shows_the_consumers() {
    let ctx = Ctx::new();
    let mut t = topic("orders");
    t["consumers"] = json!([{"service": "svc_1", "slug": "billing", "group": "default", "lag": 3},
                            {"service": "svc_2", "slug": null, "group": "audit", "lag": 0}]);
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/topics/orders")
            .query_param("environment", "staging");
        then.status(200).json_body(t);
    });
    let lines = ctx.run(&["topic", "get", "orders", "--env", "staging"]).ok().lines();
    m.assert();
    assert!(
        lines.contains(&"Consumers: billing/default (lag 3), svc_2/audit (lag 0)".to_string()),
        "{lines:?}"
    );

    let mut t = topic("orders");
    t["consumers"] = json!([]);
    t["heads"] = json!([]);
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/topics/tpc_1");
        then.status(200).json_body(t);
    });
    let lines = ctx.run(&["topic", "get", "tpc_1"]).ok().lines();
    assert!(lines.contains(&"Consumers: none".to_string()), "{lines:?}");
    assert!(lines.contains(&"Heads: -".to_string()), "{lines:?}");
}

/// An unknown topic is `404 not_found` (exit 1); a plan limit is `422 limit_exceeds_plan` (exit 1).
#[test]
fn topic_errors_exit_1_with_the_code() {
    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/topics/nope");
        then.status(404).json_body(err_json("not_found", "topic not found"));
    });
    let out = ctx.run(&["topic", "get", "nope"]).fails_with(1);
    assert!(out.stderr.contains("not_found"), "{}", out.stderr);

    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/topics");
        then.status(422)
            .json_body(err_json("limit_exceeds_plan", "partitions over the plan"));
    });
    let out = ctx
        .run(&["topic", "create", "big", "--partitions", "999"])
        .fails_with(1);
    assert!(out.stderr.contains("limit_exceeds_plan"), "{}", out.stderr);
}

#[test]
fn topic_update_patches_retention_and_cap() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(PATCH)
            .path("/v1/topics/orders")
            .query_param("project", "shop")
            .json_body(json!({"retention_hours": 168, "max_mb": 0}));
        then.status(200).json_body(topic("orders"));
    });
    ctx.run(&[
        "topic",
        "update",
        "orders",
        "--project",
        "shop",
        "--retention",
        "7d",
        "--max-mb",
        "0",
    ])
    .ok();
    m.assert();
    let out = ctx.run(&["topic", "update", "orders"]).fails_with(2);
    assert!(out.stderr.contains("nothing to change"), "{}", out.stderr);
}

#[test]
fn topic_delete_needs_yes_without_a_terminal() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/topics/orders")
            .query_param("environment", "staging");
        then.status(204);
    });
    let out = ctx
        .run(&["topic", "delete", "orders", "--env", "staging"])
        .fails_with(2);
    assert!(out.stderr.contains("--yes"), "{}", out.stderr);
    assert_eq!(m.calls(), 0);
    let out = ctx
        .run(&["topic", "delete", "orders", "--env", "staging", "--yes"])
        .ok();
    assert!(out.stdout.contains("Topic orders deleted."), "{}", out.stdout);
    let out = ctx
        .run(&["topic", "rm", "orders", "--env", "staging", "--yes", "--json"])
        .ok();
    assert_eq!(out.json(), json!({"deleted": "orders"}));
    assert_eq!(m.calls(), 2);
}

#[test]
fn topic_append_posts_one_record_with_key_headers_and_dedup() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/topics/orders/messages").json_body(json!({
            "records": [{"body": {"n": 1}, "key": "cust-1", "headers": {"a": "1", "b": "x=y"},
                         "dedup_id": "d1"}],
            "project": "shop"
        }));
        then.status(202)
            .json_body(json!({"items": [{"partition": 2, "offset": 7, "duplicate": false}]}));
    });
    let out = ctx
        .run(&[
            "topic",
            "append",
            "orders",
            "--project",
            "shop",
            "--body",
            r#"{"n":1}"#,
            "--key",
            "cust-1",
            "--header",
            "a=1",
            "--header",
            "b=x=y",
            "--dedup",
            "d1",
        ])
        .ok();
    m.assert();
    assert_eq!(out.lines(), vec!["Appended to orders: partition 2, offset 7."]);

    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/topics/orders/messages");
        then.status(202)
            .json_body(json!({"items": [{"partition": 0, "offset": 3, "duplicate": true}]}));
    });
    let out = ctx.run(&["topic", "append", "orders", "--body", "1"]).ok();
    assert!(out.stdout.contains("(duplicate: already appended)"), "{}", out.stdout);
    let out = ctx
        .run(&["topic", "append", "orders", "--body", "1", "-o", "json"])
        .ok();
    assert_eq!(out.json()["items"][0]["offset"], json!(3));
    // a bad header pair or body is a usage error
    ctx.run(&["topic", "append", "orders", "--body", "1", "--header", "nope"])
        .fails_with(2);
    ctx.run(&["topic", "append", "orders", "--body", "{"]).fails_with(2);
}

/// `topic read` passes `--from`, `--limit` and `--partition` as query parameters.
#[test]
fn topic_read_passes_from_and_limit() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/topics/orders/messages")
            .query_param("partition", "1")
            .query_param("from", "earliest")
            .query_param("limit", "2");
        then.status(200).json_body(json!({
            "items": [{"partition": 1, "offset": 0, "key": "k", "headers": {}, "body": {"a": 1}, "timestamp": T0_MS},
                      {"partition": 1, "offset": 1, "key": null, "headers": {}, "body": "txt", "timestamp": T0_MS}],
            "next": 2, "end": false
        }));
    });
    let out = ctx
        .run(&[
            "topic",
            "read",
            "orders",
            "--partition",
            "1",
            "--from",
            "earliest",
            "--limit",
            "2",
        ])
        .ok();
    m.assert();
    let lines = out.lines();
    assert_eq!(lines[0], "PARTITION OFFSET KEY TIMESTAMP BODY");
    assert!(lines[1].starts_with("1 0 k "), "{lines:?}");
    assert!(lines[1].ends_with(r#"{"a":1}"#), "{lines:?}");
    assert!(out.stderr.contains("(more records: --from 2)"), "{}", out.stderr);

    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/topics/orders/messages")
            .query_param("from_time", T0);
        then.status(200).json_body(json!({"items": [], "next": 0, "end": true}));
    });
    let out = ctx.run(&["topic", "read", "orders", "--from-time", T0]).ok();
    m.assert();
    assert!(out.stderr.contains("No records."), "{}", out.stderr);
    assert!(!out.stderr.contains("more records"), "{}", out.stderr);
    let out = ctx.run(&["topic", "read", "orders", "--from-time", T0, "--json"]).ok();
    assert_eq!(out.json()["end"], json!(true));
}

/// Without `--partition` no `partition` parameter is sent (the server decides or refuses).
#[test]
fn topic_read_without_partition_sends_none() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/topics/orders/messages")
            .query_param_missing("partition");
        then.status(422).json_body(err_json(
            "validation_failed",
            "topic `orders` has 4 partitions: pass `partition`",
        ));
    });
    let out = ctx.run(&["topic", "read", "orders"]).fails_with(1);
    m.assert();
    assert!(out.stderr.contains("pass `partition`"), "{}", out.stderr);
}

// ---- consumers ---------------------------------------------------------------------------------

#[test]
fn consumer_list_shows_one_row_per_partition() {
    let ctx = Ctx::new();
    ctx.lookup("billing", "svc_1");
    let orphan = json!({"topic": "gone", "group": "g", "declared": true, "topic_exists": false, "dead": 0,
                        "partitions": []});
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/consumers");
        then.status(200).json_body(json!({"items": [consumer_view(), orphan]}));
    });
    let lines = ctx.run(&["consumer", "list", "billing"]).ok().lines();
    m.assert();
    assert_eq!(
        lines,
        vec![
            "TOPIC GROUP PARTITION COMMITTED NEXT LAG STATE DEAD",
            "orders default 0 10 12 2 active 2",
            "orders default 1 4 5 1 blocked 2",
            "gone g (no topic) - - - - 0",
        ]
    );

    let ctx = Ctx::new();
    ctx.lookup("billing", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/consumers");
        then.status(200).json_body(json!({"items": []}));
    });
    let out = ctx.run(&["consumer", "list", "billing"]).ok();
    assert!(out.stderr.contains("has no stream consumers"), "{}", out.stderr);
    assert_eq!(
        ctx.run(&["consumer", "ls", "billing", "--json"]).ok().json(),
        json!({"items": []})
    );
}

/// `--to` takes `earliest`, `latest`, an offset (sent as a number) or an RFC 3339 time.
#[test]
fn consumer_reset_accepts_offset_time_and_keywords() {
    for (arg, to, partition) in [
        ("earliest", json!("earliest"), None),
        ("latest", json!("latest"), Some(1)),
        ("42", json!(42), Some(0)),
        (T0, json!(T0), None),
    ] {
        let ctx = Ctx::new();
        ctx.lookup("billing", "svc_1");
        let mut body = json!({"to": to});
        if let Some(p) = partition {
            body["partition"] = json!(p);
        }
        let m = ctx.server.mock(|when, then| {
            when.method(POST)
                .path("/v1/services/svc_1/consumers/orders/default/reset")
                .json_body(body);
            then.status(200).json_body(consumer_view());
        });
        let mut args = vec!["consumer", "reset", "billing", "orders", "default", "--to", arg];
        let p = partition.map(|p: u32| p.to_string());
        if let Some(p) = &p {
            args.extend(["--partition", p.as_str()]);
        }
        let out = ctx.run(&args).ok();
        m.assert();
        assert!(
            out.stdout.contains("Reset orders/default of billing (svc_1)"),
            "{}",
            out.stdout
        );
    }
    let ctx = Ctx::new();
    let out = ctx
        .run(&["consumer", "reset", "billing", "orders", "default", "--to", "yesterday"])
        .fails_with(2);
    assert!(out.stderr.contains("invalid --to"), "{}", out.stderr);

    let ctx = Ctx::new();
    ctx.lookup("billing", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/services/svc_1/consumers/orders/default/reset");
        then.status(200).json_body(consumer_view());
    });
    let out = ctx
        .run(&[
            "consumer", "reset", "billing", "orders", "default", "--to", "latest", "--json",
        ])
        .ok();
    assert_eq!(out.json()["dead"], json!(2));
}

#[test]
fn consumer_dlq_list_and_purge() {
    let ctx = Ctx::new();
    ctx.lookup("billing", "svc_1");
    let list = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/consumers/orders/default/dlq")
            .query_param("limit", "5");
        then.status(200).json_body(json!({"items": [
            {"partition": 0, "offset": 7, "key": null, "headers": {}, "body": {"x": 1}, "attempts": 10,
             "error": "guest_error", "failed_at": T0_MS}]}));
    });
    let lines = ctx
        .run(&[
            "consumer", "dlq", "list", "billing", "orders", "default", "--limit", "5",
        ])
        .ok()
        .lines();
    list.assert();
    assert_eq!(lines[0], "PARTITION OFFSET ATTEMPTS ERROR FAILED_AT BODY");
    assert!(lines[1].starts_with("0 7 10 guest_error "), "{lines:?}");

    let purge = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/consumers/orders/default/dlq");
        then.status(204);
    });
    let out = ctx
        .run(&["consumer", "dlq", "purge", "billing", "orders", "default"])
        .ok();
    assert!(
        out.stdout.contains("Purged the dead letters of orders/default"),
        "{}",
        out.stdout
    );
    let out = ctx
        .run(&["consumer", "dlq", "purge", "billing", "orders", "default", "--json"])
        .ok();
    assert_eq!(out.json(), json!({"purged": true}));
    assert_eq!(purge.calls(), 2);

    let ctx = Ctx::new();
    ctx.lookup("billing", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/consumers/orders/default/dlq");
        then.status(200).json_body(json!({"items": []}));
    });
    let out = ctx
        .run(&["consumer", "dlq", "list", "billing", "orders", "default"])
        .ok();
    assert!(out.stderr.contains("No dead letters."), "{}", out.stderr);
    let out = ctx
        .run(&["consumer", "dlq", "list", "billing", "orders", "default", "--json"])
        .ok();
    assert_eq!(out.json(), json!({"items": []}));
}

// ---- pub/sub -----------------------------------------------------------------------------------

#[test]
fn pubsub_publish_posts_subject_body_headers() {
    let ctx = Ctx::new();
    let m = ctx.server.mock(|when, then| {
        when.method(POST)
            .path("/v1/pubsub/publish")
            .header("authorization", auth())
            .json_body(json!({"subject": "orders.eu.created", "body": {"id": 1},
                              "headers": {"trace": "t1"}, "project": "shop", "environment": "staging"}));
        then.status(200).json_body(json!({"receivers": 3, "dropped": 1}));
    });
    let out = ctx
        .run(&[
            "pubsub",
            "publish",
            "orders.eu.created",
            "--body",
            r#"{"id":1}"#,
            "--header",
            "trace=t1",
            "--project",
            "shop",
            "--env",
            "staging",
        ])
        .ok();
    m.assert();
    assert_eq!(out.lines(), vec!["receivers 3, dropped 1"]);
    let out = ctx
        .run(&[
            "pubsub",
            "publish",
            "orders.eu.created",
            "--body",
            r#"{"id":1}"#,
            "--header",
            "trace=t1",
            "--project",
            "shop",
            "--env",
            "staging",
            "--json",
        ])
        .ok();
    assert_eq!(out.json(), json!({"receivers": 3, "dropped": 1}));

    let ctx = Ctx::new();
    ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/pubsub/publish");
        then.status(429)
            .header("retry-after", "1")
            .json_body(err_json("rate_limited", "slow down"));
    });
    let out = ctx.run(&["pubsub", "publish", "a.b", "--body", "1"]).fails_with(4);
    assert!(out.stderr.contains("rate_limited"), "{}", out.stderr);
}

// ---- invocations -------------------------------------------------------------------------------

#[test]
fn invocation_list_filters_by_state() {
    let ctx = Ctx::new();
    ctx.lookup("hook", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/invocations")
            .query_param("state", "failed")
            .query_param("limit", "10");
        then.status(200).json_body(json!({
            "items": [invocation("inv_1", "failed")], "next_cursor": "c2", "counts": {"pending": 4, "failed": 1}
        }));
    });
    let out = ctx
        .run(&["invocation", "list", "hook", "--state", "failed", "--limit", "10"])
        .ok();
    m.assert();
    let lines = out.lines();
    assert_eq!(lines[0], "pending 4, failed 1");
    assert_eq!(lines[1], "ID STATE REQUEST ATTEMPTS STATUS ERROR CREATED");
    assert!(
        lines[2].starts_with("inv_1 failed POST /hook?… 3 500 status_5xx "),
        "{lines:?}"
    );
    assert!(out.stderr.contains("--cursor c2"), "{}", out.stderr);

    ctx.run(&["invocation", "list", "hook", "--state", "done"])
        .fails_with(2);
}

/// `--all` follows the cursor and keeps the counts of the last page.
#[test]
fn invocation_list_all_keeps_the_counts() {
    let ctx = Ctx::new();
    ctx.lookup("hook", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/invocations")
            .query_param_missing("cursor");
        then.status(200).json_body(json!({
            "items": [invocation("inv_1", "pending")], "next_cursor": "c2", "counts": {"pending": 2, "failed": 0}
        }));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/invocations")
            .query_param("cursor", "c2");
        then.status(200).json_body(json!({
            "items": [invocation("inv_2", "pending")], "next_cursor": null, "counts": {"pending": 2, "failed": 0}
        }));
    });
    let out = ctx.run(&["invocation", "list", "hook", "--all", "--json"]).ok();
    let v = out.json();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["counts"], json!({"pending": 2, "failed": 0}));
    assert_eq!(v["next_cursor"], Value::Null);
}

#[test]
fn invocation_get_redrive_and_purge() {
    let ctx = Ctx::new();
    ctx.lookup("hook", "svc_1");
    let mut plain = invocation("inv_1", "succeeded");
    plain["has_query"] = json!(false);
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/invocations/inv_1");
        then.status(200).json_body(plain);
    });
    let lines = ctx.run(&["invocation", "get", "hook", "inv_1"]).ok().lines();
    for want in [
        "ID: inv_1",
        "State: succeeded",
        "Request: POST /hook",
        "Condition: retries_exhausted",
    ] {
        assert!(lines.contains(&want.to_string()), "missing {want:?} in {lines:?}");
    }
    let out = ctx.run(&["invocation", "get", "hook", "inv_1", "--json"]).ok();
    assert_eq!(out.json()["id"], json!("inv_1"));

    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/invocations/inv_9");
        then.status(404).json_body(err_json("not_found", "no such invocation"));
    });
    ctx.run(&["invocation", "get", "hook", "inv_9"]).fails_with(1);

    let redrive = ctx.server.mock(|when, then| {
        when.method(POST).path("/v1/services/svc_1/invocations/redrive");
        then.status(200).json_body(json!({"redriven": 3}));
    });
    let out = ctx.run(&["invocation", "redrive", "hook"]).ok();
    assert_eq!(out.lines(), vec!["Redrove 3 failed invocations of hook (svc_1)."]);
    let out = ctx.run(&["invocation", "redrive", "hook", "--json"]).ok();
    assert_eq!(out.json(), json!({"redriven": 3}));
    assert_eq!(redrive.calls(), 2);

    let purge = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/invocations")
            .query_param("state", "failed");
        then.status(204);
    });
    let out = ctx.run(&["invocation", "purge", "hook"]).ok();
    assert!(
        out.stdout.contains("Purged the failed invocations of hook (svc_1)."),
        "{}",
        out.stdout
    );
    let out = ctx.run(&["invocation", "purge", "hook", "--json"]).ok();
    assert_eq!(out.json(), json!({"purged": true}));
    assert_eq!(purge.calls(), 2);
}
