//! `21ci sockets …` and `21ci objects …`: a service's objects, their open WebSockets,
//! closing one or several, dropping an object's storage.
use crate::common;

use common::*;
use httpmock::prelude::*;
use serde_json::json;

fn socket(id: &str, tags: &[&str]) -> serde_json::Value {
    json!({"id": id, "object": "room", "tags": tags, "protocol": "chat", "created_at": T0_MS,
           "last_message_at": null, "auto_response_at": null, "buffered_amount": 0, "remote_ip": "203.0.113.4"})
}

#[test]
fn sockets_ls_lists_an_objects_sockets_with_a_tag() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/objects/room/sockets")
            .query_param("tag", "admin");
        then.status(200)
            .json_body(json!({"items": [socket("ws_A", &["admin", "u:1"])], "next_cursor": null}));
    });
    let out = ctx
        .run(&["sockets", "ls", "hello", "--object", "room", "--tag", "admin"])
        .ok();
    m.assert();
    let lines = out.lines();
    assert!(lines[0].starts_with("ID") && lines[0].contains("TAGS"), "{lines:?}");
    assert!(
        lines[1].starts_with("ws_A") && lines[1].contains("admin,u:1") && lines[1].contains("chat"),
        "{lines:?}"
    );
}

#[test]
fn sockets_ls_defaults_to_the_default_object() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/objects/default/sockets");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    ctx.run(&["sockets", "ls", "hello"]).ok();
    m.assert();
}

#[test]
fn sockets_close_closes_one_socket_with_a_code() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/objects/room/sockets/ws_A")
            .query_param("code", "4001")
            .query_param("reason", "kicked");
        then.status(204);
    });
    let out = ctx
        .run(&[
            "sockets", "close", "hello", "ws_A", "--object", "room", "--code", "4001", "--reason", "kicked",
        ])
        .ok();
    m.assert();
    assert!(out.stdout.contains("Closed socket ws_A"), "{}", out.stdout);
}

#[test]
fn sockets_close_by_tag_closes_several() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let m = ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/objects/default/sockets")
            .query_param("tag", "guest");
        then.status(200).json_body(json!({"closed": 3}));
    });
    let out = ctx.run(&["sockets", "close", "hello", "--tag", "guest"]).ok();
    m.assert();
    assert!(out.stdout.contains("Closed 3 sockets"), "{}", out.stdout);
    ctx.run(&["sockets", "close", "hello"]).fails_with(2);
}

#[test]
fn objects_ls_and_rm() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    let ls = ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/objects");
        then.status(200)
            .json_body(json!({"items": [{"name": "room-1", "sockets": 2, "storage": true}], "next_cursor": null}));
    });
    let out = ctx.run(&["objects", "ls", "hello"]).ok();
    ls.assert();
    let lines = out.lines();
    assert!(
        lines[0].starts_with("NAME") && lines[0].contains("SOCKETS") && lines[0].contains("STORAGE"),
        "{lines:?}"
    );
    assert!(
        lines[1].starts_with("room-1") && lines[1].contains('2') && lines[1].contains("yes"),
        "{lines:?}"
    );
    let rm = ctx.server.mock(|when, then| {
        when.method(DELETE).path("/v1/services/svc_1/objects/room-1");
        then.status(204);
    });
    let out = ctx.run(&["objects", "rm", "hello", "room-1"]).ok();
    rm.assert();
    assert!(
        out.stdout.contains("Dropped the storage of object room-1"),
        "{}",
        out.stdout
    );
}

#[test]
fn sockets_ls_follows_every_page_and_prints_json() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/objects/room/sockets")
            .query_param_missing("cursor");
        then.status(200)
            .json_body(json!({"items": [socket("ws_A", &[])], "next_cursor": "c2"}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/objects/room/sockets")
            .query_param("cursor", "c2");
        then.status(200)
            .json_body(json!({"items": [socket("ws_B", &["x"])], "next_cursor": null}));
    });
    let out = ctx.run(&["--json", "sockets", "ls", "hello", "--object", "room"]).ok();
    let v = out.json();
    assert_eq!(v.as_array().unwrap().len(), 2, "both pages");
    assert_eq!(v[1]["id"], "ws_B");
}

#[test]
fn sockets_ls_shows_a_dash_for_untagged_sockets() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET).path("/v1/services/svc_1/objects/default/sockets");
        then.status(200)
            .json_body(json!({"items": [socket("ws_A", &[])], "next_cursor": null}));
    });
    let out = ctx.run(&["sockets", "ls", "hello"]).ok();
    assert!(out.lines()[1].starts_with("ws_A - chat"), "{:?}", out.lines());
}

#[test]
fn sockets_close_by_tag_with_json_prints_the_reply() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(DELETE)
            .path("/v1/services/svc_1/objects/default/sockets")
            .query_param("tag", "guest");
        then.status(200).json_body(json!({"closed": 2}));
    });
    let out = ctx.run(&["--json", "sockets", "close", "hello", "--tag", "guest"]).ok();
    assert_eq!(out.json()["closed"], 2);
}

#[test]
fn objects_ls_filters_by_prefix_prints_json_and_says_when_empty() {
    let ctx = Ctx::new();
    ctx.lookup("hello", "svc_1");
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/objects")
            .query_param("prefix", "room");
        then.status(200)
            .json_body(json!({"items": [{"name": "room-1", "sockets": 0, "storage": false}], "next_cursor": null}));
    });
    ctx.server.mock(|when, then| {
        when.method(GET)
            .path("/v1/services/svc_1/objects")
            .query_param("prefix", "none");
        then.status(200).json_body(json!({"items": [], "next_cursor": null}));
    });
    let out = ctx.run(&["--json", "objects", "ls", "hello", "--prefix", "room"]).ok();
    assert_eq!(out.json()[0]["name"], "room-1");
    let out = ctx.run(&["objects", "ls", "hello", "--prefix", "none"]).ok();
    assert!(
        out.stderr.contains("No objects with sockets or storage"),
        "{}",
        out.stderr
    );
}
