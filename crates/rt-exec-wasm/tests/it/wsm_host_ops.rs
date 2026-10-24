//! The `std:host` imports of a component, called the way a guest calls them: through the generated
//! host traits on a `WasmState` bound to a live task handle. Covers the conversions between the
//! WIT records and the host facades (kv, sql, blob, schedule, sdk, sockets, durable, ws-client)
//! and the stable error codes that cross the boundary.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use futures_util::{SinkExt, StreamExt};
use rt_exec_wasm::host_wit::wit::std::host::{
    blob, call, durable, kv, platform, schedule, sdk, sockets, sql, ws_client,
};
use rt_exec_wasm::{FrozenClock, GovernorLimiter, GuardedHttpHooks, GuestLogs, WasmState};
use rt_host::TaskCtx;
use rt_mem::{MemCategory, MemoryGovernor as _, Owner};
use rt_testkit::SpecBuilder;
use rt_types::{HttpRequest, Payload, TaskId, TaskSpec};
use wasmtime::component::ResourceTable;
use wasmtime_wasi_http::WasiHttpCtx;

/// A `WasmState` over a live handle of a task of tenant 1 / service 1 (acting for `object`).
async fn live(rig: &Rig, object: Option<&str>, tweak: impl FnOnce(&mut TaskSpec)) -> WasmState {
    let mut spec = SpecBuilder::wasm().tenant(1).service(1).finish();
    if let Some(o) = object {
        spec.object = Some(rt_types::ObjectRef::named(o));
    }
    tweak(&mut spec);
    let services = rig.exec.host();
    let cfg = services.config_for(&spec).await;
    let host = services.handle_for(&spec, TaskId(900), Instant::now() + Duration::from_secs(30), cfg);
    let reservation = rig
        .governor
        .reserve(MemCategory::WasmReuse, MIB, Owner::NONE)
        .expect("reserve");
    WasmState {
        wasi_ctx: wasmtime_wasi::WasiCtxBuilder::new().build(),
        http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        limiter: GovernorLimiter::new(reservation, usize::MAX),
        http_hooks: GuardedHttpHooks { host: host.clone() },
        host,
        clock: FrozenClock::new(),
        logs: GuestLogs::new(),
    }
}

fn opts() -> kv::SetOptions {
    kv::SetOptions { ttl_seconds: None }
}

// ---- kv ----------------------------------------------------------------------------------------

#[tokio::test]
async fn kv_list_pages_by_prefix_in_either_order_and_keeps_json_and_byte_values_apart() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    kv::Host::set(&mut st, b"p/a".to_vec(), kv::Value::Json(r#"{"n":1}"#.into()), opts())
        .await
        .expect("set");
    kv::Host::set(&mut st, b"p/b".to_vec(), kv::Value::Bytes(vec![1, 2]), opts())
        .await
        .expect("set");
    kv::Host::set(&mut st, b"p/c".to_vec(), kv::Value::Bytes(vec![3]), opts())
        .await
        .expect("set");
    kv::Host::set(&mut st, b"q/x".to_vec(), kv::Value::Bytes(vec![9]), opts())
        .await
        .expect("set");

    let page = |cursor: Option<Vec<u8>>, limit, reverse| kv::ListOptions {
        prefix: b"p/".to_vec(),
        cursor,
        limit,
        reverse,
    };
    let first = kv::Host::list(&mut st, page(None, 2, false)).await.expect("first page");
    let keys: Vec<_> = first
        .entries
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert_eq!(keys, ["p/a", "p/b"]);
    assert!(
        matches!(&first.entries[0].1, kv::Value::Json(j) if j == r#"{"n":1}"#),
        "a JSON value stays JSON"
    );
    assert!(
        matches!(&first.entries[1].1, kv::Value::Bytes(b) if b == &[1, 2]),
        "a byte value stays bytes"
    );
    let cursor = first.next_cursor.expect("a full page has a cursor");

    let second = kv::Host::list(&mut st, page(Some(cursor), 2, false))
        .await
        .expect("second page");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].0, b"p/c");
    assert!(second.next_cursor.is_none(), "the last page has none");

    let backwards = kv::Host::list(&mut st, page(None, 10, true)).await.expect("reverse");
    let keys: Vec<_> = backwards.entries.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, [b"p/c".to_vec(), b"p/b".to_vec(), b"p/a".to_vec()]);
}

// ---- sql ---------------------------------------------------------------------------------------

fn stmt(text: &str, args: Vec<sql::SqlValue>) -> sql::Statement {
    sql::Statement { sql: text.into(), args }
}

#[tokio::test]
async fn sql_round_trips_every_value_type_and_reports_rows_affected_and_the_last_rowid() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    sql::Host::execute(
        &mut st,
        stmt(
            "create table t (id integer primary key, n integer, r real, s text, b blob)",
            vec![],
        ),
    )
    .await
    .expect("create");
    let insert = stmt(
        "insert into t (n, r, s, b) values (?, ?, ?, ?), (null, null, null, null)",
        vec![
            sql::SqlValue::Integer(7),
            sql::SqlValue::Real(2.5),
            sql::SqlValue::Text("hi".into()),
            sql::SqlValue::Blob(vec![0, 255]),
        ],
    );
    let ins = sql::Host::execute(&mut st, insert).await.expect("insert");
    assert_eq!((ins.rows_affected, ins.last_insert_rowid), (2, Some(2)));

    let null_arg = sql::Host::execute(&mut st, stmt("insert into t (n) values (?)", vec![sql::SqlValue::Null]))
        .await
        .expect("null");
    assert_eq!(null_arg.rows_affected, 1);
    sql::Host::execute(&mut st, stmt("delete from t where id = 3", vec![]))
        .await
        .expect("delete");

    let sel = sql::Host::execute(&mut st, stmt("select n, r, s, b from t order by id", vec![]))
        .await
        .expect("select");
    assert_eq!(sel.columns, ["n", "r", "s", "b"]);
    assert!(matches!(
        sel.rows[0].as_slice(),
        [sql::SqlValue::Integer(7), sql::SqlValue::Real(r), sql::SqlValue::Text(s), sql::SqlValue::Blob(b)]
            if *r == 2.5 && s == "hi" && b == &[0, 255]
    ));
    assert!(sel.rows[1].iter().all(|v| matches!(v, sql::SqlValue::Null)));

    let bad = sql::Host::execute(&mut st, stmt("select * from missing_table", vec![]))
        .await
        .expect_err("no such table");
    assert_eq!(bad.code, "sql_error");
}

#[tokio::test]
async fn a_sql_batch_runs_in_one_transaction_and_a_read_only_batch_refuses_writes() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    let write = vec![
        stmt("create table b (n integer)", vec![]),
        stmt("insert into b values (?)", vec![sql::SqlValue::Integer(1)]),
        stmt("select count(*) from b", vec![]),
    ];
    let results = sql::Host::batch(&mut st, write, true).await.expect("write batch");
    assert_eq!(results.len(), 3);
    assert!(matches!(results[2].rows[0].as_slice(), [sql::SqlValue::Integer(1)]));

    let read = sql::Host::batch(&mut st, vec![stmt("select n from b", vec![])], false)
        .await
        .expect("read batch");
    assert!(matches!(read[0].rows[0].as_slice(), [sql::SqlValue::Integer(1)]));
    let refused = sql::Host::batch(&mut st, vec![stmt("insert into b values (2)", vec![])], false).await;
    assert!(refused.is_err(), "a read-only batch cannot write");
    let after = sql::Host::execute(&mut st, stmt("select count(*) from b", vec![]))
        .await
        .expect("count");
    assert!(matches!(after.rows[0].as_slice(), [sql::SqlValue::Integer(1)]));
}

// ---- blob --------------------------------------------------------------------------------------

#[tokio::test]
async fn blob_put_head_get_list_and_delete_agree_on_the_metadata() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    let put = blob::Host::put(&mut st, "a/one".into(), Some("text/plain".into()), b"hello".to_vec())
        .await
        .expect("put");
    assert_eq!(
        (put.key.as_str(), put.size, put.content_type.as_deref()),
        ("a/one", 5, Some("text/plain"))
    );
    blob::Host::put(&mut st, "a/two".into(), None, b"xy".to_vec())
        .await
        .expect("put without a type");

    let head = blob::Host::head(&mut st, "a/one".into())
        .await
        .expect("head")
        .expect("exists");
    assert_eq!(head.size, 5);
    let (meta, data) = blob::Host::get(&mut st, "a/one".into())
        .await
        .expect("get")
        .expect("exists");
    assert_eq!((meta.size, data.as_slice()), (5, b"hello".as_slice()));
    assert!(blob::Host::get(&mut st, "nope".into()).await.expect("get").is_none());
    let bad_key = blob::Host::get(&mut st, String::new()).await.expect_err("an empty key");
    assert_eq!(bad_key.code, "validation_failed");

    let page = blob::Host::list(&mut st, "a/".into(), None, 1).await.expect("list");
    assert_eq!(page.items.len(), 1);
    let cursor = page.next_cursor.expect("a full page has a cursor");
    let rest = blob::Host::list(&mut st, "a/".into(), Some(cursor), 10)
        .await
        .expect("next page");
    assert_eq!(rest.items.iter().map(|m| m.key.as_str()).collect::<Vec<_>>(), ["a/two"]);

    assert!(blob::Host::delete(&mut st, "a/one".into()).await.expect("delete"));
    assert!(!blob::Host::delete(&mut st, "a/one".into()).await.expect("delete again"));
}

#[tokio::test]
async fn a_blob_larger_than_the_buffered_response_limit_is_blob_too_large() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |spec| spec.limits.max_response_buffered_bytes = 4).await;
    blob::Host::put(&mut st, "big".into(), None, vec![7; 10])
        .await
        .expect("put");
    let err = blob::Host::get(&mut st, "big".into())
        .await
        .expect_err("too large to buffer");
    assert_eq!(err.code, "blob_too_large");
    assert_eq!(
        blob::Host::head(&mut st, "big".into())
            .await
            .expect("head")
            .expect("exists")
            .size,
        10,
        "head still answers"
    );
}

// ---- schedule, sdk, calls ----------------------------------------------------------------------

#[tokio::test]
async fn a_schedule_at_a_time_is_listed_with_its_due_time_and_pages_with_a_cursor() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64;
    let due = now + 60_000;
    let one = schedule::Host::at(&mut st, due, "{}".into(), Some("job-a".into()))
        .await
        .expect("at");
    assert_eq!((one.id.as_str(), one.run_at_ms), ("job-a", due));
    schedule::Host::at(&mut st, due + 1, "{}".into(), Some("job-b".into()))
        .await
        .expect("at");

    let page = schedule::Host::list(&mut st, None, 1).await.expect("list");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.cursor.as_deref(), Some("job-a"), "a full page names the last id");
    let rest = schedule::Host::list(&mut st, page.cursor, 5).await.expect("next");
    assert_eq!(
        rest.entries.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        ["job-b"]
    );
    assert!(rest.cursor.is_none());

    let too_far = schedule::Host::at(&mut st, now + 31 * 86_400_000, "{}".into(), None)
        .await
        .expect_err("past 30 days");
    assert_eq!(too_far.code, "validation_failed");
}

#[tokio::test]
async fn an_unknown_sdk_is_a_stable_error_inside_a_task_and_unavailable_outside_one() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    let err = sdk::Host::invoke(&mut st, "billing".into(), "charge".into(), "{}".into())
        .await
        .expect_err("no such sdk");
    assert_eq!(err.code, "sdk_not_found");

    let mut detached = detached(&rig);
    let err = sdk::Host::invoke(&mut detached, "billing".into(), "charge".into(), "{}".into())
        .await
        .expect_err("detached");
    assert_eq!(err.code, "unavailable");
    let err = call::Host::call_rpc(&mut detached, "svc".into(), "m".into(), "{}".into())
        .await
        .expect_err("detached");
    assert_eq!(err.code, "unavailable");
}

fn detached(rig: &Rig) -> WasmState {
    let host = rig.exec.host().detached_handle(rt_testkit::service_ref(1, 1));
    let reservation = rig
        .governor
        .reserve(MemCategory::WasmReuse, MIB, Owner::NONE)
        .expect("reserve");
    WasmState {
        wasi_ctx: wasmtime_wasi::WasiCtxBuilder::new().build(),
        http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        limiter: GovernorLimiter::new(reservation, usize::MAX),
        http_hooks: GuardedHttpHooks { host: host.clone() },
        host,
        clock: FrozenClock::new(),
        logs: GuestLogs::new(),
    }
}

// ---- sockets (a recording hub) -----------------------------------------------------------------

fn upgrade_offer(spec: &mut TaskSpec) {
    let mut req = HttpRequest::get("/");
    req.headers
        .insert(http::header::UPGRADE, "websocket".parse().expect("header"));
    spec.payload = Payload::Http(req);
}

#[tokio::test]
async fn accepting_an_upgrade_hands_the_hub_the_tags_attachment_and_protocol_and_returns_the_ticket() {
    let hub = Arc::new(RecordingHub::default());
    let rig = Rig::new().sockets(hub.clone());
    let mut st = live(&rig, Some("room"), upgrade_offer).await;
    let accept = sockets::AcceptOptions {
        tags: vec!["a".into()],
        attachment: Some(b"u1".to_vec()),
        protocol: Some("v1".into()),
    };
    let got = sockets::Host::accept(&mut st, accept).await.expect("accept");
    assert_eq!((got.id.as_str(), got.ticket.as_str()), ("ws_1", "ticket-1"));
    assert_eq!(hub.seen(), [r#"accept room ["a"] Some(b"u1") Some("v1")"#]);
    assert_eq!(st.host.accepted_ticket().as_deref(), Some("ticket-1"));
}

#[tokio::test]
async fn socket_queries_and_edits_reach_the_hub_and_answer_with_wit_records() {
    let hub = Arc::new(RecordingHub::default());
    let rig = Rig::new().sockets(hub.clone());
    let mut st = live(&rig, Some("room"), |_| {}).await;

    let (items, next) = sockets::Host::list(&mut st, None, Some("t".into()), 5, Some("ws_0".into()))
        .await
        .expect("list");
    assert_eq!(next.as_deref(), Some("ws_1"));
    let s = &items[0];
    assert_eq!(
        (
            s.id.as_str(),
            s.object.as_str(),
            s.tags.as_slice(),
            s.protocol.as_deref()
        ),
        ("ws_1", "room", ["t".to_string()].as_slice(), Some("v1"))
    );
    assert_eq!(
        (s.created_at_ms, s.last_message_at_ms, s.auto_response_at_ms),
        (10, Some(20), Some(30))
    );
    assert_eq!((s.buffered_amount, s.remote_ip.as_deref()), (4, Some("203.0.113.5")));

    let found = sockets::Host::get(&mut st, "ws_1".into()).await.expect("get");
    assert_eq!(found.map(|s| s.id), Some("ws_1".to_string()));
    assert!(sockets::Host::get(&mut st, "ws_9".into()).await.expect("get").is_none());

    sockets::Host::set_tags(&mut st, "ws_1".into(), vec!["x".into(), "y".into()])
        .await
        .expect("set_tags");
    assert_eq!(
        sockets::Host::get_attachment(&mut st, "ws_1".into())
            .await
            .expect("attachment"),
        Some(b"att".to_vec())
    );
    sockets::Host::set_attachment(&mut st, "ws_1".into(), Some(b"new".to_vec()))
        .await
        .expect("set attachment");
    sockets::Host::set_attachment(&mut st, "ws_1".into(), None)
        .await
        .expect("clear attachment");
    sockets::Host::set_auto_response(&mut st, None, Some(("ping".into(), "pong".into())))
        .await
        .expect("auto response");
    sockets::Host::set_auto_response(&mut st, Some("other".into()), None)
        .await
        .expect("clear auto response");

    assert_eq!(
        hub.seen(),
        [
            r#"list Some("room") Some("t") 5 Some("ws_0")"#,
            r#"set_tags ws_1 ["x", "y"]"#,
            r#"set_attachment ws_1 Some(b"new")"#,
            "set_attachment ws_1 None",
            r#"set_auto_response room Some(("ping", "pong"))"#,
            r#"set_auto_response other None"#,
        ],
        "the object defaults to the task's own"
    );
}

#[tokio::test]
async fn sends_closes_and_broadcasts_wait_behind_the_output_gate_of_an_object_task_and_leave_at_commit() {
    let hub = Arc::new(RecordingHub::default());
    let rig = Rig::new().sockets(hub.clone());
    let mut st = live(&rig, Some("room"), |_| {}).await;
    sockets::Host::send(&mut st, "ws_1".into(), sockets::Payload::Text("hi".into()))
        .await
        .expect("send");
    let n = sockets::Host::broadcast(
        &mut st,
        None,
        Some("t".into()),
        sockets::Payload::Binary(vec![1]),
        vec!["ws_2".into()],
    )
    .await
    .expect("broadcast");
    assert_eq!(n, 1, "the hub's two sockets minus the one excepted");
    sockets::Host::close(&mut st, "ws_1".into(), 1000, "bye".into())
        .await
        .expect("close");
    let all = sockets::Host::close_all(&mut st, Some("room".into()), None, 1001, "gone".into())
        .await
        .expect("close_all");
    assert_eq!(all, 2);
    assert!(
        hub.seen().is_empty(),
        "nothing reaches the hub before the handler's storage committed"
    );

    st.host.commit().await.expect("commit");
    let seen = hub.seen();
    assert_eq!(seen.len(), 4);
    assert!(
        seen[0].contains("Send") && seen[0].contains(r#"Text("hi")"#),
        "{seen:?}"
    );
    assert!(
        seen[1].contains("Broadcast")
            && seen[1].contains(r#"object: "room""#)
            && seen[1].contains(r#"except: ["ws_2"]"#),
        "{seen:?}"
    );
    assert!(seen[2].contains("Close") && seen[2].contains("1000"), "{seen:?}");
    assert!(seen[3].contains("CloseAll") && seen[3].contains("1001"), "{seen:?}");
}

#[tokio::test]
async fn a_task_without_an_object_sends_at_once_and_a_send_to_a_missing_socket_is_not_found() {
    let hub = Arc::new(RecordingHub::default());
    let rig = Rig::new().sockets(hub.clone());
    let mut st = live(&rig, None, |_| {}).await;
    sockets::Host::send(&mut st, "ws_1".into(), sockets::Payload::Text("now".into()))
        .await
        .expect("send");
    assert_eq!(hub.seen().len(), 1, "no gate without an object");

    let rig = Rig::new(); // the default hub holds no sockets
    let mut st = live(&rig, Some("room"), |_| {}).await;
    let err = sockets::Host::send(&mut st, "ws_1".into(), sockets::Payload::Text("x".into()))
        .await
        .expect_err("gone");
    assert_eq!(err.code, "not_found");
    let err = sockets::Host::close(&mut st, "ws_1".into(), 1000, String::new())
        .await
        .expect_err("gone");
    assert_eq!(err.code, "not_found");
}

// ---- durable -----------------------------------------------------------------------------------

#[tokio::test]
async fn durable_storage_is_listed_in_order_with_the_tasks_own_writes_and_survives_only_a_commit() {
    let rig = Rig::new();
    let mut st = live(&rig, Some("room"), |_| {}).await;
    assert_eq!(durable::Host::object(&mut st).await.as_deref(), Some("room"));
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        durable::Host::put(&mut st, k.into(), v.as_bytes().to_vec())
            .await
            .expect("put");
    }
    st.host.commit().await.expect("commit");

    let mut next = live(&rig, Some("room"), |_| {}).await;
    let all = durable::Host::list(&mut next, String::new(), None, None, 10, false)
        .await
        .expect("list");
    assert_eq!(all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), ["a", "b", "c"]);
    let ranged = durable::Host::list(&mut next, String::new(), Some("b".into()), Some("c".into()), 10, true)
        .await
        .expect("range");
    assert_eq!(
        ranged,
        [("b".to_string(), b"2".to_vec())],
        "start inclusive, end exclusive"
    );
    assert!(durable::Host::delete(&mut next, "a".into()).await.expect("delete"));
    assert!(!durable::Host::delete(&mut next, "zzz".into())
        .await
        .expect("delete missing"));
    let seen = durable::Host::list(&mut next, String::new(), None, None, 10, false)
        .await
        .expect("list");
    assert_eq!(seen.len(), 2, "the task sees its own delete before the commit");

    // Another task of the same object that never committed sees the stored state.
    let mut other = live(&rig, Some("room"), |_| {}).await;
    assert_eq!(
        durable::Host::get(&mut other, "a".into()).await.expect("get"),
        Some(b"1".to_vec())
    );

    durable::Host::delete_all(&mut next).await.expect("delete_all");
    assert!(
        durable::Host::get(&mut next, "b".into()).await.expect("get").is_none(),
        "gone for the task at once"
    );
    next.host.commit().await.expect("commit");
    assert!(
        durable::Host::get(&mut other, "b".into()).await.expect("get").is_none(),
        "gone for everyone after the commit"
    );
}

#[tokio::test]
async fn the_alarm_is_set_read_and_cleared_through_the_task() {
    let rig = Rig::new();
    let mut st = live(&rig, Some("room"), |_| {}).await;
    assert_eq!(durable::Host::get_alarm(&mut st).await.expect("alarm"), None);
    durable::Host::set_alarm(&mut st, Some(5_000)).await.expect("set");
    assert_eq!(
        durable::Host::get_alarm(&mut st).await.expect("alarm"),
        Some(5_000),
        "the task reads its own write"
    );
    st.host.commit().await.expect("commit");
    let mut next = live(&rig, Some("room"), |_| {}).await;
    assert_eq!(durable::Host::get_alarm(&mut next).await.expect("alarm"), Some(5_000));
    durable::Host::set_alarm(&mut next, None).await.expect("clear");
    assert_eq!(durable::Host::get_alarm(&mut next).await.expect("alarm"), None);
}

#[tokio::test]
async fn durable_storage_without_an_object_is_no_object() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    assert_eq!(durable::Host::object(&mut st).await, None);
    assert_eq!(
        durable::Host::get(&mut st, "k".into()).await.expect_err("get").code,
        "no_object"
    );
    assert_eq!(
        durable::Host::delete(&mut st, "k".into())
            .await
            .expect_err("delete")
            .code,
        "no_object"
    );
    let list = durable::Host::list(&mut st, String::new(), None, None, 1, false).await;
    assert_eq!(list.expect_err("list").code, "no_object");
    assert_eq!(
        durable::Host::delete_all(&mut st).await.expect_err("delete_all").code,
        "no_object"
    );
    assert_eq!(
        durable::Host::get_alarm(&mut st).await.expect_err("get_alarm").code,
        "no_object"
    );
    assert_eq!(
        durable::Host::set_alarm(&mut st, Some(1))
            .await
            .expect_err("set_alarm")
            .code,
        "no_object"
    );
}

fn request(path: &str) -> call::Request {
    call::Request {
        method: "POST".into(),
        path_with_query: path.into(),
        headers: vec![("x-a".into(), b"1".to_vec())],
        body: b"b".to_vec(),
    }
}

#[tokio::test]
async fn a_fetch_to_a_durable_object_answers_its_response_and_maps_failures_to_stable_codes() {
    let rig = Rig::new();
    let mut st = live(&rig, None, |_| {}).await;
    let resp = durable::Host::fetch(&mut st, Some("chat".into()), "room-1".into(), request("/join"))
        .await
        .expect("fetch");
    assert_eq!(resp.status, 202);
    assert_eq!(resp.body, b"room-1 says hi");
    assert!(resp.headers.iter().any(|(k, v)| k == "x-object" && v == b"room-1"));
    assert_eq!(
        rig.calls.object_calls.lock().clone(),
        [(Some("chat".to_string()), "room-1".to_string(), "/join".to_string())]
    );

    let deep = durable::Host::fetch(&mut st, None, "deep".into(), request("/"))
        .await
        .expect("depth is a response");
    assert_eq!(deep.status, 508);
    assert!(
        deep.headers
            .iter()
            .any(|(k, v)| k == "x-rt-reason" && v == b"call_depth"),
        "{:?}",
        deep.headers
    );

    let missing = durable::Host::fetch(&mut st, None, "missing".into(), request("/"))
        .await
        .expect_err("no such service");
    assert_eq!(missing.code, "not_found");
    let big = durable::Host::fetch(&mut st, None, "big".into(), request("/"))
        .await
        .expect_err("too large");
    assert_eq!(big.code, "response_too_large");
    let bad = call::Request {
        method: "NOT A METHOD".into(),
        ..request("/")
    };
    let bad = durable::Host::fetch(&mut st, None, "room".into(), bad)
        .await
        .expect_err("invalid method");
    assert_eq!(bad.code, "invalid_request");

    let mut detached = detached(&rig);
    let outside = durable::Host::fetch(&mut detached, None, "room".into(), request("/"))
        .await
        .expect_err("detached");
    assert_eq!(outside.code, "unavailable");
}

// ---- ws-client ---------------------------------------------------------------------------------

/// A WebSocket server that picks the first offered subprotocol, answers `text` with text, `bin`
/// with a binary frame and `bye` with a close frame (1001, "going away"), and ignores the rest.
#[allow(clippy::result_large_err)] // the handshake callback must return tungstenite's own error response type
async fn scripted_server() -> u16 {
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let pick = |req: &Request, mut resp: Response| {
                    if let Some(p) = req.headers().get("sec-websocket-protocol") {
                        let first = p
                            .to_str()
                            .unwrap_or_default()
                            .split(',')
                            .next()
                            .unwrap_or_default()
                            .trim()
                            .to_string();
                        resp.headers_mut()
                            .insert("sec-websocket-protocol", first.parse().expect("header"));
                    }
                    Ok(resp)
                };
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(s, pick).await else {
                    return;
                };
                while let Some(Ok(m)) = ws.next().await {
                    let Message::Text(t) = m else { continue };
                    let reply = match t.as_str() {
                        "text" => Message::Text("pong".into()),
                        "bin" => Message::Binary(vec![1, 2, 3].into()),
                        "bye" => Message::Close(Some(CloseFrame {
                            code: CloseCode::Away,
                            reason: "going away".into(),
                        })),
                        _ => continue,
                    };
                    let _ = ws.send(reply).await;
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn an_outbound_websocket_delivers_text_binary_and_the_close_frame_and_times_out_quietly() {
    let port = scripted_server().await;
    let rig = Rig::new().egress(local_ws_guard());
    let mut st = live(&rig, None, |_| {}).await;
    let url = format!("ws://echo.test:{port}/");
    let conn = ws_client::Host::connect(&mut st, url, vec!["chat.v1".into(), "chat.v2".into()])
        .await
        .expect("connect");
    let rep = conn.rep();
    let borrow = move || wasmtime::component::Resource::<rt_exec_wasm::host_ws::WsConnection>::new_borrow(rep);
    assert_eq!(
        ws_client::HostConnection::protocol(&mut st, borrow()).await.as_deref(),
        Some("chat.v1")
    );

    // Nothing was sent yet: a short wait ends with no event, not an error.
    assert!(ws_client::HostConnection::recv(&mut st, borrow(), 30)
        .await
        .expect("recv")
        .is_none());

    ws_client::HostConnection::send(&mut st, borrow(), sockets::Payload::Text("text".into()))
        .await
        .expect("send");
    let got = ws_client::HostConnection::recv(&mut st, borrow(), 5_000)
        .await
        .expect("recv");
    assert!(matches!(got, Some(ws_client::Event::Message(sockets::Payload::Text(t))) if t == "pong"));

    ws_client::HostConnection::send(&mut st, borrow(), sockets::Payload::Text("bin".into()))
        .await
        .expect("send");
    let got = ws_client::HostConnection::recv(&mut st, borrow(), 5_000)
        .await
        .expect("recv");
    assert!(matches!(got, Some(ws_client::Event::Message(sockets::Payload::Binary(b))) if b == [1, 2, 3]));

    ws_client::HostConnection::send(&mut st, borrow(), sockets::Payload::Text("bye".into()))
        .await
        .expect("send");
    let got = ws_client::HostConnection::recv(&mut st, borrow(), 5_000)
        .await
        .expect("recv");
    assert!(
        matches!(got, Some(ws_client::Event::Closed(f)) if f.code == 1001 && f.reason == "going away"),
        "the server's close frame"
    );

    ws_client::HostConnection::close(&mut st, borrow(), 1000, "done".into())
        .await
        .expect("close");
    ws_client::HostConnection::drop(&mut st, conn).await.expect("drop");
    // The resource is gone: every call on it is an invalid_state error, and the protocol is unknown.
    let err = ws_client::HostConnection::send(&mut st, borrow(), sockets::Payload::Text("late".into()))
        .await
        .expect_err("dropped");
    assert_eq!(err.code, "invalid_state");
    assert!(ws_client::HostConnection::protocol(&mut st, borrow()).await.is_none());
}

// ---- platform ----------------------------------------------------------------------------------

/// An operator binding whose upstream answers more than a guest may buffer.
struct HugeBinding;

#[async_trait::async_trait]
impl rt_host::PlatformBindings for HugeBinding {
    async fn fetch(
        &self,
        _from: &TaskCtx,
        _binding: &str,
        _req: rt_host::Request,
    ) -> Result<rt_host::Response, rt_host::BindingError> {
        let huge = vec![b'x'; 64];
        Ok(http::Response::builder()
            .status(200)
            .body(rt_types::Body::from(huge))
            .expect("response"))
    }
}

#[tokio::test]
async fn a_binding_response_larger_than_the_buffered_limit_is_response_too_large() {
    let rig = Rig::new().platform(Arc::new(HugeBinding));
    let mut st = live(&rig, None, |spec| spec.limits.max_response_buffered_bytes = 16).await;
    let err = platform::Host::fetch(&mut st, "acme".into(), request("/"))
        .await
        .expect_err("too large to buffer");
    assert_eq!(err.code, "response_too_large");
}
