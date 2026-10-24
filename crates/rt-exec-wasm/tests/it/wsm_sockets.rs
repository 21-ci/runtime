//! WSM-WS: a component's `std:host/socket-handler` export runs socket events and alarms,
//! its `std:host/durable` writes commit with a handler that succeeded and vanish with one that
//! failed, and a component without the export takes no events (fixture `ws`).

use crate::common;

use common::*;
use rt_types::{SocketEventKind, SocketInfo, WsData};

fn socket(id: &str) -> SocketInfo {
    SocketInfo {
        id: id.to_string(),
        object: "o".to_string(),
        tags: vec![],
        protocol: None,
        created_at_ms: 0,
        last_message_at_ms: None,
        auto_response_at_ms: None,
        buffered_amount: 0,
        remote_ip: None,
    }
}

#[tokio::test]
async fn ac_wsm_ws_an_alarm_runs_the_export_and_commits_its_writes() {
    let mut rig = Rig::new();
    let p = prepared("ws");
    assert_eq!(
        rig.run(&p, Req::get("/state").object("o")).await.text(),
        "count=0 fired=0 x=-"
    );
    let alarm = Req::event(
        "o",
        SocketEventKind::Alarm {
            scheduled_ms: 1,
            retry: 0,
        },
        None,
    );
    assert_eq!(rig.run(&p, alarm).await.reply().status, 200);
    assert_eq!(
        rig.run(&p, Req::get("/state").object("o")).await.text(),
        "count=0 fired=1 x=-"
    );
    assert_eq!(
        rig.run(&p, Req::get("/state").object("other")).await.text(),
        "count=0 fired=0 x=-",
        "per object"
    );
    // A request to the object commits its writes too.
    assert_eq!(rig.run(&p, Req::get("/hit").object("o")).await.text(), "hits=1");
    assert_eq!(
        rig.run(&p, Req::get("/state").object("o")).await.text(),
        "count=1 fired=1 x=-"
    );
}

#[tokio::test]
async fn ac_wsm_ws_a_failing_event_writes_nothing() {
    let mut rig = Rig::new();
    let p = prepared("ws");
    let boom = Req::event(
        "o",
        SocketEventKind::Message(WsData::Text("boom".into())),
        Some(socket("ws_01ARZ3NDEKTSV4RRFFQ69G5FAV")),
    );
    rig.run(&p, boom).await.expect_guest_error();
    assert_eq!(
        rig.run(&p, Req::get("/state").object("o")).await.text(),
        "count=0 fired=0 x=-"
    );
    // Storage belongs to an object: a plain request has none.
    assert_eq!(rig.run(&p, Req::get("/hit")).await.text(), "error=no_object");
}

#[tokio::test]
async fn ac_wsm_ws_a_component_without_the_export_takes_no_events() {
    let mut rig = Rig::new();
    let p = prepared("hello");
    let alarm = Req::event(
        "o",
        SocketEventKind::Alarm {
            scheduled_ms: 1,
            retry: 0,
        },
        None,
    );
    assert_eq!(
        rig.run(&p, alarm).await.reply().status,
        200,
        "answered without running guest code"
    );
    assert!(rig
        .log_records()
        .iter()
        .any(|r| r.msg.contains("does not export socket-handler")));
}

async fn ws_echo_server() -> u16 {
    use futures_util::{SinkExt, StreamExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(s).await else {
                    return;
                };
                while let Some(Ok(m)) = ws.next().await {
                    if m.is_text() {
                        let _ = ws.send(m).await;
                    }
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn ac_wsm_ws_client_connects_through_the_egress_guard() {
    let port = ws_echo_server().await;
    let mut rig = Rig::new().egress(local_ws_guard());
    let p = prepared("ws");
    let out = rig
        .run(&p, Req::get(&format!("/out?url=ws://echo.test:{port}/")))
        .await
        .text();
    assert_eq!(out, "got=ping");
    let refused = rig.run(&p, Req::get("/out?url=ws://127.0.0.1:9/")).await.text();
    assert_eq!(refused, "error=egress_denied");
}

#[tokio::test]
async fn ac_wsm_ws_an_accepted_upgrade_answers_101_with_the_hubs_ticket() {
    let hub = std::sync::Arc::new(RecordingHub::default());
    let mut rig = Rig::new().sockets(hub.clone());
    let p = prepared("ws");
    let req = Req::get("/?user=ann").header("upgrade", "websocket").object("room");
    let reply = rig.run(&p, req).await.reply();
    assert_eq!(reply.status, 101);
    assert_eq!(
        reply.headers.get(rt_types::TICKET_HEADER).and_then(|v| v.to_str().ok()),
        Some("ticket-1")
    );
    assert_eq!(hub.seen(), [r#"accept room ["ann"] Some(b"user:ann") None"#]);
}

#[tokio::test]
async fn ac_wsm_ws_a_binary_message_reaches_the_export_and_its_echo_leaves_at_commit() {
    let hub = std::sync::Arc::new(RecordingHub::default());
    let mut rig = Rig::new().sockets(hub.clone());
    let p = prepared("ws");
    let sock = socket("ws_1");
    let msg = Req::event(
        "room",
        SocketEventKind::Message(WsData::Binary(vec![1, 2, 3].into())),
        Some(sock),
    );
    assert_eq!(rig.run(&p, msg).await.reply().status, 200);
    let seen = hub.seen();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0].contains("Send") && seen[0].contains("Binary") && seen[0].contains("ws_1"),
        "{seen:?}"
    );
}

#[tokio::test]
async fn ac_wsm_ws_a_close_event_runs_on_close_and_an_error_event_runs_on_error() {
    let mut rig = Rig::new();
    let p = prepared("ws");
    let sock = || Some(socket("ws_01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert_eq!(rig.run(&p, Req::get("/closed").object("room")).await.text(), "-");
    let close = SocketEventKind::Close {
        code: 1001,
        reason: "going away".into(),
        was_clean: true,
    };
    assert_eq!(rig.run(&p, Req::event("room", close, sock())).await.reply().status, 200);
    assert_eq!(
        rig.run(&p, Req::get("/closed").object("room")).await.text(),
        "1001",
        "on_close stored the code"
    );
    let err = SocketEventKind::Error {
        code: "boom".into(),
        message: "it broke".into(),
    };
    assert_eq!(rig.run(&p, Req::event("room", err, sock())).await.reply().status, 200);
}

#[tokio::test]
async fn ac_wsm_ws_a_socket_event_without_a_socket_fails_the_task() {
    let mut rig = Rig::new();
    let p = prepared("ws");
    let close = SocketEventKind::Close {
        code: 1000,
        reason: String::new(),
        was_clean: true,
    };
    rig.run(&p, Req::event("room", close, None)).await.expect_guest_error();
}
