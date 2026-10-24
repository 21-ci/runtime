//! `ws` fixture: a WebSocket echo and a durable object in WASM (`/wit/rt-host.wit`, world
//! `socket-service`).
//!
//! HTTP: an upgrade is accepted (tag and attachment `user:<?user=>`) and answered 101; `/hit` counts
//! in the object's storage; `/alarm?ms=N` sets the alarm N ms ahead; `/state` answers
//! `count=<hits> fired=<alarms> x=<x or ->`; `/accept` (no upgrade) answers the accept error code;
//! `/out?url=<ws url>` connects out (`ws-client`), sends `ping` and answers `got=<reply>` or `error=<code>`.
//! Events (`socket-handler`): `who` answers the object, `att` the attachment, `boom` writes `x`, sends
//! `leaked` and fails; anything else is echoed. A close stores `closed=<code>`; an alarm counts in
//! `fired` and broadcasts `alarm:<retry>`. `/closed` answers the close code a close event stored, `-` if none.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "socket-service",
});

use self::exports::std::host::socket_handler;
use self::std::host::{durable, sockets, ws_client};
use wasip2::exports::http::incoming_handler::Guest as HttpGuest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);
export!(Component);

fn number(key: &str) -> u64 {
    durable::get(key).ok().flatten().and_then(|v| String::from_utf8(v).ok()).and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn number_opt(key: &str) -> Option<u64> {
    durable::get(key).ok().flatten().and_then(|v| String::from_utf8(v).ok()).and_then(|s| s.parse().ok())
}

fn now_ms() -> i64 {
    let t = wasip2::clocks::wall_clock::now();
    t.seconds as i64 * 1000 + i64::from(t.nanoseconds / 1_000_000)
}

impl HttpGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        // `wasi:http` hides `Upgrade`: the runtime marks an upgrade offer with `x-rt-websocket: 1`.
        let upgrade = !request.headers().get("x-rt-websocket").is_empty();
        if upgrade {
            let user = http::query_param(query, "user").unwrap_or_else(|| "anon".to_string());
            let opts = sockets::AcceptOptions { tags: vec![user.clone()], attachment: Some(format!("user:{user}").into_bytes()), protocol: None };
            match sockets::accept(&opts) {
                Ok(_) => http::respond(response_out, 101, "text/plain", b""),
                Err(e) => http::respond_text(response_out, 429, &e.code),
            }
            return;
        }
        let text = match path {
            "/hit" => {
                let n = number("count") + 1;
                match durable::put("count", n.to_string().as_bytes()) {
                    Ok(()) => format!("hits={n}"),
                    Err(e) => format!("error={}", e.code),
                }
            }
            "/alarm" => {
                let ms: i64 = http::query_param(query, "ms").and_then(|v| v.parse().ok()).unwrap_or(200);
                match durable::set_alarm(Some(now_ms() + ms)) {
                    Ok(()) => "set".to_string(),
                    Err(e) => format!("error={}", e.code),
                }
            }
            "/closed" => number_opt("closed").map_or("-".to_string(), |n| n.to_string()),
            "/state" => {
                let x = durable::get("x").ok().flatten().map_or("-".to_string(), |v| String::from_utf8_lossy(&v).into_owned());
                format!("count={} fired={} x={x}", number("count"), number("fired"))
            }
            "/out" => {
                let url = http::query_param(query, "url").unwrap_or_default();
                match ws_client::connect(&url, &[]) {
                    Ok(conn) => {
                        let _ = conn.send(&sockets::Payload::Text("ping".to_string()));
                        let reply = match conn.recv(5000) {
                            Ok(Some(ws_client::Event::Message(sockets::Payload::Text(t)))) => format!("got={t}"),
                            Ok(other) => format!("unexpected={}", other.is_some()),
                            Err(e) => format!("error={}", e.code),
                        };
                        let _ = conn.close(1000, "bye");
                        reply
                    }
                    Err(e) => format!("error={}", e.code),
                }
            }
            "/accept" => match sockets::accept(&sockets::AcceptOptions { tags: vec![], attachment: None, protocol: None }) {
                Ok(_) => "accepted".to_string(),
                Err(e) => e.code,
            },
            _ => "?".to_string(),
        };
        http::respond_text(response_out, 200, &text);
    }
}

impl socket_handler::Guest for Component {
    fn on_message(object: String, socket: sockets::SocketInfo, data: sockets::Payload) -> Result<(), String> {
        let send = |text: String| sockets::send(&socket.id, &sockets::Payload::Text(text)).map_err(|e| e.code);
        match &data {
            sockets::Payload::Text(t) if t == "who" => send(object),
            sockets::Payload::Text(t) if t == "att" => {
                let att = sockets::get_attachment(&socket.id).map_err(|e| e.code)?.unwrap_or_default();
                send(String::from_utf8_lossy(&att).into_owned())
            }
            sockets::Payload::Text(t) if t == "boom" => {
                durable::put("x", b"1").map_err(|e| e.code)?;
                send("leaked".to_string())?;
                Err("boom".to_string())
            }
            _ => sockets::send(&socket.id, &data).map_err(|e| e.code),
        }
    }

    fn on_close(_object: String, _socket: sockets::SocketInfo, info: socket_handler::CloseInfo) -> Result<(), String> {
        durable::put("closed", info.code.to_string().as_bytes()).map_err(|e| e.code)
    }

    fn on_error(_object: String, _socket: sockets::SocketInfo, _code: String, _message: String) -> Result<(), String> {
        Ok(())
    }

    fn on_alarm(_object: String, _scheduled_ms: i64, retry: u32) -> Result<(), String> {
        let fired = number("fired") + 1;
        durable::put("fired", fired.to_string().as_bytes()).map_err(|e| e.code)?;
        sockets::broadcast(None, None, &sockets::Payload::Text(format!("alarm:{retry}")), &[]).map_err(|e| e.code)?;
        Ok(())
    }
}
