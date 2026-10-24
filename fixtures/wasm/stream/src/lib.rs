//! `stream` fixture (WSM-14): sends the response head, then writes the body in chunks with
//! real-time pauses (`wasi:clocks` `subscribe-duration` + `block`, an I/O wait) in between.
//!
//! - `GET /?chunks=N&delay_ms=D&size=S` -> `200`, `N` chunks (default 3), each `chunk-<i>\n`
//!   padded with `.` to `S` bytes (default: no padding), `D` ms (default 200) between chunks
//! - `GET /trap?delay_ms=D`             -> head + `chunk-0\n`, a pause, then a trap
//! - `GET /burn?iters=N`                 -> head + `start\n`, a busy loop of `N` iterations (CPU
//!   after the head), then `end\n`
//! - `GET /unfinished?delay_ms=D`        -> head + `chunk-0\n`, a pause (default none), then returns
//!   while still holding the outgoing body (never finished or dropped)
//! - `GET /abort?delay_ms=D`             -> head + `chunk-0\n`, a pause (default none), then drops the
//!   outgoing body without finishing it (the body errors)
//! - `GET /error?delay_ms=D`             -> answers the outparam with an `http` error instead of a
//!   response, lingers `D` ms (default none), returns
//! - `GET /noresponse?delay_ms=D`        -> drops the outparam unanswered, lingers `D` ms, returns
//! - `GET /trap?before=N&delay_ms=D`     -> as `/trap`, but `N` chunks (default 1) `D` ms apart go first
//!
//! Every response carries `x-count: <n>`, a counter in linear memory (`1` on a fresh instance;
//! `2, 3, …` only when the runtime reuses the instance).

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use wasip2::clocks::monotonic_clock;
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{ErrorCode, Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};

static COUNTER: AtomicU32 = AtomicU32::new(0);

struct Component;

wasip2::http::proxy::export!(Component);

fn pause(ms: u64) {
    monotonic_clock::subscribe_duration(ms * 1_000_000).block();
}

#[inline(never)]
fn spin(iters: u64) -> u64 {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..iters {
        x = black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407));
    }
    x
}

fn chunk(i: u32, size: usize) -> Vec<u8> {
    let mut c = format!("chunk-{i}\n").into_bytes();
    if c.len() < size {
        let nl = c.pop();
        c.resize(size - 1, b'.');
        c.extend(nl);
    }
    c
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let count = COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        let chunks: u32 = http::query_param(query, "chunks").and_then(|v| v.parse().ok()).unwrap_or(3);
        let delay_ms: u64 = http::query_param(query, "delay_ms").and_then(|v| v.parse().ok()).unwrap_or(200);
        let size: usize = http::query_param(query, "size").and_then(|v| v.parse().ok()).unwrap_or(0);

        // `delay_ms` (default none) keeps the guest running after it answered or gave up its outparam.
        let linger: u64 = http::query_param(query, "delay_ms").and_then(|v| v.parse().ok()).unwrap_or(0);
        if path == "/error" {
            ResponseOutparam::set(response_out, Err(ErrorCode::InternalError(Some("fixture error".to_string()))));
            pause(linger);
            return;
        }
        if path == "/noresponse" {
            drop(response_out); // the outparam is dropped unanswered
            pause(linger);
            return;
        }

        let headers = Fields::new();
        headers.set("content-type", &[b"text/plain".to_vec()]).expect("set content-type");
        headers.set("x-count", &[count.to_string().into_bytes()]).expect("set x-count");
        let response = OutgoingResponse::new(headers);
        response.set_status_code(200).expect("set status");
        let out_body = response.body().expect("response body");
        ResponseOutparam::set(response_out, Ok(response));
        let stream = out_body.write().expect("body stream");

        if path == "/burn" {
            let iters: u64 = http::query_param(query, "iters").and_then(|v| v.parse().ok()).unwrap_or(0);
            stream.blocking_write_and_flush(b"start\n").expect("write start");
            black_box(spin(iters));
            stream.blocking_write_and_flush(b"end\n").expect("write end");
            drop(stream);
            OutgoingBody::finish(out_body, None).expect("finish body");
            return;
        }
        if path == "/unfinished" {
            stream.blocking_write_and_flush(&chunk(0, 0)).expect("write chunk");
            drop(stream);
            pause(linger);
            // Neither finished nor dropped: the host still holds the body when the guest returns.
            std::mem::forget(out_body);
            return;
        }
        if path == "/abort" {
            stream.blocking_write_and_flush(&chunk(0, 0)).expect("write chunk");
            pause(http::query_param(query, "delay_ms").and_then(|v| v.parse().ok()).unwrap_or(0));
            drop(stream);
            drop(out_body); // dropped without `finish`: the host sees a failed body
            return;
        }
        if path == "/trap" {
            let before: u32 = http::query_param(query, "before").and_then(|v| v.parse().ok()).unwrap_or(1);
            for i in 0..before {
                if i > 0 {
                    pause(delay_ms);
                }
                stream.blocking_write_and_flush(&chunk(i, 0)).expect("write chunk");
            }
            pause(delay_ms);
            panic!("stream fixture: trap after the head was sent");
        }
        for i in 0..chunks {
            if i > 0 {
                pause(delay_ms);
            }
            for part in chunk(i, size).chunks(4096) {
                if stream.blocking_write_and_flush(part).is_err() {
                    return;
                }
            }
        }
        drop(stream);
        OutgoingBody::finish(out_body, None).expect("finish body");
    }
}
