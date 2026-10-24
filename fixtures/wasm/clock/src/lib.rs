//! `clock` fixture (WSM-11, SEC-22): reads `wasi:clocks` before and after a busy loop.
//!
//! `GET /?iters=N&fetch=HOST[&path=/p][&ready_polls=K]`
//!
//! Reads monotonic + wall clock, spins `N` iterations (default 50M of a dependent
//! multiply-add chain, about 50 ms of real time), reads the clocks again. With
//! `fetch=HOST` it then performs `GET http://HOST<path>` through `wasi:http/outgoing-handler` and
//! reads the clocks a third time. With `ready_polls=K` the busy loop is cut into `K` pieces with
//! a `subscribe-duration(0)` + `block` (a pollable that is ready at once, no real wait) after
//! each. Response (text, one `key=value` per line):
//!
//! ```text
//! start_mono_ns=…             # monotonic reading at the start of the request (reuse tests)
//! start_wall_ns=…             # wall reading at the start of the request, Unix ns
//! spin_mono_delta_ns=0        # frozen clock: no advance during synchronous execution
//! spin_wall_delta_ns=0
//! spin_std_instant_delta_ns=0
//! fetch_status=200            # only with fetch=
//! after_fetch_mono_delta_ns=… # > 0 (clock advanced at the I/O boundary)
//! after_fetch_wall_delta_ns=…
//! ```

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use std::hint::black_box;
use std::time::Instant;

use wasip2::clocks::{monotonic_clock, wall_clock};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::outgoing_handler;
use wasip2::http::types::{Fields, IncomingRequest, Method, OutgoingRequest, ResponseOutparam, Scheme};

struct Component;

wasip2::http::proxy::export!(Component);

/// ~50 ms of the dependent multiply-add chain on a recent laptop core (WSM-11 "50 ms busy loop").
const DEFAULT_ITERS: u64 = 50_000_000;

#[inline(never)]
fn spin(iters: u64) -> u64 {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..iters {
        x = black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407));
    }
    x
}

fn wall_ns() -> i128 {
    let t = wall_clock::now();
    i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanoseconds)
}

/// `GET http://authority/path`; returns the status or an `error:...` string.
fn fetch(authority: &str, path: &str) -> String {
    let request = OutgoingRequest::new(Fields::new());
    let _ = request.set_method(&Method::Get);
    let _ = request.set_scheme(Some(&Scheme::Http));
    let _ = request.set_authority(Some(authority));
    let _ = request.set_path_with_query(Some(path));
    match outgoing_handler::handle(request, None) {
        Err(e) => format!("denied:{e:?}"),
        Ok(future) => {
            future.subscribe().block();
            match future.get() {
                Some(Ok(Ok(response))) => response.status().to_string(),
                Some(Ok(Err(e))) => format!("error:{e:?}"),
                _ => "error:unexpected".to_string(),
            }
        }
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (_path, query) = http::split_query(&pq);
        let iters: u64 = http::query_param(query, "iters").and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_ITERS);
        let fetch_host = http::query_param(query, "fetch");
        let fetch_path = http::query_param(query, "path").unwrap_or_else(|| "/".to_string());

        let ready_polls: u64 = http::query_param(query, "ready_polls").and_then(|v| v.parse().ok()).unwrap_or(0);

        let (m0, w0, i0) = (monotonic_clock::now(), wall_ns(), Instant::now());
        if ready_polls == 0 {
            black_box(spin(iters));
        } else {
            for _ in 0..ready_polls {
                black_box(spin(iters / ready_polls));
                monotonic_clock::subscribe_duration(0).block();
            }
        }
        let (m1, w1, i1) = (monotonic_clock::now(), wall_ns(), Instant::now());

        let mut body = String::new();
        body.push_str(&format!("start_mono_ns={m0}\n"));
        body.push_str(&format!("start_wall_ns={w0}\n"));
        body.push_str(&format!("spin_mono_delta_ns={}\n", m1 - m0));
        body.push_str(&format!("spin_wall_delta_ns={}\n", w1 - w0));
        body.push_str(&format!("spin_std_instant_delta_ns={}\n", i1.duration_since(i0).as_nanos()));

        if let Some(host) = fetch_host {
            let status = fetch(&host, &fetch_path);
            let (m2, w2) = (monotonic_clock::now(), wall_ns());
            body.push_str(&format!("fetch_status={status}\n"));
            body.push_str(&format!("after_fetch_mono_delta_ns={}\n", m2 - m1));
            body.push_str(&format!("after_fetch_wall_delta_ns={}\n", w2 - w1));
        }
        http::respond_text(response_out, 200, &body);
    }
}
