//! `spin` fixture: a busy loop that never touches the host.
//!
//! - `GET /`               -> loops forever (CPU limit / wall timeout / `Kill` tests)
//! - `GET /?iters=N`       -> runs `N` iterations of a dependent multiply-add chain, then
//!   answers `200 spun` (preemption test: a finite hog that must still yield to its neighbours)

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use std::hint::black_box;

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

#[inline(never)]
fn spin(iters: Option<u64>) -> u64 {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut i: u64 = 0;
    loop {
        x = black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407));
        i += 1;
        if let Some(n) = iters {
            if i >= n {
                return x;
            }
        }
    }
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (_path, query) = http::split_query(&pq);
        let iters = http::query_param(query, "iters").and_then(|v| v.parse::<u64>().ok());
        let x = spin(iters);
        http::respond_text(response_out, 200, &format!("spun {x}"));
    }
}
