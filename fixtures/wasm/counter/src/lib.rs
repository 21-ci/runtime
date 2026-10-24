//! `counter` fixture (WSM-04, WSM-05).
//!
//! A global counter in linear memory (a `static` lives in the data segment).
//!
//! - `GET /...`  -> increments and returns the new value as decimal text (`1` on a fresh instance;
//!   `1, 2, 3, ...` only when the runtime reuses the instance, `wasm.reuse_instances`)
//! - `GET /trap` -> increments, then traps: a reused instance must be dropped, the next
//!   request starts again at `1`

use std::sync::atomic::{AtomicU32, Ordering};

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

static COUNTER: AtomicU32 = AtomicU32::new(0);

struct Component;

wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, _query) = http::split_query(&pq);
        let value = COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        if path == "/trap" {
            panic!("counter fixture: trap requested at count {value}");
        }
        http::respond_text(response_out, 200, &value.to_string());
    }
}
