//! `grow` fixture (WSM-03): grows linear memory 1 MiB at a time.
//!
//! - `GET /?mb=N` (default 1024) grows by `N` MiB in total, touching the first byte of every new
//!   MiB so the pages are really committed, then answers `200 grown <N>`.
//! - If the host refuses a growth step *without* trapping (`memory.grow` returns -1) the guest
//!   answers `200 grow_refused <MiB grown so far>`.
//!
//! A host limiter that returns `Err` makes `memory.grow` trap: the runtime maps that to
//! `502 oom` (`Killed(Oom)`), never to `guest_error`.

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

/// 16 wasm pages = 1 MiB.
const PAGES_PER_MIB: usize = 16;
const PAGE: usize = 65_536;

struct Component;

wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (_path, query) = http::split_query(&pq);
        let target_mb: usize = http::query_param(query, "mb").and_then(|v| v.parse().ok()).unwrap_or(1024);

        let mut grown = 0usize;
        while grown < target_mb {
            let old_pages = core::arch::wasm32::memory_grow(0, PAGES_PER_MIB);
            if old_pages == usize::MAX {
                http::respond_text(response_out, 200, &format!("grow_refused {grown}"));
                return;
            }
            // SAFETY: `old_pages * PAGE` is the first byte of memory that `memory.grow` just added.
            unsafe { core::ptr::write_volatile((old_pages * PAGE) as *mut u8, 1) };
            grown += 1;
        }
        http::respond_text(response_out, 200, &format!("grown {grown}"));
    }
}
