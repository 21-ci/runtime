//! Minimal WASI P2 HTTP component for the runtime (`runtime: "wasm"`).
//!
//! Exports `wasi:http/incoming-handler` (world `wasi:http/proxy`) and imports
//! only `wasi:http/types`, `wasi:io/{error,streams}`, `wasi:clocks/wall-clock`
//! and `wasi:cli/stdout`. That is inside the import set the runtime accepts
//!: no `wasi:filesystem`, no `wasi:sockets`.
//!
//! Why `#![no_std]`? A `std` hello-world on `wasm32-wasip2` also imports
//! `wasi:cli/{stdin,exit,terminal-*}` (pulled in by Rust's runtime). Going
//! `no_std` keeps the import list minimal. The price is three small shims:
//! a global allocator, `cabi_realloc` (canonical-ABI allocator hook) and
//! `memcmp`. Drop them, and the `no_std` attributes, if your runtime also
//! accepts the extra `wasi:cli` imports.
//!
//! A fresh instance is created per task, so `HITS` is `1` under the default
//! profiles (WSM-04); it only grows with `wasm.reuse_instances` (`min_latency`).
#![no_std]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, Ordering};

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

struct Component;

wasip2::http::proxy::export!(Component);

/// Per-instance counter: shows there is no warm state between WASM requests.
static HITS: AtomicU32 = AtomicU32::new(0);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let hits = HITS.fetch_add(1, Ordering::Relaxed) + 1;

        // The runtime strips `/s/{slug}`; the guest sees `/{rest}?{query}`.
        let path_with_query = request.path_with_query().unwrap_or_else(|| "/".to_string());
        let (path, query) = path_with_query
            .split_once('?')
            .unwrap_or((path_with_query.as_str(), ""));
        let name = query_param(query, "name").unwrap_or_else(|| "Rust".to_string());
        let method = method_name(&request.method());

        // wasi:cli/stdout -> service logs, tagged with task_id (WSM-07).
        let mut line = String::new();
        line.push_str("hello: ");
        line.push_str(&method);
        line.push(' ');
        line.push_str(&path_with_query);
        line.push('\n');
        let _ = wasip2::cli::stdout::get_stdout().blocking_write_and_flush(line.as_bytes());

        let now = wasip2::clocks::wall_clock::now();
        let body = serde_json::json!({
            "message": alloc::format!("Hello from Rust WASM component, {name}!"),
            "path": path,
            "method": method,
            "hitsInThisInstance": hits,
            "unixSeconds": now.seconds,
        })
        .to_string();

        respond(response_out, 200, "application/json", body.as_bytes());
    }
}

fn respond(response_out: ResponseOutparam, status: u16, content_type: &str, body: &[u8]) {
    let headers = Fields::new();
    headers
        .set("content-type", &[content_type.as_bytes().to_vec()])
        .expect("set content-type");

    let response = OutgoingResponse::new(headers);
    response.set_status_code(status).expect("set status");
    let out_body = response.body().expect("response body");

    // Hand the response head to the host first, then stream the body.
    ResponseOutparam::set(response_out, Ok(response));

    let stream = out_body.write().expect("body stream");
    // `blocking-write-and-flush` accepts at most 4096 bytes per call.
    for chunk in body.chunks(4096) {
        stream.blocking_write_and_flush(chunk).expect("write body");
    }
    drop(stream);
    OutgoingBody::finish(out_body, None).expect("finish body");
}

fn method_name(m: &Method) -> String {
    match m {
        Method::Get => "GET".to_string(),
        Method::Head => "HEAD".to_string(),
        Method::Post => "POST".to_string(),
        Method::Put => "PUT".to_string(),
        Method::Delete => "DELETE".to_string(),
        Method::Connect => "CONNECT".to_string(),
        Method::Options => "OPTIONS".to_string(),
        Method::Trace => "TRACE".to_string(),
        Method::Patch => "PATCH".to_string(),
        Method::Other(o) => o.clone(),
    }
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(k) == key).then(|| percent_decode(v))
    })
}

/// Minimal `application/x-www-form-urlencoded` decoder (`+` and `%XX`).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(hi), Some(lo)) => {
                    out.push(hi << 4 | lo);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// `no_std` shims (std provides these on wasm32-wasip2)
// ---------------------------------------------------------------------------

#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

/// A panic traps; the runtime reports `500 guest_error` and logs the task.
#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

/// Canonical-ABI allocator hook: the host calls it to place strings/lists
/// (e.g. the request path) into guest memory.
///
/// # Safety
/// Called by the component-model glue with the canonical-ABI contract.
#[no_mangle]
pub unsafe extern "C" fn cabi_realloc(
    old_ptr: *mut u8,
    old_len: usize,
    align: usize,
    new_len: usize,
) -> *mut u8 {
    use alloc::alloc::{alloc, dealloc, realloc, Layout};
    if old_len == 0 {
        if new_len == 0 {
            return align as *mut u8;
        }
        return alloc(Layout::from_size_align_unchecked(new_len, align));
    }
    if new_len == 0 {
        dealloc(old_ptr, Layout::from_size_align_unchecked(old_len, align));
        return align as *mut u8;
    }
    realloc(old_ptr, Layout::from_size_align_unchecked(old_len, align), new_len)
}

/// `core` slice comparison lowers to `memcmp`; without libc we provide it.
///
/// # Safety
/// `a` and `b` must be valid for reads of `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn memcmp(a: *const u8, b: *const u8, n: usize) -> i32 {
    let mut i = 0;
    while i < n {
        let (x, y) = (*a.add(i), *b.add(i));
        if x != y {
            return x as i32 - y as i32;
        }
        i += 1;
    }
    0
}
