//! `edge` fixture (scratchpad edge-cases.md, WASM section).
//!
//! - `GET /recurse?depth=N`  (WCPU-002) recurses `N` frames deep (default: until the stack overflows)
//! - `GET /stdout`           (WSTD-002) writes non-UTF-8 bytes, a NUL line, ANSI escapes and a `\r`
//!   without `\n` to stdout, then `200 written`
//! - `GET /accept`           (WWS-002) calls `sockets.accept`; `101` when accepted, else
//!   `400 accept_error:<code>`
//! - `GET /badutf8`          (WIT-007) passes a `string` that is not UTF-8 to `log.log` (the
//!   canonical ABI must trap)
//! - `GET /blobput?mb=N`     (WIT-004) puts an `N` MiB blob; `200 stored:<size>` or
//!   `500 blob_error:<code>`

use ::std::io::Write as _;

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

wit_bindgen::generate!({
    path: "../../../wit",
    world: "guest",
});

use self::std::host::{blob, log, sockets};
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

#[inline(never)]
fn recurse(n: u64, depth: u64) -> u64 {
    // A frame the optimiser cannot fold away.
    let pad = [n as u8; 256];
    if depth != 0 && n >= depth {
        return u64::from(pad[0]);
    }
    recurse(n + 1, depth).wrapping_add(u64::from(::std::hint::black_box(pad)[(n % 256) as usize]))
}

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let pq = http::path_and_query(&request);
        let (path, query) = http::split_query(&pq);
        match path {
            "/recurse" => {
                let depth: u64 = http::query_param(query, "depth").and_then(|v| v.parse().ok()).unwrap_or(0);
                let r = recurse(0, depth);
                http::respond_text(response_out, 200, &format!("depth:{r}"));
            }
            "/stdout" => {
                let mut out = ::std::io::stdout();
                let _ = out.write_all(b"bad utf8 \xff\xfe end\n");
                let _ = out.write_all(b"\0\0\0\n");
                let _ = out.write_all(b"\x1b[31mred\x1b[0m\n");
                let _ = out.write_all(b"carriage\rreturn\n");
                let _ = out.write_all(b"forged\n{\"level\":\"error\",\"msg\":\"injected\"}\n");
                let _ = out.flush();
                http::respond_text(response_out, 200, "written");
            }
            "/accept" => match sockets::accept(&sockets::AcceptOptions { tags: Vec::new(), attachment: None, protocol: None }) {
                Ok(_) => http::respond_text(response_out, 101, ""),
                Err(e) => http::respond_text(response_out, 400, &format!("accept_error:{}", e.code)),
            },
            "/badutf8" => {
                // SAFETY: deliberately not UTF-8: the host side of the canonical ABI must refuse it.
                let bad = unsafe { String::from_utf8_unchecked(vec![b'o', b'k', 0xff, 0xfe]) };
                log::log(log::Level::Info, &bad);
                http::respond_text(response_out, 200, "logged");
            }
            "/blobput" => {
                let mb: usize = http::query_param(query, "mb").and_then(|v| v.parse().ok()).unwrap_or(1);
                let data = vec![7u8; mb * 1024 * 1024];
                match blob::put("big", None, &data) {
                    Ok(m) => http::respond_text(response_out, 200, &format!("stored:{}", m.size)),
                    Err(e) => http::respond_text(response_out, 500, &format!("blob_error:{}", e.code)),
                }
            }
            _ => http::respond_text(response_out, 404, "unknown"),
        }
    }
}
