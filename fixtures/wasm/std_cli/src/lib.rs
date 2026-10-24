//! `std_cli` fixture (WSM-21): a component built with the full Rust `std` (not `no_std`), the way
//! most Rust services are. Such a component imports `wasi:cli/terminal-input`, `terminal-output`,
//! `terminal-stdin`, `terminal-stdout` and `terminal-stderr` (std's `IsTerminal` and console
//! setup), which the runtime links as "no terminal".
//!
//! `GET /` -> `println!` / `eprintln!` one line each, then `200` with
//! `terminals=<stdin>,<stdout>,<stderr>` (`IsTerminal`, all `false` here), `greeting=<$GREETING or ->`
//! and `time_ok=<SystemTime is after 2020>`.

use std::io::IsTerminal as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
mod http {
    include!("../../_shared/http.rs");
}

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;

wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(_request: IncomingRequest, response_out: ResponseOutparam) {
        println!("std_cli println line");
        eprintln!("std_cli eprintln line");
        let terminals = [
            std::io::stdin().is_terminal(),
            std::io::stdout().is_terminal(),
            std::io::stderr().is_terminal(),
        ];
        let greeting = std::env::var("GREETING").unwrap_or_else(|_| "-".to_string());
        let after_2020 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d > Duration::from_secs(1_577_836_800))
            .unwrap_or(false);
        let body = format!(
            "terminals={},{},{}\ngreeting={greeting}\ntime_ok={after_2020}\n",
            terminals[0], terminals[1], terminals[2]
        );
        http::respond_text(response_out, 200, &body);
    }
}
