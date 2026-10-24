# Rust components (`wasm32-wasip2`)

A `cdylib` built for `wasm32-wasip2` is already a component (rustc runs `wasm-component-ld`). The `wasip2` crate
provides the `wasi:http` types and the `wasi:http/incoming-handler` export; `wit-bindgen` generates the `std:host`
imports (and the `rpc` / `socket-handler` exports) from the runtime's WIT file.

Examples in https://github.com/21-ci/runtime :

| Path | Shows |
|---|---|
| [sample/wasm/rust-hello](https://github.com/21-ci/runtime/tree/main/sample/wasm/rust-hello) | minimal `#![no_std]` HTTP component, no host imports (~40 KB) |
| [sample/apps/edge-gateway/gateway](https://github.com/21-ci/runtime/tree/main/sample/apps/edge-gateway/gateway) | full `std` component: `std:host/{kv,call,queue,sdk,log}`, JWT checks, rate limits, host-side unit tests |
| [fixtures/wasm/rpc](https://github.com/21-ci/runtime/tree/main/fixtures/wasm/rpc) | serving RPC (`std:host/rpc` export) and calling it (`call-rpc`, `sdk.invoke`) |
| [fixtures/wasm/ws](https://github.com/21-ci/runtime/tree/main/fixtures/wasm/ws) | sockets / socket-handler |

## Toolchain

```bash
rustup target add wasm32-wasip2
cargo install wasm-tools        # optional: inspect and check the component
```

## `Cargo.toml` (std, with host imports)

```toml
[package]
name = "my-fn"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib", "rlib"]     # rlib lets `cargo test` run pure modules on the host

[dependencies]
wasip2 = "2"                         # wasi:http types + the incoming-handler export
wit-bindgen = "0.62"                 # std:host bindings from wit/rt-host.wit
serde_json = "1"

[profile.release]
opt-level = "s"
lto = true
strip = true
panic = "abort"                      # a panic traps: 500 guest_error
codegen-units = 1

[workspace]                          # standalone crate (stops cargo from walking up into another workspace)
```

The `21ci init --template wasm-rust` scaffold is the `no_std` rust-hello crate (`wasip2` with
`default-features = false`, `dlmalloc`, hand-written `cabi_realloc` / `memcmp`). To add host imports to a `no_std`
crate, use `wit-bindgen = { version = "0.62", default-features = false, features = ["macros", "realloc"] }` as the
`fixtures/wasm/*` crates do, or switch to `std` (simpler; the runtime links every import `std` pulls in).

## Bindings

Copy [wit/rt-host.wit](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit) to `wit/rt-host.wit` in your crate.

```rust
// HTTP + imports only (world `guest`)
mod bindings {
    // `std` is also the Rust crate name: keep the generated `std::host` module in its own scope
    wit_bindgen::generate!({ path: "wit", world: "guest" });
}
use bindings::std::host::{call as rtcall, kv, log, queue, sdk};

use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;
wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(req: IncomingRequest, out: ResponseOutparam) { /* ... */ }
}
```

Calls (shapes from the edge-gateway sample):

```rust
let n = kv::incr(&key, 1, kv::SetOptions { ttl_seconds: Some(60) });          // Result<i64, kv::Error>
let res = rtcall::call("notes", &rtcall::Request {
    method: "GET".into(), path_with_query: "/items?limit=10".into(), headers: vec![], body: vec![],
});                                                                             // Result<rtcall::Response, _>
let r = rtcall::call_rpc("billing", "createInvoice", r#"{"amount":100}"#);     // Result<String, _> (JSON)
let s = sdk::invoke("acme", "getPlatformInfo", "null");                        // operator SDK
queue::enqueue("audit", body.as_bytes(), &queue::SendOptions { delay_seconds: None, dedup_id: Some(id) });
log::log(log::Level::Info, "message");
```

Errors are `{ code, message }` structs: branch on `e.code`.

## Serving RPC (world `rpc-service`)

```rust
wit_bindgen::generate!({ path: "wit", world: "rpc-service" });

use self::exports::std::host::rpc;
use wasip2::exports::http::incoming_handler::Guest as HttpGuest;
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

struct Component;
wasip2::http::proxy::export!(Component);   // the HTTP export
export!(Component);                        // the std:host/rpc export

impl rpc::Guest for Component {
    fn handle(method: String, args_json: String, caller: Option<rpc::Caller>) -> Result<String, rpc::Error> {
        match method.as_str() {
            "echo" => Ok(args_json),
            _ => Err(rpc::Error { code: "method_not_found".into(), message: method }),
        }
    }
}
impl HttpGuest for Component {
    fn handle(_req: IncomingRequest, _out: ResponseOutparam) { /* HTTP path */ }
}
```

`caller` is stamped by the runtime (`service`, `slug`, `project`, `environment`, `tenant`, `plan`); authorize with it.
For WebSocket events use world `socket-service` and implement `exports::std::host::socket_handler::Guest`; for both
exports use world `runtime-host`.

## Writing the response

```rust
let headers = Fields::new();
headers.set("content-type", &[b"application/json".to_vec()]).expect("header");
let response = OutgoingResponse::new(headers);
response.set_status_code(200).expect("status");
let body = response.body().expect("body");
ResponseOutparam::set(out, Ok(response));          // head first
let stream = body.write().expect("stream");
for chunk in bytes.chunks(4096) {                  // blocking-write-and-flush takes at most 4096 bytes per call
    stream.blocking_write_and_flush(chunk).expect("write");
}
drop(stream);
OutgoingBody::finish(body, None).expect("finish");
```

The response streams to the client as written, up to `max_response_stream_bytes` (100 MB by default).

## Build and check

```bash
cargo build --release --target wasm32-wasip2
OUT=target/wasm32-wasip2/release/my_fn.wasm          # crate name with '-' -> '_'
wasm-tools component wit "$OUT" | grep -E '^\s*(import|export) '
# must show:   export wasi:http/incoming-handler@0.2.x
# imports only: wasi:{io,clocks,random,cli,http,filesystem,sockets}/*@0.2.x and std:host/*@0.1.0
21ci push "$OUT" --service my-fn --create --wait
wasmtime serve "$OUT"                                 # local smoke test of the HTTP path (no std:host imports)
```

`sample/apps/edge-gateway/gateway/build.sh` is a complete build script with the import check and `--test` for host
unit tests. Keep pure logic (parsing, validation, rate-limit math) in modules without bindings so `cargo test` runs it
on the host.
