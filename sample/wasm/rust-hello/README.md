# rust-hello

A WASI Preview 2 HTTP component written in Rust, for a `runtime: "wasm"` service.

## Shows

- A cdylib built for `wasm32-wasip2` is already a **component**. The `wasip2` crate embeds the `wasi:http/proxy` world, so the component
  exports `wasi:http/incoming-handler`, the one thing the runtime requires (a component without it is rejected at deploy with `422 invalid_artifact`)
- The guest sees `/{rest}?{query}` (the runtime strips `/s/{slug}`); responses are streamed through `wasi:io/streams`
- A fresh instance per task: `hitsInThisInstance` is `1` on every request under the default profiles, and only grows with `wasm.reuse_instances` (`min_latency`)
- Lines written to `wasi:cli/stdout` land in the service logs tagged with the task id (the example logs one line per request)
- A minimal import set (`wasi:http`, `wasi:io`, `wasi:clocks`, `wasi:cli/stdout`). This is a choice for size, not a runtime
  requirement: the runtime links all of `wasi:io`, `wasi:clocks`, `wasi:random`, `wasi:cli` (stdin empty, `exit` ends the task,
  `terminal-*` as "no terminal") and `wasi:http`, plus `wasi:filesystem` with nothing to open and `wasi:sockets` with every
  address denied, so a full-`std` build (which also imports `wasi:cli/{stdin,exit,terminal-*}`) deploys and runs too. The crate is
  `#![no_std]` with three small shims (allocator, `cabi_realloc`, `memcmp`) to keep the component small; see the header of `src/lib.rs`

## Build

```bash
cd sample/wasm/rust-hello
./build.sh
# -> target/wasm32-wasip2/release/rust_hello_wasm.wasm (~40 KB)
```

`build.sh` runs `cargo build --release --target wasm32-wasip2` (the repo's pinned toolchain already has the target) and, if `wasm-tools`
is installed, verifies the export and the import allowlist.

## Deploy

```bash
../../scripts/deploy.sh target/wasm32-wasip2/release/rust_hello_wasm.wasm rust-hello
```

The script creates the service with `runtime: "wasm"` and uploads `application/wasm`.

## Invoke

```bash
../../scripts/invoke.sh rust-hello '/?name=Ferris'
```

## Expected response

```json
{ "hitsInThisInstance": 1, "message": "Hello from Rust WASM component, Ferris!", "method": "GET", "path": "/", "unixSeconds": 1790881616 }
```

## Try it without the runtime

Any `wasi:http` host can run it, for example `wasmtime serve target/wasm32-wasip2/release/rust_hello_wasm.wasm`.

## Notes

- Capabilities such as KV/SQL/blob reach a Rust component through the `std:host/*` WIT interfaces (not used by this minimal example).
- `wasm-opt -Os` can shrink the file further, but the runtime pre-compiles components to `.cwasm` at build time anyway.
