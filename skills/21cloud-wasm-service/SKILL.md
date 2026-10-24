---
name: 21cloud-wasm-service
description: Builds, debugs and reviews WebAssembly component services for the 21 Cloud Functions Runtime (WASI P2 `wasi:http` components run in wasmtime), in Rust (`wasm32-wasip2`, `wasip2` + `wit-bindgen`) or JavaScript (`jco componentize`). Use when a service uses the `wasm` runtime, when code imports the `std:host` WIT package (`std:host/kv`, `call`, `queue`, `pubsub`, `streams`, `sql`, `blob`, `sockets`, `durable`, `rpc`, ...), or when the user asks to write a Rust/WASM function, gateway or RPC service for 21 Cloud.
---

# 21 Cloud: WASM component services

A WASM service is one **WASI Preview 2 component** that exports `wasi:http/incoming-handler` (any `0.2.x`). Upload it as
`application/wasm` (at most 50 MB) to a service created with `runtime: "wasm"`. The runtime precompiles it and runs a
**fresh instance per task**. Runtime capabilities come from the WIT package `std:host@0.1.0`
([wit/rt-host.wit](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit)). Docs: https://21-ci.github.io/runtime/ .
Deploying: the `21ci-deploy` skill.

References:
- [references/std-host-wit.md](references/std-host-wit.md): every `std:host` interface, the worlds, error codes.
- [references/rust.md](references/rust.md): Rust project layout, `Cargo.toml`, bindings, build and checks.
- [references/js-componentize.md](references/js-componentize.md): JavaScript with `jco componentize`.
- Limits and invoke outcomes are shared with Deno: [21cloud-deno-service limits](../21cloud-deno-service/references/limits.md)
  (if that skill is not installed: https://github.com/21-ci/runtime/blob/main/skills/21cloud-deno-service/references/limits.md).

## Contract

1. **Export `wasi:http/incoming-handler`.** A core module, a component without that export, or one using threads /
   shared memory is `422 invalid_artifact` at deploy.
2. **Import only what the runtime links:** `wasi:io`, `wasi:clocks`, `wasi:random`, `wasi:cli`, `wasi:http`,
   `wasi:filesystem` (no preopens: nothing to open), `wasi:sockets` (every address denied), and `std:host/*`. Any
   WASI `0.2.x` minor links. Full-`std` Rust (stdin, exit, terminal imports) runs fine.
3. **No state between requests.** Each task gets a fresh instance (pre-instantiated, never used before). With
   `wasm.reuse_instances` (on in `min_latency`, or per service) a clean instance may serve the next task of the same
   version: linear memory persists only as a cache. Persist in `std:host/kv`, `sql`, `blob` or durable storage.
4. **The guest sees `/{path}?{query}`**: `/s/{slug}` is stripped (original in header `x-rt-base-path`).
5. **Runtime headers** on every request: `x-rt-task-id`, `x-rt-trigger`, `x-rt-deadline` (epoch ms). HTTP requests
   also carry `x-rt-base-path`, `x-rt-client-ip` (the only way to learn the client address), `traceparent`; a WebSocket
   upgrade offer is `x-rt-websocket: 1` (`wasi:http` hides `Upgrade`). Client `x-rt-*` headers are removed;
   `Authorization: Bearer rtk_…` runtime keys are stripped, other `Authorization` values pass.
6. **Non-HTTP triggers arrive as HTTP requests** to the same handler:

| Trigger | Request | Body (JSON) |
|---|---|---|
| interval / cron schedule | `POST /__rt/scheduled` | `{"scheduleId", "scheduledTime", "payload"}` |
| one-off (`std:host/schedule`) | `POST /__rt/scheduled` | `{"id", "scheduledAt", "payload"}` |
| queue | `POST /__rt/queue` | `{"messages": [{"id", "body", "attempt"}]}`; answer `2xx` to ack the batch, anything else retries it |
| pub/sub subscriber (`events.subscribe`) | `POST /__rt/pubsub` | `{"id", "subject", "body", "headers", "replyable", "publishedAt"}`; for a `request` (`replyable: true`) the response is the reply, otherwise answer `2xx`/`204` |
| stream consumer (`events.consume`) | `POST /__rt/stream` | `{"topic", "group", "partition", "attempt", "messages": [{"partition", "offset", "key", "body", "headers", "timestamp"}]}`; `2xx` commits the batch; `2xx` with `x-rt-stream-fail-at: <offset>` commits the records before that offset and redelivers from it; anything else retries |

   Route on `x-rt-trigger` or the path. `/__rt/` is reserved for the runtime. The pub/sub and stream deliveries set
   `x-rt-trigger: pubsub` / `stream`. Delivery is at least once for streams and async invocations, at most once for pub/sub.
7. **Env and secrets** are the WASI environment (`wasi:cli/environment`); `std:host/secrets.get(name)` reads secret
   values only. Changes apply to the next task.
8. **Logs:** stdout lines go to the service log at `info`, stderr at `error`, tagged with the task id (at most 1,000
   lines per task). `std:host/log` writes records with levels and JSON fields.
9. **Clock** readings are whole milliseconds and advance only across real waits.
10. **Outbound HTTP** is `wasi:http/outgoing-handler` through the egress guard (public internet only, same rules and
    codes as Deno). Outbound WebSockets: `std:host/ws-client`.
11. **Errors** from `std:host` calls are `error { code, message }` with the Deno SDK's codes. Calls outside a task
    return `unavailable`. A trap is `500 guest_error`; memory over the limit is `502 oom`.
12. **Not yet:** blob and call bodies are buffered (no `wasi:io` streams for them). The HTTP response itself streams.

## Pick a world

| You need | Generate bindings for world | Extra export to implement |
|---|---|---|
| HTTP + host imports | `guest` | none |
| Serve typed RPC (`services.call` / `call-rpc` from other services) | `rpc-service` | `std:host/rpc` `handle(method, args-json, caller)` |
| WebSocket events and alarms (required for a durable service) | `socket-service` | `std:host/socket-handler` (`on-message`, `on-close`, `on-error`, `on-alarm`) |
| Both | `runtime-host` | both |

The `wasi:http/incoming-handler` export comes from the `wasip2` crate (Rust) or the JS world (jco), not from these worlds.
A durable WASM service without `socket-handler` is `422 missing_export`. A call to `/__rt/rpc/<method>` of a component
without the `rpc` export answers `method_not_found`. WASM services have no generated `.d.ts` (`21ci types` answers
`422 no_rpc_surface`): document the RPC methods yourself.

## Rust quick start

```bash
21ci init my-fn --template wasm-rust        # scaffold from sample/wasm/rust-hello (crate my-fn-wasm)
cd my-fn && ./build.sh                      # cargo build --release --target wasm32-wasip2
21ci push target/wasm32-wasip2/release/my_fn_wasm.wasm --service my-fn --create --wait
```

Minimal handler (from [sample/wasm/rust-hello](https://github.com/21-ci/runtime/tree/main/sample/wasm/rust-hello)):

```rust
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};

struct Component;
wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_else(|| "/".to_string());
        let headers = Fields::new();
        headers.set("content-type", &[b"text/plain".to_vec()]).unwrap();
        let response = OutgoingResponse::new(headers);
        response.set_status_code(200).unwrap();
        let body = response.body().unwrap();
        ResponseOutparam::set(out, Ok(response));
        let stream = body.write().unwrap();
        stream.blocking_write_and_flush(format!("hello {path}").as_bytes()).unwrap(); // at most 4096 bytes per call
        drop(stream);
        OutgoingBody::finish(body, None).unwrap();
    }
}
```

Host imports with `wit-bindgen` (from [sample/apps/edge-gateway](https://github.com/21-ci/runtime/tree/main/sample/apps/edge-gateway)):

```rust
mod bindings {
    // `std` is also the Rust crate name: keep the generated `std:host` module in its own scope.
    wit_bindgen::generate!({ path: "wit", world: "guest" });   // wit/rt-host.wit copied from the runtime repo
}
use bindings::std::host::{kv, log, queue};

match kv::incr(b"hits", 1, kv::SetOptions { ttl_seconds: Some(65) }) {   // atomic counter
    Ok(count) => log::log(log::Level::Info, &format!("hits={count}")),
    Err(e) => log::log(log::Level::Warn, &format!("kv: {} ({})", e.code, e.message)),
}
let opts = queue::SendOptions { delay_seconds: None, dedup_id: None };
if let Err(e) = queue::enqueue("audit", br#"{"event":"hit"}"#, &opts) { /* e.code: not_found, forbidden, queue_full, ... */ }
```

Details, `Cargo.toml`, RPC and import checks: [references/rust.md](references/rust.md).

## JavaScript quick start

```bash
21ci init my-fn --template wasm-js          # scaffold from sample/wasm/js-componentize
cd my-fn && ./build.sh                      # npm install on first run, then jco componentize src/app.js ...
21ci push dist/app.wasm --service my-fn --create --wait
```

This is **not** the Deno contract: no `export default`, no `ctx`, no `std:*` modules, no `Deno.*`. Listen for the
`fetch` event. See [references/js-componentize.md](references/js-componentize.md).

## Debug checklist

| Symptom | Check |
|---|---|
| `422 invalid_artifact` | `wasm-tools component wit <file>`: needs `export wasi:http/incoming-handler@0.2.x`; imports only from the list in rule 2; not a core module |
| `422 missing_export` | durable service without `std:host/socket-handler` (use world `socket-service`) |
| `500 guest_error` | a trap or panic (`panic = "abort"` traps); read `21ci logs <svc> --level error` |
| `502 oom` | linear memory over `memory_mb` (default 128 MB) |
| `502 cpu_limit` / `504 timeout` | CPU budget (1 s HTTP) / wall limit (30 s HTTP) |
| Host call returns `unavailable` | called outside a task (for example from a static initializer) |
| Counter always 1 | expected: fresh instance per task (rule 3) |

More examples in the repository: [fixtures/wasm](https://github.com/21-ci/runtime/tree/main/fixtures/wasm) (test
guests: `kv`, `calls`, `rpc`, `ws`, `stream`, ...).
