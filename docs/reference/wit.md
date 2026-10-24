---
title: std:host WIT package
description: Reference of the std:host@0.1.0 WIT interfaces that WASM components import for logs, KV, SQL, blobs, queues, calls, RPC, operator SDKs and bindings.
---

# `std:host` WIT package

<span class="rt-badge rt-badge--wasm">WASM</span> `std:host@0.1.0` is the set of WIT interfaces the runtime provides to
WASM components, next to the standard WASI ones: logs, key/value storage, SQL, blobs, queues, service-to-service calls
and operator bindings. This page lists every interface, type, function and error code, and shows how to generate
bindings in Rust and JavaScript. For a walkthrough, read [Writing WASM services](../guides/wasm.md); the Deno
counterparts are in the [guest API reference](guest-api.md#wasm-equivalents).

## At a glance

| Interface | Functions | Purpose |
|---|---|---|
| [`log`](#log) | `log` | write a log record at a level |
| [`kv`](#kv) | `get`, `set`, `delete`, `list`, `incr` | per-service key/value store with TTL and atomic counters |
| [`sql`](#sql) | `execute`, `batch` | per-service SQLite database |
| [`blob`](#blob) | `get`, `head`, `put`, `delete`, `list` | per-service object storage |
| [`queue`](#queue) | `enqueue` | send a message to a service's queue |
| [`call`](#call) | `call`, `call-rpc` | call another service in-process; typed RPC |
| [`sdk`](#sdk) | `invoke` | call an operator SDK (it runs in the runtime) |
| [`rpc`](#rpc-export) (export) | `handle` | serve RPC calls (world `rpc-service`) |
| [`platform`](#platform) | `fetch` | call an operator binding without credentials (operator SDK services only) |
| `sockets` | `accept`, `send`, `broadcast`, `close`, `close-all`, `list`, `get`, `set-tags`, `get-attachment`, `set-attachment`, `set-auto-response` | the service's WebSockets ([guide](../guides/websockets.md#wasm-services)) |
| `durable` | `object`, `get`, `put`, `delete`, `list`, `delete-all`, `get-alarm`, `set-alarm`, `fetch` | the task's durable object: storage, alarm, requests to objects |
| `ws-client` | `connect`; `connection.send`, `recv`, `close` | outbound WebSockets through the egress guard |
| `socket-handler` (export) | `on-message`, `on-close`, `on-error`, `on-alarm` | socket events and alarms (world `socket-service`) |

The world `guest` imports every interface above, and `secrets`, `lock` and `schedule` (see the file below); the world
`rpc-service` adds the `rpc` export and the world `socket-service` the `socket-handler` export. A component may import any subset: wit-bindgen and jco only emit imports for the
interfaces your code uses or your world lists.

## The full file

This is `wit/rt-host.wit` from the runtime repository. Copy it into your project's `wit/` directory (or
`wit/deps/rt-host/` for jco) to generate bindings.

```text title="wit/rt-host.wit"
--8<-- "wit/rt-host.wit"
```

## Conventions

- **Errors.** Every function except `log.log` returns `result<T, error>`. Each interface defines its own
  `record error { code: string, message: string }`. `code` is a stable string, the same codes the Deno SDK uses; it
  never contains host paths or driver text. Match on `code`, show `message` only in logs.
- **`unavailable`.** Code that runs outside a task (start-up code while the node pre-creates an instance) gets
  `unavailable` from every function, and `log` drops the line.
- **Bytes.** KV keys and values, queue bodies and HTTP bodies are `list<u8>`. Blob keys are `string`.
- **Buffered bodies.** In this version blob contents and the bodies of `call` and `platform` are passed as whole byte
  lists. Responses are capped at `max_response_buffered_bytes` (10 MB by default). Streaming through `wasi:io` is a
  planned follow-up.
- **Clock.** Every `std:host` call that had to wait is an I/O boundary: the frozen `wasi:clocks` catch up with real
  time when it returns.
- **Data scope.** KV, SQL and blob data belong to the service; no other service can read them.

## `log`

```text
enum level { debug, info, warn, error }
log: func(level: level, message: string);
```

Appends a record to the service's logs, tagged with the task id. It counts against the task's 1,000 log lines (later
lines are dropped and counted); a message is cut at 10,000 characters. Lines written to `wasi:cli/stdout` and
`stderr` are logged too, as `info` and `error`.

## `kv`

| Type | Definition |
|---|---|
| `value` | `variant { json(string), bytes(list<u8>) }`: JSON text, or raw bytes |
| `set-options` | `{ ttl-seconds: option<u64> }` |
| `list-options` | `{ prefix: list<u8>, cursor: option<list<u8>>, limit: u32 }` |
| `list-page` | `{ entries: list<tuple<list<u8>, value>>, next-cursor: option<list<u8>> }` |

| Function | Result |
|---|---|
| `get(key)` | `option<value>`: `none` when missing or expired |
| `set(key, value, opts)` | writes the value; with `ttl-seconds` the key expires after that many seconds |
| `delete(key)` | `bool`: whether the key existed |
| `list(opts)` | one page of entries in lexicographic key order; pass `next-cursor` back as `cursor`; `limit` is clamped to 1 to 1,000 |
| `incr(key, delta, opts)` | `s64`: the new value of the counter, see below |

`incr` adds `delta` (an `s64`, may be negative) to an integer counter atomically: the read and the write run in one
transaction on the service's single writer, so concurrent increments never lose an update. A missing or expired key
starts at 0 and gets `opts.ttl-seconds`; an existing counter keeps its expiry, which gives fixed windows. The counter
is stored as JSON text, so `get` returns `json("<n>")`.

Limits: keys are non-empty and at most 2 KB, values at most 1 MB.

| Code | When |
|---|---|
| `validation_failed` | empty key |
| `kv_key_too_large` | key or list prefix over 2 KB |
| `kv_value_too_large` | value over 1 MB |
| `storage_quota` | the service's storage quota is used up |
| `kv_not_integer` | `incr` on a value that is not a JSON integer in the `s64` range (the value is unchanged) |
| `kv_overflow` | `incr` whose result would leave the `s64` range |

## `sql`

| Type | Definition |
|---|---|
| `sql-value` | `variant { null, integer(s64), real(f64), text(string), blob(list<u8>) }` |
| `statement` | `{ sql: string, args: list<sql-value> }`, with `?` placeholders |
| `sql-result` | `{ columns: list<string>, rows: list<list<sql-value>>, rows-affected: u64, last-insert-rowid: option<s64> }` |

| Function | Result |
|---|---|
| `execute(stmt)` | one `sql-result`. Read-only statements run on read connections, writes on the service's single writer |
| `batch(stmts, write)` | one result per statement, all in one transaction. `write = false` opens a read-only transaction: a statement that writes fails |

There is no interactive transaction in WIT; put statements that must commit together in one `batch`.

Limits: 1,000 ms per statement by default (`storage.sql_budget_ms`), at most 10,000 rows per statement and 10 MB of
result values per call, statement text up to 1 MB, up to 10,000 bound parameters. The authorizer refuses `ATTACH`,
`load_extension`, the runtime's `_rt_*` tables and pragmas that change durability, limits or file layout.

| Code | When |
|---|---|
| `sql_error` | syntax error, constraint violation, a write in a read-only batch |
| `sql_timeout` | a statement ran over its budget |
| `sql_not_authorized` | refused by the authorizer |
| `sql_result_too_large` | over 10,000 rows or 10 MB of results |
| `storage_quota` | the service's storage quota is used up |

## `blob`

| Type | Definition |
|---|---|
| `meta` | `{ key: string, size: u64, content-type: option<string> }` |
| `list-page` | `{ items: list<meta>, next-cursor: option<string> }` |

| Function | Result |
|---|---|
| `get(key)` | `option<tuple<meta, list<u8>>>`: the whole object, read into guest memory |
| `head(key)` | `option<meta>` |
| `put(key, content-type, data)` | writes (or replaces) the object; returns its `meta` |
| `delete(key)` | `bool`: whether the object existed |
| `list(prefix, cursor, limit)` | one page of objects; `limit` is clamped to 1 to 1,000 |

Limits: keys are non-empty strings of at most 1 KB; an object is at most 100 MB, but `get` returns at most 10 MB and
everything you hold also counts against the instance's linear memory limit (128 MB by default).

| Code | When |
|---|---|
| `validation_failed` | empty key |
| `blob_key_too_large` | key or prefix over 1 KB |
| `blob_too_large` | `get` of an object larger than the buffered response limit |
| `storage_quota` | the object is over 100 MB, or the storage quota is used up |

## `queue`

| Type | Definition |
|---|---|
| `send-options` | `{ delay-seconds: option<u64>, dedup-id: option<string> }` |

`enqueue(slug, body, opts) -> result<string, error>` puts `body` (at most 1 MB) on the queue of the service `slug`
in the same tenant, the calling service included, and returns the message id (`msg_…`). `delay-seconds` (at most
7 days) delays delivery; a second message with the same `dedup-id` within 24 hours is dropped.

The consumer receives `body` parsed as JSON, or as a string when it is not valid JSON, so send JSON bytes. The
target's `queue` trigger must be on, its plan must allow queues, and its `call.allowed_callers` (or, when that list
is empty, its environment) must admit the caller.

| Code | When |
|---|---|
| `not_found` | no service with that slug |
| `cross_tenant_call` | another tenant's service, and the plan does not allow cross-tenant calls |
| `forbidden` | the target's `queue` trigger is off |
| `trigger_not_allowed` | the target's plan does not allow queues |
| `call_not_allowed` | the target's caller rules do not admit the caller |
| `queue_full` | the target's queue is at the plan's `max_queue_depth` |
| `call_failed` | the message was refused (over 1 MB, delay over 7 days) or could not be stored |

For `queue` errors, `message` repeats the code.

## `call`

| Type | Definition |
|---|---|
| `headers` | `list<tuple<string, list<u8>>>`: names and byte values, as in `wasi:http` fields |
| `request` | `{ method: string, path-with-query: string, headers: headers, body: list<u8> }` |
| `response` | `{ status: u16, headers: headers, body: list<u8> }` |

`call(slug, req) -> result<response, error>` runs the service `slug` of the same tenant in-process (no HTTP hop), as
a child task with trigger `call`. `path-with-query` must start with `/`; it is the target's own path, without any
`/s/{slug}` prefix. Your `x-rt-*` headers are not passed on.

- The child's wall limit is the smaller of its own and your remaining time.
- A child that fails is a `5xx` `response` with `x-rt-reason`, not an `error`.
- Past call depth 15 the result is a `508` response with `x-rt-reason: call_depth`.
- The response body is buffered, at most `max_response_buffered_bytes` (10 MB).

| Code | When |
|---|---|
| `not_found` | no service with that slug |
| `cross_tenant_call` | another tenant's service, and the plan does not allow cross-tenant calls |
| `forbidden` | the target's `call` trigger is off |
| `call_not_allowed` | the target's `call.allowed_callers` (or environment) does not admit the caller |
| `trigger_not_allowed` | the target's plan does not allow calls |
| `invalid_request` | a method, path or header that is not valid HTTP |
| `response_too_large` | the response body is over the buffered limit |
| `call_failed` | the call could not be made |
| scheduler codes (`overloaded`, …) | the node refused to start the child task |

`call-rpc(slug, method, args-json) -> result<string, error>` is typed RPC over the same path (`services.call` in Deno):
it runs the callee's method `method` (a dotted path such as `invoices.create`) with the JSON argument and returns its
JSON result, each at most 256 KiB. Errors are the callee's own `{code, message}` or the host's: `validation_failed`,
`method_not_found`, `response_too_large`, `invalid_response`, `call_depth`, `timeout` and the codes above. The callee
may be a Deno service with an `rpc` export or a component exporting [`rpc`](#rpc-export).

## `sdk`

```text
invoke: func(name: string, method: string, args-json: string) -> result<string, error>;
```

Calls `method` of the operator SDK `name` with the JSON argument and returns its JSON result (each at most 256 KiB). The
SDK runs in the runtime, never in your component: the call is bound to the release your grants select, so an SDK
update reaches you without a redeploy. Errors: the SDK's own codes, or `sdk_not_found`, `sdk_unavailable` (removed),
`sdk_not_granted`, `sdk_version_unavailable`, `method_not_found`, `validation_failed`, `call_depth`, `timeout`. See
[Use operator SDKs](../guides/use-sdks.md).

## `rpc` (export)

```text
record caller { service: string, slug: option<string>, project: option<string>, environment: option<string>,
                tenant: string, plan: option<string> }
handle: func(method: string, args-json: string, caller: option<caller>) -> result<string, error>;
```

A component of world `rpc-service` exports `rpc` to serve RPC: a call to it for `/__rt/rpc/<method>` runs `handle`
instead of the HTTP handler, with the caller the runtime stamped, and the host answers the caller with the JSON result
or the `{code, message}` error (`method_not_found` for an unknown method, by convention). A component without the
export answers such a call `method_not_found` without running any code.

## `platform`

```text
use call.{request, response};
fetch: func(binding: string, req: request) -> result<response, error>;
```

Calls the operator binding `binding` (`[[binding]]` in the node configuration), from an operator SDK service only: a
tenant's component gets `403 binding_not_allowed`. The node uses the request's method,
path, query, headers (minus `authorization`, `cookie` and `x-rt-*`) and body, sends it to the binding's `base_url`
with the operator's credentials, and returns the answer. The credentials never enter guest memory.

A refusal is a `response` with an `x-rt-reason` header, never an `error`:

| Status | `x-rt-reason` | When |
|---|---|---|
| 403 | `binding_denied` | method or path not allowed by the binding, or a `..` segment |
| 403 | `binding_not_allowed` | the tenant's plan may not use the binding |
| 404 | `binding_not_found` | no binding with that name |
| 429 | `rate_limited` | the binding's per-tenant rate limit |
| 502 | `binding_upstream` | the upstream failed |

| Code | When |
|---|---|
| `invalid_request` | a method, path or header that is not valid HTTP |
| `response_too_large` | the response body is over the buffered limit |
| `unavailable` | called outside a task |

See [Operator SDKs and bindings](../guides/operator-sdks.md).

## Linked WASI interfaces

Besides `std:host`, a component may import these packages, at any `0.2.x` version:

| Package | What the node provides |
|---|---|
| `wasi:http` | the `incoming-handler` export is called per task; `outgoing-handler` sends requests through the egress guard |
| `wasi:io` | streams and polling (a blocking wait advances the frozen clock) |
| `wasi:clocks` | frozen monotonic and wall clocks, whole milliseconds |
| `wasi:random` | random numbers |
| `wasi:cli` | `environment` = the service's env and secrets; `stdout` → `info` logs, `stderr` → `error` logs; `stdin` empty; `exit` ends the task; `terminal-*` report no terminal |
| `wasi:filesystem` | linked with no preopened directories: nothing can be opened |
| `wasi:sockets` | linked with every address denied |

Any other import, a missing `wasi:http/incoming-handler` export, or the threads proposal makes the upload fail with
`422 invalid_artifact`.

## Using it from Rust

Generate bindings with [`wit-bindgen`](https://crates.io/crates/wit-bindgen) and take the `wasi:http` side from the
[`wasip2`](https://crates.io/crates/wasip2) crate. Build for `wasm32-wasip2`.

```toml title="Cargo.toml"
[lib]
crate-type = ["cdylib"]

[dependencies]
wasip2 = "2"
wit-bindgen = "0.62"
```

```rust title="src/lib.rs"
wit_bindgen::generate!({
    path: "wit",      // directory with rt-host.wit
    world: "guest",
});

use rt::host::{kv, log, queue};

fn remember(user: &str) -> Result<i64, kv::Error> {
    let key = format!("visits/{user}");
    let n = kv::incr(key.as_bytes(), 1, kv::SetOptions { ttl_seconds: None })?;
    if n == 1 {
        let opts = queue::SendOptions { delay_seconds: None, dedup_id: Some(format!("welcome:{user}")) };
        if let Err(e) = queue::enqueue("mailer", br#"{"kind":"welcome"}"#, &opts) {
            log::log(log::Level::Warn, &format!("enqueue: {}", e.code));
        }
    }
    Ok(n)
}
```

How WIT types appear in Rust:

| WIT | Rust |
|---|---|
| `string` | `&str` as a parameter, `String` in results |
| `list<u8>` | `&[u8]` as a parameter, `Vec<u8>` in results |
| `option<T>`, `result<T, E>` | `Option<T>`, `Result<T, E>` |
| `s64`, `u64`, `u32`, `u16`, `f64`, `bool` | `i64`, `u64`, `u32`, `u16`, `f64`, `bool` |
| `record` | a struct with snake_case fields (`kv::SetOptions { ttl_seconds }`); passed by reference when it holds strings or lists |
| `variant`, `enum` | a Rust enum (`kv::Value::Json(String)`, `log::Level::Info`) |
| `tuple<A, B>` | `(A, B)` |

The interfaces end up in the module `rt::host`. The [edge gateway](../tutorials/edge-gateway.md) sample
(`sample/apps/edge-gateway/gateway` in the runtime repository) uses `kv`, `call`, `queue`, `platform` and `log` this
way.

## Using it from JavaScript

With [`jco componentize`](https://github.com/bytecodealliance/jco), list the interfaces in your world and put
`rt-host.wit` under `wit/deps/`, next to the WASI packages your world refers to:

```text
wit/
├── world.wit
└── deps/
    ├── rt-host/rt-host.wit
    ├── wasi-http/package.wit
    ├── wasi-io/package.wit
    └── wasi-clocks/package.wit
```

```text title="wit/world.wit"
package example:app;

world app {
  import std:host/kv@0.1.0;
  import std:host/sql@0.1.0;
  import std:host/log@0.1.0;
  export wasi:http/incoming-handler@0.2.10;
}
```

Each imported interface is an ES module named after it:

```js
import { incr } from "std:host/kv@0.1.0";
import { execute } from "std:host/sql@0.1.0";
import { log } from "std:host/log@0.1.0";

const enc = new TextEncoder();

function countVisit(path) {
  const n = incr(enc.encode(`visits:${path}`), 1n, {});
  execute({ sql: "INSERT INTO visits (path) VALUES (?)", args: [{ tag: "text", val: path }] });
  log("info", `visit ${n} of ${path}`);
  return n;
}
```

How WIT types appear in JavaScript (jco's mapping):

| WIT | JavaScript |
|---|---|
| `string` | `string` |
| `list<u8>` | `Uint8Array` |
| `s64`, `u64` | `bigint` |
| `u32`, `u16`, `f64` | `number` |
| `option<T>` | `T` or `undefined` |
| `result<T, E>` | returns `T`; an `err` is thrown as an error whose `payload` is the `{ code, message }` record |
| `record` | an object with camelCase fields (`{ ttlSeconds }`, `{ pathWithQuery }`) |
| `variant` | `{ tag, val }` (`{ tag: "json", val: "42" }`) |
| `enum` | a string (`"info"`) |
| `tuple<A, B>` | `[A, B]` |

Imported functions are synchronous. Build with `jco componentize src/app.js --wit wit --world-name app --out app.wasm`.

## Checking a component

`wasm-tools` lists what a component imports and exports, which is what the node checks at deploy:

```console
$ wasm-tools component wit app.wasm | grep -E '^\s*(import|export)'
  import std:host/log@0.1.0;
  import std:host/kv@0.1.0;
  import wasi:io/poll@0.2.12;
  import wasi:http/types@0.2.10;
  ...
  export wasi:http/incoming-handler@0.2.10;
```

## Versioning

The package version is `std:host@0.1.0`. A component imports specific functions, and the node links them when it
builds the version: a component that imports a function the node does not provide (built against a newer copy of
the file) passes the deploy check, but its build fails (`21ci push --wait` reports it; a lazy build answers the first
request with `502 build_failed`). Keep your vendored copy in sync with the
node you deploy to; the edge gateway's `build.sh` warns when its copy differs from the runtime's.
