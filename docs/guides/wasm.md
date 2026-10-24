---
title: Writing WASM services
description: The component contract of the WASM executor: what a component exports and may import, the request and response rules, instance lifecycle, clocks, outbound HTTP, limits and troubleshooting.
---

# Writing WASM services

This guide is the contract and the platform behavior of the WASM executor: what a component must export, what it may
import, what a task sees, how instances live and die, and what the sandbox allows. How to **build** a component in a
given language (Rust, JavaScript, Python, Go, C and C++, .NET) is in the [WASM runtime](../languages/wasm.md) page, and
how to use the built-in storage, queue and call interfaces is in [KV, SQL, blobs and other
sub-services](sub-services.md).

<span class="rt-badge rt-badge--wasm">WASM</span> A WASM service is a single `.wasm` file: a WASI Preview 2
**component** that serves HTTP. The node compiles it to native code once and gives every task its own instance with its
own linear memory. Choose WASM when you want a compiled language, a small and strictly sandboxed unit, or code that does
not depend on a JavaScript engine; choose [Deno](deno.md) when you want TypeScript with npm packages and the
streaming storage APIs ([Execution](../concepts/execution.md) compares the two executors).

## The component contract

| Requirement | Detail |
|---|---|
| Format | A WebAssembly **component** (not a core module), WASI Preview 2 |
| Export | `wasi:http/incoming-handler`, any `0.2.x` version (the `wasi:http/proxy` world) |
| Imports | only the interfaces in the table below, any `0.2.x` WASI version |
| Not allowed | the threads proposal and shared memory |
| Upload | `Content-Type: application/wasm`, at most 50 MB, to a service created with `runtime: "wasm"` |

What the node links, and how:

| Interface | Behavior |
|---|---|
| `wasi:http/incoming-handler` (export) | called once per task with the request |
| `wasi:http/outgoing-handler`, `wasi:http/types` | outbound HTTP through the egress guard, see [Outbound HTTP](#outbound-http) |
| `wasi:io/*` | streams and polling |
| `wasi:clocks/*` | frozen clocks, whole milliseconds, see [Clocks](#clocks) |
| `wasi:random/*` | random numbers |
| `wasi:cli/*` | environment = your env and secrets; stdout and stderr go to the logs; stdin is empty; `exit` ends the task; `terminal-*` report "no terminal" |
| `wasi:filesystem/*` | linked, but there are no preopened directories: nothing can be opened |
| `wasi:sockets/*` | linked, but every address is denied |
| `std:host/*` | the runtime's own capabilities: `log`, `kv`, `sql`, `blob`, `queue`, `call` (also typed RPC, `call-rpc`), `sdk` (operator SDKs), `secrets`, `lock`, `schedule`, `platform` (operator SDK services only); a component of world `rpc-service` also exports `rpc` ([WIT reference](../reference/wit.md)) |

A component that breaks any of these rules is refused at deploy with `422 invalid_artifact`, see
[Troubleshooting](#troubleshooting).

!!! note "Not the Deno contract"
    A WASM service has no `export default`, no `ctx`, no `std:*` / `sdk:*` modules and no `Deno.*`, even when it is
    written in JavaScript. Everything arrives through the HTTP request and the `std:host` imports.

## Build a component

The node does not build WASM: your language's toolchain does, and you upload the result. [WASM runtime](../languages/wasm.md)
has a minimal project, the requirements and the limits per language, and a script that checks a component against the
contract before you push. `21ci init hello-rs --template wasm-rust` (or `--template wasm-js`) scaffolds a working
starter from the runtime's samples.

Whatever the language, the shape of a handler is the same: read the request, hand over the response **head** first,
stream the **body**, and **finish** it. In Rust with the [`wasip2`](https://crates.io/crates/wasip2) crate:

```rust title="src/lib.rs"
use wasip2::exports::http::incoming_handler::Guest;
use wasip2::http::types::{Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};

struct Component;
wasip2::http::proxy::export!(Component);

impl Guest for Component {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        // 1. Read the request. The /s/{slug} prefix is already stripped from the path.
        let path = request.path_with_query().unwrap_or_else(|| "/".to_string());

        // 2. Build the head and hand it over: the client gets the status and headers now.
        let headers = Fields::new();
        headers.set("content-type", &[b"text/plain".to_vec()]).expect("header");
        let response = OutgoingResponse::new(headers);
        response.set_status_code(200).expect("status");
        let body = response.body().expect("body");
        ResponseOutparam::set(out, Ok(response));

        // 3. Stream the body, then finish it. A body that is never finished ends the response with an error.
        let stream = body.write().expect("stream");
        stream.blocking_write_and_flush(format!("hello from {path}\n").as_bytes()).expect("write");
        drop(stream);
        OutgoingBody::finish(body, None).expect("finish");
    }
}
```

A `cdylib` crate with `wasip2 = "2"` built for `wasm32-wasip2` is already a component; no other tool is needed. An
ordinary `std` crate is fine: the node links every `wasi:cli` interface, including `stdin`, `exit` and `terminal-*`,
so `println!`, `std::env` and `SystemTime` work.

## Deploy

=== "21ci"

    ```console
    $ 21ci push target/wasm32-wasip2/release/hello_min.wasm --service hello-min --create --wait
    ```

    A `.wasm` path creates a `runtime: "wasm"` service and uploads the file as `application/wasm`.

=== "curl"

    ```bash
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"hello-min","runtime":"wasm"}' "$RT_API/v1/services" | jq -r .id)

    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/wasm' \
      --data-binary @target/wasm32-wasip2/release/hello_min.wasm \
      "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    ```

```bash
21ci invoke hello-min '/?name=Ferris'
```

The upload is validated synchronously (it is a component, it exports the handler, its imports are allowed). The build
compiles it to native code on the first request (`lazy`) or right away (`--wait`, `compile=eager`) and keeps the
compiled artifact, so restarts with the same runtime version do not compile again. The whole loop, including rollback,
is in [Deploy and roll back](deploy.md).

## What the guest sees

The request your `incoming-handler` receives:

- **Path.** `path-with-query` has the `/s/{slug}` prefix stripped: `GET /s/hello-min/x?name=Ferris` arrives as
  `/x?name=Ferris`. The prefix is in `x-rt-base-path`. The client's `Host` header is not passed on; the authority
  is `localhost`.
- **Headers set by the runtime:** `x-rt-task-id`, `x-rt-trigger` (`http`, `call`, `interval`, `cron`, `queue`),
  `x-rt-deadline` (epoch milliseconds when the wall limit runs out), and for HTTP requests also `x-rt-base-path`,
  `x-rt-client-ip` and `traceparent`. Any `x-rt-*` header a client sends is removed first, so these can be trusted.
- **Client address.** `x-rt-client-ip` is the only way a component learns who called it: the TCP peer, or, behind a
  proxy listed in [`ingress.trusted_proxies`](../config.md#ingress), the first `X-Forwarded-For` hop,
  counted from the right, that is not a trusted proxy. It is set for the `http` trigger only.
- **`Authorization`** reaches the component unchanged, except an `Authorization: Bearer rtk_…` (a tenant API key),
  which is always removed. The other header rules are the same as for Deno, see
  [Writing Deno services](deno.md#what-the-handler-sees).
- **Body.** The request body arrives through `wasi:io` streams, up to `limits.max_body_bytes` (10 MB by default).

The response is streamed as you write it: the head goes out when you call `ResponseOutparam::set`, then each chunk.
The body may be up to 100 MB (`max_response_stream_bytes`). Always finish the outgoing body
(`OutgoingBody::finish`): a body you return without finishing, a trap or a kill after the head ends the response with
an error. The runtime adds `x-rt-task-id`, `x-rt-version` and `server-timing` to the response.

The client's response ends when `handle` returns. A response with a `content-length` is complete for the client as soon
as its last byte is written, so work you do after that (an audit event, a queue message) does not delay it, as long as
it stays within the wall limit.

## Using `std:host`

Storage, queues, calls and operator bindings are WIT interfaces in the package `std:host@0.1.0`, shipped as
`wit/rt-host.wit` in the runtime repository. Copy that file into your project and generate bindings from it with your
language's tooling (`wit-bindgen` for Rust, `jco` for JavaScript, see [WASM runtime](../languages/wasm.md)). Only the
interfaces your code uses end up in the component's import list.

| Interface | Gives you | Guide |
|---|---|---|
| `log` | a log record at an explicit level | [Logs](#logs) |
| `kv` | get, set, delete, list, atomic `incr` with TTL | [KV](sub-services.md#kv) |
| `sql` | `execute` and `batch` over a private SQLite database | [SQL](sub-services.md#sql) |
| `blob` | whole objects up to 100 MB (buffered, `get` up to 10 MB) | [Blobs](sub-services.md#blobs) |
| `queue` | `enqueue` to a service the component may call (same tenant by default) | [Queues](sub-services.md#queues) |
| `call` | an in-process call to a service of the same tenant | [Service calls](sub-services.md#service-calls) |
| `sdk` | an operator SDK, run by the runtime: `invoke(name, method, args-json)` | [Use operator SDKs](use-sdks.md) |
| `platform` | an operator binding, credentials added by the node (operator SDK services only) | [Operator SDKs and bindings](operator-sdks.md) |

The rules that hold for all of them:

- Every function except `log.log` returns `result<T, error>`, where `error` is a record `{ code, message }` with the same
  stable codes as the Deno SDK (`storage_quota`, `kv_value_too_large`, `not_found`, ...). Match on `code`.
- Bodies of `blob`, `call` and `platform` are whole byte lists in this version, not streams. They count against the
  instance's memory limit, and a response over 10 MB is `response_too_large` (`blob_too_large` for `blob.get`).
- There is no interactive SQL transaction; statements that must commit together go into one `sql.batch`.
- Calls made outside a task return the code `unavailable`, see [Instance lifecycle](#instance-lifecycle).

[KV, SQL, blobs and other sub-services](sub-services.md) has a complete Rust example for each interface, with the crate
layout and the `wit_bindgen::generate!` setup. For a complete program, read `sample/apps/edge-gateway/gateway` in the
runtime repository, a JWT gateway that uses `kv` for rate limits, `call` to forward to backends, `queue` and `sdk`
for an audit trail, and `log`. It is walked through in the [edge gateway tutorial](../tutorials/edge-gateway.md).

## Environment and secrets

Your service's env variables and secrets are the component's WASI environment: `std::env::var("API_KEY")` in Rust,
`wasi:cli/environment` in general. Set them with `21ci env set` / `21ci secret set` or the env and secrets endpoints,
exactly as for Deno (see [Writing Deno services](deno.md#environment-variables-and-secrets)). Changes apply to the next
task. Because language runtimes cache the environment, a reused instance whose env has changed (a rotated secret) is
discarded and the next task gets a fresh one.

## Logs

- Each line written to **stdout** becomes an `info` record, each line on **stderr** an `error` record, tagged with
  the task id. A partial line longer than 64 KB is flushed as a line of its own.
- `std:host/log` writes a record at an explicit level (`debug`, `info`, `warn`, `error`).
- A task keeps up to 1,000 lines; later ones are dropped and counted.

```bash
21ci logs hello-min -f
21ci logs hello-min --task task_0000000000000002
```

Reading, filtering and following logs is covered in [Logs, stats and usage](logs-and-usage.md).

## Schedules and queues

A component has only one entry point, so schedule fires and queue batches arrive as synthetic HTTP requests, the same
ones a Deno Form A handler gets:

| Trigger | Request | Your answer |
|---|---|---|
| `interval`, `cron` | `POST /__rt/scheduled`, JSON `{"scheduleId", "scheduledTime", "payload"}` | the status is logged |
| `queue` | `POST /__rt/queue`, JSON `{"messages": [{"id", "body", "attempt"}]}` | `2xx` acks the batch, anything else retries all of it |

Decide by the `x-rt-trigger` header (`interval`, `cron`, `queue`), which only the runtime can set, not by the path:
a client can send `POST /__rt/queue` too.

```rust
let trigger = header(&request, "x-rt-trigger");   // read from request.headers()
match trigger.as_deref() {
    Some("queue") => handle_batch(request, out),
    Some("interval") | Some("cron") => run_job(request, out),
    _ => serve_http(request, out),
}
```

To retry only some messages of a batch, answer `2xx` with an `x-rt-queue-retry` header listing their ids,
comma-separated, each optionally followed by `;delay=<seconds>` (for example `msg_01…;delay=30,msg_01…`). Every other
message of the batch is acked. Without a delay the usual backoff applies. A runnable Rust consumer and scheduled
handler are in [Queues](sub-services.md#queues) and [Schedules](sub-services.md#schedules); the delivery rules are in
[Calls and queues](calls-and-queues.md) and [Schedules](schedules.md).

## Instance lifecycle

By default every task gets a **fresh instance**: new linear memory, globals at their initial values. The node may
create it ahead of time (a warm pool), but never reuses one that has served a task. A static counter therefore reads
`1` on every request. The cost of a fresh instance depends on the component: instantiation maps the component's memory
image, then your language runtime and your start-up code run. A component with a large runtime or heavy start-up code
pays that on every task; see [Optimize your service](optimize.md#wasm-size-and-start-up-tips).

With `wasm.reuse_instances` on (the default only in the `min_latency` profile), an instance that finished a task
cleanly serves the next task of the same service version on the same processor, so linear memory survives between
requests. Turn it on for one service:

=== "21ci"

    ```bash
    21ci service update hello-min --reuse-instances true
    ```

=== "curl"

    ```bash
    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"wasm": {"reuse_instances": true}}' "$RT_API/v1/services/$SVC"
    ```

A reused instance is dropped after a trap, a kill or a timeout, after 1,000 tasks, when it sits idle past the
service's idle TTL, under memory pressure and when the env changes. It is never shared between services, tenants or
versions, so a new version always starts with new instances. As with Deno, treat memory that survives as a cache: keep state in `std:host/kv`,
`sql` or `blob`, and make sure your code is correct when every request starts fresh. Rules for reuse-safe code are in
[Optimize your service](optimize.md#write-reuse-safe-code).

Start-up code that runs while the node pre-creates an instance (before any task) cannot use `std:host`: every call
returns the code `unavailable` there. Do your `std:host` calls inside `handle`.

## Clocks

`wasi:clocks` follow the frozen-clock rule: the monotonic and wall clocks advance only when a host call or a
`wasi:io/poll` wait returns **after actually waiting** (a fetch response arrived, a timer fired, a store call
completed), never while your code computes. Readings are whole milliseconds. A busy loop measures zero elapsed time.

On a reused instance the wall clock is re-anchored to real time when the next task starts, so timestamps stay
correct; the monotonic clock does not count the idle gap between tasks.

## Outbound HTTP

`wasi:http/outgoing-handler` goes through the same egress guard as Deno's `fetch`: public addresses only, mail ports
denied, the service's `egress` rules, at most 50 requests per HTTP task (200 for schedules and queues) and 6 at once.
Refusals surface as `wasi:http` error codes:

| Egress result | `wasi:http` `error-code` |
|---|---|
| `egress_denied` | `HTTP-request-denied` |
| `egress_limit` | `connection-limit-reached` |
| `egress_too_large` | `HTTP-response-body-size` |
| `egress_timeout` | `connection-timeout` |
| `egress_dns` | `destination-not-found` |
| `egress_quota`, `egress_upstream`, `egress_insecure_redirect` | `internal-error` with the code as its message |

## Limits

| Limit | Default | When exceeded |
|---|---|---|
| Linear memory (all memories of the instance) | 128 MB | the growth fails, the task ends with `502 oom` |
| CPU time | 1,000 ms (HTTP, calls), 10,000 ms (schedules, queues) | `502 cpu_limit` |
| Wall time | 30 s (HTTP; a call at most the caller's remaining time), 300 s (schedules, queues) | `504 timeout` |
| Request body | 10 MB (HTTP, calls), 1 MB per queue message | `413 too_large` |
| Response body (streamed) | 100 MB | the body ends with an error |
| Buffered `std:host` bodies (`blob.get`, `call`, `platform`) | 10 MB | `blob_too_large` / `response_too_large` |
| Component size | 50 MB | `413 too_large` at deploy |

These defaults depend on the trigger, not on the profile. The plan's `limits_max` caps them: on the `free` plan
(30 s wall, 1,000 ms CPU) scheduled and queue tasks also get 30 s and 1 s. A trap (a Rust panic, `unreachable`) is
`500 guest_error`. The full table is in [Limits](../reference/limits.md); how to choose limits for a service is in
[Configure a service](configure.md) and [Optimize your service](optimize.md).

## Not supported

| Feature | Status |
|---|---|
| Threads, shared memory, atomics wait | refused at deploy |
| Sockets (`wasi:sockets`) | every address denied |
| Files (`wasi:filesystem`) | no preopened directories; use `std:host/blob` |
| Streaming bodies for `blob`, `call`, `platform` | buffered in `std:host@0.1.0`; streams through `wasi:io` are a follow-up |
| Pre-initialization at build time (component-init / Wizer run by the node) | <span class="rt-badge rt-badge--planned">planned</span> Pre-initialize with your toolchain instead (ComponentizeJS does) |
| `ctx.waitUntil` | not needed: finish the response body, and keep working inside `handle` until it returns (bounded by the wall limit). Set `content-length` so the client does not wait for it, as the edge gateway does for its audit trail |

## Troubleshooting

`422 invalid_artifact` at deploy means the upload is not something the node can run. The error message says which
rule failed:

| Message | Cause | Fix |
|---|---|---|
| `not a WebAssembly component (a core module was uploaded)` | a core module, for example from `wasm32-wasip1` or `wasm32-unknown-unknown` | build for `wasm32-wasip2`, or with a toolchain that emits WASI P2 components |
| `not a WebAssembly component` | not WebAssembly at all | check the file you upload |
| `invalid component (threads and shared memory are not allowed): …` | built with threads or shared memory | build without the threads feature |
| ``import `…` is not allowed`` | an interface outside the [linked set](#the-component-contract), or a WASI version other than `0.2.x` | remove the dependency that pulls it in |
| ``component does not export `wasi:http/incoming-handler` (any 0.2.x)`` | a command or library component | export the handler (`wasip2::http::proxy::export!`, a `fetch` listener in JS) |

`wasm-tools` shows what your component imports and exports before you deploy:

```bash
wasm-tools component wit target/wasm32-wasip2/release/hello_min.wasm | grep -E '^\s*(import|export)'
```

Other symptoms:

- **Deploys, then the first request is `502 build_failed`.** The node could not compile or link the component, for
  example because it imports an `std:host` function this node does not have (built against a newer copy of
  `rt-host.wit`). Check `21ci versions <svc> <n>` and `21ci build-logs <svc> <n>`; `21ci push --wait` reports it at
  deploy time.
- **`500 guest_error` on every request.** The component traps. Run it under `wasmtime serve` if it does not use
  `std:host`, and read the stderr lines in the service logs.
- **`std:host` calls fail with `unavailable`.** They ran during instance pre-creation, outside a task. Move them into
  the handler.
- **`502 oom` on large objects.** `blob.get`, `call` and `platform` bodies are held in linear memory. Check the size
  first, or move the work to a [Deno](deno.md) service, which streams.

## See also

- [WASM runtime](../languages/wasm.md): toolchains and a minimal project for each language, with their limits.
- [KV, SQL, blobs and other sub-services](sub-services.md): every `std:host` interface with a Rust example.
- [WIT reference](../reference/wit.md): every `std:host` interface, type and error code.
- [Edge gateway tutorial](../tutorials/edge-gateway.md): a production-style Rust component.
- [Execution](../concepts/execution.md): how instances, pools and limits work inside the node.
- [Writing Deno services](deno.md): the TypeScript side, for services that need npm packages.
