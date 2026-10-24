---
title: Writing Deno services
description: How to write, structure, test and debug TypeScript and JavaScript services for the Deno executor.
---

# Writing Deno services

This guide is the handler contract and platform API of the Deno executor: project layout, the handler shapes, the
request and response contract, the sandbox and its limits, warm reuse, and how to test and debug. The built-in storage
and messaging modules (`std:kv`, `std:sql`, ...) have their own guide, [KV, SQL, blobs and other
sub-services](sub-services.md); npm packages, frameworks and import rules are in the [Deno runtime](../languages/deno.md)
language page. For exact signatures, see the [guest API reference](../reference/guest-api.md).

<span class="rt-badge rt-badge--deno">Deno</span> A Deno service is a module whose default export handles requests.
The node compiles it on first use, then runs every task in a V8 isolate that has the Web platform APIs, the runtime
modules and nothing else: no file system, no sockets, no subprocesses.

## How your code runs

1. You upload source (`21ci push` or `POST /v1/services/{id}/versions`). The node validates it right away: it parses
   every file and checks every import. Nothing is fetched yet.
2. The first task that needs the version builds it: TypeScript is transpiled (no type-check), `npm:` / `jsr:` /
   `https:` dependencies are fetched and pinned in `rt.lock`, and everything is bundled with a V8 code cache.
3. Each task gets a fresh isolate from a pre-built snapshot, loads your bundle and calls your handler. **Your module's
   top-level code runs once per task**, so module variables never carry state from one request to the next (unless
   [warm reuse](#warm-reuse) is on, and even then only as a cache).

The mechanism is described in [Execution](../concepts/execution.md); this page is about writing the code.

## Project layout

The smallest service is one file. Larger services are a directory with an entry module and an optional `rt.json`.

| What you push | Sent as | Limits |
|---|---|---|
| `main.ts`, `main.js`, `x.mjs` | the file (`application/typescript` / `text/javascript`) | ≤ 1 MB |
| a directory with `rt.json` | JSON bundle `{"entry", "files", "imports"}` | ≤ 10 MB, ≤ 1,000 files, ≤ 1 MB per file |
| a directory without `rt.json` | one file as is; several as a bundle with the entry guessed from `main.ts`, `main.js`, `index.ts`, `index.js`, `mod.ts` | same |
| `x.zip`, `x.tar` | the archive (optional `rt.json` inside) | same |

`rt.json` names the entry module and an import map. Both fields are optional:

```json title="rt.json"
--8<-- "sample/deno/npm-zod/rt.json"
```

`21ci push` bundles the `.ts .tsx .mts .js .jsx .mjs .json` files below the directory and leaves out dot files,
`node_modules` and `rt.json` itself. See [`21ci push`](../cli.md#push) for every option.

### Start from a template

`21ci init` scaffolds a project from the runtime's samples:

| Template | Contents |
|---|---|
| `deno` (default) | `main.ts` from `sample/deno/hello`: a Form A handler that echoes the request and `ctx` |
| `deno-kv` | `main.ts` from `sample/deno/kv-counter`: `std:kv` get, an atomic counter with a TTL, delete, list |
| `wasm-rust`, `wasm-js` | WASM components, see [Writing WASM services](wasm.md) |

```console
$ 21ci init my-fn --template deno-kv
Created my-fn (template deno-kv)
  README.md
  main.ts
Next: cd my-fn && 21ci push . --service my-fn --create
```

### A layout that scales

The complete apps in `sample/apps/` (in the runtime repository) all use the same structure, and it is worth copying:

```text
my-service/
├── src/               what you push: 21ci push src --service my-service
│   ├── main.ts        the handler: routing, std:* calls, nothing else
│   ├── validate.ts    pure functions: parsing, validation, business rules
│   ├── http.ts        small helpers (json(), error(), readJson())
│   └── rt.json        only if you need an import map or another entry
├── test/
│   └── validate.test.ts
└── smoke.sh           deploys src/ to a node and checks the main flows
```

Only `main.ts` imports `std:*` modules. Everything else is plain TypeScript that runs anywhere, which is what makes it
[testable](#testing) without a node. Push `src/`, not the project root: the node validates **every** file of an
upload, including files nothing imports, so a test file with a `node:test` import in the upload fails the deploy
with `422`.

## Handlers

The entry module's `export default` is either a function (Form A) or an object (Form B).

### Form A: a function

The simplest shape. It receives a standard Web `Request` and a `ctx` object and returns a `Response`:

```ts title="sample/deno/hello/main.ts"
--8<-- "sample/deno/hello/main.ts"
```

A Form A handler also receives schedule fires and queue batches, as synthetic `POST` requests (see the table below).

### Form B: an object

Export an object with any of `fetch`, `scheduled` and `queue` (at least one):

```ts title="main.ts"
export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    return Response.json({ trigger: ctx.trigger });
  },
  async scheduled(event: ScheduledEvent, ctx: Ctx): Promise<void> {
    console.log("fired", event.scheduleId, event.payload);
  },
  async queue(batch: QueueBatch, ctx: Ctx): Promise<void> {
    for (const msg of batch.messages) console.log(msg.id, msg.attempt, msg.body);
  },
};
```

Which function the runtime calls depends on the trigger:

| Trigger | Called | Without that function |
|---|---|---|
| `http`, `call` | `fetch(req, ctx)` (Form A: the function) | `500 guest_error`, logged as `module has no default handler` |
| `interval`, `cron` | `scheduled(event, ctx)` | `fetch` receives `POST http://localhost/__rt/scheduled` with JSON `{scheduleId, scheduledTime, payload}`; the status is only logged |
| `queue` | `queue(batch, ctx)` | `fetch` receives `POST http://localhost/__rt/queue` with JSON `{messages: [{id, body, attempt}]}`; `2xx` acks the whole batch, anything else retries it |

The synthetic requests carry `content-type: application/json` and `x-rt-trigger` (`interval`, `cron` or `queue`).

!!! warning "Check the trigger, not the path"
    Anyone who can reach your service can also send `POST /__rt/queue` over HTTP. In a Form A handler, decide by
    `ctx.trigger` (or the `x-rt-trigger` header, which clients cannot set), not by the path alone. Form B avoids
    the question: `scheduled` and `queue` are only ever called by the runtime.

A real `queue` handler from `sample/deno/scheduled-queue` acks or retries each message on its own:

```ts title="sample/deno/scheduled-queue/main.ts (excerpt)"
--8<-- "sample/deno/scheduled-queue/main.ts:104:125"
```

Queue rules in short: a message you neither `ack()` nor `retry()` is acked when the handler returns normally; a
throw fails the task and the **whole batch** comes back. Without a delay, retries back off `2 s × 2^(attempt-1)`
(±20 %), and after `queue.max_attempts` (5 by default) the message moves to the dead-letter queue. Delivery is
at-least-once and unordered, so make the work idempotent. Calling `ack()` first turns a later `retry()` of the same
message into a no-op. Schedules and queues have their own guides: [Schedules](schedules.md) and
[Calls and queues](calls-and-queues.md).

### The `ctx` object and types

The runtime ships no types package. Declare the shapes you use in your own code:

```ts title="types.ts"
export interface Ctx {
  taskId: string;                                         // same as the x-rt-task-id response header
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  invoker?: string;                                       // x-rt-invoker, internal listener only
  client?: { ip: string };                                // http trigger only: the client address
  deadline: number;                                       // epoch ms when the wall limit runs out
  waitUntil(p: Promise<unknown>): void;
}
export interface ScheduledEvent { scheduleId: string; scheduledTime: number; payload: unknown }
export interface QueueMessage {
  id: string;
  body: unknown;                                          // parsed JSON, or a string if it is not JSON
  attempt: number;                                        // 1 on the first delivery
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}
export interface QueueBatch { messages: QueueMessage[] }
```

The [guest API reference](../reference/guest-api.md#type-declarations) also has ambient declarations for the `std:*`
modules, so a TypeScript-aware editor can check your calls. The build itself never type-checks.

`ctx.waitUntil(promise)` keeps the task alive after the response has been sent, for work the client does not need to
wait for. It is bounded by the wall limit, and a rejected promise is logged as an error:

```ts title="sample/deno/service-call/main.ts (excerpt)"
--8<-- "sample/deno/service-call/main.ts:62:72"
```

## Requests and responses

### What the handler sees

- **URL.** `req.url` is `http://localhost/<rest>?<query>`. For `GET /s/hello/x?name=Ada` the handler sees
  `http://localhost/x?name=Ada`. The stripped prefix is in the `x-rt-base-path` header (`/s/hello`); on a custom
  domain there is no prefix and no header. Build absolute links from `x-rt-base-path` if you serve HTML.
- **Headers added by the runtime:** `x-rt-trigger` (`http`, `call`, …), `x-rt-task-id`, `x-rt-base-path`,
  `x-rt-client-ip`, `traceparent` (the caller's, or a new one), and `x-rt-invoker` on the internal listener only.
- **Headers removed:** every `x-rt-*` header the client sent (a client cannot spoof them, `x-rt-key` included), the
  `Host` header and hop-by-hop headers.
- **`Authorization`** reaches you unchanged, so you can check your own tokens (an admin bearer, `Basic …`), with one
  exception: an `Authorization: Bearer rtk_…` (a tenant API key, a runtime credential) is always removed, and so is the
  root token that passed the access check of an `access: key` service. Clients of an `access: key` service that also
  need to send your own token put the invoke key in `x-rt-key` instead.
- **Client address.** `ctx.client.ip` and the `x-rt-client-ip` header hold the address the node resolved: the TCP
  peer, or, behind a proxy listed in [`ingress.trusted_proxies`](../config.md#ingress), the first
  `X-Forwarded-For` hop, counted from the right, that is not a trusted proxy. A client-sent `x-rt-client-ip` is removed first. Both are set
  for the `http` trigger only: schedules, queues and calls have no client address, and neither do requests on the
  internal Unix socket.
- **Body.** `req.body` is a `ReadableStream` that you can read while the client is still sending. Reading past
  `limits.max_body_bytes` (10 MB by default) fails with an error whose message is `request_too_large`. A request
  whose `Content-Length` is already too large never reaches you: the client gets `413 too_large`.

### What the handler returns

Return a `Response`. Anything else (`undefined`, a plain object, a string) is a `TypeError` and the client gets
`500 guest_error`.

- A streamed body is sent as you produce it: the client gets the first chunk right away. Chunks must be `Uint8Array`.
- A response body may be up to 100 MB (`max_response_stream_bytes`). Past that the body is cut off with an error.
- Several `Set-Cookie` headers are kept as separate headers.
- The runtime adds `x-rt-task-id`, `x-rt-version` and `server-timing` to your response.

A streamed response:

```ts title="main.ts"
export default function (): Response {
  const enc = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      for (let i = 1; i <= 3; i++) {
        controller.enqueue(enc.encode(`chunk ${i}\n`));
        await new Promise((r) => setTimeout(r, 100));
      }
      controller.close();
    },
  });
  return new Response(body, { headers: { "content-type": "text/plain" } });
}
```

## Runtime modules

Seven built-in modules give your code storage and platform access. Import them by name; they are part of the runtime and
never bundled, and every function returns a promise, except `std:sqlite`, which is synchronous. Failures throw an `Error` whose `message` and `code` are a stable
code such as `kv_value_too_large`, never a path or a driver message. Your data is per service: two services never see
each other's KV keys, tables or blobs.

```ts
import { kv } from "std:kv";
import { sql } from "std:sql";
import { Database } from "std:sqlite";   // also "bun:sqlite"
import { blob } from "std:blob";
import { enqueue } from "std:queue";
import { call } from "std:services";
import { acme } from "std:sdk";      // an operator SDK
```

| Module | What it gives you | Guide |
|---|---|---|
| `std:kv` | Small values by key, TTL, `list` by prefix, and `kv.incr`, an atomic counter | [KV](sub-services.md#kv) |
| `std:sql` | A private SQLite database: `execute`, `batch`, interactive `transaction`; rows come back as arrays | [SQL](sub-services.md#sql) |
| `std:sqlite` (`bun:sqlite`) | The same database with bun:sqlite's synchronous API: a few µs per short statement, 100 ms per statement at most | [Synchronous SQL](sub-services.md#synchronous-sql-stdsqlite) |
| `std:blob` | Objects up to 100 MB, streamed in both directions (`blob.put(key, req.body)`) | [Blobs](sub-services.md#blobs) |
| `std:queue` | `enqueue(slug, body, { delaySeconds, dedupId })` to a service the handler may call (same tenant by default), consumed by a `queue` handler | [Queues](sub-services.md#queues) |
| `std:services` | `services.fetch(slug, request)` runs another service of the same tenant in-process (`call` is the same); `services.call` and `services.bind` make typed RPC calls to its `rpc` methods | [Service calls](sub-services.md#service-calls), [Typed RPC](sub-services.md#typed-rpc) |
| `std:sdk` | The operator's SDKs: `import { acme } from "std:sdk"`, `await acme.method(arg)`; the SDK runs in the runtime | [Use operator SDKs](use-sdks.md) |
| `std:platform` | `platform.fetch(binding, request)`: operator bindings, for operator SDK services only | [Operator SDKs and bindings](operator-sdks.md) |
| `std:log` | `log.info(message, fields?)` and `log.with(fields)`: records with structured fields, shown by `21ci logs` | [Structured logs](#structured-logs) |
| `std:secrets` | `secrets.get(NAME)` and `secrets.require(...NAMES)`: secret values only, never a plain env var | [Secrets](#environment-variables-and-secrets) |
| `std:lock` | `lock(key, fn, { waitMs, leaseMs })` and `lock.try(key, fn, opts)`: a named mutex per service | [Locks](sub-services.md#locks) |
| `std:schedule` | `schedule.after`, `schedule.at`, `schedule.cancel`, `schedule.list`: one-off tasks of the service | [Schedules](schedules.md#one-off-schedules-from-code) |

`import acme from "sdk:acme"` is the same operator SDK as a default export; an SDK that is not published on the node
is rejected at deploy with `422`. Neither form puts SDK code in your service. See [Use operator SDKs](use-sdks.md).

A taste of the style, a counter in KV and a row in SQL:

```ts title="main.ts"
import { kv } from "std:kv";
import { sql } from "std:sql";

export default async function (): Promise<Response> {
  const hits = await kv.incr("hits");                      // atomic, no lost updates
  const r = await sql.execute("SELECT datetime('now')");   // rows are arrays: r.rows[0][0]
  return Response.json({ hits, now: r.rows[0][0] });
}
```

Limits, consistency, error codes, schema migrations, transactions and a Rust (WASM) version of every example are in
[KV, SQL, blobs and other sub-services](sub-services.md). How the data is stored is in
[Storage](../concepts/storage.md).

## Environment variables and secrets

Env variables and secrets both appear in `Deno.env`, a read-only subset of Deno's API:

| Call | Result |
|---|---|
| `Deno.env.get(name)` | the value, or `undefined` |
| `Deno.env.has(name)` | `boolean` |
| `Deno.env.toObject()` | every variable and secret as an object |
| `Deno.env.set()`, `Deno.env.delete()` | throw a `TypeError` |

```ts title="sample/deno/env-secrets/main.ts (excerpt)"
--8<-- "sample/deno/env-secrets/main.ts:28:42"
```

Env values are readable through the API; secrets are write-only and encrypted at rest. Inside the guest both are
plain strings, so never log or return a secret. Names match `[A-Z_][A-Z0-9_]*`, the `RT_` prefix is reserved, a
service has at most 100 env variables and 100 secrets, and a value is at most 32 KB. Changes apply to the **next
task**, without a redeploy.

To read only secrets, import `std:secrets`. `get` returns the value or `null`; `require` returns every value or
throws one error that names every missing secret:

```ts
import { secrets } from "std:secrets";

const { STRIPE_KEY, DB_URL } = secrets.require("STRIPE_KEY", "DB_URL");
// missing: error.code === "secret_missing", error.missing === ["DB_URL"],
// and the message says to run `21ci secret set <service> DB_URL`
```

The accessor cannot be dumped: it has no enumerable keys and `JSON.stringify(secrets)` is `undefined`.

=== "21ci"

    ```bash
    21ci env set hello GREETING=hi LOG_LEVEL=debug
    printf %s "$STRIPE_KEY" | 21ci secret set hello STRIPE_API_KEY
    21ci secret list hello          # names and update times, never values
    ```

=== "curl"

    ```bash
    # PUT replaces the whole env map
    curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"GREETING":"hi","LOG_LEVEL":"debug"}' "$RT_API/v1/services/$SVC/env"
    curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d "{\"value\":\"$STRIPE_KEY\"}" "$RT_API/v1/services/$SVC/secrets/STRIPE_API_KEY"
    ```

If the node cannot load the env when your code reads it, the read throws `env_unavailable` instead of pretending the
variable is unset.

## Dependencies

Imports resolve at **build time**, never at run time. The runtime serves only what is in the bundle.

| Import | Resolved |
|---|---|
| `./x.ts`, `../lib/y.js` | files of your upload |
| `std:*` | built into the runtime |
| `sdk:<name>`, `std:sdk` | an operator SDK: a generated shim of its contract, never its code |
| `npm:pkg@range`, `jsr:@scope/pkg@range` | fetched at build from the node's CDN (`build.npm_cdn`, `https://esm.sh` by default), pinned in `rt.lock` |
| `https://host/...` | only if the host is in `build.allowed_hosts` |
| a bare name such as `zod` | only through the `rt.json` import map |
| anything else (`file:`, `node:`, an unknown `rt:` or `sdk:`) | `422 validation_failed` at deploy |

At run time the module loader serves only what is in the bundle. Static imports and dynamic imports with a literal
specifier (`await import("./big.ts")`) are bundled; a computed specifier works only for a module that is already in the
bundle, otherwise it fails with `module_not_in_bundle`. A package that needs the file system, sockets or child
processes does not work; use `std:sql`, `std:blob` and `fetch` instead. The import rules, the bundling model, framework recipes (Hono, itty-router, tRPC, Oak, drizzle-orm) and a
checklist for "does my npm package work?" are in the [Deno runtime](../languages/deno.md) page. Keep bundles small:
[Optimize your service](optimize.md) explains what a large bundle costs.

### When the build fails

Problems with your own files show up at deploy: `422 validation_failed` with `details.diagnostics` listing file, line,
column and message (`21ci push` prints them). Problems found while building (a dependency that does not resolve, a
fetch that fails, a timeout) show up on first use:

- requests get `502` with `x-rt-reason: build_failed`;
- the version's `build_state` is `failed` with a reason: `syntax`, `resolve`, `fetch`, `lock_mismatch`, `timeout`,
  `oom`, `too_large` or `internal`;
- if the failed version was live and an older version is ready, the node rolls back to it (`deploy.auto_rollback`,
  on by default; off in the `dev` config).

```bash
21ci versions hello                      # build state of each version
21ci versions hello 3                    # one version, with its failure reason and message
21ci build-logs hello 3                  # the whole build log
21ci rebuild hello 3                     # retry a failed version after fixing the cause
```

Use `21ci push --wait` (an eager build) in CI, so a broken dependency fails the deploy instead of the first request.
See [Deploy and roll back](deploy.md).

## Web APIs

Available as globals: `fetch`, `Request`, `Response`, `Headers`, `FormData`, `URL`, `URLSearchParams`, `URLPattern`,
streams (`ReadableStream`, `WritableStream`, `TransformStream`, `TextEncoderStream`, `TextDecoderStream`,
`CompressionStream`, `DecompressionStream`, …), `TextEncoder`, `TextDecoder`, `Blob`, `File`, `FileReader`, `atob`,
`btoa`, timers, `AbortController`, `AbortSignal`, events (`Event`, `EventTarget`, `CustomEvent`, …),
`structuredClone`, `MessageChannel`, `crypto` (WebCrypto), `console`, `performance` and `WebAssembly`.

Removed: `Worker`, `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait` / `waitAsync` / `notify`, `ShadowRealm`
and `caches`. `WebSocket` is an outbound client only; accept sockets with
[`std:sockets`](websockets.md). Of `Deno` only `Deno.env` is left: `Deno.readFile`, `Deno.connect`, `Deno.Command` and the
rest do not exist.

`WebAssembly` modules work, but every wasm memory counts against your memory limit and shared memories are refused.
The full list is in the [guest API reference](../reference/guest-api.md#global-web-apis).

## Outbound `fetch`

`fetch` goes through the node's egress guard. Only the public internet is reachable: loopback, private and link-local
ranges and cloud metadata addresses are denied, and so are ports 25, 465 and 587 (mail). A service can narrow it
further with its `egress` block: a non-empty `allowed_hosts` turns into an allowlist, `denied_hosts` blocks hosts
(exact names or dot-suffixes). Redirects are followed (up to 5) and re-checked at every hop; an `https` to `http`
redirect is refused unless the operator allows it.

A refused or failed fetch rejects with a `TypeError` whose message is a code:

| Code | Meaning |
|---|---|
| `egress_denied` | the address, port, scheme or host is not allowed |
| `egress_limit` | too many fetches in this task (50 for HTTP, 200 for schedules and queues) |
| `egress_too_large` | the response is over the size cap |
| `egress_quota` | the tenant's daily egress quota is used up |
| `egress_timeout` | connect timeout, or the task's wall deadline |
| `egress_dns` | the name did not resolve |
| `egress_upstream` | connection, TLS or protocol failure |
| `egress_insecure_redirect` | an `https` URL redirected to `http` |

```ts
try {
  const res = await fetch("https://api.example.com/rates");
  if (!res.ok) return new Response(`upstream ${res.status}`, { status: 502 });
  return Response.json(await res.json());
} catch (e) {
  console.warn("fetch failed:", (e as Error).message);   // e.g. "egress_timeout"
  return new Response("upstream unavailable", { status: 503 });
}
```

At most 6 fetches run at once per task; further ones wait for a free slot. Request bodies are buffered before
sending; response bodies stream.

## The frozen clock

`Date.now()`, `new Date()` and `performance.now()` advance only at I/O and timer boundaries (an `await` on a fetch,
a storage call or a timer), never during synchronous code. A busy loop measures zero elapsed time:

```ts
const t0 = performance.now();
for (let i = 0; i < 1e8; i++) {}           // burns CPU
console.log(performance.now() - t0);       // 0
await new Promise((r) => setTimeout(r, 10));
console.log(performance.now() - t0);       // >= 10
```

This takes away the high-resolution timer that timing side-channel attacks need. Timestamps are still correct for
everything practical (`createdAt`, TTLs, logs); just do not use the clock to measure CPU time. See
[Security](../concepts/security.md).

## Warm reuse

By default every task starts from a clean isolate. With `deno.reuse_isolates` on (the default only in the
`min_latency` profile) an isolate that finished cleanly is kept and serves later requests of the same service version
on the same scheduler thread. Top-level code then runs once per isolate and module variables survive between requests.

Turn it on for one service with `21ci service update hello --reuse-isolates true`, or
`PATCH /v1/services/{id}` with `{"deno": {"reuse_isolates": true}}`.

A kept isolate is a cache, not a home for state. It is dropped after any error, a CPU or memory kill, a leftover timer
or pending promise, an env or secret change your code read, 1,000 requests, an idle period, memory pressure or a new
version, and consecutive requests may land on different isolates. Write code that is correct both ways:

- Keep state in `std:kv`, `std:sql` / `std:sqlite` or `std:blob`, never in module variables.
- Use module scope only for things that are safe to recompute: compiled regexes, parsed config, a "schema is ready"
  flag that falls back to a cheap check.
- Finish everything you start: await your promises (or hand them to `ctx.waitUntil`) and clear your timers. An isolate
  with leftovers is not reused.
- Read `Deno.env` inside the handler, not once at the top level, if you want to be explicit about it. Either way a
  changed variable reaches the next task.

What reuse saves, what it costs and a longer list of reuse-safe patterns are in
[Optimize your service](optimize.md#write-reuse-safe-code). The SQL sample shows the schema-once pattern: the flag
saves a query while an isolate is reused, and without reuse the check is one cheap read:

```ts title="sample/deno/sql-todo/main.ts (excerpt)"
--8<-- "sample/deno/sql-todo/main.ts:32:45"
```

## Limits

Defaults depend on the trigger, not on the profile. A service can change its limits within the maximums of its plan
(`PATCH /v1/services/{id}` with a `limits` block); an explicit `wall_ms` or `cpu_ms` then applies to every trigger.

| Limit | HTTP and calls | Schedules | Queue |
|---|---|---|---|
| Wall time | 30 s (a call: at most the caller's remaining time) | 300 s | 300 s |
| CPU time | 1,000 ms | 10,000 ms | 10,000 ms |
| Memory (heap, `ArrayBuffer`s, wasm memories) | 128 MB | 128 MB | 128 MB |
| Request body | 10 MB | | 1 MB per message |
| Response body (streamed) | 100 MB | | |
| Outbound fetches (total / at once) | 50 / 6 | 200 / 6 | 200 / 6 |
| Log lines per task | 1,000 | 1,000 | 1,000 |

The plan's `limits_max` caps these defaults too: the `free` plan allows at most 30 s wall and 1,000 ms CPU, so on
`free` scheduled and queue tasks also get 30 s and 1 s. The full table, including plan maximums and storage quotas,
is in [Limits](../reference/limits.md).

## Errors and logs

What the client sees when something goes wrong:

| Outcome | Status | `x-rt-reason` |
|---|---|---|
| Your handler threw, rejected, or returned something that is not a `Response` | 500 | `guest_error` |
| CPU limit | 502 | `cpu_limit` |
| Memory limit | 502 | `oom` |
| Wall limit | 504 | `timeout` |
| The version failed to build | 502 | `build_failed` |

The body of these responses is generic: the message and stack of an exception go to the service logs, never to the
client. If you want the client to see a reason, catch the error and return your own response.

`console.debug`, `log`/`info`, `warn` and `error` write log records at the matching level, tagged with the task id.
A task keeps up to 1,000 lines (later ones are dropped and counted); a message is cut at 10,000 characters.

SDK errors carry the code in `.code`. Some errors from the platform itself (`fetch`, the request body,
`env_unavailable`, the `sql.transaction` time cap) put the code only in `.message`, so read both:

```ts
try {
  await kv.set(key, value);
} catch (e) {
  const code = (e as { code?: string }).code ?? (e as Error).message;
  if (code === "storage_quota") return new Response("storage full", { status: 507 });
  throw e;   // 500 guest_error, stack in the logs
}
```

Every code is listed in [Error reasons](../reference/errors.md#codes-inside-guest-code).

### Structured logs

`std:log` writes a record with fields. `21ci logs` prints the fields after the message; `--json` and the API return
them as `fields`. Plain `console.*` lines keep working and have no fields.

```ts
import { log } from "std:log";

const orders = log.with({ route: "/orders" });
orders.info("order placed", { orderId: 42, total: 19.5 });
```

A fields object has at most 32 top-level keys and 8 KB of JSON. Anything else (an array, a `BigInt` inside, a larger
object) throws `validation_failed` and writes nothing.

## Testing

The samples test at two levels.

**Unit tests for pure modules.** Keep parsing, validation and business rules in modules that do not import `std:*`,
and test them with `deno test` or Node's test runner (Node 22.18 or later runs `.ts` files directly):

```ts title="test/validate.test.ts"
// Run: node --test test/*.test.ts   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import { validateEventCreate } from "../src/validate.ts";

test("capacity must be a positive integer", () => {
  const r = validateEventCreate({ title: "Meetup", starts_at: "2030-01-01T10:00:00Z", capacity: 0 }, 0);
  assert.equal(r.ok, false);
});
```

`node:` imports are fine in tests as long as the test files stay out of the directory you push. If a function needs
storage, pass it in as a parameter (an object with `get` and `set`) so the test can hand it a `Map`.

**Smoke tests against a node.** Each app in `sample/apps/` has a `smoke.sh` that deploys the service under its own
`*-smoke` slug with an eager build, then checks the main flows over HTTP with `curl` and `jq`:

```bash title="sample/apps/events/smoke.sh (excerpt)"
--8<-- "sample/apps/events/smoke.sh:14:35"
```

Run them against a local node ([Install](../getting-started/install.md)) or a staging node. With `21ci` the same loop
is short:

```bash
21ci push . --service my-fn-smoke --create --wait
21ci invoke my-fn-smoke '/healthz' --fail
21ci invoke my-fn-smoke /orders -d '{"sku":"a1"}' --fail
```

## Debugging

- **Find the task.** Every response carries `x-rt-task-id`. `21ci logs <svc> --task task_…` shows exactly that
  task's log lines, including the stack of an exception.
- **Follow live.** `21ci logs <svc> -f --level info` streams new records as they arrive.
- **Tell platform errors from yours.** A response with `x-rt-reason` came from the runtime (`guest_error`, `timeout`,
  `queue_timeout`, …); responses your handler returns never have it.
- **See where the time went.** `server-timing` splits a request into `queue` (waiting for a slot), `wait_runtime`
  (waiting for the build), `init` and `exec`. `21ci stats <svc>` shows the percentiles.
- **Builds.** `21ci versions <svc> <n>` shows a version's build state and failure; `21ci build-logs <svc> <n>` shows
  the whole build log.
- **Log the context.** `console.log(ctx.taskId, ctx.trigger, ctx.service.version)` makes schedule and queue runs easy
  to tell apart.

## See also

- [KV, SQL, blobs and other sub-services](sub-services.md): the `std:*` modules in depth, with Deno and WASM examples.
- [Deno runtime](../languages/deno.md): imports, npm packages, frameworks and which packages work.
- [Guest API reference](../reference/guest-api.md): every function of the `std:*` modules with parameters, results and
  error codes.
- [Calls and queues](calls-and-queues.md) and [Schedules](schedules.md): service-to-service calls, queue consumers and
  interval or cron handlers.
- [Optimize your service](optimize.md): cold starts, reuse, bundle size, batching and limits.
- [Tutorials](../tutorials/index.md): complete services built step by step.
