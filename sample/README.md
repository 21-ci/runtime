# Runtime samples

Runnable examples for the self-hosted functions runtime (Deno isolates + WASM components). They follow the
guides in `docs/`: control API and build input [api.md](../docs/api.md), guest SDK, handler contract and triggers
[handlers.md](../docs/handlers.md).

## Quickstart

Needs `bash`, `curl` and `jq` (>= 1.6).

```bash
export RT_API=http://127.0.0.1:8081      # control listener (default)
export RT_INVOKE=http://127.0.0.1:8080   # public invoke listener (default)
export RT_KEY=...                        # root token, or a tenant key rtk_… with deploy + services scopes

cd sample/deno/hello
../../scripts/deploy.sh . hello          # creates the service if needed, uploads, activates
../../scripts/invoke.sh hello '/?name=Ada'
```

`RT_KEY` may stay empty only against a debug build that has no root token.

With the `21ci` CLI instead of the scripts (`make cli` builds `target/debug/21ci`, see [docs/cli.md](../docs/cli.md);
it reads the same `RT_API` / `RT_INVOKE` / `RT_KEY`):

```bash
21ci push sample/deno/hello --service hello --create    # same upload formats as deploy.sh
21ci invoke hello '/?name=Ada'
21ci init my-fn --template deno-kv                      # scaffold from these samples (deno, deno-kv, wasm-rust, wasm-js)
```

## Examples

| Example | Runtime | Shows | Dir |
|---|---|---|---|
| hello | deno, Form A | `default function (req, ctx)`, `ctx`, no warm state | `deno/hello/` |
| kv-counter | deno, Form A | `std:kv` atomic `incr` (TTL)/get/delete/list | `deno/kv-counter/` |
| sql-todo | deno, Form A | `std:sql` `execute` / `batch`, REST CRUD | `deno/sql-todo/` |
| sqlite-sync | deno, Form A | `std:sqlite` (`bun:sqlite` API): synchronous queries, `db.transaction`, REST CRUD | `deno/sqlite-sync/` |
| blob-upload | deno, Form A | `std:blob`, streamed upload and download | `deno/blob-upload/` |
| npm-zod | deno, Form A | `npm:` dependency, `rt.json` import map | `deno/npm-zod/` |
| scheduled-queue | deno, Form B | `fetch` + `scheduled` + `queue`, `std:queue`, schedules, DLQ | `deno/scheduled-queue/` |
| streams | deno, Form B | `std:stream` topics (append, `dedupId`, consumer with `failAt`), `std:pubsub` publish, subscribe and request/reply, async invoke | `deno/streams/` (two services) |
| service-call | deno, Form A | `std:services` `call`, `ctx.waitUntil`, call depth | `deno/service-call/` |
| env-secrets | deno, Form A | env + secrets via `Deno.env`, header hygiene | `deno/env-secrets/` |
| platform-sdk | deno, Form A | the operator SDK through `std:sdk` / `sdk:acme`: no SDK code in the service (needs the operator setup) | `deno/platform-sdk/` |
| acme-sdk | operator | the SDK as a runtime service (`defineSdk`, `21ci sdk publish`), `[[binding]]` config, mock API | `operator-sdk/` |
| rust-hello | wasm | Rust `wasi:http` component (`wasm32-wasip2`) | `wasm/rust-hello/` |
| js-componentize | wasm | JS compiled with `jco componentize` | `wasm/js-componentize/` |

## Apps

Complete services you can run for yourself and share. Each has a README (setup with `21ci`, API, limits),
unit tests for its pure logic (`node --test test/*.test.ts` or `deno test`; `cargo test` for Rust) and a `smoke.sh`
that deploys it under its own `*-smoke` slug and checks the main flows against a node (helpers in `apps/lib/smoke.sh`).

| App | Runtime | What it is | Shows | Dir |
|---|---|---|---|---|
| telegram-bot | deno, Form B | Webhook Telegram bot: notes, reminders, CBU rate alerts; multi-step dialogs | secret-header check, `update_id` dedupe, per-chat state machine in `std:kv`, delayed `std:queue` self-messages, queue fan-out, `std:services` call | `apps/telegram-bot/` |
| events | deno, Form A | Events and registrations: capacity, duplicates, cancel by token, CSV export | `sql.transaction` (no overbooking), partial unique indexes, `Idempotency-Key` in `std:kv`, per-address throttle with `kv.incr` + `ctx.client.ip`, streamed CSV | `apps/events/` |
| live-queue | deno, Form A | Electronic queue with phone tickets, ETA, staff board and TV screen | concurrency-safe numbering, long-polling within the wall limit, service-served HTML with a nonce CSP, per-address throttle with `kv.incr` + `ctx.client.ip` | `apps/live-queue/` |
| cbu-rates | deno, Form B | UZS rates from the Central Bank of Uzbekistan: history, convert, change alerts | 60 s interval schedule, outage back-off, change detection in a transaction, `enqueue` with `dedupId` | `apps/cbu-rates/` |
| chat-room | deno, durable class + Form B | Group chat: rooms with history and presence behind a gate | rooms as durable objects (`durable.get(room, { service })` forwards the upgrade), `std:sockets` tags for presence, history in object storage, output gate, `ping`/`pong` auto-response, an alarm, sockets kept across a hibernation and a redeploy; the smoke drives real WebSocket clients (Node 22) | `apps/chat-room/` |
| edge-gateway | wasm + deno | JWT gateway in front of internal services, with an audit trail | Rust component with `std:host/{kv,call,queue,sdk,log}` (exact per-subject and per-address limits with `kv.incr` on `x-rt-client-ip`, audit shipped through the operator SDK, which the smoke updates without a redeploy), `access: internal` + `environment` + `call.allowed_callers` | `apps/edge-gateway/` |

## Scripts

| Script | What it does |
|---|---|
| `scripts/deploy.sh <source> [slug]` | `source` is a `.ts` / `.js` file, a `.wasm` component, a `.zip` / `.tar`, or a directory (with `rt.json`: JSON bundle `{entry, files, imports}` built with `jq`). Finds the service by slug, creates it if missing (`POST /v1/services`), uploads with `?activate=1` and prints the invoke URL. `RT_COMPILE=eager` waits for the build. |
| `scripts/invoke.sh <slug> [path] [METHOD] [body]` | `ANY /s/{slug}/{path}` with status, runtime headers (`x-rt-task-id`, `x-rt-reason`, …) and pretty-printed JSON. `body` may be `@file`. `RT_INVOKE_KEY` adds `Authorization: Bearer rtk_…` for `access=key` services. |
| `scripts/svc-id.sh <slug>` | Prints the `svc_…` id with one `GET /v1/services?slug=<slug>` call (exact filter, contracts C-9). Exit 1 if there is no such service. |

## Control API used by the samples (base `/v1`, `Authorization: Bearer rtk_…`)

| Call | Purpose |
|---|---|
| `POST /v1/services` `{slug, runtime, name?}` | create (`runtime`: `deno` or `wasm`) |
| `POST /v1/services/{id}/versions?activate=1&compile=lazy\|eager` | upload (raw body, see below) |
| `PUT /v1/services/{id}/live` `{version}` | roll back / pin a version (`If-Match` supported) |
| `PUT /v1/services/{id}/env` | whole env map |
| `PUT /v1/services/{id}/secrets/{NAME}` | write-only secret |
| `POST /v1/services/{id}/schedules` | interval / cron schedule |
| `POST /v1/services/{id}/queue` `{body, delay_seconds?, dedup_id?}` | enqueue |
| `GET /v1/services/{id}/logs?follow=1` | logs (SSE with `follow`) |

Upload formats:

| Runtime | Content-Type | Shape |
|---|---|---|
| deno | `application/typescript`, `text/javascript` | single file (<= 1 MB) |
| deno | `application/json` | `{entry, files: {path: text}, imports?}` (<= 10 MB, <= 1,000 files) |
| deno | `application/zip`, `application/x-tar` | files + optional `rt.json` `{entry, imports}` |
| wasm | `application/wasm` | WASI P2 component exporting `wasi:http/incoming-handler` (<= 50 MB) |

The build is lazy by default: `POST …/versions` answers `201` with `build_state: pending` and the **first request**
compiles (and, for `npm:` imports, fetches dependencies). Errors: control API `{"error": {code, message}}`;
invoke errors are `text/plain` plus an `x-rt-reason` header.

## Things every Deno example relies on

- **No warm state.** Each request runs in a fresh isolate and re-runs the module's top level. Keep state in `std:kv`, `std:sql` / `std:sqlite` or `std:blob`.
- **Frozen clock.** `Date.now()` does not advance during synchronous code, so never use it to measure CPU time.
- **Imports.** `std:kv`, `std:sql`, `std:sqlite` (also `bun:sqlite`), `std:blob`, `std:queue`, `std:services`, `std:platform`, `std:sdk`; operator SDKs as `std:sdk` or `sdk:<name>` (contract shims, the SDK runs in the runtime); `npm:` / `jsr:` / allowlisted `https:` resolved at build time. Anything else is rejected at deploy (`422`).
- **Handler forms.** Form A `export default function (req, ctx)`; Form B `export default { fetch, scheduled, queue }`.
  `ctx = {taskId, service: {id, slug, version}, trigger, invoker?, client?, deadline, waitUntil}`.
- **Routing.** `/s/{slug}/{rest}` reaches the handler as `/{rest}`; the prefix is in `x-rt-base-path`.
- **Egress.** Public internet only; loopback, private ranges and metadata addresses are denied.
