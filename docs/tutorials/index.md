---
title: Tutorials
description: Five complete sample apps for the runtime, the features each one exercises, how to deploy them and how to run their smoke tests.
---

# Tutorials

Each tutorial walks through one complete app from the runtime's `sample/apps/` directory: what it does, how it is
deployed, and how the important parts of the real source use the runtime. The apps are small enough to read in one
sitting and complete enough to run for yourself. Every one ships with unit tests and a smoke test you can point at
your own node.

## The apps

<div class="grid cards" markdown>

-   :material-robot-outline:{ .lg .middle } **Telegram bot**

    ---

    A webhook bot with notes, reminders and exchange-rate alerts. Per-chat dialog state in KV, delayed queue
    messages to itself, queue fan-out and a call to another service.

    [:octicons-arrow-right-24: Telegram bot](telegram-bot.md)

-   :material-calendar-check-outline:{ .lg .middle } **Event sign-ups**

    ---

    Events with a capacity, registrations, cancel tokens and a CSV export. No overbooking under concurrency,
    idempotent retries and a streamed response.

    [:octicons-arrow-right-24: Event sign-ups](events.md)

-   :material-ticket-outline:{ .lg .middle } **Live queue**

    ---

    An electronic queue with phone tickets, live ETA, a staff board and a TV screen. Gap-free numbering, long-polling
    within the wall limit and HTML pages with a nonce CSP.

    [:octicons-arrow-right-24: Live queue](live-queue.md)

-   :material-currency-usd:{ .lg .middle } **Currency rates**

    ---

    Exchange rates polled every 60 seconds with an interval schedule, history in SQL, back-off during outages and
    notifications to another service's queue.

    [:octicons-arrow-right-24: Currency rates](cbu-rates.md)

-   :material-shield-lock-outline:{ .lg .middle } **Edge gateway (WASM)**

    ---

    A Rust WebAssembly component that checks JWTs, scopes and rate limits, then forwards to internal Deno services,
    with an audit trail shipped through an operator binding.

    [:octicons-arrow-right-24: Edge gateway](edge-gateway.md)

</div>

The bot and the rates service work together: `cbu-rates` notifies `telegram-bot` when a rate moves, and the bot's
`/rates` command calls `cbu-rates`. Every other app stands alone.

## Which features each app uses

The table is based on the apps' source code. A dash means the app does not use the feature; the last column points
to a smaller example that does.

| Feature | [telegram-bot](telegram-bot.md) | [events](events.md) | [live-queue](live-queue.md) | [cbu-rates](cbu-rates.md) | [edge-gateway](edge-gateway.md) | Also see |
|---|---|---|---|---|---|---|
| Runtime | Deno, Form B | Deno, Form A | Deno, Form A | Deno, Form B | WASM (Rust) + 2 Deno services | `deno/hello`, `wasm/rust-hello` |
| `std:kv` | dialog state with TTL | `Idempotency-Key` replay with TTL; throttle with `kv.incr` | throttle with `kv.incr` | poll status, back-off | exact rate limits (`std:host/kv` `incr`) | `deno/kv-counter` |
| Client address (`ctx.client.ip`, `x-rt-client-ip`) | – | per-address throttle | per-address throttle | – | per-address limit, audit trail | [Guest API](../reference/guest-api.md) |
| `std:sql` | notes, reminders, update dedupe | `transaction`, partial unique indexes | `transaction`, read `batch` | `transaction`, upsert | notes and audit (Deno services) | `deno/sql-todo` |
| `std:blob` | – | – | – | – | – | `deno/blob-upload` |
| Queue consumer (`queue`) | yes, with `retry({delaySeconds})` | – | – | – | `edge-audit` | `deno/scheduled-queue` |
| `enqueue` | to itself, delayed, `dedupId` | – | – | to another service, `dedupId` | `std:host/queue` to `edge-audit` | `deno/scheduled-queue` |
| Schedules | – | – | – | interval, 60 s | optional, `edge-audit` every 5 min | `deno/scheduled-queue` |
| Service calls | `std:services` `call` | – | – | – | `std:host/call` | `deno/service-call` |
| Environments, `access: internal`, allowed callers | – | – | – | – | yes | [Calls and queues](../guides/calls-and-queues.md) |
| Env and secrets | yes | yes | yes | yes | yes (WASI environment) | `deno/env-secrets` |
| Outbound `fetch` | Telegram Bot API | – | – | cbu.uz | – | [Deno services](../guides/deno.md) |
| Operator SDK (`std:sdk`) | – | – | – | – | gateway (`std:host/sdk`) and `edge-audit` | `deno/platform-sdk` |
| `npm:` dependencies | – | – | – | – | – | `deno/npm-zod` |
| `access: key` | – | – | – | – | – | `scripts/invoke.sh` with `RT_INVOKE_KEY` |
| Custom domain | recommended for the HTTPS webhook | – | pages follow `x-rt-base-path` | – | – | [Custom domains](../guides/custom-domains.md) |
| Streamed response | – | CSV export | – | – | – | `deno/blob-upload` |
| HTML served by the service | – | – | yes, nonce CSP | – | – | – |
| Long-polling | – | – | yes | – | – | – |

## Prerequisites

All tutorials assume the same setup:

- **A running node.** Any node works, a local one included: see [Install](../getting-started/install.md) and
  [First run](../getting-started/first-run.md).
- **A tenant key.** Export the control and invoke URLs and the key, as in [First run](../getting-started/first-run.md):

    ```bash
    export RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080 RT_KEY=rtk_…
    ```

    A key created with the default scopes covers everything the apps need: `services:read`, `services:write`,
    `deploy`, `env:write`, `secrets:write` and `queue:write`. The root token works too.
- **A way to deploy.** Either the `21ci` CLI (see [CLI reference](../cli.md)), or the scripts in
  `sample/scripts/`, which need `bash`, `curl` and `jq` (1.6 or newer):

    | Script | What it does |
    |---|---|
    | `deploy.sh <source> [slug]` | Finds the service by slug, creates it if missing, uploads `<source>` (a file, a `.wasm` component, an archive or a directory) and makes it live. `RT_COMPILE=eager` waits for the build |
    | `invoke.sh <slug> [path] [METHOD] [body]` | Calls `/s/{slug}/{path}` and prints the status, the runtime headers and the body. `RT_INVOKE_KEY` adds a key for `access: key` services |
    | `svc-id.sh <slug>` | Prints the service's `svc_…` id |

- **For the unit tests:** Node.js 22.18 or newer (it runs TypeScript directly), or Deno. For the edge gateway:
  Rust with the `wasm32-wasip2` target.

Each tutorial shows the deploy steps both ways, in `21ci` and `curl` tabs. The apps deploy from a directory without
an `rt.json`; the CLI and `deploy.sh` both bundle the files and pick `main.ts` as the entry.

## Run the smoke tests

Every app has a `smoke.sh` that deploys it under its own slug (`events-smoke`, `live-queue-smoke`,
`cbu-rates-smoke`, `telegram-bot-smoke`, and `edge-smoke-gateway`, `edge-smoke-notes`, `edge-smoke-audit`), drives
the main flows through the public listener and checks every answer. Your own deployments are not touched.

```bash
cd sample/apps
RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080 RT_KEY=rtk_… events/smoke.sh
```

The smoke tests share one set of helpers, `sample/apps/lib/smoke.sh`. It reads the same environment as the
scripts and `21ci`:

```bash title="sample/apps/lib/smoke.sh"
--8<-- "sample/apps/lib/smoke.sh:4:14"
```

| Variable | Default | Meaning |
|---|---|---|
| `RT_API`, `RT_INVOKE` | `http://127.0.0.1:8081`, `http://127.0.0.1:8080` | Control and invoke listeners |
| `RT_KEY` | `$RT_ROOT_TOKEN` | Tenant key or root token |
| `RT_COMPILE` | `eager` | Deploys build at once, so a build error fails the deploy step |
| `SMOKE_CLEANUP` | `0` | `1` deletes the services the smoke test deployed when it ends |
| `SMOKE_TIMEOUT` | `45` | Per-request timeout in seconds |

Each step prints `ok` or `FAIL` lines; a failure also prints the HTTP status and the start of the body. The script
ends with a `passed, failed` summary and exits non-zero when anything failed, so it fits a CI job. The helpers need
`curl`, `jq` and `openssl`, and pass tokens to `curl` through a config file on a pipe, so they never show up in `ps`.

```console
$ events/smoke.sh

== deploy events-smoke
  ok    deployed events-smoke (…)
  ok    secret ADMIN_TOKEN set on events-smoke

== admin: create
  ok    create without the admin token -> 401
  ok    starts_at without an offset is rejected -> 400
  ok    create -> 201
  ok    2 spots, open
…
```

App-specific options:

| App | Options |
|---|---|
| telegram-bot | `SLUG`; `SMOKE_SLOW=1` also waits about 70 s for a reminder to fire. Runs in dry-run mode, so no bot token is needed |
| events | `SLUG` |
| live-queue | `SLUG` |
| cbu-rates | `SLUG`; `NOTIFY_SLUG=telegram-bot-smoke` notifies a real consumer; `SMOKE_SLOW=1` adds a 60 s schedule and waits for one scheduled poll |
| edge-gateway | `PREFIX` (default `edge-smoke`); `AUDIT_BINDING`; `EXPECT_SHIPPED=1` asserts that audit events reached the operator API. Builds the component first if it is missing |

To exercise the bot and the rates service together, run `telegram-bot/smoke.sh` first, then
`NOTIFY_SLUG=telegram-bot-smoke cbu-rates/smoke.sh`: the smoke deployment of the bot already calls
`cbu-rates-smoke` for `/rates`.

You can write a smoke test for your own service the same way: source `lib/smoke.sh`, then use `deploy`,
`set_secret`, `set_env`, `configure`, `inv`, `api`, `expect_code`, `expect_jq` and `finish`.

## Smaller examples

`sample/deno/` and `sample/wasm/` hold single-feature examples, a few dozen lines each. The guides use them.

| Example | Runtime | Shows | Guide |
|---|---|---|---|
| `deno/hello` | Deno, Form A | `export default function (req, ctx)`, `ctx` | [Your first service](../getting-started/first-service.md) |
| `deno/kv-counter` | Deno | `std:kv` atomic `incr` with TTL, get, delete, list | [Storage](../concepts/storage.md) |
| `deno/sql-todo` | Deno | `std:sql` `execute` and `batch`, REST CRUD | [Storage](../concepts/storage.md) |
| `deno/blob-upload` | Deno | `std:blob`, streamed upload and download | [Storage](../concepts/storage.md) |
| `deno/npm-zod` | Deno | an `npm:` dependency, `rt.json` import map | [Deno services](../guides/deno.md) |
| `deno/env-secrets` | Deno | env and secrets through `Deno.env`, header hygiene | [Deno services](../guides/deno.md) |
| `deno/scheduled-queue` | Deno, Form B | `fetch` + `scheduled` + `queue`, `std:queue`, schedules, DLQ | [Schedules](../guides/schedules.md), [Calls and queues](../guides/calls-and-queues.md) |
| `deno/service-call` | Deno | `std:services` `call`, `ctx.waitUntil`, call depth | [Calls and queues](../guides/calls-and-queues.md) |
| `deno/platform-sdk` | Deno | the operator SDK through `std:sdk` and `sdk:acme` (needs the operator setup) | [Use operator SDKs](../guides/use-sdks.md) |
| `operator-sdk/` | operator | the SDK (`defineSdk`), `[[binding]]` config, a mock API | [Operator SDKs and bindings](../guides/operator-sdks.md) |
| `wasm/rust-hello` | WASM | a Rust `wasi:http` component (`wasm32-wasip2`) | [WASM components](../guides/wasm.md) |
| `wasm/js-componentize` | WASM | JavaScript compiled with `jco componentize` | [WASM components](../guides/wasm.md) |

`21ci init <dir> --template deno|deno-kv|wasm-rust|wasm-js` scaffolds a new service from these samples, see
[`21ci init`](../cli.md#init).

## Where the code lives

All of it is in the runtime repository: `sample/apps/<name>/` for the apps, `sample/apps/lib/smoke.sh` for the
shared smoke helpers, `sample/scripts/` for the deploy scripts, and `sample/deno/`, `sample/wasm/` and
`sample/operator-sdk/` for the smaller examples.
