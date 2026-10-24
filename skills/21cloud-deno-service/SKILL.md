---
name: 21cloud-deno-service
description: Writes, debugs and reviews TypeScript/JavaScript services for the 21 Cloud Functions Runtime (Deno isolates). Use when code imports `std:kv`, `std:sql`, `std:sqlite`, `std:blob`, `std:queue`, `std:pubsub`, `std:stream`, `std:services`, `std:schedule`, `std:lock`, `std:sockets`, `std:durable` or `sdk:*`, when a handler is `export default function (req, ctx)` / `export default { fetch, scheduled, queue, pubsub, stream }` for this runtime, or when the user asks to build an HTTP endpoint, cron job, queue worker, pub/sub subscriber, topic consumer, WebSocket app, async HTTP endpoint or durable object on 21 Cloud.
---

# 21 Cloud: Deno (TypeScript / JavaScript) services

A service is one entry module (default `main.ts`) plus relative imports, deployed as a version and run in a **fresh V8
isolate per task**. Every trigger (HTTP, interval/cron schedule, one-off schedule, queue, service call, WebSocket event,
alarm) is one task with its own limits. Docs: https://21-ci.github.io/runtime/ . Deploying: the `21ci-deploy` skill.

References (read when needed):
- [references/handlers.md](references/handlers.md): handler forms, `ctx`, events, request/response rules, WebSockets and durable objects.
- [references/std-modules.md](references/std-modules.md): every `std:*` module, signatures and error codes (including `std:pubsub` and `std:stream`).
- [references/limits.md](references/limits.md): limits, plan quotas, upload rules, invoke outcomes, egress codes.

## Hard rules

1. **No state in module globals.** Top-level code runs once per task by default. With isolate reuse (`min_latency`
   profile or `deno.reuse_isolates`) globals persist only as a cache and may be seen by the next request of another
   user. Persist in `std:kv`, `std:sql` / `std:sqlite`, `std:blob` or durable object storage.
2. **Return a `Response`** from `fetch` (anything else is `500 guest_error`). A throw is `500 guest_error`; the client
   gets a generic body, the message and stack go to the service logs.
3. **Imports are resolved at build.** Allowed: relative files, `std:*`, `bun:sqlite` (= `std:sqlite`), `sdk:<name>`,
   `npm:` / `jsr:` / allowlisted `https:` URLs, and bare names mapped in `rt.json` `imports`. No `node:` (unless the
   operator allowlisted it), no `file:`, no bare specifiers without a mapping: `422` at deploy.
4. **No filesystem, no raw sockets, no workers.** `Deno` is reduced to `Deno.env` (read-only). Removed: `Worker`,
   `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait/waitAsync/notify`, `ShadowRealm`, `caches`.
5. **Frozen clock.** `Date.now()` / `performance.now()` advance only at I/O and timer boundaries. Never time CPU work with them.
6. **Outbound HTTP is public internet only** (`fetch`, outbound `WebSocket`): private ranges, loopback, metadata
   addresses and ports 25/465/587 are refused with a `TypeError` whose message is a code (`egress_denied`, ...).
7. **Delivery is at least once** for queue messages, schedules, socket events, alarms, stream records and async HTTP
   invocations. Make those handlers idempotent (dedupe by `message.id`, `event.id`, `event.scheduleId` + `event.scheduledTime`,
   the record's `partition` + `offset`, or `ctx.invocation.id`). Pub/sub is at most once: a copy that is `dropped` is gone.
8. **Errors from `std:*` are `Error`s with a stable `.code`.** Branch on `e.code`, never on the message.
9. **Do not ship a types package import.** None exists: declare `Ctx` and event interfaces in your own code (copy from
   [references/handlers.md](references/handlers.md)).

## Handler forms

```ts
// Form A: one function, called for http and call triggers (and synthetic requests for the rest)
export default async function (req: Request, ctx: Ctx): Promise<Response> {
  const name = new URL(req.url).searchParams.get("name") ?? "World";
  return Response.json({ message: `Hello, ${name}!`, taskId: ctx.taskId });
}
```

```ts
// Form B: an object; all members optional, at least one required
export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> { /* http, call */ },
  async scheduled(event, ctx) { /* interval, cron, one-off std:schedule */ },
  async queue(batch, ctx) { /* queue messages */ },
  async pubsub(msg, ctx) { /* pub/sub subscriptions; the return value answers a request */ },
  async stream(batch, ctx) { /* topic consumer groups; batch.failAt(m) to retry from m */ },
  webSocketMessage(ws, message, ctx) {}, webSocketClose(ws, code, reason, wasClean, ctx) {},
};
```

A durable service (`"durable": true`) exports a **class** instead: one instance per object. See
[references/handlers.md](references/handlers.md#websockets-and-durable-objects).

Without `scheduled` / `queue` / `pubsub` / `stream`, those triggers reach `fetch` as `POST /__rt/scheduled`, `POST /__rt/queue`,
`POST /__rt/pubsub` or `POST /__rt/stream` with a JSON body (shapes in the reference). `/__rt/` is reserved: do not route
your own paths under it.

## Request facts

- `req.url` is `http://localhost/<path>?<query>`: `/s/{slug}` is stripped (original prefix in header `x-rt-base-path`).
  Route on `new URL(req.url).pathname`.
- `ctx.client?.ip` (also header `x-rt-client-ip`) is the client address, `http` trigger only. Clients cannot spoof it.
- `Authorization` reaches the guest unchanged, except `Bearer rtk_…` runtime keys, which are stripped. Client `x-rt-*`
  headers never reach the guest.
- `req.body` streams; reading past `limits.max_body_bytes` (default 10 MB) throws `Error("request_too_large")`.
- `ctx.waitUntil(promise)` keeps the task alive after the response, bounded by the wall limit.

## Choose the right module

| Need | Use |
|---|---|
| Counters, rate limits, idempotency keys, small JSON by key | `std:kv` (`incr` is atomic; `cas` for compare-and-swap; TTL) |
| Relational data, transactions | `std:sql` (async, `sql.transaction`) or `std:sqlite` (sync, `bun:sqlite` API, 100 ms per statement) |
| Files, uploads, large objects (streamed) | `std:blob` |
| Background work, retries, fan-out | `std:queue` `enqueue(slug, body, { delaySeconds?, dedupId? })` + a `queue` handler |
| "Run this later" from code | `std:schedule` `schedule.after(seconds, payload, { id? })` + `scheduled` handler |
| Fan out now, or ask one responder (no storage) | `std:pubsub` `publish(subject, body)` to `events.subscribe` subscribers; `request(subject, body)` gets one reply from a `pubsub` handler |
| Ordered, replayable history per key, read by consumer groups | `std:stream` `append(topic, body, { key, dedupId })` + a consumer in `events.consume` with a `stream` handler |
| Recurring jobs | an interval/cron schedule (control API / `21ci schedule add`) + `scheduled` handler |
| Accept an HTTP call now, run it later with retries | async invocation: the caller sends `Prefer: respond-async` (service needs `async.enabled=true`); the handler is the normal `fetch` |
| Mutual exclusion inside one service | `std:lock` `lock(key, fn, { waitMs?, leaseMs? })` |
| Call another service of the tenant | `std:services` `services.fetch(slug, req)` or typed RPC `services.call` / `services.bind` |
| Structured logs | `std:log` (`log.info(msg, fields)`); `console.*` also lands in logs |
| Secret values | `std:secrets` `secrets.require("API_KEY")`; plain env via `Deno.env.get` |
| WebSockets | `std:sockets` (the node holds the socket; each message is a task) |
| Per-entity serialized state (rooms, sessions, counters per user) | `std:durable` (durable service) |
| Operator-provided SDK | `import { acme } from "std:sdk"` or `import acme from "sdk:acme"` |

All `std:*` calls return promises except `std:sqlite` (synchronous) and `std:sockets` / `durable.get` (synchronous).

Queue, pub/sub, stream or async invocation? Use the first row that fits:

| Situation | Use | Kept? | Who gets it |
|---|---|---|---|
| Work per item, retried until it succeeds, dead-lettered after that | `std:queue` | yes | one worker of the service |
| Something happened and any interested service reacts now; or one reply from a responder | `std:pubsub` | no (at most once) | every subscriber, or one per queue group; one responder for `request` |
| Ordered history per key, replayed or read by several groups at their own pace | `std:stream` topic + `events.consume` | yes, for the retention period | each consumer group, in order per partition |
| An HTTP endpoint that should answer `202` and run later | async invocation (`async.enabled`) | yes, until done | your `fetch`, once per attempt, with `ctx.invocation` |

Pub/sub and streams need the plan to allow the `pubsub` / `stream` triggers (`403 trigger_not_allowed` otherwise).
Declare subscriptions and consumers on the service (`21ci service update --set events.subscribe=… / events.consume=…`),
not in code.

## Patterns

```ts
// Atomic per-client rate limit (fixed 60 s window) + counter
import { kv } from "std:kv";
export default async function (req: Request, ctx: Ctx): Promise<Response> {
  if (ctx.client) {
    const hits = await kv.incr(`rl:${ctx.client.ip}:${Math.floor(Date.now() / 60_000)}`, 1, { ttlSeconds: 120 });
    if (hits > 60) return new Response("slow down", { status: 429, headers: { "retry-after": "60" } });
  }
  if (req.method === "POST") return Response.json({ count: await kv.incr("count") });
  return Response.json({ count: ((await kv.get("count")) as number | null) ?? 0 });
}
```

```ts
// SQL: rows are ARRAYS in `columns` order, not objects
import { sql } from "std:sql";
const r = await sql.execute("SELECT id, title FROM todos WHERE done = ?", [false]);
const todos = r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
```

```ts
// Queue worker: messages not retry()-ed are acked when the handler returns; a throw redelivers the batch
export default {
  async queue({ messages }: QueueBatch, ctx: Ctx) {
    for (const m of messages) {
      try { await handle(m.body); m.ack(); } catch { m.retry({ delaySeconds: 30 }); }
    }
  },
};
```

```ts
// Typed RPC between services of the same tenant
// callee "billing":
export const rpc = { async createInvoice(arg: { amount: number }, ctx: Ctx) { return { id: "inv_1", amount: arg.amount }; } };
export default () => new Response("billing");
// caller (types: `21ci types billing -d types` -> types/billing.d.ts with interface BillingRpc):
import { services, ServiceError } from "std:services";
const billing = services.bind("billing") as BillingRpc;
const inv = await billing.createInvoice({ amount: 100 });   // throws ServiceError { code, message }
```

## npm / jsr dependencies

Import them directly (`import { z } from "npm:zod@3"`) or map bare names in an `rt.json` next to the entry:

```json
{ "entry": "main.ts", "imports": { "zod": "npm:zod@3" } }
```

`rt.json` keys: `entry` (default `main.ts`, then `main.tsx`, `main.js`, `main.jsx`, `main.mjs`) and `imports`.
Nothing else is read. Service settings (access, limits, triggers, durable, ...) are **not** in `rt.json`: set them on the
service (`21ci service create/update`, see the `21ci-deploy` skill). Dependencies are fetched at build (lazy builds:
on the first request), so a bad specifier fails the build (`502 build_failed` on invoke). Deploy with `21ci push --wait`
to surface it at deploy time.

## Debug checklist

| Symptom | Look at |
|---|---|
| `422 validation_failed` on push | `details.diagnostics` (`file`, `line`, `col`, `message`): syntax or a forbidden import |
| `502 build_failed` | `21ci versions <svc> <n>` (`failure.reason`: `syntax`, `resolve`, `fetch`, ...) and `21ci build-logs <svc> <n>` |
| `500 guest_error` | `21ci logs <svc> --level error` (the stack is only in logs); find the task by the `x-rt-task-id` response header: `21ci logs <svc> --task task_…` |
| `502 cpu_limit` / `502 oom` / `504 timeout` | Heavy sync work, large buffers, slow upstreams: see [references/limits.md](references/limits.md) |
| `TypeError: egress_denied` | Private/loopback target, denied port, or the service's `egress.allowed_hosts` |
| State "lost" between requests | Module globals: move it to storage (rule 1) |
| `403 trigger_not_allowed` | The plan or the service's `triggers.*` disallows that trigger (the built-in `free` plan has no `cron`) |

Runnable examples (read the closest one before writing a new service):
[hello](https://github.com/21-ci/runtime/tree/main/sample/deno/hello),
[kv-counter](https://github.com/21-ci/runtime/tree/main/sample/deno/kv-counter),
[sql-todo](https://github.com/21-ci/runtime/tree/main/sample/deno/sql-todo),
[sqlite-sync](https://github.com/21-ci/runtime/tree/main/sample/deno/sqlite-sync),
[blob-upload](https://github.com/21-ci/runtime/tree/main/sample/deno/blob-upload),
[npm-zod](https://github.com/21-ci/runtime/tree/main/sample/deno/npm-zod),
[scheduled-queue](https://github.com/21-ci/runtime/tree/main/sample/deno/scheduled-queue),
[service-call](https://github.com/21-ci/runtime/tree/main/sample/deno/service-call),
[streams](https://github.com/21-ci/runtime/tree/main/sample/deno/streams) (topics, consumer groups, pub/sub),
[env-secrets](https://github.com/21-ci/runtime/tree/main/sample/deno/env-secrets),
[platform-sdk](https://github.com/21-ci/runtime/tree/main/sample/deno/platform-sdk).
Complete apps: [events](https://github.com/21-ci/runtime/tree/main/sample/apps/events) (SQL transactions, idempotency, throttling),
[live-queue](https://github.com/21-ci/runtime/tree/main/sample/apps/live-queue),
[telegram-bot](https://github.com/21-ci/runtime/tree/main/sample/apps/telegram-bot) (webhook, queue fan-out),
[cbu-rates](https://github.com/21-ci/runtime/tree/main/sample/apps/cbu-rates) (interval schedule, `dedupId`),
[chat-room](https://github.com/21-ci/runtime/tree/main/sample/apps/chat-room) (durable objects, WebSockets, alarms).

Testing: keep pure logic in modules without `std:*` imports and unit-test them with `deno test` or `node --test`
(the apps do this); verify the rest by deploying and invoking (`21ci invoke <svc> /path --fail`).
