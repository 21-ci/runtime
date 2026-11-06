---
title: Guest API reference
description: Complete reference of the Deno guest API, handler forms, ctx, the std:* modules, Deno.env and the global Web APIs, with their WASM equivalents.
---

# Guest API reference

<span class="rt-badge rt-badge--deno">Deno</span> Everything a TypeScript or JavaScript service can call: the handler
forms and their arguments, the seven `std:*` modules (`std:sqlite` is also importable as `bun:sqlite`), `Deno.env` and the global Web APIs. Each function lists its
parameters, result, limits and error codes. For a guided introduction, read [Writing Deno services](../guides/deno.md);
WASM components get the same capabilities through [`std:host`](wit.md), mapped [at the end of this page](#wasm-equivalents).

## Conventions

- Import the modules by name: `import { kv } from "std:kv"`. They are built into the runtime, never bundled.
- Every module function is `async` and returns a `Promise`, except `std:sqlite`, which is synchronous like `bun:sqlite`.
- A failed call rejects with an `Error` whose `message` and `code` are the same stable code (for example
  `kv_key_too_large`). Codes never contain paths, SQL driver text or upstream details.
- Data is per service. Sizes are in bytes of the UTF-8 encoding.
- Limits are the defaults; the operator's plans can change storage quotas.

## Handler forms

The entry module's default export is a function or an object.

```ts
// Form A
export default async function (req: Request, ctx: Ctx): Promise<Response>;

// Form B: every member optional, at least one present
export default {
  fetch(req: Request, ctx: Ctx): Response | Promise<Response>,
  scheduled(event: ScheduledEvent, ctx: Ctx): void | Promise<void>,
  queue(batch: QueueBatch, ctx: Ctx): void | Promise<void>,
  pubsub(msg: PubsubMessage, ctx: Ctx): unknown | Promise<unknown>,
  stream(batch: StreamBatch, ctx: Ctx): void | Promise<void>,
  webSocketMessage(ws: Socket, message: string | ArrayBuffer, ctx: Ctx): void | Promise<void>,
  webSocketClose(ws: Socket, code: number, reason: string, wasClean: boolean, ctx: Ctx): void | Promise<void>,
  webSocketError(ws: Socket, error: Error & { code: string }, ctx: Ctx): void | Promise<void>,
};

// A durable service ("durable": true): a class, one instance per object
export default class Room {
  constructor(state: { id: { name: string }; name: string; storage: Storage; blockConcurrencyWhile(fn) }, env: Env);
  // fetch, webSocketMessage, webSocketClose, webSocketError as above, and:
  alarm(info: { scheduledTime: number; retryCount: number }, ctx: Ctx): void | Promise<void>;
}
```

| Trigger | Called | Missing |
|---|---|---|
| `http`, `call` | `fetch` (Form A: the function) | `500 guest_error` |
| `interval`, `cron` | `scheduled(event, ctx)`; returning normally = success | `fetch(POST http://localhost/__rt/scheduled)` with JSON `{scheduleId, scheduledTime, payload}`; the status is logged |
| `queue` | `queue(batch, ctx)` | `fetch(POST http://localhost/__rt/queue)` with JSON `{messages: [{id, body, attempt}]}`; `2xx` acks all, anything else retries all |
| `pubsub` | `pubsub(msg, ctx)`; the return value is the reply to a `request()` | `fetch(POST http://localhost/__rt/pubsub)` with the message as JSON; the `2xx` body is the reply |
| `stream` | `stream(batch, ctx)` | `fetch(POST http://localhost/__rt/stream)` with JSON `{topic, group, partition, attempt, messages}`; see `std:stream` below |
| `websocket` | `webSocketMessage` / `webSocketClose` / `webSocketError` | the event is done |
| `alarm` | `alarm(info, ctx)` of a durable class | the alarm is done |

`fetch` must return a `Response`; any other value is a `TypeError` (`500 guest_error`). A throw or a rejected promise
in any handler fails the task; for `queue` that redelivers the whole batch, for `stream` it redelivers the batch from
its first uncommitted record.

## Types

### `Ctx`

| Field | Type | Description |
|---|---|---|
| `taskId` | `string` | id of this task (`task_…`), also in the `x-rt-task-id` response header and on every log line |
| `service` | `{ id: string; slug: string; version: number }` | the service and the version that runs |
| `trigger` | `"http" \| "interval" \| "cron" \| "schedule" \| "queue" \| "call" \| "websocket" \| "alarm" \| "pubsub" \| "stream"` | what started the task |
| `invocation` | `{ id: string; attempt: number } \| undefined` | an async HTTP invocation (`http` trigger): its `inv_…` id and 1-based attempt. `undefined` for every other request; built only from the runtime's headers |
| `object` | `{ name: string } \| undefined` | the object of a socket event, an alarm or a request to a durable object |
| `storage` | `Storage \| undefined` | that object's storage ([`std:durable`](#stddurable)) |
| `invoker` | `string \| undefined` | the `x-rt-invoker` header, only for requests on the internal listener |
| `client` | `{ ip: string } \| undefined` | the client address (same as `x-rt-client-ip`); `http` trigger only, `undefined` for schedules, queues, calls and the internal Unix socket |
| `deadline` | `number` | epoch milliseconds when the wall limit runs out |
| `waitUntil(p)` | `(p: Promise<unknown>) => void` | keep the task alive for `p` after the response, bounded by the wall limit; a rejection is logged as an error |

### `ScheduledEvent`

| Field | Type | Description |
|---|---|---|
| `scheduleId` | `string` | the schedule's id (`sch_…`) |
| `scheduledTime` | `number` | epoch milliseconds the fire was due |
| `payload` | `unknown` | the schedule's JSON payload (`{}` unless set) |

### `QueueBatch` and `QueueMessage`

`QueueBatch` is `{ messages: QueueMessage[] }`, at most `queue.batch_size` (10) messages.

| Member | Type | Description |
|---|---|---|
| `id` | `string` | message id (`msg_…`) |
| `body` | `unknown` | the body parsed as JSON; a body that is not valid JSON arrives as a string |
| `attempt` | `number` | 1 on the first delivery |
| `ack()` | `() => void` | mark as done; a later `retry()` of this message is ignored |
| `retry(o?)` | `(o?: { delaySeconds?: number }) => void` | deliver again, after `delaySeconds` or after the backoff `2 s × 2^(attempt-1)` ±20 % |

Messages neither acked nor retried are acked when the handler returns normally. After `queue.max_attempts` (5)
failed deliveries a message goes to the dead-letter queue. Delivery is at-least-once and unordered.

## Request and response

| Aspect | Rule |
|---|---|
| `req.url` | `http://localhost/<path>?<query>`; the `/s/{slug}` prefix is removed and reported in `x-rt-base-path` |
| Added request headers | `x-rt-trigger`, `x-rt-task-id`, `x-rt-base-path` (path routes), `x-rt-client-ip` (`http` trigger, when the address is known), `traceparent`, `x-rt-invoker` (internal listener) |
| Removed request headers | every client `x-rt-*` (including `x-rt-key`), `Host`, hop-by-hop headers, and `Authorization` when it is `Bearer rtk_…` (a tenant API key, on every service) or, on an `access: key` service, the root token that passed the access check; any other `Authorization` passes unchanged |
| Client address | the TCP peer, or behind a proxy in `ingress.trusted_proxies` the first `X-Forwarded-For` hop, counted from the right, that is not a trusted proxy; the raw `X-Forwarded-For` is passed on as received |
| `req.body` | a `ReadableStream` read while the client sends; past `limits.max_body_bytes` (10 MB) reads fail with `request_too_large` |
| Response body | `Uint8Array` chunks, streamed as produced, up to 100 MB |
| Added response headers | `x-rt-task-id`, `x-rt-version`, `server-timing` |

## `std:kv`

```ts
import { kv } from "std:kv";
```

Values are JSON-serializable data or a `Uint8Array` (stored with a type tag, returned as the same type). Keys are
non-empty strings of at most 2 KB; values are at most 1 MB.

### `kv.get`

```ts
kv.get<T = unknown>(key: string): Promise<T | null>
```

The value, or `null` when the key does not exist or has expired.

### `kv.getMany`

```ts
kv.getMany<T = unknown>(keys: string[]): Promise<(T | null)[]>
```

The values in the order of `keys`, `null` for missing ones.

### `kv.set`

```ts
kv.set(key: string, value: unknown, opts?: { ttlSeconds?: number }): Promise<void>
```

Writes the value. With `ttlSeconds` the key expires after that many seconds (expired keys read as `null` and are
swept in the background). A value that `JSON.stringify` cannot encode throws a `TypeError`.

### `kv.delete`

```ts
kv.delete(key: string): Promise<boolean>
```

`true` if the key existed.

### `kv.list`

```ts
kv.list<T = unknown>(opts?: { prefix?: string; cursor?: string | null; limit?: number })
  : Promise<{ entries: { key: string; value: T }[]; cursor: string | null }>
```

Keys in lexicographic order. `limit` defaults to 100 and is clamped to 1 to 1,000. Pass the returned `cursor` to get
the next page; it is `null` on the last page.

### `kv.incr`

```ts
kv.incr(key: string, delta?: number, opts?: { ttlSeconds?: number }): Promise<number>
kv.incr(key: string, delta: bigint, opts?: { ttlSeconds?: number }): Promise<bigint>
```

Atomically adds `delta` (default `1`, may be negative) to the integer stored at `key` and returns the new value. The
read and the write run in one transaction on the service's single SQLite writer, so concurrent increments from any
number of tasks never lose an update.

- A missing or expired key starts at 0.
- `ttlSeconds` applies only when the call **creates** the counter; later increments keep that expiry. This gives
  fixed windows: ``kv.incr(`rl:${ip}`, 1, { ttlSeconds: 60 })`` counts one minute from the first hit.
- `delta` is a safe integer `number` (`Number.isSafeInteger`) or a `bigint` in the signed 64-bit range. With a
  `bigint` delta the result is a `bigint`; results beyond ±2^53 are exact only that way.
- The counter is stored as JSON, so `kv.get` returns it as a number and `kv.set(key, 0)` resets it.
- Key limits apply as for `set`; the storage quota is charged as for a 20-byte value.

| Code | Meaning |
|---|---|
| `kv_not_integer` | the stored value is not a JSON integer in the signed 64-bit range (the value is unchanged) |
| `kv_overflow` | the result would leave the signed 64-bit range |
| `validation_failed` | `delta` is not a safe integer or an in-range `bigint` |

### `std:kv` errors

| Code | When |
|---|---|
| `validation_failed` | empty key |
| `kv_key_too_large` | key (or `list` prefix) over 2 KB |
| `kv_value_too_large` | value over 1 MB |
| `storage_quota` | the service's storage quota is used up |
| `kv_not_integer`, `kv_overflow` | `incr` only, see above |

## `std:sql`

```ts
import { sql } from "std:sql";
```

One SQLite database per service. Statements use `?` placeholders; there are no tagged templates.

### Values

| JavaScript argument | Stored as |
|---|---|
| `null`, `undefined` | `NULL` |
| `boolean` | `INTEGER` 0 / 1 |
| `number` | `INTEGER` if it is an integer, else `REAL` |
| `bigint` | `INTEGER` (64-bit) |
| `string` | `TEXT` |
| `Uint8Array` | `BLOB` |
| any other object or array | `TEXT` holding its JSON |

| SQLite value | JavaScript result |
|---|---|
| `NULL` | `null` |
| `INTEGER` | `number`; a `bigint` outside the safe-integer range |
| `REAL` | `number` |
| `TEXT` | `string` |
| `BLOB` | `Uint8Array` |

### `SqlResult`

```ts
interface SqlResult {
  columns: string[];
  rows: unknown[][];              // arrays in `columns` order, not objects
  rowsAffected: number;
  lastInsertRowid: number | bigint | null;
}
```

### `sql.execute`

```ts
sql.execute(sql: string, args?: SqlArg[]): Promise<SqlResult>
```

Runs one statement. Read-only statements run on read connections; anything that writes runs on the service's single
writer.

### `sql.batch`

```ts
sql.batch(stmts: (string | { sql: string; args?: SqlArg[] })[], mode?: "read" | "write"): Promise<SqlResult[]>
```

Runs all statements in one transaction and returns one result per statement, in order. `"read"` uses a read-only
transaction (a statement that writes fails with `sql_error`); any other value, including none, is `"write"`.

### `sql.transaction`

```ts
sql.transaction<T>(fn: (tx: { execute(sql: string, args?: SqlArg[]): Promise<SqlResult> }) => Promise<T>): Promise<T>
```

An interactive transaction. It commits when `fn` resolves (and resolves with `fn`'s value) and rolls back when `fn`
throws (and rethrows). It is capped at 5 seconds: past that it rolls back and rejects with an `Error` whose message is
`sql_timeout`. Calling `tx.execute` after the end throws `transaction is finished`.

### `std:sql` limits and errors

| Limit | Value |
|---|---|
| Time per statement | `storage.sql_budget_ms`, 1,000 ms by default |
| Result size | 10,000 rows per statement, 10 MB of values per call |
| Statement text | 1 MB |
| Bound parameters | 10,000 |

The SQLite authorizer refuses `ATTACH`, `load_extension`, the runtime's own `_rt_*` tables and pragmas that change
durability, limits or file layout (`journal_mode`, `synchronous`, `page_size`, `writable_schema`, …).

| Code | When |
|---|---|
| `sql_error` | syntax error, constraint violation, a write in a `"read"` batch |
| `sql_timeout` | a statement ran over its budget, or a transaction over 5 s |
| `sql_not_authorized` | the statement touches something the authorizer refuses |
| `sql_result_too_large` | over 10,000 rows or 10 MB of results |
| `storage_quota` | the service's storage quota is used up |

## `std:sqlite` (synchronous SQLite)

```ts
import { Database } from "std:sqlite"; // or "bun:sqlite": the same module
const db = new Database();
const row = db.query("SELECT * FROM todos WHERE id = ?").get(1); // no await
```

The service's `std:sql` database with [bun:sqlite](https://bun.sh/docs/api/sqlite)'s synchronous API. The statement
runs on the task's own thread. When to prefer it over `std:sql`:
[`std:sqlite` or `std:sql`?](../guides/sub-services.md#stdsqlite-or-stdsql).

| Call | Result |
|---|---|
| `new Database(options?)` | The service's own database; no path. A path or `":memory:"` throws `not_supported`. Options: `strict` (named parameters without the prefix; a missing one throws), `safeIntegers` (`INTEGER` results as `bigint`), `readonly` (writes throw `SQLITE_READONLY`) |
| `db.query(sql)` | A cached `Statement` (up to 20 by text) |
| `db.prepare(sql)` | A new `Statement`, not cached |
| `stmt.all(...params)` | Rows as objects |
| `stmt.get(...params)` | The first row, or `null` |
| `stmt.values(...params)` | Rows as arrays |
| `stmt.run(...params)` | `{ changes, lastInsertRowid }` |
| `stmt.iterate(...params)` | An iterator over the rows; the statement runs to completion first, under the result caps |
| `stmt.as(Class)`, `stmt.columnNames`, `stmt.paramsCount`, `stmt.finalize()`, `stmt.toString()` | As in bun (`as` sets the rows' prototype without calling the constructor) |
| `db.run(sql, ...params)`, `db.exec(sql)` | `{ changes, lastInsertRowid }`; without parameters the text may hold several statements |
| `db.transaction(fn)` | A function running `fn` between `BEGIN` and `COMMIT`, rolled back when it throws. `.deferred`, `.immediate`, `.exclusive` pick the `BEGIN` mode; nested, it is a `SAVEPOINT`. `fn` must be synchronous: a returned promise is rolled back and throws `TypeError` |
| `db.inTransaction`, `db.close()` | `close` drops the cached statements; the connection stays for the next request |

**Parameters**: positional (`?`, `?NNN`) as arguments or one array; named (`$x`, `:x`, `@x`) as one object. `null` and
`undefined` → NULL, `number` (a whole number within ±2^53 is an `INTEGER`, else `REAL`), `bigint`, `boolean` → 0/1,
`string`, `Uint8Array` → BLOB; anything else is a `TypeError`. Rows hold numbers, strings, `null` and `Uint8Array`.

**Errors** are `SQLiteError { message, code, errno, byteOffset, rtCode }`: `code` is SQLite's name
(`SQLITE_CONSTRAINT_UNIQUE`), `rtCode` the platform's stable code.

| Limit | Value | `rtCode` |
|---|---|---|
| Time per statement | 100 ms of SQLite time | `sql_timeout` |
| Wait for the write lock (held by an `std:sql` transaction or batch, a KV or blob commit) | 20 ms | `sql_busy` |
| Result size | 10,000 rows, 10 MiB | `sql_result_too_large` |
| Database size | the tenant's remaining `storage_mb` | `storage_quota` |
| Authorizer | as `std:sql`, plus `BEGIN` / `SAVEPOINT` / `COMMIT` in SQL text | `sql_not_authorized` |

Not supported: `serialize`, `deserialize`, `loadExtension`, `fileControl` (throw `not_supported`), `declaredTypes`,
`columnTypes`. An allowed `PRAGMA` (such as `foreign_keys`) applies to the connection of one processor: run it at
module top level.

## `std:blob`

```ts
import { blob } from "std:blob";
```

Objects stored on the node's disk, streamed in both directions. Keys are non-empty strings of at most 1 KB; an object
is at most 100 MB.

### `blob.put`

```ts
blob.put(key: string, body: string | Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | Blob,
         opts?: { contentType?: string }): Promise<void>
```

Writes (or replaces) the object. A `ReadableStream` is consumed chunk by chunk, never buffered whole, so
`blob.put(key, req.body)` stores an upload of any size up to the limit. Any other body type is a `TypeError`.

### `blob.get`

```ts
blob.get(key: string): Promise<{ body: ReadableStream<Uint8Array>; size: number; contentType: string | null } | null>
```

The object as a stream, or `null`. Return `body` in a `Response` to send it without buffering.

### `blob.head`

```ts
blob.head(key: string): Promise<{ size: number; contentType: string | null } | null>
```

### `blob.delete`

```ts
blob.delete(key: string): Promise<boolean>
```

`true` if the object existed.

### `blob.list`

```ts
blob.list(opts?: { prefix?: string; cursor?: string | null; limit?: number })
  : Promise<{ objects: { key: string; size: number; contentType: string | null }[]; cursor: string | null }>
```

`limit` defaults to 100 and is clamped to 1 to 1,000; `cursor` is `null` on the last page.

### `std:blob` errors

| Code | When |
|---|---|
| `validation_failed` | empty key |
| `blob_key_too_large` | key (or prefix) over 1 KB |
| `storage_quota` | the storage quota is used up, or the object is over 100 MB |
| `storage_error` | the write stream failed; a streamed body that passes the size limit can also end this way |

## `std:queue`

```ts
import { enqueue } from "std:queue";

enqueue(service: string, body: unknown, opts?: { delaySeconds?: number; dedupId?: string }): Promise<{ id: string }>
```

Sends a message to the queue of the service with slug `service`, in the same tenant (the current service included).

| Parameter | Description |
|---|---|
| `body` | a `Uint8Array` is sent as is; anything else is sent as JSON. At most 1 MB |
| `delaySeconds` | deliver no earlier than this, at most 7 days |
| `dedupId` | a second message with the same id within 24 hours is dropped |

Returns the message id (`msg_…`). The target's `queue` trigger must be on, its plan must allow queues and its
`call.allowed_callers` must admit the caller.

| Code | When |
|---|---|
| `not_found` | no service with that slug |
| `cross_tenant_call` | another tenant's service, and the plan does not allow cross-tenant calls |
| `forbidden` | the target's `queue` trigger is off |
| `trigger_not_allowed` | the target's plan does not allow queues |
| `call_not_allowed` | the target's caller rules do not admit this service |
| `queue_full` | the target's queue is at the plan's `max_queue_depth` |
| `call_failed` | the message was refused (for example over 1 MB, or a delay over 7 days) or could not be stored |

## `std:pubsub`

```ts
import { publish, request } from "std:pubsub";

// Fire and forget. Resolves after routing, not after delivery.
publish(subject: string, body: unknown | Uint8Array, opts?: { headers?: Record<string, string> }):
  Promise<{ receivers: number; dropped: number }>;

// One responder; the first answer wins.
request(subject: string, body: unknown | Uint8Array, opts?: { headers?: Record<string, string>; timeoutMs?: number }):
  Promise<unknown>;
```

| Parameter | Description |
|---|---|
| `subject` | concrete `.`-separated tokens (`orders.42.created`), 256 bytes and 16 tokens at most; no wildcards, not starting with `_rt` |
| `body` | a `Uint8Array` is sent as is; anything else as JSON. At most 1 MB |
| `headers` | at most 32 headers, 16 KiB in total; names `[a-z0-9][a-z0-9-]{0,63}`, not starting with `x-rt-` |
| `timeoutMs` | `request` only: default 5000, max 30000, never past the task deadline |

`publish` returns `receivers` (deliveries handed to the scheduler) and `dropped` (deliveries refused). A delivery is
dropped, and counted, when the publish has more than 100 matching subscribers (the start point rotates between
publishes), when the subscriber service already has 64 deliveries in flight, when the tenant's quota is spent for that
delivery, or when admission refuses it. Delivery is at-most-once and in memory: a restart or drain loses it.

`request` picks one responder: the first matching group by name and its next member in turn, or else the next
ungrouped subscriber in turn. The reply is parsed JSON when the reply says so, a string for `text/*`, bytes otherwise,
`undefined` when empty.

Subscriber (Form B), declared in the service config under `events.subscribe` (a subject pattern with `*` and `>`,
and an optional `group`; each group shares its messages across its members):

```ts
export default {
  async pubsub(msg: PubsubMessage, ctx: Ctx): Promise<unknown | void> {
    // the return value is the reply when msg.replyable (a request); ignored for publish
  },
};

interface PubsubMessage {
  id: string;                  // msg_…
  subject: string;             // the concrete subject that was published (never a wildcard)
  body: unknown;               // parsed JSON, else a string
  headers: Record<string, string>;
  replyable: boolean;          // true for request()
  publishedAt: number;         // epoch ms
}
```

A reply from `pubsub()`: `undefined` gives `204`, a `Response` is used as is, `Uint8Array` is
`application/octet-stream`, a `string` is `text/plain; charset=utf-8`, anything else is `application/json`. A throw
makes the requester get `responder_failed`. A service that subscribes to a subject it publishes also gets its own
messages. Publish chains are limited to 15 hops (`call_depth`).

| Code | Call | When |
|---|---|---|
| `validation_failed` | both | bad subject, wildcard in a published subject, `_rt` subject, bad header or option |
| `too_large` | both | body over 1 MB, headers over 32 or 16 KiB, reply over 1 MB |
| `unavailable` | both | outside a task, or the node runs without the event bus |
| `rate_limited` / `quota_exceeded` | both | the tenant's rate bucket or its daily invocations (checked like an invoke) |
| `call_depth` | both | called from a task already 15 hops deep |
| `no_responders` | `request` | nothing subscribes to a matching subject (returned at once) |
| `timeout` | `request` | no reply within `timeoutMs` or before the deadline |
| `responder_failed` | `request` | the responder threw, returned non-2xx, or was killed |
| `overloaded` / `queue_timeout` / `shed` / `memory` / `draining` | `request` | the responder's task was refused |

## `std:stream`

```ts
import { append, appendBatch, read, info } from "std:stream";

append(topic: string, body: unknown | Uint8Array, opts?: {
  key?: string;                // at most 256 bytes; the same key always goes to the same partition
  headers?: Record<string, string>;
  dedupId?: string;            // at most 128 bytes; same id on the topic within 2 min returns the original position
}): Promise<{ partition: number; offset: number; duplicate: boolean }>;

// 1 to 100 records, 4 MiB in total, one transaction: all or nothing.
appendBatch(topic: string, records: Array<{ body: unknown | Uint8Array; key?: string;
  headers?: Record<string, string>; dedupId?: string }>): Promise<Array<{ partition: number; offset: number; duplicate: boolean }>>;

// Replay without a consumer group. At most one of `from` / `fromTime`.
read(topic: string, opts?: {
  partition?: number;          // default 0; required when the topic has more than one partition
  from?: number | "earliest" | "latest";   // default "earliest"
  fromTime?: number;           // epoch ms: the first record at or after it
  limit?: number;              // 1 to 100, default 100 (and at most 1 MiB of bodies)
}): Promise<{ records: StreamRecord[]; next: number; end: number }>;  // next = offset after the last record; end = the partition's next offset

info(topic: string): Promise<{
  name: string;
  partitions: Array<{ partition: number; first: number; next: number }>;
  retentionHours: number;
  maxMb: number;               // 0 = only the tenant cap applies
  bytes: number;
}>;

interface StreamRecord {
  partition: number;
  offset: number;
  key: string | null;
  body: unknown;               // parsed JSON, else a string
  headers: Record<string, string>;
  timestamp: number;           // epoch ms, non-decreasing within a partition
}
```

A topic is created with the control API, not by the service; a topic name may not start with `tpc_`. Offsets are dense
per partition from 0. A partition is `fnv1a64(key) % partitions` when a key is given (partitions never change), else
round robin.

Consumer (Form B), declared under `events.consume` in the service config (topic, group, `start`, `batch_size` 1 to
1000, `max_attempts` 1 to 100, `on_exhausted` `dlq` / `skip` / `block`):

```ts
export default {
  async stream(batch: StreamBatch, ctx: Ctx): Promise<void> {
    for (const m of batch.messages) {
      try { await apply(m); }
      catch (e) { batch.failAt(m); return; }   // commit before m, redeliver from m
    }
  },                                            // a normal return commits the whole batch
};                                              // a throw commits nothing and retries

interface StreamBatch {
  topic: string;
  group: string;
  partition: number;
  attempt: number;             // 1 on the first delivery
  messages: StreamRecord[];    // ordered by offset, contiguous
  failAt(m: StreamRecord | number): void;   // the lowest offset passed wins
}
```

Delivery is at-least-once, in order per partition, with one batch in flight per consumer cursor. A record that keeps
failing gets `max_attempts` deliveries, and the batch is then narrowed to that one record, so records next to it are
never dead-lettered. After the last attempt, `dlq` copies the record to the consumer's dead letters, `skip` moves on
(logged), and `block` stops the cursor until it is reset. Retention is independent of consumption: records older than
the topic's retention, or beyond its size cap, are removed, and a consumer behind them moves to the first retained
record.

| Code | Call | When |
|---|---|---|
| `validation_failed` | all | bad topic, key, dedup id, header or option; a `read` offset or partition out of range; `read` without `partition` on a multi-partition topic (the message names the partition count) |
| `too_large` | all | body over 1 MB, batch over 100 records or 4 MiB, headers over 32 or 16 KiB |
| `unavailable` | all | outside a task, or the node runs without the event bus |
| `not_found` | all | no topic of that name in the service's namespace |
| `stream_full` | `append` | the tenant's topics hold its stream quota (`max_stream_mb`, dead letters included) |
| `rate_limited` / `quota_exceeded` | `append` | the tenant's rate bucket or daily invocations (one check per call) |

## `std:services`

```ts
import { call, services, ServiceError } from "std:services";

services.call(slug: string, method: string, arg?: unknown): Promise<unknown>      // typed RPC
services.bind(slug: string): Proxy                                                // await proxy.method(arg)
services.fetch(slug: string, request: Request | string | URL, init?: RequestInit): Promise<Response>
call(slug, request, init?)                                                        // = services.fetch
```

`services.call` runs the callee's `rpc[method](arg, ctx)` (a Deno `export const rpc`, or a WASM component's `rpc`
export) and returns its JSON result; failures throw `ServiceError` with `code` and `message`. `21ci types <slug>` writes
a `.d.ts` with an interface to cast `services.bind(slug)` to. See [Service calls](../guides/calls-and-queues.md).

`services.fetch` (and `call`) runs the service `slug` of the same tenant in-process, as a child task with trigger `call`, and resolves to its
response. `request` and `init` are what you would pass to `new Request()`; only the method, path, query, headers
(except `x-rt-*`) and body are used, the host is ignored. The request body is buffered; the response body streams.

- The child's wall limit is the smaller of its own and the caller's remaining time.
- A child that fails (throws, times out, runs out of memory) is a `5xx` response with `x-rt-reason`, not an exception.
- Calls nest up to 15 deep. A deeper call resolves to `508` with `x-rt-reason: call_depth`.
- The target's `call` trigger must be on and its `call.allowed_callers` (or, when empty, its environment) must admit
  the caller. See [Calls and queues](../guides/calls-and-queues.md).

| Code | When |
|---|---|
| `not_found` | no service with that slug |
| `cross_tenant_call` | another tenant's service, and the plan does not allow cross-tenant calls |
| `forbidden` | the target's `call` trigger is off |
| `call_not_allowed` | the target's caller rules do not admit this service |
| `trigger_not_allowed` | the target's plan does not allow calls |
| `call_failed` | the call could not be made |
| scheduler codes (`overloaded`, …) | the node refused to start the child task |

## `std:platform`

```ts
import { platform } from "std:platform";

platform.fetch(binding: string, request: Request | string | URL, init?: RequestInit): Promise<Response>
```

Operator SDK services only: a tenant service gets `403 binding_not_allowed` and reaches the operator's API through an
[SDK](#operator-sdks). Calls the operator binding named `binding` (`[[binding]]` in the node configuration). Only the method, path, query,
headers and body are used; the node sends them to the binding's `base_url`, drops your `authorization`, `cookie` and
`x-rt-*` headers and adds the operator's credentials (and, if configured, a header naming your tenant). The
credentials never reach your code; an upstream `Set-Cookie` is dropped from the response.

It never rejects for a refusal. Refusals are responses with `x-rt-reason`:

| Status | `x-rt-reason` | When |
|---|---|---|
| 403 | `binding_denied` | method or path not allowed, or a `..` segment |
| 403 | `binding_not_allowed` | your plan may not use the binding |
| 404 | `binding_not_found` | no such binding |
| 429 | `rate_limited` | the binding's per-tenant rate limit |
| 502 | `binding_upstream` | the upstream failed |

See [Operator SDKs and bindings](../guides/operator-sdks.md).

## Operator SDKs

```ts
import { acme, sdk, SdkError } from "std:sdk";        // one named export per SDK published on the node
import acme2, { getPlatformInfo } from "sdk:acme";     // the same SDK: default export plus its top-level members

await acme.invoices.create({ amount: 100 });           // method "invoices.create"
await sdk("acme").getPlatformInfo();                    // a name chosen at run time
```

An operator SDK runs in the runtime, not in your service: your build gets a shim of its contract, and each call is
bound to the release your grants select. The argument and the result are JSON, at most 256 KiB each. Failures throw
`SdkError` (a `ServiceError`) with `code`: `sdk_not_found`, `sdk_not_granted`, `sdk_unavailable`,
`sdk_version_unavailable`, `method_not_found`, `validation_failed`, `call_depth`, `timeout`, or the SDK's own. An
`sdk:` import of an SDK that is not published is refused at deploy with `422 validation_failed`. Types: `21ci sdk types
<name>`. See [Use operator SDKs](../guides/use-sdks.md).

`std:sdk/server` exports `defineSdk({ methods, authorize? })`, for writing an SDK (operators only; see
[Operator SDKs and bindings](../guides/operator-sdks.md#write-your-own-sdk)).

## `std:sockets`

WebSockets of the service, held by the node ([guide](../guides/websockets.md)). Calls work in every task of the
service; without `object` they address the task's object (a socket event, an alarm, a request to a durable object),
else every object of the service.

```ts
import { sockets, type Socket } from "std:sockets";
```

| Call | Description |
|---|---|
| `accept(req, { tags?, attachment?, protocol? }): Response` | Accept an upgrade; `fetch` must return the result (status 101). `res.socket` is the new socket. Only in an `http` or `call` task whose request offered the upgrade (`not_upgradable`) |
| `send(id, data, { immediate? })` | Text (`string`) or binary (`ArrayBuffer` or a view), at most 1 MiB |
| `broadcast(data, { object?, tag?, except?, immediate? }): { sent }` | `except`: a socket id or ids |
| `close(id, code = 1000, reason = "")` | Codes 1000 or 3000-4999; reason at most 123 bytes |
| `closeAll({ object?, tag?, code?, reason? }): { closed }` | |
| `list({ object?, tag?, limit?, cursor? }): { sockets: Socket[], cursor? }` | `limit` at most 1,000 |
| `get(id): Socket \| null`, `count({ object?, tag? }): number` | |
| `objects({ prefix?, limit?, cursor? }): { objects: [{ name, sockets }], cursor? }` | Objects holding sockets |
| `setTags(id, tags)` | At most 10 tags of 256 bytes |
| `setAttachment(id, value)`, `getAttachment(id)` | A structured clone, at most 16 KiB |
| `setAutoResponse({ request, response } \| null, { object? })` | The node answers that exact text message itself (each at most 1 KiB) |

`Socket`: `{ id, object, tags, protocol, createdAt, lastMessageAt, autoResponseAt, bufferedAmount, remoteIp,
attachment, send(data), close(code?, reason?), serializeAttachment(v), deserializeAttachment() }`.

Sends and storage writes of a task that acts for an object take effect when its handler returns: storage commits,
then the messages go out; a throw drops both. `{ immediate: true }` sends at once.

| Code | When |
|---|---|
| `not_found` | no such socket in this service |
| `invalid_request` | a bad id, tag, code or reason |
| `too_large` | a message over 1 MiB, an attachment over 16 KiB |
| `limit_exceeded` | a socket cap of the node, the plan, the object or the service; over 1,000 socket operations in one task |
| `slow_consumer` | 1 MiB already queued for the client: the socket was closed with 1013 |
| `not_upgradable` | `accept` outside a request that offered an upgrade |
| `unavailable` | outside a task |

## `std:durable`

```ts
import { durable, storage } from "std:durable";
```

| Call | Description |
|---|---|
| `durable.get(name, { service? }): { name, id, fetch(input, init?) }` | A durable object by name (1 to 256 bytes) of `service` (a durable service of the tenant, call rules apply; default this one). `fetch` runs the class's `fetch`, an upgrade included. `not_durable` for a plain service |
| `durable.idFromName(name)`, `durable.newUniqueId()` | The name itself; a random unique name |
| `storage.get(key)`, `storage.get(keys): Map` | The task's own writes first |
| `storage.put(key, value)`, `storage.put(entries)` | Structured clones; key at most 2 KiB, value at most 1 MiB, at most 1,000 writes per task |
| `storage.delete(key): boolean`, `storage.delete(keys): number`, `storage.deleteAll()` | `deleteAll` also clears the alarm |
| `storage.list({ prefix?, start?, end?, limit?, reverse? }): Map` | `[start, end)`, at most 1,000 |
| `storage.getAlarm(): number \| null`, `storage.setAlarm(Date \| ms)`, `storage.deleteAlarm()` | One alarm per object, at least once |

`storage` is the same object as `ctx.storage` and the class's `state.storage`. All writes of a task commit together
when its handler returns. Codes: `no_object` (no object in this task), `kv_key_too_large`, `kv_value_too_large`,
`storage_quota`, `limit_exceeded` (writes per task), `not_durable`.

## `Deno.env`

`Deno` holds only `env`. Env variables and secrets appear together.

| Member | Result |
|---|---|
| `Deno.env.get(name: string)` | `string \| undefined` |
| `Deno.env.has(name: string)` | `boolean` |
| `Deno.env.toObject()` | `Record<string, string>` with every variable and secret |
| `Deno.env.set()`, `Deno.env.delete()` | throw `TypeError: environment variables are read-only` |

Reads throw an `Error` with message `env_unavailable` if the node could not load the env for this task. Names match
`[A-Z_][A-Z0-9_]*` (no `RT_` prefix), at most 100 env variables and 100 secrets per service, values at most 32 KB.
Changes apply to the next task.

## Global Web APIs

| Area | Globals |
|---|---|
| Fetch | `fetch`, `Request`, `Response`, `Headers`, `FormData` |
| URL | `URL`, `URLSearchParams`, `URLPattern` |
| Streams | `ReadableStream`, `ReadableStreamDefaultReader`, `ReadableStreamBYOBReader`, `ReadableStreamBYOBRequest`, `ReadableByteStreamController`, `ReadableStreamDefaultController`, `WritableStream`, `WritableStreamDefaultWriter`, `WritableStreamDefaultController`, `TransformStream`, `TransformStreamDefaultController`, `ByteLengthQueuingStrategy`, `CountQueuingStrategy` |
| Encoding | `TextEncoder`, `TextDecoder`, `TextEncoderStream`, `TextDecoderStream`, `atob`, `btoa` |
| Compression | `CompressionStream`, `DecompressionStream` |
| Files | `Blob`, `File`, `FileReader` |
| Events | `Event`, `EventTarget`, `CustomEvent`, `ErrorEvent`, `MessageEvent`, `CloseEvent`, `ProgressEvent`, `PromiseRejectionEvent`, `reportError` |
| Messaging | `MessageChannel`, `MessagePort`, `structuredClone` |
| Timers and abort | `setTimeout`, `setInterval`, `clearTimeout`, `clearInterval`, `AbortController`, `AbortSignal` |
| Crypto | `crypto` (`getRandomValues`, `randomUUID`, `subtle`), `Crypto`, `CryptoKey`, `SubtleCrypto` |
| Performance | `performance`, `Performance`, `PerformanceEntry`, `PerformanceMark`, `PerformanceMeasure` |
| WebSocket client | `WebSocket` (outbound only) |
| Other | `console`, `DOMException`, `WebAssembly`, `Deno.env` |

Removed: `Worker`, `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait`, `Atomics.waitAsync`, `Atomics.notify`,
`ShadowRealm`, `caches`, and every `Deno.*` API except `Deno.env`.

Behavior that differs from a browser or Deno:

- **`fetch`** goes through the egress guard. Failures reject with a `TypeError` whose message is the code:
  `egress_denied`, `egress_limit`, `egress_too_large`, `egress_quota`, `egress_timeout`, `egress_dns`,
  `egress_upstream`, `egress_insecure_redirect`. Request bodies are buffered before sending.
- **`WebSocket`** connects out only (`new WebSocket("wss://…", protocols)`, WHATWG events, `binaryType`), through
  the egress guard like `fetch`: one of the task's fetches, received bytes count against the egress quota, closed
  when the task ends. An `error` event carries `.code` (`egress_denied`, `egress_timeout`, `egress_upstream`,
  `egress_limit`, `egress_quota`). Servers accept sockets with [`std:sockets`](#stdsockets).
- **Clock.** `Date.now()`, `new Date()` and `performance.now()` advance only at I/O and timer boundaries.
- **`console`.** `debug` writes `debug` records, `log` and `info` write `info`, `warn` writes `warn`, `error` writes
  `error`. At most 1,000 lines per task, 10,000 characters per message.
- **`WebAssembly`.** Every memory counts against the task's memory limit; shared memories are refused.
- **Memory.** The heap, every `ArrayBuffer` and `Blob` data count against `memory_mb`. Growing a resizable
  `ArrayBuffer` past the limit throws `RangeError`; anything else over the limit ends the task with `502 oom`.

## Type declarations

The runtime ships no type package. To let a TypeScript-aware editor check your calls, save these ambient
declarations as `rt.d.ts` next to your code; the build ignores `.d.ts` files. They mirror the signatures on this
page; add `client?: { ip: string }` and the other `ctx` fields from [Types](#types) to your own `Ctx` interface.

```ts title="rt.d.ts"
declare module "std:kv" {
  type Ttl = { ttlSeconds?: number };
  export const kv: {
    get<T = unknown>(key: string): Promise<T | null>;
    getMany<T = unknown>(keys: string[]): Promise<(T | null)[]>;
    set(key: string, value: unknown, opts?: Ttl): Promise<void>;
    delete(key: string): Promise<boolean>;
    list<T = unknown>(opts?: { prefix?: string; cursor?: string | null; limit?: number }): Promise<{
      entries: { key: string; value: T }[];
      cursor: string | null;
    }>;
    incr(key: string, delta?: number, opts?: Ttl): Promise<number>;
    incr(key: string, delta: bigint, opts?: Ttl): Promise<bigint>;
  };
}

declare module "std:sql" {
  export type SqlArg = null | undefined | boolean | number | bigint | string | Uint8Array;
  export interface SqlResult {
    columns: string[];
    rows: unknown[][];
    rowsAffected: number;
    lastInsertRowid: number | bigint | null;
  }
  export type Statement = string | { sql: string; args?: SqlArg[] };
  export interface Tx {
    execute(sql: string, args?: SqlArg[]): Promise<SqlResult>;
  }
  export const sql: {
    execute(sql: string, args?: SqlArg[]): Promise<SqlResult>;
    batch(stmts: Statement[], mode?: "read" | "write"): Promise<SqlResult[]>;
    transaction<T>(fn: (tx: Tx) => Promise<T>): Promise<T>;
  };
}

declare module "std:blob" {
  export type BlobBody = string | Uint8Array | ArrayBuffer | ReadableStream<Uint8Array> | Blob;
  export interface BlobMeta { size: number; contentType: string | null }
  export const blob: {
    put(key: string, body: BlobBody, opts?: { contentType?: string }): Promise<void>;
    get(key: string): Promise<(BlobMeta & { body: ReadableStream<Uint8Array> }) | null>;
    head(key: string): Promise<BlobMeta | null>;
    delete(key: string): Promise<boolean>;
    list(opts?: { prefix?: string; cursor?: string | null; limit?: number }): Promise<{
      objects: (BlobMeta & { key: string })[];
      cursor: string | null;
    }>;
  };
}

declare module "std:queue" {
  export function enqueue(
    service: string,
    body: unknown,
    opts?: { delaySeconds?: number; dedupId?: string },
  ): Promise<{ id: string }>;
}

declare module "std:services" {
  export function call(slug: string, request: Request | string | URL, init?: RequestInit): Promise<Response>;
}

declare module "std:platform" {
  export const platform: {
    fetch(binding: string, request: Request | string | URL, init?: RequestInit): Promise<Response>;
  };
}
```

## WASM equivalents

WASM components reach the same capabilities through the `std:host@0.1.0` WIT package ([reference](wit.md)). The
codes are the same; WASM gets them as `error { code, message }` results.

| Deno | WASM (`std:host`) | Differences |
|---|---|---|
| `console.*` | `log.log(level, message)`, or stdout (`info`) / stderr (`error`) | |
| `kv.get` | `kv.get(key)` | keys are bytes; values are `json(string)` or `bytes(list<u8>)` |
| `kv.getMany` | none | call `kv.get` per key |
| `kv.set` | `kv.set(key, value, {ttl-seconds})` | |
| `kv.delete` | `kv.delete(key)` | |
| `kv.list` | `kv.list({prefix, cursor, limit})` | `limit` is required; the page has `next-cursor` |
| `kv.incr` | `kv.incr(key, delta, {ttl-seconds})` | `delta` is required |
| `sql.execute` | `sql.execute({sql, args})` | values are a `sql-value` variant |
| `sql.batch` | `sql.batch(stmts, write)` | `write: bool` instead of `"read" \| "write"` |
| `sql.transaction` | none | use `sql.batch` |
| `std:sqlite` (`Database`) | none | use `sql.execute` and `sql.batch` |
| `blob.put` | `blob.put(key, content-type, data)` | the whole object as bytes; returns its `meta` |
| `blob.get` | `blob.get(key)` | `(meta, bytes)`, buffered, at most 10 MB (`blob_too_large`) |
| `blob.head`, `blob.delete` | `blob.head(key)`, `blob.delete(key)` | |
| `blob.list` | `blob.list(prefix, cursor, limit)` | the page has `items` and `next-cursor` |
| `enqueue` | `queue.enqueue(slug, body, {delay-seconds, dedup-id})` | `body` is bytes; returns the id |
| `call` | `call.call(slug, {method, path-with-query, headers, body})` | buffered response; over 10 MB is `response_too_large` |
| `platform.fetch` | `platform.fetch(binding, request)` | buffered response, same refusal statuses |
| `services.call` | `call.call-rpc(slug, method, args-json)` | JSON text in and out |
| `rpc` export | `rpc.handle(method, args-json, caller)` export (world `rpc-service`) | |
| `sdk(name).method(arg)` | `sdk.invoke(name, method, args-json)` | JSON text in and out |
| `Deno.env` | `wasi:cli/environment` | |
| `fetch` | `wasi:http/outgoing-handler` | errors are `wasi:http` error codes |
| `ctx.taskId`, `ctx.trigger`, `ctx.deadline` | request headers `x-rt-task-id`, `x-rt-trigger`, `x-rt-deadline` | |
| `ctx.client.ip` | request header `x-rt-client-ip` | `http` trigger only |
| `scheduled`, `queue` handlers | `POST /__rt/scheduled`, `POST /__rt/queue` | synthetic requests, like Form A |
| `publish(subject, body, opts)` | `pubsub.publish(subject, body, headers)` | `body` is bytes; returns `{receivers, dropped}` as a record |
| `request(subject, body, opts)` | `pubsub.request(subject, body, headers, timeout-ms)` | the reply is `{content-type, body}` |
| `pubsub(msg, ctx)` handler | `POST /__rt/pubsub` (the 2xx body is the reply) | the message is the same JSON as Deno's `PubsubMessage` |
| `append`, `appendBatch` | `streams.append(topic, entries)` | one call for one or many entries; returns the `appended` list |
| `read(topic, opts)`, `info(topic)` | `streams.read(topic, {partition, start, limit})`, `streams.info(topic)` | `start` is a `position` variant (`earliest`, `latest`, `offset`, `time-ms`); `partition` is always passed |
| `stream(batch, ctx)` handler | `POST /__rt/stream` | a 2xx commits the batch; `x-rt-stream-fail-at: <offset>` stops mid-batch |
| `ctx.invocation` | headers `x-rt-invocation-id`, `x-rt-invocation-attempt` | async HTTP invocations only |
| `sockets.accept(req)` | `sockets.accept(opts)`, then answer status 101 | an upgrade offer is the header `x-rt-websocket: 1` |
| `std:sockets` (`send`, `broadcast`, `close`, `closeAll`, `list`, `get`, `setTags`, attachments, `setAutoResponse`) | `sockets.*` | attachments are raw bytes; no `count` / `objects` |
| `webSocketMessage` / `webSocketClose` / `webSocketError` / `alarm` | export `socket-handler`: `on-message`, `on-close`, `on-error`, `on-alarm` (world `socket-service`) | an `err` or a trap fails the event |
| `storage.*` | `durable.get`, `put`, `delete`, `list`, `delete-all`, `get-alarm`, `set-alarm` | values are bytes |
| `durable.get(name, {service}).fetch(req)` | `durable.fetch(service, object, request)` | |
| `new WebSocket(url)` | `ws-client.connect(url, protocols)`, then `send`, `recv(timeout-ms)`, `close` | |
