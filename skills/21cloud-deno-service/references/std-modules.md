# `std:*` modules (Deno)

Named imports. Every call returns a promise except `std:sqlite` (synchronous), `std:sockets` (synchronous) and
`durable.get` / `idFromName` / `newUniqueId` (synchronous). Failures throw an `Error` with a stable `.code` (no host
paths, no driver text). Implementation: `js/bootstrap.js` and `js/rt_*.js` in https://github.com/21-ci/runtime .

```ts
import { kv } from "std:kv";
import { sql } from "std:sql";
import { Database } from "std:sqlite";           // or "bun:sqlite"; default export is Database too
import { blob } from "std:blob";
import { enqueue } from "std:queue";
import { services, call, ServiceError } from "std:services";
import { schedule } from "std:schedule";
import { lock } from "std:lock";
import { log } from "std:log";
import { secrets } from "std:secrets";
import { sockets } from "std:sockets";
import { durable } from "std:durable";
import { sdk, SdkError } from "std:sdk";        // plus one named export per granted SDK: import { acme } from "std:sdk"
import { platform } from "std:platform";        // operator SDK services only
```

KV, SQL, SQLite and durable storage share one SQLite database per service (`data.db`); blobs live beside it. Each
service sees only its own data. Storage of KV + SQL + blobs + durable objects counts against the plan's `storage_mb`
(`storage_quota` when full; reads and deletes keep working).

## `std:kv`

| Call | Result |
|---|---|
| `kv.get(key)` | value or `null` (expired keys read as `null`) |
| `kv.getMany(keys)` | `(value \| null)[]`, same order; at most 1000 keys and 10 MiB of values |
| `kv.set(key, value, { ttlSeconds? })` | `void` |
| `kv.delete(key)` | `boolean` |
| `kv.list({ prefix?, cursor?, limit?, reverse? })` | `{ entries: [{ key, value }], cursor: string \| null }`; lexicographic (descending with `reverse`); `limit` default 100, max 1000 |
| `kv.incr(key, delta = 1, { ttlSeconds? })` | new value (`number`, or `bigint` for a `bigint` delta). Atomic. A missing/expired key starts at 0; `ttlSeconds` applies only when the call creates the counter (fixed windows) |
| `kv.cas(key, expected, next, { ttlSeconds? })` | `boolean`; writes `next` (`null` deletes) only if the current value equals `expected` (`null` = absent or expired). Exactly one of concurrent swaps wins |
| `kv.expire(key, ttlSeconds \| null)` | `boolean`; set or clear the expiry of a live key; `false` when absent |
| `kv.ttl(key)` | seconds left (rounded up) or `null` |

Values: JSON-serializable or `Uint8Array`. Key 1 byte to 2 KB, value at most 1 MB. `incr` delta: a safe integer or an
`i64` `bigint`. Codes: `validation_failed`, `kv_key_too_large`, `kv_value_too_large`, `storage_quota`, `kv_not_integer`
(`incr` on a non-integer value), `kv_overflow`.

## `std:sql` (async)

| Call | Result |
|---|---|
| `sql.execute(sql, args?)` | `{ columns: string[], rows: any[][], rowsAffected, lastInsertRowid }`. **Rows are arrays** in `columns` order |
| `sql.batch(stmts, "read" \| "write")` | `result[]`; `stmt` = `string \| { sql, args }`; one transaction |
| `sql.transaction(async (tx) => { await tx.execute(sql, args?); ... })` | the callback's value; commits on resolve, rolls back on throw; 5 s cap (`sql_timeout`) |

Args: `null`/`undefined` -> NULL, `boolean` -> 0/1, `bigint`, `Uint8Array` -> BLOB. Denied: `ATTACH`, `load_extension`,
`_rt_*` tables, temp objects, `VACUUM`, schema-writing pragmas, `BEGIN`/`SAVEPOINT` in SQL text. Statement budget
1,000 ms. Results: at most 10,000 rows per statement and 10 MiB per call (page with `LIMIT`/`OFFSET`). Codes: `sql_error`,
`sql_timeout`, `sql_busy`, `sql_not_authorized`, `sql_result_too_large`, `storage_quota`.

## `std:sqlite` (also `bun:sqlite`, synchronous)

Same database as `std:sql`, with bun's synchronous API. Code written for `bun:sqlite` runs unchanged.

```ts
import { Database } from "std:sqlite";
const db = new Database();                         // the service's database; no path
db.run("CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, title TEXT, done INTEGER DEFAULT 0)");
const add = db.query("INSERT INTO todos (title) VALUES ($title)");
const byId = db.query("SELECT * FROM todos WHERE id = ?");
export default () => {
  const { lastInsertRowid } = add.run({ $title: "write docs" });
  return Response.json(byId.get(lastInsertRowid));   // a row object or null
};
```

| Call | Result |
|---|---|
| `new Database(options?)` | options `strict`, `safeIntegers`, `readonly`; any path (also `":memory:"`) throws `not_supported` |
| `db.query(sql)` / `db.prepare(sql)` | `Statement` (`query` caches up to 20 by text) |
| `stmt.all/get/values/run/iterate(...params)` | rows as objects / first row or `null` / rows as arrays / `{ changes, lastInsertRowid }` / iterator |
| `db.run(sql, ...p)` / `db.exec(sql)` | `{ changes, lastInsertRowid }` |
| `db.transaction(fn)` | a function running `fn` in `BEGIN`/`COMMIT` (rollback on throw); `.deferred` / `.immediate` / `.exclusive`; nests as savepoints. `fn` must be synchronous (`async` is a `TypeError`) |

Parameters: `?` / `?NNN` positional, `$x` / `:x` / `@x` named via one object. Errors: `SQLiteError { message, code,
errno, byteOffset, rtCode }` (`code` like `SQLITE_CONSTRAINT_UNIQUE`; `rtCode` as `std:sql` codes).
Differences from `std:sql`: **100 ms per statement**, a write waits at most 20 ms for a lock (`sql_busy`), CPU counts
against the task. Use `.immediate` for write transactions. Run allowed pragmas (`foreign_keys`) at module top level.
Not supported: `serialize`, `deserialize`, `loadExtension`, `fileControl`, `declaredTypes`, `columnTypes`.

## `std:blob`

| Call | Result |
|---|---|
| `blob.put(key, body, { contentType? })` | `void`; body: string, `Uint8Array`, `ArrayBuffer`, `ReadableStream` or Blob-like; streamed |
| `blob.get(key)` | `{ body: ReadableStream, size, contentType } \| null` |
| `blob.head(key)` | `{ size, contentType } \| null` |
| `blob.delete(key)` | `boolean` |
| `blob.list({ prefix?, cursor?, limit? })` | `{ objects: [{ key, size, contentType }], cursor: string \| null }` |

Key at most 1 KB, object at most 100 MB. Codes: `blob_key_too_large`, `storage_quota`, `validation_failed`.
Stream uploads straight through: `await blob.put(key, req.body!, { contentType })`.

## `std:queue`

`enqueue(service, body, { delaySeconds?, dedupId? }) -> { id }`. `service` is the **slug** of a service in the same
tenant (it may be the caller itself: `ctx.service.slug`). `body`: JSON or `Uint8Array`, at most 1 MB. Delay at most
7 days. The same `dedupId` within 24 h is delivered once. The target's `call.allowed_callers` applies (default: same
project and environment). Codes: `not_found`, `forbidden`, `queue_full`.

## `std:services`

| Call | Notes |
|---|---|
| `services.fetch(slug, request, init?) -> Response` (also `call(slug, request, init?)`) | In-process call (no HTTP hop); only path + query of the request URL are used. A failing callee returns a 5xx `Response`, it does not throw. Codes: `not_found`, `cross_tenant_call`, `forbidden`, `call_failed` |
| `services.call(slug, method, arg?) -> value` | RPC to the callee's `export const rpc = { async method(arg, ctx) {...} }` (or `default.rpc`); dotted names reach nested objects. Throws `ServiceError { code, message }` |
| `services.bind(slug) -> proxy` | `await proxy.method(arg)` = `services.call(slug, "method", arg)`; cast to the generated `<Slug>Rpc` type (`21ci types <slug> -d types`) |

RPC errors: a callee's own `ServiceError` keeps its `code`/`message`; any other callee error arrives as
`internal_error` (message only). Host codes: `method_not_found`, `not_found`, `forbidden`, `cross_tenant_call`,
`call_not_allowed`, `trigger_not_allowed`, `call_depth` (past depth 15), `timeout`, `validation_failed` (argument not
JSON or over 256 KiB, bad method name), `response_too_large` (result over 256 KiB). The callee's wall limit is
min(its own, the caller's remaining time). Who may call: the callee's `call.allowed_callers`; empty = services of the
same project and environment. A callee throws its own codes with `throw new ServiceError(code, message)` (see
[sample/apps/events/notify/main.ts](https://github.com/21-ci/runtime/blob/main/sample/apps/events/notify/main.ts)).

## `std:schedule` (one-off tasks of this service)

| Call | Notes |
|---|---|
| `schedule.after(seconds, payload, { id? }) -> { id, runAt }` | Due in `seconds` (at most 30 days). `payload` is JSON, at most 16 KiB. Without `id` one is generated (`sch_…`) |
| `schedule.at(time, payload, { id? }) -> { id, runAt }` | `time`: `Date` or Unix ms, at most 30 days ahead; past = due now |
| `schedule.cancel(id) -> boolean` | `false` when not pending |
| `schedule.list({ cursor?, limit? }) -> { entries: [{ id, runAt }], cursor }` | Pending, in id order, at most 100 per page |

Delivered to `scheduled(event, ctx)` with `event.id`, `event.scheduledAt`, `event.payload`, `ctx.trigger === "schedule"`,
at least once. A new `after`/`at` with a pending id replaces it. Ids: 1 to 128 of `a-z A-Z 0-9 . _ - :`. Codes:
`validation_failed`, `schedule_limit` (1,000 pending per service), `unavailable` (outside a task).

## `std:lock` (per service, per node)

| Call | Notes |
|---|---|
| `lock(key, fn, { waitMs?, leaseMs? }) -> fn's value` | Waits at most `waitMs` (default 10,000, max 300,000) else `lock_timeout`. `leaseMs` (default 30,000, max 900,000) bounds the hold: after it the next waiter gets the key, so `fn` must finish within it |
| `lock.try(key, fn, { leaseMs? }) -> { acquired: false } \| { acquired: true, value }` | Never waits |

Keys 1 to 512 bytes. Released when `fn` settles and when the task ends. In memory: not across nodes, not across
restarts. Codes: `lock_timeout`, `lock_limit` (32 holds per task, 1,024 keys per service, 64 waiters per key),
`validation_failed`.

## `std:log`

`log.debug|info|warn|error(message, fields?)`: a log record with structured fields (a plain object, at most 32
top-level keys and 8 KiB of JSON, else `validation_failed` and nothing is written). `log.with(fields)` returns a child
logger with bound fields (a call's own fields win).

## `std:secrets`

`secrets.get(NAME) -> string | null` (secret values only, never plain env vars). `secrets.require(...NAMES) -> { NAME:
value }` or one error with `code: "secret_missing"` and `missing` (the names). Names `[A-Z_][A-Z0-9_]*`, not `RT_*`
(else `validation_failed`). Set them with `21ci secret set <svc> NAME` (value from stdin or `--from-file`).

## `std:sockets` (synchronous)

| Call | Notes |
|---|---|
| `accept(req, { tags?, attachment?, protocol? }) -> Response` | Return it from `fetch`. `protocol`: a subprotocol the client offered |
| `send(id, data, { immediate? })` | `data`: string, `ArrayBuffer` or a view |
| `close(id, code = 1000, reason = "")` | Code 1000 or 3000-4999; reason at most 123 bytes |
| `broadcast(data, { object?, tag?, except?, immediate? }) -> { sent }` | To the task's object (else `default`), or the one named |
| `closeAll({ object?, tag?, code?, reason? }) -> { closed }` | |
| `list({ object?, tag?, limit?, cursor? }) -> { sockets, cursor? }`, `get(id)`, `count({ object?, tag? })` | Without `object`: the task's object, else every object of the service |
| `objects({ prefix?, limit?, cursor? }) -> { objects: [{ name, sockets }], cursor? }` | Objects holding sockets |
| `setTags(id, tags)`, `setAttachment(id, value)`, `getAttachment(id)` | At most 10 tags of 256 bytes; attachment at most 16 KiB |
| `setAutoResponse({ request, response } \| null, { object? })` | At most 1 KiB each |

Works from any task of the service (HTTP, cron, queue). Codes: `not_found`, `invalid_request`, `too_large`,
`limit_exceeded` (plan, service or node socket caps), `slow_consumer` (1 MiB queued: closed 1013), `unavailable`,
`not_upgradable`.

## `std:durable`

| Call | Notes |
|---|---|
| `durable.get(name, { service? }) -> { name, id, fetch(input, init) }` | `name`: 1 to 256 bytes. `service`: a durable service of the tenant (call rules apply); default this one. `not_durable` for a non-durable service |
| `durable.idFromName(name) -> name`, `durable.newUniqueId() -> string` | Cloudflare-compatible names; objects are addressed by name |
| `storage.get(key)` / `storage.get(keys) -> Map` | Also `ctx.storage` and the class's `state.storage`; sees the task's own pending writes; at most 1000 keys / 10 MiB |
| `storage.put(key, value)` / `storage.put({ key: value, ... })` | Structured clones; key at most 2 KiB, value at most 1 MiB, at most 1,000 writes per task |
| `storage.delete(key) -> boolean` / `storage.delete(keys) -> number` / `storage.deleteAll()` | `deleteAll` also clears the alarm |
| `storage.list({ prefix?, start?, end?, limit?, reverse? }) -> Map` | `[start, end)`, at most 1,000 |
| `storage.getAlarm() -> ms \| null`, `storage.setAlarm(Date \| ms)`, `storage.deleteAlarm()` | One alarm per object; runs `alarm(info)` at least once; survives restarts |

All storage writes of a task commit together when its handler returned; a throwing handler commits nothing. Outside an
object: `no_object`. Over quota: `storage_quota`.

## `std:sdk` (operator SDKs)

The operator publishes SDKs that run inside the runtime; a tenant imports their contract:
`import { acme } from "std:sdk"` or `import acme, { ping } from "sdk:acme"` (no SDK code enters your bundle). Each call,
`await acme.invoices.create(arg)`, runs the release your grants select. JSON argument and result, at most 256 KiB each.
Failures throw `SdkError` (a `ServiceError`): the SDK's own code, or `sdk_not_found`, `sdk_unavailable`,
`sdk_not_granted`, `sdk_version_unavailable`, `sdk_rate_limited`, `method_not_found`, `validation_failed`,
`call_depth`, `timeout`. List what you may use with `21ci sdk ls`; get types with `21ci sdk types <name> -d types`.
Example: [sample/deno/platform-sdk](https://github.com/21-ci/runtime/tree/main/sample/deno/platform-sdk).

## `std:platform`

`platform.fetch(binding, request, init?) -> Response` calls an operator binding. Only operator SDK services (system
tenant) may use it; a tenant service gets `403 binding_not_allowed`. It never throws on denial: check the status and
`x-rt-reason` (`binding_denied`, `binding_not_found`, `rate_limited`, `binding_upstream`).
