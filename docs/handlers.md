# Guest code reference

What a service author can rely on. Sources: `js/bootstrap.js` and `js/rt_*.js` (Deno), `wit/rt-host.wit` and `crates/rt-exec-wasm` (WASM). Runnable examples: [sample/](https://github.com/21-ci/runtime/blob/main/sample/README.md).

## Deno handler (TS / JS)

The entry module's `export default` is either a function (Form A) or an object (Form B):

```ts
// Form A (val.town compatible)
export default async function (req: Request, ctx: Ctx): Promise<Response> { … }

// Form B: all optional, at least one required
export default {
  fetch(req: Request, ctx: Ctx): Response | Promise<Response>,
  scheduled(event: ScheduledEvent, ctx: Ctx): void | Promise<void>,
  queue(batch: QueueBatch, ctx: Ctx): void | Promise<void>,
  webSocketMessage(ws: Socket, message: string | ArrayBuffer, ctx: Ctx): void | Promise<void>,
  webSocketClose(ws: Socket, code: number, reason: string, wasClean: boolean, ctx: Ctx): void | Promise<void>,
  webSocketError(ws: Socket, error: Error & { code: string }, ctx: Ctx): void | Promise<void>,
}

// A durable service (`"durable": true`): a class, one instance per object (see "WebSockets and durable objects")
export default class Room {
  constructor(state: { id: { name: string }; name: string; storage: Storage }, env: Env) { … }
  fetch(req, ctx); webSocketMessage(ws, message, ctx); webSocketClose(…); webSocketError(…);
  alarm(info: { scheduledTime: number; retryCount: number }, ctx): void | Promise<void>;
}
```

| Trigger | Dispatched to | Without that export |
|---|---|---|
| `http`, `call` | `fetch` (Form A function = `fetch`) | `500 guest_error` (`module has no default handler`) |
| `interval`, `cron` | `scheduled(event, ctx)` | `fetch` gets `POST http://localhost/__rt/scheduled`, JSON `{scheduleId, scheduledTime, payload}`, header `x-rt-trigger`; status is only logged |
| `queue` | `queue({messages}, ctx)` | `fetch` gets `POST /__rt/queue`, JSON `{messages: [{id, body, attempt}]}`; `2xx` = ack all, otherwise retry all |
| `websocket` | `webSocketMessage` / `webSocketClose` / `webSocketError` | the event is done (nothing runs) |
| `alarm` | `alarm(info, ctx)` (a durable class) | the alarm is done |

```ts
// No types package is shipped: declare these in your own code.
interface Ctx {
  taskId: string;                                   // = x-rt-task-id response header
  service: { id: string; slug: string; version: number };   // slug is currently "" (known gap): use id
  trigger: "http" | "interval" | "cron" | "schedule" | "queue" | "call" | "websocket" | "alarm";
  object?: { name: string };                        // the object a socket event, alarm or request to a durable object is for
  storage?: Storage;                                // that object's storage (std:durable)
  invoker?: string;                                 // x-rt-invoker, internal listener only
  client?: { ip: string };                          // http trigger only: the client address (= x-rt-client-ip)
  deadline: number;                                 // epoch ms (wall limit)
  waitUntil(p: Promise<unknown>): void;             // keep the isolate alive after the response (bounded by the wall limit)
}
interface ScheduledEvent { scheduleId: string; scheduledTime: number; payload: unknown }   // payload = the schedule's JSON
interface QueueBatch {
  messages: { id: string; body: unknown; attempt: number; ack(): void; retry(o?: { delaySeconds?: number }): void }[];
}
```

Queue semantics: `body` is parsed JSON (or a string when it is not JSON). Messages not `retry()`-ed are acked when the handler returns normally; a throw fails the task and the batch is redelivered (backoff `2 s × 2^(attempt-1)` = 2 s, 4 s, 8 s …, ±20 %, up to `queue.max_attempts` = 5, then the DLQ). At-least-once, unordered.

### Request and response

- `req` is a standard Web `Request`. `req.url` is `http://localhost/<path>?<query>`: the runtime strips `/s/{slug}` (original prefix in header `x-rt-base-path`) and the `Host`.
- Added request headers: `x-rt-trigger`, `x-rt-base-path`, `x-rt-invoker` (internal only), `x-rt-client-ip`, `traceparent`. Client `x-rt-*` headers never reach the guest.
- Caller (service-to-service calls only): `ctx.caller` is `{ service: { id, slug }, tenant, project, environment }` as the runtime stamped it on the call; `undefined` for every other trigger. `slug`, `project` and `environment` are `null` when the caller is not visible from the callee's tenant. An operator SDK call adds `plan` and `sdk: { name, version }`.
- Client address: `ctx.client.ip` and the header `x-rt-client-ip` hold the address ingress resolved, exactly as for the `ip_allow` rule: the TCP peer, or, when the peer is in `ingress.trusted_proxies`, the first `X-Forwarded-For` hop from the right that is not a trusted proxy (IPv4-mapped IPv6 reads as IPv4). A client-sent `x-rt-client-ip` is removed first, so it cannot be spoofed. Only for the `http` trigger: absent for `interval`, `cron`, `queue` and `call` (a call's headers are the caller's, minus `x-rt-*`), and on the internal Unix socket, which has no peer address. The raw `X-Forwarded-For` is passed on as received.
- `Authorization` is the guest's own and reaches it unchanged, on `public` and `access = "key"` services alike, with one exception: an `Authorization: Bearer rtk_…` (a tenant API key, a runtime credential) is removed on every invoke, whether or not it is the key that admitted the request; `x-rt-key` goes like every `x-rt-*`. So a guest can check its own bearer tokens (an admin token, `Basic …`), and a client of an `access = "key"` service that also needs its own token sends the invoke key in `x-rt-key`.
- The request body is streamed: `req.body` is a `ReadableStream` the handler can read while the client is still sending. Reading past `limits.max_body_bytes` (default 10 MB) fails with an `Error` whose message is `request_too_large`; a larger `Content-Length` is `413 too_large` at ingress.
- Return a `Response` (anything else is a `TypeError` → `500 guest_error`). Streamed bodies must yield `Uint8Array` chunks; the first chunk is sent as soon as it is produced. Several `Set-Cookie` headers are preserved.
- The runtime adds `x-rt-task-id`, `x-rt-version`, `server-timing` to the response.
- A throw, or a rejected promise, is `500 guest_error`; the body is generic, stack and message go to the service logs.

## Runtime environment

| Topic | Rule |
|---|---|
| Lifetime | By default a fresh isolate per task: module top level runs once **per task**, no state survives between requests. With `deno.reuse_isolates` (default on only in the `min_latency` profile) an isolate that finished cleanly serves later requests of the same service version on the same processor: top-level code runs once per isolate and globals persist, but only as a cache (an isolate is replaced after any error, kill or leftover timer, after an env/secret change, after 1,000 requests, when idle, under memory pressure; requests may land on different isolates). Keep state in `std:kv` / `std:sql` / `std:blob` either way. **Never keep per-user or per-request data in module globals**: on a reused isolate it is visible to the next request of the same service version, which may come from another user |
| WebAssembly | Available (`WebAssembly.Module`/`Instance`/`compile`/`instantiate`/`…Streaming`, `Memory`). Every wasm memory counts against `memory_mb`, also one a module does not export (it is tracked through an export the guest does not see). Shared memories are refused (`TypeError` / `CompileError`): there are no threads |
| Memory | `memory_mb` covers the heap, every `ArrayBuffer` (also op results, `TextEncoder` output, resizable buffers) and `Blob` data. Growing a resizable `ArrayBuffer` past the limit throws `RangeError`; anything else over the limit ends the task with `502 oom` |
| Clock | Frozen: `Date.now()` / `performance.now()` advance only at I/O and timer boundaries, never during synchronous code (do not use them to measure CPU) |
| Web APIs | `fetch`, `Request`, `Response`, `Headers`, `FormData`, `URL`, `URLPattern`, `URLSearchParams`, streams (`ReadableStream`, `WritableStream`, `TransformStream`, `TextEncoderStream`, …), `TextEncoder/Decoder`, `Blob`, `File`, `FileReader`, `atob/btoa`, timers, `AbortController`, events, `structuredClone`, `CompressionStream`, `MessageChannel`, `crypto` (WebCrypto), `console`, `performance` |
| Removed | `Worker`, `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait/waitAsync/notify`, `ShadowRealm`, `caches`; `Deno` is reduced to `Deno.env` |
| `WebSocket` | Outbound only (`new WebSocket("wss://…", protocols)`, WHATWG events, `binaryType`), through the same egress guard as `fetch`: one of the task's fetches, received bytes count against the egress quota, closed when the task ends. An `error` event carries `.code` (`egress_denied`, `egress_timeout`, `egress_upstream`, `egress_limit`, `egress_quota`) |
| `console.*` | goes to the service logs (`console.debug/log/info/warn/error` = levels); ≤ 1,000 lines per task, message ≤ 10,000 chars |
| Imports | relative files; `std:kv` `std:sql` `std:sqlite` (also `bun:sqlite`) `std:blob` `std:queue` `std:services` `std:platform` `std:log` `std:secrets` `std:lock` `std:schedule` `std:sockets` `std:durable`; operator `sdk:<name>`; `npm:` / `jsr:` / allowlisted `https:` (resolved **at build**, via `build.npm_cdn`); an `rt.json` import map is applied first. Anything else is `422` at deploy, and a runtime import outside the bundle fails with `module_not_in_bundle` |
| Egress | `fetch` goes through the SSRF guard: public internet only (loopback, private ranges, metadata addresses denied; ports 25/465/587 denied by default). Failures reject with a `TypeError` whose message is a code: `egress_denied`, `egress_limit` (fetches per task), `egress_too_large`, `egress_quota`, `egress_timeout`, `egress_dns`, `egress_upstream` |
| Env / secrets | `Deno.env.get(name)` (`undefined` if unset), `.has(name)`, `.toObject()`; `set` / `delete` throw; reads throw `env_unavailable` if the env could not be fetched. Env (readable through the API) and secrets (write-only, AES-256-GCM at rest) appear together; names `[A-Z_][A-Z0-9_]*`, `RT_` reserved, ≤ 100 entries, values ≤ 32 KB. Changes apply to the next task. `std:secrets` reads only the secret values (table below) |

## SDK modules (Deno)

Named imports; every function returns a promise, except `std:sqlite`, which is synchronous. Errors are thrown as `Error` with a stable `.code` (no paths, no driver text).

```ts
import { kv } from "std:kv";
import { sql } from "std:sql";
import { Database } from "std:sqlite"; // or "bun:sqlite"
import { blob } from "std:blob";
import { enqueue } from "std:queue";
import { call } from "std:services";
import { platform } from "std:platform";
```

### `std:kv`: per-service key/value

| Call | Result |
|---|---|
| `kv.get(key)` | value or `null` |
| `kv.getMany(keys)` | `(value \| null)[]`, same order; at most 1000 keys and 10 MiB of values (`validation_failed`) |
| `kv.set(key, value, { ttlSeconds? })` | `void`; expired keys read as `null` |
| `kv.delete(key)` | `boolean` |
| `kv.list({ prefix?, cursor?, limit?, reverse? })` | `{ entries: [{ key, value }], cursor: string \| null }`, lexicographic (descending with `reverse: true`), `limit` default 100, max 1000 |
| `kv.incr(key, delta = 1, { ttlSeconds? })` | the counter's new value: `number`, or `bigint` for a `bigint` delta |
| `kv.cas(key, expected, next, { ttlSeconds? })` | `boolean`: `next` is written (`null` deletes) only if the current value equals `expected` (`null` = absent or expired); atomic, so of concurrent swaps exactly one wins |
| `kv.expire(key, ttlSeconds \| null)` | `boolean`: sets or clears the expiry of a live key; `false` when absent |
| `kv.ttl(key)` | seconds left (rounded up) or `null` when absent or without an expiry |
| `log.debug\|info\|warn\|error(message, fields?)` | a task log record; `fields` is a plain object (≤ 32 top-level keys, ≤ 8 KiB JSON), else `validation_failed` and nothing is written. Plain `console.*` lines keep writing records without fields |
| `log.with(fields)` | a child logger: `fields` are bound to each of its calls, the call's own fields win |
| `secrets.get(NAME)` | `string` or `null`: a secret's value (never a plain env var); a name that is not `[A-Z_][A-Z0-9_]*` (or starts with `RT_`) is `validation_failed` |
| `secrets.require(...NAMES)` | `{ NAME: value }` for every name; otherwise one error with `code` `secret_missing`, `missing` (the names) and a message naming each missing secret with `21ci secret set <service> NAME` |

Values: JSON-serializable or `Uint8Array`. Key ≤ 2 KB (non-empty), value ≤ 1 MB. `ttlSeconds` is a finite number ≥ 0 (absent or `null`: no expiry); anything else (`NaN`, negative, a string) is `validation_failed`. Codes: `validation_failed`, `kv_key_too_large`, `kv_value_too_large`, `storage_quota`, and for `incr` `kv_not_integer`, `kv_overflow`.

`kv.incr` is atomic: the read and the write run in one transaction on the service's single SQLite writer, so concurrent increments (from any number of tasks) never lose an update. A missing or expired key starts at 0. `ttlSeconds` applies when the call **creates** the counter; later increments keep that expiry, which gives fixed windows (``kv.incr(`rl:${ip}`, 1, { ttlSeconds: 60 })`` counts one minute from the first hit). The counter is stored as JSON, so `kv.get` returns the number and `kv.set(key, 0)` resets it. `delta` is a safe integer (`Number.isSafeInteger`) or a `bigint` in the `i64` range (else `validation_failed`); results past `±2^53` are only exact with a `bigint` delta. A value that is not a JSON integer (a string, `1.5`, bytes, …) fails `kv_not_integer` and a result outside `i64` fails `kv_overflow`; the value is unchanged then. The storage quota is charged as for a `set` of a 20-byte value.

### `std:sql`: SQLite per service

| Call | Result |
|---|---|
| `sql.execute(sql, args?)` | `{ columns: string[], rows: any[][], rowsAffected, lastInsertRowid }`. **Rows are arrays** in `columns` order, not objects |
| `sql.batch(stmts, "read" \| "write")` | `result[]`; `stmt` = `string \| { sql, args }`; one transaction |
| `sql.transaction(async tx => { await tx.execute(sql, args?) … })` | resolves with the callback's value; commits on resolve, rolls back on throw; 5 s cap (`sql_timeout`) |

Args: `null`/`undefined` → NULL, `boolean` → 0/1, `bigint`, `Uint8Array` → BLOB. Denied by the authorizer: `ATTACH`, `load_extension`, `_rt_*` tables, schema-writing pragmas. Statement budget `storage.sql_budget_ms` (1,000 ms). Codes: `sql_error`, `sql_timeout`, `sql_busy`, `sql_not_authorized`, `sql_result_too_large`, `storage_quota`.

### `std:sqlite` (also `bun:sqlite`): synchronous SQLite per service

The same database as `std:sql`, with [bun:sqlite](https://bun.sh/docs/api/sqlite)'s synchronous API: no `await`, no promise, a few µs per call because the statement runs on the thread that runs your code. Code written for `bun:sqlite` runs unchanged; both specifiers name one module.

```ts
import { Database } from "std:sqlite";

const db = new Database();          // the service's own database; takes no path
db.run("CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, title TEXT, done INTEGER DEFAULT 0)");
const add = db.query("INSERT INTO todos (title) VALUES ($title)");
const byId = db.query("SELECT * FROM todos WHERE id = ?");

export default () => {
  const { lastInsertRowid } = add.run({ $title: "write docs" }); // { changes, lastInsertRowid }
  return Response.json(byId.get(lastInsertRowid));               // a row object, or null
};
```

| Call | Result |
|---|---|
| `new Database(options?)` | the service's own database. A path (`":memory:"` included) is refused with code `not_supported`. Options: `strict` (named parameters without the `$`/`:`/`@` prefix, and a missing one throws), `safeIntegers` (`INTEGER` results as `bigint`), `readonly` (writes throw `SQLITE_READONLY`) |
| `db.query(sql)` / `db.prepare(sql)` | a `Statement`; `query` keeps up to 20 statements by their text, `prepare` does not |
| `stmt.all(...p)` / `.get(...p)` / `.values(...p)` / `.run(...p)` | rows as objects / the first row or `null` / rows as arrays / `{ changes, lastInsertRowid }` |
| `stmt.iterate(...p)` | an iterator over the rows (the statement runs to completion first, under the result caps; there is no cursor to keep open) |
| `stmt.as(Class)`, `.columnNames`, `.paramsCount`, `.finalize()`, `.toString()` | as in bun (`as` sets the rows' prototype without calling the constructor) |
| `db.run(sql, ...p)` / `db.exec(sql)` | `{ changes, lastInsertRowid }`; without parameters `sql` may hold several statements. They run one by one: a failing statement stops the rest and the earlier ones stay committed (use `db.transaction` for all or nothing) |
| `db.transaction(fn)` | a function that runs `fn` between `BEGIN` and `COMMIT` and rolls back when it throws; `.deferred` / `.immediate` / `.exclusive` choose the `BEGIN` mode; called inside another transaction it is a `SAVEPOINT`. `fn` must be synchronous: an `async` function is refused with `TypeError` when the transaction is made, before anything begins (its code up to the first `await` would run inside the transaction, the rest after it); a plain function that returns a promise is rolled back and throws `TypeError` |
| `db.inTransaction`, `db.close()` | `close` drops the cached statements; the connection belongs to the service on this processor and stays for the next request |

Parameters: positional (`?`, `?NNN`) as arguments or one array; named (`$x`, `:x`, `@x`) as one object. Values: `null`/`undefined` → NULL, `number` (a whole number within ±2^53 is an `INTEGER`, else `REAL`), `bigint`, `boolean` → 0/1, `string`, `Uint8Array` → BLOB; anything else is a `TypeError`. Rows hold numbers, strings, `null` and `Uint8Array` (integers beyond 2^53 lose precision unless `safeIntegers` is on).

Errors are `SQLiteError { message, code, errno, byteOffset, rtCode }`: `code` is SQLite's name (`SQLITE_CONSTRAINT_UNIQUE`), `rtCode` the platform's stable code: `sql_error`, `sql_timeout`, `sql_busy`, `sql_not_authorized`, `sql_result_too_large`, `storage_quota`.

What differs from `std:sql`: the authorizer, `sqlite3_limit`s, result caps (10,000 rows, 10 MiB) and storage quota are the same, but a statement may run for at most **100 ms** (`sql_timeout`; heavy analytics belongs on the async API), a write waits at most **20 ms** for a lock held by an `std:sql` transaction or batch (`sql_busy`), its CPU time counts against the task's CPU limit, and a write commits on its own instead of joining group commit. Writes by either API are visible to the other at once. Use one API per write-heavy path. A `PRAGMA` that is allowed (such as `foreign_keys`) sets the connection of one processor, so run it at module top level, where every request does. What differs from bun: no path, no `:memory:`, `serialize`/`deserialize`/`loadExtension`/`fileControl` throw `not_supported`, `BEGIN`/`SAVEPOINT`/`COMMIT` in SQL text are denied (use `db.transaction`), no `declaredTypes`/`columnTypes`.

### `std:blob`: streamed objects (local disk)

| Call | Result |
|---|---|
| `blob.put(key, body, { contentType? })` | `void`; body = string, `Uint8Array`, `ArrayBuffer`, `ReadableStream` or Blob-like; streamed, not buffered |
| `blob.get(key)` | `{ body: ReadableStream, size, contentType } \| null` |
| `blob.head(key)` | `{ size, contentType } \| null` |
| `blob.delete(key)` | `boolean` |
| `blob.list({ prefix?, cursor?, limit? })` | `{ objects: [{ key, size, contentType }], cursor: string \| null }` |

Key ≤ 1 KB, object ≤ 100 MB (plan). Codes: `blob_key_too_large`, `storage_quota`, `validation_failed`. Storage (KV + SQL + blobs) is capped by `plan.storage_mb`.

### `std:queue`, `std:services`, `std:platform`

| Call | Notes |
|---|---|
| `enqueue(service, body, { delaySeconds?, dedupId? }) → { id }` | `service` = slug of a service the handler may call (same tenant, or another tenant's under the `services.fetch` rules: `plan.allow_cross_tenant_calls` and the target's `call.allowed_callers`); `body` JSON or `Uint8Array`, ≤ 1 MB; the target must have its queue trigger on (`forbidden`) and its plan must allow queues; delay ≤ 7 d; same `dedupId` within 24 h delivers once. Codes: `not_found`, `cross_tenant_call`, `forbidden`, `call_not_allowed`, `trigger_not_allowed`, `queue_full` |
| `services.fetch(slug, request, init?) → Response` (also `call(slug, request, init?)`) | In-process call to a service of the same tenant (no HTTP hop; only the request's path + query are used). Child wall limit = min(own, parent remaining). Depth ≤ 15 (`call_depth`, `508` at the edge). Codes: `not_found`, `cross_tenant_call` (unless `plan.allow_cross_tenant_calls`), `forbidden`, `call_failed`. A failing child returns a 5xx `Response`, it does not throw |
| `services.call(slug, method, arg?) → value` | RPC: the callee's `rpc[method](arg, ctx)` (`export const rpc = {...}`), resolved with its JSON value. Throws `ServiceError` (`code`, `message`) for every failure: the callee's own `ServiceError` code, `internal_error` for other errors (message only), or the host code (`method_not_found`, `call_depth`, `call_not_allowed`, `validation_failed`, `response_too_large`). Argument and result ≤ 256 KiB. |
| `services.bind(slug) → proxy` | `await proxy.method(arg)` is `services.call(slug, "method", arg)` |
| `platform.fetch(binding, request, init?) → Response` | Calls an operator binding (`[[binding]]`) without credentials. **Never throws on denial**: a `Response` with status `403` + `x-rt-reason: binding_denied` (method/path/plan not allowed, `..` traversal), `404 binding_not_found`, `429 rate_limited`, `502 binding_upstream` |

Operator SDKs (`[[sdk]]`, `sdk:<name>`) are plain modules importable by tenants; they must not hold credentials (use bindings).

### `std:lock`: named mutexes per service

| Call | Notes |
|---|---|
| `lock(key, fn, { waitMs?, leaseMs? }) → fn's value` | Holds `key` while `fn` runs. Waits at most `waitMs` (default 10 000, max 300 000); `lock_timeout` when it cannot get the key in time. `leaseMs` (default 30 000, max 900 000) bounds a hold: after the lease the next waiter is granted, so `fn` must finish within it. A throwing `fn` releases the key and rethrows |
| `lock.try(key, fn, { leaseMs? }) → { acquired: false } \| { acquired: true, value }` | Never waits |

Keys are 1 to 512 bytes; locks are per service and per node. The hold is released when `fn` settles, and when the task ends, is killed or hits a limit. Codes: `lock_timeout`, `lock_limit` (32 holds per task, 1 024 keys per service, 64 waiters per key), `validation_failed`. Waiting is asynchronous: a waiter does not hold a thread. Components: `std:host/lock` (`acquire(key, wait-ms, lease-ms)` returns a `lease` resource; `release()` or dropping it gives the key up).

### `std:schedule`: one-off tasks of the service

| Call | Notes |
|---|---|
| `schedule.after(seconds, payload, { id? }) → { id, runAt }` | Due in `seconds` (at most 30 days). `payload` is JSON, ≤ 16 KiB. Without `id` the id is generated (`sch_…`) |
| `schedule.at(time, payload, { id? }) → { id, runAt }` | `time` is a `Date` or Unix ms, at most 30 days ahead; a past time is due at once |
| `schedule.cancel(id) → boolean` | `false` when no such schedule is pending |
| `schedule.list({ cursor?, limit? }) → { entries: [{ id, runAt }], cursor }` | Pending schedules in id order, 100 per page (max) |

The handler gets the due schedule as `scheduled(event, ctx)`: `event.id`, `event.scheduledAt` (Unix ms), `event.payload`, `ctx.trigger === "schedule"`. Delivery is at-least-once: a failed delivery is retried with backoff, and a schedule whose 5th attempt fails is dropped. A pending id is replaced by a new `after`/`at` with the same id. Ids are 1 to 128 of `a-z A-Z 0-9 . _ - :`. Codes: `validation_failed`, `schedule_limit` (1 000 pending per service), `unavailable` (outside a task). Components: `std:host/schedule`, the same calls with `after(seconds, payload-json, id)`, `at(at-ms, payload-json, id)`, `cancel(id)`, `%list(after, limit)`.

## WebSockets and durable objects

The node holds every WebSocket: between two messages no isolate runs for it. A message, a close or an
error of a socket runs as **one task** (`ctx.trigger === "websocket"`) of the service, with its HTTP limits; the
events of one **object** run one at a time, in order. An object is the default object (`default`) of a service, or,
for a durable service, the one code chose with `durable.get(name)`.

```ts
// A plain service: every socket belongs to its `default` object.
import { sockets } from "std:sockets";
export default {
  fetch(req: Request) {
    if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("upgrade required", { status: 426 });
    return sockets.accept(req, { tags: ["room:lobby"], attachment: { user: "ann" } });   // answered 101
  },
  webSocketMessage(ws, message) {
    sockets.broadcast(`${ws.attachment.user}: ${message}`, { tag: "room:lobby", except: ws.id });
  },
  webSocketClose(ws, code, reason, wasClean) { /* the socket is gone */ },
};
```

| Rule | |
|---|---|
| Accept | Only from an `http` or `call` task whose request offered the upgrade (`not_upgradable` otherwise). The response of `accept` must be what `fetch` returns (also through a front service: a call that accepted comes back as such a response). `res.socket` is the new socket; what it sends before the client is connected is queued |
| Socket | `{ id, object, tags, protocol, createdAt, lastMessageAt, autoResponseAt, bufferedAmount, remoteIp, attachment, send(data), close(code, reason), serializeAttachment(v), deserializeAttachment() }`. `attachment` is a structured clone (≤ 16 KiB) that survives the isolate |
| Messages | Text arrives as a `string`, binary as an `ArrayBuffer`. A client message over `plan.ws_max_message_bytes` (1 MiB) closes the socket with 1009; a client over 50 messages/s (burst 200) is read more slowly and closed with 1008 when it keeps it up |
| Output gate | A task that acts for an object (an event, an alarm, a request to a durable object) sends when its handler returned and its storage writes committed; a handler that throws sends and writes nothing. The HTTP response of a request to an object leaves after the commit too (a WASM response is held, not streamed, up to the response cap). `{ immediate: true }` sends at once |
| Delivery | At least once: a failed event is delivered again with backoff (`ws.max_event_attempts`, 3); then a message's socket is closed with 1011 and the event logged. Make handlers idempotent |
| Cron, interval, queue | A schedule or queue task of a durable class acts for **no object**: the class is constructed with the name `default`, but its `state.storage` throws `no_object` and it holds no object's turn. To act for an object, call it: `durable.get(name).fetch(req)` (that request runs in the object's turn, with its storage and output gate) |
| Errors | A broken connection, a ping timeout or a cap (1006, 1008, 1009, 1013) arrives as `webSocketClose` with `wasClean` false. `webSocketError` is accepted for Cloudflare compatibility and not called yet |
| Deploys | Sockets survive a deploy; the next event runs the new version. A deploy or rollback with `close_sockets` closes them with 1012. A deleted service's and a draining node's sockets close with 1001; a node restart drops them (clients reconnect) |
| Warm objects | On by default: the isolate of an event is kept for the next event of the service, so a durable class's instance (its fields) survives between events while the next event lands on that isolate. Never required for correctness; `ws.warm = false` gives a fresh isolate per event |
| Auto-response | `sockets.setAutoResponse({ request: "ping", response: "pong" })`: the node answers that exact text message itself, no task runs (`autoResponseAt` is stamped) |
| Origin | `ws.allowed_origins` (service config) refuses other `Origin`s with `403` before any task; set it for cookie-authenticated apps |
| Browser auth | For `access = "key"` a browser passes the key as a subprotocol: `new WebSocket(url, ["rtk_…", "chat"])` (removed before the guest sees the request) |

### `std:sockets`

| Call | Notes |
|---|---|
| `accept(req, { tags?, attachment?, protocol? }) → Response` | `protocol`: the subprotocol to answer with (one the client offered) |
| `send(id, data, { immediate? })`, `close(id, code = 1000, reason = "")` | `data`: string, `ArrayBuffer` or a view. Codes 1000 or 3000-4999; reason ≤ 123 bytes |
| `broadcast(data, { object?, tag?, except?, immediate? }) → { sent }` | To the task's object (else `default`), or the one named |
| `closeAll({ object?, tag?, code?, reason? }) → { closed }` | |
| `list({ object?, tag?, limit?, cursor? }) → { sockets, cursor? }`, `get(id)`, `count({ object?, tag? })` | Without `object`: the task's object, else every object of the service |
| `objects({ prefix?, limit?, cursor? }) → { objects: [{ name, sockets }], cursor? }` | Objects holding sockets |
| `setTags(id, tags)`, `setAttachment(id, value)`, `getAttachment(id)` | ≤ 10 tags of ≤ 256 bytes |
| `setAutoResponse({ request, response } \| null, { object? })` | ≤ 1 KiB each |

Calls work from any task of the service (an HTTP handler, cron, queue), scoped to it: another service's socket is
`not_found`. Codes: `not_found`, `invalid_request`, `too_large`, `limit_exceeded` (plan, service or node socket caps),
`slow_consumer` (1 MiB queued: closed 1013), `unavailable`, `not_upgradable`.

### `std:durable`

A **durable service** (`"durable": true` in its config, `21ci service create --durable`) exports a class; each
object is one instance, chosen in code. Its requests and events run one at a time per object; different objects run
in parallel.

```ts
// chat-rooms (durable): one Room per room name
import { sockets } from "std:sockets";
export default class Room {
  constructor(state, env) { this.state = state; }               // runs again whenever no warm instance is at hand
  async fetch(req) { return sockets.accept(req, { tags: ["user:" + new URL(req.url).searchParams.get("name")] }); }
  async webSocketMessage(ws, msg) {
    const n = ((await this.state.storage.get("count")) ?? 0) + 1;
    await this.state.storage.put("count", n);                    // committed when the handler returns
    sockets.broadcast(`${n}: ${msg}`);                            // sent after the commit
  }
  async alarm(info) { /* info.scheduledTime, info.retryCount */ }
}

// gate (any service): the object is chosen in code; the upgrade is forwarded with the request
import { durable } from "std:durable";
export default { fetch: (req) => durable.get(new URL(req.url).searchParams.get("room"), { service: "chat-rooms" }).fetch(req) };
```

| Call | Notes |
|---|---|
| `durable.get(name, { service? }) → { name, id, fetch(input, init) }` | `name`: 1 to 256 bytes. `service`: a durable service of the tenant (call rules apply), default: this one. A request straight to a durable service is its object `default`. `not_durable` for another service |
| `durable.idFromName(name) → name`, `durable.newUniqueId() → string` | Cloudflare's names; an object is addressed by its name |
| `storage.get(key) → value`, `storage.get(keys) → Map` | Also `ctx.storage` and the class's `state.storage`. The task's own writes first. At most 1000 keys and 10 MiB of values (`validation_failed`) |
| `storage.put(key, value)`, `storage.put({ key: value, … })` | Structured clones; keys ≤ 2 KiB, values ≤ 1 MiB, ≤ 1 000 writes per task; counts against `storage_mb` (`storage_quota`) |
| `storage.delete(key) → boolean`, `storage.delete(keys) → number`, `storage.deleteAll()` | `deleteAll` also clears the alarm |
| `storage.list({ prefix?, start?, end?, limit?, reverse? }) → Map` | `[start, end)`, at most 1 000 |
| `storage.getAlarm() → ms \| null`, `storage.setAlarm(Date \| ms)`, `storage.deleteAlarm()` | One alarm per object; it runs `alarm(info)` at least once (`ws.max_alarm_attempts` 6, backoff), survives restarts |

All of one task's storage writes commit together when its handler returned (`code: "no_object"` outside an object).
Storage is rows of the service's own database (`data.db`), keyed by object; `DELETE /v1/services/{s}/objects/{name}`
drops an object's storage.

### Deno and WASM parity

| Feature | Deno | WASM (`wit/rt-host.wit`) |
|---|---|---|
| Accept an upgrade | `sockets.accept(req, opts)` | `sockets.accept(opts)`, then answer status 101; the offer is the header `x-rt-websocket: 1` |
| Socket API | `std:sockets` | `std:host/sockets` (`send`, `broadcast`, `close`, `close-all`, `list` (with cursor), `get`, `set-tags`, `get-attachment`, `set-attachment` (raw bytes), `set-auto-response`) |
| Events | `webSocketMessage` / `webSocketClose` / `webSocketError` / `alarm` | export `std:host/socket-handler`: `on-message`, `on-close`, `on-error`, `on-alarm` (an `err` or a trap fails the event); world `socket-service` |
| Durable storage, alarm | `std:durable` `storage` (structured clones) | `std:host/durable` (`get`, `put`, `delete`, `list`, `delete-all`, `get-alarm`, `set-alarm`; values are bytes) |
| Object requests | `durable.get(name, { service }).fetch(req)` | `durable.fetch(service, object, request)` |
| Durable service | a class default export | must export `socket-handler` (`422 missing_export` at deploy) |
| Outbound WebSocket | `new WebSocket(url)` | `std:host/ws-client` (`connect`, `connection.send` / `recv(timeout-ms)` / `close`) |
| Blob reads | `blob.get` streams the object (≤ 100 MB) | `std:host/blob` `get` buffers the object and refuses one larger than `max_response_buffered_bytes` (10 MB) with `blob_too_large` |
| Outside a task | | every import answers `unavailable` |

## Limits (defaults, `balanced`; plan maxima apply)

| Limit | HTTP / call | interval, cron | queue |
|---|---|---|---|
| Wall time | 30 s (call: min(own, parent remaining); a socket event or alarm: the HTTP row) | 300 s | 300 s |
| CPU time | 1,000 ms | 10,000 ms | 10,000 ms |
| Memory (heap / linear) | 128 MB | 128 MB | 128 MB |
| Request body | 10 MB | n/a | 1 MB per message |
| Response body | 10 MB buffered, 100 MB streamed | n/a | n/a |
| Outbound fetches / concurrent | 50 / 6 | 200 / 6 | 200 / 6 |
| Call depth | 15 | | |

`free` plan maxima: wall 30 s, CPU 1 s, 128 MB; schedules ≥ 60 s apart, ≤ 3 per service, no `cron`. Details: [api.md](api.md#plans-built-in-free-more-under-plansname).

## Outcome to HTTP

Invoke failures are `text/plain` with `x-rt-reason` ([api.md](api.md#invoke-api) has the full table).

| Outcome | HTTP | `x-rt-reason` |
|---|---|---|
| Handler returned | its status | none |
| Uncaught throw / trap / bad return | 500 | `guest_error` |
| CPU limit | 502 | `cpu_limit` |
| Out of memory | 502 | `oom` |
| Wall timeout | 504 | `timeout` |
| Build failed | 502 | `build_failed` |
| Rate / quota (plan) | 429 | `rate_limited`, `quota_exceeded` (busy capacity is not a 429: requests wait for a slot) |
| Node pressure / drain | 503 | `shed`, `overloaded`, `memory`, `queue_timeout` (with `Retry-After`), `draining`, `restarting`, `route_stale` (the service changed while the request waited; retry) |

## WASM component

| Aspect | Contract |
|---|---|
| Artifact | WASI P2 **component** (`wasm32-wasip2`, `jco componentize`, …) exporting `wasi:http/incoming-handler` (any `0.2.x`). A core module, a component without that export, threads / shared memory → `422 invalid_artifact` at deploy. Upload `Content-Type: application/wasm`, ≤ 50 MB, service `runtime: "wasm"` |
| Linked imports | `wasi:io`, `wasi:clocks` (frozen clock), `wasi:random`, `wasi:cli` (stdin empty, exit ends the task; `terminal-*` linked as "no terminal", so full-`std` Rust components with their `IsTerminal` / `println!` imports run), `wasi:http` (outbound through the egress guard), `wasi:filesystem` (no preopens), `wasi:sockets` (all addresses denied), `std:host/{log,kv,sql,blob,queue,call,platform,secrets,lock,schedule,sdk,sockets,durable,ws-client}` ([`wit/rt-host.wit`](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit), package `std:host@0.1.0`). A component of world `rpc-service` also exports `std:host/rpc`; one of world `socket-service` exports `std:host/socket-handler` (WebSocket events and alarms, see "Deno and WASM parity"). Any WASI `0.2.x` minor the toolchain emits (e.g. `@0.2.12`) links |
| `std:host/queue` | `enqueue(slug, body, {delay-seconds?, dedup-id?}) → id`: same rules and codes as the Deno `enqueue` (`not_found`, `forbidden`, `queue_full`, …) |
| `std:host/platform` | `fetch(binding, {method, path-with-query, headers, body}) → {status, headers, body}`: the Deno `platform.fetch` for WASM, through the same binding facility (`[[binding]]`), for operator SDK services only (a tenant component gets `403 binding_not_allowed`): plan, method and path checks, the operator's headers injected upstream (never visible to the guest; the guest's `authorization`, `cookie`, `x-rt-*` are dropped). **Never an error on refusal**: `403` + `x-rt-reason: binding_denied` (method/path, `..`), `403 binding_not_allowed` (plan), `404 binding_not_found`, `429 rate_limited`, `502 binding_upstream`. Errors only for `invalid_request` and `response_too_large` |
| `std:host/call` | `call(slug, {method, path-with-query, headers, body}) → {status, headers, body}`: same rules as the Deno `call` (same tenant; the target's `call.allowed_callers` / environment, plan triggers; child wall = min(own, caller remaining)). Codes: `not_found`, `cross_tenant_call`, `forbidden`, `call_not_allowed`, `trigger_not_allowed`, `call_failed`, `invalid_request`, `response_too_large` (over `max_response_buffered_bytes`). A failing child is a 5xx response; past depth 15 a `508` with `x-rt-reason: call_depth`. `call-rpc(slug, method, args-json) → result<string, error>` is `services.call`: the callee's JSON result, or its (or the host's) `{code, message}` |
| `std:host/rpc` (export) | `handle(method, args-json, caller) → result<string, error>`: a component of world `rpc-service` serves RPC; a call to `/__rt/rpc/<method>` runs it instead of the HTTP handler, with the stamped caller (`service`, `slug`, `project`, `environment`, `tenant`, `plan`). Without the export such a call answers `method_not_found` |
| `std:host/sdk` | `invoke(name, method, args-json) → result<string, error>`: an operator SDK call (`sdk(name)[method](args)` in Deno), bound to the release the caller's grants select; codes `sdk_not_found`, `sdk_unavailable`, `sdk_not_granted`, `sdk_version_unavailable`, `sdk_rate_limited`, the SDK's own |
| Not yet | Blob and call bodies are buffered (no `wasi:io` streams) |
| Triggers | HTTP as is. Interval/cron/queue arrive as the same synthetic `POST /__rt/scheduled` / `/__rt/queue` requests as for a Deno Form A handler |
| Request headers | `x-rt-task-id`, `x-rt-trigger`, `x-rt-deadline` (epoch ms) added by the runtime; for the `http` trigger also those of the Deno request (`x-rt-base-path`, `x-rt-client-ip`, `traceparent`, …, same hygiene and `Authorization` rule). `x-rt-client-ip` is the only way a component learns its client's address |
| `std:host/kv` | `get`, `set`, `delete`, `list`, and `incr(key, delta: s64, {ttl-seconds?}) → s64`: the Deno `kv.incr` (atomic, TTL on creation, `kv_not_integer` / `kv_overflow`); also `cas`, `expire`, `ttl` (the Deno calls of the same names) |
| Env / logs | env + secrets via the WASI environment; stdout → `info`, stderr → `error` log lines with `task_id`; `std:host/log.log-fields(level, message, fields-json)` writes a record with fields (refused: `validation_failed`); `std:host/secrets.get(name) → result<option<string>, error>` is `secrets.get` (only secret values) |
| Lifecycle | A fresh instance per task (possibly pre-instantiated by the warm pool, never used before). With `wasm.reuse_instances` (per service: `config.wasm.reuse_instances`; default on only in `min_latency`) a cleanly finished instance serves the next task of the same service on the same P, so linear-memory state persists; it is dropped on trap/kill/timeout and after the idle TTL. On such an instance the wall clock reads real time, the monotonic clock does not count the idle gap. A changed env (rotated secret) or another version gets a new instance. Clock readings are whole milliseconds and advance only across real waits |
| Response | Streamed as the guest writes it (head first, then chunks), up to `max_response_stream_bytes`; a trap, kill or the size limit after the head ends the body with an error |
| Errors | `std:host` calls return `error { code, message }` with the same codes as the Deno SDK |

## Examples

```ts
// Counter in KV (atomic) and a per-client rate limit: 60 requests per minute and address
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
// Form B: HTTP + schedule + queue in one module
import { sql } from "std:sql";
export default {
  async fetch(req: Request, ctx: Ctx) { return Response.json({ trigger: ctx.trigger }); },
  async scheduled(event: ScheduledEvent, ctx: Ctx) {
    await sql.execute("CREATE TABLE IF NOT EXISTS runs (at INTEGER)");
    await sql.execute("INSERT INTO runs VALUES (?)", [event.scheduledTime]);
  },
  async queue({ messages }: QueueBatch, ctx: Ctx) {
    for (const m of messages) console.log("job", m.id, m.attempt, m.body);   // return = ack all
  },
};
```

Create the schedule and enqueue through the control API ([api.md](api.md#schedules-and-queue)):

```bash
curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
  -d '{"kind":"interval","every_seconds":60,"payload":{"job":"cleanup"}}' http://127.0.0.1:8081/v1/services/$SVC/schedules
curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
  -d '{"body":{"orderId":1}}' http://127.0.0.1:8081/v1/services/$SVC/queue          # key needs queue:write
```

```ts
// Service-to-service call (same tenant, by slug)
import { call } from "std:services";
export default async function (req: Request): Promise<Response> {
  const res = await call("billing", new Request("http://svc/charge", { method: "POST", body: JSON.stringify({ sku: "a" }) }));
  return res.ok ? new Response("charged") : new Response("billing failed", { status: 502 });
}
```

## See also

[api.md](api.md) · [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md) · [sample/README.md](https://github.com/21-ci/runtime/blob/main/sample/README.md)
