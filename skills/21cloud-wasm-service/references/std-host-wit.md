# `std:host@0.1.0` WIT summary

Authoritative file: [wit/rt-host.wit](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit). Copy it into your
project's `wit/` directory (the samples vendor it) and generate bindings from it. Every interface has its own
`record error { code: string, message: string }`; codes match the Deno `std:*` modules. Any call outside a running task
returns `unavailable`. The signatures below are abridged; in the `.wit` file `list` functions are spelled `%list`
(a keyword escape) and bindings name them `list`.

## Worlds

| World | Contents |
|---|---|
| `guest` | imports `log`, `kv`, `sql`, `blob`, `queue`, `call`, `platform`, `secrets`, `lock`, `schedule`, `sdk`, `sockets`, `durable`, `ws-client`, `pubsub`, `streams` |
| `rpc-service` | `guest` + export `rpc` |
| `socket-service` | `guest` + export `socket-handler` (required for a durable service) |
| `runtime-host` | `guest` + export `rpc` + export `socket-handler` |

The component must also export `wasi:http/incoming-handler@0.2.x` (from `wasi:http/proxy`); these worlds do not
include it.

## Imports

### `log`

```wit
enum level { debug, info, warn, error }
log: func(level: level, message: string);
log-fields: func(level: level, message: string, fields-json: string) -> result<_, error>;   // JSON object, <= 32 keys, <= 8 KiB
```

### `kv`

```wit
variant value { json(string), bytes(list<u8>) }
record set-options { ttl-seconds: option<u64> }
record list-options { prefix: list<u8>, cursor: option<list<u8>>, limit: u32 /* <= 1000 */, reverse: bool }
record list-page { entries: list<tuple<list<u8>, value>>, next-cursor: option<list<u8>> }

get: func(key: list<u8>) -> result<option<value>, error>;
set: func(key: list<u8>, value: value, opts: set-options) -> result<_, error>;
delete: func(key: list<u8>) -> result<bool, error>;
list: func(opts: list-options) -> result<list-page, error>;
incr: func(key: list<u8>, delta: s64, opts: set-options) -> result<s64, error>;   // atomic; TTL only on creation
cas: func(key: list<u8>, expected: option<value>, next: option<value>, opts: set-options) -> result<bool, error>;
expire: func(key: list<u8>, opts: set-options) -> result<bool, error>;           // none = clear expiry
ttl: func(key: list<u8>) -> result<option<u64>, error>;
```

A counter is stored as JSON text: `get` returns `json("<n>")`. Codes: `validation_failed`, `kv_key_too_large`,
`kv_value_too_large`, `storage_quota`, `kv_not_integer`, `kv_overflow`.

### `sql`

```wit
variant sql-value { null, integer(s64), real(f64), text(string), blob(list<u8>) }
record statement { sql: string, args: list<sql-value> }
record sql-result { columns: list<string>, rows: list<list<sql-value>>, rows-affected: u64, last-insert-rowid: option<s64> }

execute: func(stmt: statement) -> result<sql-result, error>;
batch: func(stmts: list<statement>, write: bool) -> result<list<sql-result>, error>;   // one transaction; write=false is read-only
```

There is no interactive transaction in WASM: use `batch`. Codes as `std:sql`.

### `blob` (buffered)

```wit
record meta { key: string, size: u64, content-type: option<string> }
get: func(key: string) -> result<option<tuple<meta, list<u8>>>, error>;
head: func(key: string) -> result<option<meta>, error>;
put: func(key: string, content-type: option<string>, data: list<u8>) -> result<meta, error>;
delete: func(key: string) -> result<bool, error>;
list: func(prefix: string, cursor: option<string>, limit: u32) -> result<list-page, error>;   // { items, next-cursor }
```

### `queue`

```wit
record send-options { delay-seconds: option<u64> /* <= 7 days */, dedup-id: option<string> /* 24 h */ }
enqueue: func(slug: string, body: list<u8> /* <= 1 MB */, opts: send-options) -> result<string, error>;   // message id
```

Codes: `not_found`, `forbidden`, `trigger_not_allowed`, `queue_full`.

### `pubsub` (ephemeral subjects)

```wit
record publish-result { receivers: u32, dropped: u32 }
record reply { content-type: option<string>, body: list<u8> }
publish: func(subject: string, body: list<u8> /* <= 1 MiB */, headers: headers) -> result<publish-result, error>;
request: func(subject: string, body: list<u8>, headers: headers, timeout-ms: option<u32> /* default 5000, max 30000 */) -> result<reply, error>;
```

At most once, in memory: `publish` resolves after routing, `dropped` counts copies not delivered (fan-out cap 100,
quota, backlog, refusals). Subjects are concrete (no wildcards), `[A-Za-z0-9_-]` tokens, not `_rt…`. Subscribers declare
`events.subscribe` on the service and receive `POST /__rt/pubsub` (see [SKILL.md](../SKILL.md)); for a `request` their
response is the reply.

Codes: `validation_failed`, `too_large`, `unavailable`, `call_depth`, `rate_limited`, `quota_exceeded`, and for `request`
also `no_responders`, `timeout`, `responder_failed`, `overloaded`, `queue_timeout`, `shed`, `memory`, `draining`.

### `streams` (persistent, partitioned topics)

```wit
record new-entry { body: list<u8>, key: option<string>, headers: headers, dedup-id: option<string> }
record appended { partition: u32, offset: u64, duplicate: bool }
variant position { earliest, latest, offset(u64), time-ms(s64) }
record read-options { partition: u32, start: position, limit: u32 /* 1..100 */ }
append: func(topic: string, entries: list<new-entry> /* 1..100, <= 4 MiB, one transaction */) -> result<list<appended>, error>;
read: func(topic: string, opts: read-options) -> result<page, error>;
info: func(topic: string) -> result<topic-info, error>;
```

`key` picks the partition (same key, same partition, in order). A repeated `dedup-id` within 2 minutes returns the first
position with `duplicate: true`. `read` always names a partition (out of range is `validation_failed`). Consumers declare
`events.consume` and receive `POST /__rt/stream` batches; a `2xx` commits, a `2xx` with `x-rt-stream-fail-at: <offset>`
commits the records before that offset.

Codes: `validation_failed`, `too_large`, `not_found`, `stream_full`, `rate_limited`, `quota_exceeded`, `unavailable`.

### `call` (service-to-service, same tenant)

```wit
type headers = list<tuple<string, list<u8>>>;
record request { method: string, path-with-query: string, headers: headers, body: list<u8> }
record response { status: u16, headers: headers, body: list<u8> }      // body buffered, <= max_response_buffered_bytes

call: func(slug: string, req: request) -> result<response, error>;
call-rpc: func(slug: string, method: string, args-json: string) -> result<string, error>;   // 256 KiB each way
```

A failing callee is a 5xx `response`, not an `err`; past depth 15 the response is `508` with `x-rt-reason: call_depth`.
`call` codes: `not_found`, `cross_tenant_call`, `forbidden`, `call_not_allowed`, `trigger_not_allowed`, `call_failed`,
`invalid_request`, `response_too_large`. `call-rpc` returns the callee's own `{code, message}` or a host code:
`validation_failed`, `method_not_found`, `response_too_large`, `invalid_response`, `call_depth`, `timeout`, plus the
`call` codes. RPC works across runtimes (Deno to WASM and back).

### `sdk` (operator SDKs)

```wit
invoke: func(name: string, method: string, args-json: string) -> result<string, error>;   // 256 KiB each way
```

Codes: the SDK's own, or `sdk_not_found`, `sdk_unavailable`, `sdk_not_granted`, `sdk_version_unavailable`,
`sdk_rate_limited`, `method_not_found`, `validation_failed`, `call_depth`, `timeout`.

### `platform` (operator SDK services only)

```wit
fetch: func(binding: string, req: request) -> result<response, error>;
```

Refusals are responses, not errors: `403 binding_denied`, `403 binding_not_allowed` (also every tenant component),
`404 binding_not_found`, `429 rate_limited`, `502 binding_upstream` (reason in `x-rt-reason`). Errors only for
`invalid_request`, `response_too_large`, `unavailable`.

### `secrets`

```wit
get: func(name: string) -> result<option<string>, error>;   // secret values only; validation_failed for a bad name
```

### `lock`

```wit
resource lease { release: func(); }                          // dropping it releases too
acquire: func(key: string, wait-ms: u32, lease-ms: u32) -> result<lease, error>;
```

`wait-ms` 0 = do not wait, at most 300000; `lease-ms` 1 to 900000; key 1 to 512 bytes. Codes: `lock_timeout`,
`lock_limit`, `validation_failed`. Every lock is released when the task ends.

### `schedule` (one-off tasks of this service)

```wit
record entry { id: string, run-at-ms: s64 }
record page { entries: list<entry>, cursor: option<string> }
after: func(seconds: u64, payload-json: string, id: option<string>) -> result<entry, error>;   // <= 30 days, payload <= 16 KiB
at: func(at-ms: s64, payload-json: string, id: option<string>) -> result<entry, error>;
cancel: func(id: string) -> result<bool, error>;
list: func(after: option<string>, limit: u32) -> result<page, error>;
```

The due schedule arrives as `POST /__rt/scheduled` with `{"id", "scheduledAt", "payload"}` and
`x-rt-trigger: schedule`, at least once. Codes: `validation_failed`, `schedule_limit` (1000 pending per service).

### `sockets` (WebSockets the node holds)

```wit
variant payload { text(string), binary(list<u8>) }
record accept-options { tags: list<string> /* <= 10 x 256 B */, attachment: option<list<u8>> /* <= 16 KiB */, protocol: option<string> }
record accepted { id: string, ticket: string }

accept: func(opts: accept-options) -> result<accepted, error>;    // then answer the request with status 101
send: func(id: string, data: payload) -> result<_, error>;
broadcast: func(object: option<string>, tag: option<string>, data: payload, except: list<string>) -> result<u32, error>;
close: func(id: string, code: u16, reason: string) -> result<_, error>;          // 1000 or 3000-4999; reason <= 123 B
close-all: func(object: option<string>, tag: option<string>, code: u16, reason: string) -> result<u32, error>;
list: func(object: option<string>, tag: option<string>, limit: u32, cursor: option<string>) -> result<tuple<list<socket-info>, option<string>>, error>;
get: func(id: string) -> result<option<socket-info>, error>;
set-tags: func(id: string, tags: list<string>) -> result<_, error>;
get-attachment: func(id: string) -> result<option<list<u8>>, error>;
set-attachment: func(id: string, value: option<list<u8>>) -> result<_, error>;
set-auto-response: func(object: option<string>, pair: option<tuple<string, string>>) -> result<_, error>;
```

Accept only in an HTTP or call task whose request carries `x-rt-websocket: 1` (else `not_upgradable`). Codes:
`not_found`, `invalid_request`, `too_large`, `limit_exceeded`, `slow_consumer`, `unavailable`, `not_upgradable`.

### `durable`

```wit
object: func() -> option<string>;                                  // the object this task acts for
get: func(key: string) -> result<option<list<u8>>, error>;
put: func(key: string, value: list<u8>) -> result<_, error>;
delete: func(key: string) -> result<bool, error>;
list: func(prefix: string, start: option<string>, end: option<string>, limit: u32, reverse: bool) -> result<list<tuple<string, list<u8>>>, error>;
delete-all: func() -> result<_, error>;
get-alarm: func() -> result<option<s64>, error>;
set-alarm: func(at-ms: option<s64>) -> result<_, error>;
fetch: func(service: option<string>, object: string, req: request) -> result<response, error>;
```

Values are raw bytes. Writes commit together when the handler returned; a failing handler writes nothing. Codes:
`no_object`, `validation_failed`, `kv_key_too_large` (2 KiB), `kv_value_too_large` (1 MiB), `limit_exceeded` (1000
writes per task), `storage_quota`, `not_durable`, and the `call` codes. A durable object's response is held until the
commit (not streamed).

### `ws-client` (outbound WebSockets)

```wit
resource connection {
  protocol: func() -> option<string>;
  send: func(data: payload) -> result<_, error>;
  recv: func(timeout-ms: u32) -> result<option<event>, error>;     // event: message(payload) | closed(close-frame)
  close: func(code: u16, reason: string) -> result<_, error>;
}
connect: func(url: string, protocols: list<string>) -> result<connection, error>;
```

Same egress rules as `wasi:http`; one of the task's fetches; closed when the task ends. Codes: `egress_denied`,
`egress_dns`, `egress_timeout`, `egress_upstream`, `egress_limit`, `egress_quota`, `validation_failed`, `invalid_state`.

## Exports

### `rpc` (world `rpc-service`)

```wit
record caller { service: string, slug: option<string>, project: option<string>, environment: option<string>, tenant: string, plan: option<string> }
handle: func(method: string, args-json: string, caller: option<caller>) -> result<string, error>;
```

`args-json` is `null` when the caller passed no argument. Return JSON text (at most 256 KiB) or an error the caller
receives as is; answer `method_not_found` for unknown methods.

### `socket-handler` (world `socket-service`)

```wit
record close-info { code: u16, reason: string, was-clean: bool }
on-message: func(object: string, socket: socket-info, data: payload) -> result<_, string>;
on-close: func(object: string, socket: socket-info, info: close-info) -> result<_, string>;
on-error: func(object: string, socket: socket-info, code: string, message: string) -> result<_, string>;
on-alarm: func(object: string, scheduled-ms: s64, retry: u32) -> result<_, string>;
```

Each call is one task, one object at a time. An `err` or a trap fails the event: it is delivered again (at least
once), then given up (a message's socket closes with 1011).
