# Handlers, ctx and events (Deno)

Source of truth: `js/bootstrap.js` and `crates/rt-exec-deno` in https://github.com/21-ci/runtime ; guide:
[docs/handlers.md](https://github.com/21-ci/runtime/blob/main/docs/handlers.md).

## Declarations to copy

No types package ships with the runtime. Paste what you use:

```ts
interface Ctx {
  taskId: string;                                         // = x-rt-task-id response header
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "schedule" | "queue" | "call" | "websocket" | "alarm" | "pubsub" | "stream";
  invocation?: { id: string; attempt: number };           // async HTTP invocation only (`x-rt-invocation-id` / `-attempt`)
  object?: { name: string };                              // socket event, alarm, or request to a durable object
  storage?: DurableStorage;                               // that object's storage (std:durable)
  invoker?: string;                                       // x-rt-invoker, internal listener only
  client?: { ip: string };                                // http trigger only (= x-rt-client-ip)
  caller?: {                                              // service-to-service calls only
    service: { id: string; slug: string | null };
    tenant: string;
    project: string | null;
    environment: string | null;
    plan?: string;                                        // operator SDK calls only
    sdk?: { name: string; version: string };              // operator SDK calls only
  };
  deadline: number;                                       // epoch ms (wall limit)
  waitUntil(p: Promise<unknown>): void;                   // keep the task alive after the response
}

// interval / cron schedule
interface ScheduledEvent { scheduleId: string; scheduledTime: number; payload: unknown }
// one-off schedule from std:schedule (ctx.trigger === "schedule")
interface OneOffEvent { id: string; scheduledAt: number; payload: unknown }

interface QueueMessage {
  id: string;
  body: unknown;              // parsed JSON, or a string when the body is not JSON
  attempt: number;            // 1 on the first delivery
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}
interface QueueBatch { messages: QueueMessage[] }

// pub/sub delivery (`pubsub` trigger); `replyable` = a `request`, whose return value is the reply
interface PubsubMessage {
  id: string;
  subject: string;
  body: unknown;                  // parsed JSON, else a string
  headers: Record<string, string>;
  replyable: boolean;
  publishedAt: number;            // epoch ms
}

// topic consumer batch (`stream` trigger): one partition, in offset order
interface StreamRecord {
  partition: number;
  offset: number;
  key: string | null;
  body: unknown;
  headers: Record<string, string>;
  timestamp: number;
}
interface StreamBatch {
  topic: string;
  group: string;
  partition: number;
  attempt: number;                // deliveries of the record at the cursor, 1-based
  messages: StreamRecord[];
  failAt(m: StreamRecord | number): void;   // commit before m, redeliver from m (lowest call wins)
}
```

## Dispatch

| Trigger | Form B / class member | Form A, or no such member |
|---|---|---|
| `http`, `call` | `fetch(req, ctx)` | the default function; no handler: `500 guest_error` (`module has no default handler`) |
| `interval`, `cron` | `scheduled(event: ScheduledEvent, ctx)` | `fetch` gets `POST http://localhost/__rt/scheduled`, JSON `{scheduleId, scheduledTime, payload}`; its status is only logged |
| `schedule` (one-off) | `scheduled(event: OneOffEvent, ctx)` | `fetch` gets `POST /__rt/scheduled`, JSON `{id, scheduledAt, payload}` |
| `queue` | `queue(batch: QueueBatch, ctx)` | `fetch` gets `POST /__rt/queue`, JSON `{messages: [{id, body, attempt}]}`; `2xx` acks all, anything else retries all |
| `websocket` | `webSocketMessage` / `webSocketClose` / `webSocketError` | nothing runs; the event is done |
| `alarm` | `alarm(info, ctx)` (durable class) | nothing runs; the alarm is done |
| `pubsub` | `pubsub(msg: PubsubMessage, ctx)`; its return value answers a `request` (`undefined` -> `204`, object -> JSON, string, bytes, or a `Response`) | `fetch` gets `POST /__rt/pubsub`, JSON = the `PubsubMessage` |
| `stream` | `stream(batch: StreamBatch, ctx)`; a normal return commits the batch, a throw retries it | `fetch` gets `POST /__rt/stream`, JSON = the `StreamBatch`; `2xx` commits, `2xx` + `x-rt-stream-fail-at: <offset>` commits before that offset |

Queue semantics: messages that are not `retry()`-ed are acked when the handler returns normally. A throw fails the task
and the whole batch is redelivered. Backoff `2 s x 2^(attempt-1)` (+-20 %), up to `queue.max_attempts` (default 5),
then the dead-letter queue (`21ci queue dlq <svc>`). Default batch size is 10. Unordered, at least once.

Stream consumers: `batch.failAt(m)` commits the records before `m` and redelivers from `m`; a normal return commits the
whole batch. Records are delivered per partition, in order: one slow record holds back its partition, not the others.
Delivery is at least once, so make the work idempotent (the record's `partition` + `offset` is a stable key).

Pub/sub: a plain publish is delivered at most once. Use the return value only for a `request`.

Async HTTP invocations (service `async.enabled`): the handler is the normal `fetch`; its response body is discarded and
only the status counts (`5xx` / `429` retry, anything else is done). `ctx.invocation` is set on each attempt.

Schedules: interval/cron schedules are created on the service (control API or `21ci schedule add`), not in code. One-off
schedules are created in code with `std:schedule`.

## Request and response rules

- `req.url` is `http://localhost/<path>?<query>`; the runtime strips `/s/{slug}` and puts it in `x-rt-base-path`. With a
  custom host or `{slug}.{base_domain}` the path is unchanged.
- Added request headers: `x-rt-trigger`, `x-rt-base-path`, `x-rt-invoker` (internal listener only), `x-rt-client-ip`
  (http only), `traceparent`. Client-sent `x-rt-*` headers are removed.
- `Authorization` reaches the guest unchanged, except an `Authorization: Bearer rtk_…` runtime key, which is always
  removed. A client of an `access = "key"` service that also needs its own token sends the invoke key in `x-rt-key`.
- `req.body` is a stream. Reading past `limits.max_body_bytes` (default 10 MB) throws an `Error` with message
  `request_too_large`; a larger `Content-Length` is `413 too_large` at ingress.
- Return a `Response`. Streamed bodies must yield `Uint8Array` chunks. Several `Set-Cookie` headers are preserved.
- The runtime adds `x-rt-task-id`, `x-rt-version` and `server-timing` to the response.

## Runtime environment

| Topic | Rule |
|---|---|
| Lifetime | Fresh isolate per task by default. With `deno.reuse_isolates` (on in the `min_latency` profile) a cleanly finished isolate may serve later requests of the same version: globals persist as a cache only (replaced after an error, env/secret change, 1,000 requests, idle, memory pressure). The idle limit is `scaling.min_run_time_ms` when the service sets it: `0` drops the isolate after the answer unless requests are queued for it, so module-level caches may be cold more often |
| Web APIs | `fetch`, `Request`, `Response`, `Headers`, `FormData`, `URL`, `URLPattern`, `URLSearchParams`, streams, `TextEncoder/Decoder`, `Blob`, `File`, `FileReader`, `atob/btoa`, timers, `AbortController`, events, `structuredClone`, `CompressionStream`, `MessageChannel`, `crypto` (WebCrypto), `console`, `performance` |
| WebAssembly | Available; every wasm memory counts against `memory_mb`; shared memories are refused |
| Removed | `Worker`, `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait/waitAsync/notify`, `ShadowRealm`, `caches`; `Deno` is only `Deno.env` |
| `Deno.env` | `get(name)` (`undefined` if unset), `has(name)`, `toObject()`; `set` / `delete` throw. Env vars and secrets appear together. Changes apply to the next task, no redeploy |
| `console.*` | Service logs (`debug/log/info/warn/error` map to levels); at most 1,000 lines per task, 10,000 chars per message |
| Outbound `WebSocket` | `new WebSocket("wss://…", protocols)` with WHATWG events, through the egress guard; counts as one of the task's fetches; closed when the task ends. An `error` event carries `.code` (`egress_denied`, `egress_timeout`, `egress_upstream`, `egress_limit`, `egress_quota`) |

## WebSockets and durable objects

The node holds every WebSocket; no isolate runs between messages. Each message, close or error runs as **one task**
(`ctx.trigger === "websocket"`) with the HTTP limits. Events of one object run one at a time, in order. A plain service
has one object, `default`; a durable service chooses objects by name in code.

```ts
import { sockets } from "std:sockets";
export default {
  fetch(req: Request) {
    if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("upgrade required", { status: 426 });
    return sockets.accept(req, { tags: ["room:lobby"], attachment: { user: "ann" } });   // must be what fetch returns
  },
  webSocketMessage(ws, message) {   // message: string (text) or ArrayBuffer (binary)
    sockets.broadcast(`${ws.attachment.user}: ${message}`, { tag: "room:lobby", except: ws.id });
  },
  webSocketClose(ws, code, reason, wasClean) {},
};
```

| Rule | Detail |
|---|---|
| Accept | Only from an `http` or `call` task whose request offered the upgrade (`not_upgradable` otherwise); return the `accept` response from `fetch` |
| Socket object | `{ id, object, tags, protocol, createdAt, lastMessageAt, autoResponseAt, bufferedAmount, remoteIp, attachment, send(data), close(code, reason), serializeAttachment(v), deserializeAttachment() }`; `attachment` (structured clone, at most 16 KiB) survives between events |
| Output gate | A task acting for an object sends its messages (and its HTTP response) only after the handler returned and storage writes committed; a throwing handler sends and writes nothing. `{ immediate: true }` sends at once |
| Delivery | At least once: a failed event is retried (`ws.max_event_attempts`, default 3), then the socket closes with 1011 |
| Client caps | Message over `plan.ws_max_message_bytes` (1 MiB) closes with 1009; more than 50 msgs/s (burst 200) sustained closes with 1008 |
| Auto-response | `sockets.setAutoResponse({ request: "ping", response: "pong" })`: the node answers, no task runs |
| Origin | Set the service's `ws.allowed_origins` for cookie-authenticated apps (other origins get `403`) |
| Browser + `access = "key"` | Pass the key as a subprotocol: `new WebSocket(url, ["rtk_…", "chat"])` (removed before the guest sees it) |
| Deploys | Sockets survive a deploy (next event runs the new version) unless deployed with `close_sockets` (1012) |
| Errors | Broken connection, ping timeout or a cap arrive as `webSocketClose` with `wasClean` false; `webSocketError` is accepted but not called yet |

### Durable service

Create with `"durable": true` (`21ci service create <slug> --durable`). The default export is a class; one instance per
object name; requests and events run one at a time per object; different objects run in parallel.

```ts
// service "chat-rooms" (durable)
import { sockets } from "std:sockets";
export default class Room {
  constructor(state, env) { this.state = state; }                // state: { id: { name }, name, storage }
  async fetch(req: Request) { return sockets.accept(req, { tags: ["user:" + new URL(req.url).searchParams.get("name")] }); }
  async webSocketMessage(ws, msg) {
    const n = ((await this.state.storage.get("count")) ?? 0) + 1;
    await this.state.storage.put("count", n);                     // committed when the handler returns
    sockets.broadcast(`${n}: ${msg}`);                             // sent after the commit
  }
  async alarm(info) { /* info.scheduledTime, info.retryCount */ }
}

// any service: choose the object in code and forward (the upgrade travels with the request)
import { durable } from "std:durable";
export default { fetch: (req: Request) => durable.get(new URL(req.url).searchParams.get("room")!, { service: "chat-rooms" }).fetch(req) };
```

- A request straight to a durable service goes to its object `default`.
- Cron, interval and queue tasks of a durable class act for **no object**: the class is constructed with name `default`
  but `state.storage` throws `no_object`. To act for an object, call it: `durable.get(name).fetch(req)`.
- The instance (its fields) may survive between events while `ws.warm` is on (default); never rely on it for correctness.
- Storage API and limits: [std-modules.md](std-modules.md#stddurable).

Full example: [sample/apps/chat-room](https://github.com/21-ci/runtime/tree/main/sample/apps/chat-room).
