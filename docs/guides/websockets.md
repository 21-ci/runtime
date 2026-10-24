---
title: WebSockets and durable objects
description: Accept WebSockets in Deno and WASM services, keep per-room state in durable objects with storage and alarms, connect out with a WebSocket client, and migrate from Cloudflare Durable Objects.
---

# WebSockets and durable objects

A service can accept WebSockets. The **node** holds every socket; no isolate runs for a socket between two
messages. Each message, close or error runs **one task** of your service with the socket's event, and the events
of one **object** (a room, a document, a game) run one at a time, in order. A **durable service** goes further:
its default export is a class, each object is one instance with its own **storage** and **alarm**, and your code
chooses the object by name. The model follows Cloudflare's hibernatable WebSockets and Durable Objects, so code
written for those moves over with small changes ([migration table](#migrating-from-cloudflare)).

This guide builds a broadcast service, then a chat with one durable object per room (the runtime's
`sample/apps/chat-room`), and covers the outbound client, WASM, security, operations and limits.

## How it works

```mermaid
sequenceDiagram
    participant C as Client
    participant N as Node (socket hub)
    participant T as Task (your handler)
    C->>N: GET /s/chat/… Upgrade: websocket
    N->>T: http task: fetch(req)
    T-->>N: sockets.accept(req)  → 101
    N-->>C: 101 Switching Protocols
    Note over N,T: the task ends; the node keeps the socket
    C->>N: message "hi"
    N->>T: websocket task: webSocketMessage(ws, "hi")
    T-->>N: storage.put(…), sockets.broadcast(…)
    Note over N: commit storage, then send
    N-->>C: "hi" (to every socket of the object)
```

- **Accept.** The upgrade request runs your `fetch` like any request (same access checks, ingress rules and
  limits). `sockets.accept(req)` returns the response to give back; the node answers `101` and keeps the socket.
- **Events are tasks.** A message, a close or an error of a socket runs one task (`ctx.trigger === "websocket"`)
  with HTTP limits (wall time, CPU, memory). Cold start is the same as for a request: about 2 ms p50 for a fresh
  isolate, 0.7 ms when a warm one is at hand (measured on the reference machine).
- **Objects.** Every socket belongs to one object. In a plain service that is the object `default`; in a durable
  service it is the object the request was sent to. Events, alarms and requests of one object run one at a time
  in arrival order; different objects run in parallel.
- **Output gate.** Messages a task sends and storage it writes take effect when the handler returns: storage
  commits first, then the messages go out. A handler that throws sends nothing and writes nothing. Pass
  `{ immediate: true }` to send at once.
- **At least once.** A failed event is delivered again with backoff, 3 times by default; after that, a message's
  socket is closed with `1011`. Write handlers that tolerate a repeat.
- **Deploys keep sockets.** A new version serves the next event on the same sockets. Deploy with
  `--close-sockets` to close them with `1012` instead, so clients reconnect to the new version.
- **Warm objects.** By default the isolate of an event is kept for the next event of the service, so instance
  fields of a durable class often survive between events. Never rely on it: anything that must survive goes into
  storage or the socket's attachment. Turn it off with `ws.warm = false`.

## A broadcast service

Every socket of a plain service belongs to its object `default`. This service lets clients join a channel by tag and
broadcasts each message to everyone else in it.

```ts title="main.ts"
import { sockets } from "std:sockets";

export default {
  fetch(req: Request) {
    if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("connect with a WebSocket", { status: 426 });
    }
    const url = new URL(req.url);
    const channel = url.searchParams.get("channel") ?? "general";
    const user = url.searchParams.get("user") ?? "anon";
    return sockets.accept(req, { tags: [`channel:${channel}`], attachment: { user, channel } });
  },

  webSocketMessage(ws, message) {
    if (typeof message !== "string") return; // binary arrives as an ArrayBuffer
    const { user, channel } = ws.attachment;
    sockets.broadcast(`${user}: ${message}`, { tag: `channel:${channel}`, except: ws.id });
  },

  webSocketClose(ws, code, reason) {
    const { user, channel } = ws.attachment;
    sockets.broadcast(`${user} left (${code})`, { tag: `channel:${channel}` });
  },
};
```

```console
$ 21ci push main.ts --service talk --create --wait
$ websocat "ws://127.0.0.1:8080/s/talk/?channel=ops&user=ann"
```

The attachment (any structured-cloneable value up to 16 KiB) is how a socket remembers who it is: it is stored with
the socket on the node, not in the isolate. Tags (up to 10 per socket) select sockets for `broadcast`, `list`,
`count` and `closeAll`.

## Rooms as durable objects

A **durable service** exports a class. Each object is one instance, created for its name, with its own storage and
alarm. Requests reach an object through `durable.get(name)`, usually from a public front service that checks the
request first. The runtime's chat sample does exactly this: a **gate** serves the page and forwards each room's
requests, the upgrade included, to the room's object in the **rooms** service.

```ts title="gate.ts"
--8<-- "sample/apps/chat-room/src/gate.ts:5:5"

--8<-- "sample/apps/chat-room/src/gate.ts:34:47"
```

The socket accepted by the room belongs to the room object, not to the gate: its events run `Room` handlers, one at
a time per room.

```ts title="rooms.ts"
--8<-- "sample/apps/chat-room/src/rooms.ts:40:104"
```

What the room relies on:

- **Presence from tags.** `sockets.list()` inside the room lists that room's sockets; their `user:` tags are the
  members. Nothing is kept in memory.
- **History in storage.** `storage.put({ seq, ["msg:…"]: msg })` writes two keys in one commit; `storage.list` with
  a prefix reads the newest messages back.
- **Broadcast after commit.** `sockets.broadcast` in `webSocketMessage` goes out after the writes committed, so no
  member ever sees a message the room did not keep.
- **Auto-response.** `setAutoResponse({ request: "ping", response: "pong" })` makes the node answer heartbeats
  itself; they never start a task.
- **An alarm.** `storage.setAlarm(time)` runs `alarm()` at that time, at least once, even after a node restart.

Deploy it:

```bash
21ci service create chat-rooms --durable --access internal
21ci push src/rooms.ts --service chat-rooms --wait
21ci push src/gate.ts --service chat --create --wait
21ci env set chat ROOMS_SLUG=chat-rooms
```

`--access internal` keeps the rooms off the public listener: only service calls reach them. The gate may call
them because both are in the same project and environment ([who may call a service](calls-and-queues.md#who-may-call-a-service)).

### The class

```ts
export default class Room {
  constructor(state, env) {}                          // state: { id: { name }, name, storage, blockConcurrencyWhile }
  async fetch(req, ctx) {}                            // requests sent with durable.get(name).fetch(req)
  async webSocketMessage(ws, message, ctx) {}         // message: string | ArrayBuffer
  async webSocketClose(ws, code, reason, wasClean, ctx) {}
  async webSocketError(ws, error, ctx) {}             // error.code; see below
  async alarm(info, ctx) {}                           // info: { scheduledTime, retryCount }
}
```

The constructor runs whenever no warm instance of that object is at hand: keep it cheap and read storage lazily.
A missing handler means "nothing to do". Today the node reports a broken connection, a ping timeout or a cap
(`1006`, `1008`, `1009`, `1013`) as `webSocketClose` with `wasClean` false; `webSocketError` is accepted for
Cloudflare compatibility and is not called yet. A cron, interval or queue task of a durable service runs the class's
`scheduled` / `queue` method without an object (no storage).

### Storage

`state.storage` (also `ctx.storage` and `import { storage } from "std:durable"`) is the object's own key-value store.
It lives as rows of the service's SQLite database, keyed by object; there is no file per object.

| Call | Notes |
|---|---|
| `get(key)`, `get([keys])` | A value, or a `Map` for several keys. The task's own writes are seen first |
| `put(key, value)`, `put({ key: value, … })` | Values are structured clones (objects, arrays, `Date`, `Map`, typed arrays, …) |
| `delete(key)`, `delete([keys])`, `deleteAll()` | `deleteAll` also clears the alarm |
| `list({ prefix, start, end, limit, reverse })` | A `Map` in key order, `[start, end)`, at most 1,000 |
| `getAlarm()`, `setAlarm(Date \| ms)`, `deleteAlarm()` | One alarm per object |

All writes of one task commit together in one transaction when the handler returns. Keys are up to 2 KiB, values up
to 1 MiB, at most 1,000 writes per task, and everything counts against the tenant's `storage_mb`.
`21ci objects ls chat-rooms` shows the objects with storage; `21ci objects rm chat-rooms lobby` drops one.

## The sockets API

`import { sockets } from "std:sockets"` works in every task of the service, scoped to it. Without `object`, calls
address the task's own object (a socket event, an alarm, a request to a durable object), else every object.

| Call | Does |
|---|---|
| `accept(req, { tags?, attachment?, protocol? })` | Accept the upgrade; return the result from `fetch`. `res.socket` is the new socket (sends before the client connects are queued) |
| `send(id, data)`, `ws.send(data)` | Text or binary (up to 1 MiB) |
| `broadcast(data, { object?, tag?, except? })` | To every matching socket; returns `{ sent }` |
| `close(id, code?, reason?)`, `ws.close(…)`, `closeAll({ object?, tag?, code?, reason? })` | Codes 1000 or 3000-4999 |
| `list({ object?, tag?, limit?, cursor? })`, `get(id)`, `count({ object?, tag? })` | Socket infos: `id`, `object`, `tags`, `protocol`, `createdAt`, `lastMessageAt`, `bufferedAmount`, `remoteIp`, `attachment` |
| `objects({ prefix?, limit?, cursor? })` | Objects holding sockets |
| `setTags(id, tags)`, `setAttachment(id, v)`, `getAttachment(id)` | Also `ws.serializeAttachment(v)` / `ws.deserializeAttachment()` |
| `setAutoResponse({ request, response } \| null)` | The node answers that exact text message itself |

Errors carry a `code`: `not_found`, `invalid_request`, `too_large`, `limit_exceeded`, `slow_consumer`,
`not_upgradable`. The full reference is in the [guest API](../reference/guest-api.md#stdsockets).

## Connecting out

Deno guests have a standard `WebSocket` client for outbound connections:

```ts
const ws = new WebSocket("wss://stream.example.com/ticks", ["v1"]);
const first = await new Promise((resolve, reject) => {
  ws.onmessage = (e) => resolve(e.data);
  ws.onerror = (e) => reject(new Error(e.code)); // egress_denied, egress_timeout, egress_upstream, egress_quota
});
ws.close(1000, "done");
```

It goes through the same egress guard as `fetch`: the service's `egress` rules, public addresses only, one of the
task's fetches, received bytes counted against the daily egress quota. The connection lives as long as the task:
for a long-lived feed, read in a scheduled task and hand the data on, or let clients connect to your service instead.

## WASM services

A component accepts sockets with `std:host/sockets` and handles events by exporting `std:host/socket-handler`
(world `socket-service` in [`std:host`](../reference/wit.md)). `wasi:http` hides the `Upgrade` header, so the
runtime marks an upgrade with `x-rt-websocket: 1`.

```rust
wit_bindgen::generate!({ path: "wit", world: "socket-service" });
use exports::std::host::socket_handler;
use std::host::{durable, sockets};

// In the wasi:http handler: accept, then answer 101.
let upgrade = !request.headers().get("x-rt-websocket").is_empty();
if upgrade {
    let opts = sockets::AcceptOptions { tags: vec!["lobby".into()], attachment: None, protocol: None };
    let status = if sockets::accept(&opts).is_ok() { 101 } else { 429 };
    // respond with `status` and an empty body
}

impl socket_handler::Guest for Component {
    fn on_message(_object: String, socket: sockets::SocketInfo, data: sockets::Payload) -> Result<(), String> {
        let n = durable::get("count").ok().flatten().map_or(0, |v| v[0]) + 1;
        durable::put("count", &[n]).map_err(|e| e.code)?;
        sockets::send(&socket.id, &data).map_err(|e| e.code) // echo, after the commit
    }
    fn on_close(_: String, _: sockets::SocketInfo, _: socket_handler::CloseInfo) -> Result<(), String> { Ok(()) }
    fn on_error(_: String, _: sockets::SocketInfo, _: String, _: String) -> Result<(), String> { Ok(()) }
    fn on_alarm(_: String, _scheduled_ms: i64, _retry: u32) -> Result<(), String> { Ok(()) }
}
```

An `Err` or a trap fails the event (it is retried). `std:host/durable` gives storage as bytes and
`durable.fetch(service, object, request)`; `std:host/ws-client` is the outbound client (`connect`, then
`send` / `recv(timeout-ms)` / `close`). A durable WASM service must export `socket-handler`: a deploy without it
fails with `422 missing_export`.

## Security

- **Authenticate at the upgrade.** The upgrade is an ordinary request: `access: "key"`, ingress rules and your own
  checks in `fetch` (a cookie, a token in the query) run before you call `accept`. A browser cannot set
  `Authorization` on a WebSocket, so it may pass an `rtk_` key as the first subprotocol:
  `new WebSocket(url, ["rtk_…", "chat"])`. The runtime removes it before your code sees the request.
- **Check the origin.** Browsers send cookies with cross-site WebSocket requests. For cookie-authenticated apps set
  `ws.allowed_origins`: other origins get `403 origin_not_allowed` before any code runs.

    ```console
    $ 21ci service update chat --set 'ws.allowed_origins=["https://chat.example.com"]'
    ```

- **Size and rate caps.** A client message over `ws_max_message_bytes` (1 MiB) closes the socket with `1009`. A
  client over 50 messages per second (burst 200) is read more slowly, and closed with `1008` if it keeps it up.
  A client that does not read is closed with `1013` once 1 MiB is queued for it.
- **Isolation.** A service sees only its own sockets and objects; another service's socket id is `not_found`.
  Attachments never leave the node through the API.

See the runtime's `SECURITY.md` for the threat model.

## Operating

| Task | Command |
|---|---|
| Who is connected | `21ci objects ls chat-rooms`, `21ci sockets ls chat-rooms --object lobby` |
| Kick a client | `21ci sockets close chat-rooms ws_… --object lobby --code 4000 --reason kicked` (scope `sockets:write`) |
| Kick by tag | `21ci sockets close chat-rooms --object lobby --tag user:ann` |
| Deploy and reconnect everyone | `21ci push rooms.ts --service chat-rooms --close-sockets` (also `rollback --close-sockets`) |
| Drop a room's storage | `21ci objects rm chat-rooms lobby` (scope `data:admin`) |
| Node view | `21ci node status --json` (`sockets: {open, objects, pending, max}`, operator) |

The same views are MCP tools (`list_objects`, `list_sockets`, `close_socket`) for AI assistants. Metrics:
`rt_ws_sockets`, `rt_ws_upgrades_total{result}`, `rt_ws_messages_total{dir}`, `rt_ws_event_tasks_total{outcome}`
and `rt_ws_closed_total{code}` ([metrics](../reference/metrics.md)).

**Restarts and drains.** A drained node closes its sockets with `1001` and waits `ws.drain_grace` for running event
tasks; a node restart drops them. Clients should reconnect with backoff. Storage and alarms survive both.

## Limits

| Limit | Default | Where |
|---|---|---|
| Open sockets per node | 20,000 | `[ws] max_sockets` |
| Open sockets per tenant / per object | 1,000 / 1,000 (free plan) | plan `max_sockets`, `max_sockets_per_object` |
| Open sockets per service | no own cap (the plan's apply) | service `ws.max_sockets` |
| Client message | 1 MiB (close `1009`) | plan `ws_max_message_bytes` |
| Client rate | 50/s, burst 200 (close `1008` after 10 s) | `[ws] msgs_per_sec`, `msgs_burst`, `flood_close_after` |
| Queued for one client | 1 MiB (close `1013`) | `[ws] send_buffer_kb` |
| Message sent by a guest | 1 MiB | |
| Tags / attachment | 10 × 256 bytes / 16 KiB | |
| Event deliveries / alarm deliveries | 3 / 6 | `[ws] max_event_attempts`, `max_alarm_attempts` |
| Storage key / value / writes per task | 2 KiB / 1 MiB / 1,000 | |
| Idle socket on the node | about 5-6 KiB | measured |
| Ping | every 30 s; silent for two intervals: dropped | `[ws] ping_interval` |

## Migrating from Cloudflare

| Cloudflare | 21 Cloud runtime | Notes |
|---|---|---|
| `export class Room extends DurableObject` + a Worker | A durable service (`--durable`) whose default export is the class; a second service as the front | One durable class per service |
| `env.ROOMS.idFromName(name)`, `env.ROOMS.get(id)` | `durable.get(name, { service: "chat-rooms" })` | `idFromName` and `newUniqueId` exist; objects are addressed by name. No `idFromString` |
| `stub.fetch(req)` | `durable.get(name).fetch(req)` | Upgrades are forwarded: the socket belongs to the object |
| RPC methods on the stub | `fetch` only | Use JSON requests, or [service RPC](calls-and-queues.md) between services |
| `new WebSocketPair()`, `this.ctx.acceptWebSocket(server, tags)`, `new Response(null, { status: 101, webSocket: client })` | `return sockets.accept(req, { tags })` | No `WebSocketPair` |
| `server.accept()` (non-hibernating) | — | Every socket is hibernatable: there are no in-isolate socket listeners |
| `webSocketMessage`, `webSocketClose`, `webSocketError` | Same names and arguments, plus `ctx` | Also on a plain service's default export |
| `this.ctx.getWebSockets(tag)` | `sockets.list({ tag }).sockets` | Paged; `sockets.count({ tag })` |
| `ws.serializeAttachment` / `deserializeAttachment` | Same, or `ws.attachment` | 16 KiB |
| `this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"))` | `sockets.setAutoResponse({ request: "ping", response: "pong" })` | |
| `this.ctx.storage.get/put/delete/list/deleteAll` | `state.storage.*` | Same shapes; keys up to 2 KiB, values up to 1 MiB |
| `this.ctx.storage.sql` | — | Use `std:sql` (one database per service, shared by its objects): key rows by object name |
| `setAlarm`, `getAlarm`, `deleteAlarm`, `alarm()` | Same | `alarm(info)` gets `{ scheduledTime, retryCount }` |
| Input and output gates | One task at a time per object; writes commit and sends go out when the handler returns | No interleaving at `await` to reason about |
| `blockConcurrencyWhile(fn)` | Exists; runs `fn` | Already serialized |
| Deploys disconnect sockets | Sockets stay; the next event runs the new version | `--close-sockets` for the Cloudflare behavior |
| Objects placed worldwide | One node | No multi-node or migration of objects yet |
| `fetch()` to a `wss://` URL with `Upgrade` | `new WebSocket(url)` | Through the egress guard, bound to the task |
| 32 MiB messages | 1 MiB by default | Plan `ws_max_message_bytes` |

## Not supported

- Several nodes: sockets and objects live on one node; a restart drops sockets (storage stays).
- WebSockets over HTTP/2 (RFC 8441) and the `permessage-deflate` extension.
- Per-object SQL (`storage.sql`) and RPC stubs.
- Outbound sockets that outlive the task that opened them.

## See also

- [Service calls and queues](calls-and-queues.md): calling the durable service from a front.
- [Configure a service](configure.md): the `ws` block and triggers.
- [Guest API](../reference/guest-api.md#stdsockets) and [WIT](../reference/wit.md) references.
- [Configuration](../config.md): the `[ws]` section.
