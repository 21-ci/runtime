# chat-room

A group chat on WebSockets and durable objects. A **gate** service serves the page, checks
room and member names, and forwards each room's requests, the WebSocket upgrade included, to that
room's **durable object** in the **rooms** service. Each room is one object: it keeps its history
in its own storage, knows who is here from its sockets' tags, and answers heartbeats without
waking up.

```
browser ── GET /s/chat/ ───────────────────────▶ gate: the page
        ── WS  /s/chat/rooms/{room}/ws?name=…  ─▶ gate: room and name checked
                                                   └─ durable.get(room, { service: ROOMS_SLUG }).fetch(req)
                                                      └─ Room.fetch: sockets.accept(req, { tags: ["user:<name>"] })
                                                         welcome {history, users}, join → everyone else
        ── {"text": "…"} ───────────────────────▶ Room.webSocketMessage: storage.put(seq, msg:<seq>)
                                                   then (after the commit) broadcast to the room
        ── "ping" ──────────────────────────────▶ the node answers "pong" (auto-response, no event)
        ── close ───────────────────────────────▶ Room.webSocketClose: leave → everyone else
alarm (hourly while the room is used) ──────────▶ Room.alarm: keep the newest 200 messages
```

## Platform features used

| Feature | Where |
|---|---|
| A durable service (`"durable": true`), one class per service | `src/rooms.ts`: `export default class Room { constructor(state, env) … }`, one instance per room |
| `durable.get(name, { service })` | `src/gate.ts`: the room is chosen in code; the upgrade is forwarded, the socket belongs to the room |
| `sockets.accept` with tags and an attachment | presence is the room's sockets' `user:<name>` tags; the attachment carries the member's name |
| `res.socket.send` at accept | the welcome (history, who is here) goes out once the client is connected |
| `sockets.broadcast` with `except` | join and leave notices to everyone else |
| Object storage (`state.storage`) | history `msg:<seq>`, the counter `seq`; written in one commit per message |
| Output gate | a message is broadcast only after its write committed |
| `sockets.setAutoResponse` | `ping` → `pong` by the node: heartbeats never start a task |
| Alarms (`storage.setAlarm` / `alarm()`) | prunes the history while the room is used |
| Sockets survive deploys | a new version of `rooms` serves the next message on the same sockets; `close_sockets` closes them with 1012 |

## Setup

```bash
cd sample/apps/chat-room
21ci push src/rooms.ts --service chat-rooms --create --wait
21ci service update chat-rooms --durable true  # or: PATCH /v1/services/{id} {"durable": true}
21ci push src/gate.ts --service chat --create --wait
21ci env set chat ROOMS_SLUG=chat-rooms
open "$(21ci service get chat -o json | jq -r .invoke_url)"
```

For a cookie-authenticated app, also set `ws.allowed_origins` on the gate (cross-site WebSocket
hijacking): `PATCH /v1/services/{id} {"ws": {"allowed_origins": ["https://chat.example"]}}`.

## Smoke

`./smoke.sh` (also run by `sample/apps/run-smokes.sh`) deploys both services and drives real
WebSocket clients (`test/chat.mjs`, Node 22+): join and presence, history, the auto-response, sockets
held while the room has no isolate (`ws.warm = false`), across a redeploy (the next message runs the
new version), and closed with 1012 by a deploy with `close_sockets`.
