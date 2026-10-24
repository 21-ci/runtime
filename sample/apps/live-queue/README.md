# live-queue

An electronic queue, like the ticket machine at a passport office, without the machine. Staff open a
queue ("Passport office, window 3"). People scan a link or QR code, take a numbered ticket on their
phone, and watch their place and estimated wait update live. When it is their turn, the phone vibrates.
Staff call the next one or more people from a board, mark no-shows, or pause the queue. A TV page
shows who is being served now.

```
phone ── GET /q/{id} ─────────────▶ HTML (CSP nonce, no server-side data in the markup)
      ── POST /api/queues/{id}/tickets ──▶ per-address throttle: kv.incr("throttle/ticket/<ip>/<minute>") → 429
                                          sql.transaction: next_number++, INSERT ticket → {number, token}
      ── GET  /api/queues/{id}/tickets/{n}?since=V   (X-Ticket-Token)
              └─ long-poll: waits until queues.version ≠ V (1 read/s), at most LONGPOLL_SECONDS,
                 always inside the task's wall limit (ctx.deadline)
staff ─ POST /api/queues/{id}/call-next {count, counter} ──▶ called → done, next N waiting → called,
                                                            calls(at, admitted) row, version++
ETA = time until the next admission + people ahead × seconds per person
      (seconds per person = span of the last hour's calls ÷ people admitted)
```

## Platform features used

| Feature | Where |
|---|---|
| `std:sql` `transaction` | ticket numbering under concurrency: read and advance `next_number`, then insert, in one transaction |
| `std:sql` read `batch` | each view (summary, ticket, board) is one consistent read snapshot in one round trip |
| Long-polling in a handler | `waitForChange()`: timers plus cheap indexed reads, bounded by `ctx.deadline` (30 s wall limit for HTTP) |
| HTML served by the service | `src/pages.ts`: CSP with a per-response nonce, `x-rt-base-path` for links under `/s/{slug}` |
| `std:kv` `incr` with TTL, `ctx.client.ip` | `src/throttle.ts`: tickets per minute and client address (IPv6 per /64), an atomic fixed-window counter |
| Secrets via `Deno.env` | `ADMIN_TOKEN` (constant-time check); ticket tokens are stored only as SHA-256 |

## Setup

```bash
cd sample/apps/live-queue
21ci push src --service live-queue --create --wait
ADMIN=$(openssl rand -hex 24); printf %s "$ADMIN" | 21ci secret set live-queue ADMIN_TOKEN
21ci env set live-queue LONGPOLL_SECONDS=20 THROTTLE_PER_MINUTE=30   # optional, the defaults
```

Open `http://<node>/s/live-queue/admin`, sign in with the token, open a queue, and share its link
(`…/s/live-queue/q/<id>`, for example as a QR code at the door). Put `…/q/<id>/screen` on a TV.

| Name | Kind | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | secret | Staff token for the API and the admin pages (at least 16 chars) |
| `LONGPOLL_SECONDS` | env | How long a request waits for a change: 0-25, default 20; 0 turns long-polling off (clients poll every 3 s) |
| `THROTTLE_PER_MINUTE` | env | Tickets one client address may take per minute, default 30; `0` turns the throttle off |

## Pages

| Path | Who | What |
|---|---|---|
| `/` | anyone | landing page, enter a queue code |
| `/q/{id}` | people | queue status, "Take a ticket" |
| `/q/{id}/ticket?n=12` | the ticket holder | big number, people ahead, ETA, live updates, vibration and an optional notification on the call, "Leave the queue" |
| `/q/{id}/screen` | TV | now serving (number and counter), people waiting |
| `/admin` | staff | sign in, open a queue, list queues |
| `/admin/q/{id}` | staff | call next N at a counter, done / no-show / skip, pause, waiting list, stats |

The ticket token is kept in `localStorage` and also passed once in the URL fragment (`#lqt_…`, never sent to the
server), so the ticket page still works when storage is off. The admin token stays in `sessionStorage` for that tab only.

## API

| Method and path | Auth | Answer |
|---|---|---|
| `POST /api/queues` | admin | body `{name, minutesPerPerson?: 3, maxWaiting?: 500}` → `201 {queue, links}` |
| `GET /api/queues` | admin | `{queues: [{id, name, status, waiting, createdAt}]}` |
| `GET /api/queues/{id}?since=V` | none | `queue` = `{id, name, status, version, waiting, nowServing, secondsPerPerson, etaForNewTicket, updatedAt}` |
| `POST /api/queues/{id}/tickets` | none | body `{name?}` → `201 {token, queue, ticket, ahead, etaSeconds}`; `409 queue_paused` / `queue_full`; `429 rate_limited` with `Retry-After` |
| `GET /api/queues/{id}/tickets/{n}?since=V` | `X-Ticket-Token` | `{queue, ticket: {number, status, counter, createdAt, calledAt}, ahead, etaSeconds}` |
| `DELETE /api/queues/{id}/tickets/{n}` | `X-Ticket-Token` | leave the queue: same view with `status: cancelled` |
| `GET /api/queues/{id}/board?since=V` | admin | `{queue, called, waitingList, stats: {served, skipped}}` (last 24 h) |
| `POST /api/queues/{id}/call-next` | admin | body `{count?: 1-20, counter?: "1"}` → board plus `calledNow: [numbers]` |
| `POST /api/queues/{id}/tickets/{n}/done`, `/skip` | admin | board; `409 invalid_state` |
| `POST /api/queues/{id}/pause`, `/resume` | admin | board; while paused no tickets are issued, and calling still works |

Ticket states: `waiting → called → done`, `waiting|called → skipped` (staff), `waiting|called → cancelled` (the person).
`since=V` makes a request wait until the queue's `version` differs from `V`. Without `since`, or with a stale value,
the answer is immediate. Errors are `{"error": {"code", "message"}}`.

```bash
B=http://127.0.0.1:8080/s/live-queue
curl -sS -X POST $B/api/queues -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{"name": "Window 3"}'
curl -sS -X POST $B/api/queues/$Q/tickets -H 'Content-Type: application/json' -d '{"name": "Ada"}'     # → token
curl -sS "$B/api/queues/$Q/tickets/1?since=0" -H "X-Ticket-Token: $T"
curl -sS -X POST $B/api/queues/$Q/call-next -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"count": 2, "counter": "window 3"}'
```

## Tests

```bash
node --test test/*.test.ts   # ETA math, validation, ids; every page's inline script parses, CSP rules; throttle (Node >= 22.18)
./smoke.sh                   # against a node: deploys live-queue-smoke, 5 parallel takes, long-poll timing, all actions, throttle
```

## Notes and limits

- **Long-polling costs a task slot.** Each open ticket page holds one in-flight task for up to `LONGPOLL_SECONDS`.
  It uses almost no CPU (one indexed read per second) but counts against the service's concurrency, which is adaptive
  (D-A3, D-B2) and queues requests instead of rejecting them. For a large crowd, raise `scaling.max_instances`
  (up to the plan's `max_instances`), keep some instances warm with `scaling.min_instances`, lower
  `LONGPOLL_SECONDS`, or set it to 0 so clients poll every 3 s. The runtime has no WebSocket or server-push
  primitive for guests (`WebSocket` is removed), so long-polling is the live channel here.
- **Numbering** is gap-free and unique: the transaction serialises on SQLite's single writer.
- **ETA** comes from real throughput, the calls of the last hour, whatever the number of counters or the batch size,
  clamped to 5 s to 2 h per person. Until there are two calls, it uses the queue's `minutesPerPerson`.
- **Abuse.** Taking tickets is public. `maxWaiting` caps a queue (default 500), and each client address may take
  `THROTTLE_PER_MINUTE` tickets per minute (`429 rate_limited` with `Retry-After` after that). The address is
  `ctx.client.ip`, which the platform resolves behind `ingress.trusted_proxies` and a client cannot spoof; IPv6 counts
  per /64. `kv.incr` counts atomically, so parallel requests cannot slip past the limit, but it is a fixed window: up to
  twice the limit fits across a minute boundary. People on the venue's Wi-Fi share one address, hence the generous
  default. Keep an ingress rate limit as the global cap for public deployments
  (`{"ingress": {"rate_limit": {"rps": 5, "burst": 20}}}`). A KV failure lets the request through (logged).
- **Admin token.** Staff send it as `Authorization: Bearer …`, which reaches the service unchanged; the platform removes
  only its own API keys (`rtk_…`) from that header, so do not use one as `ADMIN_TOKEN`.
- Times on pages are shown in the viewer's local time zone.
