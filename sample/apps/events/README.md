# events

A backend for meetups, workshops and other events. An admin publishes events with a title, start time,
capacity and description. Anyone can list them and register with a name plus an email or a Telegram
username. Each registrant gets a private token for looking up or cancelling the registration, and the
admin can export the registrations as JSON or CSV. The sample is a plain JSON API, so any static page,
bot or form can sit in front of it.

```
client ──▶ events (Deno, Form A)
             │ POST /events/{id}/registrations
             │   per-address throttle: kv.incr("throttle/register/<ip>/<minute>") → 429 Retry-After
             │   Idempotency-Key? → kv "idem/<sha256>" (24 h): replay the first answer
             │   sql.transaction: event open? not started? contact not registered? seats left? → INSERT
             │   token "evr_…" returned once; only its SHA-256 is stored
             │ DELETE /registration  (X-Registration-Token) → status = cancelled (idempotent)
             │ admin (Bearer ADMIN_TOKEN): create / update / close / reopen, JSON pages, streamed CSV
             ▼
           std:sql: events, registrations (partial unique indexes per active contact)
```

## Platform features used

| Feature | Where |
|---|---|
| `std:sql` `transaction` | the capacity and duplicate checks and the insert run atomically, so there is no overbooking under concurrency |
| SQLite partial unique indexes | one active registration per email / Telegram username and event; a cancelled one frees the contact |
| `std:kv` with TTL | `Idempotency-Key` replay store (24 h) |
| `std:kv` `incr` with TTL, `ctx.client.ip` | `src/throttle.ts`: registrations per minute and client address (IPv6 per /64), an atomic fixed-window counter |
| Streamed `Response` body | CSV export in pages of 1,000 rows (`ReadableStream`, up to the 100 MB streamed limit) |
| Read-first schema creation | as in `sample/deno/sql-todo`: GETs never write |
| Secrets via `Deno.env` | `ADMIN_TOKEN`, compared in constant time |
| `std:services` RPC | `src/main.ts` `notifyRegistration`: `services.bind(NOTIFY_SERVICE).confirmRegistration(...)` after each registration; `notify/main.ts` answers with a receipt and reads the stamped `ctx.caller`. Best effort: a failing notifier logs and the registration stands |

## Setup

```bash
cd sample/apps/events
21ci push notify --service events-notify --create --wait      # the confirmation service (RPC)
21ci push src --service events --create --wait
ADMIN=$(openssl rand -hex 24); printf %s "$ADMIN" | 21ci secret set events ADMIN_TOKEN
21ci env set events NOTIFY_SERVICE=events-notify               # confirmations over RPC (optional)
21ci env set events CORS_ORIGIN=https://events.example.com      # optional, for a browser frontend elsewhere
```

| Name | Kind | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | secret | Bearer token for the admin routes (at least 16 chars) |
| `CORS_ORIGIN` | env | Optional `Access-Control-Allow-Origin` value (an origin or `*`) |
| `THROTTLE_PER_MINUTE` | env | Registrations one client address may send per minute, default 20; `0` turns the throttle off |
| `NOTIFY_SERVICE` | env | Slug of the notifier service (`notify/`, deployed next to this one); unset = no confirmations |

## API

| Method and path | Auth | Answer |
|---|---|---|
| `GET /events` | none | `{events: [event]}`: upcoming events by start time (`?past=1` lists all) |
| `GET /events/{id}` | none | `event` = `{id, title, description, location, startsAt, capacity, registered, spotsLeft, status, registrationOpen, createdAt, updatedAt}` |
| `POST /events/{id}/registrations` | none | body `{name, email?, telegram?}` (at least one contact) → `201 {registration, token}`; `409` `event_full` / `already_registered` / `event_closed` / `event_started`; `429 rate_limited` with `Retry-After`; optional `Idempotency-Key` header |
| `GET /registration` | `X-Registration-Token` | `{registration, event}` |
| `DELETE /registration` | `X-Registration-Token` | `{registration}` with `status: cancelled`; repeating it is fine; `409 event_started` after the start |
| `POST /events` | admin | body `{title, starts_at, capacity, description?, location?}` → `201 event` |
| `PATCH /events/{id}` | admin | any of those fields; `409 capacity_below_registered` |
| `POST /events/{id}/close`, `/reopen` | admin | `event` |
| `GET /events/{id}/registrations` | admin | `{event, registrations, next}`; `?status=confirmed\|cancelled\|all&limit=500&after=<id>` |
| `GET /events/{id}/registrations.csv` | admin | `text/csv` download: `id,name,email,telegram,status,registered_at,cancelled_at` |

`starts_at` must be ISO 8601 with an offset (`2026-11-20T18:00:00+05:00` or `…Z`) and in the future; responses use UTC.
Errors are `{"error": {"code", "message"}}`; validation errors are `400 validation_failed`.

```bash
B=http://127.0.0.1:8080/s/events
curl -sS -X POST $B/events -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"title": "Rust meetup", "starts_at": "2026-11-20T18:00:00+05:00", "capacity": 40, "location": "IT Park"}'
curl -sS $B/events
curl -sS -X POST $B/events/1/registrations -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"name": "Ada", "telegram": "@ada_l"}'                # keep the returned token
curl -sS -X DELETE $B/registration -H "X-Registration-Token: evr_…"
curl -sS -H "Authorization: Bearer $ADMIN" -o reg.csv $B/events/1/registrations.csv
```

## Tests

```bash
node --test test/*.test.ts     # validation, CSV escaping, tokens, throttle (Node >= 22.18), or: deno test
./smoke.sh                     # against a node: deploys events-smoke and runs the whole flow, throttle included
```

## Notes and limits

- **No overbooking.** Registration reads the event, checks for duplicates, counts seats and inserts in one SQLite
  transaction. The single writer serialises concurrent registrations, and the partial unique indexes are the backstop.
- **Tokens.** The registration token (`evr_` plus 32 random bytes) is shown once. Only its SHA-256 is stored, so a
  database export cannot cancel anyone's registration. The `Idempotency-Key` replay store keeps the first answer,
  token included, for 24 hours in the service's own KV.
- **CSV safety.** Cells that start with `= + - @`, a tab or CR get a leading `'`, so a registrant's name cannot run as a
  spreadsheet formula. The file starts with a UTF-8 BOM so Excel shows Cyrillic and Uzbek names correctly.
- **Abuse.** Registration is public. Each client address may register `THROTTLE_PER_MINUTE` times per minute
  (`429 rate_limited` with `Retry-After` after that; idempotent retries count too). The address is `ctx.client.ip`,
  which the platform resolves behind `ingress.trusted_proxies` and a client cannot spoof; IPv6 counts per /64. `kv.incr`
  counts atomically, so parallel requests cannot slip past the limit, but it is a fixed window: up to twice the limit
  fits across a minute boundary, and people behind one NAT share a budget. Keep the service's ingress rate limit as the
  global cap (`PATCH /v1/services/{id}` `{"ingress": {"rate_limit": {"rps": 5, "burst": 20}}}`). A KV failure lets the
  request through (logged). Contacts are not verified (no email is sent). Plug in a mail API through `fetch` if you
  need confirmation.
- **Admin token.** Send it as `Authorization: Bearer …`: on this public service the header reaches the handler
  unchanged (the platform removes only its own API keys, `rtk_…`, so do not use one as `ADMIN_TOKEN`).
- Lists return at most 200 events; registrations page by id (`next`).
