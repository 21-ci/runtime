# cbu-rates

UZS exchange rates from the Central Bank of Uzbekistan. An interval schedule polls
`https://cbu.uz/uz/arkhiv-kursov-valyut/json/` every 60 seconds. History is stored only when a rate
or its date changes, and when a rate moves, the service enqueues a notification (for example, to the
[`telegram-bot`](../telegram-bot/) sample). It also serves a small public read API with latest rates,
history and conversion.

```
schedule (every 60 s) ──▶ scheduled()
                            │ back-off check (3 failures in a row → wait 1, 2, 4 … 30 min)
                            │ fetch cbu.uz (10 s timeout, ≤ 1 MB, JSON array, item-by-item validation)
                            │ sql.transaction: compare with `latest` → INSERT OR IGNORE history, upsert latest
                            │ enqueue(NOTIFY_SERVICE, {type: "cbu.rate_changed", …}, {dedupId: hash})   (optional)
                            ▼
GET /latest  /history  /convert  /status        (public, CORS, cache 60 s)
POST /admin/refresh  /admin/ingest  /admin/backfill   (ADMIN_TOKEN)
```

## Platform features used

| Feature | Where |
|---|---|
| Form B `{fetch, scheduled}` + interval schedule | the poll runs in `scheduled`; schedules never overlap, missed ticks are skipped |
| Egress `fetch` with `AbortController` timeout | `fetchFeed()`; runtime errors (`egress_timeout`, `egress_dns`, …) are recorded, not thrown |
| `std:sql` `transaction` | compare-and-write in one transaction, so two concurrent polls cannot both record a change |
| `std:sql` upsert, `INSERT OR IGNORE`, unique keys | idempotent history: one row per `(currency, date, rate)` |
| `std:kv` | poll status and back-off state |
| `std:queue` `enqueue` with `dedupId` | change notifications to another service; the same change is announced once in 24 h |
| Secrets | `ADMIN_TOKEN` for the admin routes |

## Setup

```bash
cd sample/apps/cbu-rates
21ci push src --service cbu-rates --create --wait
openssl rand -hex 24 | tee /dev/stderr | 21ci secret set cbu-rates ADMIN_TOKEN     # keep the printed token
21ci schedule add cbu-rates --every 60s                                           # free plan minimum is 60 s

# optional: rate alerts through the telegram-bot sample (same tenant and environment)
21ci env set cbu-rates NOTIFY_SERVICE=telegram-bot NOTIFY_CCYS=USD,EUR,RUB

# fill 30 days of history from the CBU archive
curl -sS -X POST -H "Authorization: Bearer $ADMIN" 'http://127.0.0.1:8080/s/cbu-rates/admin/backfill?days=30'
```

Without `21ci`: `../../scripts/deploy.sh src cbu-rates`, then
`curl -X POST $RT_API/v1/services/$SVC/schedules -d '{"kind":"interval","every_seconds":60}'` (see `sample/deno/scheduled-queue`).

| Name | Kind | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | secret | Bearer token for `/admin/*` (at least 16 chars) |
| `NOTIFY_SERVICE` | env | Slug of a same-tenant queue consumer; unset = no notifications |
| `NOTIFY_CCYS` | env | Currencies to announce, default `USD,EUR,RUB` |
| `CBU_BASE_URL` | env | Feed base URL, default `https://cbu.uz/uz/arkhiv-kursov-valyut/json/` |

## API

| Method and path | Auth | Answer |
|---|---|---|
| `GET /latest?ccy=USD,EUR` | none | `{source, date, rates: [{ccy, code, name, nominal, rate, diff, date, perUnit}]}`; all currencies without `ccy`; `404 no_data` before the first poll |
| `GET /history?ccy=USD&days=30` | none | `{ccy, days, points: [{date, rate, diff, nominal, perUnit, observedAt}]}`, `days` 1-366 |
| `GET /convert?amount=100&from=USD&to=EUR` | none | `{amount, from, to, result, rates}`; `UZS` works on either side, `to` defaults to `UZS` |
| `GET /status` | none | `{lastAttemptAt, lastSuccessAt, consecutiveFailures, lastError, backingOff, nextAttemptAt}` |
| `POST /admin/refresh` | admin | poll now, ignoring the back-off: `200 {fetched, skipped, changed, notify}` or `502 {error}` |
| `POST /admin/ingest` | admin | body: a CBU-format JSON array (≤ 1 MB), processed like a poll; `422` if no item is valid |
| `POST /admin/backfill?days=30` | admin | fetches `json/all/<date>/` for each of the last `days` (≤ 31) days, 4 at a time |

Rates are decimal strings exactly as the CBU publishes them (`"11778.45"`, UZS per `nominal` units); `perUnit` is a
number for convenience. Errors are `{"error": {"code", "message"}}`.

```bash
B=http://127.0.0.1:8080/s/cbu-rates
curl -sS "$B/latest?ccy=USD,EUR,RUB"
curl -sS "$B/history?ccy=USD&days=7" | jq '.points[] | [.date, .rate]'
curl -sS "$B/convert?amount=100&from=USD&to=EUR"
```

The notification message (consumed by `telegram-bot`):

```json
{"type": "cbu.rate_changed", "key": "cbu:3f1a…", "changes": [
  {"ccy": "USD", "date": "2026-10-07", "rate": "11790.10", "diff": "11.65", "nominal": 1, "previous": "11778.45"}]}
```

## Tests

```bash
node --test test/*.test.ts       # parsing, change detection, back-off, conversion (Node >= 22.18), or: deno test
./smoke.sh                       # against a node: deploys cbu-rates-smoke
```

## Notes and limits

- **What counts as a change.** A history row is written when a currency is new, its date moves forward, or its rate is
  corrected for the same date. A date older than the stored one (backfill, a lagging mirror) only adds history.
  Notifications go out only when the rate itself moved for a currency in `NOTIFY_CCYS`, not on the first value.
- **Outages and bad data.** Non-200 answers, HTML error pages, cut-off or oversized bodies and empty arrays count as
  failures. Single bad items are skipped and logged, and the rest of the payload is kept. After 3 failures in a row
  the poll backs off exponentially, up to 30 minutes. `GET /status` shows the state.
- **Notification failures** (`not_found`, `call_not_allowed` when the consumer is in another environment, `queue_full`)
  are logged and returned in `notify.reason`. They never fail the poll. The `dedupId` makes a retried enqueue safe.
- Polling every 60 s suits a feed that changes once a day only because each poll is cheap (one request, about 26 KB).
  A schedule of `--every 5m` is just as good.
- Dates are calendar dates in Tashkent (UTC+5), which is how the CBU publishes them.
