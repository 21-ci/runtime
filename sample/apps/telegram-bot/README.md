# telegram-bot

A Telegram bot that runs on webhooks. It keeps notes and reminders for each chat, and it sends
alerts when the Central Bank of Uzbekistan changes its exchange rates (together with
[`cbu-rates`](../cbu-rates/)). Each chat has its own small state machine for multi-step dialogs.

```
Telegram ──POST /webhook──▶ telegram-bot (Deno, Form B)
            secret header      │ 1. constant-time check of X-Telegram-Bot-Api-Secret-Token
                               │ 2. INSERT OR IGNORE update_id   (std:sql, dedupe of re-deliveries)
                               │ 3. state = kv.get("fsm/<chat>") → transition() → effect
                               │ 4. effect: notes / reminders / subscribers (std:sql)
                               │            reminder → enqueue(self, {reminder}, delay ≤ 7 d)
                               │            /rates   → call("cbu-rates", GET /latest)  (std:services)
                               │ 5. sendMessage (Bot API, token from secrets), then save the state
                               ▼
cbu-rates ──enqueue {cbu.rate_changed}──▶ queue() ──fan-out──▶ one {send} message per subscriber
```

## Platform features used

| Feature | Where |
|---|---|
| Form B handler `{fetch, queue}` | `src/main.ts` |
| `std:kv` with TTL | per-chat dialog state `fsm/<chat id>`, 1 h TTL (abandoned dialogs expire) |
| `std:sql` | notes, reminders, subscribers, the `update_id` dedupe table; read-first schema creation |
| `std:queue` delayed self-messages | reminders (`delaySeconds` up to 7 days, `dedupId: rem:<id>`) |
| Queue retries, DLQ | Telegram 429 → `retry({delaySeconds: retry_after})`; other errors use the runtime backoff |
| `std:services` `call` | `/rates` asks the `cbu-rates` service in-process |
| Secrets via `Deno.env` | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_WEBHOOK_SECRET`, `ADMIN_TOKEN` |
| Egress `fetch` | `https://api.telegram.org` |

## Conversation

| Command | Does |
|---|---|
| `/note` → text | saves a note (`/note buy milk` saves at once) |
| `/notes`, `/delnote <id>` | last 10 notes; delete one |
| `/remind` → text → time → yes/no | a reminder; `/remind 2h 30m call mom` skips the dialog |
| `/reminders`, `/unremind <id>` | pending reminders; cancel one |
| `/rates` | current CBU rates through `cbu-rates` |
| `/subscribe`, `/unsubscribe` | rate change alerts |
| `/cancel` | ends the current dialog; any other command ends it too |

Times: `10m`, `in 2 hours`, `1h30m`, `18:30`, `tomorrow 9:00`, `2026-12-31 23:00`, `31.12.2026 23:00`. They are read
in a fixed UTC offset, `BOT_TZ_OFFSET_MINUTES` (default `300`, Tashkent).

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and copy its token.
2. Telegram only calls `https://` URLs on ports 443, 80, 88 or 8443. Put the node's public listener behind a TLS proxy,
   or map a custom host to the service (`PUT /v1/services/{id}/hosts`, SECURITY.md).
3. Deploy and configure:

```bash
cd sample/apps/telegram-bot
21ci push src --service telegram-bot --create --wait

printf %s "$BOT_TOKEN"            | 21ci secret set telegram-bot TELEGRAM_BOT_TOKEN
openssl rand -hex 24              | 21ci secret set telegram-bot TELEGRAM_WEBHOOK_SECRET
ADMIN=$(openssl rand -hex 24); printf %s "$ADMIN" | 21ci secret set telegram-bot ADMIN_TOKEN
21ci env set telegram-bot PUBLIC_URL=https://fn.example.com/s/telegram-bot BOT_TZ_OFFSET_MINUTES=300

# register the webhook (getMe + setWebhook with the secret token + setMyCommands)
curl -sS -X POST -H "Authorization: Bearer $ADMIN" https://fn.example.com/s/telegram-bot/setup
curl -sS -H "Authorization: Bearer $ADMIN" https://fn.example.com/s/telegram-bot/status
```

Without `21ci`: `../../scripts/deploy.sh src telegram-bot`, then `PUT /v1/services/{id}/secrets/{NAME}` and
`PUT /v1/services/{id}/env` as in `sample/deno/env-secrets`.

Rate alerts: deploy [`cbu-rates`](../cbu-rates/) in the same tenant and environment and set its `NOTIFY_SERVICE=telegram-bot`.
`/rates` calls the service named by `RATES_SERVICE` (default `cbu-rates`).

### Settings

| Name | Kind | Meaning |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | secret | Bot API token from BotFather |
| `TELEGRAM_WEBHOOK_SECRET` | secret | 16-256 chars `A-Za-z0-9_-`; Telegram sends it back in `X-Telegram-Bot-Api-Secret-Token` |
| `ADMIN_TOKEN` | secret | Bearer token for `/setup`, `/status`, `/admin/outbox` (at least 16 chars) |
| `PUBLIC_URL` | env | Public base URL of this service; the webhook is `PUBLIC_URL/webhook` |
| `BOT_TZ_OFFSET_MINUTES` | env | Users' UTC offset in minutes (default `300`) |
| `RATES_SERVICE` | env | Slug of the rates service (default `cbu-rates`) |
| `TELEGRAM_DRY_RUN` | env | `1` = record Bot API calls in the outbox instead of sending them (tests) |
| `SELF_SLUG` | env | Own slug for self-enqueues, only needed where `ctx.service.slug` is empty |

## HTTP API

| Method and path | Auth | Answer |
|---|---|---|
| `POST /webhook` | `X-Telegram-Bot-Api-Secret-Token` | `200 {ok}`, `200 {duplicate: true}` for a re-delivered `update_id`, `401`, `400 invalid_update`, `500` (Telegram retries) |
| `POST /setup` | admin | `{ok, bot, webhook, dryRun}`; optional body `{"url": "https://…", "dropPendingUpdates": true}` |
| `GET /status` | admin | `getWebhookInfo` plus counts of notes, pending reminders, subscribers |
| `GET /admin/outbox?chat_id=` | admin | last Bot API calls recorded in dry-run mode |
| `GET /` | none | which settings are present (booleans only) |

Errors are `{"error": {"code", "message"}}`.

Queue messages it accepts (from itself, from `cbu-rates`, or `21ci queue send telegram-bot --body …`):

```json
{"type": "reminder", "id": 12}
{"type": "send", "chatId": 123456, "text": "hello"}
{"type": "cbu.rate_changed", "key": "cbu:3f1a…", "changes": [{"ccy": "USD", "date": "2026-10-07", "rate": "11778.45", "diff": "5.5", "nominal": 1}]}
```

## Try it without Telegram

```bash
B=http://127.0.0.1:8080/s/telegram-bot
curl -sS -X POST $B/webhook -H "X-Telegram-Bot-Api-Secret-Token: $SECRET" -H 'Content-Type: application/json' \
  -d '{"update_id": 1001, "message": {"message_id": 1, "date": 0, "chat": {"id": 42, "type": "private"}, "text": "/note buy milk"}}'
curl -sS "$B/admin/outbox?chat_id=42" -H "Authorization: Bearer $ADMIN"     # with TELEGRAM_DRY_RUN=1
```

`./smoke.sh` deploys the bot as `telegram-bot-smoke` in dry-run mode and walks through setup, authentication,
dedupe, both dialogs, subscriptions and the queue fan-out (`SMOKE_SLOW=1` also waits for a reminder to fire).

## Tests

The dialog logic is pure (`src/bot.ts`, `src/telegram.ts`, `src/http.ts`):

```bash
node --test test/*.test.ts        # Node >= 22.18 (type stripping); or: deno test
```

## Notes and limits

- **Idempotency.** An update is claimed with `INSERT OR IGNORE` on `update_id`. If handling fails, the claim is released
  and the webhook answers `500`, so Telegram retries. Notes and reminders are keyed by `(chat, update_id)`, and the
  dialog state is saved only after the reply was sent, so a retry replays the same step.
- **Reminders** are delayed queue messages, so they are limited to 7 days ahead. Delivery is at-least-once: a crash
  between `sendMessage` and the `sent` update can send a reminder twice.
- **Fan-out** sends one queue message per subscriber, so each chat is retried on its own. Chats that blocked the bot are
  unsubscribed. A large list continues in a follow-up message before the wall limit is reached.
- Message text is plain (no `parse_mode`), so user input never needs escaping. Telegram's limit of 4,096 characters is enforced.
- The bot token is part of the Bot API URL: the code never logs URLs or requests.
- `/rates` needs `cbu-rates` in the same tenant and environment (`call.allowed_callers` rules).
