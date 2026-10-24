# scheduled-queue

Form B handlers: `export default { fetch, scheduled, queue }`.

## Shows

- `scheduled(event, ctx)`: `event = { scheduleId, scheduledTime, payload }`, called for interval and cron schedules
- `queue(batch, ctx)`: `batch.messages[i] = { id, body, attempt, ack(), retry({ delaySeconds }) }` (batches of up to 10)
- `import { enqueue } from "std:queue"`: `enqueue(service, body, { delaySeconds?, dedupId? })` (same tenant; here the service enqueues to itself)
- Retry with a delay, automatic backoff and the dead-letter queue; `dedupId` collapses duplicates within 24 h
- State in `std:sql` (every task is a fresh isolate unless `deno.reuse_isolates` is on, and even then module state is only a cache)

## Deploy

```bash
cd sample/deno/scheduled-queue
../../scripts/deploy.sh . scheduled-queue
```

## Schedules

```bash
SVC=$(../../scripts/svc-id.sh scheduled-queue)
AUTH="Authorization: Bearer $RT_KEY"

# every 10 s (plan minimum is 10 s by default)
curl -sS -X POST "$RT_API/v1/services/$SVC/schedules" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"kind": "interval", "every_seconds": 10, "payload": {"step": "maintenance"}}'

# cron (5 fields, IANA timezone, default UTC)
curl -sS -X POST "$RT_API/v1/services/$SVC/schedules" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"kind": "cron", "cron": "0 12 * * *", "timezone": "UTC", "payload": {"job": "daily-report"}}'
```

Schedules never overlap and missed ticks are skipped, not backfilled. Payload is JSON <= 64 KB.

## Queue

```bash
S=../../scripts/invoke.sh

# through the guest SDK (std:queue)
$S scheduled-queue /enqueue POST '{"text": "hello"}'

# a job that fails twice, then succeeds (attempts 1, 2 retry; attempt 3 acks)
$S scheduled-queue /enqueue POST '{"text": "flaky", "failUntilAttempt": 3}'

# delayed, de-duplicated
$S scheduled-queue /enqueue POST '{"text": "later", "delaySeconds": 5, "dedupId": "job-1"}'

# straight through the control API
curl -sS -X POST "$RT_API/v1/services/$SVC/queue" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"body": {"text": "from the API"}, "delay_seconds": 0}'

# what the handlers recorded
$S scheduled-queue /
```

A job with `"failUntilAttempt": 99` never succeeds: after `queue.max_attempts` (5) the message moves to the dead-letter queue.

```bash
curl -sS "$RT_API/v1/services/$SVC/dlq" -H "$AUTH"
curl -sS -X POST "$RT_API/v1/services/$SVC/dlq/redrive" -H "$AUTH"   # back to the queue with attempt = 1
```

Logs (`kind=guest`, tagged with the task id):

```bash
curl -N "$RT_API/v1/services/$SVC/logs?follow=1" -H "$AUTH"
```

## Expected responses

```json
// POST /enqueue (202)
{ "enqueued": { "text": "flaky", "failUntilAttempt": 3 } }

// GET /
{ "events": [
  { "id": 4, "kind": "queue-done",  "detail": "{\"id\":\"msg_01J…\",\"text\":\"flaky\"}", "attempt": 3, "created_at": 1790882271 },
  { "id": 3, "kind": "queue-retry", "detail": "{\"id\":\"msg_01J…\",\"text\":\"flaky\"}", "attempt": 2, "created_at": 1790882269 },
  { "id": 2, "kind": "queue-retry", "detail": "{\"id\":\"msg_01J…\",\"text\":\"flaky\"}", "attempt": 1, "created_at": 1790882268 },
  { "id": 1, "kind": "scheduled",   "detail": "{\"scheduleId\":\"sch_01J…\",\"scheduledTime\":…,\"payload\":{\"step\":\"maintenance\"}}", "attempt": null, "created_at": 1790882260 }
] }
```

## Notes

- Delivery is at-least-once and unordered: make handlers idempotent. A message you neither ack nor retry counts as acked when the handler returns normally, so the example catches errors per message and calls `retry()` itself.
- Without arguments `msg.retry()` uses the runtime backoff: 2 s x 2^attempt, +-20 %.
- A Form A service (a bare function) receives the same triggers as synthetic requests: `POST /__rt/scheduled` and `POST /__rt/queue`.
