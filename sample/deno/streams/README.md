# streams

Two Form B services that talk through the events layer: `orders` publishes and appends, `billing` consumes and answers.

| Service | Shows |
|---|---|
| `orders/` | `std:stream` `append` (durable, key, `dedupId`), `std:pubsub` `publish` (fan-out) and `request` (reply) |
| `billing/` | `stream(batch, ctx)` consumer with `batch.failAt`, `pubsub(msg, ctx)` subscriber and responder, idempotent writes |

Read [docs/guides/events-and-streams.md](../../../docs/guides/events-and-streams.md) for the model. The short version:
a topic is a durable log you read in order (one partition per key), a subject is an ephemeral message that is
delivered now or not at all.

## Setup

```bash
export RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080 RT_KEY=...   # see sample/README.md

# 1. the topic: two partitions, so records of different customers can go to either
21ci topic create orders --partitions 2

# 2. deploy both services (creates them if needed)
cd sample/deno/streams/billing && ../../../scripts/deploy.sh . billing
cd ../orders && ../../../scripts/deploy.sh . orders

# 3. billing: consume the topic as group `billing`, and subscribe to the fan-out and to the quote subject
21ci service update billing \
  --set 'events.consume=[{"topic":"orders","group":"billing","start":"earliest"}]' \
  --set 'events.subscribe=[{"subject":"orders.*.created","group":"billing"},{"subject":"pricing.quote"}]'

# 4. orders: async invocation of POST /orders (Prefer: respond-async), off by default
21ci service update orders --set async.enabled=true
```

`events.consume` needs the plan to allow the `stream` trigger, `events.subscribe` the `pubsub` trigger. The built-in
`free` plan allows both.

## Run

```bash
S=../../../scripts/invoke.sh

# an order: stored in the topic (key = customer) and fanned out to billing
$S orders /orders POST '{"orderId":"o-1","customer":"ada","region":"eu","amount":120}'

# the same order again: `dedupId` returns the first offset, nothing new is stored
$S orders /orders POST '{"orderId":"o-1","customer":"ada","region":"eu","amount":120}'

# request/reply: billing answers pricing.quote
$S orders '/quote?region=eu&amount=120'

# async: 202 with the inv_ id; the handler runs later, its response body is dropped
21ci invoke orders /orders -X POST -d '{"orderId":"o-2","customer":"bob","region":"us","amount":40}' --async --idempotency-key o-2

# what billing stored and counted
$S billing /
$S billing /invoices

# replay the topic without a consumer group
21ci topic read orders --partition 0 --from earliest --limit 10
21ci consumer list billing
```

## Retry and dead letters

A record with `"flaky": true` fails its first two deliveries. `billing` calls `batch.failAt(record)`, so the records
before it commit, and it is redelivered with `attempt` 1, 2, then succeeds on 3:

```bash
$S orders /orders POST '{"orderId":"o-3","customer":"ada","region":"apac","amount":10,"flaky":true}'
21ci consumer list billing   # per partition: committed cursor, lag, attempt, state, last_error
```

A consumer gives up after `max_attempts` (default 10). Then the record goes to the consumer's dead letters
(`on_exhausted: "dlq"`), and the cursor moves on:

```bash
21ci consumer dlq list billing orders billing
21ci consumer dlq purge billing orders billing
21ci consumer reset billing orders billing --to latest     # unblocks a partition with on_exhausted "block"
```

## Notes

- Delivery to the stream is at least once. `billing` stays correct on redelivery because `invoices` has a primary
  key on (partition, offset) and the insert is `INSERT OR IGNORE`.
- Publishes are at most once. If `billing` is down, its copies are `dropped`, and `orders` still answers `201` (the
  durable record is in the topic). Only the `request` for `pricing.quote` fails when no responder is up
  (`503 no_responders`).
- Module variables do not survive a task: the counter lives in `std:kv`, the rows in `std:sql`.
