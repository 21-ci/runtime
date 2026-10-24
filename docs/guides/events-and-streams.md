---
title: Events, topics and async invocations
description: Choose between queues, pub/sub, streams and async invocations; publish and request on subjects, append to and consume topics with consumer groups, retry and dead-letter records, and accept HTTP calls asynchronously.
---

# Events, topics and async invocations

Four ways to move work between services and into your handlers. They differ in whether a message is kept, who gets it,
and whether anyone waits for an answer. Pick the row that matches what you need, then read its section.

| You need | Use | Kept? | Who gets it | Answer back? |
|---|---|---|---|---|
| Do this work per message, retry it, dead-letter it | [queue](calls-and-queues.md#queues) (`std:queue`) | yes, until acked | one worker of the service | no |
| Tell everyone interested, now; or ask one responder | pub/sub (`std:pubsub`) | no, at most once | every subscriber (or one per queue group); one responder for `request` | `request` only |
| An ordered, replayable history per key (events, changes, audit) | stream (`std:stream`, a topic) | yes, for the retention period | each consumer group, in order per partition | no |
| Accept an HTTP call now and run it later, with retries | async invocation (`Prefer: respond-async`) | yes, until it succeeds or fails | your HTTP handler, once per attempt | `202` to the caller |

Queues are covered in [Service calls and queues](calls-and-queues.md). This guide covers the other three.

Everything here is tenant-scoped: a subject, topic or invocation of one tenant is never visible to another. Within a
tenant, the project and environment you address decide the namespace (see [Projects](projects.md)).

## Pub/sub: subjects

A subject is a `.`-separated name such as `orders.eu.created`. Each token is `[A-Za-z0-9_-]+`; a subject has at most
16 tokens and 256 bytes, and may not start with `_rt`. A message body is at most 1 MiB; headers at most 32 entries and
16 KiB, with no `x-rt-*` names.

### Publish and request

```ts
import { publish, request } from "std:pubsub";

// fire and forget: resolves after routing, not after delivery
const { receivers, dropped } = await publish("orders.eu.created", { id: "o-1" });

// one responder answers; the first reply wins
const quote = await request("pricing.quote", { amount: 120 }, { timeoutMs: 2000 });
```

`publish` returns `{receivers, dropped}`: `receivers` is how many subscribers were handed the message, `dropped` how
many copies were not (fan-out cap, quota, a subscriber with too much in flight, or a refused admission). Nothing is
stored: a subscriber that is down at that moment misses the message. Use a stream when that is not acceptable.

`request` picks one subscriber of the subject (round robin) and waits for its return value. `timeoutMs` defaults to
5000 and is capped at 30000 and at the task deadline. A request with no subscriber fails with `no_responders`.

The control API and the CLI publish the same way, for example from a cron job of another system:
`POST /v1/pubsub/publish` or `21ci pubsub publish orders.eu.created --body '{"id":"o-1"}'`.

### Subscribe

A service receives messages by declaring the subjects in its `events` block and exporting `pubsub(msg, ctx)`:

```bash
21ci service update billing --set 'events.subscribe=[{"subject":"orders.*.created","group":"billing"},{"subject":"pricing.quote"}]'
```

- A subject pattern may use `*` (one token) and `>` (one or more trailing tokens).
- `group` is a queue group: of all the services subscribed to the same subject with the same group, one gets each
  message. Without a group, every subscriber gets its own copy.
- The handler's return value is the reply of a `request`. A plain publish is answered `204` and the value is ignored.

```ts
export default {
  async pubsub(msg: { subject: string; body: unknown; replyable: boolean }, ctx: Ctx) {
    if (msg.subject === "pricing.quote") return { total: 120 * 1.2 };   // the reply
    // a plain message: keep the work small, or hand it to a queue or a stream
  },
};
```

Services that are not allowed the `pubsub` trigger by their plan are left out of the subscriber index. Up to 32
subscriptions per service; a service has at most 64 deliveries in flight to it, and a publisher's copies beyond that
are `dropped` with reason `backlog`. The fan-out of one publish is capped at 100 receivers.

## Topics and streams

A topic is a durable, partitioned log. Records are appended, kept for the retention period (default 24 hours, plan
maximum 72 hours) or until the size cap is reached, and read in order within a partition. Consumers keep their own
position.

### Create and append

```bash
21ci topic create orders --partitions 2 --retention 48h
```

```ts
import { append, appendBatch, read, info } from "std:stream";

// durable when this resolves; the same key always lands in the same partition, in order
const at = await append("orders", { id: "o-1", customer: "ada" }, { key: "ada", dedupId: "o-1" });
// at = { partition, offset, duplicate }

// up to 100 records (4 MiB) in one transaction: all or nothing
await appendBatch("orders", [{ body: { id: "o-2" }, key: "bob" }, { body: { id: "o-3" }, key: "bob" }]);
```

- `key` (at most 256 bytes) chooses the partition by hash. Without a key, records are spread over the partitions.
- `dedupId` (at most 128 bytes) makes a retried append safe: the same id on the same topic within 2 minutes returns the
  first record's position with `duplicate: true`, and nothing new is written.
- Each append call takes one tenant rate token; a topic that is full (its size cap or the tenant's stream storage)
  refuses the append with `stream_full`.

The control API takes the same records as `POST /v1/topics/{t}/messages` with `{records: [{body, key?, headers?,
dedup_id?}]}`, and `21ci topic append orders --body '{...}' --key ada`.

### Read without a consumer group

Replay does not move any cursor:

```ts
const page = await read("orders", { partition: 0, from: "earliest", limit: 50 });
// page = { records: [{ partition, offset, key, body, headers, timestamp }], next, end }
```

`from` is `"earliest"`, `"latest"` or an offset; `fromTime` (epoch ms) starts at the first record at or after a time
instead. `limit` is 1 to 100. When the topic has more than one partition, `partition` is **required**: a read
without it is `validation_failed` with the partition count in the message. `info(topic)` returns the first and next
offset of every partition.

### Consume with a consumer group

A consumer is a `(topic, group)` pair declared in the service's `events` block, and a handler `stream(batch, ctx)`:

```bash
21ci service update billing --set 'events.consume=[{"topic":"orders","group":"billing","start":"earliest","batch_size":100,"max_attempts":10,"on_exhausted":"dlq"}]'
```

| Field | Default | Meaning |
|---|---|---|
| `topic` | (required) | a topic name; `tpc_…` ids are not accepted here |
| `group` | `default` | the group name; cursors are per `(topic, group)` |
| `start` | `latest` | where a new cursor starts: `earliest` or `latest` |
| `batch_size` | 100 | records per call, 1 to 1,000 (and at most 1 MiB of bodies) |
| `max_attempts` | 10 | deliveries of one record before `on_exhausted` applies, 1 to 100 |
| `on_exhausted` | `dlq` | `dlq` (copy the record to the group's dead letters and go on), `skip` (go on), or `block` (stop this partition until reset) |

A service declares at most 8 consumers, and each `(topic, group)` once.

```ts
export default {
  async stream(batch: { partition: number; messages: { offset: number; body: unknown }[]; attempt: number;
                        failAt(m: { offset: number } | number): void }, ctx: Ctx) {
    for (const m of batch.messages) {
      if (isBroken(m.body) && batch.attempt < 3) {
        batch.failAt(m);   // commit everything before m; redeliver from m
        return;
      }
      await handle(m.body); // must be idempotent: a batch can come again
    }
    // a normal return commits the whole batch
  },
};
```

- A normal return commits the batch through its last record.
- `failAt(m)` commits the records before `m` and redelivers from `m`. The lowest call wins. `m` must be a record of
  this batch.
- A throw commits nothing and retries the batch.
- Retries back off; `batch.attempt` counts deliveries of the record at the cursor, starting at 1.
- Delivery is at least once: a crash after your side effect and before the commit repeats the batch. Make the side
  effect idempotent (a unique key, a `dedupId`, an upsert).

Consumers are read per partition and in order. A slow record holds back the rest of its partition, not other
partitions; that is why `key` matters: keep one entity on one key.

### Consumer state and dead letters

```bash
21ci consumer list billing                                        # per partition: committed, next, lag, attempt, state, last_error
21ci consumer reset billing orders billing --to latest            # move the cursor (also unblocks a blocked partition)
21ci consumer reset billing orders billing --to 2026-10-01T00:00:00Z --partition 1
21ci consumer dlq list billing orders billing --limit 20          # records that exhausted max_attempts
21ci consumer dlq purge billing orders billing
```

A dead letter keeps the record and its last error. The group's dead letters are capped at 1,000 (the oldest go
first) and kept 7 days. Dead letters count against the tenant's stream storage. `on_exhausted: "block"` keeps the
partition stopped until you reset it: use it when records must not be skipped.

### Topic limits

Topics are per tenant and per plan: `max_topics` (10 on the built-in `free` plan), `max_topic_partitions` (4; 16 is the
hard limit), `max_stream_retention_hours` (72) and `max_stream_mb` (100) across all topics of the tenant. A topic's own
`max_mb` cap trims its oldest records first. Change retention or the cap later with `21ci topic update` or
`PATCH /v1/topics/{t}`. A topic cannot be renamed, and its name may not start with `tpc_`.

## Async invocations

Any HTTP route of a service can be accepted now and run later. Async is off by default. Turn it on per service:

```bash
21ci service update orders --set async.enabled=true
21ci service update orders --set async.max_attempts=5 --set async.max_event_age_s=1800
```

Then a caller asks for it with `Prefer: respond-async` (RFC 7240; other preferences are ignored) or with
`X-Rt-Invocation-Type: Event`:

```bash
21ci invoke orders /orders -X POST -d '{"orderId":"o-2"}' --async --idempotency-key o-2
# 202 {"id":"inv_…","state":"pending"}
```

- The request (method, path, query, headers, body) is stored sealed with the node's key. The handler then runs once per
  attempt with the same request, plus `x-rt-invocation-id` and `x-rt-invocation-attempt` (1-based); in Deno,
  `ctx.invocation` carries `{id, attempt}`. Its response body is discarded: only the status counts.
- A `5xx` or `429` is a failed attempt and is retried after a delay until `async.max_attempts` (default 3, at most 10)
  is used up. Any other status is a success.
- An invocation still pending `async.max_event_age_s` after it was accepted (default 3600) fails with
  `event_age_exceeded`. A stored request that can no longer be opened fails at once with condition `unreadable`.
- `Idempotency-Key` (up to 255 visible characters) makes a retried accept safe: the same key with the same method, URI
  and body returns the original `202`; the same key with anything else is `409 idempotency_mismatch`. The header also
  reaches the handler unchanged.
- With `Prefer` the request runs synchronously when its body is larger than 1 MiB (or `limits.max_body_bytes`); with
  `X-Rt-Invocation-Type: Event` it is `413 too_large` instead.
- A service may hold `max_async_pending` pending invocations (10,000 on `free`); above that the accept is
  `429 async_backlog_full` with `Retry-After: 1`. A durable service or a WebSocket upgrade refuses `Event` with
  `403 async_not_allowed`. Accepted invocations keep running after their key is revoked.

Inspect and clean up with `21ci invocation list <svc> --state failed`, `21ci invocation get <svc> <inv_id>`,
`21ci invocation redrive <svc>` (failed back to pending, attempts reset) and `21ci invocation purge <svc>`. Views
never show the headers or the body. Succeeded records are kept 24 hours; failed ones 7 days, at most 1,000 per
service.

## Which one do I use

- **Work that must be done, once per item, with retries.** A queue, from [Service calls and queues](calls-and-queues.md).
- **Something happened, and any interested service should react, now.** Publish a subject. If it must not be lost,
  also append it to a topic: subjects are the wake-up, the topic is the record.
- **A question with one answer from another service.** `request` on a subject, with a `pubsub` responder.
- **History that more than one consumer reads at its own pace, or that you replay later.** A topic with a consumer
  group per reader.
- **An HTTP endpoint that should not make the caller wait.** Async invocation, with an `Idempotency-Key` from the
  caller.

## Errors

| Code | Where | Meaning |
|---|---|---|
| `validation_failed` | guest, API | bad subject, topic name, key, dedup id, partition (missing on a topic with several) |
| `too_large` / `413 too_large` | guest, API, invoke | body, headers or batch over the limit |
| `not_found` | guest | the topic does not exist |
| `stream_full` | guest, API | the topic's or the tenant's stream storage is full |
| `rate_limited` / `quota_exceeded` | guest, API | the tenant's rate or quota refused the call |
| `no_responders` / `timeout` / `responder_failed` | guest, `request` | nobody answered, or the answer was late or failed |
| `topic_exists` | API | `409` on create |
| `async_not_allowed` | invoke | `403`, `Event` on a service with async off |
| `idempotency_mismatch` | invoke | `409`, the same key for a different request |
| `async_backlog_full` | invoke | `429`, the pending limit is reached |
| `limit_exceeds_plan` | API | a partition count, retention, cap or `min_run_time_ms` above the plan |
| `trigger_not_allowed` | API | the plan does not allow `pubsub` or `stream` for an `events` block |

Guest errors are `Error`s with a stable `.code`; branch on the code, not the message.

## WASM services

The same features are open to WASM components through the `pubsub` and `streams` interfaces of the `std:host` WIT
package ([wit/rt-host.wit](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit)). A subscriber and a consumer
of a WASM service are delivered as `POST /__rt/pubsub` and `POST /__rt/stream` with `x-rt-trigger` set; the body is the
same JSON as in Deno (`{id, subject, body, headers, replyable, publishedAt}` and `{topic, group, partition, attempt,
messages}`). A consumer's `2xx` commits the batch; a `2xx` with `x-rt-stream-fail-at: <offset>` commits the records
before that offset. Async invocations work for WASM services the same way.

## Metrics

The counters are listed in [Metrics](../reference/metrics.md#triggers-and-queues) (`rt_pubsub_*`, `rt_stream_*`,
`rt_async_*`) and the admission wait by predicted run time in [Keep-alive and fair admission](../reference/metrics.md#keep-alive-and-fair-admission).
Async and consumer state also show in `21ci invocation list` (`counts`) and `21ci consumer list` (lag and state).

## Next steps

- [Service calls and queues](calls-and-queues.md): calls, queues and access rules.
- [Deno services](deno.md) and [WASM services](wasm.md): the handler forms.
- [Limits](../reference/limits.md): every bound of this guide in one table.
