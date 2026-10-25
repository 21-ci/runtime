---
title: Limits and quotas
description: Every limit and quota of the runtime, what happens when it is exceeded, and which key changes it.
---

# Limits and quotas

This page lists every limit the runtime enforces: per-task limits, tenant plan quotas, API and upload sizes,
storage, queue, log and ingress limits. For each one it says what the caller sees when the limit is hit and whether
an operator can change it.

The **Change with** column uses these forms:

| Form | Where it is set |
|---|---|
| `plans.<name>.key` | The tenant's plan in `rt.toml` ([`[plans.<name>]`](../api.md#plans-built-in-free-more-under-plansname)), or a per-tenant override (`overrides` on `POST`/`PATCH /v1/tenants/{t}`, a partial plan object). Reloaded on `SIGHUP` |
| `limits.key`, `scaling.key` | The service's own config block (`POST`/`PATCH /v1/services/{s}`), within the plan maximum |
| `section.key` | Node configuration ([configuration reference](../config.md)) |
| fixed | Built into this release |

Error codes are explained in the [error reference](./errors.md).

## Per-task limits

Every task (one request, schedule fire, queue batch or service-to-service call) runs under these limits. Defaults
depend on the trigger:

| Limit | HTTP | call | interval, cron | queue | Change with |
|---|---|---|---|---|---|
| Wall time | 30 s | min(own, caller's remaining) | 300 s | 300 s | `limits.wall_ms`, max `plans.<name>.limits_max.wall_ms` |
| CPU time | 1,000 ms | 1,000 ms, own budget | 10,000 ms | 10,000 ms | `limits.cpu_ms`, max `plans.<name>.limits_max.cpu_ms` |
| Memory (V8 heap and buffers / WASM linear memory) | 128 MB | 128 MB | 128 MB | 128 MB | `limits.memory_mb`, max `plans.<name>.limits_max.memory_mb` |
| Request body | 10 MB | 10 MB | n/a | 1 MB per message | `limits.max_body_bytes` (queue messages: fixed) |
| Response body, streamed to the client | 100 MB | 100 MB | n/a | n/a | fixed |
| Body buffered across the WASM host boundary (`call` responses, `blob.get`, `platform.fetch`) | 10 MB | 10 MB | 10 MB | 10 MB | fixed |
| Outbound `fetch` calls per task | 50 | 50 | 200 | 200 | fixed |
| Concurrent outbound fetches per task | 6 | 6 | 6 | 6 | fixed (more wait) |
| Concurrent service calls per task | 16 | 16 | 16 | 16 | fixed (more wait within the deadline) |
| Log lines per task | 1,000 | 1,000 | 1,000 | 1,000 | fixed |
| Call depth | | 15 | | | fixed |

!!! warning "Plan maxima cap the longer defaults"
    A limit the service does not set explicitly takes the trigger's default, capped by the plan maximum. The
    built-in `free` plan allows at most 30 s wall and 1,000 ms CPU, so on `free` scheduled and queue tasks also get
    30 s and 1 s. Raise `limits_max` in the plan for longer background jobs.

What happens when a task exceeds a limit:

| Exceeded | HTTP caller sees | Inside the guest |
|---|---|---|
| CPU time | `502`, `x-rt-reason: cpu_limit` | The task is stopped |
| Memory | `502`, `x-rt-reason: oom` | Growing a resizable `ArrayBuffer` past the limit throws `RangeError`; anything else stops the task |
| Wall time | `504`, `x-rt-reason: timeout` | The task is stopped |
| Request body (`Content-Length` over the limit) | `413`, `x-rt-reason: too_large` | Reading a streamed body past the limit fails with `request_too_large` |
| Response body | The stream ends with an error | |
| Buffered WASM body | | `call` and `platform.fetch` return `response_too_large`; `blob.get` returns `blob_too_large` |
| Fetch count | | `fetch` rejects with `egress_limit` |
| Log lines | | Further lines are dropped and counted |
| Call depth | `508`, `x-rt-reason: call_depth` | |

Service writes above the plan maximum are refused: `422 limit_exceeds_plan` with `details.field`, `requested` and
`max`. If a plan is later lowered below a service's explicit value, the plan wins from the next task on, and the
service's `effective` view shows that value with source `plan`. No event is recorded for it.

## Plan quotas

Plans live in `rt.toml` under `[plans.<name>]`. The `free` plan always exists; its values are the defaults of every
key, so a plan you define inherits them for keys it leaves out.

| Key | `free` | Meaning | When exceeded |
|---|---|---|---|
| `max_services` | 10 | Services per tenant | `403 quota_exceeded` on create |
| `max_concurrency` | 10 | Tasks in flight across the tenant's services. 0 = no cap | Requests wait in the queue; after `queue_timeout`, `503 queue_timeout` |
| `max_instances` | 4 | Instances of one service: caps `scaling.min_instances` and `scaling.max_instances`. 0 = the node default | Larger service values are clamped (source `plan` in `effective`) |
| `rps` | 50 | Token bucket for HTTP invocations, per tenant (refills `rps` per second, holds `rps` tokens). 0 = unlimited | `429 rate_limited` with `Retry-After` |
| `invocations_per_day` | 100,000 | HTTP invocations per UTC day; survives restarts. 0 = unlimited | `429 quota_exceeded` with `Retry-After` until UTC midnight |
| `storage_mb` | 100 | KV + SQL + blobs across all the tenant's services | Writes fail with `storage_quota`; reads keep working |
| `egress_mb_per_day` | 1,000 | Bytes received by guest `fetch` per UTC day | `fetch` fails with `egress_quota` |
| `max_schedules` | 3 | Schedules per service | `403 quota_exceeded` |
| `min_schedule_seconds` | 60 | Shortest schedule period (an interval, or the gap between a cron's next two fires) | `422 schedule_too_frequent` |
| `max_queue_depth` | 10,000 | Ready messages in a service's queue | `429 queue_full` (API), `queue_full` (guest `enqueue`) |
| `max_async_pending` | 10,000 | Pending async invocations per service (`async.enabled` on); a redrive moves back at most up to this cap | `429 async_backlog_full` with `Retry-After: 1` |
| `storage_mb` (async backlog) | 100 | Stored (sealed) headers + body bytes of the tenant's pending and failed async invocations, across all its services. A separate budget of the same size as the storage quota, not counted in it | `429 async_backlog_full` with `Retry-After: 1` |
| `max_topics` | 10 | Topics per tenant | `403 quota_exceeded` |
| `max_topic_partitions` | 4 | Partitions of one topic (the node hard maximum is 16) | `422 limit_exceeds_plan` |
| `max_stream_mb` | 100 | Record bytes across the tenant's topics, dead letters included. Not part of `storage_mb` | `429 stream_full` on append |
| `max_stream_retention_hours` | 72 | Longest retention a topic may set (the default is 24 h) | `422 limit_exceeds_plan` |
| `builds_per_hour` | 20 | Version uploads per tenant in any 60-minute window | `429 build_quota` |
| `allowed_triggers` | `http`, `interval`, `queue`, `call`, `websocket`, `pubsub`, `stream` | Trigger kinds the tenant may use (`cron` is off on `free`). `pubsub` and `stream` gate subscriptions and topic consumers | `403 trigger_not_allowed`; queue messages wait unconsumed |
| `limits_max.wall_ms` | 30,000 | Highest wall limit a service may set | `422 limit_exceeds_plan` |
| `limits_max.cpu_ms` | 1,000 | Highest CPU limit | `422 limit_exceeds_plan` |
| `limits_max.memory_mb` | 128 | Highest memory limit. Also sizes the node's WASM pool slots | `422 limit_exceeds_plan` |
| `limits_max.min_run_time_ms` | 60,000 | Highest `scaling.min_run_time_ms` (keep-alive after an answer) a service may set. The node's `sched.min_run_time_max_ms` caps it too; the lower value wins | `422 limit_exceeds_plan`; a lowered plan value is clamped at run and recorded as a plan conflict |
| `custom_host_suffixes` | none | Suffixes under which tenants may claim custom hosts (none = operator only) | `403 host_not_allowed` |
| `blocking_share` | 0.25 | Share of storage threads one tenant may hold under contention | Its storage calls wait their turn |
| `egress_default` | `allow` | Outbound stance for the tenant's services: `allow` (public internet, SSRF guard on) or `deny` (operator allowlists only) | `fetch` fails with `egress_denied` |
| `allow_cross_tenant_calls` | false | Whether services may call other tenants' services (the target must also list the caller's id) | `cross_tenant_call` |
| `max_pinned_mb` | 0 | Memory a tenant may pin resident with `scaling.keep_resident`. Accepted, but pinning is not wired up in this release <span class="rt-badge rt-badge--planned">planned</span> | n/a |

`rps` and `invocations_per_day` count HTTP invocations (public and internal listener). Busy capacity is never a
`429`: over-capacity requests queue (see [Scheduler](../concepts/scheduler.md)).

## Node capacity

Node-wide bounds on running and waiting work ([`[sched]`](../config.md#scheduler), [`[memory]`](../config.md#memory)):

| Limit | Default | When exceeded | Change with |
|---|---|---|---|
| Run slots | 10,000 | Tasks wait in their service queue | `sched.max_inflight` (live) |
| Waiting tasks | 4 × run slots; one service or tenant may hold half | `503 overloaded` + `Retry-After` | `sched.max_queued` (live), per service `scaling.max_queued` |
| Bytes held by waiting tasks | 1/16 of the memory budget (4 KiB + body per task) | `503 overloaded` | `sched.max_queued_mb` (live) |
| Wait for a slot, code or memory | 5 s (`balanced`; 1 s `min_latency`, 10 s `max_density`) | `503 queue_timeout` + `Retry-After` | `sched.queue_timeout` (live) |
| Per-service concurrency ceiling | 1,024 (`max_density`: 256), adaptive below it | Requests wait | `scaling.max_instances`, capped by the plan's `max_instances` |
| Memory budget | 85 % of the container memory limit | High: caches shrink; Critical: `503 shed` for versions not in memory | `memory.budget_mb`, `memory.high`, `memory.critical` (live) |
| Draining | | `503 draining` | `POST /v1/node/drain` (one-way) |

## Control API

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| JSON request body | 1 MB | `413 too_large` | fixed |
| Version upload, HTTP layer | 64 MB | `413 too_large` | fixed (format limits below) |
| Version uploads buffered at once, per node | 4 | `429 rate_limited` (also when the memory governor refuses the upload) | fixed |
| Page size (`limit`) on tenants, projects, services, events | 50 by default, 200 max | Clamped | fixed |
| Logs page size | 100 by default, 1,000 max | Clamped | fixed |
| Failed authentications per client | 10 per minute (control listener and `access = key` invokes counted separately) | `429 rate_limited` + `Retry-After` | fixed |
| Log and event streams (SSE) per credential | 10 | `429 rate_limited` | `ingress.sse_streams_per_key` |
| Control API `Idempotency-Key` | At most 255 bytes; replayed for 24 h, per principal, in memory | Longer: `422 validation_failed`. A different request with the same key: `422 idempotency_mismatch` | fixed |
| Slug | `^[a-z][a-z0-9-]{1,62}$` | `422 validation_failed` | fixed |
| Environment label | `^[a-z][a-z0-9-]{0,31}$` | `422 validation_failed` | fixed |
| `call.allowed_callers` entries | 64 | `422 validation_failed` | fixed |
| Projects per tenant | 100 | `403 quota_exceeded` | fixed |
| Environments per project | 1 to 20 | `422 validation_failed` | fixed |
| Live keys created through `/v1/keys` per tenant | 200 | `403 quota_exceeded` | fixed |
| MCP request body | 4 MiB; one tool result 100 KiB; `deploy_code` source 3 MiB | `413` / `result_too_large` tool error | fixed |
| Pending custom-host claims per tenant | 10, expire after 7 days | `403 quota_exceeded` | `hosts.max_pending_per_tenant`, `hosts.pending_ttl` |
| Retained versions per service | 10 | Oldest non-live versions are removed | `deploy.retain_versions` |

## Deploy and build

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| Single Deno file (`application/typescript`, `text/javascript`) | 1 MB | `413 too_large` | fixed |
| Deno archive or JSON bundle (`zip`, `tar`, `application/json`) | 10 MB total, 1,000 files, 1 MB per file | `413 too_large` | fixed |
| Archive inflate ratio | 100 (checked above 64 KiB) | `413 too_large` | fixed |
| Archive paths | No `..`, absolute paths or links | `422 validation_failed` | fixed |
| WASM component (`application/wasm`) | 50 MB | `413 too_large` | fixed |
| Builds per tenant | 20 per hour | `429 build_quota` | `plans.<name>.builds_per_hour` |
| Deno build time | 60 s | Version `failed`, reason `timeout` | `build.timeout` |
| WASM build time | 120 s | Version `failed`, reason `timeout` | `build.wasm_timeout` |
| One dependency fetch | 30 s | Version `failed`, reason `fetch` | `build.fetch_timeout` |
| Remote dependencies of one build | 50 MB total, 10 MB per module | Version `failed`, reason `too_large` | `build.max_deps_mb` (total) |
| Build memory | 256 MB | Version `failed`, reason `oom` | `build.memory_mb` |
| Dependency hosts | esm.sh, jsr.io, deno.land, cdn.jsdelivr.net, unpkg.com | the build fails with reason `resolve` | `build.allowed_hosts`, `build.npm_cdn` |

Requests for a failed version get `502 build_failed` until you upload a fix or rebuild.

## Storage

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| KV key | 2 KB, non-empty | `kv_key_too_large` | fixed |
| KV value | 1 MB | `kv_value_too_large` | fixed |
| KV `list` page | 100 by default, 1,000 max | Clamped | fixed |
| KV counter (`kv.incr`) | The stored value must be a JSON integer; the result must fit a signed 64-bit integer. Each call is charged against the storage quota as a 20-byte value | `kv_not_integer`, `kv_overflow` (the value is left unchanged) | fixed |
| SQL statement time | 1,000 ms | `sql_timeout` | `storage.sql_budget_ms` |
| SQL result per call | 10,000 rows or 10 MiB of values | `sql_result_too_large` | fixed |
| SQL statement text | 1 MiB | `sql_error` | fixed |
| SQL value or row | 16 MiB | `sql_error` | fixed |
| SQL shape | Expression depth 500, 100 compound terms, 1,000 columns, `?10000`, `LIKE` pattern 1,000 bytes | `sql_error` | fixed |
| SQL objects | `ATTACH`, extensions, `_rt_*` tables, schema-writing pragmas, temporary tables, indexes, views and triggers are denied | `sql_not_authorized` | fixed |
| Interactive SQL transaction | 5 s, then rolled back | Error in the transaction | fixed |
| `std:sqlite` statement time | 100 ms | `sql_timeout` | fixed |
| `std:sqlite` wait for the write lock | 20 ms | `sql_busy` | fixed |
| Blob key | 1 KB | `blob_key_too_large` | fixed |
| Blob object | 100 MB, and what is left of `storage_mb` | `storage_quota` | `plans.<name>.storage_mb` |
| Total storage per tenant | 100 MB (`free`) | `storage_quota` on writes | `plans.<name>.storage_mb` |

A service database sees new tenant totals every 30 s, so a tenant writing from many services at once can overshoot
`storage_mb` briefly.

## Queues and schedules

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| Message body | 1 MB | `413 too_large` (API); the guest's `enqueue` fails | fixed |
| Delivery delay | 7 days | `422 validation_failed` | fixed |
| `dedup_id` window | 24 h | A duplicate is delivered once | fixed |
| Queue depth | 10,000 per service (`free`) | `429 queue_full` / `queue_full` | `plans.<name>.max_queue_depth` |
| Messages per handler batch | 10 | Delivered in more batches | `queue.batch_size` |
| Delivery attempts | 5, backoff 2 s × 2^(attempt−1) ±20 % | Message moves to the dead-letter queue | `queue.max_attempts` |
| Concurrent queue tasks per service | 4 | Batches wait | `queue.max_concurrency` |
| DLQ listing | 100 messages | Truncated | fixed |
| Schedule payload | 64 KB of JSON | `422 validation_failed` | fixed |
| Schedules per service | 3 (`free`) | `403 quota_exceeded` | `plans.<name>.max_schedules` |
| Shortest schedule period | 60 s (`free`) | `422 schedule_too_frequent` | `plans.<name>.min_schedule_seconds` |

A schedule fire is skipped (and counted) while the previous run of the same schedule is still going.

## Events: topics, pub/sub and async invocations

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| Event body (pub/sub, stream record, async request) | 1 MiB; async also at most the service's max body | `413 too_large` (async and the control API); the guest's call fails | fixed |
| Headers per message | 32 entries, 16 KiB in all, 4 KiB per value; no CR, LF or NUL; `x-rt-` names reserved | `validation_failed` | fixed |
| Subject | 256 bytes, 16 tokens | `validation_failed` | fixed |
| Subscriptions per service / consumers per service | 32 / 8 | `validation_failed` | fixed |
| Deliveries per publish | 100; the rest are counted as `dropped` | Publish returns `{receivers, dropped}` | fixed |
| In-flight pub/sub deliveries per subscriber service | 64 | Excess deliveries are `dropped` (reason `backlog`) | fixed |
| Pub/sub request timeout | 5 s default, 30 s max, never past the caller's deadline | The request fails with `timeout` | fixed |
| Topic and group names | 63 characters. A topic name may not start with `tpc_` | `validation_failed` | fixed |
| Record key / dedup id | 256 bytes / 128 bytes | `validation_failed` | fixed |
| Append batch | 100 records, 4 MiB | `validation_failed` | fixed |
| Read page | 100 records, 1 MiB | `validation_failed` | fixed |
| Delivery batch (stream consumers) | 100 default, 1,000 max, 1 MiB | Batches are split | fixed |
| Stream dedup window | 2 minutes | A duplicate after the window is stored again | fixed |
| Partitions per topic | 1 to 16 | `validation_failed` / `limit_exceeds_plan` | `plans.<name>.max_topic_partitions` |
| Read on a multi-partition topic | `partition` is required | `validation_failed` | fixed |
| Dead letters per consumer | 1,000, kept 7 days | The oldest are dropped | fixed |
| Stream appends | One tenant rate token per call | `429 rate_limited` | `plans.<name>.rps` |
| Quota cost (`rps` and `invocations_per_day`) | A request: one token. An async invocation: one at accept and one per delivery attempt (retries and redrives included). A pub/sub publish: one per delivery, at least one (a publish with no receiver still costs one); a pub/sub request: one | `429 rate_limited` / `quota_exceeded`; a refused delivery is dropped (pub/sub) or retried later (async) | `plans.<name>.rps`, `plans.<name>.invocations_per_day` |
| Async attempts (function errors and `429`, in total) | 1 to 10, default 3 | Invocation is `failed` (`retries_exhausted`) | `async.max_attempts` |
| Async event age | 60 s to 86,400 s, default 3,600 | `event_age_exceeded` | `async.max_event_age_s` |
| Failed async records | 1,000 per service, kept 7 days | The oldest are evicted | fixed |
| Async attempts in flight | 256 per node; `queue.max_concurrency` (4) per service | Attempts wait | `queue.max_concurrency` |
| Async invoke `Idempotency-Key` | 1 to 255 visible ASCII characters; replayed while the record exists | Invalid: `400 bad_request`. A different request with the same key: `409 idempotency_mismatch` | fixed |

## Env, secrets and logs

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| Env vars per service | 100 | `422 validation_failed` | fixed |
| Secrets per service | 100 | `422 validation_failed` | fixed |
| Env or secret value | 32 KB | `422 validation_failed` | fixed |
| Names | `[A-Z_][A-Z0-9_]*`, no `RT_` prefix | `422 validation_failed` | fixed |
| Log message | 10,000 characters | Truncated, `truncated: true` | fixed |
| Log rate per service | 100 lines/s, bursts of 1,000 | Dropped; one `"N lines dropped"` record summarizes them | fixed |
| Log retention | 7 days, 50 MB per service | Oldest segments deleted | `logs.retention_days`, `logs.max_mb_per_service` |
| In-memory tail per service | 1,000 lines | Older lines come from disk | `logs.ring_lines` |

## Ingress

Connection-level limits of every TCP listener ([`[ingress]`](../config.md#ingress), restart to change):

| Limit | Default | When exceeded | Change with |
|---|---|---|---|
| Request head size | 16 KB | `431` | fixed |
| Headers per request | 100 | `431` | fixed |
| Time to send the request head | 10 s | Connection closed | `ingress.header_timeout` |
| HTTP/2 keep-alive interval and timeout | 60 s | Connection closed | `ingress.idle_timeout` |
| Connections per client IP | 256 (trusted proxies exempt) | Closed right after accept, counted in `rt_ingress_conn_rejected_total` | `ingress.max_conns_per_ip` |
| Concurrent HTTP/2 streams per connection | 100, plus a guard against rapid stream resets | Refused; abusive connections are closed | `ingress.h2_max_streams` |
| Accept backlog | 4,096 (capped by the kernel's `somaxconn`) | Connection resets | `ingress.backlog` |
| Per-service ingress rules (public listener) | `allowed_methods`, `allow_paths`, `ip_allow`, `rate_limit {rps, burst}` | `405`, `403 forbidden`, `429 rate_limited` | the service's `ingress` block |

## Egress

Every guest `fetch` passes the SSRF guard ([`[egress]`](../config.md#security)):

| Limit | Value | When exceeded | Change with |
|---|---|---|---|
| Destinations | Public addresses only: loopback, private, link-local, CGNAT and metadata addresses denied, plus the node's own addresses | `egress_denied` | `egress.denied_*` adds more; per service `egress.allowed_hosts` / `denied_hosts` |
| Ports | 25, 465, 587 denied | `egress_denied` | `egress.denied_ports` |
| Response size | 100 MB after decompression; inflate ratio over 100 aborts | `egress_too_large` | fixed |
| Redirects | 5, each hop re-validated; `https` to `http` refused | `egress_insecure_redirect` | `egress.allow_https_downgrade` |
| Connect timeout | 10 s; the whole request is bounded by the task's wall time | `egress_timeout` | fixed |
| Bytes per tenant per day | 1,000 MB (`free`) | `egress_quota` | `plans.<name>.egress_mb_per_day` |

## See also

- [Tenants and plans](../concepts/tenants-and-plans.md): how plans, overrides and services fit together.
- [Production deployment](../guides/production.md): defining plans and reloading them with `SIGHUP`.
- [Error reference](./errors.md#invoke-reasons-x-rt-reason): every `x-rt-reason` and control API code.
