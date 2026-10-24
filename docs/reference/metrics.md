---
title: Metrics
description: Every Prometheus metric the node exports, with its type and labels, plus the health endpoints and example queries.
---

# Metrics <span class="rt-badge rt-badge--operator">operator</span>

The node exports Prometheus metrics, a liveness and a readiness endpoint on its metrics listener. This page lists
every metric the code registers, with its type, labels and meaning, and ends with queries worth putting on a
dashboard. For the bigger picture (logs, `server-timing`, per-service stats), see
[Observability](../concepts/observability.md).

## Endpoints

All three are on the metrics listener (`listeners.metrics`, default `127.0.0.1:9090`; the Docker image binds
`0.0.0.0:9090` and Compose publishes it on the host's loopback). They need no credentials, so keep the listener
private.

| Endpoint | Answers |
|---|---|
| `GET /metrics` | Prometheus text format (`text/plain; version=0.0.4`) |
| `GET /healthz` | `200 ok` while the scheduler's monitor thread ticked within the last second, else `503 sysmon heartbeat stale`. Use for liveness |
| `GET /readyz` | `200 ready` once the node serves and is not draining, else `503 not ready`. Use for load balancer readiness |

Every node-level metric below is registered at startup and present in the first scrape, zero-valued, so queries and
alerts work on a fresh node. Labeled series appear as their label values are first used, except for the values the
node pre-creates (all admission reasons, memory categories, cache tiers and so on).

## Common labels

| Label | Values |
|---|---|
| `kind` | `deno`, `wasm`, `build` (task kind or runtime kind) |
| `trigger` | `http`, `interval`, `cron`, `queue`, `call`, `build` |
| `outcome` | `ok`, `guest_error`, `cpu_limit`, `oom`, `timeout`, `restarting`, `build_failed`, or an admission reason (below) |
| `reason` (admission) | `draining`, `shed`, `overloaded`, `queue_timeout`, `memory`, `rate_limited`, `quota_exceeded`, `call_depth`; `memory` (a wait for memory ends as `queue_timeout` in this release), `tenant_concurrency` and `service_concurrency` are kept for compatibility and stay 0 |
| `status_class` | `1xx` … `5xx` |
| `listener` | `public`, `internal`, `control`, `metrics` |
| `service` | A service id (`svc_…`) or `_other` (per-service metrics only) |

Histograms of durations use buckets from 0.5 ms to 120 s (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120). Admission waits use 1 ms to 30 s.

## Tasks

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_tasks_total` | counter | `kind`, `trigger`, `outcome` | Finished tasks |
| `rt_task_phase_seconds` | histogram | `kind`, `phase` = `queue`, `wait_runtime`, `init`, `exec` | Time per phase, the same phases as the `server-timing` header |
| `rt_inflight_tasks` | gauge | `kind` | Tasks running or runnable on processors |
| `rt_admission_rejects_total` | counter | `reason` | Tasks that were created and then refused (see note) |

!!! note "What `rt_admission_rejects_total` counts"
    It counts tasks that entered the scheduler and were refused afterwards, which in practice means
    `queue_timeout`. Requests refused before a task exists are not in it: `overloaded` because the queue is full,
    `draining`, `shed`, `call_depth`, and the plan quotas answered at ingress (`rate_limited`, `quota_exceeded`).
    Count those with `rt_http_requests_total` by `status_class`, or from your proxy's logs.

## Admission and autoscaling

The admission layer (run slots and service queues) and the per-service autoscaler. See
[Scheduler](../concepts/scheduler.md).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_admission_running` | gauge | | Tasks holding a run slot |
| `rt_admission_queued` | gauge | | Tasks waiting in service queues for a run slot |
| `rt_admission_queued_bytes` | gauge | | Bytes charged to waiting tasks (4 KiB plus the known body size each) |
| `rt_admission_queue_wait_seconds` | histogram | | Wait of tasks that queued, until they got a slot |
| `rt_autoscale_adjustments_total` | counter | `direction` = `up`, `down`; `cause` = `queue`, `latency`, `memory`, `cpu`, `idle` | Changes of services' adaptive concurrency limits |
| `rt_warm_target_instances` | gauge | `kind` = `deno`, `wasm` | Warm instances the autoscaler asks the executors to keep |
| `rt_sched_spinner_mismatch_total` | counter | | Internal self-check of the scheduler; it must stay 0. Report it if it grows |

## Scheduler

Updated once a second from a scheduler snapshot.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_sched_processors` | gauge | `kind` | Processors serving each kind |
| `rt_sched_grq_len` | gauge | `kind` | Tasks in the global run queue of each kind |
| `rt_sched_lrq_len` | gauge | `p` (processor number) | Tasks in each processor's local run queue |
| `rt_sched_steals_total` | counter | `kind` | Tasks one processor took from another's queue |
| `rt_sched_rekinds_total` | counter | `from`, `to` | Processors moved from one kind to the other |
| `rt_sched_preemptions_total` | counter | `kind` | Tasks preempted at the end of a time slice |
| `rt_sched_hog_handoffs_total` | counter | | Deno tasks that ran past their time slice and had their processor's queue handed off |
| `rt_sched_spinning_seconds_total` | gauge | | Registered, not populated in this release (always 0) |
| `rt_sysmon_tick_seconds` | histogram | | Registered, not populated in this release (always empty) |

## Memory

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_mem_budget_bytes` | gauge | | The memory budget (`memory.budget_mb`, or 85 % of the container limit) |
| `rt_mem_committed_bytes` | gauge | `category` = `deno_heap`, `wasm_linear`, `wasm_reuse`, `t1_cache`, `io_buffers`, `sql_cache`, `logs`, `other` | Memory reserved against the budget, per use. `wasm_reuse` is idle reusable WASM instances; `io_buffers` stays 0 in this release |
| `rt_mem_rss_bytes` | gauge | | Resident set size of the process. Read from `/proc`, so 0 on macOS |
| `rt_mem_fragmentation_ratio` | gauge | | RSS ÷ (committed + baseline RSS); a ratio that keeps rising means memory is not returned |
| `rt_mem_pressure_level` | gauge | | 0 normal, 1 high, 2 critical |

## Code cache

T1 is compiled code in memory, T2 on disk ([Caching](../concepts/caching.md)).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_cache_entries` | gauge | `tier` | Entries in the in-memory cache |
| `rt_cache_bytes` | gauge | `tier` | Bytes of those entries |
| `rt_cache_hits_total` | counter | `tier` | Requests that found their version in memory |
| `rt_cache_misses_total` | counter | `tier` | Requests that had to load or build it |
| `rt_cache_evictions_total` | counter | `tier`, `reason` | Evicted entries: `tier="t1"` with `reason` = `pressure`, `ttl`, `manual`; `tier="t2"` with `reason="disk"` (disk garbage collection) |
| `rt_cache_unreserved_inserts_total` | counter | | In-memory inserts held without a memory reservation because the budget was full and nothing was evictable |

Only `tier="t1"` is populated for entries, bytes, hits and misses; the `t2` series exist and stay 0. Use
`GET /v1/node/cache` for the disk tier.

## Executors

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_deno_isolate_create_seconds` | histogram | | Time to create a V8 isolate. Its `_count` is the number of isolates created: with warm reuse it barely moves |
| `rt_deno_code_cache_total` | counter | `result` = `hit`, `miss`, `rejected` | V8 code cache use when modules are loaded |
| `rt_wasm_instantiate_seconds` | histogram | | Time to instantiate a WASM component for a task |
| `rt_wasm_pool_slots` | gauge | | Size of the WASM pooling allocator (slots), set at startup |

## Builds

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_build_total` | counter | `kind` = `deno`, `wasm`; `result` = `ok`, `failed` | Finished builds |
| `rt_build_seconds` | histogram | `kind` | Build duration |

## HTTP and egress

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_http_requests_total` | counter | `listener`, `status_class` | Every response of every listener, including the node's own refusals and your services' responses |
| `rt_ingress_conn_rejected_total` | counter | | Connections closed right after accept because of `ingress.max_conns_per_ip` |
| `rt_egress_requests_total` | counter | `result` = `ok` or an egress code (`egress_denied`, `egress_limit`, `egress_too_large`, `egress_quota`, `egress_timeout`, `egress_dns`, `egress_upstream`, `egress_insecure_redirect`) | Guest `fetch` requests through the SSRF guard. A pre-created `result="denied"` series stays 0 |
| `rt_egress_bytes_total` | counter | | Bytes received by guest fetches |

## Triggers and queues

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_schedule_fires_total` | counter | | Schedule fires that started a task |
| `rt_schedule_skipped_total` | counter | | Ticks that started no run: the previous run of the same schedule was still going, the service had no live version, the node refused the task, or several ticks passed at once |
| `rt_schedule_denied_total` | counter | | Fires dropped because the tenant's plan no longer allows `interval` or `cron` |
| `rt_queue_depth` | gauge | | Ready messages across all services that consume a queue, refreshed every 30 s |
| `rt_queue_dlq_total` | counter | | Messages moved to a dead-letter queue |

## Logs

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_logs_records_total` | counter | `kind` = `runtime`, `build`, `system` | Service log records written |
| `rt_logs_dropped_total` | counter | | Records dropped by the per-service rate limit (100 lines/s, bursts of 1,000) |

## Per-service metrics

Off by default, because they add series per service. Turn them on in `rt.toml` (restart to apply):

```toml
[metrics]
per_service = true
per_service_top_n = 100
```

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `rt_service_requests_total` | counter | `service`, `status_class` | Finished tasks per service, by the HTTP status of their outcome |
| `rt_service_duration_seconds` | histogram | `service` | Total time (all four phases) per task |
| `rt_service_concurrency_limit` | gauge | `service` | The service's current adaptive concurrency limit |
| `rt_service_running` | gauge | `service` | Its tasks holding a run slot |
| `rt_service_queued` | gauge | `service` | Its tasks waiting for one |
| `rt_service_queue_wait_seconds` | gauge | `service` | Mean admission wait over the last autoscaler tick |

How the `service` label is bounded:

- The first `per_service_top_n` services that receive traffic get their own label (the service id), and keep it
  until the node restarts. Every other service is counted under `service="_other"`. Cardinality stays at most
  `per_service_top_n + 1` services per metric.
- The four admission gauges exist only for services that own a label and were active in the last autoscaler tick.
  A service that goes quiet has its series removed; there is no `_other` series for them.

!!! tip "Without Prometheus"
    `GET /v1/services/{s}/stats` (`21ci stats <svc>`) gives counts, error rate and p50/p95/p99 per phase for any
    service, always on, regardless of these settings.

## Useful queries

Requests per second on the public listener, by status class:

```text
sum by (status_class) (rate(rt_http_requests_total{listener="public"}[5m]))
```

Task outcomes other than success, per kind:

```text
sum by (kind, outcome) (rate(rt_tasks_total{outcome!="ok"}[5m]))
```

p99 of each phase for Deno tasks (where is the time going?):

```text
histogram_quantile(0.99, sum by (le, phase) (rate(rt_task_phase_seconds_bucket{kind="deno"}[5m])))
```

p99 admission wait, and how many tasks wait right now:

```text
histogram_quantile(0.99, sum by (le) (rate(rt_admission_queue_wait_seconds_bucket[5m])))
rt_admission_queued
```

Isolates created per second (drops to near zero when Deno warm reuse works):

```text
rate(rt_deno_isolate_create_seconds_count[5m])
```

Memory use against the budget, and what holds it:

```text
sum(rt_mem_committed_bytes) / rt_mem_budget_bytes
topk(3, rt_mem_committed_bytes)
```

In-memory code cache hit ratio:

```text
sum(rate(rt_cache_hits_total{tier="t1"}[5m]))
  / (sum(rate(rt_cache_hits_total{tier="t1"}[5m])) + sum(rate(rt_cache_misses_total{tier="t1"}[5m])))
```

Autoscaler activity, by cause:

```text
sum by (direction, cause) (rate(rt_autoscale_adjustments_total[15m]))
```

Top services by error rate (per-service metrics on):

```text
topk(10,
  sum by (service) (rate(rt_service_requests_total{status_class="5xx"}[5m]))
    / sum by (service) (rate(rt_service_requests_total[5m])))
```

Ready-made alert rules are in [Production deployment](../guides/production.md#monitoring).

## Other signals

- **`server-timing`** on every invoke response: `queue`, `wait_runtime`, `init` and `exec` in milliseconds, for one
  request. See [Performance tuning](../guides/performance.md#where-the-time-goes).
- **`GET /v1/node/status`**, **`/v1/node/scheduler`**, **`/v1/node/cache`** (operator token): mode, processors,
  memory pressure, run queues, cache tiers.
- **`RT_SCHEDTRACE=<ms>`**: prints a scheduler summary line to stderr at that interval.
- **Traces**: `[otel]` is accepted in the configuration, but this release exports no traces
  <span class="rt-badge rt-badge--planned">planned</span>.

## See also

- [Observability](../concepts/observability.md): logs, stats and metrics together.
- [Production deployment](../guides/production.md#monitoring): scrape configuration and alerts.
- [`[metrics]` configuration](../config.md#other-sections).
