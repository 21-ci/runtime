---
title: Observability
description: Service logs, stats and events for developers; Prometheus metrics, health probes and system logs for operators; and the response headers that tie them together.
---

# Observability

The runtime gives you two kinds of visibility. As a **developer** you read your service's logs, latency stats and
control-plane events through the control API or `21ci`, scoped to your tenant. As an **operator** you scrape
Prometheus metrics, probe health endpoints and read the node's own JSON logs. Response headers connect the two: every
response carries the task id and a timing breakdown you can look up later.

| Signal | For | Where |
|---|---|---|
| [Service logs](#service-logs) | Developers | `GET /v1/services/{s}/logs` (JSON or live SSE), `21ci logs` |
| [Build logs](#build-logs) | Developers | `GET …/versions/{v}/build/logs`, service logs with `kind=build` |
| [Stats](#stats) | Developers | `GET /v1/services/{s}/stats`, `21ci stats` |
| [Events](#events) | Developers, auditors | `GET /v1/services/{s}/events`, `21ci events` |
| [Response headers](#response-headers-for-debugging) | Everyone | `x-rt-task-id`, `x-rt-version`, `server-timing`, `x-rt-reason` |
| [Prometheus metrics](#prometheus-metrics) | Operators | `GET /metrics` on the metrics listener (`:9090`) |
| [Health](#health-and-readiness) | Operators, orchestrators | `GET /healthz`, `GET /readyz` on `:9090` |
| [System logs](#system-logs) | Operators | JSON lines on the node's stdout |

## Service logs

Everything your code prints ends up in its service's log, tagged with the task that printed it.

| Source | Level |
|---|---|
| Deno `console.debug` / `console.log`, `console.info` / `console.warn` / `console.error` | `debug` / `info` / `warn` / `error` |
| WASM stdout / stderr | `info` / `error` |
| WASM `std:host/log` | the level you pass |

Each record has `seq`, `ts` (Unix ms), `level`, `service_id`, `version`, `task_id`, `trigger`, `kind` and `msg`.
`kind` is `runtime` for your output, `build` for build steps and `system` for notes the runtime adds about your
service (for example when a task hit its CPU limit). `task_id` is the same id the response returned in
`x-rt-task-id`.

| Limit | Value | What happens |
|---|---|---|
| Message length | 10,000 characters | Cut, with `truncated: true` |
| Lines per task | 1,000 | Further lines of that task are dropped |
| Rate per service | 100 lines/s, bursts up to 1,000 | Excess is dropped and summarized in one `"N lines dropped"` record |
| Retention | `logs.retention_days` (7) and `logs.max_mb_per_service` (50 MB) | Oldest segments deleted first, checked hourly |

Logs are kept in a per-service in-memory ring (`logs.ring_lines`, 1,000) for live tails and in append-only segments
under `logs/<service-id>/` in the data directory. Writes are buffered and flushed every 100 ms, so a crash loses at
most the last 100 ms; a graceful shutdown loses nothing. Operators can also echo them to stdout with `logs.stdout`.

### Reading logs

=== "21ci"

    ```console
    $ 21ci logs hello --since 15m --level warn
    $ 21ci logs hello --task task_0000000000000002
    $ 21ci logs hello -f              # follow live; Ctrl-C to stop
    ```

=== "curl"

    ```bash
    # One page (oldest first): filters since/until (RFC 3339), level (minimum), kind, task_id, limit, cursor
    curl -sS -H "Authorization: Bearer $RT_KEY" \
      "$RT_API/v1/services/$SVC/logs?level=warn&since=2026-10-07T09:00:00Z&limit=100"

    # Follow as Server-Sent Events
    curl -sSN -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/logs?follow=1"
    ```

A page is `{items: [...], next_cursor}`; `limit` defaults to 100 and is capped at 1,000. With `follow=1` the response
is an SSE stream: each record is an event with `id: <seq>`, `event: log` and the record as JSON in `data`:

```text
id: 42
event: log
data: {"seq":42,"ts":1791364553026,"level":"info","service_id":"svc_01J…","version":3,"task_id":"task_000000000000002a","trigger":"http","kind":"runtime","msg":"hit /a","truncated":false}
```

If the connection drops, reconnect with the `Last-Event-ID` header set to the last `seq` you saw, and the stream
resumes after it without gaps or duplicates (`21ci logs -f` does this for you). One key may hold at most
`ingress.sse_streams_per_key` (10) streams at once; more get `429 rate_limited`. Reading logs needs the `logs:read`
scope.

## Build logs

A build writes its progress and its errors (for example `build started (deno)`, `build finished in 25 ms`) as
`kind=build` records in the service log. While a version builds, you can also follow just that build:

=== "21ci"

    ```console
    $ 21ci build-logs hello 3         # one version's build, until it is ready or failed
    $ 21ci logs hello --kind build    # build records of every version
    ```

=== "curl"

    ```bash
    curl -sSN -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/3/build/logs"
    ```

The build-log stream is SSE and ends when the version is `ready` or `failed`. A failed version also carries a
`failure` object in `GET /v1/services/{s}/versions/{v}`.

## Stats

`GET /v1/services/{s}/stats` answers "how is my service doing?" without Prometheus. It is always on, kept in memory
per service (one-minute buckets for the last 24 hours), and lags real time by about a second.

=== "21ci"

    ```console
    $ 21ci stats hello --window 1h
    Window:       1h
    Requests:     7
    Errors:       0 (0.00%)
    Outcomes:     ok=7
    Concurrency:  limit 8 (1..64), running 0, queued 0 (max 256)
    Queue wait:   0.0 ms (timeouts 0)
    Autoscaler:   demand 1.4, latency baseline 7.2 ms, warm 1/proc, idle 12s

    PHASE  P50_MS  P95_MS  P99_MS
    queue  0.1     0.2     0.2
    init   0.0     0.0     0.0
    exec   7.6     39.7    39.7
    total  8.3     39.9    39.9
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/stats?window=1h"
    ```

`window` is `5m` (default), `1h` or `24h`. The response has `count`, `errors`, `error_rate`, `outcomes` (counts by
outcome: `ok`, `guest_error`, `cpu_limit`, `queue_timeout`, …) and `p50_ms` / `p95_ms` / `p99_ms` for each phase:
`queue`, `init`, `exec` and `total`. A rising `queue` means requests wait for a slot (see
[scheduler](scheduler.md#admission-queueing-instead-of-429)); a rising `init` means cold instances.

The response also carries `admission`, the service's queueing and autoscaler state right now (the same object as
`status.admission` of the service): the adaptive concurrency `limit` with its bounds, the tasks `running` and
`queued`, the smoothed `queue_wait_ms`, the number of `queue_timeouts`, and the autoscaler's `demand`, latency
baseline and warm instances per processor. It is `null` while the service has not run recently. In `21ci stats` these
are the `Concurrency`, `Queue wait` and `Autoscaler` lines. See
[observing the scheduler](scheduler.md#observing-the-scheduler).

## Events

Control-plane actions on a service are recorded as events: deploys, rollbacks (including automatic ones after a failed
build), env and secret writes, schedule and DLQ operations, host changes and data exports. Each event has `id`, `ts`,
`kind` and `data`, where `data` names the actor (operator, key id or system). Secret values never appear.

=== "21ci"

    ```console
    $ 21ci events hello --limit 3
    TIME                  KIND         ACTOR                           DATA
    2026-10-02T06:15:36Z  dlq.redrive  key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"redriven":0}
    2026-10-02T06:15:26Z  rollback     key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"from":2,"to":1}
    2026-10-02T06:15:26Z  deploy       key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"version":2,"digest":"sha256:e5fd…","activated":true}
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/events?limit=20"
    ```

Events are newest first and paginated with `limit` / `cursor` (`services:read` scope). The full list of event kinds
is on the [security model](security.md#audit-events) page.

## Response headers for debugging

Every response produced by a task carries `x-rt-task-id` and `x-rt-version`, including runtime failures such as
`502 cpu_limit`. A response your handler returned, whatever its status, also carries `server-timing`. Refusals at
ingress (`401`, `403`, `429`, …) happen before a task exists and carry neither.

```console
$ curl -si "$RT_INVOKE/s/hello/?name=Ada"
HTTP/1.1 200 OK
content-type: application/json
x-rt-task-id: task_0000000000000002
x-rt-version: 1
server-timing: queue;dur=0.000, wait_runtime;dur=0.000, init;dur=5.932, exec;dur=4.720
```

| Header | Meaning |
|---|---|
| `x-rt-task-id` | The task that served the request. Find its log lines with `21ci logs <svc> --task <id>` or `?task_id=` |
| `x-rt-version` | The version that served it: useful right after a deploy or rollback |
| `server-timing` | Milliseconds per phase, shown in browser dev tools |

The `server-timing` phases:

| Phase | Time spent | Grows when |
|---|---|---|
| `queue` | Waiting for a run slot | The service is at its concurrency limit or the node is busy |
| `wait_runtime` | Waiting for the code to load (T2) or compile (T3) | First request after a deploy, eviction or node restart ([caching](caching.md)) |
| `init` | Creating the isolate or instance and evaluating module top level | No warm instance was available |
| `exec` | Running your handler | Your code or the APIs it calls are slow |

When the runtime itself refuses or fails a request, the body is `text/plain`, the reason is in `x-rt-reason`, and
capacity refusals add `Retry-After`. Responses your handler returns never carry `x-rt-reason`. All reasons are listed
in [error reasons](../reference/errors.md#invoke-reasons-x-rt-reason).

## Trace context

The runtime accepts a W3C `traceparent` header on incoming requests and generates one when the client sent none. Your
handler sees it as a normal request header, so you can forward it to the APIs you call and correlate across systems:

```ts title="main.ts"
export default async function (req: Request): Promise<Response> {
  const traceparent = req.headers.get("traceparent");
  const upstream = await fetch("https://api.example.com/v1/orders", {
    headers: traceparent ? { traceparent } : {},
  });
  return new Response(upstream.body, { status: upstream.status });
}
```

!!! info "Trace export"
    <span class="rt-badge rt-badge--planned">planned</span> Spans are not exported in this release: the `[otel]`
    section (`endpoint`, `sample_ratio`) is accepted but nothing is sent. Use logs, stats and metrics instead.

## Prometheus metrics

<span class="rt-badge rt-badge--operator">Operator</span> The metrics listener (`listeners.metrics`, default
`127.0.0.1:9090`) serves Prometheus text at `/metrics`. It has no authentication: keep it on loopback or a private
network.

```yaml title="prometheus.yml"
scrape_configs:
  - job_name: rt
    static_configs:
      - targets: ["127.0.0.1:9090"]
```

Key series by area (the [metrics reference](../reference/metrics.md) lists every series and label):

| Area | Series |
|---|---|
| Tasks | `rt_tasks_total{kind,trigger,outcome}`, `rt_task_phase_seconds{kind,phase}`, `rt_inflight_tasks{kind}` |
| Admission | `rt_admission_running`, `rt_admission_queued`, `rt_admission_queue_wait_seconds`, `rt_admission_rejects_total{reason}`, `rt_autoscale_adjustments_total{direction,cause}`, `rt_warm_target_instances{kind}` |
| Scheduler | `rt_sched_processors{kind}`, `rt_sched_grq_len{kind}`, `rt_sched_steals_total{kind}`, `rt_sched_preemptions_total{kind}`, `rt_sched_rekinds_total{from,to}`, `rt_sysmon_tick_seconds` |
| Memory | `rt_mem_budget_bytes`, `rt_mem_committed_bytes{category}`, `rt_mem_rss_bytes`, `rt_mem_pressure_level` |
| Cache and builds | `rt_cache_hits_total{tier}`, `rt_cache_misses_total{tier}`, `rt_cache_evictions_total{tier,reason}`, `rt_build_total{kind,result}`, `rt_build_seconds{kind}` |
| HTTP and egress | `rt_http_requests_total{listener,status_class}`, `rt_ingress_conn_rejected_total`, `rt_egress_requests_total{result}`, `rt_egress_bytes_total` |
| Triggers | `rt_schedule_fires_total`, `rt_schedule_skipped_total`, `rt_queue_depth`, `rt_queue_dlq_total` |
| Logs | `rt_logs_records_total{kind}`, `rt_logs_dropped_total` |

Useful queries:

```text
# Requests refused for capacity, by reason
sum by (reason) (rate(rt_admission_rejects_total[5m]))

# p99 time requests waited for a run slot
histogram_quantile(0.99, sum by (le) (rate(rt_admission_queue_wait_seconds_bucket[5m])))

# Share of tasks that failed, by outcome
sum by (outcome) (rate(rt_tasks_total{outcome!="ok"}[5m])) / ignoring(outcome) group_left sum(rate(rt_tasks_total[5m]))
```

**Per-service series** are off by default to keep cardinality bounded. With `metrics.per_service = true`, the first
`metrics.per_service_top_n` (100) services that get traffic get their own `service` label (and keep it until the node
restarts; it is not re-ranked by traffic) on `rt_service_requests_total`,
`rt_service_duration_seconds` and the per-service admission gauges (`rt_service_concurrency_limit`,
`rt_service_running`, `rt_service_queued`, `rt_service_queue_wait_seconds`); the rest are summed into
`service="_other"`. Service [stats](#stats) need none of this.

## Health and readiness

Both probes are on the metrics listener and need no credentials:

| Endpoint | `200` when | `503` when | Use it for |
|---|---|---|---|
| `/healthz` | The node's internal heartbeat ticked within the last second | The heartbeat is stale (the node is wedged) | Liveness: restart the container |
| `/readyz` | The node has started and is not draining | The node is draining (`POST /v1/node/drain`, or shutdown) | Readiness: load-balancer membership |

```bash
curl -fsS http://127.0.0.1:9090/healthz    # ok
curl -fsS http://127.0.0.1:9090/readyz     # ready
```

The container image's healthcheck uses `/healthz`. Point load balancers at `/readyz` so a draining node stops getting
new traffic while its in-flight tasks finish.

## System logs

<span class="rt-badge rt-badge--operator">Operator</span> The node's own logs are JSON lines on stdout
(`docker compose logs rt`). Filter them with `RT_LOG` (default `info`), using `tracing` directives such as
`RT_LOG=info,rt_sched=debug`; when `RT_LOG` is unset, `RUST_LOG` is read. System logs never contain env or secret
values. Audit events appear with the target `rt::audit`.

For scheduler debugging, `RT_SCHEDTRACE=<ms>` prints a one-line scheduler summary to stderr at that interval, and the
operator endpoints `GET /v1/node/status`, `/v1/node/scheduler` and `/v1/node/cache` return JSON snapshots (`21ci node
status|scheduler|cache`). The [scheduler](scheduler.md#observing-the-scheduler) and
[caching](caching.md#observing-the-cache) pages explain their fields.

## See also

- [Error reasons](../reference/errors.md): every `x-rt-reason` and control API code.
- [Metrics reference](../reference/metrics.md): the full list of series.
- [Running in production](../guides/production.md): probes, log shipping and alerting.
- [`[logs]`](../config.md#other-sections) and [`[metrics]`](../config.md#other-sections) configuration.
