---
title: Logs, stats and usage
description: Read a service's logs live or by time range, follow a build, read request statistics and deploy events, check your usage against the plan, and trace one request through headers and task ids.
---

# Logs, stats and usage

When a deploy looks wrong or a user reports an error, you have five sources, all scoped to your tenant:

| Source | Answers | Command | Scope |
|---|---|---|---|
| [Service logs](#service-logs) | What did my code print, and what did the runtime say about my task? | `21ci logs` | `logs:read` |
| [Build logs](#build-logs) | Why did this version not build? | `21ci build-logs` | `logs:read` |
| [Stats](#stats) | How many requests, how many failed, how slow, how busy? | `21ci stats` | `logs:read` |
| [Events](#events) | Who deployed, rolled back or changed what, and when? | `21ci events` | `services:read` |
| [Usage](#usage-and-quotas) | How much of my plan have I used? | `21ci tenant usage` | `logs:read` |

Plus [response headers](#response-headers) that link one request to its log lines. Operators have more (Prometheus
metrics, node logs); those are in [Observability](../concepts/observability.md) and
[Running in production](production.md).

Examples assume `RT_API` and `RT_KEY` are set ([Get access](access.md)), a service `hello`, and its id in `$SVC`.

## Service logs

Everything your code prints is stored with the task that printed it.

| Source | Level |
|---|---|
| Deno `console.debug`, `console.log` / `console.info`, `console.warn`, `console.error` | `debug`, `info`, `warn`, `error` |
| WASM stdout, stderr | `info`, `error` |
| WASM `std:host/log` | the level you pass |

Each record has:

| Field | Meaning |
|---|---|
| `seq` | Per-service sequence number; the cursor for paging and resuming |
| `ts` | Unix milliseconds |
| `level` | `debug`, `info`, `warn`, `error` |
| `version` | The service version that ran |
| `task_id` | The task, equal to the `x-rt-task-id` response header; empty for build and system records |
| `trigger` | `http`, `interval`, `cron`, `queue` or `call` |
| `kind` | `runtime` (your output), `build` (build steps) or `system` (notes the runtime adds about your task, such as hitting a limit) |
| `msg`, `truncated` | The line, and whether it was cut |

### Read and filter

=== "21ci"

    ```bash
    21ci logs hello                                  # one page, oldest first (100 lines)
    21ci logs hello --since 15m --level warn         # last 15 minutes, warn and above
    21ci logs hello --since 2026-10-07T09:00:00Z --until 2026-10-07T09:30:00Z
    21ci logs hello --task task_000000000000002a     # every line of one request
    21ci logs hello --kind build                     # build records of every version
    21ci logs hello -f                               # follow live; Ctrl-C to stop
    21ci logs hello --since 10m --json | jq -r '.items[] | "\(.ts) \(.msg)"'
    ```

    ```console
    2026-10-02T06:15:53.026Z info  v1 - build started (deno)
    2026-10-02T06:15:53.052Z info  v1 - build finished in 25 ms
    2026-10-02T06:15:55.560Z info  v1 task_000000000000000b hit /a
    ```

    Columns: time, level, `v<version>`, task id (`-` for build and system records), message.

    | Option | Meaning |
    |---|---|
    | `--since`, `--until` | An RFC 3339 time, or a duration back from now (`15m`, `2h`, `1d`) |
    | `--level` | Minimum level |
    | `--task` | One task id |
    | `--kind runtime\|build\|system` | One kind of record |
    | `--limit N` | Page size, at most 1000 (default 100) |
    | `--cursor C` | Continue after the cursor printed under a page |
    | `-f` | Follow (Server-Sent Events) |

=== "curl"

    ```bash
    # One page, oldest first. since/until are RFC 3339; level is a minimum
    curl -sS -H "Authorization: Bearer $RT_KEY" \
      "$RT_API/v1/services/$SVC/logs?level=warn&since=2026-10-07T09:00:00Z&limit=200"

    # One task
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/logs?task_id=task_000000000000002a"

    # Follow as Server-Sent Events
    curl -sSN -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/logs?follow=1"
    ```

    A page is `{"items": [...], "next_cursor": "43"}`; pass `cursor=43` for the next one.

    ```json
    {"seq":42,"ts":1791364553026,"level":"info","service_id":"svc_01M3XM1AMYAKX6EREP6J536ATG","version":3,"task_id":"task_000000000000002a","trigger":"http","kind":"runtime","msg":"hit /a","truncated":false}
    ```

### Follow live

`follow=1` (or `-f`) turns the response into a Server-Sent Events stream. Each record is an event with `id: <seq>`,
`event: log` and the record as JSON in `data`. Without a cursor the stream starts from the node's in-memory ring (the
last 1,000 lines of the service) and then delivers new lines as they arrive. If the connection drops, reconnect with the
`Last-Event-ID` header set to the last `seq` you saw and the stream resumes without gaps or duplicates (`21ci logs -f`
does this for you). The stream accepts the `level`, `kind` and `task_id` filters but not a time range; `21ci logs -f
--since …` prints the matching backlog first and then resumes the stream after the last record.

One key can hold at most 10 streams (`ingress.sse_streams_per_key`); more get `429 rate_limited` with a `Retry-After`.
Log and build-log streams share that budget, so close old `curl -N` sessions.

### Limits and retention

| Limit | Default | What happens |
|---|---|---|
| Message length | 10,000 characters | Cut, with `truncated: true` |
| Lines per task | 1,000 | Further lines of that task are dropped |
| Rate per service | 100 lines/s sustained, bursts up to 1,000 | The excess is dropped and summarized by one `N lines dropped` record |
| Live tail | The last 1,000 lines per service in memory | Older lines are read from disk |
| Retention | 7 days and 50 MB per service | Oldest segments are deleted first |
| Write buffering | Flushed every 100 ms | A crash can lose the last 100 ms |

The line cap and the rate limit protect the node from a service that logs in a loop, so a log storm shows up as a
`dropped` record and a gap, not as an unbounded bill. The retention values are the operator's settings
(`logs.retention_days`, `logs.max_mb_per_service`); ask if you need to keep more. If you need logs for longer, ship them
yourself: a scheduled task can read them through this API.

!!! tip "What to log"
    Log one line per request with the data you will search for (a user id, an order id), at `info`; use `warn` and `error`
    for what needs attention; never log secrets. Include `ctx.taskId` (Deno handlers get it in `ctx`) in error bodies you
    return to your own clients, so a user report leads straight to `21ci logs hello --task …`.

## Build logs

A build writes its progress and errors as `kind=build` records in the service log. To follow one version's build:

=== "21ci"

    ```bash
    21ci build-logs hello 3        # streams until the build is ready or failed
    21ci versions hello 3          # build state and the failure object
    ```

=== "curl"

    ```bash
    curl -sSN -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/3/build/logs"
    curl -sS  -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/3"
    ```

The stream is SSE in the same record format and ends when the version is `ready` or `failed`. It needs `logs:read`, while
`versions` needs `services:read`. What to do with a failed build is in [Deploy and roll back](deploy.md#builds-and-failures).

## Stats

`21ci stats` answers "how is the service doing?" without any monitoring setup. It is always on, kept in memory per
service in one-minute buckets for the last 24 hours, and lags real time by about a second.

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

`window` is `5m` (default), `1h` or `24h`. The response has:

| Field | Meaning |
|---|---|
| `count` | Tasks finished in the window |
| `errors`, `error_rate` | Tasks whose outcome is not `ok`, and their share |
| `outcomes` | Counts by outcome: `ok`, `guest_error`, `cpu_limit`, `oom`, `timeout`, `build_failed`, `queue_timeout`, `overloaded`, ... (the same words as `x-rt-reason`) |
| `queue`, `init`, `exec`, `total` | Each with `p50_ms`, `p95_ms`, `p99_ms` |
| `admission` | The service's queue and autoscaler state right now (adaptive limit, running, queued, queue wait); `null` while the service is idle |

How to read it:

- **`ok` means your handler returned a response, of any status.** A `500` your code chose is `ok`; `errors` counts what the
  runtime stopped or refused (exceptions, limits, queue timeouts). Watch your own status codes in your logs.
- **`queue`** is the time requests waited for a slot. It grows when the service is at its concurrency limit, and
  `queue_timeout` outcomes appear when the wait runs out. See [Configure a service](configure.md#scaling-and-warm-instances).
- **`init`** is creating the isolate or instance and running your module's top level. High values mean cold starts or
  heavy module initialization.
- **`exec`** is your handler. High `exec` is your code or what it waits for (network, storage).
- Percentiles are approximate (the histograms keep two significant digits), counted per node, and reset when the node
  restarts. Use them for trends and comparisons before and after a change, not as accounting.

## Events

Control-plane actions on a service are recorded: deploys, rollbacks (including automatic ones after a failed build), live
changes, rebuilds, config, env and secret writes (names only, never values), hosts, schedules, DLQ operations and data
exports. Newest first.

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

Use events to answer "what changed before the errors started": a `deploy` or `service.updated` right before the stats
turned bad is the usual answer. The full list of kinds is on the [Security model](../concepts/security.md) page.

## Usage and quotas

The node counts invocations, CPU milliseconds, egress bytes and storage bytes per service and UTC day. A tenant key with
`logs:read` can read its own tenant's usage, limited to the services the key may touch.

=== "21ci"

    ```console
    $ 21ci tenant usage --from 7d
    DAY         SERVICE                         INVOCATIONS  CPU_MS  EGRESS     STORAGE
    2026-10-06  svc_01M3XM1AMYAKX6EREP6J536ATG  1200         8400    2.0 MiB    1.0 MiB
    2026-10-07  svc_01M3XM1AMYAKX6EREP6J536ATG  300          2100    512.0 KiB  1.0 MiB
    TOTAL       -                               1500         10500   2.5 MiB    -
    ```

    Without an id the CLI uses the tenant of your key. `--from` and `--to` take a day (`2026-10-01`) or a duration back from
    today (`7d`). Storage is a level, so the total row does not sum it.

=== "curl"

    ```bash
    TENANT=$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/me" | jq -r .tenant_id)
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/tenants/$TENANT/usage?from=2026-10-01&to=2026-10-07"
    # [{"service_id":"svc_…","day":"2026-10-07","invocations":300,"cpu_ms":2100,"egress_bytes":524288,"storage_bytes":1048576}, …]
    ```

Compare it with your plan to see how much headroom you have. `whoami --json` prints the plan with the tenant's overrides
applied:

```bash
21ci whoami --json | jq '.plan.settings | {rps, invocations_per_day, storage_mb, egress_mb_per_day, max_services, builds_per_hour}'
```

A `0` in `rps` or `invocations_per_day` means unlimited. Daily quotas (`invocations_per_day`, `egress_mb_per_day`) reset at UTC midnight. When one is
used up, requests get `429 quota_exceeded` with a `Retry-After` until then. The counters are flushed every few seconds,
so today's row can be a few seconds behind. There is no separate "quota remaining" call: usage and plan together are the
view. How each plan key reacts when exceeded is in [Configure a service](configure.md#how-plan-limits-reach-your-service).

## Response headers

Every response produced by a task carries the headers below, including runtime failures such as `502 cpu_limit`. A request
that is refused at ingress (`401`, `403`, `404`, `405`, `429`) never became a task and carries none of them.

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
| `x-rt-task-id` | The task that served the request. Its log lines: `21ci logs <svc> --task <id>` |
| `x-rt-version` | The version that served it. Confirms a deploy or rollback took effect |
| `server-timing` | Milliseconds per phase (below); visible in browser dev tools |
| `x-rt-reason` | Present only when the **runtime**, not your handler, answered: `guest_error`, `cpu_limit`, `oom`, `timeout`, `queue_timeout`, `overloaded`, ... The body is `text/plain` with the same word |
| `retry-after` | Seconds to wait; on `429`, and on `503` capacity reasons |

| `server-timing` phase | Time spent | Grows when |
|---|---|---|
| `queue` | Waiting for a run slot | The service is at its concurrency limit, or the node is busy |
| `wait_runtime` | Waiting for the code to load or compile | First request after a deploy, an eviction or a node restart |
| `init` | Creating the isolate or instance and evaluating the module top level | No warm instance was available |
| `exec` | Running your handler | Your code or the APIs it calls are slow |

A response your handler returns never carries `x-rt-reason`, so a client can always tell a platform error from your own.
All reasons are listed on [Error reasons](../reference/errors.md#invoke-reasons-x-rt-reason). `21ci invoke` prints these
headers for you, and `--fail` turns a status of 400 or more into exit code 1 (4 for a busy answer).

### Find one request in the logs

1. Make or reproduce the request and keep `x-rt-task-id` (a user can send it to you if your service shows it).
2. `21ci logs hello --task task_000000000000002a` prints every line of that task, including the stack of an uncaught
   exception and any `system` note from the runtime.
3. If the id was lost, narrow by time and level: `21ci logs hello --since 10m --level error`.

**Across services.** The node accepts a W3C `traceparent` request header and generates one when the client sent none. Your
handler sees it as a normal header. Log records carry the task id, not the trace id, so log the trace id yourself when you
want to search by it, and forward it to the services you call:

```ts
export default async function (req: Request): Promise<Response> {
  const traceparent = req.headers.get("traceparent") ?? "";
  console.log(`order received trace=${traceparent}`);
  const upstream = await fetch("https://api.example.com/v1/orders", {
    headers: traceparent ? { traceparent } : {},
  });
  return new Response(upstream.body, { status: upstream.status });
}
```

```bash
21ci logs hello --since 1h --json | jq -r '.items[] | select(.msg | contains("trace=00-4bf92f3577b34da6a3ce929d0e0e4736")) | .task_id'
```

The runtime does not export trace spans yet; `traceparent` is propagation only.

## Troubleshooting

| Symptom | Look at | Likely cause and fix |
|---|---|---|
| `500` with `x-rt-reason: guest_error` | `21ci logs hello --task <id>` | Your handler threw, rejected, or returned something that is not a `Response`. The stack is in the log, never in the response |
| `502 cpu_limit` | `21ci service get hello` (`Limits`), `exec` percentiles | CPU-heavy work in the request path. Optimize, move it to a queue or schedule, or raise `limits.cpu_ms` within the plan |
| `502 oom` | `Limits`, logs of the task | Buffering large bodies in memory. Stream, or raise `limits.memory_mb` |
| `504 timeout` | `exec` p99 in `21ci stats`, logs | Slow upstream or long work. Set client timeouts, return earlier, use a queue |
| `503 queue_timeout` | `Concurrency` and `queue` percentiles in `21ci stats` | The service is at its concurrency limit, or a lazy build is slow. Check `scaling.max_instances` and the plan's `max_instances`; deploy with `--wait` |
| `503 overloaded` | `Concurrency: … queued N (max M)` | `scaling.max_queued` reached. Raise it, or reduce load |
| `502 build_failed` | `21ci versions hello <n>`, `21ci build-logs hello <n>` | The live version did not build. See [Deploy and roll back](deploy.md#builds-and-failures) |
| `429 rate_limited` | `whoami --json` (`rps`), the service's `ingress.rate_limit` | The tenant's or the service's rate limit. Back off for `Retry-After` |
| `429 quota_exceeded` | `21ci tenant usage --from 1d` against `invocations_per_day` | Daily quota used up; resets at UTC midnight |
| `404` on the invoke URL | `21ci service get hello` (`Live`, `Triggers`, `Access`), the route form | No live version, `triggers.http` off, `access: internal`, or a wrong slug |
| `401` on the invoke URL | `Access` | The service is `access: key`; send a key with `invoke` |
| Old behavior after a deploy | `x-rt-version`, `21ci versions hello`, `21ci events hello` | The new version is not live (`--no-activate`), or an automatic rollback happened (`rollback.auto`) |
| First request after a deploy is slow | `server-timing` `wait_runtime` and `init` | Lazy build or cold instance. Deploy with `--wait` |
| Log lines missing | `21ci logs hello --since 1h --kind system` | Per-task line cap, rate limit (`N lines dropped`), retention, or a `--level` filter. The last 100 ms before a crash can be lost |
| `-f` shows nothing | The filters, and `21ci logs hello --limit 5` | A `--level` above what you log, a `--task` that has finished, or no traffic |
| `429 rate_limited` on `logs -f` | Open `curl -N` sessions | Too many streams on one key (10). Close some |

## See also

- [Deploy and roll back](deploy.md) and [Configure a service](configure.md): act on what you find here.
- [Observability](../concepts/observability.md): the operator side (metrics, health, node logs) and the mechanisms.
- [Error reasons](../reference/errors.md): every `x-rt-reason` and control API code.
- [`21ci logs`](../cli.md#logs), [`stats`](../cli.md#stats), [`events`](../cli.md#events) and [`tenant usage`](../cli.md#tenant-operator).
- [Limits and quotas](../reference/limits.md): the numbers behind the table above.
