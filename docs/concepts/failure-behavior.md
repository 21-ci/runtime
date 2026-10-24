---
title: Failure behavior
description: What the node does when a handler throws, runs out of CPU, memory or time, when a build fails, when memory or disk runs short, and across crashes, restarts, reloads and key problems, with what the caller sees and what is persisted.
---

# Failure behavior

Things fail in three places: inside a **task** (your handler), inside the **node** (memory, disk, the process), and in
the **operator's actions** (a bad deploy, a reload, a lost key). This page lists each case with the mechanism, what the
caller sees, what happens to the sandbox and to stored state, and what the operator can look at. The runtime's design
rule is that a failing task must not hurt other tasks, and that a failure is visible as a distinct reason rather than a
generic error.

!!! info "Platform errors and application errors look different"
    When the runtime itself ends or refuses a request, the response is `text/plain` with the reason in `x-rt-reason`.
    A response your handler returns never carries that header, so a client can always tell the two apart. The full list
    is in [Error reasons](../reference/errors.md#invoke-reasons-x-rt-reason).

## Inside a task

| What happens | Caller sees | The sandbox | Stored state |
|---|---|---|---|
| **The handler throws**, rejects, traps (WASM), or returns something that is not a `Response` | `500 guest_error`. The body never contains the message; the exception and stack are in the service logs under the task id | Discarded, even with warm reuse on | Writes that already committed stay; a transaction that was not committed does not take effect |
| **Wall-clock limit** (`limits.wall_ms`) | `504 timeout` | The monitor terminates the task: `terminate_execution` for an isolate (also while it is waiting on I/O), an epoch trap for a WASM instance. Discarded | As above |
| **CPU limit** (`limits.cpu_ms`) | `502 cpu_limit` | Terminated as above. CPU is the thread's CPU time, so time spent waiting for I/O does not count; SQL work done on the task's behalf does | As above |
| **Memory limit** (`limits.memory_mb`) | `502 oom` | A V8 heap that needs to grow asks the memory governor in 8 MB steps; a refusal or the limit ends the task. WASM linear memory growth past the limit ends it. Resizable `ArrayBuffer` growth past the limit throws a `RangeError` the handler can catch. Discarded | As above |
| **Node memory is short** while the task grows | Same `502 oom`: under pressure the governor may refuse to grow a task before it reaches its own limit | Discarded | As above |
| **Response head already sent**, then a failure (exception in the stream, trap, kill, response over `limits.max_response_stream_bytes`, 100 MB by default) | The status cannot change. The body is **aborted**, so the client sees a truncated or reset stream, not a clean end | Discarded | The failure is in the logs and counted in `rt_tasks_total` by outcome |
| **Request body larger than `limits.max_body_bytes`** | `413 too_large` before the handler runs if `Content-Length` says so; otherwise the handler's body read fails with `request_too_large` | Normal | None |
| **An outbound request is refused** (SSRF guard, quota, size, time) | Nothing special: `fetch` rejects with a `TypeError` whose message is the code (`egress_denied`, `egress_quota`, ...). The handler decides what to answer | Normal | None |
| **A storage call fails** (`kv_value_too_large`, `sql_error`, `storage_quota`, ...) | Thrown (Deno) or returned (WASM) with a stable code. No host paths or driver text | Normal | The failed statement changed nothing; a transaction is rolled back if the handler lets it propagate |
| **The client disconnects** | While the request is still waiting for a slot, the place is freed at once. Once the task has started it runs on until it finishes or hits a limit | Normal | None |

A task that fails is never reused: warm reuse keeps a sandbox only after a clean finish (no throw, no kill, no limit, no
pending timer). One tenant's crash, loop or leak does not become the next request's problem.

??? info "Why a CPU hog still affects its neighbors a little"
    A running isolate cannot be paused partway through a time slice. A synchronous loop occupies its thread until the
    monitor terminates it at the CPU or wall limit. The scheduler moves the other work queued on that processor to
    other processors (and, in `min_latency`, hands the processor to a spare thread), so the effect on neighbors is
    bounded by detection time, not by the limit. WASM tasks yield at every slice and do not have this effect.

## Capacity and queues

| What happens | Caller sees | Detail |
|---|---|---|
| Service, tenant or node at its concurrency limit | The request **waits** in the service's FIFO queue | Not an error. `server-timing` `queue` shows the wait |
| Waited longer than `sched.queue_timeout` | `503 queue_timeout` + `Retry-After` | `Retry-After` is the estimated time for the queue to drain. Also applies to waiting for compiled code and for memory |
| Queue bounds full (`sched.max_queued`, `sched.max_queued_mb`, a service's `scaling.max_queued`) | `503 overloaded` + `Retry-After` | Refused at submit; no queue place was taken |
| Node in `shed` mode (critical memory) and the version is not in memory | `503 shed` + `Retry-After` | Versions already in memory keep being served |
| Node draining | `503 draining` | See [Shutdown and restart](#shutdown-and-restart) |
| A control change is being applied to the route table | `503 route_stale` + `Retry-After: 1` | A race, retry at once |
| Plan `rps` or `invocations_per_day` used up | `429 rate_limited`, `429 quota_exceeded` | These are chosen limits, not capacity |

## Memory pressure

All memory that grows with load is reserved against one budget. As use approaches the budget, the node reacts in steps
instead of failing at random:

| Level | Entered at | Reaction |
|---|---|---|
| Normal | below `memory.high` | Idle entries expire after `cache.idle_ttl` |
| High | `memory.high` | Evict in-memory code, drop idle reused instances and pre-created isolates, halve warm targets, cut services' concurrency limits |
| Critical | `memory.critical` | All of the above, halve limits again, warm targets to zero, enter `shed` mode, make new memory reservations wait (bounded by `queue_timeout`). Leaves `shed` after 5 s below `high` |

`rt_mem_pressure_level` (0, 1, 2) and `GET /v1/node/status` show the level. A task that cannot get memory waits, then
ends as `queue_timeout`; a running task whose heap cannot grow ends as `oom`. The node does not kill unrelated tasks.

## Builds and deploys

| What happens | Caller sees | Detail |
|---|---|---|
| Upload fails validation (does not parse, bad import specifier, unsafe archive, not a component, over size limit) | `422 validation_failed` with `details.diagnostics`, `422 invalid_artifact`, or `413 too_large` | Nothing is stored as a version; the live version is untouched |
| Eager build fails | The version is marked failed; it cannot go live (`409 version_not_ready` if you try) | The previous live version keeps serving. Read the build logs |
| Lazy build fails on the first request | `502 build_failed` | If the failed version was live, the node flips `live` back to the newest older version that is **ready** and records `build.failed` and `rollback.auto` events. If there is no older ready version, the service stays on the failed one and each request returns `502 build_failed` |
| Deploy while requests are in flight | None | The route table swap is atomic. Tasks already running finish on the version they started with; new tasks use the new live version |
| Rollback | None | One pointer flip, same atomic swap |
| Runtime upgrade with a new engine fingerprint | The first request per version waits for a rebuild | Compiled files in `cache/` are ignored, the source is kept in `artifacts/`, and `rt.lock` plus vendored modules make the rebuild reproduce the same dependencies without the CDN |
| Compiled cache file fails its MAC (tampering or a partial write) | Nothing, beyond a slower first request | The entry is deleted and rebuilt from source; a `security.artifact_tamper` event is recorded. A crash while rewriting the cache does not look like tampering |
| Builds per hour used up | `429 build_quota` on upload | Per-plan `builds_per_hour` |

## Storage and disk

| What happens | Effect |
|---|---|
| **A service's disk or quota is full** (SQLite reports a full database or the plan's `storage_mb` is reached) | Writes fail with `storage_quota`; reads keep working; deleting data frees space for new writes |
| **The data volume is full** | Service writes fail with `storage_quota` (SQLite's disk-full error is mapped to it). Writes to `meta.db` and `queue.db` fail with a generic database error: control API calls that write answer with an `internal` error code and enqueues fail. Routes, plans and compiled code for running services are held in memory, so reads of existing state and requests to already-loaded versions do not depend on free space |
| **An artifact fails its digest check, or the schema is newer than the binary** | The store reports a corrupt-data error (`internal` to API callers). Restore from backup, or run a binary that matches the data (see [Production deployment](../guides/production.md)) |
| **Compiled cache grows** | Bounded by `cache.t2_max_mb`; entries are evicted by cost |
| **Logs grow** | Bounded per service (`logs.max_mb_per_service`, `logs.retention_days`) and rate-limited per service; dropped records are counted in `rt_logs_dropped_total` |
| **A SQL statement runs too long** | Interrupted after `storage.sql_budget_ms` with `sql_timeout`; the writer is released |

!!! warning "Verify for your version"
    Disk-full behavior outside service databases is derived from reading the code, not from a fill-the-disk test.
    Before relying on it, fill a scratch volume on a test node and watch which calls fail, then free space and
    confirm the node recovers without a restart.

Durability: every database (`meta.db`, `queue.db` and each service's `data.db`) runs in WAL mode with
`synchronous=NORMAL`. A committed transaction survives a **process crash** or `kill -9`. After a **power loss or kernel
crash** the last transactions committed just before the failure may be lost (the database stays consistent, it rolls
back to an earlier commit). If that is not acceptable for your data, put the volume on storage with its own
guarantees and back up often. See [Storage](storage.md).

## Shutdown and restart

| Event | What happens |
|---|---|
| **`SIGTERM` or `SIGINT`** | The node enters drain mode: new requests get `503 draining`, `/readyz` turns `503`, and in-flight tasks get up to 30 seconds. It then stops the scheduler, closes listeners (open connections get up to 10 s), flushes usage counters, saves cache access frequencies and flushes logs |
| **`POST /v1/node/drain`** (`21ci node drain`) | The same drain without exiting. One-way: restart the process to serve again |
| **`kill -9`, OOM kill, power loss** | No graceful path. In-flight tasks are lost and their clients see a reset connection. Queue messages leased by unfinished tasks become visible again after their visibility timeout (the task's wall limit plus 10 s) and are **redelivered**, so queue handlers must be idempotent. Schedules resume at their next tick; ticks missed while down are skipped, not backfilled. Usage counters are flushed every 5 seconds, so up to that much recent usage may be missing; daily quota counters are re-seeded from the stored totals. The last batch of log records may be lost. Compiled and in-memory caches are gone and reload from `cache/` on demand |
| **Restart** | `meta.db`, `queue.db`, artifacts, service data, blobs and live pointers are intact. The route table is rebuilt from `meta.db` at startup. Services with effective `cache.preload` are loaded into memory most-used first. Warm isolates, instances and run queues start empty |
| **`SIGHUP`** | Re-reads `rt.toml`. Applies `[plans]` and the live keys (scheduler, admission bounds, memory watermarks), then refreshes the route and service tables. A file that fails to load or validate leaves the running configuration untouched. A new file that drops a plan tenants still use is refused entirely. Other changed keys are logged as needing a restart. If the new configuration took effect but the route refresh failed, the error says so |
| **A TCP internal listener** | Refused at startup unless `listeners.allow_tcp_internal = true` |

## Keys, secrets and configuration

| What happens | Effect |
|---|---|
| **No master key supplied** | The node generates `<data_dir>/master.key` (mode 0600) on first start and logs a warning. Back it up apart from the data |
| **The master key is malformed** | The node refuses to start (`invalid master.key`) |
| **The master key is lost or replaced by a different one** | Sealed secrets cannot be opened, API key hashes stop verifying (the pepper derives from the master key), and compiled-cache MACs fail so the cache rebuilds. Rotate with `RT_MASTER_KEY_PREVIOUS` and `rt secrets reseal`; never just swap the value. See [Security model](security.md#rotating-the-master-key) |
| **A secret cannot be read for a task** | The handler's `Deno.env` read throws `env_unavailable` rather than blocking the task |
| **Invalid `rt.toml` at startup** | The node does not start and reports the key; the configuration is validated before anything binds |
| **Release build without `RT_ROOT_TOKEN`** | The control API answers every route `503 control_disabled` |

## Schedules, queues and calls

| What happens | Effect |
|---|---|
| **A scheduled run fails** | The outcome is logged and counted. The schedule is not disabled and the next tick fires normally |
| **A scheduled run is slow** | Runs never overlap: ticks are skipped (counted in `rt_schedule_skipped_total`) while the previous run is still going |
| **A schedule's plan no longer allows the trigger** | The fire is dropped and counted in `rt_schedule_denied_total` |
| **A queue message's handler fails** | The message is retried with exponential backoff (`2 s x 2^(attempt-1)`, about 20 % jitter) up to `queue.max_attempts`, then moves to the service's dead-letter queue (`rt_queue_dlq_total`). You can redrive or purge the DLQ |
| **A queue batch is partly processed** | Form B handlers acknowledge or retry per message; a Form A or WASM handler acknowledges the whole batch on `2xx` and retries all of it otherwise |
| **A called service fails** | The caller receives a 5xx response, not an exception, and decides what to do. The child's wall limit is the smaller of its own and the caller's remaining time; chains stop at depth 15 (`508 call_depth`) |
| **The target does not allow the caller** | `403 call_not_allowed` (and `cross_tenant_call` across tenants) |
| **A service is deleted or its tenant disabled** | Routing stops at the next route swap. Tasks already running are not cancelled. Deleting a service also removes its data unless `keep_data` was requested, so a task still running may see its storage calls fail |

## What the platform does not retry for you

- **HTTP requests.** A request that fails is not retried. Retry in the client, and use `Idempotency-Key` where the
  control API supports it.
- **Handlers.** A handler that throws is not run again. Only queue deliveries are retried.
- **Side effects.** A task killed halfway may have made an outbound call or committed a write. Design handlers to be
  idempotent, and put the write and the "done" marker in one SQL transaction.
- **In-flight work across a crash.** Tasks are not persisted. Queue messages and schedules are.
- **Warm state.** Module-level state can vanish at any time ([Execution and isolation](execution.md)).

## Where to look

| Question | Look at |
|---|---|
| Which reason did a request get? | The `x-rt-reason` and `x-rt-task-id` headers, then `21ci logs <svc>` ([Logs, stats and usage](../guides/logs-and-usage.md)) |
| How often do tasks fail, and how? | `rt_tasks_total{outcome}`, `21ci stats <svc>` |
| Is the node short of memory? | `rt_mem_pressure_level`, `rt_mem_committed_bytes`, `GET /v1/node/status` |
| Is work queueing? | `rt_admission_queued`, `rt_admission_queue_wait_seconds`, `rt_admission_rejects_total`, `21ci node scheduler` |
| Did a deploy roll back by itself? | Service events: `build.failed`, `rollback.auto` (`21ci events <svc>`) |
| Is the cache being tampered with or corrupted? | Event `security.artifact_tamper` |
| Did a restart lose schedule ticks or queue messages? | `rt_schedule_skipped_total`, `rt_queue_depth`, `rt_queue_dlq_total` |

## See also

- [Request lifecycle](request-lifecycle.md): where each failure arises in the path of a request.
- [Execution and isolation](execution.md): limits and how a sandbox is killed.
- [Scheduler and autoscaling](scheduler.md): admission, queues, degradation modes.
- [Storage](storage.md): the engine and its durability settings.
- [Production deployment](../guides/production.md): backups, upgrades and incident runbook.
