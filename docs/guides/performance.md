---
title: Performance tuning
description: What dominates latency for Deno and WASM services, and the profiles, warm pools, queue and memory knobs that change it.
---

# Performance tuning <span class="rt-badge rt-badge--operator">operator</span>

This guide lists the knobs that change where a request's time goes, and what each change costs in memory or
isolation. It is for operators tuning a node and for developers who want to know why their service is fast or slow.
The cost model behind it is in [Performance characteristics](../concepts/performance.md); how to size a node and
measure the cost of your own services is in [Resource requirements](resources.md).

## Where the time goes

Every request passes the same phases, and every response reports them in the `server-timing` header:

```mermaid
flowchart LR
  A["queue: wait for a run slot and a processor"] --> B["wait_runtime: build or load the code"]
  B --> C["init: prepare isolate / instance"]
  C --> D["exec: run the handler"]
```

```console
$ curl -sS -o /dev/null -D - "$RT_INVOKE/s/hello/" | grep -i server-timing
server-timing: queue;dur=0.004, wait_runtime;dur=0.000, init;dur=0.120, exec;dur=0.310
```

(Example values, in milliseconds.) Read it like this:

| Phase | What it measures | Usually large because |
|---|---|---|
| `queue` | From admission to start: waiting for a run slot (the service's concurrency limit, the plan's tenant cap, `sched.max_inflight`) and for a free processor | Load above capacity; a low `scaling.max_concurrency`; a plan `max_concurrency` cap |
| `wait_runtime` | Waiting for the version's compiled code: a build (first request after a lazy deploy or an upgrade) or a load from disk into memory | Cold version, see [First-request cost](#first-request-cost) |
| `init` | Preparing the isolate or WASM instance for this task | Deno without a warm isolate |
| `exec` | Your handler running | Your code, its `fetch` calls and storage queries |

`21ci stats <svc>` (or `GET /v1/services/{s}/stats`) shows p50/p95/p99 per phase over 5 minutes, 1 hour or
24 hours, without Prometheus.

### Deno: the isolate is the cost

By default each Deno request runs in a fresh V8 isolate, so no state leaks between requests. Creating one costs CPU
on every request (read the histogram `rt_deno_isolate_create_seconds` on your hardware). The pre-created pool
(`deno.isolate_pool_per_p`) only moves that cost off the request path: the CPU is still spent, so the per-core
throughput of a trivial service is bounded by it in the default configuration.

Warm reuse removes the cost: a cleanly finished isolate serves the next request of the same service version. The
`_count` of `rt_deno_isolate_create_seconds` then stays small compared with the request count. See
[Warm reuse](#warm-reuse) for what that changes for your code.

### WASM: instantiation is cheap

A WASM component is compiled once, ahead of time, to native code, and each request gets an instance from a
pre-sized pooling allocator. That is cheap, and it does not grow with the size of your handler. For WASM services
the handler itself, its start code (large for components that embed a language runtime) and its host calls
dominate.

## Profiles

A profile is a bundle of defaults. `node.default_profile` picks one for the node; a service can pick its own with
`profile` (`max_density`, `balanced`, `min_latency`).

| Profile | Pick it when |
|---|---|
| `max_density` | Many mostly idle services per node; memory matters more than tail latency |
| `balanced` (default) | General purpose |
| `min_latency` | Few hot services; you trade memory for latency and accept warm state in isolates |

The knobs that differ (from `config/schema.json`):

| Key | Scope | `max_density` | `balanced` | `min_latency` |
|---|---|---|---|---|
| `sched.spin_us` | node | 0 | 10 | 50 |
| `sched.time_slice` | node | 20 ms | 10 ms | 5 ms |
| `sched.pin_threads` | node | false | false | true |
| `sched.handoff_on_hog` | node | false | false | true |
| `sched.queue_timeout` | service | 10 s | 5 s | 1 s |
| `sched.warm_share` | node | 0.05 | 0.15 | 0.3 |
| `sysmon.max_tick` | node | 20 ms | 10 ms | 2 ms |
| `rebalance.interval` | node | 250 ms | 100 ms | 50 ms |
| `memory.high` / `memory.critical` | node | 0.90 / 0.97 | 0.80 / 0.92 | 0.70 / 0.88 |
| `memory.purge_min_interval` | node | 5 s | 30 s | 120 s |
| `cache.t1_share` | node | 0.10 | 0.20 | 0.35 |
| `cache.idle_ttl` | service | 60 s | 600 s | 3,600 s |
| `cache.preload` | service | false | false | true |
| `deno.max_bound_per_m` | node | 128 | 64 | 32 |
| `deno.isolate_pool_per_p` | node | 0 | 1 | 4 |
| `deno.reuse_isolates` | service | false | false | true |
| `wasm.linear_memory_keep_resident` (MB) | node | 0 | 1 | 8 |
| `wasm.reuse_instances` | service | false | false | true |
| `wasm.max_unused_warm_slots` | node | 0 | 100 | 1000 |
| `build.compile` | service | lazy | lazy | eager |

The autoscaler has four more profile constants with no config key: the per-service concurrency ceiling
(256 / 1,024 / 1,024, replaced by an explicit `scaling.max_instances` or the plan's `max_instances`), the start and minimum concurrency limit
(4 and 1 / 16 and 2 / 32 and 4), the latency tolerance before it shrinks a service's limit (300 % / 200 % / 150 % of
its baseline), and the warm-instance headroom and maximum per processor (100 % and 1 / 125 % and 4 / 150 % and 16).

!!! warning "Explicit keys in `rt.toml` beat every profile"
    A profile fills only keys you left unset. The files in `config/defconfigs/` (and `make defconfig`) state every
    key, so with such a file a service's `profile` and a runtime switch of `default_profile` no longer change those
    keys: the file pins them. Keep only the keys you mean to pin: `make savedefconfig CONFIG=rt.toml` saves just the
    non-default values (to `.config`). `GET /v1/services/{s}` shows each effective value with its source
    (`service`, `config`, `profile:<name>`, `plan`, `default`).

## Warm reuse

Three knobs keep work warm between requests:

| Key | Scope | What it keeps |
|---|---|---|
| `deno.isolate_pool_per_p` | node | N blank isolates per processor, built between tasks and when idle. Single-use: they hold only the base snapshot, no tenant state. Dropped under memory pressure |
| `deno.reuse_isolates` | service | An isolate that finished a request cleanly, with your evaluated module, for the next request of the same service version on the same machine |
| `wasm.reuse_instances` | service | A WASM instance that finished cleanly, with its linear memory, for the next request of the same service version on the same processor |

Turn reuse on for the whole node in `rt.toml`, or per service in its `deno` / `wasm` block:

=== "21ci"

    ```bash
    21ci service update hello --reuse-isolates true       # Deno service
    21ci service update edge --reuse-instances true       # WASM service
    ```

=== "curl"

    ```bash
    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"deno": {"reuse_isolates": true}}' "$RT_API/v1/services/svc_01J…"
    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"wasm": {"reuse_instances": true}}' "$RT_API/v1/services/svc_01J…"
    ```

=== "rt.toml"

    ```toml
    [deno]
    reuse_isolates = true
    isolate_pool_per_p = 4

    [wasm]
    reuse_instances = true
    ```

A service's own setting wins over the node's, which wins over the profile.

### What reuse changes for your code

Reuse never crosses tenants, services or versions, even for identical code. Within one service version it changes
semantics:

- **Module state persists.** Top-level code runs once per isolate, and globals survive between requests. Data one
  request leaves in a global is visible to the next request, possibly from another user. Do not keep per-request or
  per-user data in module scope.
- **It is a cache, not storage.** Requests land on different isolates, and any isolate may be discarded at any time.
  Keep state in `std:kv`, `std:sql` or `std:blob`.
- **Only clean finishes are kept.** An isolate is reused only if the handler, the response body and every
  `waitUntil` succeeded, no timer or pending operation is left, the task was not killed, and its memory stayed
  within ¾ of `memory_mb` and did not grow more than max(16 MiB, 50 %) beyond what it held after its first request.
  It is replaced after `deno.reuse_max_uses` (1,000) requests and disposed after `deno.reuse_idle_ttl` (60 s) idle.
- **Env and secrets are read per request.** If your code read the environment and it changed since (a rotated
  secret), the isolate is discarded.
- **WASM**: linear memory (statics, allocator state, caches) persists the same way. An instance is dropped after a
  trap, kill or timeout, after 1,000 requests, and when the service's environment changed.
- **Memory**: kept isolates and instances hold memory (`rt_mem_committed_bytes{category="deno_heap"}` and
  `{category="wasm_reuse"}`). They are dropped first when memory pressure reaches `High`.

How many are kept: the autoscaler publishes a warm target per service from recent demand, capped by
`sched.warm_share` of the memory budget. Without a target, Deno keeps up to `deno.reuse_max_idle` (4) per service
version and machine, `deno.reuse_max_idle_total` (16) per machine overall; WASM up to `wasm.reuse_max_idle` (8) per
service and processor.

## First-request cost

A version moves through cache tiers: source (T3) is compiled into native code or a V8 code cache on disk (T2), which
is loaded into memory (T1). A request that finds its version cold waits in `wait_runtime`. See
[Caching](../concepts/caching.md) for the mechanism.

| Situation | What helps |
|---|---|
| First request after a deploy (lazy build) | Deploy with eager compile: the build runs at upload and the version goes live only when it is ready. Per upload `?compile=eager` (`21ci push … --wait`), per service or node `build.compile = "eager"` (on in `min_latency`) |
| First request after a restart | Nothing to do for most services: compiled code on disk survives restarts, so this is a load, not a build. With `cache.preload` (on in `min_latency`) the node loads the `cache.preload_top_n` (100) most used versions at startup |
| First request after an idle period | The in-memory entry was evicted after `cache.idle_ttl` (600 s in `balanced`). Raise it per profile, or give the cache more room with `cache.t1_share` |
| First request after an upgrade | A new engine fingerprint invalidates compiled code; versions rebuild lazily. Rebuild hot ones ahead of traffic: `21ci rebuild <svc> <N>` |

A Deno build also fetches new npm/jsr dependencies. They are cached on the node, stored with the version's artifacts
and pinned by hash in its `rt.lock`, so a rebuild after an upgrade does not depend on the CDN.

## Queueing and timeouts

The node does not answer `429` because capacity is busy: requests over capacity wait in a per-service FIFO queue,
fair across tenants. A request runs when it gets a run slot, which needs room in three limits:

1. the service's **adaptive concurrency limit**: it starts at the profile's start value, grows while requests wait
   and latency holds, and shrinks on latency regressions, memory pressure or saturated processors. An explicit
   `scaling.max_instances` on the service (at most the plan's `max_instances`) is a hard cap, and `scaling.min_instances` a floor;
2. the tenant's plan **`max_concurrency`** (in-flight tasks across all its services; 0 = no cap);
3. the node's **`sched.max_inflight`** (10,000).

Waiting is bounded twice:

- **Room to wait**: `sched.max_queued` tasks (0 = 4 × `max_inflight`) and `sched.max_queued_mb` bytes (0 = 1/16 of
  the memory budget) node-wide; one service and one tenant may each hold half. A service's `scaling.max_queued` lowers
  its share. Over it: `503 overloaded`.
- **Time to wait**: `sched.queue_timeout` (1 s / 5 s / 10 s by profile). A request that does not start in time ends
  with `503 queue_timeout`. The same timeout covers waiting for code and for memory.

Both carry a `Retry-After` computed from the recent completion rate (1 to 60 s, with jitter). A client that
disconnects while waiting leaves the queue at once.

The trade-off: a short `queue_timeout` fails fast and keeps tail latency bounded, but turns bursts into 503s; a long
one absorbs bursts and lets latency grow. For interactive APIs behind clients that retry, `min_latency`'s 1 s is a
reasonable start; for webhooks and batch work, 5 to 10 s.

Watch `rt_admission_queued`, `rt_admission_queue_wait_seconds` and, with per-service metrics on,
`rt_service_concurrency_limit` and `rt_service_queued` ([Metrics](../reference/metrics.md#admission-and-autoscaling)).

## Memory budget and pressure

All memory that scales with load is reserved against one budget, `memory.budget_mb` (0 = 85 % of the container's
memory limit). Two watermarks, as fractions of the budget, drive a pressure ladder:

| Level | When | What the node does |
|---|---|---|
| Normal | below `memory.high` | Idle entries expire after `cache.idle_ttl` |
| High | at `memory.high` (0.80 balanced) | Evicts in-memory code down to `high` − 10 %, drops idle reused WASM instances and the pre-created isolate pools, halves warm targets, cuts services' concurrency limits by 25 % |
| Critical | at `memory.critical` (0.92 balanced) | All of the above, halves concurrency limits, warm targets go to zero, the node enters `shed` mode (requests for versions not already in memory get `503 shed`), new memory reservations wait (bounded by `queue_timeout`). It leaves `shed` after 5 s below `high` |

`rt_mem_pressure_level` (0, 1, 2) and `GET /v1/node/status` show the level. Lower watermarks (`min_latency`) react
earlier and keep headroom for latency; higher ones (`max_density`) use more of the budget before shedding.

!!! note "WASM pool size"
    The WASM pooling allocator is sized at startup: slots = (memory budget ÷ 4) ÷ the largest
    `limits_max.memory_mb` of any plan (at least 128 MB), at most 10,000. Every running or idle WASM instance holds a
    slot, and `rt_wasm_pool_slots` shows the count. On a 2 GiB container that is (1,740 MB ÷ 4) ÷ 128 MB = 3 slots.
    If WASM services keep many requests in flight on a small node, give it more memory or keep plan memory maxima
    tight.

## Processors per runtime kind

The scheduler runs a fixed set of processors (`sched.processors`, 0 = CPU quota or cores), each serving one runtime
kind. `sched.kinds.deno` and `sched.kinds.wasm` set a `min` and `max` per kind (`max = 0`: processors minus the other
kinds' minimums). Every `rebalance.interval`, a processor moves to the busier kind when that kind's load per
processor exceeds `rebalance.up_threshold` (2.0) and the other kind has an idle processor above its minimum, at most
once per `rebalance.cooldown` (1 s). A kind with no live services has an effective minimum of 0.

Pin the split only when you know your mix, for example to reserve processors for a latency-sensitive WASM fleet:

```toml
[sched.kinds.wasm]
min = 2
```

Builds run on their own processors (`build.threads`, default 1) at low priority, so compiling a large component does
not take CPU from running services.

Deno code cannot be preempted in the middle of JavaScript. A task that runs past `sched.time_slice` is a hog: the
monitor moves the other work queued on its processor elsewhere, and with `sched.handoff_on_hog` (on in `min_latency`)
hands the processor to a spare thread while the hog finishes. WASM tasks yield at every slice.

## Storage: the blocking pool

KV, SQL and blob calls run on a separate thread pool, `blocking.threads` (0 = max(4, cores)), with per-tenant FIFO
queues served round-robin. Under contention one tenant may occupy `floor(blocking_share × threads)` threads, at least
one (`blocking_share` is a plan key, default 0.25), so a tenant's heavy SQL cannot starve the others' KV reads. The
pool is work-conserving: while threads are idle, a tenant may use more than its share, always leaving one thread free
for an arriving tenant.

A node with few, storage-heavy tenants may need a higher `blocking_share` in the plan or more `blocking.threads`; watch
the service's `exec` phase and the pool's queueing before changing either.

Other storage knobs, all restart-only:

| Key | Default | Effect |
|---|---|---|
| `storage.readers_per_db` | 4 | Read-only connections per service database, opened under concurrent reads, so reads do not queue behind writes |
| `storage.max_open_dbs` | 256 | All service-database connections together; the least recently used idle service is closed when it is reached |
| `storage.sql_cache_mb` | 512 | SQLite page cache of all connections together, split evenly |
| `storage.sql_budget_ms` | 1,000 | Per-statement time budget; over it the statement fails with `sql_timeout` |

Host-side work is charged: SQL thread time counts against the task's CPU limit.

## Live tuning

Two ways to change performance knobs without a restart:

- **`SIGHUP`** re-reads `rt.toml` and applies the live keys: `sched.spin_us`, `sched.time_slice`,
  `sched.handoff_on_hog`, `sched.max_inflight`, `sched.max_queued`, `sched.max_queued_mb`, `sched.queue_timeout`,
  `sysmon.max_tick`, `rebalance.interval`, `deno.max_bound_per_m`, `memory.high`, `memory.critical` (and the
  `[plans]`). See [Production deployment](production.md#reloading-configuration-sighup).
- **`PUT /v1/node/config`** switches `default_profile` (the profile's live keys apply at once, except keys your
  `rt.toml` sets explicitly) and `degradation_mode`: `shed` serves only versions already in memory, `drain` stops
  taking work for good, `normal` returns from `shed`.

=== "21ci"

    ```bash
    21ci node config set default_profile=min_latency
    21ci node config set degradation.mode=shed
    ```

=== "curl"

    ```bash
    curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"default_profile": "min_latency"}' "$RT_API/v1/node/config"
    # {"applied":["default_profile"],"pending_restart":[]}
    ```

Pool sizes, processor counts, cache shares and `sched.warm_share` need a restart.

## Benchmark your own node

Most "the runtime falls over at N requests per second" reports measure something else, and a number from a laptop, a
debug build or a shared machine says nothing about capacity. Before you trust a number:

- [ ] **Release build.** `make release` (`target/release/rt`) or the Docker image. `make dev` is a debug build on
      `config/defconfigs/dev.toml`, which also caps `sched.max_inflight` at 100, turns off the isolate pool and uses
      one blocking thread.
- [ ] **A plan without quotas.** The built-in `free` plan allows 50 requests per second and 100,000 a day per
      tenant, and 10 tasks in flight: beyond those you measure `429 rate_limited` or queueing, not the runtime. Use a
      plan with `rps = 0` and `invocations_per_day = 0` (0 = unlimited) and a high `max_concurrency`.
- [ ] **Enough connections.** One load generator is one client IP: raise `ingress.max_conns_per_ip` (256), and for
      bursts `ingress.backlog` plus the kernel's `somaxconn` (`kern.ipc.somaxconn` on macOS, 128 by default).
- [ ] **Check the status codes**, not only requests per second. A fast stream of 429s or 503s is not throughput.
- [ ] **Warm up**, then measure. The first requests build and load the code.
- [ ] **Separate machines** for the node and the load generator when you can; on one machine they compete for CPU.

The runtime repository has a ready setup in `bench/`: `bench/rt.toml` is a private node with an unlimited plan,

```toml title="bench/rt.toml (excerpt)"
[ingress]
max_conns_per_ip = 4096

[plans.bench]
rps = 0
invocations_per_day = 0
max_concurrency = 100000
max_services = 100
```

`bench/rt-warm.toml` adds `deno.reuse_isolates`, `deno.isolate_pool_per_p = 4` and `wasm.reuse_instances`, and
`bench/bench.sh` starts a node, deploys three services and runs [oha](https://github.com/hatoo/oha) against them:

```bash
make release && make fixtures
bench/bench.sh target/release/rt mine                                   # bench/out/mine.txt
BENCH_CONFIG=bench/rt-warm.toml bench/bench.sh target/release/rt warm
```

For your own service, the same idea with oha directly:

```bash
oha --no-tui -z 30s -c 64 "$RT_INVOKE/s/hello/"     # look at "Status code distribution"
```

While it runs, watch `rt_admission_queue_wait_seconds`, `rt_mem_pressure_level`,
`rt_deno_isolate_create_seconds_count` (how many isolates were created) and the `server-timing` phases of a few
requests. [Resource requirements](resources.md) turns these measurements into per-service costs and a capacity plan.

## See also

- [Performance characteristics](../concepts/performance.md): what costs what, the model behind these knobs.
- [Resource requirements](resources.md): sizing a node and measuring per-service cost.
- [Scheduler](../concepts/scheduler.md) and [Execution](../concepts/execution.md): the mechanisms behind these knobs.
- [Configuration reference](../config.md#profile-differences-at-a-glance): every key with its profile defaults.
- [Limits and quotas](../reference/limits.md): per-task limits and plan quotas.
- [Metrics](../reference/metrics.md): what to watch while tuning.
