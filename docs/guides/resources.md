---
title: Resource requirements
description: What a node needs in CPU, memory and disk, what each service and each sub-runtime (Deno isolates and WASM instances) costs, the formulas that bound them, and how to measure them on your own hardware.
---

# Resource requirements <span class="rt-badge rt-badge--operator">operator</span>

This page answers three questions: what a node needs before it serves anything, what a service costs while it is idle,
warm and running, and how to find out for your workload. It gives **rules and formulas** built from the configuration
and the mechanisms, and a **method** for measuring. Numbers depend on hardware, the profile, the code you host and the
traffic mix, so the measured values are collected in one table at the [end of the page](#measurements).

For the model behind the costs read [Performance characteristics](../concepts/performance.md); for the knobs, [Performance
tuning](performance.md).

## Base requirements of a node

| | Requirement | Why |
|---|---|---|
| OS and architecture | Linux, x86_64 or arm64, glibc. The container image is `debian:bookworm-slim` | The prebuilt V8 needs glibc. macOS works for development builds only |
| Process model | One `rt` process, one data directory (`node.data_dir`, `/data` in the image) | Everything is in the one process; see [Architecture](../concepts/architecture.md) |
| CPU | At least 2 cores recommended. `sched.processors` defaults to the cgroup CPU quota, else the number of cores | Beyond the scheduler's processors the node runs 2 I/O threads (`io.threads`), the build thread, a blocking pool of `max(4, cores)` threads, `sysmon` and an autoscaler thread. On one core they all time-slice with your tenants |
| Memory | A container or cgroup limit. The node's budget is **85 % of it** (`memory.budget_mb = 0`), else 85 % of physical RAM | Everything that scales with load is reserved against the budget; the other 15 % is for what the budget does not govern (binaries, V8 and wasmtime code, thread stacks, allocator overhead) |
| Disk | One filesystem for the data directory, local and fast enough for SQLite. Size it with the [disk formula](#disk) | All state is SQLite files, artifacts, blobs, logs and the compiled-code cache |
| File descriptors | A generous `nofile` limit (65,536 is a sensible start) | Every connection, every open service database and its readers, and every log segment uses descriptors. `storage.max_open_dbs` (256) and the connection limits bound them but are not the whole story |
| Network | Inbound on the public listener only. Outbound HTTPS from the node for builds (`build.npm_cdn` and `build.allowed_hosts`) and from tenant code through the egress guard | Control and metrics listeners stay on loopback or a VPN |
| Time | A synchronized clock (NTP) | Schedules, quotas per day, TLS at your proxy, key expiry |

### Memory budget and what is carved from it

The budget `B` is shared by:

| Consumer | Share of `B` | Config |
|---|---|---|
| In-memory compiled code (T1 cache) | `cache.t1_share` (0.10 / 0.20 / 0.35 for `max_density` / `balanced` / `min_latency`) | [Caching](../concepts/caching.md) |
| Warm instances (kept isolates, idle WASM instances, pre-created isolates) | At most `sched.warm_share` (0.05 / 0.15 / 0.30) | [Scheduler and autoscaling](../concepts/scheduler.md) |
| SQLite page caches of open service databases | Up to `storage.sql_cache_mb` (512 MB) | `storage.max_open_dbs`, `storage.sql_cache_mb` |
| Running tasks | Whatever is left: each task reserves `deno.isolate_base_mb` (4 MB) at start and grows toward `limits.memory_mb`, or holds WASM linear memory up to `limits.memory_mb` | Per-task limits, plan `limits_max` |
| Logs | The in-memory ring (`logs.ring_lines`, 1,000 lines per service) | Small |

When use reaches `memory.high` (0.70 / 0.80 / 0.90 of `B` by profile) the node starts evicting and shrinking; at
`memory.critical` it sheds ([Failure behavior](../concepts/failure-behavior.md#memory-pressure)). A node that sits at
the high watermark under normal load is undersized for its workload.

!!! warning "The WASM concurrency ceiling is set by `B` and your largest plan memory limit"
    The WASM pooling allocator is sized at startup. Slots = `(B / 4) / max(128 MB, largest limits_max.memory_mb of any plan)`,
    at most 10,000. Every running **or idle** WASM instance holds a slot, and the metric `rt_wasm_pool_slots` shows the
    count. A 2 GiB container has `B` = 1,740 MB, so with a 128 MB plan maximum there are 3 slots: only three WASM
    instances can exist at once. To run more concurrent WASM instances, raise the memory limit or keep plan memory
    maxima tight. This is the single most surprising sizing rule, so check it before you size a WASM-heavy node.

### Disk

```text
disk >= tenant data + T2 cache + artifacts + logs + metadata + headroom

tenant data  = sum over tenants of their used storage (plan storage_mb is the ceiling per tenant)
T2 cache     <= cache.t2_max_mb (4,096 MB by default); garbage-collected below disk.low_free_mb
artifacts    ~ per service: retained versions (deploy.retain_versions, 10) x source size
               (<= 10 MB per Deno archive, <= 50 MB per WASM component), plus vendored dependency modules
logs         <= logs.max_mb_per_service (50 MB) x services, for logs.retention_days (7)
metadata     meta.db and queue.db: grows with services, versions, events, usage and queued messages
headroom     SQLite WAL growth, backups taken on the same disk, and room to rebuild after an upgrade
```

The compiled-code cache is the only part that can always be deleted and rebuilt. Plan for backups to live elsewhere
([Production deployment](production.md#backups)). Put the data directory on a filesystem where a full disk is
monitored: see [Failure behavior](../concepts/failure-behavior.md#storage-and-disk).

### Starting points

These are starting points for a first node, derived from the rules above, not measurements. Replace them with the
numbers from [the method below](#measure-it-on-your-hardware).

| Node | When | CPU and memory | Notes |
|---|---|---|---|
| Minimal edge box | A device with 1 vCPU and 1 GB | The repository ships `config/defconfigs/edge-1gb.toml`: one processor, one I/O and one blocking thread, a 768 MB memory budget | The smallest configuration the project provides. Expect little concurrency; check the WASM slot rule, which gives very few slots at this size |
| Personal or trial | One developer, a handful of mostly idle services | 2 vCPU and enough memory that `B` covers the WASM pool rule and a few warm isolates | The built-in `free` plan is sized for this |
| Team platform | Tens of tenants, hundreds of services, mostly Deno | 4 or more vCPU; memory from the formulas below with margin for the high watermark | Choose `balanced`; watch `rt_mem_pressure_level` |
| Hot path or high density | A few hot services, or thousands of registered services | More cores for hot services; for density, memory for T1 and warm instances | `min_latency` for the former, `max_density` for the latter |

## What a service costs

A service has several states with different costs. "Resident" means memory the node holds while the state lasts.

| State | Deno service | WASM service |
|---|---|---|
| **Registered, idle** (no traffic, not in the cache) | A row in `meta.db`, source in `artifacts/` and, once built, a compiled bundle in T2 on disk. No RAM beyond a route-table entry | Same, with a precompiled `.cwasm` in T2 once built. No RAM beyond a route-table entry |
| **Cached (T1)** | The prepared bundle plus its V8 code cache, in RAM until evicted by pressure or `cache.idle_ttl`. Grows with bundle size | The pre-linked component (`InstancePre`); compiled code is memory-mapped, so pages count only when touched. Grows with component size |
| **Warm, waiting** | A kept isolate: V8 heap after its last task (kept only if within three quarters of `memory_mb`), up to `deno.reuse_max_idle` per version per machine and `deno.reuse_max_idle_total` per machine; pre-created isolates hold only the base snapshot (`deno.isolate_base_mb`) | An idle instance holds a pool slot and `wasm.linear_memory_keep_resident` MB of resident linear memory, up to `wasm.reuse_max_idle` per processor |
| **Running a task** | `deno.isolate_base_mb` at start, growing in 8 MB steps toward `limits.memory_mb` (default 128 MB). Counted as `deno_heap` | Linear memory up to `limits.memory_mb`, in one pool slot. Counted as `wasm_linear` |
| **Per-request CPU** | Isolate creation (skipped by reuse), module evaluation (skipped by reuse), the handler, GC | Instantiation, the component's start code (skipped by reuse), the handler. Epoch ticks cost little |

What to take from the table:

- **Idle services are cheap.** The cost of having many services is disk and metadata, not RAM. What costs RAM is
  *concurrently active* services.
- **The Deno cost per running task is dominated by heap**, which your code decides. A handler that allocates 20 MB holds
  20 MB while it runs, plus the base. Concurrent tasks multiply it.
- **The WASM cost per running task is the instance's linear memory**, which depends on the language runtime: a small
  Rust component may use a little over its data segments, a component with an embedded interpreter starts with
  megabytes. The ceiling is the slot count, not the sum of actual use.
- **Warm state trades memory for latency.** It is bounded per service by `deno.reuse_*` and `wasm.reuse_max_idle`, and
  per node by `sched.warm_share`; the autoscaler trims it by demand and at memory pressure.

### Bounds you can set

| Cost | Bound | Where |
|---|---|---|
| Memory per running task | `limits.memory_mb` | Service, capped by plan `limits_max.memory_mb` |
| Concurrent tasks per service | `scaling.max_instances`, else the plan's `max_instances` or the adaptive limit | Service, capped by plan `max_instances` |
| Warm instances per service | `scaling.min_instances` | Service, capped by plan `max_instances` |
| Concurrent tasks per tenant | `max_concurrency` | Plan |
| Concurrent tasks per node | `sched.max_inflight` (10,000) | Node |
| Concurrent WASM instances | Pool slots (rule above) | Derived from `memory.budget_mb` and plans |
| Warm instances | `sched.warm_share` of `B`, `deno.reuse_*`, `wasm.reuse_max_idle` | Node |
| Cached code | `cache.t1_share`, `cache.idle_ttl`; disk `cache.t2_max_mb` | Node, service |
| Queued requests | `sched.max_queued`, `sched.max_queued_mb` | Node |
| Storage per tenant | `storage_mb` | Plan |
| CPU per task | `limits.cpu_ms` | Service, capped by plan |

## A capacity worksheet

Fill the left column from your plans and the right from measurements:

```text
1. Budget:          B = 0.85 x container memory limit
2. Fixed:           baseline RSS of an idle node                          (measure: idle RSS)
3. Caches:          T1 = cache.t1_share x B ; SQL cache <= storage.sql_cache_mb
4. Warm:            W <= sched.warm_share x B
5. Active Deno:     A_deno x (isolate base + typical heap)                (measure: per running task)
6. Active WASM:     A_wasm x (typical linear memory) ; A_wasm <= slots
7. Check:           2 + 3 + 4 + 5 + 6 <= memory.high x B  at your peak concurrency
8. CPU:             requests/s x CPU-seconds per request <= cores x target utilization
                    (CPU per request: measure per service and runtime)
9. Disk:            use the disk formula above
```

Peak concurrency follows from Little's law: tasks in flight is roughly the arrival rate times the average task
duration. A service that mostly awaits slow upstreams has a long duration and so many tasks in flight for little CPU:
size memory for the concurrency, not for the CPU.

## Measure it on your hardware

All measurements use the node's own metrics and the operating system. Do them on a release build of the version you will
run, on hardware like production, and never against a debug build or a node that shares a machine with the load
generator ([Benchmark your own node](performance.md#benchmark-your-own-node) lists the traps).

### What to read

| Quantity | Where |
|---|---|
| Process memory (RSS) | `ps -o rss= -p $(pidof rt)`; `docker stats --no-stream`; the cgroup file `memory.current`; the metric `rt_mem_rss_bytes` (0 on macOS) |
| Governed memory by use | `rt_mem_committed_bytes{category}`: `deno_heap`, `wasm_linear`, `wasm_reuse`, `t1_cache`, `sql_cache`, `logs`, `other` |
| Budget and pressure | `rt_mem_budget_bytes`, `rt_mem_pressure_level`, `21ci node status` |
| Tasks in flight | `rt_inflight_tasks{kind}`, `rt_admission_running`, `rt_admission_queued` |
| Isolate and instance creation | `rt_deno_isolate_create_seconds` (histogram; its `_count` is the number of isolates created), `rt_wasm_instantiate_seconds`, `rt_wasm_pool_slots` |
| Time per phase | `rt_task_phase_seconds{kind,phase}`; the `server-timing` header; `21ci stats <svc>` |
| Cache size | `rt_cache_bytes{tier}`, `rt_cache_entries{tier}`, `21ci node cache` |
| CPU of the process | `docker stats` CPU %, `top -p`, or the cgroup `cpu.stat` counter `usage_usec` |
| CPU per tenant task | `21ci tenant usage`: `invocations` and `cpu_ms` per service and day; CPU per request = `cpu_ms / invocations` |
| Disk | `du -sh` on `/data` and its subdirectories: `services/`, `blobs/`, `artifacts/`, `cache/`, `logs/` |

### Protocol

1. **Idle baseline.** Start the node with your production `rt.toml` and no tenants. Wait a minute. Record RSS and
   `rt_mem_committed_bytes`. This is the fixed cost of the node.
2. **Registered cost.** Create N copies of a hello service (for example 1,000) without calling them. Record RSS and the
   disk usage of `/data`. The difference per service is the idle cost (expected: disk, almost no RAM).
3. **Cached cost.** Call each copy once. Record RSS, `t1_cache` and `cache` sizes. The difference per service is the
   cost of a cached version.
4. **Running cost.** Hold a constant number of concurrent requests (for example 16 and 64) against one service with a
   load generator on **another machine**, with a plan that has no `rps` limit. Record RSS and `rt_mem_committed_bytes`
   at steady state; divide the increase over the cached state by the concurrency. Repeat for Deno with reuse off and on,
   and for WASM with reuse off and on.
5. **CPU per request.** During step 4, record the process CPU time over a fixed interval and the number of requests
   (`rt_tasks_total` or the load generator's count). CPU per request = CPU seconds divided by requests. Cross-check with
   `21ci tenant usage` `cpu_ms` per invocation, which counts only task CPU.
6. **Your real service.** Repeat 3 to 5 with your actual handler and a representative payload; the handler's heap and CPU
   usually dominate the platform's fixed per-request cost.
7. **Repeat under pressure.** Raise concurrency until `rt_mem_pressure_level` leaves 0 and note the point; that is the
   capacity at which the node starts evicting.

Record each result with its conditions: hardware, profile, reuse settings, payload size and the runtime version. A number
without conditions cannot be compared.

### Factors that move the numbers

- **Profile.** `max_density` keeps almost nothing warm; `min_latency` keeps pools and reused sandboxes.
- **Warm reuse.** Raises resident memory per service and lowers CPU per request.
- **Code size.** Bundle and component size raise T1 and T2 use and start-up cost.
- **Handler behavior.** Allocation, buffering of bodies and the number of concurrent awaits set heap and concurrency.
- **Language runtime in WASM.** An embedded interpreter changes both memory and start code.
- **Storage use.** Open service databases hold SQLite caches until idle services are closed.
- **Hardware.** Core speed and memory bandwidth change CPU per request; storage speed changes SQL latency.
- **Node count of tenants with plans at their maxima.** The WASM slot rule and the memory budget are computed from the
  largest plan, not the average.

## Measurements

### Measured values

Release build, MacBook Air M4 (10 cores), `balanced` profile, `bench/resources.sh` in the runtime repo. The values
are for the `hello` handlers. Your own code adds its heap or linear memory on top.

| Metric | Deno | WASM | How measured |
|---|---|---|---|
| Node idle RSS (no tenants) | 33 MB (shared) | 33 MB (shared) | RSS after start |
| Running instance, `hello` held in flight | ≈ 2.9 MB | below resolution (< 0.5 MB) | 100 requests in flight, RSS delta |
| Warm instance kept with reuse on | ≈ 1 MB | below resolution | min_instances pinned, RSS delta / warm target |
| CPU time per request, reuse on (whole node, incl. HTTP) | ≈ 360 µs | ≈ 140 µs | process CPU over 20k / 50k requests at c=8 |

WASM instances live in pooling-allocator slots that are reserved up front (`rt_wasm_pool_slots`), so creating one
barely moves RSS: its cost is the linear memory the guest touches. Deno numbers grow with module size and with the
heap the handler keeps. Measure your own service with the same script and count about 2× headroom over the peak.

## See also

- [Performance characteristics](../concepts/performance.md): the cost model behind these numbers.
- [Performance tuning](performance.md): profiles, pools and the knobs that trade memory for latency.
- [Self-hosting](self-hosting.md) and [Production deployment](production.md): putting limits on the container and watching the node.
- [Metrics](../reference/metrics.md): every series used above.
- [Limits and quotas](../reference/limits.md): per-task limits and plan quotas.
