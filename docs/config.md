# Configuration Guide

This guide explains how to configure the Rust runtime, including available profiles, configuration tools, and common tuning scenarios.

## Quick Start

### Generate a Default Configuration

```bash
# List what `make defconfig` accepts: every file in config/defconfigs/
make listprofiles        # balanced, max_density, min_latency, dev, edge-1gb

# Generate balanced profile (default, recommended)
make defconfig PROFILE=balanced > rt.toml

# Or use edge-1gb for small devices
make defconfig PROFILE=edge-1gb > rt.toml

# Or use min_latency for low-latency workloads
make defconfig PROFILE=min_latency > rt.toml

# Check that the node loads it (prints the fully resolved configuration)
rt --config rt.toml --print-config
```

`make defconfig` prints `config/defconfigs/<PROFILE>.toml` as is. Those files hold exactly the keys the
node loader (`crates/rt-config`) accepts and are the reference for `make oldconfig`. Per-service blocks
such as `scaling` (`scaling.keep_resident`) are **not** `rt.toml` sections: the node rejects them
(`unknown key scaling`), so the tools never emit them. Set them on the service (`POST /v1/services`).

### Interactive Configuration

```bash
# Linux menuconfig-style curses interface
make menuconfig

# Line-by-line prompts
make config

# Validate your configuration
make validate-config
```

### Minimal Configuration (Diff-based)

```bash
# Update rt.toml with all defaults (fill missing keys)
make oldconfig

# Save only non-default values to .config
make savedefconfig
```

## Profiles

Profiles bundle a set of knobs optimized for different workload characteristics. The node knows three
(`node.default_profile`: `max_density`, `balanced`, `min_latency`); `edge-1gb` and `dev` are reference
defconfigs on top of `balanced` (`default_profile = "balanced"`) that also shrink limits. All five are
files in `config/defconfigs/` and are accepted by `make defconfig PROFILE=...` and listed by
`make listprofiles`.

| Profile | Kind | Use Case | Throughput | Latency | Memory | Build Strategy |
|---------|------|----------|-----------|---------|--------|----------------|
| `max_density` | node profile | High-throughput batch workloads | High | Medium-High | Aggressive | Lazy |
| `balanced` | node profile | General-purpose (default) | Medium | Medium | Moderate | Lazy |
| `min_latency` | node profile | Real-time, interactive workloads | Lower | Low | Conservative | Eager |
| `edge-1gb` | defconfig on `balanced` | 1vCPU/1GB edge devices | Low | Medium | Very constrained | Lazy |
| `dev` | defconfig on `balanced` | Local development | N/A | N/A | Minimal | Lazy, debug logs |

### Profile Differences at a Glance

Profile-scoped knobs are plain `rt.toml` keys; an unset key takes the value of `node.default_profile`.
`sched.queue_timeout`, `cache.idle_ttl`, `cache.preload`, `build.compile`, `wasm.reuse_instances` and
`deno.reuse_isolates` are service-scoped defaults: the node-wide value is set here, a service can override it (for example the service
config block `"wasm": {"reuse_instances": true}`; `effective.reuse_instances.source` is then `service`).

**Scheduler:**

| Key | max_density | balanced | min_latency |
|-----|-------------|----------|------------|
| `sched.spin_us` | 0 | 10 | 50 |
| `sched.time_slice` | 20ms | 10ms | 5ms |
| `sched.pin_threads` | false | false | true |
| `sched.handoff_on_hog` | false | false | true |
| `sched.queue_timeout` | 10s | 5s | 1s |

**Memory Management:**

| Key | max_density | balanced | min_latency |
|-----|-------------|----------|------------|
| `memory.high` | 0.90 | 0.80 | 0.70 |
| `memory.critical` | 0.97 | 0.92 | 0.88 |
| `memory.purge_min_interval` | 5s | 30s | 120s |
| `cache.t1_share` | 10% | 20% | 35% |
| `wasm.linear_memory_keep_resident` | 0 | 1 MB | 8 MB |
| `wasm.max_unused_warm_slots` | 0 | 100 | 1000 |

**Caching and Service Lifecycle:**

| Key | max_density | balanced | min_latency |
|-----|-------------|----------|------------|
| `cache.idle_ttl` | 60s | 600s | 3,600s |
| `cache.preload` | no | no | yes |
| `deno.isolate_pool_per_p` | 0 | 1 | 4 |
| `deno.max_bound_per_m` | 128 | 64 | 32 |
| `build.compile` | lazy | lazy | eager |
| `wasm.reuse_instances` | false | false | true |
| `deno.reuse_isolates` | false | false | true |

## Configuration Sections

### Node

Top-level runtime configuration:

```toml
[node]
data_dir = "/data"              # Working directory for runtime data
default_profile = "balanced"    # Profile applied to node and services without explicit profile
```

### Listeners

Network endpoint configuration:

```toml
[listeners]
public = "0.0.0.0:8080"        # Public API (invoke, HTTP)
internal = "unix:/data/rt.sock" # Internal IPC for control plane
control = "127.0.0.1:8081"     # Control API (localhost only, privileged)
metrics = "127.0.0.1:9090"     # Prometheus metrics endpoint
internal_allowed_uids = []      # Extra UIDs allowed on the internal Unix socket
allow_tcp_internal = false      # Opt-in for a (loopback) TCP internal listener
```

The internal listener trusts its callers (no invoke access check, `x-rt-invoker` honored). On a
Unix socket the node checks each connecting process's UID (`SO_PEERCRED` on Linux, `getpeereid`
on macOS) before serving it: the node's own UID is always admitted, any other UID only when listed
in `internal_allowed_uids`. Root (`0`) is not implied; list it explicitly if an operator tool runs
as root. Other peers are disconnected without a response. A TCP `internal` listener cannot check
who connects (every local user would get trusted invokes and `x-rt-invoker`): the node refuses to
start with one unless `allow_tcp_internal = true`, and it must bind a loopback address.

A debug build started without `RT_ROOT_TOKEN` leaves the control API open (every caller is the
operator). It prints a startup banner and refuses to start unless `control` is a loopback address
(`127.0.0.0/8`, `::1`, `localhost`).

### Routing

Service routing and discovery:

```toml
[routing]
base_domain = "fn.example.com"  # Base domain for service URLs
                                 # Services become {slug}.fn.example.com
```

### Custom Hosts

Tenant claims of custom hosts (`PUT /v1/services/{id}/hosts/{host}`, see [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md)):

```toml
[hosts]
pending_ttl = "7d"        # An unverified claim expires after this long and stops blocking others
verify_dns = true         # Activate a claim when _rt-verify.<host> TXT equals its verify_token
verify_interval = "60s"   # How often pending claims are checked (DNS and expiry)
max_pending_per_tenant = 10 # Pending claims one tenant may hold (403 quota_exceeded beyond)
```

With `verify_dns = false` the node makes no DNS lookups; pending claims then become active only
through operator approval (an operator `PUT` of the same host).

### Scheduler

Task scheduling and processor allocation:

```toml
[sched]
processors = 0                  # Number of processors
                                 # 0 = auto-detect from cgroup
                                 # Can be overridden by --cpus container limit

spare_machines = 2              # Machines to keep ready
lrq_capacity = 256              # Global run queue capacity
max_inflight = 10000            # Run slots node-wide (>= 2); tasks beyond it wait for a slot
                                 # (never rejected for capacity: excess requests queue)
max_queued = 0                  # Node bound on waiting tasks; over it 503 overloaded
                                 # 0 = auto (4 x max_inflight); one service may hold half
max_queued_mb = 0               # Node bound on bytes held by waiting tasks (MB)
                                 # 0 = auto (1/16 of the memory budget)
min_run_time_max_ms = 600000    # Largest scaling.min_run_time_ms any service may set (0..=86400000)
                                 # The plan's limits_max.min_run_time_ms may lower it
aging_factor = 4                # Admission aging k (0..=64): deadline = since + min(k*pred, queue_timeout/2)
                                 # 0 = arrival order within a tenant

spin_us = 10                    # Processor spin time (μs) before sleep
                                 # Higher = lower latency but more CPU usage
                                 # Profile-scoped

time_slice = "10ms"             # Time slice before preemption
                                 # Profile-scoped

pin_threads = false             # Pin OS threads to cores for cache affinity
                                 # Recommended for min_latency profile
                                 # Profile-scoped

handoff_on_hog = false          # Hand off task if processor is hogging
                                 # Recommended for min_latency profile
                                 # Profile-scoped

queue_timeout = "5s"            # Longest wait for a run slot / P / artifact / memory,
                                 # then 503 queue_timeout + Retry-After
                                 # Service-scoped (min_latency: 1s, max_density: 10s)

warm_share = 0.15               # Share of the memory budget warm instances may use
                                 # (autoscaler warm targets)
                                 # Profile-scoped (max_density: 0.05, min_latency: 0.3)
```

Per-service concurrency is adaptive: each service starts at a profile limit and the
scheduler grows it while requests wait and latency holds, and shrinks it on latency regressions, memory
pressure or saturated processors. An *instance* is one concurrently running copy of a service
A service's `scaling.max_instances` is a hard cap on its instances (the adaptive limit; only the
service's own self-calls and self-request replies, whose parent holds an instance, may run past it)
and `scaling.min_instances` keeps that many warm and floors the limit; both are at most the plan's
`max_instances`, and the plan's `max_concurrency` caps a tenant. Over a limit, requests wait (FIFO per
service, fair across tenants).

#### Processor Kind Allocation

```toml
[sched.kinds.deno]
min = 1                         # Minimum Deno processors
max = 0                         # Maximum (0 = processors minus the other kinds' minimums)

[sched.kinds.wasm]
min = 1                         # Minimum WASM processors
max = 0                         # Maximum (same rule)
```

`deno.min + wasm.min` must not exceed `sched.processors` (checked from two processors on).

### Memory

Memory management and pressure handling:

```toml
[memory]
budget_mb = 0                   # Memory budget in MB
                                 # 0 = 85% of cgroup memory.max (or physical RAM)

check_interval = "250ms"        # Interval between memory pressure checks

high = 0.80                     # Memory high watermark (fraction of budget)
                                 # Triggers cache eviction and GC
                                 # Profile-scoped: max_density=0.90, min_latency=0.70

critical = 0.92                 # Critical watermark (fraction of budget)
                                 # Triggers admission control and shedding
                                 # Profile-scoped: max_density=0.97, min_latency=0.88

purge_min_interval = "30s"      # Minimum interval between cache purges
                                 # Profile-scoped: max_density=5s, min_latency=120s
```

### Cache

V8 code cache and task caching configuration:

```toml
[cache]
t2_max_mb = 4096                # Maximum T2 cache size in MB
                                 # (compiled services and code cache)

t1_share = 0.20                 # T1 cache share (fraction of budget)
                                 # Warmer tier - precompiled services
                                 # Profile-scoped: max_density=0.10, min_latency=0.35

idle_ttl = "600s"               # Idle time-to-live for cached services
                                 # Service-scoped: max_density=60s, min_latency=3600s

preload = false                 # Preload top-N services into T1 cache
                                 # Service-scoped: false except min_latency=true

preload_top_n = 100             # Services (by persisted frequency) preloaded when `preload` applies
```

### Deno Executor

Deno/V8 executor configuration:

```toml
[deno]
isolate_base_mb = 4             # Base memory per isolate in MB
                                 # Actual usage depends on application

v8_platform_threads = 1         # V8 platform worker threads
                                 # For multi-threaded GC

dispose_backlog = 8             # Finished isolates are disposed between tasks and when idle;
                                 # beyond this backlog they are disposed inline

v8_flags = ["--single-threaded-gc"]  # V8 command-line flags
                                      # JIT tier tuning
                                      # The node adds --wasm-max-mem-pages = the largest
                                      # plans.*.limits_max.memory_mb (one wasm memory's cap)

max_bound_per_m = 64            # Maximum bound isolates per machine
                                 # Profile-scoped: max_density=128, min_latency=32

isolate_pool_per_p = 1          # Isolate pool size per processor
                                 # 0 = no pool (always create new)
                                 # Refilled between tasks and when idle; a pooled isolate
                                 # reserves isolate_base_mb; dropped at memory pressure High
                                 # Profile-scoped: max_density=0, min_latency=4

reuse_isolates = false          # Keep an isolate with the evaluated module for the next
                                 # request of the same service version (module state persists).
                                 # Service-scoped default:
                                 # max_density=false, min_latency=true
reuse_max_idle = 4              # Kept isolates per service version per machine when the
                                 # scheduler publishes no warm target (0 = keep none)
reuse_max_idle_total = 16       # Kept isolates per machine, all services (oldest go first)
reuse_idle_ttl = "60s"          # A kept isolate unused this long is disposed (not the published warm target's worth)
reuse_max_uses = 1000           # Requests one kept isolate serves before it is replaced
```

### WASM Executor

WASM executor configuration:

```toml
[wasm]
fuel = false                    # Enable WASM fuel metering
                                 # Use for cost tracking

reuse_max_idle = 8              # Maximum idle reused instances

linear_memory_keep_resident = 1 # Keep N MB of linear memory resident
                                 # Profile-scoped: max_density=0, min_latency=8

reuse_instances = false         # Reuse WASM instances across tasks
                                 # Service-scoped (min_latency=true); a service's
                                 # config.wasm.reuse_instances overrides it

max_unused_warm_slots = 100     # Pool slots kept warm (affine to their module) after
                                 # release, capped at the slot count
                                 # Profile-scoped: max_density=0, min_latency=1000
```

The pooling allocator is sized at startup: `slots = wasm budget / plan max memory` (at most 10,000)
component instances, memories and async stacks (`total_stacks = slots`: each running task holds one slot
and at most one stack). `max_unused_warm_slots` and `linear_memory_keep_resident` decide how much of a
released slot stays warm: more for `min_latency` (faster re-instantiation, more RSS), none for
`max_density`.

### Security

Security and sandboxing configuration:

```toml
[security]
seccomp = true                  # Linux process hardening: not dumpable, no_new_privs, no core
                                 # dumps (no seccomp filter is installed; see SECURITY.md)

clock = "frozen"                # Clock coarsening
                                 # frozen: Date.now() / performance.now() advance only at I/O
                                 # coarse: Kernel coarse-grained clock
                                 # Defends against timing side-channels

require_v8_sandbox = false      # Refuse to start without a V8 sandbox (built-in default: true)
                                 # MVP images ship prebuilt rusty_v8 (no sandbox), so every shipped
                                 # defconfig sets false; the custom V8 build is pending

reserved_hosts = []             # Hosts tenants can never claim
                                 # Prevents confusion deputy attacks

master_key_file = ""            # Path to master encryption key file
```

### Ingress

Connection-level limits of the TCP listeners (`public`, `control`, `metrics`, a TCP `internal`).
All of them need a restart to change.

```toml
[ingress]
trusted_proxies = []            # CIDRs whose X-Forwarded-For is believed (ip_allow, client IP)
header_timeout = "10s"          # Request head must arrive within this time
idle_timeout = "60s"            # HTTP/2 keep-alive interval and timeout
max_conns_per_ip = 256          # Open connections per peer IP
h2_max_streams = 100            # Concurrent HTTP/2 streams per connection
sse_streams_per_key = 10        # Concurrent log/event streams per credential
backlog = 4096                  # listen() backlog of each TCP listener
```

- **`backlog`** is the queue of connections the kernel has accepted but the node has not yet
  picked up. A burst larger than the backlog gets connection resets. The kernel silently caps
  the value: raise `kern.ipc.somaxconn` (macOS, default 128) or `net.core.somaxconn` (Linux,
  default 4096 since 5.4, 128 before) as well, e.g. `sysctl -w kern.ipc.somaxconn=4096`.
- **`max_conns_per_ip`**: a connection beyond the cap is closed right after accept, before any
  HTTP is read, and counted in `rt_ingress_conn_rejected_total`. Load tests from one machine
  hit this first; raise it for them.
- **Behind a proxy**: a peer inside `trusted_proxies` is exempt from `max_conns_per_ip`, since
  every client arrives from the proxy's address. Limit clients at the proxy instead.

### Service Data (`[storage]`, `[blocking]`)

Each service has its own SQLite `data.db` (WAL). Per database there is one writer connection
(writes, write batches, interactive transactions) and up to `readers_per_db` read-only
connections (queries, read batches, KV and blob reads), which keep reading while the writer is
busy. All of it runs on the `[blocking]` pool, queued fairly per tenant. Restart to change.

```toml
[storage]
max_open_dbs = 256              # SQLite connections of all services together (writers + readers)
readers_per_db = 4              # Read-only connections per service DB; 0 = reads use the writer
sql_cache_mb = 512              # Page-cache budget of all those connections; 0 = SQLite default
sql_budget_ms = 1000            # Per-statement time budget (error `sql_timeout`)

[blocking]
threads = 0                     # 0 = auto: max(4, available cores)
```

- **`max_open_dbs`** counts every connection. When it is reached, the least recently used idle
  service is closed whole (writer and readers); a read that finds no free slot uses the writer.
- **`readers_per_db`**: readers open only under concurrent reads, next to an open writer, so a
  service that is used sequentially keeps one connection.
- **`sql_cache_mb`** bounds SQLite page caches: each connection gets
  `sql_cache_mb / max_open_dbs` (at least 64 KiB) as `PRAGMA cache_size`.
- **`blocking.threads`**: under contention one tenant may occupy `floor(plan.blocking_share × threads)`
  of them, at least 1 (`blocking_share` default 0.25), so one tenant's heavy SQL cannot starve the others.
  While threads are idle a tenant may use more (all but one thread), so a lone tenant is not held to its share.
- Guest SQL is bounded too: `sqlite3_limit`s on every guest connection (statement 1 MiB, value or
  row 16 MiB, expression depth 500, 100 compound terms, 1,000 columns, `?10000`, 1,000-byte
  `LIKE` patterns) and at most 10,000 rows per statement / 10 MiB of materialized values per
  call, counted at their in-memory size (`sql_result_too_large`). Temp tables, views, triggers and
  indexes are denied (`sql_not_authorized`): they would live outside the database file and every
  quota. A sort that outgrows the page cache spills to a temp file for the statement's duration.
- **Storage quota**: a service database may grow only by what is left of its tenant's
  `storage_mb` across all its services; open databases pick up new totals every 30 s.

### WebSockets (`[ws]`)

The node holds every WebSocket itself; a message, close or error runs one task of the service
(docs/handlers.md "WebSockets and durable objects"). Restart to change.

```toml
[ws]
max_sockets = 20000             # Open sockets on the node, every tenant together; an upgrade over it is refused
ping_interval = "30s"           # Server ping; a socket silent for two intervals is dropped (1006)
send_buffer_kb = 1024           # Queued for one client; over it the socket closes with 1013 (slow consumer)
inbox_max = 1024                # Events queued per object; a full inbox stops reading its sockets (backpressure)
max_event_attempts = 3          # Deliveries of a socket event; then a message's socket closes with 1011
max_alarm_attempts = 6          # Deliveries of a durable object's alarm before it is dropped
msgs_per_sec = 50               # Messages one client may send per second ...
msgs_burst = 200                # ... and the burst above it; over it the socket is read more slowly
flood_close_after = "10s"       # A client throttled without a break this long is closed with 1008
drain_grace = "5s"              # How long a drain waits for event tasks after closing the sockets (1001)
```

Measured on the reference machine, an idle
socket costs 5-6.4 KiB of node memory (plus its attachment, ≤ 16 KiB), so `max_sockets = 20000`
is about 100-130 MiB; size it with the memory budget. Per tenant, plan keys bound sockets too: `max_sockets` (open sockets of the
tenant, 0 = only the node cap), `max_sockets_per_object` and `ws_max_message_bytes` (a client
message over it closes the socket with 1009); `allowed_triggers` must list `websocket`. A service
can narrow them with its `ws` block (`allowed_origins`, `warm`, `max_sockets`, docs/api.md).

### Other Sections

Remaining sections keep the shape of the defconfigs; every key is listed in `config/schema.json`.

| Section | Keys |
|---------|------|
| `[build]` | `threads`, `memory_mb`, `timeout` (Deno build), `wasm_timeout`, `fetch_timeout` (per module), `npm_cdn`, `allowed_hosts`, `max_deps_mb`, `compile` |
| `[queue]` | `batch_size`, `max_attempts`, `max_concurrency` (per-service concurrent queue tasks) |
| `[sysmon]` `[rebalance]` `[io]` `[disk]` `[deploy]` `[s3]` `[supervisor]` `[secrets]` `[egress]` `[logs]` `[metrics]` `[otel]` | see the defconfigs |
| `[plans.<name>]` | tenant plans (a `free` plan always exists), see [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#tenancy-plans-and-quotas). Event keys: `max_async_pending` (pending async invocations per service, free 10000), `max_topics` (topics per tenant, free 10), `max_topic_partitions` (1..=16, free 4), `max_stream_mb` (stream record MB per tenant, free 100), `max_stream_retention_hours` (free 72); `limits_max.min_run_time_ms` (largest `scaling.min_run_time_ms`, free 60000; the effective cap is the lower of it and `sched.min_run_time_max_ms`). `allowed_triggers` values: `http`, `interval`, `cron`, `queue`, `call`, `websocket`, `pubsub`, `stream` (free default: all but `cron`) |
| `[[binding]]` | operator platform bindings (reachable from operator SDK services only). Operator SDKs are not configured here: `21ci sdk publish`; a leftover `[[sdk]]` fails to load |

`scaling` is **not** in this list: it is a per-service block, not node configuration.

## Common Configuration Tasks

### Limiting CPU Cores

**Scenario**: Running on a 4-core machine, want to use only 2.

```toml
[sched]
processors = 2
```

Or use environment variable:

```bash
export RT__SCHED__PROCESSORS=2
```

Or via container:

```bash
docker run --cpus 2.0 myimage
```

**Note**: `processors=0` auto-detects from cgroup `cpu.max`.

### Limiting Memory Budget

**Scenario**: 8 GB machine, want to reserve 2 GB for OS.

```toml
[memory]
budget_mb = 6144  # 6 GB
```

Or as percentage (default):

```toml
[memory]
budget_mb = 0  # Use 85% of cgroup memory.max
```

### Tuning for Max Throughput

Start with `max_density` profile:

```bash
make defconfig PROFILE=max_density > rt.toml
```

Then adjust:

```toml
[sched]
processors = 16                 # Use all cores
spin_us = 0                     # No spin - go to sleep immediately
time_slice = "30ms"             # Longer time slices to reduce context switching

[memory]
high = 0.92
critical = 0.98
purge_min_interval = "2s"       # Very aggressive

[cache]
t1_share = 0.05                 # Minimize cache usage
idle_ttl = "30s"                # Evict quickly
```

### Tuning for Low Latency

Start with `min_latency` profile:

```bash
make defconfig PROFILE=min_latency > rt.toml
```

Then adjust:

```toml
[sched]
processors = 8                  # Don't over-subscribe
spin_us = 100                   # Aggressive spinning
time_slice = "2ms"              # Short time slices
pin_threads = true              # Lock to cores
handoff_on_hog = true           # Prevent hogging

[deno]
isolate_pool_per_p = 8          # Warm pool of isolates

[cache]
t1_share = 0.50                 # Large cache
idle_ttl = "7200s"              # Long TTL
preload = true                  # Preload services
```

### Development Environment

Use `dev` profile:

```bash
make defconfig PROFILE=dev > rt.toml
```

Features:

- Localhost-only listeners (security)
- Small resource limits (8GB machine limitation)
- Long timeouts (slow dev machines)
- Request logs to stdout (`logs.stdout = true`); system log filter via `RT_LOG` (see Troubleshooting)
- Mock S3 support (local minio)
- V8 inspector enabled on port 9229

`make dev` (scripts/dev.sh) runs a node on this file.

## Using Configuration Tools

### 1. Interactive Menu (menuconfig)

Linux `make menuconfig` style curses interface:

```bash
make menuconfig
```

**Navigation:**
- UP/DOWN arrow keys: navigate sections and keys
- ENTER: enter section or edit value
- `/`: search configuration space
- `L`: load a defconfig
- `S`: save configuration
- `Q`: quit (with save prompt)
- `?`: show help

**Editing values:**
- Boolean values: toggle with ENTER
- Enum values: cycle through options with ENTER
- Numeric/string: type new value and press ENTER

### 2. Line-by-Line Config (config)

Prompts for each configuration key:

```bash
make config CONFIG=rt.toml
```

Shows current value (or default) and help text. Press ENTER to keep value or type new value.

### 3. Defconfig Generation

Generate complete profile configuration:

```bash
# Generate balanced profile (write to stdout)
make defconfig PROFILE=balanced

# Save to file
make defconfig PROFILE=balanced > rt.toml

# Available profiles: balanced, max_density, min_latency, edge-1gb, dev (make listprofiles)
```

The output is the shipped file `config/defconfigs/<PROFILE>.toml`, every key stated, no per-service blocks.

### 4. Fill Missing Keys

Add all missing keys from profile defaults:

```bash
make oldconfig CONFIG=rt.toml
```

Useful after adding a new key to schema or upgrading.

### 5. Minimal Config (savedefconfig)

Save only non-default values (like Linux `.config`):

```bash
make savedefconfig CONFIG=rt.toml
```

Saves minimal config to `.config` file.

### 6. Validation

Validate configuration against schema:

```bash
make validate-config CONFIG=rt.toml
```

Checks:
- Unknown sections and keys (including `[scaling]`, which is per-service) and nested ones (`sched.kinds.deno.*`, `[plans.<name>]`, `[[binding]]`); the removed `[[sdk]]` section with its own error
- Type mismatches (durations: `"250ms"`, `"1h 30m"` or a bare integer of seconds)
- Out-of-range values
- Invalid enum values

The authoritative check is the node itself: `rt --config rt.toml --print-config` (exit 0 = loads), and
`rt serve` additionally runs the cross-field validation.

## Configuration Precedence

At startup, configuration is resolved in this order (first wins):

1. Built-in defaults (= `balanced` profile)
2. `rt.toml` file (or `--config` path)
3. Environment variables: `RT__SECTION__KEY=value`
4. Secrets: `RT_ROOT_TOKEN`, `RT_MASTER_KEY` (via env/file only)

Example:

```bash
# File sets processors=2
# Environment overrides to 3
export RT__SCHED__PROCESSORS=3

# At startup, processors=3 wins
rt --config rt.toml serve
```

## Live-Updatable vs Restart-Required

**Live-updatable** (`Config::LIVE_KEYS`): applied from the file on `SIGHUP`; the
profile-controlled ones are also re-derived when `PUT /v1/node/config` changes `default_profile` (the
endpoint takes `default_profile` and `degradation_mode`, see docs/api.md):
- `sched.spin_us`, `sched.time_slice`, `sched.handoff_on_hog`, `sched.queue_timeout`
- `sched.max_inflight`, `sched.max_queued`, `sched.max_queued_mb`
- `sysmon.max_tick`, `rebalance.interval`, `deno.max_bound_per_m`
- `memory.high`, `memory.critical`

`SIGHUP` also reloads `[plans]` (`Config::RELOAD_KEYS`) and re-reads the route and service tables from
`meta.db`. A file that fails to load or validate leaves the running configuration unchanged; other changed
keys (including `[binding]`, `[egress]`, `[logs]`, `[metrics]`) are logged and apply after a restart.

**Restart-required:** everything else, notably
- Processor count and kind allocation
- Thread pool sizes
- Storage backend
- Listeners
- Security settings

Configuration API returns `pending_restart` list for restart-required changes.

## Examples

### Example 1: Small Edge Deployment (1 vCPU, 1 GB)

```bash
make defconfig PROFILE=edge-1gb > rt.toml
```

Auto-generated:
- processors=1
- memory.budget_mb=768
- Build threads=1, memory_mb=128
- Minimal cache, log retention, connections

### Example 2: High-Throughput Batch Processing

Start with max_density, then tune:

```bash
make defconfig PROFILE=max_density > rt.toml
```

Edit `rt.toml`:

```toml
[sched]
processors = 32
spin_us = 0
time_slice = "50ms"  # Even longer slices
max_inflight = 50000

[memory]
high = 0.95
critical = 0.99
purge_min_interval = "1s"

[cache]
t1_share = 0.05
idle_ttl = "10s"

[build]
compile = "lazy"  # Don't compile eagerly
```

### Example 3: Real-Time API Server

Start with min_latency:

```bash
make defconfig PROFILE=min_latency > rt.toml
```

Edit `rt.toml`:

```toml
[sched]
processors = 8  # Fixed 8 cores (no over-subscription)
spin_us = 200   # Very aggressive
time_slice = "1ms"
pin_threads = true
handoff_on_hog = true
queue_timeout = "100ms"  # Very short queue timeout

[deno]
isolate_pool_per_p = 16  # Large warm pool

[cache]
t1_share = 0.50
idle_ttl = "3600s"
preload = true

[security]
clock = "frozen"  # Defend against timing attacks

[logs]
stdout = false  # Don't log every request
```

## Schema Reference

The complete schema is in `config/schema.json` with all keys, types, defaults, ranges, and help text.

To list all available profiles:

```bash
make listprofiles
```

To validate your configuration:

```bash
make validate-config
```

## Troubleshooting

**Error: Unknown key `sched.procesors` (typo)**

The node names the offending key path. Check the schema for correct spelling:

```bash
grep procesors config/schema.json  # No results
grep processors config/schema.json  # Found!
```

**Error: unknown key scaling**

`scaling` is a per-service block (`POST /v1/services` body), not an `rt.toml` section. Delete the
`[scaling]` table from `rt.toml`. `make defconfig` no longer emits it.

**No debug logs**

The system log filter is `RT_LOG` (e.g. `RT_LOG=info,rt_sched=debug`); `RUST_LOG` is read only when
`RT_LOG` is unset. The image, `docker-compose.yml`, `.env.example` and `scripts/dev.sh` use `RT_LOG`.

**Config doesn't take effect after editing**

Some changes require restart:

```bash
# Check pending_restart
curl http://localhost:8081/v1/node/config | jq .pending_restart

# Restart if needed
systemctl restart rt
```

**Memory pressure is too high**

Reduce memory budget or lower watermarks:

```toml
[memory]
budget_mb = 4096  # Reduce if too aggressive
high = 0.70       # Start evicting earlier
critical = 0.85   # Restrict admission earlier
```

**Task queue timeouts**

Increase queue timeout:

```toml
[sched]
queue_timeout = "10s"  # From 5s
```
