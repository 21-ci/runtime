---
title: Changelog
description: User-facing changes to 21 Cloud Functions Runtime, newest first, grouped by date and by kind of change.
---

# Changelog

User-facing changes to the runtime, newest first. Each day is grouped into Added, Changed, Fixed, Security and
Performance. Internal refactoring and test-only changes are left out.

!!! info "Version 0.1.0 is not released yet"
    The workspace version is **0.1.0** and there is no tagged release. Every entry below belongs to the unreleased
    0.1.0 work on the development branch. When 0.1.0 is tagged, this page will gain a dated release heading.

## 0.1.0 (unreleased)

### In progress

#### Added

- Operator SDKs as runtime services: the operator publishes an SDK (`POST /v1/sdks`, `21ci sdk publish`), and tenants
  import its contract (`import { acme } from "std:sdk"`, `sdk:acme`, WASM `std:host/sdk`) while every call runs in the
  release their grants select (node, plan, tenant, project, environment; channels such as `stable` and `canary`). An
  SDK update, rollback or removal never rebuilds or redeploys a tenant service. `21ci sdk ls` / `types`, MCP
  `list_sdks` / `get_sdk_contract`. See [Operator SDKs and bindings](../guides/operator-sdks.md) and
  [Use operator SDKs](../guides/use-sdks.md).
- Typed RPC for WASM components: `call-rpc` calls a service's RPC method, and the `rpc` export (world `rpc-service`)
  serves RPC calls, from Deno and WASM callers alike. `21ci types <service>` writes the `.d.ts` of a service's RPC
  surface.
- Projects and environments: `/v1/projects`, `21ci project`, `--project` / `--env` on `service create` and `push`;
  every tenant has a `default` project. See [Projects and environments](../guides/projects.md).
- Self-service tenant tokens: the `keys:write` scope, `POST|GET /v1/keys`, `DELETE /v1/keys/{key}` and `21ci token`. A
  new token never holds more scopes, projects, environments or lifetime than its creator. See
  [Get access](../guides/access.md#create-your-own-tokens).
- An MCP server at `POST /mcp` on the control listener, and the `21ci mcp` stdio bridge, so Claude Code, Cursor,
  Claude Desktop and other MCP clients can manage services with a tenant key. See [AI assistants (MCP)](../mcp.md).
- `scaling.min_instances` and `scaling.max_instances` per service (`--min-instances`, `--max-instances`), bounded by the
  new plan key `max_instances`; a service with a floor is kept warm from node start. The autoscaler also grows a
  service when its arrival rate rises fast, before requests queue. See
  [Instances and autoscaling](../concepts/scheduler.md#instances-and-autoscaling).
- `std:sqlite` (also `bun:sqlite`): the service's SQLite database with bun:sqlite's synchronous API. See
  [Synchronous SQL](../guides/sub-services.md#synchronous-sql-stdsqlite).
- Atomic counters: `kv.incr(key, delta, { ttlSeconds? })` in `std:kv` and `incr` in `std:host/kv`. The TTL applies when
  the call creates the counter, so it doubles as a fixed-window rate limiter. See the
  [guest API](../reference/guest-api.md).
- The client address of an HTTP request is available to handlers as `ctx.client.ip` and in the `x-rt-client-ip`
  header, resolved behind `ingress.trusted_proxies`.
- Control API: `GET /v1/me` (who the credentials are, their scopes and the effective plan), `GET /v1/version`,
  `GET /v1/plans` and `GET /v1/node/config` (the running configuration, node secrets left out) for the operator, and
  `GET /v1/services/{s}/hosts/{host}`. A tenant key with `logs:read` can read its own tenant's usage. See the
  [API reference](../api.md).
- Admission state per service: `status.admission` in the service object and `admission` in `GET …/stats` show the
  adaptive concurrency limit, running and queued tasks, the queue wait and the autoscaler's view.
  `GET /v1/node/scheduler` adds the node-wide admission bounds and one view per service. See
  [Scheduler and autoscaling](../concepts/scheduler.md#observing-the-scheduler).
- `21ci` covers every control API operation: new `whoami`, `doctor`, `plan list|get`, `tenant update|usage`,
  `host attach|list|get|detach`, `data export`, `build-logs`, `versions <svc> N`, `service update --set|--unset`
  and `node config get`; a global `--retry` for busy answers, `--json` on every command, and exit code 4 for "busy,
  try again later". See the [CLI reference](../cli.md).
- Pub/sub and streams for services. `std:pubsub` publishes fire-and-forget messages or sends requests with a reply,
  delivered to the services that subscribe through `events.consume`. `std:stream` writes to a persistent, replayable
  topic read by consumer groups with offsets, retries and a dead-letter store. Topics are managed with `/v1/topics`,
  `21ci topic` and MCP; consumer groups with `21ci consumer`, which can also reset offsets and list or purge dead
  letters. See the [handler reference](../handlers.md) and [error reasons](../reference/errors.md).
- Asynchronous invocation: a service with `async.enabled = true` accepts `Prefer: respond-async` or
  `X-Rt-Invocation-Type: Event` and answers `202 Accepted` with an invocation id. The runtime retries failed attempts,
  honours `Idempotency-Key` and lists records at `GET /v1/services/{s}/invocations`.
- Instance keep-alive per service: `scaling.min_run_time_ms` (`--min-run-time` on `21ci service create|update`) sets how
  long a reused Deno isolate or WASM instance may stay idle after it answered. `0` releases it at once unless tasks of
  the service are waiting for a slot. Node key `sched.min_run_time_max_ms` and the plan limit
  `limits_max.min_run_time_ms` cap the value. See [Instance lifetime and min_run_time](../handlers.md#instance-lifetime-and-min_run_time).
- Admission state shows the predicted run time (`pred_ms` in the admission view and `21ci service stats`), and the
  Prometheus metrics `rt_instance_keepalive_total`, `rt_pubsub_*`, `rt_stream_*`, `rt_async_*` and
  `rt_queue_throttled_total` are always on. See [Metrics](../reference/metrics.md).

#### Changed

- Admission order is now fair by slot time and deadline. Tenants with the least recent slot time go first; inside a
  tenant, calls go first and then the earliest deadline, which favours tasks predicted short and bounds how long a long
  task can be overtaken. The services of one tenant no longer take turns one task each. With `sched.aging_factor = 0`
  a tenant's waiting tasks run in arrival order. See [Admission order](../concepts/scheduler.md#admission-order).
- Operator bindings are reachable from operator SDK services only and act for the calling tenant (tenant header, plan,
  rate limit). A tenant service calling `platform.fetch` gets `403 binding_not_allowed`.
- The `[[sdk]]` configuration section (SDK code bundled into tenant services) and `preload` were removed; a
  configuration that still has them fails to load with a pointer to `21ci sdk publish`.

- `Authorization` headers now reach the handler unchanged, so a service can check its own tokens. Only a tenant API
  key (`Authorization: Bearer rtk_…`) is removed; clients of `access: key` services that also need their own token
  send the invoke key in `x-rt-key`.
- `status.inflight` of a service reports the tasks running now (the same number as `status.admission.running`).
- API keys are scoped by projects and environments instead of services: `project_ids` and `environments` replace
  `service_ids` on `/v1/keys`, `/v1/tenants/{t}/keys`, `/v1/me` and `21ci key` / `21ci token` (`--project`,
  `--environment`). A key with a project or environment restriction cannot create, change or delete projects. An
  `env:<name>` call entry compares environments within the target's own project only. See
  [Get access](../guides/access.md).
- The metadata schema is one consolidated version. A `meta.db` from an earlier build is not migrated; see
  [Upgrades](../guides/production.md#upgrades) for the reset.
- The service-data (blocking) pool is work-conserving: when threads are idle, a busy tenant may use more than its
  `blocking_share`, always leaving one thread free.

#### Fixed

- An asynchronous delivery whose stored request cannot be opened fails at once (`sealed_unreadable`) and no longer
  uses up its retries.
- A kept instance now expires on time on an idle node too (at most about 50 ms after `min_run_time_ms`); before, it
  stayed until the node next had work.
- With `min_run_time_ms = 0`, the only instances kept are the warm floor of `scaling.min_instances` (and an instance
  handed to a waiting task of the same service).
- A self-call, or a pub/sub request to a subject the service answers itself, at `max_instances: 1` no longer ends in
  `503 queue_timeout`: it runs on the slot its waiting parent lends.

#### Security

- Asynchronous invocation requests are stored sealed (encrypted with the node's secret key, bound to the service and the
  invocation id) in the queue database. Their headers, such as `Authorization`, and bodies are never kept in plain text
  on disk, and they are read back only to run the handler.

### 2026-10-07: review fixes and sample apps

#### Added

- Five sample apps in `sample/apps/` of the runtime repository: a Telegram bot, event sign-ups, a live queue, UZS
  currency rates from cbu.uz, and a WASM edge gateway. Each one has a [tutorial](../tutorials/index.md).
- `std:host/platform` for WASM guests: `fetch(binding, request)` calls an operator binding with the same plan, method
  and path checks as `platform.fetch` in Deno.
- WASM components built with the full Rust standard library (which import `wasi:cli/terminal-*`) now link and run.
- Per-service Deno isolate reuse: `deno.reuse_isolates` in the service config, and
  `21ci service create|update --reuse-isolates`.
- Limits for reused isolates: `deno.reuse_max_idle`, `deno.reuse_max_idle_total`, `deno.reuse_idle_ttl` and
  `deno.reuse_max_uses`.

#### Changed

- Requests may use 7/8 of `sched.max_inflight`; the rest is reserved for service calls and builds. A tenant can hold
  at most half of the node's queue, and an explicit `scaling.max_queued` is clamped to the service's share.
- `Retry-After` on `503 queue_timeout` is now the estimated time for the queue to drain (1 to 60 s, with jitter). A
  client that disconnects while waiting gives up its place.
- `scaling.max_concurrency` is now called `scaling.max_instances` (the old name is still accepted); `0` means "no
  explicit cap".
- The autoscaler runs on its own thread, and services with almost no demand get no warm instances.
- Changing a service's `environment` needs the operator token or a key with no project or environment restriction, and
  is recorded as an event.
- A service of another tenant can only be admitted in `call.allowed_callers` by its service id.
- Custom host claims keep their token and expiry when claimed again. After an unverified claim expires, the same
  tenant must wait one more TTL (`409 host_cooldown`), and a tenant can have at most 10 pending claims
  (`hosts.max_pending_per_tenant`).
- `SIGHUP` refuses to drop a plan that tenants still use. While a control change is being applied, invokes may get
  `503 route_stale` (retry after 1 s).
- A TCP internal listener now needs `listeners.allow_tcp_internal = true`.
- WASM guest clocks advance only after a real wait, in whole milliseconds.

#### Fixed

- Nested service calls no longer deadlock behind a tenant's concurrency cap, and eager builds no longer lift that cap.
- A killed or failed streamed response now aborts the body instead of ending as a clean `200`.
- Environment variables are fetched before the handler runs; a failure throws `env_unavailable` instead of blocking.
- CPU time of WASM tasks with streamed responses is measured and billed again.
- Idle WASM instances share one node-wide slot budget, so one tenant's idle instances cannot exhaust the pool.
- A cancelled request rolls back its open SQL transaction; SQL result caps count the real in-memory size; a service
  database grows only within the tenant's remaining storage quota.
- Version cleanup keeps artifacts used in the last 30 minutes, and an hourly sweep removes leftovers of failed
  builds. A crash while rewriting the disk cache no longer looks like tampering.

#### Security

- Deno memory limits also cover resizable `ArrayBuffer`s, buffers the runtime creates (KV and fetch bodies,
  `TextEncoder` output) and `Blob` data.
- `Atomics.wait` is disabled and shared WebAssembly memories are refused in Deno guests.
- Guest SQL cannot create temporary tables, views or triggers.
- Failed-authentication limits group IPv6 clients by /64 and keep separate budgets for the control and public
  listeners.
- `rt secrets reseal` writes with compare-and-swap, so a concurrent secret update is not overwritten.
- Parked WASM instances refuse every `std:host` call. Decrypted environment values are evicted after 10 minutes idle and
  when a tenant is disabled or deleted.

### 2026-10-06: queueing instead of 429, warm reuse, call access, faster request path

#### Added

- Requests over capacity wait for a slot instead of getting `429`. Each service has a FIFO queue and tenants take
  turns; a request that waits longer than `sched.queue_timeout` (1 s in `min_latency`, 5 s in `balanced`, 10 s in
  `max_density`) gets `503 queue_timeout` with `Retry-After`. The node bounds its queue with `sched.max_queued` and
  `sched.max_queued_mb` (`503 overloaded`). See [Scheduler and autoscaling](../concepts/scheduler.md).
- Adaptive per-service concurrency, and warm pools that follow recent demand within `sched.warm_share` of the memory
  budget.
- Opt-in warm isolate reuse for Deno (`deno.reuse_isolates`, on by default in `min_latency`).
- WASM: per-service `wasm.reuse_instances`, streamed responses, `std:host/call` and `std:host/queue`, and a warm pool of
  pre-created instances.
- Deno request bodies stream into the handler as a `ReadableStream`.
- Service environments (`environment`, default `production`) and `call.allowed_callers`; refused calls and enqueues
  get `403 call_not_allowed`. New CLI options `21ci service create --env --allowed-caller` and `21ci service update`.
- Custom host verification by a DNS TXT record at `_rt-verify.<host>` (`hosts.verify_dns`), and expiry of pending
  claims (`hosts.pending_ttl`).
- Master key rotation with `RT_MASTER_KEY_PREVIOUS` and `rt secrets reseal`.
- `ingress.backlog` (default 4096) and the `rt_ingress_conn_rejected_total` metric.
- A benchmark harness in `bench/`.

#### Changed

- A plan's `allowed_triggers` is enforced for HTTP (`403 trigger_not_allowed`), service calls, queue consumption and
  every schedule fire.
- `WebAssembly` inside Deno is limited by real memory use: allocations over `memory_mb` throw `RangeError`, and wasm
  memories are sampled so a task over budget ends with `oom`.
- `SIGHUP` reloads `[plans]` instead of stopping the node.
- Debug builds without `RT_ROOT_TOKEN` refuse a control listener that is not on loopback.
- Guest SQL results are capped at 10,000 rows or 10 MiB per call (`sql_result_too_large`).
- Uploads clean up old versions beyond `deploy.retain_versions`, including vendored modules no version uses.
- Disabling a tenant removes its services from routing and re-checks its cached keys.
- `blocking.threads` defaults to the larger of 4 and the number of cores.
- Plan `custom_host_suffixes` match on a DNS label boundary.
- The compiled-artifact disk cache moved to a new layout (`cache/v2/`); older cache directories are removed at startup
  and services rebuild on demand.

#### Fixed

- Interval, cron and queue tasks keep their own default limits (300 s wall time, 10 s CPU) when the service sets none,
  instead of the HTTP limits.
- A late queue acknowledgement no longer settles a newer delivery attempt.
- Rebuilds use the version's `rt.lock` and its vendored modules, so they reproduce the first build. Single-file
  JavaScript uploads are no longer rebuilt as TypeScript.
- Async `WebAssembly.compile` and `WebAssembly.instantiate` in Deno now complete.
- A Deno task killed while waiting on I/O stops right away.
- Schedules keep their next due time across restarts.

#### Security

- The SSRF guard blocks 6to4 addresses by their embedded IPv4 address, Teredo, `192.0.0.0/24`, `198.18.0.0/15`,
  `240.0.0.0/4` and `fec0::/10`, and adds the node's own interface addresses to its denylist.
- Redirects from `https` to `http` are refused unless `egress.allow_https_downgrade` is set. Outbound connections are
  pooled per tenant and per validated address, never shared between tenants.
- One failed-authentication limiter covers the control API and `access: "key"` invokes, keyed on the real client
  address behind trusted proxies.
- The internal Unix socket is created with mode 0600 and checks the peer's uid (`listeners.internal_allowed_uids`).

#### Performance

- No metadata database round trip per invocation: env, decrypted secrets, plans and egress rules are cached in
  memory, and the node needs far fewer threads under load.
- Concurrent readers for the metadata and service databases, and SQLite group commit for KV and blob index writes.
- The Deno isolate pool is refilled between tasks, env is read on first use, and the V8 code cache is stored in a
  binary format.
- The build fetch cache is capped at 64 MiB and `build.max_deps_mb` limits a build's total dependencies. Cache access
  frequencies survive restarts, and the hottest services are preloaded at startup. Logs are written in batches.

### 2026-10-02: first implementation

#### Added

- The `rt` binary: the `/v1` control API, the invoke router, OpenAPI 3.1 with a Scalar UI, drain, and
  `rt openapi`.
- A Go-style scheduler with elastic processor kinds, work stealing, a monitor thread and a deterministic simulator.
- The Deno executor: a fresh isolate per task from a build-time snapshot, the `std:*` modules, a frozen clock and CPU,
  memory and wall-time limits.
- The WASM executor: wasmtime components with a pooling allocator, epoch-based preemption and
  `std:host/{log,kv,sql,blob}`.
- The build pipeline: archive-safe validation, import resolution, guarded dependency fetches with `rt.lock`,
  deterministic bundles and WASM precompilation.
- A tiered cache (memory, disk, source) with cost-aware eviction and integrity-checked disk entries.
- Storage: the metadata database, a content-addressed artifact store, per-service KV, SQL and blob storage, and a
  durable queue with a dead-letter queue.
- Triggers: the route table, interval and cron schedules, the queue engine, service calls and rate limits.
- Security: tenant API keys with scopes, AES-GCM sealed secrets, quotas, the SSRF guard, and the memory governor.
- Observability: the log store with SSE streaming, latency statistics, Prometheus metrics and scheduler traces.
- The `21ci` CLI for contexts, tenants, keys, services, push, rollback, env and secrets, schedules, queue, logs, invoke
  and node operations.
- Dockerfile, Compose file, scripts and Makefile; runnable samples; user guides for the API, handlers and deployment.

#### Fixed

- Per-client bursts in ingress rate limits are honored.
- The `rt.json` import map is applied during builds.
- A queue retry delay is honored instead of the message being acknowledged.
- Cross-tenant calls work when the plan allows them.
- Waiting requests resume only after the build result is recorded; called services receive the base-path and trigger
  headers.
- The log filter reads `RT_LOG` first, then `RUST_LOG`.
- The configuration tools match the runtime's loader for every profile.

### 2026-10-01: specification and configuration tools

#### Added

- The design specification, a security and performance review, and the implementation plan.
- The configuration schema, profile defconfigs (`max_density`, `balanced`, `min_latency`) and the `make menuconfig`
  terminal UI.
