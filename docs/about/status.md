---
title: Status and roadmap
description: What works in the current release of 21 Cloud Functions Runtime, what is deferred, known limitations, and the non-goals for v1.
---

# Status and roadmap

This page lists what the current code does, what parts of the design are not in this release, and the limitations
you should plan around. It is written for operators deciding whether the runtime fits their workload, and for
developers who want to know what to expect.

!!! info "Release state"
    The current version is **0.1.0**. There is no tagged release yet: the [changelog](changelog.md) tracks the work
    by date. The runtime is a single node.

## What works in this release

| Area | What you get | Read more |
|---|---|---|
| Deno executor | TypeScript and JavaScript in a fresh V8 isolate per task, frozen clock, CPU/memory/wall limits; opt-in warm isolate reuse; streamed request and response bodies; `WebAssembly` inside JS with memory counted against the task | [Execution and isolation](../concepts/execution.md), [Deno guide](../guides/deno.md) |
| WASM executor | WASI Preview 2 components exporting `wasi:http/incoming-handler`, a fresh wasmtime instance per task, opt-in instance reuse, warm pool of pre-created instances, streamed responses, full-`std` Rust components | [WASM guide](../guides/wasm.md) |
| Platform APIs | Deno: `std:kv`, `std:sql`, `std:sqlite` (`bun:sqlite`), `std:blob`, `std:queue`, `std:services` (typed RPC), `std:sdk` (operator SDKs run by the runtime). WASM: `std:host/{log,kv,sql,blob,queue,call,sdk,…}` (RPC both ways) | [Guest API](../reference/guest-api.md), [WIT](../reference/wit.md) |
| Triggers | HTTP (`/s/{slug}/…`, custom hosts, `{slug}.{base_domain}`), interval and cron schedules, durable queue with retries and a dead-letter queue, in-process service calls with environments and allowed callers | [Triggers](../concepts/triggers.md), [Service calls and queues](../guides/calls-and-queues.md) |
| Storage | Per-service KV (with atomic `kv.incr` counters), SQL (SQLite, async `std:sql` and synchronous `std:sqlite`) and blob storage on local disk; environment variables; AES-256-GCM sealed secrets; SQL export per service | [Storage](../concepts/storage.md) |
| Builds and deploys | Lazy (first request) or eager builds; `npm:` and `jsr:` resolved at build time through esm.sh with an `rt.lock` per version; immutable versions, instant rollback, auto-rollback when a lazy build fails | [Services, versions and builds](../concepts/services-and-deploys.md) |
| Scheduling | Go-style scheduler over all cores; requests over capacity wait in a queue instead of getting `429`; adaptive per-service concurrency between `min_instances` and `max_instances`; warm pools that follow demand; profiles `max_density`, `balanced`, `min_latency` | [Scheduler and autoscaling](../concepts/scheduler.md) |
| Caching | Three tiers (memory, disk, source) with cost-aware eviction, integrity-checked disk cache, preload of hot services | [Caching](../concepts/caching.md) |
| Tenancy and security | Plans with quotas and limit maxima, projects and environments, scoped tenant API keys (self-service with `keys:write`), per-service access, ingress and egress rules (the resolved client IP is passed to handlers as `ctx.client.ip`), SSRF guard, audit events, custom hosts verified by DNS TXT record or operator approval, master key rotation | [Tenants, plans and keys](../concepts/tenants-and-plans.md), [Security model](../concepts/security.md) |
| Operations | One container and one volume, Prometheus metrics, health and readiness probes, live logs over SSE, per-service stats, drain, `rt.toml` with profiles and environment overrides, OpenAPI 3.1 with a Scalar UI, the `21ci` CLI, an MCP server for AI assistants | [Production deployment](../guides/production.md), [Observability](../concepts/observability.md) |
| Samples | Runnable Deno and WASM examples and five sample apps (Telegram bot, event sign-ups, live queue, currency rates, edge gateway) | [Tutorials](../tutorials/index.md) |

### New in this release

| Feature | What it adds | Read more |
|---|---|---|
| Projects and environments | Every tenant has projects (a `default` one to start); a project lists its environments and each service lives in one project and one of its environments | [Projects and environments](../guides/projects.md) |
| Self-service tenant tokens | A tenant key with the `keys:write` scope creates, lists and revokes keys of its own tenant (`/v1/keys`, `21ci token`), never with more scopes, projects, environments or lifetime than itself | [Get access](../guides/access.md#create-your-own-tokens) |
| MCP server | A Model Context Protocol endpoint (`POST /mcp`) on the control listener, authenticated with a tenant key, so AI assistants such as Claude Code, Cursor and Claude Desktop can manage services | [AI assistants (MCP)](../mcp.md) |
| Instance bounds | Per-service `scaling.min_instances` (kept warm, no scale to zero) and `scaling.max_instances`, bounded by the plan's `max_instances`; a trend-aware autoscaler | [Scheduler and autoscaling](../concepts/scheduler.md#instances-and-autoscaling) |
| Synchronous SQLite | `std:sqlite` (also importable as `bun:sqlite`): the service's SQLite database with bun's synchronous API, next to the async `std:sql` | [Sub-services](../guides/sub-services.md#synchronous-sql-stdsqlite) |
| WebSockets and durable objects | `std:sockets` (sockets held by the node, one task per event, tags, attachments, auto-response, sockets that survive deploys), `std:durable` (one object per name with storage and an alarm), an outbound `WebSocket` client; the same in WASM (`std:host/sockets`, `durable`, `ws-client`, `socket-handler`) | [WebSockets and durable objects](../guides/websockets.md) |

## Not in this release

These parts of the design are deferred or replaced. Items marked
<span class="rt-badge rt-badge--planned">planned</span> are expected in a later release; the others are explained in
the last column.

| Item | State in this release | What to do instead |
|---|---|---|
| Supervisor/executor process split <span class="rt-badge rt-badge--planned">planned</span> | One process; `--role=all` is the only role. The master key and decrypted secrets share the process with tenant code | Treat the node as one trust domain; run separate nodes for tenants that must be strictly separated |
| V8 sandbox (custom V8 build) <span class="rt-badge rt-badge--planned">planned</span> | Prebuilt V8 without the sandbox; `GET /v1/node/status` reports `v8_sandbox: false`; `security.require_v8_sandbox` must be `false` | Do not mix mutually hostile tenants with sensitive operator data on one node |
| seccomp filter and Landlock | Not planned for now. `security.seccomp = true` applies `PR_SET_DUMPABLE=0`, `NO_NEW_PRIVS` and `RLIMIT_CORE=0` on Linux only | Use container hardening: non-root user, `cap_drop: ALL`, `no-new-privileges` (the compose file sets these) |
| S3 backends for artifacts and blobs <span class="rt-badge rt-badge--planned">planned</span> | Local disk only; `[s3]` and `storage.blob_backend` are accepted but unused | Put the data volume on storage you back up |
| OTLP trace export <span class="rt-badge rt-badge--planned">planned</span> | `[otel]` is accepted; nothing is exported | Prometheus metrics, service logs, `server-timing` and `traceparent` headers |
| libSQL | SQLite through `rusqlite`, same file format | Nothing; the `std:sql` API is the same |
| WASM pre-initialization at build time <span class="rt-badge rt-badge--planned">planned</span> | Components run their start-up code on every fresh instance | Turn on `wasm.reuse_instances` for components with heavy start-up (for example language runtimes) |
| Streamed bodies for WASM blob and call APIs <span class="rt-badge rt-badge--planned">planned</span> | `std:host/blob` and `std:host/call` bodies are buffered | Keep those bodies small; HTTP responses already stream |
| Per-service V8 snapshots <span class="rt-badge rt-badge--planned">planned</span> | Not built: top-level code runs on every fresh isolate | Turn on `deno.reuse_isolates` to run top-level code once per isolate |
| Weighted traffic split and canaries <span class="rt-badge rt-badge--planned">planned</span> | One live version per service | Test on a separate staging service, then make the new version live; rolling back is one call |
| Multi-node, clustering, high availability | Not in v1 (see [non-goals](#non-goals-for-v1)) | Restart policy, health checks and backups on one node |

## Known limitations

These are properties of the current code that can surprise you. Most have a workaround.

**Deno throughput with the default configuration.** Every request creates a fresh V8 isolate. That fixed CPU cost
per request, not your handler, bounds how many requests per second a core can serve. Warm reuse
(`deno.reuse_isolates`, on by default in `min_latency`) removes that cost. See
[Performance characteristics](../concepts/performance.md) and [Performance tuning](../guides/performance.md).

**npm coverage.** `npm:` packages are built by esm.sh for an ES2022 target. `node:` imports in your own code are
rejected at deploy, and Node built-ins exist only as far as esm.sh polyfills them, so packages that need `node:net`,
`node:fs`, `child_process` and similar do not work. How many of the common API SDKs (openai, stripe, axios, pg, …)
work has not been measured yet; a Node compatibility layer is an open decision. Test your dependencies before you
commit to them.

**Storage-heavy services share one thread pool.** Storage calls (SQL, KV, blob files) run on a shared pool. Under
contention one tenant holds at most `floor(blocking_share × threads)` of its threads (plan key `blocking_share`,
default 0.25); when threads are idle, a busy tenant may borrow them, always leaving one free. For nodes with few,
storage-heavy tenants, raise `blocking_share` in the plan or `blocking.threads`. See
[Scheduler and autoscaling](../concepts/scheduler.md#blocking-pool-for-service-data) and
[Configuration](../config.md#service-data-storage-blocking).

**Frozen clock.** `Date.now()` and `performance.now()` do not advance during synchronous code, so code that times its
own CPU work measures 0 ms. This is a security measure; `security.clock = "coarse"` is the escape hatch for nodes that
need it.

**Single-process blast radius.** All tenants share one process and one memory pool. A crash or an engine
vulnerability affects every tenant on the node. Executor sharding for tenant groups is a paid capability (see
[About](index.md#editions)).

**Control and metrics listeners have no access control of their own.** Keep them on loopback or a VPN, and put TLS and
a reverse proxy in front of the public listener only.

**Backups need a stopped node or SQLite's backup command.** A hot file copy of the databases is not consistent. Drain
(`POST /v1/node/drain`) is one-way: restart the node to accept traffic again.

**WebSockets live on one node.** The node holds every socket; a node restart drops them (clients reconnect, durable
storage and alarms stay). No WebSockets over HTTP/2 and no `permessage-deflate`. Outbound sockets close when the task
that opened them ends. See [WebSockets and durable objects](../guides/websockets.md#not-supported).

## Open questions

Decisions that are still open, with the behavior that applies until they are answered:

| Question | Current behavior |
|---|---|
| A Node built-ins subset (`node:buffer`, `node:events`, …) | Only what esm.sh polyfills |
| Priority classes in run queues (latency-critical vs batch) | None; choose a profile per service |
| Periodic process recycling to reshuffle memory against slow side-channel attacks | Off |
| Disallowing `eval` and `new Function` | Allowed, because many libraries compile validators at runtime |
| Built-in backups to S3 | Operator's responsibility; per-service export exists |

## Non-goals for v1

These are out of scope for the first version by design, not missing work.

| Non-goal | Note |
|---|---|
| Hosted SaaS, multi-region, multi-node clustering, high availability | One node; the data layout keeps a later cluster possible |
| Web console | A paid management feature. The REST API, OpenAPI and the `21ci` CLI are the interfaces |
| Full Node.js compatibility, native addons | npm through esm.sh builds only |
| Email triggers | Not a trigger kind |
| Module state that survives across Deno requests by default | A fresh isolate per task is the default; reuse is an opt-in cache, not a guarantee |
| Built-in TLS or ACME | Terminate TLS in your reverse proxy |
| Billing | Usage counters exist for quotas only |
| Process-per-tenant sharding, KMS, SSO/RBAC, durable audit log | Paid editions |

## See also

- [Changelog](changelog.md): what changed and when.
- [FAQ](../faq.md): short answers about isolation, cold starts, scaling and operations.
- [Security model](../concepts/security.md): what each isolation layer does today.
- [Isolation compared](../concepts/isolation-compared.md): what this runtime gives up and gains against containers, microVMs and VPSes.
