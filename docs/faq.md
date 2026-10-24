---
title: Frequently asked questions
description: Short answers to the questions people ask most about 21 Cloud Functions Runtime, with links to the pages that go deeper.
---

# Frequently asked questions

Short answers to common questions from developers who deploy services and operators who run a node. Each answer links
to the page that explains the mechanism in full.

## General

### What is 21 Cloud Functions Runtime?

It is a self-hosted, multi-tenant serverless runtime. You run one binary (`rt`) in one container with one data
volume, and your tenants deploy TypeScript/JavaScript services (run in Deno V8 isolates) or WebAssembly components
(run by wasmtime) through a REST API or the `21ci` CLI.

Every service gets an HTTP route, interval and cron schedules, a durable queue with a dead-letter queue, its own KV,
SQL (SQLite) and blob storage, environment variables, sealed secrets and calls to other services. The platform runs
on your servers and you are the operator. See [Architecture](concepts/architecture.md) for how the pieces fit
together.

### Who is it for?

Two groups of people use it:

- **Operators** who host other people's code: a product that lets its users add webhooks, bots, integrations and
  automations, or an internal platform that needs many small services without a container per function. Operators
  install the node, define plans and quotas, and create tenants.
- **Developers** (tenants), including their AI agents, who write and deploy services through the API the operator
  gives them.

If you only need to run a handful of your own functions and do not need tenants, plans or quotas, a single-tenant
setup works too: create one tenant for yourself. Start with [Get started](getting-started/index.md).

### Is it ready for production?

The core path is implemented and tested: both executors, all triggers, storage, the scheduler, the control API and
the CLI. The current version is 0.1.0 and there is no tagged release yet. Some hardening from the design is not in
this release: the V8 sandbox, the supervisor/executor process split and a seccomp filter. The node is also a single
machine with no clustering or failover.

That makes it a good fit today for trusted or semi-trusted tenants and for platforms where you control who deploys
code. Read [Status and roadmap](about/status.md) before you put mutually hostile tenants and sensitive data on one
node.

### What do I need to run it?

A Linux x86_64 or arm64 host with Docker (Compose v2 and BuildKit), or a Linux host where you run the `rt` binary
under systemd. macOS works for development with `make dev`, but not as a production host. The node detects the
container's or cgroup's CPU and memory limits and sizes itself from them; [Resource requirements](guides/resources.md)
explains the sizing rules and how to measure the cost of your own services. Building the image the first time
downloads V8 and compiles the workspace, which takes 20 minutes or more. See [Install](getting-started/install.md)
and [Self-hosting](guides/self-hosting.md).

## Running code

### Deno or WASM: which should I use?

Use **Deno** for services you want to write quickly: webhooks, bots, API glue and code an AI assistant writes for you.
You get TypeScript, standard Web APIs, `npm:` and `jsr:` packages, and the `std:*` platform modules. Use **WASM** for
hot paths that need low per-request overhead and a small memory footprint: gateways, auth checks and stateless
high-load microservices, written in Rust, Go (TinyGo), C, Python, or JavaScript compiled with `jco componentize`.

| | <span class="rt-badge rt-badge--deno">Deno</span> | <span class="rt-badge rt-badge--wasm">WASM</span> |
|---|---|---|
| You write | TypeScript or JavaScript | Anything that builds a WASI Preview 2 component exporting `wasi:http/incoming-handler` |
| Dependencies | `npm:`, `jsr:`, allowlisted `https:` imports, bundled at build time | Whatever you compile into the component |
| What a fresh task costs | A new V8 isolate plus evaluating your module top level | A new instance from a pre-compiled component, plus your start-up code |
| What warm reuse skips | Isolate creation and top-level evaluation | Instantiation and start-up code |
| Typical artifact size | Kilobytes of source; the bundle includes your npm dependencies | Tens of KB (Rust) to tens of MB (embedded language runtimes) |

You can mix both runtimes in one tenant, and services call each other the same way regardless of runtime.
[Performance characteristics](concepts/performance.md) explains the costs, [Languages and
frameworks](languages/index.md) lists what works on each runtime, and the [Deno guide](guides/deno.md) and the
[WASM guide](guides/wasm.md) cover each one.

### Does npm work?

Yes, for most packages that run in a browser-like environment. You import `npm:pkg@range` or `jsr:@scope/pkg@range`,
and the runtime resolves it **at build time** through esm.sh (`build.npm_cdn`, `?target=es2022`), records every
fetched module's hash in the version's `rt.lock`, and vendors the modules into the artifact store. Nothing is fetched
at request time, and rebuilds reproduce the same dependencies. Bare specifiers such as `zod` work through an `rt.json`
import map, and `https:` imports only from hosts in `build.allowed_hosts`.

What does not work: `node:` imports in your own code are rejected at deploy with `422`, and Node built-ins exist only
as far as esm.sh polyfills them for that target. Packages that need real sockets, the file system or processes
(`node:net`, `node:fs`, `child_process` and similar) do not work, and native addons are not supported. A Node
compatibility layer is still an open decision. Test your dependencies early: [Deno languages and
frameworks](languages/deno.md) has a table of common frameworks and SDKs and a checklist for deciding whether a
package will work, and `sample/deno/npm-zod/` in the runtime repository is a working example. Details:
[Services, versions and builds](concepts/services-and-deploys.md).

### Which languages can I use for WASM?

Any toolchain that produces a WASI Preview 2 (0.2) **component** exporting `wasi:http/incoming-handler`: Rust with the
`wasm32-wasip2` target and JavaScript through `jco componentize` are the paths the samples use; Go through TinyGo,
Python through `componentize-py` and C through wasi-sdk are possible too. Core modules, components without that
export, components built for WASI 0.3, and components that use threads or shared memory are rejected with
`422 invalid_artifact`. The upload limit is 50 MB. [WASM languages](languages/wasm.md) has the per-language
requirements and limits.

Languages that ship a large runtime inside the component (for example Python compiled to WASM) pay their start-up
cost on every fresh instance, because build-time pre-initialization is not in this release. Turn on instance reuse
(`wasm.reuse_instances`) for such services. See the [WASM guide](guides/wasm.md) and the [`std:host` WIT
reference](reference/wit.md).

### Do I need the CLI?

No. Everything goes through the REST control API, which is described by OpenAPI 3.1 (`GET /v1/openapi.json`) and
browsable in the Scalar UI at `/docs` on the control listener. `curl` and any HTTP client work, and so does
`sample/scripts/deploy.sh` in the runtime repository.

`21ci` is a convenience: one small binary with no runtime dependencies that covers every control API operation
(contexts, `doctor` and `whoami` to check a setup, `push` with archive building and `--wait`, `rollback`, `logs -f`,
`--json` output and `--retry` for scripts, and so on). You build it from the runtime repository with
`make cli-install`. See the [CLI reference](cli.md) and the [API reference](api.md).

### Can an AI assistant deploy and manage my services?

Yes. The node serves the Model Context Protocol at `POST /mcp` on the control listener. Claude Code and Cursor connect
to it as an HTTP MCP server with a tenant key in the `Authorization` header; Claude Desktop and other clients that
start a local program use the `21ci mcp` bridge. The assistant gets tools to list and create projects and services,
deploy TypeScript or JavaScript, roll back, read logs and stats, set env variables and secrets, and manage tokens,
limited to what its key's scopes allow. Give it a token made for it with `21ci token create` (narrow scopes, an
expiry). See [AI assistants (MCP)](mcp.md).

### How do I organise many services, or staging and production?

With projects and environments. A project groups services and lists the environments they may be in; every tenant
starts with a `default` project and its `production` environment. Create `21ci project create web --env
production,staging`, then create services with `--project web --env staging`. Services of the same project in the same environment may
call each other by default (within the same project). Slugs are unique on the node, so name copies `api` and `api-staging`. See
[Projects and environments](guides/projects.md).

### Can services call each other?

Yes. A Deno service calls another with `call(slug, request)` from `std:services`, a WASM service with `std:host/call`.
The call runs in-process, without an HTTP hop; the child's wall-time limit is the smaller of its own and the caller's
remaining time, and call chains stop at depth 15 (`508 call_depth`). A failing child returns a 5xx response rather
than throwing. Services can also enqueue work for each other with `std:queue` / `std:host/queue`.

By default a service accepts calls and enqueues from services of the same tenant and project **in the same environment**
(`environment` label, default `production`). The target can list exactly who may call it in `call.allowed_callers`
(slugs, `svc_…` ids or `env:<name>`); everyone else gets `call_not_allowed`. See
[Service calls and queues](guides/calls-and-queues.md).

### Can a service call a service of another tenant?

Only if you, the operator, allow it. Cross-tenant calls are refused with `cross_tenant_call` unless the caller's plan
sets `allow_cross_tenant_calls = true`, and even then the target must list the caller **by its service id** in
`call.allowed_callers`. Both sides have to agree. See [Tenants, plans and keys](concepts/tenants-and-plans.md).

### Does it support WebSockets?

Yes. A service accepts an upgrade with `sockets.accept(req)` (`std:sockets`); the node holds the socket and runs
one task per message, close or error, so no isolate stays alive between messages. A **durable service** keeps
state per object (a room, a document) with its own storage and alarm, much like Cloudflare Durable Objects, and
sockets survive deploys. Guests can also connect out with `new WebSocket(url)`. See
[WebSockets and durable objects](guides/websockets.md). For one-way live updates, streaming the response body or
long polling, as the [live queue tutorial](tutorials/live-queue.md) does, still works too.

### Are there email triggers?

No. The triggers are HTTP (path route, custom host or `{slug}.{base_domain}`), interval and cron schedules, the
durable queue, service calls, WebSocket events and durable-object alarms. Email triggers are not part of this release. See [Triggers](concepts/triggers.md).

## Isolation and security

### Why not a Docker container per function?

Because a container per function makes every idle service pay for a process, a runtime and an image, and every cold
request pay for starting them. Here a service that is not running costs a row in a database and some files; a request
creates a V8 isolate or a WASM instance inside the already-running node. A deploy is an upload, with no image to build
or registry to run.

The trade-off is the isolation boundary: a container or microVM is a stronger wall than an isolate inside a shared
process, and it runs any Linux program. If some tenants must be strictly separated, run them on separate nodes. The
full comparison, with the rows that matter (boundary, cold start, density, compatibility, operations), is on
[Isolation compared](concepts/isolation-compared.md).

### How is tenant code isolated?

Every task runs in its own sandbox: a fresh V8 isolate for Deno (no heap shared across requests or tenants) and a
fresh wasmtime instance for WASM. When you turn on warm reuse, an isolate or instance is only reused for the same
tenant, service and version, and it is discarded after any error, kill or limit. Around that:

- **Limits per task:** CPU time, memory and wall time, enforced by the runtime (`502 cpu_limit`, `502 oom`,
  `504 timeout`).
- **No escape hatches in the guest:** Deno guests have no `Worker`, `SharedArrayBuffer`, `Atomics.wait` or
  `ShadowRealm`, `WebSocket` only connects out through the egress guard, and `Deno` is reduced to `Deno.env`. WASM components get no threads, no preopened
  directories and no sockets.
- **Frozen clock:** `Date.now()`, `performance.now()` and WASI clocks advance only at I/O and timer boundaries, which
  removes local high-resolution timers.
- **Egress guard:** every outbound request goes through an SSRF guard that denies loopback, private and metadata
  addresses and mail ports.
- **Data separation:** each service has its own SQLite file and blob directory; secrets are sealed with AES-256-GCM
  and are write-only through the API; API keys are stored hashed.

The [Security model](concepts/security.md) page explains each layer.

### What isolation features are missing in this release?

Three parts of the design are not in this release:

- **V8 sandbox.** The node uses a prebuilt V8 without the V8 sandbox, and `GET /v1/node/status` reports
  `v8_sandbox: false`. A bug in V8 would expose the whole process. <span class="rt-badge rt-badge--planned">planned</span>
- **Supervisor/executor split.** Everything runs in one process (`--role=all`), so the master key and decrypted
  secrets share an address space with tenant code. <span class="rt-badge rt-badge--planned">planned</span>
- **seccomp filter and Landlock.** With `security.seccomp = true` the node only applies `PR_SET_DUMPABLE=0`,
  `NO_NEW_PRIVS` and `RLIMIT_CORE=0` on Linux. A full syscall filter is not on the current plan.

Until the first two ship, the per-task isolate is the main boundary. Do not mix mutually hostile tenants with
sensitive operator data on one node. [Status and roadmap](about/status.md) tracks these items.

### Can tenant code reach my internal network?

Not through `fetch`, `wasi:http` or sockets. Outbound HTTP passes the SSRF guard, which resolves the host once, pins
the address and denies loopback, private ranges, link-local and cloud metadata addresses (including IPv6 forms that
embed them), the node's own listeners and configured endpoints, and ports 25, 465 and 587. You can deny more with
`egress.denied_hosts`, `egress.denied_cidrs` and `egress.denied_ports`, and plans or services can restrict egress
further. When tenants should reach your own platform, give them an **operator SDK** instead: it runs in the runtime and calls
your API through a **binding**, which adds your credentials upstream; no guest ever sees them. See
[Operator SDKs and bindings](guides/operator-sdks.md).

## Performance and scaling

### What about cold starts?

A request can be cold at three levels, and each has its own fix:

| Where the version is | What the request pays | How to avoid it |
|---|---|---|
| Not compiled yet (T3, source only) | A build: transpile, fetch dependencies, bundle, compile. Seconds, not milliseconds | Build at deploy: `?compile=eager`, `21ci push --wait`, or the `min_latency` profile (eager by default) |
| Compiled on disk (T2), not in memory | A load from the disk cache with an integrity check | `min_latency` preloads the most used services at startup and keeps them in memory longer |
| In memory (T1) | Creating a fresh isolate or instance for the task | Warm reuse: `deno.reuse_isolates`, `wasm.reuse_instances` |
| Idle long enough to be scaled to zero | No warm instance is left | `scaling.min_instances` keeps that many warm, and the node pre-loads the live version at start and on deploy |

With the default lazy builds (`balanced`, `max_density`), the first request after a deploy compiles the version; if
that build fails, the request gets `502 build_failed` and the live pointer reverts to the previous healthy version.
After a restart, the first request per service is usually a T2 hit.

The per-request cost matters most for Deno: a fresh isolate costs CPU on every request, and the pre-created isolate
pool hides that latency but not the CPU. Warm reuse (`deno.reuse_isolates`) removes most of it. WASM instances are
cheap to create from a pre-compiled component, but a component that embeds a language runtime pays its start-up code
per instance. Every response carries a `server-timing` header with `queue`, `wait_runtime`, `init` and `exec`, so you
can see where the time went. Details: [Performance characteristics](concepts/performance.md),
[Caching](concepts/caching.md) and [Performance tuning](guides/performance.md).

### Why do I get 503 queue_timeout and never 429 for capacity?

Because busy capacity is not an error. When the node, the tenant's plan or the service is at its concurrency limit,
a new request **waits** in its service's FIFO queue instead of being rejected; tenants take turns as slots free up,
then the services within a tenant. A request that waits longer than `sched.queue_timeout` (1 s in `min_latency`, 5 s
in `balanced`, 10 s in `max_density`) gets `503 queue_timeout` with a `Retry-After` header estimated from how fast the
queue drains. If the queue itself is full (`sched.max_queued`, `sched.max_queued_mb`, or a service's
`scaling.max_queued`), you get `503 overloaded` right away.

`429` is reserved for limits someone chose: plan quotas (`rps`, `invocations_per_day`), a service's ingress
`rate_limit`, repeated failed authentication, and control API quotas such as `build_quota` and `queue_full`. If you
see `503 queue_timeout` often, look at the service's admission state (`21ci stats <svc>`, or `admission` in
`GET /v1/services/{s}/stats`: its adaptive limit, queued requests and queue wait) and, as the operator, at
`21ci node scheduler`. Usually the node is short of CPU or memory for its load: turn on warm reuse for Deno services,
or give the node more resources. `21ci --retry N` rides out short busy periods in scripts. See
[Scheduler and autoscaling](concepts/scheduler.md) and the [error reference](reference/errors.md#invoke-reasons-x-rt-reason).

### How do I scale?

Vertically, on one node. Give the container more CPU and memory: with `sched.processors = 0` and
`memory.budget_mb = 0` (the defaults) the node uses the cgroup's CPU quota and 85 % of its memory limit. Within the
node, each service's concurrency limit is adaptive: it grows while requests wait and shrinks under memory pressure,
rising latency or saturated processors. A service owner can bound it with `scaling.min_instances` (kept warm) and `scaling.max_instances` (hard cap). Warm pools follow
recent demand within `sched.warm_share` of the memory budget and scale to zero when a service is idle.

Pick a profile to set the overall trade-off (`max_density`, `balanced`, `min_latency`), per node or per service. For
Deno, the biggest single lever is isolate reuse. See [Performance tuning](guides/performance.md) and
[Configuration](config.md#profile-differences-at-a-glance).

### Can I run several nodes or get high availability?

Not in this release. The runtime is a single node: there is no clustering, replication, shared metadata or failover,
and schedules and queues live on that node's volume. Two nodes you start are fully independent: they share no tenants,
services or data. State survives restarts (services, versions, live pointers, schedules, queues, env and secrets are
all on the volume), so the practical setup is a container restart policy, health checks on `/healthz` and `/readyz`,
and regular backups. The data layout is meant to keep a later multi-node design possible. See
[Production deployment](guides/production.md).

## Storage and state

### Can I keep state between requests?

Not in memory, by default. Each task gets a fresh isolate or instance, so module top-level code runs once **per task**
and globals start empty every time. With warm reuse (`deno.reuse_isolates`, `wasm.reuse_instances`, both on by
default only in `min_latency`), top-level code runs once per isolate and globals or linear memory persist, but only
as a cache: an isolate is replaced after any error, kill or leftover timer, after an env or secret change, after 1,000
requests, when idle, and under memory pressure, and consecutive requests may land on different isolates.

Keep anything that matters in the per-service storage: `std:kv` for small values (with optional TTL), `std:sql` (or
its synchronous form `std:sqlite`, also importable as `bun:sqlite`) for relational data and transactions, `std:blob` for files and large objects. WASM services use the same stores through
`std:host/kv`, `std:host/sql` and `std:host/blob`. See [Storage](concepts/storage.md) and the
[guest API reference](reference/guest-api.md).

### How do I keep a counter that is safe under concurrency?

Use `kv.incr`. It adds to an integer counter and returns the new value atomically: the read and the write run in one
transaction on the service's single SQLite writer, so concurrent requests never lose an update. A missing key starts
at 0, and `ttlSeconds` applies when the call creates the counter, which gives you fixed windows for rate limits:

```ts title="main.ts"
import { kv } from "std:kv";

export default async function (req: Request): Promise<Response> {
  const hits = await kv.incr("hits:home");                     // 1, 2, 3, … across all requests
  const minute = Math.floor(Date.now() / 60_000);
  const recent = await kv.incr(`rl:${minute}`, 1, { ttlSeconds: 120 });
  return Response.json({ hits, recent });
}
```

A plain `get` followed by `set` is not atomic and can lose updates. For counters that must change together with
other rows, use an upsert inside an `std:sql` transaction. WASM components get the same operation as `incr` in
`std:host/kv`.

### Where is the data stored? Can I use S3?

On the node's local disk, under the data volume: one SQLite file per service for KV and SQL, a blob directory per
service, and the content-addressed artifact store for uploaded sources. S3 backends for artifacts and blobs are not in
this release; the `[s3]` section and `storage.blob_backend` are accepted but unused. Storage per tenant is capped by
the plan's `storage_mb`. See [Storage](concepts/storage.md).

## Operations

### How do I back up a node?

Back up `meta.db`, `queue.db`, `artifacts/`, `services/` and `blobs/` from the data volume, **and the master key**,
stored separately. `cache/` and `deps/` can be rebuilt and may be skipped. A hot `cp -r` of SQLite files is not
consistent: stop the node first, or take `sqlite3 <db> ".backup"` copies of `meta.db` and `queue.db`. A tenant (or
you) can also export one service's KV and SQL data as a SQLite file with `GET /v1/services/{s}/data/export` (scope
`data:admin`).

```bash
docker compose stop rt
docker run --rm -v rt_rt-data:/data -v "$PWD":/backup debian:bookworm-slim \
  tar czf /backup/rt-data.tgz -C /data .
docker compose start rt
```

There is no built-in backup schedule or off-site copy; that is the operator's job. See
[Production deployment](guides/production.md).

### Where do logs and metrics go?

- **Service logs:** `console.*` in Deno and stdout/stderr in WASM go to a per-service log store on the volume (kept
  for `logs.retention_days`, 7 by default, up to `logs.max_mb_per_service`, 50 MB). Read them with
  `GET /v1/services/{s}/logs`, stream them live over SSE with `?follow=1`, or run `21ci logs <svc> -f`. Build logs
  have their own stream (`21ci build-logs <svc> <version>`).
- **Per-service stats and events:** `GET /v1/services/{s}/stats` (counts, error rate, p50/p95/p99 and the admission
  state) and `GET /v1/services/{s}/events` (deploys, rollbacks, env and secret writes, host, schedule and DLQ
  operations, data exports).
- **Node logs:** JSON on stdout, filtered with `RT_LOG`, so any container log collector picks them up.
- **Metrics:** Prometheus text at `/metrics` on the metrics listener (`:9090`), next to `/healthz` and `/readyz`.
  Per-service series are off unless you set `metrics.per_service = true`.

OTLP trace export is not in this release: the `[otel]` section is accepted but nothing is exported. Requests carry a
`traceparent` header into the guest. See [Observability](concepts/observability.md) and
[Metrics](reference/metrics.md).

### How do I rotate a secret or an API key?

Set the secret again. Secrets are write-only and apply to the next task, with no redeploy; warm isolates and instances
that saw the old value are discarded.

=== "21ci"

    ```bash
    printf %s "$NEW_TOKEN" | 21ci secret set hello BOT_TOKEN
    ```

=== "curl"

    ```bash
    curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d "{\"value\":\"$NEW_TOKEN\"}" "$RT_API/v1/services/$SVC/secrets/BOT_TOKEN"
    ```

API keys rotate the same way whoever manages them: create a new key, switch clients over, then revoke the old one.
With a key that has `keys:write` you do it yourself (`21ci token create`, `21ci token revoke`, `/v1/keys`); otherwise
the operator does (`21ci key revoke`, `DELETE /v1/tenants/{t}/keys/{key}`). Revocation takes effect at once. See
[Tenants, plans and keys](concepts/tenants-and-plans.md) and the [CLI reference](cli.md#secret).

### How do I rotate the master key?

The master key seals tenant secrets and derives the API-key pepper and the compiled-cache integrity key. Rotation
takes two restarts and no downtime beyond them:

1. Back up `meta.db` and generate a new key with `openssl rand -hex 32`.
2. Restart with `RT_MASTER_KEY=<new>` and `RT_MASTER_KEY_PREVIOUS=<old>`. New secrets are sealed with the new key, old
   ones still open, existing API keys keep working, and the compiled cache rebuilds on demand.
3. Run `rt secrets reseal` with the node's configuration (`docker compose exec rt rt secrets reseal`). It is safe to
   re-run and exits `1` if a value opens with neither key.
4. Have tenants move to new API keys: key hashes cannot be re-sealed, so old keys only work while
   `RT_MASTER_KEY_PREVIOUS` is set.
5. Restart without `RT_MASTER_KEY_PREVIOUS` and destroy the old key.

If you lose the master key, sealed secrets cannot be recovered. See [Production deployment](guides/production.md).

### Does the runtime terminate TLS?

No. `rt` serves plain HTTP/1.1 and h2c. Put a reverse proxy such as Caddy or nginx in front of the **public**
listener (`:8080`) to terminate TLS, and set `ingress.trusted_proxies` to the proxy's address so client IPs from
`X-Forwarded-For` are trusted. Keep the control (`:8081`) and metrics (`:9090`) listeners on loopback or a VPN: they
have no network access control of their own. See [Production deployment](guides/production.md) and
[Custom domains](guides/custom-domains.md).

## Troubleshooting

### Why does my deploy fail with 422?

`validation_failed` means the upload was rejected before anything was built: the source does not parse, an import
specifier is not allowed (for example `node:` in your own code), or a file or archive is over a size limit.
`details.diagnostics` in the response lists file, line and column. `invalid_artifact` means a WASM upload is not a
WASI 0.2 component that exports `wasi:http/incoming-handler`. See [Deploy and roll back](guides/deploy.md) and the
[error reference](reference/errors.md).

### Why is the first request after a deploy slow, or `502 build_failed`?

With lazy builds the first request compiles the version (see [What about cold starts?](#what-about-cold-starts)). If
that build fails, the request gets `502 build_failed` and the live pointer goes back to the previous healthy version.
Read the build logs (`21ci build-logs <svc> <version>`), fix the source and upload again. `21ci push --wait`
builds at deploy time so you see the failure immediately.

### My service answers 500, 502 or 504. What happened?

When the platform itself ends a task, the response carries an `x-rt-reason` header (a handler's own responses never
do): `guest_error` (500) means the handler threw or returned something that is not a `Response`, `cpu_limit` and `oom`
(502) mean the CPU or memory limit was exceeded, `timeout` (504) means the wall-clock limit passed. Read
`21ci logs <svc>` for the stack and the response header `x-rt-task-id` to find the task. What each outcome does to
the isolate and to in-flight work is on [Failure behavior](concepts/failure-behavior.md).

### Why does my service return 404?

Either the slug or host is wrong, the service has no live version, `triggers.http` is off, or the service has
`access: internal` and you are calling the public listener. `GET /v1/services/{s}` shows the live version and the
access mode. See [Triggers](concepts/triggers.md).

### Why does `Date.now()` not advance while my code runs?

The clock is frozen for synchronous code on purpose: it removes local high-resolution timers as a side-channel
source. It advances at I/O and timer boundaries. To measure your own latency use `server-timing` and the stats, not
`Date.now()` around CPU work. See [Execution and isolation](concepts/execution.md).

### A package works locally but fails after deploy

The build resolves `npm:` and `jsr:` imports through a CDN for a browser-like target, so a package that imports Node
server modules deploys fine and throws when it calls them. The checklist and a table of common packages are in
[Deno languages and frameworks](languages/deno.md).

### I get `403 call_not_allowed` between my own services

A service accepts calls only from the same tenant, project and environment by default. Put the caller in the target's
`call.allowed_callers` (slug, service id or `env:<name>`). See [Configure a service](guides/configure.md) and
[Service calls and queues](guides/calls-and-queues.md).

### Can I reuse code written for another fetch-based platform?

Often with small changes. The default handler shape, `export default async function (req) { return new Response(…) }`,
is a plain web-standard handler. The object form `export default { fetch, scheduled, queue }` looks like a Workers-style
module, but handlers receive `(request, ctx)` and read configuration from `Deno.env`, not from an `env` argument.

Replace platform-specific storage with `std:sql`, `std:kv` and `std:blob`, and do not rely on module state surviving
between requests. See the [Deno guide](guides/deno.md) and the [guest API reference](reference/guest-api.md).

## Licensing

### What is the license?

The runtime is licensed under the **Apache License 2.0** (SPDX identifier `Apache-2.0`). The full text is in the
[LICENSE](https://github.com/21-ci/runtime/blob/main/LICENSE) file of the repository.

### Can I use it in a commercial product?

Yes. The Apache License 2.0 allows commercial use, modification and redistribution, subject to its terms (license
and notice requirements, no trademark grant). See [About](about/index.md).

### What is in the open core, and what is paid?

| Capability | Open core | Paid |
|---|---|---|
| The runtime itself: both executors, triggers, storage, scheduler, control API, `21ci` CLI | Yes | Yes |
| Supervisor/executor split and the sandboxed V8 build, once they ship | Yes | Yes |
| Executor sharding: several executor processes per node, tenant groups pinned to them | No | Yes |
| KMS or Vault secrets backend | No | Yes |
| Durable, exportable audit log | Events API and logs only | Yes |
| SSO, OIDC and RBAC for the control API | No | Yes |
| Web console and fleet management | No | Yes |

Paid modules live outside the core and plug into it through extension points, so the open core is complete on its own.

## See also

- [Status and roadmap](about/status.md): what is in this release and what is not.
- [Concepts](concepts/index.md): how the runtime works, one mechanism per page.
- [Isolation compared](concepts/isolation-compared.md): containers, microVMs, VPSes and this runtime.
- [Error reference](reference/errors.md): every `x-rt-reason` and control API error code.
