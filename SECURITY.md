# Security

This page describes every security control the runtime has today, what each one protects, and what is **not**
protected. It describes what the code in `crates/**` does.

Short version: one `rt` process runs untrusted tenant code in **Deno isolates** and **wasmtime components**. Every
tenant is isolated by the language sandbox, per-task resource limits, an egress guard, and per-service storage files.
**There is no OS-level isolation between tenants yet** (no separate processes, no seccomp, no V8 sandbox). Read
[Known limitations](#known-limitations) before you run code you don't trust.

---

## Contents

1. [Threat model and trust boundaries](#threat-model-and-trust-boundaries)
2. [Listeners](#listeners)
3. [Authentication](#authentication)
4. [Tenancy, plans and quotas](#tenancy-plans-and-quotas)
5. [Projects, environments and service calls](#projects-environments-and-service-calls)
   - [Operator SDKs and bindings](#operator-sdks-and-bindings)
6. [Request path: headers and checks](#request-path-headers-and-checks)
7. [Deno sandbox](#deno-sandbox)
8. [WASM sandbox](#wasm-sandbox)
9. [Egress / SSRF guard](#egress--ssrf-guard)
10. [WebSockets and durable objects](#websockets-and-durable-objects)
11. [Storage isolation](#storage-isolation)
12. [Secrets](#secrets)
13. [Builds and supply chain](#builds-and-supply-chain)
14. [MCP endpoint](#mcp-endpoint)
15. [Observability and audit](#observability-and-audit)
16. [Hardening settings](#hardening-settings)
17. [Known limitations](#known-limitations)
18. [Operator hardening checklist](#operator-hardening-checklist)
19. [Reporting a vulnerability](#reporting-a-vulnerability)

---

## Threat model and trust boundaries

| Actor | Trust | What they can do |
|---|---|---|
| **Operator** | Full | Holds the root token, the master key and the host. Creates tenants, sets plans. |
| **Tenant** | Partial | Holds `rtk_` keys. Manages only its own services, within its plan and key scopes. |
| **Guest code** | None | Tenant code running in an isolate or component. Treated as hostile. |
| **Internet** | None | Calls public routes, and may attack the control API if it is exposed. |

```
                      INTERNET (untrusted)
                             │
          ┌──────────────────┼────────────────────────────────┐
          │ reverse proxy / VPN (yours)                        │
          └──────────────────┼────────────────────────────────┘
   ══════════════════════════╪═══════ boundary 1: network ═══════════
                             ▼
  ┌──────────────────────── rt process ─────────────────────────────┐
  │  public :8080      control :8081      metrics :9090    rt.sock   │
  │      │             (root / rtk_)      (no auth)       (peer uid) │
  │      ▼                  │                                 │      │
  │  auth + access ◄────────┘                                 │      │
  │      │                                                    │      │
  │ ═════╪══════════ boundary 2: tenant ══════════════════════╪════  │
  │      ▼                                                    ▼      │
  │  ┌──────────┐ ┌──────────┐ ┌──────────┐    host ops (KV, SQL,   │
  │  │ isolate  │ │ isolate  │ │ wasm     │    blob, queue, call,   │
  │  │ tenant A │ │ tenant B │ │ tenant C │    fetch) each checked  │
  │  └────┬─────┘ └────┬─────┘ └────┬─────┘                         │
  │ ══════╪════════════╪════════════╪═══ boundary 3: sandbox ═════  │
  │       └────────────┴──────┬─────┘                               │
  │                    egress guard  ──► internet                   │
  │   master key · meta.db · per-service data.db (same process)     │
  └─────────────────────────────────────────────────────────────────┘
```

Boundary 3 (the sandbox) is the most important one. Every tenant's code runs in the **same OS process** as the
control plane and the master key, so a V8 or wasmtime escape compromises the whole node.

---

## Listeners

The `[listeners]` section sets four listeners. A value is `host:port` or `unix:/path`.

| Listener | Default | Serves | Auth |
|---|---|---|---|
| `public` | `0.0.0.0:8080` | `ANY /s/{slug}/{path}` and custom hosts (invoke) | Per service: `access` rule, or an `rtk_` key with `invoke` |
| `control` | `127.0.0.1:8081` | `/v1/*`, `/v1/openapi.json`, `/docs`, `POST /mcp` | Root token or `rtk_` key |
| `metrics` | `127.0.0.1:9090` | `/metrics`, `/healthz`, `/readyz` | **None** (everyone is anonymous) |
| `internal` | `unix:/data/rt.sock` | Trusted invoke for platform calls | Unix peer UID |

### Internal Unix socket

```
 client ──connect──► rt.sock (mode 0600)
                        │
                        ▼
              peer_cred()  (SO_PEERCRED / getpeereid)
                        │   before any byte is read
           ┌────────────┴─────────────┐
   uid == node uid               uid in internal_allowed_uids?
   or listed                         no / unreadable
           │                              │
           ▼                              ▼
   trusted invoke:                 disconnect, no response,
   no access check, no ingress     warning logged
   rules, x-rt-invoker honored
```

- The socket file is created under a temporary name, set to mode `0600`, then renamed into place. Any failure
  stops startup.
- **Root is not allowed by default.** Every UID other than the node's own, root included, must be listed in
  `listeners.internal_allowed_uids`.

### Internal over TCP (opt-in)

- `internal = "127.0.0.1:…"` refuses to start unless `listeners.allow_tcp_internal = true`.
- Even with that set, the address must be loopback.
- TCP has **no peer check**. Every local user on the host gets trusted invokes and can set `x-rt-invoker`. Use
  the Unix socket unless you can't.

### Exposure

`control` and `metrics` have **no network ACL of their own**. Keep them on loopback, or put them behind a VPN or
an authenticating proxy. The container image binds every listener to `0.0.0.0`, and `docker-compose.yml`
publishes them on host `127.0.0.1` only.

---

## Authentication

### Credentials

```
                 ┌──────────────────────────────┐
                 │  ROOT TOKEN  (operator)       │  RT_ROOT_TOKEN / _FILE
                 │  all tenants, x-rt-tenant hdr │  Bearer only
                 └──────────────┬───────────────┘
                                │ creates tenants + keys
                                ▼   /v1/tenants/{t}/keys
                 ┌──────────────────────────────┐
                 │  TENANT KEY  rtk_<ulid>_<43>  │  scopes ⊆ all
                 │  scopes · services · expiry   │
                 └──────────────┬───────────────┘
                                │ keys:write → POST /v1/keys
                                ▼   (child ⊆ parent)
                 ┌──────────────────────────────┐
                 │  CHILD KEY   parent_key_id    │  scopes ⊆ parent
                 │                               │  services ⊆ parent
                 │                               │  expiry ≤ parent
                 └──────────────┬───────────────┘
                                ▼  revoking a parent revokes the whole subtree
                               ...
```

| Credential | Format | How it is stored | Accepted as |
|---|---|---|---|
| Root token | Any string | SHA-256 digest only. The plaintext is zeroized. Compared in constant time. | `Authorization: Bearer` |
| Tenant key | `rtk_<ULID key id>_<43 base62>` (about 256 bits, OsRng) | `HMAC-SHA256(pepper, secret)`. The pepper comes from the master key. Shown once. | `Authorization: Bearer`, then `x-rt-key` |

- **No root token, release build:** the control API answers `503 control_disabled`.
- **No root token, debug build:** the control API is **open** (a request without credentials is the operator). The
  node prints a "CONTROL API OPEN" banner and **refuses to start** if `listeners.control` is not loopback. Even then
  only local tools get the operator: a request with a non-loopback `Origin` (a web page of another site) or a
  non-loopback `Host` (DNS rebinding) is `401`.
- **Same answer for every failure.** A wrong secret, unknown id, revoked key and expired key all return
  `401 unauthorized`. An unknown key id still costs one HMAC, so timing doesn't reveal which ids exist.
- **Rotation-safe.** Key verification also tries the pepper from `RT_MASTER_KEY_PREVIOUS`.

### Scopes

| Scope | Allows |
|---|---|
| `services:read` | List and read services, projects, environments, versions |
| `services:write` | Create, update and delete services and projects |
| `deploy` | Deploy, rebuild, roll back |
| `invoke` | Call `access="key"` routes |
| `logs:read` | Logs and stats |
| `env:write` | Set env vars |
| `secrets:write` | Set and delete secrets |
| `queue:write` | Enqueue, DLQ redrive and purge |
| `data:admin` | Export a consistent snapshot of a service's `data.db`; drop a durable object's storage |
| `keys:write` | Self-service keys (`/v1/keys`) |
| `sockets:write` | Close a service's WebSockets through the control API (audited `sockets.closed_by_api`) |

A key can also be **restricted to some projects and environments**: `project_ids` (`prj_…` ids; `null` means every
project of the tenant, including ones created later) and `environments` (labels; `null` means every environment). A
service is touchable by the key only when its project and its environment are both admitted, and the key can create
a service (REST or the MCP `create_service` tool) only in an admitted project and environment (`403
insufficient_scope` otherwise). A key can also **expire**
(`expires_at_ms <= now` means invalid). Project create, update and delete need a key with no project or environment
restriction, or the operator.

### Self-service keys (`keys:write`)

`POST /v1/keys`, `GET /v1/keys` and `DELETE /v1/keys/{key}` let a tenant manage its own keys without the operator.

| Rule | Behavior |
|---|---|
| Scopes | Must be a subset of the creator's |
| Projects | Must be a subset of the creator's `project_ids`. A creator restricted to some projects asking for "no restriction" gets its own list. |
| Environments | Must be a subset of the creator's `environments`, by the same rule. |
| Expiry | No later than the creator's. Asking for none inherits the creator's. |
| Violation | `403 insufficient_scope`, `{"reason":"escalation"}` |
| Count | At most 200 active keys per tenant, then `403 quota_exceeded` |
| Visibility | A restricted caller sees only keys whose project and environment lists are within its own. Anything else is `404`. |
| Revoke | You can't revoke a key holding a scope you lack (`403`). Revocation cascades to all child keys (`key.deleted` lists `cascaded`). |

### Auth cache

Verified keys are cached for **1 s** (4096 entries). Revoking a key bumps an epoch and removes its cache entry. A
lookup that raced the revocation deletes what it inserted, so a revoked key is never re-cached.

### Long-lived maps

Every per-key, per-IP, per-tenant or per-service map on the request path has a cap or an eviction: the key cache
(4096), the failed-auth limiter (`LIMITER_CAPACITY`, swept), idempotency entries (24 h, pruned past 100k), SSE
counters (credentials with no open stream dropped past 1024), per-service ingress buckets and `ip_allow` lists
(dropped with their service on every route refresh), SDK caller gates (idle tenants dropped past 10k), lock slots
(removed on release), socket tickets (swept), env bundles (idle TTL), usage counters (removed once flushed).

### Control API bodies

JSON bodies are read up to 1 MiB. Artifact uploads are read up to 64 MiB, and only after the caller passed auth,
scope and service checks: a node buffers at most 4 uploads at once, and charges each one's `Content-Length` (64 MiB
when unknown) to the memory governor before reading it. A fifth upload, or one the governor refuses, gets `429
rate_limited`.

### Failed-auth limiter

| Setting | Value |
|---|---|
| Budget | 10 failures per fixed 60 s window (the window opens at the first failure) |
| Key | Client IPv4 address, or IPv6 `/64`. IPv4-mapped addresses count as IPv4. |
| Client IP | Socket peer, or `X-Forwarded-For` only through `ingress.trusted_proxies` |
| Over budget | `429 rate_limited` with retry-after |
| Budgets | Two separate ones: control, and public `access="key"` invokes. Wrong invoke keys can't lock out the operator. |
| Size | 65,536 entries in 16 shards, sampled eviction |

On the public listener, a bad `rtk_` key is treated as anonymous (not `401`) until the client is blocked.

---

## Tenancy, plans and quotas

Every service belongs to one tenant. A tenant key can only see and change its own tenant. The operator picks a tenant
with the `x-rt-tenant` header. Each tenant has a **plan**. These are the free plan defaults:

| Limit | Free default | Limit | Free default |
|---|---|---|---|
| `max_services` | 10 | `max_schedules` | 3 |
| `max_concurrency` | 10 | `min_schedule_seconds` | 60 |
| `max_instances` | 4 | `max_queue_depth` | 10k |
| `max_warm_floor_total` | 8 | `builds_per_hour` | 20 |
| `rps` | 50 | `storage_mb` (KV+SQL+blob, tenant-wide) | 100 |
| `invocations_per_day` | 100k | `egress_mb_per_day` | 1000 |
| `limits_max` | 30 s wall · 1 s CPU · 128 MB | `allowed_triggers` | http, interval, queue, call, websocket |
| `egress_default` | `Allow` | `allow_cross_tenant_calls` | `false` |

- The quota enforcer checks service count, schedule count and frequency, allowed triggers, an rps token bucket,
  daily invocations, builds per hour, queue depth, daily egress and storage.
- A service's `min_instances` and `max_instances` are clamped to the plan's `max_instances`. The sum of
  `min_instances` across a tenant must stay within `max_warm_floor_total`, otherwise `422 limit_exceeds_plan`.
  One tenant can't pin the node's memory with warm instances.
- A per-service limit above `limits_max` is clamped and recorded as a plan conflict.

---

## Projects, environments and service calls

Projects group services, and each service has an environment (default `production`). **Projects are not a security
boundary. Environments are, but only for service calls.**

```
caller ──call──► target
          │
          ├─ caller == target ............................ allow
          ├─ different tenant?
          │     plan.allow_cross_tenant_calls = false ..... 403 cross_tenant_call
          │     else target.allowed_callers has svc_<id> .. allow
          ├─ same tenant:
          │     allowed_callers empty → same project and
          │       environment ............................. allow
          │     listed by slug, svc_ id, or env:<name>
          │       (env: names the target's project) ...... allow
          ├─ target trigger `call` off .................... 403 forbidden
          ├─ target plan forbids trigger .................. trigger_not_allowed
          └─ depth > 15 ................................... 508 call_depth
```

- `allowed_callers` holds at most 64 entries. A cross-tenant caller must be listed by its exact `svc_` id; slugs
  and `env:` don't match across tenants.
- A child call gets the smaller of its own wall limit and the parent's remaining time.
- `ctx.caller` (the callee's view of its caller: service id and slug, tenant, project, environment) is stamped by
  the call dispatcher from its own route table, never from a request header, so a caller cannot name another caller.
  The same rules admit RPC (`services.call`, WASM `call-rpc`) as admit `services.fetch`: RPC adds no path around them.
- RPC uses the reserved path `/__rt/rpc/<method>`, which only an in-process call can reach (the trigger is `call`),
  and `services.fetch` refuses `/__rt/` paths. A callee's error reaches its caller as a code and a message: its stack
  never leaves the callee. Arguments and results are capped at 256 KiB of JSON each way. A WASM callee's `rpc` export
  gets the caller as a record from the same stamp.

### Operator SDKs and bindings

- **SDK code never runs in a tenant's sandbox.** An operator SDK is a service of the reserved system tenant `sys`
  (`ten_00000000000000000000000SYS`), published by the operator only (root token). A tenant build gets a shim of the
  published contract (names only); every call is an in-process call to the SDK release its grants select. Secrets and
  module state of an SDK are never in tenant memory. The system tenant is hidden from `GET /v1/tenants`, cannot be
  changed, deleted or given keys (`403 reserved`), its SDK services are never live (no slug route, no HTTP ingress), and
  `sys-` slugs are reserved. Acting as it with `x-rt-tenant`, the operator may read but never write: any other method
  than `GET`/`HEAD` (keys, services, projects, env, secrets, schedules) is `403 reserved`.
- **Grants only narrow.** Grants are written by the operator only. Resolution walks node → plan → tenant → project →
  environment; a `deny` at any level wins, every `version_req` must hold, and a missing grant means no access
  (`sdk_not_granted`). A tenant cannot see an SDK it is not granted (`/v1/sdks/{name}/contract` is 404).
- **The caller is stamped, not claimed.** An SDK's `ctx.caller` (tenant, plan, project, environment, service, release)
  comes from the dispatcher's route table and plan source. `defineSdk` refuses methods that never read `ctx.caller` (and
  publishing refuses them statically, `sdk_unchecked_caller`) unless an `authorize` hook checks every call: the
  confused-deputy guard. An SDK keeps tenant data keyed by `ctx.caller.tenant`; its own storage is shared.
  There is no signed caller token (`x-rt-caller-token`, plan SDK-10), by decision: an SDK call never leaves the
  node's process (it is an in-process child task, not an HTTP hop), the stamp is written by the dispatcher into the
  child's task spec, and the caller sends only a method name and JSON arguments (no headers). Only the runtime itself could forge the
  stamp, so a MAC over it would add nothing. Revisit this if SDK calls ever cross nodes, or if an SDK forwards the
  caller to a system outside the node (that system needs its own credentials).
- **Bindings belong to SDKs.** `platform.fetch` from a tenant service is `403 binding_not_allowed`. From an SDK it acts
  for the calling tenant: the tenant header is the caller's (never `sys`), `allowed_plans` checks the caller's plan and
  the rate limit counts per caller, so one tenant cannot spend another's budget through a shared SDK.
- **Limits.** An SDK call is a call hop (depth 15, the caller's remaining time, the per-task call concurrency); the
  SDK task runs under its own service limits, and its CPU is recorded in the calling tenant's usage. SDK tasks run
  in `sys`, so the scheduler does not count them against the caller's tenant: the dispatcher does instead, with at
  most the caller plan's `max_concurrency` SDK calls in flight and `rps` SDK calls per second (burst `rps`) per
  calling tenant, so one tenant cannot fill the SDK services for everyone (`sdk_rate_limited`). The old `[[sdk]]`
  bundling and `preload` were removed (config refuses them).

---

## Request path: headers and checks

```
client ─► public listener
            │ header_timeout 10s · max_conns_per_ip 256 · h2 streams 100
            ▼
         resolve client IP  (XFF only via trusted_proxies, right→left)
            ▼
         route: /s/{slug} or verified custom host
            ▼
         access check: ip_allow · key (invoke scope) · failed-auth limiter
            ▼
         quota: rps bucket · invocations/day · concurrency
            ▼
         rewrite headers  ──────────────────────────────┐
            ▼                                            │ strip: hop-by-hop, Host,
         isolate / component  (limits, clock, memory)    │ all x-rt-*, rtk_/root Authorization
            ▼                                            │ add:   x-rt-base-path,
         response + x-rt-reason / x-rt-task-id /         │ x-rt-client-ip, x-rt-trigger
         x-rt-version                                    ┘
```

| Direction | Removed | Added |
|---|---|---|
| Ingress → guest | Hop-by-hop headers and any named in `Connection`; `Host`; **every `x-rt-*`** (including `x-rt-key` and a client-sent `x-rt-client-ip`); `Authorization` carrying an `rtk_` key or the root token | `x-rt-base-path`, `x-rt-client-ip` (not on the Unix socket), `x-rt-trigger: http` |
| `x-rt-invoker` | Removed on public. Kept **only** on the internal listener. | |
| Service → service call | Every `x-rt-*` | `x-rt-base-path`, `x-rt-trigger: call` |
| Guest → binding upstream | `x-rt-*`, `Authorization`, `Cookie`, `Proxy-Authorization`, `Host`, `Content-Length`, hop-by-hop. `Set-Cookie` is removed from the reply. | |
| Response → client | | `x-rt-reason`, `x-rt-task-id`, `x-rt-version` |

**Trusted proxies.** `ingress.trusted_proxies` (CIDRs, default empty) is the only way `X-Forwarded-For` is
honored. The chain is walked right to left, skipping trusted hops, and stops at the first untrusted or unparsable
entry. `ip_allow` matches IPv4-mapped addresses as IPv4. A non-empty list whose entries are all malformed matches
nobody, so it fails closed.

The `Authorization` header with any other value (for example your app's own bearer token) reaches the guest
unchanged.

---

## Deno sandbox

```
 ┌────────────────────────── Deno isolate ───────────────────────────┐
 │ guest JS / TS                                                      │
 │   globals: no Worker, SharedArrayBuffer, ShadowRealm, caches,      │
 │            SharedWorker · Deno = frozen {env}                      │
 │   Date/performance → frozen clock · fetch, WebSocket → egress guard│
 ├────────────────────────────────────────────────────────────────────┤
 │ ops (rt_ext): every op goes through the task's HostHandle          │
 │   KV · SQL · blob · queue · call · fetch(→ egress guard) · log     │
 ├────────────────────────────────────────────────────────────────────┤
 │ deno_core: webidl, web, fetch, crypto only                         │
 │   no fs, net, process, ffi, node, websocket, workers extensions    │
 ├────────────────────────────────────────────────────────────────────┤
 │ V8: heap limit + near-heap-limit callback · counting ArrayBuffer   │
 │     allocator · allow_atomics_wait(false) · terminate_execution    │
 ├────────────────────────────────────────────────────────────────────┤
 │ watchdog thread (4 ms) · memory governor · tokio wall deadline     │
 └────────────────────────────────────────────────────────────────────┘
```

### What the guest gets

- Only the `webidl`, `web`, `fetch` and `crypto` extensions plus `rt_ext`. There are **no** filesystem, network
  socket, subprocess, FFI, Node, WebSocket, cache or worker ops in the binary.
- `__rt_seal()` runs on every new isolate before guest code, then deletes itself. It:
  - removes `Worker`, `SharedArrayBuffer`, `ShadowRealm`, `caches`, `SharedWorker` (`WebSocket` is the runtime's
    outbound client through the egress guard, see [Outbound WebSockets](#outbound-websockets));
  - deletes `Atomics.wait`, `Atomics.waitAsync`, `Atomics.notify` (the `Atomics` object remains);
  - replaces `Deno` with a frozen, read-only `{ env }`.
- `fetch` is replaced by `rtFetch`, which calls `op_rt_fetch` and the [egress guard](#egress--ssrf-guard).
  deno_fetch's own HTTP client is never used.
- `eval`, `Function` and `WebAssembly` stay available. The code-generation restriction is not set.

### Clock

| Mode | `Date.now()` | `performance.now()` |
|---|---|---|
| `frozen` (default) | Advances once per event-loop poll, floored to 1 ms | Advances once per poll, sub-ms precision kept |
| `coarse` | Live time floored to 1 ms | Live time floored to 1 ms |

The clock doesn't move while JS runs without yielding, so busy-loop timing attacks get no signal inside one turn.

### Memory

- The heap starts at `max(isolate_base_mb, 16) MB`. When it is close to the limit, a callback grows it in **8 MB**
  steps through the memory governor. If the governor refuses, `terminate_execution` runs.
- Each isolate has its own **counting ArrayBuffer allocator**. A refused allocation gives the guest a `RangeError`.
  The runtime charges the larger of the allocator count and V8's `external_memory`.
- Resizable ArrayBuffers are checked before they grow. Blob parts are counted.
- **Wasm memory inside Deno** is tracked, and its `byteLength` is added to the charge. Shared wasm memory is refused.
  A process-wide `--wasm-max-mem-pages` is set from the largest plan memory.
- Over the limit, the task ends as `Killed(Oom)`.

### CPU and wall time

| Trigger | Wall | CPU | Memory |
|---|---|---|---|
| HTTP / call | 30 s | 1 s | 128 MB |
| Cron / interval / queue | 300 s | 10 s | 128 MB |
| Build | 120 s | 120 s | 256 MB |

- CPU is measured with `CLOCK_THREAD_CPUTIME_ID` while the task is polled, plus host-side CPU spent for the task.
- A global `rt-deno-watchdog` thread calls `request_interrupt` every **4 ms**. The interrupt checks memory and CPU
  and calls `terminate_execution` when either is over budget.
- The wall deadline is a tokio timer checked on every poll. Hitting it ends the task with `WallTimeout`.

### Warm reuse

Isolate reuse is **off by default**. Only the `min_latency` profile turns it on.

| Rule | Value |
|---|---|
| Pool key | (tenant, service, version, artifact). **Never shared across tenants or services.** |
| Kept only if | No kill, no guest error, no unhandled rejection, empty event loop, memory within bounds |
| Discarded on | Env/secrets change, memory grew > max(16 MiB, baseline/2), 1000 uses, 60 s idle, new version, High/Critical memory pressure |
| Caps | 4 idle per service, 16 total |
| Reset per task | CPU budget and memory account |
| **Not reset** | Module globals persist between requests of the same service version |

Don't keep per-user data in module globals when reuse is on.

---

## WASM sandbox

```
 ┌───────────────────── wasmtime component ────────────────────┐
 │ guest component (wasi:http/incoming-handler export required) │
 ├──────────────────────────────────────────────────────────────┤
 │ imports: wasi:io, clocks, random, cli, http                  │
 │          wasi:filesystem (no preopens)                       │
 │          wasi:sockets (address check denies all)             │
 │          std:host {log, kv, sql, blob, queue, call, sdk, …}  │
 │ outgoing HTTP → GuardedHttpHooks → egress guard              │
 ├──────────────────────────────────────────────────────────────┤
 │ pooling allocator · GovernorLimiter · epoch every 1 ms       │
 │ wasm_threads(false) · optional fuel                          │
 └──────────────────────────────────────────────────────────────┘
```

- **Component model only.** At deploy, a component that uses threads or shared memory is rejected.
- **No filesystem, no sockets.** `wasi:filesystem` has no preopened directories. `wasi:sockets` is linked, but the
  WASI context's address check (the wasmtime default) allows no address. Stdin is empty.
- **Host imports are gated.** `std:host` calls made outside a running task return `unavailable`. Every call goes
  through the same per-task host handle as Deno ops.
- **Pooling allocator.** Slots are `wasm_budget / plan max memory` (1 to 10,000). Each slot has 1 component
  instance and 1 memory capped at the plan max. Tables are capped at 100,000 elements.
- **Memory.** Every `memory.grow` goes through `GovernorLimiter`: the plan limit first, then a governor reservation.
  A refusal traps and ends the task as `Oom`.
- **CPU and wall.** An epoch ticker runs every 1 ms. At each `time_slice` (20/10/5 ms by profile) the runtime kills
  the task if the wall deadline, CPU budget (thread CPU + host CPU) or kill switch is hit, otherwise it yields.
  Fuel (`cpu_ms × 10,000,000`) is available but **off by default**.
- **Clock.** Monotonic and wall clocks are frozen and advance only after a poll or a host call that actually waited.
  The resolution is 1 ms.
- **stdout/stderr** go to the service log (info / error), count against `max_log_lines` (1000), and partial lines
  are capped at 64 KiB.
- **Warm reuse.** The pool key is (tenant, service, version, artifact) and never crosses tenants. Never-run
  instances can serve any task of that key. Used instances are reused only with `reuse_instances` (the
  `min_latency` profile). They are discarded on env change, High memory pressure or idle TTL.

### Memory governor (both executors)

- Budget: `memory.budget_mb`, else 85% of the cgroup `memory.max`, else 85% of RAM.
- Reservations use compare-and-swap, so concurrent tasks can't jointly exceed the budget.
- Pressure **High** at 80% and **Critical** at 92%. At Critical, new reservations are refused while usage is at or
  above High; the node's `shed` mode lasts until usage has stayed below High for 5 s. Caches are purged at both levels.

---

## Egress / SSRF guard

Every outbound request from guest code (`fetch`, `wasi:http`), bindings and build fetches goes through one
`EgressGuard`.

```
 guest fetch(url)
     │
     ▼
 scheme http/https? ── no ──► refuse
 port in denied_ports (25,465,587)? ── yes ──► refuse
 host: localhost / *.localhost / denied_hosts / service denylist? ──► refuse
 service allowed_hosts set and host not in it? ──► refuse
 plan egress_default=Deny and no allowlist? ──► refuse
     │
     ▼
 resolve DNS once (connect_timeout 10 s)
 EVERY address must pass the IP rules ─ any blocked ─► refuse
 address in self-denylist? ──► refuse
     │
     ▼
 connect to the pinned IP (reqwest .resolve(), no proxy, no auto-redirect)
     │
     ▼
 3xx? ─► re-run every check on the new URL (≤ 5 hops)
         https→http refused (egress_insecure_redirect) unless allow_https_downgrade
         cross-origin: drop Authorization, Cookie, Proxy-Authorization
         method change: drop body, Content-Type, Content-Length
```

### Blocked addresses

| Family | Blocked |
|---|---|
| IPv4 | Loopback `127/8`, private `10/8` `172.16/12` `192.168/16`, link-local `169.254/16` (incl. cloud metadata `169.254.169.254`), `0/8`, CGNAT `100.64/10`, `192.0.0/24`, benchmark `198.18/15`, reserved `240/4`, broadcast, multicast |
| IPv6 | Loopback, unspecified, multicast, ULA `fc00::/7` (covers `fd00:ec2::254`), link-local `fe80::/10`, site-local `fec0::/10`, Teredo `2001::/32`, NAT64 local-use `64:ff9b:1::/48` |
| Embedded IPv4 | IPv4-mapped `::ffff:a.b.c.d`, IPv4-compatible `::a.b.c.d`, NAT64 `64:ff9b::/96` and 6to4 `2002::/16` are judged by the IPv4 address inside them |
| Self-denylist | The node's listener IPs, all local interface addresses, binding `base_url`s, S3 and OTLP endpoints, plus `egress.denied_cidrs`. Names are re-resolved every 300 s. |

### Limits

| Limit | Default |
|---|---|
| Request body | 10 MiB |
| Response body | 100 MiB (checked on `Content-Length` and on the decoded stream) |
| Decompression ratio | 100x + 64 KiB |
| Redirects | 5 |
| Fetches per task | 50 (HTTP), 200 (cron/queue); 6 concurrent |
| Daily egress | `egress_mb_per_day` per tenant |

- **DNS pinning.** The connection goes to the address that was checked, so DNS rebinding between check and connect
  doesn't work.
- **Per-tenant connection pool.** A pooled client is keyed by {tenant, scheme, host, port, pinned IP}. Connections
  are never shared between tenants or addresses. Defaults: 256 clients, 8 idle connections each, 30 s idle.
- URL userinfo and fragments are removed on every hop. `Accept-Encoding` is forced to `gzip, deflate`.

---

## WebSockets and durable objects

The node holds every WebSocket. Guest code never touches a socket: it holds socket ids and calls
`std:sockets` (or the `std:host/sockets` WIT interface); each message, close or error runs as one ordinary task of
the service, with the HTTP limits, quotas and logs. Long-lived client connections are a new kind of exposure: plan
the caps below.

```
client ──GET Upgrade: websocket──► public listener: route, access, plan, origin, memory pressure
                                     └─ task: fetch(req) → sockets.accept(req) → reserve (caps) → ticket
                                  ◄─ 101 (ticket consumed, never sent to the client)
       ◄──── frames ────► socket task (node): size cap, rate limit, ping, auto-response
                                     └─ event → object inbox → one task at a time per object
```

### Upgrade authentication and origin

- **An upgrade is an HTTP request**: the same route, the plan's `allowed_triggers` (`websocket`), the service's
  `triggers.websocket`, `access` (`public` / `key`), `ip_allow`, rate limit and header hygiene as any request. The
  guest's `fetch` decides (its own auth, which object); without `accept` its answer goes out and nothing is upgraded.
- **Browsers cannot set `Authorization`** on a WebSocket: for `access = "key"` an `rtk_` key may come as a
  subprotocol (`new WebSocket(url, ["rtk_…", "chat"])`). It is taken out of `Sec-WebSocket-Protocol` before the guest
  sees the request, exactly like an `rtk_` bearer is taken out of `Authorization`. Prefer short-lived self-service
  keys (`21ci token create --expires-in`) for this: a URL or subprotocol leaks more easily than a header.
- **Cross-site WebSocket hijacking**: a browser sends cookies with a cross-site upgrade. A service that
  authenticates with cookies must set `ws.allowed_origins`; an upgrade with any other `Origin` is refused with `403
  origin_not_allowed` before a task runs. The guest also sees `Origin` and may check it itself.
- **Tickets**: `accept` reserves a socket and returns a 128-bit random ticket in a response header (`x-rt-ws-ticket`)
  that ingress removes. A ticket binds only an upgrade that came through a route of the same tenant, once, and
  expires after 60 s. A WASM guest gets the offer as `x-rt-websocket: 1` (wasi:http hides `Upgrade`); `accept` without
  an offer is `not_upgradable`.

### Isolation

- The hub keys sockets and objects by service: every `std:sockets` call is checked against the calling task's
  service, so another service's socket id is `not_found` (the same answer as a closed socket). Socket ids are
  `ws_<ULID>`: not secret, never usable across services.
- A durable object is chosen in code (`durable.get(name)`); requests to an object of another service are ordinary
  service calls (`call.allowed_callers`, environments, the plan's `call` trigger). A service that is not durable
  answers `not_durable`.
- Object storage is rows of the service's own `data.db` (`_rt_durable`, `_rt_durable_alarm`), keyed by object
  name and behind the same SQLite authorizer (guests cannot reach `_rt_*` tables with SQL). It counts against the
  tenant's `storage_mb`; a write over the quota is `storage_quota`.
- **Output gate**: a task that acts for an object (an event, an alarm, a request to an object) commits its storage
  writes before any of its socket sends or its HTTP response leave (a WASM response is held until then, not streamed); a handler that fails writes and sends nothing. A failed event is
  delivered again (at least once, `ws.max_event_attempts`), then a message's socket is closed with 1011 and the
  event logged, so a poison message cannot loop.

### Size caps and limits

| Cap | Default | Over it |
|---|---|---|
| Message from a client (`plan.ws_max_message_bytes`) | 1 MiB | enforced before buffering: close 1009 |
| Message from a guest | 1 MiB | `too_large` |
| Messages per client (`ws.msgs_per_sec`, `ws.msgs_burst`) | 50/s, burst 200 | reads slow down (TCP backpressure); throttled for `ws.flood_close_after` (10 s): close 1008 |
| Bytes queued for a client (`ws.send_buffer_kb`) | 1 MiB | `slow_consumer`, close 1013 |
| Events queued per object (`ws.inbox_max`) | 1024 | the object's sockets stop being read until it drains |
| Open sockets per tenant / per object (`plan.max_sockets`, `plan.max_sockets_per_object`) | 1000 / 1000 | `accept` answers `limit_exceeded` |
| Open sockets of a service (`ws.max_sockets` of the service) | none | `limit_exceeded` |
| Open sockets on the node (`[ws] max_sockets`) | 20000 | `limit_exceeded` |
| Tags / attachment | 10 × 256 B / 16 KiB | `too_large` |
| Silent client (`ws.ping_interval`) | ping every 30 s | dropped after two intervals |
| Memory pressure High | | upgrades refused with `503 memory` |

- An open socket has **no memory reservation of its own**: sockets are bounded by `ws.max_sockets`, the plan's
  socket caps and the message size cap, and new ones are refused under memory pressure. A node near its socket cap
  with large in-flight messages can therefore use memory the governor does not see; size `ws.max_sockets` with that
  in mind (deferred: a per-socket reservation).

- An upgraded connection keeps its slot of `ingress.max_conns_per_ip` until the socket closes.
- **No `permessage-deflate`**: no compression bombs, no per-socket inflate buffers. Text frames must be valid UTF-8.
- Payloads are never logged; metrics and logs carry counts and close codes only.
- A deploy keeps the sockets (the next event runs the new version) unless it asks for `close_sockets` (1012); a
  deleted service's sockets close with 1001, a draining node's with 1001.

### Outbound WebSockets

`new WebSocket(url)` (WASM: `std:host/ws-client`) goes through the egress guard like `fetch`: `ws:`/`wss:` only, the
denied ports and hosts, the service's `egress.allowed_hosts` / the plan's `egress_default`, one DNS lookup and every
address public and not the node's own (no rebinding), TLS with webpki roots for `wss:`, no redirects. A connection
counts as one of the task's fetches and, while open, holds one of its `max_concurrent_fetches` slots (a further
fetch or socket waits for it); received bytes count against the daily egress quota; it closes when the task ends,
also in a warm isolate that is kept for the next task.

---

## Storage isolation

```
 data_dir/
 ├── meta.db                      tenants, services, keys (hashed), sealed secrets
 ├── services/<svc_id>/data.db    one SQLite file per service: KV + SQL + durable objects
 └── blobs/<svc_id>/              blob files, host-generated names, 0600
                                  dirs 0700
```

- **One SQLite file per service.** A guest has no way to name another service's file.
- **Blob names are generated by the host.** The guest's key is stored only in the `_rt_blobs` index and never
  becomes a path, so there is no path traversal. Files are created with `create_new`, mode `0600`. Keys are 1 to
  1024 bytes.

### SQLite authorizer (every guest statement)

| Denied | |
|---|---|
| `ATTACH` / `DETACH` | No access to other database files |
| Guest `BEGIN` / `SAVEPOINT` | Transactions only through the host API |
| Any `TEMP` table, index, trigger, view, or create in `temp` | No temp storage outside the quota |
| `load_extension` (also refused by a text precheck) and `VACUUM` | No native code, no `VACUUM INTO` file writes |
| `fts3_tokenizer` | It returns a native pointer (an address leak in a process every tenant shares) |
| Objects named `_rt_*`, and renaming a table into that namespace | Runtime-internal tables are hidden: their contents cannot be read or written (their names and sizes appear in `sqlite_master` / `dbstat`, as the service's own data) |
| 29 pragmas | e.g. `writable_schema`, `max_page_count`, `journal_mode`, `synchronous`, `mmap_size`, `trusted_schema`, `temp_store_directory`, `hard_heap_limit`, `key`/`rekey` |

### Limits

| Limit | Value |
|---|---|
| `SQLITE_LIMIT_LENGTH` / `SQL_LENGTH` | 16 MiB / 1 MiB |
| Columns · expr depth · compound select · variables · LIKE pattern | 1000 · 500 · 100 · 10000 · 1000 |
| Result | 10,000 rows per statement, 10 MiB per call |
| Statement time | 1000 ms (async), 100 ms (sync `std:sqlite`) |
| Interactive transaction | Rolled back after 5 s |
| `busy_timeout` | 5 s (async), 20 ms then `sql_busy` (sync) |
| KV | Key ≤ 2 KiB, value ≤ 1 MiB, list 1 to 1000 |
| Database size | `max_page_count` = min(node `max_db_bytes`, current size + tenant's remaining `storage_mb`) |

When a tenant is over its storage quota, its databases still serve reads and deletes but can't grow.

### Synchronous `std:sqlite` (`bun:sqlite`)

- Uses the same authorizer, limits, caps and page ceiling as the async path.
- A multi-statement batch is checked **statement by statement**, so `SELECT 1; VACUUM INTO …` can't slip through.
- Transactions only go through host `begin`/`commit` (savepoints for nesting). A failed `COMMIT` rolls back. A
  transaction left open by a killed task is rolled back. The connection never silently falls back to autocommit
  while a transaction is open.

---

## Secrets

```
 operator                       node                                 guest
 ────────                       ────                                 ─────
 RT_MASTER_KEY (32 B hex) ──► HKDF-SHA256 ─┬─ rt/secrets-aes-gcm/v1 ─┐
                                           ├─ rt/secrets-key-id/v1   │ key id (4 B)
                                           ├─ rt/api-key-pepper/v1   │
                                           └─ rt/artifact-mac/v2     │
                                                                     ▼
 PUT /v1/services/{s}/secrets/{name} ──► AES-256-GCM (random 12 B nonce,
       (secrets:write, ≤ 32 KB)           AAD = version‖key_id‖tenant|service|name)
                                                    │
                                                    ▼
                                      meta.db: 0x01‖key_id‖nonce‖ct+tag
                                                    │  on first task
                                                    ▼
                                      in-memory env bundle (per service) ──► Deno.env /
                                      evicted after 600 s idle,              WASI env
                                      dropped on secret/env change, delete
```

`std:secrets` (`secrets.get`, `secrets.require`, and `std:host/secrets.get` for components) reads only
the secrets part of that bundle, never a plain env var. The accessor object has no enumerable keys and
`JSON.stringify` of it is `undefined`, so a dump of the module cannot carry values by accident. Values are
returned only to the guest that asked: a `secret_missing` error names the missing secrets, never a value,
and the runtime never logs one.

`std:lock` keys are scoped to the calling service: another service cannot hold, wait on or release them.
A waiter cannot pin a thread (the wait is asynchronous) and cannot hold a key forever (the lease expires, and
the holds of a task are released when it ends, is killed or hits a limit). The waiters per key, the keys per
service and the holds per task are capped (`lock_limit`), so a service cannot exhaust the node's lock table.

`std:schedule` schedules belong to the calling service: `after`, `at`, `cancel` and `list` act only on its
own pending schedules, and a delivery reaches only its service's handler. The pending schedules per
service (1 000), the payload (16 KiB), the delay (30 days) and the id charset are capped and checked by the
host, and a refused argument is `validation_failed`. Delivery is at-least-once, so a handler must treat
`event.id` as its idempotency key.

| Property | Behavior |
|---|---|
| Algorithm | AES-256-GCM. The AAD binds each value to its tenant, service and name, so a sealed value can't be moved to another service. |
| Master key | `RT_MASTER_KEY` or `RT_MASTER_KEY_FILE` (not both), else `security.master_key_file`, else `<data_dir>/master.key` (created `0600`, with a warning) |
| Key id | 4 bytes derived from the master key, stored with each value. Not secret. |
| Write-only | The API returns only `{name, updated_at}`. The audit log records only the name. |
| Names | Env and secret names starting with `RT_` are rejected |
| Memory | Master key, derived keys and decrypted values are zeroized on drop. `Debug` output is redacted. |

**Rotation**

1. Set the new key as `RT_MASTER_KEY` and the old one as `RT_MASTER_KEY_PREVIOUS[_FILE]`. The previous key is only
   used to open old secrets and verify old API keys.
2. Run `rt secrets reseal [--config rt.toml]`. Values already under the current key are skipped. Each write is
   compare-and-swap. It reports resealed / already current / changed meanwhile / failed, and exits 1 on any failure.
3. Remove `RT_MASTER_KEY_PREVIOUS`. Tenant API keys stay valid only while the previous pepper is configured, so
   reissue keys first.

**Tenant keys never reach guests.** `Authorization: Bearer rtk_…`, `x-rt-key` and the root token are removed from
the request before the guest sees it (see [headers](#request-path-headers-and-checks)).

Secrets are plain env values once inside the guest. Guest code can log them; secret values of 8 characters or more
are replaced by `[secret]` in the task's log records (see [Observability and audit](#observability-and-audit)), but a
transformed value (encoded, split) is not caught.

---

## Builds and supply chain

| Control | Behavior |
|---|---|
| Allowed hosts | `build.allowed_hosts` = `esm.sh`, `jsr.io`, `deno.land`, `cdn.jsdelivr.net`, `unpkg.com`. Exact host match, case-insensitive. npm imports go to `build.npm_cdn` (`https://esm.sh`). Only `https:` (plain `http:` only to a loopback mirror: the deps cache is shared by every tenant's builds), and no credentials in the URL. |
| Egress | Build fetches go through the same egress guard as guest code |
| `rt.lock` | Records sha256, size and content type for every remote URL. A changed byte fails the build with `lock_mismatch`. |
| Vendoring | Fetched modules are stored in the artifact store, re-hashed on read, and reused by rebuilds |
| Limits | 60 s (Deno), 120 s (WASM), 30 s per fetch, 256 MB memory (from the governor), 50 MB total deps, 1 thread, `builds_per_hour` per plan |
| Artifact MAC | Compiled artifacts carry `HMAC-SHA256` (key from `rt/artifact-mac/v2`) over the source digest, runtime kind, engine fingerprint and every file. Checked **before** deserializing. |
| WASM policy | Components using threads/shared memory are rejected. The `wasi:http/incoming-handler` export is required. |

```
 cache entry ──► verify HMAC ──ok──► deserialize / load
                    │
                   bad
                    ▼
           security.artifact_tamper event, warning, entry deleted, rebuild
```

Cache files are written via temp file and rename, mode `0600`. The MAC matters because wasmtime and V8 code caches
contain native code: a forged cache file would otherwise run as machine code.

Builds run in-process. There is no separate OS sandbox for the build step.

---

## MCP endpoint

`POST /mcp` on the **control** listener (body ≤ 4 MiB; `GET`/`DELETE` → `405`).

- **Bearer.** A tenant `rtk_` key (`Authorization: Bearer` or `x-rt-key`), using the same authenticator and
  failed-auth limiter as `/v1`. Missing or bad: `401` + `WWW-Authenticate: Bearer error="invalid_token"`. The **root
  token is refused** (`403 insufficient_scope`).
- **Origin check (DNS-rebinding guard).** No `Origin` passes. An `http`/`https` origin on `localhost`, `127.0.0.0/8`
  or `::1` passes. Any other origin is `403`.
- **Scopes.** Tools call the `/v1` router in-process with the caller's credentials. `tools/list` hides tools the
  key can't use, and calls to them are refused.

| Scope | Tools |
|---|---|
| none | `whoami`, `get_limits` |
| `services:read` | `list_projects`, `list_environments`, `list_sdks`, `get_sdk_contract`, `list_services`, `get_service`, `list_versions`, `list_schedules`, `list_objects`, `list_sockets` |
| `services:write` | `create_project`, `create_service`, `update_service`, `delete_service` |
| `deploy` | `deploy_code`, `rollback` |
| `sockets:write` | `close_socket` |
| `logs:read` | `get_logs`, `get_stats` |
| `env:write` + `services:read` | `set_env` |
| `secrets:write` | `set_secret` |
| `keys:write` | `list_tokens`, `create_token`, `revoke_token` |

---

## Observability and audit

**Audit events.** Each control change writes an `events` row (actor, tenant, service, data) and a `tracing` line on
target `rt::audit`.

| Area | Events |
|---|---|
| Tenants, services | `tenant.created/updated/deleted`, `service.created/updated/deleted`, `service.environment_changed` |
| Versions | `deploy`, `rebuild`, `rollback`, `live.changed` |
| Config | `env.written` (names only), `secret.written`, `secret.deleted` |
| Hosts | `host.attached/detached/verified/expired` |
| Schedules, queues | `schedule.created/updated/deleted`, `dlq.redrive`, `dlq.purge` |
| Data, projects, keys | `data.exported`, `project.created/updated/deleted`, `key.created`, `key.deleted` (with `cascaded`) |
| Security | `security.artifact_tamper` (separate event) |

**Logs.**
- Node tracing never contains env or secret values.
- WASM guests get their secrets as WASI environment variables (by design), and any guest can print what it reads.
  Once a task has loaded its env, every secret value of 8 characters or more is replaced by `[secret]` in that
  task's log messages and in its log fields at any depth, keys included (longest secret first). Shorter values, and secrets transformed by the guest (encoded, split),
  are not caught: treat guest logs as sensitive anyway.
- Guest logs are capped at 10,000 chars per message, 100 lines/s (burst 1000), 50 MB per service and 7 days.
  Dropped lines are counted in `rt_logs_dropped_total`.

**Metrics.** `/metrics`, `/healthz` and `/readyz` on the metrics listener, with no auth. Keep it on loopback.

Failed authentications are rate-limited but not logged individually.

---

## Hardening settings

| Setting | Default | Effect |
|---|---|---|
| `RT_ROOT_TOKEN[_FILE]` | unset | Unset: control API disabled (release) or open on loopback only (debug). The `.env.example` `change-me…` placeholder is refused at startup, and so is a token shorter than 32 characters when `listeners.control` is not loopback. |
| `RT_MASTER_KEY[_FILE]` | unset | Unset: auto-generated `<data_dir>/master.key`. Set it explicitly. |
| `security.require_v8_sandbox` | `true` | The binary has no V8 sandbox, so startup fails unless this is `false`. The shipped defconfigs and the image set `false`, and a warning is logged. |
| `security.clock` | `frozen` | Guest clock mode (`frozen` / `coarse`) |
| `security.seccomp` | `true` | On Linux only: `PR_SET_DUMPABLE=0`, `PR_SET_NO_NEW_PRIVS=1`, `RLIMIT_CORE=0`. **No seccomp filter, no Landlock.** |
| `security.reserved_hosts` | | Hostnames tenants can't attach |
| `listeners.internal_allowed_uids` | `[]` | Extra UIDs allowed on `rt.sock` |
| `listeners.allow_tcp_internal` | `false` | Allow a loopback TCP internal listener |
| `ingress.trusted_proxies` | `[]` | CIDRs whose `X-Forwarded-For` is trusted (also exempt from the per-IP cap) |
| `ingress.header_timeout` / `idle_timeout` | 10 s / 60 s | Slowloris protection |
| `ingress.max_conns_per_ip` / `h2_max_streams` | 256 / 100 | Connection caps |
| `ingress.sse_streams_per_key` | 10 | Log stream cap |
| `egress.denied_ports` | `[25, 465, 587]` | No outbound SMTP |
| `egress.denied_hosts` / `denied_cidrs` | `[]` | Extra global blocks |
| `egress.allow_https_downgrade` | `false` | Allow https→http redirects |
| `build.allowed_hosts` | 5 CDNs | Build import allowlist |
| `wasm.fuel` | off | Deterministic WASM CPU metering |

**Container.** The image runs as `USER 10001:10001` with `/data` mode `0750`. `docker-compose.yml` sets
`cap_drop: [ALL]`, `no-new-privileges:true` and `init: true`, publishes ports on `127.0.0.1` only, and requires
`RT_ROOT_TOKEN` and `RT_MASTER_KEY`. It doesn't set `read_only: true`. Inside the image the control (8081) and
metrics (9090) listeners bind `0.0.0.0` so that port publishing works: publish them on `127.0.0.1` or a private
network only, never on a public interface (metrics has no auth). The image also sets
`RT__SECURITY__REQUIRE_V8_SANDBOX=false` until the custom V8 build with the sandbox ships, so a V8 escape
is contained only by the process and container boundary.

---

## Known limitations

These are **not** protected against today. Plan your deployment around them.

| Gap | What it means |
|---|---|
| **No V8 sandbox** | The prebuilt `rusty_v8` has no V8 sandbox (`v8_sandbox:false` in `/v1/node/status`). A V8 memory-corruption bug in guest JS gives code execution in the `rt` process. |
| **One process for everything** | No supervisor/executor split. Guest code shares an address space with the control plane, `meta.db` and the master key. An escape from one tenant reaches all tenants. |
| **No seccomp / Landlock** | `security.seccomp=true` only sets dumpable/no-new-privs/core limits. Syscalls and filesystem access are not filtered at the OS level. |
| **Single node** | No replication or failover. Losing the data volume loses everything, including `master.key` if it was auto-generated. |
| **Timing channels** | The frozen clock limits but doesn't remove timing signals. Frozen `performance.now` keeps sub-ms precision per poll. `eval`/`Function` and `WebAssembly` are available. |
| **Warm state** | With reuse on (`min_latency`), module globals persist between requests of the same service. Warm objects (default for socket events and durable services; `ws.warm = false` turns them off) keep a durable class instance between events of an object. |
| **WebSockets: single node** | Sockets drop when the node restarts (clients reconnect); object storage and alarms survive. No multi-node routing of objects. |
| **WASM fuel off** | WASM CPU is enforced by epoch + thread CPU time at slice boundaries, not instruction-exact. |
| **Storage quota between scans** | `storage_mb` is re-applied every 30 s from a scan of the tenant's files. Between scans each open database and blob write may use the whole remainder, so several services (or concurrent blob uploads) of one tenant can overshoot the quota until the next scan refuses further growth. |
| **Unauthenticated metrics** | `/metrics` exposes per-tenant counters to anyone who can reach it. |
| **Control API has no ACL** | The root token is the only control; there is no IP allowlist or mTLS. |
| **TCP internal listener** | No auth beyond loopback when enabled. |
| **Audit log** | Stored in `meta.db`, not signed or append-only. The operator (or an escaped guest) can change it. |
| **Guest logs** | Only secret values of 8+ characters printed as they are get redacted. A guest that encodes or splits a secret (or a WASM guest whose stdout line is split at 64 KiB through one) stores it in logs. |
| **Builds** | Run in-process with no OS sandbox. Allowlisted CDNs are trusted to serve what `rt.lock` pins. |
| **S3 / OTLP** | Config keys are accepted but have no code behind them. Blobs and artifacts are local only. |
| **Unused settings** | `secrets.cache_ttl` is not used; the env cache lives until it is invalidated or idle for 600 s. |

If you host mutually untrusting tenants, run one node per trust domain (for example one VM per tenant group) until
process and kernel isolation ship.

---

## Operator hardening checklist

- [ ] Run a **release** build.
- [ ] Set `RT_ROOT_TOKEN` (long, random) and `RT_MASTER_KEY` (`openssl rand -hex 32`) from a secret store, using
      the `_FILE` variants where you can. Back up the master key separately from the data volume.
- [ ] Keep `control` and `metrics` on loopback, or behind a VPN or an authenticating proxy. Expose only `public`.
- [ ] Put TLS and a reverse proxy in front of `public`. Set `ingress.trusted_proxies` to the proxy's addresses only.
- [ ] Keep the internal listener on the Unix socket. List extra UIDs in `internal_allowed_uids` only if needed.
- [ ] Give tenants scoped, service-restricted, expiring keys. Don't hand out the root token.
- [ ] Set plans per tenant. Keep `allow_cross_tenant_calls = false` unless you need it. Consider
      `egress_default = Deny` plus per-service `allowed_hosts`.
- [ ] Add your internal networks and hostnames to `egress.denied_cidrs` / `denied_hosts`.
- [ ] Run the container as shipped (non-root, `cap_drop: ALL`, `no-new-privileges`). Add `read_only: true` with a
      writable `/data` if your setup allows it.
- [ ] Isolate the host: a dedicated VM per trust domain, no other secrets on it, cloud metadata blocked at the
      network level too.
- [ ] Rotate the master key with `rt secrets reseal`, and keep `rt.lock` files in source control.
- [ ] Watch `rt::audit` logs and `security.artifact_tamper` events.

---

## Reporting a vulnerability

Please don't open a public issue for security bugs. Report privately to the maintainers (via GitHub's **Report a
vulnerability** / private security advisory on this repository) with:

- the affected version or commit,
- the steps to reproduce, or a proof of concept,
- the impact you expect (which boundary is crossed).

We will acknowledge the report, agree on a fix and disclosure timeline with you, and credit you unless you prefer
otherwise.

### Where to report

| Channel | Use it for |
|---|---|
| [GitHub private vulnerability report](https://github.com/21-ci/runtime/security/advisories/new) | the preferred channel: the report, the discussion, the fix and the advisory stay in one private place |
| **security@REPLACE-WITH-PROJECT-DOMAIN** | if you cannot use GitHub. Encrypt with the key published at `https://REPLACE-WITH-PROJECT-DOMAIN/.well-known/security.txt` if the report contains exploit details |

In scope: `rt`, `21ci`, the `std:*` guest APIs, the container images, the packages and the installer in this
repository, and the release pipeline itself (`.github/workflows/`). Out of scope: findings that need an operator to
run against this document's [hardening checklist](#operator-hardening-checklist) (for example a control API exposed
to the internet), the [known limitations](#known-limitations) listed above, denial of service by a tenant inside its
own plan limits, and reports from automated scanners without a demonstrated impact. Please do not test against
infrastructure you do not own, and do not access other tenants' data beyond what proves the issue.

## Supported versions

| Version | Security fixes |
|---|---|
| latest minor release (`x.y`) | yes, as patch releases (`x.y.z`) |
| previous minor release | critical fixes only, for 3 months after the next minor ships |
| older releases, pre-releases (`-rc.N`), `main` between releases | no: upgrade |

Before 1.0, a minor release may contain breaking changes; the changelog says so. Security fixes land on `main` first
and are backported to the supported `release/x.y` branches ([.github/BRANCHING.md](.github/BRANCHING.md)).

## Disclosure timeline

| When | What |
|---|---|
| within 3 business days | we acknowledge the report and assign a maintainer |
| within 10 business days | we confirm (or decline, with reasons) and agree on a severity (CVSS v4) and a target date |
| within 90 days of the report | a fixed release ships; critical issues as fast as a safe fix allows |
| on release | we publish a GitHub Security Advisory, request a CVE through GitHub, and credit the reporter |
| 90 days with no fix | the reporter may disclose; we will publish what we know and the mitigations |

We may ask for an extension when a fix needs an upstream change (V8, wasmtime, SQLite); we will tell you why.
Coordinated releases go out without pre-announcement: watch the repository's security advisories
(**Watch → Custom → Security alerts**) to hear about them.

## Verifying releases

Every release is built by `.github/workflows/release.yml` from a tag, with no build cache, and signed keyless with
[Sigstore cosign](https://docs.sigstore.dev/): the signing identity is that workflow, no long-lived key exists. The
pipeline blocks a release on any fixable HIGH or CRITICAL vulnerability found by Trivy or Grype in a tarball, a
package, an SBOM or an image (amd64 and arm64).

Release files (`gh` and `cosign` 2.4 or later):

```bash
V=1.2.3
gh release download "v$V" -R 21-ci/runtime -p SHA256SUMS -p SHA256SUMS.sigstore.json -p "rt-$V-x86_64-unknown-linux-gnu.tar.gz"

# 1. the checksum file was signed by the release workflow of this repository, at a release tag
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/21-ci/runtime/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# 2. the file matches the checksum
sha256sum --check --ignore-missing SHA256SUMS          # macOS: shasum -a 256 --check --ignore-missing SHA256SUMS

# 3. optional: SLSA build provenance (which workflow, commit and runner built it)
gh attestation verify "rt-$V-x86_64-unknown-linux-gnu.tar.gz" -R 21-ci/runtime
```

`install.sh` runs steps 1 and 2 by itself when `cosign` is installed (`--verify-signature` makes it mandatory).
Each tarball has an SPDX SBOM next to it (`*.spdx.json`), covered by `SHA256SUMS`.

Container images:

```bash
cosign verify ghcr.io/21-ci/rt:1.2.3 \
  --certificate-identity-regexp '^https://github\.com/21-ci/runtime/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/21-ci/rt:1.2.3 -R 21-ci/runtime    # provenance + SBOM attestations
```

Pin images by digest in production (`ghcr.io/21-ci/rt@sha256:…`) and verify the digest you deploy.

Package repositories: apt and rpm metadata and the rpm packages are signed with one GPG key, the apk index and
packages with an RSA key. Check the fingerprint before you trust the key:

```bash
curl -fsSL https://21-ci.github.io/runtime/packages/gpg.key | gpg --show-keys --with-fingerprint
# expected: REPLACE-WITH-THE-PACKAGE-KEY-FINGERPRINT (published here when the key is created)
```

The Homebrew formulae pin the SHA-256 of each tarball; `brew` verifies it on install.
