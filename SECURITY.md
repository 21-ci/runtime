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
4. [Tenancy, plans and quotas](#tenancy-plans-and-quotas) and [events](#events-pubsub-streams-and-async-invocations)
5. [Projects, environments and service calls](#projects-environments-and-service-calls)
   - [Operator SDKs and bindings](#operator-sdks-and-bindings)
6. [Request path: headers and checks](#request-path-headers-and-checks)
7. [Deno sandbox](#deno-sandbox)
8. [WASM sandbox](#wasm-sandbox)
9. [Egress / SSRF guard](#egress--ssrf-guard)
10. [WebSockets and durable objects](#websockets-and-durable-objects)
11. [Storage isolation](#storage-isolation)
12. [Backups](#backups)
13. [Secrets](#secrets) and [data-change events and external secrets](#data-change-events-and-external-secrets)
14. [Builds and supply chain](#builds-and-supply-chain)
15. [MCP endpoint](#mcp-endpoint)
16. [Observability and audit](#observability-and-audit)
17. [Telemetry](#telemetry) and [Hardening settings](#hardening-settings)
18. [Known limitations](#known-limitations)
19. [Operator hardening checklist](#operator-hardening-checklist)
20. [Reporting a vulnerability](#reporting-a-vulnerability)

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
| `services:read` | List and read services, projects, environments, versions, topics, consumers and invocations. This includes message payloads: `GET /v1/topics/{t}/messages`, a consumer's dead-letter list (with record headers and bodies) and the MCP `read_topic` tool |
| `services:write` | Create, update and delete services, projects and topics |
| `deploy` | Deploy, rebuild, roll back, switch a service to a branch's code ([a privilege boundary](#switches-and-branches)) |
| `invoke` | Call `access="key"` routes |
| `logs:read` | Logs and stats |
| `env:write` | Set env vars |
| `secrets:write` | Set and delete secrets |
| `queue:write` | Enqueue, publish, append to topics, consumer resets, DLQ redrive and purge, invocation redrive and purge |
| `data:admin` | Export a consistent snapshot of a service's `data.db`; drop a durable object's storage; copy a parent's data into a branch (`copy_data`) |
| `keys:write` | Self-service keys (`/v1/keys`) |
| `sockets:write` | Close a service's WebSockets through the control API (audited `sockets.closed_by_api`) |
| `data:read` | Read a service's rows, KV values and blob listing (see "Data browser"). Opt-in: implied by no other scope, and existing keys do not gain it |

A key can also be **restricted to some projects and environments**: `project_ids` (`prj_…` ids; `null` means every
project of the tenant, including ones created later) and `environments` (labels; `null` means every environment). A
service is touchable by the key only when its project and its environment are both admitted, and the key can create
a service (REST or the MCP `create_service` tool) only in an admitted project and environment (`403
insufficient_scope` otherwise). A key can also **expire**
(`expires_at_ms <= now` means invalid). Project create, update and delete need a key with no project or environment
restriction, or the operator.

### Switches and branches

A **switch is a privilege boundary**. Code on a branch runs with the branch's secrets and data. A switch makes the same code live on the parent, where it reads the parent's secrets and data and answers on its URL. So `services:write` on a branch, or `deploy` on the branch alone, is not enough: `POST /v1/services/{s}/switch` needs `deploy` **on the parent**, reached through `load_service`. A key restricted to another project or environment gets `404`.

- The source is held to the key's scope and to the parent's tenant and project; anything else is `404`, so existence is not revealed. A branch or another service as the source also needs `services:read`.
- The digest is read from the source's row. The body has no field for one.
- System services are refused as parent (`403 reserved`) and as source (`403 scratch_source_reserved`).
- Secrets and env are never copied by a switch. `env_names_from_parent` copies names with empty values; warnings list names, never values.
- `If-Match` guards against switching a service that changed since it was read. The MCP tools `switch_service` and `rollback_switch` are marked destructive, so clients ask for confirmation.
- Review what goes live before you switch. The runtime checks who switched and which digest, not what the code does with the secrets.

**Previews** (`POST /v1/scratch`) run untrusted branch code. Their isolation is enforced in the runtime, not by configuration of either side: `rt-triggers` refuses calls in both directions and enqueues on the route's `isolated` flag, `rt-host` refuses streams, pub/sub, schedules and platform bindings (`preview_isolated`), SDK calls are `sdk_not_granted`, and the store re-applies the config policy after every `PATCH`. The slug's suffix is not access control. A preview's code becomes trusted only through a `switch`, which needs `deploy`.

**`copy_data`** puts the parent's database and blobs in another service, so it needs `data:admin` on top of `services:write`. It is capped by `deploy.dry_run_max_mb`, audited as `scratch.copied_data` (source and bytes, never data), and forces `access: key` on a public service. The copy then lives under the branch's own access settings and egress rules, and a key with `services:write` on the branch can deploy code that reads it. Durable alarms stay behind. Without `copy_data` a branch starts empty.

**Pub/sub and stream consumers** are not inherited by a branch: in a shared environment it would be another member of the parent's groups and answer its request/reply calls. `events_of_parent: true` keeps them and needs `deploy`; without it they are cleared at create and after every `PATCH`.

**Backups in a bucket:** S3 retention only prunes ids with a row in this node's `meta.db`, so two nodes on one prefix do not delete each other's archives. Objects with no row are left for the operator.

**A branch's policy** is forced at create and after every `PATCH`: `scaling.min_instances` 0, timed triggers off unless `schedules`. Egress follows the parent's rules unless `egress_of_parent: false`, so a branch can reach the same third parties as its parent.

**Known limits, stated plainly:**

- **The runtime cannot recognize a console deployed as an ordinary tenant service.** The refusals above match only the system tenant and the reserved `sys-` slug prefix. A console that runs under a normal tenant and slug can be branched, copied and switched by any key with the scopes for it. Deploy it in the system tenant or under the reserved prefix, or keep it in a project that no broad key can touch.
- **`git_sha` is not recorded yet.** The audit field exists and is `null`, because no version records its commit. The audit trail names the actor and the digest, and does not say which commit that was.

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

### Deploying from a URL

Reading a view resolves its columns with the authorizer off, so a `SQLITE_DIRECTONLY` function in a view (`load_extension`, `fts3_tokenizer`) is refused by SQLite ("unsafe use of"), not by the callback: guests get `sql_not_authorized` and bundle import refuses the schema. `deploy.failed` is audited with the code and the host only.

`POST …/versions/from-url` makes the node fetch a tenant-chosen URL and hand the bytes back as readable version source,
so it could read anything the node can reach if it were looser than a guest's `fetch`. It is not:

- It goes through `GuardedEgress` with a guard of its own: every hop `https`, ports, one DNS lookup, every address public
  and not the node's, the self-denylist, at most 3 redirects, 64 MiB. There is no allow-list for private Git hosts.
- The service's `egress` rules and the plan's `egress_default` apply, and the bytes count toward the daily egress quota,
  so a `deploy` key cannot read through it what the service's guests may not reach.
- **Caller headers stop at a cross-origin redirect, all of them**, since `PRIVATE-TOKEN` and the like are not standard
  credentials. `Host`, `x-rt-*` and hop-by-hop names are refused.
- **`headers` values and the URL's query and userinfo are credentials**: not stored, logged, echoed, put in events or in
  the idempotency fingerprint. The body type has no `Debug`. The audit event names the host only.
- Extraction is the upload's (`normalize_path` before anything is stripped, file and size limits, inflate ratio; entries
  outside `subdir` still count), so a bomb outside the subdir is still a bomb. The fetched file sits in an unlinked
  temporary file, and is read into memory under a governor reservation.
- `versions.meta` is client-written. It is a claim for display and audit, never a decision: `versions.migrations` comes
  from validation only, and the commit in an audit event says what the deployer claimed.

### Long-lived maps

Every per-key, per-IP, per-tenant or per-service map on the request path has a cap or an eviction: the key cache
(4096), the failed-auth limiter (`LIMITER_CAPACITY`, swept), idempotency entries (24 h, pruned past 100k), SSE
counters (credentials with no open stream dropped past 1024), per-service ingress buckets and `ip_allow` lists
(dropped with their service on every route refresh), by-IP rate rule cells (`ingress.rate_keys_max`, 1/8 per service;
idle ones pruned after 10 min), data-change dirty flags (one per service with a writer, removed on purge), from-url
fetch slots (2 per tenant, 32 per node, removed on release), SDK caller gates (idle tenants dropped past 10k), lock slots
(removed on release), socket tickets (swept), env bundles (idle TTL), usage counters (removed once flushed).
Events: the pub/sub index (subscriptions × services, rebuilt per generation), pub/sub round-robin counters per namespace
(group, request and fan-out counters, pruned to the live namespaces whenever the subscription index changes), stream
round-robin counters per topic (bounded by topics), the stream engine's cursor cache and in-flight set (bounded by
consumers × partitions), and throttle counters per service (removed on success). Async in-flight attempts are capped
at 256 per node.

### Ingress rate rules

`ingress.rate_rules` apply when the raw path **or** its canonical form matches, the opposite of `allow_paths`, so a form
the guest would read as the limited path cannot escape: encoding (nested too), dot segments, `\\`, `//`, a trailing
slash and ASCII case all count. A path with no canonical form is `400` on a service with rules. A stored rule whose
pattern no longer compiles limits every path, and a stored `rps: 0` admits nobody. `by: ip` uses the trusted-proxy
client address, so `X-Forwarded-For` from an untrusted peer or past a malformed hop cannot mint clients. A request with
no address shares the rule's overflow cell. Cells are capped per node and per service (1/8), and active ones are
never evicted, so filling the map does not earn a fresh burst, though new clients then share the overflow cell of the
rule. `;` parameters are not stripped, and a `GET` rule does not cover `HEAD`.

### Control API bodies

JSON bodies are read up to 1 MiB. Artifact uploads are read up to 64 MiB, and only after the caller passed auth,
scope and service checks: a node buffers at most 4 uploads at once, and charges each one's `Content-Length` (64 MiB
when unknown) to the memory governor before reading it. A fifth upload, or one the governor refuses, gets `429
rate_limited`.

`DataBrowseGate` (`rt-api`): a semaphore per tenant and a rate window per service, only for ones that exist and were browsed; idle entries are dropped past 4096.

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
| `limits_max` | 30 s wall · 1 s CPU · 128 MB · 60 s `min_run_time_ms` | `allowed_triggers` | http, interval, queue, call, websocket, pubsub, stream |
| `egress_default` | `Allow` | `allow_cross_tenant_calls` | `false` |
| `max_async_pending` | 10k (pending async invocations per service) | `max_topics` | 10 |
| `max_topic_partitions` | 4 | `max_stream_mb` (record bytes across the tenant's topics; not in `storage_mb`) | 100 |
| `max_stream_retention_hours` | 72 | `max_scratch` | 5 |
| `scratch_ttl_max` | 7 days | `max_projects` | 20 |
| `max_environments_per_project` | 10 | `max_branches` | 10 |
| `max_secrets_per_service` | 100 | `max_custom_hosts` | 10 |
| `cpu_ms_per_day` | 0 (no cap) | `max_versions_retained` | 0 (no cap) |
| `log_retention_days` | 0 (no cap) | `max_fetches_per_call` | 0 (no cap) |
| `max_log_lines_per_call` | 0 (no cap) | | |

- The quota enforcer checks service count, schedule count and frequency, allowed triggers, an rps token bucket,
  daily invocations, builds per hour, queue depth, daily egress and storage, and the caps on projects, environments,
  branches, secrets and custom hosts (`403 quota_exceeded` names the limit; `GET /v1/usage` reports each).
- `cpu_ms_per_day` (`0` = no cap) refuses new requests with `429 cpu_quota` until UTC midnight. Schedules, one-off
  schedules and queue delivery stop too: fires are skipped and nothing is leased, so no delivery attempt is used. That
  check reads in-memory counters, never `meta.db`.
- A service's `min_instances` and `max_instances` are clamped to the plan's `max_instances`. The sum of
  `min_instances` across a tenant must stay within `max_warm_floor_total`, otherwise `422 limit_exceeds_plan`.
  One tenant can't pin the node's memory with warm instances.
- Re-entrant self-calls are the one exception to `max_instances`: a call from a service to itself may run on the
  slot its waiting parent lends, so a service may exceed `max_instances` by one instance per waiting parent, and
  pub/sub request replies to its own subject by at most 15 (the call depth limit). Each still takes the tenant's
  per-depth call allowance, a `max_inflight` slot and its own memory reservation.
- A per-service limit above `limits_max` is clamped and recorded as a plan conflict.
- `scaling.min_run_time_ms` above the plan's `limits_max.min_run_time_ms` (or the node's `sched.min_run_time_max_ms`,
  whichever is lower) is refused with `422 limit_exceeds_plan`. If the plan is lowered later, the runtime clamps the
  value at run and records a plan conflict.
- Pub/sub subscriptions and stream consumers need `pubsub` / `stream` in `allowed_triggers`. A service whose plan
  does not allow them is left out of the subscription index and the stream engine, and `services` create or patch
  refuses such a block with `403 trigger_not_allowed`.

---

## Events: pub/sub, streams and async invocations

Pub/sub (`std:pubsub`), streams (`std:stream`, topics with partitions and consumer groups) and async invocations
share the plan and the ingress checks, and each has its own caps.

- **Namespaces.** A subscription, topic and consumer belongs to the namespace of its service route: tenant, project
  and environment. A service in `staging` cannot read or append a `production` topic of the same name, and does not
  receive its pub/sub messages. Another tenant never does either. The namespace comes from the route table, never from
  a guest payload.
- **Opt-in async.** `async.enabled` defaults to `false`. Until a service turns it on, `Prefer: respond-async` is
  ignored and `X-Rt-Invocation-Type: Event` is refused (`403 async_not_allowed`), so an anonymous internet client
  cannot move a service's work into the background.
- **Authorization is decided at accept.** For an async invocation, `access: key` is checked when the request is
  accepted. Revoking the key later does not cancel accepted invocations; purge them with `21ci invocation purge`. At run
  the runtime re-checks only the live version, `triggers.http` and the plan's `http` trigger.
- **Stored async headers and body are sealed.** They are encrypted with the node's secret key (AES-256-GCM, additional
  data = the service id, the invocation id and `h` or `b`), so `queue.db` holds only opaque bytes for them. The
  method and the `uri` (path **and query string**) are stored in plain text, kept 24 hours after success and 7 days on
  failed records, and `request_hash` is an unsalted SHA-256 of method, uri and body: do not put tokens or personal data
  in the query string of an async request (send them in a header or the body). The stored headers are the
  guest-visible set after ingress hygiene: runtime credentials (`rtk_…`, the root token) and client `x-rt-*` headers are
  already removed, and the guest's own `Authorization` and cookies are kept because the handler needs them. The
  sealed data is wiped when the invocation succeeds and kept on failed records (7 days, so they can be redriven). If
  the key that sealed a record is gone, the record fails once with `sealed_unreadable` and is not retried. `rt secrets reseal` does not reseal async
  invocations: drain pending and failed invocations (let them run, or purge them) before removing
  `RT_MASTER_KEY_PREVIOUS` after a key rotation, otherwise they fail as `sealed_unreadable`. No API returns headers or
  bodies of an invocation.
- **Caps.** Pending async invocations: `plan.max_async_pending` per service (`429 async_backlog_full` above it). Failed
  records: 1,000 per service, the oldest evicted. Async disk footprint: the sealed headers and body bytes of a
  tenant's pending and failed invocations, across all its services, stay within `storage_mb` MiB (a separate budget
  from the storage quota; `429 async_backlog_full` above it). Each accept and each delivery attempt (retries and
  redrives included) takes one tenant rps/daily token. Async attempts: 256 in flight per node, `queue.max_concurrency` per
  service. Pub/sub: a publish reaches at most 100 receivers (the rest are counted as dropped), at most 64 deliveries in
  flight per subscriber service, nested publishes stop at depth 15, and each delivery is checked against the tenant
  quota before it is submitted. Deliveries go through admission (`max_queued`) and are dropped when it refuses them.
  Streams: each append call takes one tenant rate token; the tenant's byte cap counts records and dead letters
  (`max_stream_mb`); topics per tenant, partitions per topic and retention are capped by the plan; dead letters are
  capped at 1,000 per consumer for 7 days, and a dead-letter copy that would push the tenant over `max_stream_mb` is
  kept without its body. Reply and read pages are capped at 1 MiB; event bodies at 1 MiB.
- **Guest input.** Every number from a guest is bounded and saturating. Header names and values are validated (`x-rt-`
  is reserved; no CR, LF or NUL). Bodies are length-checked in the host and again in the store. Store SQL is
  parameterized, and no guest SQL reaches `streams.db`.
- **Storage.** `streams.db` sits beside `queue.db` with the same file permissions. Stream bytes are a separate quota
  from `storage_mb`.
- **Names and logs.** Subjects and topic names are not secrets: they appear in topic views, consumer views and the
  `pubsub.published` audit row. Do not put secrets or personal data in them. Bodies and headers never appear in logs,
  audit data or metric labels.

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

**Debug builds only: the e2e origin allowance.** Under `#[cfg(debug_assertions)]`, and so absent from release builds,
`RT_TEST_LOCAL_ORIGIN=<host>:<port>` with `RT_TEST_LOCAL_CA_FILE=<pem>` lets the archive fetcher reach exactly that
origin although its address is private, trusting that CA. `RT_TEST_LOCAL_ORIGIN_GUESTS=1` extends the same origin to
guest `fetch` (the console's calls to its Git host). Host rules and every other address stay refused. The R4 harness
(`runtime-web/e2e/git-deploy`) is the only user. `crates/rt-host/tests/local_test_origin.rs` checks every use is gated,
and release has an empty module in its place, so an ungated caller does not compile. Never run a debug build in
production.

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
  becomes a path, so there is no path traversal. The name in the row is checked against the generated format
  whenever it becomes a path ([Backups](#backups)). Files are created with `create_new`, mode `0600`. Keys are 1 to
  1024 bytes.

### Data browser

`/v1/services/{s}/data/*` and the `data_*` MCP tools read data from outside the guest, and have no write path. Each layer stands alone (`crates/rt-store/src/browse.rs`): a connection of its own per request, opened `SQLITE_OPEN_READ_ONLY` plus `query_only` and never the writer or a pooled reader, whose statement cache could hold a statement judged by the looser guest authorizer; exactly one statement, `sqlite3_stmt_readonly`, with columns; an allowlist authorizer (`SELECT`, reads, functions, and only the pragmas `table_info`, `table_xinfo`, `index_list`, `index_info`, `foreign_key_list`), which refuses `_rt_*`, all `sqlite_*` tables including `sqlite_schema` and `sqlite_dbpage`, `dbstat`, other `pragma_*` functions, and `load_extension`; a 2 s budget and row, byte and cell caps. Sorts may spill to a temp file until the budget ends them. A request waits at most 1 s for a reader permit and a connection slot, then `503 sql_busy`; it never evicts.

Every read writes its event (`data.queried`, `data.kv_read`, `data.blobs_read`) to `meta.db` before it answers, and a failed write refuses the read. Events hold the actor, a SHA-256 of the SQL or key, counts and the outcome; never SQL text, arguments or values. The runtime keeps no per-path access log, so `/data/kv/{key}` paths are not logged. The system tenant, `sys-` services and tenants on plan `rtweb` cannot be browsed (`403 reserved`). Rows reaching an assistant are prompt-injection content: MCP wraps them as `untrusted_data`. If at-rest encryption lands, this route reveals what it protects, like the export.

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

### SQL migrations

`versions.migrations` is written only from upload validation or the write-once `POST …/versions/{v}/migrations` (WASM), never from `meta`, which clients write. Migrations run at a promote on the writer, in one transaction, under the authorizer above, statement by statement and never from the prepared-statement cache, with a budget of `deploy.migration_budget`. Only the `_rt_migrations` rows are written with the authorizer off. Scope `deploy` can change a schema, as guest code already can. Errors carry a sanitized SQLite message and no host paths; audit and events carry names and hashes, never SQL. SQL is kept in `meta.db`, at most 900 KiB per version. There is no per-hour limit yet: dry runs have the heavy-job rule, and promotes the budget and the per-service lock.

### Synchronous `std:sqlite` (`bun:sqlite`)

- Uses the same authorizer, limits, caps and page ceiling as the async path.
- A multi-statement batch is checked **statement by statement**, so `SELECT 1; VACUUM INTO …` can't slip through.
- Transactions only go through host `begin`/`commit` (savepoints for nesting). A failed `COMMIT` rolls back. A
  transaction left open by a killed task is rolled back. The connection never silently falls back to autocommit
  while a transaction is open.

---

## Backups

`/v1/node/status`, `/v1/node/series`, `/v1/node/stats` and `/v1/node/events` are operator only as well: they carry host facts and rows of every tenant, a tenant key gets `403 insufficient_scope`, and MCP has no tool for them. Host facts never include a host path (the disk has a label). `x-rt-tenant` narrows `/v1/node/stats` to one tenant; anything mapped to a tenant-facing surface must set it.

`/v1/node/backups*` is operator only; a tenant key gets `403 insufficient_scope`. MCP has no backup tool. Download and delete are audited (`backup.created`, `backup.downloaded`, `backup.deleted`).

**An archive is hostile input**, signed or not. `crates/rt-backup/src/untrusted.rs` holds the rules, and app import uses the same module:

| Threat | Rule |
|---|---|
| Forged archive with consistent digests | `manifest.mac`: HMAC-SHA256 of the manifest under HKDF `rt/backup-mac/v1` of the master key, checked in constant time before the manifest is parsed. `--no-auth` is an explicit operator choice and skips this check only |
| Path traversal, absolute paths | A tar path is parsed into a closed set of shapes, and the target rebuilt from the parts. Anything else is refused |
| Symlinks, hardlinks, devices | Regular files only, created `O_CREAT \| O_EXCL \| O_NOFOLLOW`, mode `0600`, under a directory the restore made |
| Crafted `_rt_blobs.file` (`/data/master.key`) | `Path::join` with an absolute name replaces the base, so a row could read or unlink any host file. Every row must have the exact generated name format, name a file in the archive at the recorded size, and be the only row naming it. Independently, the store reaches blob files only through a checked helper, so a bad name is refused at `get`, and `delete` and overwrite unlink nothing |
| Decompression bomb | Output capped at the size the manifest declares, whose sum is checked against free disk; zstd window at most 128 MiB |
| Hostile database | Opened defensively (no triggers, no views, no trusted schema), `quick_check`ed. Node databases may hold no trigger or view; a service database none on `_rt_*`. A newer schema is refused |
| Archive path from a row | `…/archive`, `DELETE` and retention build the path from a `bkp_<ulid>` id. `backups.local_path` is never followed, since a restored `meta.db` can say anything |

**The console.** `…/archive` holds every tenant's data, and `DELETE` destroys the recovery path. A console binding lists explicit paths (`rt_api::backups::CONSOLE_BINDING_PATHS`), because `*` in `allowed_paths` spans segments.

**Exclusions.** The master key, root token and `rt.toml` never enter an archive. Sealed secrets are in `meta.db`, and need the master key to open.

**Encryption.** With `backup.encrypt_to`, the tar is streamed through `age` to the file, so no plaintext archive is on disk (one database snapshot at a time stays in the private scratch directory while packed), and `/archive` serves ciphertext only. Recipients are public, so anyone with bucket write access can encrypt to them: the MAC, not `age`, authenticates, and `.sha256`/`.complete` are transport only. Without recipients an archive is plaintext: treat it like the data volume. Local mode is `0600`/`0700`.

**S3.** Required to be encrypted unless `allow_plaintext_remote`. Credentials come from files or env, are zeroized and redacted, and errors carry only status codes. Each session checks the endpoint against the egress rules (TLS, no private or reserved address except explicit loopback, denied ports and hosts), pins the client to the checked addresses, and follows no redirect or proxy. The host is on the guest self-denylist.

**A restore brings back the past.** Revoked keys, disabled tenants and removed grants return, and the audit trail of the window is lost. `rt backup restore` prints the snapshot age and active key count, offers `--revoke-keys-before` (cascading to minted keys), and writes a `node.restored` event, reported as `last_restored_at` in the status.

### App bundles

`POST /v1/services/import` takes a file from anyone (`crates/rt-backup/src/app.rs`, `crates/rt-store/src/import_check.rs`). A fork's bundle takes the same path. Nothing in a bundle runs at import.

| Attack | Defense |
|---|---|
| Crafted `_rt_blobs.file` | `untrusted::check_service_db`, as for a restore: exact generated name, a listed file at the recorded size, one row each |
| Tar traversal, links, devices, long-name or pax entries | Raw tar entries, regular files only, exact path shapes, listed in the manifest, once each, created `O_EXCL \| O_NOFOLLOW`, mode 0600 |
| Decompression bomb | Spool counted as it arrives (`import.max_bundle_mb`); declared sizes at most 8 times that and at most 200:1 above 64 MiB, checked with free disk before reading; each entry capped at its declared size; bounded zstd window |
| Truncated or altered bundle | Each entry's size and SHA-256 must match; a listed entry that is missing refuses the bundle |
| Altered runtime tables | The `sqlite_master` text of each `_rt_*` object must equal, byte for byte, what this binary creates. Unknown `_rt_*` objects, and any object on a `_rt_*` table that is not the node's own, are refused |
| Malicious triggers, views, virtual tables | The file is opened with triggers and views off and no trusted schema. Triggers and views naming `_rt_`, any schema naming a function the authorizer denies, and virtual tables other than `fts5` and `rtree` are refused. The rest is replayed on an empty in-memory database under the guest authorizer, and what would fire each trigger is compiled, never run |
| Timers | `_rt_durable_alarm` is emptied, with triggers off, so no guest code wakes because data arrived |
| Manifest as config | Unknown fields refuse the bundle at every level. Settings go through `POST /v1/services`, so plan, trigger and `sys-` checks apply. `tenant`, `project`, `environment` and scratch state never come from it |
| Poisoned artifacts | Compiled artifacts are never imported. Digests are recomputed, and each version is validated and built from source, including the Wasm policy check |

**Fork.** A key's `?from=` goes through `load_service`, so another tenant's service is `404`. Only the root token reaches across tenants. **Export** needs `data:admin` and is audited, since a bundle holds data and env values; sealed secrets stay behind, as names. **Limits.** One export or import per tenant, at most 256 bundle records per node, 1 hour each, none across a restart. A crash between creating the service and finishing the import can leave an incomplete service, which the operator deletes; a client that disconnects cannot, because the import runs on its own task.

**Deleted data.** Archives keep deleted tenants' data until retention removes them.

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

## Data-change events and external secrets

**Data-change events.** Generated `_rt_ev_*` triggers add to the reserved `_rt_changes` table in the guest's own transaction. The authorizer allows that one insert only when the accessor is such a trigger; guests cannot read, write, make, drop or alter any of it, and `DROP TABLE` on a subscribed table is refused until it is unsubscribed. Generated SQL quotes every name read from the schema. Copies (branches, bundles) lose the triggers and rows, and an import refuses a bundle holding `_rt_ev_*`. A topic is looked up only in the service's own tenant, project and environment. Enabling requires `data:read`, also through `PATCH`. A `row` payload stays in the stream for its retention after the row is deleted. Audit: `data_events.updated`.

**External secrets.** A Vault path is `path_template` filled from the authenticated tenant and the service's project and environment, plus a tenant-chosen relative path refused on `..`, `//`, `%` and leading `/`; startup refuses a template without all three variables. Requests go through a trusted `GuardedEgress` with no redirects, verified TLS (plain http to loopback only), and the Vault host joins the guest egress self-denylist. The token is read from a file, marked sensitive, and never logged or returned; Vault's own text never reaches a tenant. Restrict the node's Vault policy per tenant prefix as well. An outage keeps the last sealed value, and running services do no Vault I/O. Revocation lags by `poll`. Audit: `secret.ref_written`, `secret.ref_deleted`; events: `secret.synced`, `secret.sync_failed` (names and codes only).

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
| `services:read` | `list_projects`, `list_environments`, `list_sdks`, `get_sdk_contract`, `list_services`, `get_service`, `list_versions`, `migrations_status`, `list_schedules`, `list_objects`, `list_sockets`, `list_topics`, `read_topic`, `list_consumers`, `list_invocations`, `list_branches`, `get_switch` |
| `services:write` | `create_project`, `create_service`, `update_service`, `delete_service`, `create_topic`, `create_branch`, `create_scratch` (`copy_data` also needs `data:admin`, checked by the route), `delete_branch` |
| `deploy` | `deploy_code`, `deploy_from_url`, `rollback`, `switch_service` (a branch or another service as the source also needs `services:read`, checked by the route), `rollback_switch` |
| `sockets:write` | `close_socket` |
| `data:read` | `data_tables`, `data_query`, `data_kv_list`, `data_kv_get`, `data_blobs_list` |
| `logs:read` | `get_logs`, `get_stats` |
| `env:write` + `services:read` | `set_env` |
| `secrets:write` | `set_secret` |
| `keys:write` | `list_tokens`, `create_token`, `revoke_token` |
| `queue:write` | `reset_consumer`, `redrive_invocations` |

`list_topics`, `read_topic`, `list_consumers`, `get_data_events` and `list_invocations` need `services:read`, and `create_topic` needs
`services:write`. Topic delete and update, append, publish and DLQ purge are not MCP tools. `read_topic` returns
record bodies as untrusted data, not instructions.

---

## Observability and audit

**Audit events.** Each control change writes an `events` row (actor, tenant, service, data) and a `tracing` line on
target `rt::audit`.

| Area | Events |
|---|---|
| Tenants, services | `tenant.created/updated/deleted`, `service.created/updated/deleted`, `service.environment_changed`, `scratch.created/deleted`, `scratch.copied_data` (source and bytes, never data) |
| Versions | `deploy`, `rebuild`, `rollback`, `live.changed`, `switch`, `version.migrations_set` |
| Config | `env.written` (names only), `secret.written`, `secret.deleted` |
| Hosts | `host.attached/detached/verified/expired` |
| Schedules, queues | `schedule.created/updated/deleted`, `dlq.redrive`, `dlq.purge` |
| Events | `topic.created/updated/deleted/appended` (`appended` carries `{count}` only), `pubsub.published` (`{subject, receivers}`), `consumer.reset`, `consumer.dlq_purged`, `invocations.redriven`, `invocations.purged` |
| App bundles | `app.exported`, `app.downloaded`, `app.imported`, `app.import_refused` (names and counts, never env values) |
| Backups, restore | `backup.created`, `backup.downloaded`, `backup.deleted`, `node.restored` (written into the restored `meta.db`) |
| Data, projects, keys | `data.exported`, `data.queried`, `data.kv_read`, `data.blobs_read`, `project.created/updated/deleted`, `key.created`, `key.deleted` (with `cascaded`) |
| Security | `security.artifact_tamper` (separate event) |

The data of the version events that flip a service:

| Event | `data` |
|---|---|
| `version.migrations_set` | `{version, migrations: [{name, sha256}]}` (never the SQL) |
| `switch` | `{switch, from, to, source_service, source_version, digest, git_sha, sockets_closed}` |
| `rollback` from `POST …/switches/{id}/rollback` | `{switch, rollback, from, to, digest, git_sha}`: `switch` is the switch undone, `rollback` the new one |
| `rollback` from `PUT …/live` to an older version; `live.changed` otherwise | `{from, to, sockets_closed}` |

`git_sha` is always `null` for now (see [Switches and branches](#switches-and-branches)). A consumer must expect both shapes of `rollback`.

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

## Telemetry

A node with a telemetry URL sends one anonymous report a day. A build without `RT_TELEMETRY_URL` and with no URL configured sends nothing.

- **Payload**, exactly: `install_id` (16 random bytes in hex, derived from nothing, created on the first send), `version`, and the counts `requests` (last 24 hours), `projects`, `services`, `organizations` (without the system tenant) and `users` (the count a console pushes with `PUT /v1/telemetry/console`, `null` when absent or older than 48 hours). No names, slugs, addresses, secrets or guest data.
- **Inspect it:** `21ci telemetry show` or `GET /v1/telemetry/preview` (operator). `rt telemetry show` reads `meta.db` offline, so it shows `requests` as 0. All of them use the function that builds the real report; none sends anything or creates an id.
- **Opt out:** `DO_NOT_TRACK=1` beats everything, then `telemetry.enabled = false`. `RT_TELEMETRY_URL` at run time beats `telemetry.url`. `telemetry.url = ""` sends nothing; `"auto"` is the build-time URL. A URL that is not plain `https` sends nothing.
- **Network:** `GuardedEgress` for both transports, so SSRF, port and host rules apply, with no redirects, no pooling, no cookies and no authorization header. WebSocket first; one POST only if that got no answer. 10 seconds per attempt, no other retries, nothing again for a day. Charged to no tenant.
- **Failure:** one debug log line. No readiness check sees it, and the task is aborted at shutdown. The first report is 24 hours after the first start. `rt serve` logs a notice at startup while telemetry is on.
- **Limits:** reports are not authenticated, so totals are estimates. A restored backup keeps the `install_id`. The receiver sees the node's address during the connection; its own rules are in its README.
- **Audit:** `telemetry.console` records the pushed count.

## Hardening settings

| Setting | Default | Effect |
|---|---|---|
| `telemetry.enabled`, `telemetry.url`, `DO_NOT_TRACK` | `true`, `"auto"`, unset | The daily report ([Telemetry](#telemetry)) |
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
| `sched.min_run_time_max_ms` | 600000 | Largest keep-alive (`scaling.min_run_time_ms`) a service may set on this node; the plan may lower it further. Kept instances count against the memory governor and the `reuse_max_idle*` caps; v1 has no per-tenant cap on kept instances, so a tenant with many services can keep up to one per service |
| `sched.aging_factor` | 4 | Admission order: a waiting task ages toward the front of its tenant's queue, so long waits are not starved. `0` = arrival order within a tenant |
| `async.enabled` (per service) | `false` | Accept async invocations (`Prefer: respond-async`, `X-Rt-Invocation-Type: Event`). The plan's `max_async_pending` bounds the backlog |
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
| **S3 / OTLP** | Only backups can go to S3 (`[backup.s3]`). The S3 and OTLP keys for blobs, artifacts and traces are accepted but have no code behind them. Blobs and artifacts are local only. |
| **Unused settings** | `secrets.cache_ttl` is not used; the env cache lives until it is invalidated or idle for 600 s. |
| **Branch plan caps** | `max_branches` and the `copy_data` size limit are checked outside the insert transaction, so concurrent creates can overshoot by the number of requests in flight. |
| **Rate-rule cell cap** | `ingress.rate_keys_max` has no upper bound, and its memory (about 96 bytes per key) is outside the memory governor. Keep it well under the node's budget. |
| **Backup retention, mixed formats** | If one backup id exists remotely as both `.tar` and `.tar.age`, retention prunes only one of them. Delete the other by hand after changing `encrypt_to`. |
| **Offline restore memory** | `rt backup verify` and `restore` may allocate up to 256 MiB from a manifest size in the archive before its MAC is checked. Run them on untrusted archives only with that much memory to spare. |

If you host mutually untrusting tenants, run one node per trust domain (for example one VM per tenant group) until
process and kernel isolation ship.

---

## Operator hardening checklist

- [ ] Run a **release** build.
- [ ] Turn on `[backup]`, copy archives off the node, and practise a restore. List backup paths in a console binding one by one.
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
| **security@21ci.uz** | if you cannot use GitHub. If the report contains exploit details, prefer a private advisory, or ask for an encryption key first |

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
# expected: the fingerprint below
```

Package signing key: `21 Cloud Packages <packages@21ci.uz>`, fingerprint
`2018 24B4 6B59 F184 7241  11EE 67A8 C631 FC31 2DB3` (RSA 4096, expires 2029-10-23; a changed key is announced in
the release notes).

The Homebrew formulae pin the SHA-256 of each tarball; `brew` verifies it on install.
