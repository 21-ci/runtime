---
title: Tenants, plans and keys
description: Who is who on a node (operator and tenants), how the root token and scoped tenant keys work, and how plans set every tenant's quotas and limit maxima.
---

# Tenants, plans and keys

A node has one **operator** and any number of **tenants**. The operator decides what each tenant may do through
**plans**; tenants work through scoped **API keys**. This page explains the credentials, the scopes, the plan
fields and how they are enforced. Operators need all of it; service developers mostly need [keys and
scopes](#tenant-keys) and the [plan limits](#limits-maxima) that cap their services.

## Operator and tenants

```text
Operator      rt.toml (plans, bindings, SDKs), RT_ROOT_TOKEN, RT_MASTER_KEY
 └─ Tenant    ten_…   plan + overrides, API keys, usage
     └─ Service   svc_…   runtime, profile, limits ≤ plan, access, triggers, egress
         └─ Version   1, 2, 3 …   immutable; one is live
```

| | Operator <span class="rt-badge rt-badge--operator">operator</span> | Tenant |
|---|---|---|
| Credential | Root token (`RT_ROOT_TOKEN` or `RT_ROOT_TOKEN_FILE`) | Tenant key `rtk_<key_id>_<secret>` |
| Sees | Every tenant, service and node route | Only its own services, data, logs and events |
| Can do | Everything, including `/v1/tenants*` and `/v1/node/*` | What the key's scopes allow, on its own services |
| Typical holder | Whoever runs the node, or the operator's own product backend | A developer, a CI job, the operator's product acting for one customer |

Resources of another tenant answer `404`, never `403`, so a tenant cannot even learn that they exist.

### The root token and `x-rt-tenant`

The root token authenticates the operator on every control route, sent as `Authorization: Bearer <token>` (or
`x-rt-key: <token>`). A release build started without `RT_ROOT_TOKEN` answers every control call with
`503 control_disabled`; a debug build without one leaves the API open and refuses to start unless the control
listener is a loopback address.

Services belong to a tenant, so the root token alone cannot create one. To act **for** a tenant, add the header
`x-rt-tenant: ten_…`: the request then runs in that tenant's scope (an unknown tenant is a `404`).

=== "21ci"

    ```bash
    21ci --as-tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ service create hello
    # or keep it in a context: 21ci config set-context acme --api "$RT_API" --token "$RT_ROOT_TOKEN" --tenant ten_…
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" -H "x-rt-tenant: ten_01M3XM135SCP9EP41ZZXAQ5CCZ" \
      -H 'Content-Type: application/json' -d '{"slug":"hello","runtime":"deno"}' "$RT_API/v1/services"
    ```

### Creating a tenant

A tenant needs a name and a plan that exists in the node's configuration (`free` always does).

=== "21ci"

    ```console
    $ 21ci tenant create acme --plan pro
    ID:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    Name:      acme
    Plan:      pro
    Disabled:  false
    Created:   2026-10-02T06:15:06Z
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"acme","plan":"pro"}' "$RT_API/v1/tenants"
    # 201 {"id":"ten_…","name":"acme","plan":"pro","overrides":{},"created_at":…,"disabled":false}
    ```

`PATCH /v1/tenants/{t}` (`21ci tenant update`) changes `name`, `plan`, `overrides` or `disabled`. Disabling a
tenant (`--disable`) takes effect at once: its services stop routing and its keys stop authenticating until it
is enabled again (`--enable`).
`DELETE /v1/tenants/{t}` answers `409 tenant_not_empty` while the tenant owns services; `?purge=1`
(`21ci tenant delete --purge`) deletes them and their data too. The tenant routes are operator-only, except
that a tenant key may read its own tenant's [usage](#usage); see [`21ci tenant`](../cli.md#tenant-operator).

## Tenant keys

A tenant key is the credential a tenant (or the operator's product on its behalf) uses for the control API and,
for `access: key` services, for invoking.

- **Format:** `rtk_<key_id>_<secret>`. The node stores only an HMAC of the secret, so the full key is returned
  **once**, in the creation response. Store it immediately; it cannot be shown again.
- **Scopes:** a non-empty list of permissions (table below).
- **Project and environment restriction:** optional `project_ids` (`prj_…`) and `environments` (labels); `null` means
  all. A service is reachable with the key only when its project and its environment are both admitted; every other
  service of the tenant is a `404` to it. An explicit `project_ids` list does not grow with new projects.
- **Expiry:** optional `expires_at` (RFC 3339). An expired key gets `401 unauthorized`.
- **Revocation:** `DELETE /v1/tenants/{t}/keys/{key}` (or `DELETE /v1/keys/{key}`) takes effect on the next request.
- **Self-service:** a key with `keys:write` creates, lists and revokes keys of its own tenant through `/v1/keys`
  (`21ci token`), never with more scopes, projects, environments or lifetime than itself, at most 200 live keys per tenant. See
  [Create your own tokens](../guides/access.md#create-your-own-tokens).
- **Brute force:** failed authentications are rate-limited per client address, 10 per minute, then
  `429 rate_limited` with `Retry-After`.

=== "21ci"

    ```console
    $ 21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name ci --scopes deploy,services:read,services:write
    ID:        key_01M3XM1AG5HN6WY9S6JQ550JQV
    Name:      ci
    Scopes:    deploy,services:read,services:write
    Projects:  all
    Environments: all
    Expires:   never
    Created:   2026-10-02T06:15:14Z
    Secret:    rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
    The secret is shown only once: store it now.

    $ 21ci key list --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    $ 21ci key revoke key_01M3XM1AG5HN6WY9S6JQ550JQV --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    ```

    Without `--scopes` the CLI grants every scope except `data:admin`. `--project prj_…,prj_…` and `--environment
    production,staging` restrict the key (both default to all), and `--expires-at 2027-01-01T00:00:00Z` sets an expiry. See [`21ci key`](../cli.md#key-operator).

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"ci","scopes":["deploy","services:read","services:write"],"expires_at":"2027-01-01T00:00:00Z"}' \
      "$RT_API/v1/tenants/$TENANT/keys"
    # 201 {"id":"key_…","name":"ci","scopes":[…],"project_ids":null,"environments":null,"expires_at":…,"created_at":…,
    #      "last_used_at":null,"secret":"rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…"}

    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/tenants/$TENANT/keys"          # no secrets
    curl -sS -X DELETE -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/tenants/$TENANT/keys/$KEY_ID"
    ```

### Scopes

| Scope | Allows |
|---|---|
| `services:read` | Every `GET` on services, versions (not build logs), env, secret names, hosts, schedules, queue stats, DLQ and events |
| `services:write` | Create, patch and delete services; hosts; schedules |
| `deploy` | Upload versions, rebuild, `PUT …/live` (rollback) |
| `invoke` | Invoke services that have `access: key` |
| `logs:read` | Logs (JSON and SSE), build logs, stats |
| `env:write` | `PUT …/env` |
| `secrets:write` | `PUT` and `DELETE …/secrets/{NAME}` |
| `queue:write` | Enqueue messages, redrive and purge the DLQ |
| `data:admin` | `GET …/data/export` (a SQLite snapshot of the service's KV and SQL) |
| `keys:write` | `POST` / `GET /v1/keys`, `DELETE /v1/keys/{key}`: the tenant's own keys, never more than the calling key holds |

A command that reads before it writes needs both scopes: `21ci push` needs `deploy` plus `services:read` for the
slug lookup, and `services:write` with `--create`. The [CLI scope table](../cli.md#scopes) lists every
command. A route the key's scopes do not cover answers `403 insufficient_scope`; operator-only routes answer
tenant keys the same way.

!!! tip "One key per job"
    Give CI a key with `deploy` and `services:read` only, limited to the services it ships, and an expiry. Give
    the operator's product a separate key per tenant. Revoking one never breaks the other.

Changing a service's `environment` decides who may call it, so it needs the root token or a key **without** a
project or environment restriction; a restricted key gets `403 insufficient_scope`.

## Plans

A plan is a named set of quotas and maxima. Plans live in the node configuration, never in the database: each
tenant references one plan by name and may carry per-tenant overrides.

The operator lists the node's plans with `GET /v1/plans` (`21ci plan list`, `21ci plan get <name>`). A tenant
sees its own plan, with overrides applied, in `GET /v1/me` (`21ci whoami --json`).

### The built-in `free` plan

`free` always exists. These are its values, and also the defaults for every key a custom plan leaves out:

| Key | `free` | What it limits |
|---|---|---|
| `max_services` | 10 | Services per tenant |
| `max_concurrency` | 10 | In-flight tasks across the tenant (0 = unlimited); over it tasks **wait** |
| `max_instances` | 4 | Most instances of one service; caps `scaling.min_instances` and `scaling.max_instances` (0 = the node default, the profile ceiling) |
| `rps` | 50 | Tenant-wide token bucket on invocations (0 = unlimited) |
| `invocations_per_day` | 100,000 | Invocations per UTC day (0 = unlimited) |
| `storage_mb` | 100 | KV + SQL + blobs across all services |
| `egress_mb_per_day` | 1,000 | Outbound `fetch` bytes per day |
| `max_schedules` | 3 | Schedules per service |
| `min_schedule_seconds` | 60 | Shortest schedule period (interval, or the gap between two cron fires) |
| `max_queue_depth` | 10,000 | Messages waiting in one service's queue |
| `builds_per_hour` | 20 | Version uploads per tenant per hour |
| `max_pinned_mb` | 0 | Memory a tenant may pin resident (`scaling.keep_resident`). Accepted, but pinning is not wired up in this release <span class="rt-badge rt-badge--planned">planned</span> |
| `max_async_pending` | 10,000 | Pending async invocations per service (`429 async_backlog_full` above it) |
| `max_topics` | 10 | Stream topics per tenant |
| `max_topic_partitions` | 4 | Partitions of one topic |
| `max_stream_mb` | 100 | Record bytes across the tenant's topics, dead letters included. Separate from `storage_mb` |
| `max_stream_retention_hours` | 72 | Longest retention a topic may set (default 24 h) |
| `allowed_triggers` | `http`, `interval`, `queue`, `call`, `websocket`, `pubsub`, `stream` | Trigger kinds the tenant may use (no `cron` on `free`) |
| `limits_max` | `wall_ms` 30,000, `cpu_ms` 1,000, `memory_mb` 128, `min_run_time_ms` 60,000 | Highest per-task limits a service may set. `min_run_time_ms` is the largest keep-alive (`scaling.min_run_time_ms`) |
| `custom_host_suffixes` | none | Domains under which tenants may claim custom hosts |
| `allow_cross_tenant_calls` | `false` | Whether this tenant's services may call other tenants' services |
| `blocking_share` | 0.25 | Share of the blocking pool (SQLite, file I/O) one tenant may hold under contention |
| `egress_default` | `allow` | `allow`: public internet through the SSRF guard; `deny`: only per-service allowlists |

### Defining plans <span class="rt-badge rt-badge--operator">operator</span>

Add plans under `[plans.<name>]` in `rt.toml`. Keys you leave out take the values above; you may also redefine
`free` itself.

```toml title="rt.toml"
[plans.pro]
max_services         = 50
max_concurrency      = 100
rps                  = 500
invocations_per_day  = 0                    # unlimited
storage_mb           = 2048
max_schedules        = 20
min_schedule_seconds = 10
allowed_triggers     = ["http", "interval", "cron", "queue", "call"]
custom_host_suffixes = [".fn.example.com"]

[plans.pro.limits_max]
wall_ms   = 120000
cpu_ms    = 5000
memory_mb = 256
```

Every key is documented in [`[plans.<name>]`](../api.md#plans-built-in-free-more-under-plansname) and
[`limits_max`](../api.md#plans-built-in-free-more-under-plansname).

### Quotas and limit maxima

A plan holds two different kinds of numbers:

- **Quotas** are counters and rates for the whole tenant (or one service queue): how many services, schedules,
  invocations, bytes. Going over one refuses the operation.
- **Limit maxima** (`limits_max`) cap the per-task limits (`wall_ms`, `cpu_ms`, `memory_mb`) a service may set
  in its `limits` block. They are not a pool: every service of the tenant may use the full maximum.

| Quota | Checked at | Over it |
|---|---|---|
| `max_services`, `max_schedules` | Control API, on create | `403 quota_exceeded` |
| `max_instances` | Service create and update | `scaling.min_instances` / `max_instances` above it are clamped; `effective` shows source `plan` |
| 100 projects, 200 self-service keys per tenant (fixed) | Control API, on create | `403 quota_exceeded` |
| `min_schedule_seconds` | Control API, on schedule create or update | `422 schedule_too_frequent` |
| `rps` | Ingress, per invocation | `429 rate_limited` + `Retry-After` |
| `invocations_per_day` | Ingress, per invocation | `429 quota_exceeded` + `Retry-After` until UTC midnight |
| `max_concurrency` | Admission | The task waits for a slot; after the queue timeout `503 queue_timeout` |
| `storage_mb` | KV, SQL and blob writes | `storage_quota` error in guest code |
| `egress_mb_per_day` | Egress guard | `fetch` rejects with `egress_quota` |
| `max_queue_depth` | Enqueue (API or guest) | `429 queue_full` / `queue_full` |
| `builds_per_hour` | Version upload | `429 build_quota` |

The daily counters are kept in memory, flushed to `meta.db` every few seconds and seeded from today's usage at
startup, so a restart does not reset them.

#### Limits maxima

A service sets its own limits in its `limits` block. A value above the plan maximum is refused, never clamped
silently:

```console
$ curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
    -d '{"limits":{"cpu_ms":5000}}' "$RT_API/v1/services/$SVC"
{"error":{"code":"limit_exceeds_plan","message":"`limits.cpu_ms` 5000 exceeds the plan maximum 1000",
          "details":{"field":"cpu_ms","requested":5000,"max":1000}}}
```

Without explicit limits a service runs with the per-trigger defaults, capped by the plan maxima. On `free` that
means scheduled and queue tasks get 30 s / 1 s rather than their 300 s / 10 s defaults; see
[limits](execution.md#limits).

### What `allowed_triggers` does

| Trigger missing from the plan | Effect |
|---|---|
| `http` | Every invoke of the tenant's services, on the public **and** internal listener, is `403 x-rt-reason: trigger_not_allowed` |
| `interval`, `cron` | Creating or updating such a schedule is `403 trigger_not_allowed`; fires of existing ones are dropped |
| `queue` | Enqueueing is refused (`403 trigger_not_allowed`, or `trigger_not_allowed` in guest code); messages already queued wait without using delivery attempts |
| `call` | `call()` to the tenant's services fails with `trigger_not_allowed` and starts no task |
| `pubsub` | Services with `events.subscribe` get no pub/sub deliveries; `events.subscribe` on a service is refused (`403 trigger_not_allowed`) |
| `stream` | Services with `events.consume` stop consuming; their topics keep their records |
| `websocket` | Upgrades are refused with `403 trigger_not_allowed` |

The service owner has the same switches per service in the `triggers` block; both must allow a trigger. See
[Triggers](triggers.md#turning-triggers-on-and-off).

### Other plan switches

- **`custom_host_suffixes`**: a tenant may claim a custom host only under one of these suffixes
  (`403 host_not_allowed` otherwise); the claim stays `pending_verification` until the node finds the DNS TXT
  record or the operator approves it. With the default empty list only the operator attaches hosts. See
  [Custom domains](../guides/custom-domains.md).
- **`allow_cross_tenant_calls`**: lets this tenant's services call services of other tenants. The target must
  still list the caller's service id in its `call.allowed_callers`. See [service calls](triggers.md#service-calls).
- **`blocking_share`**: under contention, one tenant may hold at most this share of the blocking-pool threads
  that run SQLite and file work, so one tenant's heavy queries cannot starve the others.
- **`egress_default`**: `deny` turns outbound `fetch` into allowlist-only for the plan's services. See
  [Security model](security.md).

### Per-tenant overrides

`overrides` is a JSON object of plan keys stored on the tenant. Each top-level key replaces the plan's value for
that tenant only (`limits_max` is replaced as a whole). Set it when you create the tenant or later with
`PATCH`, which replaces the stored overrides. The change applies to the tenant's next request.

=== "21ci"

    ```bash
    21ci tenant create bigcorp --plan pro --overrides '{"rps": 2000, "max_services": 200}'
    21ci tenant update ten_01M3XM135SCP9EP41ZZXAQ5CCZ --overrides '{"rps": 2000, "limits_max": {"wall_ms": 60000, "cpu_ms": 2000, "memory_mb": 256}}'
    ```

=== "curl"

    ```bash
    curl -sS -X PATCH -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"plan":"pro","overrides":{"rps":2000,"limits_max":{"wall_ms":60000,"cpu_ms":2000,"memory_mb":256}}}' \
      "$RT_API/v1/tenants/$TENANT"
    ```

## Usage

The node records usage per service and UTC day: invocations, CPU milliseconds, egress bytes and storage bytes.
The operator reads any tenant's usage. A tenant key with the `logs:read` scope may read its own tenant's usage
too, limited to the services the key may touch (`21ci tenant usage` without an id uses the key's tenant):

=== "21ci"

    ```console
    $ 21ci tenant usage ten_01M3XM135SCP9EP41ZZXAQ5CCZ --from 7d
    DAY         SERVICE                         INVOCATIONS  CPU_MS  EGRESS     STORAGE
    2026-10-06  svc_01M3XM1AMYAKX6EREP6J536ATG  1200         8400    2.0 MiB    1.0 MiB
    2026-10-07  svc_01M3XM1AMYAKX6EREP6J536ATG  300          2100    512.0 KiB  1.0 MiB
    TOTAL       -                               1500         10500   2.5 MiB    -
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/tenants/$TENANT/usage?from=2026-10-01&to=2026-10-07"
    # [{"service_id":"svc_…","day":"2026-10-07","invocations":300,"cpu_ms":2100,"egress_bytes":524288,"storage_bytes":1048576}, …]
    ```

For request counts and latencies per service, use `GET /v1/services/{s}/stats` (see
[Observability](observability.md)).

## Changing plans at runtime <span class="rt-badge rt-badge--operator">operator</span>

Edit `rt.toml` and send `SIGHUP` to the `rt` process (`docker compose kill -s SIGHUP rt`). The node re-reads the
file and swaps in the new plans without a restart; quotas, limit maxima, allowed triggers, egress stance and
blocking shares all follow on the next check. If the file does not parse or validate, nothing changes and the
error is logged. A reload that would remove a plan some tenant still uses is refused.

When you lower a plan below a value a service set explicitly, the plan wins at runtime: the service runs with
the plan maximum and its `effective` view shows the source `plan`. New API writes above the plan are still
refused with `limit_exceeds_plan`.

Moving one tenant to another plan (`21ci tenant update ten_… --plan pro`, or `PATCH /v1/tenants/{t}` with
`{"plan": "…"}`) also applies at once.

## See also

- [Security model](security.md): the threat model behind keys, scopes and tenant isolation.
- [Services, versions and builds](services-and-deploys.md): the `limits`, `access` and `triggers` blocks.
- [API reference](../reference/errors.md#control-api-codes) and [Error reasons](../api.md#control-error-codes).
- [Configuration reference: plans](../api.md#plans-built-in-free-more-under-plansname).
