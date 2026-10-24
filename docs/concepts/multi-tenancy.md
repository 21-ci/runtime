---
title: Multi-tenancy
description: What the runtime isolates per tenant and service, what all tenants on a node share, what is best-effort, and a checklist for operators who host other people's code.
---

# Multi-tenancy

A node hosts many tenants in one process. This page separates three things that are easy to blur: what is **isolated**
(enforced by a mechanism you can name), what is **shared** (and what that costs you in risk), and what is only
**best-effort** (fair in normal operation, not a hard promise under attack or overload). Read it before deciding who
may share a node. The mechanisms behind each row are explained in [Execution and isolation](execution.md),
[Security model](security.md) and [Scheduler and autoscaling](scheduler.md).

## What is isolated

| Per tenant or service | Mechanism | Where it stops |
|---|---|---|
| Code execution state | A fresh V8 isolate or wasmtime instance per task. Warm reuse only for the same `(tenant, service, version)`, discarded after errors, kills and limits | The engine's memory safety. No second wall in this release |
| Service data (KV, SQL) | One SQLite file per service under `services/<service-id>/`; the guest SQL authorizer blocks `ATTACH`, pragmas and temp objects | The queue and metadata are in node-wide databases, separated by key |
| Blobs | A directory per service | Same filesystem for everyone |
| Secrets and env | Sealed with AES-256-GCM, bound to `tenant|service|name`, write-only through the API; decrypted only for a task of that service | Decrypted values live in node memory; the master key lives in the same process |
| API keys and visibility | Keys are hashed with a pepper; every lookup is scoped to the key's tenant, and another tenant's resource is `404` | Slugs are one node-wide namespace (see below) |
| Logs, stats, events | Stored per service; readable only with that tenant's keys or the operator's | |
| Outbound network | Every `fetch` passes the egress guard; connections are pooled per tenant and per validated address and never shared | Public internet is reachable by default; restrict with allowlists |
| Resource limits | Per-task CPU, memory and wall limits; plan quotas (`rps`, `invocations_per_day`, `storage_mb`, `egress_mb_per_day`, `builds_per_hour`, `max_concurrency`, schedule and queue limits) | Limits bound a tenant; they do not partition the machine (see below) |
| Calls between services | Same-tenant, same-environment by default; `call.allowed_callers` narrows or widens; cross-tenant needs the plan switch and an explicit service-id entry in the target | |
| Compiled code | MAC-protected files, keyed by content digest | Identical code from different tenants shares compiled bytes, never a sandbox |

## What is shared

| Shared | Why it matters | What reduces the risk |
|---|---|---|
| **One process and address space** | A memory-safety bug in V8, wasmtime or a host function is a node-wide compromise: it reaches other tenants' heaps, decrypted secrets and the master key. The V8 sandbox is not enabled | Keep mutually hostile tenants on separate nodes; update promptly; control who can deploy |
| **One kernel and one filesystem** | No seccomp filter on the process. A disk-filling tenant fills everyone's disk (quotas bound service data, not everything on the volume) | Container or VM around the node; storage quotas; monitor disk |
| **CPU caches and execution units** | Side channels (Spectre-class, cache timing) are not eliminated. The frozen clock removes local high-resolution timers, but a guest can still observe timing across awaits | Not mixing hostile tenants; short-lived isolates (default) |
| **The master key** | Held by the same process that runs tenant code. Loss of the process is loss of every sealed secret | Separate nodes per trust domain; back the key up apart from the data |
| **Node-wide databases** | `meta.db` (tenants, services, hashed keys, sealed secrets, schedules, usage) and `queue.db` (all queue messages) are shared files; separation is by key, not by file | Backups; node isolation |
| **The slug namespace** | Slugs are unique on the node. Creating a service named like an existing one answers `409 slug_taken`, which reveals that the slug exists | Treat slugs as non-secret; use unguessable ones for internal-only services |
| **The memory budget** | One budget covers all tenants. Under pressure the node evicts, drops pools, queues and sheds for everyone | Plan memory maxima; `max_concurrency` per tenant |
| **Scheduler threads and the blocking pool** | Tenants run on the same processors and storage threads | Fair queues and shares (below) |
| **Build threads** | Builds from all tenants queue on the same low-priority threads | `builds_per_hour` per plan |
| **Logs and metrics endpoint** | Operator-only, but node-level metrics aggregate all tenants. Per-service series are off unless `metrics.per_service` is set | Keep the metrics listener private |

## What is best-effort

Fairness mechanisms keep ordinary neighbors from starving each other. They are not a guarantee against a determined
or buggy tenant, and none of them reserves capacity.

| Mechanism | What it does | What it does not do |
|---|---|---|
| Admission with per-tenant turns | When capacity is short, queued requests start in turns across tenants, then across a tenant's services. A tenant can hold at most half of the node's queue | It does not reserve slots for a tenant. A tenant under its cap can still wait because the node is full |
| Plan `max_concurrency` | Caps a tenant's in-flight tasks | A cap of 0 means no cap |
| Blocking-pool share (`blocking_share`) | Under contention a tenant occupies at most that share of the storage threads; idle threads may be borrowed, always leaving one free | A storage-heavy tenant can still fill its share and slow its own services |
| Per-task limits | Runaway tasks are killed at the CPU or wall limit. A running isolate cannot be paused mid-slice, so a CPU hog occupies its thread until terminated; the monitor moves other queued work away | A task can use its whole limit on every request |
| Memory governor | Reserves memory against one budget, grows task heaps in steps, evicts caches and idle sandboxes, then sheds | A tenant that fills its share can force others to queue or be refused growth |
| Quotas | Bound what a tenant may consume per day or in total | They are counted, not reserved |

The honest summary: the runtime prevents one tenant from **reading or changing** another's state, and it limits how
much one tenant can **consume**; it does not give each tenant a guaranteed slice of CPU, memory or disk bandwidth.

## Where the line is

| Situation | Is a shared node reasonable? |
|---|---|
| Your own teams' services | Yes |
| Customers of one product writing automations, with tight plans and review or monitoring | Usually yes |
| Many external customers with arbitrary code, and sensitive data on the node (other tenants' secrets, an operator database reachable by bindings) | Only with a stronger boundary around the node, or one node per customer |
| Anonymous or hostile code | No. Use microVMs or one node per tenant in a VM |

[Isolation compared](isolation-compared.md) lays out the alternatives and how to combine them with this runtime.

## Operator checklist

- **Define plans deliberately.** Set `limits_max` (CPU, wall, memory), `max_concurrency`, `rps`, `invocations_per_day`,
  `storage_mb`, `egress_mb_per_day`, `builds_per_hour`, `max_services`, schedule and queue limits. The built-in `free`
  plan is small; every plan you add widens the blast radius. See [Tenants, plans and keys](tenants-and-plans.md).
- **Restrict egress where you can.** `egress_default = "deny"` with per-service allowlists for plans that need only a
  few APIs; deny your internal ranges (`egress.denied_cidrs`); give tenants **bindings** to reach your platform rather
  than network access.
- **Control who can deploy.** Issue tenant keys with the narrowest scopes, limit keys to services, set expiry, and
  rotate. `allowed_triggers` per plan switches off triggers a tenant does not need.
- **Set `ingress.trusted_proxies`** so client IP rules and per-IP limits see real clients, and keep the control and
  metrics listeners private.
- **Give tenants your API through an operator SDK.** It runs in the runtime under the system tenant, not in theirs, and
  reaches your API through a binding whose credentials no guest sees; it acts for the calling tenant, whose identity the
  runtime stamps ([Operator SDKs and bindings](../guides/operator-sdks.md)).
- **Watch for noisy neighbors.** Per-tenant usage (`21ci tenant usage`), `rt_admission_*` metrics and the memory
  pressure level. Raise a tenant's limits deliberately, and use `tenant update --disable` for an emergency stop.
- **Separate trust domains.** One node per mutually hostile group, with their own master key and root token.
- **Patch.** V8 and wasmtime vulnerabilities are node-wide events; upgrade promptly
  ([Production deployment](../guides/production.md)).

## See also

- [Security model](security.md): the layers and the gaps in this release.
- [Isolation compared](isolation-compared.md): containers, microVMs and VPSes against this boundary.
- [Tenants, plans and keys](tenants-and-plans.md): quotas, scopes and overrides.
- [Failure behavior](failure-behavior.md): what happens when a tenant exhausts a shared resource.
