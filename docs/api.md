# API reference (user guide)

Generated source of truth: `GET /v1/openapi.json` (also committed as [`openapi/openapi.json`](https://github.com/21-ci/runtime/blob/main/openapi/openapi.json), 62 operations) and the Scalar UI at `http://127.0.0.1:8081/docs`. This page lists what the code in `crates/rt-api` does.

## Surfaces

| Listener | Default bind | Routes | Auth |
|---|---|---|---|
| control | `127.0.0.1:8081` | `/v1/*`, `POST /mcp` ([MCP server](mcp.md)), `GET /v1/openapi.json`, `GET /docs` (Scalar) | Bearer root token or `rtk_` key (`/mcp`: a tenant key only). `openapi.json` and `/docs` need none |
| public | `0.0.0.0:8080` | `ANY /s/{slug}/{path…}`, custom `Host`, `{slug}.{routing.base_domain}` | per service `access` |
| internal | `unix:/data/rt.sock` | same routes as public, trusted (no access check, no ingress rules, `x-rt-invoker` honored) | none |
| metrics | `127.0.0.1:9090` | `GET /metrics` (Prometheus), `GET /healthz`, `GET /readyz` | none |

There is no `/v1/health`: use `/healthz` (sysmon heartbeat < 1 s, `503` when stale) and `/readyz` (migrations applied, snapshot loaded, not draining) on the metrics listener.

## Authentication

| Credential | Header | Notes |
|---|---|---|
| Root token (operator) | `Authorization: Bearer <RT_ROOT_TOKEN>` only (never `x-rt-key`) | All routes. Acts on a tenant with `x-rt-tenant: ten_…` (required for service creation); the system tenant `ten_00000000000000000000000SYS` is read-only that way (`403 reserved` for any write) |
| Tenant key | `Authorization: Bearer rtk_<key_id>_<secret>` or `x-rt-key` | Own tenant only, limited by scopes and optional `project_ids` and `environments`. A resource of another tenant is `404`, and so is a service whose project or environment the key does not admit |

- Release builds without `RT_ROOT_TOKEN` answer every control route `503 control_disabled`. Debug builds leave the API open.
- Failed authentications are rate-limited per IP (`429 rate_limited` + `Retry-After`). Revoking a key takes effect at once.
- Operator-only routes (`/v1/node/*`, `/v1/tenants*`, `/v1/plans`) answer tenant keys `403 insufficient_scope`, except `GET /v1/tenants/{own id}/usage` (scope `logs:read`). `GET /v1/version` and `GET /v1/me` take any valid credential.

| Scope | Allows |
|---|---|
| `services:read` | every `GET` on services, versions (not build logs), env, secrets (names), hosts, schedules, queue stats, DLQ, events |
| `services:write` | create / patch / delete service, hosts, schedules |
| `deploy` | upload version, rebuild, `PUT live` |
| `invoke` | call services with `access: "key"` |
| `logs:read` | logs (JSON + SSE), build logs, stats, the tenant's own usage |
| `env:write` / `secrets:write` | `PUT env` / `PUT`, `DELETE` secrets |
| `queue:write` | enqueue, DLQ redrive and purge |
| `data:admin` | `GET data/export`, `DELETE objects/{name}` (a durable object's storage) |
| `sockets:write` | close open WebSockets (`DELETE …/sockets`) |
| `keys:write` | `POST` / `GET /v1/keys`, `DELETE /v1/keys/{key}`: keys of the caller's own tenant, never more than the caller holds ([self-service keys](https://github.com/21-ci/runtime/blob/main/SECURITY.md#self-service-keys-keyswrite)) |

## Conventions

| Topic | Rule |
|---|---|
| IDs | prefixed ULIDs: `ten_`, `svc_`, `prj_`, `key_`, `sch_`, `msg_`, `task_`, `evt_`. Versions are integers (`1`, `2`, …) |
| Times | response timestamps are Unix **ms** integers (`created_at`, `ts`, `next_due`, `expires_at`). Request `expires_at`, `since`, `until` are RFC 3339 |
| Bodies | JSON ≤ 1 MB. Version uploads are raw bytes (≤ 64 MB at the HTTP layer; the node buffers at most 4 uploads at once, one more, or one the memory governor refuses, is `429 rate_limited`; formats and limits under [Deploy input](#deploy-input)) |
| Errors | `{"error":{"code","message","details"}}`, `details` is `{}` unless noted (validation diagnostics, `limit_exceeds_plan` field/requested/max). Unknown routes: `404 not_found` envelope. Bad JSON/query: `422 validation_failed` |
| Pagination | `?limit=50&cursor=…` (max 200) on tenants, projects, services, events → `{items, next_cursor}`. Logs: `limit` default 100, max 1000. Keys, versions, hosts, schedules, secrets, usage are bare arrays |
| Idempotency | `Idempotency-Key` (≤ 255 bytes, else `422`) on `POST` tenants, projects, services, version upload, schedules, queue: same key + same request replays the stored response for 24 h (per principal, in memory, at most 100,000 entries per node: the oldest go first); different request → `422 idempotency_mismatch` |
| Concurrency | `ETag` header + `etag` field on service responses (`"7"`). `If-Match` on `PATCH /services/{s}` and `PUT …/live` → `412 precondition_failed`. `If-None-Match` is not supported |
| Booleans in query | `1`, `true`, `yes`, `on` (case-insensitive) |
| Slugs | `^[a-z][a-z0-9-]{1,62}$` |

## Routes

Status = success code. `Op` = operator token only.

### Node (Op)

| Method | Path | Body / query | Response |
|---|---|---|---|
| GET | `/v1/node/status` | | `{version, mode: normal\|shed\|drain, default_profile, v8_sandbox, processors, kinds, memory: {budget_bytes, committed_bytes, pressure}, inflight, sockets: {open, objects, pending, max}}` (`pending`: accepted sockets whose client is not connected yet; `max`: `ws.max_sockets`) |
| GET | `/v1/node/config` | | the configuration the node runs with (`rt.toml` sections, `plans`, …). Node secrets never appear (they come from env/files); `[[binding]].inject_headers` values other than `env:NAME` and URL user info are shown as `<redacted>` |
| PUT | `/v1/node/config` | `{degradation_mode?, default_profile?, rate_limit?}` | `{applied: [...], pending_restart: [...]}` (`rate_limit` is always `pending_restart`; `degradation_mode: "drain"` starts draining) |
| POST | `/v1/node/drain` | | `202` |
| GET | `/v1/node/scheduler` | | scheduler snapshot (P/M/queue lengths) plus `admission: {queued, queued_bytes, max_inflight, max_queued, max_queued_bytes}` and `services: [{service_id, tenant_id, slug, …admission view}]` (below) |
| GET | `/v1/node/cache` | | cache tier sizes, hit rates |

### Who am I, versions, plans

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/v1/version` | any valid credential | `{version, api}`: node version, API document version |
| GET | `/v1/me` | any valid credential | `{principal: operator\|tenant, tenant_id, key_id, scopes, project_ids, environments, plan: {name, settings}\|null, version}` (`null` = all); `plan` is the tenant's effective plan (overrides applied), `null` for the operator without `x-rt-tenant` |
| GET | `/v1/plans` | Op | `[{name, settings}]`: the configured `[plans.*]` |
| GET | `/v1/limits` | any tenant key (Op: with `x-rt-tenant`) | `{plan, day, resets_at, invocations, egress_bytes, storage_bytes, services, schedules}`: per quota `{limit, used, remaining, period}` with `period` `day` (since UTC midnight; reset at `resets_at`, unix ms), `total` or `per_service` (`schedules`: `plan.max_schedules` applies per service and `used` is the busiest one). `limit` and `remaining` are `null` when unlimited (`invocations_per_day = 0`) |

### Tenants and keys (Op)

| Method | Path | Body / query | Status, response |
|---|---|---|---|
| POST | `/v1/tenants` | `{name, plan, overrides?}` (`plan` must be a configured plan; built-in: `free`) | `201 {id, name, plan, overrides, created_at, disabled}` |
| GET | `/v1/tenants` | `limit, cursor` | `{items, next_cursor}` |
| GET / PATCH | `/v1/tenants/{t}` | PATCH `{name?, plan?, overrides?, disabled?}` | tenant |
| DELETE | `/v1/tenants/{t}` | `?purge=1` | `204`; `409 tenant_not_empty` while it owns services and `purge` is not set |
| GET | `/v1/tenants/{t}/usage` | `?from=YYYY-MM-DD&to=` | `[{service_id, day, invocations, cpu_ms, egress_bytes, storage_bytes}]`. Also a tenant key for its **own** tenant with `logs:read` (rows of the services the key may touch, by project and environment); another tenant's id is `404` |
| POST | `/v1/tenants/{t}/keys` | `{scopes: [...] (non-empty), project_ids?, environments?, expires_at?, name?}` | `201 {id, name, scopes, project_ids, environments, expires_at, created_at, last_used_at, secret}`; `secret` = full `rtk_<key_id>_<secret>`, shown **once** |
| GET | `/v1/tenants/{t}/keys` | | array of keys without `secret` |
| DELETE | `/v1/tenants/{t}/keys/{key}` | | `204`; already revoked → `404` |

### Tokens: your own keys (`keys:write`)

A tenant key with `keys:write` (or the operator with `x-rt-tenant`) manages the keys of its own tenant. A new key never holds more than its creator: a subset of its scopes, of its `project_ids` and of its `environments` (asking for `null` gives the creator's own list, so a restricted creator never mints an unrestricted key) and not past its `expires_at` (asking for none gives the creator's own); anything more is `403 insufficient_scope` with `details.reason: "escalation"`. A key lists and revokes only keys whose project and environment lists are within its own.

| Method | Path | Body | Status, response |
|---|---|---|---|
| POST | `/v1/keys` | `{scopes: [...] (non-empty), project_ids?, environments?, expires_at?, name?}` | `201 {id, name, scopes, project_ids, environments, expires_at, created_at, last_used_at, secret}`, the full `rtk_…` secret **once**; `422` on a bad scope, past expiry, a project that is not a `prj_…` id of the tenant, or a bad environment label; `403 quota_exceeded` at 200 live keys |
| GET | `/v1/keys` | | array of live keys without `secret` |
| DELETE | `/v1/keys/{key}` | | `204`, effective at once; revoked, unknown or out-of-reach → `404` |

### Projects

A project groups a tenant's services and lists their environments. Every tenant has a `default` project; a service is in one project and one of its environments. `{p}` is a slug or a `prj_…` id. Reading needs `services:read`, changing `services:write` **and** a key with no project or environment restriction (create, update and delete alike). Deleting a project removes it from every key's `project_ids`. The operator needs `x-rt-tenant`.

| Method | Path | Body / query | Status, response |
|---|---|---|---|
| POST | `/v1/projects` | `{slug (^[a-z][a-z0-9-]{1,62}$), name?, environments? (1 to 20 labels, default ["production"])}`; `Idempotency-Key` | `201 {id, tenant_id, slug, name, environments, created_at, updated_at}`; `409 project_slug_taken`; `403 quota_exceeded` at 100 projects; `422` |
| GET | `/v1/projects` | `limit, cursor` | `{items, next_cursor}` ordered by slug |
| GET / PATCH | `/v1/projects/{p}` | PATCH `{name?, environments?}` (the complete new list) | project; `409 environment_in_use` when the new list drops an environment a service is in |
| DELETE | `/v1/projects/{p}` | | `204`; `409 project_not_empty` while a service is in it |
| GET | `/v1/projects/{p}/environments` | | `{project, environments}` |

### Services

| Method | Path | Scope | Body / query | Status, response |
|---|---|---|---|---|
| POST | `/v1/services` | `services:write` | `{slug, runtime: deno\|wasm, name?, profile?, project?, environment?, limits?, scaling?, access?, triggers?, ingress?, egress?, call?, deno?, wasm?}` | `201` service; `409 slug_taken`; `422 validation_failed` / `limit_exceeds_plan`; `403 quota_exceeded` |
| GET | `/v1/services` | `services:read` | `limit, cursor, slug` (exact filter: 0 or 1 item), `project` (slug or `prj_…`), `environment` | `{items, next_cursor}` |
| GET | `/v1/services/{s}` | `services:read` | | service + `ETag` |
| PATCH | `/v1/services/{s}` | `services:write` | any of `name, profile, environment, limits, scaling, access, triggers, ingress, egress, call, deno, wasm`; `If-Match` | service. Each provided block **replaces** that block |
| DELETE | `/v1/services/{s}` | `services:write` | `?keep_data=1` | `204` |
| PUT | `/v1/services/{s}/live` | `deploy` | `{version, close_sockets?}`; `If-Match` | service; `409 version_not_ready` for a `failed` version (`pending` is accepted); rollback = point it at an older version. Open WebSockets stay and their next events run the new version; `close_sockets: true` closes them with 1012 |
| GET | `/v1/services/{s}/data/export` | `data:admin` | | SQLite snapshot (`application/vnd.sqlite3`) |

### WebSockets and objects

| Method | Path | Scope | Body / query | Status, response |
|---|---|---|---|---|
| GET | `/v1/services/{s}/objects` | `services:read` | `prefix, limit` (1-1000, default 100), `cursor` | `{items: [{name, sockets, storage}], next_cursor}`: objects holding sockets, and for a durable service the objects with storage (keys or an alarm) |
| GET | `/v1/services/{s}/objects/{name}/sockets` | `services:read` | `tag, limit, cursor` | `{items: [{id, object, tags, protocol, created_at, last_message_at, auto_response_at, buffered_amount, remote_ip}], next_cursor}` (attachments are never shown) |
| DELETE | `/v1/services/{s}/objects/{name}/sockets/{ws}` | `sockets:write` | `code` (1000 default, or 3000-4999), `reason` (≤ 123 bytes) | `204`; the guest gets `webSocketClose` |
| DELETE | `/v1/services/{s}/objects/{name}/sockets` | `sockets:write` | `code, reason, tag` | `{closed}`: every socket of the object (or with the tag) |
| DELETE | `/v1/services/{s}/objects/{name}` | `data:admin` | | `204`: drops a durable object's storage and alarm (its sockets stay; close them first); `404` when it had none or the service is not durable |

The default object of a service is `default`. The service's stats (`GET …/stats`) add `sockets: {open, objects}`.

Service object: `{id, tenant_id, slug, runtime, name, profile, project_id, project, environment, live_version, etag, limits, scaling, access, triggers, ingress, egress, call, events?, async?, deno?, wasm?, status: {build_state, tier: T1\|T2\|T3, inflight, last_error, admission}, effective}`; `effective` lists each resolved value with its source (`{"cpu_ms": {"value": 1000, "source": "profile:balanced"}}`); `deno` / `wasm` appear once a knob is set.

`status.admission` is the service's queueing and autoscaler state, `null` while it has none (never run, or idle past its idle TTL): `{kind: deno|wasm|build, limit, min, max, running, queued, queued_bytes, max_queued, queue_wait_ms, queue_timeouts, demand, latency_baseline_ms, warm_per_proc, instances, min_instances, max_instances, target_instances, rps, last_change, idle_ms, pred_ms, pred_background_ms}`. `limit` is the adaptive concurrency limit within `[min, max]` (the ceiling and the plan's tenant cap); `queue_wait_ms` is the smoothed wait of requests that had to queue (it fades to 0 while requests start without waiting); `queue_timeouts` counts `503 queue_timeout` since the state was created; `demand` is the decaying peak of running tasks and `warm_per_proc` the warm instances the autoscaler asks each processor to keep. An *instance* is one concurrently running copy of the service: `instances` is how many run now (the same as `running`), `min_instances` / `max_instances` the bounds of the limit (`max_instances` equals `max`; `min_instances` is `scaling.min_instances` within the plan), `target_instances` the warm instances wanted in total (`warm_per_proc` times the processors of the kind, at least `min_instances` while the service is known), `rps` the smoothed arrival rate and `last_change` the latest move of `limit` as `{reason: queue|trend|latency|memory|cpu|idle|bounds, from, to, ago_ms}` (`null` if it never moved). `pred_ms` is the predicted run time (EWMA) of the service's foreground tasks and `pred_background_ms` the same for queue, stream, async and schedule work; each is `null` until a task has run. `status.inflight` equals `admission.running`.

| Block | Fields (all optional; unknown fields → `422`) | Default |
|---|---|---|
| `access` | `public` \| `key` \| `internal` | `public` |
| `limits` | `wall_ms, cpu_ms, memory_mb, max_body_bytes` (above plan maximum → `422 limit_exceeds_plan`) | profile / plan |
| `scaling` | `min_instances` (kept warm, never scaled to zero; 0 = scale to zero), `max_instances` (hard cap of the adaptive limit, excess requests queue; ≥ 1, 0 = unset), `max_queued` (queue bound, over it `503 overloaded`), `keep_resident`. Both instance bounds above the plan's `max_instances` are clamped to it (`effective` shows source `plan`); `min_instances` above `max_instances` → `422 validation_failed`. `max_concurrency` is the old name of `max_instances` (accepted on input, never returned) | `min_instances` 0; `max_instances` the plan's limit, else the profile ceiling; adaptive in between |
| `triggers` | `http, interval, cron, queue, call, websocket` (booleans) | all `true` |
| `durable` | `true`: a durable service (its default export is a class, one instance per object; [handlers](handlers.md#stddurable)). A WASM durable service must export `std:host/socket-handler` (`422 missing_export` on upload, on PATCHing `durable: true` when the live version lacks it, and on moving `live` to such a version) | `false` |
| `scaling.min_run_time_ms` | Keep-alive in ms (integer, ≥ 0): how long a reused instance may stay idle after its answer, waiting for the next task of the same version. `0` = released right after the answer, unless tasks of the service wait in admission (then at most the service's `queue_timeout`). A maximum the runtime may cut short (memory pressure, version change, `reuse_max_*`), not a guarantee. Only matters while reuse is on. Above the lower of the plan's `limits_max.min_run_time_ms` and the node's `sched.min_run_time_max_ms` → `422 limit_exceeds_plan` (`details` name `field`, `requested`, `max`). The effective value and its source are in `effective.min_run_time_ms` | inherit: `deno.reuse_idle_ttl` (60 s) on Deno, `cache.idle_ttl` on WASM |
| `events` | `subscribe[]` (`{subject, group?}`, at most 32; subject tokens `[A-Za-z0-9_-]+`, `*`, or `>` as the last token; `group` names a load-balancing group), `consume[]` (`{topic, group?, start?, batch_size?, max_attempts?, on_exhausted?}`, at most 8, `(topic, group)` unique; `start` `earliest`\|`latest` where a new cursor starts, `batch_size` 1–1000, `max_attempts` 1–100, `on_exhausted` `dlq`\|`skip`\|`block`). The plan must allow `pubsub` or `stream` (`403 trigger_not_allowed`); a durable service refuses both (`422 validation_failed`) | no blocks (no subscriptions, no consumers) |
| `async` | `enabled` (bool; opt-in), `max_attempts` (1–10 function attempts in total), `max_event_age_s` (60–86400 s) | `enabled: false`, `max_attempts` 3, `max_event_age_s` 3600 |
| `ws` | `allowed_origins[]` (exact origins such as `https://app.example.com`, case-insensitive; empty = any `Origin`; a missing or other `Origin` gets `403 origin_not_allowed` at the upgrade), `warm` (bool: keep the isolate or instance of an event for the next event of the service), `max_sockets` (the service's own cap on open sockets, within the plan's) | any origin, `warm: true`, no own cap |
| `ingress` | `allowed_methods[], allow_paths[] (prefix*), ip_allow[] (CIDRs), rate_limit {rps, burst}`. A request passes `allow_paths` only if both its raw path and that path with dot segments resolved (`.`, `..`, also as `%2e`, and `\` as `/`) match | none |
| `egress` | `allowed_hosts[], denied_hosts[]` | plan default |
| `profile` | `max_density` \| `balanced` \| `min_latency` | node default |
| `environment` | label `^[a-z][a-z0-9-]{0,31}$` (e.g. `staging`, `pr-42`); must be one of the service's project's environments (`422` listing them otherwise). `project` (slug or id, create only; default `default`) cannot change: a `PATCH` naming another project is `422` | `production` |
| `call` | `allowed_callers[]`: who may call (and `std:queue`-enqueue to) this service. Each entry is a service slug, a `svc_…` id (stored in canonical form) or `env:<environment>` (the environment of the target's own project, never another project's); empty = services of the same project in the same environment; a non-empty list names every admitted caller. Another tenant's service (its plan must allow cross-tenant calls) is admitted only by its id. A slug entry admits whichever service holds the slug at call time; prefer ids where that matters. Others get `call_not_allowed` (see [handlers](handlers.md#stdqueue-stdservices-stdplatform)). Changing `environment` needs the operator or a key with no project or environment restriction | `[]` |
| `deno` | `reuse_isolates` (bool): reuse an isolate with its evaluated module for the next request of the same version | node config, then profile (`min_latency`: on) |
| `wasm` | `reuse_instances` (bool): reuse a component instance for the next request of the same version | profile (`min_latency`: on) |

### Versions and builds

| Method | Path | Scope | Notes |
|---|---|---|---|
| POST | `/v1/services/{s}/versions` | `deploy` | Raw body, `Content-Type` selects the format: `application/typescript`, `text/javascript`, `application/json` (`{entry, files, imports?}`), `application/zip`, `application/x-tar`, `application/wasm`. `?activate=1` sets `live` to the new version (`&close_sockets=1` also closes the service's WebSockets with 1012; without it they stay); `?compile=lazy\|eager`. `201` (lazy, `build_state: pending`) or `202` (eager, `building`). Errors: `422 validation_failed` (`details.diagnostics: [{file,line,col,message}]`), `422 invalid_artifact`, `413 too_large`, `429 build_quota` |
| GET | `/v1/services/{s}/versions` | `services:read` | array |
| GET | `/v1/services/{s}/versions/{v}` | `services:read` | `{version, source_digest, lock_digest, build_state: pending\|building\|ready\|failed, failure, created_at}`; `failure` is `null` or `{reason: syntax\|resolve\|fetch\|lock_mismatch\|timeout\|oom\|too_large\|internal, message, diagnostics}` |
| GET | `/v1/services/{s}/versions/{v}/source` | `services:read` | uploaded bytes |
| POST | `/v1/services/{s}/versions/{v}/rebuild` | `deploy` | `202`; clears a failed build |
| GET | `/v1/services/{s}/versions/{v}/build/logs` | `logs:read` | SSE until the build is `ready` or `failed` |
| GET | `/v1/services/{s}/types` | `services:read` | `?version=` (default live): the `.d.ts` of a Deno service's `rpc` export (`text/plain`), `21ci types`; `422 no_rpc_surface` without one or for WASM |

#### Deploy input

| Runtime | `Content-Type` | Shape | Limits |
|---|---|---|---|
| deno | `application/typescript`, `text/javascript` | one file, entry `main.ts` / `main.js` | ≤ 1 MB |
| deno | `application/zip`, `application/x-tar` | files plus an optional `rt.json` `{entry, imports}` (import map) | ≤ 10 MB total, ≤ 1,000 files, ≤ 1 MB per file; archives with `..`, absolute paths or links, or an inflate ratio over 100, are refused |
| deno | `application/json` | `{entry, files: {path: content}, imports?}` | same |
| wasm | `application/wasm` | a WASI P2 component exporting `wasi:http/incoming-handler` | ≤ 50 MB |

The upload is validated synchronously, without network: Deno sources must parse (`422 validation_failed` with `details.diagnostics`), the entry must exist, and only relative files, `std:`, allowed `sdk:`, `npm:` / `jsr:` and allowlisted `https:` specifiers are accepted; a WASM file must be a component with the right export and only allowed imports (`422 invalid_artifact`). Plan `builds_per_hour` is checked here (`429 build_quota`). The version starts as `pending`. With `compile=lazy` (the `max_density` and `balanced` default) it goes live at once and the first request builds it (that request waits); with `compile=eager` (`min_latency`) the build runs now (`202`) and `live` flips only when it is `ready`, so a failed build leaves `live` unchanged.

### Operator SDKs

Runtime services of the system tenant; tenants get the contract and a per-call binding ([handlers](handlers.md)).

| Method | Path | Scope | Notes |
|---|---|---|---|
| POST | `/v1/sdks` | Op | `{name, version, source: base64, runtime?, content_type?, contract?, members?, description?, channel?}` → `201 {sdk, version, version_no, members, channels, created_at}`. The release is a new version of the SDK's service, built at once; the first one becomes `stable`. `409 sdk_release_exists`, `422 sdk_contract_missing` (no `defineSdk` methods and no `contract`/`members`), `422 sdk_unchecked_caller` (a method never reads `ctx.caller`), the upload errors of a version |
| GET | `/v1/sdks` | Op | `[{name, state: active\|removed, description, service_id, channels: {<ch>: {channel, version, previous, updated_at}}, releases, latest, created_at}]` |
| GET | `/v1/sdks/{name}/releases` | Op | `[{sdk, version, version_no, members, channels, created_at}]`, oldest first |
| PUT | `/v1/sdks/{name}/channels/{channel}` | Op | `{version}` → `{channel, version, previous, updated_at}`: promote or roll back; `404` for an unknown release |
| DELETE | `/v1/sdks/{name}` | Op | `204`: calls answer `sdk_unavailable`; publishing again restores it |
| GET / PUT | `/v1/sdk-grants` | Op | PUT `{scope: node\|plan:<name>\|tenant:ten_…\|project:prj_…\|environment:prj_…/<env>, sdk: <name>\|*, effect?: allow\|deny, version_req?, channel?}` → `200` grant, or `204` when every field is empty (deleted). An `allow` without `version_req` is pinned to the stable major |
| GET | `/v1/sdks/effective` | `services:read` | `?project&environment` (default `default` / `production`): `[{name, description, version, channel, version_reqs, members, reason}]`, the SDKs the key's grants allow and the release each runs |
| GET | `/v1/sdks/{name}/contract` | `services:read` | the `.d.ts` of the release the key's grants select (`text/plain`); the operator may pass `?version=`. `404` when not granted |

### Env, secrets, hosts

| Method | Path | Scope | Body → response |
|---|---|---|---|
| GET / PUT | `/v1/services/{s}/env` | `services:read` / `env:write` | bare map `{"A":"1"}`; PUT replaces the whole map (≤ 100 entries, values ≤ 32 KB) |
| GET | `/v1/services/{s}/secrets` | `services:read` | `[{name, updated_at}]` (values are write-only) |
| PUT / DELETE | `/v1/services/{s}/secrets/{NAME}` | `secrets:write` | PUT `{"value":"…"}` (≤ 32 KB; name `[A-Z_][A-Z0-9_]*`, no `RT_` prefix) → `204` |
| GET | `/v1/services/{s}/hosts` | `services:read` | `[{host, status: pending_verification\|active, verify_token, created_at, expires_at}]` (unix ms; `expires_at` is `null` once active) |
| GET | `/v1/services/{s}/hosts/{host}` | `services:read` | one host, same shape (the host is normalised: lowercase, no trailing dot); `404` when it is not mapped to this service |
| PUT / DELETE | `/v1/services/{s}/hosts/{host}` | `services:write` | operator: `active` at once; tenant: `pending_verification` and only if the plan's `custom_host_suffixes` allow it (`403 host_not_allowed` otherwise). Publish the token as TXT at `_rt-verify.<host>`: the node checks pending claims every `hosts.verify_interval` and activates a match. A re-PUT of your own pending claim keeps its token and expiry, and so does a DELETE + PUT. A pending claim expires after `hosts.pending_ttl` (7 days) and then no longer blocks others; the same tenant gets `409 host_cooldown` for that host for one more TTL. At most `hosts.max_pending_per_tenant` (10) pending claims per tenant (`403 quota_exceeded`). `409 host_taken` |

Env and secret changes apply to the next task; no redeploy.

### Schedules and queue

| Method | Path | Scope | Body → response |
|---|---|---|---|
| GET / POST | `/v1/services/{s}/schedules` | `services:read` / `services:write` | POST `{kind: "interval", every_seconds (1 s to 10 years), payload?, enabled?}` or `{kind: "cron", cron: "*/5 * * * *", timezone?: "UTC", payload?, enabled?}` → `201 {id, kind, every_seconds, cron, timezone, payload, enabled, next_due}`. Payload ≤ 64 KB. A cron follows the wall clock of `timezone`: a time that a DST change skips fires just after the gap (02:30 on a 02:00 → 03:00 day fires at 03:30), a time that happens twice fires once. Its plan minimum (`min_schedule_seconds`) applies to the shortest gap between fires |
| PATCH / DELETE | `/v1/services/{s}/schedules/{id}` | `services:write` | PATCH: same body as POST (`kind` required), the same trigger and plan checks → `200` with the schedule; DELETE → `204` |
| POST | `/v1/services/{s}/queue` | `queue:write` | `{body: <any JSON>, delay_seconds?: ≤ 7 d, dedup_id?}` → `202 {id}`; body ≤ 1 MB (`413 too_large`); `429 queue_full`; `403 trigger_not_allowed` when the service's `queue` trigger is off or the tenant's plan does not allow `queue` |
| GET | `/v1/services/{s}/queue` | `services:read` | `{depth, in_flight, oldest_age_ms, dlq}` |
| GET / DELETE | `/v1/services/{s}/dlq` | `services:read` / `queue:write` | GET `{items: [{id, body, attempts, failed_at}]}` (≤ 100); DELETE `204` |
| POST | `/v1/services/{s}/dlq/redrive` | `queue:write` | `{redriven: N}` |

A disabled trigger (`triggers.cron=false`, plan `allowed_triggers`) → `403 trigger_not_allowed`. Interval below the plan minimum → `422 schedule_too_frequent`; schedule count above the plan → `403 quota_exceeded`.

### Topics, consumers and invocations

Topics are tenant resources: partitioned, replayable logs (`std:stream`), and the names that `std:pubsub` publishes
under. `{t}` is a topic id (`tpc_…`) or a topic name (`[a-z0-9][a-z0-9._-]{0,62}`, which may not start with `tpc_`).
A name resolves in the `project` and `environment` query or body fields (defaults `default` and `production`). A
topic the key cannot see is `404 not_found`.

| Method | Path | Scope | Body / query → response |
|---|---|---|---|
| POST | `/v1/topics` | `services:write` | `{name, project?, environment?, partitions? (1), retention_hours? (24), max_mb? (0)}` → `201` topic. `Idempotency-Key` is honoured. `409 topic_exists`; `403 quota_exceeded` at `max_topics`; `422 limit_exceeds_plan` for partitions, retention or `max_mb` above the plan |
| GET | `/v1/topics` | `services:read` | `?project&environment` (both optional) → `{items: [topic]}` |
| GET | `/v1/topics/{t}` | `services:read` | `?project&environment` → topic, with its `consumers` |
| PATCH | `/v1/topics/{t}` | `services:write` | `{retention_hours?, max_mb?}` → topic |
| DELETE | `/v1/topics/{t}` | `services:write` | → `204`. Records, cursors and dead letters go with it |
| POST | `/v1/topics/{t}/messages` | `queue:write` | `{records: [{body, key?, headers?, dedup_id?}]}`, at most 100 records and 4 MiB. `Idempotency-Key` is honoured → `202 {items: [{partition, offset, duplicate}]}`. `429 stream_full` at the tenant's byte cap; each call takes one tenant rate token (`429 rate_limited`) |
| GET | `/v1/topics/{t}/messages` | `services:read` | `?partition=N&from=<offset>\|earliest\|latest&from_time=<RFC3339>&limit=100` → `{items: [record], next, end}`. A page holds at most 100 records and 1 MiB. `partition` is required on a topic with more than one partition (`422 validation_failed`) |
| POST | `/v1/pubsub/publish` | `queue:write` | `{subject, body, headers?, project?, environment?}` → `200 {receivers, dropped}`. `receivers` counts the services a delivery was queued for; `dropped` counts those skipped (fan-out cap of 100, per-service backlog, quota or admission refusal) |
| GET | `/v1/services/{s}/consumers` | `services:read` | → `{items: [consumer]}` |
| POST | `/v1/services/{s}/consumers/{topic}/{group}/reset` | `queue:write` | `{to: "earliest"\|"latest"\|<offset>\|"<RFC3339>", partition?}` → the consumer. Also unblocks a blocked consumer |
| GET | `/v1/services/{s}/consumers/{topic}/{group}/dlq` | `services:read` | `?limit=100` → `{items: [dead letter]}` |
| DELETE | `/v1/services/{s}/consumers/{topic}/{group}/dlq` | `queue:write` | → `204` |
| GET | `/v1/services/{s}/invocations` | `services:read` | `?state=pending\|succeeded\|failed&limit&cursor` → `{items: [invocation], next_cursor, counts: {pending, failed}}` |
| GET | `/v1/services/{s}/invocations/{id}` | `services:read` | → invocation |
| POST | `/v1/services/{s}/invocations/redrive` | `queue:write` | → `200 {redriven}`. Failed invocations go back to pending with their attempts reset |
| DELETE | `/v1/services/{s}/invocations` | `queue:write` | `?state=failed` (the only value accepted) → `204` |

A record is `{partition, offset, key, headers, body, timestamp}`. An invocation is `{id, state, method, path, has_query,
attempts, status, condition, error, body_bytes, created_at, updated_at, expires_at}`. `path` never contains the query
string; `has_query` says whether there was one. No invocation view carries request headers or the body: those may hold
end-user credentials. Failed invocations are kept 7 days, at most 1,000 per service (the oldest are evicted first).

### Logs, stats, events

| Method | Path | Scope | Notes |
|---|---|---|---|
| GET | `/v1/services/{s}/logs` | `logs:read` | `?since&until&level=debug\|info\|warn\|error&kind=runtime\|build\|system&task_id&limit&cursor` → `{items: [{seq, ts, level, service_id, version, task_id, trigger, kind, msg, truncated}], next_cursor}` |
| GET | `/v1/services/{s}/logs?follow=1` | `logs:read` | SSE: `id: <seq>`, `event: log`, `data: <log JSON>`; resume with `Last-Event-ID`. At most `ingress.sse_streams_per_key` streams per key (`429 rate_limited`) |
| GET | `/v1/services/{s}/stats` | `logs:read` | `?window=5m\|1h\|24h` (default `5m`): counts, error rate, p50/p95/p99, plus `admission` (the service's queueing / autoscaler state, as in the service object) |
| GET | `/v1/services/{s}/events` | `services:read` | `limit, cursor` → `{items: [{id, ts, kind, data}], next_cursor}` (`deploy`, `rollback`, `key.created`, …) |

## Control error codes

Each code is explained in [Error reasons](reference/errors.md#control-api-codes): what it means and what to do about it.

| `code` | HTTP |
|---|---|
| `unauthorized` | 401 |
| `insufficient_scope`, `quota_exceeded`, `trigger_not_allowed`, `binding_not_allowed`, `host_not_allowed`, `reserved` (the system tenant) | 403 |
| `not_found` | 404 |
| `slug_taken`, `host_taken`, `host_cooldown`, `topic_exists`, `version_not_ready`, `tenant_not_empty`, `project_slug_taken`, `project_not_empty` (also with topics), `environment_in_use` (also with topics), `sdk_release_exists` | 409 |
| `precondition_failed` | 412 |
| `too_large`, `storage_quota`, `egress_quota` | 413 |
| `sql_error`, `sql_not_authorized`, `sql_result_too_large`, `kv_not_integer`, `kv_overflow` (guest data, e.g. on export) | 422 |
| `sql_busy`, `sql_timeout` | 503 |
| `validation_failed`, `invalid_artifact`, `missing_export` (a durable WASM service without `socket-handler`), `limit_exceeds_plan`, `schedule_too_frequent`, `idempotency_mismatch`, `no_rpc_surface`, `sdk_contract_missing`, `sdk_unchecked_caller` | 422 |
| `rate_limited`, `build_quota`, `queue_full`, `stream_full` | 429 (`Retry-After` when known) |
| `not_implemented` | 501 |
| `control_disabled` | 503 |
| anything else (`internal`) | 500 |

## MCP

`POST /mcp` on the control listener is a [Model Context Protocol](https://modelcontextprotocol.io) server for AI assistants: 33 tools that call the routes above as the key that connects (`whoami`, `get_limits`, projects, SDKs, services, `deploy_code`, versions and `rollback`, schedules, objects and WebSockets, logs and stats, env and secrets, tokens, topics and consumers, invocations). It takes a tenant key as a Bearer token, answers JSON, and speaks the `initialize` handshake of 2024-11-05 to 2025-11-25 and the stateless 2026-07-28 revision. See [mcp.md](mcp.md) for the tools, the transport rules and how to connect Claude Code, Claude Desktop and Cursor.

## Invoke API

`ANY /s/{slug}` and `ANY /s/{slug}/{path…}` on `public` (or `internal`). The handler sees `/{path}?query`; the prefix is passed as `x-rt-base-path`. With a custom host or `{slug}.{base_domain}` the path is unchanged.

Order of checks: route → the tenant's plan allows `http` (else `403 trigger_not_allowed`) → `access` (`key`: root token or key with `invoke` for this service, else `401`; 10 wrong `rtk_` keys per minute from one client, then `429 rate_limited`) → ingress rules (`405 method_not_allowed`, `403 forbidden`, `429`) → quotas → admission → run.

| Added | Header |
|---|---|
| to the guest request | `x-rt-trigger: http`, `x-rt-base-path`, `x-rt-invoker` (internal only), `x-rt-client-ip` (the client address behind `ingress.trusted_proxies`; none on the Unix socket), `traceparent` (generated if absent). Client `x-rt-*` (including `x-rt-client-ip` and `x-rt-key`), hop-by-hop headers and an `Authorization` carrying an `rtk_…` key or the root token are stripped; any other `Authorization` reaches the guest unchanged |
| to the response | `x-rt-task-id`, `x-rt-version`, `server-timing: queue;dur=…, wait_runtime;dur=…, init;dur=…, exec;dur=…`. The guest's hop-by-hop headers (`connection`, `transfer-encoding`, `keep-alive`, `upgrade`, …) are dropped, and so is a `content-length` that does not match the body |

Failures are `text/plain` with the reason as body and in `x-rt-reason` (never JSON). Each reason is explained in
[Error reasons](reference/errors.md#invoke-reasons-x-rt-reason).

| `x-rt-reason` | HTTP |
|---|---|
| `not_found` (unknown slug, no live version, `triggers.http=false`) | 404 |
| `unauthorized` | 401 |
| `forbidden` (path / IP rule) | 403 |
| `trigger_not_allowed` (the tenant's plan does not allow `http`) | 403 |
| `method_not_allowed` | 405 |
| `too_large` (request body over `limits.max_body_bytes`, default 10 MB) | 413 |
| `rate_limited`, `quota_exceeded` (plan quotas), `tenant_concurrency`, `service_concurrency` (legacy, no longer produced: capacity waits in a queue instead) | 429 (+ `Retry-After`) |
| `guest_error` (uncaught exception; body never contains the message, see logs) | 500 |
| `cpu_limit`, `oom`, `build_failed` | 502 |
| `draining`, `shed`, `overloaded`, `queue_timeout`, `memory`, `restarting` (a request that waits for memory ends as `queue_timeout`; `memory` refuses a WebSocket upgrade while the node is short of memory) | 503 |
| `route_stale` (a control change is being applied; retry after 1 s) | 503 |
| `timeout` (wall limit) | 504 |
| `call_depth` | 508 |
| `async_not_allowed` (`X-Rt-Invocation-Type: Event` while the service's `async.enabled` is off, or on a durable service or a WebSocket upgrade) | 403 |
| `bad_request` (an unknown `X-Rt-Invocation-Type` value, a bad `Idempotency-Key`) | 400 |
| `idempotency_mismatch` (the same `Idempotency-Key` on a different method, URI or body) | 409 |
| `async_backlog_full` (the service has `max_async_pending` pending invocations; `Retry-After: 1`) | 429 |

**Async invocations** are opt-in per service (`async.enabled`, default `false`; set it with `21ci service update <svc> --set async.enabled=true`). With it on, `Prefer: respond-async` (RFC 7240; other preferences are ignored) or `X-Rt-Invocation-Type: Event` makes the invoke path store the request and answer `202 {"id":"inv_…","state":"pending"}` with `x-rt-invocation-id`, before the handler runs. The handler then gets the same method, path, query, headers and body, plus `x-rt-invocation-id` and `x-rt-invocation-attempt` (1-based). Its response body is discarded and only the status counts: a `5xx` or `429` is a failed attempt and is retried until `async.max_attempts` is used up, after which the invocation is `failed`. An invocation still pending `async.max_event_age_s` after it was accepted fails with `event_age_exceeded`. Any other status is a success.

- With `Prefer` and a known `Content-Length` over `min(1 MiB, limits.max_body_bytes)`, the request runs synchronously. With `X-Rt-Invocation-Type: Event` it is `413 too_large`. A body of unknown length is buffered up to the same cap and is `413 too_large` in both modes.
- An `Idempotency-Key` (1–255 visible ASCII characters) replays the original `202` while the record exists: pending, then 24 h after success or 7 d after failure. The same key with a different method, URI or body is `409 idempotency_mismatch`. The header is always forwarded to the handler unchanged.
- Headers and body are stored sealed with the node's secret key. Authorization is decided at accept: revoking a key later does not cancel accepted invocations (purge them with `21ci invocation purge`). Ingress checks run first; an accepted request costs one rate-limit token and one daily invocation.
- Each service may hold `plan.max_async_pending` pending invocations (`429 async_backlog_full` above it). The node runs at most 256 async attempts at once.

Also see [Topics, consumers and invocations](#topics-consumers-and-invocations) for the inspection routes.

**WebSocket upgrades** (`GET` with `Upgrade: websocket`, HTTP/1.1) take the same path, plus, before any task runs: `triggers.websocket` and the plan allow it (`403 trigger_not_allowed`), the node is not draining (`503 draining`) or short of memory (`503 memory`), and the `Origin` is one of `ws.allowed_origins` (`403 origin_not_allowed`). For `access: "key"` a browser, which cannot set `Authorization`, may pass the key as the first subprotocol (`new WebSocket(url, ["rtk_…", "chat"])`); it is removed before the guest sees the request. The guest answers with `sockets.accept(req)` (`101`); any other response goes out as it is. A `101` that the guest did not accept is `502 bad_upgrade`. Socket caps (`ws.max_sockets`, the plan's `max_sockets` and `max_sockets_per_object`, the service's `ws.max_sockets`) fail the guest's `accept` with `limit_exceeded`. See [handlers](handlers.md#websockets-and-durable-objects).

## Plans (built-in `free`; more under `[plans.<name>]`)

`max_services` 10, `max_concurrency` 10, `max_instances` 4 (per service, 0 = node default), `rps` 50, `invocations_per_day` 100,000, `storage_mb` 100, `egress_mb_per_day` 1,000, `max_schedules` 3, `min_schedule_seconds` 60, `max_queue_depth` 10,000, `builds_per_hour` 20, `allowed_triggers` `http interval queue call websocket pubsub stream` (no `cron`), `limits_max` wall 30 s / CPU 1 s / 128 MB / `min_run_time_ms` 60 s, `custom_host_suffixes` none, `allow_cross_tenant_calls` false, `max_sockets` 1,000 (open sockets of the tenant; 0 = only the node cap), `max_sockets_per_object` 1,000, `ws_max_message_bytes` 1 MiB, `max_async_pending` 10,000 (pending async invocations per service), `max_topics` 10, `max_topic_partitions` 4, `max_stream_mb` 100 (record bytes across the tenant's topics; not part of `storage_mb`), `max_stream_retention_hours` 72.

The built-in `system` plan (the node's own tenant) uses `limits_max.min_run_time_ms` 600,000, `max_async_pending` 100,000, `max_topics` 1,000, `max_topic_partitions` 16, `max_stream_mb` 10,240 and `max_stream_retention_hours` 720. `limits_max.min_run_time_ms` is the largest `scaling.min_run_time_ms` a service of the plan may set; the node's `sched.min_run_time_max_ms` caps it too (the lower value wins).

## Example

```bash
API=http://127.0.0.1:8081; KEY=rtk_…                       # tenant key with services:read, services:write, deploy
H=(-H "Authorization: Bearer $KEY")

SVC=$(curl -sS "${H[@]}" -H 'Content-Type: application/json' -d '{"slug":"hello","runtime":"deno"}' $API/v1/services | jq -r .id)
printf 'export default () => new Response("hi")\n' > hello.ts
curl -sS "${H[@]}" -H 'Content-Type: application/typescript' --data-binary @hello.ts "$API/v1/services/$SVC/versions?activate=1"
# 201 {"version":1,"source_digest":"sha256:…","lock_digest":null,"build_state":"pending","failure":null,"created_at":…}
curl http://127.0.0.1:8080/s/hello/                          # hi  (first request compiles)

curl -sS "${H[@]}" -X PUT -H 'Content-Type: application/json' -d '{"version":1}' $API/v1/services/$SVC/live   # roll back / pin
curl -sSN "${H[@]}" "$API/v1/services/$SVC/logs?follow=1"                                                     # SSE
```

[`sample/scripts/deploy.sh`](https://github.com/21-ci/runtime/blob/main/sample/scripts/deploy.sh) does the same (find/create service, upload, activate, optional `RT_COMPILE=eager` wait); [`invoke.sh`](https://github.com/21-ci/runtime/blob/main/sample/scripts/invoke.sh) wraps the invoke call.
