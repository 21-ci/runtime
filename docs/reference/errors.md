---
title: Error reasons
description: Every control API error code and every x-rt-reason of the invoke path, with what to do about it.
---


# Error reasons

The runtime reports errors in two shapes, depending on which listener answered.

=== "Control API (JSON)"

    Every `/v1/*` error is a JSON envelope. `details` is `{}` unless noted (validation diagnostics,
    `limit_exceeds_plan` field/requested/max).

    ```json
    {"error": {"code": "slug_taken", "message": "slug already in use", "details": {}}}
    ```

=== "Invoke path (plain text)"

    When the runtime itself refuses or fails a request to `/s/{slug}/…`, the body is `text/plain` and the
    reason is repeated in the `x-rt-reason` header. Responses your handler returns never carry
    `x-rt-reason`, so a client can always tell a platform error from an application error.

    ```http
    HTTP/1.1 503 Service Unavailable
    content-type: text/plain
    x-rt-reason: queue_timeout
    retry-after: 1

    queue_timeout
    ```

!!! tip "Capacity is not a 429"
    A busy service does not answer `429`. Requests over the current concurrency wait in a per-service queue
    and run when a slot frees up. Only operator-configured plan quotas (`rps`, `invocations_per_day`) produce
    `429`. When the queue wait runs out you get `503 queue_timeout` with `Retry-After`. See
    [Scheduler and autoscaling](../concepts/scheduler.md).

## Control API codes

Returned by the control listener (`/v1/*`, default port 8081) in `error.code`.

| Code | HTTP | Meaning | What to do |
|---|---|---|---|
| `unauthorized` | 401 | No credential, or the token/key is wrong, expired or revoked. | Send `Authorization: Bearer <token>`. Repeated failures from one IP are rate-limited. |
| `insufficient_scope` | 403 | The key is valid but lacks the scope for this route, the route is operator-only, a key with a project or environment restriction tried to change an environment or a project, or a new token would hold more than its creator (`details.reason: "escalation"`). | Use a key with the needed scope (see the scope table in the API reference); a new token can hold at most the scopes, projects, environments and lifetime of the key that creates it. |
| `quota_exceeded` | 403 | A quota would be exceeded: a plan quota (services, schedules per service, pending host claims, ...), 100 projects or 200 self-service keys per tenant. | Delete something, or move the tenant to a bigger plan. |
| `trigger_not_allowed` | 403 | The trigger is switched off on the service (`triggers.*`) or the tenant's plan does not allow it. | Enable the trigger on the service, or ask the operator for a plan that allows it. |
| `binding_not_allowed` | 403 | The tenant's plan may not use this operator binding. | Ask the operator to add the plan to the binding's `allowed_plans`. |
| `host_not_allowed` | 403 | The custom host does not match the plan's `custom_host_suffixes`. | Use an allowed suffix, or ask the operator to attach the host. |
| `reserved` | 403 | The system tenant is reserved: operator SDKs live there and are managed only through `/v1/sdks`. | Use the SDK routes (`21ci sdk`) instead of the tenant routes. |
| `not_found` | 404 | The resource does not exist, belongs to another tenant, or is a service whose project or environment the key does not admit (a restricted key cannot tell these apart). | Check the id or slug, the tenant of your key and its `project_ids` and `environments` (`21ci whoami`). |
| `slug_taken` | 409 | Another service on the node already uses this slug (slugs are node-wide). | Pick another slug. |
| `host_taken` | 409 | Another service already holds this host. | Use another host, or remove it from the other service first. |
| `host_cooldown` | 409 | Your earlier pending claim for this host expired; the same tenant must wait one more TTL. | Wait for the cooldown, or have the operator attach the host. |
| `version_not_ready` | 409 | The version failed to build and cannot go live. | Read its build logs, fix the source, upload again or `rebuild`. |
| `tenant_not_empty` | 409 | The tenant still owns services. | Delete them first or pass `?purge=1`. |
| `project_slug_taken` | 409 | The tenant already has a project with this slug. | Pick another slug, or use the existing project. |
| `project_not_empty` | 409 | The project still holds services. | Delete the services first. |
| `environment_in_use` | 409 | A service is still in an environment the new list drops. | Move the service to another environment or delete it first. |
| `sdk_release_exists` | 409 | That SDK name and version was published before; releases are immutable. | Publish under a new version. |
| `precondition_failed` | 412 | `If-Match` did not match the current `ETag`: somebody changed the service meanwhile. | Re-read the service and retry with the new `ETag`. |
| `too_large` | 413 | The request body is over the limit. | Shrink the body; upload limits are in the limits reference. |
| `storage_quota` | 413 | The write would push the tenant over the plan's `storage_mb`. | Delete data or blobs, or ask for a bigger plan. |
| `egress_quota` | 413 | The tenant used up the plan's `egress_mb_per_day`. | Wait for the day window to reset, or ask for a bigger plan. |
| `sql_error` | 422 | A SQL statement of the service failed (syntax, constraint, ...). | Read the message in the response and fix the statement. |
| `sql_not_authorized` | 422 | The statement uses something the SQL authorizer denies (for example `ATTACH`, extension loading or a restricted `PRAGMA`). | Rewrite the statement without it. |
| `sql_result_too_large` | 422 | The result is over the row or byte cap. | Page the query with `LIMIT` / `OFFSET`. |
| `kv_not_integer` | 422 | `kv.incr` on a value that is not a JSON integer. The value is unchanged. | Store an integer under that key, or use another key. |
| `kv_overflow` | 422 | `kv.incr` would leave the `i64` range. The value is unchanged. | Reset the counter or use a smaller delta. |
| `sql_busy` | 503 | The database is locked by a long transaction and the wait ran out. | Retry; keep transactions short. |
| `sql_timeout` | 503 | A synchronous SQL statement ran longer than its 100 ms budget. | Use the async `std:sql` API for heavy queries. |
| `validation_failed` | 422 | Bad JSON, unknown field, invalid value, or source that does not compile; `details.diagnostics` lists file/line/col for code. | Fix the request; for uploads read `details.diagnostics`. |
| `invalid_artifact` | 422 | The upload is not a valid artifact (for example a core WASM module instead of a component). | Build a WASI P2 component that exports `wasi:http/incoming-handler`. |
| `missing_export` | 422 | A durable WASM service must export `std:host/socket-handler` (world `socket-service`). | Add the export to the component, or make the service non-durable. |
| `limit_exceeds_plan` | 422 | A service limit is above the plan maximum. Values are never clamped silently. | Lower the value; `details` names field, requested and max. |
| `schedule_too_frequent` | 422 | The interval is shorter than the plan's `min_schedule_seconds`. | Use a longer interval. |
| `idempotency_mismatch` | 422 | The `Idempotency-Key` was used before with a different request. | Use a fresh key for a different request. |
| `no_rpc_surface` | 422 | The service has no `rpc` surface to describe: it has no live version, or it is a WASM component (those declare RPC in WIT). | Deploy a Deno service that exports `rpc`. |
| `sdk_contract_missing` | 422 | The SDK upload has no contract. | Export `defineSdk({ methods })`, or send `contract` and `members` with the release. |
| `sdk_unchecked_caller` | 422 | SDK methods must authorize by `ctx.caller`, and the methods named in `details.methods` never read it. | Check `ctx.caller` in those methods, or give `defineSdk` an `authorize(caller)` hook. |
| `rate_limited` | 429 | Too many requests (failed auth per IP, SSE streams per key, ...). Carries `Retry-After`. | Wait for `Retry-After`. |
| `build_quota` | 429 | The plan's `builds_per_hour` is used up. Carries `Retry-After`. | Wait for `Retry-After` or ask for a bigger plan. |
| `queue_full` | 429 | The service queue is at the plan's `max_queue_depth`. Carries `Retry-After`. | Let consumers catch up, or check the DLQ. |
| `not_implemented` | 501 | The route exists in the API but is not implemented in this release. | None. |
| `control_disabled` | 503 | A release build started without `RT_ROOT_TOKEN`: the control API is off. | Set `RT_ROOT_TOKEN` (or `RT_ROOT_TOKEN_FILE`) and restart. |

## Invoke reasons (`x-rt-reason`)

Returned by the public (port 8080) and internal listeners when a request to a service does not reach your handler, or the task fails in a way the runtime detects.

| Code | HTTP | Meaning | What to do |
|---|---|---|---|
| `not_found` | 404 | Unknown slug or host, no live version, or `triggers.http` is off. | Check the slug and that a version is live. |
| `unauthorized` | 401 | The service has `access: key` and the request has no valid key with `invoke`. | Send `Authorization: Bearer rtk_…` with the `invoke` scope. |
| `forbidden` | 403 | An ingress rule (path prefix or IP allow list) refused the request. | Check the service's `ingress` block. |
| `trigger_not_allowed` | 403 | The tenant's plan does not allow the `http` trigger. | Ask the operator for a plan that allows `http`. |
| `method_not_allowed` | 405 | The method is not in `ingress.allowed_methods`. | Use an allowed method or change the rule. |
| `too_large` | 413 | The request body is over `limits.max_body_bytes` (default 10 MB). | Send less, or raise the service limit within the plan. |
| `rate_limited` | 429 | The plan's `rps` token bucket or a service ingress `rate_limit` is exhausted. Carries `Retry-After`. | Back off for `Retry-After`. |
| `quota_exceeded` | 429 | A plan quota such as `invocations_per_day` is used up. Carries `Retry-After`. | Wait for the quota window or upgrade the plan. |
| `tenant_concurrency` | 429 | Legacy: no longer produced. Busy capacity now waits in a queue. | None. |
| `service_concurrency` | 429 | Legacy: no longer produced. Busy capacity now waits in a queue. | None. |
| `guest_error` | 500 | The handler threw, rejected, trapped, or returned something that is not a `Response`. The body never contains the message. | Read the service logs (`21ci logs <svc>`); the stack is there. |
| `cpu_limit` | 502 | The task used more CPU time than `limits.cpu_ms`. | Optimize, move work to a queue or schedule (higher limits), or raise the limit within the plan. |
| `oom` | 502 | The task went over `limits.memory_mb`. | Stream instead of buffering, or raise the memory limit within the plan. |
| `build_failed` | 502 | The live version failed to compile on first use. The node rolls back to the previous healthy version. | Read the build logs, fix and upload again. |
| `draining` | 503 | The node is draining (shutdown or upgrade). | Retry against the restarted node. |
| `shed` | 503 | The node is shedding load (degradation mode). Carries `Retry-After`. | Retry after `Retry-After`. |
| `overloaded` | 503 | The service or node queue bound (`scaling.max_queued`, `sched.max_queued`) is full. Carries `Retry-After`. | Retry after `Retry-After`; raise the bounds or capacity. |
| `queue_timeout` | 503 | The request waited for a run slot longer than `sched.queue_timeout`. Carries `Retry-After`. | Retry after `Retry-After`; see the scheduler and performance pages. |
| `memory` | 503 | A WebSocket upgrade was refused because the node is short of memory. Carries `Retry-After`. | Retry after `Retry-After`; give the node more memory. |
| `restarting` | 503 | The executor is restarting. | Retry shortly. |
| `route_stale` | 503 | A control change (deploy, config) is being applied to the routing table. Carries `Retry-After`. | Retry after 1 s (`Retry-After: 1`). |
| `timeout` | 504 | The task hit its wall-clock limit (`limits.wall_ms`). | Return earlier, stream, or move slow work to a queue. |
| `call_depth` | 508 | Service-to-service calls nested deeper than 15. | Break the call cycle. |

## Codes inside guest code

SDK calls made from a handler fail with stable codes, never with paths or driver messages. In Deno
they are thrown as an `Error` with a `.code`; in WASM the `std:host` functions return `error { code, message }`.

| Area | Codes |
|---|---|
| Outbound `fetch` (thrown as `TypeError`, the message is the code) | `egress_denied`, `egress_limit`, `egress_too_large`, `egress_quota`, `egress_timeout`, `egress_dns`, `egress_upstream`, `egress_insecure_redirect` |
| `std:kv` | `validation_failed`, `kv_key_too_large`, `kv_value_too_large`, `storage_quota`; `kv.incr` also `kv_not_integer`, `kv_overflow` |
| `std:sql` | `sql_error`, `sql_timeout`, `sql_not_authorized`, `sql_result_too_large`, `storage_quota` |
| `std:blob` | `blob_key_too_large`, `storage_quota`, `validation_failed`; WASM `get` also `blob_too_large` (object over the buffered limit) |
| `std:queue` | `not_found`, `forbidden`, `queue_full`, `call_not_allowed`, `trigger_not_allowed`, `cross_tenant_call` |
| `std:services` | `not_found`, `cross_tenant_call`, `forbidden`, `call_not_allowed`, `trigger_not_allowed`, `call_failed` |
| `std:platform` (returned as a `Response`, never thrown) | `403 binding_denied`, `403 binding_not_allowed`, `404 binding_not_found`, `429 rate_limited`, `502 binding_upstream` |
| Environment | `env_unavailable` (reading `Deno.env` when the env could not be fetched) |
| Request body | `request_too_large` (reading past `limits.max_body_bytes`) |

The [guest API reference](guest-api.md) describes each call.
