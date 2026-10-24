# Limits, quotas and error codes

Defaults of the `balanced` profile (source: `crates/rt-types/src/limits.rs`, `crates/rt-types/src/plan.rs`). The node
operator can change profiles and plans; `21ci whoami --json` and `21ci limits` show what applies to you, and
`21ci service get <svc> --json` shows each resolved value with its source under `effective`.

## Per task

| Limit | HTTP / call / socket event / alarm | interval, cron, one-off schedule | queue |
|---|---|---|---|
| Wall time | 30 s (call: min(own, caller remaining)) | 300 s | 300 s |
| CPU time | 1,000 ms | 10,000 ms | 10,000 ms |
| Memory (heap + ArrayBuffers + wasm memory) | 128 MB | 128 MB | 128 MB |
| Request body | 10 MB | n/a | 1 MB per message |
| Response body | 10 MB buffered, 100 MB streamed | n/a | n/a |
| Outbound fetches / concurrent | 50 / 6 | 200 / 6 | 200 / 6 |
| Log lines | 1,000 (message at most 10,000 chars) | same | same |
| Service call depth | 15 | | |

A service may lower or raise `limits.wall_ms`, `cpu_ms`, `memory_mb`, `max_body_bytes` up to the plan maximum
(`422 limit_exceeds_plan` above it).

## Built-in `free` plan

`max_services` 10, `max_concurrency` 10, `max_instances` 4 per service, `rps` 50, `invocations_per_day` 100,000,
`storage_mb` 100, `egress_mb_per_day` 1,000, `max_schedules` 3 per service, `min_schedule_seconds` 60,
`max_queue_depth` 10,000, `builds_per_hour` 20, `allowed_triggers` `http interval queue call websocket` (**no `cron`**),
`limits_max` wall 30 s / CPU 1 s / 128 MB, no custom host suffixes, no cross-tenant calls, `max_sockets` 1,000,
`max_sockets_per_object` 1,000, `ws_max_message_bytes` 1 MiB. Other plans are operator-defined.

## Uploads (Deno)

| Format | Limit |
|---|---|
| Single `.ts` / `.js` file | 1 MiB |
| JSON bundle, zip, tar | 10 MiB total, at most 1,000 files, 1 MiB per file, inflate ratio at most 100x |
| Request at the HTTP layer | 64 MB |

Syntax errors and forbidden imports are `422 validation_failed` at upload, with `details.diagnostics`
(`file`, `line`, `col`, `message`). Dependency resolution happens at build: lazy builds run on the first request.

## Storage

| Store | Limits |
|---|---|
| KV | key 1 byte to 2 KB, value 1 MB, `list` limit 1 to 1000, `getMany` 1000 keys / 10 MiB |
| SQL (`std:sql`) | 1,000 ms per statement, interactive transaction rolled back after 5 s, 10,000 rows per statement, 10 MiB per call |
| SQLite (`std:sqlite`) | 100 ms per statement, 20 ms lock wait, same result caps |
| Blob | key at most 1 KB, object at most 100 MB |
| Durable storage | key 2 KiB, value 1 MiB, 1,000 writes per task |
| Total | plan `storage_mb` across KV, SQL, blobs and durable storage (`storage_quota`) |
| Env + secrets | at most 100 entries per service, values at most 32 KB, names `[A-Z_][A-Z0-9_]*`, no `RT_` prefix |
| Queue message | 1 MB, delay at most 7 days, dedup window 24 h |
| One-off schedules | 1,000 pending per service, payload 16 KiB, at most 30 days ahead |
| Interval/cron schedule payload | 64 KB |
| RPC / SDK call argument and result | 256 KiB each way |

## Invoke outcomes

Invoke failures are `text/plain` with the reason as body and in the `x-rt-reason` header (never JSON):

| `x-rt-reason` | HTTP | Meaning |
|---|---|---|
| `not_found` | 404 | unknown slug, no live version, or `triggers.http = false` |
| `unauthorized` | 401 | `access = "key"` without a valid key with `invoke` |
| `forbidden` | 403 | ingress path / IP rule |
| `trigger_not_allowed` | 403 | the plan does not allow the trigger |
| `method_not_allowed` | 405 | ingress `allowed_methods` |
| `too_large` | 413 | request body over `limits.max_body_bytes` |
| `rate_limited`, `quota_exceeded` | 429 | plan rate limit or daily quota (`Retry-After`) |
| `guest_error` | 500 | uncaught throw, rejected promise, non-`Response` return (details in logs only) |
| `cpu_limit`, `oom`, `build_failed` | 502 | CPU budget, memory limit, failed build |
| `queue_timeout`, `overloaded`, `shed`, `draining`, `restarting`, `route_stale` | 503 | node or service capacity; retry (with `Retry-After` when present) |
| `timeout` | 504 | wall limit |
| `call_depth` | 508 | service call chain deeper than 15 |

Capacity is queued, not refused: a request waits for a slot and gets `503 queue_timeout` only after the queue bound.

## Egress (`fetch`, outbound `WebSocket`)

Only `http`/`https` (`ws`/`wss` for WebSockets) to public addresses. Refused: loopback, private ranges, link-local
(including cloud metadata `169.254.169.254`), CGNAT, ULA and other reserved ranges, `localhost`, the node's own
addresses, ports 25/465/587, and hosts outside the service's `egress.allowed_hosts` when set. Redirects are followed up
to 5 hops and re-checked; https -> http is refused.

`fetch` rejects with a `TypeError` whose message is the code:

| Code | Cause |
|---|---|
| `egress_denied` | address, host, port or allowlist rule |
| `egress_limit` | too many fetches in this task |
| `egress_too_large` | response body over the limit |
| `egress_quota` | tenant's daily egress (`egress_mb_per_day`) used up |
| `egress_timeout` | connect or response timeout |
| `egress_dns` | name did not resolve |
| `egress_upstream` | upstream connection or protocol error |
| `egress_insecure_redirect` | redirect from https to http |
