# `21ci` commands

Source of truth: `crates/rt-cli/src/cli.rs` (clap definitions; `21ci <cmd> --help`) in
https://github.com/21-ci/runtime . Services are named by slug (`hello`) or id (`svc_…`). Global options may come
before or after the command.

```
21ci [--context NAME] [-o table|json | --json] [--as-tenant TEN_ID] [--config FILE] [--retry N] <command> ...
```

## Setup

| Command | Notes |
|---|---|
| `config set-context <name> --api URL [--invoke URL] [--token TOKEN] [--tenant TEN]` | create/update; only given fields change; first context becomes current |
| `config use-context <name>`, `config get-contexts`, `config current` | tokens are never printed |
| `init <dir> [--template deno\|deno-kv\|wasm-rust\|wasm-js] [--name SLUG] [--force]` | scaffold from the samples (default `deno`) |
| `doctor` | config, credentials, control API, auth, version, node mode, invoke listener; exit 1 on a failed check |
| `whoami` | principal, tenant, key, scopes, projects, environments, plan (`--json` adds the plan settings) |
| `limits` | plan quotas and what is left today (UTC day) |
| `version [--server]`, `completion <shell>`, `openapi [--out FILE]`, `docs` | |

## Services and versions

| Command | Notes |
|---|---|
| `service create <slug> [--runtime deno\|wasm] [--name N] [--profile P] [--access public\|key\|internal] [--project P] [--env ENV] [--allowed-caller C]... [--reuse-isolates B] [--reuse-instances B] [--min-instances N] [--max-instances N] [--durable] [--set BLOCK.KEY=VALUE]...` | `409 slug_taken` if it exists |
| `service update <svc> [--name] [--profile] [--access] [--env] [--allowed-caller C]... \| [--no-allowed-callers] [--reuse-isolates B] [--reuse-instances B] [--min-instances N] [--max-instances N] [--durable B] [--set ...]... [--unset BLOCK.KEY]...` | read-merge-write with `If-Match` |
| `service list [--project P] [--env ENV] [--limit N] [--cursor C] [--all]` | |
| `service get <svc>`, `service stats <svc> [--window 5m\|1h\|24h]` | |
| `service delete <svc> [--keep-data]` | destructive: drops KV, SQL and blobs unless `--keep-data` |
| `push <path> [--service SLUG] [--create [--project P] [--env ENV]] [--runtime deno\|wasm] [--no-activate] [--close-sockets] [--compile lazy\|eager] [--wait] [--wait-timeout 120s]` | new version per push (identical sources are deduplicated by digest but still numbered) |
| `versions <svc> [N]` | list, or one version with `failure` (`reason`: `syntax`, `resolve`, `fetch`, `lock_mismatch`, `timeout`, `oom`, `too_large`, `internal`) |
| `rollback <svc> [--to N] [--close-sockets]` | default: newest older version that did not fail; `--to` can roll forward |
| `rebuild <svc> <N>`, `build-logs <svc> <N>`, `source <svc> <N> [--out FILE]` | |
| `types <svc> [VERSION] [-d DIR]` | Deno RPC surface as `<slug>.d.ts` (`422 no_rpc_surface` for WASM or no `rpc` export) |

Profiles: `max_density`, `balanced`, `min_latency`. `--profile` defaults to the node's.

Callers (`--allowed-caller`): a service slug, a `svc_…` id, or `env:<environment>`. Without any, only services of the
same project and environment may call or enqueue to it. A list names every admitted caller.

Instances: `--min-instances N` keeps N warm (no scale to zero); `--max-instances N` (>= 1) caps concurrent copies,
excess requests queue. Both are clamped to the plan's `max_instances`.

## Config blocks

| Block | Fields |
|---|---|
| `limits` | `wall_ms`, `cpu_ms`, `memory_mb`, `max_body_bytes` (above the plan maximum: `422 limit_exceeds_plan`) |
| `scaling` | `min_instances`, `max_instances`, `max_queued` (over it `503 overloaded`), `keep_resident` |
| `triggers` | `http`, `interval`, `cron`, `queue`, `call`, `websocket` (booleans, all on by default) |
| `ws` | `allowed_origins[]` (empty = any), `warm` (default true), `max_sockets` |
| `ingress` | `allowed_methods[]`, `allow_paths[]` (prefix `*`), `ip_allow[]` (CIDRs), `rate_limit.rps`, `rate_limit.burst` |
| `egress` | `allowed_hosts[]` (JSON array required), `denied_hosts[]` |
| `deno` | `reuse_isolates` |
| `wasm` | `reuse_instances` |

Top-level service fields (create): `slug`, `runtime`, `name`, `profile`, `project`, `environment`, `access`, `durable`,
`call.allowed_callers[]`, and the blocks above. Unknown fields are `422 validation_failed`.

## Env, secrets, hosts, data

| Command | Notes |
|---|---|
| `env get <svc>`, `env set <svc> K=V...`, `env unset <svc> K...` | whole-map PUT under the hood (not atomic across concurrent editors) |
| `secret set <svc> NAME [--from-file FILE]` | value from the file (verbatim) or stdin (one trailing newline dropped) |
| `secret list <svc>`, `secret delete <svc> NAME` (alias `rm`) | values are never shown |
| `host attach <svc> <host>` (alias `add`), `host list <svc>`, `host get <svc> <host> [--wait] [--wait-timeout 15m] [--wait-interval 10s]`, `host detach <svc> <host>` | tenant claims are `pending_verification` until a TXT record `_rt-verify.<host>` holds the token (`--json` gives `dns_record`) |
| `data export <svc> [--out FILE]` | SQLite snapshot (scope `data:admin`) |

Env/secret names: `[A-Z_][A-Z0-9_]*`, no `RT_` prefix, at most 100 entries, values at most 32 KB.

## Triggers

| Command | Notes |
|---|---|
| `schedule add <svc> (--every D \| --cron EXPR) [--tz ZONE] [--payload JSON] [--disabled]` | `--tz` only with `--cron` (default UTC). `422 schedule_too_frequent` below the plan minimum; `403 quota_exceeded` past `max_schedules` |
| `schedule list <svc>` | interval, cron and pending one-off schedules |
| `schedule update <svc> <sch_id> [--every D \| --cron EXPR] [--tz ZONE] [--payload JSON] [--enable \| --disable]` | |
| `schedule rm <svc> <sch_id>` (aliases `remove`, `delete`) | |
| `queue send <svc> --body JSON\|@file\|- [--delay D] [--dedup ID]` | delay at most 7 days; `429 queue_full` |
| `queue stats <svc>`, `queue dlq <svc> [--redrive \| --purge]` | DLQ lists at most 100 |
| `invoke <svc> [path] [-X METHOD] [-d DATA\|@file] [-H "Name: value"]... [--key rtk_…] [--fail] [--body-only]` | `<svc>` may be a slug, `svc_…` id or base URL. `-d` implies POST; inline data starting with `{`, `[` or `"` is sent as JSON |

## Observability

| Command | Notes |
|---|---|
| `logs <svc> [-f] [--since TIME] [--until TIME] [--level debug\|info\|warn\|error] [--task task_…] [--kind runtime\|build\|system] [--limit N] [--cursor C]` | TIME: RFC 3339 or a duration back (`15m`, `2h`, `1d`); `--limit` up to 1000 |
| `stats <svc> [--window 5m\|1h\|24h]` | requests, errors, outcomes, admission, latency percentiles |
| `events <svc> [--limit N] [--cursor C]` | deploys, rollbacks, env/secret writes, ... |
| `sockets ls <svc> [--object NAME] [--tag TAG]`, `sockets close <svc> (<ws_id> \| --tag TAG) [--object NAME] [--code N] [--reason TEXT]` | object default `default`; code 1000 or 3000-4999 |
| `objects ls <svc> [--prefix P]`, `objects rm <svc> <name>` | `rm` drops a durable object's storage and alarm |

## Projects, tokens, SDKs

| Command | Notes |
|---|---|
| `project create <slug> [--name N] [--env a,b]`, `project list`, `project get <p>`, `project update <p> [--name N] [--env a,b \| --add-env x --remove-env y]`, `project delete <p>` | every tenant has `default` with environment `production` |
| `token create --scopes a,b [--name N] [--project prj_…] [--environment e] [--expires-in 24h \| --expires-at RFC3339]`, `token list`, `token revoke <key_id>` | your own tenant's keys (needs `keys:write`); never more than your own scopes |
| `sdk ls [--project P] [--environment E]`, `sdk types <name> [-d DIR] [--project P] [--environment E]` | operator SDKs your grants allow |
| `mcp` | stdio bridge to `<api>/mcp` with your tenant key |

Operator-only (root token): `tenant *`, `key *`, `plan *`, `node *`, `sdk publish|releases|promote|rollback|rm|grants`.

## Scopes

| Commands | Scope |
|---|---|
| `service list/get`, `versions`, `source`, `env get`, `secret list`, `host list/get`, `schedule list`, `queue stats`, `queue dlq` (list), `events`, `sockets ls`, `objects ls`, `project list/get`, `sdk ls/types`; every slug lookup | `services:read` |
| `service create/update/delete`, `host attach/detach`, `schedule add/update/rm`, `project create/update/delete` | `services:write` |
| `push`, `rollback`, `rebuild` | `deploy` (+ `services:read`; `services:write` for `--create`) |
| `env set/unset` | `env:write` |
| `secret set/delete` | `secrets:write` |
| `queue send`, `queue dlq --redrive/--purge` | `queue:write` |
| `logs`, `build-logs`, `stats` | `logs:read` |
| `data export`, `objects rm` | `data:admin` |
| `sockets close` | `sockets:write` |
| `token *` | `keys:write` |
| `invoke` | none for `access=public`; a key with `invoke` for `access=key` |

Default scopes of a key made by `21ci key create`: `services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write`.
