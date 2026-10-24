# CLI: `21ci`

`21ci` is a command line client for the control API ([api.md](api.md)): tenants, keys and usage, services and their config blocks, deploys, versions and rollbacks, env, secrets, custom hosts, schedules, queue, logs, stats, data export and node operations. It is one small binary (crate `crates/rt-cli`): no V8, no wasmtime, no runtime crates in its dependency tree. It only needs a reachable control API.

- [Install](#install)
- [Quickstart](#quickstart)
- [Contexts and configuration](#contexts-and-configuration)
- [Environment variables](#environment-variables)
- [Global options and output](#global-options-and-output)
- [Commands](#commands)
- [Scopes](#scopes)
- [CI and scripting](#ci-and-scripting)
- [Exit codes and errors](#exit-codes-and-errors)
- [Busy answers and `--retry`](#busy-answers-and-retry)
- [Control API coverage](#control-api-coverage)
- [Versus `sample/scripts/deploy.sh`](#versus-samplescriptsdeploysh)

## Install

From a checkout of this repository:

```bash
make cli             # cargo build -p rt-cli  ->  target/debug/21ci   (RELEASE=1 for target/release/21ci)
make cli-install     # cargo install --path crates/rt-cli --locked  ->  ~/.cargo/bin/21ci
# or directly
cargo install --path crates/rt-cli
```

Needs the Rust toolchain from `rust-toolchain.toml` (1.95). Shell completions (bash, zsh, fish, elvish, powershell):

```bash
21ci completion bash > ~/.local/share/bash-completion/completions/21ci
21ci completion zsh  > "${fpath[1]}/_21ci"
21ci completion fish > ~/.config/fish/completions/21ci.fish
```

`21ci version` prints the client version (`21ci 0.1.0`); `21ci version --server` also asks the node (`GET /v1/version`, any valid token; `node/status` on an older node). `21ci doctor` checks the whole setup (below).

## Quickstart

Against a local node (`make dev`, root token `dev-root-token`):

```bash
# 1. operator context, and a check that it works
21ci config set-context local --api http://127.0.0.1:8081 --invoke http://127.0.0.1:8080 --token dev-root-token
21ci doctor

# 2. a tenant and a key for it (the secret is shown once)
21ci tenant create demo
21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name me

# 3. work as that tenant
21ci config set-context demo --api http://127.0.0.1:8081 --invoke http://127.0.0.1:8080 --token rtk_01M3…
21ci config use-context demo

# 4. deploy and call a sample
21ci push sample/deno/hello --service hello --create --wait
21ci invoke hello '/?name=Ada'
21ci logs hello -f
```

The root token alone cannot create services (they belong to a tenant). Either use a tenant key as above or act for a tenant with the operator token: `21ci --as-tenant ten_… service create hello` (`x-rt-tenant` header, also `RT_TENANT`, or `--tenant` in `set-context`).

## Contexts and configuration

A *context* names one node: control API URL, invoke URL, token and, optionally, a tenant to act for. Contexts live in `~/.config/21ci/config.toml` (`$XDG_CONFIG_HOME/21ci/config.toml` when set, or `--config <file>`). The file holds tokens, so it is written with mode 0600.

```toml
current_context = "demo"

[contexts.demo]
api = "http://127.0.0.1:8081"
invoke = "http://127.0.0.1:8080"
token = "rtk_01M3…"
# tenant = "ten_…"     # operator token acting for a tenant (x-rt-tenant)
```

| Command | Does |
|---|---|
| `21ci config set-context <name> --api URL [--invoke URL] [--token TOKEN] [--tenant TEN]` | create or update a context; only the given fields change; the first context becomes current; a new context needs `--api` |
| `21ci config use-context <name>` | switch the current context |
| `21ci config get-contexts` | list them (the token is never printed) |
| `21ci config current` | print the current context name; with `--json` the resolved values |

```console
$ 21ci config get-contexts
CURRENT  NAME  API                    INVOKE                 TENANT  TOKEN
*        demo  http://127.0.0.1:8081  http://127.0.0.1:8080  -       yes
         root  http://127.0.0.1:8081  -                      -       yes
```

**Precedence**, highest first. Environment variables always win over the context, so a CI job can run without a config file:

| Setting | Order |
|---|---|
| control API | `RT_API`, context `api`, `http://127.0.0.1:8081` |
| invoke listener | `RT_INVOKE`, context `invoke`, `http://127.0.0.1:8080` |
| token | `RT_KEY`, `RT_ROOT_TOKEN`, context `token`, none (only a debug build without a root token accepts that) |
| tenant (`x-rt-tenant`) | `--as-tenant` / `RT_TENANT`, context `tenant`, none |
| context | `--context` / `RT_CONTEXT`, `current_context`, none |

These are the same variables `sample/scripts/*.sh` read (`RT_API`, `RT_INVOKE`, `RT_KEY`). A trailing `/` on a URL is ignored. The control token is sent only to the control API, never to the invoke listener; `invoke` uses `--key` / `RT_INVOKE_KEY` for `access=key` services.

## Environment variables

| Variable | Meaning |
|---|---|
| `RT_API` | control API base URL (default `http://127.0.0.1:8081`) |
| `RT_INVOKE` | invoke listener base URL (default `http://127.0.0.1:8080`) |
| `RT_KEY` | Bearer token: a tenant key `rtk_…` or the root token |
| `RT_ROOT_TOKEN` | fallback for `RT_KEY` (the node's own variable, handy on the node host) |
| `RT_CONTEXT` | context to use (`--context`) |
| `RT_TENANT` | tenant to act for with the operator token (`--as-tenant`) |
| `RT_INVOKE_KEY` | key sent by `invoke` to `access=key` services (`--key`) |
| `RT_RETRY` | retries of a busy answer (`--retry`, below) |
| `XDG_CONFIG_HOME` | where `21ci/config.toml` lives (default `~/.config`) |

An empty variable counts as unset.

## Global options and output

```
21ci [--context NAME] [-o table|json | --json] [--as-tenant TEN_ID] [--config FILE] [--retry N] <command> …
```

| Option | Meaning |
|---|---|
| `--context NAME` | context to use for this run (env `RT_CONTEXT`) |
| `-o`, `--output table\|json` | `table` (default): aligned columns and `Label:  value` blocks for people; `json`: the API's JSON, pretty-printed on stdout |
| `--json` | same as `-o json` |
| `--as-tenant TEN_ID` | act for a tenant with the operator token (env `RT_TENANT`) |
| `--config FILE` | config file instead of `~/.config/21ci/config.toml` |
| `--retry N` | repeat a request answered `429`/`503` with a `Retry-After` (env `RT_RETRY`, default 0; see [below](#busy-answers-and-retry)) |

Global options may come before or after the command (`21ci service list --json`).

Services are named by **slug** (`hello`) or **id** (`svc_01M3…`). A slug costs one extra request (`GET /v1/services?slug=hello`); an id is used as is. Tenants are `ten_…`, keys `key_…`, schedules `sch_…`.

Table output is stable enough to read with `awk`/`grep`, but scripts should use `--json` (with `jq`). Hints and notices (`No services found.`, `(more results: --cursor c1, or --all)`, "the secret is shown only once", the DNS hint of `host attach`) go to stderr and never into the JSON. With `--json` every command prints the API's JSON; commands with no response body print a small object instead: `{"deleted": "svc_…"}`, `{"revoked": "key_…"}`, `{"detached": "shop.example.com"}`, `{"draining": true}`, and downloads written with `--out` print `{"path", "bytes"}`. Two commands add computed fields, documented with them: `push` and `host`.

List commands (`tenant list`, `service list`) show one page; `--limit N` sets the page size, `--cursor C` continues, `--all` follows every page.

## Commands

| Group | Commands |
|---|---|
| setup | [`config`](#contexts-and-configuration), [`init`](#init), [`doctor`](#doctor), [`whoami`](#whoami), [`limits`](#limits), [`completion`](#install), [`version`](#install) |
| operator | [`tenant`](#tenant-operator), [`key`](#key-operator), [`plan`](#plan-operator), [`node`](#node-operator), [`sdk`](#sdk) (publish, grants) |
| projects and tokens | [`project`](#project), [`token`](#token) |
| AI assistants | [`mcp`](#mcp) |
| services | [`service`](#service), [`push`](#push), [`versions`, `rollback`, `rebuild`, `build-logs`, `source`](#versions-rollback-rebuild-build-logs-source), [`types`](#types), [`env`](#env), [`secret`](#secret), [`host`](#host), [`data`](#data), [`sockets`, `objects`](#sockets-objects) |
| operator SDKs | [`sdk ls`, `sdk types`](#sdk) (tenant key: what your grants allow) |
| triggers | [`schedule`](#schedule), [`queue`](#queue), [`invoke`](#invoke) (and [`invoke --async`](#invoke-async)) |
| events | [`topic`](#topic), [`consumer`](#consumer), [`pubsub`](#pubsub), [`invocation`](#invocation) |
| observability | [`logs`](#logs), [`stats`](#stats), [`events`](#events) |
| API docs | [`openapi`, `docs`](#openapi-docs) |

### init

```bash
21ci init <dir> [--template deno|deno-kv|wasm-rust|wasm-js] [--name SLUG] [--force]
```

Scaffolds a project from the repository samples (embedded in the binary): `deno` = `sample/deno/hello`, `deno-kv` = `sample/deno/kv-counter`, `wasm-rust` = `sample/wasm/rust-hello` (`wasm32-wasip2` crate), `wasm-js` = `sample/wasm/js-componentize` (`jco componentize`). The default template is `deno`. The project name (default: the directory name, cleaned up to a slug) is used in the README and the build script. A directory that is not empty is refused unless `--force`.

```console
$ 21ci init my-fn --template deno
Created my-fn (template deno)
  README.md
  main.ts
Next: cd my-fn && 21ci push . --service my-fn --create
```

### doctor

```bash
21ci doctor
```

One line per check; exit 1 when a check fails (warnings do not fail):

| Check | What it does |
|---|---|
| `config` | which context and config file are in use |
| `credentials` | the kind of token (root token or `rtk_…` key, never its value) and `--as-tenant` |
| `control api` | `GET /v1/openapi.json` (no auth needed): reachable, latency, API version |
| `auth` | `GET /v1/node/status`: `200` = operator; `403` = tenant key, then `GET /v1/services?limit=1` checks `services:read`; `401` = the token is rejected |
| `version` | client vs node version (`major.minor` must match, else a warning): from `node/status` for the operator, `GET /v1/version` for a tenant key (skipped on a node that predates it) |
| `node mode` | `normal`, or a warning for `shed` / `drain`; operator only |
| `invoke listener` | `GET <RT_INVOKE>/`: any HTTP answer counts (a warning when unreachable, as it may be on another network) |

```console
$ 21ci doctor
CHECK            STATUS  DETAIL
config           ok      context local; /home/me/.config/21ci/config.toml
credentials      ok      root token (operator)
control api      ok      http://127.0.0.1:8081 answers in 3 ms (API 1.0.0)
auth             ok      operator: node routes allowed
version          ok      client 0.1.0, node 0.1.0
node mode        ok      normal
invoke listener  ok      http://127.0.0.1:8080 answers in 2 ms (HTTP 404)
```

`--json` prints `{"ok": true, "checks": [{"check", "status": "ok|warn|fail|skip", "detail"}]}`.

### whoami

```bash
21ci whoami
```

`GET /v1/me`: who the credentials are (any valid token). For a tenant key: tenant, key, scopes, the projects and environments it is limited to, and the tenant's plan; for the operator: all scopes, and the tenant of `--as-tenant` if given. `--json` adds the plan's effective settings (overrides applied).

```console
$ 21ci whoami
Principal:     tenant
Tenant:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
Key:           key_01M3XM1AG5HN6WY9S6JQ550JQV
Scopes:        services:read,services:write,deploy,invoke,logs:read
Projects:      all
Environments:  all
Plan:          pro
Node version:  0.1.0
```

### limits

```bash
21ci limits
```

`GET /v1/limits`: the plan limits of your tenant and what is left of them today (any tenant key; the operator needs `--as-tenant`). Daily counters reset at UTC midnight.

```console
$ 21ci limits
Plan: pro
Day:  2026-10-12 (daily counters reset 2026-10-13T00:00:00Z)

QUOTA        LIMIT      USED     LEFT       PERIOD
invocations  1000000    12       999988     day
egress       9.8 GiB    3.0 MiB  9.8 GiB    day
storage      1.0 GiB    0 B      1.0 GiB    total
services     10         3        7          total
schedules    3          2        1          per_service
```

`schedules` counts per service (`plan.max_schedules` is a per-service limit) and `USED` is the busiest service. `unlimited` means the plan sets no limit. `--json` prints the API's body.

### tenant (operator)

```bash
21ci tenant create <name> [--plan free] [--overrides '{"max_services":20,"limits_max":{…}}']
21ci tenant list [--limit N] [--cursor C] [--all]
21ci tenant get <ten_id>
21ci tenant update <ten_id> [--name N] [--plan P] [--overrides JSON] [--disable | --enable]
21ci tenant delete <ten_id> [--purge]
21ci tenant usage [<ten_id>] [--from DAY] [--to DAY]
```

```console
$ 21ci tenant create acme
ID:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
Name:      acme
Plan:      free
Disabled:  false
Created:   2026-10-02T06:15:06Z

$ 21ci tenant list
ID                              NAME  PLAN  DISABLED  CREATED
ten_01M3XM135SCP9EP41ZZXAQ5CCZ  acme  free  false     2026-10-02T06:15:06Z
```

Plans are defined in the node config (`[plans.*]`); `21ci plan list` shows them, and `--plan` must name one of them (`422` otherwise). `update` sends a `PATCH` with what you give: `--overrides` replaces the tenant's overrides object; `--disable` stops the tenant at once (its services leave the routing table, its keys stop working, decrypted env and secrets leave the node's memory) and `--enable` undoes it. `delete` answers `409 tenant_not_empty` while the tenant still has services; `--purge` deletes them and their data too.

`usage` shows the daily counters per service. `--from` / `--to` take a day (`2026-10-01`) or a duration back from today (`7d`); without them the whole history is returned. The total row sums the counters; storage is a level and is not summed. `usage` is the one tenant command a **tenant key** may run too, for its own tenant and with `logs:read` (rows of the services the key may touch); without an id it uses the tenant of the credentials (`GET /v1/me`).

```console
$ 21ci tenant usage ten_01M3XM135SCP9EP41ZZXAQ5CCZ --from 7d
DAY         SERVICE                         INVOCATIONS  CPU_MS  EGRESS     STORAGE
2026-10-06  svc_01M3XM1AMYAKX6EREP6J536ATG  1200         8400    2.0 MiB    1.0 MiB
2026-10-07  svc_01M3XM1AMYAKX6EREP6J536ATG  300          2100    512.0 KiB  1.0 MiB
TOTAL       -                               1500         10500   2.5 MiB    -
```

### plan (operator)

```bash
21ci plan list
21ci plan get <name>
```

`GET /v1/plans`: the plans of the node config. `list` shows the main quotas (`0` is shown as `unlimited`); `get` shows every setting of one plan as `KEY VALUE` lines (`--json`: `{name, settings}`). A tenant sees its own plan with `21ci whoami --json`.

```console
$ 21ci plan list
NAME  SERVICES  CONCURRENCY  RPS        INVOCATIONS/DAY  STORAGE_MB  HOST_SUFFIXES
free  10        10           50         100000           100         -
pro   1000      unlimited    unlimited  unlimited        1024        .bots.example.com
```

### key (operator)

```bash
21ci key create --tenant <ten_id> [--scopes a,b,…] [--name N] [--project prj_1,prj_2] [--environment production,staging] [--expires-at RFC3339]
21ci key list   --tenant <ten_id>
21ci key revoke <key_id> --tenant <ten_id>
```

Default scopes: `services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write` (everything except `data:admin` and `keys:write`). `--project` restricts the key to those project ids (`prj_…`, comma-separated or repeated; default: all projects, including ones created later) and `--environment` to those environment labels (default: all). `none` in a listing means an explicit empty list. A key with a project or environment restriction cannot change a service's `environment`, nor create, change or delete projects.

```console
$ 21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name ci
ID:        key_01M3XM1AG5HN6WY9S6JQ550JQV
Name:      ci
Scopes:    services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write
Projects:  all
Environments: all
Expires:   never
Created:   2026-10-02T06:15:14Z
Secret:    rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
The secret is shown only once: store it now.      # stderr
```

`key list` never shows secrets. To rotate a key, create a new one, switch the clients over, then `key revoke` the old one (revocation takes effect at once).

### token

```bash
21ci token create --scopes a,b,… [--name N] [--project prj_1,prj_2] [--environment production,staging] [--expires-in 24h | --expires-at RFC3339]
21ci token list
21ci token revoke <key_id>
```

Your own keys, for your own tenant: the commands call `/v1/keys` and need a key with the scope `keys:write` (`21ci key …` is the operator's). Use them to give a CI job, a colleague or an AI assistant ([`mcp`](#mcp)) a token that can do less than yours. A new key gets at most the scopes, the project and environment restrictions and the lifetime of the key you use (`--project` and `--environment` narrow them; `--project` takes `prj_…` ids, default: your own restriction); `--scopes` is required (there is no default: say what the token may do), and the server answers `403 insufficient_scope` for anything beyond your own. Without `--project` / `--environment` the new key has your own restriction, without `--expires-in` / `--expires-at` your own expiry. `token list` shows the live keys of the tenant (a key with a project or environment restriction sees only keys within its own), never a secret; `token revoke` stops a key at once.

```console
$ 21ci token create --scopes services:read,deploy,logs:read --name claude --expires-in 7d
ID:        key_01M4Q2GB5APYBGEPYPBX1X4ZYJ
Name:      claude
Scopes:    services:read,deploy,logs:read
Projects:  all
Environments: all
Expires:   2026-10-19T04:12:20Z
Created:   2026-10-12T04:12:20Z
Secret:    rtk_01M4Q2GB5APYBGEPYPBX1X4ZYJ_…
The secret is shown only once: store it now.      # stderr
```

### project

```bash
21ci project create <slug> [--name N] [--env production,staging]
21ci project list [--limit N] [--cursor C] [--all]
21ci project get <project>
21ci project update <project> [--name N] [--env a,b | --add-env x --remove-env y]
21ci project delete <project>
```

A project groups the services of your tenant and lists the environments they may be in. Every tenant has a `default` project with the environment `production`; a service is created in one project (`service create --project`) and in one of its environments (`--env`). `<project>` is a slug (`^[a-z][a-z0-9-]{1,62}$`, fixed at creation) or a `prj_…` id. `--env` takes comma-separated and repeated values (1 to 20 labels, `^[a-z][a-z0-9-]{0,31}$`); `project update --env` replaces the list, `--add-env` / `--remove-env` change it from the current one. An environment a service is in cannot be removed (`409 environment_in_use`) and a project with services cannot be deleted (`409 project_not_empty`). Creating, changing and deleting need `services:write` and a key with no project or environment restriction (or the operator); listing needs `services:read`. Deleting a project removes it from the `project_ids` of every key.

```console
$ 21ci project create web --name "Web shop" --env production,staging
ID:            prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ
Slug:          web
Name:          Web shop
Environments:  production, staging
Created:       2026-10-12T04:12:20Z
$ 21ci project update web --add-env dev
$ 21ci service create front --project web --env staging
$ 21ci service list --project web
ID                              SLUG   RUNTIME  PROJECT  ENV      LIVE  BUILD    ACCESS
svc_01M4Q2GB5APYBGEPYPBX1X4ZYJ  front  deno     web      staging  -     pending  public
$ 21ci project list
ID                              SLUG     NAME      ENVIRONMENTS
prj_01M4Q2FNM85KHERVGN5ZJAK0TK  default  Default   production
prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ  web      Web shop  production,staging,dev
```

### mcp

```bash
21ci mcp
```

A stdio bridge to the MCP endpoint of the control API ([mcp.md](mcp.md)) for assistants that start a local program instead of connecting to a URL (Claude Desktop, many editors). It reads JSON-RPC messages from stdin, one per line, posts each to `<api>/mcp` with your tenant key as a Bearer token and prints each answer as one line on stdout (stderr is for diagnostics only). The key comes from `RT_KEY` or the current context and never appears on a command line; the operator token is refused by the endpoint. A request that cannot be answered by the server (no connection, `401`, an error status without a JSON-RPC body) gets a JSON-RPC error with its `id` so the assistant never waits for it. The bridge exits 0 when stdin closes. Give the assistant a token made for it (`21ci token create --scopes … --expires-in …`).

### service

```bash
21ci service create <slug> [--runtime deno|wasm] [--name N] [--profile P] [--access public|key|internal]
                           [--project P] [--env ENV] [--allowed-caller CALLER]...
                           [--reuse-isolates true|false] [--reuse-instances true|false]
                           [--min-instances N] [--max-instances N] [--durable]
                           [--min-run-time DURATION] [--set BLOCK.KEY=VALUE]...
21ci service update <svc> [--name N] [--profile P] [--access A] [--env ENV]
                          [--allowed-caller CALLER]... | [--no-allowed-callers]
                          [--reuse-isolates B] [--reuse-instances B]
                          [--min-instances N] [--max-instances N] [--durable B]
                          [--min-run-time DURATION] [--set BLOCK.KEY=VALUE]... [--unset BLOCK.KEY]...
21ci service list [--project P] [--env ENV] [--limit N] [--cursor C] [--all]
21ci service get <svc>
21ci service stats <svc> [--window 5m|1h|24h]                  # same as `21ci stats`
21ci service delete <svc> [--keep-data]
```

The slug must match `^[a-z][a-z0-9-]{1,62}$` (checked before anything is sent). `--profile` is `max_density`, `balanced` or `min_latency` (default: the node's).

**Projects.** `--project` names the project to create the service in (a slug or `prj_…` id, default `default`); the project of a service cannot change. `--env` must be one of the project's environments (`21ci project update <project> --add-env staging` first), else `422 validation_failed` lists the allowed ones. `service list --project P --env E` shows the services of one project and/or environment.

**Environments and callers.** `--env` sets the service's environment label (`^[a-z][a-z0-9-]{0,31}$`, default `production`). Service-to-service calls (`std:services`) and `std:queue` enqueues between services of one tenant are admitted by the target's caller list: each `--allowed-caller` is a service slug, a `svc_…` id or `env:<environment>`. Without any, only services of the **same project** in the **same environment** may call it; a list names every admitted caller (add `env:production` to keep same-environment callers of a `production` service in its project; `env:` names the target's own project, never another one). `--allowed-caller` replaces the list, `--no-allowed-callers` clears it. Entries are checked locally first and ids are sent in canonical (uppercase) form. A slug entry admits whichever service holds that slug when the call is made, so prefer `svc_…` ids where that matters; a service of another tenant is only ever admitted by its id (and only when its plan allows cross-tenant calls). Changing `--env` needs the operator token or a key with no project or environment restriction (`403 insufficient_scope`).

**Instances.** An instance is one concurrently running copy of the service on the node (a Deno isolate or a WASM instance). `--min-instances N` keeps N instances warm: the service never scales to zero and the autoscaler never takes its concurrency limit below N (default 0). `--max-instances N` caps the instances running at once (N ≥ 1; default: the plan's `max_instances`, else the profile's ceiling); requests beyond the cap wait in the service's queue. Both are at most the plan's `max_instances` (a larger value is clamped and `service get` shows `plan` as its source); `--min-instances` above `--max-instances` is a usage error (exit 2). `service get` prints the effective bounds, for example `Instances: min 2 (service), max 4 (plan)`.

**Keep-alive.** `--min-run-time DURATION` sets `scaling.min_run_time_ms`: how long a reused instance (a Deno isolate with its evaluated module, or a WASM instance) may stay idle after it answered, waiting for the next task of the same version. It takes a duration (`500ms`, `30s`, `2m`; a bare number is seconds) and `0`: after the answer the instance stays only while tasks of the service wait for it, else it is disposed at once. Unset, the instance follows the node's idle TTL (`deno.reuse_idle_ttl`, the WASM `cache.idle_ttl`). It is a maximum the runtime may cut short (memory pressure, reuse caps, a new version) and matters only when reuse is on. Above the plan's (and the node's) maximum it is `422 limit_exceeds_plan`; `--unset scaling.min_run_time_ms` goes back to inherit. `service get` prints it as `Keep-alive: inherit (node idle TTL)`, `0 ms (disposed after the answer unless work waits)` or `30000 ms (service)` (the source is `plan` when the plan's maximum clamps it).

**Durable services.** `--durable` makes a durable service: its default export is a class, one instance per object chosen in code with `durable.get(name)`, each with its own storage and alarm, one event or request at a time per object ([handlers](handlers.md#stddurable)). `update --durable false` turns it off. A WASM durable service must export `std:host/socket-handler` (`422 missing_export` at push).

**Reuse knobs.** `--reuse-isolates` sets `deno.reuse_isolates` (reuse an isolate with its evaluated module for the next request of the same version; default: node config, then the profile, on for `min_latency`). `--reuse-instances` sets `wasm.reuse_instances` (default: the profile).

**Config blocks.** `--set BLOCK.KEY=VALUE` sets one field of a block; nested fields use more dots (`ingress.rate_limit.rps=50`). The value is JSON when it parses as JSON (`500`, `false`, `["GET","POST"]`, `{"rps":1}`, `"007"`), otherwise a plain string (`egress.allowed_hosts` needs a JSON array, though). `--unset BLOCK.KEY` removes a field so it falls back to its default. The API replaces a whole block on `PATCH`, so `update` reads the service, merges your fields into each block it touches, and sends the result with `If-Match: <etag>`: a concurrent change makes it fail with `412 precondition_failed` instead of being overwritten. Unknown blocks are refused locally; unknown fields come back as `422 validation_failed`.

| Block | Fields ([api.md](api.md#services)) |
|---|---|
| `limits` | `wall_ms`, `cpu_ms`, `memory_mb`, `max_body_bytes` (above the plan maximum: `422 limit_exceeds_plan`) |
| `scaling` | `min_instances` (same as `--min-instances`), `max_instances` (same as `--max-instances`), `max_queued` (queue bound; over it `503 overloaded`), `keep_resident`, `min_run_time_ms` (same as `--min-run-time`). `scaling.max_concurrency`, the old name of `max_instances`, is still taken by `--set` / `--unset` |
| `triggers` | `http`, `interval`, `cron`, `queue`, `call`, `websocket` (booleans, all on by default) |
| `ws` | `allowed_origins[]` (origins allowed to open a WebSocket; empty = any), `warm` (keep the isolate of an event for the next one; default true), `max_sockets` (the service's own cap) |
| `ingress` | `allowed_methods[]`, `allow_paths[]`, `ip_allow[]`, `rate_limit.rps`, `rate_limit.burst` |
| `egress` | `allowed_hosts[]`, `denied_hosts[]` |
| `deno` | `reuse_isolates` (same as `--reuse-isolates`) |
| `wasm` | `reuse_instances` (same as `--reuse-instances`) |
| `events` | `subscribe[]` (`{subject, group?}`: pub/sub subjects the service receives), `consume[]` (`{topic, group?, start?, batch_size?, max_attempts?, on_exhausted?}`: stream topics it consumes); [handlers](handlers.md) |
| `async` | `enabled` (accept `invoke --async`; default false), `max_attempts` (1..10, default 3), `max_event_age_s` (60..86400, default 3600) |

```console
$ 21ci service create billing --env staging --allowed-caller env:staging --allowed-caller checkout
$ 21ci service update billing --env production --no-allowed-callers
$ 21ci service update billing --set scaling.max_queued=100 --set limits.cpu_ms=500 --set triggers.cron=false
$ 21ci service update billing --set 'ingress.allowed_methods=["GET","POST"]' --unset scaling.max_instances
$ 21ci service update billing --min-instances 2 --max-instances 16
$ 21ci service create edge --runtime wasm --reuse-instances true --set scaling.max_queued=1000
$ 21ci service update billing --min-run-time 30s
$ 21ci service update billing --set 'events.subscribe=[{"subject":"orders.>"}]' --set async.enabled=true
$ 21ci service create chat-rooms --durable --access internal --set 'ws.allowed_origins=["https://chat.example.com"]'
```

```console
$ 21ci service list
ID                              SLUG   RUNTIME  PROJECT  ENV         LIVE  BUILD  ACCESS
svc_01M3XM1AMYAKX6EREP6J536ATG  hello  deno     default  production  2     ready  public

$ 21ci service get hello
ID:               svc_01M3XM1AMYAKX6EREP6J536ATG
Slug:             hello
Runtime:          deno
Name:             hello
Profile:          -
Project:          default
Environment:      production
Allowed callers:  (services of the same project in the same environment)
Live:             2
Access:           public
Reuse isolates:   default (node config, then profile)
Limits:           cpu_ms=500
Scaling:          max_instances=16
Instances:        min 0 (default), max 16 (service)
Keep-alive:       inherit (node idle TTL)
Triggers:         off: cron
Build:            ready
Tier:             T1
In flight:        3
Concurrency:      limit 8 (1..16), running 3, queued 0 (max 256)
ETag:             "4"
Tenant:           ten_01M3XM135SCP9EP41ZZXAQ5CCZ
```

`get` shows only the fields that are set in each block (`-` for none); `Ingress`, `Egress`, `Events` and `Async` lines appear when they hold something, `Last error` when the service has one. `Concurrency` is the service's queueing state (`status.admission`): the adaptive concurrency limit with its bounds, what runs and what waits; `-` while the service has not run recently. `service stats` shows the rest of it. The resolved values with their sources (profile, plan, node) are in `effective` of `service get --json`.

### push

```bash
21ci push <path> [--service SLUG] [--create [--project P] [--env ENV]] [--runtime deno|wasm] [--no-activate]
                 [--close-sockets] [--compile lazy|eager] [--wait] [--wait-timeout 120s]
```

Uploads one version of a service and makes it live. Same formats as `sample/scripts/deploy.sh` ([formats and limits](api.md#deploy-input)):

| `<path>` | Sent as | Runtime |
|---|---|---|
| `x.ts` | `application/typescript`, the file | deno |
| `x.js`, `x.mjs` | `text/javascript`, the file | deno |
| `x.wasm` | `application/wasm`, the file (a WASI P2 component exporting `wasi:http/incoming-handler`) | wasm |
| `x.zip`, `x.tar` | `application/zip`, `application/x-tar`, the archive (optional `rt.json` inside) | deno |
| directory with `rt.json` | `application/json` bundle `{"entry", "files": {path: text}, "imports"?}` | deno |
| directory without `rt.json` | one source file: that file as is; several: a bundle with the entry guessed from `main.ts`, `main.js`, `index.ts`, `index.js`, `mod.ts` | deno |

`rt.json` is `{"entry": "main.ts", "imports": {"zod": "npm:zod@3"}}` (both optional). Bundles contain the `.ts .tsx .mts .js .jsx .mjs .json` files below the directory, sorted by path; dot files and directories, `node_modules` and `rt.json` itself are left out.

Flow: look the service up by slug, **create it only with `--create`** (`{"slug", "runtime", "name": slug}`, plus `--project` / `--env` when given: the project must exist and list the environment; set the blocks afterwards with `service update`, or create the service with `service create` first), then `POST /v1/services/{id}/versions?activate=1[&compile=…]`.

| Option | Meaning |
|---|---|
| `--service SLUG` | service to push to; default: the file or directory name, cleaned up to a slug (`My_App.ts` gives `my-app`); a name that is not a valid slug needs `--service` |
| `--create` | create the service when it does not exist (without it a missing service is an error) |
| `--project`, `--env` | project (slug or `prj_…` id) and environment of a service created by `--create` (they need `--create`; defaults `default` and `production`) |
| `--runtime` | runtime of a created service; defaults to what the artifact implies (`.wasm` is wasm); a contradicting value is a usage error |
| `--no-activate` | upload without changing the live version (make it live later with `21ci rollback SVC --to N`) |
| `--close-sockets` | close the service's open WebSockets (code 1012) once the version is live; by default they stay open and their next event runs the new version |
| `--compile lazy\|eager` | `lazy`: the first request builds it; `eager`: build now (default: the node's profile) |
| `--wait` | poll `GET …/versions/{n}` until `build_state` is `ready`; exits 1 on `failed` or after `--wait-timeout` (default 120 s). Implies `--compile eager`; `--compile lazy` with `--wait` is a usage error |

```console
$ 21ci push sample/deno/hello --service hello --create --wait
Service:  svc_01M3XM1AMYAKX6EREP6J536ATG (slug: hello, runtime: deno, created)
Upload:   application/typescript, 1287 bytes
Version:  1 (build_state: building, sha256:4128185b…)
Building: ready
Live:     yes
Invoke:   http://127.0.0.1:8080/s/hello/
```

`--json` prints one object: `{"service": {"id", "slug", "runtime", "created"}, "version": {…}, "activated": true, "invoke_url": "…"}`. A bundle or upload the server rejects (`422 validation_failed`) shows its diagnostics (`file`, `line`, `col`, `message`) under the error line. A build that fails shows its `failure`; `21ci build-logs <svc> <n>` shows the whole build log.

### versions, rollback, rebuild, build-logs, source

```bash
21ci versions   <svc> [N]
21ci rollback   <svc> [--to N] [--close-sockets]
21ci rebuild    <svc> <N>
21ci build-logs <svc> <N>
21ci source     <svc> <N> [--out FILE]
```

```console
$ 21ci versions hello
VERSION  BUILD    LIVE  DIGEST               CREATED
2        failed   -     sha256:e5fdfbd20a63  2026-10-02T06:15:26Z
1        ready    *     sha256:4128185bbcac  2026-10-02T06:15:14Z

$ 21ci versions hello 2
Version:        2
Build:          failed
Live:           no
Source digest:  sha256:e5fdfbd20a63…
Lock digest:    -
Created:        2026-10-02T06:15:26Z
Failure:        {"reason":"resolve","message":"cannot resolve npm:nope","diagnostics":[]}

$ 21ci rollback hello
Service hello (svc_01M3XM1AMYAKX6EREP6J536ATG) is now live at version 1 (was 2).
```

- `versions <svc> N` shows one version (`GET …/versions/N`): build state, digests and the build `failure` (`reason` is one of `syntax`, `resolve`, `fetch`, `lock_mismatch`, `timeout`, `oom`, `too_large`, `internal`; the invoke answer of a failed build is `502 build_failed`).
- `rollback` is `PUT /v1/services/{id}/live {"version": N}` with `If-Match: <etag>` of the service as read a moment before, so it fails with `412 precondition_failed` if somebody changed the service in between. Without `--to` it picks the newest version older than the live one that did not fail to build; `--to` can also roll forward. A failed version answers `409 version_not_ready`. `--close-sockets` closes the service's WebSockets (1012), as with `push`.
- `rebuild` clears a failed build (the negative cache) and builds again (`202`).
- `build-logs` streams `GET …/versions/N/build/logs` (Server-Sent Events) and ends when the build is `ready` or `failed`; lines look like `logs` lines, `--json` prints one record per line. It needs `logs:read`.
- `source` downloads the uploaded bytes as they were sent (a file, an archive, or the JSON bundle). Text goes to stdout; binary data on a terminal is refused, so pass `--out FILE` or redirect.

### types

```bash
21ci types <svc> [VERSION] [-d DIR]
```

Writes the TypeScript declarations of a Deno service's RPC surface (`GET /v1/services/{id}/types`): its interfaces and types
and an interface `<Slug>Rpc` with one signature per method of its `rpc` export, read from the source of the live version
(or `VERSION`), never run. With `-d DIR` (`--out-dir`) it writes `DIR/<slug>.d.ts`, else prints it.

```console
$ 21ci types billing -d types && cat types/billing.d.ts
export interface Invoice { id: string; amount: number }

export interface BillingRpc {
  createInvoice(arg: { amount: number }): Promise<Invoice>;
}
```

Use it as `const billing = services.bind("billing") as BillingRpc`. A service without an `rpc` export, or a WASM service
(its RPC is declared in WIT), is `422 no_rpc_surface`.

### sdk

Operator SDKs run in the runtime as services of the system tenant; tenants import their contract and every call is bound
to the release their grants select ([operator SDKs](api.md#operator-sdks)). Operator (root token):

```bash
21ci sdk publish  <name> <version> <path> [--channel C] [--description D] [--contract FILE --member NAME…]
21ci sdk ls
21ci sdk releases <name>
21ci sdk promote  <name> <version> [--channel stable]
21ci sdk rollback <name> [--channel stable]
21ci sdk rm       <name>
21ci sdk grants get [--sdk NAME]
21ci sdk grants set <scope> <sdk|*> [--allow | --deny] [--version-req REQ] [--channel C]
```

- `publish` sends the file or directory like `push` (`POST /v1/sdks`). The contract (`.d.ts` and the top-level names) is
  generated from the `defineSdk` methods unless `--contract` and `--member` give it (required for a WASM SDK). The first
  release becomes `stable`; later ones move only `--channel`. A method that never reads `ctx.caller` is refused
  (`422 sdk_unchecked_caller`).
- `promote` points a channel at a release; `rollback` moves it back to the release it pointed to before. Tenant services
  use the new release on their next call: nothing is redeployed.
- `rm` removes the SDK: calls answer `sdk_unavailable`, new deploys cannot import it; publishing again restores it.
- `grants set` writes the grant of a scope (`node`, `plan:<name>`, `tenant:ten_…`, `project:prj_…`,
  `environment:prj_…/<env>`). Flags not given inherit; none deletes the grant. `--allow` without `--version-req` is pinned
  to the stable major (`^1`). Lower scopes can only narrow.

Tenant key (`services:read`):

```bash
21ci sdk ls [--project P] [--environment E]       # the SDKs your grants allow, and the release each runs
21ci sdk types <name> [-d DIR] [--project P] [--environment E]
```

```console
$ 21ci sdk ls --environment staging
NAME  VERSION  CHANNEL  REQUIRES  MEMBERS                                  REASON
acme  1.1.0    canary   ^1        getPlatformInfo,listIntegrations,audit   -
$ 21ci sdk types acme -d types
Wrote types/acme.d.ts (1342 bytes).
```

`types/acme.d.ts` declares `module "sdk:acme"` and the `std:sdk` export `acme`, for your editor and `tsc`.

### env

```bash
21ci env get   <svc>
21ci env set   <svc> KEY=VALUE [KEY=VALUE …]
21ci env unset <svc> KEY [KEY …]
```

The API replaces the whole map on `PUT`, so `set` and `unset` read the current map, change it and write it back (not atomic: two people editing at once can overwrite each other). Values may contain `=`. Changes apply to the next task; no redeploy.

```console
$ 21ci env set hello GREETING=hi A=1
Set 2 variables on hello (svc_01M3XM1AMYAKX6EREP6J536ATG).
$ 21ci env get hello
KEY       VALUE
A         1
GREETING  hi
```

### secret

```bash
21ci secret set <svc> NAME [--from-file FILE]     # value from the file, else from stdin
21ci secret list <svc>
21ci secret delete <svc> NAME
```

Secrets are write-only: `list` shows names and update times, never values; `set` never echoes the value. `--from-file` sends the file verbatim; from stdin one trailing newline is dropped, so `echo token | 21ci secret set hello TOKEN` stores `token`. An empty value, or no value on an interactive terminal, is a usage error. The value never appears on the command line (no `ps` leak). Names are `[A-Z_][A-Z0-9_]*` without an `RT_` prefix.

```console
$ printf %s "$API_KEY" | 21ci secret set hello API_KEY
Secret API_KEY set on hello (svc_01M3XM1AMYAKX6EREP6J536ATG).
$ 21ci secret list hello
NAME     UPDATED
API_KEY  2026-10-02T06:15:26Z
```

Rotating the node's master key (re-sealing every stored secret under `RT_MASTER_KEY` while `RT_MASTER_KEY_PREVIOUS` still opens the old ones) is a node-side command, `rt secrets reseal`, not part of `21ci` ([SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#secrets)).

### host

```bash
21ci host attach <svc> <host>                                            # alias: add
21ci host list   <svc>
21ci host get    <svc> <host> [--wait] [--wait-timeout 15m] [--wait-interval 10s]     # alias: status
21ci host detach <svc> <host>                                            # aliases: rm, remove, delete
```

Custom hosts route `https://<host>/…` on the public listener to the service. The host is normalised first (lowercase, no trailing dot; `[a-z0-9.-]` only).

- With the **operator token** a mapping is `active` at once.
- With a **tenant key** the host must fall under one of the plan's `custom_host_suffixes` (`403 host_not_allowed`), and the claim is `pending_verification` until the node finds its token in a DNS TXT record at `_rt-verify.<host>` (it looks every `hosts.verify_interval` when `hosts.verify_dns` is on) or the operator approves it. `attach` and `get` print the record to create. A pending claim expires after `hosts.pending_ttl` (7 days); attaching it again keeps its token and expiry. After an unverified expiry the same tenant gets `409 host_cooldown` for one more TTL (the CLI prints when it can try again). At most `hosts.max_pending_per_tenant` (10) pending claims per tenant (`403 quota_exceeded`); a host someone else holds is `409 host_taken`.

```console
$ 21ci host attach shop shop.example.com
Host:        shop.example.com
Status:      pending_verification
Created:     2026-10-07T09:00:00Z
Expires:     2026-10-14T09:00:00Z (in 7d)
DNS record:  _rt-verify.shop.example.com. IN TXT "q8kB2…"
Publish the TXT record above; the node checks pending claims every hosts.verify_interval and activates      # stderr
shop.example.com when the token matches (or the operator approves it). Also point shop.example.com at the
node's public listener (A/AAAA or CNAME). Watch it with: 21ci host get shop shop.example.com --wait

$ 21ci host list shop
HOST              STATUS                CREATED               EXPIRES
api.example.com   active                2026-10-01T10:00:00Z  -
shop.example.com  pending_verification  2026-10-07T09:00:00Z  2026-10-14T09:00:00Z (in 6d 23h)

$ 21ci host get shop shop.example.com --wait
Waiting for shop.example.com to be verified (_rt-verify.shop.example.com. IN TXT "q8kB2…")…      # stderr
Host:     shop.example.com
Status:   active
Created:  2026-10-07T09:00:00Z
```

`get` reads one host (`GET …/hosts/{host}`; `404` when the host is not mapped to the service). `get --wait` polls it until the host is active; it exits 1 after `--wait-timeout`, or when the claim disappears (expired or released). With `--json`, `attach`, `list` and `get` print the API's host view (`{host, status, verify_token, created_at, expires_at}`) plus, while pending, `"dns_record": {"name": "_rt-verify.<host>", "type": "TXT", "value": "<token>"}` for DNS automation.

### data

```bash
21ci data export <svc> [--out FILE]
```

Downloads a consistent SQLite snapshot of the service's database (`GET …/data/export`, scope `data:admin`, audited as `data.exported`). Without `--out` the bytes go to stdout, which must not be a terminal.

```console
$ 21ci data export todo --out todo.db
Wrote the database of todo (svc_01M3XM2…) to todo.db (24576 bytes).
$ sqlite3 todo.db .tables
```

### sockets, objects

```bash
21ci sockets ls    <svc> [--object NAME] [--tag TAG]
21ci sockets close <svc> <ws_…> [--object NAME] [--code N] [--reason TEXT]
21ci sockets close <svc> --tag TAG [--object NAME] [--code N] [--reason TEXT]
21ci objects ls    <svc> [--prefix P]
21ci objects rm    <svc> <name>
```

The node holds a service's WebSockets; each belongs to an object (`default` unless a durable service chose another, [handlers](handlers.md#websockets-and-durable-objects)). `sockets ls` lists the open sockets of one object (`--object`, default `default`), every page; `sockets close` closes one socket or every socket of the object with a tag, with `--code` 1000 (default) or 3000-4999 (scope `sockets:write`, audited); the guest gets `webSocketClose`. `objects ls` lists the objects holding sockets and, for a durable service, those with storage. `objects rm` drops the storage and alarm of a durable object (scope `data:admin`); its sockets stay.

```console
$ 21ci objects ls chat-rooms
NAME    SOCKETS  STORAGE
lobby   2        yes
random  0        yes

$ 21ci sockets ls chat-rooms --object lobby
ID                              TAGS       PROTOCOL  CREATED               LAST MESSAGE          BUFFERED  CLIENT
ws_01M4R8J6Q1X2K3Y4Z5A6B7C8D9   user:ann   chat      2026-10-23T09:12:01Z  2026-10-23T09:14:40Z  0         203.0.113.7
$ 21ci sockets close chat-rooms --object lobby --tag user:ann --code 4000 --reason kicked
Closed 1 sockets of object lobby with tag user:ann.
```

### schedule

```bash
21ci schedule add    <svc> (--every 60s | --cron "*/5 * * * *") [--tz Europe/Berlin] [--payload '{"a":1}'] [--disabled]
21ci schedule list   <svc>
21ci schedule update <svc> <sch_id> [--every D | --cron EXPR] [--tz ZONE] [--payload JSON] [--enable | --disable]
21ci schedule rm     <svc> <sch_id>
```

`--every` takes `60s`, `5m`, `1h` (a bare number is seconds, whole seconds only); `add` needs exactly one of `--every` and `--cron`. The payload is JSON handed to the handler's `scheduled`; default `{}`. The node refuses schedules that are too frequent for the plan (`422 schedule_too_frequent`) or too many of them (`403 quota_exceeded`), and the service's `interval`/`cron` triggers must be enabled (`403 trigger_not_allowed`).

`update` changes only what you give: `--every` or `--cron` also switches the kind; `--tz` applies to cron schedules; the payload, the expression and the timezone are kept otherwise; `--disable` pauses a schedule without deleting it. (The API needs the kind on every `PATCH`; without `--every`/`--cron` the CLI reads it from the schedule list first.)

```console
$ 21ci schedule add hello --every 5m --payload '{"x":1}'
ID:        sch_01M3XM1PR5HKY89BTYGX0R5QV9
Kind:      interval
Schedule:  every 300s
Enabled:   true
Next due:  2026-10-02T06:20:26Z
Payload:   {"x":1}

$ 21ci schedule list hello
ID                              KIND      SCHEDULE    TZ  ENABLED  NEXT_DUE              PAYLOAD
sch_01M3XM1PR5HKY89BTYGX0R5QV9  interval  every 300s  -   true     2026-10-02T06:20:26Z  {"x":1}

$ 21ci schedule update hello sch_01M3XM1PR5HKY89BTYGX0R5QV9 --every 10m --disable
```

### queue

```bash
21ci queue send  <svc> --body '{"job":1}' [--delay 30s] [--dedup ID]
21ci queue stats <svc>
21ci queue dlq   <svc> [--redrive | --purge]
```

`--body` is JSON given inline, as `@file`, or `-` for stdin. `--delay` is at most 7 days; `--dedup` drops a message whose id was already queued. A full queue is `429 queue_full`; a service whose `queue` trigger is off (or whose plan does not allow queues) is `403 trigger_not_allowed`. `dlq` lists dead letters (at most 100); `--redrive` moves them all back to the queue, `--purge` deletes them.

```console
$ 21ci queue send hello --body '{"job":1}' --delay 1s
Enqueued msg_01M3XM200VVZ3TY1KA23QW4FWZ on hello (svc_01M3XM1AMYAKX6EREP6J536ATG).
$ 21ci queue stats hello
Depth:          1
In flight:      0
Oldest age ms:  9
DLQ:            0
$ 21ci queue dlq hello --redrive
Redrove 0 messages from the DLQ of hello (svc_01M3XM1AMYAKX6EREP6J536ATG).
```

### logs

```bash
21ci logs <svc> [-f] [--since TIME] [--until TIME] [--level debug|info|warn|error] [--task task_…] [--kind runtime|build|system] [--limit N] [--cursor C]
```

Without `-f`: one page of records, oldest first (`--limit` up to 1000, default 100). `--since` / `--until` take an RFC 3339 timestamp (`2026-10-02T06:00:00Z`) or a duration back from now (`15m`, `2h`, `1d`). `--level` is a minimum level. `--task` filters one invocation (the id is in the `x-rt-task-id` response header of `invoke`).

With `-f` (`--follow`): `GET …/logs?follow=1`, a Server-Sent Events stream, printed as it arrives until you press Ctrl-C or the server closes the stream. The server replays the stored history first and then streams; with `--since` the CLI prints the matching backlog itself and then resumes the stream after the last record (`Last-Event-ID`). `--json` prints one JSON record per line. A key may hold `ingress.sse_streams_per_key` streams at once (`429 rate_limited` with `Retry-After` beyond that; see `--retry`).

```console
$ 21ci logs logger -f --level info
2026-10-02T06:15:53.026Z info  v1 - build started (deno)
2026-10-02T06:15:53.052Z info  v1 - build finished in 25 ms
2026-10-02T06:15:55.560Z info  v1 task_000000000000000b hit /a
```

Columns: time, level, `v<version>`, task id (`-` for build and system records), message.

### stats

```bash
21ci stats <svc> [--window 5m|1h|24h]            # also: 21ci service stats <svc>
```

```console
$ 21ci stats hello --window 1h
Window:       1h
Requests:     7
Errors:       0 (0.00%)
Outcomes:     ok=7
Concurrency:  limit 8 (1..64), running 0, queued 0 (max 256)
Queue wait:   0.0 ms (timeouts 0), predicted run 7.6 ms
Autoscaler:   demand 1.4, latency baseline 7.2 ms, warm 1/proc, idle 12s

PHASE  P50_MS  P95_MS  P99_MS
queue  0.1     0.2     0.2
init   0.0     0.0     0.0
exec   7.6     39.7    39.7
total  8.3     39.9    39.9
```

`queue` is the time a request waited for a slot: per-service capacity limits queue requests instead of refusing them, so a growing `queue` latency (and `503 queue_timeout` outcomes) is the sign of a service at its adaptive concurrency limit.

The admission rows are the service's admission state (`admission` of `GET …/stats`, the same object as `status.admission` of the service): `Concurrency` is the adaptive limit with its bounds (`scaling.min` raised to `min_instances`, up to the ceiling and the plan's tenant cap), the tasks running and waiting, and the queue bound (`scaling.max_queued`); `Instances` is `3 running, min 2, max 64, warm target 4`: instances running now, their bounds and the warm instances the autoscaler wants kept; `Last change` is the latest move of the limit and its reason (`queue`, `trend`, `latency`, `memory`, `cpu`, `idle`, `bounds`) or `none`; `Queue wait` is the smoothed wait of requests that had to queue (it fades to 0 while requests start right away) and the number of `503 queue_timeout` since the state was created, then the predicted run time of the service version (`predicted run 4.3 ms`, the smoothed run time admission orders waiting work by, shorter first) and, when there is one, the estimate for background work (queue, stream and schedule tasks: `(background 2.1 s)`); `Autoscaler` shows the decaying peak of running tasks (`demand`), the latency baseline the limit is held against, the warm instances kept per processor (`0` = scaled to zero), the smoothed arrival rate (`12.5 req/s`) and the time since the last task. A service that has not run recently has no admission state (`Concurrency: -`).

### events

```bash
21ci events <svc> [--limit N] [--cursor C]
```

Control-plane events of the service, newest first: deploys, live changes, env/secret writes, host claims, schedule and DLQ operations, data exports.

```console
$ 21ci events hello --limit 3
TIME                  KIND         ACTOR                           DATA
2026-10-02T06:15:36Z  dlq.redrive  key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"redriven":0}
2026-10-02T06:15:26Z  rollback     key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"from":2,"to":1}
2026-10-02T06:15:26Z  deploy       key_01M3XM1PCHQ8DECFTC5TY0CXSX  {"version":2,"digest":"sha256:e5fd…","activated":true}
```

### invoke

```bash
21ci invoke <svc> [path] [-X METHOD] [-d DATA|@file] [-H "Name: value"]… [--key rtk_…] [--fail] [--body-only]
21ci invoke <svc> [path] [-X METHOD] [-d DATA|@file] [-H "Name: value"]… --async [--idempotency-key K]
```

Calls `ANY /s/{slug}/{path}` on the invoke listener (`RT_INVOKE`). `path` defaults to `/` and may carry a query string (`'/x?name=Ada'`, `'?name=Ada'`). `-d` makes the request a `POST` unless `-X` says otherwise; inline data is sent as `application/json` when it starts with `{`, `[` or `"`, otherwise `text/plain`; `@file` is sent as is as `application/octet-stream`; `-H 'Content-Type: …'` overrides. `<svc>` may also be a `svc_…` id (one control API lookup) or a base URL (`http://host:8080/s/hello`).

The output shows the request line, the status, and the headers that matter (`content-type`, `server-timing`, `retry-after` and every `x-rt-*`: `x-rt-task-id`, `x-rt-version`, `x-rt-reason` on runtime errors), then the body: JSON is pretty-printed, text as is, binary summarised.

```console
$ 21ci invoke hello '/x?name=Ada'
-> GET http://127.0.0.1:8080/s/hello/x?name=Ada
<- HTTP 200
   content-type: application/json
   server-timing: queue;dur=0.000, wait_runtime;dur=0.000, init;dur=5.932, exec;dur=4.720
   x-rt-task-id: task_0000000000000002
   x-rt-version: 1

{
  "message": "Hello, Ada!",
  …
}
```

- `--body-only` prints just the body bytes (pipe it: `21ci invoke img --body-only > out.png`).
- `--json` prints `{"status": 200, "headers": {…}, "body": …}` (`body` is parsed JSON for JSON responses, else text; `null` plus `"binary": true, "size": N` for binary).
- Like `curl` without `--fail`, a `4xx`/`5xx` answer is a successful request: exit 0. With `--fail` a status `>= 400` prints `error: HTTP 503 queue_timeout (retry after 1s)` (status, `x-rt-reason`, `Retry-After`) to stderr and exits 1, or 4 for a busy answer (`429`/`503` with `Retry-After`).
- `--retry N` repeats a busy answer (see [below](#busy-answers-and-retry)); the request body is sent again.
- `access=key` services need `--key rtk_…` (or `RT_INVOKE_KEY`) with the `invoke` scope. The control token is never sent to the invoke listener.

Invoke failures carry their reason in `x-rt-reason` (and as the `text/plain` body): `guest_error`, `cpu_limit`, `oom`, `timeout`, `forbidden`, `trigger_not_allowed`, … ([api.md](api.md)). Capacity is queued, not refused : a request waits for a slot up to the profile's bound and then gets `503 queue_timeout` with `Retry-After`; `overloaded` (`scaling.max_queued` reached), `shed` and `route_stale` are `503`s with `Retry-After` too, `draining` and `restarting` are `503`s without it (go to another node). `429` remains only for the plan's quotas (`rate_limited`, `quota_exceeded`).

#### invoke --async

`--async` sends `Prefer: respond-async`: a service with `async.enabled = true` accepts the request (`202`), queues it and runs it later with retries; the command prints the `inv_…` id and exits 0 (`--json`: `{"id", "state", "replay"}`). `--idempotency-key K` sends `Idempotency-Key: K`: the same key with the same request returns the same id (stderr says `idempotent replay`); with a different request it is `409 idempotency_mismatch`. A refusal prints its code and exits 1: `403 async_not_allowed`, `413 too_large` (body over 1 MiB), `409 idempotency_mismatch`, `400 bad_request`; `429 async_backlog_full` (with `Retry-After`) exits 4. When the service does not accept async requests the preference is ignored and the request runs synchronously: the answer is printed as without `--async`. Follow the invocation with [`invocation`](#invocation).

```console
$ 21ci invoke hook /run -d '{"order":1}' --async --idempotency-key order-1
inv_01M3XM4ZQ8JX2T4V3K9D7R5E1A
```

### topic

```bash
21ci topic create <name> [--project P] [--env E] [--partitions N] [--retention DURATION] [--max-mb M]
21ci topic list   [--project P] [--env E]
21ci topic get    <name|tpc_id> [--project P] [--env E]
21ci topic update <name|tpc_id> [--retention DURATION] [--max-mb M] [--project P] [--env E]
21ci topic delete <name|tpc_id> [--project P] [--env E] [--yes]
21ci topic append <name|tpc_id> --body JSON [--key K] [--header NAME=VALUE]... [--dedup ID] [--project P] [--env E]
21ci topic read   <name|tpc_id> [--partition N] [--from OFFSET|earliest|latest] [--from-time RFC3339] [--limit N] [--project P] [--env E]
```

A topic is a persistent, replayable log of records in one project environment (`std:stream` in [handlers](handlers.md)); services consume it through their `events.consume` block. A name (`[a-z0-9][a-z0-9._-]{0,62}`, not starting with `tpc_`) resolves in `--project` (slug or `prj_…`, default `default`) and `--env` (default `production`); a `tpc_…` id needs neither. `--partitions` is fixed at create (default 1); `--retention` is in whole hours (`24h`, `7d`; else exit 2 "retention must be a whole number of hours"; default 24h); `--max-mb` caps the topic's size, oldest records first (0 = only the plan's cap). Plan limits: more topics than `max_topics` is `403 quota_exceeded`; partitions, retention or size over the plan is `422 limit_exceeds_plan`; a taken name is `409 topic_exists`. `delete` removes the records, cursors and dead letters; it asks on a terminal and needs `--yes` otherwise.

`append` writes one record (`--body` is JSON inline, `@file` or `-`); records with the same `--key` go to the same partition, in order; `--dedup` drops a record whose id was appended before. A full topic is `429 stream_full`. `read` lists records of one partition from `--from` (an offset, `earliest`, `latest`) or `--from-time`; `--partition` is required when the topic has more than one (`422 validation_failed`) and is sent only when given. A page that does not reach the end prints `(more records: --from N)`.

```console
$ 21ci topic create orders --partitions 4 --retention 7d
$ 21ci topic append orders --body '{"id":1}' --key cust-1
Appended to orders: partition 2, offset 0.
$ 21ci topic read orders --partition 2 --from earliest
PARTITION  OFFSET  KEY     TIMESTAMP                 BODY
2          0       cust-1  2026-10-02T06:15:53.026Z  {"id":1}
```

### consumer

```bash
21ci consumer list  <svc>
21ci consumer reset <svc> <topic> <group> --to earliest|latest|<offset>|<RFC3339> [--partition N]
21ci consumer dlq list  <svc> <topic> <group> [--limit N]
21ci consumer dlq purge <svc> <topic> <group>
```

The stream consumers of a service (its `events.consume` entries): one row per partition with the committed offset, the next offset, the lag, the state (`active`, or `blocked` for `on_exhausted: "block"`) and the dead-letter count. `reset` moves the cursor (all partitions, or `--partition`) and unblocks a blocked partition; an offset is sent as a number. `dlq list` shows the records that ran out of attempts, `dlq purge` deletes them.

```console
$ 21ci consumer list billing
TOPIC   GROUP    PARTITION  COMMITTED  NEXT  LAG  STATE    DEAD
orders  default  0          10         12    2    active   0
$ 21ci consumer reset billing orders default --to earliest
```

### pubsub

```bash
21ci pubsub publish <subject> --body JSON [--header NAME=VALUE]... [--project P] [--env E]
```

Publishes one message to the subscribers of a subject in a project environment (`std:pubsub`; services subscribe with `events.subscribe`). Delivery is at most once and ephemeral: nothing is stored. Prints `receivers N, dropped M` (dropped: over the fan-out cap, the subscriber's backlog, quota or a refused task). A publish over the tenant's rate or quota is `429 rate_limited` / `403 quota_exceeded`.

### invocation

```bash
21ci invocation list    <svc> [--state pending|succeeded|failed] [--limit N] [--cursor C] [--all]
21ci invocation get     <svc> <inv_id>
21ci invocation redrive <svc>
21ci invocation purge   <svc>
```

The async invocations of a service (from `invoke --async` or any `Prefer: respond-async` / `X-Rt-Invocation-Type: Event` request). `list` prints `pending N, failed M` (the service's totals) above the page; the request column shows the method and path, with `?…` when there was a query (the query, headers and body are never shown). `redrive` moves every failed invocation back to pending with fresh attempts; `purge` deletes the failed ones.

```console
$ 21ci invocation list hook --state failed
pending 0, failed 1
ID                              STATE   REQUEST     ATTEMPTS  STATUS  ERROR       CREATED
inv_01M3XM4ZQ8JX2T4V3K9D7R5E1A  failed  POST /run   3         500     status_5xx  2026-10-02T06:15:53Z
$ 21ci invocation redrive hook
Redrove 1 failed invocation of hook (svc_01M3XM1AMYAKX6EREP6J536ATG).
```

### node (operator)

```bash
21ci node status
21ci node scheduler
21ci node cache
21ci node drain
21ci node config get [SECTION | KEY]
21ci node config set KEY=VALUE [KEY=VALUE …]
```

```console
$ 21ci node status
Version:          0.1.0
Mode:             normal
Default profile:  balanced
V8 sandbox:       false
Processors:       10
Kinds:            deno=8 wasm=2 build=1
In flight:        0
Memory:           {"budget_bytes":268435456,"pressure":"Normal"}

$ 21ci node scheduler
Uptime:         1h 2m
Mode:           normal
In flight:      7
Tasks:          120 submitted, 113 completed
Global queues:  deno=5 wasm=0 build=1
Local queues:   2
Waiting:        runtime 1, memory 0
Processors:     2 (idle 1, spinning 0)
Machines:       2 (parked 1, spinning 0, bound tasks 2)
Admission:      queued 2 (max 1024), max in flight 256

P  KIND  STATUS   LRQ  M  BUSY_MS  STEALS_IN  STEALS_OUT  PREEMPTIONS
0  deno  running  2    3  1500     1          0           4
1  wasm  idle     0    -  0        0          1           0

SERVICE  KIND  LIMIT        RUNNING  QUEUED  WAIT_MS  TIMEOUTS  WARM/P
hello    deno  8 (1..64)    3        2       12.5     1         2
edge     wasm  4 (1..16)    0        0       0.0      0         0
```

`scheduler` summarises the scheduler snapshot (`GET /v1/node/scheduler`, the schedtrace JSON): global and per-processor run queues, tasks waiting for a runtime artifact or for memory, machines (OS threads) and the tasks bound to them, the node-wide admission bounds, and one row per service with admission state (adaptive limit and bounds, running, queued, smoothed queue wait, queue timeouts, warm instances per processor; see [stats](#stats)); `--json` prints the raw JSON (`admission`, `services` with `service_id`, `tenant_id`, `slug`). The same figures are Prometheus metrics on the metrics listener (`rt_service_concurrency_limit`, `rt_service_running`, `rt_service_queued`, `rt_admission_queue_wait_seconds`). `cache` prints the cache snapshot JSON (tiers, hit rates) in both modes. `drain` puts the node into drain mode: it stops taking new work (`POST /v1/node/drain`).

`config set` updates live knobs (`PUT /v1/node/config`): `degradation.mode=normal|shed|drain`, `default_profile=max_density|balanced|min_latency`, `rate_limit.rps=N`, `rate_limit.burst=N` (or `rate_limit='{"rps":100,"burst":200}'`). Keys the node can only apply at restart come back as pending.

`config get` reads the configuration the node runs with (`GET /v1/node/config`) as `KEY VALUE` lines with dotted keys; give a section (`sched`) or a key (`sched.max_queued`, printed bare) to narrow it. Node secrets (root token, master keys) are never part of it; `[[binding]].inject_headers` values other than `env:NAME` and user info in URLs come back as `<redacted>`. `--json` prints the JSON of what was selected.

```console
$ 21ci node config set degradation.mode=shed rate_limit.rps=100
Applied: degradation_mode
Pending restart: rate_limit

$ 21ci node config get sched
KEY            VALUE
max_inflight   10000
max_queued     0
max_queued_mb  0
…
$ 21ci node config get hosts.pending_ttl
7days
```

### openapi, docs

```bash
21ci openapi [--out openapi.json]     # GET /v1/openapi.json, pretty-printed
21ci docs                             # prints http://127.0.0.1:8081/docs (the Scalar UI)
```

`21ci openapi --out openapi/openapi.json` writes the live document to a file (the repository's committed copy is generated with `make openapi`).

## Scopes

A tenant key needs these scopes ([SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#scopes)); operator-only commands need the root token. A command that reads before it writes needs both scopes.

| Commands | Scope |
|---|---|
| `tenant *`, `key *`, `plan *`, `node *` | root token (operator) |
| `token *` | `keys:write` (the keys of your own tenant, never more than your own scopes) |
| `mcp` | a tenant key (the scopes of the key decide which tools the assistant gets) |
| `project list`, `project get` | `services:read` |
| `project create`, `project update`, `project delete` | `services:write`, with a key with no project or environment restriction |
| `limits` | any tenant key |
| `tenant usage` of the key's own tenant | `logs:read` |
| `whoami`, `version --server` | any valid token |
| `service list`, `service get`, `versions`, `source`, `env get`, `secret list`, `host list`, `host get`, `schedule list`, `queue stats`, `queue dlq` (list), `events`, `topic list`, `topic get`, `topic read`, `consumer list`, `consumer dlq list`, `invocation list`, `invocation get`; the slug lookup of every command | `services:read` |
| `service create`, `service update`, `service delete`, `host attach`, `host detach`, `schedule add`, `schedule update`, `schedule rm`, `topic create`, `topic update`, `topic delete` | `services:write` (`service update --env` also needs a key with no project or environment restriction) |
| `push`, `rollback`, `rebuild` | `deploy` (+ `services:read` for the lookup; `services:write` for `--create`) |
| `env set`, `env unset` | `env:write` (+ `services:read`) |
| `secret set`, `secret delete` | `secrets:write` |
| `queue send`, `queue dlq --redrive`, `queue dlq --purge`, `topic append`, `pubsub publish`, `consumer reset`, `consumer dlq purge`, `invocation redrive`, `invocation purge` | `queue:write` |
| `logs`, `build-logs`, `stats`, `service stats` | `logs:read` |
| `data export`, `objects rm` | `data:admin` |
| `sockets ls`, `objects ls` | `services:read` |
| `sockets close` | `sockets:write` |
| `invoke` | none for `access=public` services; a key with `invoke` for `access=key` |
| `doctor`, `openapi`, `docs` | none (`doctor` reports what the token can do; its version check uses `GET /v1/version`, any valid token) |

## CI and scripting

Everything can come from the environment, so a CI job needs no config file:

```bash
export RT_API=https://rt.example.com:8081
export RT_INVOKE=https://fn.example.com
export RT_KEY="$DEPLOY_KEY"             # rtk_… with deploy + services:read (+ services:write for --create)
export RT_RETRY=3                       # ride out short busy periods (429/503 with Retry-After)

21ci doctor                                                         # fails the job early on bad config or token
21ci push ./service --service payments --wait --wait-timeout 5m     # fails the job if the build fails
21ci invoke payments /healthz --fail --body-only                    # smoke test, fails on >= 400
```

Tips:

- Use `--json` plus `jq` in scripts; the `error` object goes to stderr (below), stdout stays parseable.
- `21ci push … --wait` exits 1 when the build fails or does not finish in `--wait-timeout`, so a deploy step needs nothing else. Without `--wait` a lazy build only happens on the first request, so build-time failures (for example an `npm:` import that cannot be resolved) show up there; `--wait` (or `--compile eager`) surfaces them at deploy time. Syntax errors are rejected at upload with `422` either way. `21ci build-logs SVC N` prints the build log.
- Roll back from a job with `21ci rollback payments` (previous version) or `--to N`.
- Keep the token in a secret store and in `RT_KEY`; `set-context --token` puts it on the command line (visible in `ps` and shell history).
- `21ci logs SVC --since 10m --level error --json | jq -r .items[].msg` collects recent errors; `logs -f` is for terminals (it ends when the server closes the stream).
- Exit code 4 means "busy, try again later": retry the step (or use `--retry`) rather than failing a deploy outright.
- DNS automation: `21ci host attach SVC HOST --json | jq -r .dns_record.value` gives the TXT value to publish at `.dns_record.name`.
- Idempotence: `push` always creates a new version (the server dedups identical sources by digest but still numbers a new version). `service create` of an existing slug is `409 slug_taken`, so use `push --create` for "create if missing". `service update`, `tenant update` and `schedule update` only send what you give.

## Exit codes and errors

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | the API answered with an error, or the command failed after talking to it (build failed, `--wait` timed out, nothing to roll back to, service or host not found, a `doctor` check failed, `invoke --fail` got a status >= 400) |
| 2 | usage error: bad flags or arguments, invalid input (slug, host, JSON, file, block field), unknown context, unreadable config; nothing was sent |
| 3 | network error: the server could not be reached or timed out |
| 4 | busy: the server answered `429` or `503` **with** a `Retry-After` (rate limit, quota, `queue_timeout`, `overloaded`, `route_stale`, too many log streams), after any `--retry` attempts; try again later |

A control API error is printed from its envelope `{"error": {"code", "message", "details"}}`; non-empty `details` follow as indented JSON, and a `Retry-After` is shown with the status:

```console
$ 21ci service create hello
error: slug_taken: slug already in use (HTTP 409)

$ 21ci push broken.ts --service hello
error: validation_failed: the upload failed validation (HTTP 422)
  {
    "diagnostics": [
      {
        "file": "broken.ts",
        "line": 1,
        "col": 17,
        "message": "Expression expected"
      }
    ]
  }

$ 21ci logs hello -f
error: rate_limited: too many concurrent streams for this key (HTTP 429, retry after 1s)

$ 21ci node status        # nothing listens there
error: cannot reach http://127.0.0.1:8081: Connection refused (os error 111)
```

With `--json` the error goes to stderr as one line, `{"error": {"code", "message", "details"?, "status"?, "retry_after"?}}` (`code` is `usage`, `network_error` or `failed` for errors the CLI raises itself). A non-JSON error body (for example from a proxy) becomes `code: http_<status>`, or the `x-rt-reason` header when the answer carries one.

## Busy answers and `--retry`

The node answers `429` or `503` with a `Retry-After` (seconds) when it wants you to come back: plan rate limits and daily quotas (`429 rate_limited`, `quota_exceeded`, also failed-auth limits), a full log-stream budget, a request that waited its whole queue bound for a slot (`503 queue_timeout`), a full per-service queue (`503 overloaded`), memory pressure, shedding, or a routing change being applied (`503 route_stale`, `Retry-After: 1`). A `503` without `Retry-After` (`control_disabled`, `draining`, `restarting`) is not retried: waiting will not help there.

- Without `--retry` the CLI reports the answer at once, with the wait: `error: queue_timeout: … (HTTP 503, retry after 2s)`, and exits 4.
- `--retry N` (or `RT_RETRY=N`) sleeps for the `Retry-After` and sends the same request again, at most N times, noting each attempt on stderr: `HTTP 503 overloaded: retrying in 1s (attempt 1 of 3)`. A `Retry-After` above 60 s (a daily quota, say) is reported at once instead of sleeping. It applies to every control API request and to `invoke`.

## Control API coverage

Every operation of `openapi/openapi.json` and the command that calls it.

| Operation | Command |
|---|---|
| `GET /v1/node/status` | `node status`, `doctor`, `version --server` (older nodes) |
| `GET`, `PUT /v1/node/config` | `node config get`, `node config set` |
| `POST /v1/node/drain` | `node drain` |
| `GET /v1/node/scheduler` | `node scheduler` |
| `GET /v1/node/cache` | `node cache` |
| `GET /v1/version` | `version --server`, `doctor` (version check of a tenant key) |
| `GET /v1/me` | `whoami`, `tenant usage` without an id |
| `GET /v1/limits` | `limits` |
| `GET /v1/plans` | `plan list`, `plan get` |
| `POST /v1/tenants`, `GET /v1/tenants` | `tenant create`, `tenant list` |
| `GET`, `PATCH`, `DELETE /v1/tenants/{t}` | `tenant get`, `tenant update`, `tenant delete` |
| `GET /v1/tenants/{t}/usage` | `tenant usage` |
| `POST`, `GET /v1/tenants/{t}/keys`, `DELETE …/keys/{key}` | `key create`, `key list`, `key revoke` |
| `POST`, `GET /v1/keys`, `DELETE /v1/keys/{key}` | `token create`, `token list`, `token revoke` |
| `POST`, `GET /v1/projects`, `GET`, `PATCH`, `DELETE /v1/projects/{p}` | `project create`, `project list`, `project get`, `project update`, `project delete` |
| `GET /v1/projects/{p}/environments` | `project update --add-env` / `--remove-env` (reads the current list) |
| `POST /v1/services`, `GET /v1/services` | `service create` (`push --create`), `service list` (`?slug=` is every slug lookup; `--project`, `--env` filter) |
| `GET`, `PATCH`, `DELETE /v1/services/{s}` | `service get`, `service update`, `service delete` |
| `PUT /v1/services/{s}/live` | `rollback` |
| `POST`, `GET /v1/services/{s}/versions` | `push`, `versions` |
| `GET …/versions/{v}` | `versions <svc> N`, `push --wait` |
| `GET …/versions/{v}/source` | `source` |
| `POST …/versions/{v}/rebuild` | `rebuild` |
| `GET …/versions/{v}/build/logs` | `build-logs` |
| `GET …/types` | `types` |
| `POST`, `GET /v1/sdks`, `GET /v1/sdks/{name}/releases`, `DELETE /v1/sdks/{name}` | `sdk publish`, `sdk ls` (operator), `sdk releases`, `sdk rm` |
| `PUT /v1/sdks/{name}/channels/{channel}` | `sdk promote`, `sdk rollback` |
| `GET`, `PUT /v1/sdk-grants` | `sdk grants get`, `sdk grants set` |
| `GET /v1/sdks/effective`, `GET /v1/sdks/{name}/contract` | `sdk ls` (tenant key), `sdk types` |
| `GET`, `PUT …/env` | `env get`, `env set`, `env unset` |
| `GET …/secrets`, `PUT`, `DELETE …/secrets/{name}` | `secret list`, `secret set`, `secret delete` |
| `GET …/hosts`, `GET`, `PUT`, `DELETE …/hosts/{host}` | `host list`; `host get`; `host attach`; `host detach` |
| `GET`, `POST …/schedules`, `PATCH`, `DELETE …/schedules/{id}` | `schedule list`, `schedule add`, `schedule update`, `schedule rm` |
| `POST`, `GET …/queue` | `queue send`, `queue stats` |
| `GET`, `DELETE …/dlq`, `POST …/dlq/redrive` | `queue dlq`, `queue dlq --purge`, `queue dlq --redrive` |
| `POST`, `GET /v1/topics`, `GET`, `PATCH`, `DELETE /v1/topics/{t}` | `topic create`, `topic list`, `topic get`, `topic update`, `topic delete` |
| `POST`, `GET /v1/topics/{t}/messages` | `topic append`, `topic read` |
| `POST /v1/pubsub/publish` | `pubsub publish` |
| `GET …/consumers`, `POST …/consumers/{topic}/{group}/reset` | `consumer list`, `consumer reset` |
| `GET`, `DELETE …/consumers/{topic}/{group}/dlq` | `consumer dlq list`, `consumer dlq purge` |
| `GET`, `DELETE …/invocations`, `GET …/invocations/{id}`, `POST …/invocations/redrive` | `invocation list`, `invocation purge`, `invocation get`, `invocation redrive` |
| `GET …/logs` (JSON and SSE) | `logs`, `logs -f` |
| `GET …/stats` | `stats`, `service stats` |
| `GET …/events` | `events` |
| `GET …/data/export` | `data export` |
| `GET …/objects`, `DELETE …/objects/{name}` | `objects ls`, `objects rm` |
| `GET`, `DELETE …/objects/{name}/sockets`, `DELETE …/sockets/{ws}` | `sockets ls`, `sockets close --tag`, `sockets close <ws>` |
| `GET /v1/openapi.json`, `GET /docs` | `openapi`, `docs`, `doctor` |
| `ANY /s/{slug}/…` (public listener) | `invoke`, `invoke --async` |

`POST /mcp` is the endpoint `mcp` bridges to stdio ([mcp.md](mcp.md)).

Not covered: `/metrics`, `/healthz`, `/readyz` on the metrics listener (scrape or probe them directly), and node-local commands of the `rt` binary such as `rt secrets reseal`.

## Versus `sample/scripts/deploy.sh`

`21ci push` follows `deploy.sh` (same artifact formats, same slug cleanup, same `?activate=1&compile=`), with these differences: the service is only created with `--create` (the script always creates it); the lookup uses `GET /v1/services?slug=` instead of paging through the list; `--wait` waits for the build and implies `--compile eager`; a directory with a single source file and no `rt.json` is uploaded as that file whatever its name; and there is no `jq`/`curl` dependency. `invoke.sh` maps to `21ci invoke` (`--fail` is its exit-22 behaviour).
