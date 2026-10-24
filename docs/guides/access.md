---
title: Get access
description: How a developer gets into a node - the operator creates a tenant and a scoped key, the developer configures 21ci with it, verifies it, and rotates or revokes it.
---

# Get access

To deploy to a node you need a **tenant** (the account your services belong to) and a **tenant key** (the credential
that acts for it). The operator creates the tenant and its first key; you configure `21ci` with the key, check that it
works, and, with the `keys:write` scope, create narrower tokens yourself for CI jobs, colleagues and AI assistants. This
guide has two halves: what the operator does, and what you do. On a node you run yourself you do both.

| | Root token | Tenant key |
|---|---|---|
| Looks like | Any string the operator chose (`RT_ROOT_TOKEN`) | `rtk_<key_id>_<secret>` |
| Belongs to | The operator | One tenant |
| Can do | Every route, including `/v1/tenants*` and `/v1/node/*` | What its scopes allow, on its own tenant's services |
| Use it for | Creating tenants and keys, support work | Everything a developer or a CI job does |

Both are sent as `Authorization: Bearer <credential>` to the **control API** (an API key also works as `x-rt-key: <key>`; the root token does not)
(`RT_API`, port 8081 by default). The invoke listener (`RT_INVOKE`, port 8080) needs a credential only for services with
`access: key`. See [Tenants, plans and keys](../concepts/tenants-and-plans.md) for the model behind this.

## Operator: create a tenant and a key

Use the root token. All examples assume `RT_API` and `RT_ROOT_TOKEN` are set (or a `21ci` context with the root token,
see [First run](../getting-started/first-run.md)).

### 1. Create the tenant

=== "21ci"

    ```console
    $ 21ci plan list
    NAME  SERVICES  CONCURRENCY  RPS        INVOCATIONS/DAY  STORAGE_MB  HOST_SUFFIXES
    free  10        10           50         100000           100         -
    pro   1000      unlimited    unlimited  unlimited        1024        .bots.example.com

    $ 21ci tenant create acme --plan pro
    ID:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    Name:      acme
    Plan:      pro
    Disabled:  false
    Created:   2026-10-02T06:15:06Z
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/plans"

    TENANT=$(curl -sS -X POST "$RT_API/v1/tenants" \
      -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"acme","plan":"pro"}' | jq -r .id)
    ```

The plan must exist in the node configuration (`free` always does) or the call is `422 validation_failed`. The plan
sets the tenant's quotas and the maximum for per-service limits
([Configure a service](configure.md#limits-and-plan-maxima)).

### 2. Create a key

=== "21ci"

    ```console
    $ 21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name laptop-ada
    ID:        key_01M3XM1AG5HN6WY9S6JQ550JQV
    Name:      laptop-ada
    Scopes:    services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write
    Services:  all
    Expires:   never
    Created:   2026-10-02T06:15:14Z
    Secret:    rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
    The secret is shown only once: store it now.
    ```

    Without `--scopes`, `21ci` grants every scope except `data:admin`.

=== "curl"

    ```bash
    curl -sS -X POST "$RT_API/v1/tenants/$TENANT/keys" \
      -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"laptop-ada","scopes":["services:read","services:write","deploy","invoke","logs:read","env:write","secrets:write","queue:write"]}'
    ```

    The response has the key's fields and, once, `"secret": "rtk_…"`. `scopes` is required and must not be empty.

The node stores only a keyed hash of the secret. If the secret is lost, create another key; there is no way to show it
again.

### Scopes

| Scope | Allows | Typical command |
|---|---|---|
| `services:read` | Read services, versions, env names, secret names, hosts, schedules, queue stats, events. Also needed to look a service up by slug | `service list`, `versions`, `events` |
| `services:write` | Create, change and delete services, hosts and schedules | `service create`, `service update`, `host attach` |
| `deploy` | Upload versions, rebuild, change the live version | `push`, `rebuild`, `rollback` |
| `invoke` | Call services whose `access` is `key` | `invoke --key` |
| `logs:read` | Logs, build logs, stats, the tenant's own usage | `logs`, `stats`, `build-logs`, `tenant usage` |
| `env:write` | Replace env variables | `env set` |
| `secrets:write` | Set and delete secrets | `secret set` |
| `queue:write` | Enqueue, redrive and purge the dead-letter queue | `queue send`, `queue dlq --redrive` |
| `data:admin` | Export a service's SQLite data | `data export` |
| `keys:write` | Create, list and revoke keys of the own tenant (`/v1/keys`), never with more than the creating key holds | `token create`, `token list`, `token revoke` |

A command that reads before it writes needs both scopes: `push` needs `deploy` and `services:read` (the slug lookup), and
`services:write` with `--create`. [The CLI reference](../cli.md#scopes) lists every command.

Give each job the smallest set that works:

| Key for | `--scopes` |
|---|---|
| A developer's laptop | the default (everything but `data:admin`) |
| CI that deploys one existing service | `deploy,services:read` |
| CI that also creates services | `deploy,services:read,services:write` |
| A dashboard or on-call reader | `services:read,logs:read` |
| A client of an `access: key` service | `invoke` |
| A team lead who hands out tokens | the default plus `keys:write` |
| An AI assistant over [MCP](../mcp.md) | `services:read,deploy,logs:read`, plus `services:write` to create services |

### Restrict a key to some projects and environments, or set an expiry

```bash
21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name ci-shop \
  --scopes deploy,services:read --project prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ --environment production \
  --expires-at 2027-01-01T00:00:00Z
```

```bash
curl -sS -X POST "$RT_API/v1/tenants/$TENANT/keys" \
  -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"ci-shop","scopes":["deploy","services:read"],"project_ids":["prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ"],"environments":["production"],"expires_at":"2027-01-01T00:00:00Z"}'
```

- `--project` takes project **ids** (`prj_…`) of the tenant, comma-separated. `--environment` takes environment labels.
  Both default to all. Without `--project`, the key covers every project, including ones created later; an explicit
  list does not grow, so a project created later is not admitted.
- A service is reachable with the key only when its project **and** its environment are both admitted. Every other
  service of the tenant is `404` to it.
- A restricted key cannot change a service's `environment` (`403 insufficient_scope`), because that decides which
  services may call it. It also cannot create, change or delete projects (`403`).
- `--expires-at` is an RFC 3339 time in the future. After it the key answers `401 unauthorized`.

Use `21ci key list --tenant ten_…` to review keys (ID, NAME, SCOPES, PROJECTS, ENVIRONMENTS, EXPIRES, LAST_USED; `all`
when unrestricted, `none` for an empty list). It never shows secrets.

### 3. Hand the key over

Send the developer three things, through a channel that is not a chat log or an email thread (a password manager's
share feature, or your secret store):

- the **key** (`rtk_…`);
- the **control URL** (`RT_API`);
- the **invoke URL** (`RT_INVOKE`), and the base domain if the node has one.

Nobody needs the tenant id to use a key: `21ci whoami` shows it.

To let the tenant hand out its own tokens from then on, include `keys:write` in that first key (`--scopes` with the
default set plus `keys:write`). See [Create your own tokens](#create-your-own-tokens).

### Act as a tenant with the root token

Services belong to a tenant, so the root token alone cannot create one (`422`, "x-rt-tenant header is required").
To act **for** a tenant, add its id. This is the support and break-glass path; day-to-day work should use a tenant key,
which cannot touch other tenants or the node.

=== "21ci"

    ```bash
    21ci --as-tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ service list
    export RT_TENANT=ten_01M3XM135SCP9EP41ZZXAQ5CCZ           # the same, for a whole shell
    21ci config set-context acme --api "$RT_API" --token "$RT_ROOT_TOKEN" --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" -H "x-rt-tenant: ten_01M3XM135SCP9EP41ZZXAQ5CCZ" \
      "$RT_API/v1/services"
    ```

An unknown tenant id is `404`. A tenant key ignores `x-rt-tenant`: it always acts for its own tenant. Changes made this
way are audited with the operator as the actor.

To stop a tenant at once, `21ci tenant update ten_… --disable`: its services leave the routing table and its keys stop
authenticating. `--enable` undoes it.

## Developer: configure `21ci`

A `21ci` **context** names one node: control URL, invoke URL and token. Contexts live in `~/.config/21ci/config.toml`
(mode 0600, because it holds tokens).

```bash
21ci config set-context demo \
  --api http://127.0.0.1:8081 --invoke http://127.0.0.1:8080 --token "$RT_KEY"
21ci config use-context demo
21ci config get-contexts
```

```console
CURRENT  NAME  API                    INVOKE                 TENANT  TOKEN
*        demo  http://127.0.0.1:8081  http://127.0.0.1:8080  -       yes
```

`set-context` changes only the fields you pass. Keep one context per node (`staging`, `prod`) and pick one for a single
command with `21ci --context prod service list`, or for a shell with `RT_CONTEXT`.

**Environment variables win over the context**, so a CI job needs no config file:

| Setting | Resolved from (first wins) |
|---|---|
| Control API URL | `RT_API`, context `api`, `http://127.0.0.1:8081` |
| Invoke URL | `RT_INVOKE`, context `invoke`, `http://127.0.0.1:8080` |
| Token | `RT_KEY`, `RT_ROOT_TOKEN`, context `token` |
| Tenant to act for (operator only) | `--as-tenant` / `RT_TENANT`, context `tenant` |
| Context | `--context` / `RT_CONTEXT`, `current_context` |

`RT_KEY` is read before `RT_ROOT_TOKEN`, and both beat the context's token. So a shell that still exports
`RT_ROOT_TOKEN` (an operator's machine, after sourcing the node's `.env`) acts as the operator whatever context is
current, and `21ci whoami` says so. `unset RT_ROOT_TOKEN` to use a tenant context. A trailing `/` on a URL is ignored.

!!! warning "Keep the token off the command line"
    `set-context --token …` puts the key in your shell history and in `ps`. Prefer `export RT_KEY=…` from a secret
    store or a file that is not in version control. The `curl` examples in these docs use the same variables.

### Check that it works

=== "21ci"

    ```console
    $ 21ci doctor
    CHECK            STATUS  DETAIL
    config           ok      context demo; /home/me/.config/21ci/config.toml
    credentials      ok      tenant key (rtk_…)
    control api      ok      http://127.0.0.1:8081 answers in 3 ms (API 1.0.0)
    auth             ok      tenant key: services:read allowed
    version          ok      client 0.1.0, node 0.1.0
    invoke listener  ok      http://127.0.0.1:8080 answers in 2 ms (HTTP 404)

    $ 21ci whoami
    Principal:     tenant
    Tenant:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    Key:           key_01M3XM1AG5HN6WY9S6JQ550JQV
    Scopes:        services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write
    Services:      all
    Plan:          pro
    Node version:  0.1.0
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/me" | jq
    ```

    ```json
    {
      "principal": "tenant",
      "tenant_id": "ten_01M3XM135SCP9EP41ZZXAQ5CCZ",
      "key_id": "key_01M3XM1AG5HN6WY9S6JQ550JQV",
      "scopes": ["services:read", "services:write", "deploy", "invoke", "logs:read", "env:write", "secrets:write", "queue:write"],
      "project_ids": null,
      "environments": null,
      "plan": { "name": "pro", "settings": { "…": "the plan with the tenant's overrides applied" } },
      "version": "0.1.0"
    }
    ```

`21ci doctor` checks the context, the credential type, both listeners, the key's `services:read` and the client/node
versions, and exits 1 when a check fails (warnings do not fail). `/v1/me` accepts any valid credential, so it is the
cheapest "is this key alive" call. `21ci whoami --json` adds the plan's effective settings; `project_ids` and
`environments` are `null` for an unrestricted key.

### What 401 and 403 mean

| You see | Meaning | What to do |
|---|---|---|
| `401 unauthorized` | No credential, a malformed or wrong key, a key that expired or was revoked, or a tenant that is disabled. On the invoke path: an `access: key` service called without a key that has `invoke` | Check `RT_KEY` (a stray `RT_KEY` in the shell beats your context). Ask the operator for a new key |
| `403 insufficient_scope` | The key is valid but lacks the scope the route needs, the route is operator-only (`/v1/tenants*`, `/v1/node/*`), a restricted key tried to change `environment` or a project, or a new token would hold more than its creator (`details.reason: "escalation"`) | `21ci whoami` shows the scopes; ask for a key with the missing one |
| `404 not_found` | The id or slug does not exist, belongs to another tenant, or is not on a restricted key's list. Nothing reveals which | Check the slug; `21ci whoami` shows `Projects:` and `Environments:` |
| `422` "x-rt-tenant header is required" | You used the root token without naming a tenant | Add `--as-tenant ten_…` or use a tenant key |
| `429 rate_limited` with `Retry-After` | Too many failed authentications from one address (10 per minute), or too many open log streams on one key | Stop retrying with the wrong key; wait for `Retry-After` |
| `503 control_disabled` | The node was started without a root token | Operator: set `RT_ROOT_TOKEN` and restart |
| `error: cannot reach http://…` (exit 3) | Wrong URL, VPN down, node stopped | `21ci doctor`, check `RT_API` |

The full list is on [Error reasons](../reference/errors.md).

## Create your own tokens

A tenant key with the scope **`keys:write`** manages the keys of its own tenant, without the operator: give a CI job, a
colleague or an AI assistant a token that can do less than yours, with an expiry, and revoke it yourself.

=== "21ci"

    ```console
    $ 21ci token create --name ci-shop --scopes deploy,services:read \
        --project prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ --environment production --expires-in 90d
    ID:        key_01M4Q2GB5APYBGEPYPBX1X4ZYJ
    Name:      ci-shop
    Scopes:    deploy,services:read
    Projects:  prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ
    Environments: production
    Expires:   2027-01-10T04:12:20Z
    Created:   2026-10-12T04:12:20Z
    Secret:    rtk_01M4Q2GB5APYBGEPYPBX1X4ZYJ_…
    The secret is shown only once: store it now.

    $ 21ci token list
    $ 21ci token revoke key_01M4Q2GB5APYBGEPYPBX1X4ZYJ
    ```

    `--scopes` is required: there is no default, say what the token may do. `--expires-in` takes a duration (`24h`,
    `7d`); `--expires-at` an RFC 3339 time.

=== "curl"

    ```bash
    curl -sS -X POST "$RT_API/v1/keys" \
      -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"name":"ci-shop","scopes":["deploy","services:read"],"project_ids":["prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ"],"environments":["production"],"expires_at":"2027-01-10T00:00:00Z"}'
    # 201 {"id":"key_…","name":"ci-shop","scopes":[…],"project_ids":[…],"environments":[…],"expires_at":…,"secret":"rtk_…"}

    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/keys"                 # live keys, never a secret
    curl -sS -X DELETE -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/keys/key_01M4Q2GB5APYBGEPYPBX1X4ZYJ"   # 204
    ```

A new token never holds more than the key that creates it:

| Part | Rule | Asking for more |
|---|---|---|
| Scopes | A subset of your scopes; `keys:write` itself may be passed on | `403 insufficient_scope`, `details.reason: "escalation"` |
| Projects and environments | Within your `project_ids` and `environments`. Asking for none gives your own list, never every project or environment | the same |
| Lifetime | Not later than your own `expires_at`. Asking for none gives your own expiry | the same |

- A key with a project or environment restriction lists and revokes only keys whose lists are within its own; other
  keys behave as not found (`404`).
- A tenant holds at most 200 live keys created this way (`403 quota_exceeded`); revoking one makes room.
- Revocation is effective on the next request. Revoked, unknown and out-of-reach ids are `404`.
- Creating and revoking are audited (`key.created`, `key.deleted`) with the acting key and `self_service: true`; the
  secret is never in the event.
- The operator reaches the same routes with the root token and `x-rt-tenant`; `21ci key …` remains the operator's
  command for any tenant.

## Rotate or revoke a key

Rotation is create, switch, revoke, so nothing breaks in between:

1. Create a new key with the same scopes: `21ci token create --scopes … --name laptop-ada-2` with a `keys:write` key, or
   the operator's `21ci key create --tenant ten_… --name laptop-ada-2`.
2. Switch every user of the old key to the new one (laptop context, CI secret).
3. Check that the old key is idle: `21ci token list` (or the operator's `21ci key list --tenant ten_…`) shows when each
   key was last used.
4. Revoke the old one.

=== "21ci"

    ```bash
    21ci key list   --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    21ci key revoke key_01M3XM1AG5HN6WY9S6JQ550JQV --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/tenants/$TENANT/keys"
    curl -sS -X DELETE -H "Authorization: Bearer $RT_ROOT_TOKEN" "$RT_API/v1/tenants/$TENANT/keys/key_01M3XM1AG5HN6WY9S6JQ550JQV"
    ```

Revocation takes effect on the next request (`204`); a revoked key disappears from the list, and revoking it again is
`404`. If a key leaked, revoke first and rotate afterwards. The old key's secret stays in your git history even after
you delete the file, so treat it as burned.

## Keep keys out of repositories and CI logs

- **Use environment variables**, not files in the repository: `RT_KEY` in your shell profile's secret loader, a CI
  secret, a secret manager. `21ci` needs no config file when `RT_API` and `RT_KEY` are set.
- **One key per job and per person**, with the scopes from the table above, restricted to the services it ships and with
  an expiry. Revoking one then breaks nothing else.
- **Do not echo the key.** `21ci key create` prints the secret once, to stdout; do not run it in a CI log you keep.
  `21ci secret set` reads the secret value from stdin or a file (never from an argument), which keeps it out of `ps`.
- **Find accidents early.** Keys start with `rtk_`, so `git grep -n 'rtk_'` and a secret scanner configured for that
  prefix catch most commits that contain one.
- **Services have their own secrets.** Values a service needs at run time (API tokens for other systems) go into
  `21ci secret set`, not into the key and not into the code. See [Configure a service](configure.md#env-variables-and-secrets).

## See also

- [Deploy and roll back](deploy.md): the next step once `21ci whoami` works.
- [Projects and environments](projects.md): group services before you deploy many of them.
- [AI assistants (MCP)](../mcp.md): give an assistant its own token.
- [Tenants, plans and keys](../concepts/tenants-and-plans.md): plans, quotas and how keys are stored.
- [Security model](../concepts/security.md): isolation between tenants and what a key can reach.
- [CLI reference: contexts and configuration](../cli.md#contexts-and-configuration) and [scopes](../cli.md#scopes).
- [Error reasons](../reference/errors.md): every control API code.
