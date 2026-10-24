---
title: First run
description: The root token and the master key, then creating your first tenant and a scoped API key with 21ci or curl.
---

# First run

A fresh node has no tenants and no services. In this step you act as the **operator**: you learn what the two node
secrets do, then create a tenant and an API key that a developer (also you, for now) uses to deploy code.

## The two node secrets

Root token (`RT_ROOT_TOKEN`)
:   The operator's credential for the control API, sent as `Authorization: Bearer <token>` (the root token is never accepted in `x-rt-key`).
    It can call every route, including the operator-only ones under `/v1/node` and `/v1/tenants`. Any string works;
    use a long random one (`openssl rand -hex 32`).

Master key (`RT_MASTER_KEY`)
:   32 random bytes written as 64 hex characters. It seals tenant secrets with AES-256-GCM and derives the other node keys
    (the pepper for API-key hashes, the integrity key of the compiled-code cache). It never leaves the node.

Both can come from a file instead: `RT_ROOT_TOKEN_FILE` and `RT_MASTER_KEY_FILE` (set the variable or the `_FILE`
variant, not both). If no master key is configured at all, `rt` generates `<data_dir>/master.key` (mode 0600) on first
start.

!!! danger "Back up the master key, separately from the data"
    Without the master key, sealed secrets cannot be opened and existing API keys stop verifying. Store it apart from
    the data volume backups. To change it later, follow the rotation procedure in
    [Production deployment](../guides/production.md); never just swap the value.

## Configure the CLI as operator

A `21ci` *context* names a node: its control URL, invoke URL and token.

```bash
set -a; . ./.env; set +a      # loads RT_ROOT_TOKEN from the compose .env (on a `make dev` node it is dev-root-token)
21ci config set-context local --api http://127.0.0.1:8081 --invoke http://127.0.0.1:8080 --token "$RT_ROOT_TOKEN"
21ci doctor
```

```console
CHECK            STATUS  DETAIL
config           ok      context local; /home/me/.config/21ci/config.toml
credentials      ok      root token (operator)
control api      ok      http://127.0.0.1:8081 answers in 3 ms (API 1.0.0)
auth             ok      operator: node routes allowed
version          ok      client 0.1.0, node 0.1.0
node mode        ok      normal
invoke listener  ok      http://127.0.0.1:8080 answers in 2 ms (HTTP 404)
```

`21ci doctor` checks the context, the token, both listeners and the client/node versions in one go. `21ci node status`
shows the node itself:

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
```

## Create a tenant

A tenant owns services and keys and gets a **plan** that sets its quotas and limit maxima. The built-in plan `free`
always exists; operators add more under `[plans.<name>]` in `rt.toml`
([Tenants, plans and keys](../concepts/tenants-and-plans.md)).

=== "21ci"

    ```console
    $ 21ci tenant create demo
    ID:        ten_01M3XM135SCP9EP41ZZXAQ5CCZ
    Name:      demo
    Plan:      free
    Disabled:  false
    Created:   2026-10-02T06:15:06Z
    ```

=== "curl"

    ```bash
    export RT_API=http://127.0.0.1:8081
    curl -sS -X POST "$RT_API/v1/tenants" \
      -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"demo","plan":"free"}'
    ```

    ```json
    {"id":"ten_01M3XM135SCP9EP41ZZXAQ5CCZ","name":"demo","plan":"free","overrides":{},"created_at":1790921706000,"disabled":false}
    ```

The `free` plan allows 10 services, 50 requests per second, 100,000 invocations per day, 100 MB of storage, and per-task
limits up to 30 s wall time, 1 s CPU and 128 MB of memory. It does not allow `cron` schedules.

## Create an API key for the tenant

Tenant keys look like `rtk_<key_id>_<secret>`. The full secret is returned **once**; the node stores only a hash.

=== "21ci"

    ```console
    $ 21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name laptop
    ID:        key_01M3XM1AG5HN6WY9S6JQ550JQV
    Name:      laptop
    Scopes:    services:read,services:write,deploy,invoke,logs:read,env:write,secrets:write,queue:write
    Services:  all
    Expires:   never
    Created:   2026-10-02T06:15:14Z
    Secret:    rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
    The secret is shown only once: store it now.
    ```

=== "curl"

    ```bash
    curl -sS -X POST "$RT_API/v1/tenants/ten_01M3XM135SCP9EP41ZZXAQ5CCZ/keys" \
      -H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json' \
      -d '{"name":"laptop","scopes":["services:read","services:write","deploy","invoke","logs:read","env:write","secrets:write","queue:write"]}' \
      | jq -r .secret
    ```

Without `--scopes`, `21ci` grants every scope except `data:admin`; with the API, `scopes` is required. The scope table, how to
restrict a key to some projects and environments, expiry, rotation and acting for a tenant with the root token (`--as-tenant`) are in
[Get access](../guides/access.md).

## Switch to the tenant

Services always belong to a tenant, and day-to-day work is safer with a tenant key than with the root token: a key cannot
touch other tenants or the node. Save the key in a second context and make it current:

```bash
21ci config set-context demo --api http://127.0.0.1:8081 --invoke http://127.0.0.1:8080 --token rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
21ci config use-context demo
21ci whoami                   # tenant, key, scopes and plan behind the current token
```

!!! warning "An exported `RT_ROOT_TOKEN` beats the context"
    `21ci` takes the token from `RT_KEY`, then `RT_ROOT_TOKEN`, then the context. If the `set -a; . ./.env` step above left
    `RT_ROOT_TOKEN` in your shell, `21ci whoami` still reports `Principal: operator` after `use-context demo`. Run
    `unset RT_ROOT_TOKEN` first.

Or skip contexts and export the variables, which always win over a context and are what scripts, CI and the `curl`
examples in these docs use:

```bash
export RT_API=http://127.0.0.1:8081      # control listener
export RT_INVOKE=http://127.0.0.1:8080   # public invoke listener
export RT_KEY=rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…
```

## Next steps

You have a tenant and a key. Continue with [Your first service](first-service.md).
