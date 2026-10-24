---
title: Get started
description: The shortest path from a checkout of the runtime repository to a deployed, working service on your own node.
---

# Get started

This section takes you from nothing to a deployed service in three steps. You play both roles: the **operator** who runs
the node, and the **developer** (tenant) who deploys code to it.

1. [Install](install.md): start a node with Docker Compose (or from source) and install the `21ci` CLI.
2. [First run](first-run.md): the two node secrets, then a tenant and a scoped API key.
3. [Your first service](first-service.md): deploy a Deno service and a WASM component, call them, read the logs.

Then use the task guides: [Get access](../guides/access.md), [Deploy and roll back](../guides/deploy.md),
[Configure a service](../guides/configure.md) and [Logs, stats and usage](../guides/logs-and-usage.md).

## The short version

With Docker, a checkout of the runtime repository, `curl` and `jq`, this is the whole path. The first image build
downloads V8 and compiles the workspace, which takes 20 minutes or more. Each step is explained on the following pages.

```bash
# 1. secrets for the node (the .env.example placeholders are rejected on purpose)
cp .env.example .env
sed -i.bak "s/^RT_ROOT_TOKEN=.*/RT_ROOT_TOKEN=$(openssl rand -hex 32)/; s/^RT_MASTER_KEY=.*/RT_MASTER_KEY=$(openssl rand -hex 32)/" .env && rm .env.bak

# 2. build and start
docker compose up -d --build
until curl -fsS http://127.0.0.1:9090/readyz; do sleep 1; done

# 3. a tenant and a key for it, as the operator
set -a; . ./.env; set +a
export RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080
TENANT=$(curl -sS -X POST "$RT_API/v1/tenants" -H "Authorization: Bearer $RT_ROOT_TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"demo","plan":"free"}' | jq -r .id)
export RT_KEY=$(curl -sS -X POST "$RT_API/v1/tenants/$TENANT/keys" -H "Authorization: Bearer $RT_ROOT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"scopes":["services:read","services:write","deploy","logs:read"]}' | jq -r .secret)

# 4. deploy a sample as the tenant, then call it
sample/scripts/deploy.sh sample/deno/hello hello
curl "$RT_INVOKE/s/hello/?name=Ada"
```

The response is JSON with a greeting and the task id. The first request is slower because the version is compiled on its
first use (builds are lazy by default; `21ci push --wait` compiles at deploy time).

## Terms

| Term | Meaning |
|---|---|
| Operator | Whoever runs the node: owns `rt.toml`, the root token and the master key, defines plans, creates tenants |
| Tenant | A customer of the operator: owns services and API keys, limited by a plan |
| Service | A named deployable with a URL slug (`hello`), a runtime (`deno` or `wasm`) and a live version |
| Version | An immutable upload of a service's code, numbered 1, 2, 3, and so on. Exactly one version is live |
| Task | One execution of a handler, started by an HTTP request, a schedule, a queue message or a call from another service |

The [concepts overview](../concepts/index.md) has the full glossary.

## Before you start

| You need | For |
|---|---|
| Linux (x86_64 or arm64) with Docker, Compose v2 and BuildKit | Running a node. macOS works for development builds (`make dev`) |
| A checkout of the runtime repository | The Dockerfile, compose file, samples and the CLI source |
| `curl` and `jq` | The examples on these pages and the `sample/scripts/*.sh` helpers |
| Rust 1.95 with the `wasm32-wasip2` target | Building `21ci` and the node from source, and WASM services in Rust |

There are no published binaries or images yet: you build from the repository. A tenant who only deploys to someone else's
node needs just a key and the two URLs, plus `21ci` or `curl`; start at [Get access](../guides/access.md).

## See also

- [Install](install.md), [First run](first-run.md), [Your first service](first-service.md).
- [Self-hosting](../guides/self-hosting.md) and [Production deployment](../guides/production.md).
- [CLI reference](../cli.md) and [API reference](../api.md).
