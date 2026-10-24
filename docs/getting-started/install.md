---
title: Install
description: Run a runtime node with Docker Compose or from source, and install the 21ci command line client.
---

# Install

A node is one process, `rt`, with one data directory. The recommended way to run it is the Docker image built from
the runtime repository. For development you can also run it straight from a source checkout. This page covers both,
plus the `21ci` CLI.

!!! info "There are no published binaries or images yet"
    You build the image (or the binary) from a checkout of the runtime repository. All commands on this page run from
    the root of that checkout.

## What a node exposes

| Listener | Default address | Serves | Who should reach it |
|---|---|---|---|
| public | `0.0.0.0:8080` | `ANY /s/{slug}/…`: invoking services, custom hosts | Your users, webhooks (through a reverse proxy with TLS) |
| control | `127.0.0.1:8081` | `/v1/*` control API, `/v1/openapi.json`, `/docs` (Scalar UI) | Operators and developers (VPN, SSH, internal network) |
| metrics | `127.0.0.1:9090` | `/metrics` (Prometheus), `/healthz`, `/readyz` | Your monitoring |
| internal | `unix:/data/rt.sock` | Same routes as public, trusted (`x-rt-invoker`) | Your own platform backend on the same host |

Inside the container all three TCP listeners bind `0.0.0.0`, and Compose publishes them on `127.0.0.1` of the host
only. The control API and metrics have no network ACL of their own: keep them private.

## Option 1: Docker Compose (recommended)

You need Linux (x86_64 or arm64) with Docker, Compose v2 and BuildKit, plus `curl` and `jq` for the examples.

**1. Create the secrets file.** The node needs a root token for the control API and a 32-byte master key that
seals tenant secrets. Generate both:

```bash
cp .env.example .env
sed -i.bak "s/^RT_ROOT_TOKEN=.*/RT_ROOT_TOKEN=$(openssl rand -hex 32)/; s/^RT_MASTER_KEY=.*/RT_MASTER_KEY=$(openssl rand -hex 32)/" .env && rm .env.bak
```

`.env.example` ships `change-me-…` placeholders that are not valid hex on purpose: the node and the helper scripts
refuse them. `.env` is git-ignored. [First run](first-run.md) explains both values.

**2. Build and start.**

```bash
docker compose up -d --build
until curl -fsS http://127.0.0.1:9090/readyz; do sleep 1; done
```

The first build downloads a prebuilt V8 and compiles the whole workspace, which takes 20 minutes or more. Later builds
reuse the BuildKit caches.

**3. Check it.**

```bash
curl -fsS http://127.0.0.1:9090/healthz     # liveness
curl -fsS http://127.0.0.1:9090/readyz      # readiness: migrations applied, not draining
docker compose logs -f rt                   # JSON system logs
```

Open <http://127.0.0.1:8081/docs> for the interactive API reference served by your node.

??? example "The compose file"

    ```yaml title="docker-compose.yml"
    --8<-- "docker-compose.yml"
    ```

What the compose setup gives you:

- Ports published on `127.0.0.1` only: `RT_PUBLIC_PORT`, `RT_CONTROL_PORT` and `RT_METRICS_PORT` change the host
  ports (defaults 8080, 8081, 9090).
- A named volume `rt-data` mounted at `/data`. `docker compose down` keeps it; `docker compose down -v` deletes every
  tenant, service and secret.
- A non-root user (uid 10001), `cap_drop: ALL`, `no-new-privileges`, `nofile` 65536 and a 30 s stop grace period for
  in-flight tasks.
- No `rt.toml`: the node runs on the built-in defaults (profile `balanced`, only the `free` plan). To use your own
  file, uncomment the `./rt.toml:/etc/rt/rt.toml:ro` volume line. See [Production deployment](../guides/production.md).
- The image sets the listener addresses, `node.data_dir` and `security.require_v8_sandbox=false` through environment
  variables, and they override an `rt.toml`. The last one is there because the prebuilt V8 the image uses has no sandbox;
  `21ci node status` shows `V8 sandbox: false`.

Any configuration key can also be set as an environment variable, `RT__SECTION__KEY=value`, for example
`RT__NODE__DEFAULT_PROFILE: min_latency` under `environment:`. The [configuration reference](../config.md)
lists every key.

### Without Compose

```bash
make docker-build      # image rt:dev
make docker-run        # container rt-dev on 127.0.0.1:8080/8081/9090, volume rt-dev-data
make docker-logs
make docker-stop
```

`make docker-run` takes `RT_ROOT_TOKEN` and `RT_MASTER_KEY` from the environment or `./.env`, and generates both into
`./.env` when neither exists. It mounts `./rt.toml` read-only when present. `RT_CPUS=4 RT_MEMORY=2g` limits the
container.

## Option 2: from source (development)

You need the Rust toolchain pinned in `rust-toolchain.toml` (Rust 1.95, with the `wasm32-wasip2` target) and Python 3
for the configuration tools. macOS works for development; production nodes run on Linux.

```bash
make dev             # debug build, config/defconfigs/dev.toml, data in ./data
make dev WATCH=1     # rebuild and restart on source changes (needs cargo-watch)
```

`make dev` binds every listener to loopback, uses the root token `dev-root-token` unless `RT_ROOT_TOKEN` is set, and
creates a random master key once in `data/dev-master.key` so sealed secrets survive restarts.

For a release binary and your own configuration:

```bash
make release                                   # target/release/rt
make defconfig PROFILE=balanced > rt.toml      # or copy a file from config/defconfigs/
target/release/rt --config rt.toml --print-config   # check the file: prints the resolved configuration
RT_ROOT_TOKEN=… RT_MASTER_KEY=… target/release/rt --config rt.toml serve
```

The `rt` binary has three commands: `serve` (the default), `openapi` (print the control API's OpenAPI document) and
`secrets reseal` (used when [rotating the master key](../guides/production.md)).

!!! warning "Release builds need a root token"
    A release build started without `RT_ROOT_TOKEN` answers every control route with `503 control_disabled`. Debug
    builds leave the control API open when no token is set, which is only acceptable on a loopback development node.

## Install the CLI

`21ci` is a small client for the control API. It has no V8 or wasmtime in it and only needs to reach the control
listener, so you can install it on any machine.

```bash
make cli               # target/debug/21ci (RELEASE=1 for target/release/21ci)
make cli-install       # cargo install --path crates/rt-cli --locked -> ~/.cargo/bin/21ci
21ci version           # 21ci 0.1.0
```

Shell completions:

```bash
21ci completion bash > ~/.local/share/bash-completion/completions/21ci
21ci completion zsh  > "${fpath[1]}/_21ci"
21ci completion fish > ~/.config/fish/completions/21ci.fish
```

The CLI is optional: everything it does is a plain HTTP call, and every page in these docs shows the `curl`
equivalent. The [CLI reference](../cli.md) lists every command.

## Next steps

Your node is running. Continue with [First run](first-run.md) to create a tenant and an API key.

This page is the quick local setup. For a node that other people use, read [Self-hosting](../guides/self-hosting.md)
(including systemd and bare-metal installs) and [Production deployment](../guides/production.md) (TLS, backups,
monitoring, upgrades, key rotation).
