---
title: Self-hosting
description: Run your own node on one Linux machine with Docker Compose, a hardened systemd unit or a small VPS with automatic TLS, and what a single personal node can and cannot do.
---

# Self-hosting <span class="rt-badge rt-badge--operator">operator</span>

This guide takes one Linux machine from nothing to a node that you and a few people use. Three setups are covered, from
the quickest to the most controlled. When the node serves customers, continue with
[Production deployment](production.md).

| Path | Best for | You manage |
|---|---|---|
| [Docker Compose](#docker-compose) | A first node, a laptop, a server where Docker is already the way you run things | The compose file, the volume |
| [systemd](#systemd-without-docker) | A server without Docker, tight control of users, limits and sandboxing | A binary, a unit file, `/etc/rt` and `/var/lib/rt` |
| [A small VPS with Caddy](#a-small-vps-with-automatic-tls) | A public node for yourself or a small team, with TLS | systemd or Compose, plus a proxy |

## What a single node is, and is not

A node is one process (`rt`) with one data directory. That makes it simple to run and sets honest limits:

- **It runs on one machine.** There is no clustering and no failover. If the machine is down, the node is down;
  availability comes from restarting fast and from backups.
- **It hosts other people's code in one process.** For a personal node or a team you trust that is fine. Do not put
  mutually hostile tenants on one node ([Multi-tenancy](../concepts/multi-tenancy.md)).
- **It does not terminate TLS.** Put a reverse proxy in front of the public listener.
- **It needs no database, broker or object store.** SQLite and local disk are built in.

## Before you start

You need:

- A Linux x86_64 or arm64 machine. Size it with [Resource requirements](resources.md); 1 vCPU and 1 GB is the smallest
  configuration the project ships (`edge-1gb.toml`), 2 vCPU and 2 GB or more is a comfortable start.
- A checkout of the runtime repository on the machine you build on.
- Two secrets: the **root token** (`RT_ROOT_TOKEN`, the operator's credential for the control API) and the **master key**
  (`RT_MASTER_KEY`, 64 hex characters, which seals tenant secrets). Generate them with `openssl rand -hex 32`.
  **Back up the master key apart from the data.** Without it sealed secrets cannot be opened
  ([First run](../getting-started/first-run.md#the-two-node-secrets)).
- DNS for a public name, if the node will be reachable from the internet.

Ports, as the node uses them (full list in [Install](../getting-started/install.md#what-a-node-exposes)):

| Port | Listener | Expose it? |
|---|---|---|
| 8080 | public: service URLs | Yes, through a TLS proxy |
| 8081 | control API and `/docs` | No. Loopback, SSH tunnel or VPN |
| 9090 | metrics, `/healthz`, `/readyz` | No. Loopback or monitoring network |
| `rt.sock` | internal Unix socket | No. Local sidecars only |

## Docker Compose

The shortest path. The full walkthrough, with explanations, is on [Install](../getting-started/install.md); this is the
summary.

```bash
cp .env.example .env
sed -i.bak "s/^RT_ROOT_TOKEN=.*/RT_ROOT_TOKEN=$(openssl rand -hex 32)/; s/^RT_MASTER_KEY=.*/RT_MASTER_KEY=$(openssl rand -hex 32)/" .env && rm .env.bak
docker compose up -d --build            # the first build takes 20+ minutes
until curl -fsS http://127.0.0.1:9090/readyz; do sleep 1; done
```

What the compose file gives you: ports published on `127.0.0.1` only, a named volume for `/data`, a non-root user, all
capabilities dropped, `no-new-privileges`, a file-descriptor limit of 65,536 and a 30 s stop grace period. Limit the
container with `cpus:` and `mem_limit:` (commented in the file): the node sizes itself from them
([Resource requirements](resources.md#base-requirements-of-a-node)).

To use your own configuration, generate a file and uncomment the volume line:

```bash
make defconfig PROFILE=balanced > rt.toml
# edit rt.toml, then uncomment "- ./rt.toml:/etc/rt/rt.toml:ro" in docker-compose.yml
docker compose up -d
```

Listener addresses, `node.data_dir` and `security.require_v8_sandbox=false` are set by the image's environment and
override the file.

## systemd without Docker

### 1. Build the binary

On a Linux machine with a C toolchain and the Rust toolchain that `rust-toolchain.toml` pins (Rust 1.95), from a
checkout of the repository:

```bash
make release          # target/release/rt  (cargo build --release -p rt-server --bin rt)
```

The build downloads a prebuilt V8 static library, so it needs network access, and the first build takes a long time.
Alternatively, build in the image's builder stage and copy the binary out, which gives you the same toolchain as the
Docker image:

```bash
docker build --target builder -t rt-builder .
docker create --name rt-extract rt-builder
docker cp rt-extract:/out/rt ./rt
docker rm rt-extract
```

!!! warning "Match the glibc of the build and the target"
    The binary links glibc dynamically. It runs on hosts with a glibc at least as new as the one it was built against
    (the image uses Debian 12 "bookworm"). Build on the oldest distribution you will deploy to, or use Docker.

Install it:

```bash
sudo install -m 0755 target/release/rt /usr/local/bin/rt      # or ./rt from the extraction above
rt --version
```

### 2. Create the user, directories and secrets

```bash
sudo useradd --system --home-dir /var/lib/rt --shell /usr/sbin/nologin rt
sudo install -d -m 0755 /etc/rt

# Secrets: readable by root only. systemd reads the file before it drops privileges.
umask 077
printf 'RT_ROOT_TOKEN=%s\nRT_MASTER_KEY=%s\nRT_LOG=info\n' "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" \
  | sudo tee /etc/rt/rt.env > /dev/null
sudo chmod 600 /etc/rt/rt.env
```

Copy the master key from `/etc/rt/rt.env` to your password manager now.

### 3. Configure

```toml title="/etc/rt/rt.toml"
[node]
data_dir = "/var/lib/rt"
default_profile = "balanced"

[listeners]
public   = "127.0.0.1:8080"             # a proxy on this machine terminates TLS
internal = "unix:/var/lib/rt/rt.sock"   # the default points at /data, which does not exist here
control  = "127.0.0.1:8081"
metrics  = "127.0.0.1:9090"

[security]
require_v8_sandbox = false              # this release links a V8 without the sandbox; true refuses to start

[ingress]
trusted_proxies = ["127.0.0.1"]         # the proxy on this machine; see "A small VPS" below

# Size the node explicitly when you also set MemoryMax in the unit: about 85 % of it.
[memory]
budget_mb = 1740                        # for MemoryMax=2G

# Optional: your own domain scheme and a plan for your users. See Production deployment.
# [routing]
# base_domain = "fn.example.com"
```

Check the file before you start the service:

```bash
sudo bash -c 'set -a; . /etc/rt/rt.env; rt --config /etc/rt/rt.toml --print-config' | head -30
```

`--print-config` prints the fully resolved configuration (profile defaults filled in) and exits 0 when it is valid.

### 4. The unit

```ini title="/etc/systemd/system/rt.service"
[Unit]
Description=21 Cloud Functions Runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=rt
Group=rt
EnvironmentFile=/etc/rt/rt.env
ExecStart=/usr/local/bin/rt --config /etc/rt/rt.toml serve
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=2

# State: /var/lib/rt, owned by the rt user, created by systemd.
StateDirectory=rt
StateDirectoryMode=0750

# Stop: SIGTERM starts a drain that waits up to 30 s for in-flight tasks, then closes listeners.
KillSignal=SIGTERM
TimeoutStopSec=60

# Limits: file descriptors for connections and open databases; memory tied to memory.budget_mb.
LimitNOFILE=65536
MemoryMax=2G
# CPUQuota=200%

# Sandboxing. V8 and wasmtime generate machine code at run time, so do NOT set
# MemoryDenyWriteExecute=yes, and do not add a SystemCallFilter without testing it under load.
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
RestrictRealtime=yes
LockPersonality=yes
CapabilityBoundingSet=
AmbientCapabilities=

[Install]
WantedBy=multi-user.target
```

Start it and check:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rt
journalctl -u rt -f                              # JSON system logs
curl -fsS http://127.0.0.1:9090/readyz           # 200 when migrations are applied and the node is not draining
```

Then create the first tenant exactly as in [First run](../getting-started/first-run.md), using the root token from
`/etc/rt/rt.env`.

- **Reload** with `sudo systemctl reload rt`: `SIGHUP` re-reads `[plans]` and the live scheduler and memory keys
  without a restart. Everything else needs `systemctl restart rt`.
- **Stop** with `systemctl stop rt`: the node drains first, so give it its grace time (`TimeoutStopSec`).
- **Memory.** `MemoryMax` is the kernel's hard limit; `memory.budget_mb` is the node's soft budget. Keep the budget about
  85 % of the limit so the node starts shedding load before the kernel kills it. The node also reads cgroup limits when
  `memory.budget_mb = 0`; setting it explicitly removes any doubt about which cgroup it sees.

### 5. Upgrade, roll back, uninstall

```bash
# upgrade: build the new binary, then
sudo install -m 0755 new-rt /usr/local/bin/rt
sudo systemctl restart rt            # services rebuild lazily on first use; see Production deployment for the drain-first procedure
```

Keep the previous binary (`rt.previous`) so you can swap back. Compiled caches rebuild when the engine fingerprint
changes; data and source are untouched.

Uninstall: `systemctl disable --now rt`, remove the unit, `/usr/local/bin/rt` and `/etc/rt`. `/var/lib/rt` holds every
tenant's data: delete it only when you mean to.

## A small VPS with automatic TLS

A single public node on a VPS, with certificates managed by Caddy.

1. **DNS.** Create an A (and AAAA) record, for example `fn.example.com`, pointing at the VPS.
2. **Firewall.** Allow SSH, 80 and 443 only. The node binds to loopback, so it is not reachable on 8080, 8081 or 9090
   from outside.

   ```bash
   sudo ufw allow OpenSSH && sudo ufw allow 80,443/tcp && sudo ufw enable
   ```

3. **Run the node** with Compose or systemd as above. With Compose the ports are already on `127.0.0.1`; with systemd use
   the configuration above.
4. **Caddy.** Install it from your distribution or Caddy's repository, then:

   ```caddy title="/etc/caddy/Caddyfile"
   fn.example.com {
     reverse_proxy 127.0.0.1:8080
   }
   ```

   ```bash
   sudo systemctl reload caddy
   ```

   Caddy obtains and renews the certificate, passes `Host` unchanged, appends `X-Forwarded-For` and streams responses.
5. **Tell the node who the proxy is.** With systemd and Caddy on the same host, `trusted_proxies = ["127.0.0.1"]` (as
   above). With Compose, the proxy reaches the node through Docker's port mapping, so the node sees the Docker network
   gateway instead: see the warning in [Production deployment](production.md#reverse-proxy-and-tls). Without this, every
   client looks like the proxy to `ip_allow` rules and rate limits.
6. **Reach the control API** over SSH, never through Caddy:

   ```bash
   ssh -N -L 8081:127.0.0.1:8081 you@fn.example.com
   export RT_API=http://127.0.0.1:8081
   ```

Services are then reachable at `https://fn.example.com/s/{slug}/`. For `{slug}.fn.example.com` URLs set
`routing.base_domain` and use a wildcard certificate (which needs a DNS challenge); custom domains for tenants are in
[Custom domains](custom-domains.md).

## First-boot checklist

- [ ] `curl -fsS http://127.0.0.1:9090/readyz` answers 200 and `21ci node status` (as the root token) shows
      `Mode: normal`.
- [ ] `RT_ROOT_TOKEN` and `RT_MASTER_KEY` are not in your shell history, a repository or a world-readable file, and the
      master key is backed up elsewhere.
- [ ] Ports 8081 and 9090 are not reachable from outside (`ss -ltnp` shows `127.0.0.1`; test from another machine).
- [ ] TLS works at the public name and a request reaches a deployed service.
- [ ] A backup of the data directory has been taken and, once, restored on another machine
      ([Production deployment](production.md#backups)).
- [ ] Disk usage of the data directory is on your monitoring.

## Where things live

| Path | What |
|---|---|
| `/usr/local/bin/rt` | The binary |
| `/etc/rt/rt.toml`, `/etc/rt/rt.env` | Configuration and secrets |
| `/var/lib/rt` (`/data` in Docker) | Everything the node stores: `meta.db`, `queue.db`, `services/`, `blobs/`, `artifacts/`, `cache/`, `logs/`, `rt.sock`, and `master.key` if you did not provide one |

## See also

- [Install](../getting-started/install.md): the quick local setup and the `21ci` CLI.
- [Production deployment](production.md): TLS and proxy details, backups, monitoring, upgrades, key rotation.
- [Resource requirements](resources.md): sizing the machine.
- [Configuration reference](../config.md): every `rt.toml` key and its `RT__SECTION__KEY` override.
- [Security model](../concepts/security.md): what protects the node, and what does not yet.
