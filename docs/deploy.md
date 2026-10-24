# Deployment and operations

One node = one process running `rt serve` (`--role=all`, the only role in the MVP) with one data directory. Pick a way to run it:

| How | Where |
|---|---|
| Docker Compose, production: `ghcr.io/21-ci/rt` behind Caddy (automatic HTTPS), read-only root filesystem | [`deploy/compose/`](https://github.com/21-ci/runtime/blob/main/deploy/README.md) |
| Docker Compose, development: builds the image from source, everything on loopback | [`docker-compose.yml`](https://github.com/21-ci/runtime/blob/main/docker-compose.yml), [`.env.example`](https://github.com/21-ci/runtime/blob/main/.env.example), `scripts/docker-*.sh`; quickstart in the [README](https://github.com/21-ci/runtime/blob/main/README.md#quickstart-docker-compose) |
| Kubernetes: StatefulSet, PVC, probes, NetworkPolicy, secrets as files (one replica) | [`deploy/kubernetes/rt.yaml`](https://github.com/21-ci/runtime/blob/main/deploy/kubernetes/rt.yaml) |
| systemd (deb / rpm), Homebrew, `install.sh`, launchd | [`packaging/README.md`](https://github.com/21-ci/runtime/blob/main/packaging/README.md) |

All of them use the same rules: TLS in a reverse proxy in front of the public listener, control API and metrics never exposed, secrets in the environment or in files, never in `rt.toml`. [`deploy/rt.toml`](https://github.com/21-ci/runtime/blob/main/deploy/rt.toml) holds production defaults (loopback binds, `/var/lib/rt`, the proxy on localhost trusted); packages install it as `/etc/rt/rt.toml`, the image ships it as `/etc/rt/defconfigs/production.toml`.

## Requirements

| | |
|---|---|
| Host | Linux x86_64 / arm64 with Docker (Compose v2, BuildKit), or the deb / rpm packages. macOS runs `make dev` or the Homebrew service for development only |
| Image | release images `ghcr.io/21-ci/rt:<version>` (verify them: [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#verifying-releases)). Built from source (`make docker-build`, `docker compose up --build`) the first build downloads the prebuilt V8 and compiles the workspace (20+ min); later builds reuse BuildKit caches |
| Base | `gcr.io/distroless/cc-debian13` (glibc, needed by the prebuilt V8): no shell, no package manager, no `curl`; non-root `uid:gid 10001`; works with a read-only root filesystem (only `/data` is written) |
| Size | reference box of the perf criteria: 4 vCPU / 2 GiB (`RT_CPUS=4 RT_MEMORY=2g` for `make docker-run`). `sched.processors = 0` and `memory.budget_mb = 0` follow the cgroup limits (budget = 85 % of the cgroup max) |

## Ports and image defaults

| Listener | Container | Published by compose / `docker-run.sh` |
|---|---|---|
| public (invoke) | `0.0.0.0:8080` | `127.0.0.1:${RT_PUBLIC_PORT:-8080}` |
| control (`/v1`, `/docs`) | `0.0.0.0:8081` | `127.0.0.1:${RT_CONTROL_PORT:-8081}` |
| metrics (`/metrics /healthz /readyz`) | `0.0.0.0:9090` | `127.0.0.1:${RT_METRICS_PORT:-9090}` |
| internal | `unix:/data/rt.sock` | not published |

The image sets `RT__NODE__DATA_DIR=/data`, `RT__LISTENERS__{PUBLIC,INTERNAL,CONTROL,METRICS}`, `RT__SECURITY__REQUIRE_V8_SANDBOX=false` (MVP: prebuilt V8, no sandbox) and `RT_LOG=info`. The `RT__` overlay beats a mounted `rt.toml`, so binds and the data dir change only by overriding these variables. Entrypoint: `rt serve`. It reads `/etc/rt/rt.toml` when that file exists (mount it `:ro`; the image sets `RT_DATA=/etc/rt`), else runs on the built-in defaults (profile `balanced`, plan `free`). Reference configs are in `/etc/rt/defconfigs/` (the profiles, `edge-1gb`, `dev`, `production`). Healthcheck: the image's own probe binary, `rt-healthcheck`, does `GET 127.0.0.1:9090/healthz` (liveness, address in `RT_HEALTHCHECK_ADDR`; use `/readyz` for load-balancer probes). Because the image has no shell, `docker exec rt sh` does not work; run `rt` subcommands with `docker compose exec rt rt …`. Compose adds `cap_drop: ALL`, `no-new-privileges`, `nofile 65536`, `stop_grace_period: 30s`; the production stack also `read_only: true`.

## Environment variables

Read by `rt`:

| Variable | Meaning |
|---|---|
| `RT_ROOT_TOKEN` / `RT_ROOT_TOKEN_FILE` | Control API root token (Bearer). Unset in a release build → every control route `503 control_disabled`. Set only one of the two |
| `RT_MASTER_KEY` / `RT_MASTER_KEY_FILE` | 32 bytes as 64 hex chars (`openssl rand -hex 32`); seals tenant secrets, derives the invoke MAC key. Also `security.master_key_file`. If none is set, `rt` generates `<data_dir>/master.key` (0600). Lose it and sealed secrets are unreadable; to change it, follow [Rotating the master key](#rotating-the-master-key) |
| `RT_MASTER_KEY_PREVIOUS` / `RT_MASTER_KEY_PREVIOUS_FILE` | Optional, only during a key rotation: the old master key. Opens secrets and verifies API keys made under it; never used to seal |
| `RT_CONFIG` | path of `rt.toml` (same as `--config`); default `$RT_DATA/rt.toml` if it exists |
| `RT_DATA` | only used to find `$RT_DATA/rt.toml`; the data dir is `node.data_dir` |
| `RT__SECTION__KEY` | overlay on any config key, e.g. `RT__NODE__DEFAULT_PROFILE=min_latency`, `RT__SCHED__PROCESSORS=4`. `[[binding]]` cannot be set this way |
| `RT_LOG` | tracing filter, default `info` (read first, then `RUST_LOG` is checked) |
| `RT_SCHEDTRACE=<ms>` | every `<ms>` print one scheduler line to stderr, see [Debugging the scheduler](#debugging-the-scheduler) |

Read by compose / scripts only: `RT_IMAGE` (default `rt:dev`), `RT_PUBLIC_PORT`, `RT_CONTROL_PORT`, `RT_METRICS_PORT`; `make docker-run` also takes `IMAGE`, `CONTAINER` (`rt-dev`), `VOLUME` (`rt-dev-data`), `RT_CPUS`, `RT_MEMORY`, `RT_WAIT`, `RT_CONFIG_FILE`.

Compose refuses to start without `RT_ROOT_TOKEN` and `RT_MASTER_KEY` (from `.env` or the shell). `.env.example` ships non-hex `change-me-` placeholders on purpose: the node rejects such a key and `docker-run.sh` / `e2e.sh` refuse them. To keep secrets in files instead, replace the two `environment` entries of the compose file with `RT_ROOT_TOKEN_FILE` / `RT_MASTER_KEY_FILE` and mount the files (setting both `NAME` and `NAME_FILE` is a startup error).

## Configuration

```bash
cp config/defconfigs/balanced.toml rt.toml     # or: make defconfig PROFILE=balanced > rt.toml, make menuconfig
# edit, then mount it: uncomment `- ./rt.toml:/etc/rt/rt.toml:ro` in docker-compose.yml
docker compose up -d
```

`make defconfig PROFILE=<profile>` generates profiles: `balanced`, `max_density`, `min_latency`, or copy from `config/defconfigs/` (`edge-1gb.toml`, `dev.toml` also available). In the image the `rt.toml` listener and `data_dir` keys are overridden by the `RT__` variables above. Reference: [config.md](config.md). `rt.toml` is read at startup: restart to apply changes, except `[plans]`, which `SIGHUP` reloads (see [Live-updatable vs restart-required](config.md#live-updatable-vs-restart-required)). At runtime only `PUT /v1/node/config` changes `default_profile` and `degradation_mode`; `rate_limit` is reported as `pending_restart`.

Keys operators usually set:

| Key | Default | Why |
|---|---|---|
| `[plans.<name>]` (e.g. `[plans.pro]`) | only built-in `free` | tenant plans (quotas, limits, `custom_host_suffixes`, `allowed_triggers`) |
| `routing.base_domain` | `""` (off) | `{slug}.<domain>` routing |
| `ingress.trusted_proxies` | `[]` | CIDRs whose `X-Forwarded-For` is trusted (IP allow rules, per-IP limits) |
| `security.reserved_hosts` | `[]` | hosts tenants can never claim |
| `egress.denied_ports/hosts/cidrs` | ports 25, 465, 587 | extra egress denials on top of the SSRF guard |
| `build.npm_cdn`, `build.allowed_hosts` | esm.sh and 5 CDNs | air-gapped installs point both at an internal mirror |
| `memory.budget_mb`, `memory.high/critical` | cgroup 85 %, 0.80 / 0.92 | memory governor |
| `deploy.retain_versions`, `logs.retention_days`, `logs.max_mb_per_service` | 10, 7, 50 | disk growth |

## Reverse proxy and TLS

`rt` has no TLS. Terminate it in front of **public** only; keep control and metrics on loopback / VPN (they have no network ACL).

```caddy
fn.example.com, *.fn.example.com {          # the wildcard needs routing.base_domain = "fn.example.com" and a DNS-challenge cert
  reverse_proxy 127.0.0.1:8080
}
```

```nginx
server {
  listen 443 ssl; http2 on; server_name fn.example.com *.fn.example.com;
  ssl_certificate /etc/ssl/fn.pem; ssl_certificate_key /etc/ssl/fn.key;
  location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_buffering off;                      # streamed responses
  }
}
```

Set `ingress.trusted_proxies` to the proxy address, or `x-forwarded-for` is ignored. Built-in ingress limits: request head ≤ 16 KB and ≤ 100 headers (`431`), header timeout 10 s, 256 connections per IP, 100 HTTP/2 streams.

## Health and metrics

| Endpoint (metrics listener) | Meaning |
|---|---|
| `/healthz` | liveness: sysmon heartbeat < 1 s, else `503` |
| `/readyz` | readiness: migrations applied, base snapshot loaded, not draining, else `503` |
| `/metrics` | Prometheus text, the series below. Per-service series only with `metrics.per_service = true` |

The three endpoints need no credentials: keep the metrics listener private. Node-level metrics (always on, present zero-valued in the first scrape):

| Area | Metrics |
|---|---|
| Tasks | `rt_tasks_total{kind,trigger,outcome}`, `rt_task_phase_seconds{kind,phase=queue\|wait_runtime\|init\|exec}`, `rt_inflight_tasks{kind}`, `rt_admission_rejects_total{reason}` |
| Scheduler | `rt_sched_processors{kind}`, `rt_sched_grq_len{kind}`, `rt_sched_lrq_len{p}`, `rt_sched_steals_total{kind}`, `rt_sched_rekinds_total{from,to}`, `rt_sched_preemptions_total{kind}`, `rt_sched_hog_handoffs_total`, `rt_sched_spinning_seconds_total`, `rt_sysmon_tick_seconds` |
| Memory | `rt_mem_budget_bytes`, `rt_mem_committed_bytes{category}`, `rt_mem_rss_bytes`, `rt_mem_fragmentation_ratio`, `rt_mem_pressure_level` |
| Cache | `rt_cache_entries{tier}`, `rt_cache_bytes{tier}`, `rt_cache_hits_total{tier}`, `rt_cache_misses_total{tier}`, `rt_cache_evictions_total{tier,reason}`, `rt_cache_unreserved_inserts_total` (T1 inserts held without a memory reservation: the governor refused and nothing was evictable) |
| Executors | `rt_deno_isolate_create_seconds`, `rt_deno_code_cache_total{result}`, `rt_wasm_instantiate_seconds`, `rt_wasm_pool_slots` |
| Build | `rt_build_total{kind,result}`, `rt_build_seconds{kind}` |
| I/O | `rt_http_requests_total{listener,status_class}`, `rt_ingress_conn_rejected_total` (connections closed at accept by `ingress.max_conns_per_ip`), `rt_egress_requests_total{result}`, `rt_egress_bytes_total` |
| Triggers | `rt_schedule_fires_total`, `rt_schedule_skipped_total`, `rt_schedule_denied_total` (fires dropped because the tenant's plan no longer allows `interval` / `cron`), `rt_queue_depth` (ready messages over all services), `rt_queue_dlq_total` |
| Logs | `rt_logs_records_total{kind}`, `rt_logs_dropped_total` |
| WebSockets | `rt_ws_sockets` (open sockets), `rt_ws_upgrades_total{result}` (`ok`, `limited`: a socket cap refused the accept), `rt_ws_messages_total{dir}` (`in`, `out`), `rt_ws_event_tasks_total{outcome}` (`ok`, `failed` per attempt, `given_up`), `rt_ws_closed_total{code}` |
| Events | `rt_queue_throttled_total` (queue sends refused by rate or quota), `rt_pubsub_published_total`, `rt_pubsub_delivered_total`, `rt_pubsub_dropped_total{reason}`, `rt_stream_appended_total`, `rt_stream_delivered_total`, `rt_stream_dlq_total`, `rt_stream_trimmed_total`, `rt_stream_skipped_total{reason}`, `rt_async_accepted_total`, `rt_async_throttled_total`, `rt_async_finished_total{condition}`, `rt_async_failed_evicted_total`, `rt_instance_keepalive_total{kind,outcome}` (`kind`: `deno`, `wasm`; `outcome`: `kept`, `handoff`, `disposed`, `reused`, `expired`, `shed`) |

Per-service metrics are opt-in (`metrics.per_service = true`): `rt_service_requests_total{service,status_class}` and `rt_service_duration_seconds{service}`. Only the top `metrics.per_service_top_n` (100) services by traffic get their own `service` label; the rest aggregate into `service="_other"`. The same services (those active in the last autoscaler tick) also get `rt_service_concurrency_limit`, `rt_service_running`, `rt_service_queued`, `rt_service_queue_wait_seconds` (mean admission wait over the last tick), `rt_service_min_instances`, `rt_service_max_instances`, `rt_service_target_instances` (warm instances wanted) and `rt_service_arrival_rate` (smoothed requests per second), all labelled `{service}`.

Admission and autoscaling (registered by the scheduler on the same registry, always on): `rt_admission_running` (tasks holding a run slot), `rt_admission_queued` (tasks waiting for one), `rt_admission_queued_bytes`, `rt_admission_queue_wait_seconds` (histogram: wait of tasks that queued, until granted), `rt_autoscale_adjustments_total{direction=up\.

Per service, `GET /v1/services/{s}/stats` is backed by in-memory HDR histograms (1-minute buckets, 24 h), always on and independent of Prometheus cardinality: counts, error rate, outcome breakdown, p50/p95/p99 for `queue`, `init`, `exec` and total. Every response carries `server-timing: queue;dur=…, wait_runtime;dur=…, init;dur=…, exec;dur=…`.

Per node: `GET /v1/node/status` (mode, processors, memory pressure), `/v1/node/scheduler`, `/v1/node/cache` (operator token). Per service: `/v1/services/{s}/stats`, `/logs`, `/events`. System logs are JSON on stdout (`docker compose logs rt`, `journalctl -u rt`), filtered by `RT_LOG`; they never contain env or secret values.

### Debugging the scheduler

`GET /v1/node/scheduler` (operator token) returns a snapshot: per-processor kind and queue lengths, bound counts, global queue lengths, spinning and idle counts, and the admission state. `RT_SCHEDTRACE=1000` prints the same as one line per second to stderr (the number after `SCHED` is the node's uptime):

```
SCHED 1000ms: procs=4 deno=3 wasm=1 idle=1 spinning=0 grq=[deno:12 wasm:0 build:1] lrq=[3 0 5 | 0] bound=[2 0 1] waitrt=4 waitmem=0
```

`procs` is the number of processors (split by kind in `deno` / `wasm` / `build`), `idle` and `spinning` count processors without work and machines looking for it, `grq` is the length of each kind's global queue, `lrq` the local queue of each processor (kinds separated by `|`), `bound` the tasks pinned to each machine, `waitrt` and `waitmem` the tasks waiting for a runtime and for memory.

## Data and backup

```
/data/                      volume (compose: rt_rt-data, docker-run: rt-dev-data); bind mounts must be writable by uid 10001; packages use /var/lib/rt
  meta.db  queue.db         tenants, keys (hashed), services, versions, schedules, usage, events / queue + DLQ (SQLite, WAL)
  master.key                only when no RT_MASTER_KEY was given
  artifacts/<sha256>/       uploaded sources and wasm (content-addressed)
  services/<service-id>/data.db   KV + SQL of a service
  blobs/<service-id>/       blob store
  logs/<service-id>/        persisted log segments
  cache/v2/<engine-fp>/     compiled artifacts (T2), rebuildable (an older cache/<engine-fp>/ is deleted at startup)
  deps/                     fetched npm/jsr modules, rebuildable
  rt.sock                   internal listener
```

- A hot `cp -r` of SQLite files is **not** consistent. Stop the node (`docker compose stop rt`), then archive the volume, or take `sqlite3 <db> ".backup"` copies of `meta.db`, `queue.db`; per-service SQL/KV: `GET /v1/services/{s}/data/export` (scope `data:admin`).
- Back up `meta.db`, `queue.db`, `artifacts/`, `services/`, `blobs/` **and the master key** (store it apart). `cache/` and `deps/` may be dropped.
- Example (cold): `docker compose stop rt && docker run --rm -v rt_rt-data:/data -v "$PWD":/backup debian:trixie-slim tar czf /backup/rt-data.tgz -C /data . && docker compose start rt`.
- `docker compose down -v` / `scripts/docker-stop.sh --purge` delete the volume: tenants, services, sealed secrets.

## Upgrade

1. `docker compose build` (new image), optionally `POST /v1/node/drain` (`202`): new tasks get `503 draining`, `/readyz` turns `503`. Drain is one-way: restart to resume.
2. `docker compose up -d` (SIGTERM, 30 s grace for in-flight tasks, then restart on the same volume). Migrations are forward-only and run at startup.
3. A new engine fingerprint (V8 / wasmtime / flags) misses `cache/`: versions rebuild lazily from `artifacts/` and `rt.lock` on the next request. Force it with `POST /v1/services/{s}/versions/{v}/rebuild`.

Single node only: there is no clustering or canary tooling in v1.

**Schema change (API keys scoped by projects and environments).** The metadata schema is now one consolidated version,
and the key scope moved from `service_ids` to `project_ids` and `environments`. A `meta.db` from an earlier build is not
migrated. For a development node, stop it, delete `data/meta.db*` (and `data-test`), start it again and create the
tenants, keys and services anew. This drops every tenant, key and service in that database.

## Rotating the master key

The master key seals tenant secrets and derives the API-key pepper and the T2 MAC key. Each sealed secret records
which key sealed it, so a rotation needs no downtime beyond two restarts:

1. Back up `meta.db` (see above) and generate the new key: `openssl rand -hex 32`.
2. Restart with `RT_MASTER_KEY=<new>` and `RT_MASTER_KEY_PREVIOUS=<old>` (or the `*_FILE` variants). New secrets are
   sealed with the new key; existing ones still open with the old one; API keys issued before keep authenticating.
   The compiled-artifact cache is dropped at startup (no `security.artifact_tamper` events) and rebuilds on demand.
3. Re-seal every stored secret under the new key, with the same configuration and environment as the node:
   `rt secrets reseal` (compose: `docker compose exec rt rt secrets reseal`). It prints how many values it rewrote,
   skips values that are already current (safe to re-run) and exits `1`, listing service id and name, for any value
   neither key opens; those are left untouched.
4. API key hashes cannot be re-sealed (the plaintext is never stored): keys issued before step 2 verify only while
   `RT_MASTER_KEY_PREVIOUS` is set. Have tenants create new keys and revoke the old ones.
5. Once reseal reports `failed: 0` and the old API keys are revoked, restart without `RT_MASTER_KEY_PREVIOUS` and
   destroy the old key.

## Security checklist

| Item | State |
|---|---|
| Root token and master key random, not in the image or git (`.env` is git-ignored); prefer `*_FILE` | operator |
| Published ports on loopback; TLS and auth in front of `public`; control and metrics only via VPN / SSH | operator |
| Non-root user, `cap_drop: ALL`, `no-new-privileges`, no `docker.sock` | in compose |
| SSRF guard on every guest `fetch` (private ranges, loopback, metadata); extra `egress.*` and per-service `egress` rules | built in + config |
| Plans: set `max_services`, `rps`, `invocations_per_day`, `storage_mb`, `limits_max`, `builds_per_hour` per tenant tier | operator |
| Process hardening (Linux, `security.seccomp = true`): `PR_SET_DUMPABLE=0`, `NO_NEW_PRIVS`, `RLIMIT_CORE=0` only. seccomp filter and Landlock are MVP-deferred | partial |
| V8 sandbox: **not available** in the MVP (prebuilt `rusty_v8`, `v8_sandbox:false`). The per-task isolate is the only boundary: do not mix mutually hostile tenants with sensitive operator data on one node | MVP-deferred |
| Audit trail: `GET /v1/services/{s}/events`, service logs; secrets never appear in logs or API output | built in |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Exits at start with `v8_sandbox_required` | `security.require_v8_sandbox = true` in your `rt.toml`; set `false` (MVP) |
| `RT_MASTER_KEY … 64 hex characters` / `must be hex` | regenerate with `openssl rand -hex 32`; `change-me-` placeholders are rejected |
| `unknown key a.b` / `invalid value for a.b` | the message names the config key; check with `rt --print-config --config rt.toml` |
| Control routes `503 control_disabled` | `RT_ROOT_TOKEN` unset in a release build |
| `401 unauthorized` / `429 rate_limited` on the control API | wrong token; repeated failures are rate-limited per IP |
| `422 validation_failed: unknown plan` on tenant create | the plan must exist: `free` is built in, others under `[plans.<name>]` |
| `422 validation_failed: x-rt-tenant header is required` creating a service with the root token | send `x-rt-tenant: ten_…` or use a tenant key (`deploy.sh` needs a tenant key) |
| First request after deploy is slow | lazy build: the request compiles. Use `?compile=eager` or the `min_latency` profile; watch `GET /v1/services/{s}/versions/{v}` and `…/build/logs` |
| `502 build_failed` | read the build logs / `failure` of the version; fix and upload again, or `POST …/rebuild` |
| `503 overloaded` / `shed` / `queue_timeout` | `GET /v1/node/status` (`mode`, `memory.pressure`), `rt_admission_rejects_total{reason}`; raise the container memory, lower `deno.isolate_pool_per_p`, tune `sched.max_inflight` |
| `permission denied` on `/data` with a bind mount | `chown -R 10001:10001` the directory |
| Port already in use | set `RT_PUBLIC_PORT`, `RT_CONTROL_PORT`, `RT_METRICS_PORT` |
| No debug logs | use `RT_LOG=<filter>` (e.g. `info,rt_sched=debug`); `RT_LOG` is read first, then `RUST_LOG` |
