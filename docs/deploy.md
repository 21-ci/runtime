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

## Telemetry

Packaged builds and images with a build-time URL (`--build-arg RT_TELEMETRY_URL=...`) show this notice. A build without one sends nothing unless configured.

> 21 Cloud Runtime sends one anonymous report a day: request, project, service, organization and user counts, plus the version and a random id. No IP addresses are stored. See the exact payload with `rt telemetry show`. Opt out with `telemetry.enabled = false` or `DO_NOT_TRACK=1`. The first report is sent 24 hours after the first start.

In Docker, pass `-e DO_NOT_TRACK=1` to opt out. Details: [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#telemetry).

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
| Backups | `rt_backup_last_success_timestamp`, `rt_backup_duration_seconds`, `rt_backup_peak_wal_bytes` |
| Memory | `rt_mem_budget_bytes`, `rt_mem_committed_bytes{category}`, `rt_mem_rss_bytes`, `rt_mem_fragmentation_ratio`, `rt_mem_pressure_level` |
| Cache | `rt_cache_entries{tier}`, `rt_cache_bytes{tier}`, `rt_cache_hits_total{tier}`, `rt_cache_misses_total{tier}`, `rt_cache_evictions_total{tier,reason}`, `rt_cache_unreserved_inserts_total` (T1 inserts held without a memory reservation: the governor refused and nothing was evictable) |
| Executors | `rt_deno_isolate_create_seconds`, `rt_deno_code_cache_total{result}`, `rt_wasm_instantiate_seconds`, `rt_wasm_pool_slots` |
| Build | `rt_build_total{kind,result}`, `rt_build_seconds{kind}` |
| I/O | `rt_http_requests_total{listener,status_class}`, `rt_ingress_conn_rejected_total` (connections closed at accept by `ingress.max_conns_per_ip`), `rt_ingress_rate_limited_total{rule}` (requests refused by an `ingress.rate_rules` entry, by its index), `rt_egress_requests_total{result}`, `rt_egress_bytes_total` |
| Triggers | `rt_schedule_fires_total`, `rt_schedule_skipped_total`, `rt_schedule_denied_total` (fires dropped because the tenant's plan no longer allows `interval` / `cron`), `rt_queue_depth` (ready messages over all services), `rt_queue_dlq_total` |
| Logs | `rt_logs_records_total{kind}`, `rt_logs_dropped_total` |
| WebSockets | `rt_ws_sockets` (open sockets), `rt_ws_upgrades_total{result}` (`ok`, `limited`: a socket cap refused the accept), `rt_ws_messages_total{dir}` (`in`, `out`), `rt_ws_event_tasks_total{outcome}` (`ok`, `failed` per attempt, `given_up`), `rt_ws_closed_total{code}` |
| Events | `rt_queue_throttled_total` (queue sends refused by rate or quota), `rt_pubsub_published_total`, `rt_pubsub_delivered_total`, `rt_pubsub_dropped_total{reason}`, `rt_stream_appended_total`, `rt_stream_delivered_total`, `rt_stream_dlq_total`, `rt_stream_trimmed_total`, `rt_stream_skipped_total{reason}`, `rt_async_accepted_total`, `rt_async_throttled_total`, `rt_async_finished_total{condition}`, `rt_async_failed_evicted_total`, `rt_instance_keepalive_total{kind,outcome}` (`kind`: `deno`, `wasm`; `outcome`: `kept`, `handoff`, `disposed`, `reused`, `expired`, `shed`) |

Per-service metrics are opt-in (`metrics.per_service = true`): `rt_service_requests_total{service,status_class}` and `rt_service_duration_seconds{service}`. Only the top `metrics.per_service_top_n` (100) services by traffic get their own `service` label; the rest aggregate into `service="_other"`. The same services (those active in the last autoscaler tick) also get `rt_service_concurrency_limit`, `rt_service_running`, `rt_service_queued`, `rt_service_queue_wait_seconds` (mean admission wait over the last tick), `rt_service_min_instances`, `rt_service_max_instances`, `rt_service_target_instances` (warm instances wanted) and `rt_service_arrival_rate` (smoothed requests per second), all labelled `{service}`.

Admission and autoscaling (registered by the scheduler on the same registry, always on): `rt_admission_running` (tasks holding a run slot), `rt_admission_queued` (tasks waiting for one), `rt_admission_queued_bytes`, `rt_admission_queue_wait_seconds` (histogram: wait of tasks that queued, until granted), `rt_admission_wait_by_size_seconds{size=short|medium|long}` (histogram: that wait split by predicted run time), `rt_admission_aged_grants_total` (waiting tasks granted at or after their deadline), `rt_autoscale_adjustments_total{direction=up|down,cause}`. The keep-alive and fair-admission series are listed in [the metrics reference](reference/metrics.md#keep-alive-and-fair-admission).

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

- Prefer [`[backup]`](#backup-and-restore): it does the work below on a running node. By hand:
- A hot `cp -r` of SQLite files is **not** consistent. Stop the node (`docker compose stop rt`), then archive the volume, or take `sqlite3 <db> ".backup"` copies of `meta.db`, `queue.db`; per-service SQL/KV: `GET /v1/services/{s}/data/export` (scope `data:admin`).
- Back up `meta.db`, `queue.db`, `artifacts/`, `services/`, `blobs/` **and the master key** (store it apart). `cache/` and `deps/` may be dropped.
- Example (cold): `docker compose stop rt && docker run --rm -v rt_rt-data:/data -v "$PWD":/backup debian:trixie-slim tar czf /backup/rt-data.tgz -C /data . && docker compose start rt`.
- `docker compose down -v` / `scripts/docker-stop.sh --purge` delete the volume: tenants, services, sealed secrets.

## Backup and restore

```toml
[backup]
enabled = true
schedule = "0 3 * * *"   # cron, UTC
dir = ""                 # empty: <data_dir>/backups
keep_last = 3            # same disk: copy archives elsewhere, this does not survive losing it
```

`POST /v1/node/backups` (`21ci backup create`) or the schedule writes `rt-backup-<id>.tar`, mode `0600` in a `0700` directory. Writers keep going: each database is copied by one SQLite backup step on the backup's own low-priority thread, and a tenant's export cannot hold it up. Retention runs before a new backup. A due scheduled backup waits 10 minutes while the memory governor reports pressure. Watch `GET /v1/node/backups/status`, `rt_backup_last_success_timestamp`, `rt_backup_duration_seconds` and `rt_backup_peak_wal_bytes`.

**In the archive:** `meta.db`, `queue.db`, `streams.db`, each service's `data.db`, the blobs its rows reference, the artifacts `meta.db` references, and `manifest.json` with `manifest.mac`. **Not in it:** the master key, root token, `rt.toml`, `cache/`, `deps/`, `logs/`, `exports/`, `backups/`. Back up the master key apart: without it, sealed secrets are lost and the MAC cannot be checked. Without `encrypt_to`, archives hold all tenants' data in the clear. Deleted tenants' data stays in an archive until retention removes it.

**Encryption and S3.** `encrypt_to = ["age1..."]` streams the tar through `age` straight to `rt-backup-<id>.tar.age`, so no plaintext archive is on disk; one database snapshot at a time still sits in the private scratch directory while it is packed. The node then cannot read its own archives; stock `age -d` can. `[backup.s3]` uploads in 8 MiB parts, two at a time, and requires `encrypt_to` unless `allow_plaintext_remote = true`. Credentials come from `access_key_file`/`secret_key_file` or `RT_BACKUP_S3_ACCESS_KEY`/`RT_BACKUP_S3_SECRET_KEY`. The endpoint must be public HTTPS, or any scheme to loopback, and obeys `egress.denied_ports` and `denied_hosts`; private addresses are refused. An object counts only once `<key>.complete` exists; it and `<key>.sha256` are transport checks, not integrity, which is `manifest.mac`. A failed upload leaves `state=failed` and the local copy. Retention (`keep_last`, `keep_daily`, `keep_weekly`) counts complete objects, always keeps the newest, and touches only keys shaped like a backup. Add a bucket lifecycle rule for abandoned multipart uploads. Pass `--age-identity <file>` to `rt backup verify` and `restore`. `tests/e2e/restore_drill.sh` is the drill.

**Consistency.** Each database is one consistent commit. There is no point in time across databases: queue, stream and data state can differ by the length of the backup. Delivery is at-least-once, so a restored queue may redeliver. A service created during a backup is absent, and one deleted during it is skipped.

**Verify and restore, offline:**

```sh
rt backup verify rt-backup-<id>.tar                 # the MAC, every entry's digest and shape; writes nothing
export RT_MASTER_KEY_FILE=/secure/master.key        # or RT_MASTER_KEY, or master.key in the configured data_dir
rt backup restore rt-backup-<id>.tar --data-dir /new/data [--revoke-keys-before 2026-11-01T00:00:00Z]

# an encrypted archive: the age identity file holding the matching private key
rt backup verify  rt-backup-<id>.tar.age --age-identity /secure/backup.age
rt backup restore rt-backup-<id>.tar.age --age-identity /secure/backup.age --data-dir /new/data
```

The directory must be empty or absent. A restore refuses a tampered or forged archive (`bad_mac`), a cut one (`truncated`), a changed entry (`digest_mismatch`), any entry outside the format (`unsafe_entry`), a newer schema (`schema_too_new`) and too little disk (`insufficient_storage`), and then leaves the directory empty. `--no-auth` skips only the MAC, for an archive whose master key is lost: nothing then shows who wrote it. It checks every database, reports secrets the supplied key cannot open, records a `node.restored` event, and marks backups that were running in the snapshot as failed. Then put the master key in place (`RT_MASTER_KEY[_FILE]` or `master.key`) and start `rt` on the directory.

**After a restore.** Keys revoked, tenants disabled and grants removed after the snapshot are live again, and the audit trail of that window is gone. The command prints the snapshot's age and the active key count. Use `--revoke-keys-before <RFC 3339 or Unix ms>` (keys those keys minted go too) or rotate tenant keys, and bump the console session epoch, as the console's database comes back just as old. Practise this: restore into a scratch directory, start a node on another port, and compare.

**Console binding.** If a console reaches the node through a binding, list `allowed_paths` one by one: `/v1/node/backups` and `/v1/node/backups/status`, never `/v1/node/*`. `*` spans segments, and would hand a console bug `…/archive` (the whole node's data) and `DELETE`.

## Upgrade

1. `docker compose build` (new image), optionally `POST /v1/node/drain` (`202`): new tasks get `503 draining`, `/readyz` turns `503`. Drain is one-way: restart to resume.
2. `docker compose up -d` (SIGTERM, 30 s grace for in-flight tasks, then restart on the same volume). Migrations are forward-only and run at startup.
3. A new engine fingerprint (V8 / wasmtime / flags) misses `cache/`: versions rebuild lazily from `artifacts/` and `rt.lock` on the next request. Force it with `POST /v1/services/{s}/versions/{v}/rebuild`.

Single node only: there is no clustering or canary tooling in v1.

**Schema change (API keys scoped by projects and environments).** The metadata schema is now one consolidated version,
and the key scope moved from `service_ids` to `project_ids` and `environments`. A `meta.db` from an earlier build is not
migrated. For a development node, stop it, delete `data/meta.db*` (and `data-test`), start it again and create the
tenants, keys and services anew. This drops every tenant, key and service in that database.

## Branches and switches

A branch is a separate service with its own data and secrets. It takes a change through a real URL, then a switch makes the same built code live on the parent, adopted by digest, so nothing is rebuilt.

```bash
21ci branch create payments --name checkout --env-names     # payments-br-checkout; empty data, secret names only
printf %s "$TEST_KEY" | 21ci secret set payments-br-checkout API_KEY   # the branch keeps its own values
21ci push ./service --service payments --branch checkout --wait
21ci invoke payments-br-checkout /healthz --fail            # test it
21ci switch payments --from checkout --dry-run              # env names missing on the parent, trigger differences
21ci switch payments --from checkout --wait                 # flip, then follow the drain
21ci rollback payments --switch sw_…                        # if it went wrong
21ci branch delete payments checkout
```

The source version must be `ready`. Env, secrets, config and data do not move: after the switch the code runs with the parent's. Read the warnings of the dry run first: `env_missing_on_parent` names a variable the new code may expect and the parent does not have, so set it before you switch. Every flip of the live version, including `push` and `PUT /live`, has a switch row: `21ci switch status payments`.

### Drain semantics

The flip is one database transaction, so a crash leaves either the old or the new version live. New requests take the new version once the routes refresh. Tasks already admitted finish on the version they started on, and a task waiting in admission counts as running. After the flip the switch is `draining`:

| State | Meaning |
|---|---|
| `completed` | No task of the previous version is left. With `interrupted: true`, the node restarted during the drain and the old tasks died with it; queue leases expire and run on the new version (at-least-once) |
| `drain_timed_out` | The deadline passed with tasks still counted. The new version stays live. The deadline is the longest task wall or queue timeout plus 30 s, or `drain_timeout`, capped at `deploy.drain_max` (15 m) |
| `drain_timed_out` with `error: drain_unknown` | The tasks could not be counted, for example three versions busy at once. They may still have been running, or may have ended long ago |
| `failed` | Refused before the flip; the live version did not change |

A drain does not block the next flip, so a rollback never waits for it; only a switch being prepared answers `409 switch_in_progress`. `completed` can fire up to one admission early: a task between route resolution and admission is not yet counted. Open WebSockets stay connected and their next events run the new version, unless `--close-sockets` closes them with 1012. Queue batches already leased finish on the old version. A new version that fails burns delivery attempts toward the dead-letter queue, so after a rollback redrive those messages.

### Payload-skew contract

For the length of a drain, and longer for anything stored, **two versions of your code are running against one set of data**. The runtime does not translate between them. Your service has to accept:

- queue messages, stream records, durable state and stored rows written by the other version, in both directions, because a rollback makes the newer data meet the older code;
- a request answered by the new version followed by one from a client that still holds the old version's page, token or schema;
- a redelivery: delivery is at-least-once, and a message started on the old version can run again on the new one.

Add fields as optional, ignore fields you do not know, and do not reuse a field name with a new meaning.

### SQL migrations

Put `migrations/NNNN_name.sql` in a Deno upload: four digits, unique, applied in order. A WASM component has no room for files, so use `21ci push app.wasm --migrations dir` (`POST …/versions/{v}/migrations`, once per version, before it is live).

`PUT …/live` and `POST …/switch` run what is pending in the `migrating` step, in one transaction on the service's database, under the same SQLite authorizer as guest SQL, so no `ATTACH`, no `_rt_*` tables, no denied pragmas, and no `BEGIN`, `COMMIT` or `SAVEPOINT` (`transaction_control_forbidden`). They hold the database's writer while they run: other writes wait. Any failure rolls back every file and leaves the live version as it was; the `409 migration_failed` error's `details` have `{file, statement_index, line, sqlite_code, message}` (`statement_index` counts from 0). Over `deploy.migration_budget` (60 s) they roll back with `migration_timeout`.

- An applied file whose content changed is `migration_drift`; a new file sorting before an applied one is `migration_out_of_order`. Neither runs anything.
- There are no down-migrations. A version missing applied files can still go live.
- `upload?activate=after_build` (and `from-url` with `activate: "after_build"`) makes the version live once its build is ready, through `promote()`, migrations first. A failed build leaves live and the schema alone.
- `upload?activate=1` with migrations is refused (`activate_with_migrations_needs_build`): a failed build would leave the schema changed. Upload, then `21ci promote`.
- A crash after the commit leaves the files applied and the version not live; the next promote does not run them again.
- Automatic rollback after a failed build and `POST …/switches/{id}/rollback` run none. `migrate: false` (scope `data:admin`, `21ci promote --no-migrate`) flips without them, for repair.
- `21ci promote <svc> <version> --dry-run` runs them on a copy of the data, up to `deploy.dry_run_max_mb`, and throws it away. A `switch` dry run lists `pending_migrations` without running them.

### Expand and contract migrations

A promote or switch runs the target version's pending SQL migrations before the flip ([SQL migrations](#sql-migrations)). A rollback runs none and does not undo schema or data, and old code keeps running on the new schema until the drain ends. Change a schema in steps that each work with the version before:

1. **Expand.** Add the new table or nullable column; the live code ignores it. Ship this first, as its own switch.
2. **Migrate.** Write both shapes, read the old one, and backfill.
3. **Switch reads.** Read the new shape. Rolling back to step 2 still works, because both shapes are kept current.
4. **Contract.** Once no version you could roll back to uses the old shape, remove it in a later switch.

A branch's database starts empty unless `--copy-data` was used, and it is never merged into the parent. A migration tested on a branch has to run again on the parent, from the code that goes live.

### What is not recorded or recognized

- `git_sha` is not recorded yet. Audit events of a switch or rollback carry the key with the value `null`; the digest is the reliable link to the code.
- The runtime cannot recognize a console deployed as an ordinary tenant service. Its system-service refusals cover the system tenant and the reserved `sys-` slug prefix only. Such a console can be branched and switched like any service, so protect it with key scopes: see [SECURITY.md](https://github.com/21-ci/runtime/blob/main/SECURITY.md#switches-and-branches).

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
5. Async invocations are not resealed: their sealed headers and bodies open only while `RT_MASTER_KEY_PREVIOUS` is
   set. Before the last restart, drain them: let pending invocations run, and redrive or purge failed ones
   (`GET /v1/services/{s}/invocations?state=pending|failed`, `21ci invocation purge`). Any left behind fail once
   with `sealed_unreadable`.
6. Once reseal reports `failed: 0`, the old API keys are revoked and the async invocations are drained, restart
   without `RT_MASTER_KEY_PREVIOUS` and destroy the old key.

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

## Deploying from a URL

`POST /v1/services/{s}/versions/from-url` (`21ci from-url`, MCP `deploy_from_url`) has the node fetch an archive of the
source. rt runs no git and no build steps: it bundles what the archive contains, so a repo that needs a build uploads
the output of its CI job instead.

```bash
21ci from-url hello "https://api.github.com/repos/acme/app/tarball/$SHA" \
  --header "Authorization: Bearer $TOKEN" --strip-components 1 --subdir apps/hello \
  --meta "{\"git\":{\"sha\":\"$SHA\",\"ref\":\"refs/heads/main\"}}" --after-build
```

- The URL is checked like a guest's `fetch`: https only, ports, every hop's addresses, the self-denylist, the service's
  `egress` rules, the plan's `egress_default`. Its bytes count toward the tenant's daily egress quota. **Private Git hosts
  do not work**: no allow-list for them exists yet.
- With `--after-build`, read the result from `service.promoted` or `service.promote_failed` in `/v1/services/{s}/events`.
  A version that finishes after a newer one went live stays out (`superseded`). A node restart during the build drops the
  build itself: the version stays `building` until `21ci rebuild`, which then activates it.
- The stored source is a canonical tar of the narrowed tree (sorted, no timestamps), so the same tree has the same
  `source_digest` whatever the archive around it.

## External secrets

`[secrets.vault]` syncs Vault KV v2 values into the sealed store. Give the node's Vault policy read access only under its `path_template` prefix. Meta migration 7 adds `secret_refs`; it is applied at startup.
