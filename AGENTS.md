# AGENTS.md

Instructions for coding agents working **on this repository** (the runtime itself). If you are writing a service that
runs **on** the runtime, use the skills in [`skills/`](skills/README.md) instead: `21cloud-deno-service`,
`21cloud-wasm-service`, `21ci-deploy`.

## What this is

A self-hosted, multi-tenant functions runtime in Rust. One binary, `rt` (`crates/rt-server`), runs tenant services as
TypeScript/JavaScript in Deno (V8) isolates or as WASI P2 components in wasmtime, behind a control API (`/v1`, `:8081`)
and an invoke listener (`/s/{slug}`, `:8080`). `21ci` (`crates/rt-cli`) is the CLI client. License: Apache-2.0.

## Layout

| Path | Contents |
|---|---|
| `crates/rt-types` | shared IDs, task/outcome types, limits, reason codes (L0) |
| `crates/rt-config`, `rt-mem`, `rt-obs` | config parse/profiles/effective values; memory governor; logs, metrics, stats (L1) |
| `crates/rt-sched`, `rt-sec`, `rt-store` | GMP scheduler and admission; auth, keys, scopes, crypto, quotas, egress classifier; `meta.db`, `queue.db`, artifacts, per-service KV/SQL/blob (L2) |
| `crates/rt-cache`, `rt-host` | prepared-artifact cache; host capability handles (egress guard, storage facades, calls, bindings) (L3) |
| `crates/rt-build`, `rt-exec-deno`, `rt-exec-wasm` | upload validation, resolution, bundling; Deno executor; WASM executor and `std:host` host impl (L4) |
| `crates/rt-api`, `rt-triggers` | control API, invoke router, OpenAPI, MCP (`src/mcp/`); routes, schedules, queues, calls (L5) |
| `crates/rt-server` | binary `rt`: wiring, startup, drain |
| `crates/rt-cli` | binary `21ci` (no runtime crates in its dependency tree; keep it that way) |
| `crates/rt-testkit` | test support: virtual clock, mock executor, node harness, mock CDN, fixture builders |
| `js/` | guest bootstrap (`bootstrap.js`, `sqlite.js`) and the `std:*` modules (`rt_*.js`, imported as `std:*`), embedded in the Deno executor |
| `wit/rt-host.wit` | the `std:host@0.1.0` WIT package for WASM guests (samples vendor copies of it) |
| `config/` | `defconfigs/*.toml`, `schema.json` |
| `openapi/openapi.json` | generated OpenAPI 3.1 (commit it after API changes) |
| `fixtures/` | Deno and WASM guest fixtures used by tests (`fixtures/wasm/out/*.wasm` is built) |
| `tests/e2e/` | black-box shell tests against a container |
| `sample/` | runnable Deno/WASM examples, apps, operator SDK, `scripts/{deploy,invoke,svc-id}.sh` |
| `scripts/`, `mk/`, `tools/` | dev/test/coverage/e2e scripts; config make targets; Python config tooling |
| `skills/` | agent skills for people writing services on the runtime |
| `docs/` | the docs site pages (MkDocs, `mkdocs.yml` at the root) and the reference guides `api.md`, `cli.md`, `handlers.md`, `config.md`, `deploy.md`, `mcp.md`; see also `SECURITY.md` |

Layers: a crate depends only on lower layers (L0 < L1 < ... < L5). Executors never depend on each other; `rt-build`
never depends on an executor; `rt-api` does not depend on executors or `rt-host`. Keep the direction when adding code.

## Build and test

Toolchain: Rust 1.95 from `rust-toolchain.toml` (components rustfmt, clippy, llvm-tools; target `wasm32-wasip2`),
Python 3 for `tools/`, Docker + `curl` + `jq` for e2e.

| Command | Does |
|---|---|
| `make check` | `cargo check --workspace --all-targets` |
| `make lint` | `cargo fmt --all -- --check` + `cargo clippy --workspace --all-targets -- -D warnings` (what CI runs) |
| `make fmt` | `cargo fmt --all` |
| `make test` | `scripts/test.sh`: WASM fixtures, `cargo test --workspace`, Python tool tests |
| `scripts/test.sh --fast` | skip fixture rebuild (if built), doc-tests and Python tests |
| `scripts/test.sh --fast -- -p rt-store` | one crate; extra args go to `cargo test` |
| `cargo test -p rt-store --test it kv_cas::` | one module of a crate's integration-test binary |
| `make coverage` | `scripts/coverage.sh [MODULE...]`: tests under `cargo-llvm-cov`, per-module table, `target/coverage/lcov.info` (`MODULES="sched store" ARGS="--html --missing"`) |
| `make ci-fast` / `make ci-local` | `scripts/ci-local.sh`: the CI checks locally (lint + security scanners + docs; `ci-local` adds all tests under coverage and the smokes) |
| `make test-perf` | `#[ignore = "perf"]` benchmarks, release mode |
| `make e2e` | docker build + compose + `tests/e2e/run_all.sh` + teardown |
| `make openapi` | regenerate `openapi/openapi.json`; `scripts/openapi.sh --check` fails on drift |
| `make dev` | local node on `config/defconfigs/dev.toml`, root token `dev-root-token` |
| `make cli` | `target/debug/21ci` |

Environment caveats:
- **Disk is tight.** V8 and wasmtime make builds large. Use the one shared `target/` (`.cargo/config.toml`); never set
  another `CARGO_TARGET_DIR` or create extra target dirs. Build only the crate you need (`-p`). `scripts/coverage.sh
  --prune` deletes the instrumented build afterwards.
- **First build is slow**: it downloads a prebuilt `rusty_v8` and compiles the workspace (20+ minutes in Docker).
- **Several agents may share this checkout.** Do not `git stash`, `reset`, `checkout` or `clean` other people's changes;
  touch only the files your task needs; commit only your own files.
- WASM fixtures need the `wasm32-wasip2` target (`scripts/build-fixtures.sh`); `rt-exec-wasm` tests load
  `fixtures/wasm/out/*.wasm`.
- Coverage needs `cargo-llvm-cov` and the `llvm-tools` component; Python coverage needs `pip install coverage`.
- System logs: `RT_LOG` (then `RUST_LOG`), e.g. `RT_LOG=info,rt_sched=debug`.

## Conventions

- **Commits**: Conventional Commits, `type(scope): summary` in lowercase imperative, e.g. `fix(storage): ...`,
  `fix(deno): ...`, `test(api): ...`, `chore: ...`. Scopes used: `api`, `build`, `deno`, `host`, `queue`, `sched`,
  `storage`, `ws`, ... One logical change per commit.
- **Tests**: integration tests live in each crate's `tests/it/` module tree: one test binary per crate
  (`tests/it/main.rs` declares the modules, shared helpers in `tests/common/`), because every V8/wasmtime-linked test
  executable costs tens of MB. Add a module there, not a new `tests/*.rs` target. Unit tests stay in `src/` under
  `#[cfg(test)]`.
- **Coverage: the main logic is covered** — request paths, rules, limits, user-visible errors, auth/tenancy checks, state changes — and CI fails a module below 90 % lines (`--fail-under 90`). Exotic OS/internal failure paths may stay uncovered. A line that cannot run in a test is
  excluded in the source with a reason: `// LCOV_EXCL_LINE: <reason>`, or a block between
  `// LCOV_EXCL_START: <reason>` and `// LCOV_EXCL_STOP`. `scripts/coverage.sh --exclusions` lists them; keep them
  rare and justified. `js/bootstrap.js` is not measured (covered through the Deno executor tests).
- **Before you finish**: `make lint` and `make test` (or the `--fast` / `-p` subset for the crates you touched) must
  pass; run `make coverage MODULES="<crate>"` for touched crates.
- **Guest-facing contracts** (`js/*.js`, `wit/rt-host.wit`, `crates/rt-types` reason codes, API shapes): change docs
  (`docs/handlers.md`, `docs/api.md`, `docs/cli.md`), `openapi/openapi.json`, samples and `skills/` together. Error
  codes are stable strings; never put host paths or driver text into guest-visible errors.
- **Config keys**: unknown keys fail startup; when adding one, update `config/schema.json`, the defconfigs that need
  it and `docs/config.md` in the same change.
- **Security**: read [`SECURITY.md`](SECURITY.md) before touching auth, egress, the sandbox, storage isolation or
  secrets. Never log secret values or guest payloads.
- Style: `rustfmt.toml` settings, `clippy -D warnings`.

## Docs

Public docs: [README.md](README.md), [SECURITY.md](SECURITY.md), [RELEASING.md](RELEASING.md), [docs/api.md](docs/api.md), [docs/cli.md](docs/cli.md),
[docs/handlers.md](docs/handlers.md), [docs/config.md](docs/config.md), [docs/deploy.md](docs/deploy.md),
[docs/mcp.md](docs/mcp.md). Site: https://21-ci.github.io/runtime/ . Index for LLMs: [llms.txt](llms.txt).
