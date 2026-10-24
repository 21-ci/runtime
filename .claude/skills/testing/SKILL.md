---
name: testing
description: Use when writing or running tests for this runtime - choosing where a test goes (unit in src, the crate's tests/it module tree, rt-server api/flows/process binaries, tests/e2e, sample smokes), using rt-testkit (VirtualClock, SimRuntime, MockExecutor, NodeHarness, MockCdn) or rt-store fault injection, doing test-first bug fixes, covering the main logic with scripts/coverage.sh (CI floor 90 % lines per module) and LCOV_EXCL reasons, or saving disk/time while building tests on this V8+wasmtime workspace.
---

# Testing the runtime

Base rules are in [`AGENTS.md`](../../../AGENTS.md) ("Build and test", "Conventions") and CONTRIBUTING.md "Tests".
This skill covers the workflow.

## Test-first loop

1. **Name the rule.** Write it as one sentence, e.g. "a revoked key cannot mint a child". Commit subjects and test
   names here state rules.
2. **List the cases before writing code.**
   - Should-pass: the normal path, plus the neighbour of every boundary (exactly at the cap, empty input, the
     operator, an unrestricted key).
   - Should-fail: one per refusal, each with its exact status and stable code (`validation_failed`, `too_large`,
     `insufficient_scope`, `reserved`, `not_found`, ...). Cover over the cap, malformed, the wrong tenant, the
     wrong project/env, expired/revoked, concurrent or racing, killed mid-way, an overflowing number, and a node
     restart if state persists.
   - Both executors when the surface exists in Deno and WASM.
3. **Write the failing test at the lowest layer that shows the behaviour.** Run only it and see it fail.
4. Implement, run the module, then run the crate.
5. Run `make lint` and `make coverage MODULES="<crate>"`. The logic you touched (request paths, rules, limits, user-visible errors, auth checks) must be covered, and each module stays at or above the 90 % CI floor.

Name tests `ac_<area>_<nn>_<rule>` after a spec acceptance id (e.g. `ac_dno_34_*`, `ac_wsm_34_*`,
`ac_api_22_*`), or `<id>_<rule>` from an edge-case catalogue (e.g. `ten_005_*`, `key_028_029_*`). Give the test a
doc comment with the rule.

## Where a test goes

| Kind | Location | Run one |
|---|---|---|
| Unit (private items, parsers, math) | `#[cfg(test)] mod tests` in the same `src/` file | `cargo test -p rt-build --lib extract::` |
| Integration, crates with a module tree | `crates/{rt-build,rt-cli,rt-exec-deno,rt-exec-wasm,rt-store}/tests/it/<module>.rs`, registered in `tests/it/main.rs`; helpers in `tests/common/mod.rs` | `cargo test -p rt-store --test it kv_cas::` |
| Integration, crates with flat files | `crates/{rt-cache,rt-config,rt-host,rt-mem,rt-obs,rt-sched,rt-sec,rt-triggers,rt-types}/tests/*.rs` | `cargo test -p rt-sched --test admission` |
| Node, in-process API | `crates/rt-server/tests/api/<m>.rs` (registered in `tests/api.rs`) | `cargo test -p rt-server --test api projects::` |
| Node, flows (triggers, ws, durable, restart, ingress) | `crates/rt-server/tests/flows/<m>.rs` (`tests/flows.rs`) | `cargo test -p rt-server --test flows websockets` |
| Real `rt` child process | `crates/rt-server/tests/process/node.rs` | `cargo test -p rt-server --test process` |
| Boundary cases | `edge_*` modules: `tests/api/edge_{control,keys,mcp}.rs`, `tests/flows/edge_invoke.rs` | |
| Black box against the Docker image | `tests/e2e/NN_*.sh` (uses `lib.sh`; run by `run_all.sh`) | `make e2e` (`scripts/e2e.sh --no-build --keep`) |
| Sample apps against a real node | `sample/apps/<app>/smoke.sh`, run by `sample/apps/run-smokes.sh [rt-binary] [app...]` (`smoke-node.toml`, mock operator API) | `sample/apps/run-smokes.sh target/debug/rt chat-room` |
| Config tooling (Python) | `tools/test_config.py`, `tools/test_coverage_report.py` | `make test-config` |

**Never add a new test binary** to a crate that links V8 or wasmtime. Each one is tens of MB and links again; add a
module to the existing tree instead. In flat-file crates, prefer extending an existing file over a new one.

## Helpers

- **`rt-testkit`** (`crates/rt-testkit/src/`, dev-dependency only):
  - `VirtualClock` (`clock.rs`): deterministic time. Use `step_unix_ms`, `set_unix_ms` and `cpu_of` instead of
    sleeping.
  - `SimRuntime` / `SimConfig` (`sim.rs`): a seeded, single-thread scheduler simulation (`.seed(7)`, `step`,
    `submit`). Use it for scheduler properties.
  - `MockExecutor` plus the script DSL `cpu`, `io`, `yield_now`, `alloc`, `call`, `panic`, `loop_forever` (`mock.rs`).
  - `TestGovernor`: a budgeted `MemoryGovernor`.
  - `NodeHarness`: an in-process node on random ports with a temp data dir.
  - `MockCdn`: a local HTTP server for build imports.
  - `fixtures` and `spec` (`SpecBuilder`, deterministic ids).
- **Fault injection** (`crates/rt-store/src/faults.rs`): `Faults::fail_next(n, Fault::{DiskFull, Busy, Io, Corrupt})`.
  Attach it with `inject_faults(Arc<Faults>)` on `SqliteMetaStore`, `SqliteQueueStore` or `LocalServiceData`.
  Example: `crates/rt-store/tests/it/faults.rs`.
- **rt-server** (`crates/rt-server/tests/common/mod.rs`):
  - nodes: `start_node()`, `start_node_with(tweak)`, `Restartable`;
  - tenants and objects: `new_tenant(&node, "pro")`, `new_tenant_scopes`, `create_project`, `create_service`,
    `ship` (deploy);
  - requests: `get/post/patch/delete`, `root`, `root_for(&node, tenant, ..)` (operator plus `x-rt-tenant`), `call`;
  - polling: `eventually`.
- **Executors**:
  - `crates/rt-exec-deno/tests/common/mod.rs` has `Harness`, `cfg0()`, `spec()`. Guest source can be inline.
  - `crates/rt-exec-wasm/tests/common/mod.rs` has `Rig`, `Req`, `prepared("<fixture>")`, which loads
    `fixtures/wasm/out/<fixture>.wasm`. Build those with `make fixtures`.
- **CLI**: `crates/rt-cli/tests/common/mod.rs` has `Ctx`, with an httpmock control API and `assert_cmd`.

**Time and flakiness.**
- Prefer `VirtualClock` or `eventually` to `sleep`.
- A test that fails only under load is rerun alone before it counts as a failure. A real flake gets an issue, not
  a retry loop.
- Perf assertions belong in `#[ignore = "perf"]` tests (see the `performance` skill).

## Coverage (CI: `scripts/coverage.sh --fail-under 90 --floor tools=45 --exclusions --prune`)

- `make coverage MODULES="store" ARGS="--missing"` (or `scripts/coverage.sh rt-store --missing`) lists the uncovered
  ranges. `--html` writes `target/coverage/html`, and `--unit` / `--integration` split the run.
- Cover a line with a test first. Exclude it only if it cannot run in a test, and then always give a reason:
  - `// LCOV_EXCL_LINE: <reason>`, or
  - a block between `// LCOV_EXCL_START: <reason>` and `// LCOV_EXCL_STOP`.

  `tools/coverage_report.py` rejects a marker without a reason. `scripts/coverage.sh --exclusions` lists every
  marker; reviewers read that list, so keep it short. Examples of good reasons: the `bindgen!` closing line, an OS
  failure that cannot be provoked.
- `js/bootstrap.js` is not measured; it is covered through the Deno executor tests.

## Saving disk and time (the checkout is shared, and V8 + wasmtime are large)

- Build and test one crate: `cargo test -p <crate>`. `scripts/test.sh --fast -- -p <crate>` skips the fixture
  rebuild, doc-tests and Python.
- Use the one shared `target/` only. Never set another `CARGO_TARGET_DIR`.
- Drop debug info for test builds:
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p <crate>`. CI and `scripts/coverage.sh` do
  this; set `RT_COV_DEBUG=1` to keep it for coverage.
- `scripts/coverage.sh --prune` deletes `target/llvm-cov-target` afterwards.
- Old hashed test executables pile up in `target/debug/deps/` (`it-<hash>`, `api-<hash>`, `flows-<hash>`, ...).
  Check with `du -sh target/*`. Delete only stale **executables** that you built:
  `find target/debug/deps -maxdepth 1 -type f -perm -u+x -mtime +1 -delete`. Do not run `cargo clean`, and do not
  delete `.rlib`/`.rmeta`, while other agents are building: their builds would start over.
- Do not `git stash`, `reset`, `checkout` or `clean`, because other agents' uncommitted work is in this tree.
