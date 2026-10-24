---
name: performance
description: Use when changing code on the runtime's request or task hot path (rt-sched admission/placement, Deno isolate or WASM instance warm pools, rt-mem reservations, rt-store group commit and the blocking pool, rt-host egress client pool, rt-cache), when adding a lock/allocation/blocking call on that path, when a PR claims a speedup, or when investigating a throughput/latency/RSS regression. Covers how to measure here (bench/, scripts/bench.sh, perf-ignored tests) and what numbers a PR must report.
---

# Performance work on the runtime

Measure first and report before/after numbers on the same machine. Do not claim a speedup without numbers. Release
builds only: debug numbers are meaningless here.

## Measuring tools

| Tool | What it measures | Needs |
|---|---|---|
| `bench/bench.sh <rt-binary> <label>` | Starts a node on `bench/rt.toml` (ports 18080-18082, 19090, token `bench-root-token`) and deploys Deno `hello` (`sample/deno/hello`), Deno `sqltodo` (`sample/deno/sql-todo`) and WASM `whello` (`fixtures/wasm/out/hello.wasm`). Runs `oha` at `BENCH_C` (default `1 16 64 256`) for `BENCH_SECS` (6). Writes `bench/out/<label>.txt`: rps, p50, p99 and status codes, plus thread count and `rt_deno_isolate_create_seconds`, `rt_admission_rejects_total`, `rt_mem_committed_bytes` | `cargo build --release -p rt-server`, `make fixtures`, `oha`, `jq`, `curl` |
| `BENCH_CONFIG=bench/rt-warm.toml bench/bench.sh ...` | The same with warm reuse (`deno.reuse_isolates`, `deno.isolate_pool_per_p = 4`, `wasm.reuse_instances`) | same |
| `bench/resources.sh <bin> [N=20]` | Idle RSS, RSS per warm instance (N Deno + N WASM with `min_instances=1`), CPU per request. Output: `bench/out/resources.txt` | `python3`, `jq` |
| `bench/ws.sh <bin> [N=2000]` | WebSocket echo round trip (warm objects on and off), RSS per idle socket. Output: `bench/out/ws.txt` | `cargo build -p rt-triggers --example ws_spike`, `ulimit -n 65536` |
| `scripts/bench.sh prf-01\|prf-02\|prf-08\|url` (`make bench` lists them) | SLO scenarios against an already running node: `prf-01` WASM hello ≥50k rps and p99 ≤10 ms; `prf-02` Deno hello ≥4k rps and p99 ≤25 ms; `prf-08` mixed load. Defaults: 30 s warm-up, 120 s, 3 runs, median. `--quick` for a smoke run. Output: `bench/results/<date>-<sha>.json` | `oha` (without it, a curl-loop smoke only) |
| `make test-perf` = `scripts/test.sh --perf-only` | `#[ignore]` tests in release with `--nocapture`, for every crate that has an `#[ignore = "perf"]` test (override with `RT_PERF_CRATES`) | fixtures |

Narrow the run with `RT_PERF_CRATES="rt-exec-deno rt-store" scripts/test.sh --perf-only`. `--ignored` runs every
ignored test of those crates, not only the `"perf"` ones.

Perf-ignored tests, which act as micro-benchmarks with SLO asserts:
- `crates/rt-sched/tests/wakeups.rs`: `ac_sch_10_wake_latency_*`
- `crates/rt-sched/tests/placement.rs`: `ac_sch_23_schedules_at_least_one_million_noop_tasks_per_second`
- `crates/rt-exec-wasm/tests/it/wsm_perf.rs`: `ac_wsm_10_hello_overhead_within_slo_3_and_ceiling_slo_1`
- `crates/rt-exec-deno/tests/it/perf.rs`: `ac_dno_14_overhead_within_slo_3`, `d_a4_probe_reuse_vs_fresh_isolates`
- `crates/rt-exec-deno/tests/it/sdk_sqlite.rs`: `perf_point_select_async_rt_sql_vs_sync_rt_sqlite`
- `crates/rt-store/tests/it/{sync_sql,sql_stmt_cache}.rs`: `perf_point_select_*`

Lock-free scheduler queues also have loom tests:
`RUSTFLAGS="--cfg loom" cargo test -p rt-sched --release --test loom` (see `.cargo/config.toml`).

Disk is tight, so build release only for `-p rt-server` (and `rt-triggers --example ws_spike` for `ws.sh`). Never
use a second `CARGO_TARGET_DIR`.

## Hot paths and their rules

| Path | Where | Rules |
|---|---|---|
| Admission | `crates/rt-sched/src/admission.rs` (module doc has the check order; `Admission::admit`, `release`, `tick`); `Scheduler::submit` in `scheduler.rs` | Over capacity a request **queues**, never gets 429 (decision D-A3). Refusals are only `508 call_depth`, `503 draining/shed/overloaded/queue_timeout`. Calls (`SlotClass::Call`) must not deadlock behind their parents. Keep `admit` O(1): no store I/O, no allocation per request beyond the ticket. |
| Scheduler core | `rt-sched/src/{queues,steal,place,machine,processor,sysmon}.rs` | No blocking call on a machine thread; blocking work goes to the blocking pool. CPU limits are judged on thread CPU time, not poll wall time (`3af5687`). |
| Deno warm pool | `crates/rt-exec-deno/src/warm.rs` (`WarmStore::take/park/trim/shed_to`); `executor.rs` refills between tasks; `isolate_pool_per_p` in `config.rs` | A fresh isolate costs about 2.9 ms of CPU (bench/RESULTS.md); the pool only moves that off the request path. Reuse is opt-in (`reuse_isolates`, the `min_latency` profile). The pool key never crosses tenants. |
| WASM instance pool | `crates/rt-exec-wasm/src/warm.rs` (`IdleSlots`, `IdlePool::take/scale_down`); `reuse_instances` in `executor.rs`; `engine.rs` (`PoolSizing`) | Pooling-allocator slots = `wasm_budget / plan max memory`. |
| Memory accounting | `crates/rt-mem/src/governor.rs` (`MemoryGovernor::reserve`, `Governor::tick`), `reservation.rs` (`Reservation`, RAII `try_grow`/`shrink`) | Reserve with CAS, then release on drop. The release profile keeps unwinding for this, so do not add `panic = "abort"`. Grow in steps (8 MB heap steps for V8), not per allocation. |
| Service data / SQLite | `crates/rt-store/src/data.rs` (coalescing writer queue, `run_group`, `TX_CAP`); `durable.rs` (savepoint per job); `blocking.rs` (`BlockingRunner`, CPU charged to the task); fair pool in `rt-host/src/blocking.rs` | Writes coalesce into one group commit (B3.2, `ca4b76d`; test `crates/rt-store/tests/it/group_commit.rs`). Reads use the reader pool (`tests/it/reader_pool.rs`). No `meta.db` round trip per invocation (`3727bb3`): use in-memory route and spec state. Under contention one tenant gets at most `plan.blocking_share` of the blocking threads. The pool is work-conserving, so idle threads may be borrowed (see the module doc). |
| Egress | `crates/rt-host/src/egress.rs` (`GuardedEgress`, `ClientPool`, `POOL_IDLE_PER_CLIENT = 8`) | There is one DNS lookup per request and no DNS cache, by design (pinning). Reuse pooled clients keyed per tenant and pinned IP. Do not add a cache that crosses tenants. |
| Prepared artifacts | `crates/rt-cache/src/cache.rs` (`PreparedCache`, single-flight load), `policy.rs` (W-TinyLFU), `t2.rs` (MAC-checked disk tier) | Load single-flight; route eager builds through it too (`6ba0615`). |

**Locks and async.** rt-api, rt-host, rt-sec and rt-server enforce
`#![warn(clippy::await_holding_lock, ...)]`, and CI turns that into an error. Apply the same rule everywhere:
- Never hold a `std` or `parking_lot` guard across `.await`.
- Keep critical sections to map lookups.
- Shard hot maps, as the failed-auth limiter does with 16 shards.
- Prefer atomics for counters.

**Allocation.** On the per-request path:
- Avoid `format!` and `String` building for log or metric labels. Rely on `tracing` level filtering.
- Avoid cloning bodies: pass `Bytes`.
- Avoid `Vec` growth inside a loop without `with_capacity`.

## What a regression looks like

- `rt_admission_rejects_total` > 0 or 503s at c=256, where `bench/RESULTS.md` has "all 200".
- Thread count after a run far above about 58: threads are parked on a mutex in blocking code, as in the
  341-thread "before" row.
- `rt_deno_isolate_create_seconds_count` growing with requests in a warm run: reuse broke and every request makes an
  isolate.
- p50 at c=1 rising: there is per-request overhead, often store I/O or an allocation on the path.
- RSS per warm instance or per idle socket up in `resources.txt` / `ws.txt`.
- An SLO-asserting perf test (`ac_wsm_10`, `ac_dno_14`, `ac_sch_10`, `ac_sch_23`) failing in release.

## What the PR must report

1. **Machine** (CPU, cores, OS), **commits** for before and after, the config (`bench/rt.toml` or `rt-warm.toml`),
   and the profile.
2. Run **before** and **after** back to back on the same machine, release builds, nothing else heavy running.
   Rebuild the before binary from its commit (`git worktree` only if disk allows; otherwise build, copy
   `target/release/rt` aside, then rebuild).
3. Put the `bench/bench.sh` table in the format of `bench/RESULTS.md`: `Service | c | before | after`, with rps and
   p50 per cell, non-200 counts, and the thread-count row. Add `resources.sh` / `ws.sh` deltas if memory or sockets
   are affected.
4. Include the output of the relevant perf-ignored tests (`--nocapture` prints their measurements).
5. For a lasting change of the published numbers, add a dated section to `bench/RESULTS.md` with the method and notes,
   as the 2026-10-07 section does.
6. If you traded memory for latency, report both sides.
