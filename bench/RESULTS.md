# Benchmark results

Method: `bench/bench.sh` . Release builds, MacBook Air M4 (10 cores, fanless), oha on the same
machine, unlimited `bench` plan, `balanced` profile, 6 s per pass.

- **before** is the Part A binary (`6c610a2`), re-run right before the **after** runs so both see the same machine
  state.
- **after** is `c51c30a` with the default config.
- **after, warm** uses `bench/rt-warm.toml`: `deno.reuse_isolates = true`, `deno.isolate_pool_per_p = 4` and
  `wasm.reuse_instances = true`. These are the per-service opt-ins (decision D-A4), which `min_latency` turns on by
  default.

Cells are successful responses per second, with p50 in parentheses.

## 2026-10-07

| Service | c | before | after | after, warm |
|---|---|---|---|---|
| Deno `hello` | 1 | 350 (2.72 ms) | 458 (2.05 ms) | **6,563 (0.11 ms)** |
| | 16 | 865 (17.7 ms) | 890 (17.6 ms) | **25,274 (0.52 ms)** |
| | 64 | 1,077 (57.7 ms) | 1,012 (59.4 ms) | **21,137 (1.57 ms)** |
| | 256 | 338 ok/s, **99.6 % 429** | 1,193, all 200 | **22,672, all 200** |
| Deno `sql-todo` GET | 1 | 223 | 272 | 4,645 |
| | 16 | 479 | 672 | 10,754 |
| | 64 | 537 | 983 | 5,446 |
| | 256 | 100 ok/s, **99.9 % 429** | 890, all 200 | 4,642, all 200 |
| WASM `hello` | 1 | 6,775 (0.13 ms) | 9,985 (0.08 ms) | 10,847 (0.08 ms) |
| | 16 | 19,066 (0.78 ms) | 30,929 (0.39 ms) | **47,656 (0.28 ms)** |
| | 64 | 16,521 | 34,814 | **54,022** |
| | 256 | 12,301 ok/s, **83 % 429** | 35,904, all 200 | **61,754, all 200** |
| Node threads after the run | | 341 | 58 | 58 |

### Notes

- **WASM, default config.** Throughput roughly doubles at c=16 and c=64, and thread count falls from 341 to 58.
  Taking `meta.db` off the request path (B2.1) removed the ~310 blocking threads that were parked on its mutex.
- **No more 429s.** Requests over capacity now wait in a queue (D-A3), so every pass answered 200.
- **Deno, default config.** Every request creates a fresh isolate (~2.9 ms of CPU each), which caps throughput at
  about 1k rps; the isolate pool only moves that cost off the request path. Warm reuse removes it: only 9 isolates
  were created across the whole warm run.
- **`sql-todo` drops after c=16 in the warm run.** The fair blocking pool caps one tenant at
  `floor(blocking_share × threads)` = 2 of 10 threads, even when no other tenant is busy. This is a review follow-up:
  make the pool work-conserving.
- `sql-todo` changed between the two binaries: its GET is read-only now (B3.5), so the Deno `sql-todo` rows are not
  a like-for-like comparison.

## 2026-11-05: branching services (design §8)

MacBook Air M4 (10 cores, fanless), `bench/rt.toml`, 6 s per pass, release builds. **before** is `f3b7ede`, built from
`git archive`. **after** is `1964bfa` plus the then-uncommitted working tree (T7, F1, F2). Cells are the median of 3 runs:
rps, then p99 in ms. Every pass answered 200, with no admission rejects and 62 threads in both binaries.

Thresholds: rps >= 0.98x and p99 <= 1.05x. Round A ran before, after, 3 times, all levels. Round B ran after, before,
before, after, after, before, without c=16.

| Service | c | A before | A after | A rps, p99 ratio | B before | B after | B rps, p99 ratio |
|---|---|---|---|---|---|---|---|
| Deno `hello` | 1 | 605, 2.15 | 627, 2.23 | 1.036, 1.037 | 660, 2.10 | 611, 2.17 | **0.926**, 1.033 |
| | 16 | 1,259, 22.59 | 1,294, 23.11 | 1.028, 1.023 | | | |
| | 64 | 1,322, 83.88 | 1,269, 76.05 | **0.960**, 0.907 | 1,450, 58.88 | 1,408, 63.70 | **0.971**, **1.082** |
| | 256 | 1,353, 296.95 | 1,311, 325.69 | **0.969**, **1.097** | 1,255, 347.51 | 1,429, 280.06 | 1.139, 0.806 |
| Deno `sql-todo` GET | 1 | 361, 3.52 | 343, 4.30 | **0.950**, **1.222** | 358, 3.29 | 355, 3.48 | 0.992, **1.058** |
| | 16 | 1,058, 32.69 | 1,128, 25.19 | 1.066, 0.771 | | | |
| | 64 | 1,160, 110.48 | 1,173, 101.67 | 1.011, 0.920 | 1,205, 98.33 | 1,231, 83.82 | 1.022, 0.852 |
| | 256 | 1,227, 330.42 | 1,174, 352.04 | **0.957**, **1.065** | 1,162, 285.96 | 1,265, 268.70 | 1.089, 0.940 |
| WASM `hello` | 1 | 11,215, 0.15 | 11,136, 0.11 | 0.993, 0.733 | 11,613, 0.10 | 11,391, 0.11 | 0.981, **1.100** |
| | 16 | 38,174, 1.06 | 38,532, 1.05 | 1.009, 0.991 | | | |
| | 64 | 37,664, 6.40 | 39,488, 5.35 | 1.048, 0.836 | 41,355, 5.42 | 44,712, 4.53 | 1.081, 0.836 |
| | 256 | 42,723, 14.79 | 37,564, 14.82 | **0.879**, 1.002 | 40,913, 16.28 | 43,351, 14.78 | 1.060, 0.908 |

Bold misses a threshold.

### Verdict

**The §8 thresholds are not met as written: 5 of 12 lines fail in round A, 4 of 9 in round B.** No regression is shown
either, and this bench cannot settle it. The same binary varies by up to about 30 % in rps between runs of one round, far
more than the 2 % threshold, and the first run after a rest tends to be fastest. Reversing the order turned Deno
`hello` c=256, `sql-todo` c=256 and WASM c=256 from misses into wins. Lines that miss in both rounds: Deno `hello` c=64
and `sql-todo` c=1. Their ratios are within the run-to-run spread, but a small real cost there is not ruled out.
WASM c=1 p99 is one step of the 0.01 ms rounding.

### Hot path

- `admit_release_with_version_counter_under_200ns`, release, re-run after T3: 37, 22, 23, 23, 22 ns (pass, limit 200).
- The per-invoke `noindex` lookup (`crates/rt-api/src/invoke.rs`) is one `ArcSwap` load and one `HashMap` get, with no
  lock across an await and no store I/O. `Specs::http_task` already does the same lookup per request. It was read, not
  timed on its own; such a lookup should be well under 1 % of the WASM c=1 p50 (80 us).

## Stats ring compaction (2026-11-05)

`cargo test -p rt-obs --release --test stats_memory -- --ignored --nocapture`, MacBook Air M4. Heap held by one service
after 24 h of traffic (counting allocator, not process RSS), exec latencies 20-100 ms.

| Load | 1-minute ring | Compacted ring | Saving | Slowest merge, before / after |
|---|---|---|---|---|
| 60 req/min | 32.8 MB, 1,439 buckets | 2.86 MB, 151 buckets | 91.3 % | 72 µs / 43 µs |
| 6,000 req/min | 37.2 MB, 1,439 buckets | 3.32 MB, 151 buckets | 91.1 % | 202 µs / 207 µs |
