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
