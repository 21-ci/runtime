---
title: Optimize your service
description: How to make a service cheaper and faster on the runtime: cold versus warm, reuse-safe code, cheap module top levels, small bundles, batching and transactions, streaming, limits and WASM size.
---

# Optimize your service

This guide turns the cost model in [Performance characteristics](../concepts/performance.md) into changes you can make
to your own code and settings. Each section says what to do, why it helps (the mechanism), and how to check that it did.
Measure before and after: a change that feels faster is not evidence.

## Start with the cheat sheet

| Symptom | Likely cause | First thing to try |
|---|---|---|
| First request after a deploy is slow | Lazy build on the first request | Deploy with `21ci push --wait` (eager build) |
| Every request pays a fixed cost, even for a tiny handler | Fresh isolate or instance per request | Warm reuse, after making the code reuse-safe |
| High `init` in `server-timing` | Heavy module top level or start code | Move work into the handler, initialize lazily |
| High `queue` in `server-timing` | Load above the service's concurrency | Shorten tasks, parallelize inside the task, check storage contention before adding concurrency |
| High `exec`, mostly waiting | Slow upstream or many sequential calls | Run independent calls in parallel, add timeouts, cache results |
| High `exec`, mostly CPU | Parsing, serializing, crypto, big loops in JS | Do less, stream, or move the hot path to WASM |
| `502 oom` | Buffering large bodies, big arrays | Stream; page results; raise `memory_mb` within the plan |
| `502 cpu_limit` | Work per request exceeds `cpu_ms` | Cut the work, move it to a queue or schedule (higher defaults), raise `cpu_ms` within the plan |
| `504 timeout` | Wall time exceeded | Return earlier; stream; hand slow work to a queue |
| Storage calls dominate | Many small writes, no indexes, N+1 reads | Batch, transactions, indexes, `kv.incr` |
| Many short SQL statements per request | A thread hand-off per `std:sql` call | Synchronous `std:sqlite` for the short ones |
| First request after a quiet period is slow | The service was scaled to zero | `--min-instances 1` for latency-sensitive services |

## Read the timings

Every response carries `server-timing: queue, wait_runtime, init, exec` (milliseconds). Use it first:

```bash
curl -sS -o /dev/null -D - "$RT_INVOKE/s/hello/" | grep -i -E 'server-timing|x-rt-task-id'
```

```console
server-timing: queue;dur=0.004, wait_runtime;dur=0.000, init;dur=0.120, exec;dur=0.310
x-rt-task-id: task_0000000000000042
```

`21ci stats <svc> --window 1h` shows p50, p95 and p99 of the total and of each phase, the outcome counts and the
service's admission state (its adaptive concurrency limit, running and queued tasks). `21ci logs <svc> --task
task_0000000000000042` shows the log lines of one request. See [Logs, stats and usage](logs-and-usage.md).

| Phase is large | It means |
|---|---|
| `queue` | The service waited for a run slot. Load, or a low `scaling.max_instances` |
| `wait_runtime` | The version was built or loaded on this request |
| `init` | An isolate or instance was created and your top-level code ran |
| `exec` | Your handler, including everything it awaited |

## Cold versus warm

A request can be cold at four levels (see [Performance characteristics](../concepts/performance.md#the-four-levels-of-cold)).
What you control:

**Version not built.** Build at deploy so no request is the first to wait. `21ci push --wait` builds eagerly and
returns when the version is ready (exit code 1 if it failed). Heavy bundles and big components should always be built
this way, because a request waits for a lazy build only as long as the queue timeout.

**Built, not in memory.** Services that receive traffic stay in memory until `cache.idle_ttl` passes. For a service that
is hit rarely but must be fast, the operator can raise `cache.idle_ttl` or enable `cache.preload` for it (both profile
dependent; see [Performance tuning](performance.md)).

**No warm sandbox.** Turn on warm reuse for services whose per-request creation cost dominates:

```bash
21ci service update hello --reuse-isolates true      # Deno
21ci service update edge  --reuse-instances true     # WASM
```

Reuse is the single biggest lever for small Deno handlers, and for WASM components with heavy start code. It also
changes semantics, so make the code reuse-safe first.

**Scaled to zero.** After its idle TTL a service keeps no warm instances. For a latency-sensitive service with gaps in
its traffic, keep a floor:

```bash
21ci service update checkout --min-instances 1
```

The node then pre-loads the live version at start and on every deploy and keeps at least that many instances ready,
even when idle. Each warm instance holds memory, and the value is capped by the plan's `max_instances`. See
[Instances and autoscaling](../concepts/scheduler.md#instances-and-autoscaling).

## Write reuse-safe code

With reuse on, the module's top level runs once per kept isolate (or the component's start code once per kept
instance), and module state persists between requests, possibly of different users, on whichever isolate the next
request lands on. A reused sandbox is kept only after a clean finish, and is dropped on any error, kill, limit, env
change, new version, idle time, 1,000 uses or memory pressure.

Rules that make code correct both with and without reuse:

1. **Never keep per-request or per-user data in module scope.** Anything stored in a global is visible to the next
   request.
2. **Treat module state as a cache that can vanish.** Anything that must persist goes to `std:kv`, `std:sql` or
   `std:blob`. A counter in a variable is never a correct counter; `kv.incr` is.
3. **Make initialization idempotent and lazy.** Do it on first use, not at import time, and tolerate running it again.
4. **Do not leave work running.** Pending timers, unawaited promises and unhandled rejections stop the sandbox from
   being reused; use `ctx.waitUntil()` for work that must outlive the response.
5. **Do not rely on reuse for correctness.** Requests may land on different isolates, and a fresh one may appear at
   any time.

```ts title="main.ts"
// Safe in both modes: the cache is a pure function of its input and may vanish.
const templateCache = new Map<string, (data: unknown) => string>();

function template(name: string) {
  let t = templateCache.get(name);
  if (!t) {
    t = compile(name);          // runs once per kept isolate, or once per request without reuse
    templateCache.set(name, t);
  }
  return t;
}

// Unsafe: leaks one user's data to the next request when reuse is on.
// let currentUser: string | undefined;
```

## Keep the module top level cheap

Top-level code runs on every fresh isolate. With the default of a fresh isolate per request, that is every request.

```ts title="main.ts"
// Slow start: everything happens at import time, on every fresh isolate.
import { buildRouter } from "./router.ts";
const bigTable = JSON.parse(await (await fetch("https://example.com/big.json")).text());
const router = buildRouter(bigTable);

// Cheap start: nothing at import time; work happens on first use and is cached.
let router: ReturnType<typeof import("./router.ts").buildRouter> | undefined;
export default async function (req: Request): Promise<Response> {
  router ??= (await import("./router.ts")).buildRouter();   // bundled; loaded on first use
  return router.handle(req);
}
```

Do not do network I/O at the top level: it delays the handler on every fresh isolate, and it may be denied during
pre-instantiation of WASM components. Keep static tables small, and avoid compiling many regular expressions or
schemas at import time; build the ones a request needs when it needs them.

## Keep bundles small

The bundle contains your code and all `npm:` and `jsr:` dependencies, pulled in at build time. A bigger bundle costs
build time, cache memory and disk, and load time whenever the artifact is brought into memory.

- Prefer small, fetch-based libraries and web-standard APIs (`URL`, `fetch`, `crypto.subtle`, `structuredClone`,
  `TextEncoder`) over large utility libraries.
- Import only what you use, from subpaths (`npm:date-fns/format`) rather than the package root, so the bundle contains
  one module instead of the whole library.
- Avoid packages that pull in Node polyfills (`buffer`, `stream`, `crypto`, `process`): the CDN inlines them into your
  bundle. If a package needs them, look for a web-standard alternative.
- Do not ship what the platform already provides: `std:kv`, `std:sql` and `std:blob` replace drivers and clients.
- Watch the dependency size: a build fetches at most `build.max_deps_mb` of remote modules and 10 MB per module;
  `21ci build-logs <svc> <version>` lists what was fetched, and a version that fails with `too_large` has a dependency
  tree that is too big.
- Pin versions (`npm:hono@4.6.3`) so rebuilds and `rt.lock` stay stable and small.

## Use storage efficiently

Storage calls run on a shared blocking pool, and writes to one service database go through one writer. Fewer, larger
operations beat many small ones.

**Batch statements.** One `sql.batch` is one transaction and one round trip:

```ts title="main.ts"
import { sql } from "std:sql";

// Slow: three round trips, three writer turns.
// await sql.execute("INSERT INTO events (kind) VALUES (?)", ["a"]);
// await sql.execute("INSERT INTO events (kind) VALUES (?)", ["b"]);
// await sql.execute("UPDATE counters SET n = n + 2 WHERE name = 'events'");

// Fast: one transaction.
await sql.batch([
  { sql: "INSERT INTO events (kind) VALUES (?)", args: ["a"] },
  { sql: "INSERT INTO events (kind) VALUES (?)", args: ["b"] },
  { sql: "UPDATE counters SET n = n + 2 WHERE name = 'events'" },
], "write");
```

**Use transactions for read-modify-write.** `sql.transaction(async (tx) => ...)` (Deno) commits when the callback
resolves and rolls back when it throws, and runs on the writer without interleaving. Keep it short: a transaction is
limited to 5 seconds, and it blocks other writers of the same database while open.

**Keep read paths read-only.** Queries and `"read"` batches run on reader connections and do not wait behind writes.
A handler that issues a write on every request (even `CREATE TABLE IF NOT EXISTS`) takes the writer every time. Create
the schema only if it is missing:

```ts title="main.ts"
import { sql } from "std:sql";

let ready = false;
async function ensureSchema() {
  if (ready) return;
  const r = await sql.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name='todos_by_owner'");
  if (r.rows.length === 0) {
    await sql.batch([
      "CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, title TEXT NOT NULL)",
      "CREATE INDEX IF NOT EXISTS todos_by_owner ON todos (owner)",
    ], "write");
  }
  ready = true;      // cached only as long as the isolate lives; harmless if it vanishes
}
```

**Index what you filter and sort by**, and select only the columns you need. A statement is aborted after
`storage.sql_budget_ms` (1 s) and a result is capped at 10,000 rows and 10 MiB; page with `LIMIT` and keyset
conditions (`WHERE id > ?`) rather than large `OFFSET`s.

**Avoid N+1.** Read the list once and fetch related rows with one `WHERE id IN (...)` or a join, not one query per
item.

**Use `std:sqlite` for short statements on the request path.** Every `std:sql` call is handed to the storage pool and
back, tens of microseconds even for a point lookup. `std:sqlite` (also `bun:sqlite`) runs the statement on your task's
thread in a few microseconds, against the same database:

```ts title="main.ts"
import { Database } from "std:sqlite";

const db = new Database();
const byId = db.query("SELECT id, title FROM todos WHERE id = ?");   // prepared once per isolate

export default (req: Request) => {
  const id = Number(new URL(req.url).searchParams.get("id"));
  return Response.json(byId.get(id) ?? { error: "not found" });
};
```

Keep long queries, reports and transactions that `await` in between on `std:sql` (its statements may run 1 s, a
synchronous one 100 ms), and keep each write-heavy path on one API: a synchronous write waits at most 20 ms for the
lock of an async transaction. See [`std:sqlite` or `std:sql`?](sub-services.md#stdsqlite-or-stdsql).

**Use `kv.incr` for counters and rate limits**, not a SQL read-modify-write or `get` then `set`. It is one atomic
write, joins group commits, and its optional TTL gives fixed windows.

**Pick the right store.** KV for small values by key (value up to 1 MB, key up to 2 KB), SQL for anything you query, blobs for
large objects (stream them; do not put them in KV or SQL).

## Stream instead of buffering

Bodies and large results that are buffered in the isolate count against `memory_mb` and CPU.

- Pass `req.body` to `blob.put` and return `blob.get(...).body` as the response, instead of `await req.arrayBuffer()`.
- For large generated responses, return a `ReadableStream`; the head is sent first and chunks follow with
  backpressure, up to the response-size limit.
- Parse JSON only when you need it; do not `await req.json()` on bodies that may be large without checking
  `content-length`.
- WASM: HTTP bodies stream, but `std:host` `blob`, `call` and `platform` bodies are buffered, so keep them small.

## Outbound calls

Outbound `fetch` goes through the egress guard and counts toward the task's wall time and fetch caps (50 per HTTP task,
6 in flight at once).

- **Run independent requests in parallel** (`Promise.all`), within the concurrency cap.
- **Always set a timeout** with `AbortSignal.timeout(ms)` so a slow upstream cannot eat the wall limit.
- **Retry only idempotent requests**, with a small bounded number of attempts and backoff; a retry loop multiplies the
  wall time.
- **Cache upstream responses** that do not change per request in KV with a TTL (`kv.set(key, value, { ttlSeconds })`).
- **Set `Cache-Control` on public read endpoints.** The node does not cache responses itself; headers you send are
  honored by browsers and CDNs in front of it.
- **Stay inside the allowlist.** If the service sets `egress.allowed_hosts`, calls to other hosts fail with
  `egress_denied`.

## Choose limits deliberately

Defaults for HTTP tasks are 30 s wall, 1,000 ms CPU and 128 MB memory, capped by the plan.

| Limit | How to choose | What exceeding it looks like |
|---|---|---|
| `limits.memory_mb` | Peak of heap plus buffers plus WASM memories for your largest request, with headroom. Higher limits reserve more of the node's memory budget while the task runs | `502 oom` |
| `limits.cpu_ms` | CPU time, not waiting time. Measure with `21ci stats`; a handler that mostly awaits needs little | `502 cpu_limit` |
| `limits.wall_ms` | The longest legitimate request, including upstream waits | `504 timeout` |
| `scaling.max_instances` | Cap only when a downstream (an upstream API or your database writer) cannot take more. The default is adaptive | Requests queue; `queue` rises |
| `scaling.min_instances` | Keep 1 or more instances warm for a latency-sensitive service with gaps in its traffic, so the first request after a pause starts warm | The warm instances hold memory all the time, and count against the plan's `max_instances` |

Lower limits are a feature: a runaway request is stopped sooner and uses less of a shared node. Work that needs more
than an HTTP request allows belongs in a queue consumer or a schedule, whose default limits are 300 s wall and 10 s
CPU (capped by the plan), not in a longer HTTP limit. See [Service calls and queues](calls-and-queues.md) and
[Schedules](schedules.md).

## Choosing Deno or WASM, and a profile

- Start in Deno. Move a hot path to a WASM component when per-request overhead matters and the logic is
  compute-light, stateless and self-contained (gateways, token checks, transforms). Services call each other the same
  way regardless of runtime, so you can move one path at a time.
- WASM is not automatically faster for CPU-heavy work that was written poorly, and it adds build and tooling cost.
  A component that embeds a language runtime (JavaScript, Python) pays for that runtime at every fresh instance.
- Profiles are an operator choice that you can request per service (`--profile`): `max_density` for many mostly idle
  services, `min_latency` for a few hot ones. Details in [Performance tuning](performance.md).

## WASM size and start-up tips

Smaller components load faster, take less memory to initialize and cost less to cache. For scale, the repository's
samples: a `no_std` Rust hello component is tens of kilobytes, the Rust edge gateway with `std` and the platform
bindings a couple of hundred, and a JavaScript component with its embedded engine is over ten megabytes.

=== "Rust"

    ```toml title="Cargo.toml"
    [profile.release]
    opt-level = "s"        # or "z" for smallest; "3" if you need the speed
    lto = true
    codegen-units = 1
    panic = "abort"
    strip = true
    ```

    ```bash
    cargo build --release --target wasm32-wasip2
    # Optional: wasm-opt (binaryen) for a further reduction
    wasm-opt -Os -o small.wasm target/wasm32-wasip2/release/my_service.wasm
    wasm-tools component wit small.wasm      # confirm the exports and imports are intact
    ```

    - **`std` versus `no_std`.** `no_std` components are the smallest, but you give up `String`/`Vec` conveniences
      unless you add `alloc`. Full `std` links fine and costs a few hundred kilobytes more.
    - **Think before adding dependencies.** `regex`, `serde_json`, big formatting machinery and async runtimes add
      size. A hand-written parser for one known shape is often smaller.
    - **Avoid large static tables and eager initialization.** Data segments are copied into linear memory on every
      fresh instance; build lazily what a request needs.
    - **Do I/O in the handler, not at start.** During pre-instantiation, `std:host` imports answer `unavailable`.

=== "JavaScript (jco)"

    - The engine alone is ten megabytes or more regardless of your code, so the component is big by construction and
      costs the most per fresh instance. Turn on `wasm.reuse_instances` for such services.
    - Module top-level code runs at build time and is snapshotted, so move constant setup there and keep per-request
      work in the handler.
    - Bundle with esbuild first and tree-shake: jco takes one ESM file.

=== "Other languages"

    TinyGo and C produce small components; Python embeds an interpreter and is large. See
    [WASM languages](../languages/wasm.md) for per-language size notes and caveats.

## Checklist

- [ ] Built eagerly at deploy (`21ci push --wait`), and the build log shows no surprises.
- [ ] No per-request data in module scope; initialization is lazy and idempotent.
- [ ] No network I/O or heavy work at the module top level.
- [ ] Dependencies are few, pinned and imported from subpaths; the bundle is not carrying Node polyfills.
- [ ] Statements are batched; read paths are read-only; filters have indexes; no N+1; short request-path statements use `std:sqlite`.
- [ ] Counters use `kv.incr`; slow work goes to a queue or a schedule.
- [ ] Large bodies stream; outbound calls run in parallel with timeouts.
- [ ] Limits are as low as the real worst case allows.
- [ ] Reuse is on only for services that are reuse-safe; `min_instances` only where a cold start after idle hurts.
- [ ] You compared `server-timing` and `21ci stats` before and after.

## See also

- [Performance characteristics](../concepts/performance.md): the cost model behind this advice.
- [Performance tuning](performance.md): profiles, pools and scheduler knobs (operator).
- [KV, SQL, blobs and other sub-services](sub-services.md): the storage APIs used above.
- [Execution and isolation](../concepts/execution.md): what warm reuse keeps and when it discards.
- [Resource requirements](resources.md): what a service costs in memory and CPU, and how to measure it.
