---
title: Deno runtime
description: Which TypeScript and JavaScript code, npm packages and frameworks run on the Deno executor, the import rules, and working examples.
---

# Deno runtime

<span class="rt-badge rt-badge--deno">Deno</span> The Deno executor runs TypeScript and JavaScript as ES modules in a V8
isolate that has the Web platform APIs, the `std:*` modules and nothing else. This page answers three questions: what your
service must look like, which packages and frameworks work in that environment (and why the others do not), and how to
write the common kinds of service. The handler contract and the `std:*` API are described in
[Writing Deno services](../guides/deno.md) and the [guest API reference](../reference/guest-api.md).

## Requirements

| Item | Rule |
|---|---|
| Entry module | One module whose `export default` is a function (Form A) or an object with at least one of `fetch`, `scheduled`, `queue` (Form B). The entry is the `entry` of `rt.json`; without it `main.ts`, `main.tsx`, `main.js`, `main.jsx` or `main.mjs` (`21ci push` also guesses `index.ts`, `index.js` and `mod.ts`) |
| Language | TypeScript, TSX, JSX and JavaScript, as ES modules. `require()` and `module.exports` do not exist |
| Type checking | None. TypeScript is transpiled and the types are dropped; a type error never fails a push, a syntax error does (`422` with file, line and column) |
| Own-code imports | Relative files with the extension (`./util.ts`), `std:*`, `sdk:*`, `npm:`, `jsr:`, allow-listed `https:` URLs. `node:` and `file:` are rejected |
| Size | A single file up to 1 MB. A directory, archive or JSON bundle up to 10 MB in total, 1,000 files, 1 MB per file. Remote dependencies of one build up to 50 MB in total and 10 MB per module |
| State | None between tasks. Top-level code runs for each task, so module variables are at best a cache |
| Missing in the sandbox | Filesystem, raw sockets, threads, subprocesses, `process`, `Buffer`. WebSockets go through [`std:sockets`](../guides/websockets.md) (server) and an outbound `WebSocket` client. Details [below](#what-the-sandbox-has) |

The exact numbers are in [Limits and quotas](../reference/limits.md). The one-file limit (1 MB) is the limit of the
uploaded source, not of the bundle the node builds from it: dependencies are fetched at build time and do not count
against the 10 MB upload.

### The two handler shapes

=== "Form A: one function"

    ```ts title="main.ts"
    export default async function (req: Request, ctx: { taskId: string }): Promise<Response> {
      return Response.json({ task: ctx.taskId });
    }
    ```

    Receives HTTP requests and calls. Schedule fires and queue batches arrive as synthetic `POST` requests to
    `/__rt/scheduled` and `/__rt/queue`.

=== "Form B: an object"

    ```ts title="main.ts"
    export default {
      async fetch(req: Request, ctx: Ctx): Promise<Response> { /* http, call */ },
      async scheduled(event: ScheduledEvent, ctx: Ctx): Promise<void> { /* interval, cron */ },
      async queue(batch: QueueBatch, ctx: Ctx): Promise<void> { /* queue */ },
    };
    ```

    Every member is optional, at least one must be present. The runtime tests each member with
    `typeof member === "function"` at the time of the trigger. This matters for libraries that return a proxy
    (see [itty-router](#itty-router)).

Both shapes are explained, with the `ctx` fields and the queue rules, in [Writing Deno services](../guides/deno.md).

### What the sandbox has

The executor is built from `deno_core` with `deno_web`, `deno_fetch` and `deno_crypto`, not from the full Deno runtime.
That is why the surface is small and why no Deno API exists except `Deno.env`.

| Present | Absent |
|---|---|
| `fetch`, `Request`, `Response`, `Headers`, `FormData`, `URL`, `URLPattern`, streams, `TextEncoder`/`TextDecoder`, `Blob`, `File`, `AbortController`, timers, `structuredClone`, `CompressionStream`, `MessageChannel`, `crypto` (`getRandomValues`, `randomUUID`, `subtle`), `WebAssembly`, `console`, `Deno.env`, `WebSocket` (outbound client only) | `Worker`, `SharedWorker`, `SharedArrayBuffer`, `Atomics.wait`, `caches`, every other `Deno.*` API, `process`, `Buffer`, `require`, `__dirname`, `XMLHttpRequest`, any `node:` module in your own code |

Two differences from a browser matter for library compatibility:

- **The clock is frozen between I/O.** `Date.now()` and `performance.now()` advance only at I/O and timer boundaries,
  so a busy loop never sees time pass, and a library that spins "until 50 ms have elapsed" never ends. The CPU and wall
  limits still stop the task. Why: it removes a timing side channel between tenants ([Security](../concepts/security.md)).
- **`fetch` goes through the egress guard.** Private and loopback addresses are refused, and a task may make 50 calls
  (200 for schedules and queues) with 6 in flight. A library that retries aggressively can hit `egress_limit`.

The full list of globals is in the [guest API reference](../reference/guest-api.md#global-web-apis).

## Imports and dependencies

Imports are resolved in two stages. At **push time** the node parses every file of the upload, including files nothing
imports, and rejects forbidden specifiers with `422 validation_failed`. At **build time** (the first request, or at once
with `--wait`) the node fetches `npm:`, `jsr:` and `https:` dependencies through the egress guard, bundles everything,
and pins the result in `rt.lock`. At run time nothing is fetched: a CDN outage cannot break a running version, only a
new build.

| Specifier | What happens | Notes |
|---|---|---|
| `./util.ts`, `../lib/db.ts` | Bundled from your upload | Write the file extension. The resolver joins the specifier literally and does not try `.ts` or `index.ts` |
| `std:kv`, `std:sql`, `std:sqlite`, `std:blob`, `std:queue`, `std:services`, `std:platform`; `bun:sqlite` (the same module as `std:sqlite`) | Built into the runtime, never bundled | An unknown `rt:` name is `422` |
| `sdk:<name>` | An operator SDK published on the node (a shim of its contract; the SDK runs in the runtime) | An unknown name is `422` |
| `npm:pkg@range[/sub]` | Fetched from `{build.npm_cdn}/pkg@range[/sub]?target=es2022` | The CDN defaults to `https://esm.sh` |
| `jsr:@scope/pkg@range` | Fetched from `{build.npm_cdn}/jsr/@scope/pkg@range?target=es2022` | |
| `https://host/path` | Fetched if `host` is in `build.allowed_hosts` | Default hosts: `esm.sh`, `jsr.io`, `deno.land`, `cdn.jsdelivr.net`, `unpkg.com` |
| `zod` (bare name) | Only through the `imports` map of `rt.json` | Otherwise `422`: ``bare specifier `zod` needs an `imports` entry in rt.json`` |
| `node:fs`, `node:crypto`, any `node:` | Rejected in your own files | `422`: `` `node:` imports are not allowed `` |
| `file:` | Rejected | |

The `rt.json` import map is applied before any other rule. An exact key wins; otherwise the longest key that ends in `/`
applies as a prefix:

```json title="rt.json"
{
  "entry": "main.ts",
  "imports": {
    "hono": "npm:hono@4",
    "hono/": "npm:hono@4/",
    "zod": "npm:zod@3"
  }
}
```

With this map `import { cors } from "hono/cors"` resolves to `npm:hono@4/cors`. Without an `rt.json`, write the `npm:`
specifier in the import itself, as the examples below do.

!!! tip "Pin exact versions"
    `npm:hono@4` is a range. The first build resolves it and records the exact URLs and digests in `rt.lock`, so rebuilds
    of the same version are reproducible. A new upload resolves the range again and can pick a newer release. Write
    `npm:hono@4.6.14` when you want a deploy to change only your code. See
    [Services, deploys and builds](../concepts/services-and-deploys.md).

### What happens to `node:` inside packages

The rule "`node:` is rejected" applies to the files **you upload**. A package fetched from esm.sh is not checked that way.
esm.sh rewrites a package's `node:` imports to its own shim modules (`/node/http.mjs`, `/node/fs.mjs`, `/node/process.mjs`
on the same host), and the build accepts them like any other module on an allowed host. The shims come from the unenv
project: pure-JS ones for `buffer`, `events`, `path`, `util`, `url`, `querystring` and similar, and stubs for everything
that needs the operating system. In the stubs of `fs`, `http`, `net`, `child_process` and `process` the unimplemented
functions throw `[unenv] <name> is not implemented yet!` when called.

This has a consequence that surprises people. **A package that imports `node:http` deploys fine and fails when it calls
into it.** Express is the example: its module graph reaches `fs`, `http`, `net`, `crypto` and `zlib`, the push and the
build succeed, and the first request throws.

`process` and `Buffer` are not globals. Packages that import them get esm.sh's `process` stub (its `env` is empty, so
`process.env.OPENAI_API_KEY` reads as `undefined`) and its `buffer` polyfill. Your own code gets a `ReferenceError`. Read
configuration with `Deno.env.get()`.

!!! warning "Verify for your version"
    This section describes what esm.sh served when this page was written. Behavior depends on the CDN the operator
    configured (`build.npm_cdn`) and on its version. Check a package before you depend on it with the script in
    [Does my npm package work?](#does-my-npm-package-work).

## Frameworks and libraries

Rule of thumb: a package works if it needs only `fetch`, WebCrypto, streams and timers. It fails if it needs a server
socket, a TCP client socket, the filesystem, a native addon or a worker thread. "Works" below means the package was
exercised against the runtime's calling convention, `fetch(request, ctx)`, with `std:kv` and `std:sql` replaced by stand-ins
that return the documented result shapes. "Likely" means its esm.sh module graph is free of server and filesystem
modules and its API is `fetch`-based, but it was not run. Nothing here was run on a live node.

| Package | Verdict | How to use it | Why, and what to watch |
|---|---|---|---|
| Hono | Works | `export default app` | Web-standard `app.fetch`. The runtime's `ctx` arrives as `c.env`; `c.executionCtx` throws. Do not use `hono/node-server`, `serveStatic` or `upgradeWebSocket` |
| itty-router | Works | `export default { fetch: router.fetch }` | Tiny, no dependencies. Do not export the router object itself ([why](#itty-router)) |
| tRPC (server) | Works | `fetchRequestHandler` from `@trpc/server/adapters/fetch` | Built for fetch runtimes. Set `isDev: false` or error bodies carry stack traces |
| zod, valibot | Works | `import { z } from "npm:zod@3"` | Pure JavaScript |
| drizzle-orm | Works with an adapter | `drizzle-orm/sqlite-proxy` over `std:sql` ([example](#drizzle-orm-over-stdsql)) | Native drivers (`better-sqlite3`, `libsql`, `pg`) cannot run. `db.transaction()` fails, because `BEGIN` is denied |
| jose | Works | `npm:jose@6` | WebCrypto-based, no Node modules in its graph |
| date-fns | Works | `npm:date-fns@4` | Pure JavaScript |
| Oak | Likely | `export default { fetch: app.fetch }` from `jsr:@oak/oak@17` | `app.fetch` exists. Never call `app.listen()`. Its graph imports `node:http` (stubbed by esm.sh); whether a code path calls it was not checked. Verify |
| Elysia | Likely | `export default { fetch: app.fetch }` | Web-standard core, never call `.listen()`. Bun-only features (`Bun.file`, the static and WebSocket plugins) do not work. Its AOT compiler builds handlers with `new Function`; verify that on your node |
| openai | Likely | `new OpenAI({ apiKey: Deno.env.get("OPENAI_API_KEY") })` | `fetch`-based. Without `apiKey` it reads `process.env`, which is empty here. Verify streaming on your node |
| @anthropic-ai/sdk | Likely | `new Anthropic({ apiKey: Deno.env.get("ANTHROPIC_API_KEY") })` | Same shape as `openai`. Without `apiKey` it reads `process.env` |
| stripe | Likely | `Stripe.createFetchHttpClient()` and, for webhooks, `Stripe.createSubtleCryptoProvider()` with `constructEventAsync` | The default client uses `node:http`. When esm.sh serves the Web build the fetch client is the default; pass it explicitly anyway. Verify |
| Express, Fastify, Koa | Does not work | none | They are built on `node:http` servers and `net` sockets. The runtime has no way to hand them a request. Use Hono |

Other packages that cannot work for the same reason: database drivers over TCP (`pg`, `mysql2`, `ioredis`, `mongodb`:
no sockets, use [`std:sql`](../guides/sub-services.md) or an HTTP database API through `fetch`), `ws` and `socket.io` (they
listen on Node sockets; accept WebSockets with [`std:sockets`](../guides/websockets.md) instead), native addons (`sharp`, `bcrypt`, `better-sqlite3`), headless browsers, and packages that read
files at run time (`multer`, `fs-extra`). `jsonwebtoken` needs `node:crypto`; use `jose`.

!!! warning "Verify for your version"
    The "Likely" rows rest on the esm.sh module graph, not on a run. Before you build on one, deploy the smallest
    possible handler that imports it and calls the one function you need, then read `21ci logs <service>`. The
    [checklist](#does-my-npm-package-work) below is the same test in order.

## Examples

Every example is a complete `main.ts` (plus `rt.json` where noted) that you can push as it is. The commands assume
`RT_API` and `RT_KEY` are set; see [Get access](../guides/access.md). The Hono, itty-router, tRPC, drizzle, Form B,
zod and LLM examples were run in Node.js against the runtime's calling convention with in-memory `std:kv` and `std:sql`
stand-ins; the Oak snippet was not run.

### A plain fetch handler (Form A)

```ts title="main.ts"
export default async function (req: Request, ctx: { taskId: string; trigger: string }): Promise<Response> {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    const name = url.searchParams.get("name") ?? "world";
    return Response.json({ message: `Hello, ${name}!`, task: ctx.taskId, trigger: ctx.trigger });
  }

  if (url.pathname === "/echo" && req.method === "POST") {
    const body = await req.json().catch(() => null);
    if (body === null) return Response.json({ error: "body must be JSON" }, { status: 400 });
    return Response.json({ received: body });
  }

  return new Response("not found\n", { status: 404 });
}
```

=== "21ci"

    ```bash
    21ci push main.ts --service hello --create --wait
    21ci invoke hello '/?name=Ada'
    21ci invoke hello /echo -d '{"a":1}'
    ```

=== "curl"

    ```bash
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"hello","runtime":"deno"}' "$RT_API/v1/services" | jq -r .id)
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/typescript' \
      --data-binary @main.ts "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    curl -sS "$RT_INVOKE/s/hello/?name=Ada"
    curl -sS -d '{"a":1}' -H 'Content-Type: application/json' "$RT_INVOKE/s/hello/echo"
    ```

The handler sees `/?name=Ada`, not `/s/hello/?name=Ada`: the node strips the `/s/{slug}` prefix and reports it in
`x-rt-base-path`. The other examples rely on this too.

### Form B with `scheduled` and `queue`

One service that answers HTTP, runs a periodic job and consumes a queue. `enqueue` sends to a service of the same
tenant; here it is the service itself.

```ts title="main.ts"
import { enqueue } from "std:queue";
import { kv } from "std:kv";

interface Ctx {
  taskId: string;
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  service: { id: string; slug: string; version: number };
}
interface QueueMessage {
  id: string;
  body: unknown;
  attempt: number;
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}

export default {
  // trigger http and call
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    if (req.method === "POST") {
      const job = await req.json().catch(() => null);
      if (job === null) return Response.json({ error: "body must be JSON" }, { status: 400 });
      const { id } = await enqueue(ctx.service.slug, job);
      return Response.json({ queued: id }, { status: 202 });
    }
    return Response.json({
      processed: (await kv.get<number>("processed")) ?? 0,
      lastTick: await kv.get<number>("last-tick"),
    });
  },

  // trigger interval and cron: one call per fire
  async scheduled(event: { scheduleId: string; scheduledTime: number; payload: unknown }, ctx: Ctx): Promise<void> {
    console.log(`tick ${event.scheduleId} task=${ctx.taskId}`);
    await kv.set("last-tick", event.scheduledTime);
  },

  // trigger queue: up to 10 messages per call, at-least-once
  async queue(batch: { messages: QueueMessage[] }, ctx: Ctx): Promise<void> {
    for (const msg of batch.messages) {
      try {
        console.log(`job ${msg.id} attempt=${msg.attempt}`, msg.body);
        await kv.incr("processed", 1);
        msg.ack();
      } catch {
        msg.retry({ delaySeconds: 30 });
      }
    }
  },
};
```

```bash
21ci push main.ts --service jobs --create --wait
21ci schedule add jobs --every 5m
21ci queue send jobs --body '{"url":"https://example.com"}'
21ci invoke jobs
```

`kv.incr` is atomic, so two batches running at once cannot lose a count. Delivery is at-least-once: a message can be
delivered twice, so real work needs an idempotency key. See [Calls and queues](../guides/calls-and-queues.md) and
[Schedules](../guides/schedules.md).

### Hono: REST routes with `std:kv`

```ts title="main.ts"
import { Hono } from "npm:hono@4";
import { cors } from "npm:hono@4/cors";
import { kv } from "std:kv";

// The runtime calls fetch(request, ctx). Hono's signature is fetch(request, env, executionCtx),
// so the runtime's ctx arrives as c.env.
interface Ctx {
  taskId: string;
  waitUntil(p: Promise<unknown>): void;
}

const app = new Hono<{ Bindings: Ctx }>();
app.use("*", cors());

app.get("/", (c) => c.json({ service: "notes", task: c.env.taskId }));

app.get("/notes", async (c) => {
  const page = await kv.list<{ text: string }>({ prefix: "note:", limit: 100 });
  return c.json(page.entries.map((e) => ({ id: e.key.slice("note:".length), ...e.value })));
});

app.post("/notes", async (c) => {
  const body = await c.req.json<{ text?: string }>().catch(() => null);
  if (!body?.text) return c.json({ error: "text is required" }, 400);
  const id = crypto.randomUUID();
  await kv.set(`note:${id}`, { text: body.text, created: Date.now() });
  return c.json({ id }, 201);
});

app.get("/notes/:id", async (c) => {
  const note = await kv.get(`note:${c.req.param("id")}`);
  return note ? c.json(note) : c.json({ error: "not found" }, 404);
});

app.delete("/notes/:id", async (c) => {
  const existed = await kv.delete(`note:${c.req.param("id")}`);
  return existed ? c.body(null, 204) : c.json({ error: "not found" }, 404);
});

export default app;
```

```bash
21ci push main.ts --service notes --create --wait
21ci invoke notes /notes -d '{"text":"buy milk"}'
21ci invoke notes /notes
```

`export default app` works because a Hono app is an object with a `fetch` function, which is Form B. Two Hono details
follow from that. The runtime passes `ctx` as the second argument, which Hono treats as its `env`, hence `c.env.taskId`.
Hono's third argument, the execution context, is empty, so `c.executionCtx` throws `This context has no
ExecutionContext`. To get it, export a wrapper instead:
`export default { fetch: (req: Request, ctx: Ctx) => app.fetch(req, ctx, ctx) }`, and `c.executionCtx.waitUntil(p)`
reaches the runtime's `ctx.waitUntil`.

### itty-router

```ts title="main.ts"
import { AutoRouter, error, json } from "npm:itty-router@5";
import { kv } from "std:kv";

const router = AutoRouter();

router
  .get("/", () => ({ service: "counters" }))
  .get("/count/:name", async ({ params }) => {
    const count = await kv.incr(`count:${params.name}`, 1);
    return json({ name: params.name, count });
  })
  .post("/echo", async (req) => json(await req.json()))
  .all("*", () => error(404, "not found"));

// Export only fetch. An itty router answers every property name with a route
// registrar, so `export default router` would also look like it has queue() and scheduled().
export default { fetch: router.fetch };
```

```bash
21ci push main.ts --service counters --create --wait
21ci invoke counters /count/visits
```

An itty router is a `Proxy`: reading any property, including `queue` and `scheduled`, returns a
function that registers a route. With `export default router`, HTTP works, but the runtime finds a "function" at
`def.queue` and `def.scheduled`, calls it for queue batches and schedule fires, and treats the call as success. Messages
are acked without being handled. Exporting `{ fetch: router.fetch }` avoids it. This is a general rule: export an
object literal when a library's object is dynamic.

### tRPC with the fetch adapter

```ts title="main.ts"
import { initTRPC } from "npm:@trpc/server@11";
import { fetchRequestHandler } from "npm:@trpc/server@11/adapters/fetch";
import { z } from "npm:zod@3";
import { kv } from "std:kv";

// isDev defaults to process.env.NODE_ENV !== "production", and there is no process here: it would be
// true and error responses would carry stack traces.
const t = initTRPC.create({ isDev: false });

const appRouter = t.router({
  greet: t.procedure
    .input(z.object({ name: z.string().min(1) }))
    .query(({ input }) => ({ greeting: `Hello, ${input.name}!` })),
  visit: t.procedure.mutation(async () => ({ visits: await kv.incr("visits", 1) })),
});

export type AppRouter = typeof appRouter;

export default {
  fetch: (req: Request) =>
    fetchRequestHandler({
      endpoint: "/trpc",
      req,
      router: appRouter,
      createContext: () => ({}),
    }),
};
```

```bash
21ci push main.ts --service api --create --wait
curl -sS -G "$RT_INVOKE/s/api/trpc/greet" --data-urlencode 'input={"name":"Ada"}'
curl -sS -X POST -H 'content-type: application/json' -d '{}' "$RT_INVOKE/s/api/trpc/visit"
```

`endpoint` is `/trpc` and not `/s/api/trpc` because the node removes the service prefix before your code runs. Behind a
custom host the path is `/trpc/...` as well.

### Oak

```ts title="main.ts"
import { Application, Router } from "jsr:@oak/oak@17";

const router = new Router();
router.get("/", (ctx) => {
  ctx.response.body = { hello: "oak" };
});

const app = new Application();
app.use(router.routes());
app.use(router.allowedMethods());

// never app.listen(): there are no sockets
export default { fetch: app.fetch };
```

```bash
21ci push main.ts --service oak-api --create --wait
21ci invoke oak-api /
```

!!! warning "Verify for your version"
    Not run. Oak 17 defines `fetch` on `Application`, and its module graph on esm.sh reaches `node:http` and
    `node:stream/web`; whether a request path calls them was not checked. If the push succeeds but requests fail, `21ci logs oak-api` shows the first thrown error; use Hono
    instead.

### drizzle-orm over `std:sql`

`std:sql` is a SQLite database per service. drizzle's `sqlite-proxy` driver sends every statement to one callback and
expects `{ rows }` back, with each row as an array in column order. That is exactly what
[`sql.execute`](../reference/guest-api.md#sqlexecute) returns (`{ columns, rows, rowsAffected, lastInsertRowid }` with
`rows: unknown[][]`), so the adapter is four lines.

```ts title="main.ts"
import { drizzle } from "npm:drizzle-orm@0.44/sqlite-proxy";
import { integer, sqliteTable, text } from "npm:drizzle-orm@0.44/sqlite-core";
import { desc, eq } from "npm:drizzle-orm@0.44";
import { sql } from "std:sql";

const todos = sqliteTable("todos", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  title: text("title").notNull(),
  done: integer("done", { mode: "boolean" }).notNull().default(false),
});

// drizzle calls this for every statement. std:sql returns rows as arrays in column order,
// which is the shape the sqlite-proxy driver expects; method "get" wants a single row.
const db = drizzle(async (query, params, method) => {
  const result = await sql.execute(query, params);
  return { rows: method === "get" ? (result.rows[0] ?? []) : result.rows };
});

// One cheap read per task; only a database without the table pays for the write.
async function ensureSchema() {
  const found = await sql.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'todos'");
  if (found.rows.length > 0) return;
  await sql.execute(
    `CREATE TABLE IF NOT EXISTS todos (
       id    INTEGER PRIMARY KEY AUTOINCREMENT,
       title TEXT NOT NULL,
       done  INTEGER NOT NULL DEFAULT 0
     )`,
  );
}

export default async function (req: Request): Promise<Response> {
  await ensureSchema();
  const url = new URL(req.url);

  if (req.method === "POST" && url.pathname === "/todos") {
    const body = await req.json().catch(() => null);
    if (typeof body?.title !== "string" || body.title === "") {
      return Response.json({ error: "title is required" }, { status: 400 });
    }
    const [row] = await db.insert(todos).values({ title: body.title }).returning();
    return Response.json(row, { status: 201 });
  }

  const done = url.pathname.match(/^\/todos\/(\d+)\/done$/);
  if (req.method === "POST" && done) {
    const [row] = await db.update(todos).set({ done: true }).where(eq(todos.id, Number(done[1]))).returning();
    return row ? Response.json(row) : Response.json({ error: "not found" }, { status: 404 });
  }

  if (req.method === "GET" && url.pathname === "/todos") {
    return Response.json(await db.select().from(todos).orderBy(desc(todos.id)).limit(50));
  }

  return Response.json({ error: "not found" }, { status: 404 });
}
```

```bash
21ci push main.ts --service todos --create --wait
21ci invoke todos /todos -d '{"title":"write docs"}'
21ci invoke todos /todos
```

Limits to know about:

- **No `db.transaction()`.** drizzle sends `BEGIN` and `COMMIT` as separate statements, and the SQLite authorizer of
  `std:sql` denies them (`sql_not_authorized`). For atomic multi-statement work call `sql.batch([...])` or
  `sql.transaction(fn)` directly, and keep drizzle for single statements.
- **No migrations tool at run time.** `drizzle-kit` is a Node program. Create the schema as above, or keep numbered
  `CREATE TABLE IF NOT EXISTS` statements in your code.
- **Every statement is one call.** A statement over 1,000 ms (`storage.sql_budget_ms`) is `sql_timeout`, a result over
  10,000 rows is `sql_result_too_large`. Always `limit()` list queries.
- **Booleans.** SQLite has no boolean type: `{ mode: "boolean" }` stores 0 and 1.

### Calling an LLM API with secrets

Keep the key out of the code. `21ci secret set` stores it write-only and sealed at rest; the handler reads it with
`Deno.env.get`. Plain settings (here the model id) go in `env`. `process.env` is not available: an SDK that reads
`process.env.<KEY>` by default sees nothing, so pass the key explicitly.

```ts title="main.ts"
export default async function (req: Request): Promise<Response> {
  if (req.method !== "POST") {
    return Response.json({ error: 'POST {"question": "..."}' }, { status: 405, headers: { allow: "POST" } });
  }

  // Secrets and env both come from Deno.env. process.env is not available.
  const apiKey = Deno.env.get("ANTHROPIC_API_KEY");
  const model = Deno.env.get("MODEL");
  if (!apiKey || !model) {
    console.error("ANTHROPIC_API_KEY and MODEL must be set");
    return Response.json({ error: "not configured" }, { status: 500 });
  }

  const body = await req.json().catch(() => null);
  const question = body?.question;
  if (typeof question !== "string" || question.length === 0 || question.length > 4000) {
    return Response.json({ error: "question must be a string of 1 to 4000 characters" }, { status: 400 });
  }

  const upstream = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model,
      max_tokens: 512,
      stream: true,
      messages: [{ role: "user", content: question }],
    }),
  });

  if (!upstream.ok) {
    console.error(`anthropic answered ${upstream.status}`);
    return Response.json({ error: "upstream_error", status: upstream.status }, { status: 502 });
  }

  // Pass the event stream straight to the client. Nothing is buffered, and the API key stays here.
  return new Response(upstream.body, {
    headers: { "content-type": "text/event-stream", "cache-control": "no-store" },
  });
}
```

```bash
21ci push main.ts --service assistant --create --wait
printf %s "$ANTHROPIC_API_KEY" | 21ci secret set assistant ANTHROPIC_API_KEY
21ci env set assistant MODEL=YOUR_MODEL_ID
21ci invoke assistant -d '{"question":"What is a V8 isolate?"}'
```

The model id comes from your provider's documentation, which is why it is a setting and not a constant. Secrets and env
changes apply to the next task; no redeploy. Mechanisms to keep in mind:

- The handler streams the answer, so the response starts when the first tokens arrive. The task still lives until the
  stream ends, and the **wall limit** (30 s by default; `limits.wall_ms`, capped by the plan) cuts a long generation.
  The **CPU limit** is not an issue: waiting for the network is not CPU time.
- Outbound calls are limited to 50 per task and go through the egress guard. A tenant on a plan with
  `egress_default = "deny"` needs the provider's host on the operator's allowlist, or `fetch` fails with `egress_denied`.
- A secret is as readable as any variable inside the guest. Never log it or return it.

With an SDK the call looks like `new Anthropic({ apiKey: Deno.env.get("ANTHROPIC_API_KEY") })` after
`import Anthropic from "npm:@anthropic-ai/sdk@0.60"`; see the verdict in the [table](#frameworks-and-libraries).

### A zod-validated JSON endpoint

This is `sample/deno/npm-zod`. The import map in `rt.json` lets the code say `from "zod"`.

```json title="rt.json"
--8<-- "sample/deno/npm-zod/rt.json"
```

```ts title="main.ts"
--8<-- "sample/deno/npm-zod/main.ts"
```

```bash
21ci push . --service todo-validate --create --wait     # run in the directory with main.ts and rt.json
21ci invoke todo-validate -d '{"title":"ship it","priority":"high"}'
21ci invoke todo-validate -d '{"priority":"urgent"}'
```

The second call returns `400` with zod's `issues`. A directory with an `rt.json` is sent as a JSON bundle (`entry`, `files`,
`imports`); see [`21ci push`](../cli.md#push).

## Does my npm package work?

Answer the questions in order. The first "no" in the first column is the answer.

| # | Question | If the answer is bad |
|---|---|---|
| 1 | Does it need only `fetch`, WebCrypto, streams, timers and plain JavaScript? | It will not run. Find a `fetch`-based or pure-JS alternative |
| 2 | Does it open a **server** socket (`listen`, `createServer`, `app.listen`)? | It cannot work. The runtime calls your handler; nothing accepts connections |
| 3 | Does it open **client** sockets (a database driver, Redis, SMTP, `ws`)? | It cannot work. Use `std:sql`, `std:kv`, or an HTTP API through `fetch` |
| 4 | Does it import `node:fs`, `node:child_process`, `node:worker_threads` or native addons? | Not usable. The stubs throw when called |
| 5 | Does it read `process.env` for its configuration? | It works if the key can be passed as an option. Pass `Deno.env.get(...)` explicitly |
| 6 | Is it CommonJS-only, with no ES module build? | esm.sh converts it, but check step 7 |
| 7 | Does the module graph reach `node:` modules? | Run the script below. `buffer`, `events`, `path`, `util`, `url`, `process` are harmless when only imported. `http`, `net`, `fs`, `child_process`, `crypto` are calls that will throw |
| 8 | Is its initialization heavy (large schemas compiled at import, big tables)? | The cost is paid in every task and counts toward the CPU limit (1,000 ms by default). Read `init;dur=` in the `server-timing` header after deploying |

Step 7 as a script. It walks the module graph that esm.sh serves for the package and lists the Node shim modules it
reaches. Run it with Node 18 or later on your machine, with the same version range you will import:

```js title="check-package.mjs"
// usage: node check-package.mjs hono@4 [hono@4/cors ...]
const CDN = process.env.NPM_CDN ?? "https://esm.sh";

async function nodeImports(spec) {
  const seen = new Set();
  const found = new Set();
  const queue = [`/${spec}?target=es2022`];
  while (queue.length > 0 && seen.size < 400) {
    const path = queue.shift();
    if (seen.has(path)) continue;
    seen.add(path);
    const res = await fetch(CDN + path);
    if (!res.ok) throw new Error(`${res.status} for ${CDN}${path}`);
    const code = await res.text();
    for (const m of code.matchAll(/(?:import|export)\s*(?:[^'"]*?from\s*)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/g)) {
      const target = m[1] ?? m[2];
      if (target.startsWith("/node/")) found.add(target.slice(6).replace(/\.mjs$/, ""));
      else if (target.startsWith("/") || target.startsWith("./") || target.startsWith("../")) {
        const u = new URL(target, CDN + path);
        queue.push(u.pathname + u.search);
      }
    }
  }
  return { modules: seen.size, node: [...found].sort() };
}

for (const spec of process.argv.slice(2)) {
  try {
    const { modules, node } = await nodeImports(spec);
    console.log(`${spec}: ${modules} modules, node: ${node.length ? node.join(", ") : "none"}`);
  } catch (e) {
    console.log(`${spec}: ${e.message}`);
  }
}
```

```console
$ node check-package.mjs hono@4 express@5 openai@5
hono@4: 23 modules, node: none
express@5: 164 modules, node: async_hooks, buffer, crypto, events, fs, http, net, path, process, querystring, stream, string_decoder, url, util, zlib
openai@5: 54 modules, node: buffer, process
```

"node: none" means the package cannot fail on a Node module. A list is not a verdict: it tells you what to look at. A
package can import `node:http` for an adapter you never call (Oak does) and work. The reliable test is a smallest
possible service that imports the package and calls the one function you need.

If the operator changed `build.npm_cdn`, set `NPM_CDN` to the same URL. The size of the graph also matters for the
build: remote dependencies are limited to 50 MB per build and 10 MB per module.

## When a package fails

Failures come in two stages. A **push-time** failure is an immediate `422` and no version is created. A **build-time**
failure leaves a version in the state `failed` with a reason, and requests get `502 build_failed` until you push a fix.
A **run-time** failure is a `500 guest_error` for the one request, with the cause in the logs.

| Where | What you see | Cause | What to do |
|---|---|---|---|
| Push | `422 validation_failed`: `` `node:` imports are not allowed: `node:crypto` ``, with file, line and column | Your own code imports a `node:` module | Replace it: WebCrypto for `node:crypto`, `Uint8Array` and `btoa`/`atob` for `Buffer`, `fetch` for `node:http` clients |
| Push | ``bare specifier `zod` needs an `imports` entry in rt.json`` | Import without a scheme and no import map entry | Write `npm:zod@3`, or add the entry to `rt.json` |
| Push | ``unknown runtime module `rt:foo` `` or ``unknown sdk `sdk:foo` `` | A misspelled or unpublished module | Use one of the `std:*` modules; `21ci sdk ls` lists the SDKs you may use |
| Push | `413 too_large` | Source over 1 MB (single file) or 10 MB (bundle), more than 1,000 files, or an archive that inflates over 100 times | Push less; move assets to `std:blob`; keep dependencies as `npm:` imports instead of vendoring them |
| Build | Version `failed`, reason `resolve`: ``host `x` is not in build.allowed_hosts`` or a missing relative file | An `https:` import from a host the operator did not allow, or a typo in a relative path | Use `npm:` or `jsr:`, or ask the operator to extend `build.allowed_hosts` |
| Build | reason `fetch` | The CDN answered with an error or timed out (30 s per module), or the package or version does not exist | Check the name and version with `node check-package.mjs`; rebuild with `21ci rebuild <service> <n>` |
| Build | reason `too_large` | Dependencies over 50 MB, or one module over 10 MB | Choose a lighter package |
| Build | reason `timeout` or `oom` | The build took over 60 s or used over its memory budget | Reduce the dependency graph |
| Build | reason `lock_mismatch` | A rebuild could not reproduce the pinned `rt.lock` | Push a new version |
| Run | `500 guest_error`, log line `[unenv] <name> is not implemented yet!` | The package called into an esm.sh stub (`http`, `fs`, `net`, `child_process`) | The package needs a feature the sandbox does not have. Replace it |
| Run | `ReferenceError: process is not defined` (or `Buffer`, `require`, `__dirname`) | Your code or an inlined CJS file uses a Node global | Use `Deno.env`, `Uint8Array`, ES imports |
| Run | `TypeError: ... is not a function` on `listen`, `createServer` | The library is a server framework | Use a fetch-native framework from the [table](#frameworks-and-libraries) |
| Run | `ReferenceError: Worker is not defined` | `Worker` is removed | Split the work over queue messages |
| Run | A `WebSocketPair` or `upgradeWebSocket` helper fails | Server sockets are held by the node, not by the isolate | Return `sockets.accept(req)` from `fetch` ([WebSockets](../guides/websockets.md)) |
| Run | Queue batches or schedule fires "succeed" but nothing happens | The default export is a proxy-like object (itty-router) | Export `{ fetch: router.fetch }` |
| Run | `502`, `x-rt-reason: cpu_limit` on requests that do almost nothing | Heavy initialization at import time, paid again in every task | Initialize lazily inside the handler, import a smaller package; see [Optimize your service](../guides/optimize.md) |
| Run | `fetch` rejects with `egress_denied`, `egress_dns`, `egress_limit` | Private address, plan egress policy, or more than 50 calls in one task | Fix the destination; batch calls; see [Security](../concepts/security.md) |

`21ci versions <service> <n>` prints the failure of a build. `21ci build-logs <service> <n>` prints the whole build log and
`21ci logs <service>` the run-time log.

## See also

- [Writing Deno services](../guides/deno.md): handler contract, `ctx`, testing and debugging
- [Guest API reference](../reference/guest-api.md): every `std:*` function, error code and global
- [KV, SQL, blobs and other sub-services](../guides/sub-services.md): when to use which store
- [Services, deploys and builds](../concepts/services-and-deploys.md): the import rules, `rt.lock` and the build pipeline
- [WASM runtime](wasm.md): the other runtime, for compiled languages
- [Languages and frameworks](index.md): the decision table and the limits both runtimes share
