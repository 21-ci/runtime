// Form A (val.town compatible): `export default function (req, ctx)`.
//
// By default the Deno executor runs every request in a fresh isolate: module top-level code
// runs once per TASK (not once per process), there is no warm state between requests.
// With `deno.reuse_isolates` (on in the `min_latency` profile) a cleanly finished isolate may
// serve later requests of the same service version, so module state is a cache at best.

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  invoker?: string; // from x-rt-invoker, internal listener only
  deadline: number; // epoch ms
  waitUntil(p: Promise<unknown>): void;
}

// Looks like a process-wide counter, but it is re-created for every task: always 1 (DNO-01),
// unless isolate reuse is on (then it counts the requests this isolate served, DNO-17).
let hitsInThisTask = 0;

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  hitsInThisTask++;

  // The runtime strips `/s/{slug}`: for GET /s/hello/x?name=Ada the handler sees /x?name=Ada.
  const url = new URL(req.url);
  const name = url.searchParams.get("name") ?? "World";

  return Response.json({
    message: `Hello, ${name}!`,
    method: req.method,
    path: url.pathname,
    basePath: req.headers.get("x-rt-base-path"), // "/s/hello"
    service: ctx.service, // { id, slug, version }
    trigger: ctx.trigger, // "http"
    taskId: ctx.taskId,
    hitsInThisTask, // 1 without isolate reuse: no warm state between requests
  });
}
