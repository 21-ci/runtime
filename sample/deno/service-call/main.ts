import { call } from "std:services";

// std:services: call another service of the SAME tenant, in-process (no HTTP hop).
//   call(slug, request: Request) → Promise<Response>
// The request URL host is a placeholder; only path + query + method + headers + body matter.
//
//   GET /call?target=hello&name=Ada   call `hello` (extra query params are forwarded; &path=/x to pick a path)
//   GET /background?target=hello      answer at once, finish the call under ctx.waitUntil
//   GET /recurse                      call ourselves until the runtime stops us (call depth ≤ 15 → 508)

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  invoker?: string;
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}

async function bodyOf(res: Response): Promise<unknown> {
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  const url = new URL(req.url);

  try {
    if (url.pathname === "/call") {
      const target = url.searchParams.get("target") ?? "hello";
      const u = new URL(url.searchParams.get("path") ?? "/", "http://svc");
      for (const [k, v] of url.searchParams) {
        if (k !== "target" && k !== "path") u.searchParams.append(k, v);
      }

      let res: Response;
      try {
        res = await call(target, new Request(u.toString()));
      } catch (e) {
        // A call that cannot be made throws an error with a stable `code`: `not_found` (unknown
        // slug), `cross_tenant_call` (another tenant's service), `forbidden`, `call_failed`.
        // Report it like a response so the caller keeps working.
        const code = (e as { code?: string }).code ?? "call_failed";
        const status = code === "not_found" ? 404 : code === "cross_tenant_call" || code === "forbidden" ? 403 : 502;
        return Response.json({ caller: ctx.service.slug, target, status, reason: code, response: null, taskId: ctx.taskId });
      }
      // The callee's own status is passed on, including a timeout (504) from a child that was killed.
      return Response.json({
        caller: ctx.service.slug,
        target,
        status: res.status,
        reason: res.headers.get("x-rt-reason"),
        response: await bodyOf(res),
        taskId: ctx.taskId,
      });
    }

    if (url.pathname === "/background") {
      const target = url.searchParams.get("target") ?? "hello";
      // The child task keeps running after this response is sent; waitUntil keeps the isolate
      // alive for it (bounded by the wall-time limit).
      ctx.waitUntil(
        call(target, new Request("http://svc/background-task", { method: "POST", body: JSON.stringify({ from: ctx.service.slug }) }))
          .then(async (r) => console.log(`[background] ${target} -> ${r.status}`, await r.text()))
          .catch((e) => console.error("[background] failed", e)),
      );
      return Response.json({ message: "call scheduled", target, taskId: ctx.taskId }, { status: 202 });
    }

    if (url.pathname === "/recurse") {
      const depth = Number(url.searchParams.get("depth") ?? 0);
      if (depth >= 20) return Response.json({ depth, stoppedByGuest: true }); // safety net, never reached
      const res = await call(ctx.service.slug, new Request(`http://svc/recurse?depth=${depth + 1}`));
      // The runtime answers 508 `call_depth` once the chain is 15 calls deep; every task still finishes.
      return Response.json({
        depth,
        child: res.ok ? await res.json() : { status: res.status, reason: res.headers.get("x-rt-reason") },
      });
    }

    return Response.json({ error: "not found" }, { status: 404 });
  } catch (e) {
    console.error(e);
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
}
