// Env and secrets both reach the guest through `Deno.env.get(name)` (a READ-ONLY shim).
//
//   env      PUT /v1/services/{id}/env              whole map; readable through the API
//   secrets  PUT /v1/services/{id}/secrets/{NAME}   write-only through the API, sealed at rest
//
// Changes apply to the next task without a redeploy. Names match [A-Z_][A-Z0-9_]*; `RT_` is reserved.
//
//   GET /          plain env values
//   GET /secrets   whether each secret is set (never echo the value)
//   GET /readonly  Deno.env.set throws
//   GET /headers   which request headers the guest actually sees

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  invoker?: string;
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}

const PUBLIC_VARS = ["ENVIRONMENT", "LOG_LEVEL", "FEATURE_FLAG"];
const SECRET_NAMES = ["DATABASE_URL", "STRIPE_API_KEY"];

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    const vars: Record<string, string> = {};
    for (const name of PUBLIC_VARS) {
      const v = Deno.env.get(name);
      if (v !== undefined) vars[name] = v;
    }
    return Response.json({ environment: Deno.env.get("ENVIRONMENT") ?? "unset", vars });
  }

  if (url.pathname === "/secrets") {
    // Report presence only. A secret is as readable as env inside the guest, so never log or return it.
    const configured: Record<string, boolean> = {};
    for (const name of SECRET_NAMES) configured[name] = Deno.env.get(name) !== undefined;
    return Response.json({ configured });
  }

  if (url.pathname === "/readonly") {
    try {
      Deno.env.set("ENVIRONMENT", "hacked");
      return Response.json({ readonly: false });
    } catch (e) {
      return Response.json({ readonly: true, error: (e as Error).name });
    }
  }

  if (url.pathname === "/headers") {
    // Public callers cannot spoof x-rt-*: the runtime strips them and sets its own. The credential
    // used for invoke access (access=key) is stripped too, so it never reaches the guest.
    return Response.json({
      "x-rt-task-id": req.headers.get("x-rt-task-id"), // set by the runtime
      "x-rt-trigger": req.headers.get("x-rt-trigger"), // "http"
      "x-rt-base-path": req.headers.get("x-rt-base-path"), // "/s/env-secrets"
      "x-rt-invoker": req.headers.get("x-rt-invoker"), // null on the public listener, even if the caller sent one
      "ctx.invoker": ctx.invoker ?? null,
      hasAuthorization: req.headers.has("authorization"),
    });
  }

  return Response.json({ error: "not found" }, { status: 404 });
}
