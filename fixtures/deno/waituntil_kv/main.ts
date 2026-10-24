// DNO-07: responds at once; `ctx.waitUntil` keeps the isolate alive to write KV 500 ms later.
import { kv } from "std:kv";

interface Ctx {
  waitUntil(p: Promise<unknown>): void;
}

export default function (_req: Request, ctx: Ctx): Response {
  ctx.waitUntil(
    (async () => {
      await new Promise((r) => setTimeout(r, 500));
      await kv.set("waituntil", "done");
    })(),
  );
  return new Response("accepted", { status: 202 });
}
