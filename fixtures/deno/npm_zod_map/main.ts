// BLD-04 (import-map flavour): the bare specifier `zod` is mapped to `npm:zod@3` by rt.json.
// The import map is applied before every other resolution rule.
import { z } from "zod";

export default function (req: Request): Response {
  const name = new URL(req.url).searchParams.get("name") ?? "";
  const parsed = z.string().safeParse(name);
  return Response.json({ ok: parsed.success });
}
