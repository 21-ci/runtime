// BLD-04: imports `npm:zod@3`. At build time the specifier resolves to `{build.npm_cdn}/zod@3`
// (served by MockCdn in tests), the module and its transitive imports are fetched, hashed into
// `rt.lock` and bundled. At run time nothing is fetched.
import { z } from "npm:zod@3";

const Todo = z.object({
  title: z.string(),
  done: z.boolean(),
});

export default async function (req: Request): Promise<Response> {
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return Response.json({ error: "body must be JSON" }, { status: 400 });
  }
  const parsed = Todo.safeParse(body);
  if (!parsed.success) {
    return Response.json({ error: "validation failed" }, { status: 400 });
  }
  return Response.json({ ok: true, todo: parsed.data });
}
