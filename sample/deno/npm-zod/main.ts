// `zod` is mapped by rt.json (import map) to `npm:zod@3`. The runtime resolves `npm:` specifiers
// at BUILD time through `build.npm_cdn` (default https://esm.sh) and pins them in `rt.lock`;
// at run time nothing is fetched. A plain `import { z } from "npm:zod@3"` works the same way.
import { z } from "zod";

const TodoSchema = z.object({
  title: z.string().min(1),
  priority: z.enum(["low", "medium", "high"]).default("medium"),
  dueDate: z.string().datetime().optional(),
});

export default async function (req: Request): Promise<Response> {
  if (req.method !== "POST") {
    return Response.json({ error: "only POST is allowed" }, { status: 405, headers: { allow: "POST" } });
  }

  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return Response.json({ error: "body must be valid JSON" }, { status: 400 });
  }

  const parsed = TodoSchema.safeParse(body);
  if (!parsed.success) {
    return Response.json({ error: "validation failed", issues: parsed.error.issues }, { status: 400 });
  }
  return Response.json({ message: "valid", todo: parsed.data }, { status: 201 });
}
