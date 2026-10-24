// DNO-11 helper: dumps the own property names of `globalThis` (sorted), so a test can compare an
// isolate taken from the pool against a freshly created one. Also dirties the global scope when
// asked (`?dirty=1`), which a pooled isolate of another tenant must never observe.
export default function (req: Request): Response {
  const g = globalThis as unknown as Record<string, unknown>;
  const url = new URL(req.url);
  const dirty = url.searchParams.get("dirty");
  if (dirty) {
    g[`__leak_${dirty}`] = dirty;
    (Object.prototype as unknown as Record<string, unknown>)[`__proto_leak_${dirty}`] = dirty;
    return new Response("dirtied");
  }
  const names = Object.getOwnPropertyNames(globalThis).sort();
  return Response.json({ names, protoKeys: Object.getOwnPropertyNames(Object.prototype).sort() });
}
