import { kv } from "std:kv";

// std:kv: per-service key/value store with TTL.
//
//   GET /?key=visits&action=get          read
//   GET /?key=visits&action=increment    atomic kv.incr; the counter lives 1 hour from its first increment
//   GET /?key=visits&action=delete       delete
//   GET /?action=list[&prefix=…]         list keys (prefix / cursor / limit)
//   GET /?key=visits&action=claim        kv.cas: the first claim of the window wins, later ones see false
//   GET /?key=visits&action=ttl          kv.ttl: seconds left on the counter (null when none)

const PREFIX = "counters/";
const TTL_SECONDS = 3600;

export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);
  const name = url.searchParams.get("key") ?? "counter";
  const action = url.searchParams.get("action") ?? "get";
  const key = PREFIX + name; // keys are ≤ 2 KB, values ≤ 1 MB

  try {
    switch (action) {
      case "get":
        return Response.json({ key: name, value: (await kv.get<number>(key)) ?? 0 });

      case "increment": {
        // Atomic: concurrent tasks never lose an update. The TTL is set when the increment
        // creates the counter (a fixed 1 hour window); later increments keep that expiry.
        const value = await kv.incr(key, 1, { ttlSeconds: TTL_SECONDS });
        return Response.json({ key: name, value });
      }

      case "delete":
        await kv.delete(key);
        return Response.json({ key: name, deleted: true });

      case "claim": {
        // Compare-and-swap from absent: exactly one of concurrent calls in a window gets `true`.
        const claimed = await kv.cas(`${PREFIX}claim:${name}`, null, { at: Date.now() }, { ttlSeconds: TTL_SECONDS });
        return Response.json({ key: name, claimed });
      }

      case "ttl":
        return Response.json({ key: name, ttl: await kv.ttl(key) });

      case "list": {
        // Pass the SDK result through untouched (page + cursor for the next call).
        const prefix = url.searchParams.get("prefix") ?? PREFIX;
        const cursor = url.searchParams.get("cursor") ?? undefined;
        return Response.json(await kv.list({ prefix, cursor, limit: 100 }));
      }

      default:
        return Response.json({ error: `unknown action '${action}'` }, { status: 400 });
    }
  } catch (e) {
    console.error(e); // stack goes to the service logs
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
}
