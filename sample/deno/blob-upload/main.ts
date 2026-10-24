import { blob } from "std:blob";

// std:blob: object storage, streamed in both directions (no full buffering, objects ≤ 100 MB).
// API: get(key) → {body: ReadableStream, size, contentType} | null, head, put(key, body, {contentType}), delete, list.
//
//   GET    /            list objects (?prefix=…&cursor=…&limit=…)
//   PUT    /{key…}      upload: the request body is streamed straight into the blob store
//   GET    /{key…}      download as a stream
//   HEAD   /{key…}      size and content type only
//   DELETE /{key…}      delete
//
// `req.formData()` / `req.arrayBuffer()` would buffer the whole upload in the isolate; pass
// `req.body` (a ReadableStream) to blob.put instead.

const MAX_KEY_BYTES = 1024; // key ≤ 1 KB

export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);

  let key: string;
  try {
    key = decodeURIComponent(url.pathname.replace(/^\/+/, ""));
  } catch {
    return Response.json({ error: "malformed key" }, { status: 400 });
  }
  if (new TextEncoder().encode(key).length > MAX_KEY_BYTES) {
    return Response.json({ error: "key longer than 1 KB" }, { status: 400 });
  }

  try {
    if (key === "") {
      if (req.method !== "GET") return Response.json({ error: "method not allowed" }, { status: 405 });
      // Pass the SDK result through untouched (it carries the cursor for the next page).
      return Response.json(await blob.list({
        prefix: url.searchParams.get("prefix") ?? "",
        cursor: url.searchParams.get("cursor") ?? undefined,
        limit: Number(url.searchParams.get("limit") ?? 100),
      }));
    }

    switch (req.method) {
      case "PUT": {
        if (!req.body) return Response.json({ error: "empty body" }, { status: 400 });
        await blob.put(key, req.body, {
          contentType: req.headers.get("content-type") ?? "application/octet-stream",
        });
        const meta = await blob.head(key);
        return Response.json({ key, size: meta?.size, contentType: meta?.contentType }, { status: 201 });
      }

      case "GET": {
        const obj = await blob.get(key);
        if (!obj) return Response.json({ error: "not found" }, { status: 404 });
        return new Response(obj.body, {
          headers: {
            "content-type": obj.contentType ?? "application/octet-stream",
            "content-length": String(obj.size),
          },
        });
      }

      case "HEAD": {
        const meta = await blob.head(key);
        if (!meta) return new Response(null, { status: 404 });
        return new Response(null, {
          headers: {
            "content-type": meta.contentType ?? "application/octet-stream",
            "content-length": String(meta.size),
          },
        });
      }

      case "DELETE":
        await blob.delete(key);
        return new Response(null, { status: 204 });

      default:
        return Response.json({ error: "method not allowed" }, { status: 405 });
    }
  } catch (e) {
    console.error(e); // stack goes to the service logs
    return Response.json({ error: (e as Error).message }, { status: 500 });
  }
}
