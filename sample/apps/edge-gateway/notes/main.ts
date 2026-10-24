// Notes API behind the edge gateway (slug edge-notes). Reachable only through the gateway:
//   - access "internal": the public listener answers 404 for this service
//   - call.allowed_callers = ["edge-gateway"]: other services of the tenant get call_not_allowed
//   - this handler answers 404 to anything that is not a service call (ctx.trigger !== "call")
// So x-gw-sub (set by the gateway after it verified the token) can be trusted as the owner.
//
//   GET    /            own notes, newest first (?limit=1-100&before=<id>)
//   POST   /            {"text": "…", "tags"?: ["a", "b"]}
//   GET    /{id}
//   PUT    /{id}        {"text": "…", "tags"?: […]}
//   DELETE /{id}

import { sql } from "std:sql";

interface Ctx {
  taskId: string;
  trigger: "http" | "interval" | "cron" | "queue" | "call";
}
interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
  lastInsertRowid?: number | bigint | null;
}

const MAX_BODY = 64 * 1024; // the gateway enforces the same limit (GATEWAY_MAX_BODY)
const SUBJECT = /^[A-Za-z0-9._:@-]{1,128}$/;
const TAG = /^[a-z0-9-]{1,24}$/;

const json = (data: unknown, status = 200) => Response.json(data, { status });
const fail = (status: number, code: string, message: string) => json({ error: { code, message } }, status);

let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const r = await sql.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'notes_owner'") as SqlResult;
  if (r.rows.length === 0) {
    await sql.batch([
      `CREATE TABLE IF NOT EXISTS notes (
         id INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, text TEXT NOT NULL,
         tags TEXT NOT NULL DEFAULT '[]', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
      `CREATE INDEX IF NOT EXISTS notes_owner ON notes (owner, id)`,
    ], "write");
  }
  schemaReady = true;
}

function view(row: unknown[], columns: string[]) {
  const o = Object.fromEntries(columns.map((c, i) => [c, row[i]]));
  return {
    id: Number(o.id),
    text: String(o.text),
    tags: JSON.parse(String(o.tags)) as string[],
    createdAt: new Date(Number(o.created_at)).toISOString(),
    updatedAt: new Date(Number(o.updated_at)).toISOString(),
  };
}

async function readNote(req: Request): Promise<{ text: string; tags: string[] } | Response> {
  const raw = await req.text();
  if (raw.length > MAX_BODY) return fail(413, "too_large", "body too large");
  let b: unknown;
  try {
    b = JSON.parse(raw);
  } catch {
    return fail(400, "invalid_json", "body must be JSON");
  }
  if (typeof b !== "object" || b === null || Array.isArray(b)) return fail(400, "validation_failed", "body must be an object");
  const { text, tags = [] } = b as { text?: unknown; tags?: unknown };
  if (typeof text !== "string" || text.trim().length === 0 || text.length > 2000) {
    return fail(400, "validation_failed", "text must be 1-2000 characters");
  }
  if (!Array.isArray(tags) || tags.length > 8 || !tags.every((t) => typeof t === "string" && TAG.test(t))) {
    return fail(400, "validation_failed", "tags: up to 8 of a-z 0-9 -, 1-24 characters each");
  }
  return { text: text.trim(), tags: [...new Set(tags as string[])] };
}

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  // Defense in depth on top of access=internal and allowed_callers.
  if (ctx.trigger !== "call") return fail(404, "not_found", "not found");
  const owner = req.headers.get("x-gw-sub") ?? "";
  if (!SUBJECT.test(owner)) return fail(400, "no_subject", "missing x-gw-sub (call through the gateway)");

  const url = new URL(req.url);
  const path = url.pathname;
  try {
    await ensureSchema();
    const now = Date.now();

    if (path === "/") {
      if (req.method === "GET") {
        const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") ?? "20") || 20));
        const before = Number(url.searchParams.get("before") ?? "0");
        const r = await sql.execute(
          "SELECT * FROM notes WHERE owner = ? AND (? = 0 OR id < ?) ORDER BY id DESC LIMIT ?",
          [owner, before, before, limit],
        ) as SqlResult;
        const notes = r.rows.map((row) => view(row, r.columns));
        return json({ notes, next: notes.length === limit ? notes[notes.length - 1].id : null });
      }
      if (req.method === "POST") {
        const note = await readNote(req);
        if (note instanceof Response) return note;
        const r = await sql.execute(
          "INSERT INTO notes (owner, text, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
          [owner, note.text, JSON.stringify(note.tags), now, now],
        ) as SqlResult;
        const id = Number(r.lastInsertRowid);
        return new Response(JSON.stringify({ id, ...note }), {
          status: 201,
          headers: { "content-type": "application/json", location: `/v1/notes/${id}` },
        });
      }
      return fail(405, "method_not_allowed", "GET or POST");
    }

    const m = /^\/(\d{1,15})$/.exec(path);
    if (!m) return fail(404, "not_found", "no such route");
    const id = Number(m[1]);

    if (req.method === "GET") {
      const r = await sql.execute("SELECT * FROM notes WHERE id = ? AND owner = ?", [id, owner]) as SqlResult;
      return r.rows.length ? json(view(r.rows[0], r.columns)) : fail(404, "not_found", "no such note");
    }
    if (req.method === "PUT" || req.method === "PATCH") {
      const note = await readNote(req);
      if (note instanceof Response) return note;
      const r = await sql.execute(
        "UPDATE notes SET text = ?, tags = ?, updated_at = ? WHERE id = ? AND owner = ?",
        [note.text, JSON.stringify(note.tags), now, id, owner],
      ) as SqlResult;
      return r.rowsAffected ? json({ id, ...note }) : fail(404, "not_found", "no such note");
    }
    if (req.method === "DELETE") {
      const r = await sql.execute("DELETE FROM notes WHERE id = ? AND owner = ?", [id, owner]) as SqlResult;
      return r.rowsAffected ? json({ id, deleted: true }) : fail(404, "not_found", "no such note");
    }
    return fail(405, "method_not_allowed", "GET, PUT or DELETE");
  } catch (e) {
    console.error(e);
    return fail(500, "internal", "internal error");
  }
}
