import { sql } from "std:sql";

// std:sql: per-service SQLite. API: execute(sql, args?), batch(stmts, "read" | "write").
// Queries and read batches run on read-only connections next to the one writer, so reads do
// not queue behind writes. Keep read paths free of writes.
// There are NO tagged templates (sql`…`): pass the statement text and bind parameters as `args`.
//
//   GET    /          list todos (?done=0|1)
//   POST   /          create {"title": "…"}
//   GET    /stats     counts, via one read batch
//   GET    /{id}      read one
//   PATCH  /{id}      update {"title"?: "…", "done"?: true|false}
//   DELETE /{id}      delete

// `execute` resolves to { columns, rows, rowsAffected, lastInsertRowid }.
interface SqlResult {
  columns: string[];
  rows: ArrayLike<unknown>[];
  rowsAffected: number;
  lastInsertRowid?: number | bigint | null;
}

// Rows may be arrays or @libsql/client-style Row objects: index by column position, which works for both.
function toObjects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}

function toTodo(o: Record<string, unknown>) {
  return { id: Number(o.id), title: String(o.title), done: Number(o.done) === 1, createdAt: Number(o.created_at) };
}

// Schema once: a read checks for the schema and only a database without it pays for the write
// batch, so GETs never write. The flag only helps while an isolate is reused; without that, the
// check is one cheap read per request. Two first requests racing both create: IF NOT EXISTS makes
// that harmless, and the batch is one transaction, so the index (created last) proves both exist.
let schemaReady = false;

async function ensureSchema() {
  if (schemaReady) return;
  const found = await sql.execute(
    "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'todos_done'",
  ) as SqlResult;
  if (found.rows.length === 0) await createSchema();
  schemaReady = true;
}

async function createSchema() {
  await sql.batch([
    `CREATE TABLE IF NOT EXISTS todos (
       id         INTEGER PRIMARY KEY AUTOINCREMENT,
       title      TEXT    NOT NULL,
       done       INTEGER NOT NULL DEFAULT 0,
       created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER))
     )`,
    `CREATE INDEX IF NOT EXISTS todos_done ON todos (done)`,
  ], "write");
}

async function readJson(req: Request): Promise<Record<string, unknown> | null> {
  try {
    const v = await req.json();
    return v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

const bad = (message: string, status = 400) => Response.json({ error: message }, { status });

export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname.replace(/\/+$/, "") || "/";

  try {
    await ensureSchema();

    if (path === "/" && req.method === "GET") {
      const done = url.searchParams.get("done");
      const r = (done === "0" || done === "1")
        ? await sql.execute("SELECT id, title, done, created_at FROM todos WHERE done = ? ORDER BY id", [Number(done)])
        : await sql.execute("SELECT id, title, done, created_at FROM todos ORDER BY id");
      return Response.json({ todos: toObjects(r as SqlResult).map(toTodo) });
    }

    if (path === "/" && req.method === "POST") {
      const body = await readJson(req);
      const title = typeof body?.title === "string" ? body.title.trim() : "";
      if (!title) return bad('body must be JSON like {"title": "…"}');

      const r = await sql.execute("INSERT INTO todos (title) VALUES (?)", [title]) as SqlResult;
      return Response.json({ id: Number(r.lastInsertRowid), title, done: false }, { status: 201 });
    }

    if (path === "/stats" && req.method === "GET") {
      // One round trip, one consistent snapshot; results come back in statement order.
      const [total, open] = await sql.batch([
        "SELECT COUNT(*) AS n FROM todos",
        "SELECT COUNT(*) AS n FROM todos WHERE done = 0",
      ], "read") as SqlResult[];
      return Response.json({ total: Number(toObjects(total)[0].n), open: Number(toObjects(open)[0].n) });
    }

    const m = path.match(/^\/(\d+)$/);
    if (m) {
      const id = Number(m[1]);
      const find = async () => {
        const r = await sql.execute("SELECT id, title, done, created_at FROM todos WHERE id = ?", [id]);
        const [row] = toObjects(r as SqlResult);
        return row ? toTodo(row) : null;
      };

      if (req.method === "GET") {
        const todo = await find();
        return todo ? Response.json(todo) : bad("not found", 404);
      }

      if (req.method === "PATCH") {
        const body = await readJson(req);
        if (!body) return bad("body must be a JSON object");
        const title = typeof body.title === "string" ? body.title.trim() : null;
        const done = typeof body.done === "boolean" ? (body.done ? 1 : 0) : null;
        if (title === "") return bad("title must not be empty");

        // COALESCE keeps the current value for fields that were not sent.
        const r = await sql.execute(
          "UPDATE todos SET title = COALESCE(?, title), done = COALESCE(?, done) WHERE id = ?",
          [title, done, id],
        ) as SqlResult;
        if (r.rowsAffected === 0) return bad("not found", 404);
        return Response.json(await find());
      }

      if (req.method === "DELETE") {
        const r = await sql.execute("DELETE FROM todos WHERE id = ?", [id]) as SqlResult;
        return r.rowsAffected === 0 ? bad("not found", 404) : Response.json({ id, deleted: true });
      }
    }

    return bad("not found", 404);
  } catch (e) {
    console.error(e); // stack goes to the service logs (kind=guest)
    return bad((e as Error).message, 500);
  }
}
