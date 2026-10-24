import { Database } from "std:sqlite"; // the same module as "bun:sqlite": bun code runs unchanged

// std:sqlite: synchronous SQLite on the service's own database, with bun:sqlite's API. No await, no
// promise: a statement runs on the thread that runs this code, a few µs for a point lookup (std:sql
// costs a thread hop per call). Same guards as std:sql (no ATTACH / temp tables / _rt_* tables, result
// caps, storage quota), plus a 100 ms budget per statement (`sql_timeout`) and a 20 ms wait for a
// lock held by an std:sql transaction (`sql_busy`). Keep long analytics on the async std:sql.
//
//   GET    /          list todos (?done=0|1)
//   POST   /          create {"title": "…"}
//   POST   /bulk      create many in one transaction {"titles": ["…", "…"]}; all or none
//   GET    /stats     counts
//   GET    /{id}      read one
//   PATCH  /{id}      update {"title"?: "…", "done"?: true|false}
//   DELETE /{id}      delete

// `new Database()` opens the service's own database; it takes no path. Top-level code runs on every
// request (a fresh isolate per task by default), and each statement below is prepared once per
// connection and then reused from the connection's statement cache.
const db = new Database();
db.run(`CREATE TABLE IF NOT EXISTS todos (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  title      TEXT    NOT NULL,
  done       INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER))
)`);
db.run("CREATE INDEX IF NOT EXISTS todos_done ON todos (done)");

const COLUMNS = "id, title, done, created_at AS createdAt";
const list = db.query(`SELECT ${COLUMNS} FROM todos ORDER BY id`);
const listByDone = db.query(`SELECT ${COLUMNS} FROM todos WHERE done = ?1 ORDER BY id`);
const find = db.query(`SELECT ${COLUMNS} FROM todos WHERE id = ?1`);
const insert = db.query("INSERT INTO todos (title) VALUES ($title)");
// COALESCE keeps the current value for fields that were not sent (null).
const update = db.query("UPDATE todos SET title = COALESCE($title, title), done = COALESCE($done, done) WHERE id = $id");
const remove = db.query("DELETE FROM todos WHERE id = ?1");
const stats = db.query("SELECT COUNT(*) AS total, COALESCE(SUM(done = 0), 0) AS open FROM todos");

interface Row {
  id: number;
  title: string;
  done: number;
  createdAt: number;
}

const toTodo = (r: Row) => ({ id: r.id, title: r.title, done: r.done === 1, createdAt: r.createdAt });

// db.transaction(fn) is BEGIN … COMMIT around a synchronous function and ROLLBACK when it throws.
// Inside another transaction it becomes a SAVEPOINT. It returns the function's value.
const createMany = db.transaction((titles: string[]) =>
  titles.map((title) => Number(insert.run({ $title: title }).lastInsertRowid))
);

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
    if (path === "/" && req.method === "GET") {
      const done = url.searchParams.get("done");
      const rows = (done === "0" || done === "1") ? listByDone.all(Number(done)) : list.all();
      return Response.json({ todos: (rows as Row[]).map(toTodo) });
    }

    if (path === "/" && req.method === "POST") {
      const body = await readJson(req);
      const title = typeof body?.title === "string" ? body.title.trim() : "";
      if (!title) return bad('body must be JSON like {"title": "…"}');
      const { lastInsertRowid } = insert.run({ $title: title });
      return Response.json({ id: Number(lastInsertRowid), title, done: false }, { status: 201 });
    }

    if (path === "/bulk" && req.method === "POST") {
      const body = await readJson(req);
      const titles = Array.isArray(body?.titles) ? body.titles.map((t) => String(t).trim()) : [];
      if (titles.length === 0 || titles.some((t) => !t)) return bad('body must be JSON like {"titles": ["…", "…"]}');
      // One empty title would have thrown inside the transaction and undone the others: all or none.
      return Response.json({ ids: createMany(titles) }, { status: 201 });
    }

    if (path === "/stats" && req.method === "GET") {
      const s = stats.get() as { total: number; open: number };
      return Response.json({ total: s.total, open: s.open });
    }

    const m = path.match(/^\/(\d+)$/);
    if (m) {
      const id = Number(m[1]);

      if (req.method === "GET") {
        const row = find.get(id) as Row | null; // get() answers null when nothing matches
        return row ? Response.json(toTodo(row)) : bad("not found", 404);
      }

      if (req.method === "PATCH") {
        const body = await readJson(req);
        if (!body) return bad("body must be a JSON object");
        const title = typeof body.title === "string" ? body.title.trim() : null;
        const done = typeof body.done === "boolean" ? (body.done ? 1 : 0) : null;
        if (title === "") return bad("title must not be empty");
        if (update.run({ $id: id, $title: title, $done: done }).changes === 0) return bad("not found", 404);
        return Response.json(toTodo(find.get(id) as Row));
      }

      if (req.method === "DELETE") {
        return remove.run(id).changes === 0 ? bad("not found", 404) : Response.json({ id, deleted: true });
      }
    }

    return bad("not found", 404);
  } catch (e) {
    console.error(e); // stack goes to the service logs (kind=guest)
    const err = e as { message: string; rtCode?: string };
    return bad(err.message, err.rtCode === "sql_busy" ? 503 : 500);
  }
}
