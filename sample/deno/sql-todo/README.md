# sql-todo

REST CRUD on `std:sql` (a per-service SQLite database).

## Shows

- `import { sql } from "std:sql"`: `sql.execute(statement, args?)` and `sql.batch(statements, "read" | "write")`
- Result shape: `{ columns, rows, rowsAffected, lastInsertRowid }`; the example builds objects from `columns` + `rows`
- **No tagged templates.** Write `sql.execute("… WHERE id = ?", [id])`; a template literal tagged with `sql` does not exist
- Schema once: a read of `sqlite_master` finds the schema; only a missing schema runs the `CREATE … IF NOT EXISTS` write batch, so GET requests never write (reads run on read-only connections and do not queue behind writes)
- Guardrails: no `ATTACH`, `load_extension`, `_rt_*` tables or schema-writing pragmas; 1 s budget per statement

## Deploy

```bash
cd sample/deno/sql-todo
../../scripts/deploy.sh . sql-todo
```

## Invoke

```bash
S=../../scripts/invoke.sh
$S sql-todo / POST '{"title": "Buy milk"}'
$S sql-todo /
$S sql-todo /1
$S sql-todo /1 PATCH '{"done": true}'
$S sql-todo '/?done=1'
$S sql-todo /stats
$S sql-todo /1 DELETE
```

## Expected responses

```json
// POST / (201)
{ "id": 1, "title": "Buy milk", "done": false }

// GET /
{ "todos": [ { "id": 1, "title": "Buy milk", "done": false, "createdAt": 1790882261 } ] }

// PATCH /1
{ "id": 1, "title": "Buy milk", "done": true, "createdAt": 1790882261 }

// GET /stats
{ "total": 1, "open": 0 }

// DELETE /1
{ "id": 1, "deleted": true }
```

Unknown ids answer `404 {"error": "not found"}`; a missing or empty `title` answers `400`.
