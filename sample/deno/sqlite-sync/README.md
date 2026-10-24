# sqlite-sync

REST CRUD on `std:sqlite` (also importable as `bun:sqlite`): the same per-service SQLite database as `std:sql`, with
bun:sqlite's synchronous API.

## Shows

- `import { Database } from "std:sqlite"` and `new Database()`: the service's own database, no path
- `db.query(sql)` statements with `.all()` / `.get()` / `.run()`, called without `await`; `.get()` answers `null` for no row
- Named (`$title`) and numbered (`?1`) parameters; `.run()` answers `{ changes, lastInsertRowid }`
- `db.transaction(fn)`: `POST /bulk` creates many rows all-or-none (a throw rolls back; nested calls are savepoints)
- Schema at module scope: `CREATE … IF NOT EXISTS` is cheap and needs no `await`
- Guardrails of `std:sql` (no `ATTACH`, `load_extension`, `_rt_*` tables, schema-writing pragmas) plus a **100 ms** budget per
  statement (`sql_timeout`) and a **20 ms** wait for a write lock held by an `std:sql` transaction (`sql_busy`, answered `503` here)
- When to prefer which: `std:sqlite` for short statements on the request path (a point lookup costs a few µs instead of a thread
  hop); `std:sql` for long queries, read batches and interactive transactions that must `await` between statements

## Deploy

```bash
cd sample/deno/sqlite-sync
../../scripts/deploy.sh . sqlite-sync
```

## Invoke

```bash
S=../../scripts/invoke.sh
$S sqlite-sync / POST '{"title": "Buy milk"}'
$S sqlite-sync /bulk POST '{"titles": ["Call mom", "Pay rent"]}'
$S sqlite-sync /
$S sqlite-sync /1
$S sqlite-sync /1 PATCH '{"done": true}'
$S sqlite-sync '/?done=1'
$S sqlite-sync /stats
$S sqlite-sync /1 DELETE
```

## Expected responses

```json
// POST / (201)
{ "id": 1, "title": "Buy milk", "done": false }

// POST /bulk (201)
{ "ids": [2, 3] }

// GET /
{ "todos": [ { "id": 1, "title": "Buy milk", "done": false, "createdAt": 1790882261 } ] }

// PATCH /1
{ "id": 1, "title": "Buy milk", "done": true, "createdAt": 1790882261 }

// GET /stats
{ "total": 3, "open": 2 }

// DELETE /1
{ "id": 1, "deleted": true }
```

Unknown ids answer `404 {"error": "not found"}`; a missing or empty `title` (or any empty title in `/bulk`) answers `400`.
