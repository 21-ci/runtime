// `std:sqlite` -- synchronous SQLite on the service's own database, bun:sqlite-compatible (docs/handlers.md). Also importable as `bun:sqlite`.
//
//   new Database()                      the service's own database (no path; any path is refused)
//   db.query(sql) / db.prepare(sql)     -> Statement   (query() is cached, prepare() is not)
//   stmt.all(...p) / .get(...p) / .values(...p) / .run(...p) / .iterate(...p)
//   db.run(sql, ...p) / db.exec(sql)    -> { changes, lastInsertRowid }
//   db.transaction(fn)                  -> function; .deferred / .immediate / .exclusive; nests as
//                                          savepoints; fn must be synchronous
//   db.inTransaction, db.close()
//   options: { strict, safeIntegers (bigint results), readonly }
//
// Parameters: `?`, `?NNN` positionally; `$x`, `:x`, `@x` through one object (without the prefix too
// in `strict` mode). Values: null/undefined -> NULL, number, bigint, boolean -> 0/1, string,
// Uint8Array -> BLOB. Rows come back with numbers, strings and Uint8Array (bigint with safeIntegers).
//
// Errors are `SQLiteError { code: "SQLITE_CONSTRAINT_UNIQUE", errno, byteOffset, rtCode }`, with
// `rtCode` one of sql_error | sql_timeout | sql_busy | sql_not_authorized | sql_result_too_large |
// storage_quota. The implementation lives in the base snapshot (js/sqlite.js); this module only
// exposes it.
const s = globalThis.__rt.sqlite;
export const Database = s.Database;
export const Statement = s.Statement;
export const SQLiteError = s.SQLiteError;
export const constants = s.constants;
export default s.Database;
