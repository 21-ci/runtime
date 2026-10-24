// `std:sql` -- SQLite per service: prepared statements and a transaction API.
//
//   execute(sql, args?)  -> { columns, rows, rowsAffected, lastInsertRowid }
//   batch(stmts, mode)   -> result[]     stmt = string | { sql, args }; mode = "read" | "write"
//   transaction(fn)      -> value        fn(tx) with tx.execute(sql, args?); commits when the
//                                        promise resolves, rolls back when it throws; 5 s cap
//
// Errors carry stable codes: sql_error | sql_timeout | sql_not_authorized | storage_quota.
export const sql = globalThis.__rt.sql;
