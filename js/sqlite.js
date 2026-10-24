// std:sqlite -- synchronous SQLite on the service's own database, bun:sqlite-compatible
// (decision D-B1). Part of the BASE SNAPSHOT, imported by
// js/bootstrap.js; the guest-facing module (js/rt_sqlite.js, also importable as `bun:sqlite`)
// only re-exports `globalThis.__rt.sqlite`. This file must be 7-bit ASCII.
//
//   import { Database } from "std:sqlite";
//   const db = new Database();                       // the service's own database; no path
//   db.run("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)");
//   const insert = db.query("INSERT INTO t (v) VALUES ($v)");
//   insert.run({ $v: "hello" });                     // { changes, lastInsertRowid }
//   db.query("SELECT * FROM t WHERE id = ?").get(1); // row object | null
//
// Every call runs on the Machine thread and returns at once (no promise). Same guards as the async
// `std:sql`: guest authorizer, result caps, quota; plus a 100 ms statement budget (`sql_timeout`)
// and a short wait for the pool's writer (`sql_busy`).

import { core, primordials } from "ext:core/mod.js";

const {
  op_rt_sqlite_open,
  op_rt_sqlite_prepare,
  op_rt_sqlite_run,
  op_rt_sqlite_exec,
  op_rt_sqlite_tx,
  op_rt_sqlite_in_tx,
} = core.ops;

const {
  ArrayBufferPrototypeGetByteLength,
  ArrayFrom,
  ArrayIsArray,
  ArrayPrototypePush,
  ArrayPrototypeSlice,
  ArrayBufferIsView,
  Error,
  JSONStringify,
  Number,
  ObjectFreeze,
  ObjectGetPrototypeOf,
  ObjectHasOwn,
  ObjectPrototype,
  ObjectSetPrototypeOf,
  RangeError,
  ReflectApply,
  SafeMap,
  String,
  StringPrototypeIndexOf,
  StringPrototypeSlice,
  StringPrototypeStartsWith,
  SymbolIterator,
  TypeError,
} = primordials;

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

// `code` is SQLite's name of the failure (SQLITE_CONSTRAINT_UNIQUE, ...), as in bun; `rtCode` is the
// platform's stable code: sql_error | sql_timeout | sql_busy | sql_not_authorized |
// sql_result_too_large | storage_quota.
class SQLiteError extends Error {
  constructor(message, code, errno, byteOffset, rtCode) {
    super(message);
    this.name = "SQLiteError";
    this.code = code;
    this.errno = errno;
    this.byteOffset = byteOffset;
    this.rtCode = rtCode;
  }
}

const OP_ERROR = "@sqlite:";

// The ops report a failed statement as "@sqlite:<rtCode>:<NAME>:<errno>:<offset>:<message>".
function toError(e) {
  const m = e instanceof Error ? e.message : undefined;
  if (typeof m === "string" && StringPrototypeStartsWith(m, OP_ERROR)) {
    const fields = [];
    let at = OP_ERROR.length;
    for (let i = 0; i < 4; i++) {
      const next = StringPrototypeIndexOf(m, ":", at);
      ArrayPrototypePush(fields, StringPrototypeSlice(m, at, next));
      at = next + 1;
    }
    return new SQLiteError(
      StringPrototypeSlice(m, at),
      fields[1],
      Number(fields[2]),
      Number(fields[3]),
      fields[0],
    );
  }
  if (e instanceof Error && e.code === undefined) {
    e.code = e.message;
  }
  return e;
}

function notSupported(what) {
  const e = new Error(what);
  e.code = "not_supported";
  return e;
}

// ---------------------------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------------------------

const MODE_ALL = 0;
const MODE_GET = 1;
const MODE_VALUES = 2;
const MODE_RUN = 3;

const FLAG_SAFE_INTEGERS = 1;
const FLAG_WRITE = 2;

const TX_BEGIN = 0;
const TX_COMMIT = 1;
const TX_ROLLBACK = 2;
const TX_MODES = { __proto__: null, deferred: 0, immediate: 1, exclusive: 2 };

const CREATE = ObjectFreeze({ __proto__: null });

function isPlainObject(v) {
  if (v === null || typeof v !== "object" || ArrayBufferIsView(v)) {
    return false;
  }
  const proto = ObjectGetPrototypeOf(v);
  return proto === ObjectPrototype || proto === null;
}

function isArrayBuffer(v) {
  try {
    ArrayBufferPrototypeGetByteLength(v);
    return true;
  } catch (_) {
    return false;
  }
}

function bindValue(v, json) {
  if (v === null || v === undefined) {
    return null;
  }
  const t = typeof v;
  if (t !== "object" && t !== "function") {
    return v;
  }
  if (ArrayBufferIsView(v) || isArrayBuffer(v)) {
    return v;
  }
  if (json) {
    const text = JSONStringify(v);
    if (text === undefined) {
      throw new TypeError("cannot serialize the argument as JSON");
    }
    return text;
  }
  // refused by the op (a TypeError; it reads nothing from the object)
  return v;
}

// The bind values of a statement as the SQL ops take them: a fresh array whose elements are own
// data properties (`Array.from` defines them, never through an inherited setter), each a
// primitive or a buffer; with `json` an object is replaced by its JSON text. Every guest getter,
// `toJSON` and proxy trap runs here, in JS: an op reads its arguments without running guest code,
// because it holds the isolate's op state and the connection while it runs (a guest callback
// that re-entered an op would abort the node or deadlock the Machine thread).
export function snapshotBinds(args, json) {
  if (args === null || args === undefined) {
    return [];
  }
  if (!ArrayIsArray(args)) {
    throw new TypeError("args must be an array");
  }
  return ArrayFrom({ __proto__: null, length: args.length }, (_, i) => bindValue(args[i], json));
}

// Positional arguments for the op: `stmt.all(1, 2)`, `stmt.all([1, 2])`, or one object of named
// parameters (`{ $a: 1 }`; without the prefix too -- `strict` makes a missing one an error).
function bindArgs(names, params, strict) {
  if (params.length === 1) {
    const p = params[0];
    if (ArrayIsArray(p)) {
      return p;
    }
    if (isPlainObject(p)) {
      const out = [];
      for (let i = 0; i < names.length; i++) {
        const name = names[i];
        let found = false;
        let value = null;
        if (name !== null) {
          if (ObjectHasOwn(p, name)) {
            found = true;
            value = p[name];
          } else {
            const bare = StringPrototypeSlice(name, 1);
            if (ObjectHasOwn(p, bare)) {
              found = true;
              value = p[bare];
            }
          }
        }
        if (!found && strict) {
          throw new RangeError("Missing parameter \"" + (name === null ? "#" + (i + 1) : name) + "\"");
        }
        ArrayPrototypePush(out, value);
      }
      return out;
    }
  }
  return params;
}

class Statement {
  #db;
  #sql;
  #info;
  #strict;
  #safe;
  #readonly;
  #as = null;
  #finalized = false;

  constructor(token, db, sql, info, options) {
    if (token !== CREATE) {
      throw new TypeError("Illegal constructor");
    }
    this.#db = db;
    this.#sql = sql;
    this.#info = info;
    this.#strict = options.strict;
    this.#safe = options.safeIntegers;
    this.#readonly = options.readonly;
  }

  get columnNames() {
    return ArrayPrototypeSlice(this.#info.columns);
  }

  get paramsCount() {
    return this.#info.paramNames.length;
  }

  get isFinalized() {
    return this.#finalized;
  }

  #run(mode, params) {
    if (this.#finalized) {
      throw new Error("Statement has finalized");
    }
    const args = snapshotBinds(bindArgs(this.#info.paramNames, params, this.#strict), false);
    let flags = this.#safe ? FLAG_SAFE_INTEGERS : 0;
    if (!this.#info.readonly) {
      if (this.#readonly) {
        throw new SQLiteError("attempt to write a readonly database", "SQLITE_READONLY", 8, -1, "sql_error");
      }
      flags |= FLAG_WRITE;
    }
    let out;
    try {
      out = op_rt_sqlite_run(this.#sql, args, mode, flags);
    } catch (e) {
      throw toError(e);
    }
    const cls = this.#as;
    if (cls !== null && mode !== MODE_RUN && mode !== MODE_VALUES && out !== null) {
      if (mode === MODE_GET) {
        ObjectSetPrototypeOf(out, cls.prototype);
      } else {
        for (let i = 0; i < out.length; i++) {
          ObjectSetPrototypeOf(out[i], cls.prototype);
        }
      }
    }
    return out;
  }

  // Every row as an object.
  all(...params) {
    return this.#run(MODE_ALL, params);
  }

  // The first row as an object, or null.
  get(...params) {
    return this.#run(MODE_GET, params);
  }

  // Every row as an array of values.
  values(...params) {
    return this.#run(MODE_VALUES, params);
  }

  // Run the statement; { changes, lastInsertRowid }.
  run(...params) {
    return this.#run(MODE_RUN, params);
  }

  // The rows of the statement, one at a time. (The statement runs to completion first, under the
  // row and byte caps: there is no cursor to keep open across calls.)
  iterate(...params) {
    return this.#run(MODE_ALL, params)[SymbolIterator]();
  }

  [SymbolIterator]() {
    return this.iterate();
  }

  // Rows become instances of `Class` (its prototype, without calling the constructor), as in bun.
  as(Class) {
    this.#as = typeof Class === "function" ? Class : null;
    return this;
  }

  finalize() {
    this.#finalized = true;
    this.#db.__forget(this.#sql, this);
  }

  toString() {
    return this.#sql;
  }
}

// ---------------------------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------------------------

const MAX_QUERY_CACHE_SIZE = 20;

const AsyncFunctionPrototype = ObjectGetPrototypeOf(async function () {});
const AsyncGeneratorFunctionPrototype = ObjectGetPrototypeOf(async function* () {});

function isAsyncFunction(fn) {
  const proto = ObjectGetPrototypeOf(fn);
  return proto === AsyncFunctionPrototype || proto === AsyncGeneratorFunctionPrototype;
}

function isThenable(v) {
  return v !== null && (typeof v === "object" || typeof v === "function") && typeof v.then === "function";
}

class Database {
  #options;
  #closed = false;
  #queries = new SafeMap();

  // `new Database()`: the service's own database. bun's path argument is not supported (a guest has
  // one database, the service's): any path, `:memory:` included, is refused.
  constructor(filename, options) {
    if (filename !== undefined && filename !== null && typeof filename === "object") {
      options = filename;
      filename = undefined;
    }
    if (filename !== undefined && filename !== null && filename !== "") {
      throw notSupported("std:sqlite opens the service's own database: call new Database() without a path");
    }
    const o = options === undefined || options === null ? {} : options;
    this.#options = {
      strict: o.strict === true,
      safeIntegers: o.safeIntegers === true,
      readonly: o.readonly === true,
    };
    try {
      op_rt_sqlite_open();
    } catch (e) {
      throw toError(e);
    }
  }

  static open(filename, options) {
    return new Database(filename, options);
  }

  static get MAX_QUERY_CACHE_SIZE() {
    return MAX_QUERY_CACHE_SIZE;
  }

  get filename() {
    return "data.db";
  }

  get inTransaction() {
    this.#assertOpen();
    return op_rt_sqlite_in_tx();
  }

  #assertOpen() {
    if (this.#closed) {
      throw new Error("Cannot use a closed database");
    }
  }

  // A statement not kept in the cache.
  prepare(sql) {
    this.#assertOpen();
    const text = String(sql);
    let info;
    try {
      info = op_rt_sqlite_prepare(text);
    } catch (e) {
      throw toError(e);
    }
    return new Statement(CREATE, this, text, info, this.#options);
  }

  // A statement kept in a small LRU cache by its text (up to MAX_QUERY_CACHE_SIZE).
  query(sql) {
    this.#assertOpen();
    const text = String(sql);
    const cached = this.#queries.get(text);
    if (cached !== undefined) {
      this.#queries.delete(text);
      this.#queries.set(text, cached);
      return cached;
    }
    const stmt = this.prepare(text);
    this.#queries.set(text, stmt);
    if (this.#queries.size > MAX_QUERY_CACHE_SIZE) {
      for (const [oldest, old] of this.#queries) {
        this.#queries.delete(oldest);
        old.finalize();
        break;
      }
    }
    return stmt;
  }

  // Called by Statement.finalize.
  __forget(text, stmt) {
    if (this.#queries.get(text) === stmt) {
      this.#queries.delete(text);
    }
  }

  // Run SQL; with parameters it is one statement, without them any number. { changes, lastInsertRowid }.
  run(sql, ...params) {
    this.#assertOpen();
    if (params.length > 0) {
      return this.prepare(sql).run(...params);
    }
    if (this.#options.readonly) {
      throw new SQLiteError("attempt to write a readonly database", "SQLITE_READONLY", 8, -1, "sql_error");
    }
    try {
      return op_rt_sqlite_exec(String(sql), this.#options.safeIntegers ? FLAG_SAFE_INTEGERS : 0);
    } catch (e) {
      throw toError(e);
    }
  }

  exec(sql, ...params) {
    return this.run(sql, ...params);
  }

  // Wrap `fn` in a transaction: BEGIN ... COMMIT, ROLLBACK when it throws, SAVEPOINTs when called
  // inside another transaction. `fn` must be synchronous: an async function (or async generator)
  // is refused here with a TypeError, before anything begins -- its code up to the first `await`
  // would run inside the transaction and the rest after it. A plain function that returns a
  // promise is rolled back and throws a TypeError. The result has .deferred, .immediate and
  // .exclusive variants (the BEGIN mode; nested calls ignore it).
  transaction(fn) {
    if (typeof fn !== "function") {
      throw new TypeError("Expected first argument to be a function");
    }
    if (isAsyncFunction(fn)) {
      throw new TypeError("std:sqlite transactions are synchronous: pass a function that is not async");
    }
    const db = this;
    const make = (mode) =>
      function (...args) {
        return db.#transact(fn, mode, this, args);
      };
    const wrapper = make(TX_MODES.deferred);
    wrapper.deferred = make(TX_MODES.deferred);
    wrapper.immediate = make(TX_MODES.immediate);
    wrapper.exclusive = make(TX_MODES.exclusive);
    return wrapper;
  }

  #transact(fn, mode, self, args) {
    this.#assertOpen();
    try {
      op_rt_sqlite_tx(TX_BEGIN, mode);
    } catch (e) {
      throw toError(e);
    }
    let result;
    try {
      result = ReflectApply(fn, self, args);
    } catch (e) {
      rollbackQuietly();
      throw e;
    }
    if (isThenable(result)) {
      rollbackQuietly();
      throw new TypeError("std:sqlite transactions are synchronous: the function must not return a promise");
    }
    try {
      op_rt_sqlite_tx(TX_COMMIT, 0);
    } catch (e) {
      throw toError(e);
    }
    return result;
  }

  // Forget the cached statements. The connection belongs to the service on this Machine and stays
  // open for the next request.
  close() {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    for (const stmt of this.#queries.values()) {
      stmt.finalize();
    }
    this.#queries.clear();
  }

  loadExtension() {
    throw notSupported("loadExtension is not supported");
  }

  serialize() {
    throw notSupported("serialize is not supported (export the database with the control API)");
  }

  static deserialize() {
    throw notSupported("deserialize is not supported");
  }

  fileControl() {
    throw notSupported("fileControl is not supported");
  }
}

function rollbackQuietly() {
  try {
    op_rt_sqlite_tx(TX_ROLLBACK, 0);
  } catch (_) {
    // the transaction was already ended by the error that is being reported
  }
}

const constants = ObjectFreeze({
  __proto__: null,
  SQLITE_OPEN_READONLY: 1,
  SQLITE_OPEN_READWRITE: 2,
  SQLITE_OPEN_CREATE: 4,
});

ObjectFreeze(Statement.prototype);
ObjectFreeze(Database.prototype);
ObjectFreeze(SQLiteError.prototype);

export const sqlite = ObjectFreeze({ __proto__: null, Database, Statement, SQLiteError, constants });
