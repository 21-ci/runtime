// rt bootstrap: runs once when the BASE SNAPSHOT is built.
// Everything installed here is shared, read-only state of every isolate, so it contains no
// secrets and no per-tenant data (RS-16). Per-task data (env, ctx, request) is read through ops
// at call time. This file must be 7-bit ASCII (deno_core extension rule).
//
//  * wires the web platform from deno_web / deno_fetch / deno_crypto onto globalThis
//  * replaces the `fetch` transport with ops that go through rt-host's EgressGuard
//  * installs the frozen clock (Date.now / performance.now advance only at I/O + timer boundaries)
//  * removes banned globals: Worker, SharedArrayBuffer, Atomics.wait, ShadowRealm, Deno.* except env
//    (the `Deno` part runs in `__rt_seal()`, called by the executor on each new isolate)
//  * wraps the WebAssembly JS API so every wasm memory counts against the task's memory limit
//  * builds the std:* APIs and the task dispatcher (`__rt.start`)

import { core, primordials } from "ext:core/mod.js";
import { snapshotBinds, sqlite } from "ext:rt_ext/sqlite.js";

const ops = core.ops;
const {
  ObjectDefineProperty,
  ObjectCreate,
  ObjectFreeze,
  ObjectKeys,
  ObjectSetPrototypeOf,
  ReflectConstruct,
  ArrayFrom,
  ArrayIsArray,
  ArrayPrototypeJoin,
  ArrayPrototypeMap,
  ArrayPrototypePush,
  ArrayPrototypeSplice,
  BigInt,
  Number,
  NumberIsFinite,
  NumberIsSafeInteger,
  PromiseAllSettled,
  PromiseReject,
  PromiseResolve,
  PromiseRace,
  Error,
  TypeError,
  JSONStringify,
  JSONParse,
  String,
  Uint8Array,
  ArrayBufferIsView,
  ArrayBufferPrototypeGetByteLength,
  MathTrunc,
  RangeError,
  DataViewPrototypeGetBuffer,
  DataViewPrototypeGetByteLength,
  DataViewPrototypeGetByteOffset,
  ObjectGetOwnPropertyDescriptor,
  PromisePrototypeThen,
  Proxy,
  ReflectApply,
  RegExpPrototypeTest,
  FunctionPrototypeToString,
  SafeSet,
  SafeWeakMap,
  SafeWeakSet,
  SetPrototypeAdd,
  SetPrototypeHas,
  StringFromCharCode,
  StringPrototypeCharCodeAt,
  StringPrototypeStartsWith,
  StringPrototypeToLowerCase,
  StringPrototypeTrim,
  TypedArrayPrototypeGetBuffer,
  TypedArrayPrototypeGetByteLength,
  TypedArrayPrototypeGetByteOffset,
  TypedArrayPrototypeGetLength,
  TypedArrayPrototypeSet,
  WeakMapPrototypeGet,
  WeakMapPrototypeSet,
  WeakSetPrototypeAdd,
  WeakSetPrototypeHas,
} = primordials;

const {
  op_rt_log,
  op_rt_log_fields,
  op_rt_secret_get,
  op_rt_now,
  op_rt_perf_now,
  op_rt_env_get,
  op_rt_env_all,
  op_rt_respond,
  op_rt_fail,
  op_rt_resp_chunk,
  op_rt_resp_end,
  op_rt_resp_abort,
  op_rt_fetch,
  op_rt_body_read,
  op_rt_body_close,
  op_rt_kv_get,
  op_rt_kv_get_many,
  op_rt_kv_set,
  op_rt_kv_delete,
  op_rt_kv_list,
  op_rt_kv_incr,
  op_rt_kv_cas,
  op_rt_kv_expire,
  op_rt_kv_ttl,
  op_rt_sql_execute,
  op_rt_sql_batch,
  op_rt_sql_begin,
  op_rt_sql_tx_execute,
  op_rt_sql_tx_end,
  op_rt_blob_head,
  op_rt_blob_get,
  op_rt_blob_put_begin,
  op_rt_blob_put_write,
  op_rt_blob_put_end,
  op_rt_blob_delete,
  op_rt_blob_list,
  op_rt_enqueue,
  op_rt_lock_acquire,
  op_rt_lock_release,
  op_rt_schedule_after,
  op_rt_schedule_at,
  op_rt_schedule_cancel,
  op_rt_schedule_list,
  op_rt_call,
  op_rt_sdk_invoke,
  op_rt_platform_fetch,
  op_rt_wasm_track,
  op_rt_mem_admit,
  op_rt_ws_accept,
  op_rt_ws_send,
  op_rt_ws_broadcast,
  op_rt_ws_close,
  op_rt_ws_close_all,
  op_rt_ws_list,
  op_rt_ws_get,
  op_rt_ws_count,
  op_rt_ws_objects,
  op_rt_ws_set_tags,
  op_rt_ws_attachment,
  op_rt_ws_set_attachment,
  op_rt_ws_auto_response,
  op_rt_ws_event_data,
  op_rt_task_commit,
  op_rt_call_object,
  op_rt_storage_get,
  op_rt_storage_put,
  op_rt_storage_delete,
  op_rt_storage_list,
  op_rt_storage_delete_all,
  op_rt_alarm_get,
  op_rt_alarm_set,
  op_rt_ws_connect,
  op_rt_ws_client_send,
  op_rt_ws_client_recv,
  op_rt_ws_client_close,
} = ops;

const loadExtScript = core.loadExtScript;

// ---------------------------------------------------------------------------------------------
// Web platform
// ---------------------------------------------------------------------------------------------

const webidl = loadExtScript("ext:deno_webidl/00_webidl.js");
const urlMod = loadExtScript("ext:deno_web/00_url.js");
const urlPatternMod = loadExtScript("ext:deno_web/01_urlpattern.js");
const domException = loadExtScript("ext:deno_web/01_dom_exception.js");
const eventMod = loadExtScript("ext:deno_web/02_event.js");
const structuredCloneMod = loadExtScript("ext:deno_web/02_structured_clone.js");
const timers = loadExtScript("ext:deno_web/02_timers.js");
// Unbound copies: deno_web's timers reject any `this` other than undefined / globalThis, so
// `timers.setTimeout(...)` (this = the module object) throws "Illegal invocation".
const setTimer = timers.setTimeout;
const clearTimer = timers.clearTimeout;
const abortMod = loadExtScript("ext:deno_web/03_abort_signal.js");
const base64Mod = loadExtScript("ext:deno_web/05_base64.js");
const streams = loadExtScript("ext:deno_web/06_streams.js");
const encoding = loadExtScript("ext:deno_web/08_text_encoding.js");
const fileMod = loadExtScript("ext:deno_web/09_file.js");
const fileReaderMod = loadExtScript("ext:deno_web/10_filereader.js");
const compression = loadExtScript("ext:deno_web/14_compression.js");
const perfMod = loadExtScript("ext:deno_web/15_performance.js");
const messagePort = loadExtScript("ext:deno_web/13_message_port.js");
const consoleMod = loadExtScript("ext:deno_web/01_console.js");
const headersMod = loadExtScript("ext:deno_fetch/20_headers.js");
const formDataMod = loadExtScript("ext:deno_fetch/21_formdata.js");
const requestMod = loadExtScript("ext:deno_fetch/23_request.js");
const responseMod = loadExtScript("ext:deno_fetch/23_response.js");
const cryptoMod = loadExtScript("ext:deno_crypto/00_crypto.js");

const { Request } = requestMod;
const { Response } = responseMod;
const { Headers } = headersMod;
const { ReadableStream } = streams;

function defineGlobal(name, value) {
  ObjectDefineProperty(globalThis, name, {
    value,
    writable: true,
    configurable: true,
    enumerable: false,
  });
}

eventMod.saveGlobalThisReference(globalThis);

const globals = {
  URL: urlMod.URL,
  URLSearchParams: urlMod.URLSearchParams,
  URLPattern: urlPatternMod.URLPattern,
  DOMException: domException.DOMException,
  Event: eventMod.Event,
  EventTarget: eventMod.EventTarget,
  CustomEvent: eventMod.CustomEvent,
  ErrorEvent: eventMod.ErrorEvent,
  MessageEvent: eventMod.MessageEvent,
  CloseEvent: eventMod.CloseEvent,
  ProgressEvent: eventMod.ProgressEvent,
  PromiseRejectionEvent: eventMod.PromiseRejectionEvent,
  reportError: eventMod.reportError,
  structuredClone: structuredCloneMod.structuredClone,
  setTimeout: timers.setTimeout,
  setInterval: timers.setInterval,
  clearTimeout: timers.clearTimeout,
  clearInterval: timers.clearInterval,
  AbortController: abortMod.AbortController,
  AbortSignal: abortMod.AbortSignal,
  atob: base64Mod.atob,
  btoa: base64Mod.btoa,
  ReadableStream: streams.ReadableStream,
  ReadableStreamDefaultReader: streams.ReadableStreamDefaultReader,
  ReadableStreamBYOBReader: streams.ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest: streams.ReadableStreamBYOBRequest,
  ReadableByteStreamController: streams.ReadableByteStreamController,
  ReadableStreamDefaultController: streams.ReadableStreamDefaultController,
  WritableStream: streams.WritableStream,
  WritableStreamDefaultWriter: streams.WritableStreamDefaultWriter,
  WritableStreamDefaultController: streams.WritableStreamDefaultController,
  TransformStream: streams.TransformStream,
  TransformStreamDefaultController: streams.TransformStreamDefaultController,
  ByteLengthQueuingStrategy: streams.ByteLengthQueuingStrategy,
  CountQueuingStrategy: streams.CountQueuingStrategy,
  TextEncoder: encoding.TextEncoder,
  TextDecoder: encoding.TextDecoder,
  TextEncoderStream: encoding.TextEncoderStream,
  TextDecoderStream: encoding.TextDecoderStream,
  Blob: fileMod.Blob,
  File: fileMod.File,
  FileReader: fileReaderMod.FileReader,
  CompressionStream: compression.CompressionStream,
  DecompressionStream: compression.DecompressionStream,
  MessageChannel: messagePort.MessageChannel,
  MessagePort: messagePort.MessagePort,
  Performance: perfMod.Performance,
  PerformanceEntry: perfMod.PerformanceEntry,
  PerformanceMark: perfMod.PerformanceMark,
  PerformanceMeasure: perfMod.PerformanceMeasure,
  Headers: headersMod.Headers,
  FormData: formDataMod.FormData,
  Request: requestMod.Request,
  Response: responseMod.Response,
  Crypto: cryptoMod.Crypto,
  CryptoKey: cryptoMod.CryptoKey,
  SubtleCrypto: cryptoMod.SubtleCrypto,
};
for (const name of ObjectKeys(globals)) {
  if (globals[name] !== undefined) {
    defineGlobal(name, globals[name]);
  }
}
ObjectDefineProperty(globalThis, "crypto", {
  get() {
    return cryptoMod.crypto;
  },
  configurable: true,
  enumerable: false,
});
defineGlobal("performance", perfMod.performance);

// console -> the task log (level: 0 debug, 1 info, 2 warn, 3+ error)
const consoleObj = new consoleMod.Console((msg, level) => {
  const l = level >= 3 ? 3 : level === 2 ? 2 : level === 0 ? 0 : 1;
  op_rt_log(l, msg);
});
defineGlobal("console", consoleObj);

// ---------------------------------------------------------------------------------------------
// Frozen clock (SECURITY.md): the host advances the clock only when the event loop turns.
// ---------------------------------------------------------------------------------------------

const RealDate = globalThis.Date;
function FrozenDate(...args) {
  if (new.target === undefined) {
    return new RealDate(op_rt_now()).toString();
  }
  if (args.length === 0) {
    return ReflectConstruct(RealDate, [op_rt_now()], new.target);
  }
  return ReflectConstruct(RealDate, args, new.target);
}
ObjectSetPrototypeOf(FrozenDate, RealDate);
FrozenDate.prototype = RealDate.prototype;
ObjectDefineProperty(RealDate.prototype, "constructor", {
  value: FrozenDate,
  writable: true,
  configurable: true,
  enumerable: false,
});
ObjectDefineProperty(FrozenDate, "now", {
  value: function now() {
    return op_rt_now();
  },
  writable: true,
  configurable: true,
  enumerable: false,
});
ObjectDefineProperty(FrozenDate, "name", { value: "Date", configurable: true });
defineGlobal("Date", FrozenDate);

ObjectDefineProperty(perfMod.performance, "now", {
  value: function now() {
    return op_rt_perf_now();
  },
  writable: true,
  configurable: true,
  enumerable: false,
});

// ---------------------------------------------------------------------------------------------
// Env shim (the only thing left of `Deno`)
// ---------------------------------------------------------------------------------------------

function readOnlyEnv() {
  throw new TypeError("environment variables are read-only");
}

const env = ObjectFreeze({
  __proto__: null,
  get(name) {
    const v = op_rt_env_get(String(name));
    return v === null ? undefined : v;
  },
  has(name) {
    return op_rt_env_get(String(name)) !== null;
  },
  toObject() {
    const out = {};
    const all = op_rt_env_all();
    for (let i = 0; i < all.length; i++) {
      out[all[i][0]] = all[i][1];
    }
    return out;
  },
  set: readOnlyEnv,
  delete: readOnlyEnv,
});

// ---------------------------------------------------------------------------------------------
// Errors from ops carry a stable code as their message (SEC-26): keep it, add `.code`.
// ---------------------------------------------------------------------------------------------

function coded(e) {
  if (e instanceof Error) {
    e.code = e.message;
    return e;
  }
  const err = new Error(String(e));
  err.code = err.message;
  return err;
}

async function guard(promise) {
  try {
    return await promise;
  } catch (e) {
    throw coded(e);
  }
}

// ---------------------------------------------------------------------------------------------
// fetch through the egress guard
// ---------------------------------------------------------------------------------------------

function bodyStream(rid) {
  return new ReadableStream({
    async pull(controller) {
      const chunk = await op_rt_body_read(rid);
      if (chunk === null) {
        controller.close();
        op_rt_body_close(rid);
      } else {
        controller.enqueue(chunk);
      }
    },
    cancel() {
      op_rt_body_close(rid);
    },
  });
}

function toResponse(res) {
  const stream = res.bodyRid === null ? null : bodyStream(res.bodyRid);
  const nullBody = res.status === 204 || res.status === 205 || res.status === 304;
  return new Response(nullBody ? null : stream, {
    status: res.status,
    headers: res.headers,
  });
}

async function requestParts(input, init) {
  const req = new Request(input, init);
  let body = new Uint8Array(0);
  let hasBody = false;
  if (req.body !== null) {
    body = new Uint8Array(await req.arrayBuffer());
    hasBody = true;
  }
  const headers = [];
  for (const pair of req.headers) {
    ArrayPrototypePush(headers, [pair[0], pair[1]]);
  }
  return { method: req.method, url: req.url, headers, hasBody, body };
}

async function rtFetch(input, init) {
  const p = await requestParts(input, init);
  let res;
  try {
    res = await op_rt_fetch({ method: p.method, url: p.url, headers: p.headers, hasBody: p.hasBody }, p.body);
  } catch (e) {
    throw new TypeError(e instanceof Error ? e.message : String(e));
  }
  return toResponse(res);
}
defineGlobal("fetch", rtFetch);

// ---------------------------------------------------------------------------------------------
// std:kv
// ---------------------------------------------------------------------------------------------

const textEncoder = new encoding.TextEncoder();
const textDecoder = new encoding.TextDecoder();

function encodeValue(value) {
  if (value instanceof Uint8Array) {
    return { bytes: true, data: value };
  }
  const text = JSONStringify(value);
  if (text === undefined) {
    throw new TypeError("kv value is not JSON-serializable");
  }
  return { bytes: false, data: textEncoder.encode(text) };
}

function decodeValue(v) {
  if (v === null) {
    return null;
  }
  return v.tag === "bytes" ? v.data : JSONParse(textDecoder.decode(v.data));
}

const I64_MIN = -9223372036854775808n;
const I64_MAX = 9223372036854775807n;

// `kv.incr` delta: a safe-integer number or a bigint in the i64 range (default 1).
function incrDelta(delta) {
  if (delta === undefined) {
    return "1";
  }
  if (typeof delta === "number" && NumberIsSafeInteger(delta)) {
    return String(delta);
  }
  if (typeof delta === "bigint" && delta >= I64_MIN && delta <= I64_MAX) {
    return String(delta);
  }
  throw coded(new Error("validation_failed"));
}

// `cas`'s `expected` / `next`: `null` or `undefined` is "absent" (or "delete" for `next`).
function encodeOptional(value) {
  if (value === null || value === undefined) {
    return { has: false, bytes: false, data: new Uint8Array(0) };
  }
  const enc = encodeValue(value);
  return { has: true, bytes: enc.bytes, data: enc.data };
}

// `ttlSeconds` for `expire` (`null` clears): a finite, non-negative number, else validation_failed.
function ttlMsOf(ttlSeconds) {
  if (ttlSeconds === null || ttlSeconds === undefined) {
    return -1;
  }
  if (typeof ttlSeconds === "number" && ttlSeconds >= 0 && ttlSeconds <= Number.MAX_SAFE_INTEGER) {
    return ttlSeconds * 1000;
  }
  throw coded(new Error("validation_failed"));
}

// `opts.ttlSeconds` of `set` / `incr` / `cas`: absent or `null` = no expiry, else the same rule as
// `expire` (a NaN, negative or non-number TTL must not silently mean "forever", DOPS-001).
function ttlOptMs(opts) {
  return opts ? ttlMsOf(opts.ttlSeconds) : -1;
}

const kv = ObjectFreeze({
  __proto__: null,
  async get(key) {
    return decodeValue(await guard(op_rt_kv_get(String(key))));
  },
  async getMany(keys) {
    const out = await guard(op_rt_kv_get_many(ArrayPrototypeMap(keys, (k) => String(k))));
    return ArrayPrototypeMap(out, decodeValue);
  },
  async set(key, value, opts) {
    const enc = encodeValue(value);
    const ttl = ttlOptMs(opts);
    await guard(op_rt_kv_set(String(key), enc.data, enc.bytes, ttl));
  },
  async delete(key) {
    return await guard(op_rt_kv_delete(String(key)));
  },
  // Atomic: adds `delta` to the integer counter at `key`, returns the new value (a bigint for a
  // bigint delta). A missing or expired key starts at 0 and gets `ttlSeconds`; an existing
  // counter keeps its expiry.
  async incr(key, delta, opts) {
    const d = incrDelta(delta);
    const ttl = ttlOptMs(opts);
    const n = await guard(op_rt_kv_incr(String(key), d, ttl));
    return typeof delta === "bigint" ? BigInt(n) : Number(n);
  },
  // Atomic compare-and-swap: `next` is written (or deleted when `null`) only when the current value
  // equals `expected` (same bytes and type; `null` = absent or expired). Resolves to whether it swapped.
  async cas(key, expected, next, opts) {
    const exp = encodeOptional(expected);
    const nxt = encodeOptional(next);
    const ttl = ttlOptMs(opts);
    return await guard(
      op_rt_kv_cas(String(key), exp.has, exp.bytes, exp.data, nxt.has, nxt.bytes, nxt.data, ttl),
    );
  },
  // Sets (`ttlSeconds` a number) or clears (`null`) the expiry of a live key; false when absent.
  async expire(key, ttlSeconds) {
    return await guard(op_rt_kv_expire(String(key), ttlMsOf(ttlSeconds)));
  },
  // Seconds left until the key expires (rounded up); null when absent or without an expiry.
  async ttl(key) {
    const left = await guard(op_rt_kv_ttl(String(key)));
    return left < 0 ? null : left;
  },
  async list(opts) {
    const o = opts || {};
    const page = await guard(
      op_rt_kv_list({
        prefix: o.prefix === undefined ? "" : String(o.prefix),
        cursor: o.cursor === undefined || o.cursor === null ? null : String(o.cursor),
        limit: o.limit === undefined ? 100 : o.limit,
        reverse: o.reverse === true,
      }),
    );
    return {
      entries: ArrayPrototypeMap(page.entries, (e) => ({ key: e.key, value: decodeValue(e) })),
      cursor: page.cursor,
    };
  },
});

// ---------------------------------------------------------------------------------------------
// std:sql
// ---------------------------------------------------------------------------------------------

// Statements and results cross the op boundary as plain JS values: the ops read the arguments from
// them (null/undefined -> NULL, number, bigint, boolean -> 0/1, string, Uint8Array -> BLOB) and build
// the result as JS values (blobs come back as Uint8Array), see rt-exec-deno `v8sql.rs`.

const SQL_TX_CAP_MS = 5000;

// The statements of a batch as the op takes them (see `snapshotBinds`): `sql` and `args` are read
// here, once, and handed over in a null-prototype object of own data properties.
function snapshotStmts(stmts) {
  if (!ArrayIsArray(stmts)) {
    throw new TypeError("stmts must be an array");
  }
  return ArrayFrom({ __proto__: null, length: stmts.length }, (_, i) => {
    const s = stmts[i];
    if (typeof s === "string") {
      return s;
    }
    if (s === null || typeof s !== "object") {
      throw new TypeError("a statement is a string or { sql, args }");
    }
    const text = s.sql;
    if (text === null || text === undefined) {
      throw new TypeError("a statement needs `sql`");
    }
    return { __proto__: null, sql: String(text), args: snapshotBinds(s.args, true) };
  });
}

const sql = ObjectFreeze({
  __proto__: null,
  async execute(text, args) {
    return await guard(op_rt_sql_execute(String(text), snapshotBinds(args, true)));
  },
  async batch(stmts, mode) {
    const m = mode === "read" ? "read" : "write";
    return await guard(op_rt_sql_batch(snapshotStmts(stmts), m));
  },
  async transaction(fn) {
    const rid = await guard(op_rt_sql_begin());
    let done = false;
    const tx = {
      async execute(text, args) {
        if (done) {
          throw new Error("transaction is finished");
        }
        return await guard(op_rt_sql_tx_execute(rid, String(text), snapshotBinds(args, true)));
      },
    };
    let timer;
    const cap = new Promise((_, reject) => {
      timer = setTimer(() => reject(new Error("sql_timeout")), SQL_TX_CAP_MS);
    });
    try {
      const value = await PromiseRace([PromiseResolve(fn(tx)), cap]);
      clearTimer(timer);
      done = true;
      await guard(op_rt_sql_tx_end(rid, true));
      return value;
    } catch (e) {
      clearTimer(timer);
      done = true;
      try {
        await op_rt_sql_tx_end(rid, false);
      } catch (_) {
        // already rolled back
      }
      throw e;
    }
  },
});

// ---------------------------------------------------------------------------------------------
// std:blob
// ---------------------------------------------------------------------------------------------

async function putBody(rid, body) {
  if (body instanceof Uint8Array) {
    await op_rt_blob_put_write(rid, body);
  } else if (typeof body === "string") {
    await op_rt_blob_put_write(rid, textEncoder.encode(body));
  } else if (body instanceof ArrayBuffer) {
    await op_rt_blob_put_write(rid, new Uint8Array(body));
  } else if (body && typeof body.getReader === "function") {
    const reader = body.getReader();
    for (;;) {
      const r = await reader.read();
      if (r.done) {
        break;
      }
      await op_rt_blob_put_write(rid, r.value);
    }
  } else if (body && typeof body.arrayBuffer === "function") {
    await op_rt_blob_put_write(rid, new Uint8Array(await body.arrayBuffer()));
  } else {
    throw new TypeError("unsupported blob body");
  }
}

const blob = ObjectFreeze({
  __proto__: null,
  async get(key) {
    const r = await guard(op_rt_blob_get(String(key)));
    if (r === null) {
      return null;
    }
    return { body: bodyStream(r.rid), size: r.size, contentType: r.contentType };
  },
  async head(key) {
    return await guard(op_rt_blob_head(String(key)));
  },
  async put(key, body, opts) {
    const ct = opts && opts.contentType ? String(opts.contentType) : null;
    const rid = await guard(op_rt_blob_put_begin(String(key), ct));
    try {
      await putBody(rid, body);
    } catch (e) {
      await op_rt_blob_put_end(rid, false).catch(() => {});
      throw coded(e);
    }
    await guard(op_rt_blob_put_end(rid, true));
  },
  async delete(key) {
    return await guard(op_rt_blob_delete(String(key)));
  },
  async list(opts) {
    const o = opts || {};
    return await guard(
      op_rt_blob_list({
        prefix: o.prefix === undefined ? "" : String(o.prefix),
        cursor: o.cursor === undefined || o.cursor === null ? null : String(o.cursor),
        limit: o.limit === undefined ? 100 : o.limit,
      }),
    );
  },
});

// ---------------------------------------------------------------------------------------------
// std:lock
// ---------------------------------------------------------------------------------------------

// The bounds `rt-host` lock.rs enforces (validation_failed beyond them).
const LOCK_WAIT_DEFAULT_MS = 10000;
const LOCK_WAIT_MAX_MS = 300000;
const LOCK_LEASE_DEFAULT_MS = 30000;
const LOCK_LEASE_MAX_MS = 900000;

function lockMs(value, fallback, max) {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "number" || !NumberIsFinite(value) || value < 0 || value > max) {
    throw coded(new Error("validation_failed"));
  }
  return value;
}

// The options of `lock(key, fn, opts)` / `lock.try(key, fn, opts)` as the two waits and leases.
function lockOptions(key, fn, opts) {
  if (typeof key !== "string" || typeof fn !== "function") {
    throw coded(new Error("validation_failed"));
  }
  const o = opts === undefined || opts === null ? {} : opts;
  if (typeof o !== "object") {
    throw coded(new Error("validation_failed"));
  }
  return {
    waitMs: lockMs(o.waitMs, LOCK_WAIT_DEFAULT_MS, LOCK_WAIT_MAX_MS),
    leaseMs: lockMs(o.leaseMs, LOCK_LEASE_DEFAULT_MS, LOCK_LEASE_MAX_MS),
  };
}

// Holds `key` while `fn` runs. The hold is released however `fn` settles.
async function lockRun(key, fn, waitMs, leaseMs) {
  let id;
  try {
    id = await guard(op_rt_lock_acquire(key, waitMs, leaseMs));
  } catch (e) {
    if (e.code === "lock_timeout") {
      return { acquired: false };
    }
    throw e;
  }
  try {
    return { acquired: true, value: await fn() };
  } finally {
    op_rt_lock_release(id);
  }
}

// `lock(key, fn, opts)`: the value of `fn()`; `lock_timeout` when `key` cannot be had in `waitMs`.
async function lock(key, fn, opts) {
  const o = lockOptions(key, fn, opts);
  const r = await lockRun(key, fn, o.waitMs, o.leaseMs);
  if (!r.acquired) {
    throw coded(new Error("lock_timeout"));
  }
  return r.value;
}

// `lock.try(key, fn, opts)`: never waits; `{ acquired: false }` when `key` is held.
async function lockTry(key, fn, opts) {
  const o = lockOptions(key, fn, opts);
  return await lockRun(key, fn, 0, o.leaseMs);
}

const lockApi = lock;
ObjectDefineProperty(lockApi, "try", { value: lockTry, enumerable: false });
ObjectFreeze(lockApi);

// ---------------------------------------------------------------------------------------------
// std:schedule
// ---------------------------------------------------------------------------------------------

// 30 days, the longest delay the host accepts (rt-host schedule.rs).
const SCHEDULE_DELAY_MAX_MS = 2592000000;

// The payload as the JSON text the ops take (`undefined` is `null`). A value JSON cannot hold is
// validation_failed.
function schedulePayload(payload) {
  let text;
  try {
    text = payload === undefined ? "null" : JSONStringify(payload);
  } catch (e) {
    throw coded(new Error("validation_failed"));
  }
  if (typeof text !== "string") {
    throw coded(new Error("validation_failed"));
  }
  return text;
}

// The `id` option: `{ has, id }` for the ops; an id that is not a string is validation_failed.
function scheduleId(opts) {
  if (opts === undefined || opts === null || opts.id === undefined) {
    return { has: false, id: "" };
  }
  if (typeof opts.id !== "string") {
    throw coded(new Error("validation_failed"));
  }
  return { has: true, id: opts.id };
}

// `schedule.after` / `schedule.at` / `schedule.cancel` / `schedule.list`, the host checks the rest
// (ids, payload size, the 30-day bound, the pending cap).
const scheduleApi = ObjectFreeze({
  __proto__: null,
  async after(seconds, payload, opts) {
    if (typeof seconds !== "number" || !NumberIsFinite(seconds) || seconds < 0 || seconds * 1000 > SCHEDULE_DELAY_MAX_MS) {
      throw coded(new Error("validation_failed"));
    }
    const id = scheduleId(opts);
    const text = schedulePayload(payload);
    return await guard(op_rt_schedule_after(id.has, id.id, seconds * 1000, text));
  },
  async at(when, payload, opts) {
    let at = NaN;
    if (typeof when === "number") {
      at = when;
    } else if (when !== null && typeof when === "object" && typeof when.getTime === "function") {
      at = when.getTime();
    }
    if (!NumberIsFinite(at)) {
      throw coded(new Error("validation_failed"));
    }
    const id = scheduleId(opts);
    const text = schedulePayload(payload);
    return await guard(op_rt_schedule_at(id.has, id.id, at, text));
  },
  async cancel(id) {
    if (typeof id !== "string") {
      throw coded(new Error("validation_failed"));
    }
    return await guard(op_rt_schedule_cancel(id));
  },
  async list(opts) {
    const o = opts || {};
    if (o.limit !== undefined && (typeof o.limit !== "number" || !NumberIsFinite(o.limit))) {
      throw coded(new Error("validation_failed"));
    }
    const page = await guard(
      op_rt_schedule_list({
        after: o.cursor === undefined || o.cursor === null ? null : String(o.cursor),
        limit: o.limit === undefined ? 100 : o.limit,
      }),
    );
    return { entries: page.entries, cursor: page.cursor };
  },
});

// ---------------------------------------------------------------------------------------------
// std:queue / std:services / std:platform
// ---------------------------------------------------------------------------------------------

async function enqueue(service, body, opts) {
  const o = opts || {};
  const payload = body instanceof Uint8Array ? body : textEncoder.encode(JSONStringify(body));
  const id = await guard(
    op_rt_enqueue(
      {
        slug: String(service),
        delayMs: typeof o.delaySeconds === "number" ? o.delaySeconds * 1000 : 0,
        dedupId: o.dedupId === undefined ? null : String(o.dedupId),
      },
      payload,
    ),
  );
  return { id };
}

// `services.fetch(slug, request)` (and the legacy `call`): the callee answers any response. The paths
// under `/__rt/` are the runtime's own (RPC below) and are not reachable this way.
async function serviceFetch(slug, request, init) {
  const p = await requestParts(request, init);
  const u = new urlMod.URL(p.url);
  if (StringPrototypeStartsWith(u.pathname, RPC_PREFIX_ROOT)) {
    throw coded(new Error("validation_failed"));
  }
  const res = await guard(
    op_rt_call(
      { __proto__: null, slug: String(slug), method: p.method, url: u.pathname + u.search, headers: p.headers, hasBody: p.hasBody },
      p.body,
    ),
  );
  return callResponse(res);
}

// The answer of a call: a callee that accepted a WebSocket answers 101 with its ticket, which a
// `Response` cannot hold; it becomes an accept response the caller can return as its own.
function callResponse(res) {
  if (res.status === 101) {
    let ticket = null;
    let protocol = "";
    for (let i = 0; i < res.headers.length; i++) {
      const name = StringPrototypeToLowerCase(res.headers[i][0]);
      if (name === TICKET_HEADER) {
        ticket = res.headers[i][1];
      } else if (name === "sec-websocket-protocol") {
        protocol = res.headers[i][1];
      }
    }
    if (ticket !== null) {
      if (res.bodyRid !== null) {
        op_rt_body_close(res.bodyRid);
      }
      return acceptResponse(ticket, protocol, null);
    }
  }
  return toResponse(res);
}

// ---------------------------------------------------------------------------------------------
// std:services RPC
// ---------------------------------------------------------------------------------------------

// A failure of an RPC call, typed: `code` is the callee's error code (a `ServiceError` it threw, or
// another error's `code`, else `internal_error`) or the host's (`method_not_found`, `call_depth`,
// `call_not_allowed`, `validation_failed`, `response_too_large`, `timeout`, ...). It carries a message
// only: a callee's stack never reaches the caller.
class ServiceError extends Error {
  constructor(code, message) {
    super(String(message));
    this.name = "ServiceError";
    this.code = String(code);
  }
}

// The reserved path of an RPC: `POST /__rt/rpc/<method>` with the JSON argument as the body. The
// callee answers 200 with `{"ok":true,"value":VALUE}` or `{"ok":false,"error":{"code","message"}}`; any
// other status is a host failure whose `x-rt-reason` is the code.
const RPC_PREFIX_ROOT = "/__rt/";
const RPC_PREFIX = "/__rt/rpc/";
// The argument and the result are at most 256 KiB of JSON text each way.
const RPC_JSON_MAX_BYTES = 262144;
// A method is a dot-separated path of identifiers (`createInvoice`, `invoices.create`), 256 bytes at most.
const RPC_METHOD_MAX_BYTES = 256;
const RPC_METHOD_RE = /^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)*$/;
const RPC_CODE_RE = /^[A-Za-z][A-Za-z0-9_.-]{0,63}$/;
const RPC_MESSAGE_MAX_CHARS = 1024;

function rpcMethodName(method) {
  if (typeof method !== "string" || method.length > RPC_METHOD_MAX_BYTES || !RPC_METHOD_RE.test(method)) {
    throw new ServiceError("validation_failed", "the method is a dot-separated path of identifiers");
  }
  return method;
}

// The argument as JSON text (`undefined` is `null`): validation_failed for a value JSON cannot hold,
// or past the cap.
function rpcArgText(arg) {
  let text;
  try {
    text = arg === undefined ? "null" : JSONStringify(arg);
  } catch (e) {
    throw new ServiceError("validation_failed", "the argument is not JSON");
  }
  if (typeof text !== "string" || textEncoder.encode(text).byteLength > RPC_JSON_MAX_BYTES) {
    throw new ServiceError("validation_failed", "the argument is not JSON or is larger than 256 KiB");
  }
  return text;
}

// `services.call(slug, method, arg)`: the callee's `rpc[method](arg, ctx)`, resolved.
async function rpcCall(slug, method, arg) {
  const name = rpcMethodName(method);
  const body = textEncoder.encode(rpcArgText(arg));
  let raw;
  try {
    raw = await guard(
      op_rt_call(
        { __proto__: null, slug: String(slug), method: "POST", url: RPC_PREFIX + name, headers: [["content-type", "application/json"]], hasBody: true, rpc: true },
        body,
      ),
    );
  } catch (e) {
    throw new ServiceError(e.code === undefined ? "call_failed" : e.code, e.message);
  }
  const res = toResponse(raw);
  if (res.status !== 200) {
    const reason = res.headers.get("x-rt-reason");
    await res.body?.cancel();
    throw new ServiceError(reason === null ? "call_failed" : reason, "the call answered " + res.status);
  }
  let env;
  try {
    env = JSONParse(await res.text());
  } catch (e) {
    throw new ServiceError("invalid_response", "the callee did not answer an RPC");
  }
  if (env !== null && typeof env === "object" && env.ok === true) {
    return env.value;
  }
  if (env !== null && typeof env === "object" && env.ok === false && env.error !== null && typeof env.error === "object") {
    throw new ServiceError(env.error.code, typeof env.error.message === "string" ? env.error.message : "");
  }
  throw new ServiceError("invalid_response", "the callee did not answer an RPC");
}

// `services.bind(slug)`: `await proxy.method(arg)` is `services.call(slug, "method", arg)`. `then` and
// `toJSON` are not methods, so awaiting or serializing the proxy sends nothing.
function rpcBind(slug) {
  const target = String(slug);
  return new Proxy(ObjectCreate(null), {
    get(_t, prop) {
      if (typeof prop !== "string" || prop === "then" || prop === "toJSON") {
        return undefined;
      }
      return (arg) => rpcCall(target, prop, arg);
    },
  });
}

// The method a request for `/__rt/rpc/<method>` names, or null when the path is not an RPC.
function rpcPathOf(p) {
  if (p.request === null || typeof p.request.url !== "string") {
    return null;
  }
  const path = new urlMod.URL(p.request.url).pathname;
  if (!path.startsWith(RPC_PREFIX)) {
    return null;
  }
  return path.slice(RPC_PREFIX.length);
}

// The function `method` names in the RPC table: own properties only, dotted paths into nested
// objects. `null` for anything else (inherited names such as `constructor` included).
function rpcFunctionOf(table, method) {
  let obj = table;
  for (const part of method.split(".")) {
    if (obj === null || (typeof obj !== "object" && typeof obj !== "function")) {
      return null;
    }
    const d = ObjectGetOwnPropertyDescriptor(obj, part);
    if (d === undefined || !("value" in d)) {
      return null;
    }
    obj = d.value;
  }
  return typeof obj === "function" ? obj : null;
}

// The RPC table: `export const rpc = { ... }`, or `default.rpc` of an object default export.
function rpcTableOf(mod, def) {
  if (def !== null && typeof def === "object" && def.rpc !== undefined) {
    return def.rpc;
  }
  return mod.rpc;
}

// The error of a failed RPC method, as the caller receives it: no stack, a bounded message.
function rpcErrorOf(e) {
  let code = "internal_error";
  let message = "";
  if (e instanceof ServiceError) {
    code = e.code;
    message = e.message;
  } else if (e !== null && typeof e === "object") {
    if (typeof e.code === "string" && RPC_CODE_RE.test(e.code)) {
      code = e.code;
    }
    if (typeof e.message === "string") {
      message = e.message;
    }
  }
  return { code, message: message.length > RPC_MESSAGE_MAX_CHARS ? message.slice(0, RPC_MESSAGE_MAX_CHARS) : message };
}

// The callee's side of an RPC: runs `rpc[method](arg, ctx)` and answers the envelope.
async function rpcHandle(method, mod, def, ctx, req) {
  let reply;
  try {
    const name = decodeURIComponent(method);
    const fn =
      name.length > RPC_METHOD_MAX_BYTES || !RPC_METHOD_RE.test(name) ? null : rpcFunctionOf(rpcTableOf(mod, def), name);
    if (fn === null) {
      throw new ServiceError("method_not_found", "no RPC method " + name);
    }
    const text = await req.text();
    const value = await fn(text === "" ? undefined : JSONParse(text), ctx);
    const out = value === undefined ? "null" : JSONStringify(value);
    if (typeof out !== "string" || textEncoder.encode(out).byteLength > RPC_JSON_MAX_BYTES) {
      throw new ServiceError("response_too_large", "the result is larger than 256 KiB");
    }
    reply = '{"ok":true,"value":' + out + "}";
  } catch (e) {
    reply = JSONStringify({ ok: false, error: rpcErrorOf(e) });
  }
  return await pumpResponse(new Response(reply, { status: 200, headers: { "content-type": "application/json" } }));
}

// ---------------------------------------------------------------------------------------------
// std:sdk -- operator SDKs as runtime services
// ---------------------------------------------------------------------------------------------

// A failed SDK call: the SDK's own code and message, or the runtime's (`sdk_not_found`,
// `sdk_unavailable` (removed), `sdk_not_granted`, `sdk_version_unavailable`, `method_not_found`,
// `validation_failed`, `call_depth`, `timeout`, ...). A `ServiceError` too.
class SdkError extends ServiceError {
  constructor(code, message) {
    super(code, message);
    this.name = "SdkError";
  }
}

// `sdk(name)`: a proxy whose property paths are methods. `await sdk("acme").invoices.create(x)`
// calls the method `invoices.create` of the release this caller's grants select. No code is
// loaded and nothing is sent until a method is called; `then` and `toJSON` are not methods.
function sdkInvoke(name, method, arg) {
  const m = rpcMethodName(method);
  const text = rpcArgText(arg);
  return PromisePrototypeThen(op_rt_sdk_invoke(name, m, text), (r) => {
    if (r.code !== null && r.code !== undefined) {
      throw new SdkError(r.code, r.message ?? "");
    }
    return JSONParse(r.value);
  });
}

function sdkPath(name, path) {
  // A function target, so the proxy is callable.
  const target = function sdkMethod() {};
  return new Proxy(target, {
    get(_t, prop) {
      if (typeof prop !== "string" || prop === "then" || prop === "toJSON") {
        return undefined;
      }
      return sdkPath(name, path === "" ? prop : path + "." + prop);
    },
    apply(_t, _this, args) {
      if (path === "") {
        return PromiseReject(new SdkError("validation_failed", "call a method of the SDK, not the SDK itself"));
      }
      try {
        return sdkInvoke(name, path, args[0]);
      } catch (e) {
        return PromiseReject(e);
      }
    },
  });
}

function sdk(name) {
  if (typeof name !== "string" || name === "") {
    throw new TypeError("sdk(name) expects the SDK name");
  }
  return sdkPath(name, "");
}

// `std:sdk/server` `defineSdk({ methods, authorize? })`: the default export of an SDK service.
// Every method runs only for a caller the runtime stamped (`ctx.caller`), and must check it: a
// method whose source never reads `caller` is refused at definition, unless `authorize(caller,
// method, args)` checks every call first (a falsy answer is `forbidden`).
function defineSdk(def) {
  if (def === null || typeof def !== "object") {
    throw new TypeError("defineSdk({ methods }) expects an object");
  }
  const methods = def.methods;
  const authorize = def.authorize;
  if (methods === null || typeof methods !== "object") {
    throw new TypeError("defineSdk: `methods` is an object of async functions");
  }
  if (authorize !== undefined && typeof authorize !== "function") {
    throw new TypeError("defineSdk: `authorize` is a function (caller, method, args)");
  }
  const rpc = sdkTable(methods, "", authorize, 0);
  return ObjectFreeze({
    __proto__: null,
    rpc,
    fetch() {
      return new Response("an SDK answers RPC only", { status: 404 });
    },
  });
}

const CALLER_RE = /\bcaller\b/;

function sdkTable(obj, prefix, authorize, depth) {
  if (depth > 8) {
    throw new TypeError("defineSdk: methods nest at most 8 levels");
  }
  const out = ObjectCreate(null);
  for (const key of ObjectKeys(obj)) {
    const v = obj[key];
    const name = prefix + key;
    if (typeof v === "function") {
      if (authorize === undefined && !RegExpPrototypeTest(CALLER_RE, FunctionPrototypeToString(v))) {
        const e = new TypeError(
          "defineSdk: the method " + name + " never reads ctx.caller; check the caller in it, or pass authorize(caller)",
        );
        e.code = "sdk_unchecked_caller";
        throw e;
      }
      out[key] = async (args, ctx) => {
        if (ctx === null || typeof ctx !== "object" || ctx.caller === undefined || ctx.caller.sdk === undefined) {
          throw new ServiceError("forbidden", "an SDK method runs only for a caller the runtime stamped");
        }
        if (authorize !== undefined && !(await authorize(ctx.caller, name, args))) {
          throw new ServiceError("forbidden", "the SDK refused this caller");
        }
        return await ReflectApply(v, obj, [args, ctx]);
      };
    } else if (v !== null && typeof v === "object") {
      out[key] = sdkTable(v, name + ".", authorize, depth + 1);
    }
  }
  return ObjectFreeze(out);
}

const platform = ObjectFreeze({
  __proto__: null,
  async fetch(binding, request, init) {
    const p = await requestParts(request, init);
    const u = new urlMod.URL(p.url);
    const res = await guard(
      op_rt_platform_fetch(
        { binding: String(binding), method: p.method, url: u.pathname + u.search, headers: p.headers, hasBody: p.hasBody },
        p.body,
      ),
    );
    return toResponse(res);
  },
});

// ---------------------------------------------------------------------------------------------
// std:sockets (docs/handlers.md "WebSockets")
// ---------------------------------------------------------------------------------------------

const EMPTY_BYTES = new Uint8Array(0);
// The response header that carries an accept ticket to ingress (the runtime removes it).
const TICKET_HEADER = "x-rt-ws-ticket";
// Responses made by `sockets.accept` (or a call that accepted): `pumpResponse` answers them with
// 101 and the ticket. `{ ticket, protocol }`.
const acceptMarks = new SafeWeakMap();

function wsCall(fn) {
  try {
    return fn();
  } catch (e) {
    throw coded(e);
  }
}

function offersUpgrade(req) {
  if (!(req instanceof Request)) {
    return false;
  }
  const up = req.headers.get("upgrade");
  return up !== null && StringPrototypeToLowerCase(StringPrototypeTrim(up)) === "websocket";
}

// [isText, text, bytes] of a message: a string, or bytes (Uint8Array, ArrayBuffer, any view).
function wsData(data) {
  if (typeof data === "string") {
    return [true, data, EMPTY_BYTES];
  }
  if (data instanceof Uint8Array) {
    return [false, "", data];
  }
  if (core.isArrayBuffer(data)) {
    return [false, "", new Uint8Array(data)];
  }
  if (core.isDataView(data)) {
    return [
      false,
      "",
      new Uint8Array(DataViewPrototypeGetBuffer(data), DataViewPrototypeGetByteOffset(data), DataViewPrototypeGetByteLength(data)),
    ];
  }
  if (ArrayBufferIsView(data)) {
    return [
      false,
      "",
      new Uint8Array(TypedArrayPrototypeGetBuffer(data), TypedArrayPrototypeGetByteOffset(data), TypedArrayPrototypeGetByteLength(data)),
    ];
  }
  throw coded(new Error("invalid_request"));
}

function tagsOf(tags) {
  if (tags === undefined || tags === null) {
    return [];
  }
  if (!ArrayIsArray(tags)) {
    throw coded(new Error("invalid_request"));
  }
  return ArrayPrototypeMap(tags, (t) => String(t));
}

function immediateOf(opts) {
  return opts !== undefined && opts !== null && opts.immediate === true;
}

function closeCode(code) {
  if (code === undefined || code === null) {
    return 1000;
  }
  if (!NumberIsSafeInteger(code) || code < 0 || code > 65535) {
    throw coded(new Error("invalid_request"));
  }
  return code;
}

// An attachment is stored as V8 structured-clone bytes (at most 16 KiB): it survives the isolate.
function serializeAttachment(value) {
  try {
    return core.serialize(value);
  } catch (_) {
    throw coded(new Error("invalid_request"));
  }
}

function readAttachment(id) {
  const bytes = wsCall(() => op_rt_ws_attachment(id));
  return bytes === null ? undefined : core.deserialize(bytes);
}

function setAttachment(id, value) {
  const has = value !== undefined;
  wsCall(() => op_rt_ws_set_attachment(String(id), has, has ? serializeAttachment(value) : EMPTY_BYTES));
}

// A socket as the guest holds it: its info (a snapshot) and methods that act on it by id.
function makeSocket(info) {
  let read = false;
  let attachment;
  const id = info.id;
  return ObjectFreeze({
    id,
    object: info.object,
    tags: ObjectFreeze(ArrayFrom(info.tags || [])),
    protocol: info.protocol === null || info.protocol === undefined ? undefined : info.protocol,
    createdAt: info.createdAt,
    lastMessageAt: info.lastMessageAt === null ? undefined : info.lastMessageAt,
    autoResponseAt: info.autoResponseAt === null ? undefined : info.autoResponseAt,
    bufferedAmount: info.bufferedAmount ?? 0,
    remoteIp: info.remoteIp === null ? undefined : info.remoteIp,
    get attachment() {
      if (!read) {
        attachment = readAttachment(id);
        read = true;
      }
      return attachment;
    },
    send(data, opts) {
      socketsApi.send(id, data, opts);
    },
    close(code, reason, opts) {
      socketsApi.close(id, code, reason, opts);
    },
    // Cloudflare names (`ws.serializeAttachment(v)` / `ws.deserializeAttachment()`).
    serializeAttachment(value) {
      setAttachment(id, value);
      read = false;
    },
    deserializeAttachment() {
      return this.attachment;
    },
  });
}

function acceptResponse(ticket, protocol, socket) {
  const res = new Response(null, { status: 200 });
  WeakMapPrototypeSet(acceptMarks, res, { ticket, protocol });
  if (socket !== null) {
    ObjectDefineProperty(res, "socket", { value: socket, enumerable: false });
  }
  return res;
}

const socketsApi = ObjectFreeze({
  __proto__: null,
  // Accept the upgrade `req` offered: the response (status 101 on the wire) binds the client's
  // connection to a new socket of this object. `res.socket` is the socket (sends before the
  // client is connected are queued).
  accept(req, opts) {
    if (!offersUpgrade(req)) {
      throw coded(new Error("not_upgradable"));
    }
    const o = opts || {};
    const tags = tagsOf(o.tags);
    const hasAttachment = o.attachment !== undefined;
    const attachment = hasAttachment ? serializeAttachment(o.attachment) : EMPTY_BYTES;
    const protocol = o.protocol === undefined || o.protocol === null ? "" : String(o.protocol);
    const r = wsCall(() => op_rt_ws_accept(tags, protocol, hasAttachment, attachment));
    const socket = makeSocket({
      id: r.id,
      object: r.object,
      tags,
      protocol: protocol === "" ? null : protocol,
      createdAt: op_rt_now(),
      lastMessageAt: null,
      autoResponseAt: null,
      bufferedAmount: 0,
      remoteIp: null,
    });
    return acceptResponse(r.ticket, protocol, socket);
  },
  send(id, data, opts) {
    const m = wsData(data);
    wsCall(() => op_rt_ws_send(String(id), m[0], m[1], m[2], immediateOf(opts)));
  },
  broadcast(data, opts) {
    const o = opts || {};
    const m = wsData(data);
    const except = o.except === undefined || o.except === null ? [] : ArrayIsArray(o.except) ? ArrayPrototypeMap(o.except, String) : [String(o.except)];
    const target = { object: o.object === undefined ? null : String(o.object), tag: o.tag === undefined ? null : String(o.tag), except };
    return { sent: wsCall(() => op_rt_ws_broadcast(target, m[0], m[1], m[2], immediateOf(o))) };
  },
  close(id, code, reason, opts) {
    wsCall(() => op_rt_ws_close(String(id), closeCode(code), reason === undefined ? "" : String(reason), immediateOf(opts)));
  },
  closeAll(opts) {
    const o = opts || {};
    const target = { object: o.object === undefined ? null : String(o.object), tag: o.tag === undefined ? null : String(o.tag) };
    const reason = o.reason === undefined ? "" : String(o.reason);
    return { closed: wsCall(() => op_rt_ws_close_all(target, closeCode(o.code), reason, immediateOf(o))) };
  },
  list(opts) {
    const o = opts || {};
    const page = wsCall(() =>
      op_rt_ws_list({
        object: o.object === undefined ? null : String(o.object),
        tag: o.tag === undefined ? null : String(o.tag),
        limit: o.limit === undefined ? null : o.limit,
        cursor: o.cursor === undefined ? null : String(o.cursor),
      }),
    );
    return { sockets: ArrayPrototypeMap(page.sockets, makeSocket), cursor: page.cursor === null ? undefined : page.cursor };
  },
  get(id) {
    const info = wsCall(() => op_rt_ws_get(String(id)));
    return info === null ? null : makeSocket(info);
  },
  count(opts) {
    const o = opts || {};
    return wsCall(() =>
      op_rt_ws_count({ object: o.object === undefined ? null : String(o.object), tag: o.tag === undefined ? null : String(o.tag) }),
    );
  },
  objects(opts) {
    const o = opts || {};
    const page = wsCall(() =>
      op_rt_ws_objects({
        prefix: o.prefix === undefined ? "" : String(o.prefix),
        limit: o.limit === undefined ? null : o.limit,
        cursor: o.cursor === undefined ? null : String(o.cursor),
      }),
    );
    return { objects: page.objects, cursor: page.cursor === null ? undefined : page.cursor };
  },
  setTags(id, tags) {
    wsCall(() => op_rt_ws_set_tags(String(id), tagsOf(tags)));
  },
  setAttachment(id, value) {
    setAttachment(id, value);
  },
  getAttachment(id) {
    return readAttachment(String(id));
  },
  // `{ request, response }`: a text message equal to `request` is answered with `response` by the
  // runtime, without an event (`null` removes it).
  setAutoResponse(pair, opts) {
    const o = opts || {};
    const object = o.object === undefined ? "" : String(o.object);
    if (pair === null || pair === undefined) {
      wsCall(() => op_rt_ws_auto_response(object, false, "", ""));
      return;
    }
    wsCall(() => op_rt_ws_auto_response(object, true, String(pair.request), String(pair.response)));
  },
});

// ---------------------------------------------------------------------------------------------
// std:durable
// ---------------------------------------------------------------------------------------------

// Stored values are V8 structured clones (format 1); bytes written by a WASM component (format 0)
// come back as a Uint8Array.
const DURABLE_V8 = 1;
// Mirrors rt_store::data::MAX_KV_VALUE_BYTES (1 MiB): checked before a multi-key put writes anything.
const DURABLE_VALUE_MAX_BYTES = 1024 * 1024;
const OBJECT_NAME_MAX_BYTES = 256;

function storageKey(k) {
  if (typeof k !== "string" || k.length === 0) {
    throw coded(new Error("validation_failed"));
  }
  return k;
}

function storedValue(v) {
  if (v === null || v === undefined) {
    return undefined;
  }
  return v.format === DURABLE_V8 ? core.deserialize(v.data) : v.data;
}

function storageValue(value) {
  try {
    return core.serialize(value);
  } catch (_) {
    throw coded(new Error("validation_failed"));
  }
}

// The task's object's storage (Cloudflare's `ctx.storage` shapes): writes commit when the handler
// returned, in one transaction, before the task's socket sends leave.
const storageApi = ObjectFreeze({
  __proto__: null,
  async get(keys) {
    if (ArrayIsArray(keys)) {
      const ks = ArrayPrototypeMap(keys, storageKey);
      const values = await guard(op_rt_storage_get(ks));
      const out = new Map();
      for (let i = 0; i < ks.length; i++) {
        if (values[i] !== null) {
          out.set(ks[i], storedValue(values[i]));
        }
      }
      return out;
    }
    const values = await guard(op_rt_storage_get([storageKey(keys)]));
    return storedValue(values[0]);
  },
  async put(key, value) {
    if (typeof key === "string") {
      const bytes = storageValue(value);
      wsCall(() => op_rt_storage_put(storageKey(key), DURABLE_V8, bytes));
      return;
    }
    if (key === null || typeof key !== "object") {
      throw coded(new Error("validation_failed"));
    }
    // Serialise and check every entry before the first write: an entry that does not clone (or is
    // too large) must leave the object untouched, not hold the entries before it.
    const entries = [];
    for (const k of ObjectKeys(key)) {
      const bytes = storageValue(key[k]);
      if (bytes.byteLength > DURABLE_VALUE_MAX_BYTES) {
        throw coded(new Error("kv_value_too_large"));
      }
      entries.push([storageKey(k), bytes]);
    }
    for (const [k, bytes] of entries) {
      wsCall(() => op_rt_storage_put(k, DURABLE_V8, bytes));
    }
  },
  async delete(keys) {
    if (ArrayIsArray(keys)) {
      return await guard(op_rt_storage_delete(ArrayPrototypeMap(keys, storageKey)));
    }
    return (await guard(op_rt_storage_delete([storageKey(keys)]))) === 1;
  },
  async list(opts) {
    const o = opts || {};
    const entries = await guard(
      op_rt_storage_list({
        prefix: o.prefix === undefined ? "" : String(o.prefix),
        start: o.start === undefined ? null : String(o.start),
        end: o.end === undefined ? null : String(o.end),
        limit: o.limit === undefined ? null : o.limit,
        reverse: o.reverse === true,
      }),
    );
    const out = new Map();
    for (let i = 0; i < entries.length; i++) {
      out.set(entries[i].key, storedValue(entries[i]));
    }
    return out;
  },
  async deleteAll() {
    wsCall(() => op_rt_storage_delete_all());
  },
  async getAlarm() {
    const at = await guard(op_rt_alarm_get());
    return at < 0 ? null : at;
  },
  async setAlarm(time) {
    const at = typeof time === "number" ? time : time instanceof RealDate || time instanceof FrozenDate ? time.getTime() : NaN;
    if (!NumberIsFinite(at)) {
      throw coded(new Error("validation_failed"));
    }
    wsCall(() => op_rt_alarm_set(true, at));
  },
  async deleteAlarm() {
    wsCall(() => op_rt_alarm_set(false, 0));
  },
});

function objectName(name) {
  const n = String(name);
  if (n.length === 0 || textEncoder.encode(n).length > OBJECT_NAME_MAX_BYTES) {
    throw coded(new Error("validation_failed"));
  }
  return n;
}

// `stub.fetch(input, init)`: a request to the object, in its turn (an in-process call).
async function objectFetch(service, name, input, init) {
  const p = await requestParts(input, init);
  const u = new urlMod.URL(p.url);
  if (StringPrototypeStartsWith(u.pathname, RPC_PREFIX_ROOT)) {
    throw coded(new Error("validation_failed"));
  }
  const res = await guard(
    op_rt_call_object(
      { __proto__: null, slug: service, object: name, method: p.method, url: u.pathname + u.search, headers: p.headers, hasBody: p.hasBody },
      p.body,
    ),
  );
  return callResponse(res);
}

const durableApi = ObjectFreeze({
  __proto__: null,
  // The object `name` of this service, or of the durable service `{ service }` (a slug).
  get(name, opts) {
    const n = objectName(name);
    const service = opts && opts.service !== undefined && opts.service !== null ? String(opts.service) : "";
    return ObjectFreeze({
      name: n,
      id: n,
      fetch(input, init) {
        return objectFetch(service, n, input, init);
      },
    });
  },
  // Cloudflare's names: an object is addressed by its name.
  idFromName(name) {
    return objectName(name);
  },
  newUniqueId() {
    const b = new Uint8Array(16);
    cryptoMod.crypto.getRandomValues(b);
    let hex = "";
    for (let i = 0; i < b.length; i++) {
      hex += (b[i] < 16 ? "0" : "") + b[i].toString(16);
    }
    return hex;
  },
});

// A durable class (`export default class Room { constructor(state, env) }`): one instance per
// object. A warm isolate keeps the instance while it ran the object's previous turn (no other
// isolate ran the object since); otherwise the constructor runs again.
const objectInstances = new Map();
const OBJECT_INSTANCES_MAX = 256;

function isClass(def) {
  return typeof def === "function" && RegExpPrototypeTest(/^class[\s{]/, FunctionPrototypeToString(def));
}

function objectState(name) {
  return ObjectFreeze({
    id: ObjectFreeze({ name, toString: () => name }),
    name,
    storage: storageApi,
    // Events and requests of an object already run one at a time.
    blockConcurrencyWhile: (fn) => PromiseResolve(fn()),
  });
}

function objectInstance(p, def) {
  const o = p.ctx.object;
  const name = o === null || o === undefined ? "default" : o.name;
  const cached = objectInstances.get(name);
  if (cached !== undefined && cached.ctor === def && o && o.prev !== 0 && cached.seq === o.prev) {
    cached.seq = o.seq;
    return cached.instance;
  }
  const instance = new def(objectState(name), env);
  if (o) {
    objectInstances.delete(name);
    if (objectInstances.size >= OBJECT_INSTANCES_MAX) {
      objectInstances.delete(objectInstances.keys().next().value);
    }
    objectInstances.set(name, { ctor: def, instance, seq: o.seq });
  }
  return instance;
}

function callHandler(target, name, args) {
  if (target !== null && target !== undefined && typeof target[name] === "function") {
    return ReflectApply(target[name], target, args);
  }
  return undefined;
}

function bytesToArrayBuffer(u8) {
  const buf = TypedArrayPrototypeGetBuffer(u8);
  if (TypedArrayPrototypeGetByteOffset(u8) === 0 && ArrayBufferPrototypeGetByteLength(buf) === TypedArrayPrototypeGetByteLength(u8)) {
    return buf;
  }
  const copy = new Uint8Array(TypedArrayPrototypeGetByteLength(u8));
  TypedArrayPrototypeSet(copy, u8);
  return TypedArrayPrototypeGetBuffer(copy);
}

// One socket event or alarm (`ctx.trigger` "websocket" / "alarm"): `webSocketMessage(ws, message)`,
// `webSocketClose(ws, code, reason, wasClean)`, `webSocketError(ws, error)`, `alarm(info)`. A missing
// handler is a no-op. The task's sends wait until the handler returned (the commit).
async function objectEvent(p, target, ctx) {
  const ev = p.socket;
  const ws = ev.ws === null ? null : makeSocket(ev.ws);
  if (ev.kind === "message") {
    const data = ev.binary === true ? bytesToArrayBuffer(op_rt_ws_event_data()) : ev.text;
    await callHandler(target, "webSocketMessage", [ws, data, ctx]);
  } else if (ev.kind === "close") {
    await callHandler(target, "webSocketClose", [ws, ev.code, ev.reason, ev.wasClean, ctx]);
  } else if (ev.kind === "error") {
    const err = new Error(ev.error.message);
    err.code = ev.error.code;
    await callHandler(target, "webSocketError", [ws, err, ctx]);
  } else if (ev.kind === "alarm") {
    await callHandler(target, "alarm", [ObjectFreeze({ scheduledTime: ev.scheduledTime, retryCount: ev.retryCount }), ctx]);
  }
  await guard(op_rt_task_commit());
  return plainResponse(200);
}

// ---------------------------------------------------------------------------------------------
// Outbound WebSockets: `new WebSocket(url, protocols)` (WHATWG shape) through the egress guard
// ---------------------------------------------------------------------------------------------

// The connection belongs to the task: it is closed when the task ends. It counts as one fetch and
// its received bytes count against the egress quota. An error event carries `.code` (egress_denied,
// egress_timeout, egress_upstream, egress_limit, ...).
class RtWebSocket extends eventMod.EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  #url;
  #rid = null;
  #state = 0;
  #protocol = "";
  #binaryType = "blob";
  #buffered = 0;

  constructor(url, protocols) {
    super();
    let u;
    try {
      u = new urlMod.URL(String(url));
    } catch (_) {
      throw new domException.DOMException("invalid URL", "SyntaxError");
    }
    if (u.protocol === "http:") {
      u.protocol = "ws:";
    } else if (u.protocol === "https:") {
      u.protocol = "wss:";
    }
    if ((u.protocol !== "ws:" && u.protocol !== "wss:") || u.hash !== "") {
      throw new domException.DOMException("a WebSocket URL is ws: or wss:", "SyntaxError");
    }
    this.#url = u.href;
    const list = protocols === undefined ? [] : typeof protocols === "string" ? [protocols] : ArrayPrototypeMap(ArrayFrom(protocols), String);
    this.#connect(list);
  }

  get url() {
    return this.#url;
  }
  get readyState() {
    return this.#state;
  }
  get protocol() {
    return this.#protocol;
  }
  get extensions() {
    return "";
  }
  get bufferedAmount() {
    return this.#buffered;
  }
  get binaryType() {
    return this.#binaryType;
  }
  set binaryType(v) {
    if (v === "blob" || v === "arraybuffer") {
      this.#binaryType = v;
    }
  }

  #fail(code) {
    this.#state = 3;
    const ev = new eventMod.Event("error");
    ev.code = code;
    this.dispatchEvent(ev);
  }

  async #connect(protocols) {
    let r;
    try {
      r = await op_rt_ws_connect(this.#url, protocols);
    } catch (e) {
      this.#fail(coded(e).code);
      this.dispatchEvent(new eventMod.CloseEvent("close", { code: 1006, reason: "", wasClean: false }));
      return;
    }
    if (this.#state === 3) {
      return;
    }
    this.#rid = r.rid;
    this.#protocol = r.protocol;
    this.#state = 1;
    this.dispatchEvent(new eventMod.Event("open"));
    for (;;) {
      let m;
      try {
        m = await op_rt_ws_client_recv(this.#rid);
      } catch (e) {
        this.#fail(coded(e).code);
        this.dispatchEvent(new eventMod.CloseEvent("close", { code: 1006, reason: "", wasClean: false }));
        return;
      }
      if (m.kind === "close") {
        this.#state = 3;
        this.dispatchEvent(new eventMod.CloseEvent("close", { code: m.code, reason: m.reason, wasClean: m.code !== 1006 }));
        return;
      }
      let data = m.text;
      if (m.kind === "binary") {
        data = this.#binaryType === "arraybuffer" ? bytesToArrayBuffer(m.data) : new fileMod.Blob([m.data]);
      }
      this.dispatchEvent(new eventMod.MessageEvent("message", { data, origin: this.#url }));
    }
  }

  send(data) {
    if (this.#state === 0) {
      throw new domException.DOMException("the WebSocket is not open yet", "InvalidStateError");
    }
    if (this.#state !== 1) {
      return;
    }
    const send = (m) => {
      const n = m[0] ? m[1].length : TypedArrayPrototypeGetByteLength(m[2]);
      this.#buffered += n;
      PromisePrototypeThen(
        op_rt_ws_client_send(this.#rid, m[0], m[1], m[2]),
        () => {
          this.#buffered -= n;
        },
        (e) => {
          this.#buffered -= n;
          this.#fail(coded(e).code);
        },
      );
    };
    if (data instanceof fileMod.Blob) {
      PromisePrototypeThen(data.arrayBuffer(), (b) => send(wsData(b)));
      return;
    }
    send(wsData(data));
  }

  close(code, reason) {
    if (code !== undefined && code !== 1000 && !(code >= 3000 && code <= 4999)) {
      throw new domException.DOMException("a close code is 1000 or 3000-4999", "InvalidAccessError");
    }
    if (this.#state >= 2) {
      return;
    }
    if (this.#state === 0) {
      this.#state = 3;
      return;
    }
    this.#state = 2;
    PromisePrototypeThen(op_rt_ws_client_close(this.#rid, code === undefined ? 1000 : code, reason === undefined ? "" : String(reason)), undefined, () => {});
  }
}
for (const name of ["open", "message", "error", "close"]) {
  eventMod.defineEventHandler(RtWebSocket.prototype, name);
}
defineGlobal("WebSocket", RtWebSocket);

// ---------------------------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------------------------

function toErrorText(e) {
  if (e instanceof Error) {
    return e.stack ? String(e.stack) : String(e);
  }
  try {
    return String(e);
  } catch (_) {
    return "unprintable error";
  }
}

function makeCtx(p, pending) {
  return {
    taskId: p.ctx.taskId,
    service: p.ctx.service,
    trigger: p.ctx.trigger,
    invoker: p.ctx.invoker === null ? undefined : p.ctx.invoker,
    // HTTP trigger only: the client address resolved behind `ingress.trusted_proxies`.
    client: p.ctx.client === null || p.ctx.client === undefined ? undefined : ObjectFreeze({ ip: p.ctx.client.ip }),
    // A service-to-service call only: the caller as the runtime stamped it (slug and project null
    // when the caller is not visible from this tenant).
    caller:
      p.ctx.caller === null || p.ctx.caller === undefined
        ? undefined
        : ObjectFreeze({
            service: ObjectFreeze({ id: p.ctx.caller.service.id, slug: p.ctx.caller.service.slug }),
            tenant: p.ctx.caller.tenant,
            project: p.ctx.caller.project,
            // An operator SDK call only: the calling tenant's plan and the SDK release it is bound to.
            plan: p.ctx.caller.plan ?? undefined,
            sdk:
              p.ctx.caller.sdk === null || p.ctx.caller.sdk === undefined
                ? undefined
                : ObjectFreeze({ name: p.ctx.caller.sdk.name, version: p.ctx.caller.sdk.version }),
            environment: p.ctx.caller.environment,
          }),
    deadline: p.ctx.deadline,
    // The object this task acts for (a socket event, an alarm, a request to a durable object).
    object: p.ctx.object === null || p.ctx.object === undefined ? undefined : ObjectFreeze({ name: p.ctx.object.name }),
    storage: p.ctx.object === null || p.ctx.object === undefined ? undefined : storageApi,
    waitUntil(promise) {
      ArrayPrototypePush(pending, PromiseResolve(promise));
    },
  };
}

// The request body is a stream over a host resource (B4.4): the handler reads chunks while the
// client is still sending; reading past `limits.max_body_bytes` fails with `request_too_large`.
function buildRequest(p) {
  const r = p.request;
  const init = { __proto__: null, method: r.method, headers: r.headers };
  if (r.hasBody && r.bodyRid !== null && r.method !== "GET" && r.method !== "HEAD") {
    init.body = bodyStream(r.bodyRid);
  }
  return new Request(r.url, init);
}

function syntheticRequest(p, path, body) {
  return new Request("http://localhost" + path, {
    method: "POST",
    headers: { "content-type": "application/json", "x-rt-trigger": p.ctx.trigger },
    body: JSONStringify(body),
  });
}

function pickFetch(def) {
  if (typeof def === "function") {
    return (req, ctx) => def(req, ctx);
  }
  if (def !== null && typeof def === "object" && typeof def.fetch === "function") {
    return (req, ctx) => def.fetch(req, ctx);
  }
  return null;
}

async function pumpResponse(res) {
  const mark = WeakMapPrototypeGet(acceptMarks, res);
  if (mark !== undefined) {
    const h = [[TICKET_HEADER, mark.ticket]];
    if (mark.protocol !== "") {
      ArrayPrototypePush(h, ["sec-websocket-protocol", mark.protocol]);
    }
    op_rt_respond(101, h);
    op_rt_resp_end();
    return true;
  }
  const headers = [];
  for (const pair of res.headers) {
    ArrayPrototypePush(headers, [pair[0], pair[1]]);
  }
  const cookies = res.headers.getSetCookie ? res.headers.getSetCookie() : [];
  if (cookies.length > 1) {
    for (let i = 0; i < headers.length; i++) {
      if (headers[i][0] === "set-cookie") {
        ArrayPrototypeSplice(headers, i, 1);
        i--;
      }
    }
    for (let i = 0; i < cookies.length; i++) {
      ArrayPrototypePush(headers, ["set-cookie", cookies[i]]);
    }
  }
  op_rt_respond(res.status, headers);
  if (res.body === null) {
    op_rt_resp_end();
    return true;
  }
  const reader = res.body.getReader();
  try {
    for (;;) {
      const r = await reader.read();
      if (r.done) {
        break;
      }
      if (!(r.value instanceof Uint8Array)) {
        throw new TypeError("response body chunks must be Uint8Array");
      }
      await op_rt_resp_chunk(r.value);
    }
    op_rt_resp_end();
    return true;
  } catch (e) {
    op_rt_resp_abort(toErrorText(e));
    try {
      await reader.cancel();
    } catch (_) {
      // ignore
    }
    return false;
  }
}

function plainResponse(status, headers) {
  const h = [];
  if (headers) {
    for (const k of ObjectKeys(headers)) {
      ArrayPrototypePush(h, [k, headers[k]]);
    }
  }
  op_rt_respond(status, h);
  op_rt_resp_end();
  return true;
}

async function handle(p, pending) {
  const mod = await import(p.entry);
  let def = mod.default;
  const ctx = makeCtx(p, pending);
  const trigger = p.ctx.trigger;
  if (isClass(def)) {
    // A durable class: its instance for this task's object takes the place of a Form B object.
    def = objectInstance(p, def);
  }
  const isB = def !== null && typeof def === "object";
  const fetchFn = pickFetch(def);

  if (trigger === "call") {
    const method = rpcPathOf(p);
    if (method !== null) {
      return await rpcHandle(method, mod, def, ctx, buildRequest(p));
    }
  }

  if (trigger === "websocket" || trigger === "alarm") {
    return await objectEvent(p, def, ctx);
  }

  if (trigger === "interval" || trigger === "cron" || trigger === "schedule") {
    if (isB && typeof def.scheduled === "function") {
      await def.scheduled(p.schedule, ctx);
      return plainResponse(200);
    }
    if (fetchFn === null) {
      throw new TypeError("handler has neither scheduled() nor fetch()");
    }
    const res = await fetchFn(syntheticRequest(p, "/__rt/scheduled", p.schedule), ctx);
    return await pumpResponse(checkResponse(res));
  }

  if (trigger === "queue") {
    if (isB && typeof def.queue === "function") {
      const retried = [];
      const messages = ArrayPrototypeMap(p.queue, (m) => {
        let acked = false;
        return {
          id: m.id,
          body: m.body,
          attempt: m.attempt,
          ack() {
            acked = true;
          },
          retry(o) {
            const d = o && typeof o.delaySeconds === "number" ? o.delaySeconds : 0;
            if (!acked) {
              ArrayPrototypePush(retried, d > 0 ? m.id + ";delay=" + d : m.id);
            }
          },
        };
      });
      await def.queue({ messages }, ctx);
      const headers = { __proto__: null };
      if (retried.length > 0) {
        headers["x-rt-queue-retry"] = ArrayPrototypeJoin(retried, ",");
      }
      return plainResponse(200, headers);
    }
    if (fetchFn === null) {
      throw new TypeError("handler has neither queue() nor fetch()");
    }
    const res = await fetchFn(syntheticRequest(p, "/__rt/queue", { messages: p.queue }), ctx);
    return await pumpResponse(checkResponse(res));
  }

  if (fetchFn === null) {
    throw new TypeError("module has no default handler");
  }
  const res = checkResponse(await fetchFn(buildRequest(p), ctx));
  if (p.ctx.object !== null && p.ctx.object !== undefined) {
    // A request to an object: its storage writes commit and its sends leave before the response.
    await guard(op_rt_task_commit());
  }
  return await pumpResponse(res);
}

function checkResponse(res) {
  if (!(res instanceof Response)) {
    throw new TypeError("handler must return a Response");
  }
  return res;
}

// Resolves to `true` when the task finished cleanly: the handler and its response body did not
// fail and no `waitUntil` promise rejected. Only such an isolate may be reused (D-A4).
// Set while the executor's task runs: a guest calling `__rt.start` itself (it can read the frozen
// `__rt`) must not dispatch a second task with a payload it made up (SBX-003).
let startRunning = false;

async function start(p) {
  if (startRunning) {
    throw new TypeError("__rt.start belongs to the runtime");
  }
  startRunning = true;
  try {
    return await runStart(p);
  } finally {
    startRunning = false;
  }
}

async function runStart(p) {
  if (p.warmup === true) {
    // Building a kept isolate for the warm floor (D-B2) off the request path: evaluate the entry
    // module, as the first task would; no handler runs and nothing is sent.
    try {
      await import(p.entry);
    } catch (e) {
      op_rt_fail(toErrorText(e));
      return false;
    }
    return wasmPending === 0;
  }
  const pending = [];
  let clean = true;
  try {
    clean = (await handle(p, pending)) === true;
  } catch (e) {
    clean = false;
    op_rt_fail(toErrorText(e));
  }
  // waitUntil: keep the isolate alive for the promises (bounded by the host's wall timer).
  while (pending.length > 0) {
    const batch = ArrayPrototypeSplice(pending, 0, pending.length);
    const results = await PromiseAllSettled(batch);
    for (let i = 0; i < results.length; i++) {
      if (results[i].status === "rejected") {
        clean = false;
        op_rt_log(3, "waitUntil promise rejected: " + toErrorText(results[i].reason));
      }
    }
  }
  // A wasm compile or instantiation the guest did not await would finish inside a later task.
  return clean && wasmPending === 0;
}

// ---------------------------------------------------------------------------------------------
// std:log
// ---------------------------------------------------------------------------------------------

// The levels as `op_rt_log_fields` takes them (LEVEL_* in rt-exec-deno `ops.rs`).
const LOG_DEBUG = 0;
const LOG_INFO = 1;
const LOG_WARN = 2;
const LOG_ERROR = 3;

// A call's `fields` must be undefined or a plain object (not an array, not null).
function checkLogFields(fields) {
  if (fields === undefined) {
    return;
  }
  if (fields === null || typeof fields !== "object" || ArrayIsArray(fields)) {
    throw coded(new Error("validation_failed"));
  }
}

// A fresh object: the bound fields of a child logger, then the call's (the call's keys win).
function mergeLogFields(bound, fields) {
  const out = {};
  for (const src of [bound, fields]) {
    if (src !== undefined) {
      for (const k of ObjectKeys(src)) {
        out[k] = src[k];
      }
    }
  }
  return out;
}

function emitLog(level, bound, message, fields) {
  checkLogFields(fields);
  const merged = mergeLogFields(bound, fields);
  let text = "";
  if (bound !== undefined || fields !== undefined) {
    try {
      text = JSONStringify(merged);
    } catch (e) {
      // BigInt, cycles, and the like: not JSON.
      throw coded(new Error("validation_failed"));
    }
  }
  try {
    op_rt_log_fields(level, String(message), text);
  } catch (e) {
    throw coded(e);
  }
}

function makeLogger(bound) {
  return ObjectFreeze({
    __proto__: null,
    debug(message, fields) {
      emitLog(LOG_DEBUG, bound, message, fields);
    },
    info(message, fields) {
      emitLog(LOG_INFO, bound, message, fields);
    },
    warn(message, fields) {
      emitLog(LOG_WARN, bound, message, fields);
    },
    error(message, fields) {
      emitLog(LOG_ERROR, bound, message, fields);
    },
    // A child logger: `fields` bound to every call of it, merged with the parent's.
    with(fields) {
      checkLogFields(fields);
      return makeLogger(mergeLogFields(bound, fields));
    },
  });
}

const stdLog = makeLogger(undefined);

// ---------------------------------------------------------------------------------------------
// std:secrets
// ---------------------------------------------------------------------------------------------

// A secret name as `21ci secret set` takes it: [A-Z_][A-Z0-9_]*, at most 255 bytes, not RT_*.
const SECRET_NAME = /^(?!RT_)[A-Z_][A-Z0-9_]*$/;

function secretGet(name) {
  if (typeof name !== "string" || name.length > 255 || !SECRET_NAME.test(name)) {
    throw coded(new Error("validation_failed"));
  }
  try {
    return op_rt_secret_get(name);
  } catch (e) {
    throw coded(e);
  }
}

// `secrets.require(...names)`: `{ NAME: value }` for every name, or one `secret_missing` error that
// names every missing secret and the command to set each. Values never appear in the error.
function secretRequire(...names) {
  if (names.length === 0) {
    throw coded(new Error("validation_failed"));
  }
  const out = {};
  const missing = [];
  for (const name of names) {
    const value = secretGet(name);
    if (value === null) {
      missing.push(name);
    } else {
      out[name] = value;
    }
  }
  if (missing.length > 0) {
    const commands = ArrayPrototypeMap(missing, (n) => `21ci secret set <service> ${n}`);
    const err = new Error(
      `missing secrets: ${ArrayPrototypeJoin(missing, ", ")}; set them with ${ArrayPrototypeJoin(commands, "; ")}`,
    );
    err.code = "secret_missing";
    err.missing = missing;
    throw err;
  }
  return out;
}

// No prototype and no enumerable keys: `Object.keys` is empty, and `JSON.stringify(secrets)` is
// undefined, so the accessor cannot be dumped by accident.
const secretsApi = ObjectCreate(null);
ObjectDefineProperty(secretsApi, "get", { value: secretGet, enumerable: false });
ObjectDefineProperty(secretsApi, "require", { value: secretRequire, enumerable: false });
ObjectDefineProperty(secretsApi, "toJSON", { value: () => undefined, enumerable: false });
ObjectFreeze(secretsApi);

// The executor dispatches every task through `globalThis.__rt.start`, and the std modules read
// their APIs from it: the guest can neither replace nor delete it (a warm isolate runs the next
// task through it).
ObjectDefineProperty(globalThis, "__rt", {
  writable: false,
  configurable: false,
  enumerable: false,
  value: ObjectFreeze({
    __proto__: null,
    start,
    kv,
    log: stdLog,
    secrets: secretsApi,
    lock: lockApi,
    schedule: scheduleApi,
    sql,
    blob,
    queue: ObjectFreeze({ __proto__: null, enqueue }),
    services: ObjectFreeze({ __proto__: null, fetch: serviceFetch, call: rpcCall, bind: rpcBind }),
    serviceError: ServiceError,
    sdk: ObjectFreeze({ __proto__: null, sdk, SdkError, defineSdk }),
    platform,
    sqlite,
    sockets: socketsApi,
    durable: durableApi,
    storage: storageApi,
  }),
});

// ---------------------------------------------------------------------------------------------
// WebAssembly memory accounting (B5.1). V8 counts `WebAssembly.Memory` nowhere the host can see
// (not through the ArrayBuffer allocator, not in the heap statistics), so every memory the guest
// can reach is registered with `op_rt_wasm_track`, and the host samples their byteLength against
// `limits.memory_mb` while the task runs (rt-exec-deno `memory.rs`).
//  * `new WebAssembly.Memory()` registers the new memory;
//  * every instance registers the memories it exports (imported ones were registered where they
//    were created);
//  * a module that defines a memory without exporting it (D-A2 follow-up) is compiled again with
//    an extra export of that memory under a reserved name (`__rt_memory_<index>`), so its
//    instances register it like any other and its actual size is sampled; the added exports are
//    hidden from `instance.exports` and `WebAssembly.Module.exports`;
//  * only modules compiled through these wrappers can be instantiated.
// The bytes are copied (without the guest's `Symbol.species`) before anything is checked, so the
// guest cannot change them between the check and the compilation.
// The wrappers are Proxies of the original constructors and functions, so `instanceof`, statics
// and prototypes behave as before; the prototypes' `constructor` points at the wrappers.
// ---------------------------------------------------------------------------------------------

const wasmWrappers = new SafeWeakSet();

// Async wasm compilations / instantiations started and not yet settled (D-A4: an isolate with
// one pending is not reused).
let wasmPending = 0;

function trackPending(promise) {
  wasmPending++;
  const settled = () => {
    wasmPending--;
  };
  PromisePrototypeThen(promise, settled, settled);
  return promise;
}

// Unsigned LEB128 at `bytes[cur.p]`; advances `cur.p`.
function wasmLeb(bytes, cur) {
  let value = 0;
  let scale = 1;
  let byte;
  do {
    byte = bytes[cur.p++];
    value += (byte & 0x7f) * scale;
    scale *= 128;
  } while (byte & 0x80);
  return value;
}

// Append the unsigned LEB128 bytes of `value` to the array `out`.
function wasmPushLeb(out, value) {
  do {
    let byte = value % 128;
    value = (value - byte) / 128;
    if (value !== 0) {
      byte |= 0x80;
    }
    ArrayPrototypePush(out, byte);
  } while (value !== 0);
}

function wrapWebAssembly() {
  const WA = globalThis.WebAssembly;
  if (WA === undefined || WeakSetPrototypeHas(wasmWrappers, WA.Module)) {
    return;
  }
  const OrigMemory = WA.Memory;
  const OrigModule = WA.Module;
  const OrigInstance = WA.Instance;
  const origCompile = WA.compile;
  const origInstantiate = WA.instantiate;
  const moduleImports = OrigModule.imports;
  const moduleExports = OrigModule.exports;
  const exportsGetter = ObjectGetOwnPropertyDescriptor(OrigInstance.prototype, "exports").get;
  const CompileError = WA.CompileError;
  const memoryFields = ["initial", "minimum", "maximum", "shared", "index", "address"];
  const LinkError = WA.LinkError;
  const tracked = new SafeWeakSet(); // memories registered with the host
  const checked = new SafeWeakSet(); // modules compiled (and, if needed, rewritten) by the wrappers
  const added = new SafeWeakMap(); // module -> SafeSet of the export names added for hidden memories

  function track(memory) {
    if (!WeakSetPrototypeHas(tracked, memory)) {
      WeakSetPrototypeAdd(tracked, memory);
      op_rt_wasm_track(memory);
    }
  }

  function isModule(value) {
    try {
      moduleExports(value); // brand check: throws for anything but a WebAssembly.Module
      return true;
    } catch (_) {
      return false;
    }
  }

  // A private copy of a BufferSource (the guest cannot change it between compile and check: no
  // `slice`, whose species lookup would let the guest construct, and keep, the copy);
  // `undefined` lets the original API throw its own TypeError.
  function copyBytes(source) {
    let view;
    if (ArrayBufferIsView(source)) {
      view = core.isDataView(source)
        ? new Uint8Array(
          DataViewPrototypeGetBuffer(source),
          DataViewPrototypeGetByteOffset(source),
          DataViewPrototypeGetByteLength(source),
        )
        : new Uint8Array(
          TypedArrayPrototypeGetBuffer(source),
          TypedArrayPrototypeGetByteOffset(source),
          TypedArrayPrototypeGetByteLength(source),
        );
    } else if (core.isAnyArrayBuffer(source)) {
      view = new Uint8Array(source);
    } else {
      return undefined;
    }
    const copy = new Uint8Array(TypedArrayPrototypeGetLength(view));
    TypedArrayPrototypeSet(copy, view, 0);
    return copy;
  }

  // The (already validated) module's defined memories that it does not export, with what
  // `exposeHidden` needs: section 5 (memory) holds the defined memories (limits flag 0x02 =
  // shared), section 7 (export) the entries (kind 2 = memory); imported memories come first in
  // the index space. The length comes from a primordial: the guest can redefine the `length`
  // getter of typed arrays, and a short read would admit the module unrewritten.
  function scanMemories(module, bytes) {
    const length = TypedArrayPrototypeGetLength(bytes);
    let imported = 0;
    const imports = moduleImports(module);
    for (let i = 0; i < imports.length; i++) {
      if (imports[i].kind === "memory") {
        imported++;
      }
    }
    const cur = { p: 8 };
    let defined = 0;
    let exportSection = null; // { start: offset of the id byte, end }
    let insertAt = length; // where a new export section goes (before start/elem/datacount/code/data)
    let shared = false;
    const exportedMemories = new SafeSet();
    const names = new SafeSet(); // existing export names (byte-wise, to avoid a collision)
    while (cur.p < length) {
      const start = cur.p;
      const id = bytes[cur.p++];
      const size = wasmLeb(bytes, cur);
      const end = cur.p + size;
      if (insertAt === length && (id === 8 || id === 9 || id === 12 || id === 10 || id === 11)) {
        insertAt = start;
      }
      if (id === 5) {
        defined = wasmLeb(bytes, cur);
        for (let i = 0; i < defined; i++) {
          const flags = bytes[cur.p++];
          if (flags & 0x02) {
            shared = true;
          }
          wasmLeb(bytes, cur); // minimum
          if (flags & 0x01) {
            wasmLeb(bytes, cur); // maximum
          }
          if (flags & 0x08) {
            wasmLeb(bytes, cur); // page size (custom-page-sizes)
          }
        }
      } else if (id === 7) {
        exportSection = { start, end };
        const count = wasmLeb(bytes, cur);
        for (let i = 0; i < count; i++) {
          const nameLength = wasmLeb(bytes, cur);
          let name = "";
          for (let j = 0; j < nameLength; j++) {
            name += StringFromCharCode(bytes[cur.p + j]);
          }
          cur.p += nameLength;
          SetPrototypeAdd(names, name);
          const kind = bytes[cur.p++];
          const index = wasmLeb(bytes, cur);
          if (kind === 2) {
            SetPrototypeAdd(exportedMemories, index);
          }
        }
      }
      cur.p = end;
    }
    const hidden = [];
    for (let i = imported; i < imported + defined; i++) {
      if (!SetPrototypeHas(exportedMemories, i)) {
        ArrayPrototypePush(hidden, i);
      }
    }
    return { hidden, exportSection, insertAt, names, length, shared };
  }

  // `bytes` with every hidden memory of `info` exported under a fresh reserved name: the export
  // section is rewritten (count + old entries + new entries), or inserted when there is none.
  function exposeHidden(bytes, info) {
    const names = new SafeSet();
    const entries = [];
    for (let i = 0; i < info.hidden.length; i++) {
      let name = "__rt_memory_" + info.hidden[i];
      while (SetPrototypeHas(info.names, name)) {
        name += "_";
      }
      SetPrototypeAdd(info.names, name);
      SetPrototypeAdd(names, name);
      wasmPushLeb(entries, name.length);
      for (let j = 0; j < name.length; j++) {
        ArrayPrototypePush(entries, StringPrototypeCharCodeAt(name, j));
      }
      ArrayPrototypePush(entries, 2);
      wasmPushLeb(entries, info.hidden[i]);
    }
    const head = [];
    let from = info.insertAt;
    let to = info.insertAt;
    let oldFrom = 0;
    let oldTo = 0;
    if (info.exportSection === null) {
      wasmPushLeb(head, info.hidden.length);
    } else {
      const cur = { p: info.exportSection.start + 1 };
      wasmLeb(bytes, cur); // section size
      const count = wasmLeb(bytes, cur);
      wasmPushLeb(head, count + info.hidden.length);
      from = info.exportSection.start;
      to = info.exportSection.end;
      oldFrom = cur.p;
      oldTo = info.exportSection.end;
    }
    const bodyLength = head.length + (oldTo - oldFrom) + entries.length;
    const size = [];
    wasmPushLeb(size, bodyLength);
    const out = new Uint8Array(from + 1 + size.length + bodyLength + (info.length - to));
    let at = 0;
    const copy = (a, b) => {
      for (let i = a; i < b; i++) {
        out[at++] = bytes[i];
      }
    };
    const push = (list) => {
      for (let i = 0; i < list.length; i++) {
        out[at++] = list[i];
      }
    };
    copy(0, from);
    out[at++] = 7;
    push(size);
    push(head);
    copy(oldFrom, oldTo);
    push(entries);
    copy(to, info.length);
    return { bytes: out, names };
  }

  // `module` was compiled by the wrappers: it may be instantiated (and its added exports hidden).
  function admit(module, names) {
    WeakSetPrototypeAdd(checked, module);
    if (names !== undefined) {
      WeakMapPrototypeSet(added, module, names);
    }
    return module;
  }

  // Compile `bytes` with `compile` (sync: returns the module; async: a promise of it), then, if
  // the module hides a memory, compile the rewritten bytes instead.
  function compileChecked(bytes, compile, then) {
    return then(compile(bytes), (module) => {
      const info = scanMemories(module, bytes);
      if (info.shared) {
        // A shared memory's buffer is a SharedArrayBuffer (a banned global) and enables
        // `memory.atomic.wait`; there are no threads to share it with anyway.
        throw new CompileError("shared WebAssembly memories are not supported");
      }
      if (info.hidden.length === 0) {
        return admit(module);
      }
      const exposed = exposeHidden(bytes, info);
      return then(compile(exposed.bytes), (rewritten) => admit(rewritten, exposed.names));
    });
  }

  // Replace the instance's view of its exports by one without the added memory exports.
  function hideAdded(instance, module) {
    const names = WeakMapPrototypeGet(added, module);
    if (names === undefined) {
      return instance;
    }
    const real = ReflectApply(exportsGetter, instance, []);
    const shown = ObjectCreate(null);
    const keys = ObjectKeys(real);
    for (let i = 0; i < keys.length; i++) {
      if (!SetPrototypeHas(names, keys[i])) {
        shown[keys[i]] = real[keys[i]];
      }
    }
    ObjectDefineProperty(instance, "exports", {
      value: ObjectFreeze(shown),
      writable: false,
      enumerable: false,
      configurable: false,
    });
    return instance;
  }

  // `WebAssembly.Module.exports` without the added memory exports.
  function moduleExportsShown(module) {
    const list = moduleExports(module);
    const names = WeakMapPrototypeGet(added, module);
    if (names === undefined) {
      return list;
    }
    const out = [];
    for (let i = 0; i < list.length; i++) {
      if (!SetPrototypeHas(names, list[i].name)) {
        ArrayPrototypePush(out, list[i]);
      }
    }
    return out;
  }

  function trackInstance(instance) {
    const exports = ReflectApply(exportsGetter, instance, []);
    const names = ObjectKeys(exports);
    for (let i = 0; i < names.length; i++) {
      const value = exports[names[i]];
      if (value !== null && typeof value === "object") {
        track(value); // the host ignores anything but a WebAssembly.Memory
      }
    }
    return instance;
  }

  function requireChecked(module) {
    if (isModule(module) && !WeakSetPrototypeHas(checked, module)) {
      throw new LinkError("this WebAssembly.Module was not compiled by WebAssembly.compile or WebAssembly.Module");
    }
  }

  const compileBytes = (source) => {
    const bytes = copyBytes(source);
    if (bytes === undefined) {
      return origCompile(source);
    }
    return trackPending(compileChecked(bytes, origCompile, PromisePrototypeThen));
  };

  const wrappers = {
    Memory: new Proxy(OrigMemory, {
      construct(target, args, newTarget) {
        // Each descriptor field is read once, into a private copy V8 reads again: a getter
        // cannot answer `shared: false` here and `true` to V8.
        const given = args[0];
        if (given === null || typeof given !== "object") {
          return ReflectConstruct(target, args, newTarget); // V8 throws its own TypeError
        }
        const descriptor = { __proto__: null };
        for (let i = 0; i < memoryFields.length; i++) {
          const value = given[memoryFields[i]];
          if (value !== undefined) {
            descriptor[memoryFields[i]] = value;
          }
        }
        if (descriptor.shared) {
          throw new TypeError("shared WebAssembly.Memory is not supported");
        }
        const memory = ReflectConstruct(target, [descriptor], newTarget);
        track(memory);
        return memory;
      },
    }),
    Module: new Proxy(OrigModule, {
      construct(target, args, newTarget) {
        const bytes = copyBytes(args[0]);
        if (bytes === undefined) {
          return ReflectConstruct(target, args, newTarget);
        }
        return compileChecked(bytes, (b) => ReflectConstruct(target, [b], newTarget), (value, f) => f(value));
      },
    }),
    Instance: new Proxy(OrigInstance, {
      construct(target, args, newTarget) {
        const module = args[0];
        requireChecked(module);
        return hideAdded(trackInstance(ReflectConstruct(target, args, newTarget)), module);
      },
    }),
    compile: new Proxy(origCompile, {
      apply(_target, _this, args) {
        return compileBytes(args[0]);
      },
    }),
    instantiate: new Proxy(origInstantiate, {
      apply(_target, _this, args) {
        const source = args[0];
        const imports = args[1];
        if (isModule(source)) {
          requireChecked(source);
          return trackPending(PromisePrototypeThen(
            origInstantiate(source, imports),
            (instance) => hideAdded(trackInstance(instance), source),
          ));
        }
        // instantiate(bytes) = compile (+ rewrite) + instantiate the module the wrappers produced.
        return trackPending(PromisePrototypeThen(
          compileBytes(source),
          (module) =>
            PromisePrototypeThen(origInstantiate(module, imports), (instance) => ({
              module,
              instance: hideAdded(trackInstance(instance), module),
            })),
        ));
      },
    }),
  };
  for (const name of ["compileStreaming", "instantiateStreaming"]) {
    const orig = WA[name];
    if (typeof orig !== "function") {
      continue;
    }
    // Streaming would hand the bytes to V8 directly: read them first, then take the paths above.
    const viaBytes = (source) =>
      PromisePrototypeThen(PromiseResolve(source), (response) => response.arrayBuffer());
    wrappers[name] = new Proxy(orig, {
      apply(_target, _this, args) {
        return PromisePrototypeThen(
          viaBytes(args[0]),
          (bytes) => name === "compileStreaming" ? compileBytes(bytes) : ReflectApply(wrappers.instantiate, WA, [bytes, args[1]]),
        );
      },
    });
  }
  const names = ObjectKeys(wrappers);
  for (let i = 0; i < names.length; i++) {
    const name = names[i];
    const wrapper = wrappers[name];
    WeakSetPrototypeAdd(wasmWrappers, wrapper);
    ObjectDefineProperty(WA, name, { value: wrapper, writable: true, configurable: true, enumerable: false });
  }
  // The static is read through the Module wrapper (a Proxy of `OrigModule`).
  ObjectDefineProperty(OrigModule, "exports", {
    value: function exports(module) {
      return moduleExportsShown(module);
    },
    writable: true,
    configurable: true,
    enumerable: false,
  });
  for (const [name, wrapper] of [["Memory", wrappers.Memory], ["Module", wrappers.Module], ["Instance", wrappers.Instance]]) {
    ObjectDefineProperty(WA[name].prototype, "constructor", {
      value: wrapper,
      writable: true,
      configurable: true,
      enumerable: false,
    });
  }
}
wrapWebAssembly();

// ---------------------------------------------------------------------------------------------
// Resizable ArrayBuffers (D-A2). They commit memory through their own reservation, not the
// counting allocator, and one `fill` over a huge buffer cannot be interrupted: every growth
// (construction with `maxByteLength`, `resize`, `transfer`, `transferToFixedLength`) is admitted
// against the task's memory limit first (`op_rt_mem_admit`), else it is a `RangeError`. Each
// length is converted once and passed on as a number, so a `valueOf` cannot answer differently
// to V8. The reservation itself (`maxByteLength`) is only address space.
// ---------------------------------------------------------------------------------------------

function wrapArrayBuffer() {
  const OrigArrayBuffer = globalThis.ArrayBuffer;
  const proto = OrigArrayBuffer.prototype;
  const toLength = (value) => {
    const n = +value;
    return n !== n ? 0 : MathTrunc(n);
  };
  const admitGrowth = (from, to) => {
    if (to > from && !op_rt_mem_admit(to - from)) {
      throw new RangeError("Array buffer allocation failed");
    }
  };
  const Wrapped = new Proxy(OrigArrayBuffer, {
    construct(target, args, newTarget) {
      const options = args[1];
      if (options === null || typeof options !== "object") {
        return ReflectConstruct(target, args, newTarget);
      }
      const maxByteLength = options.maxByteLength;
      if (maxByteLength === undefined) {
        return ReflectConstruct(target, [args[0]], newTarget);
      }
      const length = toLength(args[0]);
      admitGrowth(0, length);
      return ReflectConstruct(target, [length, { maxByteLength }], newTarget);
    },
  });
  const methods = {
    resize(newLength) {
      const n = toLength(newLength);
      admitGrowth(ArrayBufferPrototypeGetByteLength(this), n);
      return ReflectApply(origResize, this, [n]);
    },
    transfer(newLength) {
      return transferWith(origTransfer, this, newLength);
    },
    transferToFixedLength(newLength) {
      return transferWith(origTransferToFixedLength, this, newLength);
    },
  };
  const origResize = proto.resize;
  const origTransfer = proto.transfer;
  const origTransferToFixedLength = proto.transferToFixedLength;
  const transferWith = (orig, buffer, newLength) => {
    const current = ArrayBufferPrototypeGetByteLength(buffer);
    if (newLength === undefined) {
      return ReflectApply(orig, buffer, []);
    }
    const n = toLength(newLength);
    admitGrowth(current, n);
    return ReflectApply(orig, buffer, [n]);
  };
  const names = ObjectKeys(methods);
  for (let i = 0; i < names.length; i++) {
    if (typeof proto[names[i]] === "function") {
      ObjectDefineProperty(proto, names[i], { value: methods[names[i]], writable: true, configurable: true, enumerable: false });
    }
  }
  ObjectDefineProperty(proto, "constructor", { value: Wrapped, writable: true, configurable: true, enumerable: false });
  defineGlobal("ArrayBuffer", Wrapped);
}
wrapArrayBuffer();

// ---------------------------------------------------------------------------------------------
// Sandbox hardening (SECURITY.md): banned globals; `Deno` is reduced to the read-only env shim by `__rt_seal`.
// ---------------------------------------------------------------------------------------------

// V8 re-installs some of these after a context is deserialized from the snapshot (conditional
// features such as `SharedArrayBuffer`), so the same removal runs again in `__rt_seal`.
function removeBannedGlobals() {
  for (const name of ["Worker", "SharedArrayBuffer", "ShadowRealm", "caches", "SharedWorker"]) {
    try {
      delete globalThis[name];
    } catch (_) {
      // non-configurable
    }
    if (typeof globalThis[name] !== "undefined") {
      try {
        ObjectDefineProperty(globalThis, name, { value: undefined, writable: false, configurable: false });
      } catch (_) {
        // ignore
      }
    }
  }
  const atomics = globalThis.Atomics;
  if (atomics) {
    for (const name of ["wait", "waitAsync", "notify"]) {
      try {
        delete atomics[name];
      } catch (_) {
        // ignore
      }
    }
  }
}
removeBannedGlobals();

// deno_core re-attaches the ops through `globalThis.Deno.core` whenever it restores a snapshot, so
// `Deno` still has to hold `core` when this module ends (the snapshot is taken now). The executor
// calls `__rt_seal()` right after it creates each isolate, before any guest code exists: it
// reduces `Deno` to the read-only env shim and removes itself.
defineGlobal("__rt_seal", function sealDeno() {
  removeBannedGlobals();
  wrapWebAssembly(); // no-op unless V8 re-installed WebAssembly when the context was deserialized
  try {
    delete globalThis.Deno;
  } catch (_) {
    // ignore
  }
  ObjectDefineProperty(globalThis, "Deno", {
    value: ObjectFreeze({ __proto__: null, env }),
    writable: false,
    configurable: false,
    enumerable: false,
  });
  try {
    delete globalThis.__rt_seal;
  } catch (_) {
    // ignore
  }
});
try {
  delete globalThis.__bootstrap;
} catch (_) {
  // ignore
}
