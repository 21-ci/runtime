//! `std:sqlite` (also `bun:sqlite`): synchronous, bun:sqlite-compatible SQL on the service's own
//! database (decision D-B1; DNO-25 … DNO-28). Every test drives the
//! real executor through `Executor::start` (see `common/mod.rs`) with guests that import the
//! module and check their own results.

use crate::common;

use std::time::{Duration, Instant};

use common::*;
use rt_exec_deno::ops_sqlite::thread_connections;
use rt_store::{ServiceDataStore, SqlStatement};
use rt_types::{KillReason, Outcome};

/// Prelude of the probes: `eq` / `throws` collect failures instead of aborting the handler.
const PRELUDE: &str = r#"
const failed: string[] = [];
const show = (v: unknown) =>
  JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? `${x}n` : x instanceof Uint8Array ? `u8[${[...x]}]` : x));
const eq = (name: string, got: unknown, want: unknown) => {
  const a = show(got);
  const b = show(want);
  if (a !== b) failed.push(`${name}: got ${a}, want ${b}`);
};
const throws = (name: string, fn: () => unknown, ok: (e: any) => boolean) => {
  try {
    fn();
    failed.push(`${name}: did not throw`);
  } catch (e: any) {
    if (!ok(e)) failed.push(`${name}: wrong error ${e && e.name}: ${e && e.message} (code ${e && e.code}, rt ${e && e.rtCode})`);
  }
};
const verdict = () => new Response(failed.length ? failed.join("\n") : "ok", { status: failed.length ? 500 : 200 });
"#;

async fn run_probe(h: &mut Harness, body: &str) -> Got {
    let src = format!("{PRELUDE}\n{body}");
    let p = h.prepare(&ts_upload(&src)).await;
    let ran = h.run(get("/"), &p).await;
    match ran.outcome {
        Outcome::Ok(_) => read(ran.outcome).await,
        other => {
            let logs: Vec<String> = h.log_records().iter().map(|r| format!("{r:?}")).collect();
            panic!("the probe did not complete: {other:?}\nlogs: {}", logs.join("\n"))
        }
    }
}

fn expect_ok(got: &Got) {
    assert_eq!((got.status, got.text()), (200, "ok".to_string()));
}

// ---------------------------------------------------------------------------------------------
// DNO-25: the bun:sqlite API
// ---------------------------------------------------------------------------------------------

const COMPAT: &str = r#"
import { Database, SQLiteError } from "bun:sqlite";

export default () => {
  // no path: the service's own database
  throws(":memory:", () => new Database(":memory:"), (e) => e.code === "not_supported");
  throws("a path", () => new Database("app.db"), (e) => e.code === "not_supported");

  const db = new Database();
  eq("filename", db.filename, "data.db");
  eq("MAX_QUERY_CACHE_SIZE", Database.MAX_QUERY_CACHE_SIZE, 20);
  db.run("DROP TABLE IF EXISTS t");
  eq("create", db.run("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, photo BLOB, flag INTEGER)").changes, 0);

  // run / lastInsertRowid / changes
  const insert = db.query("INSERT INTO t (name, score, photo, flag) VALUES ($name, $score, $photo, $flag)");
  eq("insert 1", insert.run({ $name: "ann", $score: 1.5, $photo: new Uint8Array([0, 1, 2, 255]), $flag: true }), { changes: 1, lastInsertRowid: 1 });
  eq("insert 2", insert.run({ $name: "bob", $score: 2, $photo: new Uint8Array(0), $flag: false }), { changes: 1, lastInsertRowid: 2 });
  eq("insert 3", insert.run({ $name: null, $score: undefined, $photo: null, $flag: null }), { changes: 1, lastInsertRowid: 3 });

  // get / all / values / iterate
  const byId = db.query("SELECT * FROM t WHERE id = ?");
  eq("get", byId.get(1), { id: 1, name: "ann", score: 1.5, photo: new Uint8Array([0, 1, 2, 255]), flag: 1 });
  eq("get empty blob", byId.get(2).photo, new Uint8Array(0));
  eq("get nulls", byId.get(3), { id: 3, name: null, score: null, photo: null, flag: null });
  eq("get none", byId.get(99), null);
  eq("all", db.query("SELECT id, name FROM t ORDER BY id").all(), [{ id: 1, name: "ann" }, { id: 2, name: "bob" }, { id: 3, name: null }]);
  eq("values", db.query("SELECT id, name FROM t ORDER BY id").values(), [[1, "ann"], [2, "bob"], [3, null]]);
  eq("iterate", [...db.query("SELECT id FROM t ORDER BY id").iterate()], [{ id: 1 }, { id: 2 }, { id: 3 }]);
  const seen: number[] = [];
  for (const row of db.query("SELECT id FROM t ORDER BY id")) seen.push((row as any).id);
  eq("for-of statement", seen, [1, 2, 3]);
  eq("no rows", db.query("SELECT id FROM t WHERE id > 100").all(), []);
  eq("run on a select", db.query("SELECT 1").run().changes, 0);

  // parameters: positional, array, numbered, named with every prefix, strict
  eq("positional", db.query("SELECT name FROM t WHERE id = ? AND score > ?").values(1, 1), [["ann"]]);
  eq("array", db.query("SELECT name FROM t WHERE id = ? AND score > ?").values([1, 1]), [["ann"]]);
  eq("numbered", db.query("SELECT ?2 AS a, ?1 AS b").get("x", "y"), { a: "y", b: "x" });
  eq("named $ : @", db.query("SELECT $a AS a, :b AS b, @c AS c").get({ $a: 1, ":b": 2, "@c": 3 }), { a: 1, b: 2, c: 3 });
  eq("named, bare keys", db.query("SELECT $a AS a, :b AS b").get({ a: 1, b: 2 }), { a: 1, b: 2 });
  eq("named, missing is NULL", db.query("SELECT $a AS a, $b AS b").get({ $a: 1 }), { a: 1, b: null });
  const strict = new Database(undefined, { strict: true });
  eq("strict named", strict.query("SELECT $a AS a").get({ a: 5 }), { a: 5 });
  throws("strict missing", () => strict.query("SELECT $a AS a, $b AS b").get({ a: 1 }), (e) => e instanceof RangeError);
  eq("columnNames", db.query("SELECT id AS x, name FROM t").columnNames, ["x", "name"]);
  eq("paramsCount", db.query("SELECT ?, ?, ?").paramsCount, 3);
  eq("toString", String(db.query("SELECT 7")), "SELECT 7");

  // values: types in, types out
  eq("bigint in", db.query("SELECT ? AS n").get(2n ** 40n), { n: 1099511627776 });
  eq("bool in", db.query("SELECT ? AS a, ? AS b").get(true, false), { a: 1, b: 0 });
  eq("int vs real", db.query("SELECT typeof(?) AS i, typeof(?) AS r, typeof(?) AS n").get(5, 5.5, null), { i: "integer", r: "real", n: "null" });
  eq("text", db.query("SELECT ? AS s").get("héllo \u{1f600}"), { s: "héllo \u{1f600}" });
  eq("blob", db.query("SELECT ? AS b, length(?) AS n").get(new Uint8Array([9, 8, 7]), new Uint8Array([1, 2])), { b: new Uint8Array([9, 8, 7]), n: 2 });
  throws("object binding", () => db.query("SELECT ?").get([{ a: 1 }]), (e) => e instanceof TypeError);
  throws("function binding", () => db.query("SELECT ?").get(() => 1), (e) => e instanceof TypeError);
  throws("huge bigint", () => db.query("SELECT ?").get(2n ** 70n), (e) => e instanceof RangeError);

  // safeIntegers: bigint out
  const safe = new Database(undefined, { safeIntegers: true });
  eq("safeIntegers", safe.query("SELECT ? AS n, 1.5 AS r, 'x' AS s").get(9007199254740993n), { n: 9007199254740993n, r: 1.5, s: "x" });
  eq("safeIntegers run", safe.query("INSERT INTO t (name) VALUES ('s')").run(), { changes: 1, lastInsertRowid: 4n });
  eq("plain integers stay numbers", db.query("SELECT 9007199254740993 AS n").get().n, 9007199254740992);

  // row shapes
  eq("duplicate names: the last wins", db.query("SELECT 1 AS a, 2 AS a").get(), { a: 2 });
  eq("numeric column name", db.query("SELECT 1, 'x'").get(), { "1": 1, "'x'": "x" });
  eq("expression names", db.query("SELECT count(*) FROM t").get(), { "count(*)": 4 });

  // .as(Class)
  class Person { name = ""; hello() { return "hi " + this.name; } }
  const people = db.query("SELECT name FROM t WHERE id = 1").as(Person);
  eq("as: get", (people.get() as Person).hello(), "hi ann");
  eq("as: all", people.all().map((p) => p instanceof Person), [true]);

  // errors
  try {
    db.run("INSERT INTO t (id) VALUES (1)");
    failed.push("unique: did not throw");
  } catch (e: any) {
    eq("unique error", [e instanceof SQLiteError, e instanceof Error, e.name, e.code, e.rtCode, typeof e.errno, typeof e.byteOffset], [true, true, "SQLiteError", "SQLITE_CONSTRAINT_PRIMARYKEY", "sql_error", "number", "number"]);
  }
  throws("syntax", () => db.query("SELEC 1"), (e) => e instanceof SQLiteError && e.code === "SQLITE_ERROR" && e.rtCode === "sql_error");
  throws("no such table", () => db.query("SELECT * FROM nope"), (e) => e instanceof SQLiteError && /no such table/.test(e.message));
  throws("error message has no host path", () => db.query("SELECT * FROM nope"), (e) => !String(e.message).includes("/"));

  // cache: query() is cached by text, prepare() is not; finalize ends a statement
  eq("query cache", db.query("SELECT 1") === db.query("SELECT 1"), true);
  eq("prepare is not cached", db.prepare("SELECT 1") === db.prepare("SELECT 1"), false);
  const s = db.prepare("SELECT 1 AS one");
  eq("before finalize", s.get(), { one: 1 });
  s.finalize();
  throws("after finalize", () => s.get(), () => true);
  for (let i = 0; i < 30; i++) db.query(`SELECT ${i} AS n`); // past the cache size: old ones are finalized
  eq("still works after the cache turned over", db.query("SELECT 5 AS n").get(), { n: 5 });

  // exec: several statements, no result rows
  eq("exec batch", db.exec("INSERT INTO t (name) VALUES ('e1'); INSERT INTO t (name) VALUES ('e2'); UPDATE t SET flag = 9 WHERE name LIKE 'e%'").changes, 2);
  eq("run with params", db.run("UPDATE t SET flag = ? WHERE name = ?", 7, "e1").changes, 1);
  eq("inTransaction", db.inTransaction, false);
  eq("a large result", db.query("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000) SELECT x FROM c").all().length, 5000);

  // readonly
  const ro = new Database(undefined, { readonly: true });
  eq("readonly reads", ro.query("SELECT count(*) AS n FROM t").get().n >= 4, true);
  throws("readonly writes", () => ro.query("DELETE FROM t").run(), (e) => e instanceof SQLiteError && e.code === "SQLITE_READONLY");
  throws("readonly exec", () => ro.exec("DELETE FROM t"), (e) => e.code === "SQLITE_READONLY");

  // unsupported
  throws("loadExtension", () => db.loadExtension("x"), (e) => e.code === "not_supported");
  throws("serialize", () => db.serialize(), (e) => e.code === "not_supported");

  // close
  db.close();
  throws("use after close", () => db.query("SELECT 1"), () => true);
  return verdict();
};
"#;

#[test]
fn ac_dno_25_the_bun_sqlite_api_works_on_the_services_database() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let got = run_probe(&mut h, COMPAT).await;
        expect_ok(&got);
    });
}

const RT_SPECIFIER: &str = r#"
import { Database } from "std:sqlite";
import Default from "std:sqlite";
import { Database as BunDatabase } from "bun:sqlite";
export default () => {
  eq("same class", [Database === Default, Database === BunDatabase], [true, true]);
  const db = new Database();
  eq("select", db.query("SELECT 41 + 1 AS answer").get(), { answer: 42 });
  return verdict();
};
"#;

#[test]
fn ac_dno_25_rt_sqlite_and_bun_sqlite_are_one_module() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        expect_ok(&run_probe(&mut h, RT_SPECIFIER).await);
    });
}

/// The sample service (`sample/deno/sqlite-sync`) answers its documented flows.
const SAMPLE: &str = include_str!("../../../../sample/deno/sqlite-sync/main.ts");

fn json_request(method: http::Method, path: &str, body: Option<&str>) -> rt_types::TaskSpec {
    let mut req = rt_types::HttpRequest::get(path);
    req.method = method;
    if let Some(b) = body {
        req.headers
            .insert("content-type", "application/json".parse().expect("header value"));
        req.body = rt_types::Body::from_bytes(bytes::Bytes::from(b.to_string()));
    }
    spec().payload(rt_types::Payload::Http(req)).finish()
}

async fn send(
    h: &mut Harness,
    p: &std::sync::Arc<rt_exec_deno::DenoPrepared>,
    method: http::Method,
    path: &str,
    body: Option<&str>,
) -> Got {
    let ran = h.run(json_request(method, path, body), p).await;
    read(ran.outcome).await
}

#[test]
fn ac_dno_25_the_sqlite_sync_sample_serves_its_documented_flows() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SAMPLE)).await;
        let got = send(&mut h, &p, http::Method::GET, "/", None).await;
        assert_eq!((got.status, got.json()), (200, serde_json::json!({ "todos": [] })));
        let got = send(&mut h, &p, http::Method::POST, "/", Some(r#"{"title": "Buy milk"}"#)).await;
        assert_eq!(
            (got.status, got.json()),
            (201, serde_json::json!({ "id": 1, "title": "Buy milk", "done": false }))
        );
        let got = send(
            &mut h,
            &p,
            http::Method::POST,
            "/bulk",
            Some(r#"{"titles": ["Call mom", "Pay rent"]}"#),
        )
        .await;
        assert_eq!((got.status, got.json()), (201, serde_json::json!({ "ids": [2, 3] })));
        let got = send(
            &mut h,
            &p,
            http::Method::POST,
            "/bulk",
            Some(r#"{"titles": ["ok", " "]}"#),
        )
        .await;
        assert_eq!(got.status, 400, "{}", got.text());
        let got = send(&mut h, &p, http::Method::PATCH, "/1", Some(r#"{"done": true}"#)).await;
        assert_eq!(got.status, 200, "{}", got.text());
        assert_eq!(
            (got.json()["id"].clone(), got.json()["done"].clone()),
            (serde_json::json!(1), serde_json::json!(true))
        );
        let got = send(&mut h, &p, http::Method::GET, "/stats", None).await;
        assert_eq!(got.json(), serde_json::json!({ "total": 3, "open": 2 }));
        let got = send(&mut h, &p, http::Method::GET, "/?done=1", None).await;
        assert_eq!(got.json()["todos"].as_array().map(Vec::len), Some(1));
        let got = send(&mut h, &p, http::Method::GET, "/", None).await;
        assert_eq!(got.json()["todos"].as_array().map(Vec::len), Some(3));
        let got = send(&mut h, &p, http::Method::DELETE, "/1", None).await;
        assert_eq!(got.json(), serde_json::json!({ "id": 1, "deleted": true }));
        assert_eq!(send(&mut h, &p, http::Method::GET, "/1", None).await.status, 404);
        assert_eq!(send(&mut h, &p, http::Method::DELETE, "/1", None).await.status, 404);
        assert_eq!(
            send(&mut h, &p, http::Method::POST, "/", Some(r#"{"nope": 1}"#))
                .await
                .status,
            400
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-26: the guards of the async std:sql hold, and the two APIs share one database
// ---------------------------------------------------------------------------------------------

const GUARDS: &str = r#"
import { Database } from "std:sqlite";
import { sql } from "std:sql";
import { kv } from "std:kv";

export default async () => {
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS g (v INTEGER)");
  const denied = (name: string, text: string) =>
    throws(name, () => db.run(text), (e) => e.rtCode === "sql_not_authorized" && e.code === "SQLITE_AUTH");
  denied("ATTACH", "ATTACH DATABASE ':memory:' AS other");
  denied("temp table", "CREATE TEMP TABLE scratch (x)");
  denied("temp view", "CREATE TEMP VIEW tv AS SELECT 1");
  denied("_rt_ table", "SELECT * FROM _rt_kv");
  denied("_rt_ table, other case", "SELECT * FROM _RT_Blobs");
  denied("journal_mode", "PRAGMA journal_mode = DELETE");
  denied("synchronous", "PRAGMA synchronous = OFF");
  denied("max_page_count", "PRAGMA max_page_count = 100000");
  denied("guest BEGIN", "BEGIN");
  denied("guest SAVEPOINT", "SAVEPOINT a");
  denied("VACUUM", "VACUUM");
  throws("load_extension", () => db.query("SELECT load_extension('x')").get(), (e) => e.rtCode === "sql_not_authorized");
  // an internal statement's text, replayed after the KV code used it
  await kv.set("k", 1);
  await kv.get("k");
  throws("internal text", () => db.query("SELECT value, tag FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)").get("k", 0), (e) => e.rtCode === "sql_not_authorized");
  eq("the database is still usable", db.query("SELECT 1 AS one").get(), { one: 1 });

  // result caps: 10,000 rows, 10 MiB of values
  const rows = (n: number) => db.query(`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < ${n}) SELECT x FROM c`);
  eq("10,000 rows", rows(10000).all().length, 10000);
  throws("10,001 rows", () => rows(10001).all(), (e) => e.rtCode === "sql_result_too_large");
  throws("byte cap", () => db.query("SELECT zeroblob(4000000) AS b FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)").all(), (e) => e.rtCode === "sql_result_too_large");
  eq("get stops after one row", rows(10001).get(), { x: 1 });

  // async and sync see each other's writes
  await sql.execute("INSERT INTO g VALUES (1)");
  eq("async write, sync read", db.query("SELECT count(*) AS n FROM g").get().n, 1);
  db.run("INSERT INTO g VALUES (2)");
  eq("sync write, async read", (await sql.execute("SELECT sum(v) AS s FROM g")).rows, [[3]]);
  await sql.batch([{ sql: "INSERT INTO g VALUES (?)", args: [3] }, { sql: "INSERT INTO g VALUES (?)", args: [4] }]);
  eq("batch then sync", db.query("SELECT sum(v) AS s FROM g").get().s, 10);
  db.run("ALTER TABLE g ADD COLUMN w TEXT DEFAULT 'd'");
  eq("sync DDL, async select *", (await sql.execute("SELECT * FROM g WHERE v = 1")).rows, [[1, "d"]]);
  await sql.execute("ALTER TABLE g ADD COLUMN z INTEGER DEFAULT 0");
  eq("async DDL, sync select *", db.query("SELECT * FROM g WHERE v = 1").get(), { v: 1, w: "d", z: 0 });
  await sql.transaction(async (tx) => {
    await tx.execute("INSERT INTO g (v) VALUES (100)");
  });
  eq("after an async transaction", db.query("SELECT count(*) AS n FROM g WHERE v = 100").get().n, 1);
  // a blob written by the sync API reads back through the async API as a Uint8Array
  db.run("CREATE TABLE IF NOT EXISTS b (x BLOB)");
  db.query("INSERT INTO b VALUES (?)").run(new Uint8Array([4, 5, 6]));
  const back = (await sql.execute("SELECT x FROM b")).rows[0][0];
  eq("blob across APIs", [back instanceof Uint8Array, [...back]], [true, [4, 5, 6]]);
  // KV next to sync writes
  for (let i = 0; i < 5; i++) {
    await kv.set(`k${i}`, i);
    db.run("INSERT INTO g (v) VALUES (?)", 200 + i);
  }
  eq("kv and sync sql together", [await kv.get("k4"), db.query("SELECT count(*) AS n FROM g WHERE v >= 200").get().n], [4, 5]);
  return verdict();
};
"#;

#[test]
fn ac_dno_26_guards_hold_and_sync_and_async_sql_share_one_database() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let got = run_probe(&mut h, GUARDS).await;
        expect_ok(&got);
    });
}

const RUNAWAY: &str = r#"
import { Database } from "std:sqlite";
export default () => {
  const db = new Database();
  try {
    db.query("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c").get();
    return new Response("not aborted", { status: 500 });
  } catch (e: any) {
    // the connection recovers
    const after = db.query("SELECT 7 AS n").get();
    return new Response(`${e.rtCode} ${e.code} ${after.n}`);
  }
};
"#;

#[test]
fn ac_dno_26_a_runaway_statement_is_aborted_at_the_short_sync_budget() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(RUNAWAY)).await;
        let started = Instant::now();
        let (got, _) = h.get("/", &p).await;
        let took = started.elapsed();
        assert_eq!(got.text(), "sql_timeout SQLITE_INTERRUPT 7");
        assert!(
            took >= Duration::from_millis(90),
            "the budget is 100 ms, aborted after {took:?}"
        );
        assert!(took < Duration::from_secs(5), "aborted after {took:?}");
    });
}

const QUOTA: &str = r#"
import { Database } from "std:sqlite";
export default () => {
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS q (b BLOB)");
  let inserted = 0;
  let code = "none";
  for (let i = 0; i < 100; i++) {
    try {
      db.run("INSERT INTO q VALUES (zeroblob(32768))");
      inserted++;
    } catch (e: any) {
      code = e.rtCode;
      break;
    }
  }
  db.run("DELETE FROM q"); // deleting still works over the ceiling
  return new Response(`${code} ${inserted > 0 && inserted < 100} ${db.query("SELECT count(*) AS n FROM q").get().n}`);
};
"#;

#[test]
fn ac_dno_26_the_storage_ceiling_stops_sync_writes_with_storage_quota() {
    run_local(async {
        let mut h = Harness::with_data(cfg0(), |d| d.max_db_bytes = 256 * 1024);
        let p = h.prepare(&ts_upload(QUOTA)).await;
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.text(), "storage_quota true 0");
    });
}

const BUSY: &str = r#"
import { Database } from "std:sqlite";
import { sql } from "std:sql";
export default async () => {
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS w (v INTEGER)");
  let seen = "none";
  let reads = -1;
  await sql.transaction(async (tx) => {
    await tx.execute("INSERT INTO w VALUES (1)"); // the pool's writer holds the write lock now
    reads = db.query("SELECT count(*) AS n FROM w").get().n; // WAL: reads are not blocked
    try {
      db.run("INSERT INTO w VALUES (2)");
    } catch (e: any) {
      seen = e.rtCode + " " + e.code;
    }
  });
  db.run("INSERT INTO w VALUES (3)"); // the lock is free again
  return new Response(`${seen} reads=${reads} total=${db.query("SELECT count(*) AS n FROM w").get().n}`);
};
"#;

#[test]
fn ac_dno_26_a_sync_write_behind_the_pool_writer_fails_fast_with_sql_busy() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(BUSY)).await;
        let started = Instant::now();
        let (got, _) = h.get("/", &p).await;
        assert_eq!(got.text(), "sql_busy SQLITE_BUSY reads=0 total=2");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "waited {:?}",
            started.elapsed()
        );
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-27: transactions
// ---------------------------------------------------------------------------------------------

const TRANSACTIONS: &str = r#"
import { Database, SQLiteError } from "std:sqlite";

export default async () => {
  const db = new Database();
  db.run("DROP TABLE IF EXISTS tx");
  db.run("CREATE TABLE tx (id INTEGER PRIMARY KEY, v TEXT UNIQUE)");
  const insert = db.query("INSERT INTO tx (v) VALUES (?)");
  const all = () => db.query("SELECT v FROM tx ORDER BY id").values().map((r: any) => r[0]);

  // commit; the function's result and arguments pass through; so does `this`
  const add = db.transaction(function (this: any, ...vs: string[]) {
    for (const v of vs) insert.run(v);
    return [vs.length, this && this.tag];
  });
  eq("commit", add.call({ tag: "t" }, "a", "b", "c"), [3, "t"]);
  eq("committed", all(), ["a", "b", "c"]);
  eq("not in a transaction afterwards", db.inTransaction, false);

  // a throw rolls everything back and propagates the same error
  const boom = new Error("boom");
  const failing = db.transaction(() => { insert.run("x"); throw boom; });
  throws("throw", () => failing(), (e) => e === boom);
  eq("rolled back", all(), ["a", "b", "c"]);
  eq("not in a transaction after a throw", db.inTransaction, false);

  // an SQL error inside rolls back too (UNIQUE)
  const dup = db.transaction(() => { insert.run("y"); insert.run("a"); });
  throws("unique inside", () => dup(), (e) => e instanceof SQLiteError && e.code === "SQLITE_CONSTRAINT_UNIQUE");
  eq("rolled back after the SQL error", all(), ["a", "b", "c"]);

  // nested: the inner one is a savepoint; its throw, caught by the outer, undoes only itself
  const inner = db.transaction((v: string, fail: boolean) => { insert.run(v); if (fail) throw new Error("inner"); });
  const outer = db.transaction(() => {
    insert.run("o1");
    inner("i1", false);
    try { inner("i2", true); } catch (_) { /* caught: only i2 is undone */ }
    insert.run("o2");
    eq("inside", db.inTransaction, true);
  });
  outer();
  eq("nested partial rollback", all(), ["a", "b", "c", "o1", "i1", "o2"]);

  // the outer one throwing undoes the inner one that had committed (released)
  const outer2 = db.transaction(() => { inner("j1", false); throw new Error("outer2"); });
  throws("outer throws", () => outer2(), (e) => e.message === "outer2");
  eq("outer rollback undoes released savepoints", all(), ["a", "b", "c", "o1", "i1", "o2"]);

  // BEGIN modes
  eq("immediate", db.transaction(() => { insert.run("m1"); return 1; }).immediate(), 1);
  eq("exclusive", db.transaction(() => { insert.run("m2"); return 2; }).exclusive(), 2);
  eq("deferred", db.transaction(() => { insert.run("m3"); return 3; }).deferred(), 3);
  eq("modes committed", all().slice(-3), ["m1", "m2", "m3"]);

  // a promise is refused (JS cannot await inside a transaction) and rolled back; an async
  // function is refused before anything begins
  throws("async function", () => db.transaction((async () => { insert.run("p"); }) as any), (e) => e instanceof TypeError && /synchronous/.test(e.message));
  const promiseFn = db.transaction((() => { insert.run("p"); return Promise.resolve(); }) as any);
  throws("a promise result", () => promiseFn(), (e) => e instanceof TypeError && /synchronous/.test(e.message));
  eq("async function rolled back", all().includes("p"), false);
  eq("and the connection is out of the transaction", db.inTransaction, false);

  // two Database objects share the service's connection: one transaction covers both
  const other = new Database();
  db.transaction(() => { insert.run("s1"); other.run("INSERT INTO tx (v) VALUES ('s2')"); })();
  eq("shared connection", all().slice(-2), ["s1", "s2"]);
  throws("rollback across objects", () => db.transaction(() => { other.run("INSERT INTO tx (v) VALUES ('s3')"); throw new Error("x"); })(), () => true);
  eq("rolled back across objects", all().includes("s3"), false);

  // a statement budget abort inside a transaction rolls the whole transaction back
  const slow = db.transaction(() => {
    insert.run("slow");
    db.query("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c").get();
  });
  throws("budget inside a transaction", () => slow(), (e) => e.rtCode === "sql_timeout");
  eq("rolled back after the budget abort", [all().includes("slow"), db.inTransaction], [false, false]);
  insert.run("after");
  eq("usable afterwards", all().includes("after"), true);
  return verdict();
};
"#;

#[test]
fn ac_dno_27_transactions_commit_roll_back_and_nest() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let got = run_probe(&mut h, TRANSACTIONS).await;
        expect_ok(&got);
    });
}

const KILLED_IN_TX: &str = r#"
import { Database } from "std:sqlite";
export default (req: Request) => {
  const db = new Database();
  const path = new URL(req.url).pathname;
  if (path === "/setup") {
    db.run("CREATE TABLE IF NOT EXISTS k (v INTEGER)");
    return new Response("ok");
  }
  if (path === "/stuck") {
    // BEGIN IMMEDIATE (the write lock is taken), a write, then a loop the CPU limit has to kill:
    // no JS `finally` runs.
    db.transaction(() => {
      db.run("INSERT INTO k VALUES (1)");
      for (;;) { /* spin */ }
    }).immediate();
    return new Response("unreachable");
  }
  // /after: the connection must be out of the transaction and the lock free
  const before = db.inTransaction;
  db.run("INSERT INTO k VALUES (2)");
  return Response.json({ inTx: before, rows: db.query("SELECT v FROM k ORDER BY v").values() });
};
"#;

#[test]
fn ac_dno_27_a_task_killed_inside_a_transaction_leaves_nothing_open() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(KILLED_IN_TX)).await;
        h.get("/setup", &p).await;

        let ran = h.run(spec().cpu_limit(100).http_get("/stuck").finish(), &p).await;
        assert!(
            matches!(ran.outcome, Outcome::Killed(KillReason::CpuLimit)),
            "{:?}",
            ran.outcome
        );
        h.settle(Duration::from_millis(50)).await;

        // A pool connection can write at once (the killed task's write lock is gone) ...
        h.data
            .sql(svc(1))
            .execute(SqlStatement {
                sql: "INSERT INTO k VALUES (3)".into(),
                args: vec![],
            })
            .await
            .expect("the write lock was released");
        // ... and the next request sees a connection in autocommit with the killed task's write undone.
        let (got, _) = h.get("/after", &p).await;
        let v = got.json();
        assert_eq!(v["inTx"], false, "{v}");
        assert_eq!(
            v["rows"],
            serde_json::json!([[2], [3]]),
            "the interrupted transaction's insert was rolled back: {v}"
        );
    });
}

const ASYNC_TX: &str = r#"
import { Database } from "std:sqlite";
export default () => {
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS ta (v INTEGER)");
  // An async function would run its first part inside BEGIN and the rest after COMMIT: refused
  // when the transaction is made, before anything begins or is written.
  let ran = false;
  const body = async () => { ran = true; db.run("INSERT INTO ta VALUES (1)"); await 0; };
  throws("async function", () => db.transaction(body), (e) => e instanceof TypeError && /synchronous/.test(e.message));
  throws("async generator", () => db.transaction(async function* () {}), (e) => e instanceof TypeError);
  eq("nothing ran", ran, false);
  eq("not in a transaction", db.inTransaction, false);
  eq("nothing written", db.query("SELECT count(*) AS n FROM ta").get(), { n: 0 });
  // a plain function returning a promise is still rolled back (the old guard)
  throws("promise result", () => db.transaction(() => { db.run("INSERT INTO ta VALUES (2)"); return Promise.resolve(); })(), (e) => e instanceof TypeError);
  eq("rolled back", db.query("SELECT count(*) AS n FROM ta").get(), { n: 0 });
  return verdict();
};
"#;

#[test]
fn ac_dno_27_an_async_function_is_refused_before_begin() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        expect_ok(&run_probe(&mut h, ASYNC_TX).await);
    });
}

// ---------------------------------------------------------------------------------------------
// No guest code runs inside a SQL op (the op holds the OpState borrow and the connection lock: a
// guest callback that re-entered would abort the node or deadlock its Machine thread)
// ---------------------------------------------------------------------------------------------

const NO_GUEST_CODE_IN_OPS: &str = r#"
import { Database } from "std:sqlite";
import { sql } from "std:sql";

export default async () => {
  const db = new Database();
  let reentered = 0;
  const reenter = () => { reentered++; return db.query("SELECT 1 AS one").all(); };

  // Result objects are built with own data properties, never through an inherited setter.
  const traps = ["a", "changes", "lastInsertRowid", "rows", "columns", "rowsAffected"];
  for (const k of traps) {
    Object.defineProperty(Object.prototype, k, { configurable: true, set(_v) { reenter(); } });
  }
  try {
    eq("duplicate names (all)", db.query("SELECT 1 a, 2 a").all(), [{ a: 2 }]);
    eq("duplicate names (get)", db.query("SELECT 1 a, 2 a").get(), { a: 2 });
    eq("own property", Object.hasOwn(db.query("SELECT 1 a, 2 a").get(), "a"), true);
    eq("exec result", db.run("SELECT 1").changes, 0);
    eq("run result", db.query("SELECT 1").run().changes, 0);
    eq("std:sql result", (await sql.execute("SELECT 3 AS a")).rows, [[3]]);
  } finally {
    for (const k of traps) delete (Object.prototype as any)[k];
  }
  eq("no setter ran inside an op", reentered, 0);

  // Arguments are snapshotted in JS before the op: getters run there, once, and may use the
  // database; a getter's error is the guest's own.
  let reads = 0;
  const args: unknown[] = [1];
  Object.defineProperty(args, 1, { enumerable: true, get() { reads++; reenter(); return 2; } });
  eq("getter argument", db.query("SELECT ? AS x, ? AS y").get(args), { x: 1, y: 2 });
  eq("getter read once", reads, 1);
  const holey = [1, , 3];
  Object.defineProperty(Array.prototype, 1, { configurable: true, get() { reenter(); return 22; } });
  try {
    eq("a hole read through the prototype", db.query("SELECT ? AS x, ? AS y, ? AS z").get(holey), { x: 1, y: 22, z: 3 });
  } finally {
    delete (Array.prototype as any)[1];
  }
  const bad: unknown[] = [];
  Object.defineProperty(bad, 0, { enumerable: true, get() { throw new RangeError("mine"); } });
  throws("a throwing getter", () => db.query("SELECT ?").get(bad), (e) => e instanceof RangeError && e.message === "mine");
  throws("a named object", () => db.query("SELECT ?").get([{ k: 1 }]), (e) => e instanceof TypeError);

  // std:sql: objects are bound as their JSON text, made in JS (toJSON runs there).
  let json = 0;
  const r = await sql.execute("SELECT ? AS j", [{ toJSON() { json++; reenter(); return { k: 1 }; } }]);
  eq("toJSON", r.rows, [['{"k":1}']]);
  eq("toJSON once", json, 1);
  let err: any;
  try { await sql.execute("SELECT ?", [{ toJSON() { throw new RangeError("json"); } }]); } catch (e) { err = e; }
  eq("a toJSON error is the guest's", err instanceof RangeError && err.message, "json");
  const getterArgs: unknown[] = [];
  Object.defineProperty(getterArgs, 0, { enumerable: true, get() { reenter(); return 9; } });
  eq("std:sql getter argument", (await sql.execute("SELECT ? AS n", getterArgs)).rows, [[9]]);
  const stmt = { get sql() { reenter(); return "SELECT ? AS n"; }, get args() { reenter(); return [5]; } };
  eq("batch getters", (await sql.batch([stmt], "read"))[0].rows, [[5]]);
  err = undefined;
  try { await sql.batch([{ get sql(): string { throw new RangeError("sql"); } }]); } catch (e) { err = e; }
  eq("a batch getter error is the guest's", err instanceof RangeError && err.message, "sql");
  return verdict();
};
"#;

#[test]
fn guest_getters_setters_and_to_json_never_run_inside_a_sql_op() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        expect_ok(&run_probe(&mut h, NO_GUEST_CODE_IN_OPS).await);
        // The node survived and the connection is usable.
        expect_ok(&run_probe(&mut h, RT_SPECIFIER).await);
    });
}

// ---------------------------------------------------------------------------------------------
// DNO-28: connections, CPU, perf
// ---------------------------------------------------------------------------------------------

const SHARED: &str = r#"
import { Database } from "std:sqlite";
export default () => {
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS s (v INTEGER)");
  db.run("INSERT INTO s VALUES (1)");
  return new Response(String(db.query("SELECT count(*) AS n FROM s").get().n));
};
"#;

#[test]
fn ac_dno_28_isolates_of_one_service_on_a_machine_share_one_connection() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SHARED)).await;
        let before = thread_connections(); // other tests may have run on this thread
        for want in 1..=4 {
            // fresh isolate every time (no reuse), same service
            let (got, _) = h.get("/", &p).await;
            assert_eq!(got.text(), want.to_string());
        }
        assert_eq!(
            thread_connections(),
            before + 1,
            "one connection per (service, Machine)"
        );
        assert_eq!(
            h.data.open_connections(),
            1,
            "and it holds one slot of storage.max_open_dbs"
        );

        // another service gets its own database and its own connection
        let other = h.run(spec().service(2).http_get("/").finish(), &p).await;
        assert_eq!(read(other.outcome).await.text(), "1", "service 2 has its own database");
        assert_eq!(thread_connections(), before + 2);
        assert_eq!(h.data.open_connections(), 2);
    });
}

const REUSED: &str = r#"
import { Database } from "std:sqlite";
const db = new Database();               // module scope: made once per isolate
db.run("CREATE TABLE IF NOT EXISTS r (v INTEGER)");
const insert = db.query("INSERT INTO r VALUES (?)");
export default () => {
  insert.run(1);
  return new Response(String(db.query("SELECT count(*) AS n FROM r").get().n));
};
"#;

#[test]
fn ac_dno_28_a_database_made_at_module_scope_survives_isolate_reuse() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(REUSED)).await;
        let knobs = rt_types::ServiceKnobs {
            reuse_isolates: true,
            ..rt_types::ServiceKnobs::default()
        };
        for want in 1..=3 {
            let ran = h.run(spec().knobs(knobs).http_get("/").finish(), &p).await;
            assert_eq!(read(ran.outcome).await.text(), want.to_string());
            h.settle(Duration::from_millis(20)).await;
        }
        assert_eq!(
            h.exec.warm_len(),
            1,
            "the isolate with the module-scope Database was kept and reused"
        );
    });
}

const CPU_BURN: &str = r#"
import { Database } from "std:sqlite";
export default () => {
  const db = new Database();
  const q = db.query("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 20000) SELECT count(*) AS n FROM c");
  let total = 0;
  for (;;) {
    try { total += q.get().n; } catch (_) { total++; }   // all of the work is inside SQLite (a slow box may hit the 100 ms budget)
  }
};
"#;

#[test]
fn ac_dno_28_sync_sql_cpu_counts_against_the_tasks_cpu_limit() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(CPU_BURN)).await;
        let started = Instant::now();
        let ran = h.run(spec().cpu_limit(300).http_get("/").finish(), &p).await;
        assert!(
            matches!(ran.outcome, Outcome::Killed(KillReason::CpuLimit)),
            "{:?}",
            ran.outcome
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "killed after {:?}",
            started.elapsed()
        );
    });
}

/// `std:sql` (async, one pool hop per call) against `std:sqlite` (inline) for a point SELECT, µs per
/// call, measured by the difference between a long and a short loop in one request each.
/// `cargo test -p rt-exec-deno --release --test it sdk_sqlite::perf -- --ignored --nocapture`
const PERF: &str = r#"
import { Database } from "std:sqlite";
import { sql } from "std:sql";
export default async (req: Request) => {
  const url = new URL(req.url);
  const n = Number(url.searchParams.get("n"));
  const mode = url.searchParams.get("mode");
  const db = new Database();
  db.run("CREATE TABLE IF NOT EXISTS p (id INTEGER PRIMARY KEY, v TEXT)");
  if (db.query("SELECT count(*) AS n FROM p").get().n === 0) {
    db.transaction(() => {
      const ins = db.query("INSERT INTO p (id, v) VALUES (?, ?)");
      for (let i = 1; i <= 1000; i++) ins.run(i, "value-" + i);
    })();
  }
  let hits = 0;
  if (mode === "async") {
    for (let i = 0; i < n; i++) hits += (await sql.execute("SELECT v FROM p WHERE id = ?", [(i % 1000) + 1])).rows.length;
  } else if (mode === "sync") {
    const q = db.query("SELECT v FROM p WHERE id = ?");
    for (let i = 0; i < n; i++) hits += q.get((i % 1000) + 1) === null ? 0 : 1;
  } else if (mode === "sync-values") {
    const q = db.query("SELECT v FROM p WHERE id = ?");
    for (let i = 0; i < n; i++) hits += q.values((i % 1000) + 1).length;
  }
  return new Response(String(hits));
};
"#;

#[test]
#[ignore = "perf"]
fn perf_point_select_async_rt_sql_vs_sync_rt_sqlite() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(1));
        h.warm_up().await;
        let p = h.prepare(&ts_upload(PERF)).await;
        let request = |mode: &'static str, n: u32| {
            spec()
                .cpu_limit(60_000)
                .wall_limit(120_000)
                .http_get(&format!("/?mode={mode}&n={n}"))
                .finish()
        };
        // seed + warm the code paths
        for mode in ["async", "sync", "sync-values"] {
            let ran = h.run(request(mode, 200), &p).await;
            read(ran.outcome).await;
        }
        let (short, long) = (100u32, 20_000u32);
        let mut report = Vec::new();
        for mode in ["async", "sync", "sync-values"] {
            let mut per_call = Vec::new();
            let mut cpu_per_call = Vec::new();
            for _ in 0..5 {
                let t = Instant::now();
                let ran = h.run(request(mode, short), &p).await;
                let got = read(ran.outcome).await;
                assert_eq!(got.text(), short.to_string());
                let (t_short, c_short) = (t.elapsed(), got.timings.cpu);
                let t = Instant::now();
                let ran = h.run(request(mode, long), &p).await;
                let got = read(ran.outcome).await;
                assert_eq!(got.text(), long.to_string());
                let (t_long, c_long) = (t.elapsed(), got.timings.cpu);
                per_call.push(t_long.saturating_sub(t_short).as_secs_f64() * 1e6 / f64::from(long - short));
                cpu_per_call.push(c_long.saturating_sub(c_short).as_secs_f64() * 1e6 / f64::from(long - short));
            }
            per_call.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            cpu_per_call.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            report.push((mode, per_call[0], per_call[per_call.len() / 2]));
            eprintln!(
                "  {mode:12} Machine-thread CPU: best {:.2} us/call, median {:.2} us/call",
                cpu_per_call[0],
                cpu_per_call[cpu_per_call.len() / 2]
            );
        }
        for (mode, best, median) in &report {
            eprintln!("point SELECT {mode:12}: best {best:7.2} us/call, median {median:7.2} us/call");
        }
        let async_us = report[0].2;
        let sync_us = report[1].2;
        eprintln!("std:sqlite is {:.1}x faster than std:sql (median)", async_us / sync_us);
        assert!(
            sync_us * 2.0 < async_us,
            "std:sqlite {sync_us:.2} us vs std:sql {async_us:.2} us"
        );
    });
}
