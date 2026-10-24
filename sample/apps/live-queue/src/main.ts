// Live electronic queue: people take numbered tickets on their phones and follow their place and ETA
// live; staff call the next N people from a board. Long-polling keeps every page current.
//
// Pages (HTML)                          API (JSON)
//   GET /                landing          GET    /api/queues                       admin: list
//   GET /q/{id}          take a ticket    POST   /api/queues                       admin: open a queue
//   GET /q/{id}/ticket   my ticket        GET    /api/queues/{id}?since=V          public summary (long-poll)
//   GET /q/{id}/screen   TV screen        POST   /api/queues/{id}/tickets          take a ticket
//   GET /admin           admin            GET    /api/queues/{id}/tickets/{n}?since=V   X-Ticket-Token (long-poll)
//   GET /admin/q/{id}    board            DELETE /api/queues/{id}/tickets/{n}      X-Ticket-Token: leave
//                                         GET    /api/queues/{id}/board?since=V    admin (long-poll)
//                                         POST   /api/queues/{id}/call-next        admin {count, counter}
//                                         POST   /api/queues/{id}/pause | /resume  admin
//                                         POST   /api/queues/{id}/tickets/{n}/done | /skip   admin

import { sql } from "std:sql";
import { kv } from "std:kv";
import { error, errorText, json, randomToken, readJson, requireAdmin, safeEqual, sha256Hex } from "./http.ts";
import { throttle, throttleLimit } from "./throttle.ts";
import {
  type CallSample,
  ETA_WINDOW_MS,
  etaSeconds,
  newQueueId,
  QUEUE_ID,
  secondsPerPerson,
  TICKET_TOKEN,
  validateCall,
  validateQueue,
  validateTicket,
} from "./queue.ts";
import { adminPage, boardPage, csp, landingPage, newNonce, type PageConfig, sanitizeBase, screenPage, takePage, ticketPage } from "./pages.ts";

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  deadline: number;
  client?: { ip: string }; // HTTP requests only: the real client address
}
interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
}
interface SqlTx {
  execute(sql: string, args?: unknown[]): Promise<SqlResult>;
}

const POLL_INTERVAL_MS = 1000;
const MAX_BODY = 4096;
const DAY_MS = 86_400_000;
// Tickets per minute from one address. Generous: people at the door often share the venue's Wi-Fi.
const THROTTLE_DEFAULT = 30;

const env = (name: string) => Deno.env.get(name) || undefined;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, Math.max(0, ms)));

// ---------------------------------------------------------------------------------- SQL

function objects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}

let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const found = await sql.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'calls_queue'") as SqlResult;
  if (found.rows.length === 0) {
    await sql.batch([
      // `version` grows with every change of the queue or its tickets: long-polls wait for it.
      `CREATE TABLE IF NOT EXISTS queues (
         id TEXT PRIMARY KEY, name TEXT NOT NULL,
         status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'paused')),
         next_number INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL DEFAULT 1,
         seconds_per_person REAL NOT NULL, max_waiting INTEGER NOT NULL,
         created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS tickets (
         queue_id TEXT NOT NULL, number INTEGER NOT NULL, token_hash TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
         status TEXT NOT NULL DEFAULT 'waiting' CHECK (status IN ('waiting', 'called', 'done', 'skipped', 'cancelled')),
         counter TEXT, created_at INTEGER NOT NULL, called_at INTEGER, finished_at INTEGER,
         PRIMARY KEY (queue_id, number))`,
      `CREATE INDEX IF NOT EXISTS tickets_status ON tickets (queue_id, status, number)`,
      `CREATE TABLE IF NOT EXISTS calls (
         id INTEGER PRIMARY KEY AUTOINCREMENT, queue_id TEXT NOT NULL, at INTEGER NOT NULL,
         admitted INTEGER NOT NULL, counter TEXT NOT NULL)`,
      `CREATE INDEX IF NOT EXISTS calls_queue ON calls (queue_id, at)`,
    ], "write");
  }
  schemaReady = true;
}

const bump = (qid: string, now: number) => ({ sql: "UPDATE queues SET version = version + 1, updated_at = ? WHERE id = ?", args: [now, qid] });

// ---------------------------------------------------------------------------------- views

interface Want {
  ticket?: number; // also load this ticket and its position
  board?: boolean; // also load the lists for staff
}

// Everything for one view in one read batch: a consistent snapshot, one round trip.
async function loadView(qid: string, want: Want = {}) {
  const now = Date.now();
  const stmts: { sql: string; args: unknown[] }[] = [
    { sql: "SELECT * FROM queues WHERE id = ?", args: [qid] },
    { sql: "SELECT COUNT(*) AS n FROM tickets WHERE queue_id = ? AND status = 'waiting'", args: [qid] },
    {
      sql: `SELECT number, name, counter, called_at FROM tickets WHERE queue_id = ? AND status = 'called'
            ORDER BY called_at DESC, number DESC LIMIT 50`,
      args: [qid],
    },
    { sql: "SELECT at, admitted FROM calls WHERE queue_id = ? AND at >= ? ORDER BY at", args: [qid, now - ETA_WINDOW_MS] },
  ];
  if (want.ticket !== undefined) {
    stmts.push({ sql: "SELECT * FROM tickets WHERE queue_id = ? AND number = ?", args: [qid, want.ticket] });
    stmts.push({
      sql: "SELECT COUNT(*) AS n FROM tickets WHERE queue_id = ? AND status = 'waiting' AND number < ?",
      args: [qid, want.ticket],
    });
  }
  if (want.board) {
    stmts.push({
      sql: "SELECT number, name, created_at FROM tickets WHERE queue_id = ? AND status = 'waiting' ORDER BY number LIMIT 300",
      args: [qid],
    });
    stmts.push({
      sql: `SELECT COALESCE(SUM(status = 'done'), 0) AS served, COALESCE(SUM(status = 'skipped'), 0) AS skipped
            FROM tickets WHERE queue_id = ? AND finished_at >= ?`,
      args: [qid, now - DAY_MS],
    });
  }
  const res = (await sql.batch(stmts, "read") as SqlResult[]).map(objects);
  const q = res[0][0];
  if (!q) return null;

  const waiting = Number(res[1][0].n);
  const called = res[2];
  const calls: CallSample[] = res[3].map((c) => ({ at: Number(c.at), admitted: Number(c.admitted) }));
  const spp = secondsPerPerson(calls, now, Number(q.seconds_per_person));
  const lastCall = calls.length ? calls[calls.length - 1].at : null;
  const sinceLast = lastCall === null ? null : (now - lastCall) / 1000;

  const queue = {
    id: String(q.id),
    name: String(q.name),
    status: String(q.status),
    version: Number(q.version),
    waiting,
    nowServing: called.slice(0, 12).map((t) => ({ number: Number(t.number), counter: String(t.counter), calledAt: Number(t.called_at) })),
    secondsPerPerson: Math.round(spp),
    etaForNewTicket: etaSeconds(waiting, spp, sinceLast),
    updatedAt: Number(q.updated_at),
  };

  let i = 4;
  let ticket = null;
  if (want.ticket !== undefined) {
    const t = res[i++][0];
    const ahead = Number(res[i++][0].n);
    if (t) {
      ticket = {
        tokenHash: String(t.token_hash),
        view: {
          queue,
          ticket: {
            number: Number(t.number),
            status: String(t.status),
            counter: t.counter === null ? null : String(t.counter),
            createdAt: Number(t.created_at),
            calledAt: t.called_at === null ? null : Number(t.called_at),
          },
          ahead: t.status === "waiting" ? ahead : 0,
          etaSeconds: t.status === "waiting" ? etaSeconds(ahead, spp, sinceLast) : null,
        },
      };
    }
  }

  let board = null;
  if (want.board) {
    const list = res[i++];
    const stats = res[i++][0];
    board = {
      queue,
      called: called.map((t) => ({ number: Number(t.number), name: String(t.name), counter: String(t.counter), calledAt: Number(t.called_at) })),
      waitingList: list.map((t) => ({ number: Number(t.number), name: String(t.name), createdAt: Number(t.created_at) })),
      stats: { served: Number(stats.served), skipped: Number(stats.skipped) },
    };
  }
  return { queue, ticket, board };
}

// Long-poll: return once the queue's version differs from `since`, or after LONGPOLL_SECONDS
// (default 20 s), always well inside the task's wall limit (ctx.deadline). One cheap indexed read
// per second; the clock advances across the awaited timers.
async function waitForChange(qid: string, since: number, ctx: Ctx): Promise<void> {
  if (since <= 0) return;
  const configured = Number(env("LONGPOLL_SECONDS") ?? "20");
  const budget = Number.isFinite(configured) ? Math.max(0, Math.min(25, configured)) * 1000 : 20_000;
  const until = Math.min(Date.now() + budget, ctx.deadline - 3000);
  while (Date.now() < until) {
    const r = await sql.execute("SELECT version FROM queues WHERE id = ?", [qid]) as SqlResult;
    if (r.rows.length === 0 || Number(r.rows[0][0]) !== since) return;
    await sleep(Math.min(POLL_INTERVAL_MS, until - Date.now()));
  }
}

function sinceParam(url: URL): number {
  const v = Number(url.searchParams.get("since") ?? "0");
  return Number.isSafeInteger(v) && v > 0 ? v : 0;
}

// ---------------------------------------------------------------------------------- handlers

async function listQueues(): Promise<Response> {
  const r = await sql.execute(
    `SELECT q.id, q.name, q.status, q.created_at,
            (SELECT COUNT(*) FROM tickets t WHERE t.queue_id = q.id AND t.status = 'waiting') AS waiting
     FROM queues q ORDER BY q.created_at DESC LIMIT 100`,
  ) as SqlResult;
  return json({
    queues: objects(r).map((q) => ({ id: q.id, name: q.name, status: q.status, waiting: Number(q.waiting), createdAt: Number(q.created_at) })),
  });
}

async function createQueue(req: Request, base: string): Promise<Response> {
  const body = await readJson(req, MAX_BODY);
  if (!body.ok) return body.response;
  const v = validateQueue(body.value);
  if (!v.ok) return error(400, "validation_failed", v.error);
  const now = Date.now();
  let id = "";
  for (let attempt = 0; attempt < 3 && !id; attempt++) {
    const candidate = newQueueId();
    const r = await sql.execute(
      `INSERT OR IGNORE INTO queues (id, name, seconds_per_person, max_waiting, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [candidate, v.value.name, v.value.minutesPerPerson * 60, v.value.maxWaiting, now, now],
    ) as SqlResult;
    if (r.rowsAffected === 1) id = candidate;
  }
  if (!id) return error(500, "internal", "could not allocate a queue id");
  const view = await loadView(id);
  return json({
    queue: view!.queue,
    links: { take: `${base}/q/${id}`, screen: `${base}/q/${id}/screen`, board: `${base}/admin/q/${id}` },
  }, 201);
}

async function summary(qid: string, url: URL, ctx: Ctx): Promise<Response> {
  await waitForChange(qid, sinceParam(url), ctx);
  const v = await loadView(qid);
  return v ? json(v.queue) : error(404, "not_found", "no such queue");
}

type TakeOutcome = { number: number } | { status: number; code: string; message: string };

async function takeTicket(qid: string, req: Request): Promise<Response> {
  const body = await readJson(req, MAX_BODY, true);
  if (!body.ok) return body.response;
  const v = validateTicket(body.value);
  if (!v.ok) return error(400, "validation_failed", v.error);
  const token = randomToken("lqt_", 24);
  const tokenHash = await sha256Hex(token);

  // Numbering under concurrency: read and advance next_number and insert the ticket in one
  // transaction. SQLite has one writer, so two people can never get the same number.
  const out: TakeOutcome = await sql.transaction(async (tx: SqlTx) => {
    const q = objects(await tx.execute("SELECT status, next_number, max_waiting FROM queues WHERE id = ?", [qid]))[0];
    if (!q) return { status: 404, code: "not_found", message: "no such queue" };
    if (q.status !== "open") return { status: 409, code: "queue_paused", message: "the queue is not taking new tickets" };
    const waiting = objects(await tx.execute("SELECT COUNT(*) AS n FROM tickets WHERE queue_id = ? AND status = 'waiting'", [qid]))[0];
    if (Number(waiting.n) >= Number(q.max_waiting)) return { status: 409, code: "queue_full", message: "the queue is full" };
    const number = Number(q.next_number);
    const now = Date.now();
    await tx.execute("UPDATE queues SET next_number = next_number + 1, version = version + 1, updated_at = ? WHERE id = ?", [now, qid]);
    await tx.execute(
      "INSERT INTO tickets (queue_id, number, token_hash, name, created_at) VALUES (?, ?, ?, ?, ?)",
      [qid, number, tokenHash, v.value.name, now],
    );
    return { number };
  });
  if ("status" in out) return error(out.status, out.code, out.message);
  const view = await loadView(qid, { ticket: out.number });
  return json({ token, ...view!.ticket!.view }, 201);
}

// The ticket token proves ownership; it is compared by hash in constant time.
async function ownTicket(qid: string, n: number, req: Request): Promise<Response | { view: unknown }> {
  const token = req.headers.get("x-ticket-token") ?? "";
  if (!TICKET_TOKEN.test(token)) return error(401, "invalid_token", "send the ticket token in X-Ticket-Token");
  const v = await loadView(qid, { ticket: n });
  if (!v || !v.ticket || !(await safeEqual(await sha256Hex(token), v.ticket.tokenHash))) {
    return error(404, "not_found", "no such ticket");
  }
  return { view: v.ticket.view };
}

// Ownership is checked before waiting, so a guessed token cannot hold a long-poll open.
async function ticketStatus(qid: string, n: number, req: Request, url: URL, ctx: Ctx): Promise<Response> {
  const first = await ownTicket(qid, n, req);
  if (first instanceof Response) return first;
  const since = sinceParam(url);
  if (since === 0) return json(first.view);
  await waitForChange(qid, since, ctx);
  const r = await ownTicket(qid, n, req);
  return r instanceof Response ? r : json(r.view);
}

async function leave(qid: string, n: number, req: Request): Promise<Response> {
  const r = await ownTicket(qid, n, req);
  if (r instanceof Response) return r;
  const now = Date.now();
  await sql.batch([
    {
      sql: `UPDATE tickets SET status = 'cancelled', finished_at = ?
            WHERE queue_id = ? AND number = ? AND status IN ('waiting', 'called')`,
      args: [now, qid, n],
    },
    bump(qid, now),
  ], "write");
  const after = await ownTicket(qid, n, req);
  return after instanceof Response ? after : json(after.view);
}

async function boardView(qid: string, extra: Record<string, unknown> = {}): Promise<Response> {
  const v = await loadView(qid, { board: true });
  return v ? json({ ...v.board, ...extra }) : error(404, "not_found", "no such queue");
}

async function board(qid: string, url: URL, ctx: Ctx): Promise<Response> {
  await waitForChange(qid, sinceParam(url), ctx);
  return await boardView(qid);
}

// Admits the next `count` waiting people at `counter`. Whoever this counter called before is done.
async function callNext(qid: string, req: Request): Promise<Response> {
  const body = await readJson(req, MAX_BODY, true);
  if (!body.ok) return body.response;
  const v = validateCall(body.value);
  if (!v.ok) return error(400, "validation_failed", v.error);
  const { count, counter } = v.value;
  const called: number[] | null = await sql.transaction(async (tx: SqlTx) => {
    const now = Date.now();
    const q = objects(await tx.execute("SELECT id FROM queues WHERE id = ?", [qid]))[0];
    if (!q) return null;
    await tx.execute(
      "UPDATE tickets SET status = 'done', finished_at = ? WHERE queue_id = ? AND status = 'called' AND counter = ?",
      [now, qid, counter],
    );
    const next = objects(await tx.execute(
      "SELECT number FROM tickets WHERE queue_id = ? AND status = 'waiting' ORDER BY number LIMIT ?",
      [qid, count],
    )).map((t) => Number(t.number));
    for (const n of next) {
      await tx.execute(
        "UPDATE tickets SET status = 'called', called_at = ?, counter = ? WHERE queue_id = ? AND number = ?",
        [now, counter, qid, n],
      );
    }
    if (next.length) {
      await tx.execute("INSERT INTO calls (queue_id, at, admitted, counter) VALUES (?, ?, ?, ?)", [qid, now, next.length, counter]);
    }
    await tx.execute("UPDATE queues SET version = version + 1, updated_at = ? WHERE id = ?", [now, qid]);
    return next;
  });
  if (called === null) return error(404, "not_found", "no such queue");
  return await boardView(qid, { calledNow: called });
}

async function setQueueStatus(qid: string, status: "open" | "paused"): Promise<Response> {
  const r = await sql.execute(
    "UPDATE queues SET status = ?, version = version + 1, updated_at = ? WHERE id = ?",
    [status, Date.now(), qid],
  ) as SqlResult;
  return r.rowsAffected ? await boardView(qid) : error(404, "not_found", "no such queue");
}

async function finishTicket(qid: string, n: number, action: "done" | "skip"): Promise<Response> {
  const now = Date.now();
  const allowed = action === "done" ? "('called')" : "('waiting', 'called')";
  const [r] = await sql.batch([
    {
      sql: `UPDATE tickets SET status = ?, finished_at = ? WHERE queue_id = ? AND number = ? AND status IN ${allowed}`,
      args: [action === "done" ? "done" : "skipped", now, qid, n],
    },
    bump(qid, now),
  ], "write") as SqlResult[];
  if (r.rowsAffected === 0) return error(409, "invalid_state", action === "done" ? "the ticket is not called" : "the ticket is not waiting or called");
  return await boardView(qid);
}

// ---------------------------------------------------------------------------------- routing

function html(body: string, nonce: string): Response {
  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "content-security-policy": csp(nonce),
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
      "cache-control": "no-store",
    },
  });
}

function page(path: string, base: string): Response | null {
  const nonce = newNonce();
  if (path === "/") return html(landingPage(nonce, { base }), nonce);
  if (path === "/admin") return html(adminPage(nonce, { base }), nonce);
  let m = /^\/admin\/q\/([a-z0-9]{10})$/.exec(path);
  if (m) return html(boardPage(nonce, { base, qid: m[1] }), nonce);
  m = /^\/q\/([a-z0-9]{10})(\/ticket|\/screen)?$/.exec(path);
  if (!m) return null;
  const cfg: PageConfig = { base, qid: m[1] };
  if (m[2] === "/ticket") return html(ticketPage(nonce, cfg), nonce);
  if (m[2] === "/screen") return html(screenPage(nonce, cfg), nonce);
  return html(takePage(nonce, cfg), nonce);
}

async function api(req: Request, url: URL, path: string, base: string, ctx: Ctx): Promise<Response> {
  const method = req.method;
  const isAdmin = () => requireAdmin(req, env("ADMIN_TOKEN"));

  if (path === "/api/queues") {
    const denied = await isAdmin();
    if (denied) return denied;
    if (method === "GET") return await listQueues();
    if (method === "POST") return await createQueue(req, base);
    return error(405, "method_not_allowed", "GET or POST");
  }

  const m = /^\/api\/queues\/([^/]+)(?:\/(.*))?$/.exec(path);
  if (!m) return error(404, "not_found", "no such route");
  const qid = m[1];
  const rest = m[2] ?? "";
  if (!QUEUE_ID.test(qid)) return error(404, "not_found", "no such queue");

  if (rest === "" && method === "GET") return await summary(qid, url, ctx);
  if (rest === "tickets" && method === "POST") {
    const limit = throttleLimit(env("THROTTLE_PER_MINUTE"), THROTTLE_DEFAULT);
    const limited = await throttle((k, d, o) => kv.incr(k, d, o), "ticket", ctx.client?.ip, limit);
    return limited ?? await takeTicket(qid, req);
  }
  const t = /^tickets\/(\d{1,9})$/.exec(rest);
  if (t && method === "GET") return await ticketStatus(qid, Number(t[1]), req, url, ctx);
  if (t && method === "DELETE") return await leave(qid, Number(t[1]), req);

  // Everything else is for staff.
  const denied = await isAdmin();
  if (denied) return denied;
  if (rest === "board" && method === "GET") return await board(qid, url, ctx);
  if (method !== "POST") return error(405, "method_not_allowed", "use POST");
  if (rest === "call-next") return await callNext(qid, req);
  if (rest === "pause") return await setQueueStatus(qid, "paused");
  if (rest === "resume") return await setQueueStatus(qid, "open");
  const a = /^tickets\/(\d{1,9})\/(done|skip)$/.exec(rest);
  if (a) return await finishTicket(qid, Number(a[1]), a[2] as "done" | "skip");
  return error(404, "not_found", "no such route");
}

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname.replace(/\/+$/, "") || "/";
  const base = sanitizeBase(req.headers.get("x-rt-base-path"));
  try {
    if (path.startsWith("/api/")) {
      await ensureSchema();
      return await api(req, url, path, base, ctx);
    }
    if (req.method !== "GET" && req.method !== "HEAD") return error(405, "method_not_allowed", "pages are GET only");
    return page(path, base) ?? error(404, "not_found", "no such page");
  } catch (e) {
    console.error(errorText(e));
    return error(500, "internal", "internal error");
  }
}
