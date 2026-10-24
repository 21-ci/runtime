// Events backend: an admin publishes events; anyone lists them and registers with a name plus an
// email or a Telegram username; a registrant cancels with the token returned at registration.
//
//   GET    /events                              upcoming events (?past=1 for all)
//   GET    /events/{id}
//   POST   /events/{id}/registrations           register (optional Idempotency-Key header)
//   GET    /registration                        own registration   (X-Registration-Token)
//   DELETE /registration                        cancel it          (X-Registration-Token)
//   POST   /events                              admin: create
//   PATCH  /events/{id}                         admin: update
//   POST   /events/{id}/close | /reopen         admin: stop / resume registrations
//   GET    /events/{id}/registrations           admin: JSON, paged (?status=&limit=&after=)
//   GET    /events/{id}/registrations.csv       admin: CSV export, streamed

import { sql } from "std:sql";
import { kv } from "std:kv";
import { services } from "std:services";
import { csvLine, UTF8_BOM } from "./csv.ts";
import { error, errorText, json, randomToken, readJson, requireAdmin, sha256Hex } from "./http.ts";
import { throttle, throttleLimit } from "./throttle.ts";
import {
  isValidIdempotencyKey,
  isValidRegistrationToken,
  type RegistrationInput,
  validateEventCreate,
  validateEventPatch,
  validateRegistration,
} from "./validate.ts";

interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
  lastInsertRowid?: number | bigint | null;
}
interface SqlTx {
  execute(sql: string, args?: unknown[]): Promise<SqlResult>;
}
type Exec = (text: string, args?: unknown[]) => Promise<SqlResult>;
interface Ctx {
  client?: { ip: string }; // HTTP requests only: the real client address
}

const MAX_BODY = 16 * 1024;
const IDEMPOTENCY_TTL = 86_400;
const THROTTLE_DEFAULT = 20; // registrations per minute from one address

const env = (name: string) => Deno.env.get(name) || undefined;
const nowSec = () => Math.floor(Date.now() / 1000);
const iso = (sec: unknown) => (sec === null || sec === undefined ? null : new Date(Number(sec) * 1000).toISOString());

// ---------------------------------------------------------------------------------- SQL

function objects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}
const execSql: Exec = (text, args) => sql.execute(text, args) as Promise<SqlResult>;
async function one(exec: Exec, text: string, args: unknown[] = []) {
  return objects(await exec(text, args))[0] ?? null;
}

let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const found = await execSql("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'events_starts'");
  if (found.rows.length === 0) {
    await sql.batch([
      `CREATE TABLE IF NOT EXISTS events (
         id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
         location TEXT NOT NULL DEFAULT '', starts_at INTEGER NOT NULL, capacity INTEGER NOT NULL,
         status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
         created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS registrations (
         id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES events (id),
         name TEXT NOT NULL, email TEXT, telegram TEXT, token_hash TEXT NOT NULL UNIQUE,
         status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'cancelled')),
         created_at INTEGER NOT NULL, cancelled_at INTEGER)`,
      // One active registration per contact and event; a cancelled one does not block a new one.
      `CREATE UNIQUE INDEX IF NOT EXISTS reg_email ON registrations (event_id, email)
         WHERE status = 'confirmed' AND email IS NOT NULL`,
      `CREATE UNIQUE INDEX IF NOT EXISTS reg_telegram ON registrations (event_id, telegram)
         WHERE status = 'confirmed' AND telegram IS NOT NULL`,
      `CREATE INDEX IF NOT EXISTS reg_event ON registrations (event_id, status, id)`,
      `CREATE INDEX IF NOT EXISTS events_starts ON events (starts_at)`,
    ], "write");
  }
  schemaReady = true;
}

const EVENT_SELECT = `SELECT e.*, (SELECT COUNT(*) FROM registrations r
                                   WHERE r.event_id = e.id AND r.status = 'confirmed') AS registered
                      FROM events e`;

function eventView(o: Record<string, unknown>) {
  const startsAt = Number(o.starts_at);
  const capacity = Number(o.capacity);
  const registered = Number(o.registered ?? 0);
  return {
    id: Number(o.id),
    title: String(o.title),
    description: String(o.description),
    location: String(o.location),
    startsAt: iso(startsAt),
    capacity,
    registered,
    spotsLeft: Math.max(0, capacity - registered),
    status: String(o.status),
    registrationOpen: o.status === "open" && startsAt > nowSec() && registered < capacity,
    createdAt: iso(o.created_at),
    updatedAt: iso(o.updated_at),
  };
}

function registrationView(o: Record<string, unknown>) {
  return {
    id: Number(o.id),
    eventId: Number(o.event_id),
    name: String(o.name),
    email: o.email ?? null,
    telegram: o.telegram ? `@${o.telegram}` : null,
    status: String(o.status),
    createdAt: iso(o.created_at),
    cancelledAt: iso(o.cancelled_at),
  };
}

async function findEvent(id: number) {
  return await one(execSql, `${EVENT_SELECT} WHERE e.id = ?`, [id]);
}

// ---------------------------------------------------------------------------------- public

async function listEvents(url: URL): Promise<Response> {
  const all = url.searchParams.get("past") === "1";
  const r = await execSql(
    `${EVENT_SELECT} WHERE ? OR e.starts_at > ? ORDER BY e.starts_at LIMIT 200`,
    [all ? 1 : 0, nowSec()],
  );
  return json({ events: objects(r).map(eventView) });
}

async function getEvent(id: number): Promise<Response> {
  const ev = await findEvent(id);
  return ev ? json(eventView(ev)) : error(404, "not_found", "no such event");
}

type RegisterOutcome = { status: 201; id: number } | { status: 404 | 409; code: string; message: string };

async function insertRegistration(eventId: number, input: RegistrationInput, tokenHash: string): Promise<RegisterOutcome> {
  // One transaction: the capacity check and the insert cannot interleave with another registration.
  return await sql.transaction(async (tx: SqlTx) => {
    const exec: Exec = (t, a) => tx.execute(t, a);
    const ev = await one(exec, "SELECT status, capacity, starts_at FROM events WHERE id = ?", [eventId]);
    if (!ev) return { status: 404, code: "not_found", message: "no such event" };
    if (ev.status !== "open") return { status: 409, code: "event_closed", message: "registration is closed" };
    if (Number(ev.starts_at) <= nowSec()) return { status: 409, code: "event_started", message: "the event has started" };
    const dup = await one(
      exec,
      "SELECT 1 AS x FROM registrations WHERE event_id = ? AND status = 'confirmed' AND (email = ? OR telegram = ?)",
      [eventId, input.email, input.telegram],
    );
    if (dup) return { status: 409, code: "already_registered", message: "this contact is already registered" };
    const taken = await one(exec, "SELECT COUNT(*) AS n FROM registrations WHERE event_id = ? AND status = 'confirmed'", [eventId]);
    if (Number(taken?.n) >= Number(ev.capacity)) return { status: 409, code: "event_full", message: "no spots left" };
    const r = await exec(
      "INSERT INTO registrations (event_id, name, email, telegram, token_hash, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      [eventId, input.name, input.email, input.telegram, tokenHash, nowSec()],
    );
    return { status: 201, id: Number(r.lastInsertRowid) };
  });
}

// The notifier service (sample/apps/events/notify), called over RPC when NOTIFY_SERVICE names it.
// Best effort: the registration stands when the notifier is missing or fails, and the failure is logged.
interface Notifier {
  confirmRegistration(args: { eventId: number; registrationId: number; name: string }): Promise<{ receipt: string }>;
}
async function notifyRegistration(eventId: number, registrationId: number, name: string): Promise<string | null> {
  const target = env("NOTIFY_SERVICE");
  if (!target) return null;
  try {
    const out = await (services.bind(target) as unknown as Notifier).confirmRegistration({ eventId, registrationId, name });
    return out.receipt;
  } catch (e) {
    console.error(`confirmation not sent: ${(e as { code?: string }).code ?? "error"}`);
    return null;
  }
}

async function register(eventId: number, req: Request): Promise<Response> {
  const idem = req.headers.get("idempotency-key");
  if (idem !== null && !isValidIdempotencyKey(idem)) return error(400, "invalid_idempotency_key", "8-128 characters of A-Z a-z 0-9 _ -");

  const body = await readJson(req, MAX_BODY);
  if (!body.ok) return body.response;
  const input = validateRegistration(body.value);
  if (!input.ok) return error(400, "validation_failed", input.error);

  // A retried request with the same Idempotency-Key gets the first answer (token included) for 24 h.
  const fingerprint = await sha256Hex(JSON.stringify([eventId, input.value]));
  const idemKey = idem ? `idem/${await sha256Hex(idem)}` : null;
  if (idemKey) {
    const prev = await kv.get(idemKey) as { fingerprint: string; status: number; body: unknown } | null;
    if (prev) {
      if (prev.fingerprint !== fingerprint) return error(422, "idempotency_mismatch", "this key was used for another request");
      return json(prev.body, prev.status, { "idempotent-replayed": "true" });
    }
  }

  const token = randomToken("evr_");
  const out = await insertRegistration(eventId, input.value, await sha256Hex(token));
  if (out.status !== 201) return error(out.status, out.code, out.message);

  const reg = await one(execSql, "SELECT * FROM registrations WHERE id = ?", [out.id]);
  const receipt = await notifyRegistration(eventId, out.id, input.value.name);
  const result = { registration: registrationView(reg!), token, receipt };
  if (idemKey) await kv.set(idemKey, { fingerprint, status: 201, body: result }, { ttlSeconds: IDEMPOTENCY_TTL });
  return json(result, 201);
}

async function tokenHashOf(req: Request): Promise<string | null> {
  const t = req.headers.get("x-registration-token") ?? "";
  return isValidRegistrationToken(t) ? await sha256Hex(t) : null;
}

async function myRegistration(req: Request): Promise<Response> {
  const hash = await tokenHashOf(req);
  if (!hash) return error(401, "invalid_token", "send the token from registration in X-Registration-Token");
  const reg = await one(execSql, "SELECT * FROM registrations WHERE token_hash = ?", [hash]);
  if (!reg) return error(404, "not_found", "no registration for this token");
  const ev = await findEvent(Number(reg.event_id));
  return json({ registration: registrationView(reg), event: ev ? eventView(ev) : null });
}

async function cancelRegistration(req: Request): Promise<Response> {
  const hash = await tokenHashOf(req);
  if (!hash) return error(401, "invalid_token", "send the token from registration in X-Registration-Token");
  const out = await sql.transaction(async (tx: SqlTx) => {
    const exec: Exec = (t, a) => tx.execute(t, a);
    const reg = await one(
      exec,
      "SELECT r.id, r.status, e.starts_at FROM registrations r JOIN events e ON e.id = r.event_id WHERE r.token_hash = ?",
      [hash],
    );
    if (!reg) return { status: 404, code: "not_found" };
    if (reg.status === "cancelled") return { status: 200, id: Number(reg.id) }; // idempotent
    if (Number(reg.starts_at) <= nowSec()) return { status: 409, code: "event_started" };
    await exec("UPDATE registrations SET status = 'cancelled', cancelled_at = ? WHERE id = ?", [nowSec(), reg.id]);
    return { status: 200, id: Number(reg.id) };
  });
  if (out.status === 404) return error(404, "not_found", "no registration for this token");
  if (out.status === 409) return error(409, "event_started", "the event has started; it cannot be cancelled now");
  const reg = await one(execSql, "SELECT * FROM registrations WHERE id = ?", [out.id]);
  return json({ registration: registrationView(reg!) });
}

// ---------------------------------------------------------------------------------- admin

async function createEvent(req: Request): Promise<Response> {
  const body = await readJson(req, MAX_BODY);
  if (!body.ok) return body.response;
  const v = validateEventCreate(body.value, nowSec());
  if (!v.ok) return error(400, "validation_failed", v.error);
  const e = v.value;
  const now = nowSec();
  const r = await execSql(
    `INSERT INTO events (title, description, location, starts_at, capacity, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [e.title, e.description, e.location, e.startsAt, e.capacity, now, now],
  );
  return json(eventView((await findEvent(Number(r.lastInsertRowid)))!), 201);
}

async function patchEvent(id: number, req: Request): Promise<Response> {
  const body = await readJson(req, MAX_BODY);
  if (!body.ok) return body.response;
  const v = validateEventPatch(body.value, nowSec());
  if (!v.ok) return error(400, "validation_failed", v.error);
  const p = v.value;
  const out = await sql.transaction(async (tx: SqlTx) => {
    const exec: Exec = (t, a) => tx.execute(t, a);
    const ev = await one(exec, `${EVENT_SELECT} WHERE e.id = ?`, [id]);
    if (!ev) return 404;
    if (p.capacity !== undefined && p.capacity < Number(ev.registered)) return 409;
    await exec(
      `UPDATE events SET title = COALESCE(?, title), description = COALESCE(?, description),
         location = COALESCE(?, location), starts_at = COALESCE(?, starts_at),
         capacity = COALESCE(?, capacity), updated_at = ? WHERE id = ?`,
      [p.title, p.description, p.location, p.startsAt, p.capacity, nowSec(), id],
    );
    return 200;
  });
  if (out === 404) return error(404, "not_found", "no such event");
  if (out === 409) return error(409, "capacity_below_registered", "capacity cannot go below the number of registrations");
  return json(eventView((await findEvent(id))!));
}

async function setStatus(id: number, action: string): Promise<Response> {
  const status = action === "close" ? "closed" : "open";
  const r = await execSql("UPDATE events SET status = ?, updated_at = ? WHERE id = ?", [status, nowSec(), id]);
  if (r.rowsAffected === 0) return error(404, "not_found", "no such event");
  return json(eventView((await findEvent(id))!));
}

const REG_STATUS = new Set(["confirmed", "cancelled", "all"]);

async function listRegistrations(id: number, url: URL): Promise<Response> {
  const ev = await findEvent(id);
  if (!ev) return error(404, "not_found", "no such event");
  const status = url.searchParams.get("status") ?? "all";
  const limit = Number(url.searchParams.get("limit") ?? "500");
  const after = Number(url.searchParams.get("after") ?? "0");
  if (!REG_STATUS.has(status)) return error(400, "invalid_status", "status is confirmed, cancelled or all");
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) return error(400, "invalid_limit", "limit is 1-1000");
  if (!Number.isInteger(after) || after < 0) return error(400, "invalid_after", "after is a registration id");
  const r = await execSql(
    "SELECT * FROM registrations WHERE event_id = ? AND (? = 'all' OR status = ?) AND id > ? ORDER BY id LIMIT ?",
    [id, status, status, after, limit],
  );
  const regs = objects(r).map(registrationView);
  const next = regs.length === limit ? regs[regs.length - 1].id : null;
  return json({ event: eventView(ev), registrations: regs, next });
}

// Streamed in pages of 1,000 rows: the export of a 100,000-seat event never sits in memory.
async function exportCsv(id: number): Promise<Response> {
  const ev = await findEvent(id);
  if (!ev) return error(404, "not_found", "no such event");
  const enc = new TextEncoder();
  let after = 0;
  let started = false;
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (!started) {
        started = true;
        controller.enqueue(enc.encode(UTF8_BOM + csvLine(["id", "name", "email", "telegram", "status", "registered_at", "cancelled_at"])));
        return;
      }
      try {
        const page = objects(await execSql(
          "SELECT * FROM registrations WHERE event_id = ? AND id > ? ORDER BY id LIMIT 1000",
          [id, after],
        ));
        if (page.length === 0) return controller.close();
        after = Number(page[page.length - 1].id);
        // telegram without the "@": a cell starting with "@" would get the formula guard's quote.
        controller.enqueue(enc.encode(page.map((o) => {
          const v = registrationView(o);
          return csvLine([v.id, v.name, v.email, o.telegram ?? null, v.status, v.createdAt, v.cancelledAt]);
        }).join("")));
      } catch (e) {
        console.error("csv export:", errorText(e));
        controller.error(e);
      }
    },
  });
  return new Response(stream, {
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="event-${id}-registrations.csv"`,
      "cache-control": "no-store",
    },
  });
}

// ---------------------------------------------------------------------------------- routing

type Route = {
  method: string;
  re: RegExp;
  admin: boolean;
  throttle?: string; // per-address throttle scope (public writes)
  run: (m: RegExpExecArray, req: Request, url: URL) => Promise<Response>;
};

const ROUTES: Route[] = [
  { method: "GET", re: /^\/events$/, admin: false, run: (_m, _r, u) => listEvents(u) },
  { method: "POST", re: /^\/events$/, admin: true, run: (_m, r) => createEvent(r) },
  { method: "GET", re: /^\/events\/(\d{1,12})$/, admin: false, run: (m) => getEvent(Number(m[1])) },
  { method: "PATCH", re: /^\/events\/(\d{1,12})$/, admin: true, run: (m, r) => patchEvent(Number(m[1]), r) },
  { method: "POST", re: /^\/events\/(\d{1,12})\/(close|reopen)$/, admin: true, run: (m) => setStatus(Number(m[1]), m[2]) },
  { method: "POST", re: /^\/events\/(\d{1,12})\/registrations$/, admin: false, throttle: "register", run: (m, r) => register(Number(m[1]), r) },
  { method: "GET", re: /^\/events\/(\d{1,12})\/registrations$/, admin: true, run: (m, _r, u) => listRegistrations(Number(m[1]), u) },
  { method: "GET", re: /^\/events\/(\d{1,12})\/registrations\.csv$/, admin: true, run: (m) => exportCsv(Number(m[1])) },
  { method: "GET", re: /^\/registration$/, admin: false, run: (_m, r) => myRegistration(r) },
  { method: "DELETE", re: /^\/registration$/, admin: false, run: (_m, r) => cancelRegistration(r) },
];

// Optional CORS for a frontend on another origin (CORS_ORIGIN=https://events.example.com or *).
function withCors(res: Response): Response {
  const origin = env("CORS_ORIGIN");
  if (!origin) return res;
  const h = new Headers(res.headers);
  h.set("access-control-allow-origin", origin);
  h.set("access-control-allow-headers", "authorization, content-type, idempotency-key, x-registration-token");
  h.set("access-control-allow-methods", "GET, POST, PATCH, DELETE");
  h.set("access-control-expose-headers", "idempotent-replayed, content-disposition, retry-after");
  if (origin !== "*") h.append("vary", "origin");
  return new Response(res.body, { status: res.status, headers: h });
}

async function route(req: Request, ctx: Ctx): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname.replace(/\/+$/, "") || "/";
  if (req.method === "OPTIONS") return new Response(null, { status: 204 });
  if (path === "/") {
    return json({
      service: "events",
      public: ["GET /events", "GET /events/{id}", "POST /events/{id}/registrations", "GET /registration", "DELETE /registration"],
      admin: ["POST /events", "PATCH /events/{id}", "POST /events/{id}/close", "POST /events/{id}/reopen",
        "GET /events/{id}/registrations", "GET /events/{id}/registrations.csv"],
    });
  }
  const matching = ROUTES.filter((r) => r.re.test(path));
  if (matching.length === 0) return error(404, "not_found", "no such route");
  const r = matching.find((x) => x.method === req.method);
  if (!r) return error(405, "method_not_allowed", "method not allowed", { allow: matching.map((x) => x.method).join(", ") });
  if (r.admin) {
    const denied = await requireAdmin(req, env("ADMIN_TOKEN"));
    if (denied) return denied;
  }
  if (r.throttle) {
    const limit = throttleLimit(env("THROTTLE_PER_MINUTE"), THROTTLE_DEFAULT);
    const limited = await throttle((k, d, o) => kv.incr(k, d, o), r.throttle, ctx.client?.ip, limit);
    if (limited) return limited;
  }
  await ensureSchema();
  return await r.run(r.re.exec(path)!, req, url);
}

export default async function (req: Request, ctx: Ctx): Promise<Response> {
  try {
    return withCors(await route(req, ctx));
  } catch (e) {
    console.error(errorText(e));
    return withCors(error(500, "internal", "internal error"));
  }
}
