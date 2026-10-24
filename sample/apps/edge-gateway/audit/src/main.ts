// Audit service behind the edge gateway (slug edge-audit).
//
//   queue      events from the gateway (std:host/queue, dedup id = gateway task id), after the
//              gateway has shipped each one to the operator's audit log itself (std:host/sdk):
//              validated, stored once (INSERT OR IGNORE on the event id), already shipped ones as
//              shipped; the ones the gateway could not ship go out now, in one batch, through the
//              operator SDK: sdk(AUDIT_SDK).audit.ship({ source, events })
//   scheduled  optional interval schedule: ships what is still pending, prunes old rows
//   fetch      GET /recent?limit=50&sub=alice&ip=203.0.113.7, only as a service call from the gateway
//              (the gateway exposes it as GET /v1/audit/recent with the audit:read scope)
//
// The operator SDK (sample/operator-sdk/acme-sdk) runs in the runtime, not in this service: its
// binding holds the credentials and forwards this tenant, and a new SDK release reaches this
// service without a redeploy. Events go to POST /services/audit-log on the mock platform API.
// Without the SDK (not granted, removed, AUDIT_SDK=off), events stay local and pending.

import { sql } from "std:sql";
import { kv } from "std:kv";
import { sdk } from "std:sdk";
import { type AuditEvent, isClientIp, lastGatewayAttempt, parseAuditEvent, type ShipStatus } from "./events.ts";

interface Ctx {
  taskId: string;
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  deadline: number;
}
interface QueueMessage {
  id: string;
  body: unknown;
  attempt: number;
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}
interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
}

const SHIP_BATCH = 200;
const env = (name: string) => Deno.env.get(name) || undefined;

function objects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}

let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const r = await sql.execute(
    "SELECT name FROM sqlite_master WHERE type = 'index' AND name IN ('audit_at', 'audit_ip')",
  ) as SqlResult;
  const found = new Set(r.rows.map((row) => row[0]));
  if (!found.has("audit_at")) {
    await sql.batch([
      `CREATE TABLE IF NOT EXISTS audit (
         id TEXT PRIMARY KEY, at INTEGER NOT NULL, sub TEXT, ip TEXT, method TEXT NOT NULL, path TEXT NOT NULL,
         route TEXT, status INTEGER NOT NULL, outcome TEXT NOT NULL, reason TEXT,
         received_at INTEGER NOT NULL, shipped_at INTEGER)`,
      `CREATE INDEX IF NOT EXISTS audit_pending ON audit (shipped_at, at)`,
      `CREATE INDEX IF NOT EXISTS audit_ip ON audit (ip, at)`,
      `CREATE INDEX IF NOT EXISTS audit_at ON audit (at)`,
    ], "write");
  } else if (!found.has("audit_ip")) {
    // A table from before the gateway sent the client address.
    await sql.batch([
      "ALTER TABLE audit ADD COLUMN ip TEXT",
      "CREATE INDEX IF NOT EXISTS audit_ip ON audit (ip, at)",
    ], "write");
  }
  schemaReady = true;
}

// What the gateway already shipped is stored as shipped, so it is never sent twice.
async function store(events: AuditEvent[]) {
  const now = Date.now();
  await sql.batch(events.map((e) => ({
    sql: `INSERT OR IGNORE INTO audit (id, at, sub, ip, method, path, route, status, outcome, reason, received_at, shipped_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [e.id, e.at, e.sub, e.ip, e.method, e.path, e.route, e.status, e.outcome, e.reason, now, e.shipped ? now : null],
  })), "write");
}

// Ships pending events with the operator SDK's audit.ship. A refusal throws an SdkError with a
// stable code (sdk_not_granted, sdk_unavailable, or the SDK's own, e.g. binding_not_allowed).
// Events stay pending until the operator API accepts them; the result is kept in KV for /recent.
async function ship(): Promise<{ shipped: number; pending: number; error?: string }> {
  const name = env("AUDIT_SDK") ?? "acme";
  if (name === "off") return { shipped: 0, pending: 0, error: "shipping disabled (AUDIT_SDK=off)" };
  const rows = objects(await sql.execute(
    "SELECT id, at, sub, ip, method, path, route, status, outcome, reason FROM audit WHERE shipped_at IS NULL ORDER BY at LIMIT ?",
    [SHIP_BATCH],
  ) as SqlResult);
  if (rows.length === 0) return { shipped: 0, pending: 0 };

  let error: string | undefined;
  let release: string | undefined;
  try {
    const receipt = await (sdk(name) as any).audit.ship({ source: "edge-gateway", events: rows }) as { sdk?: string };
    release = receipt?.sdk;
    await sql.execute(
      `UPDATE audit SET shipped_at = ? WHERE id IN (${rows.map(() => "?").join(",")})`,
      [Date.now(), ...rows.map((r) => r.id)],
    );
  } catch (e) {
    error = `${name}: ${(e as { code?: string }).code ?? (e as Error).message}`;
  }
  await kv.set("ship", { at: Date.now(), ok: !error, error: error ?? null, by: "edge-audit", sdk: release ?? null } satisfies ShipStatus);
  if (error) console.warn(`audit shipping failed, ${rows.length} event(s) stay pending: ${error}`);
  return { shipped: error ? 0 : rows.length, pending: error ? rows.length : 0, error };
}

async function recent(url: URL): Promise<Response> {
  const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit") ?? "50") || 50));
  const sub = url.searchParams.get("sub");
  if (sub !== null && !/^[A-Za-z0-9._:@-]{1,128}$/.test(sub)) {
    return Response.json({ error: { code: "invalid_sub", message: "bad sub" } }, { status: 400 });
  }
  const ip = url.searchParams.get("ip");
  if (ip !== null && !isClientIp(ip)) {
    return Response.json({ error: { code: "invalid_ip", message: "bad ip" } }, { status: 400 });
  }
  const [events, pending] = await sql.batch([
    {
      sql: "SELECT * FROM audit WHERE (? IS NULL OR sub = ?) AND (? IS NULL OR ip = ?) ORDER BY at DESC LIMIT ?",
      args: [sub, sub, ip, ip, limit],
    },
    "SELECT COUNT(*) AS n FROM audit WHERE shipped_at IS NULL",
  ], "read") as SqlResult[];
  return Response.json({
    events: objects(events),
    shipping: { pending: Number(objects(pending)[0].n), last: await kv.get("ship") },
  });
}

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    // Only the gateway calls this (access=internal, call.allowed_callers); it already checked
    // the audit:read scope, and this checks again what the gateway forwarded.
    if (ctx.trigger !== "call") return Response.json({ error: { code: "not_found", message: "not found" } }, { status: 404 });
    const scopes = (req.headers.get("x-gw-scopes") ?? "").split(" ");
    if (!scopes.includes("audit:read")) return Response.json({ error: { code: "forbidden", message: "audit:read required" } }, { status: 403 });
    const url = new URL(req.url);
    if (url.pathname !== "/recent" || req.method !== "GET") {
      return Response.json({ error: { code: "not_found", message: "no such route" } }, { status: 404 });
    }
    try {
      await ensureSchema();
      return await recent(url);
    } catch (e) {
      console.error(e);
      return Response.json({ error: { code: "internal", message: "internal error" } }, { status: 500 });
    }
  },

  // At-least-once delivery: the event id is the primary key, so a redelivery stores nothing new.
  async queue(batch: { messages: QueueMessage[] }, _ctx: Ctx): Promise<void> {
    await ensureSchema();
    const events: AuditEvent[] = [];
    for (const m of batch.messages) {
      const e = parseAuditEvent(m.body);
      if (e) events.push(e);
      else console.warn(`dropping malformed audit message ${m.id}`); // a retry cannot fix it
    }
    if (events.length) await store(events); // a throw here fails the task: the batch is redelivered
    // The gateway reports how its own attempt went; ship now only what it could not (or did not
    // try to) ship, together with anything still pending. Shipping failures are not delivery
    // failures: the events are stored and stay pending for the schedule.
    const attempt = lastGatewayAttempt(events);
    if (attempt) await kv.set("ship", attempt);
    if (events.some((e) => !e.shipped)) await ship();
  },

  async scheduled(_event: unknown, _ctx: Ctx): Promise<void> {
    await ensureSchema();
    const r = await ship();
    const days = Number(env("AUDIT_RETENTION_DAYS") ?? "30");
    if (Number.isFinite(days) && days > 0) {
      await sql.execute("DELETE FROM audit WHERE at < ? AND shipped_at IS NOT NULL", [Date.now() - days * 86_400_000]);
    }
    if (r.error) console.info(`pending audit events: ${r.pending}`);
  },
};
