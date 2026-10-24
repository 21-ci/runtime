// UZS exchange rates from the Central Bank of Uzbekistan, polled by an interval schedule.
//
//   scheduled             every 60 s: fetch all rates, store history rows only when a rate or its
//                         date changed, enqueue a notification for changed currencies. During a
//                         back-off a one-off std:schedule (id cbu-rates-retry) retries at its end;
//                         a one-off delivery is told apart by its `id`
//   queue                 notifications for changed currencies (see notify)
//   GET  /latest          ?ccy=USD,EUR (default: all)
//   GET  /history         ?ccy=USD&days=30
//   GET  /convert         ?amount=100&from=USD&to=EUR (UZS is a valid side)
//   GET  /status          fetch health (last success, consecutive failures, back-off)
//   POST /admin/refresh   admin: poll now (ignores the back-off)
//   POST /admin/ingest    admin: a CBU-format JSON array, processed like a poll (tests, corrections)
//   POST /admin/backfill  admin: ?days=30, history from the CBU archive

import { sql } from "std:sql";
import { kv } from "std:kv";
import { enqueue } from "std:queue";
import { schedule } from "std:schedule";
import {
  type CbuRate,
  type Change,
  changeKey,
  convert,
  detectChanges,
  type FetchStatus,
  isoDate,
  type Latest,
  nextAttemptAt,
  notifiable,
  parseCbuPayload,
  parseCcyList,
  perUnit,
} from "./cbu.ts";
import { error, errorText, json, readJson, readTextCapped, requireAdmin } from "./http.ts";

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}
// A cron/interval fire carries `scheduleId`; a one-off std:schedule delivery carries `id`.
interface ScheduledEvent {
  scheduleId?: string;
  scheduledTime?: number;
  id?: string;
  scheduledAt?: number;
  payload: unknown;
}
interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
}
interface SqlTx {
  execute(sql: string, args?: unknown[]): Promise<SqlResult>;
}

const DEFAULT_BASE = "https://cbu.uz/uz/arkhiv-kursov-valyut/json/";
const FETCH_TIMEOUT_MS = 10_000;
const MAX_FEED_BYTES = 1024 * 1024;
const STATUS_KEY = "status";
// The id of the one-off retry scheduled during a back-off (std:schedule).
const RETRY_SCHEDULE = "cbu-rates-retry";
const PUBLIC_CACHE = { "cache-control": "public, max-age=60", "access-control-allow-origin": "*" };

function env(name: string): string | undefined {
  const v = Deno.env.get(name);
  return v === undefined || v === "" ? undefined : v;
}

const feedBase = () => (env("CBU_BASE_URL") ?? DEFAULT_BASE).replace(/\/?$/, "/");

// ---------------------------------------------------------------------------------- SQL

function objects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}

let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const found = await sql.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'history_ccy_date'") as SqlResult;
  if (found.rows.length === 0) {
    await sql.batch([
      `CREATE TABLE IF NOT EXISTS latest (
         ccy TEXT PRIMARY KEY, code TEXT NOT NULL, name TEXT NOT NULL, nominal INTEGER NOT NULL,
         rate TEXT NOT NULL, diff TEXT NOT NULL, date TEXT NOT NULL, updated_at INTEGER NOT NULL)`,
      // One row per distinct (currency, date, rate): re-polls and retries insert nothing.
      `CREATE TABLE IF NOT EXISTS history (
         id INTEGER PRIMARY KEY AUTOINCREMENT, ccy TEXT NOT NULL, date TEXT NOT NULL, rate TEXT NOT NULL,
         diff TEXT NOT NULL, nominal INTEGER NOT NULL, observed_at INTEGER NOT NULL, source TEXT NOT NULL,
         UNIQUE (ccy, date, rate))`,
      `CREATE INDEX IF NOT EXISTS history_ccy_date ON history (ccy, date)`,
    ], "write");
  }
  schemaReady = true;
}

const INSERT_HISTORY = `INSERT OR IGNORE INTO history (ccy, date, rate, diff, nominal, observed_at, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?)`;
const UPSERT_LATEST = `INSERT INTO latest (ccy, code, name, nominal, rate, diff, date, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (ccy) DO UPDATE SET code = excluded.code, name = excluded.name,
                         nominal = excluded.nominal, rate = excluded.rate, diff = excluded.diff,
                         date = excluded.date, updated_at = excluded.updated_at
                       WHERE excluded.date >= latest.date`;

// Compare and write in one transaction, so two concurrent polls cannot both record a change.
async function ingest(rates: CbuRate[], source: string): Promise<{ forward: Change[]; older: number }> {
  await ensureSchema();
  const now = Date.now();
  return await sql.transaction(async (tx: SqlTx) => {
    const latest = new Map<string, Latest>();
    for (const row of objects(await tx.execute("SELECT ccy, date, rate FROM latest"))) {
      latest.set(String(row.ccy), { ccy: String(row.ccy), date: String(row.date), rate: String(row.rate) });
    }
    const { forward, older } = detectChanges(latest, rates);
    for (const r of forward) {
      await tx.execute(INSERT_HISTORY, [r.ccy, r.date, r.rate, r.diff, r.nominal, now, source]);
      await tx.execute(UPSERT_LATEST, [r.ccy, r.code, r.name, r.nominal, r.rate, r.diff, r.date, now]);
    }
    let added = 0;
    for (const r of older) {
      added += (await tx.execute(INSERT_HISTORY, [r.ccy, r.date, r.rate, r.diff, r.nominal, now, source])).rowsAffected;
    }
    return { forward, older: added };
  });
}

// ---------------------------------------------------------------------------------- feed

type FeedResult = { ok: true; rates: CbuRate[]; errors: string[] } | { ok: false; error: string; detail?: string[] };

async function fetchFeed(path: string): Promise<FeedResult> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(feedBase() + path, { headers: { accept: "application/json" }, signal: ac.signal });
    if (res.status !== 200) {
      await res.body?.cancel().catch(() => {});
      return { ok: false, error: `upstream_http_${res.status}` };
    }
    const text = await readTextCapped(res.body, MAX_FEED_BYTES);
    if (text === null) return { ok: false, error: "upstream_too_large" };
    let raw: unknown;
    try {
      raw = JSON.parse(text);
    } catch {
      return { ok: false, error: "upstream_malformed" }; // an HTML maintenance page, a cut-off body, …
    }
    const { rates, errors } = parseCbuPayload(raw);
    if (rates.length === 0) return { ok: false, error: "upstream_no_valid_rates", detail: errors.slice(0, 10) };
    if (errors.length) console.warn(`feed ${path || "/"}: skipped ${errors.length} item(s):`, errors.slice(0, 5).join("; "));
    return { ok: true, rates, errors };
  } catch (e) {
    // egress_* codes from the runtime, or AbortError on our timeout
    return { ok: false, error: ac.signal.aborted ? "upstream_timeout" : `upstream_unreachable: ${errorText(e)}` };
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------------- notify

interface NotifyResult {
  enqueued: boolean;
  reason?: string;
  key?: string;
  count?: number;
}

// Optional: works without a consumer. NOTIFY_SERVICE is the slug of a same-tenant service with a
// queue handler (the telegram-bot sample). dedupId = hash of the changes: a retried poll or a
// concurrent admin refresh announces the same change once (24 h dedupe window).
async function notify(changes: Change[]): Promise<NotifyResult> {
  const target = env("NOTIFY_SERVICE");
  if (!target) return { enqueued: false, reason: "NOTIFY_SERVICE is not set" };
  const ccys = parseCcyList(env("NOTIFY_CCYS")) ?? ["USD", "EUR", "RUB"];
  const relevant = notifiable(changes, ccys);
  if (relevant.length === 0) return { enqueued: false, reason: "no rate change in " + ccys.join(",") };
  const key = await changeKey(relevant);
  const body = {
    type: "cbu.rate_changed",
    key,
    changes: relevant.map((c) => ({ ccy: c.ccy, date: c.date, rate: c.rate, diff: c.diff, nominal: c.nominal, previous: c.previous })),
  };
  try {
    await enqueue(target, body, { dedupId: key });
    return { enqueued: true, key, count: relevant.length };
  } catch (e) {
    // not_found (no such service), call_not_allowed (other environment), forbidden (queue trigger off), queue_full
    const code = (e as { code?: string }).code ?? "enqueue_failed";
    console.warn(`notify ${target} failed: ${code}`);
    return { enqueued: false, reason: code, key };
  }
}

// ---------------------------------------------------------------------------------- poll

async function readStatus(): Promise<FetchStatus> {
  const s = await kv.get(STATUS_KEY);
  return s && typeof s === "object" ? s as FetchStatus : {};
}

async function poll(source: string) {
  const status = await readStatus();
  const now = Date.now();
  const feed = await fetchFeed("");
  if (!feed.ok) {
    const failures = (status.consecutiveFailures ?? 0) + 1;
    await kv.set(STATUS_KEY, { ...status, lastAttemptAt: now, consecutiveFailures: failures, lastError: feed.error });
    console.warn(`poll failed (${failures} in a row): ${feed.error}`);
    // Past the first failures the polls back off: one retry at the end of the back-off, as a
    // one-off schedule (the same id replaces the pending one, so there is at most one).
    const retryAt = nextAttemptAt({ consecutiveFailures: failures, lastAttemptAt: now });
    if (retryAt > 0) {
      await schedule.at(retryAt, { reason: "back-off", source }, { id: RETRY_SCHEDULE }).catch((e) =>
        console.warn(`retry not scheduled: ${errorText(e)}`)
      );
    }
    return { ok: false as const, error: feed.error, detail: feed.detail, consecutiveFailures: failures };
  }
  const { forward, older } = await ingest(feed.rates, source);
  const notified = await notify(forward);
  await kv.set(STATUS_KEY, {
    lastAttemptAt: now,
    lastSuccessAt: now,
    consecutiveFailures: 0,
    lastError: null,
    lastChangeAt: forward.length ? now : (status as Record<string, unknown>).lastChangeAt ?? null,
  });
  await schedule.cancel(RETRY_SCHEDULE).catch(() => false);
  if (forward.length) console.info(`changed: ${forward.map((c) => `${c.ccy} ${c.previous ?? "-"} -> ${c.rate} (${c.date})`).join(", ")}`);
  return {
    ok: true as const,
    fetched: feed.rates.length,
    skipped: feed.errors.length,
    changed: forward.map((c) => ({ ccy: c.ccy, date: c.date, rate: c.rate, previous: c.previous })),
    olderAdded: older,
    notify: notified,
  };
}

// The CBU archive, newest first, 4 requests at a time, stopping before the wall limit.
async function backfill(days: number, ctx: Ctx) {
  const dates = Array.from({ length: days }, (_, i) => isoDate(Date.now(), i + 1));
  const results: { date: string; ok: boolean; added?: number; error?: string }[] = [];
  let next = 0;
  const worker = async () => {
    while (next < dates.length && ctx.deadline - Date.now() > 8_000) {
      const date = dates[next++];
      const feed = await fetchFeed(`all/${date}/`);
      if (!feed.ok) {
        results.push({ date, ok: false, error: feed.error });
        continue;
      }
      const { forward, older } = await ingest(feed.rates, "backfill");
      results.push({ date, ok: true, added: forward.length + older });
    }
  };
  await Promise.all([worker(), worker(), worker(), worker()]);
  results.sort((a, b) => (a.date < b.date ? 1 : -1));
  return { requested: days, done: results.length, results };
}

// ---------------------------------------------------------------------------------- HTTP

function rateView(o: Record<string, unknown>) {
  const nominal = Number(o.nominal);
  return {
    ccy: String(o.ccy),
    code: String(o.code),
    name: String(o.name),
    nominal,
    rate: String(o.rate),
    diff: String(o.diff),
    date: String(o.date),
    perUnit: perUnit(String(o.rate), nominal),
  };
}

async function latestRows(ccys: string[] | null) {
  await ensureSchema();
  const r = ccys
    ? await sql.execute(`SELECT * FROM latest WHERE ccy IN (${ccys.map(() => "?").join(",")}) ORDER BY ccy`, ccys)
    : await sql.execute("SELECT * FROM latest ORDER BY ccy");
  return objects(r as SqlResult).map(rateView);
}

async function handleLatest(url: URL): Promise<Response> {
  const q = url.searchParams.get("ccy");
  const ccys = q === null ? null : parseCcyList(q);
  if (q !== null && !ccys) return error(400, "invalid_ccy", "ccy must be a comma-separated list of 3-letter codes");
  const rates = await latestRows(ccys);
  if (rates.length === 0) return error(404, "no_data", ccys ? "no rates for these currencies yet" : "no rates yet");
  const date = rates.map((r) => r.date).sort().at(-1);
  return json({ source: "cbu.uz", date, rates }, 200, PUBLIC_CACHE);
}

async function handleHistory(url: URL): Promise<Response> {
  const ccy = parseCcyList(url.searchParams.get("ccy"), 1)?.[0];
  if (!ccy) return error(400, "invalid_ccy", "ccy must be one 3-letter code, e.g. ?ccy=USD");
  const days = Number(url.searchParams.get("days") ?? "30");
  if (!Number.isInteger(days) || days < 1 || days > 366) return error(400, "invalid_days", "days must be 1-366");
  await ensureSchema();
  const r = await sql.execute(
    "SELECT date, rate, diff, nominal, observed_at FROM history WHERE ccy = ? AND date >= ? ORDER BY date, id",
    [ccy, isoDate(Date.now(), days)],
  ) as SqlResult;
  const points = objects(r).map((o) => ({
    date: String(o.date),
    rate: String(o.rate),
    diff: String(o.diff),
    nominal: Number(o.nominal),
    perUnit: perUnit(String(o.rate), Number(o.nominal)),
    observedAt: Number(o.observed_at),
  }));
  return json({ ccy, days, points }, 200, PUBLIC_CACHE);
}

async function handleConvert(url: URL): Promise<Response> {
  const amount = Number(url.searchParams.get("amount") ?? "1");
  const from = (url.searchParams.get("from") ?? "").toUpperCase();
  const to = (url.searchParams.get("to") ?? "UZS").toUpperCase();
  if (!Number.isFinite(amount) || amount < 0 || amount > 1e12) return error(400, "invalid_amount", "amount must be 0-1e12");
  if (!/^[A-Z]{3}$/.test(from) || !/^[A-Z]{3}$/.test(to)) return error(400, "invalid_ccy", "from and to must be 3-letter codes");
  const wanted = [from, to].filter((c) => c !== "UZS");
  const rows = wanted.length ? await latestRows(wanted) : [];
  const unit = (c: string) => (c === "UZS" ? 1 : rows.find((r) => r.ccy === c)?.perUnit);
  const [f, t] = [unit(from), unit(to)];
  if (f === undefined || t === undefined) return error(404, "no_data", "no rate for one of the currencies");
  return json({ amount, from, to, result: convert(amount, f, t), rates: rows }, 200, PUBLIC_CACHE);
}

async function handleStatus(): Promise<Response> {
  const s = await readStatus();
  const next = nextAttemptAt(s);
  return json({ ...s, backingOff: next > Date.now(), nextAttemptAt: next || null }, 200, { "access-control-allow-origin": "*" });
}

async function handleAdmin(path: string, req: Request, url: URL, ctx: Ctx): Promise<Response> {
  if (req.method !== "POST") return error(405, "method_not_allowed", "use POST");
  if (path === "/admin/refresh") {
    const r = await poll("admin");
    return json(r, r.ok ? 200 : 502);
  }
  if (path === "/admin/ingest") {
    const body = await readJson(req, MAX_FEED_BYTES);
    if (!body.ok) return body.response;
    const { rates, errors } = parseCbuPayload(body.value);
    if (rates.length === 0) return error(422, "no_valid_rates", errors.slice(0, 10).join("; ") || "empty payload");
    const { forward, older } = await ingest(rates, "ingest");
    return json({
      accepted: rates.length,
      skipped: errors,
      changed: forward.map((c) => ({ ccy: c.ccy, date: c.date, rate: c.rate, previous: c.previous })),
      olderAdded: older,
      notify: await notify(forward),
    });
  }
  if (path === "/admin/backfill") {
    const days = Number(url.searchParams.get("days") ?? "30");
    if (!Number.isInteger(days) || days < 1 || days > 31) return error(400, "invalid_days", "days must be 1-31");
    return json(await backfill(days, ctx));
  }
  return error(404, "not_found", "no such route");
}

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    try {
      if (path.startsWith("/admin/")) {
        const denied = await requireAdmin(req, env("ADMIN_TOKEN"));
        return denied ?? await handleAdmin(path, req, url, ctx);
      }
      if (req.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: { ...PUBLIC_CACHE, "access-control-allow-methods": "GET" } });
      }
      if (req.method !== "GET") return error(405, "method_not_allowed", "read-only API");
      switch (path) {
        case "/":
          return json({
            service: "cbu-rates",
            source: feedBase(),
            endpoints: ["/latest?ccy=USD,EUR", "/history?ccy=USD&days=30", "/convert?amount=100&from=USD&to=UZS", "/status"],
          });
        case "/latest":
          return await handleLatest(url);
        case "/history":
          return await handleHistory(url);
        case "/convert":
          return await handleConvert(url);
        case "/status":
          return await handleStatus();
      }
      return error(404, "not_found", "no such route");
    } catch (e) {
      console.error(errorText(e));
      return error(500, "internal", "internal error");
    }
  },

  // Interval schedule (every 60 s) and the one-off back-off retry. Schedules never overlap, missed
  // ticks are skipped. A one-off delivery (it has an `id`) polls at its time; the back-off check
  // still applies to it, with the same slack.
  async scheduled(event: ScheduledEvent, _ctx: Ctx): Promise<void> {
    const status = await readStatus();
    // The 5 s slack absorbs the schedule's jitter.
    if (Date.now() + 5_000 < nextAttemptAt(status)) {
      console.info(`backing off after ${status.consecutiveFailures} failures (${status.lastError})`);
      return;
    }
    // A failure is recorded in the status; the next tick (or the back-off) retries.
    await poll(event.id !== undefined ? "retry" : "schedule");
  },
};
