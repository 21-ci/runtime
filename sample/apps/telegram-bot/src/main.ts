// Telegram bot: notes, reminders and CBU rate alerts (webhook mode).
//
//   POST /webhook        Telegram updates (X-Telegram-Bot-Api-Secret-Token checked)
//   POST /setup          admin: getMe + setWebhook(PUBLIC_URL/webhook, secret) + setMyCommands
//   GET  /status         admin: getWebhookInfo and counters
//   GET  /admin/outbox   admin: Bot API calls recorded in dry-run mode (TELEGRAM_DRY_RUN=1)
//   GET  /               public health (which settings are present, never their values)
//   queue                {type:"reminder"} delayed self-messages, {type:"cbu.rate_changed"} from the
//                        cbu-rates sample (fanned out as one {type:"send"} message per subscriber)
//
// State: the per-chat dialog state lives in std:kv (`fsm/<chat id>`, 1 h TTL), and a std:lock on the
// same key serializes the steps of a chat; notes, reminders, subscribers and the update-id dedupe
// table live in std:sql.

import { sql } from "std:sql";
import { kv } from "std:kv";
import { lock } from "std:lock";
import { log } from "std:log";
import { enqueue } from "std:queue";
import { secrets } from "std:secrets";
import { call } from "std:services";
import { error, errorText, isObject, json, readJson, requireAdmin, safeEqual } from "./http.ts";
import {
  COMMANDS,
  type Effect,
  formatIn,
  formatLocal,
  formatRateChanges,
  formatRateLines,
  isState,
  parseRatesMessage,
  type RatesMessage,
  transition,
} from "./bot.ts";
import { BotApi, type IncomingMessage, isValidSecretToken, type OutboxEntry, parseUpdate, TelegramError } from "./telegram.ts";

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
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
  lastInsertRowid?: number | bigint | null;
}

const DEFAULT_SLUG = "telegram-bot";
const FSM_TTL_SECONDS = 3600;
const MAX_UPDATE_BYTES = 256 * 1024;
const DAY_MS = 86_400_000;
const MAX_NOTES_PER_CHAT = 1000;
const MAX_PENDING_REMINDERS = 50;
const FANOUT_PAGE = 500;
const RATES_QUERY = "/latest?ccy=USD,EUR,RUB,GBP,CNY,KZT";

// ---------------------------------------------------------------------------------- config

function env(name: string): string | undefined {
  const v = Deno.env.get(name);
  return v === undefined || v === "" ? undefined : v;
}

// A secret (`21ci secret set`): read through std:secrets, never from a plain env var. Unset: undefined.
function secret(name: string): string | undefined {
  return secrets.get(name) ?? undefined;
}

function config() {
  const tz = Number(env("BOT_TZ_OFFSET_MINUTES") ?? "300");
  return {
    botToken: secret("TELEGRAM_BOT_TOKEN"),
    webhookSecret: secret("TELEGRAM_WEBHOOK_SECRET"),
    adminToken: secret("ADMIN_TOKEN"),
    publicUrl: env("PUBLIC_URL"),
    dryRun: env("TELEGRAM_DRY_RUN") === "1",
    tzOffsetMin: Number.isInteger(tz) && Math.abs(tz) <= 14 * 60 ? tz : 300,
    ratesService: env("RATES_SERVICE") ?? "cbu-rates",
  };
}
type Config = ReturnType<typeof config>;

// ctx.service.slug may be empty on older nodes (docs/handlers.md, known gap).
const selfSlug = (ctx: Ctx) => ctx.service.slug || env("SELF_SLUG") || DEFAULT_SLUG;

const botFor = (cfg: Config, outbox: OutboxEntry[]) => new BotApi({ token: cfg.botToken ?? "", dryRun: cfg.dryRun, outbox });

// ---------------------------------------------------------------------------------- SQL

function objects(r: SqlResult): Record<string, unknown>[] {
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
}

async function one(text: string, args: unknown[] = []): Promise<Record<string, unknown> | null> {
  return objects(await sql.execute(text, args) as SqlResult)[0] ?? null;
}

// Read first; only a database without the schema pays for the write batch (the sql-todo pattern).
// The index created last proves the whole batch committed.
let schemaReady = false;
async function ensureSchema() {
  if (schemaReady) return;
  const found = await sql.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'notes_chat'") as SqlResult;
  if (found.rows.length === 0) {
    await sql.batch([
      `CREATE TABLE IF NOT EXISTS updates (update_id INTEGER PRIMARY KEY, received_at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS notes (
         id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, text TEXT NOT NULL,
         source_update INTEGER NOT NULL, created_at INTEGER NOT NULL, UNIQUE (chat_id, source_update))`,
      `CREATE TABLE IF NOT EXISTS reminders (
         id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, text TEXT NOT NULL,
         due_at INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
         source_update INTEGER NOT NULL, created_at INTEGER NOT NULL, sent_at INTEGER,
         UNIQUE (chat_id, source_update))`,
      `CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY, created_at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS outbox (
         id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, method TEXT NOT NULL, params TEXT NOT NULL)`,
      `CREATE INDEX IF NOT EXISTS reminders_chat ON reminders (chat_id, status, due_at)`,
      `CREATE INDEX IF NOT EXISTS notes_chat ON notes (chat_id, id)`,
    ], "write");
  }
  schemaReady = true;
}

// Dry-run mode keeps the last 200 Bot API calls so tests can see what the bot would have sent.
async function saveOutbox(outbox: OutboxEntry[]) {
  if (outbox.length === 0) return;
  const now = Date.now();
  await sql.batch([
    ...outbox.map((o) => ({ sql: "INSERT INTO outbox (at, method, params) VALUES (?, ?, ?)", args: [now, o.method, JSON.stringify(o.params)] })),
    "DELETE FROM outbox WHERE id <= (SELECT MAX(id) - 200 FROM outbox)",
  ], "write");
}

// ---------------------------------------------------------------------------------- webhook

async function handleWebhook(req: Request, cfg: Config, ctx: Ctx): Promise<Response> {
  if (!isValidSecretToken(cfg.webhookSecret) || (!cfg.botToken && !cfg.dryRun)) {
    return error(503, "not_configured", "the bot is not configured");
  }
  const given = req.headers.get("x-telegram-bot-api-secret-token") ?? "";
  if (!(await safeEqual(given, cfg.webhookSecret))) return error(401, "unauthorized", "bad secret token");

  const body = await readJson(req, MAX_UPDATE_BYTES);
  if (!body.ok) return body.response;
  const update = parseUpdate(body.value);
  if (!update) return error(400, "invalid_update", "not a Telegram update");

  await ensureSchema();
  const now = Date.now();
  // Telegram re-delivers an update until it gets a 2xx. Claim the id atomically; a second
  // delivery finds it and is answered 200 without doing anything.
  const claim = await sql.execute(
    "INSERT OR IGNORE INTO updates (update_id, received_at) VALUES (?, ?)",
    [update.updateId, now],
  ) as SqlResult;
  if (claim.rowsAffected === 0) return json({ ok: true, duplicate: true });

  const outbox: OutboxEntry[] = [];
  try {
    if (update.message?.text != null) await converse(update.message, update.updateId, botFor(cfg, outbox), cfg, ctx, now);
    if (update.updateId % 100 === 0) await sql.execute("DELETE FROM updates WHERE received_at < ?", [now - 3 * DAY_MS]);
  } catch (e) {
    // Release the claim and answer 5xx, so Telegram's retry processes the update again. Effects
    // are idempotent (keyed by the update id) and the dialog state is saved last.
    // A structured record: `21ci logs` shows the fields, `--json` prints them as they are.
    log.with({ updateId: update.updateId }).error("update failed", { error: errorText(e) });
    await sql.execute("DELETE FROM updates WHERE update_id = ?", [update.updateId]).catch(() => {});
    return error(500, "temporary_failure", "try again later");
  } finally {
    await saveOutbox(outbox).catch((e) => console.warn("outbox:", errorText(e)));
  }
  return json({ ok: true });
}

// One dialog step per chat at a time: a message that arrives while the chat's previous step runs
// waits for it (std:lock), so both steps read and save the state in order. A wait past 5 s is a 5xx,
// and Telegram retries the update.
async function converse(msg: IncomingMessage, updateId: number, bot: BotApi, cfg: Config, ctx: Ctx, now: number) {
  await lock(`fsm/${msg.chatId}`, () => advance(msg, updateId, bot, cfg, ctx, now), { waitMs: 5000, leaseMs: 30000 });
}

async function advance(msg: IncomingMessage, updateId: number, bot: BotApi, cfg: Config, ctx: Ctx, now: number) {
  const fsmKey = `fsm/${msg.chatId}`;
  const [stored, username] = await kv.getMany([fsmKey, "bot/username"]);
  const state = isState(stored) ? stored : null;

  const step = transition(state, msg.text ?? "", {
    now,
    tzOffsetMin: cfg.tzOffsetMin,
    botUsername: typeof username === "string" ? username : null,
    privateChat: msg.chatType === "private",
  });

  const reply = step.effect ? await applyEffect(step.effect, msg.chatId, updateId, now, cfg, ctx) : step.reply;
  if (reply) await sendOrSkip(bot, msg.chatId, reply);

  // Saved only after the reply went out: a failed send replays the same transition on retry.
  if (step.state) await kv.set(fsmKey, step.state, { ttlSeconds: FSM_TTL_SECONDS });
  else if (state) await kv.delete(fsmKey);
}

// A chat that cannot be reached (blocked bot, deleted chat) is not an error worth retrying.
async function sendOrSkip(bot: BotApi, chatId: number, text: string): Promise<boolean> {
  try {
    await bot.sendMessage(chatId, text);
    return true;
  } catch (e) {
    if (e instanceof TelegramError && e.permanent) {
      console.warn(`chat ${chatId} unreachable: ${e.message}`);
      return false;
    }
    throw e;
  }
}

const short = (at: number, tz: number) => formatLocal(at, tz).slice(0, 16);
const preview = (s: string, n = 120) => (s.length > n ? `${s.slice(0, n - 1)}…` : s).replace(/\s+/g, " ");

async function applyEffect(effect: Effect, chatId: number, updateId: number, now: number, cfg: Config, ctx: Ctx): Promise<string> {
  const tz = cfg.tzOffsetMin;
  switch (effect.kind) {
    case "save_note": {
      let row = await one("SELECT id FROM notes WHERE chat_id = ? AND source_update = ?", [chatId, updateId]);
      if (!row) {
        const count = await one("SELECT COUNT(*) AS n FROM notes WHERE chat_id = ?", [chatId]);
        if (Number(count?.n) >= MAX_NOTES_PER_CHAT) return `You have ${MAX_NOTES_PER_CHAT} notes. Delete some with /delnote first.`;
        await sql.execute(
          "INSERT OR IGNORE INTO notes (chat_id, text, source_update, created_at) VALUES (?, ?, ?, ?)",
          [chatId, effect.text, updateId, now],
        );
        row = await one("SELECT id FROM notes WHERE chat_id = ? AND source_update = ?", [chatId, updateId]);
      }
      return `Saved note #${row?.id}. /notes shows your latest notes.`;
    }

    case "list_notes": {
      const r = await sql.execute(
        "SELECT id, text, created_at FROM notes WHERE chat_id = ? ORDER BY id DESC LIMIT 10",
        [chatId],
      ) as SqlResult;
      const notes = objects(r);
      if (notes.length === 0) return "No notes yet. Send /note to add one.";
      return ["Your latest notes:", ...notes.map((n) => `#${n.id}  ${short(Number(n.created_at), tz)}  ${preview(String(n.text))}`)].join("\n");
    }

    case "delete_note": {
      const r = await sql.execute("DELETE FROM notes WHERE id = ? AND chat_id = ?", [effect.id, chatId]) as SqlResult;
      return r.rowsAffected ? `Deleted note #${effect.id}.` : `There is no note #${effect.id}.`;
    }

    case "create_reminder": {
      let row = await one("SELECT id, due_at FROM reminders WHERE chat_id = ? AND source_update = ?", [chatId, updateId]);
      if (!row) {
        const count = await one("SELECT COUNT(*) AS n FROM reminders WHERE chat_id = ? AND status = 'pending'", [chatId]);
        if (Number(count?.n) >= MAX_PENDING_REMINDERS) return `You have ${MAX_PENDING_REMINDERS} pending reminders. Cancel some with /unremind.`;
        await sql.execute(
          "INSERT OR IGNORE INTO reminders (chat_id, text, due_at, source_update, created_at) VALUES (?, ?, ?, ?, ?)",
          [chatId, effect.text, effect.dueAt, updateId, now],
        );
        row = await one("SELECT id, due_at FROM reminders WHERE chat_id = ? AND source_update = ?", [chatId, updateId]);
      }
      const id = Number(row!.id);
      const dueAt = Number(row!.due_at);
      // A delayed message to ourselves. dedupId makes a retried update enqueue it only once.
      await enqueue(selfSlug(ctx), { type: "reminder", id }, {
        delaySeconds: Math.max(0, Math.ceil((dueAt - Date.now()) / 1000)),
        dedupId: `rem:${id}`,
      });
      return `Reminder #${id} set for ${formatLocal(dueAt, tz)} (${formatIn(dueAt - now)}).`;
    }

    case "list_reminders": {
      const r = await sql.execute(
        "SELECT id, text, due_at FROM reminders WHERE chat_id = ? AND status = 'pending' ORDER BY due_at LIMIT 20",
        [chatId],
      ) as SqlResult;
      const rows = objects(r);
      if (rows.length === 0) return "No pending reminders. Send /remind to add one.";
      return ["Pending reminders:", ...rows.map((x) => `#${x.id}  ${short(Number(x.due_at), tz)}  ${preview(String(x.text))}`)].join("\n");
    }

    case "cancel_reminder": {
      const r = await sql.execute(
        "UPDATE reminders SET status = 'cancelled' WHERE id = ? AND chat_id = ? AND status = 'pending'",
        [effect.id, chatId],
      ) as SqlResult;
      return r.rowsAffected ? `Reminder #${effect.id} cancelled.` : `There is no pending reminder #${effect.id}.`;
    }

    case "subscribe":
      await sql.execute("INSERT OR IGNORE INTO subscribers (chat_id, created_at) VALUES (?, ?)", [chatId, now]);
      return "Subscribed. I will message you when the CBU changes USD, EUR or RUB rates. /unsubscribe to stop.";

    case "unsubscribe": {
      const r = await sql.execute("DELETE FROM subscribers WHERE chat_id = ?", [chatId]) as SqlResult;
      return r.rowsAffected ? "Unsubscribed." : "You were not subscribed.";
    }

    case "rates":
      return await currentRates(cfg);
  }
}

// std:services call to the cbu-rates sample (same tenant, in-process). Optional: without it the
// command answers politely instead of failing.
async function currentRates(cfg: Config): Promise<string> {
  let res: Response;
  try {
    res = await call(cfg.ratesService, new Request(`http://svc${RATES_QUERY}`));
  } catch (e) {
    console.warn(`rates call to ${cfg.ratesService} failed:`, errorText(e));
    return "The rates service is not connected.";
  }
  if (!res.ok) return "Rates are not available right now. Try again later.";
  const data = await res.json().catch(() => null) as { date?: unknown; rates?: unknown } | null;
  const rates = Array.isArray(data?.rates) ? data!.rates.filter(isRateRow) : [];
  if (rates.length === 0) return "No rates yet. Try again later.";
  return [`CBU rates (${typeof data?.date === "string" ? data.date : "latest"}):`, ...formatRateLines(rates)].join("\n");
}

function isRateRow(v: unknown): v is { ccy: string; rate: string; diff: string; nominal: number } {
  return isObject(v) && typeof v.ccy === "string" && typeof v.rate === "string" && typeof v.diff === "string" &&
    typeof v.nominal === "number";
}

// ---------------------------------------------------------------------------------- queue

type Job =
  | { type: "reminder"; id: number }
  | { type: "send"; chatId: number; text: string }
  | { type: "rates"; msg: RatesMessage; after: number };

function parseJob(body: unknown): Job | null {
  if (!isObject(body)) return null;
  if (body.type === "reminder" && Number.isSafeInteger(body.id)) return { type: "reminder", id: body.id as number };
  if (body.type === "send" && Number.isSafeInteger(body.chatId) && typeof body.text === "string" && body.text.length <= 4096) {
    return { type: "send", chatId: body.chatId as number, text: body.text };
  }
  const msg = parseRatesMessage(body);
  if (msg) {
    const after = Number.isSafeInteger(body.after) ? body.after as number : Number.MIN_SAFE_INTEGER;
    return { type: "rates", msg, after };
  }
  return null;
}

async function handleJob(m: QueueMessage, job: Job, bot: BotApi, ctx: Ctx) {
  switch (job.type) {
    case "reminder": {
      const r = await one("SELECT id, chat_id, text, due_at, status FROM reminders WHERE id = ?", [job.id]);
      if (!r || r.status !== "pending") return m.ack(); // cancelled, already sent or unknown
      const wait = Number(r.due_at) - Date.now();
      if (wait > 5000) return m.retry({ delaySeconds: Math.min(Math.ceil(wait / 1000), 7 * 86400) }); // early (e.g. a DLQ redrive)
      const sent = await sendOrSkip(bot, Number(r.chat_id), `Reminder: ${r.text}`);
      await sql.execute(
        "UPDATE reminders SET status = ?, sent_at = ? WHERE id = ? AND status = 'pending'",
        [sent ? "sent" : "failed", Date.now(), job.id],
      );
      return m.ack();
    }

    case "send": {
      // Unreachable subscribers are dropped, so the next broadcast skips them.
      if (!(await sendOrSkip(bot, job.chatId, job.text))) {
        await sql.execute("DELETE FROM subscribers WHERE chat_id = ?", [job.chatId]);
      }
      return m.ack();
    }

    case "rates": {
      // Fan-out: one "send" message per subscriber, so each chat is retried on its own (Telegram
      // 429s, network errors) and a big subscriber list does not hit one task's fetch limit.
      const text = formatRateChanges(job.msg);
      const self = selfSlug(ctx);
      let after = job.after;
      for (;;) {
        const r = await sql.execute(
          "SELECT chat_id FROM subscribers WHERE chat_id > ? ORDER BY chat_id LIMIT ?",
          [after, FANOUT_PAGE],
        ) as SqlResult;
        for (const [chatId] of r.rows) {
          await enqueue(self, { type: "send", chatId, text }, { dedupId: `rates:${job.msg.key}:${chatId}` });
          after = Number(chatId);
        }
        if (r.rows.length < FANOUT_PAGE) return m.ack();
        if (ctx.deadline - Date.now() < 20_000) {
          // Running out of wall time: continue in a new message from where we are.
          await enqueue(self, { type: "cbu.rate_changed", key: job.msg.key, changes: job.msg.changes, after }, {
            dedupId: `rates:${job.msg.key}:after:${after}`,
          });
          return m.ack();
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------------- admin

async function handleSetup(req: Request, cfg: Config): Promise<Response> {
  if (!cfg.botToken && !cfg.dryRun) return error(503, "not_configured", "set the TELEGRAM_BOT_TOKEN secret");
  if (!isValidSecretToken(cfg.webhookSecret)) {
    return error(503, "not_configured", "set TELEGRAM_WEBHOOK_SECRET: 16-256 characters of A-Z a-z 0-9 _ -");
  }
  const body = await readJson(req, 4096, true);
  if (!body.ok) return body.response;
  const opts = isObject(body.value) ? body.value : {};

  const url = typeof opts.url === "string"
    ? opts.url
    : cfg.publicUrl ? `${cfg.publicUrl.replace(/\/+$/, "")}/webhook` : null;
  if (!url) return error(400, "no_url", 'set PUBLIC_URL (e.g. https://fn.example.com/s/telegram-bot) or send {"url": "…"}');
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return error(400, "invalid_url", "url is not a valid URL");
  }
  if (parsed.protocol !== "https:" || parsed.username || parsed.password || url.length > 512) {
    return error(400, "invalid_url", "Telegram needs an https:// URL without credentials");
  }

  const outbox: OutboxEntry[] = [];
  const bot = botFor(cfg, outbox);
  try {
    const me = await bot.call<{ username?: string }>("getMe");
    if (me.username) await kv.set("bot/username", me.username);
    await bot.call("setWebhook", {
      url,
      secret_token: cfg.webhookSecret,
      allowed_updates: ["message"],
      max_connections: 20,
      drop_pending_updates: opts.dropPendingUpdates === true,
    });
    await bot.call("setMyCommands", { commands: COMMANDS });
    return json({ ok: true, bot: me.username ?? null, webhook: url, dryRun: cfg.dryRun });
  } catch (e) {
    if (e instanceof TelegramError) return error(502, "telegram_error", e.message);
    console.error("setup:", errorText(e));
    return error(502, "telegram_unreachable", "could not reach the Telegram Bot API");
  } finally {
    await ensureSchema().then(() => saveOutbox(outbox)).catch((e) => console.warn("outbox:", errorText(e)));
  }
}

async function handleStatus(cfg: Config): Promise<Response> {
  await ensureSchema();
  const counts = await one(
    `SELECT (SELECT COUNT(*) FROM notes) AS notes,
            (SELECT COUNT(*) FROM reminders WHERE status = 'pending') AS pending_reminders,
            (SELECT COUNT(*) FROM subscribers) AS subscribers`,
  );
  let webhook: unknown = null;
  try {
    webhook = await botFor(cfg, []).call("getWebhookInfo");
  } catch (e) {
    webhook = { error: e instanceof TelegramError ? e.message : "telegram unreachable" };
  }
  return json({ dryRun: cfg.dryRun, counts, webhook });
}

async function handleOutbox(url: URL): Promise<Response> {
  await ensureSchema();
  const chat = url.searchParams.get("chat_id");
  const r = await sql.execute("SELECT id, at, method, params FROM outbox ORDER BY id DESC LIMIT 100") as SqlResult;
  const entries = objects(r)
    .map((o) => ({ id: o.id, at: o.at, method: o.method, params: JSON.parse(String(o.params)) }))
    .filter((o) => chat === null || String(o.params?.chat_id) === chat);
  return json({ entries });
}

// ---------------------------------------------------------------------------------- entry

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    const cfg = config();
    try {
      if (path === "/webhook" && req.method === "POST") return await handleWebhook(req, cfg, ctx);

      if (path === "/" && req.method === "GET") {
        return json({
          service: "telegram-bot",
          dryRun: cfg.dryRun,
          configured: {
            botToken: Boolean(cfg.botToken),
            webhookSecret: isValidSecretToken(cfg.webhookSecret),
            adminToken: Boolean(cfg.adminToken),
            publicUrl: Boolean(cfg.publicUrl),
          },
        });
      }

      const admin = ["/setup", "/status", "/admin/outbox"].includes(path);
      if (!admin) return error(404, "not_found", "no such route");
      const denied = await requireAdmin(req, cfg.adminToken);
      if (denied) return denied;
      if (path === "/setup" && req.method === "POST") return await handleSetup(req, cfg);
      if (path === "/status" && req.method === "GET") return await handleStatus(cfg);
      if (path === "/admin/outbox" && req.method === "GET") return await handleOutbox(url);
      return error(405, "method_not_allowed", `${req.method} is not allowed here`);
    } catch (e) {
      console.error(errorText(e));
      return error(500, "internal", "internal error");
    }
  },

  async queue(batch: { messages: QueueMessage[] }, ctx: Ctx): Promise<void> {
    const cfg = config();
    if (!cfg.botToken && !cfg.dryRun) {
      // Not configured yet: keep the messages (they reach the DLQ after queue.max_attempts).
      console.warn("TELEGRAM_BOT_TOKEN is not set; delaying queue messages");
      for (const m of batch.messages) m.retry({ delaySeconds: 300 });
      return;
    }
    await ensureSchema();
    const outbox: OutboxEntry[] = [];
    const bot = botFor(cfg, outbox);
    for (const m of batch.messages) {
      const job = parseJob(m.body);
      if (!job) {
        console.warn(`dropping malformed message ${m.id}`); // retrying cannot fix it
        m.ack();
        continue;
      }
      try {
        await handleJob(m, job, bot, ctx);
      } catch (e) {
        console.error(`job ${job.type} (${m.id}, attempt ${m.attempt}) failed:`, errorText(e));
        if (e instanceof TelegramError && e.retryAfter) m.retry({ delaySeconds: e.retryAfter });
        else m.retry(); // runtime backoff, DLQ after queue.max_attempts
      }
    }
    await saveOutbox(outbox).catch((e) => console.warn("outbox:", errorText(e)));
  },
};
