// The bot's conversation logic: command parsing, the per-chat state machine, time parsing and
// message formatting. Pure functions only (no I/O, no std:* imports): the handler in main.ts loads
// the state, calls `transition`, runs the returned effect and stores the next state.

export const MAX_NOTE_CHARS = 2000;
export const MAX_REMINDER_CHARS = 500;
export const MIN_REMIND_AHEAD_MS = 60_000;
// std:queue delivers a delayed message at most 7 days ahead.
export const MAX_REMIND_AHEAD_MS = 7 * 24 * 3600_000;
export const TELEGRAM_TEXT_LIMIT = 4096;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

// ---------------------------------------------------------------------------------- state

// Idle is "no state" (null). Every other state is one step of a dialog.
export type State =
  | { step: "note_text" }
  | { step: "remind_text" }
  | { step: "remind_when"; text: string }
  | { step: "remind_confirm"; text: string; dueAt: number };

export type Effect =
  | { kind: "save_note"; text: string }
  | { kind: "list_notes" }
  | { kind: "delete_note"; id: number }
  | { kind: "create_reminder"; text: string; dueAt: number }
  | { kind: "list_reminders" }
  | { kind: "cancel_reminder"; id: number }
  | { kind: "subscribe" }
  | { kind: "unsubscribe" }
  | { kind: "rates" };

export interface Step {
  state: State | null; // the next state; null = idle
  reply?: string; // plain text to send (no parse_mode, so nothing needs escaping)
  effect?: Effect; // run by the handler; its result becomes the reply
}

export interface Env {
  now: number; // epoch ms
  tzOffsetMin: number; // the users' fixed UTC offset, e.g. 300 for Tashkent (UTC+5)
  botUsername?: string | null; // for "/cmd@bot" in groups
  privateChat: boolean;
}

// Validates a state read back from KV (it may be stale or from an older version of the bot).
export function isState(v: unknown): v is State {
  if (typeof v !== "object" || v === null) return false;
  const s = v as Record<string, unknown>;
  switch (s.step) {
    case "note_text":
    case "remind_text":
      return true;
    case "remind_when":
      return typeof s.text === "string";
    case "remind_confirm":
      return typeof s.text === "string" && Number.isSafeInteger(s.dueAt);
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------------- commands

export interface Command {
  name: string;
  arg: string;
}

// "/note@my_bot buy milk" -> {name: "note", arg: "buy milk"}. A command addressed to another bot
// (a group with several bots) is "foreign"; plain text is null.
export function parseCommand(text: string, botUsername?: string | null): Command | "foreign" | null {
  const m = /^\/([A-Za-z0-9_]{1,32})(?:@([A-Za-z0-9_]{3,64}))?(?:\s+([\s\S]*))?$/.exec(text.trim());
  if (!m) return null;
  if (m[2] && botUsername && m[2].toLowerCase() !== botUsername.toLowerCase()) return "foreign";
  return { name: m[1].toLowerCase(), arg: (m[3] ?? "").trim() };
}

export const COMMANDS: { command: string; description: string }[] = [
  { command: "note", description: "Save a note" },
  { command: "notes", description: "Your latest notes" },
  { command: "delnote", description: "Delete a note: /delnote <id>" },
  { command: "remind", description: "Set a reminder" },
  { command: "reminders", description: "Pending reminders" },
  { command: "unremind", description: "Cancel a reminder: /unremind <id>" },
  { command: "rates", description: "Today's CBU exchange rates" },
  { command: "subscribe", description: "Get CBU rate changes" },
  { command: "unsubscribe", description: "Stop CBU rate changes" },
  { command: "cancel", description: "Stop the current dialog" },
  { command: "help", description: "What I can do" },
];

export const HELP = [
  "I keep notes and reminders, and I watch the Central Bank of Uzbekistan exchange rates.",
  "",
  ...COMMANDS.map((c) => `/${c.command} - ${c.description}`),
  "",
  "Quick forms: /note buy milk, /remind 2h call mom.",
].join("\n");

const HINT = "I did not get that. Send /help to see what I can do.";

export function whenPrompt(tzOffsetMin: number): string {
  return `When? For example: 10m, 2h 30m, 1d, 18:30, tomorrow 9:00, 2026-12-31 23:00 (times are ${formatOffset(tzOffsetMin)}).`;
}

// ---------------------------------------------------------------------------------- transition

export function transition(state: State | null, rawText: string, env: Env): Step {
  const text = rawText.trim();
  const cmd = parseCommand(text, env.botUsername);
  if (cmd === "foreign") return { state }; // not for us: keep the dialog as it is, say nothing
  if (cmd) return onCommand(state, cmd, env);

  if (state === null) return env.privateChat ? { state: null, reply: HINT } : { state: null };

  switch (state.step) {
    case "note_text":
      return saveNote(text, state);

    case "remind_text": {
      if (!text) return { state, reply: "What should I remind you about?" };
      if (text.length > MAX_REMINDER_CHARS) {
        return { state, reply: `That is too long (max ${MAX_REMINDER_CHARS} characters). Try a shorter text.` };
      }
      return { state: { step: "remind_when", text }, reply: whenPrompt(env.tzOffsetMin) };
    }

    case "remind_when": {
      const when = parseWhen(text, env.now, env.tzOffsetMin);
      if (!when.ok) return { state, reply: `${when.error} ${whenPrompt(env.tzOffsetMin)}` };
      const range = checkRange(when.at, env.now);
      if (range) return { state, reply: range };
      return {
        state: { step: "remind_confirm", text: state.text, dueAt: when.at },
        reply: `Remind you on ${formatLocal(when.at, env.tzOffsetMin)} (${formatIn(when.at - env.now)}):\n` +
          `"${state.text}"\nReply yes or no.`,
      };
    }

    case "remind_confirm": {
      const answer = text.toLowerCase();
      if (YES.has(answer)) {
        // The time may have passed while the user was thinking.
        if (state.dueAt - env.now < MIN_REMIND_AHEAD_MS / 2) {
          return { state: { step: "remind_when", text: state.text }, reply: `That time has passed. ${whenPrompt(env.tzOffsetMin)}` };
        }
        return { state: null, effect: { kind: "create_reminder", text: state.text, dueAt: state.dueAt } };
      }
      if (NO.has(answer)) return { state: null, reply: "OK, not saved." };
      return { state, reply: "Please answer yes or no (or /cancel)." };
    }
  }
}

const YES = new Set(["yes", "y", "ok", "okay", "sure", "ha", "xa", "да", "ага"]);
const NO = new Set(["no", "n", "nope", "yo'q", "yoq", "нет"]);

function onCommand(state: State | null, cmd: Command, env: Env): Step {
  // Any command ends the current dialog.
  switch (cmd.name) {
    case "start":
    case "help":
      return { state: null, reply: HELP };
    case "cancel":
      return { state: null, reply: state ? "Cancelled." : "Nothing to cancel." };
    case "note":
      return cmd.arg ? saveNote(cmd.arg, null) : { state: { step: "note_text" }, reply: "Send me the note text (or /cancel)." };
    case "notes":
      return { state: null, effect: { kind: "list_notes" } };
    case "delnote": {
      const id = parseId(cmd.arg);
      return id ? { state: null, effect: { kind: "delete_note", id } } : { state: null, reply: "Usage: /delnote <id> (see /notes)." };
    }
    case "remind":
      return cmd.arg ? quickReminder(cmd.arg, env) : { state: { step: "remind_text" }, reply: "What should I remind you about? (or /cancel)" };
    case "reminders":
      return { state: null, effect: { kind: "list_reminders" } };
    case "unremind": {
      const id = parseId(cmd.arg);
      return id ? { state: null, effect: { kind: "cancel_reminder", id } } : { state: null, reply: "Usage: /unremind <id> (see /reminders)." };
    }
    case "rates":
      return { state: null, effect: { kind: "rates" } };
    case "subscribe":
      return { state: null, effect: { kind: "subscribe" } };
    case "unsubscribe":
      return { state: null, effect: { kind: "unsubscribe" } };
    default:
      return env.privateChat ? { state: null, reply: `Unknown command /${cmd.name}. Send /help.` } : { state: null };
  }
}

function saveNote(text: string, state: State | null): Step {
  if (!text) return { state: state ?? { step: "note_text" }, reply: "The note is empty. Send the text (or /cancel)." };
  if (text.length > MAX_NOTE_CHARS) {
    return { state: state ?? { step: "note_text" }, reply: `That is too long (max ${MAX_NOTE_CHARS} characters).` };
  }
  return { state: null, effect: { kind: "save_note", text } };
}

// "/remind 2h 30m call mom": the longest leading run of words that parses as a time, the rest is the text.
function quickReminder(arg: string, env: Env): Step {
  const words = arg.split(/\s+/);
  for (let n = Math.min(words.length - 1, 4); n >= 1; n--) {
    const when = parseWhen(words.slice(0, n).join(" "), env.now, env.tzOffsetMin);
    if (!when.ok) continue;
    const text = words.slice(n).join(" ");
    if (text.length > MAX_REMINDER_CHARS) return { state: null, reply: `The text is too long (max ${MAX_REMINDER_CHARS} characters).` };
    const range = checkRange(when.at, env.now);
    if (range) return { state: null, reply: range };
    return { state: null, effect: { kind: "create_reminder", text, dueAt: when.at } };
  }
  // No time in front: treat everything as the text and ask when.
  if (arg.length > MAX_REMINDER_CHARS) return { state: null, reply: `The text is too long (max ${MAX_REMINDER_CHARS} characters).` };
  return { state: { step: "remind_when", text: arg }, reply: whenPrompt(env.tzOffsetMin) };
}

function checkRange(at: number, now: number): string | null {
  if (at - now < MIN_REMIND_AHEAD_MS) return "That is too soon or in the past (at least 1 minute ahead).";
  if (at - now > MAX_REMIND_AHEAD_MS) return "That is too far ahead (at most 7 days).";
  return null;
}

function parseId(s: string): number | null {
  const m = /^#?(\d{1,15})$/.exec(s.trim());
  if (!m) return null;
  const id = Number(m[1]);
  return id > 0 ? id : null;
}

// ---------------------------------------------------------------------------------- time

export type WhenResult = { ok: true; at: number } | { ok: false; error: string };

const UNITS: Record<string, number> = {
  s: 1000, sec: 1000, secs: 1000, second: 1000, seconds: 1000,
  m: MINUTE, min: MINUTE, mins: MINUTE, minute: MINUTE, minutes: MINUTE,
  h: HOUR, hr: HOUR, hrs: HOUR, hour: HOUR, hours: HOUR,
  d: DAY, day: DAY, days: DAY,
};

// Parses a reminder time in the user's fixed UTC offset. Accepted:
//   relative   "10m", "in 2 hours", "1h30m", "1d 2h"
//   time       "18:30" (today, or tomorrow if it has passed), "today 18:30", "tomorrow", "tomorrow 9:00"
//   date       "2026-12-31 23:00", "31.12.2026 23:00", "2026-12-31" (09:00)
export function parseWhen(input: string, now: number, tzOffsetMin: number): WhenResult {
  let s = input.trim().toLowerCase().replace(/\s+/g, " ");
  if (!s) return { ok: false, error: "I need a time." };
  if (s.startsWith("in ")) s = s.slice(3);

  // Relative: a sequence of <number><unit> pairs that covers the whole input.
  if (/^\d/.test(s)) {
    const re = /(\d{1,5}) ?([a-z]+) ?/gy;
    let total = 0;
    let consumed = 0;
    let m: RegExpExecArray | null;
    while ((m = re.exec(s)) !== null) {
      const unit = UNITS[m[2]];
      if (unit === undefined) break;
      total += Number(m[1]) * unit;
      consumed = re.lastIndex;
    }
    if (consumed === s.length && total > 0) return { ok: true, at: now + total };
  }

  const local = new Date(now + tzOffsetMin * MINUTE); // "now" as wall-clock fields in UTC getters
  const today = { y: local.getUTCFullYear(), mo: local.getUTCMonth() + 1, d: local.getUTCDate() };

  let m = /^(today |tomorrow )?(\d{1,2})[:.](\d{2})$/.exec(s);
  if (m) {
    const at = localToEpoch(today.y, today.mo, today.d, Number(m[2]), Number(m[3]), tzOffsetMin);
    if (at === null) return { ok: false, error: "That is not a valid time." };
    if (m[1] === "tomorrow ") return { ok: true, at: at + DAY };
    if (m[1] === "today ") return { ok: true, at };
    return { ok: true, at: at > now ? at : at + DAY };
  }
  if (s === "tomorrow") {
    const at = localToEpoch(today.y, today.mo, today.d, 9, 0, tzOffsetMin)!;
    return { ok: true, at: at + DAY };
  }

  m = /^(\d{4})-(\d{2})-(\d{2})(?:[ t](\d{1,2}):(\d{2}))?$/.exec(s);
  if (m) return dateResult(Number(m[1]), Number(m[2]), Number(m[3]), m[4], m[5], tzOffsetMin);
  m = /^(\d{1,2})\.(\d{1,2})\.(\d{4})(?: (\d{1,2})[:.](\d{2}))?$/.exec(s);
  if (m) return dateResult(Number(m[3]), Number(m[2]), Number(m[1]), m[4], m[5], tzOffsetMin);

  return { ok: false, error: "I could not read that time." };
}

function dateResult(y: number, mo: number, d: number, h: string | undefined, mi: string | undefined, tz: number): WhenResult {
  const at = localToEpoch(y, mo, d, h === undefined ? 9 : Number(h), mi === undefined ? 0 : Number(mi), tz);
  return at === null ? { ok: false, error: "That is not a valid date." } : { ok: true, at };
}

// Wall-clock time in a fixed UTC offset -> epoch ms; null for impossible dates (Feb 30, 25:00).
export function localToEpoch(y: number, mo: number, d: number, h: number, mi: number, tzOffsetMin: number): number | null {
  if (h > 23 || mi > 59) return null;
  const t = Date.UTC(y, mo - 1, d, h, mi);
  const back = new Date(t);
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return t - tzOffsetMin * MINUTE;
}

const pad = (n: number) => String(n).padStart(2, "0");

export function formatOffset(tzOffsetMin: number): string {
  const sign = tzOffsetMin < 0 ? "-" : "+";
  const a = Math.abs(tzOffsetMin);
  return `UTC${sign}${pad(Math.floor(a / 60))}:${pad(a % 60)}`;
}

export function formatLocal(at: number, tzOffsetMin: number): string {
  const t = new Date(at + tzOffsetMin * MINUTE);
  return `${t.getUTCFullYear()}-${pad(t.getUTCMonth() + 1)}-${pad(t.getUTCDate())} ` +
    `${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())} ${formatOffset(tzOffsetMin)}`;
}

// "in 2 h 5 min", "in 3 d 4 h", "in 45 min"
export function formatIn(ms: number): string {
  const totalMin = Math.max(0, Math.round(ms / MINUTE));
  const d = Math.floor(totalMin / 1440);
  const h = Math.floor((totalMin % 1440) / 60);
  const mi = totalMin % 60;
  const parts: string[] = [];
  if (d) parts.push(`${d} d`);
  if (h) parts.push(`${h} h`);
  if (mi && !d) parts.push(`${mi} min`);
  return parts.length ? `in ${parts.join(" ")}` : "in less than a minute";
}

// ---------------------------------------------------------------------------------- rates

export interface RateChange {
  ccy: string;
  date: string; // YYYY-MM-DD
  rate: string; // UZS per `nominal` units, decimal string
  diff: string;
  nominal: number;
  previous?: string | null;
}

export interface RatesMessage {
  key: string; // stable id of this change set (dedup)
  changes: RateChange[];
}

// Validates a `cbu.rate_changed` queue message (sent by the cbu-rates sample).
export function parseRatesMessage(body: unknown): RatesMessage | null {
  if (typeof body !== "object" || body === null) return null;
  const b = body as Record<string, unknown>;
  if (b.type !== "cbu.rate_changed" || typeof b.key !== "string" || !/^[A-Za-z0-9:._-]{1,80}$/.test(b.key)) return null;
  if (!Array.isArray(b.changes) || b.changes.length === 0 || b.changes.length > 200) return null;
  const changes: RateChange[] = [];
  for (const c of b.changes) {
    if (typeof c !== "object" || c === null) return null;
    const r = c as Record<string, unknown>;
    if (typeof r.ccy !== "string" || !/^[A-Z]{3}$/.test(r.ccy)) return null;
    if (typeof r.date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(r.date)) return null;
    if (typeof r.rate !== "string" || !/^\d{1,12}(\.\d{1,8})?$/.test(r.rate)) return null;
    const diff = typeof r.diff === "string" && /^-?\d{1,12}(\.\d{1,8})?$/.test(r.diff) ? r.diff : "0";
    const nominal = Number.isSafeInteger(r.nominal) && (r.nominal as number) > 0 ? r.nominal as number : 1;
    const previous = typeof r.previous === "string" ? r.previous : null;
    changes.push({ ccy: r.ccy, date: r.date, rate: r.rate, diff, nominal, previous });
  }
  return { key: b.key, changes };
}

// "11778.45" -> "11 778.45"
export function groupDigits(decimal: string): string {
  const [int, frac] = decimal.split(".");
  const grouped = int.replace(/\B(?=(\d{3})+(?!\d))/g, " ");
  return frac === undefined ? grouped : `${grouped}.${frac}`;
}

function signed(diff: string): string {
  return diff.startsWith("-") ? diff : diff === "0" ? "0" : `+${diff}`;
}

export function formatRateLines(rates: { ccy: string; rate: string; diff: string; nominal: number }[]): string[] {
  return rates.map((r) => `${r.nominal === 1 ? "" : `${r.nominal} `}${r.ccy}  ${groupDigits(r.rate)} UZS  (${signed(r.diff)})`);
}

export function formatRateChanges(msg: RatesMessage): string {
  const dates = [...new Set(msg.changes.map((c) => c.date))].sort();
  const head = `CBU rates changed (${dates.join(", ")}):`;
  return clip([head, ...formatRateLines(msg.changes), "", "/unsubscribe to stop these messages."].join("\n"));
}

export function clip(text: string, limit = TELEGRAM_TEXT_LIMIT): string {
  return text.length <= limit ? text : `${text.slice(0, limit - 1)}…`;
}
