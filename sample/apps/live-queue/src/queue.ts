// Pure logic of the electronic queue: ids, input validation, throughput and ETA estimates.

export const QUEUE_ID = /^[a-z0-9]{10}$/;
export const TICKET_TOKEN = /^lqt_[A-Za-z0-9_-]{32}$/;

// No 0/o, 1/l/i: the id is read aloud and typed from posters.
const ID_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789";

export function newQueueId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(10));
  // 256 % 31 != 0: draw again on the biased tail so every character is equally likely.
  let out = "";
  for (let i = 0; out.length < 10; i++) {
    const b = i < bytes.length ? bytes[i] : crypto.getRandomValues(new Uint8Array(1))[0];
    if (b < 248) out += ID_ALPHABET[b % 31];
  }
  return out;
}

export type Result<T> = { ok: true; value: T } | { ok: false; error: string };
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

function text(v: unknown, min: number, max: number): string | null {
  if (typeof v !== "string") return null;
  const s = v.normalize("NFC").trim();
  return s.length >= min && s.length <= max && !/[\u0000-\u001F\u007F]/.test(s) ? s : null;
}

export interface QueueInput {
  name: string;
  minutesPerPerson: number; // the estimate used until there are enough real calls
  maxWaiting: number;
}

export function validateQueue(body: unknown): Result<QueueInput> {
  if (!isObj(body)) return { ok: false, error: "body must be a JSON object" };
  const name = text(body.name, 1, 80);
  if (name === null) return { ok: false, error: "name must be 1-80 characters on one line" };
  const minutes = body.minutesPerPerson ?? 3;
  if (typeof minutes !== "number" || !(minutes >= 0.1 && minutes <= 240)) return { ok: false, error: "minutesPerPerson must be 0.1-240" };
  const maxWaiting = body.maxWaiting ?? 500;
  if (!Number.isInteger(maxWaiting) || (maxWaiting as number) < 1 || (maxWaiting as number) > 5000) {
    return { ok: false, error: "maxWaiting must be an integer 1-5000" };
  }
  return { ok: true, value: { name, minutesPerPerson: minutes, maxWaiting: maxWaiting as number } };
}

// The person's optional name, shown on the admin board.
export function validateTicket(body: unknown): Result<{ name: string }> {
  if (body === null || body === undefined) return { ok: true, value: { name: "" } };
  if (!isObj(body)) return { ok: false, error: "body must be a JSON object" };
  if (body.name === undefined || body.name === "") return { ok: true, value: { name: "" } };
  const name = text(body.name, 1, 40);
  return name === null ? { ok: false, error: "name must be 1-40 characters on one line" } : { ok: true, value: { name } };
}

export interface CallInput {
  count: number; // how many people to admit at once
  counter: string; // "window 3", "desk A"
}

export function validateCall(body: unknown): Result<CallInput> {
  const b = isObj(body) ? body : {};
  if (body !== null && body !== undefined && !isObj(body)) return { ok: false, error: "body must be a JSON object" };
  const count = b.count ?? 1;
  if (!Number.isInteger(count) || (count as number) < 1 || (count as number) > 20) return { ok: false, error: "count must be 1-20" };
  const counter = b.counter === undefined ? "1" : text(b.counter, 1, 20);
  if (counter === null) return { ok: false, error: "counter must be 1-20 characters" };
  return { ok: true, value: { count: count as number, counter } };
}

// ---------------------------------------------------------------------------------- estimates

export interface CallSample {
  at: number; // epoch ms
  admitted: number;
}

export const ETA_WINDOW_MS = 60 * 60_000;

// Seconds between admissions, from the calls of the last hour: the time from the first to the
// last call divided by the people admitted after the first one. This works for one counter or
// several, and for "call next 3" as well as one at a time. Fewer than 2 calls: the queue's own
// estimate. Clamped to 5 s … 2 h so one odd gap cannot produce absurd ETAs.
export function secondsPerPerson(calls: CallSample[], now: number, fallbackSeconds: number): number {
  const recent = calls.filter((c) => c.admitted > 0 && c.at >= now - ETA_WINDOW_MS && c.at <= now).sort((a, b) => a.at - b.at);
  if (recent.length < 2) return fallbackSeconds;
  const span = (recent[recent.length - 1].at - recent[0].at) / 1000;
  const people = recent.slice(1).reduce((sum, c) => sum + c.admitted, 0);
  return Math.min(7200, Math.max(5, span / people));
}

// The person with `ahead` people before them is admitted at the (ahead + 1)-th admission from now.
// The next admission is due one interval after the last call (or now, if that is overdue).
export function etaSeconds(ahead: number, spp: number, sinceLastCallSeconds: number | null): number {
  const untilNext = Math.max(0, spp - (sinceLastCallSeconds ?? 0));
  return Math.round(untilNext + ahead * spp);
}
