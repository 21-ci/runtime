// Input validation for the events API. Pure functions: every check here runs under `node --test`.

export type Result<T> = { ok: true; value: T } | { ok: false; error: string };

const ok = <T>(value: T): Result<T> => ({ ok: true, value });
const fail = <T>(error: string): Result<T> => ({ ok: false, error });

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

// Trims and rejects control characters (newlines and tabs are kept only in multi-line fields).
export function cleanText(v: unknown, min: number, max: number, multiline = false): string | null {
  if (typeof v !== "string") return null;
  const s = v.normalize("NFC").trim();
  if (s.length < min || s.length > max) return null;
  const control = multiline ? /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/ : /[\u0000-\u001F\u007F]/;
  return control.test(s) ? null : s;
}

// ISO 8601 with an explicit offset ("2026-11-20T18:00:00+05:00" or "…Z") -> epoch seconds.
// A time without an offset is ambiguous for a public event, so it is refused.
export function parseStartsAt(v: unknown): number | null {
  if (typeof v !== "string" || v.length > 40) return null;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,3})?)?(Z|[+-]\d{2}:\d{2})$/.test(v)) return null;
  const ms = Date.parse(v);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

export interface EventInput {
  title: string;
  description: string;
  location: string;
  startsAt: number; // epoch seconds
  capacity: number;
}

const EVENT_FIELDS = new Set(["title", "description", "location", "starts_at", "capacity"]);
export const MAX_CAPACITY = 100_000;

function eventField(name: string, v: unknown, nowSec: number): Result<unknown> {
  switch (name) {
    case "title": {
      const s = cleanText(v, 1, 200);
      return s === null ? fail("title must be 1-200 characters on one line") : ok(s);
    }
    case "description": {
      const s = cleanText(v, 0, 5000, true);
      return s === null ? fail("description must be at most 5,000 characters") : ok(s);
    }
    case "location": {
      const s = cleanText(v, 0, 200);
      return s === null ? fail("location must be at most 200 characters on one line") : ok(s);
    }
    case "starts_at": {
      const t = parseStartsAt(v);
      if (t === null) return fail('starts_at must be ISO 8601 with an offset, e.g. "2026-11-20T18:00:00+05:00"');
      return t <= nowSec ? fail("starts_at must be in the future") : ok(t);
    }
    case "capacity":
      return Number.isInteger(v) && (v as number) >= 1 && (v as number) <= MAX_CAPACITY
        ? ok(v)
        : fail(`capacity must be an integer 1-${MAX_CAPACITY}`);
  }
  return fail(`unknown field ${name}`);
}

const KEY: Record<string, keyof EventInput> = {
  title: "title",
  description: "description",
  location: "location",
  starts_at: "startsAt",
  capacity: "capacity",
};

function unknownField(body: Record<string, unknown>): string | null {
  return Object.keys(body).find((k) => !EVENT_FIELDS.has(k)) ?? null;
}

export function validateEventCreate(body: unknown, nowSec: number): Result<EventInput> {
  if (!isObj(body)) return fail("body must be a JSON object");
  const extra = unknownField(body);
  if (extra) return fail(`unknown field ${extra}`);
  for (const required of ["title", "starts_at", "capacity"]) {
    if (!(required in body)) return fail(`${required} is required`);
  }
  const out: Record<string, unknown> = { description: "", location: "" };
  for (const [k, v] of Object.entries(body)) {
    const r = eventField(k, v, nowSec);
    if (!r.ok) return fail(r.error);
    out[KEY[k]] = r.value;
  }
  return ok(out as unknown as EventInput);
}

export function validateEventPatch(body: unknown, nowSec: number): Result<Partial<EventInput>> {
  if (!isObj(body)) return fail("body must be a JSON object");
  const extra = unknownField(body);
  if (extra) return fail(`unknown field ${extra}`);
  if (Object.keys(body).length === 0) return fail("nothing to change");
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(body)) {
    const r = eventField(k, v, nowSec);
    if (!r.ok) return fail(r.error);
    out[KEY[k]] = r.value;
  }
  return ok(out as Partial<EventInput>);
}

// ---------------------------------------------------------------------------------- registration

export interface RegistrationInput {
  name: string;
  email: string | null; // lower-case
  telegram: string | null; // lower-case, without "@"
}

// A practical check (not full RFC 5322): one "@", a dotted domain, no spaces, <= 254 chars.
export function normalizeEmail(v: unknown): string | null {
  if (typeof v !== "string") return null;
  const s = v.trim().toLowerCase();
  if (s.length > 254 || !/^[^\s@"<>()\[\],;:]{1,64}@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/.test(s)) {
    return null;
  }
  return s;
}

// Telegram usernames: 5-32 characters, letters, digits and underscores, starting with a letter.
export function normalizeTelegram(v: unknown): string | null {
  if (typeof v !== "string") return null;
  const s = v.trim().replace(/^@/, "").replace(/^https?:\/\/t\.me\//i, "");
  return /^[A-Za-z][A-Za-z0-9_]{4,31}$/.test(s) ? s.toLowerCase() : null;
}

export function validateRegistration(body: unknown): Result<RegistrationInput> {
  if (!isObj(body)) return fail("body must be a JSON object");
  const extra = Object.keys(body).find((k) => !["name", "email", "telegram"].includes(k));
  if (extra) return fail(`unknown field ${extra}`);
  const name = cleanText(body.name, 1, 100);
  if (name === null) return fail("name must be 1-100 characters on one line");
  const hasEmail = body.email !== undefined && body.email !== null && body.email !== "";
  const hasTg = body.telegram !== undefined && body.telegram !== null && body.telegram !== "";
  if (!hasEmail && !hasTg) return fail("give an email or a Telegram username");
  const email = hasEmail ? normalizeEmail(body.email) : null;
  if (hasEmail && email === null) return fail("email is not valid");
  const telegram = hasTg ? normalizeTelegram(body.telegram) : null;
  if (hasTg && telegram === null) return fail("telegram must be a username like @john_doe");
  return ok({ name, email, telegram });
}

// Client-chosen key for safe retries of POST …/registrations.
export function isValidIdempotencyKey(s: string): boolean {
  return /^[A-Za-z0-9_-]{8,128}$/.test(s);
}

export function isValidRegistrationToken(s: string): boolean {
  return /^evr_[A-Za-z0-9_-]{43}$/.test(s);
}
