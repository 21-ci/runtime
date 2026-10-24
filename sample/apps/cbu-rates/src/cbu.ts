// Parsing and change detection for the Central Bank of Uzbekistan rate feed. Pure functions only.
//
// Feed: GET https://cbu.uz/uz/arkhiv-kursov-valyut/json/            all currencies, current rates
//       GET https://cbu.uz/uz/arkhiv-kursov-valyut/json/USD/        one currency
//       GET https://cbu.uz/uz/arkhiv-kursov-valyut/json/all/2026-10-01/   all, as of a date
// Item: {"id":68,"Code":"840","Ccy":"USD","CcyNm_EN":"US Dollar","Nominal":"1","Rate":"11778.45",
//        "Diff":"5.5","Date":"06.10.2026", …}. `Rate` is UZS per `Nominal` units; `Date` is dd.mm.yyyy.

export interface CbuRate {
  ccy: string; // ISO 4217 letters
  code: string; // ISO 4217 number, "" if missing
  name: string;
  nominal: number;
  rate: string; // canonical decimal string, UZS per `nominal` units
  diff: string; // canonical signed decimal string
  date: string; // YYYY-MM-DD
}

export interface Latest {
  ccy: string;
  date: string;
  rate: string;
}

export interface Change extends CbuRate {
  previous: string | null; // null = first value seen for this currency
  previousDate: string | null;
}

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

// "0011778.450" -> "11778.45", "-0.00" -> "0". Strings or finite numbers; at most 12 integer and 8
// fraction digits. Returns null for anything else (empty, "1e5", "12,5", "abc").
export function normalizeDecimal(v: unknown, allowNegative = false): string | null {
  const s = typeof v === "number" && Number.isFinite(v) ? String(v) : typeof v === "string" ? v.trim() : null;
  if (s === null) return null;
  const m = /^([+-]?)(\d{1,12})(?:\.(\d{1,8}))?$/.exec(s);
  if (!m) return null;
  const negative = m[1] === "-";
  if (negative && !allowNegative) return null;
  const int = m[2].replace(/^0+(?=\d)/, "");
  const frac = (m[3] ?? "").replace(/0+$/, "");
  const abs = frac ? `${int}.${frac}` : int;
  return negative && abs !== "0" ? `-${abs}` : abs;
}

// "06.10.2026" -> "2026-10-06"; null for impossible or implausible dates.
export function cbuDateToIso(v: unknown): string | null {
  if (typeof v !== "string") return null;
  const m = /^(\d{2})\.(\d{2})\.(\d{4})$/.exec(v.trim());
  if (!m) return null;
  const [d, mo, y] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (y < 1994 || y > 2100) return null; // the som was introduced in 1994
  const t = new Date(Date.UTC(y, mo - 1, d));
  if (t.getUTCFullYear() !== y || t.getUTCMonth() !== mo - 1 || t.getUTCDate() !== d) return null;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function parseNominal(v: unknown): number | null {
  const n = typeof v === "number" ? v : typeof v === "string" && /^\d{1,7}$/.test(v.trim()) ? Number(v) : NaN;
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}

// Validates the whole payload item by item: bad items are skipped and reported, good ones kept.
export function parseCbuPayload(raw: unknown, maxItems = 500): { rates: CbuRate[]; errors: string[] } {
  const rates: CbuRate[] = [];
  const errors: string[] = [];
  if (!Array.isArray(raw)) return { rates, errors: ["payload is not a JSON array"] };
  if (raw.length > maxItems) return { rates, errors: [`payload has ${raw.length} items (max ${maxItems})`] };
  const seen = new Set<string>();
  raw.forEach((item, i) => {
    if (!isObj(item)) return void errors.push(`#${i}: not an object`);
    const ccy = item.Ccy;
    if (typeof ccy !== "string" || !/^[A-Z]{3}$/.test(ccy)) return void errors.push(`#${i}: bad Ccy`);
    const rate = normalizeDecimal(item.Rate);
    if (rate === null || rate === "0") return void errors.push(`${ccy}: bad Rate`);
    const nominal = parseNominal(item.Nominal);
    if (nominal === null) return void errors.push(`${ccy}: bad Nominal`);
    const date = cbuDateToIso(item.Date);
    if (date === null) return void errors.push(`${ccy}: bad Date`);
    if (seen.has(ccy)) return void errors.push(`${ccy}: duplicate`);
    seen.add(ccy);
    rates.push({
      ccy,
      code: typeof item.Code === "string" && /^\d{3}$/.test(item.Code) ? item.Code : "",
      name: typeof item.CcyNm_EN === "string" && item.CcyNm_EN.trim() ? item.CcyNm_EN.trim().slice(0, 100) : ccy,
      nominal,
      rate,
      diff: normalizeDecimal(item.Diff, true) ?? "0", // informational only: tolerate a missing Diff
      date,
    });
  });
  return { rates, errors };
}

// Compares fetched rates with the stored latest ones.
//   forward:  a new currency, a newer date, or a corrected rate for the same date
//             -> history row + new latest value
//   older:    a date before the stored one (backfill, a lagging mirror) -> history only
//   unchanged rates produce nothing.
export function detectChanges(latest: Map<string, Latest>, incoming: CbuRate[]): { forward: Change[]; older: CbuRate[] } {
  const forward: Change[] = [];
  const older: CbuRate[] = [];
  for (const r of incoming) {
    const prev = latest.get(r.ccy);
    if (!prev) {
      forward.push({ ...r, previous: null, previousDate: null });
    } else if (r.date < prev.date) {
      older.push(r);
    } else if (r.date !== prev.date || r.rate !== prev.rate) {
      forward.push({ ...r, previous: prev.rate, previousDate: prev.date });
    }
  }
  return { forward, older };
}

// Which changes users should hear about: a known currency whose rate actually moved.
export function notifiable(changes: Change[], ccys: string[]): Change[] {
  return changes.filter((c) => c.previous !== null && c.rate !== c.previous && ccys.includes(c.ccy));
}

// Stable id of a change set, used as the queue dedupId (the same change is announced once).
export async function changeKey(changes: Change[]): Promise<string> {
  const text = changes.map((c) => `${c.ccy}:${c.date}:${c.rate}`).sort().join("|");
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)));
  return `cbu:${Array.from(digest.slice(0, 16), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

// "usd, EUR" -> ["USD", "EUR"]; null if any entry is not a currency code.
export function parseCcyList(s: string | null | undefined, max = 30): string[] | null {
  if (!s) return null;
  const list = [...new Set(s.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean))];
  if (list.length === 0 || list.length > max || !list.every((c) => /^[A-Z]{3}$/.test(c))) return null;
  return list;
}

// Calendar date in a fixed UTC offset (Tashkent is UTC+5 all year), `days` days back.
export function isoDate(now: number, daysBack = 0, tzOffsetMin = 300): string {
  return new Date(now + tzOffsetMin * 60_000 - daysBack * 86_400_000).toISOString().slice(0, 10);
}

export const perUnit = (rate: string, nominal: number) => Number(rate) / nominal;

// amount × (UZS per unit of `from`) ÷ (UZS per unit of `to`); UZS itself is 1.
export function convert(amount: number, fromPerUnit: number, toPerUnit: number): number {
  return Math.round((amount * fromPerUnit / toPerUnit) * 1e4) / 1e4;
}

// ---------------------------------------------------------------------------------- outages

export interface FetchStatus {
  lastAttemptAt?: number;
  lastSuccessAt?: number;
  consecutiveFailures?: number;
  lastError?: string | null;
}

// Circuit breaker for the 60 s schedule: after 3 failures in a row, wait 1, 2, 4 … up to 30 min
// between attempts, so an outage at the CBU is not hammered every minute.
export function nextAttemptAt(s: FetchStatus): number {
  const failures = s.consecutiveFailures ?? 0;
  if (failures < 3 || !s.lastAttemptAt) return 0;
  return s.lastAttemptAt + Math.min(60_000 * 2 ** (failures - 3), 30 * 60_000);
}
