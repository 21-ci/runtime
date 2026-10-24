// Run: node --test test/*.test.ts   (Node >= 22.18)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type CbuRate,
  cbuDateToIso,
  changeKey,
  convert,
  detectChanges,
  isoDate,
  type Latest,
  nextAttemptAt,
  normalizeDecimal,
  notifiable,
  parseCbuPayload,
  parseCcyList,
} from "../src/cbu.ts";
import { readTextCapped } from "../src/http.ts";

// Shape of the real feed (2026-10-06), trimmed.
const SAMPLE = [
  { id: 68, Code: "840", Ccy: "USD", CcyNm_EN: "US Dollar", Nominal: "1", Rate: "11778.45", Diff: "5.5", Date: "06.10.2026" },
  { id: 20, Code: "978", Ccy: "EUR", CcyNm_EN: "Euro", Nominal: "1", Rate: "13198.93", Diff: "-25.62", Date: "06.10.2026" },
  { id: 30, Code: "360", Ccy: "IDR", CcyNm_EN: "Indonesian Rupiah", Nominal: "10", Rate: "7.12", Diff: "0", Date: "06.10.2026" },
];

test("normalizeDecimal", () => {
  assert.equal(normalizeDecimal("11778.450"), "11778.45");
  assert.equal(normalizeDecimal("0012.0"), "12");
  assert.equal(normalizeDecimal("0.28"), "0.28");
  assert.equal(normalizeDecimal(12.5), "12.5");
  assert.equal(normalizeDecimal("-25.62"), null, "negative needs allowNegative");
  assert.equal(normalizeDecimal("-25.62", true), "-25.62");
  assert.equal(normalizeDecimal("-0.00", true), "0");
  for (const bad of ["", "1e5", "12,5", "abc", " ", null, NaN, "1.123456789"]) assert.equal(normalizeDecimal(bad), null, String(bad));
});

test("cbuDateToIso", () => {
  assert.equal(cbuDateToIso("06.10.2026"), "2026-10-06");
  assert.equal(cbuDateToIso("29.02.2028"), "2028-02-29");
  assert.equal(cbuDateToIso("29.02.2027"), null);
  assert.equal(cbuDateToIso("2026-10-06"), null);
  assert.equal(cbuDateToIso("01.01.1990"), null);
});

test("parseCbuPayload keeps good items and reports bad ones", () => {
  const { rates, errors } = parseCbuPayload([
    ...SAMPLE,
    { Ccy: "XXX", Rate: "abc", Nominal: "1", Date: "06.10.2026" },
    { Ccy: "usd", Rate: "1", Nominal: "1", Date: "06.10.2026" },
    { Ccy: "USD", Rate: "1", Nominal: "1", Date: "06.10.2026" },
    { Ccy: "GBP", Rate: "15000", Nominal: "0", Date: "06.10.2026" },
    "junk",
  ]);
  assert.deepEqual(rates.map((r) => r.ccy), ["USD", "EUR", "IDR"]);
  assert.deepEqual(rates[0], { ccy: "USD", code: "840", name: "US Dollar", nominal: 1, rate: "11778.45", diff: "5.5", date: "2026-10-06" });
  assert.equal(rates[2].nominal, 10);
  assert.equal(errors.length, 5);
  assert.deepEqual(parseCbuPayload({ not: "an array" }).rates, []);
  assert.deepEqual(parseCbuPayload([]).rates, []);
  assert.equal(parseCbuPayload(new Array(501).fill(SAMPLE[0])).rates.length, 0);
});

const rate = (ccy: string, date: string, r: string): CbuRate => ({ ccy, code: "", name: ccy, nominal: 1, rate: r, diff: "0", date });

test("detectChanges: new, unchanged, newer date, correction, older", () => {
  const latest = new Map<string, Latest>([
    ["USD", { ccy: "USD", date: "2026-10-06", rate: "11778.45" }],
    ["EUR", { ccy: "EUR", date: "2026-10-06", rate: "13198.93" }],
    ["RUB", { ccy: "RUB", date: "2026-10-06", rate: "145.1" }],
    ["GBP", { ccy: "GBP", date: "2026-10-06", rate: "15000" }],
  ]);
  const { forward, older } = detectChanges(latest, [
    rate("USD", "2026-10-06", "11778.45"), // unchanged
    rate("EUR", "2026-10-07", "13200"), // newer date
    rate("RUB", "2026-10-06", "145.2"), // corrected same day
    rate("GBP", "2026-10-05", "14990"), // older (backfill)
    rate("CNY", "2026-10-07", "1650"), // first value
  ]);
  assert.deepEqual(forward.map((c) => [c.ccy, c.previous]), [["EUR", "13198.93"], ["RUB", "145.1"], ["CNY", null]]);
  assert.deepEqual(older.map((r) => r.ccy), ["GBP"]);
});

test("notifiable: only known currencies whose rate moved", () => {
  const changes = detectChanges(
    new Map([
      ["USD", { ccy: "USD", date: "2026-10-06", rate: "11778.45" }],
      ["EUR", { ccy: "EUR", date: "2026-10-06", rate: "13198.93" }],
    ]),
    [rate("USD", "2026-10-07", "11778.45"), rate("EUR", "2026-10-07", "13200"), rate("CNY", "2026-10-07", "1650")],
  ).forward;
  // USD: same rate on a new date (stored, not announced); CNY: first value; EUR: moved.
  assert.deepEqual(notifiable(changes, ["USD", "EUR", "CNY"]).map((c) => c.ccy), ["EUR"]);
  assert.deepEqual(notifiable(changes, ["USD"]), []);
});

test("changeKey is stable and order-independent", async () => {
  const a = { ...rate("USD", "2026-10-07", "1"), previous: "2", previousDate: "2026-10-06" };
  const b = { ...rate("EUR", "2026-10-07", "3"), previous: "4", previousDate: "2026-10-06" };
  const k1 = await changeKey([a, b]);
  assert.equal(k1, await changeKey([b, a]));
  assert.match(k1, /^cbu:[0-9a-f]{32}$/);
  assert.notEqual(k1, await changeKey([a]));
});

test("helpers", () => {
  assert.deepEqual(parseCcyList("usd, EUR,usd"), ["USD", "EUR"]);
  assert.equal(parseCcyList("USD,EURO"), null);
  assert.equal(parseCcyList(""), null);
  assert.equal(parseCcyList("USD,EUR", 1), null);
  assert.equal(convert(100, 11778.45, 1), 1177845);
  assert.equal(convert(1000, 1, 11778.45), 0.0849);
  // 2026-10-06 21:00 UTC is already 2026-10-07 in Tashkent
  assert.equal(isoDate(Date.UTC(2026, 9, 6, 21, 0)), "2026-10-07");
  assert.equal(isoDate(Date.UTC(2026, 9, 6, 21, 0), 30), "2026-09-07");
});

test("nextAttemptAt backs off after 3 failures, capped at 30 min", () => {
  const t = 1_000_000;
  assert.equal(nextAttemptAt({}), 0);
  assert.equal(nextAttemptAt({ consecutiveFailures: 2, lastAttemptAt: t }), 0);
  assert.equal(nextAttemptAt({ consecutiveFailures: 3, lastAttemptAt: t }), t + 60_000);
  assert.equal(nextAttemptAt({ consecutiveFailures: 5, lastAttemptAt: t }), t + 240_000);
  assert.equal(nextAttemptAt({ consecutiveFailures: 40, lastAttemptAt: t }), t + 30 * 60_000);
});

test("readTextCapped", async () => {
  const body = (s: string) => new Response(s).body;
  assert.equal(await readTextCapped(body("héllo"), 100), "héllo");
  assert.equal(await readTextCapped(body("x".repeat(101)), 100), null);
  assert.equal(await readTextCapped(null, 100), "");
});
