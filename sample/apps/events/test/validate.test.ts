// Run: node --test test/*.test.ts   (Node >= 22.18)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  cleanText,
  isValidIdempotencyKey,
  isValidRegistrationToken,
  normalizeEmail,
  normalizeTelegram,
  parseStartsAt,
  validateEventCreate,
  validateEventPatch,
  validateRegistration,
} from "../src/validate.ts";
import { csvCell, csvLine, toCsv } from "../src/csv.ts";
import { randomToken, sha256Hex } from "../src/http.ts";

const NOW = Math.floor(Date.UTC(2026, 9, 7) / 1000);

test("parseStartsAt needs an explicit offset", () => {
  assert.equal(parseStartsAt("2026-11-20T18:00:00+05:00"), Date.UTC(2026, 10, 20, 13) / 1000);
  assert.equal(parseStartsAt("2026-11-20T13:00Z"), Date.UTC(2026, 10, 20, 13) / 1000);
  assert.equal(parseStartsAt("2026-11-20T18:00:00"), null);
  assert.equal(parseStartsAt("2026-11-20"), null);
  assert.equal(parseStartsAt("next friday"), null);
  assert.equal(parseStartsAt(1790000000), null);
});

test("cleanText", () => {
  assert.equal(cleanText("  Rust meetup ", 1, 200), "Rust meetup");
  assert.equal(cleanText("a\nb", 1, 200), null);
  assert.equal(cleanText("a\nb", 1, 200, true), "a\nb");
  assert.equal(cleanText("bell\u0007", 1, 200, true), null);
  assert.equal(cleanText("", 1, 200), null);
  assert.equal(cleanText("x".repeat(201), 1, 200), null);
});

test("validateEventCreate", () => {
  const good = { title: "Rust meetup", starts_at: "2026-11-20T18:00:00+05:00", capacity: 40, description: "Talks\nand pizza" };
  assert.deepEqual(validateEventCreate(good, NOW), {
    ok: true,
    value: { title: "Rust meetup", startsAt: Date.UTC(2026, 10, 20, 13) / 1000, capacity: 40, description: "Talks\nand pizza", location: "" },
  });
  const err = (b: unknown) => {
    const r = validateEventCreate(b, NOW);
    return r.ok ? null : r.error;
  };
  assert.match(err({ ...good, capacity: 0 })!, /capacity/);
  assert.match(err({ ...good, capacity: 2.5 })!, /capacity/);
  assert.match(err({ ...good, starts_at: "2020-01-01T00:00:00Z" })!, /future/);
  assert.match(err({ ...good, extra: 1 })!, /unknown field extra/);
  assert.match(err({ title: "x", capacity: 1 })!, /starts_at is required/);
  assert.match(err([])!, /JSON object/);
});

test("validateEventPatch", () => {
  assert.deepEqual(validateEventPatch({ capacity: 10 }, NOW), { ok: true, value: { capacity: 10 } });
  assert.equal(validateEventPatch({}, NOW).ok, false);
  assert.equal(validateEventPatch({ status: "closed" }, NOW).ok, false);
});

test("contacts", () => {
  assert.equal(normalizeEmail(" Ada@Example.COM "), "ada@example.com");
  for (const bad of ["ada", "ada@", "@x.io", "a b@x.io", "ada@localhost", "ada@x..io", 42]) assert.equal(normalizeEmail(bad), null, String(bad));
  assert.equal(normalizeTelegram("@John_Doe"), "john_doe");
  assert.equal(normalizeTelegram("https://t.me/john_doe"), "john_doe");
  assert.equal(normalizeTelegram("@jd"), null);
  assert.equal(normalizeTelegram("@1john"), null);
});

test("validateRegistration", () => {
  assert.deepEqual(validateRegistration({ name: " Ada ", email: "ADA@x.io" }), { ok: true, value: { name: "Ada", email: "ada@x.io", telegram: null } });
  assert.deepEqual(validateRegistration({ name: "Ada", telegram: "@ada_l" }), { ok: true, value: { name: "Ada", email: null, telegram: "ada_l" } });
  assert.equal(validateRegistration({ name: "Ada" }).ok, false);
  assert.equal(validateRegistration({ name: "", email: "a@x.io" }).ok, false);
  assert.equal(validateRegistration({ name: "Ada", email: "nope" }).ok, false);
  assert.equal(validateRegistration({ name: "Ada", email: "a@x.io", admin: true }).ok, false);
});

test("tokens and keys", async () => {
  const t = randomToken("evr_");
  assert.equal(isValidRegistrationToken(t), true);
  assert.notEqual(t, randomToken("evr_"));
  assert.equal(isValidRegistrationToken("evr_short"), false);
  assert.equal(isValidIdempotencyKey("retry-1234"), true);
  assert.equal(isValidIdempotencyKey("short"), false);
  assert.equal(await sha256Hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
});

test("CSV escaping and formula guard", () => {
  assert.equal(csvCell("plain"), "plain");
  assert.equal(csvCell('say "hi", ok'), '"say ""hi"", ok"');
  assert.equal(csvCell("two\nlines"), '"two\nlines"');
  assert.equal(csvCell("=HYPERLINK(\"x\")"), "\"'=HYPERLINK(\"\"x\"\")\"");
  assert.equal(csvCell("+1 555"), "'+1 555");
  assert.equal(csvCell("@john"), "'@john");
  assert.equal(csvCell(-5), "-5", "numbers are not guarded");
  assert.equal(csvCell(null), "");
  assert.equal(csvLine([1, "Ada", null]), "1,Ada,\r\n");
  assert.equal(toCsv(["a", "b"], [[1, 2]], false), "a,b\r\n1,2\r\n");
  assert.ok(toCsv(["a"], []).startsWith(String.fromCharCode(0xfeff)));
});
