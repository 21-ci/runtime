// Run: node --test test/*.test.ts   (Node >= 22.18 strips the types)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type Env,
  formatIn,
  formatLocal,
  formatRateChanges,
  groupDigits,
  isState,
  localToEpoch,
  MAX_NOTE_CHARS,
  parseCommand,
  parseRatesMessage,
  parseWhen,
  type State,
  transition,
} from "../src/bot.ts";

const TZ = 300; // UTC+5
// 2026-10-07 10:00 in UTC+5 = 05:00 UTC
const NOW = Date.UTC(2026, 9, 7, 5, 0);
const env: Env = { now: NOW, tzOffsetMin: TZ, botUsername: "notes_bot", privateChat: true };

test("parseCommand", () => {
  assert.deepEqual(parseCommand("/note buy milk"), { name: "note", arg: "buy milk" });
  assert.deepEqual(parseCommand("/Notes@notes_bot", "notes_bot"), { name: "notes", arg: "" });
  assert.equal(parseCommand("/notes@other_bot", "notes_bot"), "foreign");
  assert.equal(parseCommand("hello"), null);
  assert.equal(parseCommand("/"), null);
  assert.deepEqual(parseCommand("  /remind 2h\ncall mom "), { name: "remind", arg: "2h\ncall mom" });
});

test("parseWhen: relative", () => {
  assert.deepEqual(parseWhen("10m", NOW, TZ), { ok: true, at: NOW + 10 * 60_000 });
  assert.deepEqual(parseWhen("in 2 hours", NOW, TZ), { ok: true, at: NOW + 2 * 3600_000 });
  assert.deepEqual(parseWhen("1h30m", NOW, TZ), { ok: true, at: NOW + 90 * 60_000 });
  assert.deepEqual(parseWhen("1d 2h", NOW, TZ), { ok: true, at: NOW + 26 * 3600_000 });
  assert.equal(parseWhen("10 parsecs", NOW, TZ).ok, false);
  assert.equal(parseWhen("", NOW, TZ).ok, false);
});

test("parseWhen: clock times in the user's offset", () => {
  // 18:30 today local = 13:30 UTC
  assert.deepEqual(parseWhen("18:30", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 7, 13, 30) });
  // 09:00 has passed (it is 10:00 local): tomorrow
  assert.deepEqual(parseWhen("9:00", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 8, 4, 0) });
  assert.deepEqual(parseWhen("tomorrow", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 8, 4, 0) });
  assert.deepEqual(parseWhen("tomorrow 7.15", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 8, 2, 15) });
  assert.equal(parseWhen("25:00", NOW, TZ).ok, false);
});

test("parseWhen: dates", () => {
  assert.deepEqual(parseWhen("2026-10-09 23:00", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 9, 18, 0) });
  assert.deepEqual(parseWhen("09.10.2026 23:00", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 9, 18, 0) });
  assert.deepEqual(parseWhen("2026-10-09", NOW, TZ), { ok: true, at: Date.UTC(2026, 9, 9, 4, 0) });
  assert.equal(parseWhen("2026-02-30 10:00", NOW, TZ).ok, false);
  assert.equal(localToEpoch(2026, 2, 29, 0, 0, 0), null);
});

test("formatting", () => {
  assert.equal(formatLocal(Date.UTC(2026, 9, 7, 13, 30), TZ), "2026-10-07 18:30 UTC+05:00");
  assert.equal(formatLocal(0, -90), "1969-12-31 22:30 UTC-01:30");
  assert.equal(formatIn(125 * 60_000), "in 2 h 5 min");
  assert.equal(formatIn(3 * 86400_000 + 4 * 3600_000 + 59_000), "in 3 d 4 h");
  assert.equal(formatIn(10_000), "in less than a minute");
  assert.equal(groupDigits("11778.45"), "11 778.45");
  assert.equal(groupDigits("123"), "123");
});

test("note dialog: /note, text, saved", () => {
  const s1 = transition(null, "/note", env);
  assert.deepEqual(s1.state, { step: "note_text" });
  const s2 = transition(s1.state, "  buy milk ", env);
  assert.equal(s2.state, null);
  assert.deepEqual(s2.effect, { kind: "save_note", text: "buy milk" });
  assert.deepEqual(transition(null, "/note quick one", env).effect, { kind: "save_note", text: "quick one" });
  const tooLong = transition(s1.state, "x".repeat(MAX_NOTE_CHARS + 1), env);
  assert.deepEqual(tooLong.state, { step: "note_text" });
  assert.equal(tooLong.effect, undefined);
});

test("reminder dialog: text, bad time, time, confirm", () => {
  let s = transition(null, "/remind", env);
  assert.deepEqual(s.state, { step: "remind_text" });
  s = transition(s.state, "call mom", env);
  assert.deepEqual(s.state, { step: "remind_when", text: "call mom" });
  const bad = transition(s.state, "someday", env);
  assert.deepEqual(bad.state, s.state, "stays in remind_when");
  assert.match(bad.reply!, /could not read/);
  s = transition(s.state, "2h", env);
  assert.deepEqual(s.state, { step: "remind_confirm", text: "call mom", dueAt: NOW + 2 * 3600_000 });
  const unsure = transition(s.state, "maybe", env);
  assert.deepEqual(unsure.state, s.state);
  const yes = transition(s.state, "Yes", env);
  assert.equal(yes.state, null);
  assert.deepEqual(yes.effect, { kind: "create_reminder", text: "call mom", dueAt: NOW + 2 * 3600_000 });
  assert.equal(transition(s.state, "no", env).state, null);
});

test("reminder: confirm after the time passed asks again", () => {
  const state: State = { step: "remind_confirm", text: "x", dueAt: NOW - 1000 };
  const s = transition(state, "yes", env);
  assert.deepEqual(s.state, { step: "remind_when", text: "x" });
});

test("reminder: quick form and range checks", () => {
  assert.deepEqual(transition(null, "/remind 2h 30m call mom", env).effect, {
    kind: "create_reminder",
    text: "call mom",
    dueAt: NOW + 150 * 60_000,
  });
  assert.match(transition(null, "/remind 8d call mom", env).reply!, /too far/);
  assert.match(transition(null, "/remind 30s call mom", env).reply!, /too soon/);
  // No time in front: ask when.
  assert.deepEqual(transition(null, "/remind call mom", env).state, { step: "remind_when", text: "call mom" });
});

test("cancel and other commands end a dialog", () => {
  const st: State = { step: "remind_when", text: "x" };
  assert.deepEqual(transition(st, "/cancel", env), { state: null, reply: "Cancelled." });
  assert.deepEqual(transition(null, "/cancel", env), { state: null, reply: "Nothing to cancel." });
  assert.deepEqual(transition(st, "/notes", env), { state: null, effect: { kind: "list_notes" } });
  assert.deepEqual(transition(null, "/delnote #12", env).effect, { kind: "delete_note", id: 12 });
  assert.equal(transition(null, "/delnote abc", env).effect, undefined);
  assert.deepEqual(transition(st, "/start@other_bot", env), { state: st }, "foreign command keeps the dialog");
});

test("plain text: hint in private chats, silence in groups", () => {
  assert.match(transition(null, "hi", env).reply!, /\/help/);
  assert.deepEqual(transition(null, "hi", { ...env, privateChat: false }), { state: null });
});

test("isState rejects junk from KV", () => {
  assert.equal(isState({ step: "note_text" }), true);
  assert.equal(isState({ step: "remind_confirm", text: "x", dueAt: 1.5 }), false);
  assert.equal(isState({ step: "nope" }), false);
  assert.equal(isState(null), false);
});

test("rates message validation and formatting", () => {
  const body = {
    type: "cbu.rate_changed",
    key: "cbu:abc123",
    changes: [{ ccy: "USD", date: "2026-10-07", rate: "11778.45", diff: "5.5", nominal: 1, previous: "11772.95" }],
  };
  const msg = parseRatesMessage(body)!;
  assert.equal(msg.changes[0].ccy, "USD");
  assert.match(formatRateChanges(msg), /USD {2}11 778\.45 UZS {2}\(\+5\.5\)/);
  assert.equal(parseRatesMessage({ ...body, key: "bad key!" }), null);
  assert.equal(parseRatesMessage({ ...body, changes: [{ ...body.changes[0], ccy: "usd" }] }), null);
  assert.equal(parseRatesMessage({ ...body, type: "other" }), null);
});
