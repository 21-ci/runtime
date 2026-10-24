// Run: node --test test/*.test.ts   (Node >= 22.18)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  etaSeconds,
  newQueueId,
  QUEUE_ID,
  secondsPerPerson,
  TICKET_TOKEN,
  validateCall,
  validateQueue,
  validateTicket,
} from "../src/queue.ts";
import { adminPage, boardPage, csp, landingPage, sanitizeBase, screenPage, scriptJson, takePage, ticketPage } from "../src/pages.ts";
import { randomToken } from "../src/http.ts";

const NOW = 1_800_000_000_000;
const MIN = 60_000;

test("queue ids and ticket tokens", () => {
  const ids = new Set(Array.from({ length: 200 }, newQueueId));
  assert.equal(ids.size, 200);
  for (const id of ids) assert.match(id, QUEUE_ID);
  for (const id of ids) assert.doesNotMatch(id, /[01ilo]/);
  assert.match(randomToken("lqt_", 24), TICKET_TOKEN);
});

test("secondsPerPerson: fallback, single counter, batches, window, clamp", () => {
  assert.equal(secondsPerPerson([], NOW, 180), 180);
  assert.equal(secondsPerPerson([{ at: NOW - MIN, admitted: 1 }], NOW, 180), 180, "one call is not a rate");
  // calls every 2 min, one person each
  const single = [0, 1, 2, 3].map((i) => ({ at: NOW - 8 * MIN + i * 2 * MIN, admitted: 1 }));
  assert.equal(secondsPerPerson(single, NOW, 180), 120);
  // "call next 3" every 6 min: 2 min per person as well
  const batch = [0, 1, 2].map((i) => ({ at: NOW - 20 * MIN + i * 6 * MIN, admitted: 3 }));
  assert.equal(secondsPerPerson(batch, NOW, 180), 120);
  // calls older than an hour are ignored
  assert.equal(secondsPerPerson([{ at: NOW - 3 * 60 * MIN, admitted: 1 }, { at: NOW - MIN, admitted: 1 }], NOW, 180), 180);
  // two calls one second apart: clamped to 5 s
  assert.equal(secondsPerPerson([{ at: NOW - 2000, admitted: 1 }, { at: NOW - 1000, admitted: 1 }], NOW, 180), 5);
  // calls that admitted nobody do not count
  assert.equal(secondsPerPerson([{ at: NOW - 4 * MIN, admitted: 0 }, { at: NOW - 2 * MIN, admitted: 1 }], NOW, 180), 180);
});

test("etaSeconds", () => {
  assert.equal(etaSeconds(0, 120, null), 120, "first in line, nobody called yet: one interval");
  assert.equal(etaSeconds(0, 120, 30), 90, "next call due in 90 s");
  assert.equal(etaSeconds(0, 120, 500), 0, "overdue: any moment");
  assert.equal(etaSeconds(4, 120, 30), 90 + 4 * 120);
});

test("validation", () => {
  assert.deepEqual(validateQueue({ name: " Passport office, window 3 " }), {
    ok: true,
    value: { name: "Passport office, window 3", minutesPerPerson: 3, maxWaiting: 500 },
  });
  assert.equal(validateQueue({ name: "" }).ok, false);
  assert.equal(validateQueue({ name: "x", minutesPerPerson: 0 }).ok, false);
  assert.equal(validateQueue({ name: "x", maxWaiting: 1.5 }).ok, false);
  assert.deepEqual(validateTicket(null), { ok: true, value: { name: "" } });
  assert.deepEqual(validateTicket({ name: " Ada " }), { ok: true, value: { name: "Ada" } });
  assert.equal(validateTicket({ name: "x".repeat(41) }).ok, false);
  assert.deepEqual(validateCall(null), { ok: true, value: { count: 1, counter: "1" } });
  assert.deepEqual(validateCall({ count: 3, counter: "window 3" }), { ok: true, value: { count: 3, counter: "window 3" } });
  assert.equal(validateCall({ count: 21 }).ok, false);
  assert.equal(validateCall({ counter: "a\nb" }).ok, false);
});

test("pages: base path, JSON escaping, CSP", () => {
  assert.equal(sanitizeBase("/s/live-queue"), "/s/live-queue");
  assert.equal(sanitizeBase(null), "");
  assert.equal(sanitizeBase('/s/x"><script>'), "");
  assert.equal(scriptJson({ a: "</script><b>" }), '{"a":"\\u003c/script>\\u003cb>"}');
  assert.match(csp("abc"), /script-src 'nonce-abc'/);
  assert.doesNotMatch(csp("abc"), /unsafe-inline/);
});

test("pages: every inline script parses and carries the nonce", () => {
  const cfg = { base: "/s/live-queue", qid: "abcdefghjk" };
  const pages = [landingPage, takePage, ticketPage, screenPage, adminPage, boardPage].map((p) => p("NONCE", cfg));
  for (const html of pages) {
    const scripts = [...html.matchAll(/<script nonce="NONCE">([\s\S]*?)<\/script>/g)].map((m) => m[1]);
    assert.equal(scripts.length, 1);
    assert.doesNotThrow(() => new Function(scripts[0]), "client script must be valid JavaScript");
    assert.match(html, /<style nonce="NONCE">/);
    assert.doesNotMatch(html, /\son[a-z]+="/, "no inline event handlers");
    assert.doesNotMatch(html, /\sstyle="/, "no style attributes (blocked by the CSP)");
    assert.ok(html.includes('<script type="application/json" id="cfg">{"base":"/s/live-queue","qid":"abcdefghjk"}</script>'));
  }
});
