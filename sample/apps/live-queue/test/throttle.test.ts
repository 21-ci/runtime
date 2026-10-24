// Run: node --test test/*.test.ts   (Node >= 22.18)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import { clientBucket, decide, type Incr, throttle, throttleKey, throttleLimit, windowOf } from "../src/throttle.ts";

const NOW = 1_800_000_125_000; // 125 s into a minute boundary: 1_800_000_120 s
const W = windowOf(NOW);

// kv.incr stand-in: counts per key, remembers the options of the call that created the key.
function fakeKv() {
  const counts = new Map<string, number>();
  const created = new Map<string, { ttlSeconds?: number }>();
  const incr: Incr = async (key, delta, opts) => {
    if (!counts.has(key)) created.set(key, opts);
    const n = (counts.get(key) ?? 0) + delta;
    counts.set(key, n);
    return n;
  };
  return { incr, counts, created };
}

test("limit setting", () => {
  assert.equal(throttleLimit(undefined, 20), 20);
  assert.equal(throttleLimit("", 20), 20);
  assert.equal(throttleLimit(" 5 ", 20), 5);
  assert.equal(throttleLimit("0", 20), 0, "0 turns it off");
  assert.equal(throttleLimit("-1", 20), 20);
  assert.equal(throttleLimit("1.5", 20), 20);
  assert.equal(throttleLimit("lots", 20), 20);
});

test("windows and keys", () => {
  assert.deepEqual(W, { start: 1_800_000_120, resetIn: 55 });
  assert.deepEqual(windowOf(1_800_000_120_000), { start: 1_800_000_120, resetIn: 60 });
  assert.equal(throttleKey("ticket", "203.0.113.7", W), "throttle/ticket/203.0.113.7/1800000120");
  assert.deepEqual(decide(3, 3, W), { allowed: true, remaining: 0, retryAfter: 55 });
  assert.deepEqual(decide(4, 3, W), { allowed: false, remaining: 0, retryAfter: 55 });
  assert.equal(decide(1, 3, W).remaining, 2);
});

test("client buckets: IPv4, IPv6 per /64, mapped IPv4", () => {
  assert.equal(clientBucket("203.0.113.7"), "203.0.113.7");
  assert.equal(clientBucket("2001:db8:1:2::1"), "2001:db8:1:2::/64");
  assert.equal(clientBucket("2001:0db8:0001:0002:aaaa:bbbb:cccc:dddd"), "2001:db8:1:2::/64");
  assert.equal(clientBucket("2001:db8::1"), "2001:db8:0:0::/64");
  assert.equal(clientBucket("::1"), "0:0:0:0::/64");
  assert.equal(clientBucket("64:ff9b::192.0.2.33"), "64:ff9b:0:0::/64");
  assert.equal(clientBucket("::ffff:198.51.100.4"), "198.51.100.4");
  assert.notEqual(clientBucket("2001:db8:1:3::1"), clientBucket("2001:db8:1:2::1"));
  assert.equal(clientBucket("weird:::x"), "weird:::x", "unexpected input is used as it is");
});

test("throttle: budget per address and minute, then 429 with Retry-After", async () => {
  const kv = fakeKv();
  for (let i = 0; i < 3; i++) assert.equal(await throttle(kv.incr, "ticket", "203.0.113.7", 3, NOW), null);
  const res = await throttle(kv.incr, "ticket", "203.0.113.7", 3, NOW);
  assert.ok(res);
  assert.equal(res.status, 429);
  assert.equal(res.headers.get("retry-after"), "55");
  const body = await res.json();
  assert.equal(body.error.code, "rate_limited");
  assert.match(body.error.message, /55 s/);
  // the TTL is set when the window's counter is created, a little past the window
  assert.deepEqual(kv.created.get("throttle/ticket/203.0.113.7/1800000120"), { ttlSeconds: 65 });
  // another address, another scope and the next minute have their own budgets
  assert.equal(await throttle(kv.incr, "ticket", "198.51.100.4", 3, NOW), null);
  assert.equal(await throttle(kv.incr, "other", "203.0.113.7", 3, NOW), null);
  assert.equal(await throttle(kv.incr, "ticket", "203.0.113.7", 3, NOW + 60_000), null);
  // one IPv6 /64 shares a budget
  for (let i = 0; i < 3; i++) await throttle(kv.incr, "ticket", `2001:db8:1:2::${i + 1}`, 3, NOW);
  assert.equal((await throttle(kv.incr, "ticket", "2001:db8:1:2::99", 3, NOW))?.status, 429);
});

test("throttle: off, no client address, store failure", async () => {
  const kv = fakeKv();
  assert.equal(await throttle(kv.incr, "ticket", "203.0.113.7", 0, NOW), null, "THROTTLE_PER_MINUTE=0");
  assert.equal(await throttle(kv.incr, "ticket", undefined, 1, NOW), null, "no ctx.client");
  assert.equal(kv.counts.size, 0, "nothing counted");
  const failing: Incr = async () => {
    throw Object.assign(new Error("over quota"), { code: "storage_quota" });
  };
  const warn = console.warn;
  const logged: string[] = [];
  console.warn = (m: string) => logged.push(m);
  try {
    assert.equal(await throttle(failing, "ticket", "203.0.113.7", 1, NOW), null, "fails open");
  } finally {
    console.warn = warn;
  }
  assert.deepEqual(logged, ["throttle store: storage_quota: over quota"]);
});
