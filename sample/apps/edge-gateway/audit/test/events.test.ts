// Run: node --test test/*.test.ts   (Node >= 22.18)   or   deno test
import { test } from "node:test";
import assert from "node:assert/strict";
import { type AuditEvent, isClientIp, lastGatewayAttempt, parseAuditEvent } from "../src/events.ts";

// What the gateway sends (src/audit.rs: Event::to_json + queue_message).
const event = {
  v: 1,
  id: "task_0000000000000002",
  at: 1_800_000_000_000,
  sub: "alice",
  ip: "203.0.113.7",
  method: "POST",
  path: "/v1/notes",
  route: "notes",
  status: 201,
  outcome: "ok",
  reason: null,
  shipped: true,
  ship_error: null,
};

test("accepts gateway events", () => {
  assert.deepEqual(parseAuditEvent(event), {
    id: event.id,
    at: event.at,
    sub: "alice",
    ip: "203.0.113.7",
    method: "POST",
    path: "/v1/notes",
    route: "notes",
    status: 201,
    outcome: "ok",
    reason: null,
    shipped: true,
    shipError: null,
  });
  // a denied request before the token was verified: no subject, a reason
  assert.ok(parseAuditEvent({ ...event, sub: null, status: 401, outcome: "denied", reason: "bad_signature" }));
  // cut by the per-address limit, IPv6 client
  assert.equal(parseAuditEvent({ ...event, sub: null, ip: "2001:db8::1", status: 429, outcome: "limited", reason: "ip_rate_limited" })?.ip, "2001:db8::1");
  // an invalid path: no route
  assert.ok(parseAuditEvent({ ...event, route: null, status: 404, outcome: "invalid", reason: "not_found" }));
  // no client address (not an HTTP trigger)
  assert.equal(parseAuditEvent({ ...event, ip: null })?.ip, null);
});

test("shipping state from the gateway", () => {
  const failed = parseAuditEvent({ ...event, shipped: false, ship_error: "acme: sdk_unavailable" });
  assert.equal(failed?.shipped, false);
  assert.equal(failed?.shipError, "acme: sdk_unavailable");
  // GATEWAY_SDK=off: not attempted
  const off = parseAuditEvent({ ...event, shipped: false, ship_error: null });
  assert.deepEqual([off?.shipped, off?.shipError], [false, null]);
  // an event from an older gateway (no ip, shipped, ship_error) is pending
  const { ip: _ip, shipped: _s, ship_error: _e, ...old } = event;
  const legacy = parseAuditEvent(old);
  assert.deepEqual([legacy?.ip, legacy?.shipped, legacy?.shipError], [null, false, null]);
  // shipped wins over a stray error
  assert.equal(parseAuditEvent({ ...event, shipped: true, ship_error: "x" })?.shipError, null);
});

test("rejects malformed events", () => {
  const bad = (patch: Record<string, unknown>) => parseAuditEvent({ ...event, ...patch });
  assert.equal(bad({ v: 2 }), null);
  assert.equal(bad({ id: "" }), null);
  assert.equal(bad({ id: "a b" }), null);
  assert.equal(bad({ at: -1 }), null);
  assert.equal(bad({ at: 1.5 }), null);
  assert.equal(bad({ sub: "bad sub" }), null);
  assert.equal(bad({ ip: "localhost" }), null);
  assert.equal(bad({ ip: "203.0.113.7:443" }), null);
  assert.equal(bad({ ip: "fe80::1%eth0" }), null);
  assert.equal(bad({ ip: 1 }), null);
  assert.equal(bad({ method: "get" }), null);
  assert.equal(bad({ path: "relative" }), null);
  assert.equal(bad({ path: "/a\nb" }), null);
  assert.equal(bad({ route: "admin" }), null);
  assert.equal(bad({ status: 99 }), null);
  assert.equal(bad({ outcome: "great" }), null);
  assert.equal(bad({ reason: "x".repeat(121) }), null);
  assert.equal(bad({ shipped: "yes" }), null);
  assert.equal(bad({ shipped: false, ship_error: "x".repeat(121) }), null);
  assert.equal(parseAuditEvent("text"), null);
  assert.equal(parseAuditEvent([event]), null);
});

test("client addresses", () => {
  for (const ok of ["203.0.113.7", "2001:db8::1", "::1", "::ffff:198.51.100.4"]) assert.ok(isClientIp(ok), ok);
  for (const bad of ["", "abc", "1234", "203.0.113.7/24", " 203.0.113.7", "a".repeat(46), null]) assert.ok(!isClientIp(bad), String(bad));
});

test("the gateway's latest attempt becomes the shipping status", () => {
  const e = (at: number, shipped: boolean, shipError: string | null) =>
    ({ ...parseAuditEvent(event)!, id: `t${at}`, at, shipped, shipError }) satisfies AuditEvent;
  assert.equal(lastGatewayAttempt([]), null);
  assert.equal(lastGatewayAttempt([e(1, false, null)]), null, "not attempted (GATEWAY_SDK=off)");
  assert.deepEqual(lastGatewayAttempt([e(1, true, null), e(3, false, "acme: rate_limited"), e(2, true, null)]), {
    at: 3,
    ok: false,
    error: "acme: rate_limited",
    by: "gateway",
  });
  assert.deepEqual(lastGatewayAttempt([e(5, true, null), e(4, false, "acme: HTTP 500"), e(6, false, null)]), {
    at: 5,
    ok: true,
    error: null,
    by: "gateway",
  });
});
