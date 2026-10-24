// Per-client-address throttle for public POSTs: a fixed one-minute window counted with kv.incr
// (atomic, so concurrent requests never share a count; the TTL is set when the window's first
// request creates the key). The address is ctx.client.ip: the platform resolves it behind
// ingress.trusted_proxies and strips a client-sent one, so it cannot be spoofed. No std:* imports:
// the handler passes kv.incr in, so `node --test` / `deno test` can load this module.

import { error, errorText } from "./http.ts";

export const WINDOW_SECONDS = 60;

export type Incr = (key: string, delta: number, opts: { ttlSeconds?: number }) => Promise<number>;

// THROTTLE_PER_MINUTE: requests per minute and address; 0 turns the throttle off. Unset, empty or
// invalid: the app's default.
export function throttleLimit(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const n = Number(raw);
  return Number.isSafeInteger(n) && n >= 0 && n <= 1_000_000 ? n : fallback;
}

export interface Window {
  start: number; // epoch seconds
  resetIn: number; // seconds until the next window
}

export function windowOf(nowMs: number, size = WINDOW_SECONDS): Window {
  const now = Math.floor(nowMs / 1000);
  const start = now - (now % size);
  return { start, resetIn: start + size - now };
}

// What one budget covers: an IPv4 address, or the /64 of an IPv6 address (a subscriber usually
// holds a whole /64). An IPv4-mapped IPv6 address counts as its IPv4 address. Anything unexpected
// is used as it is.
export function clientBucket(ip: string): string {
  if (!ip.includes(":")) return ip;
  const mapped = /^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/i.exec(ip);
  if (mapped) return mapped[1];
  const halves = ip.split("::");
  if (halves.length > 2) return ip;
  const groups = (s: string) => (s === "" ? [] : s.split(":"));
  const head = groups(halves[0]);
  const tail = halves.length === 2 ? groups(halves[1]) : [];
  // an embedded IPv4 address at the end stands for two groups
  const width = head.length + tail.reduce((n, g) => n + (g.includes(".") ? 2 : 1), 0);
  const all = halves.length === 2 ? [...head, ...Array(Math.max(0, 8 - width)).fill("0"), ...tail] : head;
  const prefix = all.slice(0, 4);
  if (prefix.length < 4 || !prefix.every((g) => /^[0-9a-f]{1,4}$/i.test(g))) return ip;
  return `${prefix.map((g) => parseInt(g, 16).toString(16)).join(":")}::/64`;
}

export function throttleKey(scope: string, ip: string, w: Window): string {
  return `throttle/${scope}/${clientBucket(ip)}/${w.start}`;
}

export interface Verdict {
  allowed: boolean;
  remaining: number;
  retryAfter: number; // seconds
}

// `count` = what kv.incr returned: the requests of this window, this one included.
export function decide(count: number, limit: number, w: Window): Verdict {
  return { allowed: count <= limit, remaining: Math.max(0, limit - count), retryAfter: w.resetIn };
}

// Counts the request and answers 429 once the address has used its budget for this minute; null
// lets the request through: allowed, throttle off, no client address (not an HTTP request), or the
// store failed (logged; availability over strictness).
export async function throttle(
  incr: Incr,
  scope: string,
  ip: string | undefined,
  limit: number,
  nowMs = Date.now(),
): Promise<Response | null> {
  if (!ip || limit <= 0) return null;
  const w = windowOf(nowMs);
  let count: number;
  try {
    count = Number(await incr(throttleKey(scope, ip, w), 1, { ttlSeconds: WINDOW_SECONDS + 5 }));
  } catch (e) {
    console.warn(`throttle store: ${errorText(e)}`);
    return null;
  }
  const v = decide(count, limit, w);
  if (v.allowed) return null;
  return error(429, "rate_limited", `too many requests from this address; try again in ${v.retryAfter} s`, {
    "retry-after": String(v.retryAfter),
  });
}
