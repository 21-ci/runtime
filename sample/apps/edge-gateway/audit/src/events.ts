// Audit events sent by the edge gateway (std:host/queue). Validation is strict: the queue is
// reachable only by allowed callers, but a malformed event must never reach storage or the
// operator's audit API.

export interface AuditEvent {
  id: string; // the gateway's task id: the primary key, so a redelivered message is stored once
  at: number; // epoch ms
  sub: string | null;
  ip: string | null; // the client address the platform resolved (x-rt-client-ip)
  method: string;
  path: string;
  route: string | null;
  status: number;
  outcome: string;
  reason: string | null;
  shipped: boolean; // the gateway already delivered it to the operator API: never shipped again
  shipError: string | null; // why the gateway's attempt failed; null when it did not try
}

// The latest shipping attempt, kept in KV for GET /recent.
export interface ShipStatus {
  at: number;
  ok: boolean;
  error: string | null;
  by: "gateway" | "edge-audit";
  /** The SDK release that shipped (edge-audit's own attempts). */
  sdk?: string | null;
}

const METHODS = new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE"]);
const OUTCOMES = new Set(["ok", "denied", "invalid", "limited", "upstream_error", "misconfigured"]);
const SUBJECT = /^[A-Za-z0-9._:@-]{1,128}$/;
// An IPv4 or IPv6 address as the gateway prints it (no zone, no port, no prefix).
const IPV4 = /^\d{1,3}(?:\.\d{1,3}){3}$/;
const IPV6 = /^(?=(?:[^:]*:){2})[0-9A-Fa-f:]{2,39}(?:\d{1,3}(?:\.\d{1,3}){3})?$/;

const str = (v: unknown, max: number): v is string => typeof v === "string" && v.length > 0 && v.length <= max;

export const isClientIp = (v: unknown): v is string =>
  typeof v === "string" && v.length <= 45 && (IPV4.test(v) || IPV6.test(v));

export function parseAuditEvent(body: unknown): AuditEvent | null {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return null;
  const b = body as Record<string, unknown>;
  if (b.v !== 1) return null;
  if (!str(b.id, 100) || !/^[A-Za-z0-9_:-]+$/.test(b.id)) return null;
  if (!Number.isSafeInteger(b.at) || (b.at as number) <= 0) return null;
  if (b.sub !== null && (typeof b.sub !== "string" || !SUBJECT.test(b.sub))) return null;
  // ip, shipped and ship_error are absent in events from an older gateway.
  if (b.ip !== undefined && b.ip !== null && !isClientIp(b.ip)) return null;
  if (typeof b.method !== "string" || !(METHODS.has(b.method) || /^[A-Z]{1,16}$/.test(b.method))) return null;
  if (!str(b.path, 512) || !b.path.startsWith("/") || /[\u0000-\u001F\u007F]/.test(b.path)) return null;
  if (b.route !== null && b.route !== "notes" && b.route !== "audit") return null;
  if (!Number.isInteger(b.status) || (b.status as number) < 100 || (b.status as number) > 599) return null;
  if (typeof b.outcome !== "string" || !OUTCOMES.has(b.outcome)) return null;
  if (b.reason !== null && !str(b.reason, 120)) return null;
  if (b.shipped !== undefined && typeof b.shipped !== "boolean") return null;
  if (b.ship_error !== undefined && b.ship_error !== null && !str(b.ship_error, 120)) return null;
  return {
    id: b.id,
    at: b.at as number,
    sub: b.sub as string | null,
    ip: (b.ip as string | null | undefined) ?? null,
    method: b.method,
    path: b.path,
    route: b.route as string | null,
    status: b.status as number,
    outcome: b.outcome,
    reason: b.reason as string | null,
    shipped: b.shipped === true,
    shipError: b.shipped === true ? null : (b.ship_error as string | null | undefined) ?? null,
  };
}

// The gateway's latest shipping attempt in a batch. Events it did not try to ship
// (GATEWAY_SDK=off) say nothing about shipping.
export function lastGatewayAttempt(events: AuditEvent[]): ShipStatus | null {
  let last: AuditEvent | null = null;
  for (const e of events) {
    if ((e.shipped || e.shipError !== null) && (last === null || e.at >= last.at)) last = e;
  }
  return last && { at: last.at, ok: last.shipped, error: last.shipError, by: "gateway" };
}
