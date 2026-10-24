// `std:pubsub` -- ephemeral subjects with fan-out and request/reply, NATS style (docs/handlers.md).
//
//   publish(subject, body, { headers }) -> Promise<{ receivers: number, dropped: number }>
//       fire and forget, at most once, in memory; resolves after routing (not after delivery):
//       `receivers` = deliveries handed to the scheduler, `dropped` = copies left out (fan-out cap of
//       100, quota, backlog, refusals)
//   request(subject, body, { headers, timeoutMs = 5000 }) -> Promise<unknown>
//       one responder, first answer wins; the reply is parsed JSON for application/json, a string for
//       text/*, a Uint8Array otherwise and undefined when empty. `timeoutMs` at most 30000 and never
//       past ctx.deadline.
//
// Bodies: a Uint8Array is sent as is, anything else as JSON.stringify(body); at most 1 MiB. Subjects
// are `.`-separated tokens of [A-Za-z0-9_-] (no wildcards, at most 256 bytes and 16 tokens, not
// `_rt...`). Headers: at most 32 entries / 16 KiB, no `x-rt-*` names, values without CR/LF/NUL.
// Subscribers declare `events.subscribe` in the service config and export `pubsub(msg, ctx)`:
//   msg = { id, subject, body, headers, replyable, publishedAt }; its return value is the reply
//   for a request (undefined -> 204, Response as is, Uint8Array, string, else JSON).
// Error codes: validation_failed, too_large, unavailable, rate_limited, quota_exceeded, call_depth,
// no_responders, timeout, responder_failed, overloaded, queue_timeout, shed, memory, draining.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const publish = globalThis.__rt.pubsub.publish;
export const request = globalThis.__rt.pubsub.request;
