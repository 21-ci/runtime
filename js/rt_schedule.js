// `std:schedule` -- one-off tasks of this service, created from code.
//
//   schedule.after(seconds, payload, { id? }) -> { id, runAt }   due in `seconds` (at most 30 days)
//   schedule.at(time, payload, { id? })       -> { id, runAt }   `time`: a Date or Unix ms
//   schedule.cancel(id)                       -> boolean         false when no such schedule is pending
//   schedule.list({ cursor?, limit? })        -> { entries: [{ id, runAt }], cursor }   pending, by id
//
// The due schedule runs the service's `scheduled(event, ctx)` handler with `event.id`,
// `event.scheduledAt` (the due time, Unix ms) and `event.payload`; `ctx.trigger` is "schedule". Delivery is
// at-least-once: a handler may see the same event again, so make its work idempotent by `event.id`.
// Without `id` a schedule gets a generated `sch_…` id. An id is 1 to 128 of a-z, A-Z, 0-9, `.`, `_`, `-`, `:`;
// a schedule with a pending id replaces it. The payload is JSON, at most 16 KiB. At most 1000 schedules
// are pending per service (`schedule_limit`). Refused arguments are `validation_failed`.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const schedule = globalThis.__rt.schedule;
