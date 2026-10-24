// `std:log` -- structured logs (the call shapes are in docs/handlers.md).
//
//   log.debug|info|warn|error(message, fields?) -> void   a record of the task's log, with fields
//   log.with(fields)                             -> child logger with `fields` bound to every call
//
// `fields` is a plain object (not an array): at most 32 top-level keys and 8 KiB of JSON; a call
// with a refused object throws `validation_failed` and writes nothing. A call's fields win over
// the bound ones. `console.*` keeps writing plain records without fields.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const log = globalThis.__rt.log;
