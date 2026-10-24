// `std:durable` -- durable objects of a durable service.
//
//   durable.get(name, { service? }) -> { name, id, fetch(input, init) }   an object, by name: its
//       requests and events run one at a time; `service` is another durable service (a slug)
//   durable.idFromName(name) -> name          durable.newUniqueId() -> 32 hex characters
//
//   storage (the task's object; also `ctx.storage` and a durable class's `state.storage`):
//   storage.get(key) -> value | undefined     storage.get(keys) -> Map
//   storage.put(key, value) | put({ key: value, ... })      values are structured clones
//   storage.delete(key) -> boolean            storage.delete(keys) -> number
//   storage.list({ prefix?, start?, end?, limit?, reverse? }) -> Map      storage.deleteAll()
//   storage.getAlarm() -> ms | null           storage.setAlarm(Date | ms)    storage.deleteAlarm()
//
// A durable service's default export is a class: `constructor(state, env)` with `state.storage`,
// `state.id.name`; methods `fetch`, `webSocketMessage`, `webSocketClose`, `webSocketError`, `alarm`.
// A task's writes commit when its handler returned, in one transaction, before its socket sends
// leave; a handler that throws writes nothing (and its event is delivered again). Keys: 1 to 2048
// bytes; values: 1 MiB; 1000 writes per task. One alarm per object, delivered at least once.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const durable = globalThis.__rt.durable;
export const storage = globalThis.__rt.storage;
