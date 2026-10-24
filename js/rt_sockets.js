// `std:sockets` -- WebSockets the node holds for this service (docs/handlers.md "WebSockets").
//
//   sockets.accept(req, { tags?, attachment?, protocol? }) -> Response   from fetch(): upgrade req
//   sockets.send(id, data, { immediate? })        data: string | ArrayBuffer | ArrayBufferView
//   sockets.broadcast(data, { object?, tag?, except?, immediate? }) -> { sent }
//   sockets.close(id, code = 1000, reason = "")    sockets.closeAll({ object?, tag?, code?, reason? }) -> { closed }
//   sockets.list({ object?, tag?, limit?, cursor? }) -> { sockets: Socket[], cursor? }
//   sockets.get(id) -> Socket | null               sockets.count({ object?, tag? }) -> number
//   sockets.objects({ prefix?, limit?, cursor? }) -> { objects: [{ name, sockets }], cursor? }
//   sockets.setTags(id, tags)   sockets.setAttachment(id, value)   sockets.getAttachment(id)
//   sockets.setAutoResponse({ request, response } | null, { object? })
//
// Events run the default export's `webSocketMessage(ws, message, ctx)`, `webSocketClose(ws, code,
// reason, wasClean, ctx)` and `webSocketError(ws, error, ctx)`, one task per event, one at a time per
// object. Sends of an event task leave when its handler returned; a failed handler is delivered again.
// Errors carry `.code`: not_found, invalid_request, too_large, limit_exceeded, slow_consumer,
// unavailable, not_upgradable. Tags: at most 10 of 256 bytes; an attachment: 16 KiB serialized.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const sockets = globalThis.__rt.sockets;
