// `std:queue` -- enqueue to a service of the same tenant (contracts C-6).
//
//   enqueue(service, body, { delaySeconds?, dedupId? }) -> { id }
//
// `service` is the **slug** of a service in the same tenant.
export const enqueue = globalThis.__rt.queue.enqueue;
