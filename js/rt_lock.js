// `std:lock` -- named mutexes per service (the shapes are in docs/handlers.md).
//
//   lock(key, fn, { waitMs = 10000, leaseMs = 30000 }) -> the value of fn()
//       holds `key` while fn runs; waits at most `waitMs` for it (`lock_timeout` when it cannot get it)
//   lock.try(key, fn, { leaseMs = 30000 }) -> { acquired: false } | { acquired: true, value }
//       the same without waiting
//
// The lock is released when fn settles (also when it throws), when the task ends, is killed or hits a
// limit. `leaseMs` bounds a hold whose holder never releases it: after the lease the key is granted to
// the next waiter, so fn must finish within its lease. Keys are 1 to 512 bytes; `waitMs` at most
// 300000, `leaseMs` 1 to 900000. Locks are per service and per node.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const lock = globalThis.__rt.lock;
