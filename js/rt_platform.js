// `std:platform` -- operator bindings without credentials.
//
//   platform.fetch(binding, request) -> Response
//
// A denied call (method/path not allowed, `..` traversal, plan not allowed) does NOT throw: it
// resolves to a Response with status 403 and header `x-rt-reason: binding_denied`.
// Injected credentials never reach guest memory.
export const platform = globalThis.__rt.platform;
