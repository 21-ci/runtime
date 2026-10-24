// `std:services` -- service-to-service calls in the same tenant.
//
//   services.fetch(slug, request) -> Response    the HTTP shape: the callee answers any response
//   services.call(slug, method, arg?) -> value   RPC: the callee's `rpc[method](arg, ctx)`, resolved;
//                                                throws ServiceError
//   services.bind(slug) -> proxy                 `await proxy.method(arg)` is services.call(slug, "method", arg)
//   call(slug, request) -> Response              the old name of services.fetch, kept
//   ServiceError: { code, message }              every RPC failure, typed (no stack from the callee)
//
// A callee exports its methods as `export const rpc = { async method(arg, ctx) {...} }` (or `default.rpc`);
// dotted names reach nested objects. `ctx.caller` is `{ service: { id, slug }, project, environment }` as
// the runtime stamped it. The argument and the result are JSON, at most 256 KiB each way.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const services = globalThis.__rt.services;
export const call = globalThis.__rt.services.fetch;
export const ServiceError = globalThis.__rt.serviceError;
