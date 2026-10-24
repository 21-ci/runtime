// `std:sdk/server` -- the SDK side, for an operator SDK service only.
//
//   export default defineSdk({
//     authorize?(caller, method, args) -> boolean     optional: checks every call first
//     methods: { async ping(args, ctx) {...}, invoices: { async create(args, ctx) {...} } },
//   });
//
// `ctx.caller` is the calling service as the runtime stamped it: `{ tenant, project, environment,
// service: { id, slug }, plan, sdk: { name, version } }`. Every method must authorize by it: a
// method whose source never reads `caller` is refused when the SDK is defined
// (`sdk_unchecked_caller`) unless `authorize` is given. A method runs only for an SDK call.
// Operator bindings (`platform.fetch`) from an SDK forward the caller's tenant, not `sys`.
export const defineSdk = globalThis.__rt.sdk.defineSdk;
