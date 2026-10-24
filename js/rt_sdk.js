// `std:sdk` -- operator SDKs, run by the runtime.
//
//   sdk(name) -> proxy                 `await sdk("acme").invoices.create(arg)` calls the method
//                                      `invoices.create` of the SDK release this service's grants
//                                      select; the result is the method's JSON value
//   SdkError: { code, message }        every failure, typed (a ServiceError): the SDK's own codes, or
//                                      sdk_not_found, sdk_unavailable (removed), sdk_not_granted,
//                                      sdk_version_unavailable, method_not_found, validation_failed, ...
//
// No SDK code is in this service: its build replaces `import { acme } from "std:sdk"` (and
// `import acme from "sdk:acme"`) with a generated shim over `sdk("acme")` built from the SDK's
// contract. An SDK update, rollback or removal never rebuilds or redeploys it. The argument and
// the result are JSON, at most 256 KiB each way.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const sdk = globalThis.__rt.sdk.sdk;
export const SdkError = globalThis.__rt.sdk.SdkError;
