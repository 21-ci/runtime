// `std:secrets` -- the service's secrets (the shapes are in docs/handlers.md).
//
//   secrets.get(NAME)          -> string | null     a secret's value; null when it is not set
//   secrets.require(...NAMES)  -> { NAME: string }  every value, or one `secret_missing` error naming
//                                                   every missing secret and how to set it
//
// Only secret values are reachable (a plain env var is not one). Names are `[A-Z_][A-Z0-9_]*`, not
// `RT_*`; another name is `validation_failed`. The object is not enumerable and not serializable.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const secrets = globalThis.__rt.secrets;
