//! Edge gateway: a WASI P2 HTTP component in front of internal services.
//!
//! Per request, in this order, before any backend sees anything:
//!   1. method and path validation, routing          (400 / 404 / 405)
//!   2. per-client-address rate limit in `std:host/kv` (429 + Retry-After), keyed on the platform's
//!      `x-rt-client-ip`, so a flood of bad tokens is cut before any token work
//!   3. `Authorization: Bearer <HS256 JWT>` checks    (401): signature in constant time, alg pinned,
//!      short lifetime, audience, optional issuer; keys from the `GATEWAY_KEYS` secret
//!   4. scope for the route and method                (403)
//!   5. per-subject rate limit in `std:host/kv`        (429 + Retry-After)
//!   6. body rules: JSON only, size cap, well-formed  (411 / 413 / 415 / 400)
//!   7. forward with `std:host/call` to a backend that accepts calls only from this gateway; the
//!      backend gets `x-gw-sub`, `x-gw-scopes`, `x-gw-request-id`, `x-gw-client-ip` and nothing
//!      the client invented
//!   8. after the response: the audit event to the operator's audit log through the operator SDK
//!      (`std:host/sdk`, `audit.ship`), then to the audit service with `std:host/queue` (dedup id = task id)
//!
//! Everything runs inside the component: a fresh instance per request is ready in microseconds
//! and is gone afterwards, so no request can see another's memory.

pub mod audit;
pub mod config;
pub mod ratelimit;
pub mod request;
pub mod token;

#[cfg(target_arch = "wasm32")]
mod component;
