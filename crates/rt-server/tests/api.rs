//! Control API tests (wave 2, lane T-F): the OpenAPI contract (API-01), the API conventions
//! (API-02..14) and auth / scopes / tenancy / quotas at the control API (SEC-02..06, 09, 14, 20,
//! 32, 34). Everything boots the real server in-process through `NodeHarness`.
//!
//! One test binary per theme (`api`, `flows`, `process`) keeps the number of V8/wasmtime-linked
//! executables small (disk is tight) while isolating process-level failures.

#[path = "common/mod.rs"]
mod common;

#[path = "api/access.rs"]
mod access;
#[path = "api/control.rs"]
mod control;
#[path = "api/edge_control.rs"]
mod edge_control;
#[path = "api/edge_keys.rs"]
mod edge_keys;
#[path = "api/edge_mcp.rs"]
mod edge_mcp;
#[path = "api/edge_upload.rs"]
mod edge_upload;
#[path = "api/introspection.rs"]
mod introspection;
#[path = "api/invocations_api.rs"]
mod invocations_api;
#[path = "api/mcp.rs"]
mod mcp;
#[path = "api/openapi.rs"]
mod openapi;
#[path = "api/projects.rs"]
mod projects;
#[path = "api/sdk.rs"]
mod sdk;
#[path = "api/tenancy.rs"]
mod tenancy;
#[path = "api/tokens.rs"]
mod tokens;
#[path = "api/topics_api.rs"]
mod topics_api;
