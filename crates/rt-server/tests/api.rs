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
#[path = "api/app_bundle.rs"]
mod app_bundle;
#[path = "api/backups.rs"]
mod backups;
#[path = "api/branches.rs"]
mod branches;
#[path = "api/control.rs"]
mod control;
#[path = "api/data_browse.rs"]
mod data_browse;
#[path = "api/data_events_secrets.rs"]
mod data_events_secrets;
#[path = "api/edge_control.rs"]
mod edge_control;
#[path = "api/edge_keys.rs"]
mod edge_keys;
#[path = "api/edge_mcp.rs"]
mod edge_mcp;
#[path = "api/edge_upload.rs"]
mod edge_upload;
#[path = "api/from_url.rs"]
mod from_url;
#[path = "api/introspection.rs"]
mod introspection;
#[path = "api/invocations_api.rs"]
mod invocations_api;
#[path = "api/mcp.rs"]
mod mcp;
#[path = "api/migrations.rs"]
mod migrations;
#[path = "api/monitoring.rs"]
mod monitoring;
#[path = "api/openapi.rs"]
mod openapi;
#[path = "api/projects.rs"]
mod projects;
#[path = "api/promote.rs"]
mod promote;
#[path = "api/scratch.rs"]
mod scratch;
#[path = "api/sdk.rs"]
mod sdk;
#[path = "api/switch.rs"]
mod switch;
#[path = "api/telemetry.rs"]
mod telemetry;
#[path = "api/tenancy.rs"]
mod tenancy;
#[path = "api/tokens.rs"]
mod tokens;
#[path = "api/topics_api.rs"]
mod topics_api;
