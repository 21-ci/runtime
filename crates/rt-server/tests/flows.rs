//! Flow tests (wave 2, lane T-F): end-to-end flows (E2E-01..07), restart (E2E-08, ARC-04, OBS-05,
//! TRG-08), observability (OBS-*), the invoke surface (TRG-01/04, SEC-07/08/15/18/19/25..29/35) and
//! queue / service-call triggers (TRG-09..17). Real server, in-process, temp data dir.

#[path = "common/mod.rs"]
mod common;

#[path = "flows/build_exec.rs"]
mod build_exec;
#[path = "flows/durable.rs"]
mod durable;
#[path = "flows/e2e.rs"]
mod e2e;
#[path = "flows/edge_invoke.rs"]
mod edge_invoke;
#[path = "flows/edge_ws.rs"]
mod edge_ws;
#[path = "flows/hot_path.rs"]
mod hot_path;
#[path = "flows/ingress.rs"]
mod ingress;
#[path = "flows/instances.rs"]
mod instances;
#[path = "flows/lock_schedule.rs"]
mod lock_schedule;
#[path = "flows/obs.rs"]
mod obs;
#[path = "flows/restart.rs"]
mod restart;
#[path = "flows/sdk.rs"]
mod sdk;
#[path = "flows/services_rpc.rs"]
mod services_rpc;
#[path = "flows/soak.rs"]
mod soak;
#[path = "flows/triggers.rs"]
mod triggers;
#[path = "flows/websockets.rs"]
mod websockets;
#[path = "flows/wiring.rs"]
mod wiring;
