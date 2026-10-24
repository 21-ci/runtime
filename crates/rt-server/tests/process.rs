//! Tests that need the real `rt` binary as a child process (wave 2, lane T-F): SIGTERM drain
//! (ARC-03), the control API without a root token (SEC-01), secrets never reaching logs (SEC-17)
//! and the CLI. Kept in their own test binary so a misbehaving process cannot take the in-process
//! suites down with it.

#[path = "common/mod.rs"]
mod common;

#[path = "process/node.rs"]
mod node;

#[path = "process/edge_faults.rs"]
mod edge_faults;
