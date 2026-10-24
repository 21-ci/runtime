//! The integration tests of `rt-cli`, one test binary (`cargo test -p rt-cli --test it`).
//! Every V8/wasmtime/SQLite-linked test executable costs tens of MB, so the files are modules
//! of this one target instead of one target each. Select a file with a name filter:
//! `cargo test -p rt-cli --test it <module>::`.

#[path = "../common/mod.rs"]
mod common;

mod config_cmd;
mod env_secret;
mod exit_codes;
mod hosts;
mod invoke;
mod mcp;
mod meta;
mod node_misc;
mod observe;
mod project;
mod push;
mod retry_doctor;
mod schedule_queue;
mod sdk;
mod service;
mod sockets;
mod streams;
mod tenant_key;
mod token;
mod versions;
