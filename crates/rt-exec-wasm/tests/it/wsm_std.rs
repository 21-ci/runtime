//! WSM-21: a component built with the full Rust `std` for `wasm32-wasip2` (the default for Rust
//! services; `wasip2` crate pinned at WASI 0.2.12 like `sample/apps/edge-gateway`) imports
//! `wasi:cli/terminal-{input,output,stdin,stdout,stderr}`. The linker provides them (no terminal),
//! so such a component deploys, instantiates and runs: `println!` reaches the logs, `IsTerminal`
//! answers false, env and the wall clock work.

use crate::common;

use common::{fixture_bytes, prepared, shared, Req, Rig};
use rt_obs::LogLevel;
use rt_types::{RuntimeKind, UploadValidator};

const TERMINAL: [&str; 5] = [
    "terminal-input",
    "terminal-output",
    "terminal-stdin",
    "terminal-stdout",
    "terminal-stderr",
];

#[test]
fn ac_wsm_21_a_full_std_component_imports_the_terminal_interfaces_and_links() {
    let p = prepared("std_cli"); // compiles and links (`InstancePre`) or panics with the reason
    let ty = p.component.component_type();
    let imports: Vec<String> = ty.imports(&shared().engine).map(|(name, _)| name.to_string()).collect();
    for t in TERMINAL {
        let want = format!("wasi:cli/{t}@0.2.12");
        assert!(
            imports.contains(&want),
            "{want} not imported (the fixture is not what this test is about): {imports:?}"
        );
    }
    // and the deploy-time policy (contracts C-7) accepts it
    let builder = rt_build::DefaultBuilder::new(rt_build::UploadLimits::default());
    let bytes = bytes::Bytes::from(fixture_bytes("std_cli"));
    UploadValidator::validate(&builder, RuntimeKind::Wasm, "application/wasm", &bytes).expect("std component deploys");
}

#[tokio::test]
async fn ac_wsm_21_a_full_std_component_runs_without_a_terminal() {
    let mut rig = Rig::new();
    rig.control.env.lock().env.push(("GREETING".into(), "hi".into()));
    let p = prepared("std_cli");
    let r = rig.run(&p, Req::get("/")).await.reply();
    assert_eq!(r.status, 200);
    assert_eq!(r.kv("terminals"), "false,false,false");
    assert_eq!(r.kv("greeting"), "hi");
    assert_eq!(r.kv("time_ok"), "true");
    let logs = rig.log_records();
    assert!(
        logs.iter()
            .any(|l| l.msg == "std_cli println line" && l.level == LogLevel::Info),
        "{logs:?}"
    );
    assert!(
        logs.iter()
            .any(|l| l.msg == "std_cli eprintln line" && l.level == LogLevel::Error),
        "{logs:?}"
    );
}
