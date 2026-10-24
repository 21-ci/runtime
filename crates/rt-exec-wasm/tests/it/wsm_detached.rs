//! WSM-16: a pre-instantiated (warm) or parked instance runs no task: its start code gets a
//! detached host handle, and every `std:host` import refuses (`unavailable`), outbound HTTP is
//! denied, logs are dropped, and nothing is charged to the tenant.

use crate::common;

use common::{Req, Rig, MIB};
use rt_exec_wasm::host_wit::wit::std::host::{blob, call, kv, log, platform, queue, sql};
use rt_exec_wasm::{FrozenClock, GovernorLimiter, GuardedHttpHooks, GuestLogs, WasmState};
use rt_mem::{MemCategory, MemoryGovernor as _, Owner};
use wasmtime::component::ResourceTable;
use wasmtime_wasi_http::WasiHttpCtx;

fn detached_state(rig: &Rig) -> WasmState {
    let svc = rt_testkit::service_ref(1, 1);
    let host = rig.exec.host().detached_handle(svc);
    let reservation = rig
        .governor
        .reserve(MemCategory::WasmReuse, MIB, Owner::NONE)
        .expect("reserve");
    WasmState {
        wasi_ctx: wasmtime_wasi::WasiCtxBuilder::new().build(),
        http_ctx: WasiHttpCtx::new(),
        table: ResourceTable::new(),
        limiter: GovernorLimiter::new(reservation, usize::MAX),
        http_hooks: GuardedHttpHooks { host: host.clone() },
        host,
        clock: FrozenClock::new(),
        logs: GuestLogs::new(),
    }
}

#[tokio::test]
async fn ac_wsm_16_every_rt_host_import_refuses_outside_a_task() {
    let rig = Rig::new();
    let mut st = detached_state(&rig);
    assert_eq!(
        kv::Host::get(&mut st, b"k".to_vec()).await.expect_err("kv").code,
        "unavailable"
    );
    let set = kv::Host::set(
        &mut st,
        b"k".to_vec(),
        kv::Value::Bytes(vec![1]),
        kv::SetOptions { ttl_seconds: None },
    )
    .await;
    assert_eq!(set.expect_err("kv set").code, "unavailable");
    let incr = kv::Host::incr(&mut st, b"k".to_vec(), 1, kv::SetOptions { ttl_seconds: None }).await;
    assert_eq!(incr.expect_err("kv incr").code, "unavailable");
    let stmt = sql::Statement {
        sql: "select 1".into(),
        args: Vec::new(),
    };
    assert_eq!(
        sql::Host::execute(&mut st, stmt).await.expect_err("sql").code,
        "unavailable"
    );
    assert_eq!(
        blob::Host::head(&mut st, "k".into()).await.expect_err("blob").code,
        "unavailable"
    );
    let opts = queue::SendOptions {
        delay_seconds: None,
        dedup_id: None,
    };
    assert_eq!(
        queue::Host::enqueue(&mut st, "b".into(), Vec::new(), opts)
            .await
            .expect_err("queue")
            .code,
        "unavailable"
    );
    let req = call::Request {
        method: "GET".into(),
        path_with_query: "/".into(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    assert_eq!(
        call::Host::call(&mut st, "b".into(), req.clone())
            .await
            .expect_err("call")
            .code,
        "unavailable"
    );
    assert_eq!(
        platform::Host::fetch(&mut st, "acme".into(), req)
            .await
            .expect_err("platform")
            .code,
        "unavailable"
    );
    log::Host::log(&mut st, log::Level::Info, "from start code".into()).await;
    assert!(
        rig.log_records().is_empty(),
        "start code logged under no task: {:?}",
        rig.log_records()
    );
    assert!(rig.calls.calls.lock().is_empty() && rig.calls.enqueued.lock().is_empty());
}

#[tokio::test]
async fn ac_wsm_16_a_parked_instance_keeps_no_task_capabilities() {
    // After a clean task the instance is parked with a detached handle: a later task gets its own.
    let mut rig = Rig::new();
    let kvp = common::prepared("kv");
    assert_eq!(
        rig.run(&kvp, Req::get("/roundtrip?key=a&value=1").reuse()).await.text(),
        "1"
    );
    assert_eq!(
        rig.run(&kvp, Req::get("/get?key=a").reuse()).await.text(),
        "1",
        "the next task has working capabilities"
    );
}
