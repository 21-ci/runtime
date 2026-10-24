//! The T1 value of a WASM service: the compiled `Component` plus the linked `ProxyPre`.

use rt_types::PreparedKey;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::p2::add_to_linker_async;
use wasmtime_wasi::p2::bindings::clocks::monotonic_clock;
use wasmtime_wasi::p2::bindings::io::poll;
use wasmtime_wasi_http::p2::add_only_http_to_linker_async;
use wasmtime_wasi_http::p2::bindings::{Proxy, ProxyIndices, ProxyPre};

use crate::clock::{FrozenMonotonicHost, FrozenPoll, HasFrozenMonotonic, HasFrozenPoll};
use crate::host_wit;
use crate::state::WasmState;

/// The `std:host/rpc` export of a component (world `rpc-service`).
pub type RpcIndices = host_wit::wit::exports::std::host::rpc::GuestIndices;
/// The `std:host/rpc` export of one instance.
pub type RpcExport = host_wit::wit::exports::std::host::rpc::Guest;
/// The `std:host/socket-handler` export of a component (world `socket-service`).
pub type SocketIndices = host_wit::wit::exports::std::host::socket_handler::GuestIndices;
/// The `std:host/socket-handler` export of one instance.
pub type SocketExport = host_wit::wit::exports::std::host::socket_handler::Guest;

/// `Component` (from the mmap'd, MAC-verified `.cwasm`) + `InstancePre` with the linker wired.
/// Shared (`Arc`) by every task of the same component.
pub struct WasmPrepared {
    pub key: PreparedKey,
    pub component: Component,
    pub pre: ProxyPre<WasmState>,
    /// Where `wasi:http/incoming-handler` is (looked up once).
    proxy: ProxyIndices,
    /// Where the `std:host/rpc` export is; `None` when the component does not serve RPC.
    pub rpc: Option<RpcIndices>,
    /// Where the `std:host/socket-handler` export is; `None` when the component takes no socket
    /// events or alarms.
    pub socket: Option<SocketIndices>,
    /// Bytes charged to the T1 cache budget.
    pub accounted_bytes: u64,
}

/// One instance's exports: the HTTP handler and, when the component has it, the RPC export.
pub struct GuestExports {
    pub proxy: Proxy,
    pub rpc: Option<RpcExport>,
    pub socket: Option<SocketExport>,
}

impl WasmPrepared {
    pub fn new(
        key: PreparedKey,
        component: Component,
        pre: ProxyPre<WasmState>,
        accounted_bytes: u64,
    ) -> wasmtime::Result<Self> {
        let proxy = ProxyIndices::new(pre.instance_pre())?;
        let rpc = RpcIndices::new(pre.instance_pre()).ok();
        let socket = SocketIndices::new(pre.instance_pre()).ok();
        Ok(Self {
            key,
            component,
            pre,
            proxy,
            rpc,
            socket,
            accounted_bytes,
        })
    }

    /// A new instance in `store`, with its exports.
    pub(crate) async fn instantiate(&self, store: &mut Store<WasmState>) -> wasmtime::Result<GuestExports> {
        let instance = self.pre.instance_pre().instantiate_async(&mut *store).await?;
        let proxy = self.proxy.load(&mut *store, &instance)?;
        let rpc = match &self.rpc {
            Some(ix) => Some(ix.load(&mut *store, &instance)?),
            None => None,
        };
        let socket = match &self.socket {
            Some(ix) => Some(ix.load(&mut *store, &instance)?),
            None => None,
        };
        Ok(GuestExports { proxy, rpc, socket })
    }
}

/// Wire WASI + wasi:http (and, once added, `std:host/*`) imports for `component`.
///
/// Import policy (contracts C-7, see `rt_build::wasm_policy`): `wasi:io|clocks|random|cli|http`
/// and `std:host/*` are linked normally; `wasi:filesystem/*` is linked but the task's `WasiCtx`
/// has **no preopens**; `wasi:sockets/*` is linked with every address denied
/// (`WasiCtxBuilder::socket_addr_check` returning false). Any `0.2.x` WASI minor is accepted.
pub fn build_proxy_pre(engine: &Engine, component: &Component) -> wasmtime::Result<ProxyPre<WasmState>> {
    let mut linker: Linker<WasmState> = Linker::new(engine);
    add_to_linker_async(&mut linker)?;
    add_only_http_to_linker_async(&mut linker)?;
    // `wasi:io/poll` again, over the same resource table but advancing the frozen clock when a
    // blocking wait returns (SEC-22). Replaces the definition added above.
    linker.allow_shadowing(true);
    poll::add_to_linker::<WasmState, HasFrozenPoll>(&mut linker, |s| FrozenPoll {
        table: &mut s.table,
        clock: &s.clock,
    })?;
    // `wasi:clocks/monotonic-clock` again: a due subscription is ready without yielding, so ready
    // pollables never count as a wait (wasmtime-wasi ≥ 48 yields there).
    monotonic_clock::add_to_linker::<WasmState, HasFrozenMonotonic>(&mut linker, |s| FrozenMonotonicHost {
        table: &mut s.table,
        clock: &s.clock,
    })?;
    linker.allow_shadowing(false);
    host_wit::add_to_linker(&mut linker)?;
    ProxyPre::new(linker.instantiate_pre(component)?)
}
