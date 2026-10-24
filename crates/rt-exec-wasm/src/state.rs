//! Per-task `Store` data: WASI + wasi-http contexts, the resource table, the governor-backed
//! memory limiter and the guarded outbound-HTTP hooks.
//!
//! Sandbox: **no preopens** (`wasi:filesystem` has nothing to open), sockets denied, only the
//! wasi:http outgoing handler (through the egress guard) and the `std:host/*` imports reach the
//! outside.

use std::future::Future;
use std::sync::Arc;

use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use rt_host::{EgressError, HostHandle};
use rt_mem::Reservation;
use rt_types::{Body, BodyError};
use wasmtime::component::ResourceTable;
use wasmtime::ResourceLimiter;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    Error as HttpError, RequestOptions, WasiBody, WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks, WasiHttpView,
};

use crate::clock::FrozenClock;
use crate::stdio::GuestLogs;

/// Linear-memory limiter bound to the task's governor reservation.
///
/// Growth is charged with `Reservation::try_grow`; a refusal (budget exhausted or the plan's
/// `memory_mb` exceeded) traps the guest and the task ends as `Killed(Oom)` (WSM-03).
pub struct GovernorLimiter {
    pub reservation: Reservation,
    /// Plan/service memory ceiling in bytes.
    pub limit_bytes: usize,
    /// Set when a growth was refused, so the executor reports `Oom` rather than `GuestError`.
    pub oom: bool,
    /// Linear memory currently held by the instance (all memories).
    pub used: usize,
}

impl GovernorLimiter {
    pub fn new(reservation: Reservation, limit_bytes: usize) -> Self {
        Self {
            reservation,
            limit_bytes,
            oom: false,
            used: 0,
        }
    }
}

impl ResourceLimiter for GovernorLimiter {
    fn memory_growing(&mut self, current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        let new_used = self.used.saturating_add(desired.saturating_sub(current));
        if new_used > self.limit_bytes {
            self.oom = true;
            return Err(wasmtime::Error::msg("linear memory limit exceeded"));
        }
        // The task's initial reservation already covers the first bytes; charge only the excess.
        let held = self.reservation.bytes();
        if new_used as u64 > held && !self.reservation.try_grow(new_used as u64 - held) {
            self.oom = true;
            return Err(wasmtime::Error::msg("memory budget exhausted"));
        }
        self.used = new_used;
        Ok(true)
    }

    fn table_growing(&mut self, _current: usize, desired: usize, _maximum: Option<usize>) -> wasmtime::Result<bool> {
        // Tables are tiny next to linear memory; a flat cap keeps them bounded.
        Ok(desired <= 100_000)
    }
}

/// `wasi:http/outgoing-handler` gate: every guest request goes through `rt-host`'s egress guard.
pub struct GuardedHttpHooks {
    pub host: HostHandle,
}

fn body_error_to_http(e: BodyError) -> HttpError {
    match e {
        BodyError::TooLarge { limit } => HttpError::HttpResponseBodySize(Some(limit)),
        BodyError::Aborted(m) | BodyError::Io(m) => HttpError::InternalError(Some(m)),
    }
}

fn egress_error_to_http(e: &EgressError) -> HttpError {
    match e {
        EgressError::Denied => HttpError::HttpRequestDenied,
        EgressError::Limit => HttpError::ConnectionLimitReached,
        EgressError::TooLarge => HttpError::HttpResponseBodySize(None),
        EgressError::Timeout => HttpError::ConnectionTimeout,
        EgressError::Dns => HttpError::DestinationNotFound,
        // Stable codes only: the guest sees `egress_quota` / `egress_upstream` /
        // `egress_insecure_redirect`, never driver text.
        EgressError::Quota | EgressError::Upstream | EgressError::InsecureRedirect => {
            HttpError::InternalError(Some(e.code().to_string()))
        }
    }
}

/// The future `send_request` answers with: the response, and a future for the end of its I/O
/// (the egress guard streams the body itself, so that one is done at once).
type SendFuture = Box<
    dyn Future<
            Output = Result<
                (
                    http::Response<WasiBody>,
                    Box<dyn Future<Output = Result<(), HttpError>> + Send>,
                ),
                HttpError,
            >,
        > + Send,
>;

impl WasiHttpHooks for GuardedHttpHooks {
    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: Box<dyn Future<Output = Result<(), HttpError>> + Send>,
    ) -> SendFuture {
        // Timeouts: the egress guard's connect timeout and the task's deadline bound every fetch.
        let host = self.host.clone();
        let (parts, body) = request.into_parts();
        let stream = body
            .into_data_stream()
            .map(|r| r.map_err(|_| BodyError::Io(EgressError::Upstream.code().to_string())));
        let req = http::Request::from_parts(parts, Body::from_stream(stream));
        Box::new(async move {
            let resp = host.fetch(req).await.map_err(|e| egress_error_to_http(&e))?;
            let (parts, body) = resp.into_parts();
            let frames = body.map(|r| r.map(Frame::data).map_err(body_error_to_http));
            let incoming = http::Response::from_parts(parts, StreamBody::new(frames).boxed_unsync());
            let io: Box<dyn Future<Output = Result<(), HttpError>> + Send> = Box::new(async { Ok(()) });
            Ok((incoming, io))
        })
    }
}

/// Store data of one task.
pub struct WasmState {
    pub wasi_ctx: WasiCtx,
    pub http_ctx: WasiHttpCtx,
    pub table: ResourceTable,
    pub limiter: GovernorLimiter,
    pub http_hooks: GuardedHttpHooks,
    /// Scoped capabilities for the `std:host/*` imports.
    pub host: HostHandle,
    /// The guest's frozen `wasi:clocks` (advanced at I/O boundaries only).
    pub clock: Arc<FrozenClock>,
    /// stdout/stderr → log plumbing shared with the `WasiCtx`.
    pub logs: Arc<GuestLogs>,
}

impl WasiView for WasmState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for WasmState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http_ctx,
            table: &mut self.table,
            hooks: &mut self.http_hooks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_too_large_body_reports_its_limit_and_other_body_errors_are_internal() {
        assert!(matches!(
            body_error_to_http(BodyError::TooLarge { limit: 9 }),
            HttpError::HttpResponseBodySize(Some(9))
        ));
        for (e, msg) in [
            (BodyError::Aborted("cut".into()), "cut"),
            (BodyError::Io("reset".into()), "reset"),
        ] {
            assert!(matches!(body_error_to_http(e), HttpError::InternalError(Some(m)) if m == msg));
        }
    }

    #[test]
    fn egress_refusals_reach_the_guest_as_the_matching_wasi_http_error_or_a_stable_code() {
        assert!(matches!(
            egress_error_to_http(&EgressError::Denied),
            HttpError::HttpRequestDenied
        ));
        assert!(matches!(
            egress_error_to_http(&EgressError::Limit),
            HttpError::ConnectionLimitReached
        ));
        assert!(matches!(
            egress_error_to_http(&EgressError::TooLarge),
            HttpError::HttpResponseBodySize(None)
        ));
        assert!(matches!(
            egress_error_to_http(&EgressError::Timeout),
            HttpError::ConnectionTimeout
        ));
        assert!(matches!(
            egress_error_to_http(&EgressError::Dns),
            HttpError::DestinationNotFound
        ));
        for e in [EgressError::Quota, EgressError::Upstream, EgressError::InsecureRedirect] {
            let want = e.code().to_string();
            assert!(
                matches!(egress_error_to_http(&e), HttpError::InternalError(Some(c)) if c == want),
                "{e:?}"
            );
        }
    }
}
