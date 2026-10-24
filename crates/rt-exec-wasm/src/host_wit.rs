//! The `std:host` WIT imports (`/wit/rt-host.wit`: `log`, `kv`, `sql`, `blob`, `queue`, `call`,
//! `platform`, `sdk`, …) implemented on [`WasmState`] over the task's [`rt_host::HostHandle`].
//!
//! `platform.fetch` goes through the same `rt_host::PlatformBindings` facility as the Deno
//! `std:platform` module: plan / method / path checks, credentials injected upstream (never in
//! guest memory), refusals as responses with `x-rt-reason` (contracts C-8).
//!
//! `queue` and `call` go through the same `rt_host::Calls` seam as the Deno SDK (`std:queue`,
//! `std:services`), so the target's `call.allowed_callers` / environment, the plan's triggers,
//! cross-tenant rules and call depth are enforced there, not here.
//!
//! Every error crossing into the guest is a stable `{code, message}` record built from
//! [`rt_host::HostError`] (SEC-26: no paths, no driver text). A host call that waited is an I/O
//! boundary, so the frozen clock catches up with real time afterwards.
//!
//! Outside a task (an instance's start code during pre-instantiation, see `crate::warm`) the
//! store holds a detached handle: every import refuses with `unavailable`, `log` drops the line.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method};
use rt_host::{CallError, EnqueueOpts, HostError};
use rt_obs::LogLevel;
use rt_store::{
    BatchMode, BlobListOptions, BlobMeta, KvListOptions, KvValue, SqlResult, SqlStatement, SqlValue, ValueTag,
};
use rt_types::Body;
use wasmtime::component::{HasSelf, Linker, Resource};

use crate::state::WasmState;

/// Code of every `std:host` call made outside a task.
const UNAVAILABLE: &str = "unavailable";
const UNAVAILABLE_MSG: &str = "std:host is not available outside a task";

pub mod wit {
    // World `runtime-host` = every `guest` import plus the optional `rpc` and `socket-handler`
    // exports (looked up per component: [`crate::prepared::WasmPrepared`]).
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "runtime-host",
        imports: { default: async },
        exports: { default: async },
        with: { "std:host/lock.lease": super::LockLease, "std:host/ws-client.connection": crate::host_ws::WsConnection },
    }); // LCOV_EXCL_LINE: the bindgen! expansion's code is attributed to its closing line
}

use wit::std::host::{blob, call, kv, lock, log, platform, queue, schedule, sdk, secrets, sql};

/// Link `std:host/{log,kv,sql,blob,queue,call,platform,secrets}` into `linker`.
pub fn add_to_linker(linker: &mut Linker<WasmState>) -> wasmtime::Result<()> {
    log::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    kv::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    secrets::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    lock::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    schedule::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    sql::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    blob::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    queue::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    call::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    platform::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    sdk::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    wit::std::host::sockets::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    wit::std::host::durable::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    wit::std::host::ws_client::add_to_linker::<WasmState, HasSelf<WasmState>>(linker, |s| s)?;
    Ok(())
}

// ---- log ---------------------------------------------------------------------------------------

fn log_level(level: log::Level) -> LogLevel {
    match level {
        log::Level::Debug => LogLevel::Debug,
        log::Level::Info => LogLevel::Info,
        log::Level::Warn => LogLevel::Warn,
        log::Level::Error => LogLevel::Error,
    }
}

impl log::Host for WasmState {
    async fn log(&mut self, level: log::Level, message: String) {
        let level = log_level(level);
        self.host.log(level, message);
    }

    async fn log_fields(&mut self, level: log::Level, message: String, fields_json: String) -> Result<(), log::Error> {
        let level = log_level(level);
        self.host
            .log_fields(level, message, &fields_json)
            .map_err(|e| log::Error {
                code: e.code.to_string(),
                message: e.message,
            })
    }
}

// ---- secrets -----------------------------------------------------------------------------------

impl secrets::Host for WasmState {
    async fn get(&mut self, name: String) -> Result<Option<String>, secrets::Error> {
        let r = self.clock.io(self.host.secret(&name)).await;
        r.map_err(|e| secrets::Error {
            code: e.code.to_string(),
            message: e.message,
        })
    }
}

// ---- lock --------------------------------------------------------------------------------------

/// The host side of a `lock.lease` resource: the hold ID of the task (see `HostHandle::lock_acquire`).
pub struct LockLease {
    id: u64,
}

impl lock::Host for WasmState {
    async fn acquire(&mut self, key: String, wait_ms: u32, lease_ms: u32) -> Result<Resource<LockLease>, lock::Error> {
        let wait = Duration::from_millis(u64::from(wait_ms));
        let lease = Duration::from_millis(u64::from(lease_ms));
        let acquired = self.clock.io(self.host.lock_acquire(&key, wait, lease)).await;
        let id = acquired.map_err(|e| lock::Error {
            code: e.code.to_string(),
            message: e.message,
        })?;
        self.table.push(LockLease { id }).map_err(|e| lock::Error {
            code: "internal".to_string(),
            message: e.to_string(),
        })
    }
}

impl lock::HostLease for WasmState {
    async fn release(&mut self, this: Resource<LockLease>) {
        if let Ok(lease) = self.table.get(&this) {
            self.host.lock_release(lease.id);
        }
    }

    async fn drop(&mut self, this: Resource<LockLease>) -> wasmtime::Result<()> {
        let lease = self.table.delete(this)?;
        self.host.lock_release(lease.id);
        Ok(())
    }
}

// ---- schedule ----------------------------------------------------------------------------------

fn schedule_err(e: HostError) -> schedule::Error {
    schedule::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn schedule_entry(id: String, run_at_ms: i64) -> schedule::Entry {
    schedule::Entry { id, run_at_ms }
}

impl schedule::Host for WasmState {
    async fn after(
        &mut self,
        seconds: u64,
        payload_json: String,
        id: Option<String>,
    ) -> Result<schedule::Entry, schedule::Error> {
        let r = self
            .clock
            .io(self
                .host
                .schedule_after(id, Duration::from_secs(seconds), Bytes::from(payload_json)))
            .await;
        r.map(|(id, run_at)| schedule_entry(id, run_at)).map_err(schedule_err)
    }

    async fn at(
        &mut self,
        at_ms: i64,
        payload_json: String,
        id: Option<String>,
    ) -> Result<schedule::Entry, schedule::Error> {
        let r = self
            .clock
            .io(self.host.schedule_at(id, at_ms, Bytes::from(payload_json)))
            .await;
        r.map(|(id, run_at)| schedule_entry(id, run_at)).map_err(schedule_err)
    }

    async fn cancel(&mut self, id: String) -> Result<bool, schedule::Error> {
        let r = self.clock.io(self.host.schedule_cancel(&id)).await;
        r.map_err(schedule_err)
    }

    async fn list(&mut self, after: Option<String>, limit: u32) -> Result<schedule::Page, schedule::Error> {
        let limit = (limit as usize).clamp(1, 100);
        let r = self.clock.io(self.host.schedule_list(after.as_deref(), limit)).await;
        let entries = r.map_err(schedule_err)?;
        let cursor = (entries.len() == limit)
            .then(|| entries.last().map(|e| e.id.clone()))
            .flatten();
        Ok(schedule::Page {
            entries: entries.into_iter().map(|e| schedule_entry(e.id, e.run_at_ms)).collect(),
            cursor,
        })
    }
}

// ---- kv ----------------------------------------------------------------------------------------

fn kv_err(e: HostError) -> kv::Error {
    kv::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn kv_in(v: kv::Value) -> KvValue {
    match v {
        kv::Value::Json(s) => KvValue {
            tag: ValueTag::Json,
            data: Bytes::from(s),
        },
        kv::Value::Bytes(b) => KvValue {
            tag: ValueTag::Bytes,
            data: Bytes::from(b),
        },
    }
}

fn kv_out(v: KvValue) -> kv::Value {
    match v.tag {
        ValueTag::Json => kv::Value::Json(String::from_utf8_lossy(&v.data).into_owned()),
        ValueTag::Bytes => kv::Value::Bytes(v.data.to_vec()),
    }
}

fn kv_gate(st: &WasmState) -> Result<(), kv::Error> {
    if st.host.is_detached() {
        return Err(kv::Error {
            code: UNAVAILABLE.into(),
            message: UNAVAILABLE_MSG.into(),
        });
    }
    Ok(())
}

impl kv::Host for WasmState {
    async fn get(&mut self, key: Vec<u8>) -> Result<Option<kv::Value>, kv::Error> {
        kv_gate(self)?;
        let r = self.clock.io(self.host.kv().get(&key)).await;
        r.map(|v| v.map(kv_out)).map_err(kv_err)
    }

    async fn set(&mut self, key: Vec<u8>, value: kv::Value, opts: kv::SetOptions) -> Result<(), kv::Error> {
        kv_gate(self)?;
        let ttl = opts.ttl_seconds.map(Duration::from_secs);
        let r = self.clock.io(self.host.kv().set(&key, kv_in(value), ttl)).await;
        r.map_err(kv_err)
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<bool, kv::Error> {
        kv_gate(self)?;
        let r = self.clock.io(self.host.kv().delete(&key)).await;
        r.map_err(kv_err)
    }

    async fn incr(&mut self, key: Vec<u8>, delta: i64, opts: kv::SetOptions) -> Result<i64, kv::Error> {
        kv_gate(self)?;
        let ttl = opts.ttl_seconds.map(Duration::from_secs);
        let r = self.clock.io(self.host.kv().incr(&key, delta, ttl)).await;
        r.map_err(kv_err)
    }

    async fn cas(
        &mut self,
        key: Vec<u8>,
        expected: Option<kv::Value>,
        next: Option<kv::Value>,
        opts: kv::SetOptions,
    ) -> Result<bool, kv::Error> {
        kv_gate(self)?;
        let ttl = opts.ttl_seconds.map(Duration::from_secs);
        let r = self
            .clock
            .io(self.host.kv().cas(&key, expected.map(kv_in), next.map(kv_in), ttl))
            .await;
        r.map_err(kv_err)
    }

    async fn expire(&mut self, key: Vec<u8>, opts: kv::SetOptions) -> Result<bool, kv::Error> {
        kv_gate(self)?;
        let ttl = opts.ttl_seconds.map(Duration::from_secs);
        let r = self.clock.io(self.host.kv().expire(&key, ttl)).await;
        r.map_err(kv_err)
    }

    async fn ttl(&mut self, key: Vec<u8>) -> Result<Option<u64>, kv::Error> {
        kv_gate(self)?;
        let r = self.clock.io(self.host.kv().ttl(&key)).await;
        // Rounded up, as `kv.ttl` in the Deno guest: a live key never reads 0.
        r.map(|left| left.map(|d| d.as_millis().div_ceil(1000) as u64))
            .map_err(kv_err)
    }

    async fn list(&mut self, opts: kv::ListOptions) -> Result<kv::ListPage, kv::Error> {
        kv_gate(self)?;
        let opts = KvListOptions {
            prefix: Bytes::from(opts.prefix),
            cursor: opts.cursor.map(Bytes::from),
            limit: opts.limit as usize,
            reverse: opts.reverse,
        };
        let r = self.clock.io(self.host.kv().list(opts)).await;
        r.map(|page| kv::ListPage {
            entries: page.entries.into_iter().map(|(k, v)| (k.to_vec(), kv_out(v))).collect(),
            next_cursor: page.next_cursor.map(|c| c.to_vec()),
        })
        .map_err(kv_err)
    }
}

// ---- sql ---------------------------------------------------------------------------------------

fn sql_err(e: HostError) -> sql::Error {
    sql::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn sql_in(v: sql::SqlValue) -> SqlValue {
    match v {
        sql::SqlValue::Null => SqlValue::Null,
        sql::SqlValue::Integer(i) => SqlValue::Integer(i),
        sql::SqlValue::Real(f) => SqlValue::Real(f),
        sql::SqlValue::Text(t) => SqlValue::Text(t),
        sql::SqlValue::Blob(b) => SqlValue::Blob(b),
    }
}

fn sql_out(v: SqlValue) -> sql::SqlValue {
    match v {
        SqlValue::Null => sql::SqlValue::Null,
        SqlValue::Integer(i) => sql::SqlValue::Integer(i),
        SqlValue::Real(f) => sql::SqlValue::Real(f),
        SqlValue::Text(t) => sql::SqlValue::Text(t),
        SqlValue::Blob(b) => sql::SqlValue::Blob(b),
    }
}

fn stmt_in(s: sql::Statement) -> SqlStatement {
    SqlStatement {
        sql: s.sql,
        args: s.args.into_iter().map(sql_in).collect(),
    }
}

fn result_out(r: SqlResult) -> sql::SqlResult {
    sql::SqlResult {
        columns: r.columns,
        rows: r
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(sql_out).collect())
            .collect(),
        rows_affected: r.rows_affected,
        last_insert_rowid: r.last_insert_rowid,
    }
}

fn sql_gate(st: &WasmState) -> Result<(), sql::Error> {
    if st.host.is_detached() {
        return Err(sql::Error {
            code: UNAVAILABLE.into(),
            message: UNAVAILABLE_MSG.into(),
        });
    }
    Ok(())
}

impl sql::Host for WasmState {
    async fn execute(&mut self, stmt: sql::Statement) -> Result<sql::SqlResult, sql::Error> {
        sql_gate(self)?;
        let r = self.clock.io(self.host.sql().execute(stmt_in(stmt))).await;
        r.map(result_out).map_err(sql_err)
    }

    async fn batch(&mut self, stmts: Vec<sql::Statement>, write: bool) -> Result<Vec<sql::SqlResult>, sql::Error> {
        sql_gate(self)?;
        let mode = if write { BatchMode::Write } else { BatchMode::Read };
        let r = self
            .clock
            .io(self.host.sql().batch(stmts.into_iter().map(stmt_in).collect(), mode))
            .await;
        r.map(|v| v.into_iter().map(result_out).collect()).map_err(sql_err)
    }
}

// ---- blob --------------------------------------------------------------------------------------

fn blob_err(e: HostError) -> blob::Error {
    blob::Error {
        code: e.code.to_string(),
        message: e.message,
    }
}

fn meta_out(m: BlobMeta) -> blob::Meta {
    blob::Meta {
        key: m.key,
        size: m.size,
        content_type: m.content_type,
    }
}

fn blob_gate(st: &WasmState) -> Result<(), blob::Error> {
    if st.host.is_detached() {
        return Err(blob::Error {
            code: UNAVAILABLE.into(),
            message: UNAVAILABLE_MSG.into(),
        });
    }
    Ok(())
}

impl blob::Host for WasmState {
    async fn get(&mut self, key: String) -> Result<Option<(blob::Meta, Vec<u8>)>, blob::Error> {
        blob_gate(self)?;
        // Bodies are buffered in this WIT revision (see rt-host.wit); bounded by the response cap.
        let cap = self.host.limits().max_response_buffered_bytes;
        let facade = self.host.blob();
        let read = async {
            match facade.get(&key).await {
                Ok(Some(obj)) => match obj.body.collect(cap).await {
                    Ok(bytes) => Ok(Some((meta_out(obj.meta), bytes.to_vec()))),
                    Err(_) => Err(HostError::new(
                        "blob_too_large",
                        "blob exceeds the buffered response limit",
                    )),
                },
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        };
        let r = self.clock.io(read).await;
        r.map_err(blob_err)
    }

    async fn head(&mut self, key: String) -> Result<Option<blob::Meta>, blob::Error> {
        blob_gate(self)?;
        let r = self.clock.io(self.host.blob().head(&key)).await;
        r.map(|m| m.map(meta_out)).map_err(blob_err)
    }

    async fn put(
        &mut self,
        key: String,
        content_type: Option<String>,
        data: Vec<u8>,
    ) -> Result<blob::Meta, blob::Error> {
        blob_gate(self)?;
        let r = self
            .clock
            .io(self.host.blob().put(&key, Body::from(data), content_type))
            .await;
        r.map(meta_out).map_err(blob_err)
    }

    async fn delete(&mut self, key: String) -> Result<bool, blob::Error> {
        blob_gate(self)?;
        let r = self.clock.io(self.host.blob().delete(&key)).await;
        r.map_err(blob_err)
    }

    async fn list(
        &mut self,
        prefix: String,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<blob::ListPage, blob::Error> {
        blob_gate(self)?;
        let opts = BlobListOptions {
            prefix,
            cursor,
            limit: limit as usize,
        };
        let r = self.clock.io(self.host.blob().list(opts)).await;
        r.map(|p| blob::ListPage {
            items: p.items.into_iter().map(meta_out).collect(),
            next_cursor: p.next_cursor,
        })
        .map_err(blob_err)
    }
}

// ---- queue -------------------------------------------------------------------------------------

impl queue::Host for WasmState {
    async fn enqueue(&mut self, slug: String, body: Vec<u8>, opts: queue::SendOptions) -> Result<String, queue::Error> {
        if self.host.is_detached() {
            return Err(queue::Error {
                code: UNAVAILABLE.into(),
                message: UNAVAILABLE_MSG.into(),
            });
        }
        let o = EnqueueOpts {
            delay: Duration::from_secs(opts.delay_seconds.unwrap_or(0)),
            dedup_id: opts.dedup_id,
        };
        let r = self.clock.io(self.host.enqueue(&slug, Bytes::from(body), o)).await;
        r.map(|id| id.to_string()).map_err(|e| queue::Error {
            code: e.code().to_string(),
            message: e.code().to_string(),
        })
    }
}

// ---- call --------------------------------------------------------------------------------------

pub(crate) fn call_err(code: &str, message: impl Into<String>) -> call::Error {
    call::Error {
        code: code.to_string(),
        message: message.into(),
    }
}

/// The guest's request as an `rt_host` request: method, path + query (relative), headers, body.
pub(crate) fn call_request(req: call::Request) -> Result<rt_host::Request, call::Error> {
    let invalid = |what: &str| call_err("invalid_request", format!("invalid {what}"));
    let method = Method::from_bytes(req.method.as_bytes()).map_err(|_| invalid("method"))?;
    if !req.path_with_query.starts_with('/') {
        return Err(invalid("path-with-query (must start with `/`)"));
    }
    let uri: http::Uri = req.path_with_query.parse().map_err(|_| invalid("path-with-query"))?;
    let mut builder = http::Request::builder().method(method).uri(uri);
    for (name, value) in req.headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid("header name"))?;
        let value = HeaderValue::from_bytes(&value).map_err(|_| invalid("header value"))?;
        builder = builder.header(name, value);
    }
    builder.body(Body::from(req.body)).map_err(|_| invalid("request"))
}

impl call::Host for WasmState {
    async fn call(&mut self, slug: String, req: call::Request) -> Result<call::Response, call::Error> {
        if self.host.is_detached() {
            return Err(call_err(UNAVAILABLE, UNAVAILABLE_MSG));
        }
        let request = call_request(req)?;
        let cap = self.host.limits().max_response_buffered_bytes;
        let host = self.host.clone();
        let exchange = async move {
            match host.call(&slug, request).await {
                Ok(resp) => {
                    let (parts, body) = resp.into_parts();
                    let headers = parts
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
                        .collect();
                    match body.collect(cap).await {
                        Ok(bytes) => Ok(call::Response {
                            status: parts.status.as_u16(),
                            headers,
                            body: bytes.to_vec(),
                        }),
                        Err(_) => Err(call_err(
                            "response_too_large",
                            "the response exceeds the buffered response limit",
                        )),
                    }
                }
                // Past depth 15 the call resolves to `508` + `x-rt-reason: call_depth` (TRG-15).
                Err(e @ CallError::Depth) => Ok(call::Response {
                    status: 508,
                    headers: vec![("x-rt-reason".to_string(), e.code().as_bytes().to_vec())],
                    body: Vec::new(),
                }),
                Err(e) => Err(call_err(e.code(), e.code())),
            }
        };
        self.clock.io(exchange).await
    }

    async fn call_rpc(&mut self, slug: String, method: String, args_json: String) -> Result<String, call::Error> {
        if self.host.is_detached() {
            return Err(call_err(UNAVAILABLE, UNAVAILABLE_MSG));
        }
        let host = self.host.clone();
        let r = self
            .clock
            .io(async move { host.rpc_call(&slug, &method, &args_json).await })
            .await;
        r.map_err(|e| call::Error {
            code: e.code,
            message: e.message,
        })
    }
}

// ---- sdk ---------------------------------------------------------------------------------------

impl sdk::Host for WasmState {
    async fn invoke(&mut self, name: String, method: String, args_json: String) -> Result<String, sdk::Error> {
        if self.host.is_detached() {
            return Err(sdk::Error {
                code: UNAVAILABLE.into(),
                message: UNAVAILABLE_MSG.into(),
            });
        }
        let host = self.host.clone();
        let r = self
            .clock
            .io(async move { host.sdk_invoke(&name, &method, &args_json).await })
            .await;
        r.map_err(|e| sdk::Error {
            code: e.code,
            message: e.message,
        })
    }
}

// ---- platform ----------------------------------------------------------------------------------

impl platform::Host for WasmState {
    async fn fetch(&mut self, binding: String, req: call::Request) -> Result<call::Response, platform::Error> {
        let err = |e: call::Error| platform::Error {
            code: e.code,
            message: e.message,
        };
        if self.host.is_detached() {
            return Err(err(call_err(UNAVAILABLE, UNAVAILABLE_MSG)));
        }
        let request = call_request(req).map_err(err)?;
        let cap = self.host.limits().max_response_buffered_bytes;
        let host = self.host.clone();
        let exchange = async move {
            // Denials, plan refusals and rate limits arrive as responses already (C-8); an
            // unknown binding or a failed upstream becomes one here, the same way.
            let resp = match host.platform_fetch(&binding, request).await {
                Ok(resp) => resp,
                Err(e) => e.to_response(),
            };
            let (parts, body) = resp.into_parts();
            let headers = parts
                .headers
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
                .collect();
            match body.collect(cap).await {
                Ok(bytes) => Ok(call::Response {
                    status: parts.status.as_u16(),
                    headers,
                    body: bytes.to_vec(),
                }),
                Err(_) => Err(err(call_err(
                    "response_too_large",
                    "the response exceeds the buffered response limit",
                ))),
            }
        };
        self.clock.io(exchange).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_guest_log_level_maps_to_the_service_log_level() {
        assert_eq!(log_level(log::Level::Debug), LogLevel::Debug);
        assert_eq!(log_level(log::Level::Info), LogLevel::Info);
        assert_eq!(log_level(log::Level::Warn), LogLevel::Warn);
        assert_eq!(log_level(log::Level::Error), LogLevel::Error);
    }
}
