//! Ops behind the bootstrap JS (`js/bootstrap.js`): log, clock, env, response channel, `fetch`
//! through the egress guard, and the `std:kv|sql|blob|queue|services|platform` capabilities.
//! Every host call goes through the per-task [`HostHandle`] kept in `OpState` ([`TaskOps`]).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use deno_core::serde_v8::ToJsBuffer;
use deno_core::{op2, v8, JsBuffer, OpState, Resource, ResourceId};
use deno_error::JsErrorBox;
use futures_util::StreamExt;
use rt_host::{CallError, EnqueueOpts, HostError, HostHandle, Request, Response};
use rt_obs::LogLevel;
use rt_store::{BatchMode, BlobListOptions, BlobMeta, KvListOptions, KvValue, SqlStatement, SqlTx, ValueTag};
use rt_types::{Body, BodyError, BodySender, EnvBundle, KillReason, TaskId};
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use zeroize::Zeroizing;

use crate::clock::ClockState;
use crate::memory::MemAccount;
use crate::v8sql::{ArgsV8, SqlResultV8, SqlResultsV8, StmtsV8};

/// What the guest's response head turned into.
pub enum Head {
    Ok {
        status: http::StatusCode,
        headers: http::HeaderMap,
        body: Body,
    },
    /// The handler threw before producing a response.
    Failed,
    /// The runtime stopped the task (CPU / memory / wall limit) before a response existed.
    Killed(KillReason),
}

/// Per-task state read by the ops.
pub struct TaskOps {
    pub host: HostHandle,
    pub task: TaskId,
    /// `Deno.env`: prefetched before dispatch when the code may read it (the executor awaits
    /// it, B2.1 caches make that cheap), else fetched on the guest's first read. `None` = not
    /// fetched yet, `Some(Err(()))` = the fetch failed (reads throw `env_unavailable`). See
    /// [`env_of`].
    pub env: Option<Result<Arc<Zeroizing<EnvBundle>>, ()>>,
    /// The guest read `Deno.env` in this task (a reused isolate may hold the values, D-A4).
    pub env_read: bool,
    /// Task deadline: bounds the (rare) blocking wait of a cold env fetch.
    pub deadline: Instant,
    pub head_tx: Option<oneshot::Sender<Head>>,
    pub body_tx: Option<BodySender>,
    pub streamed: u64,
    pub max_stream: u64,
}

fn host_of(state: &Rc<RefCell<OpState>>) -> Result<HostHandle, JsErrorBox> {
    state
        .borrow()
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| JsErrorBox::generic("no_task_context"))
}

fn host_err(e: HostError) -> JsErrorBox {
    JsErrorBox::new("Error", e.code)
}

fn code_err(code: &'static str) -> JsErrorBox {
    JsErrorBox::new("Error", code)
}

/// A KV list cursor is opaque to guests and carries the store's exact key bytes as hex: a key that
/// is not UTF-8 must round-trip, or the next page would start from a different key.
fn cursor_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn cursor_decode(s: &str) -> Result<Vec<u8>, JsErrorBox> {
    let digits = s.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return Err(code_err("validation_failed"));
    }
    digits
        .chunks(2)
        .map(|p| match ((p[0] as char).to_digit(16), (p[1] as char).to_digit(16)) {
            (Some(hi), Some(lo)) => Ok((hi * 16 + lo) as u8),
            _ => Err(code_err("validation_failed")),
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// log / clock / env
// ---------------------------------------------------------------------------------------------

pub const LEVEL_DEBUG: u32 = 0;
pub const LEVEL_INFO: u32 = 1;
pub const LEVEL_WARN: u32 = 2;
pub const LEVEL_ERROR: u32 = 3;

fn level_of(l: u32) -> LogLevel {
    match l {
        LEVEL_DEBUG => LogLevel::Debug,
        LEVEL_INFO => LogLevel::Info,
        LEVEL_WARN => LogLevel::Warn,
        _ => LogLevel::Error,
    }
}

/// `console.*` -> task log (the host handle bounds it by `limits.max_log_lines` and counts drops).
#[op2(fast)]
pub fn op_rt_log(state: &mut OpState, #[smi] level: u32, #[string] msg: &str) {
    let Some(t) = state.try_borrow::<TaskOps>() else { return };
    t.host.log(level_of(level), msg.trim_end_matches('\n').to_string());
}

/// `std:log` with fields: `fields` is the JSON object text, empty for none. A refused object throws
/// `validation_failed` and writes no record (the caps are `rt_obs::MAX_LOG_FIELDS` / `_BYTES`).
#[op2(fast)]
pub fn op_rt_log_fields(
    state: &mut OpState,
    #[smi] level: u32,
    #[string] msg: &str,
    #[string] fields: &str,
) -> Result<(), JsErrorBox> {
    let Some(t) = state.try_borrow::<TaskOps>() else {
        return Ok(());
    };
    let (level, msg) = (level_of(level), msg.trim_end_matches('\n').to_string());
    if fields.is_empty() {
        t.host.log(level, msg);
        return Ok(());
    }
    t.host.log_fields(level, msg, fields).map_err(host_err)
}

#[op2(fast)]
pub fn op_rt_now(state: &mut OpState) -> f64 {
    state.try_borrow::<ClockState>().map_or(0.0, ClockState::now_ms)
}

#[op2(fast)]
pub fn op_rt_perf_now(state: &mut OpState) -> f64 {
    state.try_borrow::<ClockState>().map_or(0.0, ClockState::perf_ms)
}

/// Upper bound on how long a cold env fetch may block the Machine thread (see [`env_of`]).
pub const ENV_FETCH_MAX_WAIT: Duration = Duration::from_secs(2);

/// The task's env bundle for a `Deno.env` read.
///
/// Normally prefetched: the executor awaits `HostHandle::env` before dispatch whenever the code
/// mentions `Deno` (`DenoPrepared::may_read_env`). Only code that reaches `Deno.env` some other
/// way gets here without it: `Deno.env.get` is synchronous, so this op then parks the Machine
/// thread until the fetch resolves, at most [`ENV_FETCH_MAX_WAIT`] and never past the deadline (a
/// fetch that needs this thread's own runtime cannot finish meanwhile). A failed or timed-out
/// fetch throws `env_unavailable`: the guest never mistakes it for an empty env.
fn env_of(state: &mut OpState) -> Result<Option<Arc<Zeroizing<EnvBundle>>>, JsErrorBox> {
    let Some(t) = state.try_borrow_mut::<TaskOps>() else {
        return Ok(None);
    };
    if t.env.is_none() {
        let until = t.deadline.min(Instant::now() + ENV_FETCH_MAX_WAIT);
        let fetched = block_until(t.host.env(), until).and_then(Result::ok);
        if fetched.is_none() {
            tracing::warn!(task = %t.task, "env fetch failed or timed out: Deno.env throws env_unavailable");
        }
        t.env = Some(fetched.ok_or(()));
    }
    t.env_read = true;
    match t.env.clone() {
        Some(Ok(env)) => Ok(Some(env)),
        _ => Err(code_err("env_unavailable")),
    }
}

struct UnparkThread(std::thread::Thread);

impl std::task::Wake for UnparkThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive `fut` on this thread until it resolves or `until` passes (`None`). Only for futures whose
/// progress does not depend on this thread's runtime (timers, I/O, local tasks): their wakers are
/// called from other threads.
fn block_until<F: std::future::Future>(fut: F, until: Instant) -> Option<F::Output> {
    let waker = std::task::Waker::from(Arc::new(UnparkThread(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let std::task::Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return Some(v);
        }
        let now = Instant::now();
        if now >= until {
            return None;
        }
        std::thread::park_timeout(until - now);
    }
}

#[op2]
#[serde]
pub fn op_rt_env_get(state: &mut OpState, #[string] name: String) -> Result<serde_json::Value, JsErrorBox> {
    let found = env_of(state)?.and_then(|env| {
        env.env
            .iter()
            .chain(env.secrets.iter())
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.clone())
    });
    Ok(found.map_or(serde_json::Value::Null, serde_json::Value::String))
}

/// `std:secrets` `get`: one secret of the service, or null. Only the secrets part of the bundle is
/// searched, so a plain env var is never a secret; reads go through the same fetch as `Deno.env`.
#[op2]
#[serde]
pub fn op_rt_secret_get(state: &mut OpState, #[string] name: String) -> Result<serde_json::Value, JsErrorBox> {
    let found = env_of(state)?.and_then(|env| {
        env.secrets
            .iter()
            .rev()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.clone())
    });
    Ok(found.map_or(serde_json::Value::Null, serde_json::Value::String))
}

#[op2]
#[serde]
pub fn op_rt_env_all(state: &mut OpState) -> Result<Vec<(String, String)>, JsErrorBox> {
    let Some(env) = env_of(state)? else {
        return Ok(Vec::new());
    };
    Ok(env.env.iter().chain(env.secrets.iter()).cloned().collect())
}

/// Count a `WebAssembly.Memory` the guest can reach against the task's memory limit (B5.1; the
/// bootstrap's WebAssembly wrappers call it once per memory). Anything else is ignored.
#[op2(fast)]
pub fn op_rt_wasm_track<'a>(
    scope: &mut v8::PinScope<'a, '_>,
    state: Rc<RefCell<OpState>>,
    memory: v8::Local<'a, v8::Value>,
) {
    let Ok(memory) = v8::Local::<v8::WasmMemoryObject>::try_from(memory) else {
        return;
    };
    let account = state.borrow().try_borrow::<Arc<MemAccount>>().cloned();
    if let Some(account) = account {
        account.track_wasm(v8::Weak::new(scope, memory));
    }
}

/// May a resizable `ArrayBuffer` grow by `bytes` (D-A2)? It commits memory without the
/// allocator, so the bootstrap's `ArrayBuffer` wrappers ask before every growth (construction,
/// `resize`, `transfer`); `false` = the guest gets a `RangeError`.
#[op2(fast)]
pub fn op_rt_mem_admit<'a>(scope: &mut v8::PinScope<'a, '_>, state: Rc<RefCell<OpState>>, bytes: f64) -> bool {
    let account = state.borrow().try_borrow::<Arc<MemAccount>>().cloned();
    let bytes = if bytes.is_finite() && bytes > 0.0 {
        bytes as u64
    } else {
        0
    };
    account.is_none_or(|a| a.admit_growth(scope, bytes))
}

// ---------------------------------------------------------------------------------------------
// response channel (head first, then bounded body chunks)
// ---------------------------------------------------------------------------------------------

/// Sample the task's memory now (B5.1): `true` = over `limits.memory_mb` (the account is `oom`,
/// the driver terminates the task). Checked before a response head leaves, so a task that grew
/// past its limit right before responding is not reported as a success.
fn memory_over(scope: &mut v8::PinScope<'_, '_>, state: &Rc<RefCell<OpState>>) -> bool {
    let account = state.borrow().try_borrow::<Arc<MemAccount>>().cloned();
    account.is_some_and(|a| a.oom() || !a.check(scope))
}

/// Most headers a guest response may carry; more is a guest error (`502`).
pub const MAX_RESPONSE_HEADERS: usize = 256;
/// Most bytes of header names and values a guest response may carry.
pub const MAX_RESPONSE_HEADER_BYTES: usize = 256 * 1024;

#[op2]
pub fn op_rt_respond<'a>(
    scope: &mut v8::PinScope<'a, '_>,
    state: Rc<RefCell<OpState>>,
    #[smi] status: u32,
    #[serde] headers: Vec<(String, String)>,
) {
    let oom = memory_over(scope, &state);
    let mut state = state.borrow_mut();
    let Some(t) = state.try_borrow_mut::<TaskOps>() else {
        return;
    };
    let Some(tx) = t.head_tx.take() else { return };
    if oom {
        let _ = tx.send(Head::Killed(KillReason::Oom));
        return;
    }
    let status = u16::try_from(status)
        .ok()
        .and_then(|s| http::StatusCode::from_u16(s).ok());
    let Some(status) = status else {
        let _ = tx.send(Head::Failed);
        return;
    };
    // `HeaderMap::append` panics past its capacity (~24k names), and a panic in an op aborts the
    // node: cap the count and the size first (security review pass 2).
    let bytes: usize = headers.iter().map(|(k, v)| k.len() + v.len()).sum();
    if headers.len() > MAX_RESPONSE_HEADERS || bytes > MAX_RESPONSE_HEADER_BYTES {
        t.host.log(
            LogLevel::Error,
            format!("response refused: at most {MAX_RESPONSE_HEADERS} headers and {MAX_RESPONSE_HEADER_BYTES} bytes of them"),
        );
        let _ = tx.send(Head::Failed);
        return;
    }
    // At most MAX_RESPONSE_HEADERS entries (checked above), far below the map's capacity: `append`
    // cannot overflow. Names and values the HTTP types refuse are dropped.
    let mut map = http::HeaderMap::with_capacity(headers.len());
    let valid = headers.iter().filter_map(|(k, v)| {
        Some((
            http::HeaderName::from_bytes(k.as_bytes()).ok()?,
            http::HeaderValue::from_str(v).ok()?,
        ))
    });
    for (name, val) in valid {
        map.append(name, val);
    }
    let (sender, body) = rt_types::body_channel(4);
    t.body_tx = Some(sender);
    let _ = tx.send(Head::Ok {
        status,
        headers: map,
        body,
    });
}

#[op2(fast)]
pub fn op_rt_fail<'a>(scope: &mut v8::PinScope<'a, '_>, state: Rc<RefCell<OpState>>, #[string] msg: String) {
    // Over the memory limit, or an unhandled `RangeError` of an `ArrayBuffer` refused over it
    // (`502 oom`), rather than a failure of the guest's own.
    let oom = memory_over(scope, &state)
        || state
            .borrow()
            .try_borrow::<Arc<MemAccount>>()
            .is_some_and(|m| m.buffers_refused());
    let mut state = state.borrow_mut();
    let Some(t) = state.try_borrow_mut::<TaskOps>() else {
        return;
    };
    t.host.log(LogLevel::Error, msg.clone());
    if let Some(tx) = t.head_tx.take() {
        let _ = tx.send(if oom {
            Head::Killed(KillReason::Oom)
        } else {
            Head::Failed
        });
    } else if let Some(sender) = t.body_tx.take() {
        tokio::task::spawn_local(async move { sender.abort(BodyError::Aborted("guest_error".into())).await });
    } // LCOV_EXCL_LINE: JS answers (`respond`) before it can fail, and a failure ends the task
}

#[op2]
pub async fn op_rt_resp_chunk(state: Rc<RefCell<OpState>>, #[buffer] chunk: JsBuffer) -> Result<(), JsErrorBox> {
    let (sender, over) = {
        let mut s = state.borrow_mut();
        let t = s
            .try_borrow_mut::<TaskOps>()
            .ok_or_else(|| code_err("no_task_context"))?;
        t.streamed += chunk.len() as u64;
        (t.body_tx.clone(), t.streamed > t.max_stream)
    };
    if over {
        return Err(code_err("response_too_large"));
    }
    let sender = sender.ok_or_else(|| code_err("no_response"))?;
    sender
        .send(Bytes::copy_from_slice(&chunk))
        .await
        .map_err(|_| code_err("body_closed"))
}

#[op2(fast)]
pub fn op_rt_resp_end(state: &mut OpState) {
    if let Some(t) = state.try_borrow_mut::<TaskOps>() {
        t.body_tx = None;
    }
}

#[op2]
pub async fn op_rt_resp_abort(state: Rc<RefCell<OpState>>, #[string] msg: String) {
    let sender = {
        let mut s = state.borrow_mut();
        let Some(t) = s.try_borrow_mut::<TaskOps>() else { return };
        t.host.log(LogLevel::Error, msg);
        t.body_tx.take()
    };
    if let Some(sender) = sender {
        sender.abort(BodyError::Aborted("guest_error".into())).await;
    }
}

// ---------------------------------------------------------------------------------------------
// fetch / services / platform (same wire shape)
// ---------------------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpReq {
    #[serde(default)]
    pub slug: String,
    /// `op_rt_call_object`: the object's name.
    #[serde(default)]
    pub object: String,
    #[serde(default)]
    pub binding: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub has_body: bool,
    /// `services.call` (RPC): the only caller that may use the reserved `/__rt/rpc/` path.
    #[serde(default)]
    pub rpc: bool,
}

/// The reserved `/__rt/` call paths and the RPC argument cap, checked here as well as in JS: the JS
/// checks use built-ins a guest can replace (security review pass 2).
fn check_call_path(req: &HttpReq, body: &[u8]) -> Result<(), JsErrorBox> {
    let path = req.url.split(['?', '#']).next().unwrap_or("");
    let ok = if req.rpc {
        path.starts_with(rt_host::rpc::RPC_PREFIX) && body.len() <= rt_host::rpc::RPC_JSON_MAX_BYTES
    } else {
        !path.starts_with(rt_host::rpc::RPC_PREFIX_ROOT)
    };
    if ok {
        Ok(())
    } else {
        Err(code_err("validation_failed"))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpRes {
    status: u16,
    headers: Vec<(String, String)>,
    body_rid: Option<u32>,
}

/// A body read by the guest chunk by chunk (`op_rt_body_read`): a response of `fetch`/calls, a
/// blob, or the task's own request body (B4.4).
struct BodyRes {
    body: Mutex<Body>,
    /// Bytes the guest may read in total (the request's `limits.max_body_bytes`).
    limit: Option<u64>,
    read: std::cell::Cell<u64>,
    /// Error code past the limit.
    too_large: &'static str,
}

impl BodyRes {
    fn new(body: Body) -> Self {
        Self {
            body: Mutex::new(body),
            limit: None,
            read: std::cell::Cell::new(0),
            too_large: "egress_too_large",
        }
    }
}

impl Resource for BodyRes {
    fn name(&self) -> std::borrow::Cow<'_, str> {
        "rtBody".into()
    }
}

/// Hand the task's request body to the guest as a stream (B4.4): the bootstrap wraps the returned
/// resource in a `ReadableStream`, so the handler reads chunks as the client sends them. Reading
/// past `limit` bytes fails with `request_too_large`.
pub fn add_request_body(state: &mut OpState, body: Body, limit: u64) -> ResourceId {
    let res = BodyRes {
        limit: Some(limit),
        too_large: "request_too_large",
        ..BodyRes::new(body)
    };
    state.resource_table.add(res)
}

fn build_request(req: &HttpReq, body: &JsBuffer) -> Result<Request, JsErrorBox> {
    let mut b = http::Request::builder()
        .method(req.method.as_str())
        .uri(req.url.as_str());
    for (k, v) in &req.headers {
        b = b.header(k.as_str(), v.as_str());
    }
    let bytes = if req.has_body {
        Bytes::copy_from_slice(body)
    } else {
        Bytes::new()
    };
    b.body(Body::from_bytes(bytes))
        .map_err(|_| JsErrorBox::type_error("invalid_request"))
}

fn finish_response(state: &Rc<RefCell<OpState>>, resp: Response) -> HttpRes {
    let status = resp.status().as_u16();
    let headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                String::from_utf8_lossy(v.as_bytes()).to_string(),
            )
        })
        .collect();
    let body = resp.into_body();
    let rid = if body.exact_len() == Some(0) {
        None
    } else {
        Some(state.borrow_mut().resource_table.add(BodyRes::new(body)))
    };
    HttpRes {
        status,
        headers,
        body_rid: rid,
    }
}

#[op2]
#[serde]
pub async fn op_rt_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] req: HttpReq,
    #[buffer] body: JsBuffer,
) -> Result<HttpRes, JsErrorBox> {
    let host = host_of(&state)?;
    let request = build_request(&req, &body)?;
    match host.fetch(request).await {
        Ok(resp) => Ok(finish_response(&state, resp)),
        Err(e) => {
            // The guest only ever sees the stable code; the operator gets the target in the log.
            host.log_system(LogLevel::Warn, format!("egress {}: {}", e.code(), req.url));
            Err(code_err(e.code()))
        }
    }
}

#[op2]
#[serde]
pub async fn op_rt_call(
    state: Rc<RefCell<OpState>>,
    #[serde] req: HttpReq,
    #[buffer] body: JsBuffer,
) -> Result<HttpRes, JsErrorBox> {
    let host = host_of(&state)?;
    check_call_path(&req, &body)?;
    let request = build_request(&req, &body)?;
    match host.call(&req.slug, request).await {
        Ok(resp) => Ok(finish_response(&state, resp)),
        // Past depth 15 the call resolves to `508` + `x-rt-reason: call_depth` (TRG-15).
        Err(e @ CallError::Depth) => Ok(HttpRes {
            status: 508,
            headers: vec![("x-rt-reason".to_string(), e.code().to_string())],
            body_rid: None,
        }),
        Err(e) => Err(code_err(e.code())),
    }
}

/// `durable.get(name, { service }).fetch(req)`: a request to an object of a durable service (`slug`
/// empty: this service), in the object's turn.
#[op2]
#[serde]
pub async fn op_rt_call_object(
    state: Rc<RefCell<OpState>>,
    #[serde] req: HttpReq,
    #[buffer] body: JsBuffer,
) -> Result<HttpRes, JsErrorBox> {
    let host = host_of(&state)?;
    if req.rpc {
        return Err(code_err("validation_failed")); // LCOV_EXCL_LINE: the JS side never marks an object call as RPC
    }
    check_call_path(&req, &body)?;
    let request = build_request(&req, &body)?;
    let slug = (!req.slug.is_empty()).then_some(req.slug.as_str());
    match host.call_object(slug, &req.object, request).await {
        Ok(resp) => Ok(finish_response(&state, resp)),
        Err(e @ CallError::Depth) => Ok(HttpRes {
            status: 508,
            headers: vec![("x-rt-reason".to_string(), e.code().to_string())],
            body_rid: None,
        }),
        Err(e) => Err(code_err(e.code())),
    }
}

/// The answer of `op_rt_sdk_invoke`: the JSON text of the result, or the error.
#[derive(Serialize)]
pub struct SdkReply {
    value: Option<String>,
    code: Option<String>,
    message: Option<String>,
}

/// `std:sdk`: `method` of the operator SDK `name` with the JSON argument `args` (already checked by
/// the bootstrap), bound to the release the caller's grants select.
#[op2]
#[serde]
pub async fn op_rt_sdk_invoke(
    state: Rc<RefCell<OpState>>,
    #[string] name: String,
    #[string] method: String,
    #[string] args: String,
) -> Result<SdkReply, JsErrorBox> {
    let host = host_of(&state)?;
    Ok(match host.sdk_invoke(&name, &method, &args).await {
        Ok(value) => SdkReply {
            value: Some(value),
            code: None,
            message: None,
        },
        Err(e) => SdkReply {
            value: None,
            code: Some(e.code),
            message: Some(e.message),
        },
    })
}

/// Denied calls resolve to a 403 `x-rt-reason: binding_denied` response, they do not throw (C-8).
#[op2]
#[serde]
pub async fn op_rt_platform_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] req: HttpReq,
    #[buffer] body: JsBuffer,
) -> Result<HttpRes, JsErrorBox> {
    let host = host_of(&state)?;
    let request = build_request(&req, &body)?;
    match host.platform_fetch(&req.binding, request).await {
        Ok(resp) => Ok(finish_response(&state, resp)),
        // Denials and rate limits arrive as responses already (C-8); the rest become one here, as in WASM.
        Err(e) => Ok(finish_response(&state, e.to_response())),
    }
}

#[op2]
#[serde]
pub async fn op_rt_body_read(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
) -> Result<Option<ToJsBuffer>, JsErrorBox> {
    let res = state
        .borrow()
        .resource_table
        .get::<BodyRes>(rid)
        .map_err(|_| code_err("bad_resource"))?;
    let mut body = res.body.lock().await;
    match body.next().await {
        None => Ok(None),
        Some(Ok(chunk)) => {
            let read = res.read.get().saturating_add(chunk.len() as u64);
            res.read.set(read);
            if res.limit.is_some_and(|limit| read > limit) {
                return Err(code_err(res.too_large));
            }
            Ok(Some(ToJsBuffer::from(chunk.to_vec())))
        }
        Some(Err(BodyError::TooLarge { .. })) => Err(code_err(res.too_large)),
        Some(Err(_)) => Err(code_err("body_error")),
    }
}

#[op2(fast)]
pub fn op_rt_body_close(state: &mut OpState, #[smi] rid: ResourceId) {
    if let Ok(r) = state.resource_table.take_any(rid) {
        r.close();
    }
}

// ---------------------------------------------------------------------------------------------
// std:kv
// ---------------------------------------------------------------------------------------------

#[derive(Serialize)]
pub struct KvOut {
    tag: &'static str,
    data: ToJsBuffer,
}

#[derive(Serialize)]
pub struct KvEntryOut {
    key: String,
    tag: &'static str,
    data: ToJsBuffer,
}

#[derive(Serialize)]
pub struct KvListOut {
    entries: Vec<KvEntryOut>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
pub struct ListIn {
    prefix: String,
    cursor: Option<String>,
    limit: usize,
    #[serde(default)]
    reverse: bool,
}

fn kv_out(v: KvValue) -> KvOut {
    KvOut {
        tag: tag_name(v.tag),
        data: ToJsBuffer::from(v.data.to_vec()),
    }
}

fn tag_name(t: ValueTag) -> &'static str {
    match t {
        ValueTag::Json => "json",
        ValueTag::Bytes => "bytes",
    }
}

#[op2]
#[serde]
pub async fn op_rt_kv_get(state: Rc<RefCell<OpState>>, #[string] key: String) -> Result<Option<KvOut>, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    Ok(kv.get(key.as_bytes()).await.map_err(host_err)?.map(kv_out))
}

#[op2]
#[serde]
pub async fn op_rt_kv_get_many(
    state: Rc<RefCell<OpState>>,
    #[serde] keys: Vec<String>,
) -> Result<Vec<Option<KvOut>>, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let keys: Vec<Bytes> = keys.into_iter().map(Bytes::from).collect();
    Ok(kv
        .get_many(&keys)
        .await
        .map_err(host_err)?
        .into_iter()
        .map(|v| v.map(kv_out))
        .collect())
}

#[op2]
pub async fn op_rt_kv_set(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
    #[buffer] data: JsBuffer,
    is_bytes: bool,
    ttl_ms: f64,
) -> Result<(), JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let value = KvValue {
        tag: if is_bytes { ValueTag::Bytes } else { ValueTag::Json },
        data: Bytes::copy_from_slice(&data),
    };
    let ttl = (ttl_ms >= 0.0).then(|| Duration::from_millis(ttl_ms as u64));
    kv.set(key.as_bytes(), value, ttl).await.map_err(host_err)
}

/// `kv.incr(key, delta, {ttlSeconds})`: `delta` and the result travel as decimal strings, so
/// the whole `i64` range is exact (the JS side returns a `number`, or a `bigint` for a `bigint`
/// delta).
#[op2]
#[string]
pub async fn op_rt_kv_incr(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
    #[string] delta: String,
    ttl_ms: f64,
) -> Result<String, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let delta: i64 = delta.parse().map_err(|_| code_err("validation_failed"))?;
    let ttl = (ttl_ms >= 0.0).then(|| Duration::from_millis(ttl_ms as u64));
    kv.incr(key.as_bytes(), delta, ttl)
        .await
        .map(|n| n.to_string())
        .map_err(host_err)
}

/// `kv.cas(key, expected, next, {ttlSeconds})`: a value is `(is_bytes, data)`; `has_*` is false for
/// `null` (absent / delete). Returns whether the swap happened.
#[op2]
pub async fn op_rt_kv_cas(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
    has_expected: bool,
    expected_bytes: bool,
    #[buffer] expected: JsBuffer,
    has_next: bool,
    next_bytes: bool,
    #[buffer] next: JsBuffer,
    ttl_ms: f64,
) -> Result<bool, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let value = |is_bytes: bool, data: &[u8]| KvValue {
        tag: if is_bytes { ValueTag::Bytes } else { ValueTag::Json },
        data: Bytes::copy_from_slice(data),
    };
    let expected = has_expected.then(|| value(expected_bytes, &expected));
    let next = has_next.then(|| value(next_bytes, &next));
    let ttl = (ttl_ms >= 0.0).then(|| Duration::from_millis(ttl_ms as u64));
    kv.cas(key.as_bytes(), expected, next, ttl).await.map_err(host_err)
}

/// `kv.expire(key, {ttlSeconds} | null)`: `ttl_ms` < 0 clears the expiry. `false` when absent.
#[op2]
pub async fn op_rt_kv_expire(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
    ttl_ms: f64,
) -> Result<bool, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let ttl = (ttl_ms >= 0.0).then(|| Duration::from_millis(ttl_ms as u64));
    kv.expire(key.as_bytes(), ttl).await.map_err(host_err)
}

/// `kv.ttl(key)`: seconds left, rounded up; `-1` when the key is absent or has no expiry.
#[op2]
pub async fn op_rt_kv_ttl(state: Rc<RefCell<OpState>>, #[string] key: String) -> Result<f64, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let left = kv.ttl(key.as_bytes()).await.map_err(host_err)?;
    Ok(left.map_or(-1.0, |d| (d.as_millis().saturating_add(999) / 1000) as f64))
}

#[op2]
pub async fn op_rt_kv_delete(state: Rc<RefCell<OpState>>, #[string] key: String) -> Result<bool, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    kv.delete(key.as_bytes()).await.map_err(host_err)
}

#[op2]
#[serde]
pub async fn op_rt_kv_list(state: Rc<RefCell<OpState>>, #[serde] opts: ListIn) -> Result<KvListOut, JsErrorBox> {
    let kv = host_of(&state)?.kv();
    let page = kv
        .list(KvListOptions {
            prefix: Bytes::from(opts.prefix),
            cursor: opts.cursor.as_deref().map(cursor_decode).transpose()?.map(Bytes::from),
            limit: opts.limit,
            reverse: opts.reverse,
        })
        .await
        .map_err(host_err)?;
    Ok(KvListOut {
        entries: page
            .entries
            .into_iter()
            .map(|(k, v)| KvEntryOut {
                key: String::from_utf8_lossy(&k).to_string(),
                tag: tag_name(v.tag),
                data: ToJsBuffer::from(v.data.to_vec()),
            })
            .collect(),
        cursor: page.next_cursor.map(|c| cursor_encode(&c)),
    })
}

// ---------------------------------------------------------------------------------------------
// std:sql
// ---------------------------------------------------------------------------------------------

struct SqlTxRes {
    tx: Mutex<Option<Box<dyn SqlTx>>>,
}

impl Resource for SqlTxRes {}

/// `sql.execute(sql, args)`: arguments are read from the JS values and the result is built as JS
/// values (`v8sql.rs`), with no serde round trip.
#[op2]
pub async fn op_rt_sql_execute(
    state: Rc<RefCell<OpState>>,
    #[string] sql: String,
    #[scoped] args: ArgsV8,
) -> Result<SqlResultV8, JsErrorBox> {
    let backend = host_of(&state)?.sql();
    backend
        .execute(SqlStatement { sql, args: args.0 })
        .await
        .map(SqlResultV8)
        .map_err(host_err)
}

#[op2]
pub async fn op_rt_sql_batch(
    state: Rc<RefCell<OpState>>,
    #[scoped] stmts: StmtsV8,
    #[string] mode: String,
) -> Result<SqlResultsV8, JsErrorBox> {
    let backend = host_of(&state)?.sql();
    let mode = if mode == "read" {
        BatchMode::Read
    } else {
        BatchMode::Write
    };
    backend.batch(stmts.0, mode).await.map(SqlResultsV8).map_err(host_err)
}

#[op2]
#[smi]
pub async fn op_rt_sql_begin(state: Rc<RefCell<OpState>>) -> Result<ResourceId, JsErrorBox> {
    let sql = host_of(&state)?.sql();
    let tx = sql.begin().await.map_err(host_err)?;
    Ok(state.borrow_mut().resource_table.add(SqlTxRes {
        tx: Mutex::new(Some(tx)),
    }))
}

#[op2]
pub async fn op_rt_sql_tx_execute(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
    #[string] sql: String,
    #[scoped] args: ArgsV8,
) -> Result<SqlResultV8, JsErrorBox> {
    let res = state
        .borrow()
        .resource_table
        .get::<SqlTxRes>(rid)
        .map_err(|_| code_err("bad_resource"))?;
    let mut guard = res.tx.lock().await;
    let tx = guard.as_mut().ok_or_else(|| code_err("transaction_finished"))?;
    tx.execute(SqlStatement { sql, args: args.0 })
        .await
        .map(SqlResultV8)
        .map_err(|e| host_err(HostError::from(e)))
}

#[op2]
pub async fn op_rt_sql_tx_end(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
    commit: bool,
) -> Result<(), JsErrorBox> {
    let res = state
        .borrow_mut()
        .resource_table
        .take::<SqlTxRes>(rid)
        .map_err(|_| code_err("bad_resource"))?;
    let tx = res
        .tx
        .lock()
        .await
        .take()
        .ok_or_else(|| code_err("transaction_finished"))?;
    let r = if commit { tx.commit().await } else { tx.rollback().await };
    r.map_err(|e| host_err(HostError::from(e)))
}

// ---------------------------------------------------------------------------------------------
// std:blob
// ---------------------------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobMetaOut {
    size: u64,
    content_type: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobGetOut {
    rid: u32,
    size: u64,
    content_type: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobItemOut {
    key: String,
    size: u64,
    content_type: Option<String>,
}

#[derive(Serialize)]
pub struct BlobListOut {
    objects: Vec<BlobItemOut>,
    cursor: Option<String>,
}

struct BlobPutRes {
    tx: RefCell<Option<BodySender>>,
    task: RefCell<Option<tokio::task::JoinHandle<Result<BlobMeta, HostError>>>>,
}

impl Resource for BlobPutRes {}

#[op2]
#[serde]
pub async fn op_rt_blob_head(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
) -> Result<Option<BlobMetaOut>, JsErrorBox> {
    let blob = host_of(&state)?.blob();
    Ok(blob.head(&key).await.map_err(host_err)?.map(|m| BlobMetaOut {
        size: m.size,
        content_type: m.content_type,
    }))
}

#[op2]
#[serde]
pub async fn op_rt_blob_get(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
) -> Result<Option<BlobGetOut>, JsErrorBox> {
    let blob = host_of(&state)?.blob();
    let Some(obj) = blob.get(&key).await.map_err(host_err)? else {
        return Ok(None);
    };
    let rid = state.borrow_mut().resource_table.add(BodyRes::new(obj.body));
    Ok(Some(BlobGetOut {
        rid,
        size: obj.meta.size,
        content_type: obj.meta.content_type,
    }))
}

#[op2]
#[smi]
pub fn op_rt_blob_put_begin(
    state: &mut OpState,
    #[string] key: String,
    #[string] content_type: Option<String>,
) -> Result<ResourceId, JsErrorBox> {
    let host = state
        .try_borrow::<TaskOps>()
        .map(|t| t.host.clone())
        .ok_or_else(|| code_err("no_task_context"))?;
    let blob = host.blob();
    let (sender, body) = rt_types::body_channel(4);
    let task = tokio::task::spawn_local(async move { blob.put(&key, body, content_type).await });
    Ok(state.resource_table.add(BlobPutRes {
        tx: RefCell::new(Some(sender)),
        task: RefCell::new(Some(task)),
    }))
}

#[op2]
pub async fn op_rt_blob_put_write(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
    #[buffer] chunk: JsBuffer,
) -> Result<(), JsErrorBox> {
    let res = state
        .borrow()
        .resource_table
        .get::<BlobPutRes>(rid)
        .map_err(|_| code_err("bad_resource"))?;
    let sender = res.tx.borrow().clone().ok_or_else(|| code_err("blob_put_finished"))?;
    sender
        .send(Bytes::copy_from_slice(&chunk))
        .await
        .map_err(|_| code_err("storage_error"))
}

#[op2]
pub async fn op_rt_blob_put_end(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
    commit: bool,
) -> Result<(), JsErrorBox> {
    let res = state
        .borrow_mut()
        .resource_table
        .take::<BlobPutRes>(rid)
        .map_err(|_| code_err("bad_resource"))?;
    let sender = res.tx.borrow_mut().take();
    if let Some(s) = sender.filter(|_| !commit) {
        s.abort(BodyError::Aborted("cancelled".into())).await;
    }
    let task = res.task.borrow_mut().take();
    let Some(task) = task else { return Ok(()) };
    match task.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(host_err(e)),
        Err(_) => Err(code_err("storage_error")), // LCOV_EXCL_LINE: the upload task is local and is only cancelled with its isolate
    }
}

#[op2]
pub async fn op_rt_blob_delete(state: Rc<RefCell<OpState>>, #[string] key: String) -> Result<bool, JsErrorBox> {
    let blob = host_of(&state)?.blob();
    blob.delete(&key).await.map_err(host_err)
}

#[op2]
#[serde]
pub async fn op_rt_blob_list(state: Rc<RefCell<OpState>>, #[serde] opts: ListIn) -> Result<BlobListOut, JsErrorBox> {
    let blob = host_of(&state)?.blob();
    let page = blob
        .list(BlobListOptions {
            prefix: opts.prefix,
            cursor: opts.cursor,
            limit: opts.limit,
        })
        .await
        .map_err(host_err)?;
    Ok(BlobListOut {
        objects: page
            .items
            .into_iter()
            .map(|m| BlobItemOut {
                key: m.key,
                size: m.size,
                content_type: m.content_type,
            })
            .collect(),
        cursor: page.next_cursor,
    })
}

// ---------------------------------------------------------------------------------------------
// std:queue
// ---------------------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueueIn {
    slug: String,
    delay_ms: f64,
    dedup_id: Option<String>,
}

#[op2]
#[string]
pub async fn op_rt_enqueue(
    state: Rc<RefCell<OpState>>,
    #[serde] opts: EnqueueIn,
    #[buffer] body: JsBuffer,
) -> Result<String, JsErrorBox> {
    let host = host_of(&state)?;
    let o = EnqueueOpts {
        delay: Duration::from_millis(opts.delay_ms.max(0.0) as u64),
        dedup_id: opts.dedup_id,
    };
    match host.enqueue(&opts.slug, Bytes::copy_from_slice(&body), o).await {
        Ok(id) => Ok(id.to_string()),
        Err(e) => Err(code_err(e.code())),
    }
}

// ---------------------------------------------------------------------------------------------
// std:lock
// ---------------------------------------------------------------------------------------------

/// `lock.acquire(key, waitMs, leaseMs)`: waits as a future of the task, never on the thread.
/// Resolves to the hold's ID; `lock_timeout`, `lock_limit` and `validation_failed` are errors.
#[op2]
pub async fn op_rt_lock_acquire(
    state: Rc<RefCell<OpState>>,
    #[string] key: String,
    wait_ms: f64,
    lease_ms: f64,
) -> Result<f64, JsErrorBox> {
    let host = host_of(&state)?;
    let wait = Duration::from_millis(wait_ms.max(0.0) as u64);
    let lease = Duration::from_millis(lease_ms.max(0.0) as u64);
    host.lock_acquire(&key, wait, lease)
        .await
        .map(|id| id as f64)
        .map_err(host_err)
}

/// `lock.release(id)`: releases one of the task's holds (a no-op for any other ID).
#[op2(fast)]
pub fn op_rt_lock_release(state: &mut OpState, id: f64) {
    if let Some(task) = state.try_borrow::<TaskOps>() {
        task.host.lock_release(id as u64);
    }
}

// ---------------------------------------------------------------------------------------------
// std:schedule
// ---------------------------------------------------------------------------------------------

/// `{ id, runAt }` of a stored schedule (`runAt` in Unix ms).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleOut {
    id: String,
    run_at: f64,
}

/// `schedule.after(seconds, payload, { id })`: `has_id` false = a generated id. `payload` is the
/// JSON text of the payload.
#[op2]
#[serde]
pub async fn op_rt_schedule_after(
    state: Rc<RefCell<OpState>>,
    has_id: bool,
    #[string] id: String,
    delay_ms: f64,
    #[string] payload: String,
) -> Result<ScheduleOut, JsErrorBox> {
    let host = host_of(&state)?;
    let id = has_id.then_some(id);
    let delay = Duration::from_millis(delay_ms.max(0.0) as u64);
    let (id, run_at) = host
        .schedule_after(id, delay, Bytes::from(payload))
        .await
        .map_err(host_err)?;
    Ok(ScheduleOut {
        id,
        run_at: run_at as f64,
    })
}

/// `schedule.at(time, payload, { id })`: the due time in Unix ms.
#[op2]
#[serde]
pub async fn op_rt_schedule_at(
    state: Rc<RefCell<OpState>>,
    has_id: bool,
    #[string] id: String,
    at_ms: f64,
    #[string] payload: String,
) -> Result<ScheduleOut, JsErrorBox> {
    let host = host_of(&state)?;
    let id = has_id.then_some(id);
    let (id, run_at) = host
        .schedule_at(id, at_ms as i64, Bytes::from(payload))
        .await
        .map_err(host_err)?;
    Ok(ScheduleOut {
        id,
        run_at: run_at as f64,
    })
}

/// `schedule.cancel(id)`.
#[op2]
pub async fn op_rt_schedule_cancel(state: Rc<RefCell<OpState>>, #[string] id: String) -> Result<bool, JsErrorBox> {
    let host = host_of(&state)?;
    host.schedule_cancel(&id).await.map_err(host_err)
}

#[derive(Deserialize)]
pub struct ScheduleListIn {
    #[serde(default)]
    after: Option<String>,
    limit: usize,
}

#[derive(Serialize)]
pub struct ScheduleListOut {
    entries: Vec<ScheduleEntryOut>,
    cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleEntryOut {
    id: String,
    run_at: f64,
}

/// `schedule.list({ after, limit })`: a page in id order; the cursor is the last id of a full page.
#[op2]
#[serde]
pub async fn op_rt_schedule_list(
    state: Rc<RefCell<OpState>>,
    #[serde] opts: ScheduleListIn,
) -> Result<ScheduleListOut, JsErrorBox> {
    let host = host_of(&state)?;
    let limit = opts.limit.clamp(1, 100);
    let page = host
        .schedule_list(opts.after.as_deref(), limit)
        .await
        .map_err(host_err)?;
    let cursor = (page.len() == limit)
        .then(|| page.last().map(|e| e.id.clone()))
        .flatten();
    Ok(ScheduleListOut {
        entries: page
            .into_iter()
            .map(|e| ScheduleEntryOut {
                id: e.id,
                run_at: e.run_at_ms as f64,
            })
            .collect(),
        cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(url: &str, rpc: bool) -> HttpReq {
        serde_json::from_value(serde_json::json!({
            "method": "POST", "url": url, "headers": [], "hasBody": false, "rpc": rpc,
        }))
        .expect("an op request")
    }

    #[test]
    fn the_reserved_call_paths_are_refused_to_a_plain_call_and_the_rpc_path_only_to_an_rpc() {
        for url in ["/__rt/rpc/m", "/__rt/scheduled?x=1", "/__rt/"] {
            assert!(check_call_path(&req(url, false), b"").is_err(), "{url}");
        }
        assert!(check_call_path(&req("/orders?x=/__rt/y", false), b"").is_ok());
        assert!(check_call_path(&req("/__rt/rpc/m", true), b"{}").is_ok());
        assert!(
            check_call_path(&req("/orders", true), b"{}").is_err(),
            "an RPC goes to the RPC path"
        );
        let big = vec![b' '; rt_host::rpc::RPC_JSON_MAX_BYTES + 1];
        assert!(
            check_call_path(&req("/__rt/rpc/m", true), &big).is_err(),
            "and its argument is capped"
        );
    }
}
