//! The extension set of a guest isolate.
//!
//! One consistent Deno release: **v2.9.7** (`deno_core 0.412.0`, `deno_webidl 0.259.0`,
//! `deno_web 0.290.0`, `deno_fetch 0.283.0`, `deno_crypto 0.273.0`). In this release
//! `deno_console` and `deno_url` are deprecated stubs: console, URL, `URLPattern`, timers,
//! streams, `TextEncoder`, `AbortController`, `Blob`, `structuredClone` all live in `deno_web`,
//! and their JS is loaded lazily with `core.loadExtScript` (see `js/bootstrap.js`) when the base
//! snapshot is built (`build.rs`); the JS stays in the cargo registry and is never read at runtime.
//!
//! Excluded on purpose: `deno_fs`/`deno_io` (as ops; they are only pulled in as build-time
//! dependencies of `deno_fetch`), `deno_net`, `deno_process`, `deno_ffi`, `deno_node`,
//! `deno_websocket`, `deno_cache`, workers, `Deno.*` (except a read-only `Deno.env` shim).
//! `deno_fetch`'s HTTP client is **not used**: the bootstrap replaces the global `fetch` with
//! ops that go through `rt-host`'s `EgressGuard`.

use deno_core::Extension;

use crate::ops::{
    op_rt_blob_delete, op_rt_blob_get, op_rt_blob_head, op_rt_blob_list, op_rt_blob_put_begin, op_rt_blob_put_end,
    op_rt_blob_put_write, op_rt_body_close, op_rt_body_read, op_rt_call, op_rt_call_object, op_rt_enqueue,
    op_rt_env_all, op_rt_env_get, op_rt_fail, op_rt_fetch, op_rt_kv_cas, op_rt_kv_delete, op_rt_kv_expire,
    op_rt_kv_get, op_rt_kv_get_many, op_rt_kv_incr, op_rt_kv_list, op_rt_kv_set, op_rt_kv_ttl, op_rt_lock_acquire,
    op_rt_lock_release, op_rt_log, op_rt_log_fields, op_rt_mem_admit, op_rt_now, op_rt_perf_now, op_rt_platform_fetch,
    op_rt_resp_abort, op_rt_resp_chunk, op_rt_resp_end, op_rt_respond, op_rt_schedule_after, op_rt_schedule_at,
    op_rt_schedule_cancel, op_rt_schedule_list, op_rt_sdk_invoke, op_rt_secret_get, op_rt_sql_batch, op_rt_sql_begin,
    op_rt_sql_execute, op_rt_sql_tx_end, op_rt_sql_tx_execute, op_rt_wasm_track,
};
use crate::ops_durable::{
    op_rt_alarm_get, op_rt_alarm_set, op_rt_storage_delete, op_rt_storage_delete_all, op_rt_storage_get,
    op_rt_storage_list, op_rt_storage_put,
};
use crate::ops_sqlite::{
    op_rt_sqlite_exec, op_rt_sqlite_in_tx, op_rt_sqlite_open, op_rt_sqlite_prepare, op_rt_sqlite_run, op_rt_sqlite_tx,
};
use crate::ops_ws::{
    op_rt_task_commit, op_rt_ws_accept, op_rt_ws_attachment, op_rt_ws_auto_response, op_rt_ws_broadcast,
    op_rt_ws_client_close, op_rt_ws_client_recv, op_rt_ws_client_send, op_rt_ws_close, op_rt_ws_close_all,
    op_rt_ws_connect, op_rt_ws_count, op_rt_ws_event_data, op_rt_ws_get, op_rt_ws_list, op_rt_ws_objects,
    op_rt_ws_send, op_rt_ws_set_attachment, op_rt_ws_set_tags,
};

/// Log levels used by `op_rt_log` (match `rt_obs::LogLevel`).
pub use crate::ops::{LEVEL_DEBUG, LEVEL_ERROR, LEVEL_INFO, LEVEL_WARN};

deno_core::extension!(
    rt_ext,
    deps = [deno_webidl, deno_web, deno_fetch, deno_crypto],
    ops = [
        op_rt_log,
        op_rt_log_fields,
        op_rt_now,
        op_rt_perf_now,
        op_rt_env_get,
        op_rt_env_all,
        op_rt_secret_get,
        op_rt_respond,
        op_rt_fail,
        op_rt_resp_chunk,
        op_rt_resp_end,
        op_rt_resp_abort,
        op_rt_fetch,
        op_rt_call,
        op_rt_sdk_invoke,
        op_rt_platform_fetch,
        op_rt_body_read,
        op_rt_body_close,
        op_rt_kv_get,
        op_rt_kv_get_many,
        op_rt_kv_set,
        op_rt_kv_delete,
        op_rt_kv_list,
        op_rt_kv_incr,
        op_rt_kv_cas,
        op_rt_kv_expire,
        op_rt_kv_ttl,
        op_rt_sql_execute,
        op_rt_sql_batch,
        op_rt_sql_begin,
        op_rt_sql_tx_execute,
        op_rt_sql_tx_end,
        op_rt_blob_head,
        op_rt_blob_get,
        op_rt_blob_put_begin,
        op_rt_blob_put_write,
        op_rt_blob_put_end,
        op_rt_blob_delete,
        op_rt_blob_list,
        op_rt_enqueue,
        op_rt_lock_acquire,
        op_rt_lock_release,
        op_rt_schedule_after,
        op_rt_schedule_at,
        op_rt_schedule_cancel,
        op_rt_schedule_list,
        op_rt_wasm_track,
        op_rt_mem_admit,
        op_rt_sqlite_open,
        op_rt_sqlite_prepare,
        op_rt_sqlite_run,
        op_rt_sqlite_exec,
        op_rt_sqlite_tx,
        op_rt_sqlite_in_tx,
        op_rt_ws_accept,
        op_rt_ws_send,
        op_rt_ws_broadcast,
        op_rt_ws_close,
        op_rt_ws_close_all,
        op_rt_ws_list,
        op_rt_ws_get,
        op_rt_ws_count,
        op_rt_ws_objects,
        op_rt_ws_set_tags,
        op_rt_ws_attachment,
        op_rt_ws_set_attachment,
        op_rt_ws_auto_response,
        op_rt_ws_event_data,
        op_rt_task_commit,
        op_rt_call_object,
        op_rt_storage_get,
        op_rt_storage_put,
        op_rt_storage_delete,
        op_rt_storage_list,
        op_rt_storage_delete_all,
        op_rt_alarm_get,
        op_rt_alarm_set,
        op_rt_ws_connect,
        op_rt_ws_client_send,
        op_rt_ws_client_recv,
        op_rt_ws_client_close,
    ],
);

/// The ordered extension list for a guest `JsRuntime`.
///
/// Order matters: `deno_webidl` -> `deno_web` -> `deno_fetch`/`deno_crypto` -> `rt_ext`. It must
/// stay in step with `guest_extensions()` in `build.rs`, which builds the base snapshot: that is
/// where every extension's JS (and `js/bootstrap.js`, the `rt_ext` ESM entry point) is evaluated,
/// so no JS source is attached here (`snapshot::base_runtime_options` checks the two lists agree).
pub fn guest_extensions() -> Vec<Extension> {
    guest_extensions_with(deno_web::BlobStore::default_arc())
}

/// [`guest_extensions`] with `blob_store` holding the isolate's `Blob` parts (a guest isolate
/// passes one that charges its memory account, `memory::CountedBlobStore`).
pub fn guest_extensions_with(blob_store: std::sync::Arc<dyn deno_web::BlobStoreTrait>) -> Vec<Extension> {
    vec![
        deno_webidl::deno_webidl::init(),
        deno_web::deno_web::init(blob_store, None, false, deno_web::InMemoryBroadcastChannel::default()),
        deno_fetch::deno_fetch::init(deno_fetch::Options::default()),
        deno_crypto::deno_crypto::init(None),
        rt_ext::init(),
    ]
}
