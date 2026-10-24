//! Build-time V8 startup snapshot of the guest isolate.
//!
//! The `deno_web` / `deno_fetch` / `deno_crypto` / `deno_webidl` crates of this Deno release keep
//! their JavaScript on disk (`LoadedFromFsDuringSnapshot`: absolute paths into the cargo registry),
//! so an isolate can only be created from a snapshot that already contains that code. Building the
//! snapshot here makes the `rt` binary self-contained: nothing is read from the registry at
//! runtime (the Docker runtime image only has the binary).
//!
//! Outputs in `OUT_DIR`:
//! - `RT_SNAPSHOT.bin`: the snapshot (extensions + `js/bootstrap.js`, no tenant data);
//! - `residual/*.js`: `lazy_loaded_*` sources the bootstrap did not consume, which deno_core wants
//!   as `residual_lazy_*_sources` at runtime;
//! - `rt_snapshot_tables.rs`: those tables plus the extension/op list the snapshot was built with,
//!   which `src/snapshot.rs` checks against the real extension set before any isolate starts.
//!
//! `rt_ext` here has **stub ops**: the real ops (`src/ops.rs`) need the whole host stack, which a
//! build script cannot link. A snapshot only records op *positions* and the JS wrappers built from
//! op shapes, so the stubs keep the real ops' names, order, sync/async kind and argument count.
//! Keep the stub list in sync with `src/exts.rs` / `src/ops.rs`; `src/snapshot.rs` fails the first
//! isolate with a precise message when they drift.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::rc::Rc;
use std::{env, fs};

use deno_core::snapshot::{create_snapshot, CreateSnapshotOptions};
use deno_core::{op2, Extension, ExtensionFileSource, OpState};

/// One stub op per real op. Only the shape matters: `deno_core` builds the JS-side wrapper of an
/// async op from its argument count (`setUpAsyncStub` switches on `fn.length`), so each stub has
/// the real op's `state` parameter and as many dummy arguments as the real one has JS arguments.
///
/// Every stub body must be **unique**. V8 serializes a native function as the index of its address
/// in the external reference table, so two stubs whose machine code is identical must not exist:
/// in an optimized build (this script is built with `opt-level = 3`, Docker/`--release`) LLVM
/// merges identical functions into one address, both ops then serialize as the first of them, and
/// at run time e.g. `op_rt_respond(status, headers)` lands in `op_rt_log` (`no_response` /
/// `guest_error` on every request). `black_box(stringify!(name))` gives each stub its own constant.
macro_rules! stub_ops {
    ($( $kind:ident $name:ident ( $($arg:ident),* ); )*) => {
        $( stub_ops!(@one $kind $name ( $($arg),* )); )*
    };
    (@one fast $name:ident ( $($arg:ident),* )) => {
        #[op2(fast)] fn $name(_state: &mut OpState $(, #[smi] $arg: u32 )*) { std::hint::black_box(stringify!($name)); }
    };
    (@one sync $name:ident ( $($arg:ident),* )) => {
        #[op2(nofast)] fn $name(_state: &mut OpState $(, #[smi] $arg: u32 )*) { std::hint::black_box(stringify!($name)); }
    };
    (@one asynchronous $name:ident ( $($arg:ident),* )) => {
        #[op2] async fn $name(_state: Rc<RefCell<OpState>> $(, #[smi] $arg: u32 )*) { std::hint::black_box(stringify!($name)); }
    };
}

stub_ops! {
    fast op_rt_log(_a, _b);
    fast op_rt_log_fields(_a, _b, _c);
    fast op_rt_now();
    fast op_rt_perf_now();
    sync op_rt_env_get(_a);
    sync op_rt_env_all();
    sync op_rt_secret_get(_a);
    sync op_rt_respond(_a, _b, _c);
    fast op_rt_fail(_a, _b);
    asynchronous op_rt_resp_chunk(_a);
    fast op_rt_resp_end();
    asynchronous op_rt_resp_abort(_a);
    asynchronous op_rt_fetch(_a, _b);
    asynchronous op_rt_call(_a, _b);
    asynchronous op_rt_sdk_invoke(_a, _b, _c);
    asynchronous op_rt_platform_fetch(_a, _b);
    asynchronous op_rt_body_read(_a);
    fast op_rt_body_close(_a);
    asynchronous op_rt_kv_get(_a);
    asynchronous op_rt_kv_get_many(_a);
    asynchronous op_rt_kv_set(_a, _b, _c, _d);
    asynchronous op_rt_kv_delete(_a);
    asynchronous op_rt_kv_list(_a);
    asynchronous op_rt_kv_incr(_a, _b, _c);
    asynchronous op_rt_kv_cas(_a, _b, _c, _d, _e, _f, _g, _h);
    asynchronous op_rt_kv_expire(_a, _b);
    asynchronous op_rt_kv_ttl(_a);
    asynchronous op_rt_sql_execute(_a, _b);
    asynchronous op_rt_sql_batch(_a, _b);
    asynchronous op_rt_sql_begin();
    asynchronous op_rt_sql_tx_execute(_a, _b, _c);
    asynchronous op_rt_sql_tx_end(_a, _b);
    asynchronous op_rt_blob_head(_a);
    asynchronous op_rt_blob_get(_a);
    sync op_rt_blob_put_begin(_a, _b);
    asynchronous op_rt_blob_put_write(_a, _b);
    asynchronous op_rt_blob_put_end(_a, _b);
    asynchronous op_rt_blob_delete(_a);
    asynchronous op_rt_blob_list(_a);
    asynchronous op_rt_enqueue(_a, _b);
    asynchronous op_rt_lock_acquire(_a, _b, _c);
    fast op_rt_lock_release(_a);
    asynchronous op_rt_schedule_after(_a, _b, _c, _d);
    asynchronous op_rt_schedule_at(_a, _b, _c, _d);
    asynchronous op_rt_schedule_cancel(_a);
    asynchronous op_rt_schedule_list(_a);
    fast op_rt_wasm_track(_a, _b);
    fast op_rt_mem_admit(_a, _b);
    sync op_rt_sqlite_open();
    sync op_rt_sqlite_prepare(_a);
    // (the `scope` parameter counts as an argument: sql, args, mode, flags + scope)
    sync op_rt_sqlite_run(_a, _b, _c, _d, _e);
    sync op_rt_sqlite_exec(_a, _b, _c);
    sync op_rt_sqlite_tx(_a, _b);
    fast op_rt_sqlite_in_tx();
    sync op_rt_ws_accept(_a, _b, _c, _d);
    sync op_rt_ws_send(_a, _b, _c, _d, _e);
    sync op_rt_ws_broadcast(_a, _b, _c, _d, _e);
    fast op_rt_ws_close(_a, _b, _c, _d);
    sync op_rt_ws_close_all(_a, _b, _c, _d);
    sync op_rt_ws_list(_a);
    sync op_rt_ws_get(_a);
    sync op_rt_ws_count(_a);
    sync op_rt_ws_objects(_a);
    sync op_rt_ws_set_tags(_a, _b);
    sync op_rt_ws_attachment(_a);
    sync op_rt_ws_set_attachment(_a, _b, _c);
    fast op_rt_ws_auto_response(_a, _b, _c, _d);
    sync op_rt_ws_event_data();
    asynchronous op_rt_task_commit();
    asynchronous op_rt_call_object(_a, _b);
    asynchronous op_rt_storage_get(_a);
    sync op_rt_storage_put(_a, _b, _c);
    asynchronous op_rt_storage_delete(_a);
    asynchronous op_rt_storage_list(_a);
    fast op_rt_storage_delete_all();
    asynchronous op_rt_alarm_get();
    fast op_rt_alarm_set(_a, _b);
    asynchronous op_rt_ws_connect(_a, _b);
    asynchronous op_rt_ws_client_send(_a, _b, _c, _d);
    asynchronous op_rt_ws_client_recv(_a);
    asynchronous op_rt_ws_client_close(_a, _b, _c);
}

// Same order as `rt_ext` in `src/exts.rs`.
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

/// `deno_fetch`'s `22_http_client.js` (pulled in by `Request`) loads `ext:deno_net/02_tls.js`, but
/// `deno_net` is not part of the guest runtime (no sockets, no custom HTTP clients: `fetch` goes
/// through the egress guard). This stand-in only has to exist and export `loadTlsKeyPair`.
const NET_TLS_SHIM: &str = "(function () {
function loadTlsKeyPair(api) {
  throw new TypeError(api + ' is not available in the guest runtime');
}
return { loadTlsKeyPair };
})();
";

/// Same list and order as `exts::guest_extensions()`; `rt_ext` carries the bootstrap as ESM entry.
fn guest_extensions() -> Vec<Extension> {
    let mut rt = rt_ext::init();
    rt.lazy_loaded_js_files = Cow::Owned(vec![ExtensionFileSource::loaded_from_memory_during_snapshot(
        "ext:deno_net/02_tls.js",
        deno_core::ascii_str!(NET_TLS_SHIM),
    )]);
    rt.esm_files = Cow::Owned(vec![
        ExtensionFileSource::loaded_during_snapshot(
            "ext:rt_ext/bootstrap.js",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../js/bootstrap.js"),
        ),
        // `std:sqlite` (imported by the bootstrap).
        ExtensionFileSource::loaded_during_snapshot(
            "ext:rt_ext/sqlite.js",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../js/sqlite.js"),
        ),
    ]);
    rt.esm_entry_point = Some("ext:rt_ext/bootstrap.js");
    vec![
        deno_webidl::deno_webidl::init(),
        deno_web::deno_web::init(
            deno_web::BlobStore::default_arc(),
            None,
            false,
            deno_web::InMemoryBroadcastChannel::default(),
        ),
        deno_fetch::deno_fetch::init(deno_fetch::Options::default()),
        deno_crypto::deno_crypto::init(None),
        rt,
    ]
}

fn main() -> Result<(), String> {
    println!("cargo:rerun-if-changed=build.rs");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is not set")?);

    // Same V8 flag the runtime always sets (`config::BASE_V8_FLAGS`), so the snapshot is taken
    // under the flags it is later deserialized with.
    let _unrecognized = deno_core::v8_set_flags(vec!["rt".to_string(), "--single-threaded-gc".to_string()]);

    // Names and order of what goes into the snapshot, for the runtime parity check.
    let mut extension_names = Vec::new();
    let mut ops = Vec::new();
    for ext in guest_extensions() {
        extension_names.push(ext.name);
        ops.extend(ext.ops.iter().map(|d| (d.name, d.is_async, d.arg_count)));
    }

    let created = create_snapshot(
        CreateSnapshotOptions {
            cargo_manifest_dir: env!("CARGO_MANIFEST_DIR"),
            startup_snapshot: None,
            skip_op_registration: false,
            extensions: guest_extensions(),
            extension_transpiler: None,
            with_runtime_cb: None,
        },
        None,
    )
    .map_err(|e| format!("cannot create the base V8 snapshot: {e}"))?;

    for path in &created.files_loaded_during_snapshot {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-changed=../../js/bootstrap.js");
    println!("cargo:rerun-if-changed=../../js/sqlite.js");
    fs::write(out_dir.join("RT_SNAPSHOT.bin"), &created.output)
        .map_err(|e| format!("cannot write RT_SNAPSHOT.bin: {e}"))?;

    // lazy_loaded_* sources the bootstrap never loaded are not in the snapshot: ship them as the
    // residual tables (read from the registry now, embedded into the binary).
    let consumed: HashSet<&str> = created.consumed_lazy_specifiers.iter().map(String::as_str).collect();
    let residual_dir = out_dir.join("residual");
    fs::create_dir_all(&residual_dir).map_err(|e| format!("cannot create {}: {e}", residual_dir.display()))?;
    let mut tables = String::new();
    let mut n = 0usize;
    for (table, js) in [("RESIDUAL_LAZY_JS", true), ("RESIDUAL_LAZY_ESM", false)] {
        // deno_core binary-searches these tables: they must be sorted by specifier.
        let mut residual = Vec::new();
        for ext in guest_extensions() {
            let files = if js {
                &ext.lazy_loaded_js_files
            } else {
                &ext.lazy_loaded_esm_files
            };
            for file in files.iter().filter(|f| !consumed.contains(f.specifier)) {
                let code = file
                    .load()
                    .map_err(|e| format!("cannot read {}: {e}", file.specifier))?;
                residual.push((file.specifier, code.as_str().to_string()));
            }
        }
        residual.sort_by_key(|(specifier, _)| *specifier);
        writeln!(tables, "pub static {table}: &[(&str, &str)] = &[").map_err(|e| e.to_string())?;
        for (specifier, code) in residual {
            let path = residual_dir.join(format!("{n}.js"));
            n += 1;
            fs::write(&path, code).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
            writeln!(
                tables,
                "    ({specifier:?}, include_str!({:?})),",
                path.display().to_string()
            )
            .map_err(|e| e.to_string())?;
        }
        writeln!(tables, "];").map_err(|e| e.to_string())?;
    }
    writeln!(tables, "pub static EXTENSIONS: &[&str] = &{extension_names:?};").map_err(|e| e.to_string())?;
    writeln!(tables, "/// `(op name, is_async, arg_count)` in registration order.").map_err(|e| e.to_string())?;
    writeln!(tables, "pub static OPS: &[(&str, bool, u8)] = &{ops:?};").map_err(|e| e.to_string())?;
    fs::write(out_dir.join("rt_snapshot_tables.rs"), tables)
        .map_err(|e| format!("cannot write the snapshot tables: {e}"))?;
    Ok(())
}
