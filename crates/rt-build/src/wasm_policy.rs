//! WASM import policy (contracts C-7), shared by deploy-time validation (`rt-build`) and the
//! linker (`rt-exec-wasm`).
//!
//! Linked: `wasi:io/*`, `wasi:clocks/*`, `wasi:random/*`, `wasi:cli/*` (stdin empty, exit = task
//! end), `wasi:http/*`, `std:host/*`.
//! `wasi:filesystem/*` is linked with **no preopens** (nothing to open); `wasi:sockets/*` is
//! linked with **all addresses denied**. Any `0.2.x` WASI minor is accepted.
//! Rejected at deploy: the threads proposal / shared memory, and a component without the
//! `wasi:http/incoming-handler` export.

/// WASI packages guests may import.
pub const LINKED_WASI_PACKAGES: &[&str] = &[
    "wasi:io",
    "wasi:clocks",
    "wasi:random",
    "wasi:cli",
    "wasi:http",
    "wasi:filesystem",
    "wasi:sockets",
];

/// Operator-provided package.
pub const STD_HOST_PACKAGE: &str = "std:host";

/// The export a guest must provide (any `0.2.x`).
pub const REQUIRED_EXPORT: &str = "wasi:http/incoming-handler";

/// Split `namespace:package/interface@version` into `(package, interface, version)` where
/// `package` is `namespace:package`.
pub fn split_interface(name: &str) -> Option<(&str, &str, Option<&str>)> {
    let (path, version) = match name.split_once('@') {
        Some((p, v)) => (p, Some(v)),
        None => (name, None),
    };
    let (package, interface) = path.split_once('/')?;
    Some((package, interface, version))
}

/// Is `version` a `0.2.x` WASI version (any minor)?
pub fn is_wasi_02(version: Option<&str>) -> bool {
    matches!(version, Some(v) if v == "0.2" || v.starts_with("0.2."))
}

/// May a component import `name` (e.g. `wasi:http/types@0.2.6`, `std:host/kv@0.1.0`)?
pub fn is_import_allowed(name: &str) -> bool {
    let Some((package, _interface, version)) = split_interface(name) else {
        return false;
    };
    if package == STD_HOST_PACKAGE {
        return true;
    }
    LINKED_WASI_PACKAGES.contains(&package) && is_wasi_02(version)
}

/// The error text for a WIT import of the operator package renamed before release
/// (`rt:host/kv@0.1.0` -> `std:host/kv@0.1.0`).
pub fn renamed_import(name: &str) -> Option<String> {
    let rest = name.strip_prefix("rt:host/")?;
    Some(format!("`{name}` was renamed to `std:host/{rest}`"))
}

/// Does `export` satisfy [`REQUIRED_EXPORT`] (any `0.2.x`)?
pub fn is_incoming_handler(export: &str) -> bool {
    match split_interface(export) {
        Some((package, interface, version)) => {
            package == "wasi:http" && interface == "incoming-handler" && is_wasi_02(version)
        }
        None => false,
    }
}
