//! Embedded JavaScript (`/js` at the repository root): the bootstrap run when the base snapshot
//! is built, and the `std:*` modules served to guests by the bundle loader (not bundled).

pub const BOOTSTRAP_JS: &str = include_str!("../../../js/bootstrap.js");
pub const RT_KV_JS: &str = include_str!("../../../js/rt_kv.js");
pub const RT_SQL_JS: &str = include_str!("../../../js/rt_sql.js");
pub const RT_SQLITE_JS: &str = include_str!("../../../js/rt_sqlite.js");
pub const RT_BLOB_JS: &str = include_str!("../../../js/rt_blob.js");
pub const RT_QUEUE_JS: &str = include_str!("../../../js/rt_queue.js");
pub const RT_SERVICES_JS: &str = include_str!("../../../js/rt_services.js");
pub const RT_PLATFORM_JS: &str = include_str!("../../../js/rt_platform.js");
pub const RT_LOG_JS: &str = include_str!("../../../js/rt_log.js");
pub const RT_SECRETS_JS: &str = include_str!("../../../js/rt_secrets.js");
pub const RT_LOCK_JS: &str = include_str!("../../../js/rt_lock.js");
pub const RT_SCHEDULE_JS: &str = include_str!("../../../js/rt_schedule.js");
pub const RT_DURABLE_JS: &str = include_str!("../../../js/rt_durable.js");
pub const RT_SOCKETS_JS: &str = include_str!("../../../js/rt_sockets.js");
pub const RT_SDK_JS: &str = include_str!("../../../js/rt_sdk.js");
pub const RT_SDK_SERVER_JS: &str = include_str!("../../../js/rt_sdk_server.js");

/// `(specifier, source)` of every built-in `std:*` module.
pub const RT_MODULES: &[(&str, &str)] = &[
    ("std:kv", RT_KV_JS),
    ("std:log", RT_LOG_JS),
    ("std:secrets", RT_SECRETS_JS),
    ("std:lock", RT_LOCK_JS),
    ("std:schedule", RT_SCHEDULE_JS),
    ("std:sockets", RT_SOCKETS_JS),
    ("std:durable", RT_DURABLE_JS),
    ("std:sql", RT_SQL_JS),
    ("std:sqlite", RT_SQLITE_JS),
    ("std:blob", RT_BLOB_JS),
    ("std:queue", RT_QUEUE_JS),
    ("std:services", RT_SERVICES_JS),
    ("std:platform", RT_PLATFORM_JS),
    ("std:sdk", RT_SDK_JS),
    ("std:sdk/server", RT_SDK_SERVER_JS),
];

/// Source of a built-in `std:*` module.
pub fn rt_module_source(specifier: &str) -> Option<&'static str> {
    RT_MODULES.iter().find(|(s, _)| *s == specifier).map(|(_, src)| *src)
}

/// The error text for an import of a module renamed before release (`rt:kv` -> `std:kv`).
pub fn renamed_module_message(specifier: &str) -> Option<String> {
    rt_build::renamed_to(specifier).map(|to| format!("`{specifier}` was renamed to `{to}`"))
}
