//! The T1 value of a Deno service and the V8 engine fingerprint.

use std::sync::{Once, OnceLock};

use rt_build::{Bundle, CodeCache};
use rt_types::{HandlerForm, PreparedKey};

use crate::config::DenoExecutorConfig;

/// Prepared bundle: module map + V8 code cache + handler form.
/// Shared (`Arc`) between every task of the same code; immutable.
#[derive(Debug)]
pub struct DenoPrepared {
    pub key: PreparedKey,
    pub bundle: Bundle,
    /// `None` when the cache was rejected or never produced (falls back to compile).
    pub code_cache: Option<CodeCache>,
    pub handler_form: HandlerForm,
    /// Bytes charged to the T1 cache budget.
    pub accounted_bytes: u64,
    /// [`DenoPrepared::may_read_env`], computed on first use.
    pub reads_env: OnceLock<bool>,
}

impl DenoPrepared {
    /// Whether the code may read `Deno.env` or `std:secrets`: some module mentions `Deno` or imports
    /// `std:secrets`. The executor then fetches the env before dispatch (asynchronously) instead of
    /// on the synchronous read.
    pub fn may_read_env(&self) -> bool {
        *self.reads_env.get_or_init(|| {
            self.bundle
                .modules
                .iter()
                .any(|m| m.source.contains("Deno") || m.source.contains("std:secrets"))
        })
    }
}

/// Fingerprint of the V8 build and flags: part of every `PreparedKey`, so artifacts compiled
/// under another engine are never loaded.
pub fn engine_fingerprint(flags: &[String]) -> u64 {
    // FNV-1a over `V8 version` + flags (sorted for stability).
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for b in bytes {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h ^= 0xff;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    };
    feed(deno_core::v8::V8::get_version().as_bytes());
    let mut f: Vec<&String> = flags.iter().collect();
    f.sort();
    for flag in f {
        feed(flag.as_bytes());
    }
    h
}

/// The flags `v8_set_flags` handed back as not understood: it returns the program name first, then
/// the arguments V8 left unparsed.
fn unrecognized_flags(returned: &[String]) -> &[String] {
    returned.get(1..).unwrap_or_default()
}

/// Initialize V8 once per process: flags and the platform with `v8_platform_threads` workers.
/// Must run before the first `JsRuntime` is created.
pub fn init_v8_once(cfg: &DenoExecutorConfig) {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let mut args = vec!["rt".to_string()];
        args.extend(cfg.v8_flags.iter().cloned());
        args.extend(cfg.derived_v8_flags());
        let returned = deno_core::v8_set_flags(args);
        let unrecognized = unrecognized_flags(&returned);
        if !unrecognized.is_empty() {
            tracing::warn!(flags = ?unrecognized, "V8 did not recognize these flags");
            // LCOV_EXCL_LINE: V8 starts once per process, the first test's flags decide
        }
        let platform = crate::platform::new_platform(cfg.v8_platform_threads);
        deno_core::JsRuntime::init_platform(Some(platform));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_arguments_after_the_program_name_count_as_unrecognized() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert!(unrecognized_flags(&s(&["rt"])).is_empty());
        assert!(unrecognized_flags(&[]).is_empty());
        assert_eq!(
            unrecognized_flags(&s(&["rt", "--bogus", "--other"])),
            &s(&["--bogus", "--other"])[..]
        );
    }
}
