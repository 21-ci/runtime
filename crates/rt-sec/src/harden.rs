//! Process hardening (SECURITY.md).
//!
//! MVP: `PR_SET_DUMPABLE=0` and `RLIMIT_CORE=0` on Linux (SEC-33). seccomp-bpf and Landlock are
//! stretch (`security.seccomp`); the hooks exist so `rt-server` calls them in the right order
//! (after init, before accepting traffic).

use std::path::PathBuf;

use thiserror::Error;

#[derive(Clone, Debug, Default)]
pub struct HardeningConfig {
    /// `security.seccomp` (stretch; Linux best effort).
    pub seccomp: bool,
    /// Paths the process may touch with Landlock (`$RT_DATA` + read-only binary paths).
    pub landlock_rw: Vec<PathBuf>,
    pub landlock_ro: Vec<PathBuf>,
}

#[derive(Debug, Error)]
pub enum HardenError {
    #[error("hardening step `{step}` failed: {reason}")]
    Step { step: &'static str, reason: String },
}

/// Apply hardening. Non-Linux platforms are a no-op (development on macOS).
pub fn harden_process(cfg: &HardeningConfig) -> Result<(), HardenError> {
    let _ = cfg;
    #[cfg(target_os = "linux")]
    {
        // SAFETY: plain prctl/setrlimit calls with valid arguments.
        unsafe {
            if libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0 {
                return Err(HardenError::Step {
                    step: "PR_SET_DUMPABLE",
                    reason: std::io::Error::last_os_error().to_string(),
                });
            }
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(HardenError::Step {
                    step: "PR_SET_NO_NEW_PRIVS",
                    reason: std::io::Error::last_os_error().to_string(),
                });
            }
            let lim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &lim) != 0 {
                return Err(HardenError::Step {
                    step: "RLIMIT_CORE",
                    reason: std::io::Error::last_os_error().to_string(),
                });
            }
        }
    }
    Ok(())
}

/// `Some(dumpable)` on Linux (`prctl(PR_GET_DUMPABLE)`), `None` elsewhere. After
/// [`harden_process`] it must be `Some(false)` (SEC-33).
pub fn is_dumpable() -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: PR_GET_DUMPABLE takes no pointer arguments.
        Some(unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) } == 1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
