//! System logs: `tracing` JSON to stdout filtered by `RT_LOG`, falling
//! back to the conventional `RUST_LOG` when `RT_LOG` is unset. Never contains env/secret values.

use tracing_subscriber::EnvFilter;

use crate::ObsError;

#[derive(Clone, Debug)]
pub struct TracingOptions {
    /// Filter directive used when neither `RT_LOG` nor `RUST_LOG` is set (e.g. `info,rt_sched=debug`).
    pub default_filter: String,
    /// JSON lines (default) or human-readable.
    pub json: bool,
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            default_filter: "info".into(),
            json: true,
        }
    }
}

/// Pick the filter: `RT_LOG` when set (an invalid value falls back to `default`), else `RUST_LOG`,
/// else `default`.
fn env_filter(rt_log: Option<&str>, rust_log: Option<&str>, default: &str) -> EnvFilter {
    match rt_log.or(rust_log) {
        Some(directives) => EnvFilter::try_new(directives).unwrap_or_else(|_| EnvFilter::new(default)),
        None => EnvFilter::new(default),
    }
}

/// Install the global subscriber. Safe to call once; a second call returns an error.
pub fn init_tracing(opts: &TracingOptions) -> Result<(), ObsError> {
    let (rt_log, rust_log) = (std::env::var("RT_LOG").ok(), std::env::var("RUST_LOG").ok());
    let filter = env_filter(rt_log.as_deref(), rust_log.as_deref(), &opts.default_filter);
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    let res = if opts.json {
        builder.json().try_init()
    } else {
        builder.try_init()
    };
    res.map_err(|e| ObsError::Tracing(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::env_filter;

    #[test]
    fn rt_log_wins_then_rust_log_then_default() {
        assert_eq!(env_filter(Some("warn"), Some("debug"), "info").to_string(), "warn");
        assert_eq!(env_filter(None, Some("debug"), "info").to_string(), "debug");
        assert_eq!(env_filter(None, None, "info").to_string(), "info");
        // an invalid RT_LOG does not silently defer to RUST_LOG
        assert_eq!(env_filter(Some("[[["), Some("debug"), "info").to_string(), "info");
    }
}
