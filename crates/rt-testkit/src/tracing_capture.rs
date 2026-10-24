//! Capture the `tracing` events of the current thread as text, to assert on log lines.
//!
//! ```ignore
//! let (_guard, logs) = rt_testkit::capture_tracing();
//! do_something_that_warns();
//! assert!(logs.text().contains("could not append event"));
//! ```
//! The capture lasts as long as the returned guard and covers the thread that created it (a
//! `#[tokio::test]` runs on one), not tasks spawned onto other threads.

use std::io::Write;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::subscriber::DefaultGuard;

/// What was logged since [`capture_tracing`].
#[derive(Clone, Default)]
pub struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Every captured event, one per line: level, target, message and fields.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }

    /// Whether any captured event contains `needle`.
    pub fn contains(&self, needle: &str) -> bool {
        self.text().contains(needle)
    }
}

struct Sink(Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut held = self.0.lock();
        // A process-wide capture must not grow without bound: keep the newest 4 MiB.
        if held.len() > 8 * 1024 * 1024 {
            held.drain(..4 * 1024 * 1024);
        }
        held.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Start capturing this thread's `tracing` events (all levels) until the guard is dropped.
pub fn capture_tracing() -> (DefaultGuard, CapturedLogs) {
    let logs = CapturedLogs::default();
    let buf = logs.0.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || Sink(buf.clone()))
        .finish();
    (tracing::subscriber::set_default(subscriber), logs)
}

/// Capture the `tracing` events of the whole process, from every thread, for the rest of the
/// process: for tests that run a node on a multi-thread runtime. Installed once; every call returns
/// the same capture, which also holds what other tests of the process logged (assert on text only
/// your test produces).
pub fn capture_tracing_globally() -> CapturedLogs {
    static LOGS: std::sync::OnceLock<CapturedLogs> = std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = CapturedLogs::default();
        let buf = logs.0.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || Sink(buf.clone()))
            .finish();
        // Another subscriber installed first (a test of its own) wins; the capture is then empty.
        let _ = tracing::subscriber::set_global_default(subscriber);
        logs
    })
    .clone()
}
