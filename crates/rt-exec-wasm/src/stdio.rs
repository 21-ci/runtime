//! `wasi:cli` stdout/stderr → the service log (WSM-07).
//!
//! Complete lines are appended through the task's [`HostHandle`] (tagged with the task id,
//! counted against `max_log_lines`): stdout lines are `info`, stderr lines are `error`
//!. The sink is shared by every stream the guest opens and re-pointed at the current
//! task when an instance is reused, so a reused instance never logs under a previous task's id.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use parking_lot::Mutex;
use rt_host::HostHandle;
use rt_obs::LogLevel;
use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::{OutputStream, Pollable, StreamResult};

/// A partial line longer than this is flushed as a line of its own.
const MAX_PARTIAL: usize = 64 * 1024;
/// Bytes a single `write` may carry (the guest is told through `check_write`).
const WRITE_PERMIT: usize = 64 * 1024;

#[derive(Default)]
struct Lines {
    buf: Vec<u8>,
}

/// A log line's text: lossy UTF-8 without the carriage returns a CRLF writer leaves before the `\n`.
fn line_text(line: &[u8]) -> String {
    let mut text = String::from_utf8_lossy(line).into_owned();
    while text.ends_with('\r') {
        text.pop();
    }
    text
}

/// Shared log plumbing of one guest instance.
pub struct GuestLogs {
    target: Mutex<Option<HostHandle>>,
    stdout: Mutex<Lines>,
    stderr: Mutex<Lines>,
}

impl GuestLogs {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            target: Mutex::new(None),
            stdout: Mutex::new(Lines::default()),
            stderr: Mutex::new(Lines::default()),
        })
    }

    /// Route subsequent lines to `host`'s task.
    pub fn attach(&self, host: HostHandle) {
        *self.target.lock() = Some(host);
    }

    /// End of a task: emit what is left of an unterminated line and stop logging.
    pub fn detach(&self) {
        self.flush_partial();
        *self.target.lock() = None;
    }

    fn emit(&self, level: LogLevel, line: &[u8]) {
        if let Some(host) = self.target.lock().as_ref() {
            host.log(level, line_text(line));
        }
    }

    fn feed(&self, lines: &Mutex<Lines>, level: LogLevel, data: &[u8]) {
        let mut complete: Vec<Vec<u8>> = Vec::new();
        {
            let mut g = lines.lock();
            g.buf.extend_from_slice(data);
            while let Some(pos) = g.buf.iter().position(|b| *b == b'\n') {
                let rest = g.buf.split_off(pos + 1);
                let mut line = std::mem::replace(&mut g.buf, rest);
                line.pop();
                complete.push(line);
            }
            if g.buf.len() > MAX_PARTIAL {
                complete.push(std::mem::take(&mut g.buf));
            }
        }
        for line in complete {
            self.emit(level, &line);
        }
    }

    fn flush_partial(&self) {
        for (lines, level) in [(&self.stdout, LogLevel::Info), (&self.stderr, LogLevel::Error)] {
            let rest = std::mem::take(&mut lines.lock().buf);
            if !rest.is_empty() {
                self.emit(level, &rest);
            }
        }
    }

    pub fn stdout_stream(self: &Arc<Self>) -> LogStream {
        LogStream {
            logs: self.clone(),
            is_stderr: false,
        }
    }

    pub fn stderr_stream(self: &Arc<Self>) -> LogStream {
        LogStream {
            logs: self.clone(),
            is_stderr: true,
        }
    }
}

/// `wasi:cli` stdout or stderr backed by [`GuestLogs`].
#[derive(Clone)]
pub struct LogStream {
    logs: Arc<GuestLogs>,
    is_stderr: bool,
}

impl LogStream {
    fn write_bytes(&self, data: &[u8]) {
        if self.is_stderr {
            self.logs.feed(&self.logs.stderr, LogLevel::Error, data);
        } else {
            self.logs.feed(&self.logs.stdout, LogLevel::Info, data);
        }
    }
}

impl IsTerminal for LogStream {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for LogStream {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }

    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

#[async_trait::async_trait]
impl Pollable for LogStream {
    async fn ready(&mut self) {}
}

impl OutputStream for LogStream {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.write_bytes(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(WRITE_PERMIT)
    }
}

impl AsyncWrite for LogStream {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.write_bytes(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn a_line_loses_its_trailing_carriage_returns_and_invalid_utf8_becomes_replacement_characters() {
        assert_eq!(line_text(b"done\r\r"), "done");
        assert_eq!(line_text(b"a\rb"), "a\rb", "only trailing returns go");
        assert_eq!(line_text(b"ok\xff"), "ok\u{fffd}");
    }

    #[test]
    fn a_partial_line_longer_than_the_cap_is_flushed_instead_of_growing() {
        let logs = GuestLogs::new();
        logs.feed(&logs.stdout, LogLevel::Info, &vec![b'x'; MAX_PARTIAL]);
        assert_eq!(
            logs.stdout.lock().buf.len(),
            MAX_PARTIAL,
            "a partial line at the cap is kept"
        );
        logs.feed(&logs.stdout, LogLevel::Info, b"y");
        assert!(
            logs.stdout.lock().buf.is_empty(),
            "past the cap the partial line goes out as a line"
        );
    }

    #[test]
    fn detach_flushes_the_unterminated_tail_of_both_streams() {
        let logs = GuestLogs::new();
        logs.feed(&logs.stdout, LogLevel::Info, b"out");
        logs.feed(&logs.stderr, LogLevel::Error, b"err\nrest");
        assert_eq!(logs.stderr.lock().buf, b"rest");
        logs.detach();
        assert!(logs.stdout.lock().buf.is_empty() && logs.stderr.lock().buf.is_empty());
    }

    #[tokio::test]
    async fn the_async_writer_takes_everything_and_the_stream_is_never_a_terminal() {
        let logs = GuestLogs::new();
        let stdout = logs.stdout_stream();
        assert!(!stdout.is_terminal());
        // The stream wasi hands the guest writes through the same buffer as the async writer.
        let mut writer = Box::into_pin(stdout.async_stream());
        assert_eq!(writer.write(b"abc\nde").await.expect("write"), 6);
        writer.flush().await.expect("flush");
        writer.shutdown().await.expect("shutdown");
        assert_eq!(logs.stdout.lock().buf, b"de", "the complete line left, the tail waits");
        assert!(
            logs.stderr.lock().buf.is_empty(),
            "stdout does not write to stderr's buffer"
        );

        let mut stderr = logs.stderr_stream();
        OutputStream::write(&mut stderr, Bytes::from_static(b"warn")).expect("write");
        OutputStream::flush(&mut stderr).expect("flush");
        assert_eq!(OutputStream::check_write(&mut stderr).expect("permit"), WRITE_PERMIT);
        Pollable::ready(&mut stderr).await;
        assert_eq!(logs.stderr.lock().buf, b"warn");
    }
}
