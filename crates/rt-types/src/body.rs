//! Byte-stream bodies used for request payloads and responses.
//!
//! A [`Body`] is a `Send` stream of `Bytes` chunks. Ingress hands the executor a request body;
//! the executor hands back a response body that is pumped to the I/O thread through a bounded
//! channel ([`body_channel`]) so slow clients apply backpressure to guests.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio::sync::mpsc;

/// Errors while reading or producing a body.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BodyError {
    /// More bytes than the configured limit.
    #[error("body exceeds the limit of {limit} bytes")]
    TooLarge { limit: u64 },
    /// The peer went away or the producer dropped the channel.
    #[error("body stream aborted: {0}")]
    Aborted(String),
    /// Transport / IO error (sanitized message).
    #[error("body io error: {0}")]
    Io(String),
}

/// A streaming body.
pub struct Body {
    inner: BoxStream<'static, Result<Bytes, BodyError>>,
    /// Exact length when known (e.g. `content-length`, buffered bodies).
    exact_len: Option<u64>,
}

impl Body {
    /// Empty body.
    pub fn empty() -> Self {
        Self {
            inner: stream::empty().boxed(),
            exact_len: Some(0),
        }
    }

    /// A fully buffered body (one chunk; zero chunks when empty).
    pub fn from_bytes(b: impl Into<Bytes>) -> Self {
        let b: Bytes = b.into();
        let len = b.len() as u64;
        if b.is_empty() {
            return Self::empty();
        }
        Self {
            inner: stream::once(async move { Ok(b) }).boxed(),
            exact_len: Some(len),
        }
    }

    /// Wrap any `Send` stream of chunks.
    pub fn from_stream<S>(s: S) -> Self
    where
        S: Stream<Item = Result<Bytes, BodyError>> + Send + 'static,
    {
        Self {
            inner: s.boxed(),
            exact_len: None,
        }
    }

    /// Like [`Body::from_stream`] but with a known total length.
    pub fn from_stream_sized<S>(s: S, len: u64) -> Self
    where
        S: Stream<Item = Result<Bytes, BodyError>> + Send + 'static,
    {
        Self {
            inner: s.boxed(),
            exact_len: Some(len),
        }
    }

    /// Exact length if known up front.
    pub fn exact_len(&self) -> Option<u64> {
        self.exact_len
    }

    /// Read the whole body, failing with [`BodyError::TooLarge`] past `limit` bytes.
    pub async fn collect(mut self, limit: u64) -> Result<Bytes, BodyError> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = self.inner.next().await {
            let chunk = chunk?;
            if (buf.len() as u64).saturating_add(chunk.len() as u64) > limit {
                return Err(BodyError::TooLarge { limit });
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }

    /// Unwrap into the boxed stream.
    pub fn into_stream(self) -> BoxStream<'static, Result<Bytes, BodyError>> {
        self.inner
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body")
            .field("exact_len", &self.exact_len)
            .finish_non_exhaustive()
    }
}

impl Stream for Body {
    type Item = Result<Bytes, BodyError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl From<Bytes> for Body {
    fn from(b: Bytes) -> Self {
        Body::from_bytes(b)
    }
}

impl From<Vec<u8>> for Body {
    fn from(v: Vec<u8>) -> Self {
        Body::from_bytes(Bytes::from(v))
    }
}

impl From<&'static str> for Body {
    fn from(s: &'static str) -> Self {
        Body::from_bytes(Bytes::from_static(s.as_bytes()))
    }
}

impl From<String> for Body {
    fn from(s: String) -> Self {
        Body::from_bytes(Bytes::from(s))
    }
}

/// Producer half of a bounded body channel.
#[derive(Debug, Clone)]
pub struct BodySender {
    tx: mpsc::Sender<Result<Bytes, BodyError>>,
}

impl BodySender {
    /// Send a chunk, waiting while the channel is full (backpressure).
    /// Returns `Err` when the consumer is gone.
    pub async fn send(&self, chunk: Bytes) -> Result<(), BodyError> {
        self.tx
            .send(Ok(chunk))
            .await
            .map_err(|_| BodyError::Aborted("receiver dropped".into()))
    }

    /// Terminate the stream with an error.
    pub async fn abort(&self, err: BodyError) {
        let _ = self.tx.send(Err(err)).await;
    }

    /// True when the consumer dropped its half.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

struct ChannelStream {
    rx: mpsc::Receiver<Result<Bytes, BodyError>>,
}

impl Stream for ChannelStream {
    type Item = Result<Bytes, BodyError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Bounded producer/consumer body channel (`capacity` chunks in flight, >= 1).
pub fn body_channel(capacity: usize) -> (BodySender, Body) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (BodySender { tx }, Body::from_stream(ChannelStream { rx }))
}
