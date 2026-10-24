//! `MockCdn`: a local HTTP server standing in for esm.sh / jsr.io in build tests (BLD-04..06,
//! BLD-16): it serves fixture modules, counts hits per path (so tests can assert "no network
//! call", BLD-05) and lets a test change a file mid-run (`lock_mismatch`, BLD-06).
//!
//! The server binds `127.0.0.1`, which the egress guard blocks by design; builder tests must
//! point the fetcher at the mock through the guard's test allowance (see `rt-build` tests).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
struct Fixture {
    body: Bytes,
    content_type: String,
}

#[derive(Default)]
struct CdnState {
    files: Mutex<BTreeMap<String, Fixture>>,
    hits: Mutex<BTreeMap<String, u32>>,
}

pub struct MockCdn {
    addr: SocketAddr,
    state: Arc<CdnState>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl MockCdn {
    /// Bind a random local port and start serving.
    pub async fn start() -> std::io::Result<Self> {
        let state = Arc::new(CdnState::default());
        let app = Router::new().fallback(serve_fixture).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await;
        });
        Ok(Self {
            addr,
            state,
            shutdown: Some(tx),
            task: Some(task),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port><path>`
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// Serve `body` at `path` (replacing any previous content: BLD-06 changes a locked URL).
    pub fn set(&self, path: &str, body: impl Into<Bytes>, content_type: &str) {
        self.state.files.lock().insert(
            path.to_string(),
            Fixture {
                body: body.into(),
                content_type: content_type.to_string(),
            },
        );
    }

    /// Serve a JavaScript module.
    pub fn set_js(&self, path: &str, source: &str) {
        self.set(path, Bytes::from(source.to_string()), "application/javascript");
    }

    /// Requests received for `path`.
    pub fn hits(&self, path: &str) -> u32 {
        self.state.hits.lock().get(path).copied().unwrap_or(0)
    }

    pub fn total_hits(&self) -> u32 {
        self.state.hits.lock().values().sum()
    }

    pub fn reset_hits(&self) {
        self.state.hits.lock().clear();
    }

    /// Stop the server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.task.take() {
            let _ = t.await;
        }
    }
}

impl Drop for MockCdn {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn serve_fixture(State(state): State<Arc<CdnState>>, uri: Uri) -> Response {
    let path = uri.path().to_string();
    *state.hits.lock().entry(path.clone()).or_insert(0) += 1;
    match state.files.lock().get(&path).cloned() {
        Some(f) => ([(header::CONTENT_TYPE, f.content_type)], f.body).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
