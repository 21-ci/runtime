//! An S3 on a loopback port: path-style objects, multipart, list, faults on demand.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use parking_lot::Mutex;

#[derive(Default)]
pub struct Inner {
    pub objects: HashMap<String, Vec<u8>>,
    uploads: HashMap<String, BTreeMap<u32, Vec<u8>>>,
    pub part_sizes: Vec<usize>,
    pub aborted: usize,
    pub auth_headers: Vec<String>,
    /// Fail this part number once.
    pub fail_part_once: Option<u32>,
    pub fail_every_part: bool,
    pub part_attempts: usize,
    /// Refuse a PUT whose key ends so, with a body echoing this text.
    pub refuse_suffix: Option<String>,
    pub error_body: String,
    pub requests: usize,
}

#[derive(Default)]
pub struct Shared {
    pub inner: Mutex<Inner>,
    in_flight: AtomicUsize,
    pub max_in_flight: AtomicUsize,
}

pub struct MockS3 {
    pub url: String,
    pub state: Arc<Shared>,
}

pub const BUCKET: &str = "backups";

fn answer(code: StatusCode, body: impl Into<Body>) -> Response {
    let mut r = Response::new(body.into());
    *r.status_mut() = code;
    r
}

fn query(req: &Request) -> HashMap<String, String> {
    req.uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (k.to_string(), v.replace("%2F", "/"))
        })
        .collect()
}

async fn handle(State(s): State<Arc<Shared>>, req: Request) -> Response {
    let q = query(&req);
    let method = req.method().clone();
    let path = req.uri().path().trim_start_matches('/').to_string();
    let key = path
        .strip_prefix(BUCKET)
        .unwrap_or("")
        .trim_start_matches('/')
        .to_string();
    {
        let mut i = s.inner.lock();
        i.requests += 1;
        if let Some(a) = req.headers().get("authorization") {
            i.auth_headers.push(a.to_str().unwrap_or("").to_string());
        } else {
            return answer(StatusCode::FORBIDDEN, "unsigned");
        }
    }
    let body = to_bytes(req.into_body(), 1 << 30).await.expect("body").to_vec();

    if method == Method::POST && q.contains_key("uploads") {
        let id = format!("up{}", ulid::Ulid::new());
        s.inner.lock().uploads.insert(id.clone(), BTreeMap::new());
        return answer(
            StatusCode::OK,
            format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
        );
    }
    if method == Method::PUT && q.contains_key("partNumber") {
        let n: u32 = q["partNumber"].parse().expect("part");
        let now = s.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        s.max_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        s.in_flight.fetch_sub(1, Ordering::SeqCst);
        let mut i = s.inner.lock();
        i.part_attempts += 1;
        if i.fail_every_part || i.fail_part_once == Some(n) {
            i.fail_part_once = None;
            return answer(StatusCode::INTERNAL_SERVER_ERROR, "boom");
        }
        i.part_sizes.push(body.len());
        let Some(up) = i.uploads.get_mut(&q["uploadId"]) else {
            return answer(StatusCode::NOT_FOUND, "NoSuchUpload");
        };
        up.insert(n, body);
        let mut r = answer(StatusCode::OK, "");
        r.headers_mut()
            .insert("etag", format!("\"e{n}\"").parse().expect("etag"));
        return r;
    }
    if method == Method::POST && q.contains_key("uploadId") {
        let mut i = s.inner.lock();
        let Some(parts) = i.uploads.remove(&q["uploadId"]) else {
            return answer(StatusCode::NOT_FOUND, "NoSuchUpload");
        };
        i.objects.insert(key, parts.into_values().flatten().collect());
        return answer(StatusCode::OK, "<CompleteMultipartUploadResult/>");
    }
    if method == Method::DELETE && q.contains_key("uploadId") {
        let mut i = s.inner.lock();
        i.uploads.remove(&q["uploadId"]);
        i.aborted += 1;
        return answer(StatusCode::NO_CONTENT, "");
    }
    if method == Method::GET && q.contains_key("list-type") {
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let i = s.inner.lock();
        let mut keys: Vec<&String> = i.objects.keys().filter(|k| k.starts_with(&prefix)).collect();
        keys.sort();
        let items: String = keys
            .iter()
            .map(|k| format!("<Contents><Key>{k}</Key></Contents>"))
            .collect();
        return answer(StatusCode::OK, format!("<ListBucketResult>{items}</ListBucketResult>"));
    }
    let mut i = s.inner.lock();
    match method {
        Method::PUT => {
            if i.refuse_suffix.as_deref().is_some_and(|x| key.ends_with(x)) {
                return answer(StatusCode::FORBIDDEN, i.error_body.clone());
            }
            i.objects.insert(key, body);
            answer(StatusCode::OK, "")
        }
        Method::HEAD => match i.objects.get(&key) {
            Some(b) => {
                let mut r = answer(StatusCode::OK, "");
                r.headers_mut().insert("content-length", b.len().into());
                r
            }
            None => answer(StatusCode::NOT_FOUND, ""),
        },
        Method::GET => match i.objects.get(&key) {
            Some(b) => answer(StatusCode::OK, b.clone()),
            None => answer(StatusCode::NOT_FOUND, ""),
        },
        Method::DELETE => {
            i.objects.remove(&key);
            answer(StatusCode::NO_CONTENT, "")
        }
        _ => answer(StatusCode::METHOD_NOT_ALLOWED, ""),
    }
}

impl MockS3 {
    pub async fn start() -> Self {
        let state = Arc::new(Shared::default());
        let app = axum::Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, state }
    }

    pub fn keys(&self) -> Vec<String> {
        let mut k: Vec<String> = self.state.inner.lock().objects.keys().cloned().collect();
        k.sort();
        k
    }

    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.state.inner.lock().objects.get(key).cloned()
    }

    pub fn put(&self, key: &str, body: &[u8]) {
        self.state.inner.lock().objects.insert(key.to_string(), body.to_vec());
    }
}
