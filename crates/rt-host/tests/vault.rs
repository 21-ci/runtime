//! The Vault client against a local mock: no real Vault is involved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rt_host::egress::{EgressGuardConfig, GuardedEgress};
use rt_host::vault::{VaultAuth, VaultClient, VaultSettings};
use rt_sec::secret_source::{SecretSource, SourceError};

/// status, body, location
type Answer = (u16, String, Option<String>);

#[derive(Default)]
struct Mock {
    seen: Mutex<Vec<(String, Option<String>)>>,
    logins: AtomicUsize,
    /// path => answer
    answers: Mutex<HashMap<String, Answer>>,
}

/// A tiny HTTP/1.1 server: one request per connection.
async fn serve(mock: Arc<Mock>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = l.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.expect("accept");
            let mock = mock.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = text.split_whitespace().nth(1).unwrap_or("").to_string();
                let token = text.lines().find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("x-vault-token: ")
                        .map(str::to_string)
                });
                mock.seen.lock().push((path.clone(), token));
                if path.ends_with("/auth/approle/login") {
                    mock.logins.fetch_add(1, Ordering::Relaxed);
                }
                let (status, body, loc) = mock
                    .answers
                    .lock()
                    .get(&path)
                    .cloned()
                    .unwrap_or((404, "{}".into(), None));
                let loc = loc.map_or(String::new(), |l| format!("location: {l}\r\n"));
                let out = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n{loc}\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(out.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

fn client(address: String, auth: VaultAuth) -> VaultClient {
    let guard = GuardedEgress::for_trusted_endpoints(
        EgressGuardConfig::default(),
        Arc::new(rt_sec::egress::SelfDenylist::new(Vec::new(), Vec::new())),
    );
    VaultClient::new(
        VaultSettings {
            address,
            mount: "secret".into(),
            auth,
        },
        Arc::new(guard),
    )
}

fn token_file(dir: &tempfile::TempDir) -> VaultAuth {
    let p = dir.path().join("token");
    std::fs::write(&p, "s.node-token\n").expect("write");
    VaultAuth::TokenFile(p)
}

const OK: &str = r#"{"data":{"data":{"password":"hunter2"},"metadata":{"version":3}}}"#;

#[tokio::test]
async fn reads_a_field_with_the_token_and_its_version() {
    let mock = Arc::new(Mock::default());
    mock.answers
        .lock()
        .insert("/v1/secret/data/rt/a".into(), (200, OK.into(), None));
    let dir = tempfile::tempdir().expect("dir");
    let v = client(serve(mock.clone()).await, token_file(&dir));
    let got = v.read("rt/a", "password").await.map_err(|e| e.code()).expect("read");
    assert_eq!(*got.value, "hunter2");
    assert_eq!(got.version, Some(3));
    assert_eq!(mock.seen.lock()[0].1.as_deref(), Some("s.node-token"));
    assert_eq!(v.read("rt/a", "nope").await.err(), Some(SourceError::NotFound));
    assert_eq!(v.read("rt/b", "password").await.err(), Some(SourceError::NotFound));
}

#[tokio::test]
async fn a_redirect_is_not_followed_so_the_token_stays_home() {
    let mock = Arc::new(Mock::default());
    let other = serve(mock.clone()).await;
    mock.answers.lock().insert(
        "/v1/secret/data/rt/a".into(),
        (307, "{}".into(), Some(format!("{other}/stolen"))),
    );
    let dir = tempfile::tempdir().expect("dir");
    let v = client(other.clone(), token_file(&dir));
    assert_eq!(v.read("rt/a", "password").await.err(), Some(SourceError::Unavailable));
    assert!(!mock.seen.lock().iter().any(|(p, _)| p == "/stolen"));
}

#[tokio::test]
async fn an_outage_and_a_server_error_are_unavailable_with_no_text() {
    let dir = tempfile::tempdir().expect("dir");
    let down = client("http://127.0.0.1:1".into(), token_file(&dir));
    assert_eq!(down.read("rt/a", "f").await.err(), Some(SourceError::Unavailable));
    let mock = Arc::new(Mock::default());
    mock.answers.lock().insert(
        "/v1/secret/data/rt/a".into(),
        (500, r#"{"errors":["secret text"]}"#.into(), None),
    );
    let v = client(serve(mock).await, token_file(&dir));
    assert_eq!(v.read("rt/a", "f").await.err(), Some(SourceError::Unavailable));
}

#[tokio::test]
async fn approle_logs_in_reuses_and_renews_after_a_refusal() {
    let mock = Arc::new(Mock::default());
    let dir = tempfile::tempdir().expect("dir");
    let (r, s) = (dir.path().join("r"), dir.path().join("s"));
    std::fs::write(&r, "role").expect("w");
    std::fs::write(&s, "sec").expect("w");
    mock.answers.lock().insert(
        "/v1/auth/approle/login".into(),
        (
            200,
            r#"{"auth":{"client_token":"t1","lease_duration":3600}}"#.into(),
            None,
        ),
    );
    mock.answers
        .lock()
        .insert("/v1/secret/data/a".into(), (200, OK.into(), None));
    let v = client(
        serve(mock.clone()).await,
        VaultAuth::AppRole {
            role_id_file: r,
            secret_id_file: s,
        },
    );
    v.read("a", "password").await.map_err(|e| e.code()).expect("read");
    v.read("a", "password").await.map_err(|e| e.code()).expect("read");
    assert_eq!(mock.logins.load(Ordering::Relaxed), 1);
    mock.answers
        .lock()
        .insert("/v1/secret/data/a".into(), (403, "{}".into(), None));
    assert_eq!(v.read("a", "password").await.err(), Some(SourceError::Unavailable));
    assert_eq!(mock.logins.load(Ordering::Relaxed), 2);
}
