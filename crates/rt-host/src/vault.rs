//! Vault KV v2 as a [`SecretSource`] (R15), through [`GuardedEgress`]: no redirects, so the token
//! header can never follow one. Nothing here logs; a token and a value live only in memory that is
//! zeroed. A token is read from its file when needed, so it can be rotated in place.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use parking_lot::Mutex;
use rt_sec::secret_source::{SecretSource, SourceError, SourceValue};
use rt_types::Body;
use serde_json::Value;
use tokio::sync::Semaphore;
use url::Url;
use zeroize::Zeroizing;

use crate::egress::{EgressBudget, EgressGuard, EgressPolicy, EgressUsage, GuardedEgress};

/// One request of a pass; a slow Vault only delays the next sync.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Largest answer read.
const MAX_BODY: usize = 1024 * 1024;
/// Largest value, as `PUT /secrets/{name}` allows.
const MAX_VALUE: usize = 32 * 1024;

/// How the node signs in.
#[derive(Clone)]
pub enum VaultAuth {
    TokenFile(PathBuf),
    AppRole {
        role_id_file: PathBuf,
        secret_id_file: PathBuf,
    },
}

/// `[secrets.vault]`, already checked.
pub struct VaultSettings {
    pub address: String,
    pub mount: String,
    pub auth: VaultAuth,
}

pub struct VaultClient {
    settings: VaultSettings,
    guard: Arc<GuardedEgress>,
    login: Mutex<Option<(Zeroizing<String>, Instant)>>,
}

impl VaultClient {
    pub fn new(settings: VaultSettings, guard: Arc<GuardedEgress>) -> Self {
        Self {
            settings,
            guard,
            login: Mutex::new(None),
        }
    }

    fn read_file(path: &PathBuf) -> Result<Zeroizing<String>, SourceError> {
        let text = Zeroizing::new(std::fs::read_to_string(path).map_err(|_| SourceError::Unavailable)?);
        Ok(Zeroizing::new(text.trim().to_string()))
    }

    async fn send(
        &self,
        method: http::Method,
        path: &str,
        token: Option<&str>,
        body: Option<Vec<u8>>,
    ) -> Result<(u16, Value), SourceError> {
        let base = self.settings.address.trim_end_matches('/');
        let url = Url::parse(&format!("{base}/v1/{path}")).map_err(|_| SourceError::Unavailable)?;
        let mut req = http::Request::new(Body::from_bytes(Bytes::from(body.unwrap_or_default())));
        *req.method_mut() = method;
        *req.uri_mut() = url.as_str().parse().map_err(|_| SourceError::Unavailable)?;
        if let Some(t) = token {
            let mut v = http::HeaderValue::from_str(t).map_err(|_| SourceError::Unavailable)?;
            v.set_sensitive(true);
            req.headers_mut().insert("x-vault-token", v);
        }
        let usage = EgressUsage::new(None);
        let mut budget = EgressBudget::shared(1, Arc::new(Semaphore::new(1)), usage, Instant::now() + REQUEST_TIMEOUT);
        let policy = EgressPolicy::default();
        let go = async {
            let resp = self
                .guard
                .send(req, &policy, &mut budget)
                .await
                .map_err(|_| SourceError::Unavailable)?;
            let status = resp.status().as_u16();
            let mut stream = resp.into_body().into_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk.map_err(|_| SourceError::Unavailable)?);
                if bytes.len() > MAX_BODY {
                    return Err(SourceError::Unavailable);
                }
            }
            Ok((status, serde_json::from_slice(&bytes).unwrap_or(Value::Null)))
        };
        tokio::time::timeout(REQUEST_TIMEOUT, go)
            .await
            .map_err(|_| SourceError::Unavailable)?
    }

    async fn token(&self) -> Result<Zeroizing<String>, SourceError> {
        let (role_file, secret_file) = match &self.settings.auth {
            VaultAuth::TokenFile(p) => return Self::read_file(p),
            VaultAuth::AppRole {
                role_id_file,
                secret_id_file,
            } => (role_id_file, secret_id_file),
        };
        if let Some((t, until)) = &*self.login.lock() {
            if Instant::now() < *until {
                return Ok(t.clone());
            }
        }
        let body = Zeroizing::new(
            serde_json::json!({
                "role_id": *Self::read_file(role_file)?,
                "secret_id": *Self::read_file(secret_file)?,
            })
            .to_string(),
        );
        let (status, v) = self
            .send(
                http::Method::POST,
                "auth/approle/login",
                None,
                Some(body.as_bytes().to_vec()),
            )
            .await?;
        let token = v["auth"]["client_token"].as_str().filter(|_| status == 200);
        let Some(token) = token else {
            return Err(SourceError::Unavailable);
        };
        let lease = v["auth"]["lease_duration"].as_u64().unwrap_or(60);
        let token = Zeroizing::new(token.to_string());
        // Log in again well before the lease ends.
        let until = Instant::now() + Duration::from_secs(lease.saturating_mul(4) / 5);
        *self.login.lock() = Some((token.clone(), until));
        Ok(token)
    }
}

fn percent_free(path: &str) -> bool {
    !path.contains('%')
}

#[async_trait]
impl SecretSource for VaultClient {
    async fn read(&self, full_path: &str, field: &str) -> Result<SourceValue, SourceError> {
        if !percent_free(full_path) {
            return Err(SourceError::NotFound);
        }
        let path = format!("{}/data/{full_path}", self.settings.mount);
        let mut retried = false;
        loop {
            let token = self.token().await?;
            let (status, v) = self.send(http::Method::GET, &path, Some(&token), None).await?;
            return match status {
                200 => {
                    let value = match &v["data"]["data"][field] {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        _ => return Err(SourceError::NotFound),
                    };
                    if value.is_empty() || value.len() > MAX_VALUE {
                        return Err(SourceError::NotFound);
                    }
                    Ok(SourceValue {
                        value: Zeroizing::new(value),
                        version: v["data"]["metadata"]["version"].as_i64(),
                    })
                }
                404 => Err(SourceError::NotFound),
                // An expired login: one more try with a fresh one.
                403 if !retried && matches!(self.settings.auth, VaultAuth::AppRole { .. }) => {
                    retried = true;
                    *self.login.lock() = None;
                    continue;
                }
                _ => Err(SourceError::Unavailable),
            };
        }
    }
}
