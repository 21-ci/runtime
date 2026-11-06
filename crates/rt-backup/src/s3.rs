//! A small S3 client: SigV4 over the workspace's `reqwest`, path-style, with multipart upload.
//!
//! Hand-written because `object_store` failed the size gate of the platform plan (R1): its `aws`
//! feature added about 3.6 MB to a release binary, with a second `reqwest` (0.13).
//!
//! The endpoint is operator configuration, and passes the egress rules all the same: TLS (plain
//! HTTP to loopback only), no private, link-local or metadata address, `egress.denied_ports` and
//! `egress.denied_hosts`, no redirects, no proxy. Addresses are checked once per session and the
//! client is pinned to them, so a later DNS answer cannot move it.
//!
//! Errors say what failed and a status code, never a URL, a header or a response body: those could
//! carry credentials or text from the other side.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use url::Url;
use zeroize::Zeroizing;

use crate::{BackupError, Result};

/// Size of an upload part: at most [`PART_CONCURRENCY`] of them are in memory.
pub const PART_BYTES: u64 = 8 << 20;
pub const PART_CONCURRENCY: usize = 2;
/// S3's limit on parts per upload.
const MAX_PARTS: u64 = 10_000;
const ATTEMPTS: u32 = 3;
const SMALL_BODY_MAX: usize = 16 << 20;
const MAX_LIST_PAGES: usize = 1000;

/// Access key and secret. No `Display`, and `Debug` shows neither.
#[derive(Clone)]
pub struct S3Credentials {
    access_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
}

impl S3Credentials {
    pub fn new(access_key: &str, secret_key: &str) -> Self {
        Self {
            access_key: Zeroizing::new(access_key.trim().to_string()),
            secret_key: Zeroizing::new(secret_key.trim().to_string()),
        }
    }
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("S3Credentials(redacted)")
    }
}

/// `[backup.s3]`, resolved, plus the node's egress rules.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub credentials: S3Credentials,
    pub part_bytes: u64,
    pub denied_ports: Vec<u16>,
    pub denied_hosts: Vec<String>,
}

impl S3Config {
    pub fn new(endpoint: &str, bucket: &str, prefix: &str, region: &str, credentials: S3Credentials) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            region: region.to_string(),
            credentials,
            part_bytes: PART_BYTES,
            denied_ports: Vec::new(),
            denied_hosts: Vec::new(),
        }
    }
}

fn refused(why: &str) -> BackupError {
    BackupError::new("endpoint_refused", format!("backup.s3.endpoint: {why}"))
}

fn unreachable_err(_: reqwest::Error) -> BackupError {
    BackupError::new("s3_error", "the S3 endpoint could not be reached")
}

fn status_err(what: &str, status: StatusCode) -> BackupError {
    BackupError::new("s3_error", format!("{what}: S3 answered {}", status.as_u16()))
}

fn canon(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

/// What the egress rules say of the addresses of one endpoint. All of them must pass.
fn check_addrs(addrs: &[IpAddr], https: bool) -> Result<()> {
    if addrs.is_empty() {
        return Err(refused("the host does not resolve"));
    }
    let loopback = addrs.iter().all(|ip| canon(*ip).is_loopback());
    if !https && !loopback {
        return Err(refused("plain http is for loopback only"));
    }
    if !loopback && addrs.iter().any(|ip| rt_sec::is_blocked_ip(canon(*ip))) {
        return Err(refused("the host resolves to a private or reserved address"));
    }
    Ok(())
}

/// A validated S3 destination. Connections come from a [`S3Session`].
pub struct S3Client {
    cfg: S3Config,
    url: Url,
    host_header: String,
    prefix: String,
}

impl fmt::Debug for S3Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Client").field("bucket", &self.cfg.bucket).finish()
    }
}

impl S3Client {
    pub fn new(cfg: S3Config) -> Result<Self> {
        let url = Url::parse(&cfg.endpoint).map_err(|_| refused("not a URL"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(refused("the scheme must be https"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(refused("credentials do not belong in the URL"));
        }
        if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err(refused("give the origin only: scheme, host and port"));
        }
        let host = url.host_str().ok_or_else(|| refused("no host"))?;
        let host_header = match url.port() {
            Some(p) => format!("{host}:{p}"),
            None => host.to_string(),
        };
        let bucket_ok = (3..=63).contains(&cfg.bucket.len())
            && cfg
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
        if !bucket_ok {
            return Err(BackupError::new("bad_config", "backup.s3.bucket is not a bucket name"));
        }
        let prefix_ok = cfg.prefix.split('/').all(|seg| seg != "." && seg != "..")
            && cfg
                .prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'));
        if !prefix_ok {
            return Err(BackupError::new("bad_config", "backup.s3.prefix has unsafe characters"));
        }
        let trimmed = cfg.prefix.trim_matches('/');
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };
        Ok(Self {
            url,
            host_header,
            prefix,
            cfg,
        })
    }

    pub fn endpoint_host(&self) -> &str {
        self.url.host_str().unwrap_or("")
    }

    pub fn bucket(&self) -> &str {
        &self.cfg.bucket
    }

    /// The object key of `name`: the configured prefix and that.
    pub fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Check the endpoint against the egress rules now, and pin a client to the answer.
    pub async fn session(&self) -> Result<S3Session<'_>> {
        let host = self.url.host_str().ok_or_else(|| refused("no host"))?;
        let port = self.url.port_or_known_default().unwrap_or(443);
        if self.cfg.denied_ports.contains(&port) {
            return Err(refused("the port is in egress.denied_ports"));
        }
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        if rt_sec::host_denied(bare, &self.cfg.denied_hosts) {
            return Err(refused("the host is in egress.denied_hosts"));
        }
        let https = self.url.scheme() == "https";
        let literal = bare.parse::<IpAddr>().ok();
        let addrs: Vec<SocketAddr> = match literal {
            Some(ip) => vec![SocketAddr::new(ip, port)],
            None => tokio::time::timeout(Duration::from_secs(10), tokio::net::lookup_host((bare, port)))
                .await
                .map_err(|_| refused("the host did not resolve in time"))?
                .map_err(|_| refused("the host does not resolve"))?
                .collect(),
        };
        let ips: Vec<IpAddr> = addrs.iter().map(SocketAddr::ip).collect();
        check_addrs(&ips, https)?;
        let mut b = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .https_only(https);
        if literal.is_none() {
            b = b.resolve_to_addrs(bare, &addrs);
        }
        let http = b.build().map_err(unreachable_err)?;
        Ok(S3Session { s3: self, http })
    }
}

/// One run's connections, pinned to addresses that passed the rules.
pub struct S3Session<'a> {
    s3: &'a S3Client,
    http: reqwest::Client,
}

struct Signer {
    access_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
    region: String,
    host: String,
    base: Url,
    bucket: String,
}

impl<'a> S3Session<'a> {
    fn signer(&self) -> Arc<Signer> {
        let c = &self.s3.cfg;
        Arc::new(Signer {
            access_key: c.credentials.access_key.clone(),
            secret_key: c.credentials.secret_key.clone(),
            region: c.region.clone(),
            host: self.s3.host_header.clone(),
            base: self.s3.url.clone(),
            bucket: c.bucket.clone(),
        })
    }

    async fn call(&self, method: Method, key: &str, query: &[(&str, &str)], body: Bytes) -> Result<reqwest::Response> {
        send(&self.http, &self.signer(), method, key, query, body).await
    }

    /// One object from memory (sidecars). Retried.
    pub async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        let signer = self.signer();
        retry("upload of a small object", || {
            send(&self.http, &signer, Method::PUT, key, &[], body.clone())
        })
        .await
        .map(|_| ())
    }

    /// `None` when the object does not exist.
    pub async fn head(&self, key: &str) -> Result<Option<u64>> {
        let r = self.call(Method::HEAD, key, &[], Bytes::new()).await?;
        match r.status() {
            StatusCode::NOT_FOUND => Ok(None),
            s if s.is_success() => {
                let size = r
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| BackupError::new("s3_error", "HEAD did not give a size"))?;
                Ok(Some(size))
            }
            s => Err(status_err("HEAD", s)),
        }
    }

    /// A small object, into memory.
    pub async fn get_small(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let r = self.call(Method::GET, key, &[], Bytes::new()).await?;
        match r.status() {
            StatusCode::NOT_FOUND => Ok(None),
            s if s.is_success() => Ok(Some(read_capped(r).await?)),
            s => Err(status_err("GET", s)),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let r = self.call(Method::DELETE, key, &[], Bytes::new()).await?;
        match r.status() {
            s if s.is_success() || s == StatusCode::NOT_FOUND => Ok(()),
            s => Err(status_err("DELETE", s)),
        }
    }

    /// Every key under `prefix`.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let mut q = vec![("list-type", "2"), ("prefix", prefix)];
            if let Some(t) = &token {
                q.push(("continuation-token", t));
            }
            let r = self.call(Method::GET, "", &q, Bytes::new()).await?;
            if !r.status().is_success() {
                return Err(status_err("LIST", r.status()));
            }
            let body = read_capped(r).await?;
            let text = String::from_utf8_lossy(&body);
            keys.extend(all_tags(&text, "Key").into_iter().map(unescape));
            match first_tag(&text, "NextContinuationToken") {
                Some(t) => token = Some(unescape(t)),
                None => return Ok(keys),
            }
        }
        Err(BackupError::new("s3_error", "the listing did not end"))
    }

    /// Upload the file as one object: parts of `part_bytes`, two at a time, each retried. A failed
    /// upload is aborted, and shows no object.
    pub async fn upload_file(&self, key: &str, path: &Path) -> Result<u64> {
        let file = Arc::new(std::fs::File::open(path)?);
        let size = file.metadata()?.len();
        let part = self.s3.cfg.part_bytes.max(size.div_ceil(MAX_PARTS)).max(1);
        let r = self.call(Method::POST, key, &[("uploads", "")], Bytes::new()).await?;
        if !r.status().is_success() {
            return Err(status_err("starting the upload", r.status()));
        }
        let text = String::from_utf8_lossy(&read_capped(r).await?).into_owned();
        let upload_id = first_tag(&text, "UploadId")
            .map(unescape)
            .ok_or_else(|| BackupError::new("s3_error", "S3 gave no upload id"))?;
        let result = self.send_parts(key, &upload_id, &file, size, part).await;
        let finished = match result {
            Ok(xml) => {
                let q = [("uploadId", upload_id.as_str())];
                match self.call(Method::POST, key, &q, Bytes::from(xml)).await {
                    Ok(r) if r.status().is_success() => {
                        // S3 can answer 200 with an error in the body.
                        match read_capped(r).await {
                            Ok(b) if !String::from_utf8_lossy(&b).contains("<Error>") => Ok(()),
                            _ => Err(BackupError::new("s3_error", "S3 did not complete the upload")),
                        }
                    }
                    Ok(r) => Err(status_err("completing the upload", r.status())),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };
        if let Err(e) = finished {
            let q = [("uploadId", upload_id.as_str())];
            let _ = self.call(Method::DELETE, key, &q, Bytes::new()).await;
            return Err(e);
        }
        Ok(size)
    }

    async fn send_parts(
        &self,
        key: &str,
        upload_id: &str,
        file: &Arc<std::fs::File>,
        size: u64,
        part: u64,
    ) -> Result<String> {
        let parts = size.div_ceil(part).max(1);
        let mut tasks: JoinSet<Result<(u64, String)>> = JoinSet::new();
        let mut etags: Vec<(u64, String)> = Vec::with_capacity(parts as usize);
        let signer = self.signer();
        for n in 1..=parts {
            // Reads the next part only when a slot is free: at most two parts are in memory.
            while tasks.len() >= PART_CONCURRENCY {
                collect(&mut tasks, &mut etags).await?;
            }
            let offset = (n - 1) * part;
            let len = part.min(size - offset);
            let mut buf = vec![0u8; len as usize];
            file.read_exact_at(&mut buf, offset)?;
            let body = Bytes::from(buf);
            let (http, signer, key, id) = (
                self.http.clone(),
                signer.clone(),
                key.to_string(),
                upload_id.to_string(),
            );
            tasks.spawn(async move {
                let number = n.to_string();
                let q = [("partNumber", number.as_str()), ("uploadId", id.as_str())];
                let r = retry("an upload part", || {
                    send(&http, &signer, Method::PUT, &key, &q, body.clone())
                })
                .await?;
                let etag = r
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| BackupError::new("s3_error", "S3 gave no ETag for a part"))?
                    .to_string();
                Ok((n, etag))
            });
        }
        while !tasks.is_empty() {
            collect(&mut tasks, &mut etags).await?;
        }
        etags.sort_by_key(|(n, _)| *n);
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in etags {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{}</ETag></Part>",
                escape(&etag)
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        Ok(xml)
    }
}

async fn collect(tasks: &mut JoinSet<Result<(u64, String)>>, etags: &mut Vec<(u64, String)>) -> Result<()> {
    match tasks.join_next().await {
        Some(Ok(done)) => {
            etags.push(done?);
            Ok(())
        }
        Some(Err(_)) => Err(BackupError::new("internal", "an upload task did not finish")),
        None => Ok(()),
    }
}

async fn retry<F, Fut>(what: &str, mut op: F) -> Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response>>,
{
    let mut last = BackupError::new("s3_error", "no attempt was made");
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
        match op().await {
            Ok(r) if r.status().is_success() => return Ok(r),
            Ok(r) => {
                last = status_err(what, r.status());
                // A refusal stays one: credentials and policy do not fix themselves.
                if r.status().is_client_error() && r.status() != StatusCode::TOO_MANY_REQUESTS {
                    return Err(last);
                }
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

async fn read_capped(mut r: reqwest::Response) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = r.chunk().await.map_err(unreachable_err)? {
        if out.len() + chunk.len() > SMALL_BODY_MAX {
            return Err(BackupError::new("s3_error", "the S3 answer is too large"));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

async fn send(
    http: &reqwest::Client,
    s: &Signer,
    method: Method,
    key: &str,
    query: &[(&str, &str)],
    body: Bytes,
) -> Result<reqwest::Response> {
    let path = if key.is_empty() {
        format!("/{}", s.bucket)
    } else {
        format!("/{}/{}", s.bucket, encode(key, false))
    };
    let qs = canonical_query(query);
    let payload_hash = hex::encode(Sha256::digest(&body));
    let now = chrono::Utc::now();
    let stamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let authorization = authorization(
        &s.access_key,
        &s.secret_key,
        &s.region,
        method.as_str(),
        &path,
        &qs,
        &[("host", &s.host)],
        &stamp,
        &payload_hash,
    );
    let mut url = s.base.clone();
    url.set_path(&path);
    url.set_query((!qs.is_empty()).then_some(qs.as_str()));
    http.request(method, url)
        .header("x-amz-date", stamp)
        .header("x-amz-content-sha256", payload_hash)
        .header(reqwest::header::AUTHORIZATION, authorization.as_str())
        .body(body)
        .send()
        .await
        .map_err(unreachable_err)
}

fn encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query.iter().map(|(k, v)| (encode(k, true), encode(v, true))).collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    #[allow(clippy::expect_used, reason = "HMAC takes a key of any length")]
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// The `Authorization` value of AWS Signature Version 4. `extra` holds the headers to sign besides
/// `x-amz-content-sha256` and `x-amz-date`, with lowercase names.
#[allow(clippy::too_many_arguments)]
fn authorization(
    access_key: &str,
    secret_key: &str,
    region: &str,
    method: &str,
    path: &str,
    canonical_query: &str,
    extra: &[(&str, &str)],
    stamp: &str,
    payload_hash: &str,
) -> Zeroizing<String> {
    let date = stamp.get(..8).unwrap_or("");
    let mut headers: Vec<(&str, &str)> = extra.to_vec();
    headers.push(("x-amz-content-sha256", payload_hash));
    headers.push(("x-amz-date", stamp));
    headers.sort();
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();
    let signed: String = headers.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
    let request = format!("{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(request.as_bytes()))
    );
    let seed = Zeroizing::new(format!("AWS4{secret_key}"));
    let k = Zeroizing::new(hmac(seed.as_bytes(), date.as_bytes()));
    let k = Zeroizing::new(hmac(&k, region.as_bytes()));
    let k = Zeroizing::new(hmac(&k, b"s3"));
    let k = Zeroizing::new(hmac(&k, b"aws4_request"));
    let signature = hex::encode(hmac(&k, to_sign.as_bytes()));
    Zeroizing::new(format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed},Signature={signature}"
    ))
}

fn first_tag<'t>(text: &'t str, tag: &str) -> Option<&'t str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let start = text.find(&open)? + open.len();
    let end = text[start..].find(&close)? + start;
    Some(&text[start..end])
}

fn all_tags<'t>(mut text: &'t str, tag: &str) -> Vec<&'t str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    while let Some(i) = text.find(&open) {
        let rest = &text[i + open.len()..];
        let Some(j) = rest.find(&close) else { break };
        out.push(&rest[..j]);
        text = &rest[j + close.len()..];
    }
    out
}

fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const HOST: &str = "examplebucket.s3.amazonaws.com";

    /// The worked examples of AWS's "Signature Calculations for the Authorization Header".
    #[test]
    fn signatures_match_the_aws_examples() {
        let get = authorization(
            ACCESS,
            SECRET,
            "us-east-1",
            "GET",
            "/test.txt",
            "",
            &[("host", HOST), ("range", "bytes=0-9")],
            "20130524T000000Z",
            EMPTY,
        );
        assert!(
            get.ends_with("Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            "{}",
            *get
        );
        assert!(get.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
        let list = authorization(
            ACCESS,
            SECRET,
            "us-east-1",
            "GET",
            "/",
            &canonical_query(&[("max-keys", "2"), ("prefix", "J")]),
            &[("host", HOST)],
            "20130524T000000Z",
            EMPTY,
        );
        assert!(
            list.ends_with("Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"),
            "{}",
            *list
        );
    }

    #[test]
    fn names_and_queries_encode_as_sigv4_wants() {
        assert_eq!(encode("a b/c~d", false), "a%20b/c~d");
        assert_eq!(encode("a/b", true), "a%2Fb");
        assert_eq!(
            canonical_query(&[("uploadId", "x/y"), ("partNumber", "1")]),
            "partNumber=1&uploadId=x%2Fy"
        );
        assert_eq!(canonical_query(&[("uploads", "")]), "uploads=");
    }

    #[test]
    fn the_address_rules() {
        let ip = |s: &str| s.parse::<IpAddr>().expect("ip");
        assert!(check_addrs(&[ip("127.0.0.1")], false).is_ok());
        assert!(check_addrs(&[ip("::1")], true).is_ok());
        assert!(check_addrs(&[ip("::ffff:127.0.0.1")], false).is_ok());
        assert!(check_addrs(&[ip("93.184.216.34")], true).is_ok());
        assert!(check_addrs(&[ip("93.184.216.34")], false).is_err());
        assert!(check_addrs(&[ip("10.0.0.1")], true).is_err());
        assert!(check_addrs(&[ip("::ffff:10.0.0.1")], true).is_err());
        assert!(check_addrs(&[ip("169.254.169.254")], true).is_err());
        assert!(
            check_addrs(&[ip("93.184.216.34"), ip("10.0.0.1")], true).is_err(),
            "mixed"
        );
        assert!(
            check_addrs(&[ip("127.0.0.1"), ip("93.184.216.34")], false).is_err(),
            "mixed"
        );
        assert!(check_addrs(&[], true).is_err());
    }

    #[test]
    fn tags_are_found_and_unescaped() {
        let x = "<R><Key>a&amp;b</Key><Key>c</Key><Next>t</Next></R>";
        assert_eq!(all_tags(x, "Key"), ["a&amp;b", "c"]);
        assert_eq!(first_tag(x, "Next"), Some("t"));
        assert_eq!(first_tag(x, "None"), None);
        assert_eq!(unescape("a&amp;b"), "a&b");
        assert_eq!(escape("\"e\""), "&quot;e&quot;");
    }
}
