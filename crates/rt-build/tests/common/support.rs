//! Shared test support for the build pipeline.
//!
//! Included by `rt-build`'s own tests (`mod common`) and, through `#[path]`, by `rt-exec-deno`'s
//! tests: it only uses crates both of them can name (`rt_build`, `rt_host`, `rt_store`, `rt_types`,
//! `rt_mem`, `rt_obs`, `rt_testkit`, `url`, `http`, `tokio`, `zip`, `bytes`, `async_trait`).
//!
//! Contents: fixture loading (`fixtures/deno/<name>`), upload builders (single file, JSON, zip,
//! hand-written tar so malicious entries can be crafted), an in-memory T3 store, a loopback
//! egress shim for `MockCdn`, fake `CodeCacheProducer` / `ComponentPrecompiler`, and build
//! context helpers.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rt_build::{
    BuildCx, BuildRequest, Builder, BuiltArtifact, BuiltFile, Bundle, CodeCache, CodeCacheProducer,
    ComponentPrecompiler, DefaultBuilder, DefaultResolver, FetchError, Fetched, FetcherConfig, GuardedFetcher,
    ModuleFetcher, ResolverConfig, RtLock, SdkCatalog, StaticSdkCatalog, Upload, UploadLimits, Validated,
};
use rt_host::{EgressBudget, EgressError, EgressGuard, EgressPolicy, Request, Response};
use rt_mem::{MemCategory, Reservation};
use rt_obs::MemoryLogSink;
use rt_store::{ArtifactStore, StoreError};
use rt_testkit::MockCdn;
use rt_types::{Body, BuildFailure, Digest, PreparedKey, RuntimeKind};
use url::Url;

// ---------------------------------------------------------------------------------------------
// Fixtures on disk: <workspace>/fixtures/deno/<name>/...
// ---------------------------------------------------------------------------------------------

pub fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn collect_files(dir: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<u8>>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read fixture dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.expect("fixture dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        let rel = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, &rel, out);
        } else {
            out.insert(
                rel,
                std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
            );
        }
    }
}

/// Every file of `fixtures/deno/<name>` as `relative/path -> bytes` (sorted).
pub fn fixture_files(name: &str) -> BTreeMap<String, Vec<u8>> {
    let dir = fixtures_root().join("deno").join(name);
    assert!(dir.is_dir(), "missing fixture directory {}", dir.display());
    let mut out = BTreeMap::new();
    collect_files(&dir, "", &mut out);
    out
}

/// `main.ts` of a single-file fixture.
pub fn fixture_source(name: &str) -> String {
    let files = fixture_files(name);
    String::from_utf8(
        files
            .get("main.ts")
            .unwrap_or_else(|| panic!("fixture {name} has no main.ts"))
            .clone(),
    )
    .expect("fixture is utf-8")
}

/// A fixture as a deploy upload: a lone `main.ts` is `application/typescript`; anything else
/// (extra files, `rt.json`) is a deterministic `application/zip`.
pub fn fixture_upload(name: &str) -> Upload {
    let files = fixture_files(name);
    if files.len() == 1 && files.contains_key("main.ts") {
        return ts_upload(&fixture_source(name));
    }
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();
    zip_upload(&refs)
}

/// The same fixture as a hand-rolled ustar archive.
pub fn fixture_tar_upload(name: &str) -> Upload {
    let files = fixture_files(name);
    let entries: Vec<TarEntry> = files.iter().map(|(k, v)| TarEntry::file(k, v)).collect();
    tar_upload(&entries)
}

// ---------------------------------------------------------------------------------------------
// Uploads
// ---------------------------------------------------------------------------------------------

pub fn upload(content_type: &str, body: impl Into<Bytes>) -> Upload {
    Upload {
        content_type: content_type.to_string(),
        body: body.into(),
    }
}

pub fn ts_upload(src: &str) -> Upload {
    upload("application/typescript", Bytes::from(src.to_string()))
}

pub fn js_upload(src: &str) -> Upload {
    upload("text/javascript", Bytes::from(src.to_string()))
}

/// `application/json` deploy payload `{entry, files, imports?}`.
pub fn json_upload(entry: &str, files: &[(&str, &str)], imports: Option<&[(&str, &str)]>) -> Upload {
    let files: serde_json::Map<String, serde_json::Value> = files
        .iter()
        .map(|(p, c)| (p.to_string(), serde_json::json!(c)))
        .collect();
    let mut v = serde_json::json!({"entry": entry, "files": files});
    if let Some(imports) = imports {
        let m: serde_json::Map<String, serde_json::Value> = imports
            .iter()
            .map(|(k, t)| (k.to_string(), serde_json::json!(t)))
            .collect();
        v["imports"] = serde_json::Value::Object(m);
    }
    upload("application/json", Bytes::from(v.to_string()))
}

fn stored() -> zip::write::SimpleFileOptions {
    zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored)
}

fn deflated() -> zip::write::SimpleFileOptions {
    zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated)
}

/// Deterministic zip (stored, default timestamps). Entry names are written verbatim, so callers
/// may pass `../x.ts` or `/etc/x`.
pub fn zip_bytes(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut zw = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, content) in files {
        zw.start_file(*name, stored()).expect("zip start_file");
        zw.write_all(content).expect("zip write");
    }
    zw.finish().expect("zip finish").into_inner()
}

pub fn zip_upload(files: &[(&str, &[u8])]) -> Upload {
    upload("application/zip", Bytes::from(zip_bytes(files)))
}

/// A zip containing a symlink entry `link -> target` next to a normal `main.ts`.
pub fn zip_with_symlink(link: &str, target: &str) -> Upload {
    let mut zw = zip::ZipWriter::new(Cursor::new(Vec::new()));
    zw.start_file("main.ts", stored()).expect("start");
    zw.write_all(b"export default () => new Response('x');\n")
        .expect("write");
    zw.add_symlink(link, target, stored()).expect("symlink");
    upload(
        "application/zip",
        Bytes::from(zw.finish().expect("finish").into_inner()),
    )
}

/// `files` entries of `each` zero bytes, deflated: tiny on the wire, huge when inflated.
pub fn zip_bomb(files: usize, each: usize) -> Upload {
    let zeros = vec![0u8; each];
    let mut zw = zip::ZipWriter::new(Cursor::new(Vec::new()));
    zw.start_file("main.ts", deflated()).expect("start");
    zw.write_all(b"export default () => new Response('x');\n")
        .expect("write");
    for i in 0..files {
        zw.start_file(format!("pad{i}.json"), deflated()).expect("start");
        zw.write_all(&zeros).expect("write");
    }
    upload(
        "application/zip",
        Bytes::from(zw.finish().expect("finish").into_inner()),
    )
}

/// Deterministic incompressible bytes (xorshift64*), so size limits trip before ratio limits.
pub fn random_bytes(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        out.extend_from_slice(&x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes());
    }
    out.truncate(n);
    out
}

/// A valid JSON document (`["<random alphanumerics>"]`) of exactly `n` bytes (n >= 4): hard to
/// compress, so archive size limits trip before the inflate-ratio limit.
pub fn random_json(n: usize, seed: u64) -> Vec<u8> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    assert!(n >= 4);
    let raw = random_bytes(n - 4, seed);
    let mut out = Vec::with_capacity(n);
    out.extend_from_slice(b"[\"");
    out.extend(raw.iter().map(|b| ALPHABET[*b as usize % ALPHABET.len()]));
    out.extend_from_slice(b"\"]");
    out
}

pub enum TarKind {
    File,
    Dir,
    Symlink(String),
    Hardlink(String),
}

pub struct TarEntry {
    pub name: String,
    pub kind: TarKind,
    pub content: Vec<u8>,
}

impl TarEntry {
    pub fn file(name: &str, content: &[u8]) -> Self {
        Self {
            name: name.to_string(),
            kind: TarKind::File,
            content: content.to_vec(),
        }
    }
    pub fn symlink(name: &str, target: &str) -> Self {
        Self {
            name: name.to_string(),
            kind: TarKind::Symlink(target.to_string()),
            content: Vec::new(),
        }
    }
    pub fn hardlink(name: &str, target: &str) -> Self {
        Self {
            name: name.to_string(),
            kind: TarKind::Hardlink(target.to_string()),
            content: Vec::new(),
        }
    }
    pub fn dir(name: &str) -> Self {
        Self {
            name: name.to_string(),
            kind: TarKind::Dir,
            content: Vec::new(),
        }
    }
}

fn put_octal(field: &mut [u8], value: u64) {
    let width = field.len() - 1;
    let s = format!("{value:0width$o}");
    field[..width].copy_from_slice(s.as_bytes());
    field[width] = 0;
}

fn tar_header(e: &TarEntry) -> [u8; 512] {
    let mut h = [0u8; 512];
    let name = e.name.as_bytes();
    assert!(name.len() <= 100, "tar test helper only supports names up to 100 bytes");
    h[..name.len()].copy_from_slice(name);
    put_octal(&mut h[100..108], 0o644);
    put_octal(&mut h[108..116], 0);
    put_octal(&mut h[116..124], 0);
    let size = if matches!(e.kind, TarKind::File) {
        e.content.len() as u64
    } else {
        0
    };
    put_octal(&mut h[124..136], size);
    put_octal(&mut h[136..148], 0);
    h[148..156].fill(b' ');
    let (flag, link) = match &e.kind {
        TarKind::File => (b'0', ""),
        TarKind::Dir => (b'5', ""),
        TarKind::Symlink(t) => (b'2', t.as_str()),
        TarKind::Hardlink(t) => (b'1', t.as_str()),
    };
    h[156] = flag;
    h[157..157 + link.len()].copy_from_slice(link.as_bytes());
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    let sum: u32 = h.iter().map(|b| u32::from(*b)).sum();
    let chk = format!("{sum:06o}\0 ");
    h[148..156].copy_from_slice(chk.as_bytes());
    h
}

/// Plain ustar bytes. Names are written verbatim (no `..` filtering, unlike the `tar` crate's
/// own builder), so path-traversal archives can be produced.
pub fn tar_bytes(entries: &[TarEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out.extend_from_slice(&tar_header(e));
        if matches!(e.kind, TarKind::File) {
            out.extend_from_slice(&e.content);
            let pad = (512 - e.content.len() % 512) % 512;
            out.extend(std::iter::repeat_n(0u8, pad));
        }
    }
    out.extend(std::iter::repeat_n(0u8, 1024));
    out
}

pub fn tar_upload(entries: &[TarEntry]) -> Upload {
    upload("application/x-tar", Bytes::from(tar_bytes(entries)))
}

/// Valid TypeScript of roughly `target_bytes` bytes (BLD-03: "1 MB valid source").
pub fn big_valid_ts(target_bytes: usize) -> String {
    let mut s = String::with_capacity(target_bytes + 128);
    s.push_str("export default function (_req: Request): Response { return new Response(String(f0(1))); }\n");
    let mut i = 0usize;
    while s.len() < target_bytes {
        s.push_str(&format!(
            "export function f{i}(a: number): number {{ const b: number = a + {i}; return b * 2; }}\n"
        ));
        i += 1;
    }
    s
}

// ---------------------------------------------------------------------------------------------
// In-memory T3
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
pub struct MemArtifacts {
    items: Mutex<HashMap<Digest, Bytes>>,
    vendored: Mutex<HashMap<Digest, std::collections::HashSet<String>>>,
}

impl MemArtifacts {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn len(&self) -> usize {
        self.items.lock().expect("lock").len()
    }
}

#[async_trait]
impl ArtifactStore for MemArtifacts {
    async fn put(&self, bytes: Bytes) -> rt_store::Result<Digest> {
        let d = Digest::of(&bytes);
        self.items.lock().expect("lock").insert(d, bytes);
        Ok(d)
    }
    async fn put_vendored(&self, bytes: Bytes, url: &str) -> rt_store::Result<Digest> {
        let d = self.put(bytes).await?;
        self.vendored
            .lock()
            .expect("lock")
            .entry(d)
            .or_default()
            .insert(url.to_string());
        Ok(d)
    }
    async fn vendored_from(&self, d: &Digest, url: &str) -> rt_store::Result<bool> {
        Ok(self
            .vendored
            .lock()
            .expect("lock")
            .get(d)
            .is_some_and(|u| u.contains(url)))
    }
    async fn get(&self, d: &Digest) -> rt_store::Result<Bytes> {
        self.items
            .lock()
            .expect("lock")
            .get(d)
            .cloned()
            .ok_or(StoreError::NotFound)
    }
    async fn exists(&self, d: &Digest) -> rt_store::Result<bool> {
        Ok(self.items.lock().expect("lock").contains_key(d))
    }
    async fn delete(&self, d: &Digest) -> rt_store::Result<()> {
        self.items.lock().expect("lock").remove(d);
        Ok(())
    }
    async fn size(&self, d: &Digest) -> rt_store::Result<Option<u64>> {
        Ok(self.items.lock().expect("lock").get(d).map(|b| b.len() as u64))
    }
}

// ---------------------------------------------------------------------------------------------
// Egress shim for MockCdn
// ---------------------------------------------------------------------------------------------

fn host_matches(host: &str, list: &[String]) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    list.iter().any(|e| {
        let e = e.trim_end_matches('.').to_ascii_lowercase();
        host == e || host.ends_with(&format!(".{e}"))
    })
}

fn dechunk(mut b: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let eol = b.windows(2).position(|w| w == b"\r\n")?;
        let size = usize::from_str_radix(std::str::from_utf8(&b[..eol]).ok()?.split(';').next()?.trim(), 16).ok()?;
        b = &b[eol + 2..];
        if size == 0 {
            return Some(out);
        }
        out.extend_from_slice(b.get(..size)?);
        b = b.get(size + 2..)?;
    }
}

/// `(status, lower-cased headers, body)` of a raw HTTP/1.1 response.
type ParsedHttp = (u16, Vec<(String, String)>, Vec<u8>);

fn parse_http(raw: &[u8]) -> Option<ParsedHttp> {
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next()?.split_whitespace().nth(1)?.parse().ok()?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let mut body = raw[split + 4..].to_vec();
    if headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked"))
    {
        body = dechunk(&body)?;
    }
    Some((status, headers, body))
}

/// An [`EgressGuard`] that lets guest/build fetches reach **only** the listed loopback addresses
/// (the `MockCdn`) and denies everything else. The production guard blocks `127.0.0.1` by design
/// (SEC-10), so builder and executor tests inject this shim at the same seam
/// (`GuardedFetcher::new(egress, cfg)` / `HostDeps::egress`) instead.
///
/// It honours `EgressPolicy` the way the real guard does: a non-empty `allowed_hosts` is an
/// allowlist, `denied_hosts` always wins, `default_deny` with no allowlist denies everything.
pub struct LoopbackEgress {
    allowed: Vec<SocketAddr>,
    requests: AtomicUsize,
    denied: Mutex<Vec<String>>,
    seen: Mutex<Vec<String>>,
}

impl LoopbackEgress {
    pub fn new(allowed: &[SocketAddr]) -> Arc<Self> {
        Arc::new(Self {
            allowed: allowed.to_vec(),
            requests: AtomicUsize::new(0),
            denied: Mutex::new(Vec::new()),
            seen: Mutex::new(Vec::new()),
        })
    }

    pub fn for_cdn(cdn: &MockCdn) -> Arc<Self> {
        Self::new(&[cdn.addr()])
    }

    /// Requests handed to the guard (allowed or not).
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub fn denied_urls(&self) -> Vec<String> {
        self.denied.lock().expect("lock").clone()
    }

    /// URLs that were actually sent over the wire.
    pub fn sent_urls(&self) -> Vec<String> {
        self.seen.lock().expect("lock").clone()
    }
}

#[async_trait]
impl EgressGuard for LoopbackEgress {
    async fn send(
        &self,
        req: Request,
        policy: &EgressPolicy,
        _budget: &mut EgressBudget,
    ) -> Result<Response, EgressError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let uri = req.uri().clone();
        let url = uri.to_string();
        let deny = || {
            self.denied.lock().expect("lock").push(url.clone());
            EgressError::Denied
        };
        let Some(host) = uri.host() else { return Err(deny()) };
        if uri.scheme_str() != Some("http") {
            return Err(deny());
        }
        if (!policy.allowed_hosts.is_empty() && !host_matches(host, &policy.allowed_hosts))
            || host_matches(host, &policy.denied_hosts)
            || (policy.default_deny && policy.allowed_hosts.is_empty())
        {
            return Err(deny());
        }
        let port = uri.port_u16().unwrap_or(80);
        let Ok(addr) = format!("{host}:{port}").parse::<SocketAddr>() else {
            return Err(deny());
        };
        if !self.allowed.contains(&addr) {
            return Err(deny());
        }
        self.seen.lock().expect("lock").push(url.clone());

        let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
        let exchange = async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
            s.write_all(request.as_bytes()).await.ok()?;
            let mut raw = Vec::new();
            s.read_to_end(&mut raw).await.ok()?;
            parse_http(&raw)
        };
        let parsed = tokio::time::timeout(Duration::from_secs(10), exchange).await;
        let Ok(Some((status, headers, body))) = parsed else {
            return Err(EgressError::Upstream);
        };
        let mut rb = http::Response::builder().status(status);
        for (k, v) in &headers {
            if k == "content-type" {
                rb = rb.header(k.as_str(), v.as_str());
            }
        }
        rb.body(Body::from_bytes(Bytes::from(body)))
            .map_err(|_| EgressError::Upstream)
    }
}

// ---------------------------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------------------------

/// Never answers (BLD-10: a build that exceeds its timeout).
pub struct HangingFetcher;

#[async_trait]
impl ModuleFetcher for HangingFetcher {
    async fn fetch(&self, _url: &Url, _expected: Option<Digest>) -> Result<Fetched, FetchError> {
        std::future::pending::<()>().await;
        unreachable!("pending never resolves")
    }
}

/// Serves fixed bytes for any URL and counts calls (no network at all).
pub struct StaticFetcher {
    pub body: Bytes,
    pub calls: AtomicUsize,
}

impl StaticFetcher {
    pub fn new(body: impl Into<Bytes>) -> Arc<Self> {
        Arc::new(Self {
            body: body.into(),
            calls: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl ModuleFetcher for StaticFetcher {
    async fn fetch(&self, url: &Url, _expected: Option<Digest>) -> Result<Fetched, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Fetched {
            url: url.clone(),
            bytes: self.body.clone(),
            digest: Digest::of(&self.body),
            content_type: Some("application/javascript".into()),
            from_cache: false,
        })
    }
}

/// Fails the test if the build ever tries to fetch.
pub struct NoNetworkFetcher;

#[async_trait]
impl ModuleFetcher for NoNetworkFetcher {
    async fn fetch(&self, url: &Url, _expected: Option<Digest>) -> Result<Fetched, FetchError> {
        panic!("unexpected network fetch of {url}");
    }
}

// ---------------------------------------------------------------------------------------------
// MockCdn content
// ---------------------------------------------------------------------------------------------

pub const ZOD_ENTRY_PATH: &str = "/zod@3";
pub const ZOD_IMPL_PATH: &str = "/zod@3.23.8/es2022/zod.mjs";

/// Serve the esm.sh-style stand-in for `zod@3`: `/zod@3` re-exports `/zod@3.23.8/es2022/zod.mjs`.
pub fn install_zod(cdn: &MockCdn) {
    let dir = fixtures_root().join("deno/_cdn");
    let entry = std::fs::read_to_string(dir.join("zod_entry.mjs")).expect("fixtures/deno/_cdn/zod_entry.mjs");
    let imp = std::fs::read_to_string(dir.join("zod_impl.mjs")).expect("fixtures/deno/_cdn/zod_impl.mjs");
    cdn.set_js(ZOD_ENTRY_PATH, &entry);
    cdn.set_js(ZOD_IMPL_PATH, &imp);
}

pub async fn cdn_with_zod() -> MockCdn {
    let cdn = MockCdn::start().await.expect("start MockCdn");
    install_zod(&cdn);
    cdn
}

// ---------------------------------------------------------------------------------------------
// Fakes for the executor-provided traits
// ---------------------------------------------------------------------------------------------

/// Deterministic stand-in for `V8CodeCacheProducer`.
pub struct FakeCodeCache {
    pub fp: u64,
    pub calls: AtomicUsize,
    pub last_bundle: Mutex<Option<Bundle>>,
}

impl FakeCodeCache {
    pub fn new(fp: u64) -> Arc<Self> {
        Arc::new(Self {
            fp,
            calls: AtomicUsize::new(0),
            last_bundle: Mutex::new(None),
        })
    }
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    pub fn blob_for(specifier: &str) -> Vec<u8> {
        format!("fake-code-cache:{specifier}").into_bytes()
    }
}

impl CodeCacheProducer for FakeCodeCache {
    fn produce(&self, bundle: &Bundle) -> Result<CodeCache, BuildFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_bundle.lock().expect("lock") = Some(bundle.clone());
        Ok(CodeCache {
            engine_fp: self.fp,
            per_module: bundle
                .modules
                .iter()
                .map(|m| (m.specifier.clone(), Self::blob_for(&m.specifier)))
                .collect(),
        })
    }
    fn engine_fp(&self) -> u64 {
        self.fp
    }
}

/// Deterministic stand-in for the wasmtime precompiler: output = `b"CWASM1:" + sha256(input)`.
pub struct FakePrecompiler {
    pub fp: u64,
    pub calls: AtomicUsize,
    pub inputs: Mutex<Vec<Vec<u8>>>,
    pub fail_with: Option<BuildFailure>,
}

impl FakePrecompiler {
    pub fn new(fp: u64) -> Arc<Self> {
        Arc::new(Self {
            fp,
            calls: AtomicUsize::new(0),
            inputs: Mutex::new(Vec::new()),
            fail_with: None,
        })
    }
    pub fn failing(fp: u64, failure: BuildFailure) -> Arc<Self> {
        Arc::new(Self {
            fp,
            calls: AtomicUsize::new(0),
            inputs: Mutex::new(Vec::new()),
            fail_with: Some(failure),
        })
    }
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    pub fn output_for(input: &[u8]) -> Vec<u8> {
        let mut out = b"CWASM1:".to_vec();
        out.extend_from_slice(&Digest::of(input).0);
        out
    }
}

impl ComponentPrecompiler for FakePrecompiler {
    fn precompile(&self, wasm: &[u8]) -> Result<Vec<u8>, BuildFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inputs.lock().expect("lock").push(wasm.to_vec());
        if let Some(f) = &self.fail_with {
            return Err(f.clone());
        }
        Ok(Self::output_for(wasm))
    }
    fn engine_fp(&self) -> u64 {
        self.fp
    }
}

// ---------------------------------------------------------------------------------------------
// Build wiring
// ---------------------------------------------------------------------------------------------

pub const TEST_ENGINE_FP: u64 = 0xE57;

pub fn default_builder() -> DefaultBuilder {
    DefaultBuilder::new(UploadLimits::default())
}

pub fn url(s: &str) -> Url {
    Url::parse(s).unwrap_or_else(|e| panic!("bad url {s}: {e}"))
}

/// Resolver config pointing `npm:`/`jsr:` at `npm_cdn` and allowing `allowed_hosts`.
pub fn resolver_cfg(npm_cdn: &str, allowed_hosts: &[&str]) -> ResolverConfig {
    ResolverConfig {
        npm_cdn: url(npm_cdn),
        allowed_hosts: allowed_hosts.iter().map(|s| s.to_string()).collect(),
        import_map: BTreeMap::new(),
    }
}

/// Resolver config for a local `MockCdn` mirror (BLD-16: mirror is the only allowed host).
pub fn mirror_resolver_cfg(cdn: &MockCdn) -> ResolverConfig {
    resolver_cfg(&cdn.url("/"), &["127.0.0.1"])
}

/// Published operator SDKs: `(name, top-level members)`.
pub fn sdk_catalog(sdks: &[(&str, &[&str])]) -> Arc<dyn SdkCatalog> {
    Arc::new(StaticSdkCatalog(
        sdks.iter()
            .map(|(n, m)| (n.to_string(), m.iter().map(|x| x.to_string()).collect()))
            .collect(),
    ))
}

/// Fetcher over the loopback shim, allowed to reach only `allowed_hosts`.
pub fn guarded_fetcher(egress: Arc<LoopbackEgress>, allowed_hosts: &[&str]) -> Arc<GuardedFetcher> {
    Arc::new(GuardedFetcher::new(egress, fetcher_cfg(allowed_hosts)))
}

pub fn fetcher_cfg(allowed_hosts: &[&str]) -> FetcherConfig {
    FetcherConfig {
        allowed_hosts: allowed_hosts.iter().map(|s| s.to_string()).collect(),
        timeout: Duration::from_secs(5),
        max_module_bytes: 8 * 1024 * 1024,
        max_total_bytes: 50 * 1024 * 1024,
    }
}

/// A build context with the given collaborators and a detached 256 MB reservation.
pub fn build_cx(artifacts: Arc<MemArtifacts>, fetcher: Arc<dyn ModuleFetcher>, resolver: ResolverConfig) -> BuildCx {
    BuildCx {
        artifacts,
        fetcher,
        resolver: Arc::new(DefaultResolver::new(resolver)),
        code_cache: None,
        precompiler: None,
        log: MemoryLogSink::new(),
        sdks: None,
        reservation: Reservation::detached(MemCategory::Other, 256 * 1024 * 1024),
        deadline: Instant::now() + Duration::from_secs(60),
    }
}

/// A context for builds that must not touch the network at all.
pub fn offline_cx(artifacts: Arc<MemArtifacts>) -> BuildCx {
    build_cx(
        artifacts,
        Arc::new(NoNetworkFetcher),
        resolver_cfg("https://esm.sh", &["esm.sh"]),
    )
}

/// Validate `upload` (must pass), store its bytes in T3 and build it.
pub async fn validate_and_build(
    builder: &DefaultBuilder,
    cx: &mut BuildCx,
    upload: &Upload,
    kind: RuntimeKind,
    lock: Option<RtLock>,
    timeout: Duration,
) -> Result<BuiltArtifact, BuildFailure> {
    let validated = builder
        .validate(kind, upload)
        .unwrap_or_else(|d| panic!("validation failed: {d:?}"));
    let digest = cx.artifacts.put(upload.body.clone()).await.expect("store source in T3");
    let _ = validated;
    let req = BuildRequest {
        key: PreparedKey {
            digest,
            kind,
            engine_fp: TEST_ENGINE_FP,
        },
        kind,
        source_digest: digest,
        lock,
        timeout,
    };
    builder.build(&req, cx).await
}

/// Deno build with defaults: no lock, 60 s timeout.
pub async fn build_deno(
    builder: &DefaultBuilder,
    cx: &mut BuildCx,
    upload: &Upload,
) -> Result<BuiltArtifact, BuildFailure> {
    validate_and_build(builder, cx, upload, RuntimeKind::Deno, None, Duration::from_secs(60)).await
}

pub fn built_file<'a>(a: &'a BuiltArtifact, name: &str) -> Option<&'a BuiltFile> {
    a.files.iter().find(|f| f.name == name)
}

pub fn bundle_of(a: &BuiltArtifact) -> Bundle {
    let f = built_file(a, "bundle").expect("artifact has a `bundle` file");
    Bundle::from_bytes(&f.bytes).expect("bundle parses")
}

/// The `Validated` of an upload that must pass validation.
pub fn validated(builder: &DefaultBuilder, upload: &Upload) -> Validated {
    builder
        .validate(RuntimeKind::Deno, upload)
        .unwrap_or_else(|d| panic!("validation failed: {d:?}"))
}
