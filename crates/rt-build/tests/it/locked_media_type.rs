//! A rebuild from `rt.lock` compiles each locked module as the media type the lock recorded, not
//! whatever the network or a cache reports now (review finding 10); a locked module with no
//! recorded type and no extension builds as JavaScript instead of failing as `Unknown`.

use crate::common;

use async_trait::async_trait;
use bytes::Bytes;
use common::*;
use rt_build::{FetchError, Fetched, ModuleFetcher, RtLock};
use rt_types::{Digest, RuntimeKind};
use url::Url;

/// Serves `body` for any URL with the given `Content-Type` (or none, like a vendor-store hit).
struct TypedFetcher {
    body: Bytes,
    content_type: Option<&'static str>,
}

#[async_trait]
impl ModuleFetcher for TypedFetcher {
    async fn fetch(&self, url: &Url, _expected: Option<Digest>) -> Result<Fetched, FetchError> {
        Ok(Fetched {
            url: url.clone(),
            bytes: self.body.clone(),
            digest: Digest::of(&self.body),
            content_type: self.content_type.map(str::to_string),
            from_cache: true,
        })
    }
}

const DEP: &str = "https://esm.sh/dep";

/// Rebuild from a lock pinning `dep`; returns the bundled source of the dependency.
async fn rebuild(dep: &'static [u8], served_as: Option<&'static str>, locked_as: Option<&str>) -> String {
    let builder = default_builder();
    let upload = ts_upload(&format!(
        "import d from \"{DEP}\";\nexport default () => new Response(String(d));\n"
    ));
    let fetcher = std::sync::Arc::new(TypedFetcher {
        body: Bytes::from_static(dep),
        content_type: served_as,
    });
    let mut cx = build_cx(
        MemArtifacts::new(),
        fetcher,
        resolver_cfg("https://esm.sh/", &["esm.sh"]),
    );
    let mut lock = RtLock::default();
    lock.record_typed(DEP, Digest::of(dep), dep.len() as u64, locked_as.map(str::to_string));
    let built = validate_and_build(
        &builder,
        &mut cx,
        &upload,
        RuntimeKind::Deno,
        Some(lock),
        std::time::Duration::from_secs(60),
    )
    .await
    .unwrap_or_else(|f| panic!("rebuild failed: {f}"));
    let bundle = bundle_of(&built);
    bundle
        .modules
        .iter()
        .find(|m| m.specifier == DEP)
        .map(|m| m.source.clone())
        .expect("the dependency is bundled")
}

#[tokio::test]
async fn a_locked_module_builds_as_the_type_the_lock_recorded() {
    // TypeScript locked as TypeScript, now served (or cached) as JavaScript: still TypeScript.
    let ts: &'static [u8] = b"const n: number = 1;\nexport default n;\n";
    let out = rebuild(
        ts,
        Some("application/javascript; charset=utf-8"),
        Some("application/typescript"),
    )
    .await;
    assert!(
        !out.contains(": number"),
        "the locked TypeScript was transpiled, not shipped as JavaScript: {out}"
    );
}

#[tokio::test]
async fn an_untyped_extensionless_locked_module_builds_as_javascript() {
    let js: &'static [u8] = b"export default 1;\n";
    let out = rebuild(js, None, None).await;
    assert!(
        out.contains("export default 1") || out.contains("default"),
        "bundled as JavaScript: {out}"
    );
}
