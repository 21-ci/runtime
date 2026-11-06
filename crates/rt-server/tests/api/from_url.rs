//! `POST …/versions/from-url` and `activate: "after_build"` (R4): the fetch obeys the guard and the
//! service's rules, its credentials go nowhere, and a build-done version goes live through
//! `promote()` with its migrations.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use reqwest::Method;
use rt_api::{ArchiveFetchError, ArchiveFetcher, ArchiveRequest};
use rt_host::{EgressGuardConfig, GuardedEgress, Resolver};
use rt_sec::EgressDenylist;
use rt_server::archive_fetch::{archive_guard_config, GuardedArchiveFetcher};
use rt_server::wiring::StartOptions;
use rt_testkit::NodeHarness;
use serde_json::{json, Value};

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

const SECRET: &str = "s3cr3t-signature";
const TOKEN: &str = "glpat-t0ken";
const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

const CODE: &str = r#"import { sql } from "std:sql";
export default {
  async fetch(): Promise<Response> {
    const r = await sql.execute("SELECT name FROM sqlite_master WHERE name = 'from_git'");
    return Response.json({ tag: "TAG", tables: r.rows.length });
  },
};"#;

/// What the mock origin saw of one request.
#[derive(Clone, Debug)]
struct Seen {
    host: String,
    path_and_query: String,
    headers: HeaderMap,
}

#[derive(Clone)]
enum Reply {
    Bytes(Vec<u8>),
    Redirect(String),
    Status(u16),
}

#[derive(Clone, Default)]
struct Origin {
    delay_ms: Arc<std::sync::atomic::AtomicU64>,
    routes: Arc<Mutex<HashMap<String, Reply>>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    port: Arc<Mutex<u16>>,
}

impl Origin {
    async fn start() -> Self {
        let me = Origin::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        *me.port.lock().unwrap() = listener.local_addr().expect("addr").port();
        let state = me.clone();
        let app = axum::Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
            let state = state.clone();
            async move {
                let ms = state.delay_ms.load(std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                state.answer(uri, headers)
            }
        });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        me
    }

    fn answer(&self, uri: Uri, headers: HeaderMap) -> Response {
        self.seen.lock().unwrap().push(Seen {
            host: headers
                .get(header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string(),
            path_and_query: uri.path_and_query().map(ToString::to_string).unwrap_or_default(),
            headers,
        });
        match self.routes.lock().unwrap().get(uri.path()).cloned() {
            Some(Reply::Bytes(b)) => b.into_response(),
            Some(Reply::Redirect(to)) => (StatusCode::FOUND, [(header::LOCATION, to)]).into_response(),
            Some(Reply::Status(s)) => StatusCode::from_u16(s).expect("status").into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn route(&self, path: &str, reply: Reply) {
        self.routes.lock().unwrap().insert(path.to_string(), reply);
    }

    fn url(&self, host: &str, path: &str) -> String {
        format!("http://{host}:{}{path}", *self.port.lock().unwrap())
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

/// `*.public.test` is a public address; `internal.test` a private one. Everything dials the mock.
struct TestDns;

#[async_trait]
impl Resolver for TestDns {
    async fn resolve(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        let ip = if host.ends_with("public.test") {
            "93.184.216.34"
        } else {
            "10.1.2.3"
        };
        Ok(vec![ip.parse().expect("ip")])
    }
}

struct NoDeny;

impl EgressDenylist for NoDeny {
    fn contains(&self, _ip: IpAddr) -> bool {
        false
    }
    fn refresh(&self) {}
}

/// The real fetcher and guard with archive settings, minus TLS: the mock origin speaks plain HTTP.
fn test_fetcher(dir: &std::path::Path) -> Arc<dyn ArchiveFetcher> {
    let cfg = EgressGuardConfig {
        require_https: false,
        ..archive_guard_config(&EgressGuardConfig::default())
    };
    let guard = GuardedEgress::new(cfg, Arc::new(NoDeny))
        .with_resolver(Arc::new(TestDns))
        .with_connect_rewrite(Arc::new(|_| "127.0.0.1".parse().expect("ip")));
    Arc::new(GuardedArchiveFetcher::new(Arc::new(guard), dir.to_path_buf()))
}

struct Rig {
    node: NodeHarness,
    origin: Origin,
    key: String,
    tenant: String,
    id: String,
    slug: String,
    _tmp: tempfile::TempDir,
}

async fn rig(prefix: &str) -> Rig {
    let origin = Origin::start().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let opts = StartOptions {
        archive_fetcher: Some(test_fetcher(tmp.path())),
        ..StartOptions::default()
    };
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let slug = format!("{prefix}-svc");
    let id = create_service(&node, &t.key, &slug, "deno").await;
    Rig {
        node,
        origin,
        key: t.key,
        tenant: t.id,
        id,
        slug,
        _tmp: tmp,
    }
}

/// A gzipped tar laid out as a provider's: one top directory, `apps/x` holding the service.
fn tarball(tag: &str, migrations: &[(&str, &str)]) -> Vec<u8> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    files.insert("repo-abc/README.md".into(), b"outside the subdir".to_vec());
    files.insert("repo-abc/apps/x/main.ts".into(), CODE.replace("TAG", tag).into_bytes());
    for (name, sql) in migrations {
        files.insert(format!("repo-abc/apps/x/migrations/{name}"), sql.as_bytes().to_vec());
    }
    let mut tar = tar::Builder::new(Vec::new());
    for (path, body) in &files {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append_data(&mut h, path, body.as_slice()).expect("append");
    }
    let raw = tar.into_inner().expect("tar");
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, &raw).expect("gz");
    gz.finish().expect("gz")
}

/// 8 MiB of zeros per entry, far past the inflate ratio.
fn zip_bomb() -> Vec<u8> {
    let mut out = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let opts = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    out.start_file("main.ts", opts).expect("start");
    std::io::Write::write_all(&mut out, &vec![0u8; 8 * 1024 * 1024]).expect("write");
    out.finish().expect("zip").into_inner()
}

async fn from_url(r: &Rig, body: Value) -> Resp {
    post(
        &r.node,
        &r.key,
        &format!("/v1/services/{}/versions/from-url", r.id),
        body,
    )
    .await
}

async fn from_url_as(r: &Rig, key: &str, svc: &str, body: Value) -> Resp {
    post(&r.node, key, &format!("/v1/services/{svc}/versions/from-url"), body).await
}

fn code(r: &Resp) -> (u16, Option<&str>) {
    (r.s(), r.json["error"]["code"].as_str())
}

async fn version_count(r: &Rig) -> usize {
    let list = get(&r.node, &r.key, &format!("/v1/services/{}/versions", r.id)).await;
    list.json.as_array().expect("versions").len()
}

async fn events(r: &Rig) -> Vec<Value> {
    let page = get(&r.node, &r.key, &format!("/v1/services/{}/events", r.id)).await;
    assert_eq!(page.s(), 200, "{}", page.text);
    page.json["items"]
        .as_array()
        .or(page.json.as_array())
        .cloned()
        .unwrap_or_default()
}

fn kind_of(ev: &Value) -> &str {
    ev["kind"].as_str().or(ev["type"].as_str()).unwrap_or_default()
}

async fn wait_event(r: &Rig, kind: &str) -> Value {
    for _ in 0..1200 {
        if let Some(ev) = events(r).await.into_iter().find(|e| kind_of(e) == kind) {
            return ev;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("no `{kind}` event: {:?}", events(r).await);
}

async fn live_version(r: &Rig) -> Option<u64> {
    get(&r.node, &r.key, &format!("/v1/services/{}", r.id)).await.json["live_version"].as_u64()
}

node_test! {
    /// M6: a slow origin costs its own tenant its fetch slots, nobody else's. L12: userinfo is never
    /// silently dropped.
    async fn a_tenant_has_two_fetches_at_a_time_and_userinfo_is_refused() {
        let r = rig("m6").await;
        r.origin.route("/a.tgz", Reply::Bytes(tarball("a", &[])));
        r.origin.delay_ms.store(3000, std::sync::atomic::Ordering::SeqCst);
        let url = r.origin.url("o.public.test", "/a.tgz");
        let body = json!({"url": url, "strip_components": 1, "subdir": "apps/x"});
        assert_eq!(from_url(&r, body.clone()).await.s(), 202);
        assert_eq!(from_url(&r, body.clone()).await.s(), 202);
        let third = from_url(&r, body.clone()).await;
        assert_eq!(code(&third), (429, Some("rate_limited")), "{}", third.text);

        let other = new_tenant(&r.node, "pro").await;
        let svc = create_service(&r.node, &other.key, "m6-other", "deno").await;
        for _ in 0..2 {
            let up = from_url_as(&r, &other.key, &svc, body.clone()).await;
            assert_eq!(up.s(), 202, "another tenant has its own slots: {}", up.text);
        }
        let creds = from_url(&r, json!({"url": r.origin.url("me:pw@o.public.test", "/a.tgz")})).await;
        assert_eq!(code(&creds), (422, Some("validation_failed")), "{}", creds.text);
        assert!(!creds.text.contains("pw@"), "{}", creds.text);
        r.node.shutdown().await;
    }
}

node_test! {
    /// One deploy is one build token: the check before the `202` is the only charge.
    async fn a_deploy_from_a_url_takes_one_build_token() {
        let origin = Origin::start().await;
        let tmp = tempfile::tempdir().expect("tmp");
        let opts = StartOptions {
            archive_fetcher: Some(test_fetcher(tmp.path())),
            ..StartOptions::default()
        };
        let node = start_node_opts(opts, None, |c| {
            let mut p = pro_plan();
            p.builds_per_hour = 2;
            c.plans.insert("pro".to_string(), p);
        })
        .await;
        let t = new_tenant(&node, "pro").await;
        let id = create_service(&node, &t.key, "one-token", "deno").await;
        let r = Rig {
            node,
            origin,
            key: t.key,
            tenant: t.id,
            id,
            slug: "one-token".to_string(),
            _tmp: tmp,
        };
        r.origin.route("/a.tgz", Reply::Bytes(tarball("a", &[])));
        let body = json!({"url": r.origin.url("o.public.test", "/a.tgz"), "strip_components": 1, "subdir": "apps/x"});
        for deploy in 1..=2u64 {
            let up = from_url(&r, body.clone()).await;
            assert_eq!(up.s(), 202, "deploy {deploy}: {}", up.text);
            let path = format!("/v1/services/{}/versions/{}", r.id, up.json["version"]);
            eventually(std::time::Duration::from_secs(60), "the deploy to leave `fetching`", || {
                let path = path.clone();
                let r = &r;
                async move {
                    let v = get(&r.node, &r.key, &path).await.json;
                    v["build_state"] != json!("fetching") && v["build_state"] != json!("queued")
                }
            })
            .await;
            let v = get(&r.node, &r.key, &path).await.json;
            assert_ne!(v["failure"]["code"], json!("build_quota"), "deploy {deploy}: {v}");
        }
        let third = from_url(&r, body).await;
        assert_eq!(code(&third), (429, Some("build_quota")), "{}", third.text);
        r.node.shutdown().await;
    }
}

node_test! {
    /// The whole path: signed URL, strip + subdir, migrations, promoted from the build-done path,
    /// SHA visible on the version and in the switch audit; the credential is nowhere.
    async fn a_tarball_with_migrations_goes_live_after_its_build() {
        let r = rig("fu-ok").await;
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("v1", &[("0001_a.sql", "CREATE TABLE from_git (x);")])));
        let url = format!("{}?token={SECRET}", r.origin.url("git.public.test", "/repo.tar.gz"));
        let up = from_url(&r, json!({
            "url": url,
            "headers": {"PRIVATE-TOKEN": TOKEN},
            "strip_components": 1,
            "subdir": "apps/x",
            "activate": "after_build",
            "meta": {"git": {"sha": SHA, "ref": "refs/heads/main"}},
        })).await;
        assert_eq!(up.s(), 202, "{}", up.text);
        assert_eq!(up.json["build_state"], "fetching");
        assert_eq!(up.json["source_digest"], Value::Null);
        assert_eq!(up.json["meta"]["git"]["sha"], SHA);

        let promoted = wait_event(&r, "service.promoted").await;
        assert_eq!(promoted["data"]["git_sha"], SHA, "{promoted}");
        assert_eq!(promoted["data"]["migrations_applied"], json!(["0001_a"]), "{promoted}");
        assert_eq!(live_version(&r).await, Some(1));
        let served = invoke(&r.node, &r.slug, "/").await;
        assert_eq!(served.json, json!({"tag": "v1", "tables": 1}), "{}", served.text);

        // The origin got the credentials; nothing kept them.
        let seen = r.origin.seen();
        assert!(seen[0].path_and_query.contains(SECRET));
        assert_eq!(seen[0].headers["private-token"], TOKEN);
        let list = get(&r.node, &r.key, &format!("/v1/services/{}/versions", r.id)).await;
        let source = get(&r.node, &r.key, &format!("/v1/services/{}/versions/1/source", r.id)).await;
        let evs = serde_json::to_string(&events(&r).await).expect("events");
        for text in [&up.text, &list.text, &source.text, &evs] {
            assert!(!text.contains(SECRET) && !text.contains(TOKEN), "{text}");
        }
        assert_eq!(list.json[0]["meta"]["git"]["sha"], SHA);
        r.node.shutdown().await;
    }
}

node_test! {
    /// Same bytes through the raw upload route: `activate=after_build` is what `activate=1` refuses.
    /// The switch's audit carries the commit (it used to be always null).
    async fn an_upload_with_migrations_goes_live_after_its_build_and_audits_its_sha() {
        let r = rig("fu-up").await;
        let body = serde_json::to_vec(&json!({"files": {
            "main.ts": CODE.replace("TAG", "up"),
            "migrations/0001_a.sql": "CREATE TABLE from_git (x);",
        }})).expect("json");
        let meta = base64::engine::general_purpose::STANDARD
            .encode(json!({"git": {"sha": SHA}}).to_string());
        let resp = send(
            api(&r.node, &r.key, Method::POST, &format!("/v1/services/{}/versions?activate=after_build", r.id))
                .header("content-type", "application/json")
                .header("x-rt-version-meta", meta)
                .body(body),
        ).await;
        assert_eq!(resp.s(), 202, "{}", resp.text);
        wait_event(&r, "service.promoted").await;
        assert_eq!(live_version(&r).await, Some(1));

        let bad = send(
            api(&r.node, &r.key, Method::POST, &format!("/v1/services/{}/versions", r.id))
                .header("content-type", "application/typescript")
                .header("x-rt-version-meta", "not base64!")
                .body(b"export default {}".to_vec()),
        ).await;
        assert_eq!(code(&bad), (422, Some("validation_failed")), "{}", bad.text);

        // A later plain upload, a flip to it, and a rollback whose target carries the SHA.
        let two = upload_ts(&r.node, &r.key, &r.id, "export default () => new Response('2');", "activate=1&compile=eager").await;
        assert!(two.s() == 201 || two.s() == 202, "{}", two.text);
        let switches = get(&r.node, &r.key, &format!("/v1/services/{}/switches", r.id)).await;
        let last = switches.json["items"][0]["id"].as_str().expect("switch id").to_string();
        let back = post(&r.node, &r.key, &format!("/v1/services/{}/switches/{last}/rollback", r.id), json!({})).await;
        assert_eq!(back.s(), 202, "{}", back.text);
        let rollback = wait_event(&r, "rollback").await;
        assert_eq!(rollback["data"]["data"]["git_sha"], SHA, "{rollback}");

        // And a switch to it, the other audited flip.
        let three = upload_ts(&r.node, &r.key, &r.id, "export default () => new Response('3');", "activate=1&compile=eager").await;
        assert!(three.s() == 201 || three.s() == 202, "{}", three.text);
        let switched = post(&r.node, &r.key, &format!("/v1/services/{}/switch", r.id), json!({"from": {"version": 1}})).await;
        assert_eq!(switched.s(), 202, "{}", switched.text);
        let audited = wait_event(&r, "switch").await;
        assert_eq!(audited["data"]["data"]["git_sha"], SHA, "{audited}");
        r.node.shutdown().await;
    }
}

node_test! {
    async fn a_failed_migration_or_build_leaves_live_alone() {
        let r = rig("fu-bad").await;
        let up = upload_ts(&r.node, &r.key, &r.id, "export default () => new Response('one');", "activate=1").await;
        assert!(up.s() == 201 || up.s() == 202, "{}", up.text);

        r.origin.route("/mig.tar.gz", Reply::Bytes(tarball("v2", &[("0001_a.sql", "CREATE TABLE broken (;")])));
        let one = from_url(&r, json!({
            "url": r.origin.url("git.public.test", "/mig.tar.gz"),
            "strip_components": 1, "subdir": "apps/x", "activate": "after_build",
        })).await;
        assert_eq!(one.s(), 202, "{}", one.text);
        let failed = wait_event(&r, "service.migration_failed").await;
        assert_eq!(failed["data"]["version"], 2, "{failed}");
        wait_event(&r, "service.promote_failed").await;
        assert_eq!(live_version(&r).await, Some(1));

        // Nothing resolves this import, so the build fails after validation passed.
        let broken = serde_json::to_vec(&json!({"files": {
            "main.ts": "import x from 'https://127.0.0.1:1/x.ts'; export default () => new Response(String(x));",
        }})).expect("json");
        let two = upload(&r.node, &r.key, &r.id, "application/json", broken, "activate=after_build").await;
        assert_eq!(two.s(), 202, "{}", two.text);
        wait_event(&r, "build.failed").await;
        assert_eq!(live_version(&r).await, Some(1));
        r.node.shutdown().await;
    }
}

node_test! {
    /// The real guard configuration, nothing mocked: no connection is attempted for any of these.
    async fn metadata_and_private_addresses_and_plain_http_are_denied() {
        let node = start_node().await;
        let t = new_tenant(&node, "pro").await;
        let id = create_service(&node, &t.key, "fu-ssrf", "deno").await;
        for url in [
            "https://169.254.169.254/latest/meta-data/",
            "https://[fd00:ec2::254]/latest/meta-data/",
            "https://metadata.google.internal.localhost/",
            "https://127.0.0.1/x.tar.gz",
            "https://[::1]/x.tar.gz",
            "https://[::ffff:169.254.169.254]/x.tar.gz",
            "https://10.0.0.1/x.tar.gz",
            "https://192.168.1.1/x.tar.gz",
            "https://localhost/x.tar.gz",
            "https://93.184.216.34:25/x.tar.gz",
            "http://93.184.216.34/x.tar.gz",
            "ftp://93.184.216.34/x.tar.gz",
        ] {
            let r = post(&node, &t.key, &format!("/v1/services/{id}/versions/from-url"), json!({"url": url})).await;
            assert_eq!(r.s(), 202, "{url}: {}", r.text);
            let n = r.json["version"].as_u64().expect("number");
            let v = ended(&node, &t.key, &id, n).await;
            assert_eq!(v["failure"]["code"], "egress_denied", "{url}: {v}");
        }
        let list = get(&node, &t.key, &format!("/v1/services/{id}/versions")).await;
        assert!(list.json.as_array().expect("versions").iter().all(|v| v["build_state"] == "failed"));
        node.shutdown().await;
    }
}

node_test! {
    /// A public URL does not launder a private one.
    async fn a_redirect_to_a_private_address_is_denied_on_the_hop() {
        let r = rig("fu-rd").await;
        let internal = r.origin.url("internal.test", "/repo.tar.gz");
        r.origin.route("/meta", Reply::Redirect("http://169.254.169.254/latest/meta-data/".into()));
        r.origin.route("/internal", Reply::Redirect(internal));
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        for path in ["/meta", "/internal"] {
            let res = from_url(&r, json!({"url": r.origin.url("git.public.test", path)})).await;
            assert_eq!(failed_with(&r, &res).await, "egress_denied", "{path}");
        }
        assert!(r.origin.seen().iter().all(|s| s.path_and_query != "/repo.tar.gz"), "reached the private hop");
        assert_eq!(version_count(&r).await, 2);
        r.node.shutdown().await;
    }
}

node_test! {
    /// GitHub redirects to codeload: `PRIVATE-TOKEN` must not follow, though a same-origin hop keeps it.
    async fn caller_headers_stop_at_a_cross_origin_redirect() {
        let r = rig("fu-hdr").await;
        let other = r.origin.url("codeload.public.test", "/repo.tar.gz");
        r.origin.route("/cross", Reply::Redirect(other));
        r.origin.route("/same", Reply::Redirect("/cross".into()));
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        let res = from_url(&r, json!({
            "url": r.origin.url("git.public.test", "/same"),
            "headers": {"PRIVATE-TOKEN": TOKEN, "Authorization": "Bearer x", "X-Custom": "y"},
            "strip_components": 1, "subdir": "apps/x",
        })).await;
assert_eq!(res.s(), 202, "{}", res.text);
        ended(&r.node, &r.key, &r.id, 1).await;
        let seen = r.origin.seen();
        assert_eq!(seen.len(), 3, "{seen:?}");
        for hop in &seen[..2] {
            assert!(hop.host.starts_with("git.public.test"), "{hop:?}");
            assert_eq!(hop.headers["private-token"], TOKEN);
        }
        let last = &seen[2];
        assert!(last.host.starts_with("codeload.public.test"), "{last:?}");
        for name in ["private-token", "authorization", "x-custom"] {
            assert!(!last.headers.contains_key(name), "{name} crossed: {last:?}");
        }
        r.node.shutdown().await;
    }
}

node_test! {
    async fn host_and_runtime_and_hop_by_hop_headers_are_refused() {
        let r = rig("fu-hn").await;
        for name in ["Host", "x-rt-tenant", "Connection", "Transfer-Encoding"] {
            let res = from_url(&r, json!({
                "url": r.origin.url("git.public.test", "/x"),
                "headers": {name: SECRET},
            })).await;
            assert_eq!(code(&res), (422, Some("validation_failed")), "{name}: {}", res.text);
            assert!(!res.text.contains(SECRET), "{}", res.text);
        }
        assert!(r.origin.seen().is_empty());
        r.node.shutdown().await;
    }
}

node_test! {
    /// A `deploy` key must not read through `from-url` what the service's guests may not reach.
    async fn the_services_egress_rules_apply() {
        let r = rig("fu-eg").await;
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        let url = r.origin.url("git.public.test", "/repo.tar.gz");
        let patched = patch(&r.node, &r.key, &format!("/v1/services/{}", r.id),
            json!({"egress": {"denied_hosts": ["git.public.test"]}})).await;
        assert_eq!(patched.s(), 200, "{}", patched.text);
        let res = from_url(&r, json!({"url": url})).await;
        assert_eq!(failed_with(&r, &res).await, "egress_denied");

        let patched = patch(&r.node, &r.key, &format!("/v1/services/{}", r.id),
            json!({"egress": {"allowed_hosts": ["other.public.test"], "denied_hosts": []}})).await;
        assert_eq!(patched.s(), 200, "{}", patched.text);
        let res = from_url(&r, json!({"url": url})).await;
        assert_eq!(failed_with(&r, &res).await, "egress_denied");
        assert!(r.origin.seen().is_empty());

        let patched = patch(&r.node, &r.key, &format!("/v1/services/{}", r.id),
            json!({"egress": {"allowed_hosts": ["git.public.test"]}})).await;
        assert_eq!(patched.s(), 200, "{}", patched.text);
        let res = from_url(&r, json!({"url": url, "strip_components": 1, "subdir": "apps/x"})).await;
        assert!(res.s() == 201 || res.s() == 202, "{}", res.text);
        r.node.shutdown().await;
    }
}

node_test! {
    async fn bombs_and_traversal_and_bad_answers_are_refused() {
        let r = rig("fu-zip").await;
        r.origin.route("/bomb.zip", Reply::Bytes(zip_bomb()));
        let res = from_url(&r, json!({"url": r.origin.url("git.public.test", "/bomb.zip")})).await;
        assert_eq!(failed_with(&r, &res).await, "too_large");

        let mut raw = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        let evil = b"x";
        h.as_gnu_mut().expect("gnu").name[..14].copy_from_slice(b"a/../../evil.t");
        h.set_size(evil.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        raw.append(&h, evil.as_slice()).expect("append");
        r.origin.route("/evil.tar", Reply::Bytes(raw.into_inner().expect("tar")));
        // ustar magic is absent from a gnu header at the checked offset only for odd tars: sniffing
        // must still take it for a tar, and stripping must not launder `..`.
        let res = from_url(&r, json!({"url": r.origin.url("git.public.test", "/evil.tar"), "strip_components": 1})).await;
        assert_eq!(failed_with(&r, &res).await, "validation_failed");

        r.origin.route("/gone", Reply::Status(404));
        let res = from_url(&r, json!({"url": format!("{}?token={SECRET}", r.origin.url("git.public.test", "/gone"))})).await;
        assert_eq!(failed_with(&r, &res).await, "fetch_failed");
        assert!(!res.text.contains(SECRET), "{}", res.text);

        r.origin.route("/loop", Reply::Redirect("/loop".into()));
        let res = from_url(&r, json!({"url": r.origin.url("git.public.test", "/loop")})).await;
        assert_eq!(failed_with(&r, &res).await, "fetch_failed");
        assert_eq!(r.origin.seen().iter().filter(|s| s.path_and_query == "/loop").count(), 4, "three redirects, then stop");

        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        let res = from_url(&r, json!({
            "url": r.origin.url("git.public.test", "/repo.tar.gz"),
            "strip_components": 1, "subdir": "apps/x", "sha256": "ab".repeat(32),
        })).await;
        assert_eq!(failed_with(&r, &res).await, "validation_failed");
        r.node.shutdown().await;
    }
}

node_test! {
    /// A re-signed URL is the same deploy; the fingerprint holds no credential, and a changed
    /// request under the same key is refused.
    async fn the_idempotency_fingerprint_ignores_credentials() {
        let r = rig("fu-idem").await;
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        let url = r.origin.url("git.public.test", "/repo.tar.gz");
        let path = format!("/v1/services/{}/versions/from-url", r.id);
        let call = |url: String, subdir: &'static str| {
            send(api(&r.node, &r.key, Method::POST, &path)
                .header("idempotency-key", "k1")
                .json(&json!({"url": url, "strip_components": 1, "subdir": subdir})))
        };
        let one = call(format!("{url}?sig=a"), "apps/x").await;
        assert!(one.s() == 201 || one.s() == 202, "{}", one.text);
        let two = call(format!("{url}?sig=b"), "apps/x").await;
        assert_eq!((two.s(), &two.json["version"]), (one.s(), &one.json["version"]), "{}", two.text);
        let three = call(format!("{url}?sig=c"), "apps").await;
        assert_eq!(code(&three), (422, Some("idempotency_mismatch")), "{}", three.text);
        assert_eq!(version_count(&r).await, 1);
        ended(&r.node, &r.key, &r.id, 1).await;
        assert_eq!(r.origin.seen().len(), 1, "a replay does not fetch");
        r.node.shutdown().await;
    }
}

node_test! {
    async fn scopes_and_tenants_and_argument_rules_hold() {
        let r = rig("fu-auth").await;
        let path = format!("/v1/services/{}/versions/from-url", r.id);
        let body = json!({"url": r.origin.url("git.public.test", "/x")});

        let other = new_tenant(&r.node, "pro").await;
        let res = post(&r.node, &other.key, &path, body.clone()).await;
        assert_eq!(code(&res), (404, Some("not_found")), "{}", res.text);

        let (_, ro) = make_key(&r.node, &r.tenant, &["services:read"], None).await;
        let res = post(&r.node, &ro, &path, body.clone()).await;
        assert_eq!(code(&res), (403, Some("insufficient_scope")), "{}", res.text);

        for bad in [
            json!({"url": "not a url"}),
            json!({"url": "https://x.test/a", "unknown": 1}),
            json!({"url": "https://x.test/a", "activate": "now"}),
            json!({"url": "https://x.test/a", "sha256": "abc"}),
            json!({"url": "https://x.test/a", "strip_components": 99}),
            json!({"url": "https://x.test/a", "meta": {"git": {"sha": "nope"}}}),
        ] {
            let res = post(&r.node, &r.key, &path, bad.clone()).await;
            assert_eq!(code(&res), (422, Some("validation_failed")), "{bad}: {}", res.text);
        }
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        let res = from_url(&r, json!({
            "url": r.origin.url("git.public.test", "/repo.tar.gz"),
            "strip_components": 1, "subdir": "apps/x", "activate": "after_build", "compile": "lazy",
        })).await;
        assert_eq!(code(&res), (422, Some("validation_failed")), "{}", res.text);
        r.node.shutdown().await;
    }
}

/// A `MakeWriter` into one buffer.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Captured {
        self.clone()
    }
}

/// Everything emitted during fetches (success, refusal, failure), at TRACE, holds no credential.
#[tokio::test(flavor = "current_thread")]
async fn no_log_line_holds_the_query_or_a_header_value() {
    let origin = Origin::start().await;
    origin.route("/ok", Reply::Bytes(b"archive".to_vec()));
    origin.route("/hop", Reply::Redirect("/ok".into()));
    origin.route("/private", Reply::Redirect("http://10.0.0.1/x".into()));
    origin.route("/gone", Reply::Status(500));

    let logs = Captured::default();
    let sub = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(sub);

    let tmp = tempfile::tempdir().expect("tmp");
    let fetcher = test_fetcher(tmp.path());
    let fetch = |path: &str| ArchiveRequest {
        url: format!("{}?token={SECRET}", origin.url("git.public.test", path)),
        headers: vec![("private-token".to_string(), TOKEN.to_string())],
        tenant: rt_types::TenantId::from_u128(7),
        rules: rt_types::EgressRules::default(),
        default_deny: false,
        quota_left: None,
    };
    let ok = fetcher.fetch(fetch("/hop")).await.expect("fetched");
    assert_eq!(ok.len, 7);
    let denied = fetcher.fetch(fetch("/private")).await.expect_err("denied");
    assert_eq!(denied.error, ArchiveFetchError::Denied);
    let failed = fetcher.fetch(fetch("/gone")).await.expect_err("failed");
    assert_eq!(failed.error, ArchiveFetchError::Failed { status: Some(500) });
    assert!(failed.received < 1024);

    let text = String::from_utf8_lossy(&logs.0.lock().unwrap()).to_string();
    assert!(!text.contains(SECRET) && !text.contains(TOKEN), "{text}");
    assert!(
        origin.seen().iter().any(|s| s.path_and_query.contains(SECRET)),
        "the fetch used it"
    );
}

/// The quota of a plan cuts the download and is reported as such.
#[tokio::test(flavor = "current_thread")]
async fn a_used_up_quota_stops_the_fetch() {
    let origin = Origin::start().await;
    origin.route("/big", Reply::Bytes(vec![7u8; 64 * 1024]));
    let tmp = tempfile::tempdir().expect("tmp");
    let fetcher = test_fetcher(tmp.path());
    let req = |quota_left| ArchiveRequest {
        url: origin.url("git.public.test", "/big"),
        headers: Vec::new(),
        tenant: rt_types::TenantId::from_u128(7),
        rules: rt_types::EgressRules::default(),
        default_deny: false,
        quota_left,
    };
    let none = fetcher.fetch(req(Some(0))).await.expect_err("refused");
    assert_eq!(none.error, ArchiveFetchError::Quota);
    let cut = fetcher.fetch(req(Some(1024))).await.expect_err("cut");
    assert_eq!(cut.error, ArchiveFetchError::Quota);
    assert!(cut.received > 0);
    let plan_deny = fetcher
        .fetch(ArchiveRequest {
            default_deny: true,
            ..req(None)
        })
        .await
        .expect_err("denied");
    assert_eq!(plan_deny.error, ArchiveFetchError::Denied);
}

/// Poll a version until it left `fetching`.
async fn ended(node: &impl Node, key: &str, id: &str, n: u64) -> Value {
    for _ in 0..600 {
        let v = get(node, key, &format!("/v1/services/{id}/versions/{n}")).await;
        if v.json["build_state"] != "fetching" {
            return v.json;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("version {n} stuck fetching");
}

async fn failed_with(r: &Rig, res: &Resp) -> String {
    assert_eq!(res.s(), 202, "{}", res.text);
    let n = res.json["version"].as_u64().expect("number");
    let v = ended(&r.node, &r.key, &r.id, n).await;
    assert_eq!(v["build_state"], "failed", "{v}");
    assert_eq!(v["source_digest"], Value::Null, "{v}");
    v["failure"]["code"].as_str().expect("code").to_string()
}

node_test! {
    /// The answer does not wait for the origin; a replay names the same version; a flip is refused
    /// until the source is here.
    async fn the_answer_comes_before_the_fetch_and_a_replay_is_the_same_version() {
        let r = rig("fu-202").await;
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        r.origin.delay_ms.store(1500, std::sync::atomic::Ordering::SeqCst);
        let path = format!("/v1/services/{}/versions/from-url", r.id);
        let body = json!({"url": r.origin.url("git.public.test", "/repo.tar.gz"), "strip_components": 1, "subdir": "apps/x"});
        let started = std::time::Instant::now();
        let one = send(api(&r.node, &r.key, Method::POST, &path).header("idempotency-key", "a").json(&body)).await;
        assert!(started.elapsed().as_millis() < 1000, "held for the download");
        assert_eq!((one.s(), one.json["build_state"].as_str()), (202, Some("fetching")), "{}", one.text);
        let two = send(api(&r.node, &r.key, Method::POST, &path).header("idempotency-key", "a").json(&body)).await;
        assert_eq!((two.s(), &two.json["version"]), (202, &one.json["version"]));
        let live = put(&r.node, &r.key, &format!("/v1/services/{}/live", r.id), json!({"version": 1})).await;
        assert_eq!(code(&live), (409, Some("version_not_ready")), "{}", live.text);
        let v = ended(&r.node, &r.key, &r.id, 1).await;
        assert_ne!(v["build_state"], "failed", "{v}");
        assert!(v["source_digest"].is_string(), "{v}");
        assert_eq!(version_count(&r).await, 1);
        r.node.shutdown().await;
    }
}

node_test! {
    /// A client that goes away right after sending does not stop the deploy.
    async fn a_disconnected_client_does_not_stop_the_fetch() {
        use tokio::io::AsyncWriteExt;
        let r = rig("fu-gone").await;
        r.origin.route("/repo.tar.gz", Reply::Bytes(tarball("x", &[])));
        r.origin.delay_ms.store(300, std::sync::atomic::Ordering::SeqCst);
        let body = json!({"url": r.origin.url("git.public.test", "/repo.tar.gz"), "strip_components": 1, "subdir": "apps/x"}).to_string();
        let addr = r.node.control_url("").replace("http://", "");
        let mut c = tokio::net::TcpStream::connect(&addr).await.expect("connect");
        let req = format!(
            "POST /v1/services/{}/versions/from-url HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            r.id, r.key, body.len());
        c.write_all(req.as_bytes()).await.expect("send");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(c);
        for _ in 0..200 {
            if version_count(&r).await == 1 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let v = ended(&r.node, &r.key, &r.id, 1).await;
        assert!(v["source_digest"].is_string(), "{v}");
        r.node.shutdown().await;
    }
}

node_test! {
    /// An import whose body stops halfway leaves no service and frees the tenant's turn.
    async fn a_disconnected_import_leaves_no_service() {
        use tokio::io::AsyncWriteExt;
        let r = rig("fu-imp").await;
        let addr = r.node.control_url("").replace("http://", "");
        let mut c = tokio::net::TcpStream::connect(&addr).await.expect("connect");
        let req = format!(
            "POST /v1/services/import?slug=half-done HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {}\r\ncontent-type: application/zstd\r\ncontent-length: 100000\r\n\r\npartial",
            r.key);
        c.write_all(req.as_bytes()).await.expect("send");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(c);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let list = get(&r.node, &r.key, "/v1/services?slug=half-done").await;
        assert!(!list.text.contains("half-done"), "{}", list.text);
        let again = send(api(&r.node, &r.key, Method::POST, "/v1/services/import?slug=half-done")
            .header("content-type", "application/zstd").body(b"junk".to_vec())).await;
        assert_eq!(code(&again), (422, Some("bundle_invalid")), "{}", again.text);
        r.node.shutdown().await;
    }
}

/// A restart ends a fetch as failed and takes out what an interrupted import left; a service that
/// merely shares nothing with the note stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_fails_fetches_and_removes_interrupted_imports() {
    let mut node = Restartable::boot(|_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let keep = create_service(&node, &t.key, "keep-me", "deno").await;
    let half = create_service(&node, &t.key, "half-import", "deno").await;
    let empty = create_service(&node, &t.key, "old-enough", "deno").await;
    node.stop().await;
    {
        let db = rusqlite::Connection::open(node.data_dir().join("meta.db")).expect("meta.db");
        db.execute(
            "INSERT INTO versions (service_id, number, source_digest, build_state, pending_reason, created_at, activate_after_build) \
             VALUES (?1, 1, ?2, 'pending', 'fetching', 1, 'someone')",
            [keep.as_str(), &format!("sha256:{}", "0".repeat(64))],
        ).expect("fetching row");
        db.execute(
            "INSERT INTO pending_imports (id, tenant_id, slug, service_id, started_at) VALUES ('a', ?1, 'half-import', ?2, 1)",
            [t.id.as_str(), half.as_str()],
        ).expect("bound note");
        // Unbound and newer than the service: it was there first, so it stays.
        db.execute(
            "INSERT INTO pending_imports (id, tenant_id, slug, started_at) VALUES ('b', ?1, 'old-enough', 9007199254740991)",
            [t.id.as_str()],
        ).expect("unbound note");
    }
    node.start().await;
    let v = get(&node, &t.key, &format!("/v1/services/{keep}/versions/1")).await;
    assert_eq!(v.json["build_state"], "failed", "{}", v.text);
    assert_eq!(v.json["failure"]["code"], "fetch_failed", "{}", v.text);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{half}")).await.s(), 404);
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{empty}")).await.s(), 200);
    node.stop().await;
}

node_test! {
    /// `?async=1`: the switch comes back first; polling it shows the flip, or the migration failure
    /// with live unchanged. A second one meanwhile is refused, not queued.
    async fn async_promote_answers_with_a_switch_to_poll() {
        let r = rig("fu-async").await;
        let good = serde_json::to_vec(&json!({"files": {
            "main.ts": CODE.replace("TAG", "a"), "migrations/0001_a.sql": "CREATE TABLE from_git (x);",
        }})).expect("json");
        assert_eq!(upload(&r.node, &r.key, &r.id, "application/json", good, "compile=eager").await.s(), 202);
        ended(&r.node, &r.key, &r.id, 1).await;
        let path = format!("/v1/services/{}/live?async=1", r.id);
        let up = put(&r.node, &r.key, &path, json!({"version": 1})).await;
        assert_eq!(up.s(), 202, "{}", up.text);
        let id = up.json["id"].as_str().expect("switch id").to_string();
        assert_eq!(up.json["to_version"], 1);
        let poll = |id: String| {
            let (node, key, svc) = (&r.node, &r.key, &r.id);
            async move {
                for _ in 0..400 {
                    let sw = get(node, key, &format!("/v1/services/{svc}/switches/{id}")).await;
                    if !matches!(sw.json["state"].as_str(), Some("preparing" | "migrating")) {
                        return sw.json;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                panic!("switch stuck");
            }
        };
        assert_ne!(poll(id).await["state"], "failed");
        assert_eq!(live_version(&r).await, Some(1));

        let bad = serde_json::to_vec(&json!({"files": {
            "main.ts": CODE.replace("TAG", "b"),
            "migrations/0001_a.sql": "CREATE TABLE from_git (x);",
            "migrations/0002_b.sql": "CREATE TABLE broken (;",
        }})).expect("json");
        assert_eq!(upload(&r.node, &r.key, &r.id, "application/json", bad, "compile=eager").await.s(), 202);
        ended(&r.node, &r.key, &r.id, 2).await;
        let up = put(&r.node, &r.key, &path, json!({"version": 2})).await;
        assert_eq!(up.s(), 202, "{}", up.text);
        let sw = poll(up.json["id"].as_str().expect("id").to_string()).await;
        assert_eq!((sw["state"].as_str(), sw["error"].as_str()), (Some("failed"), Some("migration_failed")), "{sw}");
        assert_eq!(live_version(&r).await, Some(1));

        let dry = put(&r.node, &r.key, &path, json!({"version": 1, "dry_run": true})).await;
        assert_eq!(code(&dry), (422, Some("validation_failed")));
        let none = put(&r.node, &r.key, &path, json!({"version": 99})).await;
        assert_eq!(code(&none), (404, Some("not_found")));
        r.node.shutdown().await;
    }
}
