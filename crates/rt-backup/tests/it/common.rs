//! A small real node on disk (stores only) and a tool for rewriting archives.
#![allow(dead_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use rt_backup::archive::{sign_manifest, Manifest, MANIFEST};
use rt_backup::create::{create_archive, Sources};
use rt_backup::restore::{restore_archive, verify_archive, Auth, RestoreOptions, RestoreReport};
use rt_backup::{BackupError, Result};
use rt_sec::{secret_aad, LocalSecrets, MasterKey, SecretsBackend};
use rt_store::{
    DataConfig, LocalArtifactStore, LocalServiceData, MetaStore, ServiceDataStore, SqlStatement, SqliteMetaStore,
    SqliteQueueStore, SqliteStreamStore, TenantScope,
};
use rt_types::{Body, CompileMode, NewService, NewTenant, NewVersion, RuntimeKind, ServiceConfig, ServiceId, TenantId};

pub fn st(sql: &str) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        args: Vec::new(),
    }
}

pub struct Node {
    pub dir: tempfile::TempDir,
    pub master: MasterKey,
    pub meta: Arc<SqliteMetaStore>,
    pub data: Arc<LocalServiceData>,
    pub secrets: Arc<LocalSecrets>,
    pub tenant: TenantId,
    pub service: ServiceId,
    pub source: rt_types::Digest,
    _queue: Arc<SqliteQueueStore>,
    _streams: Arc<SqliteStreamStore>,
}

pub const BLOB_MAX: u64 = 1 << 30;

impl Node {
    /// A node with one service: rows, a KV key, a blob, a version with an artifact and a secret.
    pub async fn seeded() -> Self {
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path();
        let master = MasterKey::generate();
        let meta = SqliteMetaStore::open(&root.join("meta.db")).expect("meta");
        let queue = SqliteQueueStore::open(&root.join("queue.db"), 5).expect("queue");
        let streams = SqliteStreamStore::open(&root.join("streams.db")).expect("streams");
        let data = LocalServiceData::open(DataConfig::new(root)).expect("data");
        let artifacts = LocalArtifactStore::open(&root.join("artifacts")).expect("artifacts");
        let secrets = Arc::new(LocalSecrets::new(&master));

        let tenant = meta
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let svc = meta
            .create_service(
                TenantScope::Tenant(tenant.id),
                tenant.id,
                NewService {
                    slug: "app".into(),
                    runtime: RuntimeKind::Deno,
                    name: None,
                    profile: None,
                    environment: rt_types::DEFAULT_ENVIRONMENT.to_string(),
                    project: rt_types::DEFAULT_PROJECT.to_string(),
                    config: ServiceConfig::default(),
                },
            )
            .await
            .expect("service");
        let source = rt_store::ArtifactStore::put(&*artifacts, Bytes::from_static(b"export default {}"))
            .await
            .expect("artifact");
        meta.add_version(
            svc.id,
            NewVersion {
                source_digest: source,
                compile: CompileMode::Lazy,
            },
        )
        .await
        .expect("version");
        let sealed = secrets.seal(&secret_aad(tenant.id, svc.id, "TOKEN"), b"s3cret");
        meta.put_secret(svc.id, "TOKEN", sealed.to_bytes())
            .await
            .expect("secret");

        let sql = data.sql(svc.id);
        sql.execute(st("CREATE TABLE pairs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)"))
            .await
            .expect("create");
        sql.execute(st("INSERT INTO pairs (a, b) VALUES (1, -1)"))
            .await
            .expect("insert");
        data.kv(svc.id)
            .set(
                b"hello",
                rt_store::KvValue {
                    tag: rt_store::ValueTag::Bytes,
                    data: Bytes::from_static(b"world"),
                },
                Default::default(),
            )
            .await
            .expect("kv");
        data.blob(svc.id)
            .put(
                "file.bin",
                Body::from_bytes(Bytes::from_static(b"blob bytes")),
                None,
                BLOB_MAX,
            )
            .await
            .expect("blob");
        Self {
            dir,
            master,
            meta,
            data,
            secrets,
            tenant: tenant.id,
            service: svc.id,
            source,
            _queue: queue,
            _streams: streams,
        }
    }

    pub fn sources(&self) -> Sources {
        Sources {
            data_dir: self.dir.path().to_path_buf(),
            data: self.data.clone(),
            secrets: self.secrets.clone(),
            master: self.master.clone(),
        }
    }

    /// Back up into `out`; returns the archive path.
    pub fn backup_to(&self, out: &Path) -> PathBuf {
        let tmp = out.join("scratch");
        std::fs::create_dir_all(&tmp).expect("scratch");
        let id = format!("bkp_{}", ulid::Ulid::new());
        create_archive(&self.sources(), out, &tmp, &id, 1, 0, &AtomicBool::new(false), "test")
            .expect("archive")
            .path
    }

    pub fn keys(&self) -> Vec<MasterKey> {
        vec![self.master.clone()]
    }

    pub fn verify(&self, archive: &Path) -> Result<rt_backup::restore::VerifyReport> {
        verify_archive(archive, &Auth::Keys(&self.keys()))
    }

    pub fn restore(&self, archive: &Path, to: &Path) -> Result<RestoreReport> {
        let keys = self.keys();
        let secrets = self.secrets.clone();
        restore_archive(
            archive,
            to,
            &RestoreOptions {
                auth: Auth::Keys(&keys),
                revoke_keys_before: None,
                secrets: Some(secrets.as_ref()),
                now_ms: 1_000_000,
            },
        )
    }
}

/// Every entry of an archive, for rewriting it. Test archives are small.
#[derive(Clone)]
pub struct RawEntry {
    pub path: String,
    pub body: Vec<u8>,
    pub kind: tar::EntryType,
    pub link: Option<String>,
}

pub fn read_entries(archive: &Path) -> Vec<RawEntry> {
    let mut a = tar::Archive::new(File::open(archive).expect("open"));
    a.entries()
        .expect("entries")
        .map(|e| {
            let mut e = e.expect("entry");
            let path = String::from_utf8(e.path_bytes().to_vec()).expect("utf8");
            let mut body = Vec::new();
            e.read_to_end(&mut body).expect("read");
            RawEntry {
                path,
                body,
                kind: tar::EntryType::Regular,
                link: None,
            }
        })
        .collect()
}

/// Write entries as raw tar blocks: the names go in verbatim, as an attacker's tool would write them.
pub fn write_entries(path: &Path, entries: &[RawEntry]) {
    let mut out = File::create(path).expect("create");
    for e in entries {
        let mut h = tar::Header::new_old();
        {
            let old = h.as_old_mut();
            assert!(e.path.len() < old.name.len(), "test path too long");
            old.name[..e.path.len()].copy_from_slice(e.path.as_bytes());
            if let Some(link) = &e.link {
                old.linkname[..link.len()].copy_from_slice(link.as_bytes());
            }
        }
        h.set_size(e.body.len() as u64);
        h.set_mode(0o600);
        h.set_entry_type(e.kind);
        h.set_cksum();
        out.write_all(h.as_bytes()).expect("header");
        out.write_all(&e.body).expect("body");
        let pad = (512 - e.body.len() % 512) % 512;
        out.write_all(&vec![0u8; pad]).expect("pad");
    }
    out.write_all(&[0u8; 1024]).expect("end");
}

/// Change `f` the manifest and re-sign it with `key`.
pub fn resign(entries: &mut [RawEntry], key: &MasterKey, f: impl FnOnce(&mut Manifest)) {
    let i = entries.iter().position(|e| e.path == MANIFEST).expect("manifest");
    let mut m: Manifest = serde_json::from_slice(&entries[i].body).expect("json");
    f(&mut m);
    entries[i].body = serde_json::to_vec(&m).expect("json");
    let tag = sign_manifest(key, &entries[i].body);
    entries[i + 1].body = tag.into_bytes();
}

pub fn code<T: std::fmt::Debug>(r: Result<T, BackupError>) -> &'static str {
    r.expect_err("must be refused").code
}

pub fn is_empty(dir: &Path) -> bool {
    !dir.exists() || std::fs::read_dir(dir).expect("list").next().is_none()
}
