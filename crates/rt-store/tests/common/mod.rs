//! Shared helpers for the rt-store integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rt_store::{DataConfig, LocalServiceData, MetaStore, SqlStatement, SqlValue, SqliteMetaStore, TenantScope};
use rt_types::{
    CompileMode, Digest, NewService, NewTenant, NewVersion, RuntimeKind, Service, ServiceConfig, ServiceId, Tenant,
    Version,
};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

pub fn meta() -> Arc<SqliteMetaStore> {
    SqliteMetaStore::open_in_memory().expect("in-memory meta store opens")
}

pub fn new_tenant(name: &str) -> NewTenant {
    NewTenant {
        name: name.to_string(),
        plan: "free".to_string(),
        overrides: None,
    }
}

pub fn new_service(slug: &str) -> NewService {
    NewService {
        slug: slug.to_string(),
        runtime: RuntimeKind::Deno,
        name: None,
        profile: None,
        environment: rt_types::DEFAULT_ENVIRONMENT.to_string(),
        project: rt_types::DEFAULT_PROJECT.to_string(),
        config: ServiceConfig::default(),
    }
}

pub async fn mk_tenant(m: &SqliteMetaStore, name: &str) -> Tenant {
    m.create_tenant(new_tenant(name)).await.expect("create tenant")
}

pub async fn mk_service(m: &SqliteMetaStore, t: &Tenant, slug: &str) -> Service {
    m.create_service(TenantScope::Tenant(t.id), t.id, new_service(slug))
        .await
        .expect("create service")
}

/// Distinct, deterministic digest per `n`.
pub fn digest_n(n: u64) -> Digest {
    Digest::of(format!("artifact-{n}").as_bytes())
}

pub fn new_version(n: u64) -> NewVersion {
    NewVersion {
        source_digest: digest_n(n),
        compile: CompileMode::Lazy,
    }
}

/// Add `count` versions whose digests are `digest_n(from)`, `digest_n(from + 1)`, ….
pub async fn mk_versions(m: &SqliteMetaStore, svc: ServiceId, from: u64, count: u64) -> Vec<Version> {
    let mut out = Vec::new();
    for n in from..from + count {
        out.push(m.add_version(svc, new_version(n)).await.expect("add version"));
    }
    out
}

pub fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

pub fn data_cfg(root: &Path) -> DataConfig {
    DataConfig::new(root)
}

pub fn data(root: &Path) -> Arc<LocalServiceData> {
    LocalServiceData::open(data_cfg(root)).expect("service data store opens")
}

pub fn st(sql: &str) -> SqlStatement {
    SqlStatement {
        sql: sql.to_string(),
        args: Vec::new(),
    }
}

pub fn st_args(sql: &str, args: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.to_string(),
        args,
    }
}

/// All regular files below `root` whose file name equals `name`.
pub fn find_files(root: &Path, name: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, &mut |p| {
        if p.file_name().is_some_and(|n| n == name) {
            out.push(p.to_path_buf());
        }
    });
    out
}

/// Every regular file below `root`.
pub fn all_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, &mut |p| out.push(p.to_path_buf()));
    out
}

fn walk(dir: &Path, f: &mut dyn FnMut(&Path)) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for entry in rd.flatten() {
        let p = entry.path();
        match entry.file_type() {
            Ok(t) if t.is_dir() => walk(&p, f),
            Ok(t) if t.is_file() => f(&p),
            _ => {}
        }
    }
}

/// Deterministic pseudo-random bytes (xorshift), for content that does not compress or repeat.
pub fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}
