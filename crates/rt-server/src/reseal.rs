//! `rt secrets reseal` (SECURITY.md SEC-53, docs/deploy.md): rewrite every
//! stored secret under the current master key.
//!
//! Values already sealed in the current format under the current key are left alone, so the
//! command is idempotent and safe to re-run. Values that open under neither the current nor the
//! previous key are reported (service id and name only, never a value) and left untouched.
//!
//! The node may be running: each value is replaced with a compare-and-swap on the bytes that
//! were read, so a secret written or deleted meanwhile is never overwritten with an older value
//! (it is counted as `changed`; a live write already uses the current key).

use std::path::Path;

use anyhow::Context as _;
use rt_config::{Config, NodeSecrets};
use rt_sec::{secret_aad, MasterKey, Sealed, SecretsBackend};
use rt_store::{MetaStore, PageReq, ServiceFilter, SqliteMetaStore, TenantScope};
use rt_types::ServiceId;

/// Outcome of one re-seal run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResealReport {
    /// Rewritten under the current key.
    pub resealed: u64,
    /// Already in the current format under the current key.
    pub current: u64,
    /// Could not be opened (or parsed): left as they are.
    pub failed: Vec<(ServiceId, String)>,
    /// Written or deleted by someone else between the read and the rewrite: left as they are.
    pub changed: u64,
}

impl ResealReport {
    pub fn summary(&self) -> String {
        format!(
            "secrets resealed: {}, already current: {}, changed meanwhile: {}, failed: {}",
            self.resealed,
            self.current,
            self.changed,
            self.failed.len()
        )
    }
}

/// Re-seal every secret of every service in `meta` with `backend`'s current key.
pub async fn reseal_secrets(meta: &dyn MetaStore, backend: &dyn SecretsBackend) -> rt_store::Result<ResealReport> {
    let mut report = ResealReport::default();
    let mut cursor = None;
    loop {
        let page = meta
            .list_services(
                TenantScope::Operator,
                ServiceFilter::default(),
                PageReq {
                    limit: 200,
                    cursor: cursor.take(),
                },
            )
            .await?;
        for svc in &page.items {
            for (name, bytes) in meta.get_sealed_secrets(svc.id).await? {
                let aad = secret_aad(svc.tenant_id, svc.id, &name);
                let Ok(sealed) = Sealed::from_bytes(&bytes) else {
                    report.failed.push((svc.id, name));
                    continue;
                };
                if backend.is_current(&sealed) {
                    report.current += 1;
                    continue;
                }
                match backend.open(&aad, &sealed) {
                    Ok(plaintext) => {
                        let resealed = backend.seal(&aad, &plaintext);
                        if meta
                            .replace_secret_if(svc.id, &name, bytes, resealed.to_bytes())
                            .await?
                        {
                            report.resealed += 1;
                        } else {
                            report.changed += 1;
                        }
                    }
                    Err(_) => report.failed.push((svc.id, name)),
                }
            }
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(report)
}

/// The CLI command: the node's `meta.db` and master keys from its configuration and secrets
/// (`RT_MASTER_KEY[_FILE]` or `<data_dir>/master.key`, plus `RT_MASTER_KEY_PREVIOUS[_FILE]`).
pub async fn run_reseal(config: &Config, secrets: &NodeSecrets) -> anyhow::Result<ResealReport> {
    let data_dir = Path::new(&config.node.data_dir);
    let master = match &secrets.master_key {
        Some(s) => MasterKey::from_hex(s.expose()).context("RT_MASTER_KEY")?,
        None => {
            let path = data_dir.join("master.key");
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("no master key: set RT_MASTER_KEY or create {}", path.display()))?;
            MasterKey::from_hex(text.trim()).context("master.key")?
        }
    };
    let previous = match &secrets.master_key_previous {
        Some(s) => Some(MasterKey::from_hex(s.expose()).context("RT_MASTER_KEY_PREVIOUS")?),
        None => None,
    };
    let meta_path = data_dir.join("meta.db");
    anyhow::ensure!(meta_path.is_file(), "{} does not exist", meta_path.display());
    let meta = SqliteMetaStore::open(&meta_path).context("meta.db")?;
    let backend = crate::wiring::local_secrets(&master, previous.as_ref());
    Ok(reseal_secrets(meta.as_ref(), &backend).await?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rt_sec::LocalSecrets;
    use rt_types::{NewService, NewTenant, RuntimeKind, TenantId};

    use super::*;

    struct Db {
        _dir: tempfile::TempDir,
        meta: Arc<dyn MetaStore>,
        tenant: TenantId,
        services: Vec<ServiceId>,
    }

    async fn db(services: usize) -> Db {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta: Arc<dyn MetaStore> = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let t = meta
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let mut ids = Vec::new();
        for i in 0..services {
            let new = NewService {
                slug: format!("svc-{i}"),
                runtime: RuntimeKind::Wasm,
                name: None,
                profile: None,
                environment: rt_types::default_environment(),
                project: rt_types::default_project(),
                config: Default::default(),
            };
            ids.push(
                meta.create_service(TenantScope::Operator, t.id, new)
                    .await
                    .expect("service")
                    .id,
            );
        }
        Db {
            _dir: dir,
            meta,
            tenant: t.id,
            services: ids,
        }
    }

    fn key(b: u8) -> MasterKey {
        MasterKey::from_bytes([b; 32])
    }

    async fn put(d: &Db, backend: &LocalSecrets, svc: ServiceId, name: &str, value: &str) {
        let sealed = backend.seal(&secret_aad(d.tenant, svc, name), value.as_bytes());
        d.meta.put_secret(svc, name, sealed.to_bytes()).await.expect("put");
    }

    async fn stored(d: &Db, svc: ServiceId, name: &str) -> Sealed {
        let all = d.meta.get_sealed_secrets(svc).await.expect("get");
        let (_, bytes) = all.into_iter().find(|(n, _)| n == name).expect("stored");
        Sealed::from_bytes(&bytes).expect("parses")
    }

    #[tokio::test]
    async fn ac_sec_53_reseal_rewrites_every_secret_under_the_current_key() {
        let d = db(3).await;
        let old = LocalSecrets::new(&key(1));
        for (i, svc) in d.services.iter().enumerate() {
            put(&d, &old, *svc, "TOKEN", &format!("value-{i}")).await;
            put(&d, &old, *svc, "OTHER", "x").await;
        }
        let rotated = LocalSecrets::new(&key(2)).with_previous(&key(1));
        let report = reseal_secrets(d.meta.as_ref(), &rotated).await.expect("reseal");
        assert_eq!(
            report,
            ResealReport {
                resealed: 6,
                current: 0,
                failed: Vec::new(),
                changed: 0
            }
        );

        // Every value now opens with the new key alone.
        let current_only = LocalSecrets::new(&key(2));
        for (i, svc) in d.services.iter().enumerate() {
            let sealed = stored(&d, *svc, "TOKEN").await;
            assert_eq!(sealed.key_id, Some(current_only.key_id()));
            let pt = current_only
                .open(&secret_aad(d.tenant, *svc, "TOKEN"), &sealed)
                .expect("opens under the new key");
            assert_eq!(pt.as_slice(), format!("value-{i}").as_bytes());
        }

        // Idempotent: a second run changes nothing.
        let again = reseal_secrets(d.meta.as_ref(), &rotated).await.expect("reseal again");
        assert_eq!(
            again,
            ResealReport {
                resealed: 0,
                current: 6,
                failed: Vec::new(),
                changed: 0
            }
        );
    }

    /// A value written (or deleted) after reseal read it is never overwritten with the old one.
    #[tokio::test]
    async fn reseal_replaces_a_value_only_while_it_is_unchanged() {
        let d = db(1).await;
        let svc = d.services[0];
        let old = LocalSecrets::new(&key(1));
        let current = LocalSecrets::new(&key(2)).with_previous(&key(1));
        put(&d, &old, svc, "TOKEN", "v1").await;
        let read = stored(&d, svc, "TOKEN").await.to_bytes();
        // A live write lands between reseal's read and its rewrite.
        put(&d, &current, svc, "TOKEN", "v2").await;
        let stale = current.seal(&secret_aad(d.tenant, svc, "TOKEN"), b"v1").to_bytes();
        assert!(!d
            .meta
            .replace_secret_if(svc, "TOKEN", read.clone(), stale.clone())
            .await
            .expect("cas"));
        let now = stored(&d, svc, "TOKEN").await;
        let pt = current.open(&secret_aad(d.tenant, svc, "TOKEN"), &now).expect("opens");
        assert_eq!(pt.as_slice(), b"v2", "the newer value survives");
        // A deleted secret is not resurrected either.
        d.meta.delete_secret(svc, "TOKEN").await.expect("delete");
        assert!(!d
            .meta
            .replace_secret_if(svc, "TOKEN", now.to_bytes(), stale)
            .await
            .expect("cas"));
        assert!(d.meta.get_sealed_secrets(svc).await.expect("get").is_empty());
        // Unchanged bytes are replaced.
        put(&d, &old, svc, "OTHER", "x").await;
        let bytes = stored(&d, svc, "OTHER").await.to_bytes();
        let fresh = current.seal(&secret_aad(d.tenant, svc, "OTHER"), b"x").to_bytes();
        assert!(d.meta.replace_secret_if(svc, "OTHER", bytes, fresh).await.expect("cas"));
    }

    #[tokio::test]
    async fn ac_sec_53_reseal_reports_values_no_configured_key_opens_and_keeps_them() {
        let d = db(1).await;
        let svc = d.services[0];
        put(&d, &LocalSecrets::new(&key(1)), svc, "LOST", "unrecoverable").await;
        put(&d, &LocalSecrets::new(&key(2)), svc, "OK", "fine").await;
        let before = stored(&d, svc, "LOST").await;

        let report = reseal_secrets(d.meta.as_ref(), &LocalSecrets::new(&key(3)).with_previous(&key(2)))
            .await
            .expect("reseal");
        assert_eq!(report.resealed, 1);
        assert_eq!(report.failed, vec![(svc, "LOST".to_string())]);
        assert_eq!(
            stored(&d, svc, "LOST").await,
            before,
            "an unopenable value is left untouched"
        );
        assert!(!report.summary().contains("unrecoverable"));
    }

    #[tokio::test]
    async fn reseal_reports_a_value_that_is_not_a_sealed_blob_at_all() {
        let d = db(1).await;
        let svc = d.services[0];
        d.meta
            .put_secret(svc, "BAD", b"not sealed".to_vec())
            .await
            .expect("put");
        let report = reseal_secrets(d.meta.as_ref(), &LocalSecrets::new(&key(1)))
            .await
            .expect("reseal");
        assert_eq!(report.failed, vec![(svc, "BAD".to_string())]);
        assert_eq!(
            d.meta.get_sealed_secrets(svc).await.expect("get"),
            vec![("BAD".to_string(), b"not sealed".to_vec())]
        );
    }

    #[tokio::test]
    async fn reseal_walks_every_page_of_services() {
        let d = db(201).await;
        let old = LocalSecrets::new(&key(1));
        for svc in &d.services {
            put(&d, &old, *svc, "T", "v").await;
        }
        let report = reseal_secrets(d.meta.as_ref(), &LocalSecrets::new(&key(2)).with_previous(&key(1)))
            .await
            .expect("reseal");
        assert_eq!(report.resealed, 201, "past the first page of 200");
    }

    /// A backend that, as reseal opens a value, lets a live write land: reseal's rewrite must then lose.
    struct RacingBackend {
        inner: LocalSecrets,
        meta: Arc<dyn MetaStore>,
        write: parking_lot::Mutex<Option<(ServiceId, String, Vec<u8>)>>,
    }

    impl SecretsBackend for RacingBackend {
        fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Sealed {
            self.inner.seal(aad, plaintext)
        }

        fn open(&self, aad: &[u8], sealed: &Sealed) -> Result<zeroize::Zeroizing<Vec<u8>>, rt_sec::SecretError> {
            if let Some((svc, name, bytes)) = self.write.lock().take() {
                let meta = self.meta.clone();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current()
                        .block_on(async move { meta.put_secret(svc, &name, bytes).await.expect("live write") })
                });
            }
            self.inner.open(aad, sealed)
        }

        fn is_current(&self, sealed: &Sealed) -> bool {
            self.inner.is_current(sealed)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_secret_written_while_reseal_runs_is_counted_as_changed_and_kept() {
        let d = db(1).await;
        let svc = d.services[0];
        let old = LocalSecrets::new(&key(1));
        put(&d, &old, svc, "TOKEN", "v1").await;
        put(&d, &old, svc, "ZZ_OTHER", "untouched").await;
        let newer = LocalSecrets::new(&key(2)).with_previous(&key(1));
        let live = newer.seal(&secret_aad(d.tenant, svc, "TOKEN"), b"v2").to_bytes();
        let backend = RacingBackend {
            inner: newer,
            meta: d.meta.clone(),
            write: parking_lot::Mutex::new(Some((svc, "TOKEN".to_string(), live.clone()))),
        };
        let report = reseal_secrets(d.meta.as_ref(), &backend).await.expect("reseal");
        assert_eq!(
            report,
            ResealReport {
                resealed: 1,
                current: 0,
                failed: Vec::new(),
                changed: 1
            }
        );
        assert_eq!(
            stored(&d, svc, "TOKEN").await.to_bytes(),
            live,
            "the live write survives"
        );
    }

    // ---- the CLI command ---------------------------------------------------------------------

    fn node(dir: &std::path::Path, master: Option<&str>, previous: Option<&str>) -> (Config, NodeSecrets) {
        let mut config = Config::default();
        config.node.data_dir = dir.display().to_string();
        let secret = |s: &str| rt_config::Secret::new(s.to_string());
        (
            config,
            NodeSecrets {
                root_token: None,
                master_key: master.map(secret),
                master_key_previous: previous.map(secret),
            },
        )
    }

    fn hex_of(b: u8) -> String {
        hex::encode([b; 32])
    }

    #[tokio::test]
    async fn the_reseal_command_reads_the_master_keys_from_the_secrets_or_the_data_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("meta.db");
        let tenant = meta
            .create_tenant(NewTenant {
                name: "t".into(),
                plan: "free".into(),
                overrides: None,
            })
            .await
            .expect("tenant");
        let new = NewService {
            slug: "s".into(),
            runtime: RuntimeKind::Wasm,
            name: None,
            profile: None,
            environment: rt_types::default_environment(),
            project: rt_types::default_project(),
            config: Default::default(),
        };
        let svc = meta
            .create_service(TenantScope::Operator, tenant.id, new)
            .await
            .expect("service")
            .id;
        let sealed = LocalSecrets::new(&key(1)).seal(&secret_aad(tenant.id, svc, "T"), b"v");
        meta.put_secret(svc, "T", sealed.to_bytes()).await.expect("put");
        drop(meta);

        // Key one in a file, key two as the current key and key one as the previous.
        std::fs::write(dir.path().join("master.key"), format!("{}\n", hex_of(1))).expect("master.key");
        let (config, secrets) = node(dir.path(), None, None);
        let report = run_reseal(&config, &secrets)
            .await
            .expect("current key from master.key");
        assert_eq!((report.resealed, report.current), (0, 1));

        let (config, secrets) = node(dir.path(), Some(&hex_of(2)), Some(&hex_of(1)));
        let report = run_reseal(&config, &secrets).await.expect("rotated");
        assert_eq!(report.resealed, 1);
    }

    #[tokio::test]
    async fn the_reseal_command_says_what_is_missing_or_malformed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let msg = |r: anyhow::Result<ResealReport>| format!("{:#}", r.expect_err("fails"));

        let (config, secrets) = node(dir.path(), None, None);
        assert!(
            msg(run_reseal(&config, &secrets).await).contains("no master key"),
            "no key anywhere"
        );

        std::fs::write(dir.path().join("master.key"), "not hex").expect("master.key");
        assert!(msg(run_reseal(&config, &secrets).await).contains("master.key"));

        let (config, secrets) = node(dir.path(), Some("zz"), None);
        assert!(msg(run_reseal(&config, &secrets).await).contains("RT_MASTER_KEY"));

        let (config, secrets) = node(dir.path(), Some(&hex_of(1)), Some("zz"));
        assert!(msg(run_reseal(&config, &secrets).await).contains("RT_MASTER_KEY_PREVIOUS"));

        let (config, secrets) = node(dir.path(), Some(&hex_of(1)), None);
        assert!(
            msg(run_reseal(&config, &secrets).await).contains("meta.db"),
            "a node that never ran has no database"
        );
    }
}
