//! Backups in the `rt` binary: the API's view of the manager, and the offline commands.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use rt_api::backups::{BackupControl, BackupFacts};
use rt_api::ApiError;
use rt_backup::encrypt::AgeIdentity;
use rt_backup::manager::BackupManager;
use rt_backup::remote::{RemoteConfig, RetentionPolicy};
use rt_backup::restore::{restore_archive_age, verify_archive_age, Auth, RestoreOptions};
use rt_backup::s3::{S3Config, S3Credentials};
use rt_backup::BackupError;
use rt_config::{Config, NodeSecrets};
use rt_sec::MasterKey;
use rt_store::{BackupRow, Page, PageReq};

use crate::cli::BackupCommand;

/// One S3 credential: from its file, else from the environment. Never logged.
fn s3_credential(file: &str, env: &str) -> Result<zeroize::Zeroizing<String>, BackupError> {
    let missing = || BackupError::new("bad_config", format!("backup.s3: no credential: set its file or {env}"));
    let value = if file.is_empty() {
        std::env::var(env).map_err(|_| missing())?
    } else {
        std::fs::read_to_string(file).map_err(|_| missing())?
    };
    Ok(zeroize::Zeroizing::new(value))
}

/// `[backup.s3]` resolved: `None` when no endpoint is set. Egress rules bind the endpoint too.
pub(crate) fn remote_config(cfg: &Config) -> Result<Option<RemoteConfig>, BackupError> {
    let c = &cfg.backup.s3;
    if c.endpoint.is_empty() {
        return Ok(None);
    }
    let access = s3_credential(&c.access_key_file, "RT_BACKUP_S3_ACCESS_KEY")?;
    let secret = s3_credential(&c.secret_key_file, "RT_BACKUP_S3_SECRET_KEY")?;
    let mut s3 = S3Config::new(
        &c.endpoint,
        &c.bucket,
        &c.prefix,
        &c.region,
        S3Credentials::new(&access, &secret),
    );
    s3.denied_ports = cfg.egress.denied_ports.clone();
    s3.denied_hosts = cfg.egress.denied_hosts.clone();
    Ok(Some(RemoteConfig {
        s3,
        keep_local: c.keep_local,
        retention: RetentionPolicy {
            keep_last: c.keep_last,
            keep_daily: c.keep_daily,
            keep_weekly: c.keep_weekly,
        },
    }))
}

fn identity(path: Option<&Path>) -> Result<Option<AgeIdentity>, BackupError> {
    path.map(AgeIdentity::from_file).transpose()
}

fn api_error(e: BackupError) -> ApiError {
    let code = match e.code {
        "backup_disabled" => "backup_disabled",
        "backup_running" => "backup_running",
        "insufficient_storage" => "insufficient_storage",
        _ => "internal",
    };
    ApiError::new(code, e.message)
}

/// `rt-api`'s handle on the node's [`BackupManager`].
pub(crate) struct BackupCtl(pub(crate) Arc<BackupManager>);

#[async_trait]
impl BackupControl for BackupCtl {
    async fn create(&self) -> Result<BackupRow, ApiError> {
        self.0.start("api").await.map_err(api_error)
    }
    async fn list(&self, page: PageReq) -> Result<Page<BackupRow>, ApiError> {
        self.0.list(page).await.map_err(api_error)
    }
    async fn get(&self, id: &str) -> Result<Option<BackupRow>, ApiError> {
        self.0.get(id).await.map_err(api_error)
    }
    async fn status(&self) -> Result<BackupFacts, ApiError> {
        let s = self.0.status().await.map_err(api_error)?;
        Ok(BackupFacts {
            enabled: s.enabled,
            schedule: s.schedule,
            last_ok_at: s.last_ok_at,
            last_ok_id: s.last_ok_id,
            last_failure: s.last_failure,
            next_at: s.next_at,
            running_id: s.running_id,
            last_restored_at: s.last_restored_at,
            remote_host: s.remote_host,
            remote_bucket: s.remote_bucket,
        })
    }
    fn archive_path(&self, id: &str) -> Option<std::path::PathBuf> {
        self.0.archive_path(id)
    }
    async fn delete(&self, id: &str) -> Result<bool, ApiError> {
        self.0.delete(id).await.map_err(api_error)
    }
    fn cancel(&self) {
        self.0.cancel();
    }
}

/// `--revoke-keys-before`: RFC 3339 or Unix milliseconds.
pub fn parse_time(s: &str) -> Option<i64> {
    if let Ok(ms) = s.parse::<i64>() {
        return (ms >= 0).then_some(ms);
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.timestamp_millis())
}

fn fail(e: &BackupError) -> i32 {
    eprintln!("refused: {} ({})", e.message, e.code);
    1
}

/// The keys an archive is checked and its secrets opened with, as `rt secrets reseal` finds them.
fn keys(config: &Config, secrets: &NodeSecrets) -> anyhow::Result<Vec<MasterKey>> {
    let (master, previous) = crate::reseal::master_keys(config, secrets)?;
    Ok(std::iter::once(master).chain(previous).collect())
}

/// `rt backup …`. Returns the exit code.
pub fn run_offline(cmd: &BackupCommand, config: &Config, secrets: &NodeSecrets) -> i32 {
    match run(cmd, config, secrets) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{e:#}");
            1
        }
    }
}

fn run(cmd: &BackupCommand, config: &Config, secrets: &NodeSecrets) -> anyhow::Result<i32> {
    match cmd {
        BackupCommand::Verify {
            archive,
            no_auth,
            age_identity,
        } => {
            let id = match identity(age_identity.as_deref()) {
                Ok(id) => id,
                Err(e) => return Ok(fail(&e)),
            };
            let keys = if *no_auth { Vec::new() } else { keys(config, secrets)? };
            let auth = if *no_auth { Auth::None } else { Auth::Keys(&keys) };
            match verify_archive_age(archive, &auth, id.as_ref()) {
                Ok(r) => {
                    println!(
                        "ok: {} (rt {}), {} files, {} bytes, taken at {}",
                        r.id,
                        r.rt_version,
                        r.files,
                        r.bytes,
                        time(r.finished_at)
                    );
                    if !r.authenticated {
                        println!("warning: --no-auth: nothing shows who wrote this archive");
                    }
                    Ok(0)
                }
                Err(e) => Ok(fail(&e)),
            }
        }
        BackupCommand::Restore {
            archive,
            data_dir,
            no_auth,
            revoke_keys_before,
            age_identity,
        } => {
            let id = match identity(age_identity.as_deref()) {
                Ok(id) => id,
                Err(e) => return Ok(fail(&e)),
            };
            let revoke = match revoke_keys_before {
                Some(s) => match parse_time(s) {
                    Some(t) => Some(t),
                    None => anyhow::bail!("--revoke-keys-before: want RFC 3339 or Unix ms, got {s:?}"),
                },
                None => None,
            };
            // Without a key a restore can still run under --no-auth; nothing then opens secrets.
            let keys = match keys(config, secrets) {
                Ok(k) => k,
                Err(_) if *no_auth => Vec::new(),
                Err(e) => return Err(e),
            };
            let backend = keys.first().map(|m| crate::wiring::local_secrets(m, keys.get(1)));
            let opts = RestoreOptions {
                auth: if *no_auth { Auth::None } else { Auth::Keys(&keys) },
                revoke_keys_before: revoke,
                secrets: backend.as_ref().map(|b| b as &dyn rt_sec::SecretsBackend),
                now_ms: chrono::Utc::now().timestamp_millis(),
            };
            match restore_archive_age(archive, data_dir, &opts, id.as_ref()) {
                Ok(r) => {
                    print_restore(&r, data_dir, backend.is_some());
                    Ok(0)
                }
                Err(e) => Ok(fail(&e)),
            }
        }
    }
}

fn time(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).map_or_else(|| ms.to_string(), |t| t.to_rfc3339())
}

fn print_restore(r: &rt_backup::restore::RestoreReport, dir: &Path, opened: bool) {
    println!("restored {} into {}", r.id, dir.display());
    println!(
        "{} services, {} blobs, {} secrets; snapshot taken at {} ({} hours ago)",
        r.services,
        r.blobs,
        r.secrets,
        time(r.finished_at),
        r.snapshot_age_ms / 3_600_000
    );
    println!(
        "{} API keys are active, {} revoked now. Keys revoked and tenants disabled after the snapshot are live again:",
        r.active_keys, r.revoked_keys
    );
    println!("use --revoke-keys-before or rotate tenant keys, and bump the console session epoch.");
    if !r.authenticated {
        println!("warning: --no-auth: nothing shows who wrote this archive");
    }
    if !opened {
        println!("warning: no master key given: secrets were not checked");
    }
    for (service, name) in &r.unopened_secrets {
        println!("warning: secret {name} of service {service} does not open with the supplied master key");
    }
    println!("Put the master key in place before starting the node: an archive never holds it.");
}

#[cfg(test)]
mod tests {
    use super::parse_time;

    #[test]
    fn revoke_times_are_unix_ms_or_rfc3339() {
        assert_eq!(parse_time("1700000000000"), Some(1_700_000_000_000));
        assert_eq!(parse_time("1970-01-01T00:00:01Z"), Some(1000));
        assert_eq!(parse_time("-1"), None);
        assert_eq!(parse_time("yesterday"), None);
    }
}
