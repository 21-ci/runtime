//! The S3 destination: how an archive is committed there, and how long it stays.
//!
//! An object counts only once `<key>.complete` exists. That marker and `<key>.sha256` are transport
//! reliability, nothing more: a party with write access to the bucket can write them too, and
//! `manifest.mac` is what tells a real archive from a forged one.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use bytes::Bytes;
use chrono::{DateTime, Datelike};

use crate::s3::{S3Config, S3Session};
use crate::{BackupError, Result};

/// An object with no marker this old is a dead upload, not one in progress.
pub const STALE_INCOMPLETE_MS: i64 = 7 * 24 * 3600 * 1000;

const SHA256: &str = ".sha256";
const COMPLETE: &str = ".complete";

/// `[backup.s3]`.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub s3: S3Config,
    /// Archives kept on the local disk once uploaded.
    pub keep_local: u32,
    pub retention: RetentionPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub keep_last: u32,
    pub keep_daily: u32,
    pub keep_weekly: u32,
}

/// Which of `(id, time ms)` a policy keeps. The newest is always among them, whatever the policy.
pub fn keep_set(items: &[(String, i64)], policy: &RetentionPolicy) -> HashSet<String> {
    let mut sorted: Vec<&(String, i64)> = items.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
    let mut keep: HashSet<String> = HashSet::new();
    if let Some((id, _)) = sorted.first() {
        keep.insert(id.clone());
    }
    for (id, _) in sorted.iter().take(policy.keep_last as usize) {
        keep.insert(id.clone());
    }
    // The newest of each of the last N days, and weeks, that have a backup.
    let mut days = HashSet::new();
    let mut weeks = HashSet::new();
    for (id, t) in sorted {
        let Some(at) = DateTime::from_timestamp_millis(*t) else {
            continue;
        };
        let (day, week) = (at.date_naive(), at.iso_week());
        if days.len() < policy.keep_daily as usize && days.insert(day) {
            keep.insert(id.clone());
        }
        if weeks.len() < policy.keep_weekly as usize && weeks.insert((week.year(), week.week())) {
            keep.insert(id.clone());
        }
    }
    keep
}

/// `(id)` of a data object's name: `rt-backup-<id>.tar[.age]`. Anything else is not ours.
fn id_of(name: &str) -> Option<&str> {
    let id = name
        .strip_prefix("rt-backup-")?
        .strip_suffix(".tar.age")
        .or_else(|| name.strip_prefix("rt-backup-")?.strip_suffix(".tar"))?;
    crate::manager::id_ok(id).then_some(id)
}

fn time_of(id: &str) -> Option<i64> {
    let u = ulid::Ulid::from_string(id.strip_prefix("bkp_")?).ok()?;
    i64::try_from(u.timestamp_ms()).ok()
}

/// Upload an archive and commit it: object, size check, sidecar, read-back, marker last.
pub async fn upload_archive(session: &S3Session<'_>, key: &str, path: &Path, sha256: &str) -> Result<()> {
    let size = session.upload_file(key, path).await?;
    match session.head(key).await? {
        Some(n) if n == size => {}
        _ => {
            return Err(BackupError::new(
                "size_mismatch",
                "the uploaded object does not have the archive's size",
            ))
        }
    }
    let name = key.rsplit('/').next().unwrap_or(key);
    let line = format!("{sha256}  {name}\n");
    let sidecar = format!("{key}{SHA256}");
    session.put(&sidecar, Bytes::from(line.clone())).await?;
    if session.get_small(&sidecar).await?.as_deref() != Some(line.as_bytes()) {
        return Err(BackupError::new(
            "digest_mismatch",
            "the uploaded sidecar does not read back",
        ));
    }
    session.put(&format!("{key}{COMPLETE}"), Bytes::from(line)).await
}

/// Delete one backup's objects: the marker first, so that a half-done delete is never `complete`.
pub async fn delete_objects(session: &S3Session<'_>, key: &str) -> Result<()> {
    session.delete(&format!("{key}{COMPLETE}")).await?;
    session.delete(key).await?;
    session.delete(&format!("{key}{SHA256}")).await
}

/// What a remote retention pass removed.
#[derive(Debug, Default)]
pub struct Pruned {
    pub ids: Vec<String>,
}

/// Apply the policy to complete objects under the prefix. Objects without a marker are not counted;
/// only stale ones are deleted, as a fresh one may be an upload in progress. Keys that are not
/// exactly a backup's stay: the bucket may be shared. So do backups outside `owned`, this node's
/// rows: with a copied config, another node may write to the same prefix.
pub async fn apply_retention(
    session: &S3Session<'_>,
    prefix: &str,
    policy: &RetentionPolicy,
    now_ms: i64,
    owned: &HashSet<String>,
) -> Result<Pruned> {
    let keys = session.list(prefix).await?;
    let present: HashSet<&str> = keys.iter().map(String::as_str).collect();
    // By id, every variant: a backup may be there as `.tar` and as `.tar.age`.
    let mut objects: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for key in &keys {
        // Also an orphan marker or sidecar names a backup.
        let base = key
            .strip_suffix(COMPLETE)
            .or_else(|| key.strip_suffix(SHA256))
            .unwrap_or(key);
        let Some(name) = base.strip_prefix(prefix) else {
            continue;
        };
        if let Some(id) = id_of(name).filter(|id| owned.contains(*id)) {
            objects.entry(id.to_string()).or_default().insert(base.to_string());
        }
    }
    let mut complete = Vec::new();
    let mut doomed = Vec::new();
    for (id, bases) in &objects {
        let Some(t) = time_of(id) else { continue };
        let done = bases
            .iter()
            .any(|base| present.contains(base.as_str()) && present.contains(format!("{base}{COMPLETE}").as_str()));
        if done {
            complete.push((id.clone(), t));
        } else if now_ms.saturating_sub(t) > STALE_INCOMPLETE_MS {
            doomed.push(id.clone());
        }
    }
    let keep = keep_set(&complete, policy);
    doomed.extend(
        complete
            .iter()
            .filter(|(id, _)| !keep.contains(id))
            .map(|(id, _)| id.clone()),
    );
    let mut pruned = Pruned::default();
    for id in doomed {
        if let Some(bases) = objects.get(&id) {
            for base in bases {
                delete_objects(session, base).await?;
            }
            pruned.ids.push(id);
        }
    }
    Ok(pruned)
}
