//! The `backups` table of `meta.db`: the record of node backups (`rt-backup`).

use rusqlite::{params, OptionalExtension, Row};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::meta::{Page, PageReq, SqliteMetaStore};
use crate::sqlutil::{bad_col, page_limit, paginate};

const COLS: &str = "id, state, trigger, started_at, finished_at, bytes, sha256, encrypted, local_path, remote_key, \
                    verified_at, rt_version, failure";

/// One backup run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackupRow {
    pub id: String,
    /// `running`, `verifying`, `ok`, `failed` (`uploading` is for remote destinations).
    pub state: String,
    /// `schedule` or `api`.
    pub trigger: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub bytes: Option<u64>,
    pub sha256: Option<String>,
    pub encrypted: bool,
    pub local_path: Option<String>,
    pub remote_key: Option<String>,
    pub verified_at: Option<i64>,
    pub rt_version: String,
    pub failure: Option<serde_json::Value>,
}

fn row(r: &Row<'_>) -> rusqlite::Result<BackupRow> {
    let failure: Option<String> = r.get(12)?;
    Ok(BackupRow {
        id: r.get(0)?,
        state: r.get(1)?,
        trigger: r.get(2)?,
        started_at: r.get(3)?,
        finished_at: r.get(4)?,
        bytes: r.get::<_, Option<i64>>(5)?.map(|b| b.max(0) as u64),
        sha256: r.get(6)?,
        encrypted: r.get::<_, i64>(7)? != 0,
        local_path: r.get(8)?,
        remote_key: r.get(9)?,
        verified_at: r.get(10)?,
        rt_version: r.get(11)?,
        failure: failure
            .map(|f| serde_json::from_str(&f).map_err(|_| bad_col(12, "failure")))
            .transpose()?,
    })
}

impl SqliteMetaStore {
    pub async fn backup_insert(&self, b: BackupRow) -> Result<()> {
        self.db
            .call(move |c| {
                c.execute(
                    "INSERT INTO backups (id, state, trigger, started_at, rt_version, local_path, encrypted) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        b.id,
                        b.state,
                        b.trigger,
                        b.started_at,
                        b.rt_version,
                        b.local_path,
                        b.encrypted as i64
                    ],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn backup_set_state(&self, id: &str, state: &'static str) -> Result<()> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                c.execute("UPDATE backups SET state = ?2 WHERE id = ?1", params![id, state])?;
                Ok(())
            })
            .await
    }

    /// The run ended well: its archive is at `local_path` and passed verification.
    pub async fn backup_mark_ok(
        &self,
        id: &str,
        at: i64,
        bytes: u64,
        sha256: String,
        local_path: String,
    ) -> Result<()> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE backups SET state = 'ok', finished_at = ?2, verified_at = ?2, bytes = ?3, sha256 = ?4, \
                     local_path = ?5, failure = NULL WHERE id = ?1",
                    params![id, at, bytes as i64, sha256, local_path],
                )?;
                Ok(())
            })
            .await
    }

    /// The archive is committed to remote storage under `key`.
    pub async fn backup_set_remote(&self, id: &str, key: String) -> Result<()> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                c.execute("UPDATE backups SET remote_key = ?2 WHERE id = ?1", params![id, key])?;
                Ok(())
            })
            .await
    }

    /// The local copy of an uploaded archive is gone; the row stays while the remote one does.
    pub async fn backup_clear_local(&self, id: &str) -> Result<()> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                c.execute("UPDATE backups SET local_path = NULL WHERE id = ?1", params![id])?;
                Ok(())
            })
            .await
    }

    /// `failure` is a stable `{code, message}`; its message names no host path.
    pub async fn backup_mark_failed(&self, id: &str, at: i64, failure: serde_json::Value) -> Result<()> {
        let id = id.to_string();
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE backups SET state = 'failed', finished_at = ?2, failure = ?3, local_path = NULL WHERE id = ?1",
                    params![id, at, failure.to_string()],
                )?;
                Ok(())
            })
            .await
    }

    /// Runs a previous process left unfinished; returns how many. Startup only.
    pub async fn backup_fail_unfinished(&self, at: i64) -> Result<u64> {
        self.db
            .call(move |c| {
                Ok(c.execute(
                    "UPDATE backups SET state = 'failed', finished_at = ?1, local_path = NULL, \
                     failure = '{\"code\":\"interrupted\",\"message\":\"the node stopped during the backup\"}' \
                     WHERE state IN ('running', 'uploading', 'verifying')",
                    params![at],
                )? as u64)
            })
            .await
    }

    pub async fn backup_get(&self, id: &str) -> Result<Option<BackupRow>> {
        let id = id.to_string();
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!("SELECT {COLS} FROM backups WHERE id = ?1"))?
                    .query_row([id], row)
                    .optional()?)
            })
            .await
    }

    /// Newest first; the cursor is the last id returned.
    pub async fn backup_list(&self, page: PageReq) -> Result<Page<BackupRow>> {
        self.db
            .read(move |c| {
                let limit = page_limit(page.limit);
                let after = page.cursor.unwrap_or_else(|| "bkp_~".to_string());
                let rows = c
                    .prepare_cached(&format!(
                        "SELECT {COLS} FROM backups WHERE id < ?1 ORDER BY id DESC LIMIT ?2"
                    ))?
                    .query_map(params![after, limit as i64 + 1], row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(paginate(rows, limit, |b| b.id.clone()))
            })
            .await
    }

    pub async fn backup_delete(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.db
            .call(move |c| Ok(c.execute("DELETE FROM backups WHERE id = ?1", [id])? > 0))
            .await
    }

    /// The newest finished good run.
    pub async fn backup_last_ok(&self) -> Result<Option<BackupRow>> {
        self.db
            .read(|c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {COLS} FROM backups WHERE state = 'ok' ORDER BY finished_at DESC LIMIT 1"
                ))?
                .query_row([], row)
                .optional()?)
            })
            .await
    }

    /// The newest failed run, when it is newer than the last good one.
    pub async fn backup_last_failure(&self) -> Result<Option<BackupRow>> {
        self.db
            .read(|c| {
                Ok(
                    c.prepare_cached(&format!("SELECT {COLS} FROM backups ORDER BY id DESC LIMIT 1"))?
                        .query_row([], row)
                        .optional()?
                        .filter(|b| b.state == "failed"),
                )
            })
            .await
    }

    /// Finished good runs past the newest `keep`, oldest first, for retention.
    pub async fn backup_old_ok(&self, keep: u32) -> Result<Vec<BackupRow>> {
        self.db
            .read(move |c| {
                Ok(c.prepare_cached(&format!(
                    "SELECT {COLS} FROM backups WHERE state = 'ok' ORDER BY finished_at DESC LIMIT -1 OFFSET ?1"
                ))?
                .query_map([keep], row)?
                .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await
    }

    /// When this data directory was last restored from a backup (the `node.restored` event). Walks
    /// only events with no service, which `events_service` indexes.
    pub async fn last_restored_at(&self) -> Result<Option<i64>> {
        self.db
            .read(|c| {
                Ok(c.prepare_cached(
                    "SELECT ts FROM events WHERE service_id IS NULL AND kind = 'node.restored' ORDER BY id DESC LIMIT 1",
                )?
                .query_row([], |r| r.get(0))
                .optional()?)
            })
            .await
    }
}
