//! Node backups (docs/deploy.md "Backup and restore", SECURITY.md "Backups").
//!
//! - [`archive`]: the archive format: a tar with one zstd frame per database, `manifest.json` and
//!   `manifest.mac` last.
//! - [`untrusted`]: what an archive may contain. Shared with app import (R12): every archive is
//!   hostile input until its entries, paths and database rows have passed this module.
//! - [`create`]: taking a backup, on the backup's own low-priority thread.
//! - [`restore`]: offline verify and restore.
//! - [`manager`]: the one running backup, retention, schedule and status for the node.
//!
//! - [`encrypt`]: `age` around the stream of tar bytes, both ways. No plaintext archive on disk.
//! - [`s3`], [`remote`]: the S3 destination: client, commit protocol, retention.

pub mod app;
pub mod archive;
pub mod create;
pub mod encrypt;
pub mod manager;
pub mod remote;
pub mod restore;
pub mod s3;
pub mod untrusted;

/// A refused or failed backup operation: a stable code and a message that names no host path.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct BackupError {
    pub code: &'static str,
    pub message: String,
}

impl BackupError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"code": self.code, "message": self.message})
    }
}

impl From<std::io::Error> for BackupError {
    fn from(e: std::io::Error) -> Self {
        // The kind only: an io::Error can carry a path.
        Self::new("io_error", format!("file operation failed: {:?}", e.kind()))
    }
}

impl From<rusqlite::Error> for BackupError {
    fn from(e: rusqlite::Error) -> Self {
        let code = e
            .sqlite_error_code()
            .map_or("unknown".to_string(), |c| format!("{c:?}"));
        Self::new("corrupt", format!("a database failed: {code}"))
    }
}

impl From<rt_store::StoreError> for BackupError {
    fn from(e: rt_store::StoreError) -> Self {
        match e {
            rt_store::StoreError::Conflict(code) => Self::new(code, "the store refused the snapshot"),
            other => Self::new("store_error", other.code().to_string()),
        }
    }
}

pub type Result<T, E = BackupError> = std::result::Result<T, E>;
