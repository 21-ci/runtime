//! SQL migrations of a version: what is checked at upload and what decides what runs at a promote.
//!
//! Names are the file stem, `NNNN_name`; four digits make the order of names the order of numbers.
//! The list of a version comes only from the validator or the write-once WASM route, never from
//! `versions.meta`, which clients can write.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::digest::Digest;

/// Files per version.
pub const MAX_MIGRATION_FILES: usize = 256;
/// SQL bytes per version, all files together. They are kept in `meta.db`.
pub const MAX_MIGRATION_BYTES: usize = 900 * 1024;

/// One migration a version carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationEntry {
    pub name: String,
    /// `sha256:<hex>` of the SQL text.
    pub sha256: String,
}

/// [`MigrationEntry`] with its SQL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationFile {
    pub name: String,
    pub sha256: String,
    pub sql: String,
}

impl MigrationFile {
    pub fn entry(&self) -> MigrationEntry {
        MigrationEntry {
            name: self.name.clone(),
            sha256: self.sha256.clone(),
        }
    }
}

/// One row of `_rt_migrations`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub name: String,
    pub sha256: String,
    pub applied_at_ms: i64,
    pub version: u32,
}

/// Why a migration did not run. Everything but `code` is optional in the error's `details`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MigrationFailure {
    /// `migration_failed`, `migration_drift`, `migration_out_of_order`, `migration_timeout` or
    /// `transaction_control_forbidden`.
    #[serde(skip)]
    pub code: &'static str,
    /// `NNNN_name.sql`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Which statement of the file, from 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_index: Option<u32>,
    /// From 1: where a syntax error is, otherwise where the statement starts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    /// SQLite's extended result code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlite_code: Option<i32>,
    /// Sanitized: no host paths.
    pub message: String,
}

impl MigrationFailure {
    fn of_file(code: &'static str, name: &str, message: String) -> Self {
        Self {
            code,
            file: Some(file_of(name)),
            statement_index: None,
            line: None,
            sqlite_code: None,
            message,
        }
    }
}

/// The file a migration name came from.
pub fn file_of(name: &str) -> String {
    format!("{name}.sql")
}

/// A set of migration files that cannot be accepted, naming the offending file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationSetError {
    pub file: String,
    pub message: String,
}

fn valid_stem(stem: &str) -> bool {
    let b = stem.as_bytes();
    b.len() >= 6
        && b.len() <= 128
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'_'
        && b[5..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'-')
}

/// Check files named `NNNN_name.sql` (or the bare stem) and return them in order.
pub fn build_migrations<'a>(
    inputs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<MigrationFile>, MigrationSetError> {
    let mut by_name: BTreeMap<String, MigrationFile> = BTreeMap::new();
    let mut numbers = BTreeSet::new();
    let mut total = 0usize;
    for (raw, sql) in inputs {
        let refuse = |message: String| MigrationSetError {
            file: raw.to_string(),
            message,
        };
        let stem = raw.strip_suffix(".sql").unwrap_or(raw);
        if !valid_stem(stem) {
            return Err(refuse(
                "a migration is named NNNN_name.sql (four digits, then letters, digits, `_` or `-`)".into(),
            ));
        }
        if !numbers.insert(stem[..4].to_string()) {
            return Err(refuse(format!("another migration has the number {}", &stem[..4])));
        }
        total = total.saturating_add(sql.len());
        if total > MAX_MIGRATION_BYTES {
            return Err(refuse(format!(
                "the migrations of a version exceed {MAX_MIGRATION_BYTES} bytes"
            )));
        }
        if by_name.len() == MAX_MIGRATION_FILES {
            return Err(refuse(format!(
                "a version has at most {MAX_MIGRATION_FILES} migrations"
            )));
        }
        by_name.insert(
            stem.to_string(),
            MigrationFile {
                name: stem.to_string(),
                sha256: Digest::of(sql.as_bytes()).to_string(),
                sql: sql.to_string(),
            },
        );
    }
    Ok(by_name.into_values().collect())
}

/// What of `target` has to run, in order. Runs nothing: an applied name whose hash changed is
/// `migration_drift`, a pending name before the newest applied one is `migration_out_of_order`.
/// Applied names `target` lacks are fine, since an older version can go live again.
pub fn plan_migrations(
    target: &[MigrationEntry],
    applied: &[AppliedMigration],
) -> Result<Vec<MigrationEntry>, MigrationFailure> {
    let by_name: BTreeMap<&str, &AppliedMigration> = applied.iter().map(|a| (a.name.as_str(), a)).collect();
    for t in target {
        if by_name.get(t.name.as_str()).is_some_and(|a| a.sha256 != t.sha256) {
            return Err(MigrationFailure::of_file(
                "migration_drift",
                &t.name,
                "this migration was applied with different content".into(),
            ));
        }
    }
    let newest = by_name.keys().next_back().copied();
    let pending: Vec<MigrationEntry> = target
        .iter()
        .filter(|t| !by_name.contains_key(t.name.as_str()))
        .cloned()
        .collect();
    if let (Some(first), Some(newest)) = (pending.first(), newest) {
        if first.name.as_str() < newest {
            return Err(MigrationFailure::of_file(
                "migration_out_of_order",
                &first.name,
                format!("this migration sorts before the applied {newest}"),
            ));
        }
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, sql: &str) -> MigrationEntry {
        MigrationEntry {
            name: name.into(),
            sha256: Digest::of(sql.as_bytes()).to_string(),
        }
    }

    fn applied(name: &str, sql: &str) -> AppliedMigration {
        AppliedMigration {
            name: name.into(),
            sha256: Digest::of(sql.as_bytes()).to_string(),
            applied_at_ms: 1,
            version: 1,
        }
    }

    #[test]
    fn files_come_back_in_order_with_the_bare_stem_and_hash() {
        let got = build_migrations([("0002_b.sql", "b"), ("0001_a", "a")]).expect("valid");
        assert_eq!(got[0].name, "0001_a");
        assert_eq!(got[1].name, "0002_b");
        assert_eq!(got[0].sha256, Digest::of(b"a").to_string());
    }

    #[test]
    fn bad_names_duplicate_numbers_and_oversize_sets_are_refused() {
        for bad in [
            "1_a.sql",
            "00001_a.sql",
            "0001_.sql",
            "0001_a/b.sql",
            "../0001_a.sql",
            "0001 a.sql",
            "abcd_e.sql",
        ] {
            assert!(build_migrations([(bad, "x")]).is_err(), "{bad}");
        }
        let dup = build_migrations([("0001_a.sql", "a"), ("0001_b.sql", "b")]).expect_err("dup");
        assert_eq!(dup.file, "0001_b.sql");
        let big = "x".repeat(MAX_MIGRATION_BYTES + 1);
        assert!(build_migrations([("0001_a.sql", big.as_str())]).is_err());
        let names: Vec<String> = (0..=MAX_MIGRATION_FILES).map(|i| format!("{i:04}_a")).collect();
        assert!(build_migrations(names.iter().map(|n| (n.as_str(), ""))).is_err());
    }

    #[test]
    fn the_plan_runs_only_what_is_new() {
        let t = [entry("0001_a", "a"), entry("0002_b", "b")];
        assert_eq!(plan_migrations(&t, &[]).expect("plan").len(), 2);
        let got = plan_migrations(&t, &[applied("0001_a", "a")]).expect("plan");
        assert_eq!(got, vec![entry("0002_b", "b")]);
    }

    #[test]
    fn a_changed_hash_is_drift_and_an_earlier_pending_name_is_out_of_order() {
        let drift = plan_migrations(&[entry("0001_a", "changed")], &[applied("0001_a", "a")]).expect_err("drift");
        assert_eq!(
            (drift.code, drift.file.as_deref()),
            ("migration_drift", Some("0001_a.sql"))
        );
        let order = plan_migrations(&[entry("0001_a", "a"), entry("0003_c", "c")], &[applied("0003_c", "c")])
            .expect_err("order");
        assert_eq!(order.code, "migration_out_of_order");
    }

    /// Rollback: an older target misses applied names, and nothing runs or is refused.
    #[test]
    fn an_older_target_missing_applied_names_has_nothing_pending() {
        let got = plan_migrations(
            &[entry("0001_a", "a")],
            &[applied("0001_a", "a"), applied("0002_b", "b")],
        )
        .expect("plan");
        assert!(got.is_empty());
    }
}
