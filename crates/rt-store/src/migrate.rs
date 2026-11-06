//! Run a version's SQL migrations on a service database: one `BEGIN IMMEDIATE`, every statement
//! prepared under the armed guest authorizer. A failure leaves the database as it was.

use std::time::Duration;

use rt_types::{
    file_of, plan_migrations, AppliedMigration, MigrationEntry, MigrationFailure, MigrationFile, VersionNo,
};
use rusqlite::{params, Connection, ErrorCode};

use crate::data::{map_sql_err, precheck, sanitize, skip_comments, Conn, RollbackOnUnwind};
use crate::error::{Result, StoreError};
use crate::sqlutil::now_ms;

/// Longest `message` of a failure.
const MAX_MESSAGE: usize = 512;
/// `SQLITE_AUTH`.
const SQLITE_AUTH: i32 = 23;

pub(crate) fn read_applied(c: &Connection) -> Result<Vec<AppliedMigration>> {
    let mut st = c
        .prepare_cached("SELECT name, sha256, applied_at, version FROM _rt_migrations ORDER BY name")
        .map_err(map_sql_err)?;
    let rows = st
        .query_map([], |r| {
            Ok(AppliedMigration {
                name: r.get(0)?,
                sha256: r.get(1)?,
                applied_at_ms: r.get(2)?,
                version: r.get(3)?,
            })
        })
        .map_err(map_sql_err)?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(map_sql_err)
}

/// Byte ranges of the statements of `sql`, comments and strings understood. A `;` ends a statement
/// except inside a `CREATE TRIGGER`, which ends at the `END;` closing its `BEGIN` (`CASE … END` does
/// not count). Text after the last `;` is a statement too; blank ranges are left out.
pub(crate) fn split_statements(sql: &str) -> Vec<(usize, usize)> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i) = (0, 0);
    let (mut words, mut trigger, mut cases, mut last_end) = (0u32, false, 0u32, false);
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' | b'[' => {
                let close = if b[i] == b'[' { b']' } else { b[i] };
                i += 1;
                while i < b.len() {
                    if b[i] == close {
                        if close != b']' && b.get(i + 1) == Some(&close) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
                i += 1;
                last_end = false;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(b.len());
            }
            b';' => {
                i += 1;
                if !trigger || last_end {
                    out.push((start, i));
                    start = i;
                    (words, trigger, cases, last_end) = (0, false, 0, false);
                }
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let from = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &sql[from..i];
                words += 1;
                last_end = false;
                if words <= 3 && word.eq_ignore_ascii_case("trigger") {
                    trigger = true;
                } else if words == 1 && !word.eq_ignore_ascii_case("create") {
                    words = u32::MAX / 2; // not a CREATE: never a trigger
                } else if trigger && word.eq_ignore_ascii_case("case") {
                    cases += 1;
                } else if trigger && word.eq_ignore_ascii_case("end") {
                    if cases > 0 {
                        cases -= 1;
                    } else {
                        last_end = true;
                    }
                }
            }
            c => {
                if !c.is_ascii_whitespace() {
                    last_end = false;
                }
                i += 1;
            }
        }
    }
    if start < b.len() {
        out.push((start, b.len()));
    }
    out.retain(|&(s, e)| !skip_comments(&sql[s..e]).is_empty());
    out
}

fn line_at(sql: &str, offset: usize) -> u32 {
    let end = offset.min(sql.len());
    let lines = sql.as_bytes()[..end].iter().filter(|c| **c == b'\n').count();
    u32::try_from(lines).unwrap_or(u32::MAX).saturating_add(1)
}

fn is_transaction_control(text: &str) -> bool {
    let rest = skip_comments(text);
    let word = rest.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    ["begin", "commit", "end", "rollback", "savepoint", "release"]
        .iter()
        .any(|k| word.eq_ignore_ascii_case(k))
}

fn bounded(mut msg: String) -> String {
    if msg.len() > MAX_MESSAGE {
        let mut end = MAX_MESSAGE;
        while !msg.is_char_boundary(end) {
            end -= 1;
        }
        msg.truncate(end);
    }
    msg
}

/// Where and why one statement failed. `start` is the offset of its first token in the file, `chunk`
/// its range start: a syntax error's offset is relative to the text that was prepared.
fn failure(
    file: &MigrationFile,
    index: u32,
    chunk: usize,
    start: usize,
    text: &str,
    e: rusqlite::Error,
) -> MigrationFailure {
    let (code, extended, msg, offset) = match &e {
        rusqlite::Error::SqliteFailure(f, m) => (
            Some(f.code),
            Some(f.extended_code),
            m.clone().unwrap_or_else(|| f.to_string()),
            None,
        ),
        rusqlite::Error::SqlInputError { error, msg, offset, .. } => (
            Some(error.code),
            Some(error.extended_code),
            msg.clone(),
            usize::try_from(*offset).ok(),
        ),
        other => (None, None, other.to_string(), None),
    };
    let denied = code == Some(ErrorCode::AuthorizationForStatementDenied) || msg.starts_with("not authorized");
    let code_str = if denied && is_transaction_control(text) {
        "transaction_control_forbidden"
    } else if code == Some(ErrorCode::OperationInterrupted) {
        "migration_timeout"
    } else {
        "migration_failed"
    };
    let line = line_at(&file.sql, offset.map_or(start, |o| chunk + o));
    MigrationFailure {
        code: code_str,
        file: Some(file_of(&file.name)),
        statement_index: Some(index),
        line: Some(line),
        sqlite_code: extended,
        message: bounded(sanitize(&msg)),
    }
}

fn run_file(c: &Connection, file: &MigrationFile) -> std::result::Result<(), MigrationFailure> {
    for (index, (from, to)) in split_statements(&file.sql).into_iter().enumerate() {
        let index = u32::try_from(index).unwrap_or(u32::MAX);
        let text = &file.sql[from..to];
        let start = from + (text.len() - skip_comments(text).len());
        let fail = |e| failure(file, index, from, start, text, e);
        if precheck(text).is_err() {
            return Err(MigrationFailure {
                code: "migration_failed",
                file: Some(file_of(&file.name)),
                statement_index: Some(index),
                line: Some(line_at(&file.sql, start)),
                sqlite_code: Some(SQLITE_AUTH),
                message: "not authorized".into(),
            });
        }
        // Never the statement cache: each text meets the authorizer here, in this transaction.
        let mut st = c.prepare(text).map_err(fail)?;
        let mut rows = st.raw_query();
        loop {
            match rows.next() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => return Err(fail(e)),
            }
        }
    }
    Ok(())
}

/// Plan and run under one write transaction; `commit: false` rolls back (a dry run on a copy).
pub(crate) fn run(
    conn: &Conn,
    files: &[MigrationFile],
    version: VersionNo,
    budget: Duration,
    commit: bool,
) -> Result<Vec<MigrationEntry>> {
    conn.c.execute_batch("BEGIN IMMEDIATE").map_err(map_sql_err)?;
    let _unwind = RollbackOnUnwind(&conn.c);
    let res = apply(conn, files, version, budget);
    conn.disarm();
    let res = res.and_then(|done| {
        if commit {
            conn.c.execute_batch("COMMIT").map_err(map_sql_err)?;
        }
        Ok(done)
    });
    if !conn.c.is_autocommit() {
        let _ = conn.c.execute_batch("ROLLBACK");
    }
    res
}

fn apply(conn: &Conn, files: &[MigrationFile], version: VersionNo, budget: Duration) -> Result<Vec<MigrationEntry>> {
    let entries: Vec<MigrationEntry> = files.iter().map(MigrationFile::entry).collect();
    let applied = read_applied(&conn.c)?;
    let pending = plan_migrations(&entries, &applied).map_err(|f| StoreError::Migration(Box::new(f)))?;
    if pending.is_empty() {
        return Ok(pending);
    }
    let todo: Vec<&MigrationFile> = files
        .iter()
        .filter(|f| pending.iter().any(|p| p.name == f.name))
        .collect();
    conn.with_guest_budget(budget, |c| todo.iter().try_for_each(|f| run_file(c, f)))
        .map_err(|f| StoreError::Migration(Box::new(f)))?;
    conn.disarm();
    // The authorizer is off again: this one statement writes the reserved table.
    let now = now_ms();
    for p in &pending {
        conn.c
            .execute(
                "INSERT INTO _rt_migrations (name, sha256, applied_at, version) VALUES (?1, ?2, ?3, ?4)",
                params![p.name, p.sha256, now, version.0],
            )
            .map_err(map_sql_err)?;
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(sql: &str) -> Vec<&str> {
        split_statements(sql)
            .into_iter()
            .map(|(s, e)| sql[s..e].trim())
            .collect()
    }

    #[test]
    fn statements_split_on_semicolons_outside_strings_and_comments() {
        assert_eq!(parts("A; B"), ["A;", "B"]);
        assert_eq!(
            parts("A 'x;y' \"p;q\" [r;s] `t;u`;B;"),
            ["A 'x;y' \"p;q\" [r;s] `t;u`;", "B;"]
        );
        assert_eq!(parts("A 'it''s;';"), ["A 'it''s;';"]);
        assert_eq!(parts("A -- x;y\n z; /* p;q */ B;"), ["A -- x;y\n z;", "/* p;q */ B;"]);
        assert!(parts("  ;; -- only\n /* a */ ").is_empty());
    }

    #[test]
    fn a_trigger_ends_at_its_own_end() {
        let t = "CREATE TEMP TRIGGER t AFTER INSERT ON a BEGIN SELECT CASE WHEN 1 THEN 2 END; DELETE FROM a; END; SELECT 1;";
        assert_eq!(parts(t).len(), 2);
        assert_eq!(parts("SELECT 'trigger'; SELECT 1;").len(), 2);
        assert_eq!(parts("DROP TRIGGER t; SELECT 1;").len(), 2);
    }

    #[test]
    fn unterminated_text_does_not_panic() {
        for s in ["'abc", "[abc", "A 'é", "A /* abc", "A -- abc"] {
            assert_eq!(parts(s).len(), 1, "{s}");
        }
        assert!(parts("/* abc").is_empty() && parts("-- abc").is_empty());
    }
}
