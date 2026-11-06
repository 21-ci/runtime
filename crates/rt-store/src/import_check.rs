//! Is the schema of a `data.db` from somewhere else one this node may open (R12)?
//!
//! The runtime's own objects must be byte for byte what this binary creates, and nothing else may
//! touch them. A guest's own objects may stay, since guests can make them, but each is replayed on
//! an empty in-memory database under the guest authorizer: a statement the authorizer would deny
//! to a guest is refused here, before any of it could ever fire.

use std::collections::{BTreeSet, HashMap};

use rusqlite::{Connection, ErrorCode};

use crate::data::{guest_auth, set_guest_limits, BLOB_BOOTSTRAP, DENIED_FUNCTIONS, SERVICE_DATA_BOOTSTRAP};
use crate::Result;

/// Virtual table modules a bundle may use. Others (`dbstat`, `fts3` with its pointer tokenizers…)
/// have no business in imported data.
const ALLOWED_MODULES: [&str; 2] = ["fts5", "rtree"];
/// SQLite's own tables that may appear.
const ALLOWED_SYSTEM: [&str; 3] = ["sqlite_sequence", "sqlite_stat1", "sqlite_stat4"];

struct Object {
    kind: String,
    name: String,
    table: String,
    rootpage: i64,
    sql: Option<String>,
}

fn read_objects(c: &Connection) -> rusqlite::Result<Vec<Object>> {
    c.prepare("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master ORDER BY rowid")?
        .query_map([], |r| {
            Ok(Object {
                kind: r.get(0)?,
                name: r.get(1)?,
                table: r.get(2)?,
                rootpage: r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                sql: r.get(4)?,
            })
        })?
        .collect()
}

fn reserved_name(name: &str) -> bool {
    name.get(..4).is_some_and(|p| p.eq_ignore_ascii_case("_rt_"))
}

fn module_of(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let mut words = lower.split_whitespace();
    words.find(|w| *w == "using")?;
    let module = words.next()?;
    Some(module.split('(').next().unwrap_or(module).to_string())
}

/// SQLite's code and message; prepare-time failures of a statement carry its offset in another shape.
fn failure_of(e: &rusqlite::Error) -> Option<(ErrorCode, &str)> {
    match e {
        rusqlite::Error::SqliteFailure(f, msg) => Some((f.code, msg.as_deref().unwrap_or_default())),
        rusqlite::Error::SqlInputError { error, msg, .. } => Some((error.code, msg)),
        _ => None,
    }
}

fn is_auth_error(e: &rusqlite::Error) -> bool {
    // A denied function is a plain SQLITE_ERROR ("not authorized to use function: …").
    failure_of(e)
        .is_some_and(|(code, m)| code == ErrorCode::AuthorizationForStatementDenied || m.starts_with("not authorized"))
}

/// A body reaching a function it may not, as SQLite says it.
///
/// Reading a view first works out its columns with the authorizer **switched off**
/// (`sqlite3ViewGetColumnNames`), so a `SQLITE_DIRECTONLY` function in a view (or in anything the
/// view or a trigger reads) stops the compile right there with "unsafe use of f()", and the
/// authorizer's function callback never fires. `load_extension` and `fts3_tokenizer` are both such
/// functions. A function this build lacks is no business of imported data either.
fn is_refused_body(e: &rusqlite::Error) -> bool {
    is_auth_error(e)
        || failure_of(e).is_some_and(|(_, m)| m.starts_with("unsafe use of") || m.starts_with("no such function"))
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn arm(c: &Connection) -> rusqlite::Result<()> {
    c.authorizer(Some(|ctx: rusqlite::hooks::AuthContext<'_>| guest_auth(&ctx)))
}

fn disarm(c: &Connection) -> rusqlite::Result<()> {
    c.authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>)
}

fn exists(c: &Connection, name: &str) -> rusqlite::Result<bool> {
    disarm(c)?;
    let n: i64 = c.query_row("SELECT count(*) FROM sqlite_master WHERE name = ?1", [name], |r| {
        r.get(0)
    })?;
    arm(c)?;
    Ok(n > 0)
}

/// Compile what would fire a trigger, or read a view, without running it: only then does SQLite
/// hand the bodies to the authorizer.
fn rehearse(m: &Connection) -> Result<Option<&'static str>> {
    disarm(m)?;
    let objects = read_objects(m)?;
    let mut targets = BTreeSet::new();
    let mut statements = Vec::new();
    for o in &objects {
        match o.kind.as_str() {
            "view" if !reserved_name(&o.name) => statements.push(format!("SELECT * FROM {}", quoted(&o.name))),
            "trigger" if !reserved_name(&o.table) => {
                targets.insert(o.table.clone());
            }
            _ => {}
        }
    }
    for t in targets {
        let q = quoted(&t);
        statements.push(format!("DELETE FROM {q}"));
        statements.push(format!("INSERT INTO {q} DEFAULT VALUES"));
        let columns = m
            .prepare(&format!(
                "SELECT name FROM pragma_table_info({})",
                quoted(&t).replace('"', "'")
            ))?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !columns.is_empty() {
            let sets: Vec<String> = columns.iter().map(|c| format!("{0} = {0}", quoted(c))).collect();
            statements.push(format!("UPDATE {q} SET {}", sets.join(", ")));
        }
    }
    arm(m)?;
    for sql in statements {
        if let Err(e) = m.prepare(&sql) {
            // Anything else (no such column in a dangling trigger) just fails for the guest too, as
            // it would have on the node it came from.
            if is_refused_body(&e) {
                return Ok(Some("a trigger or view does what a guest may not"));
            }
        }
    }
    Ok(None)
}

/// Why the schema behind `c` may not be imported, if so. `c` is only read.
pub fn schema_problem(c: &Connection) -> Result<Option<&'static str>> {
    let m = Connection::open_in_memory()?;
    m.execute_batch(SERVICE_DATA_BOOTSTRAP)?;
    m.execute_batch(BLOB_BOOTSTRAP)?;
    let reserved: HashMap<String, Object> = read_objects(&m)?.into_iter().map(|o| (o.name.clone(), o)).collect();
    let mut guest = Vec::new();
    for o in read_objects(c)? {
        if reserved_name(&o.name) || reserved_name(&o.table) {
            let Some(r) = reserved.get(&o.name) else {
                return Ok(Some("the database holds an unknown runtime object"));
            };
            if r.kind != o.kind || r.table != o.table || r.sql != o.sql {
                return Ok(Some("the definition of a runtime table was changed"));
            }
            continue;
        }
        if o.name.to_ascii_lowercase().starts_with("sqlite_") {
            let autoindex = o.kind == "index" && o.sql.is_none();
            if !autoindex && !ALLOWED_SYSTEM.contains(&o.name.to_ascii_lowercase().as_str()) {
                return Ok(Some("the database holds an unknown system object"));
            }
            continue;
        }
        let Some(sql) = o.sql else {
            return Ok(Some("a schema object has no definition"));
        };
        if !sql.get(..6).is_some_and(|p| p.eq_ignore_ascii_case("create")) {
            return Ok(Some("a schema object has a bad definition"));
        }
        let lower = sql.to_ascii_lowercase();
        if matches!(o.kind.as_str(), "trigger" | "view") && lower.contains("_rt_") {
            return Ok(Some("a trigger or view refers to a runtime table"));
        }
        // The rehearsal cannot compile a body that dangles (`FROM` a table that is not there yet, and
        // a guest could create later), so the names the authorizer denies are refused as text too.
        // No schema needs them.
        if DENIED_FUNCTIONS.iter().any(|f| lower.contains(f)) {
            return Ok(Some("a schema statement names a function a guest may not call"));
        }
        if o.kind == "table" && o.rootpage == 0 {
            let allowed = module_of(&sql).is_some_and(|mo| ALLOWED_MODULES.contains(&mo.as_str()));
            if !allowed {
                return Ok(Some("a virtual table uses a module imports do not allow"));
            }
        }
        guest.push((o.name, sql));
    }

    set_guest_limits(&m)?;
    arm(&m)?;
    for (name, sql) in guest {
        // A virtual table made the shadow tables that follow it.
        if exists(&m, &name)? {
            continue;
        }
        let done = m.prepare(&sql).and_then(|mut st| st.raw_execute());
        if let Err(e) = done {
            return Ok(Some(if is_auth_error(&e) {
                "a schema statement does what a guest may not"
            } else {
                "a schema statement does not replay"
            }));
        }
    }
    rehearse(&m)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the rehearsal alone says of `schema`: the text check in `schema_problem` would hide it.
    fn rehearsed(schema: &str) -> Option<&'static str> {
        let m = Connection::open_in_memory().unwrap();
        m.execute_batch(schema).unwrap();
        rehearse(&m).unwrap()
    }

    #[test]
    fn a_view_or_trigger_reaching_a_denied_function_is_refused_however_indirect() {
        const V: &str = "CREATE VIEW a AS SELECT load_extension('x') AS y;";
        for schema in [
            V.to_string(),
            "CREATE VIEW a AS SELECT fts3_tokenizer('simple') AS y;".to_string(),
            format!("{V} CREATE VIEW b AS SELECT * FROM a;"),
            format!("{V} CREATE VIEW b AS SELECT 1 WHERE EXISTS (SELECT y FROM a);"),
            "CREATE TABLE t(a); CREATE TRIGGER r AFTER INSERT ON t BEGIN SELECT load_extension('x'); END;".to_string(),
            format!("{V} CREATE TABLE t(a); CREATE TRIGGER r AFTER DELETE ON t BEGIN SELECT y FROM a; END;"),
            "CREATE TABLE t(a); CREATE TABLE u(a); CREATE TRIGGER r AFTER UPDATE ON t BEGIN INSERT INTO u VALUES (1); END; \
             CREATE TRIGGER s AFTER INSERT ON u BEGIN SELECT load_extension('x'); END;"
                .to_string(),
            "CREATE TABLE t(a); CREATE VIEW v AS SELECT a FROM t; \
             CREATE TRIGGER r INSTEAD OF INSERT ON v BEGIN SELECT load_extension('x'); END;"
                .to_string(),
        ] {
            assert!(rehearsed(&schema).is_some(), "{schema}");
        }
    }

    #[test]
    fn ordinary_views_and_triggers_and_dangling_bodies_pass_the_rehearsal() {
        for schema in [
            "CREATE TABLE t(a); CREATE VIEW v AS SELECT abs(a) AS y FROM t;",
            "CREATE TABLE t(a); CREATE TABLE u(a); CREATE TRIGGER r AFTER INSERT ON t BEGIN INSERT INTO u VALUES (upper(new.a)); END;",
            "CREATE VIEW v AS SELECT x FROM not_there;",
        ] {
            assert_eq!(rehearsed(schema), None, "{schema}");
        }
    }

    /// The text check is what catches the body the rehearsal cannot compile.
    #[test]
    fn a_dangling_view_naming_a_denied_function_is_refused_as_text() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE VIEW v AS SELECT load_extension('x') FROM not_there;")
            .unwrap();
        assert!(schema_problem(&c).unwrap().is_some());
    }
}
