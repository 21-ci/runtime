//! SQL migrations (R6): one transaction under the guest authorizer, on the service's writer.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rt_store::faults::{Fault, Faults};
use rt_store::{LocalServiceData, ServiceDataStore, SqlBackend, SqlValue, StoreError};
use rt_types::{build_migrations, MigrationFailure, MigrationFile, VersionNo};

const BUDGET: Duration = Duration::from_secs(30);

fn open_store() -> (Arc<LocalServiceData>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("dir");
    (common::data(dir.path()), dir)
}

fn files(list: &[(&str, &str)]) -> Vec<MigrationFile> {
    build_migrations(list.iter().copied()).expect("valid names")
}

fn sql_of(store: &LocalServiceData) -> Arc<dyn SqlBackend> {
    store.sql(svc(1))
}

fn failure(r: Result<impl std::fmt::Debug, StoreError>) -> MigrationFailure {
    match r.expect_err("the migration must fail") {
        StoreError::Migration(f) => *f,
        other => panic!("{other:?}"),
    }
}

async fn has_table(sql: &Arc<dyn SqlBackend>, name: &str) -> bool {
    let r = sql
        .execute(st(&format!(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}'"
        )))
        .await
        .expect("query");
    r.rows[0][0] == SqlValue::Integer(1)
}

#[tokio::test]
async fn pending_files_apply_once_and_are_recorded_with_their_hash_and_version() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let v1 = files(&[("0001_a.sql", "CREATE TABLE a (x);\nINSERT INTO a VALUES (1);")]);
    let done = sql.migrate(v1.clone(), VersionNo(3), BUDGET).await.expect("migrate");
    assert_eq!(done, vec![v1[0].entry()]);
    let rows = sql.applied_migrations().await.expect("applied");
    assert_eq!((rows[0].name.as_str(), rows[0].version), ("0001_a", 3));
    assert_eq!(rows[0].sha256, v1[0].sha256);

    // The same list again runs nothing: the INSERT would make a second row.
    assert!(sql.migrate(v1, VersionNo(4), BUDGET).await.expect("again").is_empty());
    let n = sql.execute(st("SELECT count(*) FROM a")).await.expect("count");
    assert_eq!(n.rows[0][0], SqlValue::Integer(1));
}

/// Failure in file 2 of 3 leaves the schema and `_rt_migrations` as they were.
#[tokio::test]
async fn a_failure_in_any_file_rolls_back_every_file() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let list = files(&[
        ("0001_a.sql", "CREATE TABLE a (x);"),
        (
            "0002_b.sql",
            "CREATE TABLE b (x);\n\n-- note\nINSERT INTO missing VALUES (1);",
        ),
        ("0003_c.sql", "CREATE TABLE c (x);"),
    ]);
    let f = failure(sql.migrate(list, VersionNo(1), BUDGET).await);
    assert_eq!(f.code, "migration_failed");
    assert_eq!(f.file.as_deref(), Some("0002_b.sql"));
    assert_eq!(f.statement_index, Some(1));
    assert_eq!(f.line, Some(4));
    assert_eq!(f.sqlite_code, Some(1));
    assert!(f.message.contains("missing"), "{}", f.message);
    for t in ["a", "b", "c"] {
        assert!(!has_table(&sql, t).await, "{t}");
    }
    assert!(sql.applied_migrations().await.expect("applied").is_empty());
    // The writer is usable afterwards.
    sql.execute(st("CREATE TABLE after_failure (x)"))
        .await
        .expect("still works");
}

#[tokio::test]
async fn a_syntax_error_reports_its_own_line() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let f = failure(
        sql.migrate(
            files(&[("0001_a.sql", "CREATE TABLE a (x);\nCREATE\nTABEL b;")]),
            VersionNo(1),
            BUDGET,
        )
        .await,
    );
    assert_eq!((f.statement_index, f.line), (Some(1), Some(3)));
    assert!(!has_table(&sql, "a").await);
}

/// Hostile SQL meets the guest authorizer; nothing of the file stays and no host path comes back.
#[tokio::test]
async fn hostile_sql_is_refused_like_guest_sql() {
    let cases = [
        "SELECT * FROM _rt_kv",
        "INSERT INTO _rt_migrations VALUES ('0000_x', 'h', 1, 1)",
        "DELETE FROM _RT_MIGRATIONS",
        "DROP TABLE _rt_kv",
        "CREATE INDEX i ON _rt_kv(key)",
        "CREATE TABLE _rt_mine (x)",
        "CREATE TABLE t (x); ALTER TABLE t RENAME TO _rt_mine",
        "CREATE TRIGGER trg AFTER DELETE ON _rt_kv BEGIN SELECT 1; END",
        "PRAGMA writable_schema = 1",
        "PRAGMA journal_mode = DELETE",
        "PRAGMA max_page_count = 1",
        "ATTACH DATABASE '/etc/passwd' AS x",
        "VACUUM",
        "SELECT load_extension('/tmp/x.so')",
        "CREATE TEMP TABLE t2 (x)",
    ];
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    for hostile in cases {
        let text = format!("CREATE TABLE canary (x);\n{hostile};");
        let r = sql.migrate(files(&[("0001_h.sql", &text)]), VersionNo(1), BUDGET).await;
        assert!(r.is_err(), "{hostile} was accepted");
        let f = failure(r);
        assert_eq!(f.code, "migration_failed", "{hostile}");
        assert!(!f.message.contains('/'), "{hostile}: {}", f.message);
        assert!(!has_table(&sql, "canary").await, "{hostile}");
        assert!(sql.applied_migrations().await.expect("applied").is_empty(), "{hostile}");
    }
}

/// A view or trigger naming a reserved table may be created, as in guest SQL, but gives no access:
/// every use meets the authorizer.
#[tokio::test]
async fn deferred_objects_over_reserved_tables_stay_unusable() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let text = "CREATE VIEW v AS SELECT * FROM _rt_kv;\nCREATE TABLE u (x);\nCREATE TRIGGER t AFTER INSERT ON u BEGIN DELETE FROM _rt_kv; END;";
    if sql
        .migrate(files(&[("0001_h.sql", text)]), VersionNo(1), BUDGET)
        .await
        .is_ok()
    {
        let e = sql.execute(st("SELECT * FROM v")).await.expect_err("view");
        assert_eq!(e.code(), "sql_not_authorized");
        let e = sql.execute(st("INSERT INTO u VALUES (1)")).await.expect_err("trigger");
        assert_eq!(e.code(), "sql_not_authorized");
    }
}

#[tokio::test]
async fn transaction_control_has_its_own_code_and_ends_nothing() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    for stmt in [
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT s",
        "RELEASE s",
        "/* c */ begin immediate",
    ] {
        let text = format!("CREATE TABLE canary (x);\n{stmt};\nCREATE TABLE z (x);");
        let f = failure(sql.migrate(files(&[("0001_t.sql", &text)]), VersionNo(1), BUDGET).await);
        assert_eq!(f.code, "transaction_control_forbidden", "{stmt}");
        assert_eq!((f.statement_index, f.line), (Some(1), Some(2)), "{stmt}");
        assert!(!has_table(&sql, "canary").await, "{stmt}");
    }
}

/// `;` in strings, comments and trigger bodies does not split a statement.
#[tokio::test]
async fn multi_statement_text_is_split_where_sqlite_would() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let text = "-- a; comment\nCREATE TABLE a (x TEXT);\n/* b; */ INSERT INTO a VALUES ('c;d');\n\
        CREATE TRIGGER tr AFTER INSERT ON a BEGIN\n  UPDATE a SET x = CASE WHEN x = 'q' THEN 'r' ELSE x END;\n  SELECT 1;\nEND;\n\
        INSERT INTO a VALUES ('q');\nSELECT * FROM a;\n;;\n-- trailing";
    sql.migrate(files(&[("0001_a.sql", text)]), VersionNo(1), BUDGET)
        .await
        .expect("migrate");
    let r = sql.execute(st("SELECT x FROM a ORDER BY rowid")).await.expect("rows");
    assert_eq!(r.rows[0][0], SqlValue::Text("c;d".into()));
    assert_eq!(r.rows[1][0], SqlValue::Text("r".into()));
}

#[tokio::test]
async fn drift_and_order_refuse_before_anything_runs() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    sql.migrate(files(&[("0002_b.sql", "CREATE TABLE b (x);")]), VersionNo(1), BUDGET)
        .await
        .expect("first");
    let f = failure(
        sql.migrate(
            files(&[("0002_b.sql", "CREATE TABLE changed (x);")]),
            VersionNo(2),
            BUDGET,
        )
        .await,
    );
    assert_eq!(f.code, "migration_drift");
    assert!(!has_table(&sql, "changed").await);
    let f = failure(
        sql.migrate(
            files(&[
                ("0001_a.sql", "CREATE TABLE a (x);"),
                ("0002_b.sql", "CREATE TABLE b (x);"),
            ]),
            VersionNo(2),
            BUDGET,
        )
        .await,
    );
    assert_eq!(f.code, "migration_out_of_order");
    assert!(!has_table(&sql, "a").await);
}

#[tokio::test]
async fn the_budget_rolls_back_with_migration_timeout() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    let spin = "CREATE TABLE canary (x);\nWITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c) SELECT count(*) FROM c;";
    let f = failure(
        sql.migrate(files(&[("0001_a.sql", spin)]), VersionNo(1), Duration::from_millis(100))
            .await,
    );
    assert_eq!((f.code, f.statement_index), ("migration_timeout", Some(1)));
    assert!(!has_table(&sql, "canary").await);
}

/// A store failure at the start of the job leaves nothing applied, and the retry applies it.
#[tokio::test]
async fn a_fault_applies_nothing_and_a_retry_succeeds() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    sql.execute(st("CREATE TABLE before (x)")).await.expect("open");
    let faults = Faults::new();
    store.inject_faults(faults.clone());
    faults.fail_next(1, Fault::Io);
    let list = files(&[("0001_a.sql", "CREATE TABLE a (x);")]);
    assert!(sql.migrate(list.clone(), VersionNo(1), BUDGET).await.is_err());
    assert!(!has_table(&sql, "a").await);
    assert!(sql.applied_migrations().await.expect("applied").is_empty());
    assert_eq!(sql.migrate(list, VersionNo(1), BUDGET).await.expect("retry").len(), 1);
}

#[tokio::test]
async fn the_reserved_table_is_hidden_from_guest_sql() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    for q in [
        "SELECT * FROM _rt_migrations",
        "DELETE FROM _rt_migrations",
        "DROP TABLE _rt_migrations",
    ] {
        let e = sql.execute(st(q)).await.expect_err(q);
        assert_eq!(e.code(), "sql_not_authorized", "{q}");
    }
}

#[tokio::test]
async fn a_dry_run_reports_the_real_failure_and_changes_nothing() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    sql.execute(st("CREATE TABLE seen (x)")).await.expect("seed");
    let ok = files(&[("0001_a.sql", "CREATE TABLE a (x); INSERT INTO seen VALUES (1);")]);
    let got = sql
        .migrate_dry_run(ok, VersionNo(2), BUDGET, u64::MAX)
        .await
        .expect("dry run");
    assert_eq!(got.pending, vec!["0001_a".to_string()]);
    assert!(!has_table(&sql, "a").await);
    assert!(sql.applied_migrations().await.expect("applied").is_empty());
    let n = sql.execute(st("SELECT count(*) FROM seen")).await.expect("count");
    assert_eq!(n.rows[0][0], SqlValue::Integer(0));

    let bad = files(&[("0001_a.sql", "INSERT INTO nowhere VALUES (1);")]);
    let f = failure(sql.migrate_dry_run(bad, VersionNo(2), BUDGET, u64::MAX).await);
    assert_eq!((f.code, f.statement_index), ("migration_failed", Some(0)));
}

#[tokio::test]
async fn a_dry_run_over_the_size_limit_is_refused() {
    let (store, _d) = open_store();
    let sql = sql_of(&store);
    sql.execute(st("CREATE TABLE seen (x)")).await.expect("seed");
    let list = files(&[("0001_a.sql", "CREATE TABLE a (x);")]);
    let e = sql
        .migrate_dry_run(list, VersionNo(2), BUDGET, 1)
        .await
        .expect_err("too big");
    assert_eq!(e.code(), "dry_run_too_large");
}

// ---- meta.db ---------------------------------------------------------------------------------

mod meta {
    use super::*;
    use rt_store::{MetaStore, SqliteMetaStore};

    /// A database at the previous schema keeps its versions, which carry no migrations.
    #[tokio::test]
    async fn meta_upgrade_from_the_previous_schema_keeps_versions() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("meta.db");
        let (service, tenant_slug) = {
            let m = SqliteMetaStore::open(&path).expect("open");
            let t = mk_tenant(&m, "t").await;
            let s = mk_service(&m, &t, "upg").await;
            m.add_version(s.id, new_version(1)).await.expect("version");
            (s.id, t.id)
        };
        {
            // Back to the schema before this migration, with the row in it.
            let c = rusqlite::Connection::open(&path).expect("raw");
            c.execute_batch("DROP TABLE secret_refs; DROP TABLE pending_imports; ALTER TABLE versions DROP COLUMN migrations; ALTER TABLE versions DROP COLUMN migrations_sql; ALTER TABLE versions DROP COLUMN meta; ALTER TABLE versions DROP COLUMN activate_after_build; PRAGMA user_version = 3;")
                .expect("downgrade");
        }
        let m = SqliteMetaStore::open(&path).expect("upgrade");
        let v = m.get_version(service, VersionNo(1)).await.expect("get").expect("row");
        assert!(v.migrations.is_empty());
        let _ = tenant_slug;
        m.set_version_migrations(service, VersionNo(1), files(&[("0001_a.sql", "SELECT 1;")]))
            .await
            .expect("set");
    }

    #[tokio::test]
    async fn the_list_is_write_once_and_the_sql_is_read_back() {
        let m = common::meta();
        let t = mk_tenant(&m, "t").await;
        let s = mk_service(&m, &t, "once").await;
        let v = m.add_version(s.id, new_version(1)).await.expect("version");
        assert!(v.migrations.is_empty());
        let list = files(&[("0001_a.sql", "SELECT 1;")]);
        m.set_version_migrations(s.id, v.number, list.clone())
            .await
            .expect("set");
        let got = m.get_version(s.id, v.number).await.expect("get").expect("row");
        assert_eq!(got.migrations, vec![list[0].entry()]);
        assert_eq!(m.version_migration_files(s.id, v.number).await.expect("files"), list);
        let e = m
            .set_version_migrations(s.id, v.number, list)
            .await
            .expect_err("second write");
        assert_eq!(e.code(), "migrations_already_set");
    }

    #[tokio::test]
    async fn a_live_version_takes_no_list() {
        let m = common::meta();
        let t = mk_tenant(&m, "t").await;
        let s = mk_service(&m, &t, "live").await;
        let v = m.add_version(s.id, new_version(1)).await.expect("version");
        m.set_live(s.id, v.number, None).await.expect("live");
        let e = m
            .set_version_migrations(s.id, v.number, files(&[("0001_a.sql", "SELECT 1;")]))
            .await
            .expect_err("live");
        assert_eq!(e.code(), "version_is_live");
    }

    #[tokio::test]
    async fn an_upload_stores_the_list_atomically_with_the_version() {
        let m = common::meta();
        let t = mk_tenant(&m, "t").await;
        let s = mk_service(&m, &t, "upl").await;
        let list = files(&[("0001_a.sql", "SELECT 1;")]);
        let v = m
            .add_version_with_migrations(s.id, new_version(1), list.clone())
            .await
            .expect("version");
        assert_eq!(v.migrations, vec![list[0].entry()]);
        assert_eq!(m.version_migration_files(s.id, v.number).await.expect("files"), list);
    }

    /// Adoption carries the list and its SQL, and picks up a list set on the source later.
    #[tokio::test]
    async fn adoption_copies_the_migrations() {
        let m = common::meta();
        let t = mk_tenant(&m, "t").await;
        let parent = mk_service(&m, &t, "par").await;
        let branch = mk_service(&m, &t, "bra").await;
        let list = files(&[("0001_a.sql", "SELECT 1;")]);
        let plain = m.add_version(branch.id, new_version(1)).await.expect("v");
        let with = m
            .add_version_with_migrations(branch.id, new_version(2), list.clone())
            .await
            .expect("v");
        for n in [plain.number, with.number] {
            m.set_build_state(branch.id, n, rt_types::BuildState::Ready, None, None)
                .await
                .expect("ready");
        }
        let adopted = m.adopt_version(parent.id, branch.id, with.number).await.expect("adopt");
        assert_eq!(adopted.migrations, vec![list[0].entry()]);
        assert_eq!(
            m.version_migration_files(parent.id, adopted.number).await.expect("sql"),
            list
        );

        let first = m
            .adopt_version(parent.id, branch.id, plain.number)
            .await
            .expect("adopt");
        assert!(first.migrations.is_empty());
        m.set_version_migrations(branch.id, plain.number, list.clone())
            .await
            .expect("set");
        let again = m
            .adopt_version(parent.id, branch.id, plain.number)
            .await
            .expect("reuse");
        assert_eq!(again.number, first.number);
        assert_eq!(again.migrations, vec![list[0].entry()]);
    }
}
