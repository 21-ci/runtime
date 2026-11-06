//! The data browser's sandbox (R9, SECURITY.md "Data browser"): every attack refused on its own,
//! `rows_estimate` without a scan, and no way to reach the writer.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rt_store::{
    BatchMode, BrowseCell, BrowseLimits, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, SqlBackend,
    SqlValue, StoreError, ValueTag,
};
use rt_types::ServiceId;

use crate::common::*;

async fn seeded() -> (tempfile::TempDir, Arc<LocalServiceData>, ServiceId, Arc<dyn SqlBackend>) {
    let dir = tempfile::tempdir().unwrap();
    let store = data(dir.path());
    let svc = ServiceId::new();
    let sql = store.sql(svc);
    sql.batch(
        vec![
            st("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, photo BLOB)"),
            st("INSERT INTO users (name, photo) VALUES ('ann', x'0102'), ('bob', NULL)"),
        ],
        BatchMode::Write,
    )
    .await
    .unwrap();
    (dir, store, svc, sql)
}

async fn q(sql: &Arc<dyn SqlBackend>, text: &str) -> rt_store::Result<rt_store::BrowseResult> {
    sql.query_readonly(st(text), BrowseLimits::default()).await
}

/// Every statement a browse must refuse, with the one stable code.
#[tokio::test]
async fn the_attack_table_is_refused_and_changes_nothing() {
    let (_d, _store, _svc, sql) = seeded().await;
    for attack in [
        "INSERT INTO users (name) VALUES ('x')",
        "UPDATE users SET name = 'x'",
        "DELETE FROM users",
        "DROP TABLE users",
        "WITH x AS (SELECT 1) INSERT INTO users (name) SELECT 'x' FROM x",
        "WITH x AS (SELECT 1) DELETE FROM users",
        "INSERT INTO users (name) VALUES ('x') RETURNING id",
        "ATTACH DATABASE '/tmp/evil.db' AS evil",
        "DETACH DATABASE main",
        "PRAGMA writable_schema = 1",
        "PRAGMA journal_mode",
        "PRAGMA database_list",
        "PRAGMA compile_options",
        "VACUUM",
        "VACUUM INTO '/tmp/evil.db'",
        "BEGIN",
        "COMMIT",
        "SAVEPOINT s",
        "ANALYZE",
        "REINDEX",
        "SELECT 1; SELECT 2",
        "SELECT 1; DROP TABLE users",
        "SELECT load_extension('evil')",
        "SELECT fts3_tokenizer('simple')",
        "SELECT readfile('/etc/passwd')",
        "SELECT sqlite_compileoption_get(0)",
        "SELECT * FROM _rt_kv",
        "SELECT * FROM _RT_KV",
        "SELECT * FROM main._rt_blobs",
        "SELECT * FROM users, _rt_durable",
        "SELECT * FROM sqlite_master",
        "SELECT * FROM sqlite_schema",
        "SELECT sql FROM main.sqlite_schema WHERE name = '_rt_kv'",
        "SELECT * FROM sqlite_temp_master",
        "SELECT * FROM pragma_database_list",
        "SELECT 1 FROM pragma_database_list",
        "SELECT * FROM pragma_compile_options",
        "SELECT * FROM pragma_function_list",
        "SELECT * FROM pragma_table_info('_rt_kv')",
        "SELECT * FROM sqlite_dbpage",
        "SELECT * FROM dbstat",
        "PRAGMA table_info(_rt_kv)",
        "PRAGMA index_list('_rt_kv')",
        "CREATE TEMP TABLE t (a)",
        "CREATE TABLE t (a)",
    ] {
        let e = q(&sql, attack).await.expect_err(attack);
        // A table-valued function or table a build does not have is `no such table` at prepare:
        // refused all the same.
        let refused = matches!(e, StoreError::SqlNotReadonly)
            || matches!(&e, StoreError::SqlFailed { message, .. } if message.contains("no such"));
        assert!(refused, "{attack}: {e:?}");
    }
    let n = sql.execute(st("SELECT count(*) FROM users")).await.unwrap();
    assert_eq!(n.rows[0][0], SqlValue::Integer(2));
}

/// A view cannot launder a read past the authorizer: the inner read is judged too.
#[tokio::test]
async fn a_guest_view_over_a_denied_table_is_refused() {
    let (_d, _store, _svc, sql) = seeded().await;
    sql.execute(st("CREATE VIEW v AS SELECT * FROM sqlite_schema"))
        .await
        .unwrap();
    assert!(matches!(
        q(&sql, "SELECT * FROM v").await,
        Err(StoreError::SqlNotReadonly)
    ));
}

/// The neighbours of the attacks stay allowed.
#[tokio::test]
async fn reads_of_the_services_own_data_work() {
    let (_d, _store, _svc, sql) = seeded().await;
    let r = q(
        &sql,
        "  -- a comment\nSELECT id, name, photo FROM users ORDER BY id; -- end\n",
    )
    .await
    .unwrap();
    assert_eq!(r.columns, ["id", "name", "photo"]);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], BrowseCell::Integer(1));
    assert_eq!(
        r.rows[0][2],
        BrowseCell::Blob {
            head: vec![1, 2],
            size: 2
        }
    );
    assert!(!r.truncated);
    assert!(q(&sql, "WITH x AS (SELECT name FROM users) SELECT * FROM x")
        .await
        .is_ok());
    let r = q(&sql, "PRAGMA table_info(users)").await.unwrap();
    assert_eq!(r.rows.len(), 3);
    let r = q(&sql, "SELECT name FROM pragma_table_info('users')").await.unwrap();
    assert_eq!(r.rows.len(), 3);
    assert!(q(&sql, "PRAGMA foreign_key_list(users)").await.is_ok());
    // A string that merely mentions a denied name is not a read of it.
    assert!(q(&sql, "SELECT '_rt_kv', 'load_extension'").await.is_ok());
    let r = sql
        .query_readonly(
            st_args("SELECT name FROM users WHERE id = ?1", vec![SqlValue::Integer(2)]),
            BrowseLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.rows[0][0],
        BrowseCell::Text {
            text: "bob".into(),
            size: 3,
            cut: false
        }
    );
}

#[tokio::test]
async fn a_syntax_error_has_its_code_and_offset_and_no_path() {
    let (_d, _store, _svc, sql) = seeded().await;
    match q(&sql, "SELECT FROM WHERE").await {
        Err(StoreError::SqlFailed { code, offset, message }) => {
            assert_eq!(code, 1);
            assert!(offset.is_some());
            assert!(!message.contains('/'), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        q(&sql, "SELECT * FROM nope").await,
        Err(StoreError::SqlFailed { .. })
    ));
}

#[tokio::test]
async fn huge_results_stop_at_the_row_and_byte_budgets() {
    let (_d, _store, _svc, sql) = seeded().await;
    const RANGE: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100000000)";
    // A cross join of anything stops as soon as the rows are in.
    let started = Instant::now();
    let join = (0..30).map(|i| format!("users u{i}")).collect::<Vec<_>>().join(", ");
    let r = q(&sql, &format!("SELECT u0.name FROM {join}")).await.unwrap();
    assert!(r.truncated);
    assert_eq!(r.rows.len(), 100);
    assert!(started.elapsed() < Duration::from_secs(1));
    // Exactly as many rows as the limit is not truncated.
    let r = q(
        &sql,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100) SELECT x FROM c",
    )
    .await
    .unwrap();
    assert!(!r.truncated);
    // Bytes run out before rows.
    let lim = BrowseLimits {
        rows: 500,
        bytes: 10_000,
        cell: 4096,
        ..Default::default()
    };
    let r = sql
        .query_readonly(st(&format!("{RANGE} SELECT hex(zeroblob(1000)) FROM c")), lim)
        .await
        .unwrap();
    assert!(r.truncated);
    assert!(!r.rows.is_empty() && r.rows.len() < 10);
    // A cell is cut at `cell_max` on a character boundary, with its real size.
    let lim = BrowseLimits {
        cell: 5,
        ..Default::default()
    };
    let r = sql.query_readonly(st("SELECT 'ааааа'"), lim).await.unwrap();
    assert_eq!(
        r.rows[0][0],
        BrowseCell::Text {
            text: "аа".into(),
            size: 10,
            cut: true
        }
    );
    // A blob keeps its head only.
    let r = q(&sql, "SELECT zeroblob(10000000)").await.unwrap();
    match &r.rows[0][0] {
        BrowseCell::Blob { head, size } => assert_eq!((head.len(), *size), (64, 10_000_000)),
        other => panic!("{other:?}"),
    }
    // Even the first row does not fit.
    let lim = BrowseLimits {
        bytes: 10,
        ..Default::default()
    };
    let e = sql.query_readonly(st("SELECT 'a long enough text'"), lim).await;
    assert!(matches!(e, Err(StoreError::SqlResultTooLarge)), "{e:?}");
}

#[tokio::test]
async fn a_slow_query_dies_at_the_budget() {
    let (_d, _store, _svc, sql) = seeded().await;
    let lim = BrowseLimits {
        budget: Duration::from_millis(200),
        ..Default::default()
    };
    let started = Instant::now();
    let r = sql
        .query_readonly(
            st("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"),
            lim,
        )
        .await;
    assert!(matches!(r, Err(StoreError::SqlTimeout)), "{r:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// A browse needs no writer: it runs while a transaction holds the writer, sees committed data only.
#[tokio::test]
async fn a_query_never_touches_the_writer() {
    let (_d, _store, _svc, sql) = seeded().await;
    let mut tx = sql.begin().await.unwrap();
    tx.execute(st("INSERT INTO users (name) VALUES ('uncommitted')"))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(1), q(&sql, "SELECT count(*) FROM users"))
        .await
        .expect("a browse does not wait for the writer")
        .unwrap();
    assert_eq!(r.rows[0][0], BrowseCell::Integer(2));
    tx.rollback().await.unwrap();
}

/// Even without the authorizer the connection cannot write: OS flag and `query_only`.
#[test]
fn the_connection_is_read_only_without_any_authorizer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.db");
    let w = rusqlite::Connection::open(&path).unwrap();
    w.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t (a); INSERT INTO t VALUES (1)")
        .unwrap();
    let c = rusqlite::Connection::open_with_flags(
        &path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let e = c.execute("INSERT INTO t VALUES (2)", []).unwrap_err();
    assert!(e.to_string().contains("readonly"), "{e}");
}

/// A browse of a service with no data creates nothing and reads as empty.
#[tokio::test]
async fn browsing_a_service_without_a_database_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = data(dir.path());
    let svc = ServiceId::new();
    let sql = store.sql(svc);
    assert!(sql.browse_tables().await.unwrap().is_empty());
    assert!(q(&sql, "SELECT 1").await.is_ok());
    assert!(sql.browse_kv_list(vec![], None, 10, 16).await.unwrap().items.is_empty());
    assert!(sql.browse_kv_get(b"k".to_vec(), 16).await.unwrap().is_none());
    assert!(!dir
        .path()
        .join("services")
        .join(svc.to_string())
        .join("data.db")
        .exists());
}

#[tokio::test]
async fn a_retired_service_is_not_found() {
    let (_d, store, svc, sql) = seeded().await;
    store.retire(svc).unwrap();
    assert!(matches!(q(&sql, "SELECT 1").await, Err(StoreError::NotFound)));
}

/// `rows_estimate` is never a count: rows deleted below `MAX(rowid)` stay in the estimate.
#[tokio::test]
async fn rows_estimate_does_not_scan() {
    let (_d, _store, _svc, sql) = seeded().await;
    sql.batch(
        vec![
            st("CREATE TABLE gaps (id INTEGER PRIMARY KEY)"),
            st("INSERT INTO gaps (id) VALUES (1), (1000)"),
            st("DELETE FROM gaps WHERE id = 1"),
            st("CREATE TABLE no_rowid (k TEXT PRIMARY KEY) WITHOUT ROWID"),
            st("CREATE TABLE shadow (rowid TEXT)"),
            st("CREATE VIEW names AS SELECT name FROM users"),
            st("CREATE TABLE \"odd\"\"name\" (a)"),
            st("INSERT INTO \"odd\"\"name\" VALUES (1)"),
            st("CREATE TABLE empty (a)"),
        ],
        BatchMode::Write,
    )
    .await
    .unwrap();
    let tables = sql.browse_tables().await.unwrap();
    let est = |n: &str| tables.iter().find(|t| t.name == n).unwrap().rows_estimate;
    assert_eq!(est("gaps"), Some(1000), "MAX(rowid), not a count");
    assert_eq!(est("no_rowid"), None);
    assert_eq!(est("shadow"), None);
    assert_eq!(est("names"), None);
    assert_eq!(est("odd\"name"), Some(1));
    assert_eq!(est("empty"), Some(0));
    assert!(tables
        .iter()
        .all(|t| !t.name.starts_with("_rt_") && !t.name.starts_with("sqlite_")));
    let users = tables.iter().find(|t| t.name == "users").unwrap();
    assert_eq!(users.kind, "table");
    assert_eq!(users.columns[0].name, "id");
    assert!(users.columns[0].pk && !users.columns[1].pk);
    assert_eq!(users.columns[1].decl_type, "TEXT");
    assert_eq!(tables.iter().find(|t| t.name == "names").unwrap().kind, "view");

    // `sqlite_stat1` wins where it has the table.
    sql.execute(st("ANALYZE gaps")).await.unwrap();
    assert_eq!(
        sql.browse_tables()
            .await
            .unwrap()
            .iter()
            .find(|t| t.name == "gaps")
            .unwrap()
            .rows_estimate,
        Some(1)
    );
}

#[tokio::test]
async fn kv_browse_pages_by_prefix_and_never_shows_expired_keys() {
    let (_d, store, svc, _sql) = seeded().await;
    let kv = store.kv(svc);
    let val = |b: &[u8]| KvValue {
        tag: ValueTag::Json,
        data: bytes::Bytes::copy_from_slice(b),
    };
    for k in ["a:1", "a:2", "a:3", "b:1"] {
        kv.set(k.as_bytes(), val(b"\"0123456789\""), KvSetOptions::default())
            .await
            .unwrap();
    }
    kv.set(
        b"a:gone",
        val(b"1"),
        KvSetOptions {
            ttl: Some(Duration::from_millis(1)),
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let sql = store.sql(svc);
    let p = sql.browse_kv_list(b"a:".to_vec(), None, 2, 4).await.unwrap();
    assert_eq!(
        p.items.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
        [b"a:1".to_vec(), b"a:2".to_vec()]
    );
    assert_eq!((p.items[0].size, p.items[0].head.len()), (12, 4));
    let p = sql.browse_kv_list(b"a:".to_vec(), p.next_cursor, 2, 4).await.unwrap();
    assert_eq!(p.items.len(), 1, "the expired key and b:1 are out");
    assert!(p.next_cursor.is_none());
    let e = sql.browse_kv_get(b"a:1".to_vec(), 3).await.unwrap().unwrap();
    assert_eq!((e.head.len(), e.size), (3, 12));
    assert!(sql.browse_kv_get(b"a:gone".to_vec(), 3).await.unwrap().is_none());
}
