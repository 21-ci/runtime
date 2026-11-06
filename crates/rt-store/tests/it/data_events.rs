//! The data-change outbox (R14).

use crate::common;

use std::sync::Arc;

use common::*;
use rt_store::data_events::DataEventsStore;
use rt_store::{BatchMode, KvSetOptions, KvValue, LocalServiceData, ServiceDataStore, SqlBackend};
use rt_types::events::DataEventsConfig;

fn cfg(json: serde_json::Value) -> DataEventsConfig {
    serde_json::from_value(json).expect("config")
}

fn orders() -> DataEventsConfig {
    cfg(serde_json::json!({"topic": "t", "tables": [{"name": "orders"}]}))
}

async fn setup(config: &DataEventsConfig) -> (Arc<LocalServiceData>, Arc<dyn SqlBackend>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("dir");
    let store = common::data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE orders (id INTEGER PRIMARY KEY, note, data BLOB)"))
        .await
        .expect("table");
    store.sync_triggers(svc(1), config).await.expect("sync");
    (store, sql, dir)
}

async fn changes(store: &LocalServiceData) -> Vec<rt_store::data_events::ChangeRow> {
    store.read_changes(svc(1), 500, 1 << 24).await.expect("read")
}

#[tokio::test]
async fn a_commit_emits_each_op_with_its_key_and_a_rollback_emits_nothing() {
    let (store, sql, _d) = setup(&orders()).await;
    sql.execute(st("INSERT INTO orders (id, note) VALUES (7, 'a')"))
        .await
        .expect("i");
    sql.execute(st("UPDATE orders SET note = 'b' WHERE id = 7"))
        .await
        .expect("u");
    sql.execute(st("DELETE FROM orders WHERE id = 7")).await.expect("d");
    let rows = changes(&store).await;
    let got: Vec<_> = rows
        .iter()
        .map(|r| (r.src.as_str(), r.op.as_str(), r.key.as_str()))
        .collect();
    assert_eq!(
        got,
        [
            ("sql:orders", "insert", "[7]"),
            ("sql:orders", "update", "[7]"),
            ("sql:orders", "delete", "[7]")
        ]
    );
    assert!(rows.iter().all(|r| r.row.is_none() && r.ts > 0));
    store.ack(svc(1), rows[2].seq).await.expect("ack");

    let mut tx = sql.begin().await.expect("begin");
    tx.execute(st("INSERT INTO orders (id) VALUES (8)")).await.expect("i");
    tx.rollback().await.expect("rollback");
    let bad = vec![
        st("INSERT INTO orders (id) VALUES (9)"),
        st("INSERT INTO nope VALUES (1)"),
    ];
    sql.batch(bad, BatchMode::Write).await.expect_err("second fails");
    assert_eq!(store.backlog(svc(1)).await.expect("backlog"), 0);

    let ok = vec![
        st("INSERT INTO orders (id) VALUES (10)"),
        st("INSERT INTO orders (id) VALUES (11)"),
    ];
    sql.batch(ok, BatchMode::Write).await.expect("batch");
    assert_eq!(store.backlog(svc(1)).await.expect("backlog"), 2);
}

#[tokio::test]
async fn row_payloads_survive_blobs_and_oversized_rows_become_keys_only() {
    let c = cfg(serde_json::json!({"topic": "t", "tables": [{"name": "orders", "payload": "row"}]}));
    let (store, sql, _d) = setup(&c).await;
    sql.execute(st("INSERT INTO orders VALUES (1, 'x', x'00ff')"))
        .await
        .expect("blob must not fail the write");
    sql.execute(st("INSERT INTO orders VALUES (2, zeroblob(70000), NULL)"))
        .await
        .expect("big");
    let rows = changes(&store).await;
    assert_eq!(rows[0].row.as_deref(), Some(r#"{"id":1,"note":"x","data":"00FF"}"#));
    assert_eq!(rows[1].row, None);
    assert_eq!(rows[1].key, "[2]");
}

#[tokio::test]
async fn guest_writes_keep_their_counts_and_rowid() {
    let (_store, sql, _d) = setup(&orders()).await;
    let r = sql
        .execute(st("INSERT INTO orders (id) VALUES (41), (42)"))
        .await
        .expect("i");
    assert_eq!(r.rows_affected, 2);
    assert_eq!(r.last_insert_rowid, Some(42));
}

#[tokio::test]
async fn a_guest_cannot_read_forge_or_disable() {
    let (store, sql, _d) = setup(&orders()).await;
    sql.execute(st("INSERT INTO orders (id) VALUES (1)")).await.expect("i");
    for attack in [
        "SELECT * FROM _rt_changes",
        "INSERT INTO _rt_changes(ts,src,op,key) VALUES (1,'sql:orders','insert','[1]')",
        "DELETE FROM _rt_changes",
        "UPDATE _rt_changes SET key = '[]'",
        "DROP TABLE _rt_changes",
        "SELECT * FROM _rt_changes_state",
        "DELETE FROM _rt_changes_state",
        "DROP TRIGGER _rt_ev_t6f7264657273_i",
        "DROP TRIGGER \"_RT_EV_t6f7264657273_i\"",
        "CREATE TRIGGER _rt_ev_mine AFTER INSERT ON orders BEGIN INSERT INTO _rt_changes(ts,src,op,key) VALUES (1,'a','b','c'); END",
        "CREATE TRIGGER mine AFTER INSERT ON orders BEGIN INSERT INTO _rt_changes(ts,src,op,key) VALUES (1,'a','b','c'); END",
        "CREATE VIEW _rt_ev_v AS SELECT 1",
        "PRAGMA writable_schema=1",
        // Dropping the table would silently remove its triggers: unsubscribe first.
        "DROP TABLE orders",
    ] {
        let r = sql.execute(st(attack)).await;
        // A guest trigger may compile at CREATE and only fail when it fires; it never writes.
        if r.is_ok() {
            assert!(attack.starts_with("CREATE TRIGGER mine"), "{attack}");
            sql.execute(st("INSERT INTO orders (id) VALUES (2)")).await.expect_err("firing is denied");
            sql.execute(st("DROP TRIGGER mine")).await.expect("own trigger");
        }
    }
    let rows = changes(&store).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].key, "[1]");
}

#[tokio::test]
async fn hostile_table_and_column_names_are_only_ever_names() {
    let hostile = r#"x"; DROP TABLE orders; --'"#;
    let dir = tempfile::tempdir().expect("dir");
    let store = common::data(dir.path());
    let sql = store.sql(svc(1));
    let q = hostile.replace('"', "\"\"");
    sql.execute(st(&format!("CREATE TABLE \"{q}\" (\"{q}\" PRIMARY KEY, v)")))
        .await
        .expect("table");
    sql.execute(st("CREATE TABLE orders (id)")).await.expect("table");
    let c = cfg(serde_json::json!({"topic":"t","tables":[{"name":hostile,"payload":"row"}]}));
    store.sync_triggers(svc(1), &c).await.expect("sync");
    sql.execute(st(&format!("INSERT INTO \"{q}\" VALUES (1, 2)")))
        .await
        .expect("i");
    sql.execute(st("SELECT * FROM orders")).await.expect("still there");
    let rows = changes(&store).await;
    assert_eq!(rows[0].src, format!("sql:{hostile}"));
    let row: serde_json::Value = serde_json::from_str(rows[0].row.as_deref().expect("row")).expect("json");
    assert_eq!(row[hostile], 1);
}

#[tokio::test]
async fn kv_prefixes_emit_and_others_do_not() {
    let c = cfg(serde_json::json!({"topic":"t","kv_prefixes":[{"prefix":"user:"}]}));
    let (store, _sql, _d) = setup(&c).await;
    let kv = store.kv(svc(1));
    let v = KvValue {
        data: bytes::Bytes::from_static(b"1"),
        tag: rt_store::ValueTag::Json,
    };
    kv.set(b"user:1", v.clone(), KvSetOptions::default())
        .await
        .expect("set");
    kv.set(b"other", v, KvSetOptions::default()).await.expect("set");
    kv.delete(b"user:1").await.expect("del");
    let rows = changes(&store).await;
    let ops: Vec<_> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(ops, ["insert", "delete"]);
    assert_eq!(rows[0].key, r#"["757365723A31"]"#);
}

#[tokio::test]
async fn schema_drift_regenerates_and_a_missing_table_only_warns() {
    let c = cfg(serde_json::json!({"topic":"t","tables":[{"name":"orders","payload":"row"},{"name":"later"}]}));
    let (store, sql, _d) = setup(&c).await;
    let report = store.sync_triggers(svc(1), &c).await.expect("sync");
    assert_eq!(report.warnings, ["table `later` does not exist"]);
    sql.execute(st("ALTER TABLE orders ADD COLUMN extra"))
        .await
        .expect("alter");
    sql.execute(st("CREATE TABLE later (id INTEGER PRIMARY KEY)"))
        .await
        .expect("later");
    assert!(store.sync_triggers(svc(1), &c).await.expect("sync").warnings.is_empty());
    sql.execute(st("INSERT INTO orders (id, extra) VALUES (1, 'e')"))
        .await
        .expect("i");
    sql.execute(st("INSERT INTO later VALUES (1)")).await.expect("i");
    let rows = changes(&store).await;
    assert!(rows[0].row.as_deref().expect("row").contains("\"extra\":\"e\""));
    assert_eq!(rows[1].src, "sql:later");
}

#[tokio::test]
async fn pause_drops_the_triggers_until_resumed_and_unsubscribed_writes_are_free() {
    let (store, sql, _d) = setup(&orders()).await;
    sql.execute(st("CREATE TABLE plain (a)")).await.expect("t");
    sql.execute(st("INSERT INTO plain VALUES (1)")).await.expect("i");
    assert_eq!(store.backlog(svc(1)).await.expect("b"), 0);
    store.pause(svc(1)).await.expect("pause");
    assert!(store.sync_triggers(svc(1), &orders()).await.expect("sync").paused);
    sql.execute(st("INSERT INTO orders (id) VALUES (1)"))
        .await
        .expect("guest writes go on");
    assert_eq!(store.backlog(svc(1)).await.expect("b"), 0);
    store.resume(svc(1)).await.expect("resume");
    assert!(!store.sync_triggers(svc(1), &orders()).await.expect("sync").paused);
    sql.execute(st("INSERT INTO orders (id) VALUES (2)")).await.expect("i");
    assert_eq!(store.backlog(svc(1)).await.expect("b"), 1);
    store.disable(svc(1)).await.expect("disable");
    sql.execute(st("INSERT INTO orders (id) VALUES (3)")).await.expect("i");
    assert_eq!(store.backlog(svc(1)).await.expect("b"), 0);
}

#[tokio::test]
async fn a_commit_sets_the_dirty_flag() {
    let (store, sql, _d) = setup(&orders()).await;
    store.take_dirty(svc(1));
    assert!(!store.take_dirty(svc(1)));
    sql.execute(st("INSERT INTO orders (id) VALUES (1)")).await.expect("i");
    assert!(store.take_dirty(svc(1)));
    assert!(!store.take_dirty(svc(1)));
}

#[tokio::test]
async fn a_branch_copy_has_no_triggers_and_no_rows() {
    let (store, sql, _d) = setup(&orders()).await;
    sql.execute(st("INSERT INTO orders (id) VALUES (1)")).await.expect("i");
    sql.copy_into(svc(2)).await.expect("copy");
    let copy = store.sql(svc(2));
    copy.execute(st("INSERT INTO orders (id) VALUES (2)")).await.expect("i");
    assert_eq!(store.backlog(svc(2)).await.expect("b"), 0);
    assert_eq!(store.backlog(svc(1)).await.expect("b"), 1);
    copy.execute(st("DROP TABLE orders"))
        .await
        .expect("no trigger holds it");
}

#[test]
fn import_check_refuses_a_planted_trigger_and_accepts_a_stripped_database() {
    let c = rusqlite::Connection::open_in_memory().expect("db");
    c.execute_batch(include_str!("../../migrations/service_data_0001_init.sql"))
        .expect("bootstrap");
    assert_eq!(rt_store::import_check::schema_problem(&c).expect("check"), None);
    c.execute_batch("CREATE TABLE t (a); CREATE TRIGGER _rt_ev_x AFTER INSERT ON t BEGIN DELETE FROM _rt_kv; END;")
        .expect("plant");
    assert!(rt_store::import_check::schema_problem(&c).expect("check").is_some());
    rt_store::data_events::strip(&c).expect("strip");
    assert_eq!(rt_store::import_check::schema_problem(&c).expect("check"), None);
}
