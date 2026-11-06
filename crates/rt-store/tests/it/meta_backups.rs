//! The `backups` table (`meta_0003_backups.sql`): upgrade with data present, and its queries.

use crate::common;

use common::*;
use rt_store::{BackupRow, MetaStore, PageReq, SqliteMetaStore, META_MIGRATIONS};

fn row(id: &str, state: &str) -> BackupRow {
    BackupRow {
        id: id.into(),
        state: state.into(),
        trigger: "api".into(),
        started_at: 1,
        finished_at: None,
        bytes: None,
        sha256: None,
        encrypted: false,
        local_path: None,
        remote_key: None,
        verified_at: None,
        rt_version: "t".into(),
        failure: None,
    }
}

/// A `meta.db` from before the table, with tenants in it, upgrades and keeps them.
#[tokio::test(flavor = "multi_thread")]
async fn upgrading_a_populated_database_adds_the_table() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("meta.db");
    {
        let mut c = rusqlite::Connection::open(&path).expect("open");
        for (v, sql) in META_MIGRATIONS.iter().filter(|(v, _)| *v < 3) {
            let tx = c.transaction().expect("tx");
            tx.execute_batch(sql).expect("migration");
            tx.pragma_update(None, "user_version", v).expect("version");
            tx.commit().expect("commit");
        }
        c.execute(
            "INSERT INTO tenants (id, name, plan, created_at, updated_at) VALUES ('ten_x', 'kept', 'free', 1, 1)",
            [],
        )
        .ok();
    }
    let before: i64 = rusqlite::Connection::open(&path)
        .expect("open")
        .query_row("SELECT count(*) FROM tenants", [], |r| r.get(0))
        .expect("count");
    let m = SqliteMetaStore::open(&path).expect("upgrades");
    let t = mk_tenant(&m, "new").await;
    assert_eq!(m.get_tenant(t.id).await.expect("get").expect("tenant").name, "new");
    let after: i64 = rusqlite::Connection::open(&path)
        .expect("open")
        .query_row("SELECT count(*) FROM tenants", [], |r| r.get(0))
        .expect("count");
    assert_eq!(after, before + 1);
    m.backup_insert(row("bkp_1", "running")).await.expect("insert");
    assert!(m.backup_get("bkp_1").await.expect("get").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_queries_page_and_sort_out_the_states() {
    let m = meta();
    assert!(m.backup_last_ok().await.expect("ok").is_none());
    assert!(m.last_restored_at().await.expect("restored").is_none());
    for id in ["bkp_1", "bkp_2", "bkp_3"] {
        m.backup_insert(row(id, "running")).await.expect("insert");
    }
    m.backup_mark_ok("bkp_1", 10, 5, "aa".into(), "/p".into())
        .await
        .expect("ok");
    m.backup_mark_ok("bkp_2", 20, 6, "bb".into(), "/q".into())
        .await
        .expect("ok");
    let failure = serde_json::json!({"code": "x"});
    m.backup_mark_failed("bkp_3", 30, failure.clone())
        .await
        .expect("failed");
    assert_eq!(m.backup_last_ok().await.expect("ok").expect("row").id, "bkp_2");
    assert_eq!(
        m.backup_last_failure().await.expect("f").expect("row").failure,
        Some(failure)
    );
    assert_eq!(
        m.backup_old_ok(1)
            .await
            .expect("old")
            .iter()
            .map(|b| b.id.as_str())
            .collect::<Vec<_>>(),
        ["bkp_1"]
    );
    let page = m.backup_list(PageReq { limit: 2, cursor: None }).await.expect("page");
    assert_eq!(page.items[0].id, "bkp_3");
    let next = m
        .backup_list(PageReq {
            limit: 2,
            cursor: page.next_cursor,
        })
        .await
        .expect("page");
    assert_eq!(next.items.len(), 1);
    // A good run after the failure hides it from `last_failure`.
    m.backup_insert(row("bkp_4", "running")).await.expect("insert");
    assert_eq!(m.backup_fail_unfinished(40).await.expect("sweep"), 1);
    m.backup_set_state("bkp_4", "ok").await.expect("state");
    assert!(m.backup_last_failure().await.expect("f").is_none());
    assert!(m.backup_delete("bkp_4").await.expect("del"));
    assert!(!m.backup_delete("bkp_4").await.expect("del"));
    m.append_event(rt_types::NodeEvent::new("node.restored", 77))
        .await
        .expect("event");
    assert_eq!(m.last_restored_at().await.expect("restored"), Some(77));
}
