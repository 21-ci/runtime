//! Statement cache of guest SQL on the pool connections (B3.5): repeated statements are prepared
//! once per connection, and the cache never lets guest SQL reach what the authorizer denies.

use crate::common;

use std::time::Instant;

use common::*;
use rt_store::{BatchMode, KvSetOptions, KvValue, ServiceDataStore, SqlValue, StoreError, ValueTag};

fn text(s: &str) -> SqlValue {
    SqlValue::Text(s.to_string())
}

/// The exact text of statements the KV code keeps in the same per-connection cache (`data.rs`).
/// A guest that replays one must still go through the authorizer, which only runs at prepare time.
const INTERNAL_KV_GET: &str =
    "SELECT value, tag FROM _rt_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)";
const INTERNAL_KV_SET: &str = "INSERT INTO _rt_kv (key, value, tag, expires_at) VALUES (?1, ?2, 0, ?3) \
                               ON CONFLICT(key) DO UPDATE SET value = excluded.value, tag = 0, expires_at = excluded.expires_at";
const INTERNAL_KV_DELETE: &str = "DELETE FROM _rt_kv WHERE key = ?1";

#[tokio::test]
async fn ac_sto_20_the_statement_cache_never_hands_guests_an_internal_statement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let kv = store.kv(svc(1));
    let sql = store.sql(svc(1));

    // Warm every connection's cache with the internal statements: KV writes on the writer, KV
    // reads on a reader (and the writer, before a reader exists).
    for i in 0..4 {
        kv.set(
            format!("k{i}").as_bytes(),
            KvValue {
                tag: ValueTag::Json,
                data: "1".into(),
            },
            KvSetOptions::default(),
        )
        .await
        .expect("kv set");
        kv.get(format!("k{i}").as_bytes()).await.expect("kv get");
        kv.delete(format!("gone{i}").as_bytes()).await.expect("kv delete");
    }

    for (name, stmt, args) in [
        ("get", INTERNAL_KV_GET, vec![text("k0"), SqlValue::Integer(0)]),
        (
            "set",
            INTERNAL_KV_SET,
            vec![text("evil"), SqlValue::Blob(vec![1]), SqlValue::Null],
        ),
        ("delete", INTERNAL_KV_DELETE, vec![text("k0")]),
    ] {
        // Twice: a first denial must not leave something cached that the second call finds.
        for round in 0..2 {
            let err = sql
                .execute(st_args(stmt, args.clone()))
                .await
                .expect_err("internal statement must be denied");
            assert!(
                matches!(err, StoreError::SqlNotAuthorized),
                "{name} round {round}: got {err:?}"
            );
        }
    }
    assert!(
        kv.get(b"evil").await.expect("kv get").is_none(),
        "the denied write must not have happened"
    );
    assert!(
        kv.get(b"k0").await.expect("kv get").is_some(),
        "the denied delete must not have happened"
    );
}

#[tokio::test]
async fn ac_sto_20_cached_statements_see_schema_changes_and_fresh_bindings() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .expect("create");
    for i in 1..=3 {
        sql.execute(st_args(
            "INSERT INTO t (id, v) VALUES (?1, ?2)",
            vec![SqlValue::Integer(i), text(&format!("v{i}"))],
        ))
        .await
        .expect("insert");
    }

    let select = "SELECT * FROM t WHERE id = ?1 OR ?1 IS NULL ORDER BY id";
    // Same text three times on the same connection with different bindings (NULL last: nothing from
    // an earlier call may stay bound).
    let one = sql
        .execute(st_args(select, vec![SqlValue::Integer(2)]))
        .await
        .expect("select 2");
    assert_eq!(one.rows, vec![vec![SqlValue::Integer(2), text("v2")]]);
    let three = sql
        .execute(st_args(select, vec![SqlValue::Integer(3)]))
        .await
        .expect("select 3");
    assert_eq!(three.rows, vec![vec![SqlValue::Integer(3), text("v3")]]);
    let all = sql
        .execute(st_args(select, vec![SqlValue::Null]))
        .await
        .expect("select all");
    assert_eq!(all.rows.len(), 3);
    assert_eq!(all.columns, vec!["id", "v"]);

    // A schema change between two calls of one cached statement (a reader connection learns of a
    // change another connection made when it steps the statement): the new column shows up at once.
    sql.execute(st("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7"))
        .await
        .expect("alter");
    let after = sql
        .execute(st_args(select, vec![SqlValue::Integer(1)]))
        .await
        .expect("select after alter");
    assert_eq!(after.columns, vec!["id", "v", "extra"]);
    assert_eq!(
        after.rows,
        vec![vec![SqlValue::Integer(1), text("v1"), SqlValue::Integer(7)]]
    );

    // A write statement repeated: each call executes once, and the result reports its own effect.
    for expect in 1..=3 {
        let r = sql
            .execute(st_args(
                "UPDATE t SET extra = ?1 WHERE id = ?2",
                vec![SqlValue::Integer(expect), SqlValue::Integer(expect)],
            ))
            .await
            .expect("update");
        assert_eq!(r.rows_affected, 1);
    }
    // And inside batches (same cache).
    let out = sql
        .batch(
            vec![st_args("SELECT extra FROM t WHERE id = ?1", vec![SqlValue::Integer(2)])],
            BatchMode::Read,
        )
        .await
        .expect("batch");
    assert_eq!(out[0].rows, vec![vec![SqlValue::Integer(2)]]);
}

/// Point SELECT latency through the pool, µs per call (prints; run with `--ignored --nocapture`,
/// release for real numbers). The statement cache removes the parse/plan from every call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "perf"]
async fn perf_point_select_through_the_pool() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = data(dir.path());
    let sql = store.sql(svc(1));
    sql.execute(st("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .await
        .expect("create");
    let rows: Vec<_> = (1..=1000)
        .map(|i| {
            st_args(
                "INSERT INTO t (id, v) VALUES (?1, ?2)",
                vec![SqlValue::Integer(i), text(&format!("value-{i}"))],
            )
        })
        .collect();
    sql.batch(rows, BatchMode::Write).await.expect("seed");

    for round in 0..3 {
        let n = 5000;
        let started = Instant::now();
        for i in 0..n {
            let r = sql
                .execute(st_args(
                    "SELECT v FROM t WHERE id = ?1",
                    vec![SqlValue::Integer(i % 1000 + 1)],
                ))
                .await
                .expect("select");
            assert_eq!(r.rows.len(), 1);
        }
        let us = started.elapsed().as_secs_f64() * 1e6 / n as f64;
        eprintln!("pool point SELECT round {round}: {us:.1} us/call");
    }
}
