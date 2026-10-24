//! SQL-016: a real full disk. Linux only: the test mounts a 4 MiB tmpfs (passwordless `sudo`, as
//! on the CI runners) and skips itself where it cannot. The portable half (an injected
//! `SQLITE_FULL`) is in `faults.rs`.

#[cfg(target_os = "linux")]
mod linux {
    use crate::common::*;

    use std::process::Command;

    use rt_store::{ServiceDataStore, SqlValue, StoreError};

    struct Tmpfs(std::path::PathBuf);

    impl Drop for Tmpfs {
        fn drop(&mut self) {
            let _ = Command::new("sudo").args(["-n", "umount"]).arg(&self.0).status();
        }
    }

    fn mount(dir: &std::path::Path) -> Option<Tmpfs> {
        let ok = Command::new("sudo")
            .args(["-n", "mount", "-t", "tmpfs", "-o", "size=4m,mode=0777", "tmpfs"])
            .arg(dir)
            .status()
            .is_ok_and(|s| s.success());
        ok.then(|| Tmpfs(dir.to_path_buf()))
    }

    /// SQL-016: writes into a database whose disk fills up fail with `storage_quota`; every row
    /// committed before is still there and the file passes `integrity_check`; deleting rows makes
    /// writes work again.
    #[tokio::test]
    async fn sql_016_a_full_disk_is_storage_quota_and_the_database_stays_consistent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let Some(_mounted) = mount(dir.path()) else {
            eprintln!("skipped: cannot mount a tmpfs here (needs passwordless sudo)");
            return;
        };
        let sql = data(dir.path()).sql(svc(1));
        sql.execute(st("CREATE TABLE t (b BLOB)")).await.expect("create");
        let mut committed = 0i64;
        let err = loop {
            match sql
                .execute(st_args(
                    "INSERT INTO t VALUES (?)",
                    vec![SqlValue::Blob(vec![7u8; 64 * 1024])],
                ))
                .await
            {
                Ok(_) => committed += 1,
                Err(e) => break e,
            }
            assert!(committed < 1_000, "4 MiB never filled up");
        };
        assert!(matches!(err, StoreError::StorageQuota), "{err:?}");
        let n = sql
            .execute(st("SELECT count(*) FROM t"))
            .await
            .expect("read after the failure");
        assert_eq!(
            n.rows,
            vec![vec![SqlValue::Integer(committed)]],
            "every committed row survives"
        );
        let ok = sql.execute(st("PRAGMA integrity_check")).await.expect("integrity");
        assert_eq!(ok.rows, vec![vec![SqlValue::Text("ok".into())]]);
        sql.execute(st("DELETE FROM t")).await.expect("delete frees pages");
        sql.execute(st("VACUUM")).await.ok();
        sql.execute(st_args(
            "INSERT INTO t VALUES (?)",
            vec![SqlValue::Blob(vec![1u8; 1024])],
        ))
        .await
        .expect("writes work again");
    }
}
