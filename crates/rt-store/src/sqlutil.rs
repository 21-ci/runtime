//! Small helpers shared by the SQLite-backed stores: clock, migrations, [`Db`] (a writer
//! connection plus read-only connections, each on its own dedicated thread), paging and
//! row-conversion utilities.

use std::error::Error as StdError;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rusqlite::types::Type;
use rusqlite::{Connection, OpenFlags, TransactionBehavior};

use crate::error::{Result, StoreError};

/// Unix time in milliseconds.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A unique, filesystem-safe name (temp files, blob object files).
pub(crate) fn unique_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{:x}-{:x}-{:x}",
        std::process::id(),
        nanos,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Apply forward-only `migrations` (`(version, sql)`, ascending) tracked in `PRAGMA user_version`.
/// Refuses a database that is newer than this binary. Returns the schema version afterwards.
pub(crate) fn migrate(conn: &mut Connection, migrations: &[(u32, &str)]) -> Result<u32> {
    let current: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let newest = migrations.iter().map(|(v, _)| *v).max().unwrap_or(0);
    if current > newest {
        return Err(StoreError::Corrupt(format!(
            "database schema version {current} is newer than this binary supports ({newest})"
        )));
    }
    for (version, sql) in migrations.iter().filter(|(v, _)| *v > current) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", *version)?;
        tx.commit()?;
    }
    Ok(newest)
}

/// Open a read-only connection to the WAL database at `path` (the writer must have created it and
/// switched it to WAL first). `SQLITE_OPEN_READ_ONLY` plus `query_only` make writes impossible.
pub(crate) fn open_reader(path: &Path, stmt_cache: usize) -> Result<Connection> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )?;
    c.busy_timeout(Duration::from_secs(5))?;
    c.pragma_update(None, "query_only", true)?;
    c.pragma_update(None, "temp_store", "MEMORY")?;
    c.set_prepared_statement_cache_capacity(stmt_cache);
    Ok(c)
}

/// A job for a database worker thread.
type Job = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

/// One SQLite database served by a fixed set of dedicated threads: one writer thread owning the
/// only read-write connection and `readers` threads owning read-only connections. In WAL mode the
/// readers see the last committed snapshot while the writer is inside a transaction, so reads never
/// queue behind writes. Callers never block a runtime thread and never spawn a thread per call
/// (the old `spawn_blocking` per call parked hundreds of blocking threads on one mutex under load):
/// jobs travel over channels and results come back on oneshots. Without readers (in-memory
/// databases) reads run on the writer.
pub(crate) struct Db {
    writer: Sender<Job>,
    /// Shared queue of the reader threads (`None`: reads go to the writer).
    readers: Option<Sender<Job>>,
    /// Faults injected by tests ([`crate::faults`]); empty in production.
    faults: crate::faults::FaultSlot,
}

impl Db {
    /// `conn` is the (migrated) read-write connection, `readers` the read-only ones. `name` labels
    /// the threads (`{name}-w`, `{name}-r0`, …).
    pub(crate) fn new(name: &str, conn: Connection, readers: Vec<Connection>) -> Result<Arc<Self>> {
        let (writer, rx) = std::sync::mpsc::channel::<Job>();
        spawn_worker(format!("{name}-w"), conn, move || rx.recv().ok(), true)?;
        let readers = if readers.is_empty() {
            None
        } else {
            let (tx, rx) = std::sync::mpsc::channel::<Job>();
            let rx = Arc::new(Mutex::new(rx));
            for (i, c) in readers.into_iter().enumerate() {
                let rx = rx.clone();
                spawn_worker(format!("{name}-r{i}"), c, move || rx.lock().recv().ok(), false)?;
            }
            Some(tx)
        };
        Ok(Arc::new(Self {
            writer,
            readers,
            faults: crate::faults::FaultSlot::default(),
        }))
    }

    /// Run `f` with the read-write connection on the writer thread.
    pub(crate) async fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        self.faults.check()?;
        submit(&self.writer, f).await
    }

    /// Make this database's next operations fail as `faults` plans (tests only).
    pub(crate) fn inject_faults(&self, faults: std::sync::Arc<crate::faults::Faults>) {
        self.faults.set(faults);
    }

    /// Run a read-only `f` on a reader thread (the writer when there are no readers). A read
    /// started after a write returned sees that write.
    pub(crate) async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        self.faults.check()?;
        submit(self.readers.as_ref().unwrap_or(&self.writer), move |c| f(c)).await
    }

    /// Run `f` inside an immediate transaction: commit on `Ok`, roll back on `Err`.
    pub(crate) async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        self.call(move |c| {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let out = f(&tx)?;
            tx.commit()?;
            Ok(out)
        })
        .await
    }
}

async fn submit<T, F>(queue: &Sender<Job>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    let job: Job = Box::new(move |c| {
        let _ = tx.send(f(c));
    });
    queue
        .send(job)
        .map_err(|_| StoreError::Internal("database worker stopped".into()))?;
    // A dropped sender means the job panicked (the worker caught it and kept serving).
    rx.await
        .map_err(|_| StoreError::Internal("database job failed".into()))?
}

/// Serve jobs from `next` on a new thread until the queue closes. A panicking job is contained
/// (its caller gets an error; an open transaction rolls back as it unwinds). On exit the writer
/// runs `PRAGMA optimize` before closing.
fn spawn_worker(
    name: String,
    mut conn: Connection,
    next: impl Fn() -> Option<Job> + Send + 'static,
    optimize_on_close: bool,
) -> Result<()> {
    std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            while let Some(job) = next() {
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| job(&mut conn)));
            }
            if optimize_on_close {
                let _ = conn.execute_batch("PRAGMA optimize");
            }
        })
        .map_err(|e| StoreError::Internal(format!("cannot start a database thread: {e}")))?;
    Ok(())
}

pub(crate) fn is_unique_violation(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(f, _)
            if f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                || f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )
}

// ---- row conversion --------------------------------------------------------------------------

pub(crate) fn bad_col(col: usize, e: impl Into<Box<dyn StdError + Send + Sync>>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(col, Type::Text, e.into())
}

pub(crate) fn parse_col<T>(s: &str, col: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: Into<Box<dyn StdError + Send + Sync>>,
{
    s.parse::<T>().map_err(|e| bad_col(col, e))
}

pub(crate) fn json_col<T: serde::de::DeserializeOwned>(s: &str, col: usize) -> rusqlite::Result<T> {
    serde_json::from_str(s).map_err(|e| bad_col(col, e))
}

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| StoreError::Internal(format!("json encode: {e}")))
}

// ---- paging ----------------------------------------------------------------------------------

/// Clamp a requested page size to `1..=1000`.
pub(crate) fn page_limit(limit: u32) -> usize {
    (limit as usize).clamp(1, 1000)
}

/// Turn `limit + 1` fetched rows into a page.
pub(crate) fn paginate<T>(mut rows: Vec<T>, limit: usize, cursor_of: impl Fn(&T) -> String) -> crate::meta::Page<T> {
    let next_cursor = if rows.len() > limit {
        rows.truncate(limit);
        rows.last().map(cursor_of)
    } else {
        None
    };
    crate::meta::Page {
        items: rows,
        next_cursor,
    }
}

/// Exclusive upper bound of the key range with prefix `prefix` (`None` = unbounded).
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn file_db(dir: &Path, readers: usize) -> Arc<Db> {
        let path = dir.join("t.db");
        let c = Connection::open(&path).expect("open");
        c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0))
            .expect("wal");
        c.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")
            .expect("seed");
        let rs = (0..readers).map(|_| open_reader(&path, 8).expect("reader")).collect();
        Db::new("t", c, rs).expect("db")
    }

    fn count(c: &Connection) -> Result<i64> {
        Ok(c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?)
    }

    #[tokio::test]
    async fn reads_proceed_while_the_writer_is_inside_a_transaction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = file_db(dir.path(), 2);
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let writer = {
            let db = db.clone();
            tokio::spawn(async move {
                db.write(move |c| {
                    c.execute("INSERT INTO t VALUES (2)", [])?;
                    let _ = started_tx.send(());
                    let _ = release_rx.recv_timeout(Duration::from_secs(10));
                    Ok(())
                })
                .await
            })
        };
        started_rx.await.expect("writer started");
        // The writer holds an open write transaction: a read is not queued behind it and sees the
        // last committed snapshot.
        let n = tokio::time::timeout(Duration::from_secs(5), db.read(count))
            .await
            .expect("a read must not wait for the writer")
            .expect("read");
        assert_eq!(n, 1);
        release_tx.send(()).expect("release");
        writer.await.expect("join").expect("write");
        assert_eq!(
            db.read(count).await.expect("read"),
            2,
            "a read after the write returns sees it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn jobs_run_on_a_fixed_set_of_threads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = file_db(dir.path(), 2);
        let mut handles = Vec::new();
        for i in 0..200 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let id = |_: &Connection| Ok(std::thread::current().id());
                if i % 4 == 0 {
                    db.call(move |c| id(c)).await
                } else {
                    db.read(id).await
                }
            }));
        }
        let mut threads = HashSet::new();
        for h in handles {
            threads.insert(h.await.expect("join").expect("job"));
        }
        assert!(
            threads.len() <= 3,
            "1 writer + 2 readers, got {} threads",
            threads.len()
        );
    }

    #[tokio::test]
    async fn a_panicking_job_fails_its_caller_and_the_worker_survives() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = file_db(dir.path(), 1);
        let err = db
            .call(|_| -> Result<()> { panic!("boom") })
            .await
            .expect_err("panicked job");
        assert!(matches!(err, StoreError::Internal(_)), "got {err:?}");
        let err = db
            .read(|_| -> Result<()> { panic!("boom") })
            .await
            .expect_err("panicked read");
        assert!(matches!(err, StoreError::Internal(_)), "got {err:?}");
        assert_eq!(db.call(|c| count(c)).await.expect("writer alive"), 1);
        assert_eq!(db.read(count).await.expect("reader alive"), 1);
    }

    #[tokio::test]
    async fn readers_cannot_write_and_without_readers_reads_use_the_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = file_db(dir.path(), 1);
        assert!(
            db.read(|c| Ok(c.execute("INSERT INTO t VALUES (9)", [])?))
                .await
                .is_err(),
            "readers are read-only"
        );

        let mem = Db::new("m", Connection::open_in_memory().expect("mem"), Vec::new()).expect("db");
        mem.call(|c| Ok(c.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")?))
            .await
            .expect("seed");
        assert_eq!(mem.read(count).await.expect("read on the writer"), 1);
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn a_reader_cannot_open_a_database_that_does_not_exist() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            open_reader(&dir.path().join("missing.db"), 4).is_err(),
            "a read-only open never creates the file"
        );
    }

    #[test]
    fn a_bad_column_names_its_index_and_is_a_text_conversion_failure() {
        let e = bad_col(3, "not a number");
        assert!(
            matches!(e, rusqlite::Error::FromSqlConversionFailure(3, Type::Text, _)),
            "{e:?}"
        );
    }

    #[test]
    fn a_prefix_of_only_max_bytes_has_no_upper_bound() {
        assert_eq!(
            prefix_end(b"ab\xff"),
            Some(b"ac".to_vec()),
            "the last byte carries into the one before it"
        );
        assert_eq!(prefix_end(b"\xff\xff"), None, "nothing above all-0xff");
        assert_eq!(prefix_end(b""), None);
    }
}
