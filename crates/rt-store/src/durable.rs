//! Durable object storage.
//!
//! Every object of a durable service keeps its keys and its alarm as rows of the service's own
//! `data.db` (`_rt_durable`, `_rt_durable_alarm`, keyed by object name), not a file per object.
//! Reads run on the reader pool; a task's writes are applied by [`DurableBackend::commit`] as one
//! coalescable writer job (one savepoint inside the group commit), so all of one event's writes
//! land together or not at all.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rt_types::ServiceId;
use rusqlite::{params, Connection, OptionalExtension};

use crate::data::{
    map_internal_err, GroupFn, GroupOut, Shared, MAX_KV_KEY_BYTES, MAX_KV_LIST_LIMIT, MAX_KV_VALUE_BYTES,
};
use crate::error::{Result, StoreError};

/// A value written as raw bytes (WASM).
pub const DURABLE_FORMAT_BYTES: u8 = 0;
/// A value written as a V8 structured clone (Deno).
pub const DURABLE_FORMAT_V8: u8 = 1;

/// A stored value and how it was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableValue {
    pub format: u8,
    pub data: Bytes,
}

/// What one task changes in an object, applied atomically.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurableWrite {
    /// Drop every key and the alarm first (`storage.deleteAll()`).
    pub delete_all: bool,
    /// Puts (`Some`) and deletes (`None`), in order.
    pub writes: Vec<(Bytes, Option<DurableValue>)>,
    /// `Some(Some(at))` sets the alarm, `Some(None)` removes it, `None` leaves it.
    pub alarm: Option<Option<i64>>,
}

impl DurableWrite {
    pub fn is_empty(&self) -> bool {
        !self.delete_all && self.writes.is_empty() && self.alarm.is_none()
    }
}

/// A range of keys: `prefix`, then `[start, end)`, at most `limit` (≤ 1000), ascending or not.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurableListOptions {
    pub prefix: Bytes,
    pub start: Option<Bytes>,
    pub end: Option<Bytes>,
    pub limit: usize,
    pub reverse: bool,
}

impl DurableListOptions {
    /// `key` is in the range (prefix and bounds; not the limit).
    pub fn contains(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
            && self.start.as_ref().is_none_or(|s| key >= s.as_ref())
            && self.end.as_ref().is_none_or(|e| key < e.as_ref())
    }
}

#[async_trait]
pub trait DurableBackend: Send + Sync {
    /// The values of `keys` of `object` (`None`: absent).
    async fn get(&self, object: &str, keys: &[Bytes]) -> Result<Vec<Option<DurableValue>>>;
    /// Keys of `object` in a range, in key order (descending with `reverse`).
    async fn list(&self, object: &str, opts: DurableListOptions) -> Result<Vec<(Bytes, DurableValue)>>;
    /// Apply one task's writes to `object` atomically.
    async fn commit(&self, object: &str, w: DurableWrite) -> Result<()>;
    /// The object's alarm (Unix ms).
    async fn alarm(&self, object: &str) -> Result<Option<i64>>;
    /// Every object's alarm (loaded when the node starts).
    async fn alarms(&self) -> Result<Vec<(String, i64)>>;
    /// The alarm due at `at_ms` was delivered (or given up): remove it unless it was set again
    /// meanwhile. `true` when it was removed.
    async fn alarm_done(&self, object: &str, at_ms: i64) -> Result<bool>;
    /// Objects with keys or an alarm, by name, after `after`, starting with `prefix`.
    async fn objects(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<(Vec<String>, Option<String>)>;
    /// Drop an object's keys and alarm (`DELETE /v1/services/{s}/objects/{name}`). `true` when it
    /// had any.
    async fn delete_object(&self, object: &str) -> Result<bool>;
}

pub(crate) struct DurableHandle {
    pub(crate) shared: Arc<Shared>,
    pub(crate) svc: ServiceId,
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KV_KEY_BYTES {
        return Err(StoreError::Invalid(format!("a key is 1 to {MAX_KV_KEY_BYTES} bytes")));
    }
    Ok(())
}

fn value_of(r: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<DurableValue> {
    let data: Vec<u8> = r.get(at)?;
    let format: i64 = r.get(at + 1)?;
    Ok(DurableValue {
        format: u8::try_from(format).unwrap_or(DURABLE_FORMAT_BYTES),
        data: Bytes::from(data),
    })
}

fn apply(c: &Connection, object: &str, w: &DurableWrite) -> Result<()> {
    if w.delete_all {
        c.prepare_cached("DELETE FROM _rt_durable WHERE object = ?1")
            .and_then(|mut st| st.execute([object]))
            .map_err(map_internal_err)?;
        c.prepare_cached("DELETE FROM _rt_durable_alarm WHERE object = ?1")
            .and_then(|mut st| st.execute([object]))
            .map_err(map_internal_err)?;
    }
    for (key, value) in &w.writes {
        match value {
            Some(v) => c
                .prepare_cached(
                    "INSERT INTO _rt_durable (object, key, value, format) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(object, key) DO UPDATE SET value = excluded.value, format = excluded.format",
                )
                .and_then(|mut st| st.execute(params![object, key.as_ref(), v.data.as_ref(), i64::from(v.format)])),
            None => c
                .prepare_cached("DELETE FROM _rt_durable WHERE object = ?1 AND key = ?2")
                .and_then(|mut st| st.execute(params![object, key.as_ref()])),
        }
        .map_err(map_internal_err)?;
    }
    match w.alarm {
        Some(Some(at)) => c
            .prepare_cached(
                "INSERT INTO _rt_durable_alarm (object, at_ms) VALUES (?1, ?2) ON CONFLICT(object) DO UPDATE SET at_ms = excluded.at_ms",
            )
            .and_then(|mut st| st.execute(params![object, at]))
            .map(|_| ()),
        Some(None) => c
            .prepare_cached("DELETE FROM _rt_durable_alarm WHERE object = ?1")
            .and_then(|mut st| st.execute([object]))
            .map(|_| ()),
        None => Ok(()),
    }
    .map_err(map_internal_err)
}

impl DurableHandle {
    /// One writer job in its own savepoint: inside a group commit only this job rolls back on
    /// failure; alone it is still one transaction.
    async fn write<F>(&self, mut f: F) -> Result<GroupOut>
    where
        F: FnMut(&Connection) -> Result<GroupOut> + Send + 'static,
    {
        let job: GroupFn = Box::new(move |c| {
            c.execute_batch("SAVEPOINT rt_durable").map_err(map_internal_err)?;
            let r = f(c);
            let end = if r.is_ok() {
                "RELEASE rt_durable"
            } else {
                "ROLLBACK TO rt_durable; RELEASE rt_durable"
            };
            c.execute_batch(end).map_err(map_internal_err)?;
            r
        });
        self.shared.write_queued(self.svc, job).await
    }
}

#[async_trait]
impl DurableBackend for DurableHandle {
    async fn get(&self, object: &str, keys: &[Bytes]) -> Result<Vec<Option<DurableValue>>> {
        for k in keys {
            check_key(k)?;
        }
        if keys.is_empty() || self.shared.is_purged(self.svc) {
            return Ok(vec![None; keys.len()]);
        }
        let (object, keys) = (object.to_string(), keys.to_vec());
        self.shared
            .read(self.svc, move |conn| {
                keys.iter()
                    .map(|k| {
                        conn.c
                            .prepare_cached("SELECT value, format FROM _rt_durable WHERE object = ?1 AND key = ?2")
                            .and_then(|mut st| st.query_row(params![object, k.as_ref()], |r| value_of(r, 0)).optional())
                            .map_err(map_internal_err)
                    })
                    .collect()
            })
            .await
    }

    async fn list(&self, object: &str, opts: DurableListOptions) -> Result<Vec<(Bytes, DurableValue)>> {
        if self.shared.is_purged(self.svc) {
            return Ok(Vec::new());
        }
        let limit = opts.limit.clamp(1, MAX_KV_LIST_LIMIT);
        let object = object.to_string();
        self.shared
            .read(self.svc, move |conn| {
                let mut conds = vec!["object = ?"];
                let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(object)];
                let lo = match (&opts.start, opts.prefix.is_empty()) {
                    (Some(s), _) if s.as_ref() > opts.prefix.as_ref() => Some(s.to_vec()),
                    (_, false) => Some(opts.prefix.to_vec()),
                    (Some(s), true) => Some(s.to_vec()),
                    (None, true) => None,
                };
                if let Some(lo) = lo {
                    conds.push("key >= ?");
                    args.push(Box::new(lo));
                }
                let hi_prefix = crate::sqlutil::prefix_end(&opts.prefix).filter(|_| !opts.prefix.is_empty());
                let hi = match (opts.end.as_ref().map(|e| e.to_vec()), hi_prefix) {
                    (Some(e), Some(p)) => Some(e.min(p)),
                    (e, p) => e.or(p),
                };
                if let Some(hi) = hi {
                    conds.push("key < ?");
                    args.push(Box::new(hi));
                }
                args.push(Box::new(limit as i64));
                let sql = format!(
                    "SELECT key, value, format FROM _rt_durable WHERE {} ORDER BY key {} LIMIT ?",
                    conds.join(" AND "),
                    if opts.reverse { "DESC" } else { "ASC" }
                );
                conn.c
                    .prepare_cached(&sql)
                    .and_then(|mut st| {
                        st.query_map(rusqlite::params_from_iter(args.iter().map(|a| a.as_ref())), |r| {
                            let k: Vec<u8> = r.get(0)?;
                            Ok((Bytes::from(k), value_of(r, 1)?))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(map_internal_err)
            })
            .await
    }

    async fn commit(&self, object: &str, w: DurableWrite) -> Result<()> {
        if w.is_empty() {
            return Ok(());
        }
        for (k, v) in &w.writes {
            check_key(k)?;
            if v.as_ref().is_some_and(|v| v.data.len() > MAX_KV_VALUE_BYTES) {
                return Err(StoreError::Invalid(format!(
                    "a value is at most {MAX_KV_VALUE_BYTES} bytes"
                )));
            }
        }
        self.shared.unpurge(self.svc);
        let object = object.to_string();
        self.write(move |c| apply(c, &object, &w).map(|()| GroupOut::Done))
            .await
            .map(|_| ())
    }

    async fn alarm(&self, object: &str) -> Result<Option<i64>> {
        if self.shared.is_purged(self.svc) {
            return Ok(None);
        }
        let object = object.to_string();
        self.shared
            .read(self.svc, move |conn| {
                conn.c
                    .prepare_cached("SELECT at_ms FROM _rt_durable_alarm WHERE object = ?1")
                    .and_then(|mut st| st.query_row([object], |r| r.get::<_, i64>(0)).optional())
                    .map_err(map_internal_err)
            })
            .await
    }

    async fn alarms(&self) -> Result<Vec<(String, i64)>> {
        if self.shared.is_purged(self.svc) {
            return Ok(Vec::new());
        }
        self.shared
            .read(self.svc, move |conn| {
                conn.c
                    .prepare_cached("SELECT object, at_ms FROM _rt_durable_alarm ORDER BY at_ms")
                    .and_then(|mut st| {
                        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(map_internal_err)
            })
            .await
    }

    async fn alarm_done(&self, object: &str, at_ms: i64) -> Result<bool> {
        if self.shared.is_purged(self.svc) {
            return Ok(false);
        }
        let object = object.to_string();
        let out = self
            .write(move |c| {
                let n = c
                    .prepare_cached("DELETE FROM _rt_durable_alarm WHERE object = ?1 AND at_ms = ?2")
                    .and_then(|mut st| st.execute(params![object, at_ms]))
                    .map_err(map_internal_err)?;
                Ok(GroupOut::Flag(n > 0))
            })
            .await?;
        Ok(matches!(out, GroupOut::Flag(true)))
    }

    async fn objects(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<(Vec<String>, Option<String>)> {
        if self.shared.is_purged(self.svc) {
            return Ok((Vec::new(), None));
        }
        let limit = limit.clamp(1, MAX_KV_LIST_LIMIT);
        let (prefix, after) = (prefix.to_string(), after.unwrap_or("").to_string());
        self.shared
            .read(self.svc, move |conn| {
                let mut names: Vec<String> = conn
                    .c
                    .prepare_cached(
                        "SELECT object FROM (SELECT DISTINCT object FROM _rt_durable UNION SELECT object FROM _rt_durable_alarm) \
                         WHERE object > ?1 AND substr(object, 1, length(?2)) = ?2 ORDER BY object LIMIT ?3",
                    )
                    .and_then(|mut st| {
                        st.query_map(params![after, prefix, limit as i64 + 1], |r| r.get(0))?.collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(map_internal_err)?;
                let next = if names.len() > limit {
                    names.truncate(limit);
                    names.last().cloned()
                } else {
                    None
                };
                Ok((names, next))
            })
            .await
    }

    async fn delete_object(&self, object: &str) -> Result<bool> {
        if self.shared.is_purged(self.svc) {
            return Ok(false);
        }
        let object = object.to_string();
        let out = self
            .write(move |c| {
                let keys = c
                    .prepare_cached("DELETE FROM _rt_durable WHERE object = ?1")
                    .and_then(|mut st| st.execute([&object]))
                    .map_err(map_internal_err)?;
                let alarm = c
                    .prepare_cached("DELETE FROM _rt_durable_alarm WHERE object = ?1")
                    .and_then(|mut st| st.execute([&object]))
                    .map_err(map_internal_err)?;
                Ok(GroupOut::Flag(keys + alarm > 0))
            })
            .await?;
        Ok(matches!(out, GroupOut::Flag(true)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{DataConfig, LocalServiceData, ServiceDataStore};

    fn v(s: &str) -> DurableValue {
        DurableValue {
            format: DURABLE_FORMAT_V8,
            data: Bytes::copy_from_slice(s.as_bytes()),
        }
    }

    fn store() -> (tempfile::TempDir, Arc<dyn DurableBackend>) {
        let dir = tempfile::tempdir().expect("tmp");
        let data = LocalServiceData::open(DataConfig::new(dir.path())).expect("open");
        (dir, data.durable(ServiceId::from_u128(7)))
    }

    fn put(k: &str, val: &str) -> (Bytes, Option<DurableValue>) {
        (Bytes::copy_from_slice(k.as_bytes()), Some(v(val)))
    }

    #[tokio::test]
    async fn objects_keep_their_own_keys_and_a_commit_is_all_or_nothing() {
        let (_d, s) = store();
        s.commit(
            "room-1",
            DurableWrite {
                writes: vec![put("a", "1"), put("b", "2")],
                ..Default::default()
            },
        )
        .await
        .expect("commit");
        s.commit(
            "room-2",
            DurableWrite {
                writes: vec![put("a", "other")],
                ..Default::default()
            },
        )
        .await
        .expect("commit");
        let got = s
            .get("room-1", &[Bytes::from_static(b"a"), Bytes::from_static(b"zz")])
            .await
            .expect("get");
        assert_eq!(got, vec![Some(v("1")), None]);
        assert_eq!(
            s.get("room-2", &[Bytes::from_static(b"a")]).await.expect("get"),
            vec![Some(v("other"))]
        );
        // A write that fails (an empty key) leaves nothing of its commit behind.
        let bad = DurableWrite {
            writes: vec![put("c", "3"), (Bytes::new(), Some(v("x")))],
            ..Default::default()
        };
        assert!(s.commit("room-1", bad).await.is_err());
        assert_eq!(
            s.get("room-1", &[Bytes::from_static(b"c")]).await.expect("get"),
            vec![None]
        );
    }

    #[tokio::test]
    async fn list_ranges_delete_all_alarms_and_objects() {
        let (_d, s) = store();
        let w = DurableWrite {
            writes: vec![
                put("user:1", "a"),
                put("user:2", "b"),
                put("user:3", "c"),
                put("msg:1", "m"),
            ],
            alarm: Some(Some(1_000)),
            ..Default::default()
        };
        s.commit("r", w).await.expect("commit");
        let keys = |page: Vec<(Bytes, DurableValue)>| {
            page.into_iter()
                .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
                .collect::<Vec<_>>()
        };
        let o = DurableListOptions {
            prefix: Bytes::from_static(b"user:"),
            limit: 10,
            ..Default::default()
        };
        assert_eq!(
            keys(s.list("r", o.clone()).await.unwrap()),
            ["user:1", "user:2", "user:3"]
        );
        let o2 = DurableListOptions {
            start: Some(Bytes::from_static(b"user:2")),
            reverse: true,
            limit: 1,
            ..o.clone()
        };
        assert_eq!(keys(s.list("r", o2).await.unwrap()), ["user:3"]);
        let o3 = DurableListOptions {
            end: Some(Bytes::from_static(b"user:3")),
            ..o.clone()
        };
        assert_eq!(keys(s.list("r", o3).await.unwrap()), ["user:1", "user:2"]);
        assert_eq!(s.alarm("r").await.unwrap(), Some(1_000));
        s.commit(
            "q",
            DurableWrite {
                alarm: Some(Some(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            s.alarms().await.unwrap(),
            vec![("q".to_string(), 5), ("r".to_string(), 1_000)]
        );
        assert_eq!(s.objects("", None, 10).await.unwrap().0, ["q", "r"]);
        // A delivered alarm that was set again meanwhile stays.
        s.commit(
            "r",
            DurableWrite {
                alarm: Some(Some(2_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!s.alarm_done("r", 1_000).await.unwrap());
        assert!(s.alarm_done("r", 2_000).await.unwrap());
        s.commit(
            "r",
            DurableWrite {
                delete_all: true,
                writes: vec![put("fresh", "1")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            keys(
                s.list(
                    "r",
                    DurableListOptions {
                        limit: 10,
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
            ),
            ["fresh"]
        );
        assert!(s.delete_object("r").await.unwrap());
        assert_eq!(s.objects("", None, 10).await.unwrap().0, ["q"]);
    }
}
