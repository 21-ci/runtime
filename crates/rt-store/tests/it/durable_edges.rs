//! Durable object storage edges: range membership, deleting a key and the alarm, an empty start
//! bound with no prefix, and an objects page that has more names than it returns.

use crate::common;

use bytes::Bytes;
use common::*;
use rt_store::{DurableListOptions, DurableValue, DurableWrite, ServiceDataStore};

fn value() -> DurableValue {
    DurableValue {
        format: 1,
        data: Bytes::from_static(b"v"),
    }
}

fn put(key: &'static [u8]) -> DurableWrite {
    DurableWrite {
        writes: vec![(Bytes::from_static(key), Some(value()))],
        ..Default::default()
    }
}

#[test]
fn a_list_range_is_its_prefix_intersected_with_the_start_and_end_bounds() {
    let range = |start: Option<&'static [u8]>, end: Option<&'static [u8]>| DurableListOptions {
        prefix: Bytes::from_static(b"p"),
        start: start.map(Bytes::from_static),
        end: end.map(Bytes::from_static),
        limit: 10,
        reverse: false,
    };
    let r = range(Some(b"pb"), Some(b"pd"));
    assert!(r.contains(b"pc"));
    assert!(r.contains(b"pb"), "the start is inclusive");
    assert!(!r.contains(b"pa"), "below the start");
    assert!(!r.contains(b"pd"), "the end is exclusive");
    assert!(!r.contains(b"qc"), "outside the prefix");
    assert!(range(None, None).contains(b"pz"), "no bounds: the whole prefix");
}

#[tokio::test]
async fn a_delete_in_a_write_removes_the_key_and_a_cleared_alarm_is_gone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = data(dir.path()).durable(svc(3));
    d.commit("obj", put(b"k")).await.expect("put");
    assert!(d.get("obj", &[Bytes::from_static(b"k")]).await.expect("get")[0].is_some());

    let del = DurableWrite {
        writes: vec![(Bytes::from_static(b"k"), None)],
        ..Default::default()
    };
    d.commit("obj", del).await.expect("delete");
    assert!(d.get("obj", &[Bytes::from_static(b"k")]).await.expect("get")[0].is_none());

    d.commit(
        "obj",
        DurableWrite {
            alarm: Some(Some(1_000)),
            ..Default::default()
        },
    )
    .await
    .expect("alarm");
    assert_eq!(d.alarm("obj").await.expect("alarm"), Some(1_000));
    d.commit(
        "obj",
        DurableWrite {
            alarm: Some(None),
            ..Default::default()
        },
    )
    .await
    .expect("clear alarm");
    assert_eq!(d.alarm("obj").await.expect("alarm"), None);
}

#[tokio::test]
async fn an_empty_start_with_no_prefix_lists_from_the_first_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = data(dir.path()).durable(svc(4));
    for k in [&b"a"[..], b"m", b"z"] {
        d.commit(
            "o",
            DurableWrite {
                writes: vec![(Bytes::copy_from_slice(k), Some(value()))],
                ..Default::default()
            },
        )
        .await
        .expect("put");
    }
    let opts = DurableListOptions {
        prefix: Bytes::new(),
        start: Some(Bytes::new()),
        end: None,
        limit: 10,
        reverse: false,
    };
    let keys: Vec<_> = d
        .list("o", opts)
        .await
        .expect("list")
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        keys,
        [
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
            Bytes::from_static(b"z")
        ]
    );
}

#[tokio::test]
async fn an_objects_page_that_has_more_names_returns_a_cursor_to_the_next_page() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = data(dir.path()).durable(svc(5));
    for name in ["o1", "o2", "o3", "o4", "o5"] {
        d.commit(name, put(b"k")).await.expect("commit");
    }
    let (first, next) = d.objects("o", None, 2).await.expect("first page");
    assert_eq!(first, ["o1", "o2"]);
    let next = next.expect("more objects, so a cursor");
    let (rest, last) = d.objects("o", Some(&next), 10).await.expect("second page");
    assert_eq!(rest, ["o3", "o4", "o5"]);
    assert_eq!(last, None);
}
