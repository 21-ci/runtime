//! `telemetry_state`: the id is kept, the first start is written once, reads write nothing.

use rt_store::{MetaStore, SqliteMetaStore, TelemetryState};

fn open() -> (std::sync::Arc<SqliteMetaStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let m = SqliteMetaStore::open(&dir.path().join("meta.db")).expect("open");
    (m, dir)
}

#[tokio::test]
async fn nothing_exists_until_it_is_written() {
    let (m, _d) = open();
    assert_eq!(m.telemetry_state().await.unwrap(), TelemetryState::default());
    assert_eq!(m.telemetry_state().await.unwrap().install_id, None);
}

#[tokio::test]
async fn the_first_start_and_the_id_are_written_once() {
    let (m, _d) = open();
    assert_eq!(m.telemetry_note_start(10).await.unwrap().first_start_ms, Some(10));
    assert_eq!(m.telemetry_note_start(20).await.unwrap().first_start_ms, Some(10));
    assert_eq!(m.telemetry_install_id("a".into()).await.unwrap(), "a");
    assert_eq!(m.telemetry_install_id("b".into()).await.unwrap(), "a");
    m.telemetry_mark_sent(30).await.unwrap();
    m.telemetry_set_console_users(7, 40).await.unwrap();
    let s = m.telemetry_state().await.unwrap();
    assert_eq!(s.last_sent_ms, Some(30));
    assert_eq!((s.console_users, s.console_users_at), (Some(7), Some(40)));
    assert_eq!(s.install_id.as_deref(), Some("a"));
}

#[tokio::test]
async fn a_console_count_alone_creates_no_id() {
    let (m, _d) = open();
    m.telemetry_set_console_users(1, 1).await.unwrap();
    let s = m.telemetry_state().await.unwrap();
    assert_eq!(s.install_id, None);
    assert_eq!(s.first_start_ms, None);
}

#[tokio::test]
async fn counts_start_at_zero() {
    let (m, _d) = open();
    let c = m.telemetry_counts().await.unwrap();
    assert_eq!((c.projects, c.services, c.organizations), (0, 0, 0));
}
