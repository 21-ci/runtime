//! The anonymous daily report (SECURITY.md "Telemetry"): one background task, off the request path.
//! WebSocket first, one POST if that got no answer, then nothing until the next day. A failure ends in
//! one debug line: it reaches no readiness check and no caller.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use rand::{Rng as _, RngCore as _};
use rt_api::telemetry::build_report;
use rt_config::Config;
use rt_host::{ClientEvent, EgressBudget, EgressGuard, EgressGuardConfig, EgressPolicy, EgressUsage, WsClient};
use rt_obs::{HdrStatsStore, Metrics, StatsStore};
use rt_store::{MetaStore, SqliteMetaStore, TelemetryState};
use rt_types::Body;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const DAY_MS: i64 = 86_400_000;
const MAX_JITTER_MS: i64 = 3_600_000;
/// Each transport's whole exchange.
const LIMIT: Duration = Duration::from_secs(10);

/// Shown at startup while telemetry is on, and by the install scripts.
pub const NOTICE: &str = "21 Cloud Runtime sends one anonymous report a day: request, project, service, organization and user counts, plus the version and a random id. No IP addresses are stored. See the exact payload with `rt telemetry show`. Opt out with `telemetry.enabled = false` or `DO_NOT_TRACK=1`. The first report is sent 24 hours after the first start.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Delivered,
    /// The server answered no. Final until the next day.
    Refused,
    Failed,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Delivered => "ok",
            Outcome::Refused => "refused",
            Outcome::Failed => "failed",
        }
    }
}

pub struct Sender {
    pub meta: Arc<dyn MetaStore>,
    pub stats: Arc<dyn StatsStore>,
    pub guard: Arc<dyn EgressGuard>,
    pub metrics: Option<Arc<Metrics>>,
    pub url: String,
}

/// The node's guard settings, tightened: `https` only, no redirects, no pooling, a small answer.
pub fn guard_config(base: &EgressGuardConfig) -> EgressGuardConfig {
    EgressGuardConfig {
        max_redirects: 0,
        max_response_bytes: 4096,
        max_request_bytes: 4096,
        allow_https_downgrade: false,
        max_pooled_clients: 0,
        require_https: true,
        ..base.clone()
    }
}

fn policy() -> EgressPolicy {
    EgressPolicy::from_rules(&rt_types::EgressRules::default(), false)
}

fn answer(text: &str) -> Option<Outcome> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    match v.get("ok")?.as_bool()? {
        true => Some(Outcome::Delivered),
        false => Some(Outcome::Refused),
    }
}

/// `None`: no answer, so the POST may try.
async fn over_websocket(guard: &dyn EgressGuard, base: &str, body: &str) -> Option<Outcome> {
    let rest = base.strip_prefix("https://")?;
    let url = url::Url::parse(&format!("wss://{}/v1/ws", rest.trim_end_matches('/'))).ok()?;
    let io = guard.dial(&url, &policy()).await.ok()?;
    let ws = WsClient::handshake(io, &url, &[], EgressUsage::new(None), None)
        .await
        .ok()?;
    ws.send(rt_types::WsData::Text(body.to_string())).await.ok()?;
    let got = match ws.recv().await.ok()? {
        ClientEvent::Message(rt_types::WsData::Text(t)) => answer(&t),
        _ => None,
    };
    let _ = ws.close(1000, "").await;
    got
}

async fn over_post(guard: &dyn EgressGuard, base: &str, body: &str) -> Outcome {
    let uri = format!("{}/v1/report", base.trim_end_matches('/'));
    let Ok(req) = http::Request::builder()
        .method(http::Method::POST)
        .uri(uri)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string().into_bytes()))
    else {
        return Outcome::Failed;
    };
    // No tenant on the budget: this is charged to no tenant's egress quota.
    let mut budget = EgressBudget::shared(
        1,
        Arc::new(Semaphore::new(1)),
        EgressUsage::new(None),
        Instant::now() + LIMIT,
    );
    match guard.send(req, &policy(), &mut budget).await {
        Ok(r) if r.status() == http::StatusCode::OK => Outcome::Delivered,
        Ok(_) => Outcome::Refused,
        Err(_) => Outcome::Failed,
    }
}

/// One report, at most two attempts.
pub async fn deliver(guard: &dyn EgressGuard, base: &str, body: &str) -> Outcome {
    if let Ok(Some(done)) = tokio::time::timeout(LIMIT, over_websocket(guard, base, body)).await {
        return done;
    }
    tokio::time::timeout(LIMIT, over_post(guard, base, body))
        .await
        .unwrap_or(Outcome::Failed)
}

fn new_install_id() -> String {
    let mut b = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

/// The first send is 24 hours after the first start; the rest a day and the jitter after the last.
pub fn next_due_ms(state: &TelemetryState, now_ms: i64, jitter_ms: i64) -> i64 {
    match (state.last_sent_ms, state.first_start_ms) {
        (Some(last), _) => last + DAY_MS + jitter_ms,
        (None, Some(first)) => first + DAY_MS,
        (None, None) => now_ms + DAY_MS,
    }
}

impl Sender {
    pub async fn send_once(&self, now_ms: i64) -> rt_store::Result<Outcome> {
        let id = self.meta.telemetry_install_id(new_install_id()).await?;
        let report = build_report(&*self.meta, &*self.stats, now_ms, id).await?;
        let body = serde_json::to_string(&report).unwrap_or_default();
        let outcome = deliver(&*self.guard, &self.url, &body).await;
        if let Some(m) = &self.metrics {
            m.telemetry_sends_total.with_label_values(&[outcome.label()]).inc();
        }
        self.meta.telemetry_mark_sent(now_ms).await?;
        Ok(outcome)
    }

    async fn run(self) {
        let now = || chrono::Utc::now().timestamp_millis();
        let jitter = || rand::thread_rng().gen_range(0..=MAX_JITTER_MS);
        let mut due = match self.meta.telemetry_note_start(now()).await {
            Ok(state) => next_due_ms(&state, now(), jitter()),
            Err(e) => {
                tracing::debug!(error = %e, "telemetry state unavailable");
                now() + DAY_MS
            }
        };
        loop {
            let wait = due.saturating_sub(now()).max(0) as u64;
            tokio::time::sleep(Duration::from_millis(wait)).await;
            match self.send_once(now()).await {
                Ok(outcome) => tracing::debug!(result = outcome.label(), "telemetry report"),
                Err(e) => tracing::debug!(error = %e, "telemetry report failed"),
            }
            due = now() + DAY_MS + jitter();
        }
    }
}

/// Start the task if telemetry is on and has a URL, with the startup notice. Otherwise nothing runs
/// and nothing is written.
pub fn start(
    config: &Config,
    meta: Arc<dyn MetaStore>,
    stats: Arc<dyn StatsStore>,
    guard: Arc<dyn EgressGuard>,
    metrics: Option<Arc<Metrics>>,
) -> Option<JoinHandle<()>> {
    let url = rt_config::telemetry_url(&config.telemetry)?;
    tracing::info!("{NOTICE}");
    let sender = Sender {
        meta,
        stats,
        guard,
        metrics,
        url,
    };
    Some(tokio::spawn(sender.run()))
}

/// `rt telemetry show`. Requests are counted in the memory of the running node, so this offline view
/// shows 0 for them and says so; `21ci telemetry show` asks the node.
pub async fn show(config: &Config) -> anyhow::Result<String> {
    let path = std::path::Path::new(&config.node.data_dir).join("meta.db");
    anyhow::ensure!(
        path.is_file(),
        "{} does not exist: start the node first",
        path.display()
    );
    let meta = SqliteMetaStore::open(&path).context("meta.db")?;
    let stats = HdrStatsStore::new(Arc::new(|| chrono::Utc::now().timestamp_millis()));
    let url = rt_config::telemetry_url(&config.telemetry);
    let now = chrono::Utc::now().timestamp_millis();
    let p = rt_api::telemetry::preview(meta.as_ref(), stats.as_ref(), now, url).await?;
    Ok(format!(
        "Sending: {}\nURL: {}\nNote: `requests` lives in the running node's memory; `21ci telemetry show` has the real figure.\n{}",
        if p.sending { "on" } else { "off" },
        p.url.as_deref().unwrap_or("-"),
        serde_json::to_string_pretty(&p.report)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures_util::{SinkExt as _, StreamExt as _};
    use parking_lot::Mutex;
    use rt_host::{EgressError, EgressIo};
    use tokio_tungstenite::tungstenite::Message;

    const BASE: &str = "https://t.example/s/telemetry";

    /// What the WebSocket peer does.
    #[derive(Clone, Copy)]
    enum Ws {
        Answer(&'static str),
        CloseSilently,
        Refuse,
        Hang,
    }

    struct Guard {
        ws: Ws,
        post: Result<u16, EgressError>,
        ws_bodies: Arc<Mutex<Vec<String>>>,
        post_uris: Mutex<Vec<String>>,
        ws_urls: Mutex<Vec<String>>,
    }

    impl Guard {
        fn new(ws: Ws, post: Result<u16, EgressError>) -> Self {
            Self {
                ws,
                post,
                ws_bodies: Arc::default(),
                post_uris: Mutex::default(),
                ws_urls: Mutex::default(),
            }
        }
    }

    #[async_trait]
    impl EgressGuard for Guard {
        async fn send(
            &self,
            req: rt_host::Request,
            _policy: &EgressPolicy,
            _budget: &mut EgressBudget,
        ) -> Result<rt_host::Response, EgressError> {
            assert!(req.headers().get(http::header::AUTHORIZATION).is_none());
            assert!(req.headers().get(http::header::COOKIE).is_none());
            self.post_uris.lock().push(req.uri().to_string());
            let status = self.post.as_ref().map(|s| *s).map_err(|_| EgressError::Upstream)?;
            let mut r = http::Response::new(Body::empty());
            *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
            Ok(r)
        }

        async fn dial(&self, url: &url::Url, _policy: &EgressPolicy) -> Result<Box<dyn EgressIo>, EgressError> {
            self.ws_urls.lock().push(url.to_string());
            let mode = self.ws;
            if matches!(mode, Ws::Refuse) {
                return Err(EgressError::Denied);
            }
            let (client, server) = tokio::io::duplex(4096);
            let bodies = self.ws_bodies.clone();
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(server).await.unwrap();
                if let Some(Ok(Message::Text(t))) = ws.next().await {
                    bodies.lock().push(t.as_str().to_string());
                }
                match mode {
                    Ws::Answer(text) => {
                        let _ = ws.send(Message::text(text)).await;
                        let _ = ws.close(None).await;
                    }
                    Ws::CloseSilently => {
                        let _ = ws.close(None).await;
                    }
                    Ws::Hang => std::future::pending::<()>().await,
                    Ws::Refuse => {}
                }
            });
            Ok(Box::new(client))
        }
    }

    #[tokio::test]
    async fn the_websocket_answer_ends_it() {
        let g = Guard::new(Ws::Answer(r#"{"ok":true}"#), Ok(200));
        assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Delivered);
        assert_eq!(g.ws_urls.lock()[0], "wss://t.example/s/telemetry/v1/ws");
        assert!(g.post_uris.lock().is_empty());
    }

    #[tokio::test]
    async fn a_refusal_does_not_fall_back() {
        let g = Guard::new(Ws::Answer(r#"{"ok":false,"code":"rate_limited"}"#), Ok(200));
        assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Refused);
        assert!(g.post_uris.lock().is_empty());
    }

    #[tokio::test]
    async fn no_answer_falls_back_to_one_post() {
        for ws in [Ws::CloseSilently, Ws::Refuse, Ws::Answer("not json")] {
            let g = Guard::new(ws, Ok(200));
            assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Delivered);
            assert_eq!(*g.post_uris.lock(), ["https://t.example/s/telemetry/v1/report"]);
        }
    }

    #[tokio::test]
    async fn a_post_refusal_or_error_ends_it_without_a_retry() {
        let g = Guard::new(Ws::Refuse, Ok(429));
        assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Refused);
        assert_eq!(g.post_uris.lock().len(), 1);
        let g = Guard::new(Ws::Refuse, Err(EgressError::Upstream));
        assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Failed);
        assert_eq!(g.post_uris.lock().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_socket_is_cut_at_the_limit() {
        let g = Guard::new(Ws::Hang, Err(EgressError::Upstream));
        assert_eq!(deliver(&g, BASE, "{}").await, Outcome::Failed);
        assert_eq!(g.post_uris.lock().len(), 1);
    }

    #[test]
    fn the_first_send_is_a_day_after_the_first_start_and_a_restart_does_not_send_early() {
        let first = TelemetryState {
            first_start_ms: Some(100),
            ..TelemetryState::default()
        };
        assert_eq!(next_due_ms(&first, 5_000, 7), 100 + DAY_MS);
        let sent = TelemetryState {
            first_start_ms: Some(100),
            last_sent_ms: Some(200),
            ..TelemetryState::default()
        };
        assert_eq!(next_due_ms(&sent, 5_000, 7), 200 + DAY_MS + 7);
        assert_eq!(next_due_ms(&TelemetryState::default(), 5_000, 7), 5_000 + DAY_MS);
    }

    fn sender(guard: Arc<dyn EgressGuard>) -> (Sender, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let meta = SqliteMetaStore::open(&dir.path().join("meta.db")).unwrap();
        let s = Sender {
            meta,
            stats: HdrStatsStore::new(Arc::new(|| 0)),
            guard,
            metrics: None,
            url: BASE.to_string(),
        };
        (s, dir)
    }

    #[tokio::test]
    async fn the_sent_body_is_the_preview_and_the_id_is_kept() {
        let g = Arc::new(Guard::new(Ws::Answer(r#"{"ok":true}"#), Ok(200)));
        let (s, _d) = sender(g.clone());
        let before = rt_api::telemetry::preview(&*s.meta, &*s.stats, 1_000, Some(BASE.into()))
            .await
            .unwrap();
        assert_eq!(before.report.install_id, rt_api::telemetry::PLACEHOLDER_INSTALL_ID);
        assert_eq!(
            s.meta.telemetry_state().await.unwrap().install_id,
            None,
            "a preview creates no id"
        );
        assert_eq!(s.send_once(1_000).await.unwrap(), Outcome::Delivered);
        let state = s.meta.telemetry_state().await.unwrap();
        assert_eq!(state.last_sent_ms, Some(1_000));
        let id = state.install_id.unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        let after = rt_api::telemetry::preview(&*s.meta, &*s.stats, 1_000, Some(BASE.into()))
            .await
            .unwrap();
        assert_eq!(g.ws_bodies.lock()[0], serde_json::to_string(&after.report).unwrap());
        s.send_once(2_000).await.unwrap();
        assert_eq!(s.meta.telemetry_state().await.unwrap().install_id.unwrap(), id);
    }

    #[tokio::test]
    async fn an_unreachable_url_is_only_a_counted_failure() {
        let (s, _d) = sender(Arc::new(Guard::new(Ws::Refuse, Err(EgressError::Dns))));
        assert_eq!(s.send_once(1_000).await.unwrap(), Outcome::Failed);
        assert_eq!(s.meta.telemetry_state().await.unwrap().last_sent_ms, Some(1_000));
    }

    #[tokio::test]
    async fn the_real_guard_refuses_a_private_address() {
        struct Loopback;
        #[async_trait]
        impl rt_host::Resolver for Loopback {
            async fn resolve(&self, _host: &str) -> std::io::Result<Vec<std::net::IpAddr>> {
                Ok(vec![std::net::IpAddr::from([127, 0, 0, 1])])
            }
        }
        let cfg = guard_config(&EgressGuardConfig::default());
        let guard = rt_host::GuardedEgress::new(cfg, Arc::new(rt_sec::SelfDenylist::new(Vec::new(), Vec::new())))
            .with_resolver(Arc::new(Loopback));
        assert_eq!(deliver(&guard, BASE, "{}").await, Outcome::Failed);
    }

    #[test]
    fn nothing_starts_without_a_url() {
        let mut cfg = Config::default();
        cfg.telemetry.url = String::new();
        assert_eq!(rt_config::telemetry_url(&cfg.telemetry), None);
    }
}
