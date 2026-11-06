//! The sync of `secret_refs` (R15), run from the node's maintenance tick. A pass runs on a task of
//! its own, so a hung Vault delays only the next pass. On an error the last value stays.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_api::secret_refs::{sync_one, SecretSync};
use rt_api::state::{ControlEvent, NodeInfo};
use rt_sec::secret_source::SecretSource;
use rt_sec::secrets::SecretsBackend;
use rt_store::MetaStore;
use rt_types::NodeEvent;
use rt_types::ServiceId;
use tokio::sync::mpsc;

/// Reads per pass.
pub const READS_PER_PASS: u32 = 50;
const SYNCS_PER_MINUTE: u32 = 6;
/// Services remembered by the sync limiter and the wake list.
const MAX_TRACKED: usize = 10_000;

pub struct VaultSync {
    pub meta: Arc<dyn MetaStore>,
    pub secrets: Arc<dyn SecretsBackend>,
    pub source: Arc<dyn SecretSource>,
    pub template: String,
    pub poll: Duration,
    pub events: mpsc::UnboundedSender<NodeEvent>,
    busy: AtomicBool,
    state: Mutex<State>,
}

struct State {
    last_pass: Option<Instant>,
    cursor: Option<(ServiceId, String)>,
    limits: HashMap<ServiceId, (Instant, u32)>,
    asked: HashSet<ServiceId>,
}

impl VaultSync {
    pub fn new(
        meta: Arc<dyn MetaStore>,
        secrets: Arc<dyn SecretsBackend>,
        source: Arc<dyn SecretSource>,
        template: String,
        poll: Duration,
        events: mpsc::UnboundedSender<NodeEvent>,
    ) -> Arc<Self> {
        Arc::new(Self {
            meta,
            secrets,
            source,
            template,
            poll,
            events,
            busy: AtomicBool::new(false),
            state: Mutex::new(State {
                last_pass: None,
                cursor: None,
                limits: HashMap::new(),
                asked: HashSet::new(),
            }),
        })
    }

    /// Called often: starts a pass when one is due and none runs.
    pub fn tick(self: &Arc<Self>, node: Arc<dyn NodeInfo>) {
        {
            let st = self.state.lock();
            let due = st.last_pass.is_none_or(|t| t.elapsed() >= self.poll) || !st.asked.is_empty();
            if !due {
                return;
            }
        }
        if self.busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.pass(node.as_ref()).await;
            this.busy.store(false, Ordering::Release);
        });
    }

    /// One pass: at most [`READS_PER_PASS`] refs, carrying on where the last one stopped.
    pub async fn pass(&self, node: &dyn NodeInfo) {
        let (cursor, asked) = {
            let mut st = self.state.lock();
            st.last_pass = Some(Instant::now());
            (st.cursor.clone(), std::mem::take(&mut st.asked))
        };
        let Ok(refs) = self.meta.scan_secret_refs(cursor, READS_PER_PASS).await else {
            return; // LCOV_EXCL_LINE: meta.db read failure; the next pass tries again
        };
        let wrapped = (refs.len() as u32) < READS_PER_PASS;
        let mut targets = refs;
        // Asked for by the API: its refs come first, whatever the cursor says.
        if !asked.is_empty() {
            if let Ok(all) = self.meta.scan_secret_refs(None, 10 * READS_PER_PASS).await {
                let extra: Vec<_> = all
                    .into_iter()
                    .filter(|t| asked.contains(&t.secret.service) && !targets.contains(t))
                    .collect();
                targets.extend(extra);
            }
        }
        let next = if wrapped {
            None
        } else {
            targets.last().map(|t| (t.secret.service, t.secret.name.clone()))
        };
        self.state.lock().cursor = next;
        let now = chrono::Utc::now().timestamp_millis();
        for t in targets {
            let (svc, name) = (t.secret.service, t.secret.name.clone());
            match sync_one(&self.meta, &self.secrets, &self.source, &self.template, &t, now).await {
                Ok(true) => {
                    node.notify(ControlEvent::SecretsChanged(svc));
                    self.event("secret.synced", &t, None, now);
                }
                Ok(false) => {}
                Err(e) => {
                    let code = e.code();
                    if t.secret.last_error.as_deref() != Some(code) {
                        self.event("secret.sync_failed", &t, Some(code), now);
                    }
                    let _ = self
                        .meta
                        .set_secret_ref_status(svc, &name, None, None, Some(code))
                        .await;
                }
            }
        }
    }

    /// Names and codes only: a value or a source's own text never reaches an event.
    fn event(&self, action: &str, t: &rt_store::SecretRefTarget, code: Option<&str>, now: i64) {
        let mut ev = NodeEvent::new(action.to_string(), now);
        ev.tenant = Some(t.tenant);
        ev.service = Some(t.secret.service);
        ev.data = serde_json::json!({"name": t.secret.name, "code": code});
        let _ = self.events.send(ev);
    }
}

impl SecretSync for VaultSync {
    fn source(&self) -> Arc<dyn SecretSource> {
        self.source.clone()
    }

    fn path_template(&self) -> String {
        self.template.clone()
    }

    fn request_sync(&self, service: ServiceId) -> bool {
        let mut st = self.state.lock();
        let now = Instant::now();
        if st.limits.len() >= MAX_TRACKED {
            st.limits
                .retain(|_, (t, _)| now.duration_since(*t) < Duration::from_secs(60));
            if st.limits.len() >= MAX_TRACKED {
                return false; // LCOV_EXCL_LINE: ten thousand services asked within the same minute
            }
        }
        let e = st.limits.entry(service).or_insert((now, 0));
        if now.duration_since(e.0) >= Duration::from_secs(60) {
            *e = (now, 0);
        }
        if e.1 >= SYNCS_PER_MINUTE {
            return false;
        }
        e.1 += 1;
        st.asked.insert(service);
        true
    }
}
