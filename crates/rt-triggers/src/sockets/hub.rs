//! [`SocketHub`]: sockets, objects, tickets and the per-object event driver (see the module docs).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rt_host::{AcceptOpts, Accepted, HostError, SocketHost, SocketOp, SocketPage, TaskCtx};
use rt_types::{
    valid_app_close_code, valid_object_name, valid_tag, ObjectRef, Outcome, RejectReason, ServiceId, SocketEvent,
    SocketEventKind, SocketInfo, TaskSpec, TenantId, WsData, MAX_ATTACHMENT_BYTES, MAX_CLOSE_REASON_BYTES,
    MAX_SOCKET_TAGS,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};

use super::conn;

/// Node-wide socket settings (`[ws]`, docs/config.md).
#[derive(Clone, Debug)]
pub struct HubConfig {
    /// Open sockets on the node (`ws.max_sockets`); an upgrade over it is refused.
    pub max_sockets: u32,
    /// Server pings every interval; a socket silent for two intervals is dropped (1006).
    pub ping_interval: Duration,
    /// Bytes queued for one client; over it the socket is closed with 1013 (`slow_consumer`).
    pub send_buffer_bytes: u64,
    /// Events queued per object; a full inbox stops reading that object's sockets (backpressure).
    pub inbox_max: usize,
    /// Deliveries of a socket event before the hub gives up (then 1011 for a message).
    pub max_event_attempts: u32,
    /// Deliveries of an alarm before it is dropped.
    pub max_alarm_attempts: u32,
    /// Backoff before the second delivery; doubles per attempt (capped at 30 s).
    pub retry_base: Duration,
    /// An accepted socket whose connection never arrives is released after this long.
    pub ticket_ttl: Duration,
    /// Messages a client may send per second (token bucket) and the burst above it.
    pub msgs_per_sec: u32,
    pub msgs_burst: u32,
    /// A client throttled without a break for this long is closed with 1008.
    pub flood_close_after: Duration,
    /// A write that does not finish within this time drops the connection.
    pub write_timeout: Duration,
    /// Initial read buffer of a socket (grows with a message).
    pub read_buffer_bytes: usize,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            max_sockets: 20_000,
            ping_interval: Duration::from_secs(30),
            send_buffer_bytes: 1024 * 1024,
            inbox_max: 1_024,
            max_event_attempts: 3,
            max_alarm_attempts: 6,
            retry_base: Duration::from_millis(100),
            ticket_ttl: Duration::from_secs(60),
            msgs_per_sec: 50,
            msgs_burst: 200,
            flood_close_after: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            read_buffer_bytes: 1024,
        }
    }
}

/// A tenant's socket caps (from its plan).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketLimits {
    /// Open sockets across the tenant (0 = no tenant cap).
    pub max_sockets: u32,
    /// Open sockets on one object (0 = no cap).
    pub max_per_object: u32,
    /// Open sockets of the service (`ws.max_sockets` of the service; 0 = no cap).
    pub max_per_service: u32,
    /// Largest message a client may send.
    pub max_message_bytes: u32,
}

impl Default for SocketLimits {
    fn default() -> Self {
        Self {
            max_sockets: 0,
            max_per_object: 0,
            max_per_service: 0,
            max_message_bytes: 1024 * 1024,
        }
    }
}

/// Builds event tasks and answers plan questions. Implemented by `rt-server`.
pub trait SocketTaskFactory: Send + Sync {
    /// The task that delivers `event` to `object` of `service`, or `None` when the service cannot
    /// take it (deleted, WebSockets switched off or not in the plan): the event is dropped.
    fn event_task(&self, service: ServiceId, object: ObjectRef, event: SocketEvent) -> Option<TaskSpec>;
    /// The caps of the tenant's plan and of the service.
    fn limits(&self, tenant: TenantId, service: ServiceId) -> SocketLimits;
    /// A runtime log line for the service (an event given up, an alarm dropped).
    fn log(&self, tenant: TenantId, service: ServiceId, message: String) {
        tracing::warn!(%tenant, %service, "{message}");
    }
    /// An alarm event of `object` was delivered (`ok`) or given up: its stored alarm due at
    /// `scheduled_ms` is done. Durable services only.
    fn alarm_done(&self, service: ServiceId, object: &str, scheduled_ms: i64) {
        let _ = (service, object, scheduled_ms);
    }
}

/// Starts a task and resolves to its outcome (the scheduler's `submit`; a fake in tests).
pub type Submitter =
    Arc<dyn Fn(TaskSpec) -> Result<Pin<Box<dyn Future<Output = Outcome> + Send>>, RejectReason> + Send + Sync>;

/// A request's turn on a durable object ([`SocketHub::turn`]): the object runs nothing else
/// while it is held. Dropping it ends the turn.
pub struct ObjectTurn {
    pub object: ObjectRef,
    _done: oneshot::Sender<()>,
}

/// Why no turn was granted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnError {
    /// The node is draining.
    Draining,
    /// The object's inbox is full.
    Busy,
}

/// Socket counts for the control API and metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HubStats {
    pub sockets: u32,
    pub objects: u32,
    pub pending: u32,
}

type ObjKey = (ServiceId, Arc<str>);

/// Durable objects' alarms, by due time (one per object).
#[derive(Default)]
struct Alarms {
    due: BTreeSet<(i64, ServiceId, Arc<str>)>,
    by_object: HashMap<ObjKey, (i64, TenantId)>,
}

#[derive(Default)]
struct Counts {
    tenants: HashMap<TenantId, u32>,
    services: HashMap<ServiceId, u32>,
}

enum Item {
    Event(Box<SocketEvent>, Option<OwnedSemaphorePermit>),
    Turn(oneshot::Sender<ObjectTurn>),
}

#[derive(Default)]
struct ObjState {
    inbox: VecDeque<Item>,
    running: bool,
    /// The last turn's sequence number (0: none since the slot was made).
    seq: u64,
    sockets: BTreeSet<String>,
    /// Accepted sockets not connected yet (count against the per-object cap).
    pending: u32,
    auto: Option<(String, String)>,
    /// Forgotten by the hub: nothing may be queued here any more (a new slot takes over).
    dead: bool,
}

pub(super) struct ObjectSlot {
    pub(super) tenant: TenantId,
    pub(super) service: ServiceId,
    pub(super) name: Arc<str>,
    st: Mutex<ObjState>,
    pub(super) permits: Arc<Semaphore>,
}

impl ObjectSlot {
    /// The automatic answer to `text`, if the object has one for it.
    pub(super) fn auto_response(&self, text: &str) -> Option<String> {
        self.st
            .lock()
            .auto
            .as_ref()
            .filter(|(req, _)| req == text)
            .map(|(_, resp)| resp.clone())
    }
}

/// What the socket task writes.
pub(super) enum Out {
    Data(WsData),
    /// `by_guest`: the guest (or the control API) closed it; no close event is delivered.
    Close {
        code: u16,
        reason: String,
        by_guest: bool,
    },
}

pub(super) struct Meta {
    tags: Vec<String>,
    attachment: Option<Bytes>,
    protocol: Option<String>,
    pub(super) last_message_at: Option<i64>,
    pub(super) auto_response_at: Option<i64>,
}

pub(super) struct Entry {
    pub(super) id: String,
    pub(super) tenant: TenantId,
    pub(super) service: ServiceId,
    pub(super) slot: Arc<ObjectSlot>,
    pub(super) meta: Mutex<Meta>,
    created_at_ms: i64,
    remote_ip: Option<String>,
    out: mpsc::UnboundedSender<Out>,
    pub(super) buffered: AtomicU64,
    /// A close was queued: further sends are refused.
    closing: AtomicBool,
}

impl Entry {
    pub(super) fn info(&self) -> SocketInfo {
        let m = self.meta.lock();
        SocketInfo {
            id: self.id.clone(),
            object: self.slot.name.to_string(),
            tags: m.tags.clone(),
            protocol: m.protocol.clone(),
            created_at_ms: self.created_at_ms,
            last_message_at_ms: m.last_message_at,
            auto_response_at_ms: m.auto_response_at,
            buffered_amount: self.buffered.load(Ordering::Relaxed),
            remote_ip: self.remote_ip.clone(),
        }
    }

    fn has_tag(&self, tag: Option<&str>) -> bool {
        tag.is_none_or(|t| self.meta.lock().tags.iter().any(|x| x == t))
    }
}

struct Ticket {
    secret: String,
    socket: String,
    tenant: TenantId,
    slot: Arc<ObjectSlot>,
    tags: Vec<String>,
    attachment: Option<Bytes>,
    protocol: Option<String>,
    expires: Instant,
    /// Sends made before the connection arrived (at most [`PENDING_SENDS`]).
    pending: Vec<Out>,
}

/// Sends a socket queues between `accept` and its connection.
const PENDING_SENDS: usize = 64;

#[derive(Default)]
struct Tickets {
    by_socket: HashMap<String, Ticket>,
    by_secret: HashMap<String, String>,
}

/// An upgrade that may bind its connection ([`SocketHub::claim`]). Dropping it unbound releases the
/// reserved socket.
pub struct Claim {
    hub: Arc<SocketHub>,
    ticket: Option<Ticket>,
}

impl Claim {
    /// The subprotocol the guest chose.
    pub fn protocol(&self) -> Option<&str> {
        self.ticket.as_ref().and_then(|t| t.protocol.as_deref())
    }

    pub fn socket_id(&self) -> &str {
        self.ticket.as_ref().map_or("", |t| t.socket.as_str())
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if let Some(t) = self.ticket.take() {
            self.hub.release_reservation(&t);
        }
    }
}

/// The node's WebSockets. Cheap to share (`Arc`).
pub struct SocketHub {
    cfg: HubConfig,
    submit: Submitter,
    tasks: Arc<dyn SocketTaskFactory>,
    /// The I/O runtime: socket and driver tasks run here, never on a Machine thread (guest calls
    /// reach the hub from Machines).
    rt: tokio::runtime::Handle,
    objects: Mutex<HashMap<ObjKey, Arc<ObjectSlot>>>,
    sockets: RwLock<BTreeMap<String, Arc<Entry>>>,
    tickets: Mutex<Tickets>,
    /// Open plus reserved sockets per tenant and per service.
    counts: Mutex<Counts>,
    /// Open plus reserved sockets on the node.
    total: AtomicU32,
    /// Global turn counter (see [`ObjectRef`]).
    turns: AtomicU64,
    draining: AtomicBool,
    alarms: Mutex<Alarms>,
    alarm_wake: Notify,
    metrics: std::sync::OnceLock<Arc<rt_obs::Metrics>>,
}

fn now_ms() -> i64 {
    rt_host::handle::now_ms()
}

fn not_found() -> HostError {
    HostError::new("not_found", "no such socket")
}

fn check_tags(tags: &[String]) -> Result<(), HostError> {
    if tags.len() > MAX_SOCKET_TAGS {
        return Err(HostError::new(
            "too_large",
            format!("a socket has at most {MAX_SOCKET_TAGS} tags"),
        ));
    }
    if !tags.iter().all(|t| valid_tag(t)) {
        return Err(HostError::new(
            "invalid_request",
            "a tag is 1 to 256 bytes without control characters",
        ));
    }
    Ok(())
}

fn check_attachment(a: &Option<Bytes>) -> Result<(), HostError> {
    if a.as_ref().is_some_and(|b| b.len() > MAX_ATTACHMENT_BYTES) {
        return Err(HostError::new(
            "too_large",
            format!("an attachment is at most {MAX_ATTACHMENT_BYTES} bytes serialized"),
        ));
    }
    Ok(())
}

fn check_close(code: u16, reason: &str) -> Result<(), HostError> {
    if !valid_app_close_code(code) {
        return Err(HostError::new("invalid_request", "a close code is 1000 or 3000-4999"));
    }
    if reason.len() > MAX_CLOSE_REASON_BYTES {
        return Err(HostError::new(
            "too_large",
            format!("a close reason is at most {MAX_CLOSE_REASON_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn random_secret() -> String {
    format!("{:032x}{:032x}", rand::random::<u128>(), rand::random::<u128>())
}

impl SocketHub {
    pub fn new(
        cfg: HubConfig,
        submit: Submitter,
        tasks: Arc<dyn SocketTaskFactory>,
        rt: tokio::runtime::Handle,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            submit,
            tasks,
            rt,
            objects: Mutex::new(HashMap::new()),
            sockets: RwLock::new(BTreeMap::new()),
            tickets: Mutex::new(Tickets::default()),
            counts: Mutex::new(Counts::default()),
            total: AtomicU32::new(0),
            turns: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            alarms: Mutex::new(Alarms::default()),
            alarm_wake: Notify::new(),
            metrics: std::sync::OnceLock::new(),
        })
    }

    /// Report `rt_ws_*` (sockets, accepts, messages, event tasks, closes) to `metrics`.
    pub fn set_metrics(&self, metrics: Arc<rt_obs::Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    fn metric(&self, f: impl FnOnce(&rt_obs::Metrics)) {
        if let Some(m) = self.metrics.get() {
            f(m);
        }
    }

    /// One message in (`in`) or out (`out`) of a socket.
    pub(super) fn count_message(&self, dir: &'static str) {
        self.metric(|m| m.ws_messages_total.with_label_values(&[dir]).inc());
    }

    // ---- alarms ----------------------------------------------------------------------------

    /// Set (`Some`) or clear the alarm of an object. Committed alarms come here from the tasks
    /// that set them, and from storage when the node starts.
    pub fn set_alarm_for(&self, tenant: TenantId, service: ServiceId, object: &str, at: Option<i64>) {
        let key: ObjKey = (service, Arc::from(object));
        {
            let mut a = self.alarms.lock();
            if let Some((old, _)) = a.by_object.remove(&key) {
                a.due.remove(&(old, service, key.1.clone()));
            }
            if let Some(at) = at {
                a.due.insert((at, service, key.1.clone()));
                a.by_object.insert(key, (at, tenant));
            }
        }
        self.alarm_wake.notify_one();
    }

    /// Alarms waiting to fire.
    pub fn pending_alarms(&self) -> usize {
        self.alarms.lock().due.len()
    }

    /// Deliver due alarms as events of their objects until the future is dropped. An alarm
    /// fires once per setting; a failed `alarm()` is delivered again (`retryCount`) up to
    /// `max_alarm_attempts`, then dropped.
    pub async fn run_alarms(self: Arc<Self>) {
        loop {
            let notified = self.alarm_wake.notified();
            let next = self.alarms.lock().due.first().map(|(at, _, _)| *at);
            let now = now_ms();
            match next {
                Some(at) if at <= now => {
                    let fired: Vec<(i64, ServiceId, Arc<str>, TenantId)> = {
                        let mut a = self.alarms.lock();
                        let mut out = Vec::new();
                        while let Some((at, service, object)) = a.due.first().cloned().filter(|(at, _, _)| *at <= now) {
                            a.due.remove(&(at, service, object.clone()));
                            if let Some((_, tenant)) = a.by_object.remove(&(service, object.clone())) {
                                out.push((at, service, object, tenant));
                            }
                        }
                        out
                    };
                    for (at, service, object, tenant) in fired {
                        if !self.is_draining() {
                            self.push_event(
                                tenant,
                                service,
                                &object,
                                SocketEventKind::Alarm {
                                    scheduled_ms: at,
                                    retry: 0,
                                },
                            );
                        }
                    }
                }
                Some(at) => {
                    let wait = Duration::from_millis(u64::try_from(at - now).unwrap_or(0).min(60_000));
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = notified => {}
                    }
                }
                None => notified.await,
            }
        }
    }

    pub fn config(&self) -> &HubConfig {
        &self.cfg
    }

    // ---- objects ---------------------------------------------------------------------------

    fn slot(&self, tenant: TenantId, service: ServiceId, name: &str) -> Arc<ObjectSlot> {
        let mut objects = self.objects.lock();
        objects
            .entry((service, Arc::from(name)))
            .or_insert_with(|| {
                Arc::new(ObjectSlot {
                    tenant,
                    service,
                    name: Arc::from(name),
                    st: Mutex::new(ObjState::default()),
                    permits: Arc::new(Semaphore::new(self.cfg.inbox_max.max(1))),
                })
            })
            .clone()
    }

    fn existing_slot(&self, service: ServiceId, name: &str) -> Option<Arc<ObjectSlot>> {
        self.objects.lock().get(&(service, Arc::from(name))).cloned()
    }

    /// Forget an object nothing refers to any more (no sockets, nothing queued or running, no
    /// auto-response). Its turn sequence restarts from the global counter, so no stale `prev` can
    /// match.
    fn maybe_forget(&self, slot: &Arc<ObjectSlot>) {
        let mut objects = self.objects.lock();
        let key = (slot.service, slot.name.clone());
        if !objects.get(&key).is_some_and(|s| Arc::ptr_eq(s, slot)) {
            return;
        }
        let mut st = slot.st.lock();
        if !st.running && st.inbox.is_empty() && st.sockets.is_empty() && st.pending == 0 && st.auto.is_none() {
            st.dead = true;
            drop(st);
            objects.remove(&key);
        }
    }

    /// The next turn of `slot`: a new global sequence number after the object's previous one.
    fn next_turn(&self, slot: &ObjectSlot) -> ObjectRef {
        let seq = self.turns.fetch_add(1, Ordering::Relaxed) + 1;
        let mut st = slot.st.lock();
        let prev = std::mem::replace(&mut st.seq, seq);
        ObjectRef {
            name: slot.name.clone(),
            seq,
            prev,
        }
    }

    fn enqueue(self: &Arc<Self>, slot: &Arc<ObjectSlot>, item: Item) {
        let mut slot = slot.clone();
        let mut item = Some(item);
        loop {
            let start = {
                let mut st = slot.st.lock();
                if st.dead {
                    None
                } else {
                    st.inbox.extend(item.take());
                    Some(!std::mem::replace(&mut st.running, true))
                }
            };
            match start {
                Some(true) => {
                    let (hub, slot) = (self.clone(), slot.clone());
                    self.rt.spawn(async move { hub.drive(slot).await });
                    return;
                }
                Some(false) => return,
                // Forgotten meanwhile: the object's live slot (a new one) takes the item.
                None => slot = self.slot(slot.tenant, slot.service, &slot.name),
            }
        }
    }

    /// Run `slot`'s inbox until it is empty, one item at a time.
    async fn drive(self: Arc<Self>, slot: Arc<ObjectSlot>) {
        loop {
            let item = {
                let mut st = slot.st.lock();
                match st.inbox.pop_front() {
                    Some(item) => item,
                    None => {
                        st.running = false;
                        break;
                    }
                }
            };
            match item {
                Item::Turn(tx) => {
                    let (done_tx, done_rx) = oneshot::channel();
                    let turn = ObjectTurn {
                        object: self.next_turn(&slot),
                        _done: done_tx,
                    };
                    if tx.send(turn).is_ok() {
                        // The holder drops its turn when its task answered (or gave up).
                        let _ = done_rx.await;
                    }
                }
                Item::Event(ev, permit) => {
                    self.run_event(&slot, *ev).await;
                    drop(permit);
                }
            }
        }
        self.maybe_forget(&slot);
    }

    /// Deliver one event: again with backoff while it fails, until it succeeds or its attempts are
    /// spent.
    async fn run_event(self: &Arc<Self>, slot: &Arc<ObjectSlot>, mut ev: SocketEvent) {
        let alarm = matches!(ev.kind, SocketEventKind::Alarm { .. });
        let max = if alarm {
            self.cfg.max_alarm_attempts
        } else {
            self.cfg.max_event_attempts
        }
        .max(1);
        loop {
            if let SocketEventKind::Alarm { retry, .. } = &mut ev.kind {
                *retry = ev.attempt.saturating_sub(1);
            }
            // The socket as it is now: a previous event may have changed its tags or attachment.
            if let Some(info) = &ev.socket {
                if let Some(e) = self.entry(&info.id) {
                    ev.socket = Some(e.info());
                }
            }
            let object = self.next_turn(slot);
            let Some(spec) = self.tasks.event_task(slot.service, object, ev.clone()) else {
                return;
            };
            let outcome = match (self.submit)(spec) {
                Ok(fut) => fut.await,
                Err(r) => Outcome::Rejected(r),
            };
            let ok = matches!(&outcome, Outcome::Ok(reply) if reply.status.is_success());
            self.metric(|m| {
                m.ws_event_tasks_total
                    .with_label_values(&[if ok { "ok" } else { "failed" }])
                    .inc()
            });
            if ok {
                if let SocketEventKind::Alarm { scheduled_ms, .. } = ev.kind {
                    self.tasks.alarm_done(slot.service, &slot.name, scheduled_ms);
                }
                return;
            }
            if matches!(outcome, Outcome::Rejected(RejectReason::Draining)) {
                return;
            }
            if ev.attempt >= max {
                self.give_up(slot, &ev, &outcome);
                return;
            }
            let shift = ev.attempt.saturating_sub(1).min(8);
            let delay = (self.cfg.retry_base * (1u32 << shift)).min(Duration::from_secs(30));
            tokio::time::sleep(delay).await;
            ev.attempt += 1;
        }
    }

    fn give_up(&self, slot: &ObjectSlot, ev: &SocketEvent, outcome: &Outcome) {
        self.metric(|m| m.ws_event_tasks_total.with_label_values(&["given_up"]).inc());
        let why = outcome.reason().unwrap_or("handler_failed");
        let socket = ev.socket.as_ref().map(|s| s.id.as_str()).unwrap_or("-");
        self.tasks.log(
            slot.tenant,
            slot.service,
            format!(
                "websocket {} event of object `{}` (socket {socket}) failed {} times ({why}); given up",
                ev.kind.label(),
                slot.name,
                ev.attempt
            ),
        );
        match &ev.kind {
            SocketEventKind::Message(_) => {
                if let Some(info) = &ev.socket {
                    if let Some(e) = self.entry(&info.id) {
                        self.queue_close(&e, 1011, "event handler failed", false);
                    }
                }
            }
            SocketEventKind::Alarm { scheduled_ms, .. } => {
                self.tasks.alarm_done(slot.service, &slot.name, *scheduled_ms)
            }
            _ => {}
        }
    }

    /// Wait for a turn on `object` of `service` (a request to a durable object). Turns and events
    /// of one object run one at a time, in arrival order.
    pub async fn turn(
        self: &Arc<Self>,
        tenant: TenantId,
        service: ServiceId,
        object: &str,
    ) -> Result<ObjectTurn, TurnError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(TurnError::Draining);
        }
        let slot = self.slot(tenant, service, object);
        if slot.st.lock().inbox.len() >= self.cfg.inbox_max {
            return Err(TurnError::Busy);
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(&slot, Item::Turn(tx));
        rx.await.map_err(|_| TurnError::Draining)
    }

    /// Queue an event of `object` (an alarm; socket events come from the socket tasks).
    pub fn push_event(self: &Arc<Self>, tenant: TenantId, service: ServiceId, object: &str, kind: SocketEventKind) {
        let slot = self.slot(tenant, service, object);
        self.enqueue(
            &slot,
            Item::Event(
                Box::new(SocketEvent {
                    socket: None,
                    kind,
                    attempt: 1,
                }),
                None,
            ),
        );
    }

    /// Queue an event of a socket. A message waits for room in the object's inbox (the socket's
    /// reads stop meanwhile); a close or error never waits.
    pub(super) async fn socket_event(self: &Arc<Self>, entry: &Arc<Entry>, kind: SocketEventKind) {
        let permit = match kind {
            SocketEventKind::Message(_) => entry.slot.permits.clone().acquire_owned().await.ok(),
            _ => None,
        };
        let ev = SocketEvent {
            socket: Some(entry.info()),
            kind,
            attempt: 1,
        };
        self.enqueue(&entry.slot, Item::Event(Box::new(ev), permit));
    }

    // ---- sockets ---------------------------------------------------------------------------

    pub(super) fn entry(&self, id: &str) -> Option<Arc<Entry>> {
        self.sockets.read().get(id).cloned()
    }

    /// The socket `id` if it belongs to `service`.
    fn scoped(&self, service: ServiceId, id: &str) -> Option<Arc<Entry>> {
        self.entry(id).filter(|e| e.service == service)
    }

    fn queue_close(&self, e: &Entry, code: u16, reason: &str, by_guest: bool) {
        if !e.closing.swap(true, Ordering::AcqRel) {
            let _ = e.out.send(Out::Close {
                code,
                reason: reason.to_string(),
                by_guest,
            });
        }
    }

    fn queue_send(&self, e: &Entry, data: WsData) -> Result<(), HostError> {
        if e.closing.load(Ordering::Acquire) {
            return Err(not_found());
        }
        let len = data.len() as u64;
        let buffered = e.buffered.fetch_add(len, Ordering::AcqRel) + len;
        if buffered > self.cfg.send_buffer_bytes {
            e.buffered.fetch_sub(len, Ordering::AcqRel);
            self.queue_close(e, 1013, "slow consumer", false);
            return Err(HostError::new(
                "slow_consumer",
                "the client does not read: the socket was closed",
            ));
        }
        if e.out.send(Out::Data(data)).is_err() {
            e.buffered.fetch_sub(len, Ordering::AcqRel);
            return Err(not_found());
        }
        Ok(())
    }

    /// A socket of `service` that is accepted but not connected yet: queue `out` for it.
    fn queue_pending(&self, service: ServiceId, id: &str, out: Out) -> Result<(), HostError> {
        let mut t = self.tickets.lock();
        let ticket = t
            .by_socket
            .get_mut(id)
            .filter(|t| t.slot.service == service)
            .ok_or_else(not_found)?;
        if ticket.pending.len() >= PENDING_SENDS {
            return Err(HostError::new(
                "slow_consumer",
                "too many messages before the socket connected",
            ));
        }
        ticket.pending.push(out);
        Ok(())
    }

    fn sockets_of(&self, service: ServiceId, object: Option<&str>, tag: Option<&str>) -> Vec<Arc<Entry>> {
        match object {
            Some(name) => {
                let Some(slot) = self.existing_slot(service, name) else {
                    return Vec::new();
                };
                let ids: Vec<String> = slot.st.lock().sockets.iter().cloned().collect();
                let map = self.sockets.read();
                ids.iter()
                    .filter_map(|id| map.get(id).cloned())
                    .filter(|e| e.has_tag(tag))
                    .collect()
            }
            None => self
                .sockets
                .read()
                .values()
                .filter(|e| e.service == service && e.has_tag(tag))
                .cloned()
                .collect(),
        }
    }

    /// Reserve a socket (see [`SocketHost::accept`]); `remote_ip` is filled in at bind.
    fn reserve(
        &self,
        tenant: TenantId,
        service: ServiceId,
        object: &str,
        opts: AcceptOpts,
    ) -> Result<Accepted, HostError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(HostError::new("unavailable", "the node is draining"));
        }
        if !valid_object_name(object) {
            return Err(HostError::new(
                "invalid_request",
                "an object name is 1 to 256 bytes without control characters",
            ));
        }
        check_tags(&opts.tags)?;
        check_attachment(&opts.attachment)?;
        if let Some(p) = &opts.protocol {
            if p.is_empty() || p.len() > 256 || p.contains(|c: char| c.is_control() || c == ',' || c.is_whitespace()) {
                return Err(HostError::new("invalid_request", "a subprotocol is one token"));
            }
        }
        self.sweep_tickets(Instant::now());
        let limits = self.tasks.limits(tenant, service);
        let limited = |e: HostError| {
            self.metric(|m| m.ws_upgrades_total.with_label_values(&["limited"]).inc());
            e
        };
        let slot = self.slot(tenant, service, object);
        {
            let mut counts = self.counts.lock();
            let mut st = slot.st.lock();
            let open = st.sockets.len() as u32 + st.pending;
            if limits.max_per_object > 0 && open >= limits.max_per_object {
                return Err(limited(HostError::new(
                    "limit_exceeded",
                    "the object has as many sockets as the plan allows",
                )));
            }
            let of_service = counts.services.get(&service).copied().unwrap_or(0);
            if limits.max_per_service > 0 && of_service >= limits.max_per_service {
                return Err(limited(HostError::new(
                    "limit_exceeded",
                    "the service has as many sockets as its ws.max_sockets allows",
                )));
            }
            let of_tenant = counts.tenants.get(&tenant).copied().unwrap_or(0);
            if limits.max_sockets > 0 && of_tenant >= limits.max_sockets {
                return Err(limited(HostError::new(
                    "limit_exceeded",
                    "the tenant has as many sockets as its plan allows",
                )));
            }
            if self.total.load(Ordering::Acquire) >= self.cfg.max_sockets {
                return Err(limited(HostError::new(
                    "limit_exceeded",
                    "the node holds as many sockets as it allows",
                )));
            }
            *counts.tenants.entry(tenant).or_insert(0) += 1;
            *counts.services.entry(service).or_insert(0) += 1;
            st.pending += 1;
            self.total.fetch_add(1, Ordering::AcqRel);
        }
        let socket = format!("ws_{}", ulid::Ulid::new());
        let secret = random_secret();
        let ticket = Ticket {
            secret: secret.clone(),
            socket: socket.clone(),
            tenant,
            slot,
            tags: opts.tags,
            attachment: opts.attachment,
            protocol: opts.protocol,
            expires: Instant::now() + self.cfg.ticket_ttl,
            pending: Vec::new(),
        };
        let mut t = self.tickets.lock();
        t.by_secret.insert(secret.clone(), socket.clone());
        t.by_socket.insert(socket.clone(), ticket);
        Ok(Accepted { socket, ticket: secret })
    }

    fn release_reservation(&self, t: &Ticket) {
        {
            let mut st = t.slot.st.lock();
            st.pending = st.pending.saturating_sub(1);
        }
        self.uncount(t.tenant, t.slot.service);
        self.maybe_forget(&t.slot);
    }

    fn uncount(&self, tenant: TenantId, service: ServiceId) {
        let mut counts = self.counts.lock();
        if let Some(n) = counts.tenants.get_mut(&tenant) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.tenants.remove(&tenant);
            }
        }
        if let Some(n) = counts.services.get_mut(&service) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.services.remove(&service);
            }
        }
        let _ = self
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }

    fn sweep_tickets(&self, now: Instant) {
        let expired: Vec<Ticket> = {
            let mut t = self.tickets.lock();
            let ids: Vec<String> = t
                .by_socket
                .iter()
                .filter(|(_, x)| x.expires <= now)
                .map(|(id, _)| id.clone())
                .collect();
            ids.iter()
                .filter_map(|id| {
                    let x = t.by_socket.remove(id)?;
                    t.by_secret.remove(&x.secret);
                    Some(x)
                })
                .collect()
        };
        for t in expired {
            self.release_reservation(&t);
        }
    }

    /// Take the reserved socket of an accept `ticket` of `tenant` (the tenant whose route the
    /// upgrade came through: a ticket never crosses tenants). `None` for an unknown, expired or
    /// foreign ticket.
    pub fn claim(self: &Arc<Self>, secret: &str, tenant: TenantId) -> Option<Claim> {
        let now = Instant::now();
        self.sweep_tickets(now);
        let mut t = self.tickets.lock();
        let socket = t.by_secret.get(secret)?.clone();
        if t.by_socket.get(&socket).is_none_or(|x| x.tenant != tenant) {
            return None;
        }
        t.by_secret.remove(secret);
        let ticket = t.by_socket.remove(&socket)?;
        Some(Claim {
            hub: self.clone(),
            ticket: Some(ticket),
        })
    }

    /// Drop the reservation of `secret` (a response carried a ticket but answered no upgrade).
    pub fn cancel_ticket(self: &Arc<Self>, secret: &str) {
        let ticket = {
            let mut t = self.tickets.lock();
            t.by_secret
                .remove(secret)
                .and_then(|socket| t.by_socket.remove(&socket))
        };
        if let Some(t) = ticket {
            self.release_reservation(&t);
        }
    }

    /// Bind an upgraded connection to a claimed socket and start the socket's task. `hold` lives
    /// as long as the socket (the per-IP connection slot).
    pub fn bind_io<S>(
        self: &Arc<Self>,
        mut claim: Claim,
        io: S,
        remote_ip: Option<String>,
        hold: Box<dyn std::any::Any + Send>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let Some(t) = claim.ticket.take() else { return };
        let (tx, rx) = mpsc::unbounded_channel();
        let entry = Arc::new(Entry {
            id: t.socket.clone(),
            tenant: t.tenant,
            service: t.slot.service,
            slot: t.slot.clone(),
            meta: Mutex::new(Meta {
                tags: t.tags,
                attachment: t.attachment,
                protocol: t.protocol,
                last_message_at: None,
                auto_response_at: None,
            }),
            created_at_ms: now_ms(),
            remote_ip,
            out: tx,
            buffered: AtomicU64::new(0),
            closing: AtomicBool::new(false),
        });
        {
            let mut st = t.slot.st.lock();
            st.pending = st.pending.saturating_sub(1);
            st.sockets.insert(entry.id.clone());
        }
        self.sockets.write().insert(entry.id.clone(), entry.clone());
        for out in t.pending {
            match out {
                Out::Data(d) => {
                    let _ = self.queue_send(&entry, d);
                }
                Out::Close { code, reason, by_guest } => self.queue_close(&entry, code, &reason, by_guest),
            }
        }
        let limits = self.tasks.limits(entry.tenant, entry.service);
        self.metric(|m| {
            m.ws_sockets.inc();
            m.ws_upgrades_total.with_label_values(&["ok"]).inc();
        });
        let hub = self.clone();
        self.rt.spawn(async move {
            conn::run(hub, entry, io, rx, limits.max_message_bytes as usize, hold).await;
        });
    }

    /// The socket's task ended: forget the socket and deliver its close event (unless the guest
    /// closed it itself).
    pub(super) fn socket_ended(self: &Arc<Self>, entry: &Arc<Entry>, close: Option<SocketEventKind>, code: u16) {
        let removed = self.sockets.write().remove(&entry.id).is_some();
        if !removed {
            return;
        }
        self.metric(|m| {
            m.ws_sockets.dec();
            m.ws_closed_total.with_label_values(&[&code.to_string()]).inc();
        });
        entry.slot.st.lock().sockets.remove(&entry.id);
        self.uncount(entry.tenant, entry.service);
        match close {
            Some(kind) => {
                let ev = SocketEvent {
                    socket: Some(entry.info()),
                    kind,
                    attempt: 1,
                };
                self.enqueue(&entry.slot, Item::Event(Box::new(ev), None));
            }
            None => self.maybe_forget(&entry.slot),
        }
    }

    pub(super) fn mark_auto_response(&self, entry: &Entry) {
        entry.meta.lock().auto_response_at = Some(now_ms());
    }

    // ---- operator surface ------------------------------------------------------------------

    /// Counts for the control API (`GET /v1/node/status`, service stats).
    pub fn stats(&self) -> HubStats {
        let objects = self
            .objects
            .lock()
            .values()
            .filter(|s| !s.st.lock().sockets.is_empty())
            .count() as u32;
        HubStats {
            sockets: self.sockets.read().len() as u32,
            objects,
            pending: self.tickets.lock().by_socket.len() as u32,
        }
    }

    /// Open sockets and objects with sockets of one service.
    pub fn service_stats(&self, service: ServiceId) -> HubStats {
        let sockets = self.sockets.read().values().filter(|e| e.service == service).count() as u32;
        let objects = self
            .objects
            .lock()
            .iter()
            .filter(|((s, _), slot)| *s == service && !slot.st.lock().sockets.is_empty())
            .count() as u32;
        HubStats {
            sockets,
            objects,
            pending: 0,
        }
    }

    /// Sockets of `service` (the control API's view; no tenant check: the caller resolved the
    /// service within the caller's scope).
    pub fn list_service(
        &self,
        service: ServiceId,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> SocketPage {
        page(self.sockets_of(service, object, tag), limit, cursor)
    }

    /// Objects of `service` that hold sockets, with their counts.
    pub fn objects_of(
        &self,
        service: ServiceId,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> rt_host::ObjectPage {
        let mut all: Vec<(String, u32)> = self
            .objects
            .lock()
            .iter()
            .filter(|((s, name), _)| *s == service && name.starts_with(prefix))
            .map(|((_, name), slot)| (name.to_string(), slot.st.lock().sockets.len() as u32))
            .filter(|(_, n)| *n > 0)
            .collect();
        all.sort();
        let limit = limit.clamp(1, 1000);
        let mut out: Vec<(String, u32)> = all
            .into_iter()
            .filter(|(n, _)| cursor.is_none_or(|c| n.as_str() > c))
            .take(limit + 1)
            .collect();
        let next = if out.len() > limit {
            out.truncate(limit);
            out.last().map(|(n, _)| n.clone())
        } else {
            None
        };
        (out, next)
    }

    /// Close one socket of `service` (control API). `false` when there is none.
    /// The object holding open socket `id` of `service` (`None`: no such open socket of it).
    pub fn object_of_socket(&self, service: ServiceId, id: &str) -> Option<String> {
        self.scoped(service, id).map(|e| e.slot.name.to_string())
    }

    pub fn close_socket(&self, service: ServiceId, id: &str, code: u16, reason: &str) -> bool {
        match self.scoped(service, id) {
            Some(e) => {
                self.queue_close(&e, code, reason, true);
                true
            }
            None => false,
        }
    }

    /// Close the sockets of `service` (of one object / with one tag when given) with `code`:
    /// a deploy with `close_sockets` (1012), a deleted service (1001), the control API.
    pub fn close_service(
        &self,
        service: ServiceId,
        object: Option<&str>,
        tag: Option<&str>,
        code: u16,
        reason: &str,
    ) -> u32 {
        let all = self.sockets_of(service, object, tag);
        for e in &all {
            self.queue_close(e, code, reason, true);
        }
        all.len() as u32
    }

    /// Stop accepting upgrades and turns, close every socket with 1001 ("going away").
    pub fn drain(&self) {
        self.draining.store(true, Ordering::Relaxed);
        let all: Vec<Arc<Entry>> = self.sockets.read().values().cloned().collect();
        for e in &all {
            self.queue_close(e, 1001, "going away", true);
        }
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }
}

/// A page of `entries` in id order after `cursor`.
fn page(mut entries: Vec<Arc<Entry>>, limit: usize, cursor: Option<&str>) -> SocketPage {
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    let limit = limit.clamp(1, 1000);
    let mut out: Vec<SocketInfo> = entries
        .iter()
        .filter(|e| cursor.is_none_or(|c| e.id.as_str() > c))
        .take(limit + 1)
        .map(|e| e.info())
        .collect();
    let next = if out.len() > limit {
        out.truncate(limit);
        out.last().map(|s| s.id.clone())
    } else {
        None
    };
    (out, next)
}

impl SocketHost for SocketHub {
    fn accept(&self, ctx: &TaskCtx, object: &str, opts: AcceptOpts) -> Result<Accepted, HostError> {
        self.reserve(ctx.tenant, ctx.service, object, opts)
    }

    fn apply(&self, ctx: &TaskCtx, op: SocketOp) -> Result<u32, HostError> {
        match op {
            SocketOp::Send { socket, data } => match self.scoped(ctx.service, &socket) {
                Some(e) => self.queue_send(&e, data).map(|_| 1),
                None => self.queue_pending(ctx.service, &socket, Out::Data(data)).map(|_| 1),
            },
            SocketOp::Broadcast {
                object,
                tag,
                except,
                data,
            } => {
                let mut n = 0;
                for e in self.sockets_of(ctx.service, Some(&object), tag.as_deref()) {
                    if !except.contains(&e.id) && self.queue_send(&e, data.clone()).is_ok() {
                        n += 1;
                    }
                }
                Ok(n)
            }
            SocketOp::Close { socket, code, reason } => {
                check_close(code, &reason)?;
                match self.scoped(ctx.service, &socket) {
                    Some(e) => {
                        self.queue_close(&e, code, &reason, true);
                        Ok(1)
                    }
                    None => self
                        .queue_pending(
                            ctx.service,
                            &socket,
                            Out::Close {
                                code,
                                reason,
                                by_guest: true,
                            },
                        )
                        .map(|_| 1),
                }
            }
            SocketOp::CloseAll {
                object,
                tag,
                code,
                reason,
            } => {
                check_close(code, &reason)?;
                Ok(self.close_service(ctx.service, Some(&object), tag.as_deref(), code, &reason))
            }
        }
    }

    fn list(
        &self,
        ctx: &TaskCtx,
        object: Option<&str>,
        tag: Option<&str>,
        limit: usize,
        cursor: Option<&str>,
    ) -> SocketPage {
        self.list_service(ctx.service, object, tag, limit, cursor)
    }

    fn get(&self, ctx: &TaskCtx, socket: &str) -> Option<SocketInfo> {
        self.scoped(ctx.service, socket).map(|e| e.info())
    }

    fn exists(&self, ctx: &TaskCtx, socket: &str) -> bool {
        self.scoped(ctx.service, socket).is_some()
            || self
                .tickets
                .lock()
                .by_socket
                .get(socket)
                .is_some_and(|t| t.slot.service == ctx.service)
    }

    fn count(&self, ctx: &TaskCtx, object: Option<&str>, tag: Option<&str>) -> u32 {
        self.sockets_of(ctx.service, object, tag).len() as u32
    }

    fn objects(&self, ctx: &TaskCtx, prefix: &str, limit: usize, cursor: Option<&str>) -> rt_host::ObjectPage {
        self.objects_of(ctx.service, prefix, limit, cursor)
    }

    fn set_tags(&self, ctx: &TaskCtx, socket: &str, tags: Vec<String>) -> Result<(), HostError> {
        check_tags(&tags)?;
        let e = self.scoped(ctx.service, socket).ok_or_else(not_found)?;
        e.meta.lock().tags = tags;
        Ok(())
    }

    fn attachment(&self, ctx: &TaskCtx, socket: &str) -> Result<Option<Bytes>, HostError> {
        let e = self.scoped(ctx.service, socket).ok_or_else(not_found)?;
        let a = e.meta.lock().attachment.clone();
        Ok(a)
    }

    fn set_attachment(&self, ctx: &TaskCtx, socket: &str, value: Option<Bytes>) -> Result<(), HostError> {
        check_attachment(&value)?;
        let e = self.scoped(ctx.service, socket).ok_or_else(not_found)?;
        e.meta.lock().attachment = value;
        Ok(())
    }

    fn set_auto_response(&self, ctx: &TaskCtx, object: &str, pair: Option<(String, String)>) -> Result<(), HostError> {
        if !valid_object_name(object) {
            return Err(HostError::new(
                "invalid_request",
                "an object name is 1 to 256 bytes without control characters",
            ));
        }
        if let Some((req, resp)) = &pair {
            if req.is_empty() || req.len() > 1024 || resp.len() > 1024 {
                return Err(HostError::new(
                    "too_large",
                    "an auto-response request is 1 to 1024 bytes, its response at most 1024",
                ));
            }
        }
        let slot = self.slot(ctx.tenant, ctx.service, object);
        slot.st.lock().auto = pair;
        self.maybe_forget(&slot);
        Ok(())
    }

    fn set_alarm(&self, ctx: &TaskCtx, object: &str, at: Option<i64>) {
        self.set_alarm_for(ctx.tenant, ctx.service, object, at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::TaskId;

    struct Caps(SocketLimits);

    impl SocketTaskFactory for Caps {
        fn event_task(&self, _: ServiceId, _: ObjectRef, _: SocketEvent) -> Option<TaskSpec> {
            None
        }
        fn limits(&self, _: TenantId, _: ServiceId) -> SocketLimits {
            self.0
        }
    }

    fn hub(limits: SocketLimits, max_sockets: u32) -> Arc<SocketHub> {
        let submit: Submitter = Arc::new(|_| Err(RejectReason::Draining));
        let cfg = HubConfig {
            max_sockets,
            ..HubConfig::default()
        };
        SocketHub::new(cfg, submit, Arc::new(Caps(limits)), tokio::runtime::Handle::current())
    }

    fn ctx(tenant: u128, service: u128) -> TaskCtx {
        TaskCtx {
            tenant: TenantId::from_u128(tenant),
            service: ServiceId::from_u128(service),
            task: TaskId(1),
            depth: 0,
            deadline: Instant::now() + Duration::from_secs(30),
            trace: None,
            on_behalf_of: None,
            object: None,
        }
    }

    fn limit_code(r: Result<Accepted, HostError>) -> &'static str {
        r.err().map_or("ok", |e| e.code)
    }

    #[tokio::test]
    async fn caps_count_reserved_sockets_and_a_cancelled_ticket_gives_its_place_back() {
        let h = hub(
            SocketLimits {
                max_sockets: 3,
                max_per_object: 2,
                max_per_service: 0,
                max_message_bytes: 1024,
            },
            100,
        );
        let a = ctx(1, 1);
        let first = h.accept(&a, "room", AcceptOpts::default()).expect("first");
        h.accept(&a, "room", AcceptOpts::default()).expect("second");
        assert_eq!(
            limit_code(h.accept(&a, "room", AcceptOpts::default())),
            "limit_exceeded",
            "per object"
        );
        h.accept(&a, "lobby", AcceptOpts::default()).expect("another object");
        assert_eq!(
            limit_code(h.accept(&a, "hall", AcceptOpts::default())),
            "limit_exceeded",
            "per tenant"
        );
        assert_eq!(h.stats().pending, 3);
        h.cancel_ticket(&first.ticket);
        h.accept(&a, "room", AcceptOpts::default())
            .expect("the cancelled place is free again");
    }

    #[tokio::test]
    async fn the_node_cap_and_the_service_cap_apply_across_tenants() {
        let h = hub(
            SocketLimits {
                max_sockets: 0,
                max_per_object: 0,
                max_per_service: 1,
                max_message_bytes: 1024,
            },
            2,
        );
        h.accept(&ctx(1, 1), "o", AcceptOpts::default()).expect("t1");
        assert_eq!(
            limit_code(h.accept(&ctx(1, 1), "p", AcceptOpts::default())),
            "limit_exceeded",
            "service cap 1"
        );
        h.accept(&ctx(2, 2), "o", AcceptOpts::default()).expect("t2");
        assert_eq!(
            limit_code(h.accept(&ctx(3, 3), "o", AcceptOpts::default())),
            "limit_exceeded",
            "node cap 2"
        );
    }

    /// SOCK-006: a ticket that is never claimed (the client never upgraded) gives its reservation
    /// back on the sweep after `ticket_ttl`.
    #[tokio::test]
    async fn sock_006_an_unclaimed_ticket_is_swept_after_its_ttl() {
        let submit: Submitter = Arc::new(|_| Err(RejectReason::Draining));
        let cfg = HubConfig {
            max_sockets: 100,
            ticket_ttl: Duration::from_millis(20),
            ..HubConfig::default()
        };
        let limits = SocketLimits {
            max_sockets: 1,
            max_per_object: 0,
            max_per_service: 0,
            max_message_bytes: 1024,
        };
        let h = SocketHub::new(cfg, submit, Arc::new(Caps(limits)), tokio::runtime::Handle::current());
        let a = ctx(1, 1);
        let t = h.accept(&a, "o", AcceptOpts::default()).expect("accept");
        assert_eq!(limit_code(h.accept(&a, "o", AcceptOpts::default())), "limit_exceeded");
        h.sweep_tickets(Instant::now() + Duration::from_millis(50));
        assert_eq!(h.stats().pending, 0, "the reservation is back");
        assert!(
            h.claim(&t.ticket, TenantId::from_u128(1)).is_none(),
            "the expired ticket is gone"
        );
        h.accept(&a, "o", AcceptOpts::default()).expect("the place is free");
    }

    /// SOCK-007: sends to a socket that is accepted but not connected yet are buffered up to 64;
    /// the 65th is `slow_consumer`.
    #[tokio::test]
    async fn sock_007_the_65th_send_before_connect_is_slow_consumer() {
        let h = hub(SocketLimits::default(), 100);
        let a = ctx(1, 1);
        let t = h.accept(&a, "o", AcceptOpts::default()).expect("accept");
        for i in 0..PENDING_SENDS {
            h.apply(
                &a,
                SocketOp::Send {
                    socket: t.socket.clone(),
                    data: WsData::Text(format!("{i}")),
                },
            )
            .expect("buffered");
        }
        let e = h
            .apply(
                &a,
                SocketOp::Send {
                    socket: t.socket.clone(),
                    data: WsData::Text("one more".into()),
                },
            )
            .expect_err("full");
        assert_eq!(e.code, "slow_consumer");
    }

    #[tokio::test]
    async fn a_ticket_is_claimed_once_and_only_by_its_tenant() {
        let h = hub(SocketLimits::default(), 100);
        let t = h
            .accept(
                &ctx(1, 1),
                "o",
                AcceptOpts {
                    protocol: Some("chat".into()),
                    ..AcceptOpts::default()
                },
            )
            .expect("accept");
        assert!(
            h.claim(&t.ticket, TenantId::from_u128(2)).is_none(),
            "another tenant's route"
        );
        let claim = h.claim(&t.ticket, TenantId::from_u128(1)).expect("claim");
        assert_eq!((claim.socket_id(), claim.protocol()), (t.socket.as_str(), Some("chat")));
        assert!(h.claim(&t.ticket, TenantId::from_u128(1)).is_none(), "once");
        drop(claim);
        assert_eq!(h.stats().pending, 0, "an unbound claim releases its reservation");
    }

    #[tokio::test]
    async fn accept_checks_tags_attachments_and_object_names() {
        let h = hub(SocketLimits::default(), 100);
        let c = ctx(1, 1);
        let opts = |tags: Vec<&str>, att: usize| AcceptOpts {
            tags: tags.into_iter().map(String::from).collect(),
            attachment: (att > 0).then(|| Bytes::from(vec![0u8; att])),
            protocol: None,
        };
        assert_eq!(limit_code(h.accept(&c, "o", opts(vec!["t"; 11], 0))), "too_large");
        assert_eq!(limit_code(h.accept(&c, "o", opts(vec![""], 0))), "invalid_request");
        assert_eq!(
            limit_code(h.accept(&c, "o", opts(vec![], MAX_ATTACHMENT_BYTES + 1))),
            "too_large"
        );
        assert_eq!(limit_code(h.accept(&c, "", opts(vec![], 0))), "invalid_request");
        assert_eq!(
            limit_code(h.accept(&c, "o", opts(vec!["a"; 10], MAX_ATTACHMENT_BYTES))),
            "ok"
        );
    }

    /// WSUP-010: any open socket of an object can be found (and closed) by id, also past the first
    /// 1000 a listing pages through.
    #[tokio::test]
    async fn wsup_010_an_open_socket_is_found_by_id_beyond_a_listing_page() {
        let h = hub(SocketLimits::default(), 5_000);
        let c = ctx(1, 1);
        let mut ids = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..1_100 {
            let t = h.accept(&c, "o", AcceptOpts::default()).expect("accept");
            let claim = h.claim(&t.ticket, TenantId::from_u128(1)).expect("claim");
            let (ours, theirs) = tokio::io::duplex(1024);
            h.bind_io(claim, ours, None, Box::new(()));
            peers.push(theirs);
            ids.push(t.socket);
        }
        assert_eq!(
            h.list_service(ServiceId::from_u128(1), Some("o"), None, 1000, None)
                .0
                .len(),
            1000,
            "a page holds 1000"
        );
        let last = ids.last().expect("ids").clone();
        assert_eq!(h.object_of_socket(ServiceId::from_u128(1), &last).as_deref(), Some("o"));
        assert_eq!(
            h.object_of_socket(ServiceId::from_u128(2), &last),
            None,
            "another service"
        );
        assert!(h.close_socket(ServiceId::from_u128(1), &last, 1000, "bye"));
    }

    #[tokio::test]
    async fn sends_before_the_connection_are_queued_for_the_socket_and_scoped_to_its_service() {
        let h = hub(SocketLimits::default(), 100);
        let t = h.accept(&ctx(1, 1), "o", AcceptOpts::default()).expect("accept");
        let send = |svc: u128| {
            h.apply(
                &ctx(1, svc),
                SocketOp::Send {
                    socket: t.socket.clone(),
                    data: WsData::Text("hi".into()),
                },
            )
        };
        assert_eq!(send(1), Ok(1));
        assert_eq!(send(2).map_err(|e| e.code), Err("not_found"), "another service");
        let close = h.apply(
            &ctx(1, 1),
            SocketOp::Close {
                socket: t.socket.clone(),
                code: 1011,
                reason: String::new(),
            },
        );
        assert_eq!(
            close.map_err(|e| e.code),
            Err("invalid_request"),
            "1011 is the runtime's"
        );
    }

    #[tokio::test]
    async fn a_draining_hub_refuses_new_sockets_and_turns() {
        let h = hub(SocketLimits::default(), 100);
        h.drain();
        assert!(h.is_draining());
        assert_eq!(
            limit_code(h.accept(&ctx(1, 1), "o", AcceptOpts::default())),
            "unavailable"
        );
        let Err(e) = h.turn(TenantId::from_u128(1), ServiceId::from_u128(1), "o").await else {
            panic!("a turn is refused while the node drains");
        };
        assert_eq!(e, TurnError::Draining);
    }

    #[tokio::test]
    async fn a_subprotocol_is_one_token_or_it_is_refused() {
        let h = hub(SocketLimits::default(), 100);
        let c = ctx(1, 1);
        let with = |p: &str| AcceptOpts {
            protocol: Some(p.to_string()),
            ..AcceptOpts::default()
        };
        assert_eq!(limit_code(h.accept(&c, "o", with(""))), "invalid_request");
        assert_eq!(
            limit_code(h.accept(&c, "o", with("chat, v2"))),
            "invalid_request",
            "a comma separates offers"
        );
        assert_eq!(
            limit_code(h.accept(&c, "o", with("chat v2"))),
            "invalid_request",
            "no whitespace"
        );
        assert_eq!(limit_code(h.accept(&c, "o", with("chat.v2"))), "ok");
    }

    #[tokio::test]
    async fn an_alarm_is_replaced_and_cleared_without_a_stale_due_time() {
        let h = hub(SocketLimits::default(), 100);
        let (t, s) = (TenantId::from_u128(1), ServiceId::from_u128(1));
        h.set_alarm_for(t, s, "o", Some(1_000));
        h.set_alarm_for(t, s, "o", Some(2_000));
        assert_eq!(h.pending_alarms(), 1, "the replaced alarm is gone");
        h.set_alarm_for(t, s, "o", None);
        assert_eq!(h.pending_alarms(), 0, "cleared");
    }

    #[tokio::test]
    async fn a_service_at_its_cap_takes_a_socket_again_once_a_ticket_is_cancelled() {
        let h = hub(
            SocketLimits {
                max_sockets: 0,
                max_per_object: 0,
                max_per_service: 1,
                max_message_bytes: 1024,
            },
            100,
        );
        let a = ctx(1, 1);
        let t = h.accept(&a, "o", AcceptOpts::default()).expect("first");
        assert_eq!(limit_code(h.accept(&a, "p", AcceptOpts::default())), "limit_exceeded");
        h.cancel_ticket(&t.ticket);
        h.accept(&a, "p", AcceptOpts::default())
            .expect("the released place is free");
    }

    /// A reserved socket (accepted, not connected) is not open: it is not counted, and the
    /// accessors that act on an open socket answer `not_found` for it.
    #[tokio::test]
    async fn a_reserved_socket_is_not_counted_and_has_no_tags_yet() {
        let h = hub(SocketLimits::default(), 100);
        let a = ctx(1, 1);
        let t = h.accept(&a, "a", AcceptOpts::default()).expect("a1");
        h.accept(&a, "b", AcceptOpts::default()).expect("b1");
        assert_eq!(h.stats().pending, 2);
        assert_eq!(h.count(&a, None, None), 0, "only open sockets count");
        assert_eq!(
            h.set_tags(&a, &t.socket, vec!["news".into()]).map_err(|e| e.code),
            Err("not_found")
        );
        assert_eq!(h.attachment(&a, &t.socket).map_err(|e| e.code), Err("not_found"));
        assert_eq!(
            h.set_attachment(&a, &t.socket, None).map_err(|e| e.code),
            Err("not_found")
        );
        assert_eq!(
            h.set_tags(&a, &t.socket, vec!["x".into(); 11]).map_err(|e| e.code),
            Err("too_large")
        );
    }

    #[tokio::test]
    async fn an_auto_response_pair_is_checked_and_an_object_name_too() {
        let h = hub(SocketLimits::default(), 100);
        let a = ctx(1, 1);
        h.set_auto_response(&a, "o", Some(("ping".into(), "pong".into())))
            .expect("set");
        assert_eq!(
            h.set_auto_response(&a, "", None).map_err(|e| e.code),
            Err("invalid_request")
        );
        assert_eq!(
            h.set_auto_response(&a, "o", Some((String::new(), "x".into())))
                .map_err(|e| e.code),
            Err("too_large")
        );
        h.set_auto_response(&a, "o", None).expect("removed");
    }
}
