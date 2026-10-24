//! Idle instances: instances kept after a clean task (`wasm.reuse_instances`) and **warm**
//! instances pre-instantiated off the request path for the autoscaler's targets
//! (`rt_sched::WarmTargets`, decisions D-A3/D-A4).
//!
//! Each Machine keeps its own [`IdlePool`], keyed by `(tenant, service, version, prepared
//! artifact, P)`: an instance never crosses a tenant, a service, a version or a Processor. Per
//! key there are two kinds:
//! - **fresh**: instantiated but never ran a task. A task takes one whatever the reuse knob says,
//!   so with reuse off every task still sees a never-used instance (WSM-04 holds);
//! - **used**: ran tasks cleanly; only a task of a service with `reuse_instances` takes one.
//!
//! An idle instance remembers the environment it was built with: a task with another env (a
//! rotated secret: wasi-libc caches the environment on first read) discards it.
//!
//! Idle instances hold pooling-allocator slots, which running tasks need too. One process-wide
//! [`IdleSlots`] budget, shared by every Machine, bounds them (half the pool by default); when it
//! is full a new idle instance replaces the least recently used one of any Machine, and a task
//! whose instantiation finds the pool exhausted evicts the same way and retries (WSM-19).
//!
//! All are evictable memory (`wasm_reuse`), dropped at memory pressure `High`, and dropped once
//! their key has no warm target and the service's idle TTL (`cache.idle_ttl`) passed.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_host::HostServices;
use rt_mem::{MemCategory, Owner};
use rt_types::{
    EnvBundle, PreparedKey, ProcId, SecretsVersion, ServiceId, ServiceKnobs, ServiceRef, TenantId, VersionNo,
};
use wasmtime::component::ResourceTable;
use wasmtime::{Engine, Store, UpdateDeadline};
use wasmtime_wasi_http::WasiHttpCtx;
use zeroize::Zeroizing;

use crate::clock::FrozenClock;
use crate::engine::PoolSizing;
use crate::executor::build_wasi_ctx;
use crate::prepared::{GuestExports, WasmPrepared};
use crate::state::{GovernorLimiter, GuardedHttpHooks, WasmState};
use crate::stdio::GuestLogs;

/// Smallest `wasm_reuse` reservation of an idle instance.
pub(crate) const MIN_IDLE_BYTES: usize = 64 * 1024;
/// Wall-clock bound of one pre-instantiation (component start code included).
const PREWARM_WALL: Duration = Duration::from_secs(1);
/// Fuel of one pre-instantiation (only when the engine meters fuel).
const PREWARM_FUEL: u64 = 1_000_000_000;
/// Idle-slot budget of an executor built without a shared one (tests, embedders).
pub(crate) const DEFAULT_IDLE_SLOTS: usize = 16;

pub(crate) type Env = Arc<Zeroizing<EnvBundle>>;

/// Same environment: the very same shared bundle, or equal contents.
pub(crate) fn same_env(a: &Env, b: &Env) -> bool {
    let (x, y): (&EnvBundle, &EnvBundle) = (a, b);
    Arc::ptr_eq(a, b) || x == y
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IdleKey {
    pub tenant: TenantId,
    pub service: ServiceId,
    pub version: VersionNo,
    /// The artifact (digest + kind + engine): never reused across code.
    pub prepared: PreparedKey,
    pub proc: ProcId,
}

// ---------------------------------------------------------------------------------------------
// The process-wide slot budget
// ---------------------------------------------------------------------------------------------

/// Pooling-allocator slots that idle (reusable or warm) instances may hold, over all Machines.
pub struct IdleSlots {
    cap: usize,
    held: AtomicUsize,
    pools: Mutex<Vec<Weak<Mutex<IdlePool>>>>,
}

impl std::fmt::Debug for IdleSlots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleSlots")
            .field("cap", &self.cap)
            .field("held", &self.held())
            .finish()
    }
}

impl IdleSlots {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            held: AtomicUsize::new(0),
            pools: Mutex::new(Vec::new()),
        })
    }

    /// The node budget: half of the pool's slots, so the other half always stays for running
    /// tasks (a 3-slot pool keeps 1 idle instance).
    pub fn for_pool(sizing: &PoolSizing) -> Arc<Self> {
        Self::new(sizing.slots as usize / 2)
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Idle instances held (and pre-instantiations in flight) over all Machines.
    pub fn held(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }

    pub(crate) fn register(&self, pool: &Arc<Mutex<IdlePool>>) {
        let mut pools = self.pools.lock();
        pools.retain(|w| w.strong_count() > 0);
        pools.push(Arc::downgrade(pool));
    }

    /// A slot if the budget has one.
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<SlotToken> {
        self.held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |h| (h < self.cap).then_some(h + 1))
            .ok()
            .map(|_| SlotToken { slots: self.clone() })
    }

    /// A slot, evicting the least recently used idle instance of any Machine when the budget is
    /// full. `None` when nothing could be evicted (or the budget is 0).
    pub(crate) fn acquire_evicting(self: &Arc<Self>) -> Option<SlotToken> {
        for _ in 0..4 {
            if let Some(t) = self.try_acquire() {
                return Some(t);
            }
            if self.cap == 0 || !self.evict_lru() {
                return None;
            }
        }
        None // LCOV_EXCL_LINE: reached only when another thread takes the slot every one of four evictions freed
    }

    /// Drop the least recently used idle instance of any Machine; `false` when none is held.
    pub fn evict_lru(&self) -> bool {
        let pools: Vec<Arc<Mutex<IdlePool>>> = {
            let mut g = self.pools.lock();
            g.retain(|w| w.strong_count() > 0);
            g.iter().filter_map(Weak::upgrade).collect()
        };
        // Pass 1: the oldest instance (one pool locked at a time).
        let oldest = pools
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.lock().oldest().map(|t| (i, t)))
            .min_by_key(|(_, t)| *t);
        let Some((i, _)) = oldest else { return false };
        // Pass 2: take that pool's oldest now (it may have changed meanwhile); drop it unlocked.
        let victim = pools[i].lock().pop_oldest();
        victim.is_some()
    }
}

/// One slot of the [`IdleSlots`] budget, released on drop.
pub(crate) struct SlotToken {
    slots: Arc<IdleSlots>,
}

impl Drop for SlotToken {
    fn drop(&mut self) {
        self.slots.held.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------------------------
// One Machine's idle instances
// ---------------------------------------------------------------------------------------------

pub(crate) struct IdleInstance {
    pub store: Store<WasmState>,
    pub guest: GuestExports,
    /// Tasks run on this instance (0 = fresh).
    pub uses: u32,
    /// The environment its WASI context was built with.
    pub env: Env,
    /// Idle since (LRU order of [`IdleSlots::evict_lru`]).
    pub since: Instant,
    pub slot: SlotToken,
    /// Its own keep-alive end (`min_run_time`): `None` follows the list's `idle_ttl` rule.
    pub expires: Option<Instant>,
    /// Kept only for queued work (`min_run_time = 0`): dropped once none waits.
    pub handoff: bool,
}

/// A used instance's own keep-alive is over: its expiry passed, or it was kept for queued work
/// and none waits any more (`waiting`: tasks of its service wait in admission).
pub(crate) fn used_expired(inst: &IdleInstance, now: Instant, waiting: bool) -> bool {
    inst.expires.is_some_and(|e| now >= e) || (inst.handoff && !waiting)
}

pub(crate) struct IdleList {
    pub fresh: Vec<IdleInstance>,
    pub used: Vec<IdleInstance>,
    /// Pre-instantiations in flight for this key.
    pub pending: usize,
    pub last_used: Instant,
    /// The service's `cache.idle_ttl` (last task seen; the built-in default before that).
    pub idle_ttl: Duration,
}

impl IdleList {
    fn new() -> Self {
        Self {
            fresh: Vec::new(),
            used: Vec::new(),
            pending: 0,
            last_used: Instant::now(),
            idle_ttl: ServiceKnobs::default().idle_ttl,
        }
    }

    /// Ready instances plus those being instantiated: what counts toward a warm target.
    pub fn ready(&self) -> usize {
        self.fresh.len() + self.used.len() + self.pending
    }

    fn is_empty(&self) -> bool {
        self.fresh.is_empty() && self.used.is_empty() && self.pending == 0
    }

    pub fn touch(&mut self, idle_ttl: Duration) {
        self.last_used = Instant::now();
        self.idle_ttl = idle_ttl;
    }
}

#[derive(Default)]
pub(crate) struct IdlePool {
    pub map: HashMap<IdleKey, IdleList>,
}

/// What [`IdlePool::take`] hands out: the instance, plus instances to drop outside the lock.
pub(crate) struct Taken {
    pub inst: Option<IdleInstance>,
    pub discard: Vec<IdleInstance>,
}

impl IdlePool {
    /// An idle instance for a task of `key`: with `reuse`, a used one first (warmest), else a
    /// fresh one. Without reuse only a fresh one, and leftover used instances (the knob was
    /// turned off) are discarded.
    pub fn take(&mut self, key: &IdleKey, reuse: bool, idle_ttl: Duration) -> Taken {
        let mut discard = Vec::new();
        let Some(list) = self.map.get_mut(key) else {
            return Taken { inst: None, discard };
        };
        list.touch(idle_ttl);
        let inst = if reuse {
            list.used.pop().or_else(|| list.fresh.pop())
        } else {
            discard.append(&mut list.used);
            list.fresh.pop()
        };
        if list.is_empty() {
            self.map.remove(key);
        }
        Taken { inst, discard }
    }

    pub fn list(&mut self, key: IdleKey) -> &mut IdleList {
        self.map.entry(key).or_insert_with(IdleList::new)
    }

    pub fn ready(&self, key: &IdleKey) -> usize {
        self.map.get(key).map_or(0, IdleList::ready)
    }

    /// Instances held (fresh + used).
    pub fn instances(&self) -> usize {
        self.map.values().map(|l| l.fresh.len() + l.used.len()).sum()
    }

    /// Drop every idle instance (memory pressure); returns them to drop outside the lock.
    pub fn drain_all(&mut self) -> Vec<IdleInstance> {
        let mut out = Vec::new();
        for list in self.map.values_mut() {
            out.append(&mut list.fresh);
            out.append(&mut list.used);
        }
        self.map.retain(|_, l| !l.is_empty());
        out
    }

    fn oldest(&self) -> Option<Instant> {
        self.map
            .values()
            .flat_map(|l| l.fresh.iter().chain(l.used.iter()))
            .map(|i| i.since)
            .min()
    }

    fn pop_oldest(&mut self) -> Option<IdleInstance> {
        let (key, used, idx) = self
            .map
            .iter()
            .flat_map(|(k, l)| {
                let fresh = l.fresh.iter().enumerate().map(move |(i, x)| (*k, false, i, x.since));
                let used = l.used.iter().enumerate().map(move |(i, x)| (*k, true, i, x.since));
                fresh.chain(used)
            })
            .min_by_key(|(_, _, _, since)| *since)
            .map(|(k, u, i, _)| (k, u, i))?;
        let list = self.map.get_mut(&key)?;
        let inst = if used {
            list.used.remove(idx)
        } else {
            list.fresh.remove(idx)
        };
        if list.is_empty() {
            self.map.remove(&key);
        }
        Some(inst)
    }

    /// Scale down: a key without a target (`want == 0`) keeps its instances until the idle TTL
    /// passed; a key with a target drops fresh instances above it. Used instances above a target
    /// stay (bounded by `wasm.reuse_max_idle`) until the TTL. A used instance with its own
    /// keep-alive (`min_run_time`) ignores the TTL: it goes once [`used_expired`], as long as the
    /// key stays at or above its target (the floor beats `min_run_time`). `has_waiting(service)`:
    /// tasks of that service wait in admission. Returns what to drop.
    pub fn scale_down(
        &mut self,
        now: Instant,
        want: impl Fn(&IdleKey) -> usize,
        has_waiting: impl Fn(ServiceId) -> bool,
    ) -> Vec<IdleInstance> {
        let mut out = Vec::new();
        self.map.retain(|k, l| {
            let w = want(k);
            let waiting = l.used.iter().any(|i| i.handoff) && has_waiting(k.service);
            let mut i = 0;
            while i < l.used.len() && l.ready() > w {
                if used_expired(&l.used[i], now, waiting) {
                    out.push(l.used.remove(i));
                } else {
                    i += 1;
                }
            }
            if w == 0 {
                if l.pending == 0 && now.saturating_duration_since(l.last_used) >= l.idle_ttl {
                    out.append(&mut l.fresh);
                    let (inherit, own): (Vec<_>, Vec<_>) = std::mem::take(&mut l.used)
                        .into_iter()
                        .partition(|i| i.expires.is_none());
                    out.extend(inherit);
                    l.used = own;
                }
            } else {
                while l.ready() > w && !l.fresh.is_empty() {
                    out.extend(l.fresh.pop());
                }
            }
            !l.is_empty()
        });
        out
    }
}

/// Retry delay after a failed pre-instantiation of one key: 1 s, doubling, at most 30 s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Backoff {
    pub until: Instant,
    pub failures: u32,
}

impl Backoff {
    pub fn after_failure(prev: Option<Backoff>, now: Instant) -> Backoff {
        let failures = prev.map_or(0, |b| b.failures).saturating_add(1);
        let delay = Duration::from_secs(1u64 << (failures - 1).min(5)).min(Duration::from_secs(30));
        Backoff {
            until: now + delay,
            failures,
        }
    }
}

/// The order in which one hook call visits `n` keys: starting at `cursor % n`, wrapping around,
/// so consecutive calls start at consecutive keys and none is starved.
pub(crate) fn rotation(n: usize, cursor: usize) -> impl Iterator<Item = usize> {
    let start = if n == 0 { 0 } else { cursor % n };
    (0..n).map(move |i| (start + i) % n)
}

// ---------------------------------------------------------------------------------------------
// Artifacts known to the process
// ---------------------------------------------------------------------------------------------

/// Compiled artifacts any Machine's executor has run, so every Machine can pre-instantiate them
/// (the T1 cache owns them: `Weak`, an evicted artifact is not kept alive or warmed).
fn artifacts() -> &'static Mutex<HashMap<PreparedKey, Weak<WasmPrepared>>> {
    static ARTIFACTS: OnceLock<Mutex<HashMap<PreparedKey, Weak<WasmPrepared>>>> = OnceLock::new();
    ARTIFACTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Above this many entries dead ones are pruned on insert.
const ARTIFACTS_PRUNE_AT: usize = 4096;

pub(crate) fn remember(p: &Arc<WasmPrepared>) {
    insert_pruning(&mut artifacts().lock(), p.key, Arc::downgrade(p), ARTIFACTS_PRUNE_AT);
}

/// Record an artifact in `map`, dropping the entries whose artifact is gone first once it holds
/// `prune_at`.
pub(crate) fn insert_pruning(
    map: &mut HashMap<PreparedKey, Weak<WasmPrepared>>,
    key: PreparedKey,
    artifact: Weak<WasmPrepared>,
    prune_at: usize,
) {
    if map.len() >= prune_at {
        map.retain(|_, w| w.strong_count() > 0);
    }
    map.insert(key, artifact);
}

/// The live artifact for `key`, compiled for `engine`.
pub(crate) fn lookup(key: &PreparedKey, engine: &Engine) -> Option<Arc<WasmPrepared>> {
    let p = artifacts().lock().get(key)?.upgrade()?;
    Engine::same(p.component.engine(), engine).then_some(p)
}

// ---------------------------------------------------------------------------------------------
// Pre-instantiation
// ---------------------------------------------------------------------------------------------

pub(crate) type PrewarmJob = Pin<Box<dyn Future<Output = Option<IdleInstance>>>>;

/// A fresh instance of `prepared` for `svc`, built without a task. Its store holds a detached
/// host handle (`rt_host::HostServices::detached_handle`): every `std:host` import refuses,
/// outbound HTTP is denied, logs are dropped, nothing is charged to the tenant. The WASI env is
/// the service's (fetched here from the control plane, like a task's), so a task with the same
/// env can take the instance. Memory is charged to `wasm_reuse` (evictable) under the service;
/// the component's start code runs in 1 ms epoch slices (each slice yields, so the caller bounds
/// its work per poll by one tick) within [`PREWARM_WALL`].
///
/// `None` when the governor refuses the reservation.
pub(crate) fn prewarm_job(
    engine: &Engine,
    host: &HostServices,
    svc: ServiceRef,
    prepared: Arc<WasmPrepared>,
    slot: SlotToken,
) -> Option<PrewarmJob> {
    let reservation = host
        .deps()
        .mem
        .reserve(
            MemCategory::WasmReuse,
            MIN_IDLE_BYTES as u64,
            Owner::service(svc.tenant, svc.service),
        )
        .ok()?;
    let engine = engine.clone();
    let host = host.clone();
    Some(Box::pin(async move {
        let env = host
            .deps()
            .control
            .env_shared(svc.service, SecretsVersion::default())
            .await
            .ok()?;
        let handle = host.detached_handle(svc);
        let clock = FrozenClock::new();
        let logs = GuestLogs::new();
        let state = WasmState {
            wasi_ctx: build_wasi_ctx(&clock, &logs, &env),
            http_ctx: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            // The service's `memory_mb` is checked when a task takes the instance.
            limiter: GovernorLimiter::new(reservation, usize::MAX),
            http_hooks: GuardedHttpHooks { host: handle.clone() },
            host: handle,
            clock,
            logs,
        };
        let mut store = Store::new(&engine, state);
        store.limiter(|s| &mut s.limiter);
        let _ = store.set_fuel(PREWARM_FUEL);
        let deadline = Instant::now() + PREWARM_WALL;
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_cx| Ok(prewarm_epoch(deadline, Instant::now())));
        let guest = prepared.instantiate(&mut store).await.ok()?;
        Some(IdleInstance {
            store,
            guest,
            uses: 0,
            env,
            since: Instant::now(),
            slot,
            expires: None,
            handoff: false,
        })
    }))
}

/// What a pre-instantiation does at an epoch tick: yield to the Machine, or give up once the
/// start code ran past its wall budget.
fn prewarm_epoch(deadline: Instant, now: Instant) -> UpdateDeadline {
    if now >= deadline {
        UpdateDeadline::Interrupt
    } else {
        UpdateDeadline::Yield(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_one_second_up_to_thirty() {
        let now = Instant::now();
        let mut b = None;
        let mut delays = Vec::new();
        for _ in 0..8 {
            let next = Backoff::after_failure(b, now);
            delays.push((next.until - now).as_secs());
            b = Some(next);
        }
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    }

    #[test]
    fn rotation_starts_one_key_later_each_call() {
        let order = |cursor| rotation(3, cursor).collect::<Vec<_>>();
        assert_eq!(order(0), vec![0, 1, 2]);
        assert_eq!(order(1), vec![1, 2, 0]);
        assert_eq!(order(5), vec![2, 0, 1]);
        assert_eq!(rotation(0, 7).count(), 0);
    }

    #[test]
    fn the_slot_budget_is_counted_by_tokens() {
        let slots = IdleSlots::new(2);
        let a = slots.try_acquire().expect("first");
        let b = slots.try_acquire().expect("second");
        assert!(slots.try_acquire().is_none(), "over the budget");
        assert_eq!(slots.held(), 2);
        drop(a);
        assert_eq!(slots.held(), 1);
        assert!(slots.acquire_evicting().is_some());
        drop(b);
        assert!(!slots.evict_lru(), "no pools, nothing to evict");
    }

    #[test]
    fn the_idle_budget_prints_its_cap_and_what_is_held() {
        let slots = IdleSlots::new(3);
        let _held = slots.try_acquire().expect("one slot");
        assert_eq!(format!("{slots:?}"), "IdleSlots { cap: 3, held: 1 }");
    }

    #[test]
    fn a_budget_of_zero_never_hands_out_a_slot_not_even_by_evicting() {
        assert!(IdleSlots::new(0).acquire_evicting().is_none());
    }

    #[test]
    fn remembered_artifacts_that_are_gone_are_pruned_once_the_map_is_full() {
        let key = |n: u64| PreparedKey {
            digest: rt_types::Digest::of(&n.to_le_bytes()),
            kind: rt_types::RuntimeKind::Wasm,
            engine_fp: 1,
        };
        let mut map = HashMap::new();
        insert_pruning(&mut map, key(1), Weak::new(), 2);
        insert_pruning(&mut map, key(2), Weak::new(), 2);
        assert_eq!(map.len(), 2, "below the cap nothing is pruned");
        insert_pruning(&mut map, key(3), Weak::new(), 2);
        assert_eq!(
            map.keys().copied().collect::<Vec<_>>(),
            vec![key(3)],
            "the dead entries went, the new one stays"
        );
    }

    #[test]
    fn a_pre_instantiation_yields_to_the_machine_until_its_wall_budget_is_spent() {
        let now = Instant::now();
        assert!(matches!(
            prewarm_epoch(now + Duration::from_secs(1), now),
            UpdateDeadline::Yield(1)
        ));
        assert!(matches!(prewarm_epoch(now, now), UpdateDeadline::Interrupt));
        assert!(matches!(
            prewarm_epoch(now, now + Duration::from_secs(1)),
            UpdateDeadline::Interrupt
        ));
    }
}
