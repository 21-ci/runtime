//! Warm isolate reuse (decision D-A4, `deno.reuse_isolates`).
//!
//! With reuse on, an isolate whose task finished **cleanly** keeps its evaluated module graph and
//! waits here for the next task of the same `(tenant, service, version, artifact)` on the same
//! Machine (the store is per executor, and an executor is per Machine). The next task skips module
//! evaluation; module state (globals, caches) persists between those tasks.
//!
//! Isolation rules:
//! - the key is the whole [`ServiceRef`] plus the [`PreparedKey`]: nothing is shared across
//!   tenants, services, versions or engines, even for identical code;
//! - one version per service is kept: the first task of another version discards what is kept
//!   for the old one (version change), and a finishing task of a version that is no longer
//!   current is not kept;
//! - only clean finishes are kept (the executor checks: no kill, no guest error or unhandled
//!   rejection, nothing left on the event loop, memory within bounds, `reuse_max_uses`);
//! - every kept isolate carries its own expiry, set when it is parked: the service's
//!   `min_run_time` after the answer, else `reuse_idle_ttl` (inherit). A *handoff* isolate
//!   (`min_run_time = 0` while work of the service waited) is also dropped at the first trim that
//!   sees no waiting work. The isolates the scheduler's published warm target asks for are
//!   *protected* from expiry (D-B2): that many per service never expire. Kept isolates are shed at
//!   memory pressure `High` (down to the protected ones) or `Critical` (all).

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::Instant;

use rt_types::{EnvBundle, PreparedKey, ServiceId, ServiceRef, TenantId};

use crate::isolate::Isolate;

/// What a kept isolate may be reused for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WarmId {
    pub service: ServiceRef,
    pub key: PreparedKey,
}

/// Reuse bookkeeping carried by an isolate (`Isolate::reuse`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ReuseState {
    /// Tasks this isolate has served.
    pub uses: u32,
    /// Hash of the env bundle the guest read in this isolate: its module state may hold those
    /// values, so the isolate is reused only while the service's env (secrets version) is the same.
    pub env_seen: Option<u64>,
    /// Bytes charged after the first task (the evaluated module); later growth is measured
    /// against it.
    pub baseline: Option<u64>,
}

/// Order-sensitive hash of an env bundle (never leaves the process).
pub fn env_hash(env: &EnvBundle) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    env.env.hash(&mut h);
    0xffu8.hash(&mut h);
    env.secrets.hash(&mut h);
    h.finish()
}

/// Growth of a reused isolate's charged memory over its baseline that discards it: the larger of
/// this and half the baseline.
pub const REUSE_MAX_GROWTH: u64 = 16 * 1024 * 1024;

/// How many isolates one service may keep on this Machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keep {
    /// At most this many (the published warm target, else `deno.reuse_max_idle`).
    pub bound: usize,
    /// This many are exempt from the idle TTL: the published warm target (0 when there is none).
    pub protected: usize,
}

/// An idle kept isolate.
pub struct Kept {
    pub iso: Isolate,
    pub parked: Instant,
    /// Not taken nor kept (above the protected count) from this instant on.
    pub expires: Instant,
    /// Kept only while work of its service waits in admission (`min_run_time = 0`).
    pub handoff: bool,
}

impl Kept {
    /// Past its keep-alive at `now`, or a handoff with nothing left waiting.
    fn done(&self, now: Instant, waiting: bool) -> bool {
        now >= self.expires || (self.handoff && !waiting)
    }
}

struct Entry {
    id: WarmId,
    /// Oldest first; tasks take the most recently parked (warmest) one.
    idle: VecDeque<Kept>,
}

/// The kept isolates of one Machine.
#[derive(Default)]
pub struct WarmStore {
    services: HashMap<(TenantId, ServiceId), Entry>,
    total: usize,
}

impl WarmStore {
    /// Idle isolates kept, all services.
    pub fn len(&self) -> usize {
        self.total
    }

    /// Nothing kept and no service tracked (not even an emptied one awaiting [`WarmStore::trim`]).
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Services with an entry (kept isolates or the marker of their current version).
    pub fn services(&self) -> usize {
        self.services.len()
    }

    /// Idle isolates kept for `id`.
    pub fn len_of(&self, id: &WarmId) -> usize {
        self.services
            .get(&(id.service.tenant, id.service.service))
            .filter(|e| e.id == *id)
            .map_or(0, |e| e.idle.len())
    }

    /// Take the warmest idle isolate for `id`. Isolates kept for another version of the same
    /// service, and expired ones (`now >= expires`), are moved to `dispose`; this task's version
    /// then becomes the one kept for the service. The first `protected` isolates of a service do
    /// not expire. A service with nothing kept gets no entry here (only `park` creates one), and
    /// entries left empty are dropped by [`WarmStore::trim`].
    pub fn take(&mut self, id: &WarmId, now: Instant, protected: usize, dispose: &mut Vec<Isolate>) -> Option<Kept> {
        let entry = self.services.get_mut(&(id.service.tenant, id.service.service))?;
        if entry.id != *id {
            self.total -= entry.idle.len();
            dispose.extend(entry.idle.drain(..).map(|k| k.iso));
            entry.id = *id;
        }
        loop {
            let held = entry.idle.len();
            let k = entry.idle.pop_back()?;
            self.total -= 1;
            if held <= protected || now < k.expires {
                return Some(k);
            }
            dispose.push(k.iso);
        }
    }

    /// Keep `iso` for `id` until `expires` (`handoff`: see [`Kept::handoff`]), at most `bound`
    /// for the service, `total_bound` overall: the oldest go first, those above their service's
    /// protected count before the protected ones. Returns the isolates to dispose: `iso` itself
    /// when its version is no longer the service's current one or nothing may be kept.
    #[allow(clippy::too_many_arguments)]
    pub fn park(
        &mut self,
        id: &WarmId,
        iso: Isolate,
        now: Instant,
        expires: Instant,
        handoff: bool,
        bound: usize,
        total_bound: usize,
        protected: impl Fn(&WarmId) -> usize,
    ) -> Vec<Isolate> {
        let mut out = Vec::new();
        if bound == 0 || total_bound == 0 {
            out.push(iso);
            return out;
        }
        let entry = self
            .services
            .entry((id.service.tenant, id.service.service))
            .or_insert_with(|| Entry {
                id: *id,
                idle: VecDeque::new(),
            });
        if entry.id != *id {
            out.push(iso);
            return out;
        }
        while entry.idle.len() >= bound {
            if let Some(k) = entry.idle.pop_front() {
                self.total -= 1;
                out.push(k.iso);
            }
        }
        entry.idle.push_back(Kept {
            iso,
            parked: now,
            expires,
            handoff,
        });
        self.total += 1;
        while self.total > total_bound {
            let Some(k) = self.pop_oldest(&protected) else { break };
            out.push(k.iso);
        }
        out
    }

    /// The oldest idle isolate of a service that holds more than its protected count; only when
    /// every service is down to it, the oldest of all.
    fn pop_oldest(&mut self, protected: &impl Fn(&WarmId) -> usize) -> Option<Kept> {
        let oldest = |only_unprotected: bool| {
            self.services
                .iter()
                .filter(|(_, e)| !only_unprotected || e.idle.len() > protected(&e.id))
                .filter_map(|(k, e)| e.idle.front().map(|f| (*k, f.parked)))
                .min_by_key(|(_, parked)| *parked)
                .map(|(k, _)| k)
        };
        let key = oldest(true).or_else(|| oldest(false))?;
        let k = self.services.get_mut(&key)?.idle.pop_front()?;
        self.total -= 1;
        Some(k)
    }

    /// Trim every service to its [`Keep::bound`] (oldest first) and drop the isolates whose
    /// keep-alive ended ([`Kept::expires`] passed, or a handoff while `has_waiting` is false for
    /// the service), except the newest [`Keep::protected`] of a service; services left empty are
    /// forgotten. Returns what to dispose.
    pub fn trim(
        &mut self,
        now: Instant,
        mut keep_of: impl FnMut(&WarmId) -> Keep,
        has_waiting: impl Fn(&WarmId) -> bool,
    ) -> Vec<Isolate> {
        let mut out = Vec::new();
        for entry in self.services.values_mut() {
            let keep = keep_of(&entry.id);
            while entry.idle.len() > keep.bound {
                if let Some(k) = entry.idle.pop_front() {
                    out.push(k.iso);
                }
            }
            let mut removable = entry.idle.len().saturating_sub(keep.protected);
            if removable == 0 {
                continue;
            }
            let waiting = entry.idle.iter().any(|k| k.handoff) && has_waiting(&entry.id);
            let mut i = 0;
            while i < entry.idle.len() && removable > 0 {
                if entry.idle[i].done(now, waiting) {
                    if let Some(k) = entry.idle.remove(i) {
                        out.push(k.iso);
                    }
                    removable -= 1;
                } else {
                    i += 1;
                }
            }
        }
        self.total -= out.len();
        self.services.retain(|_, e| !e.idle.is_empty());
        out
    }

    /// Memory pressure `High`: every service keeps at most `keep(id)` isolates (the newest); the
    /// rest, TTL or not, are returned to dispose.
    pub fn shed_to(&mut self, mut keep: impl FnMut(&WarmId) -> usize) -> Vec<Isolate> {
        let mut out = Vec::new();
        for entry in self.services.values_mut() {
            let keep = keep(&entry.id);
            while entry.idle.len() > keep {
                if let Some(k) = entry.idle.pop_front() {
                    out.push(k.iso);
                }
            }
        }
        self.total -= out.len();
        self.services.retain(|_, e| !e.idle.is_empty());
        out
    }

    /// Everything kept (memory pressure).
    pub fn drain(&mut self) -> Vec<Isolate> {
        let out: Vec<Isolate> = self
            .services
            .drain()
            .flat_map(|(_, e)| e.idle.into_iter().map(|k| k.iso))
            .collect();
        self.total = 0;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_types::{Digest, RuntimeKind, VersionNo};

    #[test]
    fn taking_for_a_service_with_nothing_kept_leaves_no_entry() {
        let service = ServiceRef {
            tenant: TenantId::from_u128(1),
            service: ServiceId::from_u128(2),
            version: VersionNo(1),
            digest: Digest::of(b"x"),
        };
        let id = WarmId {
            service,
            key: PreparedKey {
                kind: RuntimeKind::Deno,
                digest: service.digest,
                engine_fp: 1,
            },
        };
        let mut store = WarmStore::default();
        let mut dispose = Vec::new();
        for _ in 0..3 {
            assert!(store.take(&id, Instant::now(), 2, &mut dispose).is_none());
        }
        assert_eq!((store.services(), store.len(), dispose.len()), (0, 0, 0));
        assert!(store.is_empty());
        assert!(store
            .trim(Instant::now(), |_| Keep { bound: 4, protected: 0 }, |_| false)
            .is_empty());
        assert!(store.shed_to(|_| 0).is_empty());
    }
}
