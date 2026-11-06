//! Per-path ingress rate rules (`ingress.rate_rules`).
//!
//! `by: "service"` is a [`ShardedBucket`]. `by: "ip"` is a GCRA cell, one `u64` theoretical arrival
//! time per client in a `DashMap`: about 90 bytes a client and nothing to rebalance. The cells are
//! capped per node and per service, so one service's IP churn cannot push the clients of another
//! into overflow. Past a cap, new clients share the rule's one overflow cell; active cells are never
//! evicted, since that would hand an attacker a fresh burst. Rule state is keyed by rule content and
//! outlives route refreshes, so a deploy does not reset limits.
//!
//! The check path takes no lock across anything but a map lookup and never touches a store.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rt_sched::Clock;
use rt_types::path_pattern::{canonical_path, PathPattern};
use rt_types::{RateBy, RateRule, ServiceId};

use crate::ratelimit::ShardedBucket;

/// A cell whose arrival time is this far in the past holds a full burst again, and may go.
const IDLE: Duration = Duration::from_secs(10 * 60);
const NANOS_PER_SEC: u64 = 1_000_000_000;
const CELL_SHARDS: usize = 16;

/// Cell caps, resolved only when a new client appears.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateCaps {
    pub global: usize,
    pub per_service: usize,
}

impl RateCaps {
    /// `configured` keys (0: as many as fit in 1/64 of the memory budget at 96 bytes each), never
    /// more than the whole budget holds: rt-mem does not see these cells. One service may hold 1/8
    /// of them.
    pub fn resolve(configured: u64, budget_bytes: u64) -> Self {
        let global = if configured > 0 {
            configured.min((budget_bytes / 96).max(1024))
        } else {
            (budget_bytes / 64 / 96).max(1024)
        };
        let global = usize::try_from(global).unwrap_or(usize::MAX);
        Self {
            global,
            per_service: (global / 8).max(1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateVerdict {
    Allow,
    /// Rule `rule` (its index in the list) refused this request.
    Limited {
        rule: usize,
        retry_after: Duration,
    },
    /// The path has no canonical form, so no rule can be ruled out: refuse it.
    Malformed,
}

/// A client: an IPv4 address or the /64 of an IPv6 address.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum IpKey {
    V4(u32),
    V6(u64),
}

impl From<IpAddr> for IpKey {
    fn from(ip: IpAddr) -> Self {
        match ip.to_canonical() {
            IpAddr::V4(a) => Self::V4(a.to_bits()),
            IpAddr::V6(a) => Self::V6((a.to_bits() >> 64) as u64),
        }
    }
}

/// Generic cell rate: `Some(wait)` when refused. Arrival times are ns from the limiter's base + 1.
fn gcra(cell: &AtomicU64, now: u64, emission: u64, tolerance: u64) -> Option<Duration> {
    let mut tat = cell.load(Ordering::Relaxed);
    loop {
        let next = tat.max(now).saturating_add(emission);
        let ahead = next - now;
        if ahead > tolerance {
            return Some(Duration::from_nanos(ahead - tolerance));
        }
        match cell.compare_exchange_weak(tat, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return None,
            Err(t) => tat = t,
        }
    }
}

enum Limit {
    Bucket(ShardedBucket),
    Cells {
        emission: u64,
        tolerance: u64,
        cells: DashMap<IpKey, AtomicU64>,
        overflow: AtomicU64,
    },
    /// `rps: 0` is refused by validation; a stored one admits nobody.
    Closed,
}

/// The state of one rule's content, shared by every rule list that holds the same rule.
struct RuleState {
    content: RateRule,
    pattern: PathPattern,
    limit: Limit,
    global: Arc<AtomicUsize>,
    service: Arc<AtomicUsize>,
}

impl Drop for RuleState {
    fn drop(&mut self) {
        if let Limit::Cells { cells, .. } = &self.limit {
            self.global.fetch_sub(cells.len(), Ordering::Relaxed);
            self.service.fetch_sub(cells.len(), Ordering::Relaxed);
        }
    }
}

impl RuleState {
    fn method_applies(&self, method: &str) -> bool {
        let m = &self.content.methods;
        m.is_empty() || m.iter().any(|x| x.eq_ignore_ascii_case(method))
    }

    fn path_applies(&self, raw: &[u8], canonical: &[u8]) -> bool {
        // An already canonical path is borrowed from `raw`: one match is enough.
        self.pattern.matches_folded(canonical)
            || (!std::ptr::eq(raw.as_ptr(), canonical.as_ptr()) && self.pattern.matches_folded(raw))
    }

    /// `Some(wait)` when refused.
    fn take(&self, now: u64, client: Option<IpAddr>, caps: impl FnOnce() -> RateCaps) -> Option<Duration> {
        match &self.limit {
            Limit::Closed => Some(Duration::from_secs(60)),
            Limit::Bucket(b) => (!b.try_acquire(0, 1)).then(|| b.retry_after()),
            Limit::Cells {
                emission,
                tolerance,
                cells,
                overflow,
            } => {
                // Without an address no key exists: such requests share the overflow cell.
                let Some(key) = client.map(IpKey::from) else {
                    return gcra(overflow, now, *emission, *tolerance);
                };
                if let Some(cell) = cells.get(&key) {
                    return gcra(&cell, now, *emission, *tolerance);
                }
                let caps = caps();
                // Under churn at the cap this returns before taking any write lock.
                if self.service.load(Ordering::Relaxed) >= caps.per_service
                    || self.global.load(Ordering::Relaxed) >= caps.global
                {
                    return gcra(overflow, now, *emission, *tolerance);
                }
                match cells.entry(key) {
                    Entry::Occupied(e) => gcra(e.get(), now, *emission, *tolerance),
                    Entry::Vacant(v) => {
                        if !self.reserve(caps) {
                            drop(v);
                            return gcra(overflow, now, *emission, *tolerance);
                        }
                        let cell = v.insert(AtomicU64::new(0));
                        gcra(&cell, now, *emission, *tolerance)
                    }
                }
            }
        }
    }

    /// Take one key from both caps, exactly: racers cannot pass them together.
    fn reserve(&self, caps: RateCaps) -> bool {
        if self.service.fetch_add(1, Ordering::Relaxed) >= caps.per_service {
            self.service.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        if self.global.fetch_add(1, Ordering::Relaxed) >= caps.global {
            self.global.fetch_sub(1, Ordering::Relaxed);
            self.service.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    fn prune(&self, now: u64, idle: u64) {
        if let Limit::Cells { cells, .. } = &self.limit {
            let mut removed = 0usize;
            cells.retain(|_, tat| {
                let keep = tat.load(Ordering::Relaxed).saturating_add(idle) >= now;
                removed += usize::from(!keep);
                keep
            });
            self.global.fetch_sub(removed, Ordering::Relaxed);
            self.service.fetch_sub(removed, Ordering::Relaxed);
        }
    }
}

/// One service's compiled rule list.
struct ServiceRules {
    rules: Vec<Arc<RuleState>>,
    keys: Arc<AtomicUsize>,
}

impl ServiceRules {
    fn same_list(&self, rules: &[RateRule]) -> bool {
        self.rules.len() == rules.len() && self.rules.iter().zip(rules).all(|(a, b)| a.content == *b)
    }
}

/// Every service's rule state. Cheap to keep: an entry exists only for a service with rules.
#[derive(Default)]
pub struct RateRuleLimiter {
    services: DashMap<ServiceId, Arc<ServiceRules>>,
    keys: Arc<AtomicUsize>,
    base: OnceLock<Instant>,
}

impl RateRuleLimiter {
    fn now(&self, clock: &Arc<dyn Clock>) -> u64 {
        let now = clock.now();
        let base = *self.base.get_or_init(|| now);
        u64::try_from(now.saturating_duration_since(base).as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }

    /// Run the first rule that applies. `caps` is called only when a new client appears. Call it
    /// only for a service with rules: a service without any must not pay for this.
    #[allow(clippy::too_many_arguments)]
    pub fn check(
        &self,
        clock: &Arc<dyn Clock>,
        service: ServiceId,
        rules: &[RateRule],
        method: &str,
        path: &str,
        client: Option<IpAddr>,
        caps: impl FnOnce() -> RateCaps,
    ) -> RateVerdict {
        let Ok(canonical) = canonical_path(path) else {
            return RateVerdict::Malformed;
        };
        let compiled = self.compiled(clock, service, rules);
        for (i, rule) in compiled.rules.iter().enumerate() {
            if rule.method_applies(method) && rule.path_applies(path.as_bytes(), &canonical) {
                return match rule.take(self.now(clock), client, caps) {
                    None => RateVerdict::Allow,
                    Some(retry_after) => RateVerdict::Limited { rule: i, retry_after },
                };
            }
        }
        RateVerdict::Allow
    }

    fn compiled(&self, clock: &Arc<dyn Clock>, service: ServiceId, rules: &[RateRule]) -> Arc<ServiceRules> {
        if let Some(s) = self.services.get(&service) {
            if s.same_list(rules) {
                return s.clone();
            }
        }
        // Compile under the entry lock: racers must not each get state of their own, which would
        // admit a burst per racer. Compiling touches no store and is rare.
        let mut slot = self.services.entry(service);
        let old = match &mut slot {
            Entry::Occupied(e) => Some(e.get().clone()),
            Entry::Vacant(_) => None,
        };
        if let Some(s) = old.as_ref().filter(|s| s.same_list(rules)) {
            return s.clone();
        }
        let keys = old
            .as_ref()
            .map_or_else(|| Arc::new(AtomicUsize::new(0)), |s| s.keys.clone());
        let compiled = rules
            .iter()
            .map(|r| {
                old.as_ref()
                    .and_then(|s| s.rules.iter().find(|o| o.content == *r).cloned())
                    .unwrap_or_else(|| Arc::new(self.compile(clock, r, &keys)))
            })
            .collect();
        let new = Arc::new(ServiceRules { rules: compiled, keys });
        match slot {
            Entry::Occupied(mut e) => {
                e.insert(new.clone());
            }
            Entry::Vacant(v) => {
                v.insert(new.clone());
            }
        }
        new
    }

    fn compile(&self, clock: &Arc<dyn Clock>, rule: &RateRule, keys: &Arc<AtomicUsize>) -> RuleState {
        let burst = if rule.burst == 0 { rule.rps } else { rule.burst };
        let limit = match rule.by {
            _ if rule.rps == 0 => Limit::Closed,
            // One shard, as `IngressBuckets`: requests are spread over server threads, not Ms.
            RateBy::Service => Limit::Bucket(ShardedBucket::new(
                clock.clone(),
                u64::from(rule.rps),
                u64::from(burst),
                1,
            )),
            RateBy::Ip => {
                let emission = (NANOS_PER_SEC / u64::from(rule.rps)).max(1);
                Limit::Cells {
                    emission,
                    tolerance: emission.saturating_mul(u64::from(burst)),
                    cells: DashMap::with_shard_amount(CELL_SHARDS),
                    overflow: AtomicU64::new(0),
                }
            }
        };
        RuleState {
            content: rule.clone(),
            // A rule stored before validation knew better: limit everything rather than nothing.
            pattern: PathPattern::parse(&rule.path).unwrap_or_else(|_| PathPattern::everything()),
            limit,
            global: self.keys.clone(),
            service: keys.clone(),
        }
    }

    /// Drop idle cells. Not for the request path: it walks every cell.
    pub fn prune(&self, clock: &Arc<dyn Clock>) {
        let now = self.now(clock);
        let idle = u64::try_from(IDLE.as_nanos()).unwrap_or(u64::MAX);
        let all: Vec<Arc<ServiceRules>> = self.services.iter().map(|s| s.value().clone()).collect();
        for s in all {
            for r in &s.rules {
                r.prune(now, idle);
            }
        }
    }

    /// Drop the state of services `keep` rejects (deleted ones, on every route refresh).
    pub fn retain_services(&self, keep: impl Fn(&ServiceId) -> bool) {
        self.services.retain(|s, _| keep(s));
    }

    /// Client cells held on this node.
    pub fn keys(&self) -> usize {
        self.keys.load(Ordering::Relaxed)
    }

    /// Services with rule state.
    pub fn services(&self) -> usize {
        self.services.len()
    }
}

#[cfg(test)]
mod caps_tests {
    use super::RateCaps;

    #[test]
    fn a_configured_cap_stays_within_the_memory_budget() {
        let budget = 96 * 100_000;
        assert_eq!(RateCaps::resolve(u64::MAX, budget).global, 100_000);
        assert_eq!(RateCaps::resolve(5000, budget).global, 5000);
    }
}
