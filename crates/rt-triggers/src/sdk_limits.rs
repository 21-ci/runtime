//! Per-caller-tenant caps on operator SDK calls (plan SDK-14).
//!
//! An SDK call runs as a task of the `sys` tenant, so the scheduler counts it against `sys`, not
//! against the tenant that made it. Without a cap of its own, one tenant could fill the SDK
//! services for every other tenant. Each caller tenant therefore gets at most its plan's
//! `max_concurrency` SDK calls in flight and its plan's `rps` SDK calls per second (burst = `rps`);
//! `0` means no cap, as in the plan. A refused call is `sdk_rate_limited`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rt_sched::Clock;
use rt_types::{Plan, TenantId};

use crate::ratelimit::ShardedBucket;

/// Tenants tracked before idle entries (nothing in flight) are dropped. Tenants are created by
/// the operator, so this only bounds what deleted tenants leave behind.
pub const MAX_TRACKED_SDK_TENANTS: usize = 10_000;

struct Gate {
    inflight: AtomicU32,
    rps: AtomicU32,
    bucket: ShardedBucket,
}

/// What a caller tenant holds while its SDK call runs.
pub struct SdkPermit {
    gate: Arc<Gate>,
}

impl Drop for SdkPermit {
    fn drop(&mut self) {
        self.gate.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Why an SDK call was refused before it started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkRefusal {
    /// `plan.max_concurrency` SDK calls already in flight.
    Concurrency,
    /// More than `plan.rps` SDK calls in the last second.
    Rate,
}

/// The SDK call gates of every caller tenant.
pub struct SdkCallerLimits {
    clock: Arc<dyn Clock>,
    gates: DashMap<TenantId, Arc<Gate>>,
}

impl SdkCallerLimits {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            gates: DashMap::new(),
        }
    }

    /// Tenants with a gate (for tests of the eviction).
    pub fn tracked(&self) -> usize {
        self.gates.len()
    }

    fn gate(&self, tenant: TenantId, rps: u32) -> Arc<Gate> {
        if let Some(g) = self.gates.get(&tenant) {
            if g.rps.swap(rps, Ordering::AcqRel) != rps {
                g.bucket.set_limit(u64::from(rps), u64::from(rps.max(1)));
            }
            return g.clone();
        }
        if self.gates.len() >= MAX_TRACKED_SDK_TENANTS {
            self.gates.retain(|_, g| g.inflight.load(Ordering::Acquire) > 0);
        }
        self.gates
            .entry(tenant)
            .or_insert_with(|| {
                Arc::new(Gate {
                    inflight: AtomicU32::new(0),
                    rps: AtomicU32::new(rps),
                    bucket: ShardedBucket::new(self.clock.clone(), u64::from(rps), u64::from(rps.max(1)), 1),
                })
            })
            .clone()
    }

    /// Admit one SDK call of `tenant` under `plan`, or say why not.
    pub fn admit(&self, tenant: TenantId, plan: &Plan) -> Result<SdkPermit, SdkRefusal> {
        let gate = self.gate(tenant, plan.rps);
        let max = plan.max_concurrency;
        let prev = gate.inflight.fetch_add(1, Ordering::AcqRel);
        let permit = SdkPermit { gate };
        if max > 0 && prev >= max {
            return Err(SdkRefusal::Concurrency);
        }
        if plan.rps > 0 && !permit.gate.bucket.try_acquire(0, 1) {
            return Err(SdkRefusal::Rate);
        }
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_sched::SystemClock;

    fn plan(max_concurrency: u32, rps: u32) -> Plan {
        Plan {
            max_concurrency,
            rps,
            ..Plan::default()
        }
    }

    #[test]
    fn in_flight_and_per_second_caps_per_tenant() {
        let l = SdkCallerLimits::new(Arc::new(SystemClock));
        let (a, b) = (TenantId::from_u128(1), TenantId::from_u128(2));
        let held: Vec<_> = (0..3)
            .map(|_| l.admit(a, &plan(3, 0)).expect("within the cap"))
            .collect();
        assert_eq!(l.admit(a, &plan(3, 0)).err(), Some(SdkRefusal::Concurrency));
        assert!(l.admit(b, &plan(3, 0)).is_ok(), "another tenant has its own cap");
        drop(held);
        assert!(l.admit(a, &plan(3, 0)).is_ok(), "released on drop");

        let c = TenantId::from_u128(3);
        let ok = (0..10).filter(|_| l.admit(c, &plan(0, 4)).is_ok()).count();
        assert_eq!(ok, 4, "burst = rps");
        assert!(l.admit(c, &plan(0, 0)).is_ok(), "0 = no cap");
    }

    #[test]
    fn idle_tenants_are_dropped_past_the_cap() {
        let l = SdkCallerLimits::new(Arc::new(SystemClock));
        let busy = l.admit(TenantId::from_u128(u128::MAX), &plan(0, 0)).expect("admit");
        for n in 0..MAX_TRACKED_SDK_TENANTS as u128 {
            drop(l.admit(TenantId::from_u128(n + 1), &plan(0, 0)));
        }
        assert!(l.tracked() <= MAX_TRACKED_SDK_TENANTS + 1, "{}", l.tracked());
        assert!(
            l.gates.contains_key(&TenantId::from_u128(u128::MAX)),
            "a tenant with a call in flight stays"
        );
        drop(busy);
    }
}
