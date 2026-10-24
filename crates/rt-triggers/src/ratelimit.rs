//! Sharded token buckets (RP-06).
//!
//! One bucket (global, tenant or service) is split into `shards` shards, one per Machine. Each
//! shard refills `rate_per_sec / shards` tokens per second and normally holds at most
//! `ceil(burst / shards)` tokens. [`ShardedBucket::try_acquire`] only locks the caller's shard (an
//! uncontended mutex: no shared atomic on the hot path). [`ShardedBucket::rebalance`] (sysmon,
//! every 100 ms) pools the tokens of idle shards and the refill they could not store, and hands
//! them to the shards that ran dry, so skewed load still sees ≈ the configured total rate.
//!
//! Tokens are fixed-point (1 token = 1e6 micro-tokens) so refills are exact integer math.
//! Time is read through the injected [`Clock`] (virtual in tests).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rt_sched::Clock;

const MICRO: u128 = 1_000_000;

struct ShardState {
    /// Micro-tokens currently held.
    tokens: u128,
    /// Refill that did not fit into the shard (micro-tokens), reclaimed by `rebalance`.
    spill: u128,
    /// Last refill, nanoseconds since the bucket's base instant.
    last_ns: u128,
    /// Tokens requested (granted or not) since the last rebalance.
    demand: u128,
    /// Limit generation this shard last saw.
    epoch: u64,
}

/// Cache-line aligned so shards of different Ms never share a line.
#[repr(align(64))]
struct Shard {
    state: Mutex<ShardState>,
}

/// A token bucket sharded per Machine.
pub struct ShardedBucket {
    clock: Arc<dyn Clock>,
    base: Instant,
    shards: Box<[Shard]>,
    rate: AtomicU64,
    burst: AtomicU64,
    epoch: AtomicU64,
}

impl ShardedBucket {
    /// `rate_per_sec` tokens per second in total, `burst` tokens of capacity in total.
    /// `shards` ≥ 1 (the number of Ms).
    pub fn new(clock: Arc<dyn Clock>, rate_per_sec: u64, burst: u64, shards: usize) -> Self {
        let n = shards.max(1);
        let base = clock.now();
        let cap = Self::cap_micro_for(burst, n);
        let shards: Vec<Shard> = (0..n)
            .map(|_| Shard {
                state: Mutex::new(ShardState {
                    tokens: cap,
                    spill: 0,
                    last_ns: 0,
                    demand: 0,
                    epoch: 0,
                }),
            })
            .collect();
        Self {
            clock,
            base,
            shards: shards.into_boxed_slice(),
            rate: AtomicU64::new(rate_per_sec),
            burst: AtomicU64::new(burst),
            epoch: AtomicU64::new(0),
        }
    }

    /// Per-shard capacity: `ceil(burst / shards)`, at least one token.
    fn cap_micro_for(burst: u64, shards: usize) -> u128 {
        let n = shards as u128;
        let per = u128::from(burst).div_ceil(n);
        per.max(1) * MICRO
    }

    fn cap_micro(&self) -> u128 {
        Self::cap_micro_for(self.burst.load(Ordering::Relaxed), self.shards.len())
    }

    fn now_ns(&self) -> u128 {
        self.clock.now().saturating_duration_since(self.base).as_nanos()
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    /// Bring one shard up to `now_ns`. Refill that does not fit goes to `spill`.
    fn refill(&self, st: &mut ShardState, now_ns: u128, cap: u128) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        if st.epoch != epoch {
            st.epoch = epoch;
            st.tokens = st.tokens.min(cap);
            st.spill = 0;
        }
        if now_ns <= st.last_ns {
            return;
        }
        let rate = u128::from(self.rate.load(Ordering::Relaxed));
        let n = self.shards.len() as u128;
        // micro-tokens = ns × (rate / n) / 1e9 × 1e6 = ns × rate / (n × 1000)
        let add = (now_ns - st.last_ns) * rate / (n * 1000);
        if add == 0 {
            // Keep the elapsed time so sub-micro-token remainders accumulate.
            return;
        }
        st.last_ns = now_ns;
        if st.tokens >= cap {
            st.spill += add;
        } else {
            let room = cap - st.tokens;
            if add <= room {
                st.tokens += add;
            } else {
                st.tokens = cap;
                st.spill += add - room;
            }
        }
    }

    /// Take `n` tokens from `shard` (modulo `shards`). `false` = rate limited (no tokens taken).
    pub fn try_acquire(&self, shard: usize, n: u64) -> bool {
        let shard = &self.shards[shard % self.shards.len()];
        let need = u128::from(n) * MICRO;
        let now_ns = self.now_ns();
        let cap = self.cap_micro();
        let mut st = shard.state.lock();
        self.refill(&mut st, now_ns, cap);
        st.demand = st.demand.saturating_add(need);
        if st.tokens >= need {
            st.tokens -= need;
            true
        } else {
            false
        }
    }

    /// Redistribute unused tokens between shards (called by sysmon every 100 ms).
    pub fn rebalance(&self) {
        let now_ns = self.now_ns();
        let cap = self.cap_micro();
        let burst_total = u128::from(self.burst.load(Ordering::Relaxed)) * MICRO;
        let n = self.shards.len();

        // Only sysmon locks several shards at once, always in index order.
        let mut guards: Vec<_> = self.shards.iter().map(|s| s.state.lock()).collect();
        for g in guards.iter_mut() {
            self.refill(g, now_ns, cap);
        }
        let held: u128 = guards.iter().map(|g| g.tokens).sum();
        let spill: u128 = guards.iter().map(|g| g.spill).sum();
        // The whole bucket never holds more than `burst`.
        let total = (held + spill).min(burst_total.max(cap * n as u128));
        let demand: u128 = guards.iter().map(|g| g.demand).sum();

        if demand == 0 {
            // Idle: even split (capped; the cap times the shard count is ≥ burst).
            let each = (total / n as u128).min(cap);
            for g in guards.iter_mut() {
                g.tokens = each;
                g.spill = 0;
            }
            return;
        }

        // Every shard keeps a small reserve (so a suddenly busy shard can serve its first
        // requests); the rest follows the demand seen since the last rebalance.
        let reserve = (cap / 10).min(total / n as u128);
        let spare = total - reserve * n as u128;
        for g in guards.iter_mut() {
            g.tokens = reserve + spare * g.demand / demand;
            g.spill = 0;
            g.demand = 0;
        }
    }

    /// Change the limits live (`PUT /v1/node/config` rate-limit tweaks, plan reload).
    pub fn set_limit(&self, rate_per_sec: u64, burst: u64) {
        self.rate.store(rate_per_sec, Ordering::Relaxed);
        self.burst.store(burst, Ordering::Relaxed);
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Time until at least one token is available somewhere (`retry-after` of a `429`).
    pub fn retry_after(&self) -> Duration {
        let now_ns = self.now_ns();
        let cap = self.cap_micro();
        let rate = u128::from(self.rate.load(Ordering::Relaxed));
        let n = self.shards.len() as u128;
        let mut best: Option<u128> = None;
        for s in self.shards.iter() {
            let mut st = s.state.lock();
            self.refill(&mut st, now_ns, cap);
            if st.tokens >= MICRO {
                return Duration::ZERO;
            }
            if rate == 0 {
                continue;
            }
            let missing = MICRO - st.tokens;
            // ns = missing × n × 1000 / rate, rounded up.
            let ns = (missing * n * 1000).div_ceil(rate);
            best = Some(best.map_or(ns, |b| b.min(ns)));
        }
        match best {
            Some(ns) => Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX)),
            // A zero rate never refills: ask the client to come back in a second.
            None => Duration::from_secs(1),
        }
    }
}
