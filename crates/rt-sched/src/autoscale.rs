//! Per-service autoscaling (decisions D-A3, D-B2): pure decision
//! functions, so they can be unit-tested and replayed in simulation.
//!
//! An *instance* is one concurrently running copy of a service (a Deno isolate or a WASM
//! instance), so a service's concurrency limit **is** its instance target. The limit stays within
//! `[min_instances, max_instances]` (`min` and `max` of [`ScaleInput`]): `max` is the service's
//! `scaling.max_instances`, the plan's `max_instances` or the profile ceiling, and the plan's tenant
//! cap; `min` is the profile floor raised to `scaling.min_instances`.
//!
//! **Concurrency limit (AIMD with slow start, latency-gradient guard; up fast, down slowly).**
//! Every autoscaler tick (sysmon, `rebalance.interval`) each service's limit moves once:
//!
//! | Signal (first match wins) | New limit |
//! |---|---|
//! | memory pressure `Critical` | halve (of the used part, see below) |
//! | memory pressure `High`, mean latency > baseline × tolerance, or its kind's Ps saturated while it runs more than `start` | −25 % |
//! | tasks queued and the limit was reached | grow: double (slow start, below `ssthresh`, or when the requests waited longer than [`ScaleInput::wait_urgent`]), else `+√limit` |
//! | the arrival rate rises fast and the limit is at least [`BUSY_PCT`] % used | grow like the queue rule, before a queue forms; bounded by Little's law when the latency is known |
//! | no queue and under half the limit used, and the last change is older than [`SCALE_DOWN_COOLDOWN`] | decay 1/8 toward `start` |
//!
//! A decrease applies to `max(peak running, start)` (never raising the limit), so a service that
//! did not cause a node-wide signal settles at `start` less the cut instead of its floor; a CPU
//! cut only hits services running more than `start`. A decrease sets `ssthresh` (slow start ends
//! there). Queue growth never adds more than the queue asked for. The only decrease that waits is
//! the decay of an unused limit: it runs at most once per cooldown (and not within a cooldown of a
//! growth), so the limit follows load up in ticks and down in tens of seconds. Memory, latency and
//! CPU cuts protect the node and apply at once.
//!
//! **Arrival-rate trend.** Two time-based EWMAs of the arrival rate (requests per second): a fast
//! one ([`RATE_FAST_TAU`]) and a slow one ([`RATE_SLOW_TAU`]). The rate is *rising* when the fast
//! average is at least [`RISE_RATIO`] times the slow one and exceeds it by at least
//! [`RISE_MIN_DELTA`] requests per second (small rates are noise). A service that shows a rising
//! rate while its limit is nearly used grows before its queue does; with the latency known the new
//! limit does not exceed `rate × latency × 1.25` (Little's law), so a rate that fits the current
//! limit asks for nothing.
//!
//! The latency **baseline** is a moving average of the per-tick mean latency (grant → finish,
//! artifact waits excluded) that follows improvements fast (α = 0.25) and regressions slowly
//! (α = 0.02), so a lasting change of the workload (a slower upstream) becomes the new normal
//! within seconds while a sudden regression shrinks the limit. A latency is only "unhealthy" with
//! at least [`MIN_SAMPLES`] completions in the tick and more than [`LATENCY_SLACK`] above the
//! baseline (sub-millisecond services do not oscillate on noise).
//!
//! **Warm targets.** Demand is a decaying peak of running tasks (half-life [`DEMAND_HALF_LIFE`]);
//! `per_proc = ceil(demand × headroom / Ps of the kind)` (0 below half an instance), capped by
//! the profile, then by the node's warm memory share (lowest demand trimmed first); halved at
//! `High` pressure, zero at `Critical`; a service idle longer than its idle TTL gets no target
//! (scale to zero). `min_instances` is a **floor** under all of that: exactly `min` instances
//! node-wide (`min / Ps` per P, the first `min % Ps` Ps one more), not cut by the profile cap or
//! the idle TTL, so a service with `min_instances > 0` does not scale to zero. Floors together
//! take at most [`FLOOR_SHARE_NORMAL`] of the warm memory share (a quarter at `High`, where the
//! demand-driven targets are halved first); `Critical` releases everything.

use std::time::Duration;

use rt_mem::Pressure;

/// Completions a tick needs before its mean latency is trusted.
pub const MIN_SAMPLES: u32 = 3;
/// A latency within this much of the baseline is always healthy.
pub const LATENCY_SLACK: Duration = Duration::from_millis(2);
/// Half-life of the demand peak used for warm targets.
pub const DEMAND_HALF_LIFE: Duration = Duration::from_secs(15);
/// Baseline smoothing when latency improves / regresses.
const ALPHA_DOWN: f64 = 0.25;
const ALPHA_UP: f64 = 0.02;
/// An unused limit decays at most once per this long, and never within this long of any change.
pub const SCALE_DOWN_COOLDOWN: Duration = Duration::from_secs(10);
/// Time constants (seconds) of the fast and slow arrival-rate averages.
pub const RATE_FAST_TAU: f64 = 0.5;
pub const RATE_SLOW_TAU: f64 = 10.0;
/// The rate is rising when `fast >= RISE_RATIO × slow` and `fast − slow >= RISE_MIN_DELTA` (req/s).
pub const RISE_RATIO: f64 = 1.5;
pub const RISE_MIN_DELTA: f64 = 5.0;
/// A rising rate only grows a limit that is at least this much used (percent).
pub const BUSY_PCT: u32 = 75;
/// Headroom over Little's law (`rate × latency`) when sizing a trend step.
const LITTLE_HEADROOM: f64 = 1.25;

/// Why a limit moved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cause {
    /// Tasks waited and the limit was the bottleneck.
    Queue,
    /// Mean latency rose above the moving baseline × tolerance.
    Latency,
    /// Memory pressure `High` or `Critical`.
    Memory,
    /// The kind's Ps are saturated (p90 run-queue wait above the bound).
    Cpu,
    /// Mostly unused: decay toward `start`.
    Idle,
    /// The arrival rate rose fast while the limit was nearly used (grown before a queue formed).
    Trend,
    /// The bounds moved (service updated: `min_instances` / `max_instances`, plan) and the limit
    /// was brought back inside.
    Bounds,
}

impl Cause {
    /// Metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Cause::Queue => "queue",
            Cause::Latency => "latency",
            Cause::Memory => "memory",
            Cause::Cpu => "cpu",
            Cause::Idle => "idle",
            Cause::Trend => "trend",
            Cause::Bounds => "bounds",
        }
    }
}

/// What the autoscaler saw for one service during one tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScaleInput {
    /// Current limit.
    pub limit: u32,
    /// Floor and ceiling (already clamped: `min <= max`).
    pub min: u32,
    pub max: u32,
    /// Limit a service starts with (idle decay stops here).
    pub start: u32,
    /// Most tasks holding a run slot at once during the tick.
    pub peak_running: u32,
    /// Most tasks waiting in the service queue at once during the tick.
    pub peak_queued: u32,
    /// Mean latency of the tick's completions and how many there were.
    pub latency: Option<Duration>,
    pub samples: u32,
    /// `ScalingKnobs::latency_tolerance_pct`.
    pub tolerance_pct: u32,
    pub pressure: Pressure,
    /// The service's kind has saturated Ps.
    pub cpu_saturated: bool,
    /// Time since the previous step (zero for the first one).
    pub elapsed: Duration,
    /// Requests that arrived during the tick (admitted, queued or refused).
    pub arrivals: u32,
    /// Mean admission wait of the tasks granted a slot during the tick (`None`: none waited).
    pub mean_wait: Option<Duration>,
    /// A mean wait this long means the limit is far too low (a quarter of the queue timeout).
    pub wait_urgent: Duration,
}

/// Autoscaler memory of one service (only the autoscaler touches it).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ScaleState {
    /// Moving latency baseline.
    pub baseline: Option<Duration>,
    /// Slow-start threshold (0 = not set yet: slow start up to `max`).
    pub ssthresh: u32,
    /// Decaying peak of running tasks (warm-target demand).
    pub demand: f64,
    /// Smoothed arrival rate, requests per second: the fast and the slow average.
    pub rps_fast: f64,
    pub rps_slow: f64,
    /// Time since the limit last changed (`None`: it never did); the scale-down cooldown.
    pub since_change: Option<Duration>,
}

/// The new limit and why it moved (`None` = unchanged).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    pub limit: u32,
    pub cause: Option<Cause>,
}

/// Integer square root (floor).
fn isqrt(n: u32) -> u32 {
    (f64::from(n)).sqrt() as u32
}

/// Is `latency` healthy against `baseline`?
pub fn latency_healthy(latency: Duration, baseline: Duration, tolerance_pct: u32) -> bool {
    let bound = baseline.as_secs_f64() * f64::from(tolerance_pct.max(100)) / 100.0;
    latency.as_secs_f64() <= bound || latency <= baseline + LATENCY_SLACK
}

/// Fold this tick's arrivals into the two rate averages; returns whether the rate is rising and
/// the fast average (requests per second).
fn update_rate(st: &mut ScaleState, arrivals: u32, elapsed: Duration) -> (bool, f64) {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return (false, st.rps_fast);
    }
    let rate = f64::from(arrivals) / secs;
    let fast = st.rps_fast + (1.0 - (-secs / RATE_FAST_TAU).exp()) * (rate - st.rps_fast);
    let rising = fast >= RISE_RATIO * st.rps_slow && fast - st.rps_slow >= RISE_MIN_DELTA;
    st.rps_slow += (1.0 - (-secs / RATE_SLOW_TAU).exp()) * (rate - st.rps_slow);
    st.rps_fast = fast;
    (rising, fast)
}

/// One step of the limit (see the module doc), updating the baseline, the rates, the cooldown and
/// `ssthresh` in `st`.
pub fn decide(i: &ScaleInput, st: &mut ScaleState) -> Decision {
    let (min, max) = (i.min.min(i.max), i.max);
    let limit = i.limit.clamp(min, max);
    let trusted = if i.samples >= MIN_SAMPLES { i.latency } else { None };
    let healthy = match (trusted, st.baseline) {
        (Some(l), Some(b)) => latency_healthy(l, b, i.tolerance_pct),
        _ => true,
    };
    // Little's law needs the latency a request takes: this tick's, else the moving baseline.
    let service_time = trusted.or(st.baseline);
    if let Some(l) = trusted {
        st.baseline = Some(match st.baseline {
            None => l,
            Some(b) => {
                let alpha = if l < b { ALPHA_DOWN } else { ALPHA_UP };
                Duration::from_secs_f64(b.as_secs_f64() + alpha * (l.as_secs_f64() - b.as_secs_f64()))
            }
        });
    }
    let (rising, rate) = update_rate(st, i.arrivals, i.elapsed);
    st.since_change = st.since_change.map(|d| d.saturating_add(i.elapsed));
    let cooled = st.since_change.is_none_or(|d| d >= SCALE_DOWN_COOLDOWN);
    // A decrease cuts what is used, counting at least the service's `start` allowance: a service
    // that did not cause a node-wide signal keeps `start - cut` instead of falling to its floor.
    let start = i.start.clamp(min, max);
    let cut = |num: u32, den: u32| {
        let base = i.peak_running.max(start).max(1);
        limit.min(base.saturating_sub((base * num / den).max(1))).max(min)
    };
    // Growth step: double in slow start (or when requests wait far too long), else +sqrt(limit).
    let grow = |urgent: bool, st: &ScaleState| {
        let ssthresh = if st.ssthresh == 0 { max } else { st.ssthresh };
        if urgent || limit < ssthresh {
            limit.max(1)
        } else {
            isqrt(limit).max(1)
        }
    };
    let busy = i.peak_running.saturating_mul(100) >= limit.saturating_mul(BUSY_PCT);
    let (next, cause) = if i.pressure == Pressure::Critical {
        (cut(1, 2), Cause::Memory)
    } else if i.pressure == Pressure::High {
        (cut(1, 4), Cause::Memory)
    } else if !healthy {
        (cut(1, 4), Cause::Latency)
    } else if i.cpu_saturated && i.peak_running > start {
        // Only services running more than their start allowance load the saturated Ps.
        (cut(1, 4), Cause::Cpu)
    } else if i.peak_queued > 0 && i.peak_running >= limit {
        let urgent = i.mean_wait.is_some_and(|w| w >= i.wait_urgent);
        let step = grow(urgent, st);
        (
            limit.saturating_add(step.min(i.peak_queued).max(1)).min(max),
            Cause::Queue,
        )
    } else if rising && busy && i.peak_queued == 0 {
        // Early scale-up on a fast-rising arrival rate: no queue yet, so no queue bound; Little's
        // law (concurrent instances = rate x time in system) bounds it when the latency is known.
        let step = grow(false, st);
        let mut next = limit.saturating_add(step).min(max);
        if let Some(t) = service_time {
            let need = (rate * t.as_secs_f64() * LITTLE_HEADROOM).ceil();
            next = next.min((need.min(f64::from(u32::MAX)) as u32).max(limit));
        }
        (next, Cause::Trend)
    } else if i.peak_queued == 0 && i.peak_running.saturating_mul(2) < limit && limit > start && cooled {
        let floor = start.max(i.peak_running.saturating_mul(2)).clamp(min, max);
        (limit.saturating_sub((limit / 8).max(1)).max(floor), Cause::Idle)
    } else {
        (limit, if limit == i.limit { Cause::Idle } else { Cause::Bounds })
    };
    if next < limit {
        st.ssthresh = next.max(min);
    }
    let next = next.clamp(min, max);
    if next != i.limit {
        st.since_change = Some(Duration::ZERO);
    }
    Decision {
        limit: next,
        cause: (next != i.limit).then_some(cause),
    }
}

/// Decay `demand` by `elapsed` (half-life [`DEMAND_HALF_LIFE`]) and fold in the tick's `peak`.
pub fn update_demand(demand: f64, peak: u32, elapsed: Duration) -> f64 {
    let decay = 0.5f64.powf(elapsed.as_secs_f64() / DEMAND_HALF_LIFE.as_secs_f64());
    (demand * decay).max(f64::from(peak))
}

/// Below this many instances of (headroom-weighted) demand a service gets no warm target.
pub const WARM_MIN_INSTANCES: f64 = 0.5;

/// `ceil(demand × headroom / procs)`, capped by `max_per_proc`; 0 without Ps or when the
/// weighted demand is below [`WARM_MIN_INSTANCES`] (a decayed peak releases its instances
/// before the idle TTL).
pub fn warm_per_proc(demand: f64, headroom_pct: u32, procs: u32, max_per_proc: u32) -> u32 {
    if procs == 0 || !demand.is_finite() || demand * f64::from(headroom_pct) / 100.0 < WARM_MIN_INSTANCES {
        return 0;
    }
    let want = (demand * f64::from(headroom_pct) / 100.0 / f64::from(procs)).ceil();
    (want.min(f64::from(u32::MAX)) as u32).min(max_per_proc)
}

/// Share of the warm memory budget all `min_instances` floors together may take at `Normal`
/// pressure (the rest stays for demand-driven targets).
pub const FLOOR_SHARE_NORMAL: f64 = 0.5;
/// The same at `High` pressure: floors are scaled down too, after the demand-driven targets
/// (halved) and before `Critical` releases everything.
pub const FLOOR_SHARE_HIGH: f64 = 0.25;

/// How `min_instances` warm instances are spread over the Ps of the kind: `(base, extra)` where
/// every P keeps `base` and the first `extra` Ps one more, so exactly `min_instances` are kept
/// node-wide; `(0, 0)` without Ps.
pub fn floor_spread(min_instances: u32, procs: u32) -> (u32, u32) {
    match (min_instances.checked_div(procs), min_instances.checked_rem(procs)) {
        (Some(base), Some(extra)) => (base, extra),
        _ => (0, 0),
    }
}

/// One candidate warm target before the memory cap.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WarmWant {
    pub demand: f64,
    /// Wanted per P from demand (the floor is accounted separately).
    pub per_proc: u32,
    /// Instances kept node-wide for `scaling.min_instances` (spread by [`floor_spread`]).
    pub floor: u32,
    /// Ps of the artifact's kind.
    pub procs: u32,
    /// Estimated bytes of one warm instance.
    pub instance_bytes: u64,
}

impl WarmWant {
    /// Warm instances node-wide: the demand target or the floor, whichever is larger.
    pub fn instances(&self) -> u64 {
        (u64::from(self.per_proc) * u64::from(self.procs)).max(u64::from(self.floor))
    }

    /// `(base, extra)`: every P keeps `base`, the first `extra` Ps one more (see
    /// [`floor_spread`]); a demand target at or above the floor is uniform.
    pub fn spread(&self) -> (u32, u32) {
        if u64::from(self.per_proc) * u64::from(self.procs) >= u64::from(self.floor) {
            (self.per_proc, 0)
        } else {
            floor_spread(self.floor, self.procs)
        }
    }
}

/// Apply pressure and the memory budget to the wanted targets, in place. `Critical` zeroes
/// everything. Otherwise `High` halves the demand targets; floors are granted first, one instance
/// at a time round-robin by descending demand, within [`FLOOR_SHARE_NORMAL`] of `budget_bytes`
/// ([`FLOOR_SHARE_HIGH`] at `High`); then demand targets above the granted floors, by descending
/// demand (stable on input order), until `budget_bytes` is used up.
pub fn fit_warm(wants: &mut [WarmWant], pressure: Pressure, budget_bytes: u64) {
    let floor_share = match pressure {
        Pressure::Normal => FLOOR_SHARE_NORMAL,
        Pressure::High => FLOOR_SHARE_HIGH,
        Pressure::Critical => {
            for w in wants.iter_mut() {
                w.per_proc = 0;
                w.floor = 0;
            }
            return;
        }
    };
    for w in wants.iter_mut() {
        if pressure == Pressure::High {
            w.per_proc /= 2;
        }
        if w.procs == 0 {
            w.floor = 0;
        }
    }
    let bytes = |w: &WarmWant| w.instance_bytes.max(1);
    let mut order: Vec<usize> = (0..wants.len()).collect();
    order.sort_by(|&a, &b| {
        wants[b]
            .demand
            .partial_cmp(&wants[a].demand)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // floors: round-robin one instance each until the floor cap is spent
    let mut floor_left = (budget_bytes as f64 * floor_share) as u64;
    let wanted: Vec<u32> = wants.iter().map(|w| w.floor).collect();
    for w in wants.iter_mut() {
        w.floor = 0;
    }
    let mut progress = true;
    while progress {
        progress = false;
        for &i in &order {
            let w = &mut wants[i];
            if w.floor < wanted[i] && bytes(w) <= floor_left {
                floor_left -= bytes(w);
                w.floor += 1;
                progress = true;
            }
        }
    }
    let mut left = budget_bytes;
    for w in wants.iter() {
        left = left.saturating_sub(bytes(w).saturating_mul(u64::from(w.floor)));
    }
    // demand targets: what they add above the granted floor must fit what is left
    for i in order {
        let w = &mut wants[i];
        let affordable = u64::from(w.floor).saturating_add(left / bytes(w));
        let per_proc = if w.procs == 0 {
            0
        } else {
            u32::try_from(affordable / u64::from(w.procs))
                .unwrap_or(u32::MAX)
                .min(w.per_proc)
        };
        let added = (u64::from(per_proc) * u64::from(w.procs)).saturating_sub(u64::from(w.floor));
        left = left.saturating_sub(added.saturating_mul(bytes(w)));
        w.per_proc = per_proc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn input(limit: u32) -> ScaleInput {
        ScaleInput {
            limit,
            min: 2,
            max: 1024,
            start: 16,
            peak_running: limit,
            peak_queued: 0,
            latency: None,
            samples: 0,
            tolerance_pct: 200,
            pressure: Pressure::Normal,
            cpu_saturated: false,
            elapsed: ms(100),
            arrivals: 0,
            mean_wait: None,
            wait_urgent: Duration::from_secs(1),
        }
    }

    #[test]
    fn queued_demand_grows_the_limit_by_slow_start_then_additively() {
        let mut st = ScaleState::default();
        let i = ScaleInput {
            peak_queued: 1000,
            ..input(16)
        };
        assert_eq!(
            decide(&i, &mut st),
            Decision {
                limit: 32,
                cause: Some(Cause::Queue)
            },
            "slow start doubles"
        );
        st.ssthresh = 32;
        let i = ScaleInput {
            peak_queued: 1000,
            ..input(64)
        };
        assert_eq!(decide(&i, &mut st).limit, 72, "above ssthresh: +sqrt(64)");
        let i = ScaleInput {
            peak_queued: 3,
            ..input(16)
        };
        assert_eq!(
            decide(&i, &mut ScaleState::default()).limit,
            19,
            "never more than the queue asked for"
        );
        let i = ScaleInput {
            peak_queued: 5,
            peak_running: 10,
            ..input(16)
        };
        assert_eq!(
            decide(&i, &mut ScaleState::default()).cause,
            None,
            "a queue below the limit is transient: hold"
        );
        let i = ScaleInput {
            peak_queued: 5000,
            max: 20,
            ..input(16)
        };
        assert_eq!(
            decide(&i, &mut ScaleState::default()).limit,
            20,
            "bounded by the ceiling"
        );
    }

    #[test]
    fn memory_pressure_latency_and_cpu_shrink_and_set_ssthresh() {
        let mut st = ScaleState::default();
        let d = decide(
            &ScaleInput {
                pressure: Pressure::High,
                peak_queued: 50,
                ..input(64)
            },
            &mut st,
        );
        assert_eq!(
            d,
            Decision {
                limit: 48,
                cause: Some(Cause::Memory)
            },
            "High wins over queued demand"
        );
        assert_eq!(st.ssthresh, 48);
        let d = decide(
            &ScaleInput {
                pressure: Pressure::Critical,
                ..input(64)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 32);
        let d = decide(
            &ScaleInput {
                cpu_saturated: true,
                peak_queued: 9,
                ..input(40)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(
            d,
            Decision {
                limit: 30,
                cause: Some(Cause::Cpu)
            }
        );
        // the cut starts from what is used, but never below `start` (16): 4 running of a 64 limit
        let d = decide(
            &ScaleInput {
                pressure: Pressure::High,
                peak_running: 4,
                ..input(64)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 12);
        let d = decide(
            &ScaleInput {
                pressure: Pressure::Critical,
                ..input(2)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 2, "never below min");

        let mut st = ScaleState {
            baseline: Some(ms(10)),
            ..ScaleState::default()
        };
        let slow = ScaleInput {
            latency: Some(ms(30)),
            samples: 10,
            peak_queued: 9,
            ..input(40)
        };
        assert_eq!(
            decide(&slow, &mut st),
            Decision {
                limit: 30,
                cause: Some(Cause::Latency)
            }
        );
        assert!(
            st.baseline.unwrap() > ms(10) && st.baseline.unwrap() < ms(11),
            "the baseline follows regressions slowly"
        );
        let mut st = ScaleState {
            baseline: Some(ms(10)),
            ..ScaleState::default()
        };
        let few = ScaleInput {
            latency: Some(ms(30)),
            samples: 2,
            ..input(40)
        };
        assert_eq!(decide(&few, &mut st).cause, None, "too few samples to judge");
        let mut st = ScaleState {
            baseline: Some(Duration::from_micros(100)),
            ..ScaleState::default()
        };
        let noise = ScaleInput {
            latency: Some(Duration::from_micros(900)),
            samples: 50,
            ..input(40)
        };
        assert_eq!(decide(&noise, &mut st).cause, None, "within the absolute slack");
    }

    /// Review 6: node-wide signals must not push services that do not cause them to their floor.
    #[test]
    fn node_wide_signals_spare_idle_services_and_cpu_cuts_only_heavy_users() {
        let mut st = ScaleState::default();
        let mut limit = 64;
        for _ in 0..10 {
            let d = decide(
                &ScaleInput {
                    pressure: Pressure::Critical,
                    peak_running: 0,
                    ..input(limit)
                },
                &mut st,
            );
            limit = d.limit;
        }
        assert_eq!(
            limit, 8,
            "an idle service settles at start / 2 under Critical, not at its floor of 2"
        );
        let d = decide(
            &ScaleInput {
                cpu_saturated: true,
                peak_running: 10,
                ..input(16)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.cause, None, "within its start allowance a service is not the CPU hog");
        let d = decide(
            &ScaleInput {
                cpu_saturated: true,
                peak_running: 40,
                ..input(40)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(
            d,
            Decision {
                limit: 30,
                cause: Some(Cause::Cpu)
            }
        );
    }

    #[test]
    fn an_unused_limit_decays_toward_start_and_the_baseline_follows_improvements_fast() {
        let d = decide(
            &ScaleInput {
                peak_running: 3,
                ..input(128)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(
            d,
            Decision {
                limit: 112,
                cause: Some(Cause::Idle)
            }
        );
        let d = decide(
            &ScaleInput {
                peak_running: 3,
                ..input(17)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 16, "stops at start");
        let d = decide(
            &ScaleInput {
                peak_running: 0,
                ..input(16)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.cause, None);
        let mut st = ScaleState {
            baseline: Some(ms(40)),
            ..ScaleState::default()
        };
        decide(
            &ScaleInput {
                latency: Some(ms(8)),
                samples: 10,
                ..input(16)
            },
            &mut st,
        );
        let b = st.baseline.expect("baseline").as_secs_f64();
        assert!((b - 0.032).abs() < 1e-9, "40 ms + 0.25 x (8 - 40) ms = 32 ms, got {b}");
    }

    #[test]
    fn warm_targets_follow_demand_and_fit_the_budget() {
        assert_eq!(warm_per_proc(0.0, 125, 4, 4), 0);
        assert_eq!(
            warm_per_proc(0.01, 125, 4, 4),
            0,
            "near-zero demand keeps nothing (review 9)"
        );
        assert_eq!(
            warm_per_proc(0.4, 125, 4, 4),
            1,
            "half an instance of demand keeps one per P"
        );
        assert_eq!(warm_per_proc(10.0, 125, 4, 4), 4, "ceil(12.5 / 4) = 4");
        assert_eq!(warm_per_proc(100.0, 150, 4, 16), 16, "profile cap");
        assert_eq!(warm_per_proc(5.0, 100, 0, 16), 0, "no P of the kind");
        let d = update_demand(8.0, 2, DEMAND_HALF_LIFE);
        assert!((d - 4.0).abs() < 1e-9, "one half-life halves the peak: {d}");
        assert_eq!(update_demand(1.0, 6, ms(100)), 6.0, "a new peak wins");

        let w = |demand: f64, per_proc: u32| WarmWant {
            demand,
            per_proc,
            floor: 0,
            procs: 2,
            instance_bytes: 10,
        };
        let mut wants = [w(1.0, 3), w(9.0, 3), w(5.0, 3)];
        fit_warm(&mut wants, Pressure::Normal, 100);
        assert_eq!(
            wants.map(|w| w.per_proc),
            [0, 3, 2],
            "highest demand first: 60 + 40 bytes"
        );
        let mut wants = [w(1.0, 3), w(9.0, 3)];
        fit_warm(&mut wants, Pressure::High, 1000);
        assert_eq!(wants.map(|w| w.per_proc), [1, 1]);
        fit_warm(&mut wants, Pressure::Critical, 1000);
        assert_eq!(wants.map(|w| w.per_proc), [0, 0]);
    }

    // -----------------------------------------------------------------------------------------
    // D-B2: instance bounds, arrival-rate trend, queue wait, cooldown, warm floor
    // -----------------------------------------------------------------------------------------

    /// A service at steady `rps` (both smoothed rates there), as the state a long run leaves.
    fn steady(rps: f64) -> ScaleState {
        ScaleState {
            rps_fast: rps,
            rps_slow: rps,
            ..ScaleState::default()
        }
    }

    /// `arrivals` requests in the 100 ms tick of `input()`.
    fn burst(limit: u32, arrivals: u32) -> ScaleInput {
        ScaleInput {
            arrivals,
            peak_running: limit,
            ..input(limit)
        }
    }

    #[test]
    fn a_fast_rising_arrival_rate_grows_the_limit_before_any_queue_forms() {
        // 100 rps steady, then 400 rps in this tick (40 arrivals in 100 ms); the limit is 90 % busy
        let mut st = steady(100.0);
        let i = ScaleInput {
            peak_running: 14,
            ..burst(16, 40)
        };
        let d = decide(&i, &mut st);
        assert_eq!(
            d,
            Decision {
                limit: 32,
                cause: Some(Cause::Trend)
            },
            "no queue yet: grown early, slow start doubles"
        );
        assert!(
            st.rps_fast > 100.0 && st.rps_fast < 400.0,
            "the fast average follows: {}",
            st.rps_fast
        );
        assert!(
            st.rps_slow > 100.0 && st.rps_slow < st.rps_fast,
            "the slow one lags: {}",
            st.rps_slow
        );
    }

    #[test]
    fn the_trend_growth_is_bounded_by_littles_law_when_the_latency_is_known() {
        // the fast rate lands at ~154 rps (see the first test); at 100 ms per request that needs
        // ceil(154.4 x 0.1 x 1.25) = 20 instances, less than the doubling to 32
        let mut st = steady(100.0);
        st.baseline = Some(ms(100));
        let i = ScaleInput {
            latency: Some(ms(100)),
            samples: 10,
            ..burst(16, 40)
        };
        assert_eq!(
            decide(&i, &mut st),
            Decision {
                limit: 20,
                cause: Some(Cause::Trend)
            }
        );
        // at 20 ms the need is ceil(154.4 x 0.02 x 1.25) = 4, below the limit: nothing to grow
        let mut st = steady(100.0);
        st.baseline = Some(ms(20));
        let calm = ScaleInput {
            latency: Some(ms(20)),
            samples: 10,
            ..burst(16, 40)
        };
        assert_eq!(
            decide(&calm, &mut st).cause,
            None,
            "the estimated need is below the limit"
        );
    }

    #[test]
    fn a_steady_or_small_or_underused_rate_is_not_a_trend() {
        // steady 100 rps (10 arrivals per 100 ms), 90 % busy
        let d = decide(
            &ScaleInput {
                peak_running: 14,
                ..burst(16, 10)
            },
            &mut steady(100.0),
        );
        assert_eq!(d.cause, None, "steady");
        // rising but the limit is only half used
        let d = decide(
            &ScaleInput {
                peak_running: 8,
                ..burst(16, 40)
            },
            &mut steady(100.0),
        );
        assert_eq!(d.cause, None, "plenty of room");
        // 2 -> 4 rps: doubled, but a handful of requests are noise
        let d = decide(
            &ScaleInput {
                peak_running: 16,
                ..burst(16, 1)
            },
            &mut steady(4.0),
        );
        assert_eq!(d.cause, None, "below the absolute rise");
        let mut tiny = steady(2.0);
        let d = decide(
            &ScaleInput {
                peak_running: 16,
                elapsed: Duration::from_millis(250),
                ..burst(16, 1)
            },
            &mut tiny,
        );
        assert_eq!(d.cause, None, "4 rps against 2 rps is not a trend");
    }

    #[test]
    fn a_burst_after_idle_counts_as_rising() {
        // never saw traffic (both rates 0): 300 rps with the limit nearly full
        let d = decide(
            &ScaleInput {
                peak_running: 15,
                ..burst(16, 30)
            },
            &mut ScaleState::default(),
        );
        assert_eq!(d.cause, Some(Cause::Trend));
    }

    #[test]
    fn the_smoothed_rates_are_time_based_ewmas() {
        let mut st = ScaleState::default();
        // 200 rps for ten 100 ms ticks: the fast average (tau 1 s) is ahead of the slow one (10 s)
        for _ in 0..10 {
            decide(&burst(64, 20), &mut st);
        }
        assert!(
            st.rps_fast > st.rps_slow * 2.0,
            "fast {} slow {}",
            st.rps_fast,
            st.rps_slow
        );
        assert!(st.rps_fast < 200.0 && st.rps_slow < 200.0);
        // a tick with no time elapsed leaves them alone
        let before = (st.rps_fast, st.rps_slow);
        decide(
            &ScaleInput {
                elapsed: Duration::ZERO,
                ..burst(64, 20)
            },
            &mut st,
        );
        assert_eq!(before, (st.rps_fast, st.rps_slow));
    }

    /// AUTOS-004: ticks whose elapsed time jumps (a suspended host, a stepped sim clock: hours, or
    /// a nanosecond with many arrivals) keep the averages finite and the limit inside its bounds.
    #[test]
    fn autos_004_clock_jumps_keep_the_averages_finite() {
        let mut st = steady(100.0);
        for (elapsed, arrivals) in [
            (Duration::from_secs(6 * 3600), 0),
            (Duration::from_nanos(1), u32::MAX),
            (Duration::from_secs(u64::from(u32::MAX)), u32::MAX),
            (Duration::from_nanos(1), 0),
            (Duration::ZERO, u32::MAX),
        ] {
            let d = decide(
                &ScaleInput {
                    elapsed,
                    ..burst(64, arrivals)
                },
                &mut st,
            );
            assert!(
                st.rps_fast.is_finite() && st.rps_slow.is_finite(),
                "{elapsed:?}: {} {}",
                st.rps_fast,
                st.rps_slow
            );
            assert!(st.rps_fast >= 0.0 && st.rps_slow >= 0.0);
            assert!((2..=1024).contains(&d.limit), "{elapsed:?}: limit {}", d.limit);
        }
    }

    #[test]
    fn a_long_queue_wait_grows_the_limit_by_doubling_even_above_ssthresh() {
        let queued = ScaleInput {
            peak_queued: 500,
            ..input(64)
        };
        let mut st = ScaleState {
            ssthresh: 16,
            ..ScaleState::default()
        };
        assert_eq!(
            decide(&queued, &mut st).limit,
            72,
            "short waits: +sqrt(limit) above ssthresh"
        );
        let mut st = ScaleState {
            ssthresh: 16,
            ..ScaleState::default()
        };
        let waited = ScaleInput {
            mean_wait: Some(Duration::from_secs(2)),
            ..queued
        };
        assert_eq!(
            decide(&waited, &mut st),
            Decision {
                limit: 128,
                cause: Some(Cause::Queue)
            },
            "waits past a quarter of the queue timeout: scale up fast"
        );
        let short = ScaleInput {
            mean_wait: Some(ms(5)),
            ..queued
        };
        assert_eq!(
            decide(
                &short,
                &mut ScaleState {
                    ssthresh: 16,
                    ..ScaleState::default()
                }
            )
            .limit,
            72
        );
        // still bounded by the queue and by the ceiling
        let few = ScaleInput {
            peak_queued: 3,
            mean_wait: Some(Duration::from_secs(2)),
            ..input(64)
        };
        assert_eq!(decide(&few, &mut ScaleState::default()).limit, 67);
        let capped = ScaleInput { max: 100, ..waited };
        assert_eq!(decide(&capped, &mut ScaleState::default()).limit, 100);
    }

    #[test]
    fn scaling_down_is_slow_one_step_per_cooldown() {
        let idle = |limit: u32| ScaleInput {
            peak_running: 3,
            ..input(limit)
        };
        // fresh state (never changed): the first step is allowed
        let mut st = ScaleState::default();
        let d = decide(&idle(128), &mut st);
        assert_eq!(
            d,
            Decision {
                limit: 112,
                cause: Some(Cause::Idle)
            }
        );
        assert_eq!(st.since_change, Some(Duration::ZERO), "the step starts a cooldown");
        // the next ticks hold until the cooldown has run
        let mut limit = d.limit;
        let mut elapsed = Duration::ZERO;
        while elapsed + ms(100) < SCALE_DOWN_COOLDOWN {
            let d = decide(&idle(limit), &mut st);
            assert_eq!(d.cause, None, "{elapsed:?} into the cooldown");
            elapsed += ms(100);
        }
        let d = decide(
            &ScaleInput {
                elapsed: SCALE_DOWN_COOLDOWN,
                ..idle(limit)
            },
            &mut st,
        );
        assert_eq!(
            d,
            Decision {
                limit: 98,
                cause: Some(Cause::Idle)
            },
            "112 - 112/8"
        );
        limit = d.limit;
        assert_eq!(
            decide(&idle(limit), &mut st).cause,
            None,
            "and the cooldown starts over"
        );
    }

    #[test]
    fn growth_and_protective_cuts_are_not_held_by_the_scale_down_cooldown() {
        let mut st = ScaleState {
            since_change: Some(Duration::ZERO),
            ..ScaleState::default()
        };
        let grow = decide(
            &ScaleInput {
                peak_queued: 100,
                ..input(16)
            },
            &mut st,
        );
        assert_eq!(grow.limit, 32, "scale up right after a change");
        let mut st = ScaleState {
            since_change: Some(Duration::ZERO),
            ..ScaleState::default()
        };
        let cut = decide(
            &ScaleInput {
                pressure: Pressure::High,
                ..input(64)
            },
            &mut st,
        );
        assert_eq!(cut.cause, Some(Cause::Memory), "memory pressure cuts at once");
        // a decrease or an increase restarts the cooldown
        assert_eq!(st.since_change, Some(Duration::ZERO));
    }

    #[test]
    fn the_limit_stays_within_the_instance_bounds() {
        // never below min_instances (the floor): decay, cuts and memory pressure stop there
        let floor = |i: ScaleInput| ScaleInput { min: 6, start: 2, ..i };
        let d = decide(
            &floor(ScaleInput {
                peak_running: 0,
                ..input(7)
            }),
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 6, "decays to the floor, not below");
        let d = decide(
            &floor(ScaleInput {
                pressure: Pressure::Critical,
                ..input(8)
            }),
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 6);
        let d = decide(
            &floor(ScaleInput {
                cpu_saturated: true,
                peak_running: 20,
                ..input(30)
            }),
            &mut ScaleState::default(),
        );
        assert!(d.limit >= 6);
        // a limit below the floor (the service was updated) is raised by the next step
        let d = decide(&floor(input(3)), &mut ScaleState::default());
        assert_eq!(d.limit, 6, "clamped into the bounds");
        assert_eq!(
            d.cause,
            Some(Cause::Bounds),
            "no signal of its own: the bounds moved it"
        );
        // never above max_instances: queue pressure and trend growth stop at the cap
        let cap = |i: ScaleInput| ScaleInput { max: 12, ..i };
        let d = decide(
            &cap(ScaleInput {
                peak_queued: 100,
                ..input(10)
            }),
            &mut ScaleState::default(),
        );
        assert_eq!(d.limit, 12);
        let d = decide(
            &cap(ScaleInput {
                peak_running: 10,
                ..burst(10, 50)
            }),
            &mut steady(100.0),
        );
        assert_eq!(d.limit, 12);
        let d = decide(
            &cap(ScaleInput {
                peak_queued: 100,
                ..input(12)
            }),
            &mut ScaleState::default(),
        );
        assert_eq!(d, Decision { limit: 12, cause: None }, "at the cap: nothing to do");
    }

    #[test]
    fn a_warm_floor_spreads_exactly_min_instances_over_the_ps_of_the_kind() {
        assert_eq!(floor_spread(0, 4), (0, 0));
        assert_eq!(
            floor_spread(1, 32),
            (0, 1),
            "min 1 on 32 Ps: one instance node-wide, not 32"
        );
        assert_eq!(floor_spread(2, 4), (0, 2));
        assert_eq!(floor_spread(5, 4), (1, 1));
        assert_eq!(floor_spread(8, 4), (2, 0));
        assert_eq!(floor_spread(3, 1), (3, 0));
        assert_eq!(floor_spread(3, 0), (0, 0), "no P of the kind");
        let w = WarmWant {
            demand: 0.0,
            per_proc: 0,
            floor: 1,
            procs: 32,
            instance_bytes: 10,
        };
        assert_eq!((w.instances(), w.spread()), (1, (0, 1)));
        let w = WarmWant { per_proc: 1, ..w };
        assert_eq!(
            (w.instances(), w.spread()),
            (32, (1, 0)),
            "demand above the floor is uniform"
        );
    }

    #[test]
    fn floors_are_reserved_per_instance_and_capped_at_half_the_warm_budget() {
        // min 1 on 32 Ps reserves one instance, not 32
        let mut wants = [WarmWant {
            demand: 0.0,
            per_proc: 0,
            floor: 1,
            procs: 32,
            instance_bytes: 10,
        }];
        fit_warm(&mut wants, Pressure::Normal, 20);
        assert_eq!(wants[0].floor, 1);
        assert_eq!(wants[0].instances(), 1);
        // floors together take at most half the budget, round-robin by demand
        let w = |demand: f64, per_proc: u32, floor: u32| WarmWant {
            demand,
            per_proc,
            floor,
            procs: 2,
            instance_bytes: 10,
        };
        let mut wants = [w(1.0, 0, 4), w(2.0, 0, 4)];
        fit_warm(&mut wants, Pressure::Normal, 60);
        assert_eq!(
            wants.map(|w| w.floor),
            [1, 2],
            "30 B of floors: 3 instances, highest demand first"
        );
        let mut wants = [w(0.0, 1, 1)];
        fit_warm(&mut wants, Pressure::Normal, 0);
        assert_eq!(
            wants.map(|w| (w.per_proc, w.floor)),
            [(0, 0)],
            "no warm budget, no floor"
        );
    }

    #[test]
    fn the_floor_comes_first_then_demand_high_shrinks_both_critical_releases_all() {
        let w = |demand: f64, per_proc: u32, floor: u32| WarmWant {
            demand,
            per_proc,
            floor,
            procs: 2,
            instance_bytes: 10,
        };
        let mut wants = [w(9.0, 3, 0), w(0.0, 0, 2), w(5.0, 3, 0)];
        fit_warm(&mut wants, Pressure::Normal, 60);
        assert_eq!(
            wants.map(|w| w.instances()),
            [4, 2, 0],
            "20 B floor first, then 40 B: highest demand gets 2 per P"
        );
        // High halves demand first, and floors get a quarter of the budget
        let mut wants = [w(9.0, 4, 0), w(0.0, 0, 4)];
        fit_warm(&mut wants, Pressure::High, 1000);
        assert_eq!(wants.map(|w| (w.per_proc, w.floor)), [(2, 0), (0, 4)]);
        let mut wants = [w(0.0, 0, 4)];
        fit_warm(&mut wants, Pressure::High, 80);
        assert_eq!(wants[0].floor, 2, "High: floors within 25 % of the budget");
        let mut wants = [w(0.0, 0, 4)];
        fit_warm(&mut wants, Pressure::Normal, 80);
        assert_eq!(wants[0].floor, 4, "Normal: within 50 %");
        fit_warm(&mut wants, Pressure::Critical, 1000);
        assert_eq!(wants.map(|w| w.instances()), [0]);
    }
}
