//! Scheduler trace formatting (`RT_SCHEDTRACE=<ms>`).
//!
//! ```text
//! SCHED 1000ms: procs=4 deno=3 wasm=1 idle=1 spinning=0 grq=[deno:12 wasm:0 build:1] lrq=[3 0 5 | 0] bound=[2 0 1] waitrt=4 waitmem=0
//! ```

use std::fmt::Write as _;

use rt_types::{SchedSnapshot, TaskKind};

/// One-line trace of `snap`.
///
/// `lrq` lists local queue lengths per P, grouped by kind (deno | wasm | build) with `|`
/// between groups; `bound` lists bound task counts per M.
pub fn format_schedtrace(snap: &SchedSnapshot) -> String {
    let count = |k: TaskKind| snap.procs.iter().filter(|p| p.kind == k).count();
    let mut s = String::new();
    let _ = write!(
        s,
        "SCHED {}ms: procs={} deno={} wasm={} idle={} spinning={} grq=[deno:{} wasm:{} build:{}] lrq=[",
        snap.uptime_ms,
        snap.procs.len(),
        count(TaskKind::DENO),
        count(TaskKind::WASM),
        snap.idle,
        snap.spinning,
        snap.grq.deno,
        snap.grq.wasm,
        snap.grq.build,
    );
    let mut first_group = true;
    for kind in TaskKind::ALL {
        let lens: Vec<String> = snap
            .procs
            .iter()
            .filter(|p| p.kind == kind)
            .map(|p| p.lrq_len.to_string())
            .collect();
        if lens.is_empty() && kind == TaskKind::Build {
            continue;
        }
        if !first_group {
            s.push_str(" | ");
        }
        first_group = false;
        s.push_str(&lens.join(" "));
    }
    let bound: Vec<String> = snap.machines.iter().map(|m| m.bound_count.to_string()).collect();
    let _ = write!(
        s,
        "] bound=[{}] waitrt={} waitmem={}",
        bound.join(" "),
        snap.waiting_runtime,
        snap.waiting_memory
    );
    s
}

/// `RT_SCHEDDETAIL=1`: one extra line per P and per M.
pub fn format_schedtrace_detail(snap: &SchedSnapshot) -> Vec<String> {
    let mut out = Vec::with_capacity(snap.procs.len() + snap.machines.len());
    for p in &snap.procs {
        out.push(format!(
            "  P{} kind={} status={:?} lrq={} runnext={} schedtick={} m={} steals_in={} steals_out={} preempt={} rekinds={}",
            p.id.0,
            p.kind,
            p.status,
            p.lrq_len,
            p.runnext.map(|t| t.to_string()).unwrap_or_else(|| "-".into()),
            p.schedtick,
            p.machine.map(|m| m.0.to_string()).unwrap_or_else(|| "-".into()),
            p.steals_in,
            p.steals_out,
            p.preemptions,
            p.rekinds,
        ));
    }
    for m in &snap.machines {
        out.push(format!(
            "  M{} p={} bound={} spinning={} parked={}",
            m.id.0,
            m.proc.map(|p| p.0.to_string()).unwrap_or_else(|| "-".into()),
            m.bound_count,
            m.spinning,
            m.parked
        ));
    }
    out
}
