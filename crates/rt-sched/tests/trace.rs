//! schedtrace (`RT_SCHEDTRACE=<ms>`), `Scheduler::snapshot()` and the event-trace recorder
//!.
//!
//! ACs: SCH-22.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use parking_lot::Mutex;
use rt_obs::{format_schedtrace, format_schedtrace_detail};
use rt_sched::trace::{PickSource, TraceEvent, TraceRecorder};
use rt_testkit::{io, script, Script, SimRuntime};
use rt_types::{
    DegradationMode, GrqLens, MachineId, MachineSnapshot, ProcId, ProcSnapshot, ProcStatus, SchedSnapshot, TaskId,
    TaskKind,
};

/// The text after `SCHED <n>ms: `.
fn body(line: &str) -> &str {
    line.split_once("ms: ")
        .unwrap_or_else(|| panic!("not a schedtrace line: {line}"))
        .1
}

/// The `n` of `key=n`.
fn number_after(line: &str, key: &str) -> u64 {
    let rest = line
        .split_once(key)
        .unwrap_or_else(|| panic!("`{key}` missing in {line}"))
        .1;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits
        .parse()
        .unwrap_or_else(|_| panic!("`{key}` is not a number in {line}"))
}

/// The numbers inside `key=[ ... ]` (ignoring `|`).
fn numbers_in_list(line: &str, key: &str) -> Vec<u64> {
    let rest = line
        .split_once(key)
        .unwrap_or_else(|| panic!("`{key}` missing in {line}"))
        .1;
    let inner = rest.split_once(']').expect("closing bracket").0;
    inner
        .split_whitespace()
        .filter(|t| *t != "|")
        .map(|t| t.parse().expect("number"))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// SCH-22 (simulation)
// ---------------------------------------------------------------------------------------------

/// SCH-22: `RT_SCHEDTRACE=1000`, 3 s pass -> 3 schedtrace lines with all fields; `snapshot()`
/// returns matching data.
#[test]
fn ac_sch_22_three_seconds_print_three_schedtrace_lines_sim() {
    let mut sim = SimRuntime::new(fixed(3, 1).schedtrace(ms(1000)));
    for i in 0..6u128 {
        let spec = if i % 2 == 0 {
            d(1 + i).finish()
        } else {
            w(1 + i).finish()
        };
        admitted(sim.submit(spec, script![io(100)]));
    }
    sim.run_for(ms(3_100));

    let lines = sim.schedtrace_lines();
    assert_eq!(lines.len(), 3, "one line per second: {lines:#?}");
    for (i, line) in lines.iter().enumerate() {
        assert_schedtrace_fields(line);
        assert!(line.starts_with(&format!("SCHED {}ms: ", (i + 1) * 1000)), "{line}");
    }

    // `snapshot()` matches: after the early work finished the node is idle and steady.
    let snap = sim.snapshot();
    let now = format_schedtrace(&snap);
    assert_eq!(
        body(&lines[2]),
        body(&now),
        "the last line and a fresh snapshot disagree"
    );
    assert_eq!(number_after(&lines[2], "procs="), snap.procs.len() as u64);
    assert_eq!(number_after(&lines[2], " deno="), 3);
    assert_eq!(number_after(&lines[2], " wasm="), 1);
    assert_eq!(numbers_in_list(&lines[2], "bound=[").len(), snap.machines.len());
}

#[test]
fn ac_sch_22_lines_reflect_waiting_and_bound_tasks() {
    let mut sim = SimRuntime::new(fixed(2, 1).schedtrace(ms(1000)).auto_ready(false));
    sim.source().make_ready(key_of(&d(1).finish()));
    // Two bound Deno tasks waiting 1.5 s on I/O, four tasks waiting for an artifact.
    admitted(sim.submit(d(1).finish(), script![io(1_500)]));
    admitted(sim.submit_existing(d(1).finish()));
    for _ in 0..4 {
        admitted(sim.submit(d(9).finish(), Script::noop()));
    }
    sim.run_for(ms(2_100));

    let lines = sim.schedtrace_lines();
    assert_eq!(lines.len(), 2);
    assert_eq!(number_after(&lines[0], "waitrt="), 4, "{}", lines[0]);
    assert_eq!(
        numbers_in_list(&lines[0], "bound=[").iter().sum::<u64>(),
        2,
        "{}",
        lines[0]
    );
    assert_eq!(number_after(&lines[0], "waitmem="), 0);
    assert_eq!(number_after(&lines[1], "waitrt="), 4, "builds never completed");
    assert_eq!(
        numbers_in_list(&lines[1], "bound=[").iter().sum::<u64>(),
        0,
        "the bound tasks finished at 1.5 s"
    );
}

#[test]
fn ac_sch_22_snapshot_describes_the_scheduler() {
    let mut sim = SimRuntime::new(fixed(2, 1));
    let snap = sim.snapshot();
    assert_eq!(snap.mode, DegradationMode::Normal);
    assert_eq!(snap.procs_of(TaskKind::DENO), 2);
    assert_eq!(snap.procs_of(TaskKind::WASM), 1);
    assert_eq!(snap.procs_of(TaskKind::Build), 1);
    assert_eq!((snap.inflight, snap.tasks_submitted, snap.tasks_completed), (0, 0, 0));

    let mut t = admitted(sim.submit(d(1).finish(), script![io(10)]));
    assert_eq!(sim.snapshot().tasks_submitted, 1);
    assert_eq!(sim.snapshot().inflight, 1);
    sim.run_until_idle();
    assert_ok(sim.outcome(&mut t));
    let snap = sim.snapshot();
    assert_eq!((snap.inflight, snap.tasks_submitted, snap.tasks_completed), (0, 1, 1));
    assert!(snap.uptime_ms >= 10, "uptime follows virtual time");
}

// ---------------------------------------------------------------------------------------------
// SCH-22 (real threads)
// ---------------------------------------------------------------------------------------------

#[test]
fn ac_sch_22_three_seconds_print_three_schedtrace_lines_real_threads() {
    let _serial = serial();
    let lines: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = lines.clone();
    let real = build_real_with(real_cfg(2, 2), move |b| {
        b.schedtrace(ms(1000), Arc::new(move |l: &str| sink.lock().push(l.to_string())))
    });

    std::thread::sleep(Duration::from_millis(3_500));
    let got = lines.lock().clone();
    assert_eq!(got.len(), 3, "one line per second over 3.5 s: {got:#?}");
    for (i, line) in got.iter().enumerate() {
        assert_schedtrace_fields(line);
        let at = number_after(line, "SCHED ") as i64;
        assert!(
            (at - (i as i64 + 1) * 1000).abs() <= 100,
            "line {i} printed at {at} ms: {line}"
        );
    }
    let snap = real.sched.snapshot();
    assert_eq!(number_after(&got[2], "procs="), snap.procs.len() as u64);
    assert_eq!(
        body(&got[2]),
        body(&format_schedtrace(&snap)),
        "an idle node's snapshot matches its schedtrace line"
    );
    real.shutdown();
}

// ---------------------------------------------------------------------------------------------
// Formatting (rt-obs) as consumed by schedtrace
// ---------------------------------------------------------------------------------------------

fn proc_snap(id: u32, kind: TaskKind, lrq: u32) -> ProcSnapshot {
    ProcSnapshot {
        id: ProcId(id),
        kind,
        status: ProcStatus::Running,
        lrq_len: lrq,
        runnext: None,
        schedtick: 0,
        machine: Some(MachineId(id)),
        busy_ns: 0,
        steals_in: 0,
        steals_out: 0,
        preemptions: 0,
        rekinds: 0,
    }
}

fn machine_snap(id: u32, bound: u32) -> MachineSnapshot {
    MachineSnapshot {
        id: MachineId(id),
        proc: Some(ProcId(id)),
        bound_count: bound,
        spinning: false,
        parked: false,
    }
}

fn docs_snapshot() -> SchedSnapshot {
    SchedSnapshot {
        uptime_ms: 1000,
        mode: DegradationMode::Normal,
        procs: vec![
            proc_snap(0, TaskKind::DENO, 3),
            proc_snap(1, TaskKind::DENO, 0),
            proc_snap(2, TaskKind::DENO, 5),
            proc_snap(3, TaskKind::WASM, 0),
        ],
        machines: vec![machine_snap(0, 2), machine_snap(1, 0), machine_snap(2, 1)],
        grq: GrqLens {
            deno: 12,
            wasm: 0,
            build: 1,
        },
        idle: 1,
        spinning: 0,
        waiting_runtime: 4,
        waiting_memory: 0,
        inflight: 0,
        tasks_submitted: 0,
        tasks_completed: 0,
    }
}

#[test]
fn schedtrace_line_matches_the_documented_example() {
    assert_eq!(
        format_schedtrace(&docs_snapshot()),
        "SCHED 1000ms: procs=4 deno=3 wasm=1 idle=1 spinning=0 grq=[deno:12 wasm:0 build:1] lrq=[3 0 5 | 0] bound=[2 0 1] waitrt=4 waitmem=0"
    );
}

#[test]
fn schedtrace_line_lists_build_processors_as_a_third_lrq_group() {
    let mut snap = docs_snapshot();
    snap.procs.push(proc_snap(4, TaskKind::Build, 2));
    let line = format_schedtrace(&snap);
    assert_schedtrace_fields(&line);
    assert!(line.contains("lrq=[3 0 5 | 0 | 2]"), "{line}");
    assert!(line.contains("procs=5"), "{line}");
}

#[test]
fn schedtrace_detail_has_one_line_per_p_and_per_m() {
    let snap = docs_snapshot();
    let detail = format_schedtrace_detail(&snap);
    assert_eq!(detail.len(), snap.procs.len() + snap.machines.len());
    assert!(
        detail[0].contains("P0") && detail[0].contains("kind=deno") && detail[0].contains("lrq=3"),
        "{}",
        detail[0]
    );
    assert!(
        detail[4].contains("M0") && detail[4].contains("bound=2"),
        "{}",
        detail[4]
    );
}

// ---------------------------------------------------------------------------------------------
// TraceRecorder
// ---------------------------------------------------------------------------------------------

fn fnv1a(events: &[TraceEvent]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for ev in events {
        for b in format!("{ev:?}").bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h
}

fn sample_events() -> Vec<TraceEvent> {
    let t = TaskId(1);
    vec![
        TraceEvent::Submit {
            task: t,
            kind: TaskKind::DENO,
        },
        TraceEvent::Place {
            task: t,
            proc: Some(ProcId(0)),
            grq: false,
            runnext: false,
        },
        TraceEvent::Pick {
            task: t,
            proc: ProcId(0),
            machine: MachineId(0),
            from: PickSource::Lrq,
        },
        TraceEvent::Start {
            task: t,
            machine: MachineId(0),
        },
        TraceEvent::Finish { task: t },
    ]
}

#[test]
fn recorder_hash_is_fnv1a_of_the_debug_encoding() {
    let empty = TraceRecorder::new(false);
    assert_eq!(
        empty.hash(),
        0xcbf2_9ce4_8422_2325,
        "FNV offset basis for an empty trace"
    );
    let mut rec = TraceRecorder::new(false);
    let events = sample_events();
    for e in &events {
        rec.record(*e);
    }
    assert_eq!(rec.hash(), fnv1a(&events));
}

#[test]
fn recorder_hash_depends_on_event_order_and_content() {
    let events = sample_events();
    let hash = |evs: &[TraceEvent]| {
        let mut r = TraceRecorder::new(false);
        evs.iter().for_each(|e| r.record(*e));
        r.hash()
    };
    let mut swapped = events.clone();
    swapped.swap(0, 1);
    assert_ne!(hash(&events), hash(&swapped));
    assert_ne!(hash(&events), hash(&events[..4]));
    assert_eq!(hash(&events), hash(&events.clone()));
}

#[test]
fn recorder_keeps_events_only_when_asked() {
    let events = sample_events();
    let mut keep = TraceRecorder::new(true);
    let mut hash_only = TraceRecorder::new(false);
    for e in &events {
        keep.record(*e);
        hash_only.record(*e);
    }
    assert_eq!(keep.events(), events.as_slice());
    assert!(hash_only.events().is_empty());
    assert_eq!(keep.hash(), hash_only.hash());
}
