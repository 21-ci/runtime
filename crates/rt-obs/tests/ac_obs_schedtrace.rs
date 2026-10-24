//! `RT_SCHEDTRACE` / `RT_SCHEDDETAIL` line formats (SCH-22):
//!
//! ```text
//! SCHED 1000ms: procs=4 deno=3 wasm=1 idle=1 spinning=0 grq=[deno:12 wasm:0 build:1] lrq=[3 0 5 | 0] bound=[2 0 1] waitrt=4 waitmem=0
//! ```
//!
//! The formatter is real code; these tests run green today and pin the format.

use rt_obs::{format_schedtrace, format_schedtrace_detail};
use rt_types::{
    DegradationMode, GrqLens, MachineId, MachineSnapshot, ProcId, ProcSnapshot, ProcStatus, SchedSnapshot, TaskId,
    TaskKind,
};

/// The example line of docs/deploy.md, verbatim.
const DOC_LINE: &str = "SCHED 1000ms: procs=4 deno=3 wasm=1 idle=1 spinning=0 grq=[deno:12 wasm:0 build:1] lrq=[3 0 5 | 0] bound=[2 0 1] waitrt=4 waitmem=0";

fn proc(id: u32, kind: TaskKind, lrq: u32) -> ProcSnapshot {
    ProcSnapshot {
        id: ProcId(id),
        kind,
        status: ProcStatus::Running,
        lrq_len: lrq,
        runnext: None,
        schedtick: 0,
        machine: None,
        busy_ns: 0,
        steals_in: 0,
        steals_out: 0,
        preemptions: 0,
        rekinds: 0,
    }
}

fn machine(id: u32, bound: u32) -> MachineSnapshot {
    MachineSnapshot {
        id: MachineId(id),
        proc: None,
        bound_count: bound,
        spinning: false,
        parked: false,
    }
}

/// The snapshot behind the documented example line.
fn doc_snapshot() -> SchedSnapshot {
    SchedSnapshot {
        uptime_ms: 1000,
        procs: vec![
            proc(0, TaskKind::DENO, 3),
            proc(1, TaskKind::DENO, 0),
            proc(2, TaskKind::DENO, 5),
            proc(3, TaskKind::WASM, 0),
        ],
        machines: vec![machine(0, 2), machine(1, 0), machine(2, 1)],
        grq: GrqLens {
            deno: 12,
            wasm: 0,
            build: 1,
        },
        idle: 1,
        spinning: 0,
        waiting_runtime: 4,
        waiting_memory: 0,
        ..SchedSnapshot::default()
    }
}

#[test]
fn the_documented_example_is_reproduced_byte_for_byte() {
    assert_eq!(format_schedtrace(&doc_snapshot()), DOC_LINE);
}

#[test]
fn the_example_in_the_docs_is_still_this_line() {
    // guards the guard: the constant above must be the line printed in docs/deploy.md
    const DOC: &str = include_str!("../../../docs/deploy.md");
    assert!(
        DOC.contains(DOC_LINE),
        "docs/deploy.md no longer contains the schedtrace example; update DOC_LINE and the formatter together"
    );
}

#[test]
fn sch_22_the_line_carries_every_field_once_in_the_documented_order() {
    let line = format_schedtrace(&doc_snapshot());
    assert!(!line.contains('\n'), "one line per tick");
    assert!(!line.ends_with(' ') && !line.starts_with(' '));
    let keys = [
        "SCHED ",
        " procs=",
        " deno=",
        " wasm=",
        " idle=",
        " spinning=",
        " grq=[",
        " lrq=[",
        " bound=[",
        " waitrt=",
        " waitmem=",
    ];
    let mut at = 0;
    for k in keys {
        let found = line[at..]
            .find(k)
            .unwrap_or_else(|| panic!("`{k}` missing or out of order in `{line}`"));
        assert_eq!(line.matches(k).count(), 1, "`{k}` must appear exactly once in `{line}`");
        at += found + k.len();
    }
    assert!(line.starts_with("SCHED 1000ms:"));
}

#[test]
fn values_come_from_the_snapshot_not_from_constants() {
    let snap = SchedSnapshot {
        uptime_ms: 3_000,
        procs: vec![
            proc(0, TaskKind::DENO, 7),
            proc(1, TaskKind::DENO, 1),
            proc(2, TaskKind::WASM, 2),
            proc(3, TaskKind::WASM, 4),
            proc(4, TaskKind::WASM, 6),
        ],
        machines: vec![machine(0, 9), machine(1, 8)],
        grq: GrqLens {
            deno: 1,
            wasm: 22,
            build: 333,
        },
        idle: 2,
        spinning: 3,
        waiting_runtime: 40,
        waiting_memory: 5,
        ..SchedSnapshot::default()
    };
    assert_eq!(
        format_schedtrace(&snap),
        "SCHED 3000ms: procs=5 deno=2 wasm=3 idle=2 spinning=3 grq=[deno:1 wasm:22 build:333] lrq=[7 1 | 2 4 6] bound=[9 8] waitrt=40 waitmem=5"
    );
}

#[test]
fn procs_count_equals_deno_plus_wasm_plus_build() {
    let snap = SchedSnapshot {
        procs: vec![
            proc(0, TaskKind::DENO, 0),
            proc(1, TaskKind::WASM, 0),
            proc(2, TaskKind::Build, 0),
            proc(3, TaskKind::Build, 0),
        ],
        ..SchedSnapshot::default()
    };
    let line = format_schedtrace(&snap);
    assert!(line.contains("procs=4 deno=1 wasm=1"), "{line}");
}

#[test]
fn build_processors_get_a_third_lrq_group_only_when_they_exist() {
    let mut snap = doc_snapshot();
    assert!(
        format_schedtrace(&snap).contains("lrq=[3 0 5 | 0]"),
        "no build group without build processors"
    );
    snap.procs.push(proc(4, TaskKind::Build, 2));
    assert!(
        format_schedtrace(&snap).contains("lrq=[3 0 5 | 0 | 2]"),
        "{}",
        format_schedtrace(&snap)
    );
}

#[test]
fn lrq_lengths_follow_processor_order_within_a_kind() {
    let snap = SchedSnapshot {
        procs: vec![
            proc(0, TaskKind::DENO, 9),
            proc(1, TaskKind::WASM, 8),
            proc(2, TaskKind::DENO, 7),
            proc(3, TaskKind::WASM, 6),
        ],
        ..SchedSnapshot::default()
    };
    let line = format_schedtrace(&snap);
    assert!(line.contains("lrq=[9 7 | 8 6]"), "{line}");
}

#[test]
fn bound_counts_are_listed_per_machine() {
    let mut snap = doc_snapshot();
    snap.machines = vec![machine(0, 0), machine(1, 0), machine(2, 0), machine(3, 17)];
    assert!(format_schedtrace(&snap).contains("bound=[0 0 0 17]"));
    snap.machines.clear();
    assert!(format_schedtrace(&snap).contains("bound=[]"));
}

#[test]
fn uptime_is_printed_in_milliseconds() {
    for ms in [0u64, 1, 999, 1_000, 61_000, 86_400_000] {
        let snap = SchedSnapshot {
            uptime_ms: ms,
            ..doc_snapshot()
        };
        assert!(
            format_schedtrace(&snap).starts_with(&format!("SCHED {ms}ms: ")),
            "uptime {ms}"
        );
    }
}

#[test]
fn the_trace_is_deterministic() {
    let snap = doc_snapshot();
    assert_eq!(format_schedtrace(&snap), format_schedtrace(&snap.clone()));
}

#[test]
fn the_trace_stays_one_line_for_a_large_node() {
    let procs: Vec<ProcSnapshot> = (0..64)
        .map(|i| proc(i, if i % 4 == 0 { TaskKind::WASM } else { TaskKind::DENO }, i))
        .collect();
    let snap = SchedSnapshot {
        procs,
        machines: (0..70).map(|i| machine(i, i % 3)).collect(),
        ..SchedSnapshot::default()
    };
    let line = format_schedtrace(&snap);
    assert!(!line.contains('\n'));
    assert!(line.contains("procs=64 deno=48 wasm=16"), "{line}");
    assert_eq!(
        line.matches('[').count(),
        3,
        "grq, lrq (all kind groups share one bracket pair) and bound: {line}"
    );
}

// ---------------------------------------------------------------------------------------------
// RT_SCHEDDETAIL
// ---------------------------------------------------------------------------------------------

#[test]
fn detail_has_one_line_per_p_then_one_per_m() {
    let lines = format_schedtrace_detail(&doc_snapshot());
    assert_eq!(lines.len(), 4 + 3);
    for (i, l) in lines.iter().take(4).enumerate() {
        assert!(l.trim_start().starts_with(&format!("P{i} ")), "line {i}: `{l}`");
    }
    for (i, l) in lines.iter().skip(4).enumerate() {
        assert!(l.trim_start().starts_with(&format!("M{i} ")), "line {}: `{l}`", i + 4);
    }
    assert!(lines.iter().all(|l| !l.contains('\n')));
}

#[test]
fn detail_lines_report_kind_queue_and_ownership() {
    let mut snap = doc_snapshot();
    snap.procs[0].runnext = Some(TaskId(0x2a));
    snap.procs[0].machine = Some(MachineId(1));
    snap.procs[0].schedtick = 77;
    snap.procs[0].steals_in = 5;
    snap.procs[0].steals_out = 6;
    snap.procs[0].preemptions = 7;
    snap.procs[0].rekinds = 8;
    snap.machines[0].proc = Some(ProcId(0));
    snap.machines[0].spinning = true;
    let lines = format_schedtrace_detail(&snap);

    let p0 = &lines[0];
    for needle in [
        "kind=deno",
        "lrq=3",
        "runnext=task_000000000000002a",
        "schedtick=77",
        "m=1",
        "steals_in=5",
        "steals_out=6",
        "preempt=7",
        "rekinds=8",
    ] {
        assert!(p0.contains(needle), "P0 line lacks `{needle}`: {p0}");
    }
    let p3 = &lines[3];
    assert!(
        p3.contains("kind=wasm") && p3.contains("runnext=-") && p3.contains("m=-"),
        "unset runnext / machine print as `-`: {p3}"
    );

    let m0 = &lines[4];
    for needle in ["p=0", "bound=2", "spinning=true", "parked=false"] {
        assert!(m0.contains(needle), "M0 line lacks `{needle}`: {m0}");
    }
    assert!(
        lines[5].contains("p=-"),
        "a machine without a P prints `p=-`: {}",
        lines[5]
    );
}

#[test]
fn detail_of_an_empty_snapshot_is_empty() {
    assert!(format_schedtrace_detail(&SchedSnapshot::default()).is_empty());
}

#[test]
fn the_degradation_mode_does_not_change_the_trace_line() {
    let normal = format_schedtrace(&doc_snapshot());
    let drained = format_schedtrace(&SchedSnapshot {
        mode: DegradationMode::Drain,
        ..doc_snapshot()
    });
    assert_eq!(normal, drained, "the documented line has no mode field");
}
