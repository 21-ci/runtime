//! Several isolates alive on one Machine thread at once (isolate per task + the idle pool).
//!
//! rusty_v8 enters an isolate when it is created and expects strictly nested enter/exit, so
//! interleaved polling of two tasks used to abort the process with "PinnedRef<HandleScope> and
//! Context do not belong to the same Isolate". The executor now exits idle isolates and enters
//! them only around a synchronous use (isolate per task).

use crate::common;

use std::time::Duration;

use common::*;
use futures_util::future::join_all;

/// Awaits several times with an id-dependent delay, so the tasks finish in a different order than
/// they started and every poll of one isolate lands between polls of the others.
const SLOW_SUM: &str = "export default async (req: Request) => {\n\
    const id = Number(new URL(req.url).searchParams.get('id') ?? '0');\n\
    let acc = 0;\n\
    for (let i = 0; i < 6; i++) {\n\
      await new Promise((r) => setTimeout(r, 2 + ((id + i) % 3) * 3));\n\
      acc += i;\n\
    }\n\
    return new Response(`done:${id}:${acc}`);\n\
};\n";

#[test]
fn dno_four_concurrent_tasks_interleaving_awaits_all_succeed() {
    run_local(async {
        let mut h = Harness::new(cfg0());
        let p = h.prepare(&ts_upload(SLOW_SUM)).await;
        let started: Vec<_> = (0..4)
            .map(|i| h.start(spec().http_get(&format!("/?id={i}")).finish(), &p).1)
            .collect();
        let outcomes = join_all(started).await;
        for (i, outcome) in outcomes.into_iter().enumerate() {
            let got = read(outcome).await;
            assert_eq!(got.status, 200);
            assert_eq!(got.text(), format!("done:{i}:15"));
        }
    });
}

#[test]
fn dno_concurrent_tasks_with_pooled_isolates_and_idle_disposal() {
    run_local(async {
        let mut h = Harness::new(cfg_with_pool(2));
        let p = h.prepare(&ts_upload(SLOW_SUM)).await;
        h.exec.on_idle(Duration::from_millis(500)); // fill the pool: idle isolates exist beside running ones
        assert_eq!(h.exec.pool_len(), 2);
        for round in 0..3 {
            let started: Vec<_> = (0..3)
                .map(|i| {
                    h.start(spec().http_get(&format!("/?id={}", round * 10 + i)).finish(), &p)
                        .1
                })
                .collect();
            for (i, outcome) in join_all(started).await.into_iter().enumerate() {
                let got = read(outcome).await;
                assert_eq!(got.text(), format!("done:{}:15", round * 10 + i), "round {round}");
            }
            h.settle(Duration::from_millis(20)).await;
            // dispose finished isolates while the pool refills with new ones
            h.exec.on_idle(Duration::from_millis(500));
        }
        assert_eq!(h.exec.dispose_backlog_len(), 0);
    });
}
