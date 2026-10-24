//! The argument rules of `std:schedule`: schedule ids, payload size, delay
//! bound and generated ids, shared by the Deno and WASM guests through `rt_host`.

use std::collections::HashSet;
use std::time::Duration;

use rt_host::schedule::{check_delay, check_id, check_payload, new_id, SCHEDULE_MAX_DELAY, SCHEDULE_PAYLOAD_MAX_BYTES};

#[test]
fn ids_are_1_to_128_bytes_of_safe_characters() {
    for ok in ["a", "order.2026-10-23:retry_1", &"x".repeat(128)] {
        assert!(check_id(ok).is_ok(), "{ok}");
    }
    for bad in ["", "has space", "slash/no", "emoji-😀", &"x".repeat(129)] {
        let err = check_id(bad).expect_err(bad);
        assert_eq!(err.code, "validation_failed", "{bad}");
    }
}

#[test]
fn generated_ids_are_valid_and_unique() {
    let mut seen = HashSet::new();
    for _ in 0..1000 {
        let id = new_id();
        assert!(id.starts_with("sch_"), "{id}");
        assert!(check_id(&id).is_ok(), "{id}");
        assert!(seen.insert(id));
    }
}

#[test]
fn payload_is_capped() {
    assert!(check_payload(&vec![b'a'; SCHEDULE_PAYLOAD_MAX_BYTES]).is_ok());
    let err = check_payload(&vec![b'a'; SCHEDULE_PAYLOAD_MAX_BYTES + 1]).expect_err("too large");
    assert_eq!(err.code, "validation_failed");
}

#[test]
fn delay_is_capped_at_thirty_days() {
    assert_eq!(SCHEDULE_MAX_DELAY, Duration::from_secs(30 * 24 * 3600));
    assert!(check_delay(Duration::ZERO).is_ok());
    assert!(check_delay(SCHEDULE_MAX_DELAY).is_ok());
    assert_eq!(
        check_delay(SCHEDULE_MAX_DELAY + Duration::from_secs(1))
            .expect_err("too far")
            .code,
        "validation_failed"
    );
}
