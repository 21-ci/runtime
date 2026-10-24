//! The argument rules of `std:schedule`: schedule ids, payload size and delay
//! bound, shared by the Deno and WASM guests. Refusals are `validation_failed`.

use std::time::Duration;

use crate::facade::HostError;

/// Longest schedule id, in bytes.
pub const SCHEDULE_ID_MAX_BYTES: usize = 128;
/// Largest payload (its JSON text), in bytes.
pub const SCHEDULE_PAYLOAD_MAX_BYTES: usize = 16 * 1024;
/// Longest delay from now (`after`, `at`).
pub const SCHEDULE_MAX_DELAY: Duration = Duration::from_secs(30 * 24 * 3600);

/// A schedule id: 1 to 128 bytes of ASCII letters, digits, `.`, `_`, `-` and `:`.
pub fn check_id(id: &str) -> Result<(), HostError> {
    let ok = !id.is_empty()
        && id.len() <= SCHEDULE_ID_MAX_BYTES
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b':'));
    if ok {
        Ok(())
    } else {
        Err(HostError::new(
            "validation_failed",
            "schedule id: 1 to 128 of a-z, A-Z, 0-9, . _ - :",
        ))
    }
}

/// A fresh id for a schedule created without one (`sch_` and a ULID).
pub fn new_id() -> String {
    format!("sch_{}", ulid::Ulid::new())
}

/// The payload's JSON text, at most [`SCHEDULE_PAYLOAD_MAX_BYTES`].
pub fn check_payload(json: &[u8]) -> Result<(), HostError> {
    if json.len() <= SCHEDULE_PAYLOAD_MAX_BYTES {
        Ok(())
    } else {
        Err(HostError::new(
            "validation_failed",
            "schedule payload is larger than 16 KiB",
        ))
    }
}

/// A delay of at most [`SCHEDULE_MAX_DELAY`].
pub fn check_delay(delay: Duration) -> Result<(), HostError> {
    if delay <= SCHEDULE_MAX_DELAY {
        Ok(())
    } else {
        Err(HostError::new(
            "validation_failed",
            "schedule delay is longer than 30 days",
        ))
    }
}
