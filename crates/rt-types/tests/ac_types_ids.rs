//! rt-types identifiers: prefixed ULIDs (`ten_`, `svc_`, ...), node-local `TaskId`, numeric
//! newtypes. Behavior only: wire format, round-trips, rejection of malformed / cross-type input.

use std::collections::HashSet;
use std::str::FromStr;

use rt_types::{
    EventId, IdError, KeyId, MachineId, MsgId, ProcId, ScheduleId, SecretsVersion, ServiceId, TaskId, TenantId,
    UploadId, VersionNo,
};

/// Crockford base32 alphabet used by ULIDs.
fn is_ulid_text(s: &str) -> bool {
    s.len() == 26 && s.chars().all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c))
}

/// Runs the same assertions for every prefixed-ULID id type.
macro_rules! prefixed_id_suite {
    ($($modname:ident: $ty:ty, $prefix:literal;)*) => {$(
        mod $modname {
            use super::*;

            #[test]
            fn display_is_prefix_underscore_26_char_ulid() {
                let id = <$ty>::new();
                let s = id.to_string();
                let rest = s.strip_prefix(concat!($prefix, "_")).unwrap_or_else(|| panic!("`{s}` lacks prefix"));
                assert!(is_ulid_text(rest), "`{rest}` is not a 26 char Crockford ULID");
                assert_eq!(<$ty>::PREFIX, $prefix);
            }

            #[test]
            fn display_parse_round_trip() {
                let id = <$ty>::new();
                let parsed: $ty = id.to_string().parse().expect("own Display output must parse");
                assert_eq!(parsed, id);
                assert_eq!(<$ty>::from_str(&id.to_string()).unwrap(), id);
            }

            #[test]
            fn bare_form_round_trip_and_has_no_prefix() {
                let id = <$ty>::new();
                let bare = id.bare();
                assert!(is_ulid_text(&bare));
                assert_eq!(<$ty>::from_bare(&bare).unwrap(), id);
                assert_eq!(format!("{}_{}", $prefix, bare), id.to_string());
            }

            #[test]
            fn from_u128_is_deterministic_and_ordered() {
                assert_eq!(<$ty>::from_u128(42), <$ty>::from_u128(42));
                assert_ne!(<$ty>::from_u128(42), <$ty>::from_u128(43));
                assert!(<$ty>::from_u128(1) < <$ty>::from_u128(2));
                assert_eq!(
                    <$ty>::from_u128(0).to_string(),
                    concat!($prefix, "_00000000000000000000000000")
                );
            }

            #[test]
            fn missing_or_wrong_prefix_is_rejected_with_wrong_prefix_error() {
                let bare = <$ty>::new().bare();
                match bare.parse::<$ty>() {
                    Err(IdError::WrongPrefix { expected, got }) => {
                        assert_eq!(expected, $prefix);
                        assert_eq!(got, bare);
                    }
                    other => panic!("bare ulid must be WrongPrefix, got {other:?}"),
                }
                let wrong = format!("zzz_{bare}");
                assert!(matches!(wrong.parse::<$ty>(), Err(IdError::WrongPrefix { .. })));
                // prefix without the underscore is not the prefix
                let no_us = format!("{}{bare}", $prefix);
                assert!(matches!(no_us.parse::<$ty>(), Err(IdError::WrongPrefix { .. })));
            }

            #[test]
            fn malformed_ulid_after_correct_prefix_is_bad_ulid() {
                for bad in ["", "short", "not-valid-id!!!", "00000000000000000000000000000", "0000000000000000000000000U"] {
                    let s = format!("{}_{bad}", $prefix);
                    assert!(
                        matches!(s.parse::<$ty>(), Err(IdError::BadUlid(_))),
                        "`{s}` must be BadUlid, got {:?}",
                        s.parse::<$ty>()
                    );
                }
                assert!(matches!(<$ty>::from_bare("nope"), Err(IdError::BadUlid(_))));
            }

            #[test]
            fn serde_is_the_prefixed_string() {
                let id = <$ty>::new();
                let json = serde_json::to_string(&id).unwrap();
                assert_eq!(json, format!("\"{id}\""));
                let back: $ty = serde_json::from_str(&json).unwrap();
                assert_eq!(back, id);
                // wrong prefix / garbage are deserialization errors, not panics
                assert!(serde_json::from_str::<$ty>("\"zzz_00000000000000000000000000\"").is_err());
                assert!(serde_json::from_str::<$ty>("\"garbage\"").is_err());
                assert!(serde_json::from_str::<$ty>("42").is_err());
            }

            #[test]
            fn fresh_ids_are_unique() {
                let set: HashSet<$ty> = (0..10_000).map(|_| <$ty>::new()).collect();
                assert_eq!(set.len(), 10_000);
            }
        }
    )*};
}

prefixed_id_suite! {
    tenant_id: TenantId, "ten";
    service_id: ServiceId, "svc";
    key_id: KeyId, "key";
    schedule_id: ScheduleId, "sch";
    msg_id: MsgId, "msg";
    event_id: EventId, "evt";
    upload_id: UploadId, "upl";
}

#[test]
fn ids_of_different_types_never_parse_as_each_other() {
    let svc = ServiceId::new().to_string();
    assert!(svc.parse::<TenantId>().is_err());
    assert!(svc.parse::<KeyId>().is_err());
    assert!(svc.parse::<ScheduleId>().is_err());
    assert!(svc.parse::<MsgId>().is_err());
    assert!(svc.parse::<EventId>().is_err());
    assert!(svc.parse::<UploadId>().is_err());
    assert!(svc.parse::<ServiceId>().is_ok());
}

#[test]
fn ids_created_in_later_milliseconds_sort_after_earlier_ones() {
    let a = ServiceId::new();
    std::thread::sleep(std::time::Duration::from_millis(3));
    let b = ServiceId::new();
    assert!(a < b, "ULIDs must be time-ordered: {a} !< {b}");
    assert!(a.to_string() < b.to_string(), "string order must match creation order");
}

#[test]
fn id_error_messages_name_the_input() {
    let e = "xxx_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<ServiceId>().unwrap_err();
    let msg = e.to_string();
    assert!(
        msg.contains("svc_") && msg.contains("xxx_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        "{msg}"
    );
    let e = "svc_nope".parse::<ServiceId>().unwrap_err();
    assert!(e.to_string().contains("nope"), "{e}");
}

// ---------------------------------------------------------------------------------------------
// TaskId
// ---------------------------------------------------------------------------------------------

#[test]
fn task_id_renders_as_task_underscore_16_lower_hex() {
    assert_eq!(TaskId(0).to_string(), "task_0000000000000000");
    assert_eq!(TaskId(0x2a).to_string(), "task_000000000000002a");
    assert_eq!(TaskId(u64::MAX).to_string(), "task_ffffffffffffffff");
    assert_eq!(format!("{:?}", TaskId(255)), "task_00000000000000ff");
}

#[test]
fn task_id_parse_round_trip_and_errors() {
    for n in [0u64, 1, 0x2a, 0xdead_beef, u64::MAX] {
        let id = TaskId(n);
        assert_eq!(id.to_string().parse::<TaskId>().unwrap(), id);
    }
    assert!(matches!(
        "2a".parse::<TaskId>(),
        Err(IdError::WrongPrefix { expected: "task", .. })
    ));
    assert!(matches!(
        "svc_000000000000002a".parse::<TaskId>(),
        Err(IdError::WrongPrefix { .. })
    ));
    assert!("task_zzzz".parse::<TaskId>().is_err());
    assert!("task_".parse::<TaskId>().is_err());
    assert!(
        "task_1ffffffffffffffff".parse::<TaskId>().is_err(),
        "17 hex digits overflow u64"
    );
}

#[test]
fn task_id_serde_is_the_rendered_string_and_orders_numerically() {
    let id = TaskId(0x2a);
    assert_eq!(serde_json::to_string(&id).unwrap(), "\"task_000000000000002a\"");
    assert_eq!(serde_json::from_str::<TaskId>("\"task_000000000000002a\"").unwrap(), id);
    assert!(serde_json::from_str::<TaskId>("42").is_err());
    assert!(TaskId(9) < TaskId(10));
    // zero padding makes lexicographic order equal numeric order
    assert!(TaskId(9).to_string() < TaskId(10).to_string());
}

// ---------------------------------------------------------------------------------------------
// Numeric newtypes
// ---------------------------------------------------------------------------------------------

#[test]
fn numeric_newtypes_serialize_as_bare_numbers() {
    assert_eq!(serde_json::to_string(&VersionNo(3)).unwrap(), "3");
    assert_eq!(serde_json::to_string(&SecretsVersion(7)).unwrap(), "7");
    assert_eq!(serde_json::to_string(&ProcId(2)).unwrap(), "2");
    assert_eq!(serde_json::to_string(&MachineId(5)).unwrap(), "5");
    assert_eq!(serde_json::from_str::<VersionNo>("12").unwrap(), VersionNo(12));
    assert_eq!(
        serde_json::from_str::<SecretsVersion>("12").unwrap(),
        SecretsVersion(12)
    );
    assert_eq!(serde_json::from_str::<ProcId>("0").unwrap(), ProcId(0));
    assert_eq!(serde_json::from_str::<MachineId>("9").unwrap(), MachineId(9));
    assert!(serde_json::from_str::<VersionNo>("-1").is_err());
    assert!(serde_json::from_str::<VersionNo>("\"3\"").is_err());
}

#[test]
fn version_no_displays_as_plain_number_and_orders() {
    assert_eq!(VersionNo(3).to_string(), "3");
    assert!(VersionNo(2) < VersionNo(10));
    assert_eq!(VersionNo::default(), VersionNo(0));
}

#[test]
fn ids_default_to_a_fresh_value_and_debug_prints_the_wire_form() {
    let a = TenantId::default();
    let b = TenantId::default();
    assert_ne!(a, b, "a default id is a new id, not a zero id");
    assert_eq!(format!("{a:?}"), a.to_string());
}
