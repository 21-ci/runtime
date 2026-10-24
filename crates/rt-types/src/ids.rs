//! Identifiers.
//!
//! Control-plane IDs are ULIDs with a type prefix (`ten_`, `svc_`, `key_`, `sch_`, `msg_`,
//! `evt_`, `upl_`, `prj_`), see docs/api.md. They sort by creation time, are safe in URLs and
//! round-trip through `Display`/`FromStr`/serde as the prefixed string.
//!
//! [`TaskId`] is a node-local monotonic `u64` (scheduler hot path); it is rendered as
//! `task_<16 hex digits>` for the `x-rt-task-id` header and log records.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use ulid::Ulid;

/// Failure to parse a prefixed ID.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("expected prefix `{expected}_` in id `{got}`")]
    WrongPrefix { expected: &'static str, got: String },
    #[error("invalid ulid in id `{0}`")]
    BadUlid(String),
}

macro_rules! prefixed_ulid {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Ulid);

        impl $name {
            /// The wire prefix without the underscore.
            pub const PREFIX: &'static str = $prefix;

            /// A fresh ID (time-ordered, random).
            pub fn new() -> Self {
                Self(Ulid::new())
            }

            /// Wrap an existing ULID.
            pub const fn from_ulid(u: Ulid) -> Self {
                Self(u)
            }

            /// The underlying ULID.
            pub const fn ulid(&self) -> Ulid {
                self.0
            }

            /// Deterministic ID from a number (tests, simulation, fixtures): `from_u128(n)`.
            pub fn from_u128(n: u128) -> Self {
                Self(Ulid::from(n))
            }

            /// The 26-char ULID without the prefix (used in `rtk_<id>_<secret>` keys).
            pub fn bare(&self) -> String {
                self.0.to_string()
            }

            /// Parse the bare 26-char ULID form.
            pub fn from_bare(s: &str) -> Result<Self, IdError> {
                Ulid::from_string(s).map(Self).map_err(|_| IdError::BadUlid(s.to_string()))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "_{}"), self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{self}")
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, IdError> {
                let rest = s
                    .strip_prefix(concat!($prefix, "_"))
                    .ok_or_else(|| IdError::WrongPrefix { expected: $prefix, got: s.to_string() })?;
                Self::from_bare(rest)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                s.parse().map_err(de::Error::custom)
            }
        }
    };
}

prefixed_ulid!(
    /// Tenant (`ten_…`).
    TenantId, "ten"
);
prefixed_ulid!(
    /// Service (`svc_…`).
    ServiceId, "svc"
);
prefixed_ulid!(
    /// API key (`key_…`); the bare ULID is embedded in `rtk_<id>_<secret>`.
    KeyId, "key"
);
prefixed_ulid!(
    /// Schedule (`sch_…`).
    ScheduleId, "sch"
);
prefixed_ulid!(
    /// Queue message (`msg_…`).
    MsgId, "msg"
);
prefixed_ulid!(
    /// Node event (`evt_…`).
    EventId, "evt"
);
prefixed_ulid!(
    /// Upload being validated (`upl_…`).
    UploadId, "upl"
);
prefixed_ulid!(
    /// Project (`prj_…`, decision D-B3).
    ProjectId, "prj"
);

/// Node-local monotonic task identifier. Rendered as `task_<16 hex>`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct TaskId(pub u64);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task_{:016x}", self.0)
    }
}

impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FromStr for TaskId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, IdError> {
        let rest = s.strip_prefix("task_").ok_or_else(|| IdError::WrongPrefix {
            expected: "task",
            got: s.to_string(),
        })?;
        u64::from_str_radix(rest, 16)
            .map(TaskId)
            .map_err(|_| IdError::BadUlid(s.to_string()))
    }
}

impl Serialize for TaskId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TaskId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(de::Error::custom)
    }
}

/// Immutable version number of a service (1-based, per service).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VersionNo(pub u32);

impl fmt::Display for VersionNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Generation counter of a service's env + secrets (bumped on every write).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretsVersion(pub u64);

/// Scheduler processor index (stable for the life of the node).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcId(pub u32);

/// Scheduler machine (OS thread) index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MachineId(pub u32);
