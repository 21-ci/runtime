//! `serde` adapters for `std::time::Duration` as human strings (`"250ms"`, `"10s"`, `"1h"`),
//! matching the style of `rt.toml` (docs/config.md). A bare integer is also accepted on input and
//! means seconds.
//!
//! Use with `#[serde(with = "crate::serde_duration")]` or `#[serde(with = "crate::serde_duration::opt")]`.

use std::time::Duration;

use serde::{de, Deserialize, Deserializer, Serializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum Raw {
    Text(String),
    Secs(u64),
}

fn parse(raw: Raw) -> Result<Duration, String> {
    match raw {
        Raw::Secs(s) => Ok(Duration::from_secs(s)),
        Raw::Text(t) => humantime::parse_duration(t.trim()).map_err(|e| format!("invalid duration `{t}`: {e}")),
    }
}

pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&humantime::format_duration(*d))
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    parse(Raw::deserialize(d)?).map_err(de::Error::custom)
}

/// `Option<Duration>` variant.
pub mod opt {
    use super::*;

    pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.collect_str(&humantime::format_duration(*d)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Option::<Raw>::deserialize(d)?
            .map(parse)
            .transpose()
            .map_err(de::Error::custom)
    }
}

/// `Vec<Duration>` variant (e.g. `executor_restart_backoff = ["100ms", "1s", "5s"]`).
pub mod vec {
    use super::*;

    pub fn serialize<S: Serializer>(v: &[Duration], s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for d in v {
            seq.serialize_element(&humantime::format_duration(*d).to_string())?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Duration>, D::Error> {
        Vec::<Raw>::deserialize(d)?
            .into_iter()
            .map(parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(de::Error::custom)
    }
}
