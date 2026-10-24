//! Runtime and task kinds.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Which guest runtime a service uses.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    Deno,
    Wasm,
}

impl RuntimeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            RuntimeKind::Deno => "deno",
            RuntimeKind::Wasm => "wasm",
        }
    }
}

impl fmt::Display for RuntimeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RuntimeKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "deno" => Ok(RuntimeKind::Deno),
            "wasm" => Ok(RuntimeKind::Wasm),
            other => Err(format!("unknown runtime `{other}` (expected deno|wasm)")),
        }
    }
}

/// What a scheduler task is: guest execution of one runtime, or a build.
/// Processors have exactly one kind and only run tasks of that kind.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub enum TaskKind {
    Exec(RuntimeKind),
    Build,
}

impl TaskKind {
    pub const DENO: TaskKind = TaskKind::Exec(RuntimeKind::Deno);
    pub const WASM: TaskKind = TaskKind::Exec(RuntimeKind::Wasm);
    /// All kinds, in the stable order used for per-kind arrays (`idx()`).
    pub const ALL: [TaskKind; 3] = [TaskKind::DENO, TaskKind::WASM, TaskKind::Build];
    pub const COUNT: usize = 3;

    /// Dense index 0..3 for per-kind arrays (deno, wasm, build).
    pub const fn idx(self) -> usize {
        match self {
            TaskKind::Exec(RuntimeKind::Deno) => 0,
            TaskKind::Exec(RuntimeKind::Wasm) => 1,
            TaskKind::Build => 2,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            TaskKind::Exec(RuntimeKind::Deno) => "deno",
            TaskKind::Exec(RuntimeKind::Wasm) => "wasm",
            TaskKind::Build => "build",
        }
    }
}

impl fmt::Display for TaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "deno" => Ok(TaskKind::DENO),
            "wasm" => Ok(TaskKind::WASM),
            "build" => Ok(TaskKind::Build),
            other => Err(format!("unknown task kind `{other}`")),
        }
    }
}

impl Serialize for TaskKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TaskKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Named tuning bundle (docs/config.md). Knob tables live in `rt-config`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    MaxDensity,
    #[default]
    Balanced,
    MinLatency,
}

impl Profile {
    pub const ALL: [Profile; 3] = [Profile::MaxDensity, Profile::Balanced, Profile::MinLatency];

    pub const fn as_str(self) -> &'static str {
        match self {
            Profile::MaxDensity => "max_density",
            Profile::Balanced => "balanced",
            Profile::MinLatency => "min_latency",
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Profile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "max_density" => Ok(Profile::MaxDensity),
            "balanced" => Ok(Profile::Balanced),
            "min_latency" => Ok(Profile::MinLatency),
            other => Err(format!(
                "unknown profile `{other}` (expected max_density|balanced|min_latency)"
            )),
        }
    }
}

/// Which listener a request arrived on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Listener {
    Public,
    Internal,
    Control,
    Metrics,
}

/// Node-wide admission mode (admission, pressure ladder).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DegradationMode {
    #[default]
    Normal,
    /// Only tasks whose prepared artifact is already in T1 are admitted.
    Shed,
    /// Nothing new is admitted.
    Drain,
}
