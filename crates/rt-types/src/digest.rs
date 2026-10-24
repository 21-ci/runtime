//! Content digests (`sha256:<hex>`), the key of the content-addressed artifact store.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DigestError {
    #[error("digest must look like `sha256:<64 hex chars>`: `{0}`")]
    Malformed(String),
}

/// SHA-256 digest of some bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub const PREFIX: &'static str = "sha256:";

    /// Digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(bytes);
        Self(h.finalize().into())
    }

    /// Lower-case hex of the 32 bytes (no prefix). Used as the artifact directory name.
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse the bare 64-char hex form.
    pub fn from_hex(s: &str) -> Result<Self, DigestError> {
        let mut out = [0u8; 32];
        if s.len() != 64 || hex::decode_to_slice(s, &mut out).is_err() {
            return Err(DigestError::Malformed(s.to_string()));
        }
        Ok(Self(out))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sha256:{}", self.hex())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FromStr for Digest {
    type Err = DigestError;
    fn from_str(s: &str) -> Result<Self, DigestError> {
        let rest = s
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| DigestError::Malformed(s.to_string()))?;
        Self::from_hex(rest)
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(de::Error::custom)
    }
}
