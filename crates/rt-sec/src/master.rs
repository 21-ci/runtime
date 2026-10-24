//! The node master key and purpose-separated key derivation (HKDF-SHA256).
//!
//! One 32-byte master key (`RT_MASTER_KEY` / `security.master_key_file`) derives every other
//! key: the API-key pepper, the artifact MAC key, the secrets wrapping key. Each purpose has
//! its own HKDF `info`, so a leak of one derived key never reveals another.

use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroizing;

/// HKDF `info` strings (stable; changing one invalidates everything derived with it).
pub mod purpose {
    pub const KEY_PEPPER: &str = "rt/api-key-pepper/v1";
    /// The legacy T2 layout's MAC key (`cache/<fp>/…`, no file-name binding). No longer used to
    /// sign or verify; kept so its derivation stays pinned.
    pub const ARTIFACT_MAC: &str = "rt/artifact-mac/v1";
    /// MAC key of T2 layout v2 (`cache/v2/…`): tags bind the file name too (SEC-55).
    pub const ARTIFACT_MAC_V2: &str = "rt/artifact-mac/v2";
    pub const SECRETS: &str = "rt/secrets-aes-gcm/v1";
    /// Source of the non-secret key id stored with sealed secrets (SEC-53).
    pub const SECRETS_KEY_ID: &str = "rt/secrets-key-id/v1";
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MasterKeyError {
    #[error("master key must be 64 hex characters (32 bytes)")]
    BadLength,
    #[error("master key is not valid hex")]
    BadHex,
}

/// 32-byte master key; zeroized on drop; never printed.
#[derive(Clone)]
pub struct MasterKey(Zeroizing<[u8; 32]>);

impl MasterKey {
    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(Zeroizing::new(b))
    }

    /// Parse `RT_MASTER_KEY` (hex, 32 bytes).
    pub fn from_hex(s: &str) -> Result<Self, MasterKeyError> {
        let s = s.trim();
        if s.len() != 64 {
            return Err(MasterKeyError::BadLength);
        }
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).map_err(|_| MasterKeyError::BadHex)?;
        Ok(Self::from_bytes(out))
    }

    /// Random key (tests, first-run key generation).
    pub fn generate() -> Self {
        let mut b = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut b);
        Self::from_bytes(b)
    }

    /// HKDF-SHA256(master, info = `purpose`) → 32 bytes.
    #[allow(clippy::expect_used, reason = "32 bytes is always a valid HKDF-SHA256 output length")]
    pub fn derive(&self, purpose: &str) -> Zeroizing<[u8; 32]> {
        let hk = Hkdf::<Sha256>::new(None, &self.0[..]);
        let mut okm = Zeroizing::new([0u8; 32]);
        hk.expand(purpose.as_bytes(), &mut okm[..])
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        okm
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(<redacted>)")
    }
}

/// A key file that users other than its owner may read or write (SRV-002): the message to log, or
/// `None` for a 0600-style file (and on platforms without Unix modes).
pub fn key_file_mode_problem(path: &std::path::Path) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Some(format!(
                "{} has mode {mode:o}: other users can read the master key; chmod 600 it",
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    None
}
