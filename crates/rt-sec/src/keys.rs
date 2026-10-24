//! API keys: `rtk_<key_id>_<secret>` (SECURITY.md).
//!
//! `key_id` is the bare ULID of the `key_…` ID; `secret` is 43 base62 characters (≈256 bits).
//! Only `HMAC-SHA256(pepper, secret)` is stored; the pepper derives from the master key.

use hmac::{Hmac, Mac as _};
use rand::RngCore;
use rt_types::{KeyId, KeySecret, NewKey, ProjectId, Scopes, UnixMs};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::master::{purpose, MasterKey};

pub const KEY_PREFIX: &str = "rtk";
/// Base62 characters in a key secret.
pub const SECRET_LEN: usize = 43;
const BASE62: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KeyParseError {
    #[error("not an rtk_ key")]
    NotAKey,
    #[error("malformed key id")]
    BadId,
    #[error("malformed key secret")]
    BadSecret,
}

/// Key split into its parts. `secret` borrows from the input; `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct ParsedKey<'a> {
    pub id: KeyId,
    pub secret: &'a str,
}

impl std::fmt::Debug for ParsedKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsedKey")
            .field("id", &self.id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Parse `rtk_<id>_<secret>`.
pub fn parse_key(s: &str) -> Result<ParsedKey<'_>, KeyParseError> {
    let mut it = s.splitn(3, '_');
    if it.next() != Some(KEY_PREFIX) {
        return Err(KeyParseError::NotAKey);
    }
    let id = it.next().ok_or(KeyParseError::BadId)?;
    let secret = it.next().ok_or(KeyParseError::BadSecret)?;
    let id = KeyId::from_bare(id).map_err(|_| KeyParseError::BadId)?;
    if secret.len() != SECRET_LEN || !secret.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(KeyParseError::BadSecret);
    }
    Ok(ParsedKey { id, secret })
}

/// Render the plaintext key.
pub fn format_key(id: KeyId, secret: &str) -> String {
    format!("{KEY_PREFIX}_{}_{secret}", id.bare())
}

/// A fresh random secret of [`SECRET_LEN`] base62 characters (rejection sampling, no bias).
pub fn generate_secret() -> String {
    let mut out = String::with_capacity(SECRET_LEN);
    let mut buf = [0u8; 64];
    while out.len() < SECRET_LEN {
        rand::rngs::OsRng.fill_bytes(&mut buf);
        for b in buf {
            if b < 248 && out.len() < SECRET_LEN {
                out.push(BASE62[(b % 62) as usize] as char);
            }
        }
    }
    out
}

/// Keyed hasher for key secrets.
#[derive(Clone)]
pub struct KeyHasher {
    pepper: zeroize::Zeroizing<[u8; 32]>,
    /// Pepper of the previous master key (`RT_MASTER_KEY_PREVIOUS`): keys issued before a
    /// rotation keep verifying. Never used to hash new keys.
    previous: Option<zeroize::Zeroizing<[u8; 32]>>,
}

#[allow(clippy::expect_used, reason = "HMAC accepts a key of any length")]
fn peppered(pepper: &[u8], secret: &str) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as hmac::Mac>::new_from_slice(pepper).expect("hmac accepts any key length");
    m.update(secret.as_bytes());
    m.finalize().into_bytes().to_vec()
}

impl KeyHasher {
    /// Derive the pepper from the master key.
    pub fn new(master: &MasterKey) -> Self {
        Self {
            pepper: master.derive(purpose::KEY_PEPPER),
            previous: None,
        }
    }

    /// Also accept hashes made under the previous master key (key rotation, docs/deploy.md).
    pub fn with_previous(mut self, previous: &MasterKey) -> Self {
        self.previous = Some(previous.derive(purpose::KEY_PEPPER));
        self
    }

    /// `HMAC-SHA256(pepper, secret)`.
    pub fn hash(&self, secret: &str) -> Vec<u8> {
        peppered(&self.pepper[..], secret)
    }

    /// Constant-time comparison of `secret` against a stored hash (current pepper, then the
    /// previous one when configured).
    pub fn verify(&self, secret: &str, stored: &[u8]) -> bool {
        let matches = |h: Vec<u8>| h.len() == stored.len() && bool::from(h.ct_eq(stored));
        matches(self.hash(secret))
            || self
                .previous
                .as_ref()
                .is_some_and(|p| matches(peppered(&p[..], secret)))
    }
}

/// Inputs for issuing a key.
#[derive(Clone, Debug)]
pub struct KeySpec {
    pub name: Option<String>,
    pub scopes: Scopes,
    /// See [`rt_types::ApiKey::project_ids`].
    pub project_ids: Option<Vec<ProjectId>>,
    /// See [`rt_types::ApiKey::environments`].
    pub environments: Option<Vec<String>>,
    pub expires_at_ms: Option<UnixMs>,
}

impl KeyHasher {
    /// Generate ID + secret + hash. Persist the returned [`NewKey`] with `MetaStore::create_key`
    /// and show the [`KeySecret`] (the full `rtk_…` string) to the caller exactly once.
    pub fn issue(&self, spec: KeySpec) -> (KeySecret, NewKey) {
        let id = KeyId::new();
        let secret = generate_secret();
        let full = format_key(id, &secret);
        let new = NewKey {
            id,
            name: spec.name,
            scopes: spec.scopes,
            project_ids: spec.project_ids,
            environments: spec.environments,
            expires_at_ms: spec.expires_at_ms,
            hmac: self.hash(&secret),
        };
        (KeySecret::new(full), new)
    }
}
