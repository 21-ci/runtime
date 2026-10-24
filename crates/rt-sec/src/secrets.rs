//! Secrets at rest: AES-256-GCM with a per-value random nonce and AAD = `tenant|service|name`
//!. The wrapping key is derived from the master key.
//!
//! Storage format (`secrets.sealed` in `meta.db`, SEC-53):
//!
//! | Format | Bytes | Written |
//! |---|---|---|
//! | v1 | `0x01 ‖ key_id(4) ‖ nonce(12) ‖ ciphertext+tag`; the AEAD AAD is `0x01 ‖ key_id ‖ aad` | always |
//! | legacy | `nonce(12) ‖ ciphertext+tag`, AAD = `aad` | never; still opened |
//!
//! `key_id` names the master key that sealed the value (HKDF-derived, not secret), so a node can
//! keep the previous master key for decryption only (`RT_MASTER_KEY_PREVIOUS`) while
//! `rt secrets reseal` rewrites every value under the current one.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use rt_types::{ServiceId, TenantId};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::master::{purpose, MasterKey};

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// Leading byte of the v1 storage format.
pub const SEALED_V1: u8 = 0x01;
/// Length of a [`SealKeyId`].
pub const SEAL_KEY_ID_LEN: usize = 4;
const V1_HEADER_LEN: usize = 1 + SEAL_KEY_ID_LEN;

/// Identifies the master key a v1 value was sealed with: the first 4 bytes of
/// `HKDF(master, "rt/secrets-key-id/v1")`. Reveals nothing about the key.
pub type SealKeyId = [u8; SEAL_KEY_ID_LEN];

/// A sealed secret value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sealed {
    pub nonce: [u8; NONCE_LEN],
    /// Ciphertext followed by the 16-byte tag.
    pub ciphertext: Vec<u8>,
    /// `Some` for the v1 format (the sealing key's id), `None` for the legacy format.
    pub key_id: Option<SealKeyId>,
}

impl Sealed {
    /// Storage form (`secrets.sealed` BLOB in `meta.db`): v1 when the value carries a key id.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(V1_HEADER_LEN + NONCE_LEN + self.ciphertext.len());
        if let Some(id) = self.key_id {
            v.push(SEALED_V1);
            v.extend_from_slice(&id);
        }
        v.extend_from_slice(&self.nonce);
        v.extend_from_slice(&self.ciphertext);
        v
    }

    /// Parse either format. A blob that starts with the v1 byte and is long enough is read as v1;
    /// a legacy value whose random nonce happens to start with that byte is recovered when it is
    /// opened (see [`Sealed::as_legacy`]).
    pub fn from_bytes(b: &[u8]) -> Result<Self, SecretError> {
        if b.len() >= V1_HEADER_LEN + NONCE_LEN + TAG_LEN && b[0] == SEALED_V1 {
            let mut id = [0u8; SEAL_KEY_ID_LEN];
            id.copy_from_slice(&b[1..V1_HEADER_LEN]);
            let body = &b[V1_HEADER_LEN..];
            let mut nonce = [0u8; NONCE_LEN];
            nonce.copy_from_slice(&body[..NONCE_LEN]);
            return Ok(Self {
                nonce,
                ciphertext: body[NONCE_LEN..].to_vec(),
                key_id: Some(id),
            });
        }
        Self::legacy_from_bytes(b)
    }

    fn legacy_from_bytes(b: &[u8]) -> Result<Self, SecretError> {
        if b.len() < NONCE_LEN + TAG_LEN {
            return Err(SecretError::Malformed);
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&b[..NONCE_LEN]);
        Ok(Self {
            nonce,
            ciphertext: b[NONCE_LEN..].to_vec(),
            key_id: None,
        })
    }

    /// The same bytes read as the legacy format (only meaningful for a value parsed as v1).
    pub fn as_legacy(&self) -> Option<Sealed> {
        self.key_id.and_then(|_| Self::legacy_from_bytes(&self.to_bytes()).ok())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SecretError {
    /// Wrong key, wrong AAD (value moved between services), or tampering.
    #[error("secret could not be decrypted")]
    Decrypt,
    #[error("sealed value is malformed")]
    Malformed,
    /// Backend (KMS/Vault in paid builds) unavailable.
    #[error("secrets backend unavailable: {0}")]
    Unavailable(String),
}

/// AAD binding a secret to its owner: `tenant|service|name`.
pub fn secret_aad(tenant: TenantId, service: ServiceId, name: &str) -> Vec<u8> {
    format!("{tenant}|{service}|{name}").into_bytes()
}

/// Extension point: core = local AES-GCM; paid builds may plug KMS / Vault.
pub trait SecretsBackend: Send + Sync {
    fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Sealed;
    fn open(&self, aad: &[u8], sealed: &Sealed) -> Result<Zeroizing<Vec<u8>>, SecretError>;
    /// True when `sealed` is already in the current format under the current key, so a re-seal
    /// would change nothing. The default re-seals everything.
    fn is_current(&self, sealed: &Sealed) -> bool {
        let _ = sealed;
        false
    }
}

/// One AES-256-GCM key and its id.
struct SealKey {
    id: SealKeyId,
    cipher: Aes256Gcm,
}

impl SealKey {
    #[allow(
        clippy::expect_used,
        reason = "AES-256-GCM takes any 32-byte key, and HKDF gives 32 bytes"
    )]
    fn derive(master: &MasterKey) -> Self {
        let key = master.derive(purpose::SECRETS);
        let id_full = master.derive(purpose::SECRETS_KEY_ID);
        let mut id = [0u8; SEAL_KEY_ID_LEN];
        id.copy_from_slice(&id_full[..SEAL_KEY_ID_LEN]);
        Self {
            id,
            cipher: Aes256Gcm::new_from_slice(&key[..]).expect("32-byte key"),
        }
    }

    fn decrypt(&self, aad: &[u8], sealed: &Sealed) -> Option<Zeroizing<Vec<u8>>> {
        let aad = match sealed.key_id {
            Some(id) => v1_aad(id, aad),
            None => aad.to_vec(),
        };
        self.cipher
            .decrypt(
                Nonce::from_slice(&sealed.nonce),
                Payload {
                    msg: &sealed.ciphertext,
                    aad: &aad,
                },
            )
            .ok()
            .map(Zeroizing::new)
    }
}

/// The v1 AEAD AAD: the header is authenticated, so a value cannot be re-labelled with another
/// key id or stripped down to the legacy format.
fn v1_aad(id: SealKeyId, aad: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(V1_HEADER_LEN + aad.len());
    v.push(SEALED_V1);
    v.extend_from_slice(&id);
    v.extend_from_slice(aad);
    v
}

/// Local AES-256-GCM backend: seals under the current master key, opens under the current or
/// the previous one.
pub struct LocalSecrets {
    current: SealKey,
    previous: Option<SealKey>,
}

impl LocalSecrets {
    pub fn new(master: &MasterKey) -> Self {
        Self {
            current: SealKey::derive(master),
            previous: None,
        }
    }

    /// Also open values sealed under `previous` (`RT_MASTER_KEY_PREVIOUS`, key rotation). Never
    /// used to seal.
    pub fn with_previous(mut self, previous: &MasterKey) -> Self {
        self.previous = Some(SealKey::derive(previous));
        self
    }

    /// Id of the key new values are sealed with.
    pub fn key_id(&self) -> SealKeyId {
        self.current.id
    }

    fn keys(&self) -> impl Iterator<Item = &SealKey> {
        std::iter::once(&self.current).chain(self.previous.as_ref())
    }
}

impl SecretsBackend for LocalSecrets {
    #[allow(
        clippy::expect_used,
        reason = "AES-GCM fails only past 64 GiB of plaintext; secrets are capped at 32 KiB"
    )]
    fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Sealed {
        // A fresh random 96-bit nonce per seal, from the OS RNG (the AEAD crate's own generator).
        let nonce: [u8; NONCE_LEN] = Aes256Gcm::generate_nonce(&mut aes_gcm::aead::OsRng).into();
        let aad = v1_aad(self.current.id, aad);
        let ciphertext = self
            .current
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .expect("AES-GCM encryption of in-memory data cannot fail");
        Sealed {
            nonce,
            ciphertext,
            key_id: Some(self.current.id),
        }
    }

    fn open(&self, aad: &[u8], sealed: &Sealed) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        if let Some(id) = sealed.key_id {
            if let Some(pt) = self.keys().filter(|k| k.id == id).find_map(|k| k.decrypt(aad, sealed)) {
                return Ok(pt);
            }
            // A legacy value whose nonce happens to start with the v1 byte.
            return match sealed.as_legacy() {
                Some(legacy) => self
                    .keys()
                    .find_map(|k| k.decrypt(aad, &legacy))
                    .ok_or(SecretError::Decrypt),
                None => Err(SecretError::Decrypt),
            };
        }
        self.keys()
            .find_map(|k| k.decrypt(aad, sealed))
            .ok_or(SecretError::Decrypt)
    }

    fn is_current(&self, sealed: &Sealed) -> bool {
        sealed.key_id == Some(self.current.id)
    }
}
