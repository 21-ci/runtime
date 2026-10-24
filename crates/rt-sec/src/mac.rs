//! MAC over compiled artifacts (SEC-21, SEC-55).
//!
//! Every T2 file (`.cwasm`, V8 code cache, snapshots) carries `HMAC-SHA256` under a node key
//! derived from the master key, bound to its [`PreparedKey`] **and its file name**, so a file
//! swapped between services, engine versions or between the files of one entry fails
//! verification. Verified **before** any deserialization.
//!
//! This is the MAC of T2 layout v2 (`cache/v2/…`, key `rt/artifact-mac/v2`). The legacy layout's
//! tags (no file name, key `rt/artifact-mac/v1`) are never verified: its directories are deleted
//! at startup.

use hmac::{Hmac, Mac as _};
use rt_types::PreparedKey;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::master::{purpose, MasterKey};

/// Domain-separation prefix of every v2 tag input.
const DOMAIN_V2: &[u8] = b"rt-t2/v2\0";

/// 32-byte HMAC-SHA256 tag.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Mac(pub [u8; 32]);

pub trait ArtifactMac: Send + Sync {
    /// Tag over `bytes`, the content of T2 file `file` of entry `key`.
    fn sign(&self, key: &PreparedKey, file: &str, bytes: &[u8]) -> Mac;
    /// Constant-time verification.
    fn verify(&self, key: &PreparedKey, file: &str, bytes: &[u8], mac: &Mac) -> bool;
    /// Non-secret identifier of the MAC key (hex). T2 records it so that entries written under
    /// another key (master-key rotation) are dropped quietly instead of failing as tampered.
    fn key_id(&self) -> String;
}

/// HKDF-derived HMAC-SHA256 implementation.
pub struct HkdfArtifactMac {
    key: Zeroizing<[u8; 32]>,
}

impl HkdfArtifactMac {
    pub fn new(master: &MasterKey) -> Self {
        Self {
            key: master.derive(purpose::ARTIFACT_MAC_V2),
        }
    }

    #[allow(clippy::expect_used, reason = "HMAC accepts a key of any length")]
    fn hmac(&self) -> Hmac<Sha256> {
        <Hmac<Sha256> as hmac::Mac>::new_from_slice(&self.key[..]).expect("hmac accepts any key length")
    }

    fn mac_for(&self, key: &PreparedKey, file: &str, bytes: &[u8]) -> Hmac<Sha256> {
        let mut m = self.hmac();
        // Domain separation, then every field length-prefixed or fixed-size: digest, runtime
        // kind, engine fingerprint, file name, content.
        m.update(DOMAIN_V2);
        m.update(&key.digest.0);
        m.update(&[key.kind as u8]);
        m.update(&key.engine_fp.to_le_bytes());
        m.update(&(file.len() as u64).to_le_bytes());
        m.update(file.as_bytes());
        m.update(&(bytes.len() as u64).to_le_bytes());
        m.update(bytes);
        m
    }
}

impl ArtifactMac for HkdfArtifactMac {
    fn sign(&self, key: &PreparedKey, file: &str, bytes: &[u8]) -> Mac {
        Mac(self.mac_for(key, file, bytes).finalize().into_bytes().into())
    }

    fn verify(&self, key: &PreparedKey, file: &str, bytes: &[u8], mac: &Mac) -> bool {
        self.mac_for(key, file, bytes).verify_slice(&mac.0).is_ok()
    }

    fn key_id(&self) -> String {
        let mut m = self.hmac();
        m.update(b"rt-t2/key-id");
        hex::encode(&m.finalize().into_bytes()[..8])
    }
}
