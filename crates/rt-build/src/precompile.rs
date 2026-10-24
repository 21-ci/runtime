//! Engine-specific compile steps injected into the builder.
//!
//! `rt-build` never depends on an executor (layer rules), so the executors implement
//! these traits and `rt-server` injects them: `rt-exec-deno` → [`CodeCacheProducer`],
//! `rt-exec-wasm` → [`ComponentPrecompiler`].

use std::collections::BTreeMap;

use rt_types::BuildFailure;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::bundle::Bundle;

/// V8 code cache for a bundle: one blob per module specifier.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeCache {
    /// Engine fingerprint it was produced under (V8 version + flags); a different engine rejects it.
    pub engine_fp: u64,
    pub per_module: BTreeMap<String, Vec<u8>>,
}

/// Leading bytes of a serialized [`CodeCache`] (T2 `code_cache.bin`).
pub const CODE_CACHE_MAGIC: &[u8; 4] = b"RTCC";
/// Version of the binary layout; a reader rejects any other value (a cache miss, never a crash).
pub const CODE_CACHE_FORMAT: u8 = 1;

/// Why a `code_cache.bin` could not be parsed. Every case is a cache miss for the loader: the
/// file is MAC-verified before parsing, so this means an older/newer format, not tampering.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodeCacheError {
    /// No `RTCC` magic: not a code cache, or the JSON layout written before format 1.
    #[error("not a binary code cache")]
    Magic,
    #[error("unsupported code cache format {0}")]
    Format(u8),
    #[error("code cache truncated")]
    Truncated,
    #[error("code cache has {0} trailing bytes")]
    Trailing(usize),
    #[error("code cache module specifier is not utf-8")]
    Specifier,
    #[error("code cache lists module `{0}` twice")]
    Duplicate(String),
}

impl CodeCache {
    /// Binary, length-prefixed, little-endian:
    /// `"RTCC"` · format `u8` · `engine_fp: u64` · `count: u32` ·
    /// `count` × (`spec_len: u32` · specifier (utf-8) · `blob_len: u32` · blob), in specifier order.
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload: usize = self.per_module.iter().map(|(k, v)| 8 + k.len() + v.len()).sum();
        let mut out = Vec::with_capacity(CODE_CACHE_MAGIC.len() + 1 + 8 + 4 + payload);
        out.extend_from_slice(CODE_CACHE_MAGIC);
        out.push(CODE_CACHE_FORMAT);
        out.extend_from_slice(&self.engine_fp.to_le_bytes());
        out.extend_from_slice(&len_u32(self.per_module.len()).to_le_bytes());
        for (spec, blob) in &self.per_module {
            out.extend_from_slice(&len_u32(spec.len()).to_le_bytes());
            out.extend_from_slice(spec.as_bytes());
            out.extend_from_slice(&len_u32(blob.len()).to_le_bytes());
            out.extend_from_slice(blob);
        }
        out
    }

    /// Parse [`CodeCache::to_bytes`] output. Never panics and never allocates more than the
    /// input's size, whatever the declared lengths say.
    pub fn from_bytes(b: &[u8]) -> Result<Self, CodeCacheError> {
        let mut r = Reader(b);
        if r.take(CODE_CACHE_MAGIC.len()).ok() != Some(CODE_CACHE_MAGIC.as_slice()) {
            return Err(CodeCacheError::Magic);
        }
        let format = r.take(1)?[0];
        if format != CODE_CACHE_FORMAT {
            return Err(CodeCacheError::Format(format));
        }
        let engine_fp = u64::from_le_bytes(r.array()?);
        let count = u32::from_le_bytes(r.array()?);
        let mut per_module = BTreeMap::new();
        for _ in 0..count {
            let spec = r.chunk()?;
            let spec = std::str::from_utf8(spec)
                .map_err(|_| CodeCacheError::Specifier)?
                .to_string();
            let blob = r.chunk()?.to_vec();
            if per_module.contains_key(&spec) {
                return Err(CodeCacheError::Duplicate(spec));
            }
            per_module.insert(spec, blob);
        }
        if !r.0.is_empty() {
            return Err(CodeCacheError::Trailing(r.0.len()));
        }
        Ok(Self { engine_fp, per_module })
    }
}

/// Module count, specifier and blob lengths are `u32` (a V8 code cache is far below 4 GiB).
fn len_u32(n: usize) -> u32 {
    u32::try_from(n).expect("code cache section larger than 4 GiB")
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodeCacheError> {
        if self.0.len() < n {
            return Err(CodeCacheError::Truncated);
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodeCacheError> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    /// A `u32`-length-prefixed byte string.
    fn chunk(&mut self) -> Result<&'a [u8], CodeCacheError> {
        let n = u32::from_le_bytes(self.array()?) as usize;
        self.take(n)
    }
}

/// Compiles all modules (no evaluation) in a throwaway isolate from the base snapshot and
/// returns the V8 code cache (stage 6).
pub trait CodeCacheProducer: Send + Sync {
    fn produce(&self, bundle: &Bundle) -> Result<CodeCache, BuildFailure>;
    fn engine_fp(&self) -> u64;
}

/// `Engine::precompile_component`, bound to the build threads (never a process-wide rayon pool
/// sized to all cores, RP-01).
pub trait ComponentPrecompiler: Send + Sync {
    fn precompile(&self, wasm: &[u8]) -> Result<Vec<u8>, BuildFailure>;
    fn engine_fp(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CodeCache {
        let mut cc = CodeCache {
            engine_fp: 0x0123_4567_89ab_cdef,
            per_module: BTreeMap::new(),
        };
        cc.per_module
            .insert("file:///main.js".into(), (0..=255u8).cycle().take(4096).collect());
        cc.per_module.insert("https://esm.sh/zod@3/ünïcode.mjs".into(), vec![]);
        cc.per_module.insert("file:///b.js".into(), vec![0, 1, 2]);
        cc
    }

    #[test]
    fn roundtrips() {
        let cc = sample();
        assert_eq!(CodeCache::from_bytes(&cc.to_bytes()).expect("parse"), cc);
        let empty = CodeCache::default();
        assert_eq!(CodeCache::from_bytes(&empty.to_bytes()).expect("parse"), empty);
    }

    #[test]
    fn the_format_is_binary_with_a_magic_and_version_and_no_per_byte_overhead() {
        let cc = sample();
        let bytes = cc.to_bytes();
        assert!(bytes.starts_with(b"RTCC\x01"), "magic + format version first");
        let payload: usize = cc.per_module.iter().map(|(k, v)| k.len() + v.len()).sum();
        assert!(
            bytes.len() <= payload + 64,
            "{} bytes for {payload} bytes of payload",
            bytes.len()
        );
    }

    #[test]
    fn the_old_json_format_is_rejected_as_unparsable() {
        let json = serde_json::to_vec(&sample()).expect("json");
        assert!(
            CodeCache::from_bytes(&json).is_err(),
            "pre-binary T2 entries are a cache miss"
        );
    }

    #[test]
    fn an_unknown_format_version_is_rejected() {
        let mut bytes = sample().to_bytes();
        bytes[4] = 2;
        assert!(CodeCache::from_bytes(&bytes).is_err());
    }

    #[test]
    fn truncated_or_padded_input_is_rejected_without_panicking() {
        let bytes = sample().to_bytes();
        for n in 0..bytes.len() {
            assert!(
                CodeCache::from_bytes(&bytes[..n]).is_err(),
                "prefix of {n} bytes parsed"
            );
        }
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(CodeCache::from_bytes(&padded).is_err(), "trailing bytes");
        assert!(CodeCache::from_bytes(b"garbage").is_err());
    }

    #[test]
    fn huge_declared_lengths_do_not_allocate() {
        let mut bytes = b"RTCC\x01".to_vec();
        bytes.extend_from_slice(&7u64.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // module count
        bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // specifier length
        assert!(CodeCache::from_bytes(&bytes).is_err());
    }

    #[test]
    fn a_module_listed_twice_is_rejected() {
        let mut one = CodeCache {
            engine_fp: 7,
            per_module: BTreeMap::new(),
        };
        one.per_module.insert("file:///a.js".into(), vec![1, 2]);
        let bytes = one.to_bytes();
        // Header: magic, format, engine_fp, count. The one entry follows it; repeat it under count 2.
        let header = CODE_CACHE_MAGIC.len() + 1 + 8;
        let entry = bytes[header + 4..].to_vec();
        let mut dup = bytes[..header].to_vec();
        dup.extend_from_slice(&2u32.to_le_bytes());
        dup.extend_from_slice(&entry);
        dup.extend_from_slice(&entry);
        assert_eq!(
            CodeCache::from_bytes(&dup),
            Err(CodeCacheError::Duplicate("file:///a.js".into()))
        );
    }
}
