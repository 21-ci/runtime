//! `age` encryption of an archive: public-key only, so the node cannot read what it wrote.
//!
//! The tar goes through the encryptor straight to the file: there is no plaintext archive on disk,
//! at any point. Reading decrypts on demand and seeks, so verify and restore write no plaintext tar
//! either. Encryption hides; it authenticates nothing, because recipients are public. That is the
//! job of `manifest.mac`.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use age::x25519::{Identity, Recipient};

use crate::archive::HashWriter;
use crate::{BackupError, Result};

const AGE_MAGIC: &[u8] = b"age-encryption.org/v1";

pub fn parse_recipients(keys: &[String]) -> Result<Vec<Recipient>> {
    keys.iter()
        .map(|k| {
            Recipient::from_str(k.trim())
                .map_err(|_| BackupError::new("bad_recipient", "backup.encrypt_to holds an invalid age public key"))
        })
        .collect()
}

/// `--age-identity`: the private keys that open archives. Never printed.
pub struct AgeIdentity(Vec<Identity>);

impl std::fmt::Debug for AgeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AgeIdentity(..)")
    }
}

impl AgeIdentity {
    /// An `age-keygen` file: comments and `AGE-SECRET-KEY-1...` lines.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = zeroize::Zeroizing::new(std::fs::read_to_string(path)?);
        let bad = || BackupError::new("bad_identity", "the age identity file holds no valid key");
        let keys = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| Identity::from_str(l).map_err(|_| bad()))
            .collect::<Result<Vec<_>>>()?;
        if keys.is_empty() {
            return Err(bad());
        }
        Ok(Self(keys))
    }
}

/// Where the tar bytes go. The digest is of what lands in the file.
pub(crate) enum Sink {
    Plain(HashWriter<BufWriter<File>>),
    Age(age::stream::StreamWriter<HashWriter<BufWriter<File>>>),
}

impl Sink {
    pub(crate) fn new(file: File, recipients: &[Recipient]) -> Result<Self> {
        let out = HashWriter::new(BufWriter::with_capacity(1 << 20, file));
        if recipients.is_empty() {
            return Ok(Self::Plain(out));
        }
        let enc = age::Encryptor::with_recipients(recipients.iter().map(|r| r as &dyn age::Recipient))
            .map_err(|_| BackupError::new("bad_recipient", "age refused the recipients"))?;
        Ok(Self::Age(enc.wrap_output(out)?))
    }

    /// The final age chunk is written here: a file without it does not decrypt.
    pub(crate) fn finish(self) -> Result<HashWriter<BufWriter<File>>> {
        Ok(match self {
            Self::Plain(w) => w,
            Self::Age(w) => w.finish()?,
        })
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(w) => w.write(buf),
            Self::Age(w) => w.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(w) => w.flush(),
            Self::Age(w) => w.flush(),
        }
    }
}

/// An archive being read: plain, or decrypted on demand with seeks.
pub(crate) enum Input {
    Plain(File),
    Age(Box<age::stream::StreamReader<BufReader<File>>>, Arc<AtomicBool>),
}

impl Input {
    pub(crate) fn open(path: &Path, identity: Option<&AgeIdentity>) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut head = [0u8; AGE_MAGIC.len()];
        let mut got = 0;
        while got < head.len() {
            match file.read(&mut head[got..])? {
                0 => break,
                n => got += n,
            }
        }
        file.seek(SeekFrom::Start(0))?;
        let encrypted = &head[..got] == AGE_MAGIC;
        match (encrypted, identity) {
            (false, None) => Ok(Self::Plain(file)),
            (false, Some(_)) => Err(BackupError::new(
                "not_encrypted",
                "--age-identity was given, but this archive is not encrypted",
            )),
            (true, None) => Err(BackupError::new(
                "age_identity_required",
                "this archive is encrypted: pass --age-identity",
            )),
            (true, Some(id)) => {
                let dec = age::Decryptor::new_buffered(BufReader::new(file))
                    .map_err(|_| BackupError::new("corrupt", "the age header is malformed"))?;
                let reader = dec
                    .decrypt(id.0.iter().map(|i| i as &dyn age::Identity))
                    .map_err(|e| match e {
                        age::DecryptError::NoMatchingKeys => {
                            BackupError::new("wrong_identity", "this identity does not open the archive")
                        }
                        _ => BackupError::new("corrupt", "the age header is malformed"),
                    })?;
                Ok(Self::Age(Box::new(reader), Arc::new(AtomicBool::new(false))))
            }
        }
    }

    /// A chunk that does not authenticate shows as an I/O error or a short tar below; name it for
    /// what it is. A cut inside a chunk looks the same as a changed one to AEAD.
    pub(crate) fn name_error(&self, e: BackupError) -> BackupError {
        match self {
            Self::Age(_, failed) if failed.load(Ordering::Relaxed) => {
                BackupError::new("corrupt", "the ciphertext does not authenticate")
            }
            _ => e,
        }
    }
}

impl Read for Input {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(f) => f.read(buf),
            Self::Age(r, failed) => {
                let got = r.read(buf);
                if got.as_ref().is_err_and(|e| e.kind() == io::ErrorKind::InvalidData) {
                    failed.store(true, Ordering::Relaxed);
                }
                got
            }
        }
    }
}

impl Seek for Input {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Plain(f) => f.seek(pos),
            Self::Age(r, failed) => {
                let got = r.seek(pos);
                if got.as_ref().is_err_and(|e| e.kind() == io::ErrorKind::InvalidData) {
                    failed.store(true, Ordering::Relaxed);
                }
                got
            }
        }
    }
}
