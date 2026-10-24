//! rt-sec cryptography (SECURITY.md, SEC-21): master key + HKDF purpose separation, API key
//! issue/parse/hash/verify, AES-256-GCM secrets with `tenant|service|name` AAD, node-keyed MAC over
//! compiled artifacts.
//!
//! This code is implemented in the skeleton, so these tests run green today; known-answer vectors
//! (computed independently with Python's `hmac`/`hashlib`/`cryptography`) pin the on-disk formats.

use std::collections::HashSet;

use rt_sec::{
    format_key, generate_secret, parse_key, purpose, secret_aad, ArtifactMac, HkdfArtifactMac, KeyHasher,
    KeyParseError, KeySpec, LocalSecrets, Mac, MasterKey, MasterKeyError, Sealed, SecretError, SecretsBackend,
    KEY_PREFIX, SEALED_V1, SECRET_LEN,
};
use rt_types::{Digest, KeyId, PreparedKey, ProjectId, RuntimeKind, Scope, Scopes, ServiceId, TenantId};

const SEQ_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn seq_master() -> MasterKey {
    MasterKey::from_hex(SEQ_HEX).unwrap()
}

fn zero_master() -> MasterKey {
    MasterKey::from_bytes([0u8; 32])
}

fn hex_of(d: &[u8]) -> String {
    hex::encode(d)
}

// ---------------------------------------------------------------------------------------------
// Master key + HKDF
// ---------------------------------------------------------------------------------------------

#[test]
fn master_key_hex_parsing() {
    assert!(MasterKey::from_hex(SEQ_HEX).is_ok());
    assert!(
        MasterKey::from_hex(&SEQ_HEX.to_uppercase()).is_ok(),
        "upper-case hex is fine"
    );
    assert!(
        MasterKey::from_hex(&format!("  {SEQ_HEX}\n")).is_ok(),
        "surrounding whitespace is trimmed (key files end in a newline)"
    );
    let plus_one = format!("{SEQ_HEX}0");
    let plus_two = format!("{SEQ_HEX}00");
    let not_hex = "zz".repeat(32);
    let last_not_hex = format!("{}g", &SEQ_HEX[..63]);
    let cases: Vec<(&str, MasterKeyError)> = vec![
        ("", MasterKeyError::BadLength),
        ("00", MasterKeyError::BadLength),
        (&SEQ_HEX[..62], MasterKeyError::BadLength),
        (&SEQ_HEX[..63], MasterKeyError::BadLength),
        (plus_one.as_str(), MasterKeyError::BadLength),
        (plus_two.as_str(), MasterKeyError::BadLength),
        (not_hex.as_str(), MasterKeyError::BadHex),
        (last_not_hex.as_str(), MasterKeyError::BadHex),
    ];
    for (input, want) in cases {
        assert_eq!(MasterKey::from_hex(input).err(), Some(want), "input `{input}`");
    }
}

#[test]
fn master_key_hex_and_bytes_constructors_agree() {
    let bytes: [u8; 32] = std::array::from_fn(|i| i as u8);
    let a = MasterKey::from_bytes(bytes);
    let b = seq_master();
    for p in [purpose::KEY_PEPPER, purpose::ARTIFACT_MAC, purpose::SECRETS] {
        assert_eq!(*a.derive(p), *b.derive(p));
    }
}

#[test]
fn hkdf_derivation_matches_known_answer_vectors() {
    // HKDF-SHA256, salt = none, info = purpose string, 32 bytes. Changing any of these silently
    // invalidates every stored key hash / secret / artifact MAC.
    let zero = zero_master();
    assert_eq!(
        hex_of(&*zero.derive(purpose::KEY_PEPPER)),
        "d4041f81c5b7f30204819ef3c9be89e98735d039cdcc6037c7b3df1bb7931e3a"
    );
    assert_eq!(
        hex_of(&*zero.derive(purpose::ARTIFACT_MAC)),
        "b1a32f34f810cce4c92925e6a359d9625454213714967f23e5395365525e6eab"
    );
    assert_eq!(
        hex_of(&*zero.derive(purpose::SECRETS)),
        "3323184adfc53cdd259b5419f6c5ca32c8763e4515a44a6656ae6d2a9a5d3476"
    );

    let seq = seq_master();
    assert_eq!(
        hex_of(&*seq.derive(purpose::KEY_PEPPER)),
        "5c52186b4a32891b59b171402591450b03b713e115a3b5b89fe6ef73eeae1aec"
    );
    assert_eq!(
        hex_of(&*seq.derive(purpose::ARTIFACT_MAC)),
        "a5334e1186f06a6e60c64fb4c0d69e1eadee18da70a1d73a599fcfd2cd407e06"
    );
    assert_eq!(
        hex_of(&*seq.derive(purpose::SECRETS)),
        "b1e2d0cdfb3d0d5ee2fd5f8b4b310d8dc32b28b577897d57d2692cdd7cab7300"
    );
}

#[test]
fn derived_keys_are_purpose_separated_and_never_the_master_itself() {
    let m = seq_master();
    let keys = [
        m.derive(purpose::KEY_PEPPER),
        m.derive(purpose::ARTIFACT_MAC),
        m.derive(purpose::SECRETS),
        m.derive("rt/something-else/v1"),
    ];
    let distinct: HashSet<[u8; 32]> = keys.iter().map(|k| **k).collect();
    assert_eq!(distinct.len(), keys.len(), "each purpose derives its own key");
    let master_bytes: [u8; 32] = std::array::from_fn(|i| i as u8);
    assert!(keys.iter().all(|k| **k != master_bytes));
    assert_eq!(
        *m.derive(purpose::SECRETS),
        *m.derive(purpose::SECRETS),
        "derivation is deterministic"
    );
    assert_ne!(
        *m.derive(purpose::SECRETS),
        *zero_master().derive(purpose::SECRETS),
        "different masters derive different keys"
    );
}

#[test]
fn generated_master_keys_are_random() {
    let a = MasterKey::generate();
    let b = MasterKey::generate();
    assert_ne!(*a.derive(purpose::SECRETS), *b.derive(purpose::SECRETS));
    assert_ne!(*a.derive(purpose::SECRETS), *zero_master().derive(purpose::SECRETS));
}

#[test]
fn master_key_debug_never_prints_key_material() {
    let dbg = format!("{:?}", seq_master());
    assert!(dbg.contains("redacted"), "{dbg}");
    assert!(
        !dbg.contains("0001") && !dbg.contains("1f") && !dbg.contains(SEQ_HEX),
        "{dbg}"
    );
}

// ---------------------------------------------------------------------------------------------
// API key format
// ---------------------------------------------------------------------------------------------

#[test]
fn generated_secrets_are_43_base62_characters_and_unique() {
    let mut seen = HashSet::new();
    for _ in 0..2_000 {
        let s = generate_secret();
        assert_eq!(s.len(), SECRET_LEN);
        assert_eq!(SECRET_LEN, 43, "256 bits in base62");
        assert!(s.bytes().all(|b| b.is_ascii_alphanumeric()), "`{s}` is not base62");
        assert!(seen.insert(s), "duplicate secret");
    }
}

#[test]
fn generated_secrets_use_all_62_characters_without_visible_bias() {
    // 1,442 secrets x 43 chars = 62,006 draws; expected 1,000 per symbol, sigma ~ 31.
    let mut counts = std::collections::BTreeMap::<char, u32>::new();
    for _ in 0..1_442 {
        for c in generate_secret().chars() {
            *counts.entry(c).or_default() += 1;
        }
    }
    assert_eq!(counts.len(), 62, "all of 0-9 A-Z a-z must occur");
    for (c, n) in counts {
        assert!(
            (750..=1250).contains(&n),
            "symbol `{c}` appeared {n} times (expected ~1000): modulo bias?"
        );
    }
}

#[test]
fn key_format_is_rtk_underscore_bare_ulid_underscore_secret() {
    let id = KeyId::new();
    let secret = generate_secret();
    let key = format_key(id, &secret);
    assert_eq!(KEY_PREFIX, "rtk");
    assert_eq!(key, format!("rtk_{}_{}", id.bare(), secret));
    assert_eq!(key.len(), 3 + 1 + 26 + 1 + 43);
    assert!(
        !key.contains("key_"),
        "the embedded id is the bare ULID without its `key_` prefix"
    );
}

#[test]
fn parse_key_round_trips_what_format_key_writes() {
    for _ in 0..200 {
        let (id, secret) = (KeyId::new(), generate_secret());
        let key = format_key(id, &secret);
        let parsed = parse_key(&key).expect("own output parses");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.secret, secret);
    }
}

#[test]
fn a_parsed_key_never_debug_prints_its_secret() {
    let (id, secret) = (KeyId::new(), generate_secret());
    let key = format_key(id, &secret);
    let shown = format!("{:?}", parse_key(&key).expect("parses"));
    assert!(shown.contains(&id.to_string()) && !shown.contains(&secret), "{shown}");
}

#[test]
fn parse_key_rejects_malformed_keys_with_the_right_error() {
    let id = KeyId::new();
    let secret = generate_secret();
    let good = format_key(id, &secret);
    assert!(parse_key(&good).is_ok());

    // not an rtk_ key at all
    let sk = good.replacen("rtk", "sk", 1);
    let leading_space = format!(" {good}");
    for s in ["", "invalid", "rtx_x_y", "RTK_a_b", sk.as_str(), leading_space.as_str()] {
        assert_eq!(parse_key(s).err(), Some(KeyParseError::NotAKey), "`{s}`");
    }
    assert!(parse_key("rtk").is_err());
    // bad id
    let too_short = id.bare()[..25].to_string();
    let too_long = format!("{}0", id.bare());
    let bangs = "!".repeat(26);
    for bad_id in [
        "",
        "short",
        "not-a-ulid-not-a-ulid-xx",
        too_short.as_str(),
        too_long.as_str(),
        bangs.as_str(),
    ] {
        let s = format!("rtk_{bad_id}_{secret}");
        assert_eq!(parse_key(&s).err(), Some(KeyParseError::BadId), "id `{bad_id}`");
    }
    // the `key_`-prefixed id is not the embedded form
    assert_eq!(
        parse_key(&format!("rtk_{id}_{secret}")).err(),
        Some(KeyParseError::BadId)
    );
    // bad secret: wrong length or alphabet
    for bad_secret in [
        String::new(),
        secret[..42].to_string(),
        format!("{secret}x"),
        format!("{}-", &secret[..42]),
        format!("{}_", &secret[..42]),
        format!("{} ", &secret[..42]),
        format!("{}é", &secret[..41]),
    ] {
        let s = format!("rtk_{}_{bad_secret}", id.bare());
        assert_eq!(
            parse_key(&s).err(),
            Some(KeyParseError::BadSecret),
            "secret `{bad_secret}`"
        );
    }
    // no secret at all
    assert!(parse_key(&format!("rtk_{}", id.bare())).is_err());
}

// ---------------------------------------------------------------------------------------------
// Key hashing (HMAC-SHA256 with the derived pepper)
// ---------------------------------------------------------------------------------------------

const KAT_SECRET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";

#[test]
fn key_hash_matches_known_answer() {
    // HMAC-SHA256(HKDF(master, "rt/api-key-pepper/v1"), secret), computed with Python.
    let h = KeyHasher::new(&seq_master()).hash(KAT_SECRET);
    assert_eq!(
        hex_of(&h),
        "ce59054370872b4eb5de2babc25be21f2f4f45dd35ac50c229a2fecb23d25328"
    );
    assert_eq!(h.len(), 32);
}

#[test]
fn key_hash_is_deterministic_secret_dependent_and_pepper_dependent() {
    let hasher = KeyHasher::new(&seq_master());
    let s = generate_secret();
    assert_eq!(hasher.hash(&s), hasher.hash(&s));
    assert_eq!(
        hasher.hash(&s),
        KeyHasher::new(&seq_master()).hash(&s),
        "same master => same pepper after a restart"
    );
    assert_ne!(hasher.hash(&s), hasher.hash(&generate_secret()));
    assert_ne!(
        hasher.hash(&s),
        KeyHasher::new(&zero_master()).hash(&s),
        "a different master key must not verify old hashes"
    );
    assert_ne!(hasher.hash("a"), hasher.hash("b"));
    assert_ne!(hasher.hash(&s), hasher.hash(&format!("{s} ")));
}

#[test]
fn verify_accepts_the_right_secret_and_rejects_everything_else() {
    let hasher = KeyHasher::new(&seq_master());
    let secret = generate_secret();
    let stored = hasher.hash(&secret);
    assert!(hasher.verify(&secret, &stored));

    // wrong secret
    assert!(!hasher.verify(&generate_secret(), &stored));
    // every single-character change of the right secret is rejected
    for i in 0..secret.len() {
        let mut bytes = secret.clone().into_bytes();
        bytes[i] = if bytes[i] == b'A' { b'B' } else { b'A' };
        let wrong = String::from_utf8(bytes).unwrap();
        assert!(
            !hasher.verify(&wrong, &stored),
            "secret differing at position {i} must be rejected"
        );
    }
    // prefix / suffix of the right secret
    assert!(!hasher.verify(&secret[..42], &stored));
    assert!(!hasher.verify(&format!("{secret}0"), &stored));
    assert!(!hasher.verify("", &stored));
    // a different node (other master key) cannot verify it
    assert!(!KeyHasher::new(&zero_master()).verify(&secret, &stored));
}

#[test]
fn verify_rejects_stored_hashes_of_the_wrong_shape_and_flipped_bits() {
    let hasher = KeyHasher::new(&seq_master());
    let secret = generate_secret();
    let stored = hasher.hash(&secret);

    assert!(!hasher.verify(&secret, &[]), "empty stored hash");
    assert!(!hasher.verify(&secret, &stored[..31]), "truncated stored hash");
    let mut longer = stored.clone();
    longer.push(0);
    assert!(!hasher.verify(&secret, &longer), "extended stored hash");
    assert!(!hasher.verify(&secret, &[0u8; 32]));
    // a mismatch at the first byte and at the last byte are both rejected: the whole hash is compared
    for byte in 0..stored.len() {
        for bit in [0u8, 7] {
            let mut flipped = stored.clone();
            flipped[byte] ^= 1 << bit;
            assert!(
                !hasher.verify(&secret, &flipped),
                "flip of byte {byte} bit {bit} accepted"
            );
        }
    }
}

#[test]
fn issue_returns_a_one_time_plaintext_and_a_store_record_that_verifies_it() {
    let hasher = KeyHasher::new(&seq_master());
    let svc = ProjectId::new();
    let scopes: Scopes = [Scope::Deploy, Scope::LogsRead].into_iter().collect();
    let spec = KeySpec {
        name: Some("ci".into()),
        scopes: scopes.clone(),
        project_ids: Some(vec![svc]),
        environments: Some(vec!["production".into()]),
        expires_at_ms: Some(1_900_000_000_000),
    };
    let (secret, new) = hasher.issue(spec);

    let parsed = parse_key(secret.expose()).expect("plaintext key parses");
    assert_eq!(parsed.id, new.id, "the id embedded in the key is the stored id");
    assert!(
        hasher.verify(parsed.secret, &new.hmac),
        "stored hmac must verify the issued secret"
    );
    assert_eq!(new.hmac, hasher.hash(parsed.secret));
    assert_eq!(new.hmac.len(), 32);
    assert_ne!(
        new.hmac,
        parsed.secret.as_bytes(),
        "the secret itself must not be what is stored"
    );

    assert_eq!(new.name.as_deref(), Some("ci"));
    assert_eq!(new.scopes, scopes);
    assert_eq!(new.project_ids, Some(vec![svc]));
    assert_eq!(new.environments, Some(vec!["production".to_string()]));
    assert_eq!(new.expires_at_ms, Some(1_900_000_000_000));

    // tampering with one character of the issued key no longer verifies
    let mut bytes = parsed.secret.as_bytes().to_vec();
    bytes[10] = if bytes[10] == b'x' { b'y' } else { b'x' };
    assert!(!hasher.verify(std::str::from_utf8(&bytes).unwrap(), &new.hmac));
}

#[test]
fn issued_keys_are_unique_and_unrestricted_by_default() {
    let hasher = KeyHasher::new(&seq_master());
    let spec = || KeySpec {
        name: None,
        scopes: Scopes::default(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
    };
    let mut ids = HashSet::new();
    let mut plaintexts = HashSet::new();
    for _ in 0..500 {
        let (secret, new) = hasher.issue(spec());
        assert!(ids.insert(new.id));
        assert!(plaintexts.insert(secret.expose().to_string()));
        assert!(new.name.is_none() && new.project_ids.is_none() && new.expires_at_ms.is_none());
        assert!(new.scopes.iter().next().is_none());
    }
}

#[test]
fn key_secret_debug_is_redacted() {
    let hasher = KeyHasher::new(&seq_master());
    let (secret, _) = hasher.issue(KeySpec {
        name: None,
        scopes: Scopes::all(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
    });
    let dbg = format!("{secret:?}");
    assert!(dbg.contains("redacted"), "{dbg}");
    assert!(!dbg.contains(secret.expose()) && !dbg.contains("rtk_"), "{dbg}");
}

// ---------------------------------------------------------------------------------------------
// Secrets at rest: AES-256-GCM
// ---------------------------------------------------------------------------------------------

fn ids() -> (TenantId, ServiceId) {
    (TenantId::from_u128(1), ServiceId::from_u128(2))
}

#[test]
fn secret_aad_is_tenant_pipe_service_pipe_name() {
    let (t, s) = ids();
    assert_eq!(
        secret_aad(t, s, "DATABASE_URL"),
        b"ten_00000000000000000000000001|svc_00000000000000000000000002|DATABASE_URL".to_vec()
    );
    assert_ne!(secret_aad(t, s, "A"), secret_aad(t, s, "B"));
    assert_ne!(secret_aad(t, s, "A"), secret_aad(TenantId::from_u128(3), s, "A"));
    assert_ne!(secret_aad(t, s, "A"), secret_aad(t, ServiceId::from_u128(3), "A"));
    assert_eq!(
        secret_aad(t, s, ""),
        b"ten_00000000000000000000000001|svc_00000000000000000000000002|".to_vec()
    );
}

#[test]
fn opens_a_known_answer_ciphertext_produced_by_an_independent_implementation() {
    // AES-256-GCM, key = HKDF(master 00..1f, "rt/secrets-aes-gcm/v1"), nonce 64..6f, AAD as above,
    // produced with Python's `cryptography` AESGCM.
    let (t, s) = ids();
    let sealed = Sealed {
        nonce: std::array::from_fn(|i| 100 + i as u8),
        ciphertext: hex::decode("a5cf2c3916eb3cc1c6e8c064c8bf785c0318e5adb452e16ae3dc7ea381a0f3df35cbb87477ba30c48e")
            .unwrap(),
        // the legacy storage format (no version byte, no key id): still opened (SEC-53)
        key_id: None,
    };
    let secrets = LocalSecrets::new(&seq_master());
    let pt = secrets
        .open(&secret_aad(t, s, "DATABASE_URL"), &sealed)
        .expect("known-answer ciphertext must open");
    assert_eq!(pt.as_slice(), b"postgres://user:pw@db/app");
    // the same ciphertext under another name / other master does not
    assert_eq!(
        secrets.open(&secret_aad(t, s, "OTHER"), &sealed).err(),
        Some(SecretError::Decrypt)
    );
    assert_eq!(
        LocalSecrets::new(&zero_master())
            .open(&secret_aad(t, s, "DATABASE_URL"), &sealed)
            .err(),
        Some(SecretError::Decrypt)
    );
}

#[test]
fn seal_open_round_trip_for_assorted_plaintexts() {
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let aad = secret_aad(t, s, "API_KEY");
    let big = vec![0xA5u8; 1024 * 1024];
    let cases: [&[u8]; 6] = [
        b"",
        b"x",
        b"hello world",
        "pässwörd-日本語".as_bytes(),
        &[0u8, 1, 2, 255, 254],
        &big,
    ];
    for pt in cases {
        let sealed = secrets.seal(&aad, pt);
        assert_eq!(
            sealed.ciphertext.len(),
            pt.len() + 16,
            "ciphertext = plaintext + 16 byte tag"
        );
        let opened = secrets.open(&aad, &sealed).expect("round trip");
        assert_eq!(opened.as_slice(), pt);
    }
}

#[test]
fn sealing_is_randomized_and_does_not_leak_the_plaintext() {
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let aad = secret_aad(t, s, "TOKEN");
    let pt = b"super-secret-token-value-123456";
    let mut nonces = HashSet::new();
    let mut ciphertexts = HashSet::new();
    for _ in 0..1_000 {
        let sealed = secrets.seal(&aad, pt);
        assert!(nonces.insert(sealed.nonce), "nonce reuse breaks AES-GCM");
        assert!(ciphertexts.insert(sealed.ciphertext.clone()));
        assert!(
            !sealed.ciphertext.windows(pt.len()).any(|w| w == pt),
            "ciphertext contains the plaintext"
        );
        assert!(!sealed.to_bytes().windows(8).any(|w| pt.windows(8).any(|p| p == w)));
    }
}

#[test]
fn opening_with_a_different_aad_fails() {
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let sealed = secrets.seal(&secret_aad(t, s, "TOKEN"), b"value");
    let wrong: Vec<(&str, Vec<u8>)> = vec![
        ("other name", secret_aad(t, s, "TOKEN2")),
        (
            "other service (value moved between services)",
            secret_aad(t, ServiceId::from_u128(99), "TOKEN"),
        ),
        ("other tenant", secret_aad(TenantId::from_u128(99), s, "TOKEN")),
        ("empty aad", Vec::new()),
        ("truncated aad", secret_aad(t, s, "TOKEN")[..20].to_vec()),
    ];
    for (what, aad) in wrong {
        assert_eq!(secrets.open(&aad, &sealed).err(), Some(SecretError::Decrypt), "{what}");
    }
    assert!(secrets.open(&secret_aad(t, s, "TOKEN"), &sealed).is_ok());
}

#[test]
fn opening_with_a_different_master_key_fails() {
    let (t, s) = ids();
    let aad = secret_aad(t, s, "TOKEN");
    let sealed = LocalSecrets::new(&seq_master()).seal(&aad, b"value");
    assert_eq!(
        LocalSecrets::new(&zero_master()).open(&aad, &sealed).err(),
        Some(SecretError::Decrypt)
    );
    assert_eq!(
        LocalSecrets::new(&MasterKey::generate()).open(&aad, &sealed).err(),
        Some(SecretError::Decrypt)
    );
    // a restart with the same master key still opens it
    assert!(LocalSecrets::new(&seq_master()).open(&aad, &sealed).is_ok());
}

#[test]
fn any_modification_of_the_sealed_value_is_detected() {
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let aad = secret_aad(t, s, "TOKEN");
    let sealed = secrets.seal(&aad, b"a reasonably long secret value to tamper with");

    // flip every bit of the nonce and of the ciphertext+tag
    for byte in 0..sealed.nonce.len() {
        let mut bad = sealed.clone();
        bad.nonce[byte] ^= 0x01;
        assert_eq!(
            secrets.open(&aad, &bad).err(),
            Some(SecretError::Decrypt),
            "nonce byte {byte}"
        );
    }
    for byte in 0..sealed.ciphertext.len() {
        for bit in 0..8 {
            let mut bad = sealed.clone();
            bad.ciphertext[byte] ^= 1 << bit;
            assert_eq!(
                secrets.open(&aad, &bad).err(),
                Some(SecretError::Decrypt),
                "ciphertext byte {byte} bit {bit}"
            );
        }
    }
    let mut truncated = sealed.clone();
    truncated.ciphertext.pop();
    assert_eq!(secrets.open(&aad, &truncated).err(), Some(SecretError::Decrypt));
    let mut extended = sealed.clone();
    extended.ciphertext.push(0);
    assert_eq!(secrets.open(&aad, &extended).err(), Some(SecretError::Decrypt));
    let mut empty = sealed;
    empty.ciphertext.clear();
    assert_eq!(secrets.open(&aad, &empty).err(), Some(SecretError::Decrypt));
}

#[test]
fn sealed_storage_form_is_version_key_id_nonce_then_ciphertext() {
    // SEC-53: was `nonce ‖ ciphertext`; sealing now always writes the v1 form.
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let aad = secret_aad(t, s, "TOKEN");
    let sealed = secrets.seal(&aad, b"value");
    let bytes = sealed.to_bytes();
    assert_eq!(bytes.len(), 1 + 4 + 12 + 5 + 16);
    assert_eq!(bytes[0], SEALED_V1);
    assert_eq!(&bytes[1..5], &secrets.key_id());
    assert_eq!(&bytes[5..17], &sealed.nonce);
    assert_eq!(&bytes[17..], &sealed.ciphertext[..]);
    let back = Sealed::from_bytes(&bytes).expect("parses");
    assert_eq!(back, sealed);
    assert_eq!(secrets.open(&aad, &back).unwrap().as_slice(), b"value");
}

// ---------------------------------------------------------------------------------------------
// SEC-53: versioned sealed format, legacy values, key rotation
// ---------------------------------------------------------------------------------------------

fn other_master() -> MasterKey {
    MasterKey::from_bytes([0x42; 32])
}

/// A value in the legacy format (`nonce ‖ ciphertext`, AAD without header), as written before
/// SEC-53, sealed under `master` with an explicit nonce.
fn legacy_blob(master: &MasterKey, aad: &[u8], nonce: [u8; 12], pt: &[u8]) -> Vec<u8> {
    use aes_gcm::aead::{Aead, KeyInit, Payload};
    let key = master.derive(purpose::SECRETS);
    let cipher = aes_gcm::Aes256Gcm::new_from_slice(&key[..]).unwrap();
    let ct = cipher
        .encrypt(aes_gcm::Nonce::from_slice(&nonce), Payload { msg: pt, aad })
        .unwrap();
    [nonce.to_vec(), ct].concat()
}

#[test]
fn ac_sec_53_legacy_values_still_open_including_nonces_that_start_with_the_version_byte() {
    let (t, s) = ids();
    let aad = secret_aad(t, s, "DATABASE_URL");
    let secrets = LocalSecrets::new(&seq_master());
    for first in [0x00u8, SEALED_V1, 0x02, 0xff] {
        let nonce: [u8; 12] = std::array::from_fn(|i| if i == 0 { first } else { 7 + i as u8 });
        let blob = legacy_blob(&seq_master(), &aad, nonce, b"postgres://legacy");
        let parsed = Sealed::from_bytes(&blob).expect("parses");
        assert_eq!(
            parsed.key_id.is_some(),
            first == SEALED_V1,
            "a leading 0x01 is read as v1 first"
        );
        assert_eq!(
            secrets.open(&aad, &parsed).expect("legacy opens").as_slice(),
            b"postgres://legacy",
            "nonce[0] = {first:#x}"
        );
        assert!(
            !secrets.is_current(&parsed) || first == SEALED_V1,
            "legacy values need a re-seal"
        );
    }
}

#[test]
fn ac_sec_53_a_previous_master_key_opens_old_values_and_new_values_use_the_current_key() {
    let (t, s) = ids();
    let aad = secret_aad(t, s, "TOKEN");
    let old = LocalSecrets::new(&other_master());
    let v1_old = old.seal(&aad, b"sealed before rotation");
    let legacy_old =
        Sealed::from_bytes(&legacy_blob(&other_master(), &aad, [9; 12], b"legacy before rotation")).unwrap();

    let rotated = LocalSecrets::new(&seq_master()).with_previous(&other_master());
    assert_ne!(
        rotated.key_id(),
        old.key_id(),
        "different masters have different key ids"
    );
    assert_eq!(
        rotated.open(&aad, &v1_old).unwrap().as_slice(),
        b"sealed before rotation"
    );
    assert_eq!(
        rotated.open(&aad, &legacy_old).unwrap().as_slice(),
        b"legacy before rotation"
    );
    assert!(
        !rotated.is_current(&v1_old) && !rotated.is_current(&legacy_old),
        "both need a re-seal"
    );

    let resealed = rotated.seal(&aad, b"sealed before rotation");
    assert_eq!(
        resealed.key_id,
        Some(rotated.key_id()),
        "sealing always uses the current key"
    );
    assert!(rotated.is_current(&resealed));
    // Once the previous key is dropped, only re-sealed values open.
    let current_only = LocalSecrets::new(&seq_master());
    assert_eq!(
        current_only.open(&aad, &resealed).unwrap().as_slice(),
        b"sealed before rotation"
    );
    assert_eq!(current_only.open(&aad, &v1_old).err(), Some(SecretError::Decrypt));
    assert_eq!(current_only.open(&aad, &legacy_old).err(), Some(SecretError::Decrypt));
}

#[test]
fn ac_sec_53_the_header_is_authenticated() {
    let (t, s) = ids();
    let aad = secret_aad(t, s, "TOKEN");
    let secrets = LocalSecrets::new(&seq_master()).with_previous(&other_master());
    let sealed = secrets.seal(&aad, b"value");
    let bytes = sealed.to_bytes();
    // Another key id (even the previous key's), or the header stripped down to the legacy form.
    let mut relabelled = bytes.clone();
    relabelled[1..5].copy_from_slice(&LocalSecrets::new(&other_master()).key_id());
    let mut flipped = bytes.clone();
    flipped[2] ^= 0x01;
    let stripped = bytes[5..].to_vec();
    for (what, b) in [
        ("relabelled", relabelled),
        ("flipped key id", flipped),
        ("stripped header", stripped),
    ] {
        let parsed = Sealed::from_bytes(&b).expect("structurally valid");
        assert_eq!(secrets.open(&aad, &parsed).err(), Some(SecretError::Decrypt), "{what}");
    }
    assert_eq!(
        secrets
            .open(&aad, &Sealed::from_bytes(&bytes).unwrap())
            .unwrap()
            .as_slice(),
        b"value"
    );
}

#[test]
fn ac_sec_53_api_keys_issued_before_a_rotation_keep_verifying_with_the_previous_key() {
    let old = KeyHasher::new(&other_master());
    let (secret, new) = old.issue(KeySpec {
        name: None,
        scopes: Scopes::default(),
        project_ids: None,
        environments: None,
        expires_at_ms: None,
    });
    let parsed = parse_key(secret.expose()).expect("parses");
    assert!(old.verify(parsed.secret, &new.hmac));
    assert!(
        !KeyHasher::new(&seq_master()).verify(parsed.secret, &new.hmac),
        "a rotated pepper alone rejects it"
    );
    let rotated = KeyHasher::new(&seq_master()).with_previous(&other_master());
    assert!(
        rotated.verify(parsed.secret, &new.hmac),
        "the previous pepper still verifies it"
    );
    assert!(!rotated.verify("wrong-secret", &new.hmac));
    assert_eq!(
        rotated.hash("x"),
        KeyHasher::new(&seq_master()).hash("x"),
        "new hashes use the current pepper"
    );
}

#[test]
fn malformed_storage_blobs_are_rejected_without_panicking() {
    for len in [0usize, 1, 11, 12, 27] {
        assert_eq!(
            Sealed::from_bytes(&vec![7u8; len]),
            Err(SecretError::Malformed),
            "{len} bytes cannot hold nonce + tag"
        );
    }
    let min = Sealed::from_bytes(&[7u8; 28]).expect("12 byte nonce + 16 byte tag is the minimum structure");
    assert_eq!(min.ciphertext.len(), 16);
    // structurally valid garbage fails at decrypt, not at parse
    let (t, s) = ids();
    assert_eq!(
        LocalSecrets::new(&seq_master())
            .open(&secret_aad(t, s, "X"), &min)
            .err(),
        Some(SecretError::Decrypt)
    );
}

#[test]
fn secret_errors_do_not_reveal_why_decryption_failed() {
    // wrong key, wrong aad and tampering all produce the same error value and message
    let (t, s) = ids();
    let secrets = LocalSecrets::new(&seq_master());
    let sealed = secrets.seal(&secret_aad(t, s, "A"), b"v");
    let wrong_aad = secrets.open(&secret_aad(t, s, "B"), &sealed).err().unwrap();
    let wrong_key = LocalSecrets::new(&zero_master())
        .open(&secret_aad(t, s, "A"), &sealed)
        .err()
        .unwrap();
    assert_eq!(wrong_aad, wrong_key);
    assert!(!wrong_aad.to_string().contains('A'));
}

// ---------------------------------------------------------------------------------------------
// Artifact MAC (SEC-21)
// ---------------------------------------------------------------------------------------------

fn pkey(code: &[u8], kind: RuntimeKind, fp: u64) -> PreparedKey {
    PreparedKey {
        digest: Digest::of(code),
        kind,
        engine_fp: fp,
    }
}

fn payload() -> Vec<u8> {
    (0..96u8).collect()
}

/// The T2 file name every tag below is bound to (SEC-55).
const F: &str = "component.cwasm";

#[test]
fn artifact_mac_signs_and_verifies() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"module a", RuntimeKind::Wasm, 7);
    let bytes = payload();
    let tag = mac.sign(&key, F, &bytes);
    assert!(mac.verify(&key, F, &bytes, &tag));
    assert_eq!(mac.sign(&key, F, &bytes), tag, "signing is deterministic");
    // a restarted node with the same master key accepts files written before the restart
    assert!(HkdfArtifactMac::new(&seq_master()).verify(&key, F, &bytes, &tag));
    // empty payloads are signable too
    let empty = mac.sign(&key, F, b"");
    assert!(mac.verify(&key, F, b"", &empty));
    assert_ne!(empty, tag);
}

#[test]
fn sec_21_one_flipped_bit_anywhere_in_the_artifact_fails_verification() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"module a", RuntimeKind::Wasm, 7);
    let bytes = payload();
    let tag = mac.sign(&key, F, &bytes);
    for byte in 0..bytes.len() {
        for bit in 0..8 {
            let mut bad = bytes.clone();
            bad[byte] ^= 1 << bit;
            assert!(
                !mac.verify(&key, F, &bad, &tag),
                "flip of byte {byte} bit {bit} verified"
            );
        }
    }
}

#[test]
fn sec_21_truncated_extended_or_empty_artifacts_fail_verification() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"module a", RuntimeKind::Deno, 1);
    let bytes = payload();
    let tag = mac.sign(&key, F, &bytes);
    assert!(!mac.verify(&key, F, &bytes[..bytes.len() - 1], &tag));
    assert!(!mac.verify(&key, F, &bytes[1..], &tag));
    let mut extended = bytes.clone();
    extended.push(0);
    assert!(!mac.verify(&key, F, &extended, &tag));
    assert!(!mac.verify(&key, F, b"", &tag));
}

#[test]
fn sec_21_a_file_swapped_between_services_or_engines_fails_verification() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let bytes = payload();
    let a = pkey(b"service A bundle", RuntimeKind::Deno, 7);
    let tag = mac.sign(&a, F, &bytes);

    // identical bytes, but the entry belongs to another prepared key
    let swapped_digest = pkey(b"service B bundle", RuntimeKind::Deno, 7);
    let swapped_kind = pkey(b"service A bundle", RuntimeKind::Wasm, 7);
    let swapped_engine = pkey(b"service A bundle", RuntimeKind::Deno, 8);
    for (what, other) in [
        ("digest", swapped_digest),
        ("runtime kind", swapped_kind),
        ("engine fingerprint", swapped_engine),
    ] {
        assert!(!mac.verify(&other, F, &bytes, &tag), "tag must be bound to the {what}");
    }
    assert!(mac.verify(&a, F, &bytes, &tag));
    // and signing for each key yields a different tag over the same bytes
    let tags: HashSet<[u8; 32]> = [a, swapped_digest, swapped_kind, swapped_engine]
        .iter()
        .map(|k| mac.sign(k, F, &bytes).0)
        .collect();
    assert_eq!(tags.len(), 4);
}

#[test]
fn ac_sec_55_the_tag_is_bound_to_the_file_name() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"service A", RuntimeKind::Deno, 7);
    let bytes = payload();
    let tag = mac.sign(&key, "bundle", &bytes);
    for other in ["code_cache.bin", "bundle.mac", "Bundle", "bundl", "bundlee", ""] {
        assert!(
            !mac.verify(&key, other, &bytes, &tag),
            "a tag for `bundle` must not verify as `{other}`"
        );
    }
    assert!(mac.verify(&key, "bundle", &bytes, &tag));
    // Moving bytes between the name and the content does not produce the same tag.
    assert_ne!(mac.sign(&key, "ab", b"c"), mac.sign(&key, "a", b"bc"));
}

#[test]
fn ac_sec_55_v2_tags_use_their_own_key_and_a_stable_non_secret_key_id() {
    let master = seq_master();
    let mac = HkdfArtifactMac::new(&master);
    let key = pkey(b"x", RuntimeKind::Deno, 1);
    let bytes = payload();
    // A tag made the legacy way (v1 key, no file name) never verifies under v2.
    use hmac::Mac as _;
    let mut legacy =
        <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(&master.derive(purpose::ARTIFACT_MAC)[..]).unwrap();
    legacy.update(&key.digest.0);
    legacy.update(&[key.kind as u8]);
    legacy.update(&key.engine_fp.to_le_bytes());
    legacy.update(&(bytes.len() as u64).to_le_bytes());
    legacy.update(&bytes);
    let legacy_tag = Mac(legacy.finalize().into_bytes().into());
    assert!(!mac.verify(&key, F, &bytes, &legacy_tag));
    assert!(!mac.verify(&key, "", &bytes, &legacy_tag));

    assert_eq!(
        mac.key_id(),
        HkdfArtifactMac::new(&seq_master()).key_id(),
        "deterministic"
    );
    assert_ne!(
        mac.key_id(),
        HkdfArtifactMac::new(&zero_master()).key_id(),
        "differs per master key"
    );
    assert_eq!(mac.key_id().len(), 16);
    let derived = hex::encode(&master.derive(purpose::ARTIFACT_MAC_V2)[..]);
    assert!(!derived.contains(&mac.key_id()), "the id is not a prefix of the key");
}

#[test]
fn sec_21_a_tag_from_another_node_key_fails_verification() {
    let key = pkey(b"module a", RuntimeKind::Wasm, 7);
    let bytes = payload();
    let tag = HkdfArtifactMac::new(&seq_master()).sign(&key, F, &bytes);
    assert!(
        !HkdfArtifactMac::new(&zero_master()).verify(&key, F, &bytes, &tag),
        "key swap: another node's tag must not verify"
    );
    assert!(!HkdfArtifactMac::new(&MasterKey::generate()).verify(&key, F, &bytes, &tag));
}

#[test]
fn sec_21_every_bit_of_the_stored_tag_matters() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"module a", RuntimeKind::Wasm, 7);
    let bytes = payload();
    let tag = mac.sign(&key, F, &bytes);
    for byte in 0..32 {
        for bit in 0..8 {
            let mut bad = tag;
            bad.0[byte] ^= 1 << bit;
            assert!(!mac.verify(&key, F, &bytes, &bad), "tag byte {byte} bit {bit}");
        }
    }
    assert!(!mac.verify(&key, F, &bytes, &Mac([0u8; 32])));
    assert!(!mac.verify(&key, F, &bytes, &Mac([0xFF; 32])));
}

#[test]
fn artifact_mac_handles_large_payloads() {
    let mac = HkdfArtifactMac::new(&seq_master());
    let key = pkey(b"big", RuntimeKind::Wasm, 1);
    let mut bytes = vec![0x5Au8; 8 * 1024 * 1024];
    let tag = mac.sign(&key, F, &bytes);
    assert!(mac.verify(&key, F, &bytes, &tag));
    *bytes.last_mut().unwrap() ^= 1;
    assert!(!mac.verify(&key, F, &bytes, &tag));
}

#[test]
fn mac_key_is_independent_of_the_secrets_and_pepper_keys() {
    // The MAC must not be computable from (or usable as) the key-hash pepper: a stored API key hash
    // is never a valid artifact tag, even for matching input.
    let master = seq_master();
    let mac = HkdfArtifactMac::new(&master);
    let hasher = KeyHasher::new(&master);
    let key = pkey(b"x", RuntimeKind::Deno, 1);
    let hash = hasher.hash("whatever");
    let as_tag = Mac(hash.clone().try_into().unwrap());
    assert!(!mac.verify(&key, F, b"whatever", &as_tag));
}

/// SRV-002: a master key file other users can read is reported (the node warns at startup); a
/// 0600 file is fine.
#[cfg(unix)]
#[test]
fn srv_002_a_master_key_file_readable_by_others_is_reported() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("master.key");
    std::fs::write(&path, "00").expect("write");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let problem = rt_sec::key_file_mode_problem(&path).expect("0644 is a problem");
    assert!(problem.contains("644") && problem.contains("chmod 600"), "{problem}");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    assert_eq!(rt_sec::key_file_mode_problem(&path), None);
}
