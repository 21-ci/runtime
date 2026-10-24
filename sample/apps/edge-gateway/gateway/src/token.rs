//! Verification of short-lived HS256 JWTs (RFC 7519 compact form, HMAC-SHA256 per RFC 7518).
//!
//! Order matters: the header is parsed only to pick the algorithm and key, the signature is checked
//! (in constant time) before the payload is parsed, and only then are the claims trusted.
//! Rejected outright: any `alg` other than `HS256` (including `none`), a `crit` header, padding,
//! oversized parts, a lifetime (`exp - iat`) over the policy maximum.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::config::Key;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TokenError {
    Malformed,
    UnsupportedAlg,
    UnknownKey,
    BadSignature,
    Expired,
    NotYetValid,
    IssuedInFuture,
    LifetimeTooLong,
    WrongAudience,
    WrongIssuer,
    InvalidClaims,
}

impl TokenError {
    /// Stable code for logs and the audit trail (clients only see `invalid_token`).
    pub fn code(self) -> &'static str {
        match self {
            TokenError::Malformed => "malformed",
            TokenError::UnsupportedAlg => "unsupported_alg",
            TokenError::UnknownKey => "unknown_key",
            TokenError::BadSignature => "bad_signature",
            TokenError::Expired => "expired",
            TokenError::NotYetValid => "not_yet_valid",
            TokenError::IssuedInFuture => "issued_in_future",
            TokenError::LifetimeTooLong => "lifetime_too_long",
            TokenError::WrongAudience => "wrong_audience",
            TokenError::WrongIssuer => "wrong_issuer",
            TokenError::InvalidClaims => "invalid_claims",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Claims {
    pub sub: String,
    pub scopes: Vec<String>,
    pub exp: u64,
}

impl Claims {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}

pub struct Policy<'a> {
    pub keys: &'a [Key],
    pub audience: &'a str,
    pub issuer: Option<&'a str>,
    pub max_ttl: u64,
    pub leeway: u64,
}

const MAX_TOKEN: usize = 4096;

fn b64(part: &str, max_decoded: usize) -> Result<Vec<u8>, TokenError> {
    if part.is_empty() || part.len() > max_decoded * 4 / 3 + 4 {
        return Err(TokenError::Malformed);
    }
    URL_SAFE_NO_PAD.decode(part).map_err(|_| TokenError::Malformed)
}

fn object(bytes: &[u8]) -> Result<serde_json::Map<String, Value>, TokenError> {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Object(m)) => Ok(m),
        _ => Err(TokenError::Malformed),
    }
}

/// `sub` as it is passed to backends in `x-gw-sub`: 1-128 characters of A-Z a-z 0-9 . _ : @ -.
pub fn valid_subject(s: &str) -> bool {
    (1..=128).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b':' | b'@' | b'-'))
}

fn valid_scope(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b':' | b'.' | b'_' | b'-'))
}

pub fn verify(token: &str, p: &Policy, now: u64) -> Result<Claims, TokenError> {
    if token.len() > MAX_TOKEN {
        return Err(TokenError::Malformed);
    }
    let mut parts = token.split('.');
    let (h, pl, s) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(pl), Some(s), None) => (h, pl, s),
        _ => return Err(TokenError::Malformed),
    };

    // 1. Header: algorithm and key only.
    let header = object(&b64(h, 512)?)?;
    if header.get("alg").and_then(Value::as_str) != Some("HS256") || header.contains_key("crit") {
        return Err(TokenError::UnsupportedAlg);
    }
    if let Some(typ) = header.get("typ") {
        if typ.as_str().map(|t| t.eq_ignore_ascii_case("JWT")) != Some(true) {
            return Err(TokenError::Malformed);
        }
    }
    let key = match header.get("kid") {
        Some(Value::String(kid)) => p.keys.iter().find(|k| k.kid == *kid),
        Some(_) => None,
        None if p.keys.len() == 1 => p.keys.first(),
        None => None,
    }
    .ok_or(TokenError::UnknownKey)?;

    // 2. Signature over "<header>.<payload>", compared in constant time by `verify_slice`.
    let sig = b64(s, 64)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.secret).map_err(|_| TokenError::UnknownKey)?;
    mac.update(h.as_bytes());
    mac.update(b".");
    mac.update(pl.as_bytes());
    mac.verify_slice(&sig).map_err(|_| TokenError::BadSignature)?;

    // 3. Claims, now authenticated.
    let c = object(&b64(pl, 2048)?)?;
    let num = |name: &str| c.get(name).and_then(Value::as_u64);
    let exp = num("exp").ok_or(TokenError::InvalidClaims)?;
    let iat = num("iat").ok_or(TokenError::InvalidClaims)?;
    if now > exp.saturating_add(p.leeway) {
        return Err(TokenError::Expired);
    }
    match c.get("nbf") {
        None => {}
        Some(v) => match v.as_u64() {
            Some(nbf) if nbf <= now.saturating_add(p.leeway) => {}
            Some(_) => return Err(TokenError::NotYetValid),
            None => return Err(TokenError::InvalidClaims),
        },
    }
    if iat > now.saturating_add(p.leeway) {
        return Err(TokenError::IssuedInFuture);
    }
    if exp <= iat || exp - iat > p.max_ttl {
        return Err(TokenError::LifetimeTooLong);
    }
    let aud_ok = match c.get("aud") {
        Some(Value::String(a)) => a == p.audience,
        Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(p.audience)),
        _ => false,
    };
    if !aud_ok {
        return Err(TokenError::WrongAudience);
    }
    if let Some(iss) = p.issuer {
        if c.get("iss").and_then(Value::as_str) != Some(iss) {
            return Err(TokenError::WrongIssuer);
        }
    }
    let sub = c
        .get("sub")
        .and_then(Value::as_str)
        .filter(|s| valid_subject(s))
        .ok_or(TokenError::InvalidClaims)?;
    let scopes: Vec<String> = match c.get("scope") {
        None => Vec::new(),
        Some(Value::String(s)) => s.split_whitespace().map(str::to_string).collect(),
        Some(_) => return Err(TokenError::InvalidClaims),
    };
    if scopes.len() > 32 || !scopes.iter().all(|s| valid_scope(s)) {
        return Err(TokenError::InvalidClaims);
    }
    Ok(Claims {
        sub: sub.to_string(),
        scopes,
        exp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: u64 = 1_800_000_000;

    fn keys() -> Vec<Key> {
        vec![
            Key {
                kid: "k1".into(),
                secret: vec![7; 32],
            },
            Key {
                kid: "k2".into(),
                secret: vec![9; 32],
            },
        ]
    }

    fn mint(header: Value, payload: Value, secret: &[u8]) -> String {
        let h = URL_SAFE_NO_PAD.encode(header.to_string());
        let p = URL_SAFE_NO_PAD.encode(payload.to_string());
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(format!("{h}.{p}").as_bytes());
        format!("{h}.{p}.{}", URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn claims() -> Value {
        json!({"sub": "alice", "aud": "edge-gateway", "iat": NOW - 10, "exp": NOW + 290, "scope": "notes:read notes:write"})
    }

    fn check(token: &str) -> Result<Claims, TokenError> {
        let k = keys();
        verify(
            token,
            &Policy {
                keys: &k,
                audience: "edge-gateway",
                issuer: None,
                max_ttl: 900,
                leeway: 30,
            },
            NOW,
        )
    }

    fn hs(kid: &str) -> Value {
        json!({"alg": "HS256", "typ": "JWT", "kid": kid})
    }

    #[test]
    fn accepts_a_valid_token() {
        let c = check(&mint(hs("k1"), claims(), &[7; 32])).unwrap();
        assert_eq!(c.sub, "alice");
        assert!(c.has_scope("notes:write"));
        assert!(!c.has_scope("audit:read"));
        // key rotation: the second key works under its own kid
        assert!(check(&mint(hs("k2"), claims(), &[9; 32])).is_ok());
    }

    #[test]
    fn rejects_forgery_and_algorithm_tricks() {
        assert_eq!(
            check(&mint(hs("k1"), claims(), &[8; 32])),
            Err(TokenError::BadSignature)
        );
        assert_eq!(
            check(&mint(hs("k2"), claims(), &[7; 32])),
            Err(TokenError::BadSignature),
            "kid of another key"
        );
        assert_eq!(check(&mint(hs("k9"), claims(), &[7; 32])), Err(TokenError::UnknownKey));
        assert_eq!(
            check(&mint(json!({"alg": "HS256"}), claims(), &[7; 32])),
            Err(TokenError::UnknownKey),
            "no kid, two keys"
        );
        assert_eq!(
            check(&mint(json!({"alg": "none", "kid": "k1"}), claims(), &[7; 32])),
            Err(TokenError::UnsupportedAlg)
        );
        assert_eq!(
            check(&mint(json!({"alg": "RS256", "kid": "k1"}), claims(), &[7; 32])),
            Err(TokenError::UnsupportedAlg)
        );
        assert_eq!(
            check(&mint(
                json!({"alg": "HS256", "kid": "k1", "crit": ["x"]}),
                claims(),
                &[7; 32]
            )),
            Err(TokenError::UnsupportedAlg)
        );
        // payload swapped after signing
        let good = mint(hs("k1"), claims(), &[7; 32]);
        let mut admin = claims();
        admin["scope"] = json!("audit:read");
        let forged = mint(hs("k1"), admin, &[8; 32]);
        let spliced = format!(
            "{}.{}.{}",
            good.split('.').next().unwrap(),
            forged.split('.').nth(1).unwrap(),
            good.split('.').nth(2).unwrap()
        );
        assert_eq!(check(&spliced), Err(TokenError::BadSignature));
    }

    #[test]
    fn rejects_malformed_input() {
        for t in ["", "a.b", "a.b.c.d", "....", "%%%.x.y"] {
            assert_eq!(check(t), Err(TokenError::Malformed), "{t}");
        }
        let good = mint(hs("k1"), claims(), &[7; 32]);
        assert_eq!(check(&format!("{good}=")), Err(TokenError::Malformed), "padding");
        assert_eq!(check(&"a".repeat(5000)), Err(TokenError::Malformed));
    }

    #[test]
    fn checks_time_and_audience() {
        let with = |f: &dyn Fn(&mut Value)| {
            let mut c = claims();
            f(&mut c);
            check(&mint(hs("k1"), c, &[7; 32]))
        };
        assert_eq!(
            with(&|c| {
                c["iat"] = json!(NOW - 400);
                c["exp"] = json!(NOW - 31);
            }),
            Err(TokenError::Expired)
        );
        assert!(
            with(&|c| {
                c["iat"] = json!(NOW - 400);
                c["exp"] = json!(NOW - 20);
            })
            .is_ok(),
            "within leeway"
        );
        assert_eq!(with(&|c| c["nbf"] = json!(NOW + 120)), Err(TokenError::NotYetValid));
        assert_eq!(
            with(&|c| {
                c["iat"] = json!(NOW + 120);
                c["exp"] = json!(NOW + 300);
            }),
            Err(TokenError::IssuedInFuture)
        );
        assert_eq!(
            with(&|c| c["exp"] = json!(NOW + 3600)),
            Err(TokenError::LifetimeTooLong)
        );
        assert_eq!(with(&|c| c["aud"] = json!("other")), Err(TokenError::WrongAudience));
        assert!(with(&|c| c["aud"] = json!(["other", "edge-gateway"])).is_ok());
        assert_eq!(
            with(&|c| {
                c.as_object_mut().unwrap().remove("aud");
            }),
            Err(TokenError::WrongAudience)
        );
        assert_eq!(
            with(&|c| {
                c.as_object_mut().unwrap().remove("exp");
            }),
            Err(TokenError::InvalidClaims)
        );
        assert_eq!(
            with(&|c| c["exp"] = json!(1.8e9)),
            Err(TokenError::InvalidClaims),
            "float exp"
        );
        assert_eq!(with(&|c| c["sub"] = json!("bad sub")), Err(TokenError::InvalidClaims));
        assert_eq!(
            with(&|c| c["scope"] = json!("Notes:Read")),
            Err(TokenError::InvalidClaims)
        );
    }

    #[test]
    fn checks_issuer_when_configured() {
        let k = keys();
        let p = Policy {
            keys: &k,
            audience: "edge-gateway",
            issuer: Some("https://auth.example.com"),
            max_ttl: 900,
            leeway: 30,
        };
        let mut c = claims();
        assert_eq!(
            verify(&mint(hs("k1"), c.clone(), &[7; 32]), &p, NOW),
            Err(TokenError::WrongIssuer)
        );
        c["iss"] = json!("https://auth.example.com");
        assert!(verify(&mint(hs("k1"), c, &[7; 32]), &p, NOW).is_ok());
    }

    /// Interop with ../mint-token.sh (openssl): GATEWAY_KEY_HEX=07…07 (32 bytes) MINT_NOW=1799999990
    /// ./mint-token.sh alice "notes:read notes:write" 300
    #[test]
    fn accepts_a_token_from_mint_token_sh() {
        const MINTED: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImsxIn0.\
            eyJzdWIiOiJhbGljZSIsImF1ZCI6ImVkZ2UtZ2F0ZXdheSIsImlhdCI6MTc5OTk5OTk5MCwiZXhwIjoxODAwMDAwMjkwLCJzY29wZSI6Im5vdGVzOnJlYWQgbm90ZXM6d3JpdGUifQ.\
            gMSrsnwZzoRihNrpieNu9HrBKWyaInTiHfNqI5cH-Oo";
        let c = check(MINTED).unwrap();
        assert_eq!(c.sub, "alice");
        assert_eq!(c.scopes, ["notes:read", "notes:write"]);
        assert_eq!(c.exp, 1_800_000_290);
    }
}
