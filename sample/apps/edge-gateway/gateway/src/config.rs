//! Gateway settings from the WASI environment (service env + secrets).
//!
//! | Name | Default | Meaning |
//! |---|---|---|
//! | `GATEWAY_KEYS` (secret) | required | `kid:hex,kid2:hex`: HS256 keys, each >= 32 bytes; several for rotation |
//! | `GATEWAY_AUDIENCE` | `edge-gateway` | required `aud` claim |
//! | `GATEWAY_ISSUER` | unset | required `iss` claim when set |
//! | `GATEWAY_MAX_TOKEN_TTL` | `900` | max `exp - iat` in seconds: only short-lived tokens pass |
//! | `GATEWAY_RATE_LIMIT` | `120` | requests per window and subject; `0` = off |
//! | `GATEWAY_IP_RATE_LIMIT` | `600` | requests per window and client address (IPv6: per /64), checked before the token; `0` = off |
//! | `GATEWAY_RATE_WINDOW` | `60` | window in seconds (both limits) |
//! | `GATEWAY_NOTES_SERVICE` | `edge-notes` | slug behind `/v1/notes` |
//! | `GATEWAY_AUDIT_SERVICE` | `edge-audit` | slug that receives audit events (queue) and serves `/v1/audit/recent` |
//! | `GATEWAY_AUDIT` | `mutations` | `all`, `mutations` (writes and every denial) or `off` |
//! | `GATEWAY_SDK` | `acme` | operator SDK (`std:host/sdk`): the gateway ships each event with its `audit.ship` and serves `/v1/platform` from its `getPlatformInfo`; `off` = neither (the audit service ships) |
//! | `GATEWAY_MAX_BODY` | `65536` | request body limit in bytes |

pub struct Key {
    pub kid: String,
    pub secret: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AuditMode {
    All,
    Mutations,
    Off,
}

pub struct Config {
    pub keys: Vec<Key>,
    pub audience: String,
    pub issuer: Option<String>,
    pub max_ttl: u64,
    pub leeway: u64,
    pub rate_limit: u64,
    pub ip_rate_limit: u64,
    pub rate_window: u64,
    pub notes_service: String,
    pub audit_service: String,
    pub audit: AuditMode,
    /// The operator SDK; None = the gateway does not ship events itself and has no `/v1/platform`.
    pub sdk: Option<String>,
    pub max_body: usize,
}

pub fn from_env<I: IntoIterator<Item = (String, String)>>(vars: I) -> Result<Config, String> {
    let mut c = Config {
        keys: Vec::new(),
        audience: "edge-gateway".into(),
        issuer: None,
        max_ttl: 900,
        leeway: 30,
        rate_limit: 120,
        ip_rate_limit: 600,
        rate_window: 60,
        notes_service: "edge-notes".into(),
        audit_service: "edge-audit".into(),
        audit: AuditMode::Mutations,
        sdk: Some("acme".into()),
        max_body: 64 * 1024,
    };
    let mut keys = None;
    for (name, value) in vars {
        let v = value.trim();
        match name.as_str() {
            "GATEWAY_KEYS" => keys = Some(parse_keys(v)?),
            "GATEWAY_AUDIENCE" if !v.is_empty() => c.audience = v.to_string(),
            "GATEWAY_ISSUER" if !v.is_empty() => c.issuer = Some(v.to_string()),
            "GATEWAY_MAX_TOKEN_TTL" => c.max_ttl = number(&name, v, 30, 86_400)?,
            "GATEWAY_RATE_LIMIT" => c.rate_limit = number(&name, v, 0, 1_000_000)?,
            "GATEWAY_IP_RATE_LIMIT" => c.ip_rate_limit = number(&name, v, 0, 1_000_000)?,
            "GATEWAY_RATE_WINDOW" => c.rate_window = number(&name, v, 1, 86_400)?,
            "GATEWAY_NOTES_SERVICE" => c.notes_service = slug(&name, v)?,
            "GATEWAY_AUDIT_SERVICE" => c.audit_service = slug(&name, v)?,
            "GATEWAY_MAX_BODY" => c.max_body = number(&name, v, 1, 1 << 20)? as usize,
            "GATEWAY_SDK" if v == "off" => c.sdk = None,
            "GATEWAY_SDK" if !v.is_empty() => c.sdk = Some(sdk_name(&name, v)?),
            "GATEWAY_AUDIT" => {
                c.audit = match v {
                    "all" => AuditMode::All,
                    "mutations" | "" => AuditMode::Mutations,
                    "off" => AuditMode::Off,
                    _ => return Err("GATEWAY_AUDIT must be all, mutations or off".into()),
                }
            }
            _ => {}
        }
    }
    c.keys = keys.ok_or("GATEWAY_KEYS is not set")?;
    Ok(c)
}

fn number(name: &str, v: &str, min: u64, max: u64) -> Result<u64, String> {
    match v.parse::<u64>() {
        Ok(n) if (min..=max).contains(&n) => Ok(n),
        _ => Err(format!("{name} must be an integer {min}-{max}")),
    }
}

pub fn valid_slug(s: &str) -> bool {
    let b = s.as_bytes();
    (2..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn slug(name: &str, v: &str) -> Result<String, String> {
    if valid_slug(v) {
        Ok(v.to_string())
    } else {
        Err(format!("{name} is not a service slug"))
    }
}

/// An operator SDK name (`21ci sdk ls`): `^[a-z][a-z0-9_]{1,31}$`.
fn sdk_name(name: &str, v: &str) -> Result<String, String> {
    let b = v.as_bytes();
    let ok = (2..=32).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_');
    if ok {
        Ok(v.to_string())
    } else {
        Err(format!("{name} is not an SDK name (a-z, 0-9, _; 2-32 characters) or off"))
    }
}

/// `kid:hex[,kid:hex…]`. Each key must be at least 32 bytes (64 hex digits), as RFC 7518 asks
/// for HS256.
pub fn parse_keys(s: &str) -> Result<Vec<Key>, String> {
    let mut keys: Vec<Key> = Vec::new();
    for item in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (kid, hex) = item.split_once(':').ok_or("GATEWAY_KEYS entries look like kid:hex")?;
        let kid_ok = (1..=32).contains(&kid.len())
            && kid
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_' || c == b'.');
        if !kid_ok {
            return Err("GATEWAY_KEYS: a kid is 1-32 characters of A-Z a-z 0-9 . _ -".into());
        }
        let secret = hex_decode(hex).ok_or("GATEWAY_KEYS: a key is not valid hex")?;
        if secret.len() < 32 {
            return Err("GATEWAY_KEYS: keys must be at least 32 bytes".into());
        }
        if keys.iter().any(|k| k.kid == kid) {
            return Err("GATEWAY_KEYS: duplicate kid".into());
        }
        keys.push(Key {
            kid: kid.to_string(),
            secret,
        });
    }
    if keys.is_empty() {
        return Err("GATEWAY_KEYS has no keys".into());
    }
    Ok(keys)
}

pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    b.chunks(2).map(|p| Some(digit(p[0])? << 4 | digit(p[1])?)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    const KEY: &str = "k1:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    #[test]
    fn defaults_and_overrides() {
        let c = from_env(env(&[
            ("GATEWAY_KEYS", KEY),
            ("GATEWAY_RATE_LIMIT", "5"),
            ("GATEWAY_AUDIT", "all"),
            ("PATH", "x"),
        ]))
        .unwrap();
        assert_eq!(c.keys.len(), 1);
        assert_eq!(c.keys[0].kid, "k1");
        assert_eq!(c.rate_limit, 5);
        assert_eq!(c.audit, AuditMode::All);
        assert_eq!(c.audience, "edge-gateway");
        assert_eq!(c.notes_service, "edge-notes");
        assert_eq!(c.ip_rate_limit, 600);
        assert_eq!(c.sdk.as_deref(), Some("acme"));

        let c = from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_IP_RATE_LIMIT", "0"), ("GATEWAY_SDK", "off")])).unwrap();
        assert_eq!(c.ip_rate_limit, 0);
        assert_eq!(c.sdk, None);
        let c = from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_SDK", "ops_audit")])).unwrap();
        assert_eq!(c.sdk.as_deref(), Some("ops_audit"));
    }

    #[test]
    fn rejects_bad_settings() {
        assert!(from_env(env(&[])).is_err(), "keys are required");
        assert!(from_env(env(&[("GATEWAY_KEYS", "k1:abcd")])).is_err(), "short key");
        assert!(from_env(env(&[("GATEWAY_KEYS", "k1:zz")])).is_err(), "not hex");
        assert!(
            from_env(env(&[("GATEWAY_KEYS", &format!("{KEY},{KEY}"))])).is_err(),
            "duplicate kid"
        );
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_NOTES_SERVICE", "Bad Slug")])).is_err());
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_RATE_WINDOW", "0")])).is_err());
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_AUDIT", "sometimes")])).is_err());
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_IP_RATE_LIMIT", "-1")])).is_err());
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_SDK", "a b")])).is_err());
        assert!(from_env(env(&[("GATEWAY_KEYS", KEY), ("GATEWAY_SDK", "Acme")])).is_err());
    }

    #[test]
    fn rotation_keys() {
        let keys = parse_keys(&format!("{KEY}, k2:{}", "ab".repeat(32))).unwrap();
        assert_eq!(keys.iter().map(|k| k.kid.as_str()).collect::<Vec<_>>(), ["k1", "k2"]);
        assert_eq!(hex_decode("0aFf"), Some(vec![0x0a, 0xff]));
        assert_eq!(hex_decode("abc"), None);
    }
}
