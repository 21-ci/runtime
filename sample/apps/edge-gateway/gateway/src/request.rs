//! Request validation and routing: everything is decided before any backend sees the request.

pub type Headers = Vec<(String, Vec<u8>)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "GET" => Self::Get,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "PATCH" => Self::Patch,
            "DELETE" => Self::Delete,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    pub fn takes_body(self) -> bool {
        matches!(self, Self::Post | Self::Put | Self::Patch)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Notes,
    Audit,
    /// Answered by the gateway from the operator SDK (`getPlatformInfo`).
    Platform,
}

impl Target {
    pub fn name(self) -> &'static str {
        match self {
            Target::Notes => "notes",
            Target::Audit => "audit",
            Target::Platform => "platform",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Route {
    pub target: Target,
    /// Path the backend sees (the gateway prefix removed), always starting with `/`.
    pub rest: String,
}

/// The public API surface. Anything else is 404 at the edge.
///   /v1/notes, /v1/notes/{…}  -> notes service
///   /v1/audit/recent          -> audit service, GET only
///   /v1/platform              -> the operator SDK's getPlatformInfo, GET only
pub fn match_route(path: &str) -> Option<Route> {
    if path == "/v1/platform" {
        return Some(Route {
            target: Target::Platform,
            rest: "/".into(),
        });
    }
    if path == "/v1/notes" {
        return Some(Route {
            target: Target::Notes,
            rest: "/".into(),
        });
    }
    if let Some(rest) = path.strip_prefix("/v1/notes/") {
        return Some(Route {
            target: Target::Notes,
            rest: format!("/{rest}"),
        });
    }
    if path == "/v1/audit/recent" {
        return Some(Route {
            target: Target::Audit,
            rest: "/recent".into(),
        });
    }
    None
}

pub fn method_allowed(target: Target, m: HttpMethod) -> bool {
    match target {
        Target::Notes => true,
        Target::Audit | Target::Platform => m == HttpMethod::Get,
    }
}

pub fn required_scope(target: Target, m: HttpMethod) -> &'static str {
    match (target, m) {
        (Target::Notes | Target::Platform, HttpMethod::Get) => "notes:read",
        (Target::Notes | Target::Platform, _) => "notes:write",
        (Target::Audit, _) => "audit:read",
    }
}

/// Plain paths only: unreserved characters and `/`, no percent-encoding (so no encoded `/` or
/// `.` can sneak past these checks), no empty, `.` or `..` segments, at most 512 bytes.
pub fn validate_path(path: &str) -> Result<(), &'static str> {
    if !path.starts_with('/') || path.len() > 512 {
        return Err("path must start with / and be at most 512 bytes");
    }
    if !path
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'/' | b'-' | b'.' | b'_' | b'~'))
    {
        return Err("path may only contain A-Z a-z 0-9 - . _ ~ /");
    }
    if path.len() > 1
        && path[1..]
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err("path has an empty, . or .. segment");
    }
    Ok(())
}

/// Visible ASCII without space or `#`, at most 1024 bytes. Backends validate the parameters.
pub fn validate_query(q: &str) -> Result<(), &'static str> {
    if q.len() > 1024 || !q.bytes().all(|c| c.is_ascii_graphic() && c != b'#') {
        return Err("query must be at most 1024 bytes of visible ASCII");
    }
    Ok(())
}

pub fn values<'a>(headers: &'a Headers, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
    headers
        .iter()
        .filter(move |(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_slice())
}

pub fn header<'a>(headers: &'a Headers, name: &'a str) -> Option<&'a [u8]> {
    values(headers, name).next()
}

/// Exactly one `Authorization: Bearer <token>` header.
pub fn bearer(headers: &Headers) -> Result<&str, &'static str> {
    let mut all = values(headers, "authorization");
    let v = all.next().ok_or("missing Authorization header")?;
    if all.next().is_some() {
        return Err("more than one Authorization header");
    }
    let s = std::str::from_utf8(v).map_err(|_| "invalid Authorization header")?;
    let (scheme, token) = s
        .trim()
        .split_once(' ')
        .ok_or("expected Authorization: Bearer <token>")?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err("expected Authorization: Bearer <token>");
    }
    let token = token.trim();
    if token.is_empty()
        || !token
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
    {
        return Err("malformed bearer token");
    }
    Ok(token)
}

/// The client address the platform resolved for an HTTP request (`x-rt-client-ip`: behind
/// `ingress.trusted_proxies`, and a client-sent one is stripped first, so it cannot be spoofed).
/// None when absent (the internal socket, other triggers) or not an IP address.
pub fn client_ip(headers: &Headers) -> Option<std::net::IpAddr> {
    std::str::from_utf8(header(headers, "x-rt-client-ip")?)
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// `application/json`, optionally with parameters (`; charset=utf-8`).
pub fn is_json(content_type: Option<&[u8]>) -> bool {
    let Some(v) = content_type.and_then(|v| std::str::from_utf8(v).ok()) else {
        return false;
    };
    v.split(';')
        .next()
        .map(|t| t.trim().eq_ignore_ascii_case("application/json"))
        == Some(true)
}

/// Declared body length: Some(0) for "no body", None when not declared or unparsable.
pub fn content_length(headers: &Headers) -> Option<u64> {
    header(headers, "content-length")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|s| s.trim().parse().ok())
}

/// Request headers a backend may see. Everything else (cookies, the client's Authorization,
/// spoofed x-gw-*, the platform's x-rt-*, hop-by-hop headers) stops at the gateway; the gateway
/// adds its own x-gw-* (the client address as `x-gw-client-ip`).
pub const FORWARD_REQUEST: &[&str] = &["content-type", "accept", "if-match", "if-none-match"];

/// Response headers passed back to the client.
pub const PASS_RESPONSE: &[&str] = &["content-type", "etag", "location", "cache-control", "retry-after"];

pub fn filter(headers: &Headers, allow: &[&str]) -> Headers {
    headers
        .iter()
        .filter(|(k, _)| allow.iter().any(|a| k.eq_ignore_ascii_case(a)))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> Headers {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn routes() {
        assert_eq!(
            match_route("/v1/notes"),
            Some(Route {
                target: Target::Notes,
                rest: "/".into()
            })
        );
        assert_eq!(
            match_route("/v1/notes/42"),
            Some(Route {
                target: Target::Notes,
                rest: "/42".into()
            })
        );
        assert_eq!(
            match_route("/v1/audit/recent"),
            Some(Route {
                target: Target::Audit,
                rest: "/recent".into()
            })
        );
        assert_eq!(match_route("/v1/platform").map(|r| r.target), Some(Target::Platform));
        assert!(!method_allowed(Target::Platform, HttpMethod::Post));
        assert_eq!(required_scope(Target::Platform, HttpMethod::Get), "notes:read");
        assert_eq!(match_route("/v1/notesx"), None);
        assert_eq!(match_route("/v1/audit"), None);
        assert_eq!(match_route("/admin"), None);
        assert_eq!(required_scope(Target::Notes, HttpMethod::Get), "notes:read");
        assert_eq!(required_scope(Target::Notes, HttpMethod::Delete), "notes:write");
        assert!(!method_allowed(Target::Audit, HttpMethod::Post));
        assert_eq!(HttpMethod::parse("TRACE"), None);
    }

    #[test]
    fn paths() {
        for ok in ["/", "/v1/notes", "/v1/notes/42", "/v1/notes/a-b_c.d~e"] {
            assert!(validate_path(ok).is_ok(), "{ok}");
        }
        for bad in [
            "v1",
            "/v1//notes",
            "/v1/notes/",
            "/v1/../admin",
            "/v1/./x",
            "/v1/%2e%2e/admin",
            "/v1/notes%2F1",
            "/a\\b",
            "/a b",
        ] {
            assert!(validate_path(bad).is_err(), "{bad}");
        }
        assert!(validate_path(&format!("/{}", "a".repeat(600))).is_err());
        assert!(validate_query("limit=10&q=abc").is_ok());
        assert!(validate_query("a=b c").is_err());
        assert!(validate_query("a=b#frag").is_err());
    }

    #[test]
    fn bearer_header() {
        assert_eq!(
            bearer(&h(&[("authorization", "Bearer abc.def-ghi_j")])),
            Ok("abc.def-ghi_j")
        );
        assert_eq!(bearer(&h(&[("Authorization", "bearer  tok ")])), Ok("tok"));
        assert!(bearer(&h(&[])).is_err());
        assert!(bearer(&h(&[("authorization", "Basic abc")])).is_err());
        assert!(bearer(&h(&[("authorization", "Bearer a"), ("authorization", "Bearer b")])).is_err());
        assert!(bearer(&h(&[("authorization", "Bearer a\"b")])).is_err());
    }

    #[test]
    fn content_type_and_filters() {
        assert!(is_json(Some(b"application/json")));
        assert!(is_json(Some(b"Application/JSON; charset=utf-8")));
        assert!(!is_json(Some(b"text/plain")));
        assert!(!is_json(None));
        let req = h(&[
            ("Content-Type", "application/json"),
            ("cookie", "s=1"),
            ("authorization", "Bearer x"),
            ("x-gw-sub", "admin"),
            ("x-rt-client-ip", "203.0.113.7"),
            ("accept", "*/*"),
        ]);
        let fwd = filter(&req, FORWARD_REQUEST);
        assert_eq!(
            fwd.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            ["content-type", "accept"]
        );
        assert_eq!(content_length(&h(&[("content-length", " 12 ")])), Some(12));
        assert_eq!(content_length(&h(&[])), None);
    }

    #[test]
    fn client_address() {
        assert_eq!(
            client_ip(&h(&[("X-RT-Client-IP", "203.0.113.7")])),
            Some("203.0.113.7".parse().unwrap())
        );
        assert_eq!(
            client_ip(&h(&[("x-rt-client-ip", " 2001:db8::1 ")])),
            Some("2001:db8::1".parse().unwrap())
        );
        assert_eq!(client_ip(&h(&[])), None, "not an HTTP trigger");
        assert_eq!(client_ip(&h(&[("x-rt-client-ip", "unknown")])), None);
        assert_eq!(client_ip(&h(&[("x-rt-client-ip", "203.0.113.7, 10.0.0.1")])), None);
    }
}
