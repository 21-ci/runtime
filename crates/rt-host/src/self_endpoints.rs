//! The node's own outbound endpoints, fed to the egress self-denylist (SEC-24,
//! SEC-51): guests must not reach the node's own services under another name or address.

use std::time::Duration;

use rt_config::Config;

/// How often the self-denylist re-resolves these names (SECURITY.md).
pub const SELF_DENYLIST_REFRESH: Duration = Duration::from_secs(300);

/// `host:port` of every endpoint the node itself talks to: binding `base_url`s, the S3 endpoint
/// and the OTLP endpoint (empty settings are skipped, duplicates removed). The listeners are not
/// listed: their addresses (and every interface address) are added literally at bind time.
///
/// The npm CDN (`build.npm_cdn`) is deliberately not listed. It is a public upstream that the
/// builder fetches through the same guard, so denying its addresses would break builds and keep
/// guests off a public site. If it is served from this node, its addresses are this node's
/// interface addresses, which the denylist already holds.
pub fn self_endpoint_hosts(cfg: &Config) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let urls = cfg
        .binding
        .iter()
        .map(|b| b.base_url.as_str())
        .chain([cfg.s3.endpoint.as_str(), cfg.otel.endpoint.as_str()]);
    for raw in urls {
        if let Some(h) = endpoint_host(raw) {
            if !out.contains(&h) {
                out.push(h);
            }
        }
    }
    out
}

/// `scheme://host[:port]/…` → `host:port` (`[v6]:port` for IPv6, the scheme's default port when
/// none is given); a bare `host[:port]` is kept as is. Empty or unparsable → `None`.
fn endpoint_host(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if !raw.contains("://") {
        return Some(raw.trim_end_matches('/').to_string());
    }
    let u = url::Url::parse(raw).ok()?;
    let host = u.host_str().filter(|h| !h.is_empty())?;
    Some(match u.port_or_known_default() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rt_config::{BindingConfig, OtelConfig, S3Config};

    #[test]
    fn ac_sec_51_bindings_s3_and_otlp_endpoints_are_listed_as_host_port() {
        let binding = |name: &str, base_url: &str| BindingConfig {
            name: name.into(),
            base_url: base_url.into(),
            ..BindingConfig::default()
        };
        let cfg = Config {
            binding: vec![
                binding("voicelab", "https://api.voicelab.test/v1"),
                binding("local", "http://10.0.0.5:8080"),
                binding("v6", "http://[2001:db8::5]:9000/"),
                binding("dup", "https://api.voicelab.test/other"),
            ],
            s3: S3Config {
                endpoint: "https://s3.example.test".into(),
                ..S3Config::default()
            },
            otel: OtelConfig {
                endpoint: "otlp.example.test:4317".into(),
                ..OtelConfig::default()
            },
            ..Config::default()
        };
        assert_eq!(
            self_endpoint_hosts(&cfg),
            vec![
                "api.voicelab.test:443".to_string(),
                "10.0.0.5:8080".into(),
                "[2001:db8::5]:9000".into(),
                "s3.example.test:443".into(),
                "otlp.example.test:4317".into(),
            ]
        );
    }

    #[test]
    fn unset_endpoints_and_the_npm_cdn_are_not_listed() {
        let cfg = Config::default();
        assert!(
            self_endpoint_hosts(&cfg).is_empty(),
            "defaults configure no endpoint: {:?}",
            self_endpoint_hosts(&cfg)
        );
        assert!(
            !cfg.build.npm_cdn.is_empty(),
            "the CDN is configured but stays reachable"
        );
    }
}
