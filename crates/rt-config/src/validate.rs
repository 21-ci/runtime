//! Cross-field and range validation. Collects every problem (never stops at the first) and
//! names each by its dotted key. Bounds mirror `config/schema.json`.

use std::net::{IpAddr, Ipv6Addr};
use std::time::Duration;

use crate::Config;

fn at_least(p: &mut Vec<String>, key: &str, v: u64, min: u64) {
    if v < min {
        p.push(format!("{key} must be >= {min} (got {v})"));
    }
}

fn at_most(p: &mut Vec<String>, key: &str, v: u64, max: u64) {
    if v > max {
        p.push(format!("{key} must be <= {max} (got {v})"));
    }
}

fn fraction(p: &mut Vec<String>, key: &str, v: f64) {
    if !(0.0..=1.0).contains(&v) {
        p.push(format!("{key} must be within 0.0..=1.0 (got {v})"));
    }
}

/// `host:port` (port 0..=65535, host name / IPv4 / `[IPv6]`) or `unix:/path`.
pub(crate) fn listener_problem(s: &str) -> Option<String> {
    if let Some(path) = s.strip_prefix("unix:") {
        return path
            .is_empty()
            .then(|| "unix listener needs a socket path (`unix:/path`)".to_string());
    }
    let Some((host, port)) = s.rsplit_once(':') else {
        return Some(format!("`{s}` is not `host:port` or `unix:/path`"));
    };
    if port.parse::<u16>().is_err() {
        return Some(format!("`{s}`: `{port}` is not a port number (0-65535)"));
    }
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return inner
            .parse::<Ipv6Addr>()
            .is_err()
            .then(|| format!("`{s}`: `{inner}` is not an IPv6 address"));
    }
    let host_ok = !host.is_empty()
        && host.len() <= 253
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    (!host_ok).then(|| format!("`{s}`: invalid host `{host}`"))
}

/// `1.2.3.0/24`, `2001:db8::/32` or a bare address.
pub(crate) fn cidr_problem(s: &str) -> Option<String> {
    let (addr, prefix) = match s.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (s, None),
    };
    let Ok(ip) = addr.trim().parse::<IpAddr>() else {
        return Some(format!("`{s}` is not an IP address or CIDR"));
    };
    if let Some(p) = prefix {
        let max = if ip.is_ipv4() { 32 } else { 128 };
        match p.trim().parse::<u8>() {
            Ok(n) if n <= max => {}
            _ => return Some(format!("`{s}`: prefix length must be 0..={max}")),
        }
    }
    None
}

pub(crate) fn problems(cfg: &Config) -> Vec<String> {
    let mut p: Vec<String> = Vec::new();

    // --- listeners
    for (key, value) in [
        ("listeners.public", &cfg.listeners.public),
        ("listeners.internal", &cfg.listeners.internal),
        ("listeners.control", &cfg.listeners.control),
        ("listeners.metrics", &cfg.listeners.metrics),
    ] {
        if let Some(why) = listener_problem(value) {
            p.push(format!("{key}: {why}"));
        }
    }

    // --- scheduler
    let s = &cfg.sched;
    at_least(&mut p, "sched.lrq_capacity", u64::from(s.lrq_capacity), 1);
    // One run slot stays reserved for nested service calls (ADM-003): at least 2 (CFG-005).
    at_least(&mut p, "sched.max_inflight", u64::from(s.max_inflight), 2);
    at_most(&mut p, "sched.min_run_time_max_ms", s.min_run_time_max_ms, 86_400_000);
    at_most(&mut p, "sched.aging_factor", u64::from(s.aging_factor), 64);
    if let Some(share) = s.warm_share {
        fraction(&mut p, "sched.warm_share", share);
    }
    // With a single P the kinds share it elastically (a kind without live services has
    // an effective minimum of 0), so the sum rule only applies from two processors on.
    if s.processors >= 2 {
        let sum = u64::from(s.kinds.deno.min) + u64::from(s.kinds.wasm.min);
        if sum > u64::from(s.processors) {
            p.push(format!(
                "sched.kinds: deno.min + wasm.min ({sum}) exceeds sched.processors ({})",
                s.processors
            ));
        }
    }
    for (name, k) in [("deno", &s.kinds.deno), ("wasm", &s.kinds.wasm)] {
        if k.max != 0 && k.max < k.min {
            p.push(format!("sched.kinds.{name}: max ({}) is below min ({})", k.max, k.min));
        }
    }
    let up = cfg.rebalance.up_threshold;
    if up.is_nan() || up < 1.0 {
        p.push(format!(
            "rebalance.up_threshold must be >= 1.0 (got {})",
            cfg.rebalance.up_threshold
        ));
    }

    // --- thread pools
    at_least(&mut p, "io.threads", u64::from(cfg.io.threads), 1);

    // --- memory budget: 0 = detect; at most 16 TiB (its bytes must fit the governor's u64 math)
    const MAX_BUDGET_MB: u64 = 16 * 1024 * 1024;
    if cfg.memory.budget_mb > MAX_BUDGET_MB {
        p.push(format!(
            "memory.budget_mb must be <= {MAX_BUDGET_MB} (got {})",
            cfg.memory.budget_mb
        ));
    }

    // --- memory watermarks (judged against the profile value of the other when only one is set)
    if let Some(h) = cfg.memory.high {
        fraction(&mut p, "memory.high", h);
        if h == 0.0 {
            // Every tick would be High pressure (GOV-005).
            p.push("memory.high must be above 0".into());
        }
    }
    if let Some(c) = cfg.memory.critical {
        fraction(&mut p, "memory.critical", c);
    }
    let knobs = cfg.node_knobs();
    let (high, critical) = (knobs.memory_high.value, knobs.memory_critical.value);
    let in_range = (0.0..=1.0).contains(&high) && (0.0..=1.0).contains(&critical);
    if in_range && high >= critical {
        p.push(format!(
            "memory.high ({high}) must be below memory.critical ({critical})"
        ));
    }
    fraction(&mut p, "cache.t1_share", knobs.cache_t1_share.value);

    let telemetry_url = cfg.telemetry.url.as_str();
    if !(telemetry_url.is_empty()
        || telemetry_url == crate::TELEMETRY_URL_AUTO
        || crate::valid_telemetry_url(telemetry_url))
    {
        p.push(format!(
            "telemetry.url must be \"auto\", empty or an https URL without credentials, query or fragment (got {telemetry_url:?})"
        ));
    }

    // --- deno / wasm / build / deploy / storage
    at_least(&mut p, "deno.isolate_base_mb", u64::from(cfg.deno.isolate_base_mb), 1);
    at_least(
        &mut p,
        "deno.v8_platform_threads",
        u64::from(cfg.deno.v8_platform_threads),
        1,
    );
    at_least(&mut p, "deno.dispose_backlog", u64::from(cfg.deno.dispose_backlog), 1);
    at_least(&mut p, "deno.reuse_max_uses", u64::from(cfg.deno.reuse_max_uses), 1);
    if let Some(v) = cfg.deno.max_bound_per_m {
        at_least(&mut p, "deno.max_bound_per_m", u64::from(v), 1);
    }
    at_least(&mut p, "build.threads", u64::from(cfg.build.threads), 1);
    at_least(&mut p, "build.memory_mb", u64::from(cfg.build.memory_mb), 1);
    at_least(&mut p, "build.max_deps_mb", u64::from(cfg.build.max_deps_mb), 1);
    at_least(
        &mut p,
        "deploy.retain_versions",
        u64::from(cfg.deploy.retain_versions),
        1,
    );
    let drain = cfg.deploy.drain_max;
    if drain < Duration::from_secs(1) || drain > Duration::from_secs(24 * 3600) {
        p.push(format!(
            "deploy.drain_max must be between 1s and 24h (got {})",
            humantime::format_duration(drain)
        ));
    }
    at_least(&mut p, "backup.keep_last", u64::from(cfg.backup.keep_last), 1);
    if cfg.backup.encrypt_to.iter().any(|k| !k.starts_with("age1")) {
        p.push("backup.encrypt_to must hold age public keys (age1...)".to_string());
    }
    at_least(&mut p, "backup.s3.keep_last", u64::from(cfg.backup.s3.keep_last), 1);
    check_vault(&cfg.secrets.vault, &mut p);
    let s3 = &cfg.backup.s3;
    if !s3.endpoint.is_empty() {
        if s3.bucket.is_empty() {
            p.push("backup.s3.bucket must be set with backup.s3.endpoint".to_string());
        }
        let loopback = ["http://127.", "http://localhost", "http://[::1]"]
            .iter()
            .any(|x| s3.endpoint.starts_with(x));
        if !s3.endpoint.starts_with("https://") && !loopback {
            p.push("backup.s3.endpoint must be https (plain http to loopback only)".to_string());
        }
        // Decision D2: the node must not be able to read what a bucket holds.
        if cfg.backup.encrypt_to.is_empty() && !cfg.backup.allow_plaintext_remote {
            p.push("backup.s3.endpoint needs backup.encrypt_to, or backup.allow_plaintext_remote = true".to_string());
        }
    }
    at_least(&mut p, "deploy.dry_run_max_mb", u64::from(cfg.deploy.dry_run_max_mb), 1);
    at_least(&mut p, "import.max_bundle_mb", u64::from(cfg.import.max_bundle_mb), 1);
    let budget = cfg.deploy.migration_budget;
    if !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&budget) {
        p.push(format!(
            "deploy.migration_budget must be between 1s and 1h (got {})",
            humantime::format_duration(budget)
        ));
    }
    at_least(&mut p, "storage.max_open_dbs", u64::from(cfg.storage.max_open_dbs), 1);
    if cfg.storage.readers_per_db > 64 {
        p.push(format!(
            "storage.readers_per_db must be <= 64 (got {})",
            cfg.storage.readers_per_db
        ));
    }
    at_least(&mut p, "storage.sql_budget_ms", cfg.storage.sql_budget_ms, 1);
    at_least(&mut p, "supervisor.reserve_mb", u64::from(cfg.supervisor.reserve_mb), 1);
    at_least(
        &mut p,
        "secrets.max_services_per_min",
        u64::from(cfg.secrets.max_services_per_min),
        1,
    );

    // --- ingress / egress
    at_least(
        &mut p,
        "ingress.max_conns_per_ip",
        u64::from(cfg.ingress.max_conns_per_ip),
        1,
    );
    at_least(
        &mut p,
        "ingress.h2_max_streams",
        u64::from(cfg.ingress.h2_max_streams),
        1,
    );
    at_least(
        &mut p,
        "ingress.sse_streams_per_key",
        u64::from(cfg.ingress.sse_streams_per_key),
        1,
    );
    at_least(&mut p, "ingress.backlog", u64::from(cfg.ingress.backlog), 1);
    for (i, c) in cfg.ingress.trusted_proxies.iter().enumerate() {
        if let Some(why) = cidr_problem(c) {
            p.push(format!("ingress.trusted_proxies[{i}]: {why}"));
        }
    }
    for (i, c) in cfg.egress.denied_cidrs.iter().enumerate() {
        if let Some(why) = cidr_problem(c) {
            p.push(format!("egress.denied_cidrs[{i}]: {why}"));
        }
    }

    // --- queue / logs / metrics / otel
    at_least(&mut p, "queue.batch_size", u64::from(cfg.queue.batch_size), 1);
    at_least(&mut p, "queue.max_attempts", u64::from(cfg.queue.max_attempts), 1);
    at_least(&mut p, "queue.max_concurrency", u64::from(cfg.queue.max_concurrency), 1);
    // --- ws
    let ws = &cfg.ws;
    for (key, v) in [
        ("ws.max_sockets", ws.max_sockets),
        ("ws.send_buffer_kb", ws.send_buffer_kb),
        ("ws.inbox_max", ws.inbox_max),
        ("ws.max_event_attempts", ws.max_event_attempts),
        ("ws.max_alarm_attempts", ws.max_alarm_attempts),
        ("ws.msgs_per_sec", ws.msgs_per_sec),
        ("ws.msgs_burst", ws.msgs_burst),
    ] {
        at_least(&mut p, key, u64::from(v), 1);
    }
    at_least(&mut p, "logs.ring_lines", u64::from(cfg.logs.ring_lines), 1);
    at_least(&mut p, "logs.retention_days", u64::from(cfg.logs.retention_days), 1);
    at_least(&mut p, "logs.max_mb_per_service", cfg.logs.max_mb_per_service, 1);
    at_least(
        &mut p,
        "metrics.per_service_top_n",
        u64::from(cfg.metrics.per_service_top_n),
        1,
    );
    fraction(&mut p, "otel.sample_ratio", cfg.otel.sample_ratio);

    // --- plans
    for (name, plan) in &cfg.plans {
        fraction(&mut p, &format!("plans.{name}.blocking_share"), plan.blocking_share);
        // A zero maximum would fail every task of the plan's tenants (CFG-004).
        at_least(
            &mut p,
            &format!("plans.{name}.limits_max.wall_ms"),
            plan.limits_max.wall_ms,
            1,
        );
        at_least(
            &mut p,
            &format!("plans.{name}.limits_max.cpu_ms"),
            plan.limits_max.cpu_ms,
            1,
        );
        at_least(
            &mut p,
            &format!("plans.{name}.limits_max.memory_mb"),
            u64::from(plan.limits_max.memory_mb),
            1,
        );
        let parts = format!("plans.{name}.max_topic_partitions");
        at_least(&mut p, &parts, u64::from(plan.max_topic_partitions), 1);
        at_most(
            &mut p,
            &parts,
            u64::from(plan.max_topic_partitions),
            u64::from(rt_types::events::PARTITIONS_HARD_MAX),
        );
    }
    for (i, b) in cfg.binding.iter().enumerate() {
        for plan in &b.allowed_plans {
            if plan != "free" && !cfg.plans.contains_key(plan) {
                p.push(format!("binding[{i}].allowed_plans: unknown plan `{plan}`"));
            }
        }
    }

    p
}

/// `[secrets.vault]`: a node token can read anything, so no setting may leave a tenant's prefix open.
fn check_vault(v: &crate::sections::VaultConfig, p: &mut Vec<String>) {
    if v.address.is_empty() {
        return;
    }
    let loopback = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
        .iter()
        .any(|a| {
            v.address
                .strip_prefix(a)
                .is_some_and(|r| r.is_empty() || r.starts_with(':') || r.starts_with('/'))
        });
    if !v.address.starts_with("https://") && !loopback {
        p.push("secrets.vault.address must be https (plain http to loopback only)".to_string());
    }
    if !["{tenant}", "{project}", "{environment}"]
        .iter()
        .all(|n| v.path_template.contains(n))
    {
        p.push("secrets.vault.path_template must name {tenant}, {project} and {environment}".to_string());
    }
    let approle = !v.role_id_file.is_empty() && !v.secret_id_file.is_empty();
    let partial = !approle && (!v.role_id_file.is_empty() || !v.secret_id_file.is_empty());
    if v.token_file.is_empty() != approle || partial {
        p.push("secrets.vault needs token_file, or role_id_file with secret_id_file".to_string());
    }
    if v.mount.is_empty() || v.mount.contains("..") || v.mount.contains('%') || v.mount.contains("//") {
        p.push("secrets.vault.mount is not a valid mount".to_string());
    }
    if v.poll < std::time::Duration::from_secs(10) {
        p.push("secrets.vault.poll must be at least 10s".to_string());
    }
}

#[cfg(test)]
mod vault_tests {
    use super::check_vault;
    use crate::sections::VaultConfig;

    fn problems(f: impl FnOnce(&mut VaultConfig)) -> Vec<String> {
        let mut v = VaultConfig {
            address: "https://vault:8200".into(),
            token_file: "/t".into(),
            ..VaultConfig::default()
        };
        f(&mut v);
        let mut p = Vec::new();
        check_vault(&v, &mut p);
        p
    }

    #[test]
    fn vault_settings_are_checked_only_when_it_is_on() {
        assert!(problems(|_| {}).is_empty());
        assert!(problems(|v| *v = VaultConfig {
            path_template: "x".into(),
            ..VaultConfig::default()
        })
        .is_empty());
        assert!(problems(|v| v.address = "http://127.0.0.1:8200".into()).is_empty());
        assert_eq!(problems(|v| v.address = "http://vault:8200".into()).len(), 1);
        assert_eq!(problems(|v| v.address = "http://127.0.0.1.evil.test".into()).len(), 1);
        assert_eq!(problems(|v| v.path_template = "rt/{tenant}/{project}/".into()).len(), 1);
        assert_eq!(problems(|v| v.token_file.clear()).len(), 1);
        assert_eq!(problems(|v| v.role_id_file = "/r".into()).len(), 1);
        assert_eq!(problems(|v| v.poll = std::time::Duration::from_secs(1)).len(), 1);
        assert_eq!(problems(|v| v.mount = "a/../b".into()).len(), 1);
    }
}
