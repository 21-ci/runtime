//! Specifier classification and the resolution table:
//! relative, `std:*`, `sdk:*`, `npm:`, `jsr:`, `https://` allowlist, import map, everything else.

use crate::common;

use std::collections::BTreeMap;

use common::{resolver_cfg, url};
use proptest::prelude::*;
use rt_build::{
    classify_specifier, renamed_to, DefaultResolver, ResolveError, Resolved, ResolverConfig, SpecifierKind,
    SpecifierResolver, BUILTIN_MODULES,
};

const DEFAULT_HOSTS: &[&str] = &["esm.sh", "jsr.io", "deno.land", "cdn.jsdelivr.net", "unpkg.com"];

fn default_cfg() -> ResolverConfig {
    resolver_cfg("https://esm.sh", DEFAULT_HOSTS)
}

fn resolver(cfg: ResolverConfig) -> DefaultResolver {
    DefaultResolver::new(cfg)
}

fn main_url() -> url::Url {
    url("file:///main.ts")
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

// ---------------------------------------------------------------------------------------------
// classify_specifier
// ---------------------------------------------------------------------------------------------

#[test]
fn classify_by_prefix() {
    use SpecifierKind::*;
    let table: &[(&str, SpecifierKind)] = &[
        ("./x.ts", Relative),
        ("../y.js", Relative),
        ("/abs/z.ts", Relative),
        ("std:kv", Builtin),
        ("rt:kv", Builtin),
        ("rt:nope", Builtin),
        ("sdk:telegram", Sdk),
        ("npm:zod@3", Npm),
        ("npm:@scope/pkg@1/sub", Npm),
        ("jsr:@std/path@1", Jsr),
        ("https://esm.sh/x.js", Remote),
        ("http://example.com/x.js", Remote),
        ("file:///etc/passwd", File),
        ("node:fs", Node),
        ("node:child_process", Node),
        ("lodash", Bare),
        ("@scope/pkg", Bare),
    ];
    for (s, want) in table {
        assert_eq!(classify_specifier(s), *want, "classify({s:?})");
    }
}

#[test]
fn builtin_modules_are_the_documented_set() {
    for m in [
        "std:kv",
        "std:sql",
        "std:sqlite",
        "std:blob",
        "std:queue",
        "std:services",
        "std:platform",
    ] {
        assert!(BUILTIN_MODULES.contains(&m), "{m} missing from BUILTIN_MODULES");
        assert_eq!(classify_specifier(m), SpecifierKind::Builtin);
    }
}

// ---------------------------------------------------------------------------------------------
// ./x.ts, ../y.js -> bundle files
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_relative_specifiers_are_bundle_files() {
    let r = resolver(default_cfg());
    let cases = [
        ("./x.ts", "file:///main.ts", "file:///x.ts"),
        ("./sub/z.ts", "file:///a/b.ts", "file:///a/sub/z.ts"),
        ("../y.js", "file:///a/b/c.ts", "file:///a/y.js"),
        ("../../top.ts", "file:///a/b/c.ts", "file:///top.ts"),
    ];
    for (spec, referrer, want) in cases {
        let got = r
            .resolve(spec, &url(referrer))
            .unwrap_or_else(|e| panic!("{spec} from {referrer}: {e}"));
        assert_eq!(got, Resolved::Local(url(want)), "{spec} from {referrer}");
    }
}

// ---------------------------------------------------------------------------------------------
// std:* built-ins
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_rt_modules_are_builtin_not_bundled() {
    let r = resolver(default_cfg());
    for m in BUILTIN_MODULES {
        assert_eq!(r.resolve(m, &main_url()), Ok(Resolved::Builtin(m.to_string())), "{m}");
    }
    assert!(matches!(
        r.resolve("std:nope", &main_url()),
        Err(ResolveError::UnknownBuiltin(_))
    ));
}

#[test]
fn bld_resolve_renamed_rt_modules_name_the_new_module() {
    let r = resolver(default_cfg());
    for (old, new) in [
        ("rt:kv", "std:kv"),
        ("rt:sqlite", "std:sqlite"),
        ("rt:services", "std:services"),
    ] {
        let err = r.resolve(old, &main_url()).expect_err(old);
        assert_eq!(
            err,
            ResolveError::Renamed {
                from: old.to_string(),
                to: new.to_string()
            }
        );
        assert_eq!(err.to_string(), format!("`{old}` was renamed to `{new}`"));
    }
    assert_eq!(renamed_to("rt:nope"), None);
    assert_eq!(renamed_to("std:kv"), None);
}

#[test]
fn bld_resolve_bun_sqlite_is_rt_sqlite() {
    // bun code imports `bun:sqlite`; the bundle names the module `std:sqlite`.
    let r = resolver(default_cfg());
    assert_eq!(classify_specifier("bun:sqlite"), SpecifierKind::Builtin);
    assert_eq!(
        r.resolve("bun:sqlite", &main_url()),
        Ok(Resolved::Builtin("std:sqlite".to_string()))
    );
    // other `bun:` modules are not provided
    assert!(r.resolve("bun:ffi", &main_url()).is_err());
}

// ---------------------------------------------------------------------------------------------
// npm: / jsr: through build.npm_cdn
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_npm_goes_through_the_cdn_with_target_es2022() {
    let r = resolver(default_cfg());
    let cases = [
        ("npm:zod@3", "https://esm.sh/zod@3?target=es2022"),
        (
            "npm:zod@3/lib/index.js",
            "https://esm.sh/zod@3/lib/index.js?target=es2022",
        ),
        ("npm:@scope/pkg@1.2.3", "https://esm.sh/@scope/pkg@1.2.3?target=es2022"),
        (
            "npm:@scope/pkg@1.2.3/sub",
            "https://esm.sh/@scope/pkg@1.2.3/sub?target=es2022",
        ),
    ];
    for (spec, want) in cases {
        assert_eq!(r.resolve(spec, &main_url()), Ok(Resolved::Remote(url(want))), "{spec}");
    }
}

#[test]
fn bld_resolve_jsr_goes_through_the_cdn_jsr_prefix() {
    let r = resolver(default_cfg());
    let cases = [
        ("jsr:@std/path@1", "https://esm.sh/jsr/@std/path@1?target=es2022"),
        (
            "jsr:@std/path@1/posix",
            "https://esm.sh/jsr/@std/path@1/posix?target=es2022",
        ),
    ];
    for (spec, want) in cases {
        assert_eq!(r.resolve(spec, &main_url()), Ok(Resolved::Remote(url(want))), "{spec}");
    }
}

#[test]
fn bld_resolve_npm_cdn_is_configurable_with_or_without_trailing_slash() {
    for cdn in ["https://mirror.internal:8443/npm", "https://mirror.internal:8443/npm/"] {
        let r = resolver(resolver_cfg(cdn, &["mirror.internal"]));
        assert_eq!(
            r.resolve("npm:zod@3", &main_url()),
            Ok(Resolved::Remote(url(
                "https://mirror.internal:8443/npm/zod@3?target=es2022"
            ))),
            "npm_cdn = {cdn}"
        );
        assert_eq!(
            r.resolve("jsr:@std/path@1", &main_url()),
            Ok(Resolved::Remote(url(
                "https://mirror.internal:8443/npm/jsr/@std/path@1?target=es2022"
            ))),
            "npm_cdn = {cdn}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// https:// allowlist
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_https_only_for_allowed_hosts() {
    let r = resolver(default_cfg());
    for u in [
        "https://esm.sh/zod@3.23.8/es2022/zod.mjs",
        "https://deno.land/std@0.224.0/path/mod.ts",
        "https://cdn.jsdelivr.net/npm/foo@1/index.js",
        "https://unpkg.com/foo@1/index.js",
        "https://jsr.io/@std/path/1.0.0/mod.ts",
    ] {
        assert_eq!(r.resolve(u, &main_url()), Ok(Resolved::Remote(url(u))), "{u}");
    }
    assert_eq!(
        r.resolve("https://evil.example/x.js", &main_url()),
        Err(ResolveError::HostNotAllowed("evil.example".to_string()))
    );

    // The allowlist is configuration, not a constant: an air-gapped install allows only its mirror.
    let airgap = resolver(resolver_cfg("https://mirror.internal", &["mirror.internal"]));
    assert!(matches!(
        airgap.resolve("https://esm.sh/x.js", &main_url()),
        Err(ResolveError::HostNotAllowed(_))
    ));
    assert!(airgap.resolve("https://mirror.internal/x.js", &main_url()).is_ok());

    // Plain `http:` only for a loopback mirror: an unverified fetch over the network would be
    // shared with every tenant's builds through the deps cache (security review pass 2).
    assert!(matches!(
        airgap.resolve("http://mirror.internal/x.js", &main_url()),
        Err(ResolveError::Unsupported(_))
    ));
    let local = resolver(resolver_cfg("http://127.0.0.1:9", &["127.0.0.1", "localhost"]));
    assert!(local.resolve("http://127.0.0.1:9/x.js", &main_url()).is_ok());
    assert!(local.resolve("http://localhost:9/x.js", &main_url()).is_ok());
    // Credentials in an import URL would end up in build logs and errors: refused.
    assert!(matches!(
        r.resolve("https://user:tok@esm.sh/x.js", &main_url()),
        Err(ResolveError::Invalid(_))
    ));
}

#[test]
fn bld_resolve_relative_imports_inside_remote_modules_stay_remote() {
    let r = resolver(default_cfg());
    let referrer = url("https://esm.sh/zod@3.23.8/es2022/zod.mjs");
    let cases = [
        ("./helpers.mjs", "https://esm.sh/zod@3.23.8/es2022/helpers.mjs"),
        ("../x.js", "https://esm.sh/zod@3.23.8/x.js"),
        ("/v135/foo.js", "https://esm.sh/v135/foo.js"),
    ];
    for (spec, want) in cases {
        assert_eq!(r.resolve(spec, &referrer), Ok(Resolved::Remote(url(want))), "{spec}");
    }
    // A remote module cannot reach outside the allowlist or the sandbox.
    assert!(matches!(
        r.resolve("https://evil.example/x.js", &referrer),
        Err(ResolveError::HostNotAllowed(_))
    ));
    assert!(r.resolve("file:///etc/passwd", &referrer).is_err());
}

// ---------------------------------------------------------------------------------------------
// anything else -> error
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_everything_else_is_unsupported() {
    let r = resolver(default_cfg());
    for s in ["file:///etc/passwd", "node:fs", "node:child_process"] {
        assert!(
            matches!(r.resolve(s, &main_url()), Err(ResolveError::Unsupported(_))),
            "{s}"
        );
    }
    for s in [
        "lodash",
        "@scope/pkg",
        "data:text/javascript,1",
        "blob:https://x/y",
        "ftp://host/x.js",
        "sdk:x",
    ] {
        assert!(r.resolve(s, &main_url()).is_err(), "{s} must not resolve");
    }
    assert!(matches!(
        r.resolve("sdk:x", &main_url()),
        Err(ResolveError::UnknownSdk(_))
    ));
}

// ---------------------------------------------------------------------------------------------
// import map applied first
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_import_map_is_applied_before_the_other_rules() {
    let mut cfg = default_cfg();
    cfg.import_map = map(&[
        ("zod", "npm:zod@3"),
        ("alias", "./local.ts"),
        ("kv", "std:kv"),
        ("lib/", "./src/lib/"),
        ("https://esm.sh/old.js", "https://esm.sh/new.js"),
    ]);
    let r = resolver(cfg);
    let m = main_url();

    assert_eq!(
        r.resolve("zod", &m),
        Ok(Resolved::Remote(url("https://esm.sh/zod@3?target=es2022")))
    );
    assert_eq!(r.resolve("alias", &m), Ok(Resolved::Local(url("file:///local.ts"))));
    assert_eq!(r.resolve("kv", &m), Ok(Resolved::Builtin("std:kv".to_string())));
    assert_eq!(
        r.resolve("lib/util.ts", &m),
        Ok(Resolved::Local(url("file:///src/lib/util.ts")))
    );
    assert_eq!(
        r.resolve("https://esm.sh/old.js", &m),
        Ok(Resolved::Remote(url("https://esm.sh/new.js")))
    );

    // Unmapped bare specifiers still fail.
    assert!(r.resolve("lodash", &m).is_err());
}

#[test]
fn bld_resolve_import_map_cannot_smuggle_a_forbidden_target() {
    let mut cfg = default_cfg();
    cfg.import_map = map(&[
        ("passwd", "file:///etc/passwd"),
        ("cp", "node:child_process"),
        ("evil", "https://evil.example/x.js"),
    ]);
    let r = resolver(cfg);
    for s in ["passwd", "cp", "evil"] {
        assert!(
            r.resolve(s, &main_url()).is_err(),
            "mapped target of `{s}` must go through the same rules"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// sdk:<name> (operator SDKs are shims of the build, never resolved to code)
// ---------------------------------------------------------------------------------------------

#[test]
fn bld_resolve_sdk_specifiers_are_not_resolved_to_code() {
    let r = resolver(default_cfg());
    assert!(matches!(
        r.resolve("sdk:telegram", &main_url()),
        Err(ResolveError::UnknownSdk(_))
    ));
    assert_eq!(
        r.resolve("std:sdk", &main_url()),
        Ok(Resolved::Builtin("std:sdk".into()))
    );
    assert_eq!(
        r.resolve("std:sdk/server", &main_url()),
        Ok(Resolved::Builtin("std:sdk/server".into()))
    );
}

// ---------------------------------------------------------------------------------------------
// properties
// ---------------------------------------------------------------------------------------------

proptest! {
    #[test]
    fn classify_never_panics(s in ".{0,60}") {
        let _ = classify_specifier(&s);
    }

    #[test]
    fn resolve_never_panics_and_never_escapes(s in "[a-zA-Z0-9:/._@^~%?#&=+-]{0,60}") {
        let r = resolver(default_cfg());
        let referrers = [url("file:///main.ts"), url("https://esm.sh/zod@3/es2022/zod.mjs")];
        for referrer in &referrers {
            match r.resolve(&s, referrer) {
                Ok(Resolved::Local(u)) => prop_assert_eq!(u.scheme(), "file"),
                Ok(Resolved::Remote(u)) => {
                    prop_assert!(u.scheme() == "http" || u.scheme() == "https");
                    let host = u.host_str().unwrap_or("");
                    prop_assert!(DEFAULT_HOSTS.contains(&host), "remote host {host} not in the allowlist");
                }
                Ok(Resolved::Builtin(b)) => prop_assert!(BUILTIN_MODULES.contains(&b.as_str())),
                Ok(Resolved::Generated { .. }) => prop_assert!(false, "the resolver never generates modules"),
                Err(_) => {}
            }
        }
    }

    #[test]
    fn file_and_node_specifiers_never_resolve(tail in "[a-z0-9/_.-]{0,30}") {
        let r = resolver(default_cfg());
        let file_spec = format!("file:///{}", tail);
        let node_spec = format!("node:{}", tail);
        prop_assert!(r.resolve(&file_spec, &main_url()).is_err());
        prop_assert!(r.resolve(&node_spec, &main_url()).is_err());
    }
}

/// RESV-002: an import URL with credentials is refused, and neither the user nor the password
/// appears in the error.
#[test]
fn resv_002_credentials_in_import_urls_are_refused_and_not_echoed() {
    let r = resolver(default_cfg());
    for u in [
        "https://user:s3cretpw@esm.sh/x.js",
        "https://tokenonly@esm.sh/x.js",
        "https://:pw-only@esm.sh/x.js",
    ] {
        let e = r.resolve(u, &main_url()).expect_err(u);
        let text = format!("{e} {e:?}");
        assert!(matches!(e, ResolveError::Invalid(_)), "{u}: {e:?}");
        for secret in ["s3cretpw", "tokenonly", "pw-only", "user:"] {
            assert!(!text.contains(secret), "{u}: `{secret}` leaked in {text}");
        }
    }
}

#[test]
fn an_http_ipv6_loopback_mirror_still_has_to_be_allow_listed() {
    // Plain http is accepted only to a loopback host, and that host must still be on the list.
    let err = resolver(default_cfg())
        .resolve("http://[::1]/x.js", &main_url())
        .unwrap_err();
    assert!(matches!(err, ResolveError::HostNotAllowed(_)), "{err:?}");
}

#[test]
fn a_relative_specifier_from_a_referrer_with_another_scheme_is_unsupported() {
    let err = resolver(default_cfg()).resolve("./x.js", &url("foo:/bar")).unwrap_err();
    assert!(matches!(err, ResolveError::Unsupported(_)), "{err:?}");
}
