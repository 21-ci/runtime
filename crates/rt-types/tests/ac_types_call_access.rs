//! Service environments and `call.allowed_callers` (decision D-A1, TRG-16, TRG-19..21):
//! the pure admission rule, entry validation and canonical form, and the wire defaults that keep
//! existing services (all `production`, empty list) behaving as before.

use rt_types::{
    valid_environment, CallAccess, CallTarget, Caller, DenoServiceConfig, NewService, ProjectId, RouteEntry, Service,
    ServiceConfig, ServiceId, ServicePatch, TenantId, DEFAULT_ENVIRONMENT,
};

fn svc(n: u128) -> ServiceId {
    ServiceId::from_u128(n)
}

fn list(entries: &[&str]) -> CallAccess {
    CallAccess {
        allowed_callers: entries.iter().map(|s| s.to_string()).collect(),
    }
}

const T1: u128 = 1;
const T2: u128 = 2;
const TARGET: u128 = 100;
/// The project of the target and of the callers in these tests.
const PROJECT_A: u128 = 40;
/// Another project of the same tenant.
const PROJECT_B: u128 = 41;

fn project(n: u128) -> ProjectId {
    ProjectId::from_u128(n)
}

fn caller<'a>(n: u128, slug: &'a str, env: &'a str) -> Caller<'a> {
    Caller {
        tenant: TenantId::from_u128(T1),
        service: svc(n),
        slug: Some(slug),
        project: Some(project(PROJECT_A)),
        environment: Some(env),
    }
}

fn foreign<'a>(n: u128, slug: &'a str, env: &'a str) -> Caller<'a> {
    Caller {
        tenant: TenantId::from_u128(T2),
        ..caller(n, slug, env)
    }
}

fn target(env: &str) -> CallTarget<'_> {
    CallTarget {
        tenant: TenantId::from_u128(T1),
        service: svc(TARGET),
        project: project(PROJECT_A),
        environment: env,
    }
}

/// A caller of project B (same tenant), in environment `env`.
fn caller_in_b<'a>(n: u128, slug: &'a str, env: &'a str) -> Caller<'a> {
    Caller {
        project: Some(project(PROJECT_B)),
        ..caller(n, slug, env)
    }
}

#[test]
fn ac_trg_19_an_empty_list_admits_same_environment_callers_only() {
    let open = CallAccess::default();
    assert!(open.admits(&target("production"), &caller(1, "web", "production")));
    assert!(
        !open.admits(&target("production"), &caller(1, "web", "staging")),
        "cross-environment needs an env: entry"
    );
    assert!(open.admits(&target("staging"), &caller(1, "web", "staging")));
}

#[test]
fn ac_trg_20_an_env_entry_admits_that_environment() {
    let a = list(&["env:staging"]);
    assert!(a.admits(&target("production"), &caller(1, "web", "staging")));
    // A non-empty list names every admitted caller: the target's own environment is not implied.
    assert!(!a.admits(&target("production"), &caller(2, "api", "production")));
    assert!(!a.admits(&target("production"), &caller(3, "x", "dev")));
}

#[test]
fn ac_trg_21_slug_and_id_entries_admit_exactly_that_service() {
    let id = svc(7).to_string();
    let a = list(&["billing", id.as_str()]);
    assert!(
        a.admits(&target("production"), &caller(1, "billing", "staging")),
        "listed by slug, any environment"
    );
    assert!(
        a.admits(&target("production"), &caller(1, "BILLING", "staging")),
        "slugs compare case-insensitively"
    );
    assert!(
        a.admits(&target("production"), &caller(7, "renamed", "dev")),
        "listed by id"
    );
    assert!(
        !a.admits(&target("production"), &caller(8, "web", "production")),
        "same environment, not listed"
    );
}

#[test]
fn ac_trg_16_another_tenant_is_admitted_only_by_its_service_id() {
    let t = target("production");
    // Neither the empty list (same environment), nor `env:` or slug entries admit another tenant.
    assert!(!CallAccess::default().admits(&t, &foreign(9, "web", "production")));
    assert!(!list(&["env:production", "web"]).admits(&t, &foreign(9, "web", "production")));
    let id = svc(9).to_string();
    assert!(
        list(&[id.as_str()]).admits(&t, &foreign(9, "web", "staging")),
        "listed by id: admitted"
    );
    assert!(
        !list(&[id.as_str()]).admits(&t, &foreign(10, "web", "production")),
        "another id"
    );
}

#[test]
fn a_service_may_always_call_itself() {
    let a = list(&["someone-else"]);
    assert!(a.admits(&target("production"), &caller(TARGET, "me", "production")));
}

#[test]
fn an_unroutable_caller_matches_only_id_entries() {
    let unknown = Caller {
        tenant: TenantId::from_u128(T1),
        service: svc(5),
        slug: None,
        project: None,
        environment: None,
    };
    assert!(!CallAccess::default().admits(&target("production"), &unknown));
    assert!(!list(&["env:production"]).admits(&target("production"), &unknown));
    let id = svc(5).to_string();
    assert!(list(&[id.as_str()]).admits(&target("production"), &unknown));
}

#[test]
fn ids_are_stored_in_canonical_form_so_they_match() {
    let id = svc(7).to_string();
    let mut a = list(&[id.to_ascii_lowercase().as_str(), "billing", "env:dev"]);
    a.canonicalize().expect("a lowercase ULID is a valid id");
    assert_eq!(a.allowed_callers, vec![id.clone(), "billing".into(), "env:dev".into()]);
    assert!(a.admits(&target("production"), &caller(7, "x", "dev")));
    // Even an uncanonicalized list matches ids case-insensitively.
    let raw = list(&[id.to_ascii_lowercase().as_str()]);
    assert!(raw.admits(&target("production"), &caller(7, "x", "staging")));
}

#[test]
fn allowed_caller_entries_are_validated() {
    let id = svc(9).to_string();
    assert!(list(&["billing", "env:staging", id.as_str(), "env:pr-42"])
        .validate()
        .is_ok());
    for bad in [
        "", "env:", "env:Prod", "Billing", "b", "svc_nope", "env:a b", "-x", "a_b",
    ] {
        assert!(list(&[bad]).validate().is_err(), "`{bad}` must be rejected");
    }
    let many: Vec<String> = (0..65).map(|i| format!("svc-{i}")).collect();
    assert!(
        CallAccess { allowed_callers: many }.validate().is_err(),
        "at most 64 entries"
    );
}

#[test]
fn environments_are_short_slugs() {
    for ok in ["production", "staging", "dev", "pr-42", "a", "x".repeat(32).as_str()] {
        assert!(valid_environment(ok), "{ok}");
    }
    for bad in [
        "",
        "Prod",
        "1st",
        "-dev",
        "a_b",
        "a b",
        "x".repeat(33).as_str(),
        "prod.",
    ] {
        assert!(!valid_environment(bad), "{bad}");
    }
}

#[test]
fn existing_records_default_to_production_and_an_empty_caller_list() {
    // A service created before environments: no `environment` key, no `call` block.
    let new: NewService = serde_json::from_value(serde_json::json!({"slug": "api", "runtime": "deno"})).unwrap();
    assert_eq!(new.environment, DEFAULT_ENVIRONMENT);
    assert_eq!(new.config.call, CallAccess::default());
    let cfg: ServiceConfig = serde_json::from_value(serde_json::json!({"access": "key"})).unwrap();
    assert!(cfg.call.allowed_callers.is_empty());

    let svc_json = serde_json::json!({
        "id": svc(1).to_string(), "tenant_id": TenantId::from_u128(1).to_string(), "slug": "api", "runtime": "deno",
        "project_id": project(PROJECT_A).to_string(), "project": "default",
        "name": null, "profile": null, "config": {}, "live_version": null, "etag": 1, "created_at_ms": 0, "updated_at_ms": 0
    });
    let old: Service = serde_json::from_value(svc_json).expect("a pre-environment service record");
    assert_eq!(old.environment, "production");

    let patch: ServicePatch =
        serde_json::from_value(serde_json::json!({"environment": "staging", "call": {"allowed_callers": ["env:dev"]}}))
            .unwrap();
    assert_eq!(patch.environment.as_deref(), Some("staging"));
    assert_eq!(patch.call, Some(list(&["env:dev"])));
    assert!(
        serde_json::from_value::<ServicePatch>(serde_json::json!({"call": {"callers": []}})).is_err(),
        "unknown keys rejected"
    );
}

#[test]
fn the_deno_block_is_optional_and_left_out_while_unset() {
    let cfg: ServiceConfig = serde_json::from_value(serde_json::json!({"deno": {"reuse_isolates": true}})).unwrap();
    assert_eq!(
        cfg.deno,
        DenoServiceConfig {
            reuse_isolates: Some(true)
        }
    );
    let json = serde_json::to_value(ServiceConfig::default()).unwrap();
    assert!(json.get("deno").is_none(), "unset: omitted from the stored JSON");
    assert!(serde_json::from_value::<ServiceConfig>(serde_json::json!({"deno": {"reuse": true}})).is_err());
    let patch: ServicePatch = serde_json::from_value(serde_json::json!({"deno": {"reuse_isolates": false}})).unwrap();
    assert_eq!(
        patch.deno,
        Some(DenoServiceConfig {
            reuse_isolates: Some(false)
        })
    );
}

#[test]
fn route_entries_without_the_new_fields_still_deserialize() {
    let e: RouteEntry = serde_json::from_value(serde_json::json!({
        "tenant": TenantId::from_u128(1).to_string(), "service": svc(1).to_string(), "slug": "a", "runtime": "deno",
        "project": project(PROJECT_A).to_string(), "version": 1, "digest": format!("sha256:{}", "0".repeat(64)), "access": "public",
        "triggers": {}, "ingress": {}, "profile": null, "hosts": []
    }))
    .expect("route entry");
    assert_eq!(e.environment, "production");
    assert!(e.call.allowed_callers.is_empty());
}

#[test]
fn an_env_entry_is_project_local_so_another_projects_production_is_not_admitted() {
    // `env:production` of project A never admits project B's production service, and an empty
    // list does not either: the same label in another project is another environment.
    let by_env = list(&["env:production"]);
    assert!(by_env.admits(&target("production"), &caller(1, "web", "production")));
    assert!(
        !by_env.admits(&target("production"), &caller_in_b(1, "web", "production")),
        "project B's production is not `env:production` of project A"
    );
    assert!(
        !CallAccess::default().admits(&target("production"), &caller_in_b(1, "web", "production")),
        "an empty list admits the same environment of the same project only"
    );
}

#[test]
fn a_slug_or_id_entry_still_admits_a_service_of_another_project_of_the_tenant() {
    // Only the environment rule is project-local: an explicit service entry is an explicit grant.
    let a = list(&["web"]);
    assert!(a.admits(&target("production"), &caller_in_b(1, "web", "staging")));
}

/// CALL-005 (pinned, docs/handlers.md `call.allowed_callers`): a caller listed by slug is
/// admitted by its slug, so a new service that takes over a deleted caller's slug is admitted;
/// listing the caller's id instead admits only that service.
#[test]
fn call_005_slug_entries_follow_the_slug_id_entries_the_service() {
    let by_slug = list(&["worker"]);
    assert!(by_slug.admits(&target("production"), &caller(7, "worker", "production")));
    assert!(
        by_slug.admits(&target("production"), &caller(8, "worker", "production")),
        "same slug, new service"
    );
    let by_id = list(&[&svc(7).to_string()]);
    assert!(by_id.admits(&target("production"), &caller(7, "worker", "production")));
    assert!(
        !by_id.admits(&target("production"), &caller(8, "worker", "production")),
        "the id names one service"
    );
}
