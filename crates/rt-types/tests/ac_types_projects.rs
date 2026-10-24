//! D-B3 / D-B4 in the shared model: project ids and rules, the `keys:write` scope, and the
//! `project` field of services.

use rt_types::{
    check_project_environments, default_project, default_project_id, valid_project_slug, NewProject, NewService,
    ProjectId, Scope, Scopes, Service, TenantId, DEFAULT_PROJECT,
};

#[test]
fn project_ids_use_the_prj_prefix_and_round_trip() {
    let id = ProjectId::new();
    assert!(id.to_string().starts_with("prj_"), "{id}");
    assert_eq!(id.to_string().parse::<ProjectId>().unwrap(), id);
    assert!(
        "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<ProjectId>().is_err(),
        "wrong prefix"
    );
    assert_eq!(serde_json::to_string(&id).unwrap(), format!("\"{id}\""));
}

#[test]
fn the_default_project_id_is_derived_from_the_tenant_id() {
    let t = TenantId::new();
    let p = default_project_id(t);
    assert_eq!(
        p.bare(),
        t.bare(),
        "same ULID under another prefix: the migration derives it in SQL"
    );
    assert_eq!(default_project_id(t), p, "stable");
    assert_eq!(default_project(), DEFAULT_PROJECT);
    assert_eq!(DEFAULT_PROJECT, "default");
}

#[test]
fn project_slugs_follow_the_service_slug_shape() {
    for ok in ["default", "web-app", "a1", "x".repeat(63).as_str()] {
        assert!(valid_project_slug(ok), "{ok}");
    }
    for bad in [
        "",
        "a",
        "Web",
        "1abc",
        "-abc",
        "a_b",
        "has space",
        "x".repeat(64).as_str(),
    ] {
        assert!(!valid_project_slug(bad), "{bad:?}");
    }
}

#[test]
fn environment_lists_need_one_valid_distinct_entry_each() {
    let s = |v: &[&str]| v.iter().map(|e| e.to_string()).collect::<Vec<_>>();
    assert!(check_project_environments(&s(&["production"])).is_ok());
    assert!(check_project_environments(&s(&["production", "staging", "pr-42"])).is_ok());
    assert!(
        check_project_environments(&s(&[])).is_err(),
        "a project with no environment can host nothing"
    );
    assert!(check_project_environments(&s(&["Prod"])).is_err());
    assert!(check_project_environments(&s(&["a", "a"])).is_err(), "duplicates");
    let many: Vec<String> = (0..21).map(|i| format!("e{i}")).collect();
    assert!(check_project_environments(&many).is_err(), "more than 20");
}

#[test]
fn a_new_project_defaults_to_the_production_environment() {
    let p: NewProject = serde_json::from_value(serde_json::json!({"slug": "web"})).unwrap();
    assert_eq!(
        (p.slug.as_str(), p.name.clone(), p.environments.clone()),
        ("web", None, vec!["production".to_string()])
    );
    assert_eq!(NewProject::new("web"), p);
}

#[test]
fn keys_write_is_a_scope_with_its_documented_name() {
    assert_eq!(Scope::KeysWrite.to_string(), "keys:write");
    assert_eq!("keys:write".parse::<Scope>().unwrap(), Scope::KeysWrite);
    assert_eq!(serde_json::to_string(&Scope::KeysWrite).unwrap(), "\"keys:write\"");
    assert!(Scope::ALL.contains(&Scope::KeysWrite));
    assert!(Scopes::all().contains(Scope::KeysWrite));
}

#[test]
fn services_carry_a_project_that_defaults_to_default() {
    let new: NewService = serde_json::from_value(serde_json::json!({"slug": "hello", "runtime": "deno"})).unwrap();
    assert_eq!(new.project, "default");
    let new: NewService =
        serde_json::from_value(serde_json::json!({"slug": "hello", "runtime": "deno", "project": "web"})).unwrap();
    assert_eq!(new.project, "web");
    // A service record names its project by id; its slug is display data.
    let pid = default_project_id(TenantId::new());
    let rec = serde_json::json!({
        "id": rt_types::ServiceId::new().to_string(), "tenant_id": TenantId::new().to_string(), "slug": "old",
        "runtime": "deno", "project_id": pid.to_string(), "project": "default", "name": null, "profile": null,
        "config": {}, "live_version": null, "etag": 1, "created_at_ms": 0, "updated_at_ms": 0
    });
    let svc: Service = serde_json::from_value(rec).unwrap();
    assert_eq!((svc.project_id, svc.project.as_str()), (pid, "default"));
    assert_eq!(svc.environment, "production");
}
