//! Self-service tokens (decision D-B4, SECURITY.md): what a key may hand to a key it
//! creates, and that an expired key stops authenticating while it is in use.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use rt_sec::{
    delegate, AuthError, Authenticator, DefaultAuthenticator, Escalation, Grant, KeyHasher, KeyLookup, KeySpec,
    MasterKey, Principal, Scope, Scopes, WallClock,
};
use rt_types::{ApiKey, KeyId, Listener, ProjectId, TenantId, UnixMs};

const NOW: UnixMs = 1_800_000_000_000;

fn scopes(s: &[Scope]) -> Scopes {
    s.iter().copied().collect()
}

fn grant(s: &[Scope], projects: Option<Vec<ProjectId>>, expires_at_ms: Option<UnixMs>) -> Grant {
    Grant {
        scopes: scopes(s),
        project_ids: projects,
        environments: None,
        expires_at_ms,
    }
}

fn grant_in(s: &[Scope], projects: Option<Vec<ProjectId>>, envs: &[&str]) -> Grant {
    Grant {
        environments: Some(envs.iter().map(|e| e.to_string()).collect()),
        ..grant(s, projects, None)
    }
}

// ---- delegate ----------------------------------------------------------------------------------

#[test]
fn a_new_key_may_hold_any_subset_of_the_creators_scopes() {
    let parent = grant(&[Scope::ServicesRead, Scope::Deploy, Scope::KeysWrite], None, None);
    let child = delegate(&parent, &grant(&[Scope::ServicesRead, Scope::Deploy], None, None)).expect("subset");
    assert_eq!(child.scopes, scopes(&[Scope::ServicesRead, Scope::Deploy]));
    let same = delegate(&parent, &parent).expect("the same set is a subset");
    assert_eq!(same.scopes, parent.scopes, "keys:write itself may be passed on");
}

#[test]
fn a_scope_the_creator_lacks_is_an_escalation() {
    let parent = grant(&[Scope::ServicesRead, Scope::KeysWrite], None, None);
    let r = delegate(&parent, &grant(&[Scope::ServicesRead, Scope::SecretsWrite], None, None));
    assert_eq!(r, Err(Escalation::Scope(Scope::SecretsWrite)));
    let r = delegate(&parent, &grant(&[Scope::DataAdmin], None, None));
    assert_eq!(r, Err(Escalation::Scope(Scope::DataAdmin)));
    assert!(Escalation::Scope(Scope::DataAdmin).to_string().contains("data:admin"));
}

#[test]
fn an_unrestricted_creator_may_restrict_the_new_key_to_any_projects() {
    let (a, b) = (ProjectId::new(), ProjectId::new());
    let parent = grant(&[Scope::ServicesRead], None, None);
    assert_eq!(
        delegate(&parent, &grant(&[Scope::ServicesRead], None, None))
            .expect("all")
            .project_ids,
        None
    );
    let child = delegate(&parent, &grant(&[Scope::ServicesRead], Some(vec![a, b]), None)).expect("some");
    assert_eq!(child.project_ids, Some(vec![a, b]));
}

#[test]
fn a_restricted_creator_cannot_widen_the_project_restriction() {
    let (a, b, c) = (ProjectId::new(), ProjectId::new(), ProjectId::new());
    let parent = grant(&[Scope::ServicesRead], Some(vec![a, b]), None);
    let child = delegate(&parent, &grant(&[Scope::ServicesRead], Some(vec![b]), None)).expect("narrower");
    assert_eq!(child.project_ids, Some(vec![b]));
    let r = delegate(&parent, &grant(&[Scope::ServicesRead], Some(vec![a, c]), None));
    assert_eq!(r, Err(Escalation::Project(c)), "a project outside the restriction");
    let inherited = delegate(&parent, &grant(&[Scope::ServicesRead], None, None)).expect("omitted: inherited");
    assert_eq!(
        inherited.project_ids,
        Some(vec![a, b]),
        "no restriction asked for means the creator's own, never every project"
    );
}

#[test]
fn a_child_key_may_only_narrow_the_environments_of_its_creator() {
    let parent = grant_in(&[Scope::ServicesRead], None, &["production", "staging"]);
    let child = delegate(&parent, &grant_in(&[Scope::ServicesRead], None, &["production"])).expect("narrower");
    assert_eq!(child.environments, Some(vec!["production".to_string()]));
    assert_eq!(
        delegate(&parent, &grant_in(&[Scope::ServicesRead], None, &["dev"])),
        Err(Escalation::Environment("dev".into())),
        "an environment the creator does not hold"
    );
    let inherited = delegate(&parent, &grant(&[Scope::ServicesRead], None, None)).expect("omitted: inherited");
    assert_eq!(
        inherited.environments, parent.environments,
        "no environment list asked for means the creator's own, never every environment"
    );
}

#[test]
fn an_unrestricted_creator_may_limit_a_child_to_environments_and_projects() {
    let p = ProjectId::new();
    let parent = grant(&[Scope::ServicesRead], None, None);
    let child = delegate(
        &parent,
        &grant_in(&[Scope::ServicesRead], Some(vec![p]), &["production"]),
    )
    .expect("ok");
    assert_eq!(
        (child.project_ids, child.environments),
        (Some(vec![p]), Some(vec!["production".to_string()]))
    );
    let free = delegate(&parent, &grant(&[Scope::ServicesRead], None, None)).expect("ok");
    assert_eq!((free.project_ids, free.environments), (None, None));
}

#[test]
fn a_key_cannot_outlive_its_creator() {
    let parent_free = grant(&[Scope::ServicesRead], None, None);
    assert_eq!(
        delegate(&parent_free, &grant(&[Scope::ServicesRead], None, Some(NOW + 5)))
            .expect("ok")
            .expires_at_ms,
        Some(NOW + 5)
    );
    assert_eq!(
        delegate(&parent_free, &grant(&[Scope::ServicesRead], None, None))
            .expect("ok")
            .expires_at_ms,
        None
    );

    let parent = grant(&[Scope::ServicesRead], None, Some(NOW + 1_000));
    assert_eq!(
        delegate(&parent, &grant(&[Scope::ServicesRead], None, Some(NOW + 1_000)))
            .expect("same")
            .expires_at_ms,
        Some(NOW + 1_000)
    );
    assert_eq!(
        delegate(&parent, &grant(&[Scope::ServicesRead], None, Some(NOW + 10)))
            .expect("sooner")
            .expires_at_ms,
        Some(NOW + 10)
    );
    assert_eq!(
        delegate(&parent, &grant(&[Scope::ServicesRead], None, Some(NOW + 1_001))),
        Err(Escalation::Expiry),
        "a short-lived key cannot mint a longer-lived one"
    );
    let inherited = delegate(&parent, &grant(&[Scope::ServicesRead], None, None)).expect("omitted: inherited");
    assert_eq!(
        inherited.expires_at_ms,
        Some(NOW + 1_000),
        "a key without an expiry cannot be minted from one that has it"
    );
}

// ---- expiry while in use -----------------------------------------------------------------------

#[derive(Default)]
struct Store(Mutex<HashMap<KeyId, ApiKey>>);

impl KeyLookup for Store {
    fn find(&self, id: KeyId) -> Option<ApiKey> {
        self.0.lock().unwrap().get(&id).cloned()
    }
}

struct Clock(AtomicI64);

impl WallClock for Clock {
    fn now_ms(&self) -> UnixMs {
        self.0.load(Ordering::SeqCst)
    }
}

#[test]
fn a_key_stops_authenticating_the_moment_it_expires() {
    let store = Arc::new(Store::default());
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let hasher = KeyHasher::new(&MasterKey::from_bytes(std::array::from_fn(|i| 0x11 + i as u8)));
    let auth = DefaultAuthenticator::new(
        Some("root-token-for-tests-0123456789".into()),
        hasher.clone(),
        store.clone(),
        clock.clone(),
    );

    let tenant = TenantId::new();
    let (secret, new) = hasher.issue(KeySpec {
        name: Some("short".into()),
        scopes: scopes(&[Scope::ServicesRead]),
        project_ids: None,
        environments: None,
        expires_at_ms: Some(NOW + 60_000),
    });
    store.0.lock().unwrap().insert(
        new.id,
        ApiKey {
            id: new.id,
            tenant_id: tenant,
            name: new.name,
            scopes: new.scopes,
            project_ids: new.project_ids,
            environments: new.environments,
            expires_at_ms: new.expires_at_ms,
            created_at_ms: NOW,
            last_used_at_ms: None,
            revoked: false,
            parent_key_id: None,
            hmac: new.hmac,
        },
    );
    let call = |token: &str| {
        let (parts, ()) = http::Request::builder()
            .uri("/v1/me")
            .header("authorization", format!("Bearer {token}"))
            .body(())
            .unwrap()
            .into_parts();
        auth.authenticate(&parts, Listener::Control)
    };

    assert!(
        matches!(call(secret.expose()), Ok(Principal::Tenant { .. })),
        "valid before the expiry"
    );
    clock.0.store(NOW + 59_999, Ordering::SeqCst);
    assert!(call(secret.expose()).is_ok(), "one millisecond to go");
    clock.0.store(NOW + 60_000, Ordering::SeqCst);
    assert_eq!(
        call(secret.expose()),
        Err(AuthError::Invalid),
        "at the expiry instant: refused, the same answer as a wrong key"
    );
    clock.0.store(NOW + 3_600_000, Ordering::SeqCst);
    assert_eq!(call(secret.expose()), Err(AuthError::Invalid), "and later");
}
