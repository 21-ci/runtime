//! B2.1: the per-invocation path never reaches `meta.db` (service config, plan and env come from
//! in-memory tables and caches), and env or secret changes still reach the very next request.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rt_server::StartOptions;
use rt_store::{
    MetaStore, Page, PageReq, Result, SecretMeta, ServiceFilter, TenantPatch, TenantScope, UsageRow, WarmFloorCap,
};
use rt_types::{
    ApiKey, BuildFailure, BuildState, Digest, Etag, EventId, HostMapping, KeyId, NewKey, NewSchedule, NewService,
    NewTenant, NewVersion, NodeEvent, RouteSnapshot, Schedule, ScheduleId, SecretsVersion, Service, ServiceId,
    ServicePatch, Tenant, TenantId, UnixMs, UsageBatch, Version, VersionNo,
};
use rt_types::{NewSdkRelease, Sdk, SdkChannel, SdkGrant, SdkRelease};
use serde_json::json;

use crate::common::*;

/// Echoes its env and the length of its secret.
const ENV_READER: &str = r#"
export default function (): Response {
  return Response.json({ mode: Deno.env.get("MODE") ?? null, secret_len: (Deno.env.get("TOKEN") ?? "").length });
}
"#;

/// Calls per `MetaStore` method.
#[derive(Default)]
struct MetaCalls {
    by_method: Mutex<BTreeMap<&'static str, u64>>,
}

impl MetaCalls {
    fn snapshot(&self) -> BTreeMap<&'static str, u64> {
        self.by_method.lock().clone()
    }
}

/// `meta.db` with every call counted.
struct CountingMeta {
    inner: Arc<dyn MetaStore>,
    calls: Arc<MetaCalls>,
}

impl CountingMeta {
    fn hit(&self, method: &'static str) {
        *self.calls.by_method.lock().entry(method).or_default() += 1;
    }
}

#[async_trait]
impl MetaStore for CountingMeta {
    async fn schema_version(&self) -> Result<u32> {
        self.hit("schema_version");
        self.inner.schema_version().await
    }

    async fn create_tenant(&self, t: NewTenant) -> Result<Tenant> {
        self.hit("create_tenant");
        self.inner.create_tenant(t).await
    }

    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>> {
        self.hit("get_tenant");
        self.inner.get_tenant(id).await
    }

    async fn list_tenants(&self, page: PageReq) -> Result<Page<Tenant>> {
        self.hit("list_tenants");
        self.inner.list_tenants(page).await
    }

    async fn update_tenant(&self, id: TenantId, patch: TenantPatch) -> Result<Tenant> {
        self.hit("update_tenant");
        self.inner.update_tenant(id, patch).await
    }

    async fn delete_tenant(&self, id: TenantId, purge: bool) -> Result<()> {
        self.hit("delete_tenant");
        self.inner.delete_tenant(id, purge).await
    }

    async fn create_key(&self, tenant: TenantId, k: NewKey) -> Result<ApiKey> {
        self.hit("create_key");
        self.inner.create_key(tenant, k).await
    }

    async fn create_child_key_capped(
        &self,
        tenant: TenantId,
        k: NewKey,
        parent: Option<KeyId>,
        max_live: u64,
        now_ms: UnixMs,
    ) -> Result<ApiKey> {
        self.hit("create_child_key_capped");
        self.inner
            .create_child_key_capped(tenant, k, parent, max_live, now_ms)
            .await
    }

    async fn find_key(&self, id: KeyId) -> Result<Option<ApiKey>> {
        self.hit("find_key");
        self.inner.find_key(id).await
    }

    async fn list_keys(&self, tenant: TenantId) -> Result<Vec<ApiKey>> {
        self.hit("list_keys");
        self.inner.list_keys(tenant).await
    }

    async fn revoke_key(&self, tenant: TenantId, id: KeyId) -> Result<bool> {
        self.hit("revoke_key");
        self.inner.revoke_key(tenant, id).await
    }

    async fn touch_key(&self, id: KeyId, at_ms: UnixMs) -> Result<()> {
        self.hit("touch_key");
        self.inner.touch_key(id, at_ms).await
    }

    async fn create_service(&self, scope: TenantScope, tenant: TenantId, s: NewService) -> Result<Service> {
        self.hit("create_service");
        self.inner.create_service(scope, tenant, s).await
    }

    async fn get_service(&self, scope: TenantScope, id: ServiceId) -> Result<Option<Service>> {
        self.hit("get_service");
        self.inner.get_service(scope, id).await
    }

    async fn tenant_projects_among(
        &self,
        tenant: TenantId,
        ids: &[rt_types::ProjectId],
    ) -> Result<Vec<rt_types::ProjectId>> {
        self.hit("tenant_projects_among");
        self.inner.tenant_projects_among(tenant, ids).await
    }

    async fn list_services(&self, scope: TenantScope, filter: ServiceFilter, page: PageReq) -> Result<Page<Service>> {
        self.hit("list_services");
        self.inner.list_services(scope, filter, page).await
    }

    async fn count_services(&self, tenant: TenantId) -> Result<u64> {
        self.hit("count_services");
        self.inner.count_services(tenant).await
    }

    async fn busiest_service_schedules(&self, tenant: TenantId) -> Result<u64> {
        self.hit("busiest_service_schedules");
        self.inner.busiest_service_schedules(tenant).await
    }

    async fn create_project(&self, tenant: TenantId, p: rt_types::NewProject) -> Result<rt_types::Project> {
        self.hit("create_project");
        self.inner.create_project(tenant, p).await
    }

    async fn get_project(&self, tenant: TenantId, slug: &str) -> Result<Option<rt_types::Project>> {
        self.hit("get_project");
        self.inner.get_project(tenant, slug).await
    }

    async fn get_project_by_id(&self, tenant: TenantId, id: rt_types::ProjectId) -> Result<Option<rt_types::Project>> {
        self.hit("get_project_by_id");
        self.inner.get_project_by_id(tenant, id).await
    }

    async fn list_projects(&self, tenant: TenantId, page: PageReq) -> Result<Page<rt_types::Project>> {
        self.hit("list_projects");
        self.inner.list_projects(tenant, page).await
    }

    async fn update_project(
        &self,
        tenant: TenantId,
        slug: &str,
        patch: rt_types::ProjectPatch,
    ) -> Result<rt_types::Project> {
        self.hit("update_project");
        self.inner.update_project(tenant, slug, patch).await
    }

    async fn delete_project(&self, tenant: TenantId, slug: &str) -> Result<()> {
        self.hit("delete_project");
        self.inner.delete_project(tenant, slug).await
    }

    async fn patch_service(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
    ) -> Result<Service> {
        self.hit("patch_service");
        self.inner.patch_service(scope, id, patch, if_match).await
    }

    async fn create_service_capped(
        &self,
        scope: TenantScope,
        tenant: TenantId,
        s: NewService,
        cap: WarmFloorCap,
    ) -> Result<Service> {
        self.hit("create_service");
        self.inner.create_service_capped(scope, tenant, s, cap).await
    }

    async fn patch_service_capped(
        &self,
        scope: TenantScope,
        id: ServiceId,
        patch: ServicePatch,
        if_match: Option<Etag>,
        cap: WarmFloorCap,
    ) -> Result<Service> {
        self.hit("patch_service");
        self.inner.patch_service_capped(scope, id, patch, if_match, cap).await
    }

    async fn delete_service(&self, scope: TenantScope, id: ServiceId, keep_data: bool) -> Result<()> {
        self.hit("delete_service");
        self.inner.delete_service(scope, id, keep_data).await
    }

    async fn add_version(&self, id: ServiceId, v: NewVersion) -> Result<Version> {
        self.hit("add_version");
        self.inner.add_version(id, v).await
    }

    async fn get_version(&self, id: ServiceId, v: VersionNo) -> Result<Option<Version>> {
        self.hit("get_version");
        self.inner.get_version(id, v).await
    }

    async fn list_versions(&self, id: ServiceId) -> Result<Vec<Version>> {
        self.hit("list_versions");
        self.inner.list_versions(id).await
    }

    async fn set_build_state(
        &self,
        id: ServiceId,
        v: VersionNo,
        state: BuildState,
        failure: Option<BuildFailure>,
        lock_digest: Option<Digest>,
    ) -> Result<()> {
        self.hit("set_build_state");
        self.inner.set_build_state(id, v, state, failure, lock_digest).await
    }

    async fn set_live(&self, id: ServiceId, v: VersionNo, if_match: Option<Etag>) -> Result<Service> {
        self.hit("set_live");
        self.inner.set_live(id, v, if_match).await
    }

    async fn count_builds_since(&self, tenant: TenantId, since_ms: UnixMs) -> Result<u64> {
        self.hit("count_builds_since");
        self.inner.count_builds_since(tenant, since_ms).await
    }

    async fn gc_versions(&self, id: ServiceId, keep: u32) -> Result<Vec<Digest>> {
        self.hit("gc_versions");
        self.inner.gc_versions(id, keep).await
    }

    async fn digest_referenced(&self, digest: &Digest) -> Result<bool> {
        self.hit("digest_referenced");
        self.inner.digest_referenced(digest).await
    }

    async fn lock_for_source(&self, source: &Digest) -> Result<Option<Digest>> {
        self.hit("lock_for_source");
        self.inner.lock_for_source(source).await
    }

    async fn route_snapshot(&self) -> Result<RouteSnapshot> {
        self.hit("route_snapshot");
        self.inner.route_snapshot().await
    }

    async fn get_env(&self, id: ServiceId) -> Result<Vec<(String, String)>> {
        self.hit("get_env");
        self.inner.get_env(id).await
    }

    async fn put_env(&self, id: ServiceId, env: Vec<(String, String)>) -> Result<SecretsVersion> {
        self.hit("put_env");
        self.inner.put_env(id, env).await
    }

    async fn list_secrets(&self, id: ServiceId) -> Result<Vec<SecretMeta>> {
        self.hit("list_secrets");
        self.inner.list_secrets(id).await
    }

    async fn put_secret(&self, id: ServiceId, name: &str, sealed: Vec<u8>) -> Result<SecretsVersion> {
        self.hit("put_secret");
        self.inner.put_secret(id, name, sealed).await
    }

    async fn replace_secret_if(&self, id: ServiceId, name: &str, expected: Vec<u8>, sealed: Vec<u8>) -> Result<bool> {
        self.hit("replace_secret_if");
        self.inner.replace_secret_if(id, name, expected, sealed).await
    }

    async fn delete_secret(&self, id: ServiceId, name: &str) -> Result<SecretsVersion> {
        self.hit("delete_secret");
        self.inner.delete_secret(id, name).await
    }

    async fn get_sealed_secrets(&self, id: ServiceId) -> Result<Vec<(String, Vec<u8>)>> {
        self.hit("get_sealed_secrets");
        self.inner.get_sealed_secrets(id).await
    }

    async fn secrets_version(&self, id: ServiceId) -> Result<SecretsVersion> {
        self.hit("secrets_version");
        self.inner.secrets_version(id).await
    }

    async fn put_host(&self, h: HostMapping) -> Result<()> {
        self.hit("put_host");
        self.inner.put_host(h).await
    }

    async fn delete_host(&self, id: ServiceId, host: &str) -> Result<bool> {
        self.hit("delete_host");
        self.inner.delete_host(id, host).await
    }

    async fn list_hosts(&self, id: ServiceId) -> Result<Vec<HostMapping>> {
        self.hit("list_hosts");
        self.inner.list_hosts(id).await
    }

    async fn find_host(&self, host: &str) -> Result<Option<HostMapping>> {
        self.hit("find_host");
        self.inner.find_host(host).await
    }

    async fn pending_hosts(&self) -> Result<Vec<HostMapping>> {
        self.hit("pending_hosts");
        self.inner.pending_hosts().await
    }

    async fn activate_host(&self, host: &str, service: ServiceId, token: &str) -> Result<bool> {
        self.hit("activate_host");
        self.inner.activate_host(host, service, token).await
    }

    async fn expire_pending_hosts(&self, created_before_ms: rt_types::UnixMs) -> Result<Vec<HostMapping>> {
        self.hit("expire_pending_hosts");
        self.inner.expire_pending_hosts(created_before_ms).await
    }

    async fn record_host_release(&self, r: rt_store::HostRelease) -> Result<()> {
        self.hit("record_host_release");
        self.inner.record_host_release(r).await
    }

    async fn host_release(&self, host: &str, tenant: TenantId) -> Result<Option<rt_store::HostRelease>> {
        self.hit("host_release");
        self.inner.host_release(host, tenant).await
    }

    async fn prune_host_releases(&self, released_before_ms: rt_types::UnixMs) -> Result<u64> {
        self.hit("prune_host_releases");
        self.inner.prune_host_releases(released_before_ms).await
    }

    async fn count_pending_hosts(&self, tenant: TenantId) -> Result<u64> {
        self.hit("count_pending_hosts");
        self.inner.count_pending_hosts(tenant).await
    }

    async fn create_schedule(&self, id: ServiceId, s: NewSchedule) -> Result<Schedule> {
        self.hit("create_schedule");
        self.inner.create_schedule(id, s).await
    }

    async fn list_schedules(&self, id: ServiceId) -> Result<Vec<Schedule>> {
        self.hit("list_schedules");
        self.inner.list_schedules(id).await
    }

    async fn all_schedules(&self) -> Result<Vec<Schedule>> {
        self.hit("all_schedules");
        self.inner.all_schedules().await
    }

    async fn update_schedule(&self, id: ServiceId, sch: ScheduleId, s: NewSchedule) -> Result<Schedule> {
        self.hit("update_schedule");
        self.inner.update_schedule(id, sch, s).await
    }

    async fn delete_schedule(&self, id: ServiceId, sch: ScheduleId) -> Result<bool> {
        self.hit("delete_schedule");
        self.inner.delete_schedule(id, sch).await
    }

    async fn set_schedule_next_due(&self, sch: ScheduleId, next_due_ms: Option<UnixMs>) -> Result<()> {
        self.hit("set_schedule_next_due");
        self.inner.set_schedule_next_due(sch, next_due_ms).await
    }

    async fn ensure_tenant(&self, id: TenantId, name: &str, plan: &str) -> Result<()> {
        self.hit("ensure_tenant");
        self.inner.ensure_tenant(id, name, plan).await
    }

    async fn get_sdk(&self, name: &str) -> Result<Option<Sdk>> {
        self.hit("get_sdk");
        self.inner.get_sdk(name).await
    }

    async fn list_sdks(&self) -> Result<Vec<Sdk>> {
        self.hit("list_sdks");
        self.inner.list_sdks().await
    }

    async fn upsert_sdk(&self, name: &str, service: ServiceId, description: Option<String>) -> Result<Sdk> {
        self.hit("upsert_sdk");
        self.inner.upsert_sdk(name, service, description).await
    }

    async fn add_sdk_release(&self, r: NewSdkRelease) -> Result<SdkRelease> {
        self.hit("add_sdk_release");
        self.inner.add_sdk_release(r).await
    }

    async fn list_sdk_releases(&self, sdk: &str) -> Result<Vec<SdkRelease>> {
        self.hit("list_sdk_releases");
        self.inner.list_sdk_releases(sdk).await
    }

    async fn get_sdk_release(&self, sdk: &str, version: &str) -> Result<Option<SdkRelease>> {
        self.hit("get_sdk_release");
        self.inner.get_sdk_release(sdk, version).await
    }

    async fn list_sdk_channels(&self, sdk: &str) -> Result<Vec<SdkChannel>> {
        self.hit("list_sdk_channels");
        self.inner.list_sdk_channels(sdk).await
    }

    async fn set_sdk_channel(&self, sdk: &str, channel: &str, version: &str) -> Result<SdkChannel> {
        self.hit("set_sdk_channel");
        self.inner.set_sdk_channel(sdk, channel, version).await
    }

    async fn remove_sdk(&self, name: &str) -> Result<bool> {
        self.hit("remove_sdk");
        self.inner.remove_sdk(name).await
    }

    async fn list_sdk_grants(&self) -> Result<Vec<SdkGrant>> {
        self.hit("list_sdk_grants");
        self.inner.list_sdk_grants().await
    }

    async fn put_sdk_grant(&self, g: SdkGrant) -> Result<Option<SdkGrant>> {
        self.hit("put_sdk_grant");
        self.inner.put_sdk_grant(g).await
    }

    async fn add_usage(&self, day: &str, batch: UsageBatch) -> Result<()> {
        self.hit("add_usage");
        self.inner.add_usage(day, batch).await
    }

    async fn get_usage(&self, tenant: TenantId, from_day: &str, to_day: &str) -> Result<Vec<UsageRow>> {
        self.hit("get_usage");
        self.inner.get_usage(tenant, from_day, to_day).await
    }

    async fn append_event(&self, ev: NodeEvent) -> Result<()> {
        self.hit("append_event");
        self.inner.append_event(ev).await
    }

    async fn list_events(&self, service: ServiceId, page: PageReq) -> Result<Page<NodeEvent>> {
        self.hit("list_events");
        self.inner.list_events(service, page).await
    }

    async fn event_by_id(&self, id: EventId) -> Result<Option<NodeEvent>> {
        self.hit("event_by_id");
        self.inner.event_by_id(id).await
    }

    async fn save_cache_freq(&self, snapshot: Vec<u8>) -> Result<()> {
        self.hit("save_cache_freq");
        self.inner.save_cache_freq(snapshot).await
    }

    async fn load_cache_freq(&self) -> Result<Option<Vec<u8>>> {
        self.hit("load_cache_freq");
        self.inner.load_cache_freq().await
    }
}

async fn ok_eventually(node: &impl Node, slug: &str) {
    let slug = slug.to_string();
    eventually(Duration::from_secs(60), &format!("`{slug}` answers 200"), || {
        let slug = slug.clone();
        async move { invoke(node, &slug, "/").await.s() == 200 }
    })
    .await;
}

/// B2.1 acceptance: once a service is deployed and warm, invoking it reads nothing from
/// `meta.db`: no service lookup for the host config, no env/secrets load, no plan read. Only
/// the periodic usage flush (every 5 s, not per request) may write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_invocations_make_no_meta_db_calls() {
    let calls = Arc::new(MetaCalls::default());
    let counted = calls.clone();
    let opts = StartOptions {
        meta_layer: Some(Box::new(move |inner| {
            Arc::new(CountingMeta { inner, calls: counted }) as Arc<dyn MetaStore>
        })),
        ..StartOptions::default()
    };
    let node = start_node_opts(opts, None, |_| {}).await;
    let t = new_tenant(&node, "pro").await;
    let wasm = uniq("whot");
    ship_wasm(&node, &t.key, &wasm, "hello").await;
    let deno = uniq("dhot");
    let deno_id = ship(&node, &t.key, &deno, ENV_READER).await;
    assert_eq!(
        put(
            &node,
            &t.key,
            &format!("/v1/services/{deno_id}/env"),
            json!({"MODE": "warm"})
        )
        .await
        .s(),
        200
    );
    let r = put(
        &node,
        &t.key,
        &format!("/v1/services/{deno_id}/secrets/TOKEN"),
        json!({"value": "abc"}),
    )
    .await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);

    // Warm-up: builds finish, artifacts load, the env cache fills.
    ok_eventually(&node, &wasm).await;
    ok_eventually(&node, &deno).await;

    let before = calls.snapshot();
    for _ in 0..25 {
        assert_eq!(invoke(&node, &wasm, "/").await.s(), 200);
        let r = invoke(&node, &deno, "/").await;
        assert_eq!(r.json, json!({"mode": "warm", "secret_len": 3}), "{}", r.text);
    }
    let after = calls.snapshot();
    let per_request: BTreeMap<&str, u64> = after
        .iter()
        .filter(|(m, _)| **m != "add_usage")
        .map(|(m, n)| (*m, n - before.get(m).copied().unwrap_or(0)))
        .filter(|(_, n)| *n > 0)
        .collect();
    assert!(
        per_request.is_empty(),
        "meta.db calls made by 50 warm invocations: {per_request:?}"
    );
}

/// B2.1 acceptance: the env cache never serves a stale bundle after a change: a rotated secret,
/// a new env and a deleted secret are each visible to the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_secret_is_visible_to_the_next_request() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let slug = uniq("rotate");
    let id = ship(&node, &t.key, &slug, ENV_READER).await;
    let env = |v: &str| json!({"MODE": v});
    let secret_path = format!("/v1/services/{id}/secrets/TOKEN");
    assert_eq!(
        put(&node, &t.key, &format!("/v1/services/{id}/env"), env("one"))
            .await
            .s(),
        200
    );
    let r = put(&node, &t.key, &secret_path, json!({"value": "abc"})).await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);
    ok_eventually(&node, &slug).await;
    for _ in 0..3 {
        assert_eq!(
            invoke(&node, &slug, "/").await.json,
            json!({"mode": "one", "secret_len": 3})
        );
    }

    let r = put(&node, &t.key, &secret_path, json!({"value": "abcdefgh"})).await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);
    assert_eq!(
        invoke(&node, &slug, "/").await.json,
        json!({"mode": "one", "secret_len": 8}),
        "rotated secret"
    );

    assert_eq!(
        put(&node, &t.key, &format!("/v1/services/{id}/env"), env("two"))
            .await
            .s(),
        200
    );
    assert_eq!(
        invoke(&node, &slug, "/").await.json,
        json!({"mode": "two", "secret_len": 8}),
        "new env"
    );

    let r = delete(&node, &t.key, &secret_path).await;
    assert!(r.s() == 200 || r.s() == 204, "{}", r.text);
    assert_eq!(
        invoke(&node, &slug, "/").await.json,
        json!({"mode": "two", "secret_len": 0}),
        "deleted secret"
    );
}
