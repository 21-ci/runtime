//! Decision D-B3 end to end: projects are first-class per tenant (API-22), a service lives in one
//! project and one of its environments (API-23), and `GET /v1/limits` shows what is left of the
//! plan (API-24).

use reqwest::Method;
use serde_json::json;

use crate::common::*;

macro_rules! node_test {
    ($(#[$m:meta])* async fn $name:ident() $body:block) => {
        $(#[$m])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() $body
    };
}

node_test! {
async fn ac_api_22_projects_are_created_read_changed_and_deleted_per_tenant() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;

    // Every tenant starts with its `default` project and the `production` environment.
    let list = get(&node, &t.key, "/v1/projects").await;
    assert_eq!(list.s(), 200, "{}", list.text);
    let slugs: Vec<_> = list.items().iter().map(|p| p["slug"].clone()).collect();
    assert_eq!(slugs, vec![json!("default")]);
    assert_eq!(list.items()[0]["environments"], json!(["production"]));
    assert!(list.items()[0]["id"].as_str().is_some_and(|i| i.starts_with("prj_")));
    assert_eq!(list.items()[0]["tenant_id"], json!(t.id));
    assert!(list.json["next_cursor"].is_null());

    // Create.
    let web = post(&node, &t.key, "/v1/projects", json!({"slug": "web", "name": "Web shop", "environments": ["production", "staging"]})).await;
    assert_eq!(web.s(), 201, "{}", web.text);
    assert_eq!((web.json["slug"].clone(), web.json["name"].clone()), (json!("web"), json!("Web shop")));
    assert_eq!(web.json["environments"], json!(["production", "staging"]));
    let web_id = web.json["id"].as_str().expect("id").to_string();
    let plain = post(&node, &t.key, "/v1/projects", json!({"slug": "jobs"})).await;
    assert_eq!(plain.s(), 201, "{}", plain.text);
    assert_eq!((plain.json["name"].clone(), plain.json["environments"].clone()), (json!("jobs"), json!(["production"])), "defaults");

    // Rejections.
    let dup = post(&node, &t.key, "/v1/projects", json!({"slug": "web"})).await;
    assert_eq!((dup.s(), dup.code()), (409, "project_slug_taken".to_string()), "{}", dup.text);
    for bad in [
        json!({"slug": "Web"}),
        json!({"slug": "x"}),
        json!({"slug": "ok", "environments": []}),
        json!({"slug": "ok", "environments": ["Prod"]}),
        json!({"slug": "ok", "environments": ["a", "a"]}),
        json!({"slug": "ok", "name": "  "}),
        json!({"environments": ["production"]}),
    ] {
        let r = post(&node, &t.key, "/v1/projects", bad.clone()).await;
        assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{bad}: {}", r.text);
    }

    // Read: by slug, by id, its environments, the listing (ordered by slug, paged).
    let by_slug = get(&node, &t.key, "/v1/projects/web").await;
    let by_id = get(&node, &t.key, &format!("/v1/projects/{web_id}")).await;
    assert_eq!((by_slug.s(), by_id.s()), (200, 200), "{} / {}", by_slug.text, by_id.text);
    assert_eq!(by_slug.json, by_id.json);
    let envs = get(&node, &t.key, "/v1/projects/web/environments").await;
    assert_eq!(envs.json, json!({"project": "web", "environments": ["production", "staging"]}));
    assert_eq!(get(&node, &t.key, "/v1/projects/nope").await.s(), 404);
    assert_eq!(get(&node, &t.key, "/v1/projects/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV").await.s(), 404);
    assert_eq!(get(&node, &t.key, "/v1/projects/nope/environments").await.s(), 404);
    let first = get(&node, &t.key, "/v1/projects?limit=2").await;
    assert_eq!(first.items().iter().map(|p| p["slug"].as_str().unwrap().to_string()).collect::<Vec<_>>(), ["default", "jobs"]);
    let cursor = first.json["next_cursor"].as_str().expect("more").to_string();
    let rest = get(&node, &t.key, &format!("/v1/projects?limit=2&cursor={cursor}")).await;
    assert_eq!(rest.items().iter().map(|p| p["slug"].clone()).collect::<Vec<_>>(), vec![json!("web")]);

    // Patch: the name, the environment list; the slug is not a field.
    let r = patch(&node, &t.key, "/v1/projects/web", json!({"name": "Storefront"})).await;
    assert_eq!((r.s(), r.json["name"].clone(), r.json["environments"].clone()), (200, json!("Storefront"), json!(["production", "staging"])), "{}", r.text);
    let r = patch(&node, &t.key, "/v1/projects/web", json!({"environments": ["production", "staging", "dev"]})).await;
    assert_eq!((r.s(), r.json["environments"].clone()), (200, json!(["production", "staging", "dev"])), "{}", r.text);
    assert_eq!(patch(&node, &t.key, "/v1/projects/web", json!({"environments": []})).await.s(), 422);
    assert_eq!(patch(&node, &t.key, "/v1/projects/nope", json!({"name": "x"})).await.s(), 404);

    // A project with a service in it cannot be deleted, and keeps the service's environment.
    let svc = post(&node, &t.key, "/v1/services", json!({"slug": uniq("front"), "runtime": "wasm", "project": "web", "environment": "staging"})).await;
    assert_eq!(svc.s(), 201, "{}", svc.text);
    let svc_id = svc.json["id"].as_str().expect("id").to_string();
    let r = patch(&node, &t.key, "/v1/projects/web", json!({"environments": ["production", "dev"]})).await;
    assert_eq!((r.s(), r.code()), (409, "environment_in_use".to_string()), "{}", r.text);
    let r = delete(&node, &t.key, "/v1/projects/web").await;
    assert_eq!((r.s(), r.code()), (409, "project_not_empty".to_string()), "{}", r.text);

    assert_eq!(delete(&node, &t.key, &format!("/v1/services/{svc_id}")).await.s(), 204);
    assert_eq!(delete(&node, &t.key, "/v1/projects/web").await.s(), 204);
    assert_eq!(get(&node, &t.key, "/v1/projects/web").await.s(), 404);
    assert_eq!(delete(&node, &t.key, "/v1/projects/web").await.s(), 404, "twice");

    // The slug is free again.
    assert_eq!(post(&node, &t.key, "/v1/projects", json!({"slug": "web"})).await.s(), 201);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_22_projects_belong_to_one_tenant_and_need_the_service_scopes() {
    let node = start_node().await;
    let (a, b) = (new_tenant(&node, "pro").await, new_tenant(&node, "pro").await);
    assert_eq!(post(&node, &a.key, "/v1/projects", json!({"slug": "shared-name"})).await.s(), 201);

    // Another tenant neither sees nor changes it, and may use the same slug.
    assert_eq!(get(&node, &b.key, "/v1/projects/shared-name").await.s(), 404);
    assert_eq!(patch(&node, &b.key, "/v1/projects/shared-name", json!({"name": "x"})).await.s(), 404);
    assert_eq!(delete(&node, &b.key, "/v1/projects/shared-name").await.s(), 404);
    assert_eq!(post(&node, &b.key, "/v1/projects", json!({"slug": "shared-name"})).await.s(), 201);
    let ids: Vec<_> = get(&node, &b.key, "/v1/projects").await.items().iter().map(|p| p["slug"].clone()).collect();
    assert_eq!(ids, vec![json!("default"), json!("shared-name")]);

    // Scopes: read needs services:read, changes need services:write.
    let (_, reader) = make_key(&node, &a.id, &["services:read"], None).await;
    let (_, other_scope) = make_key(&node, &a.id, &["deploy"], None).await;
    assert_eq!(get(&node, &reader, "/v1/projects").await.s(), 200);
    assert_eq!(get(&node, &reader, "/v1/projects/shared-name/environments").await.s(), 200);
    for r in [
        post(&node, &reader, "/v1/projects", json!({"slug": "nope"})).await,
        patch(&node, &reader, "/v1/projects/shared-name", json!({"name": "x"})).await,
        delete(&node, &reader, "/v1/projects/shared-name").await,
        get(&node, &other_scope, "/v1/projects").await,
    ] {
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    }

    // A key limited to some projects or environments may read projects but not change them.
    let (_, scoped) = make_key_in(&node, &a.id, &["services:read", "services:write"], None, Some(vec!["production".into()])).await;
    assert_eq!(get(&node, &scoped, "/v1/projects").await.s(), 200);
    let r = post(&node, &scoped, "/v1/projects", json!({"slug": "scoped"})).await;
    assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);

    // No credential: 401. The operator needs a tenant to act for.
    let r = send(node.http().get(node.control_url("/v1/projects"))).await;
    assert_eq!(r.s(), 401);
    let r = call(root(&node, Method::GET, "/v1/projects"), None).await;
    assert_eq!(r.s(), 422, "x-rt-tenant is required: {}", r.text);
    let r = call(root_for(&node, &a.id, Method::GET, "/v1/projects"), None).await;
    assert_eq!(r.s(), 200, "{}", r.text);
    let r = call(root_for(&node, &a.id, Method::POST, "/v1/projects"), Some(json!({"slug": "by-operator"}))).await;
    assert_eq!(r.s(), 201, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_22_a_tenant_may_own_a_bounded_number_of_projects() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    for i in 0..99 {
        let r = post(&node, &t.key, "/v1/projects", json!({"slug": format!("p{i:03}")})).await;
        assert_eq!(r.s(), 201, "{i}: {}", r.text);
    }
    let r = post(&node, &t.key, "/v1/projects", json!({"slug": "one-too-many"})).await;
    assert_eq!((r.s(), r.code()), (403, "quota_exceeded".to_string()), "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_23_a_services_environment_is_one_of_its_projects() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(post(&node, &t.key, "/v1/projects", json!({"slug": "web", "environments": ["production", "staging"]})).await.s(), 201);

    // Default project and environment.
    let plain = post(&node, &t.key, "/v1/services", json!({"slug": uniq("plain"), "runtime": "wasm"})).await;
    assert_eq!(plain.s(), 201, "{}", plain.text);
    assert_eq!((plain.json["project"].clone(), plain.json["environment"].clone()), (json!("default"), json!("production")));

    // In a project, in one of its environments (the project by slug or by id).
    let web_id = get(&node, &t.key, "/v1/projects/web").await.json["id"].as_str().expect("id").to_string();
    let a = post(&node, &t.key, "/v1/services", json!({"slug": uniq("stg"), "runtime": "wasm", "project": "web", "environment": "staging"})).await;
    let b = post(&node, &t.key, "/v1/services", json!({"slug": uniq("prd"), "runtime": "wasm", "project": web_id})).await;
    assert_eq!((a.s(), b.s()), (201, 201), "{} / {}", a.text, b.text);
    assert_eq!((a.json["project"].clone(), a.json["environment"].clone()), (json!("web"), json!("staging")));
    assert_eq!((b.json["project"].clone(), b.json["environment"].clone()), (json!("web"), json!("production")), "the id names the project too");
    assert_eq!(get(&node, &t.key, &format!("/v1/services/{}", a.json["id"].as_str().unwrap())).await.json["project"], json!("web"));

    // An environment the project does not list, or a project that does not exist: 422.
    let r = post(&node, &t.key, "/v1/services", json!({"slug": uniq("dev"), "runtime": "wasm", "project": "web", "environment": "dev"})).await;
    assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    assert_eq!(r.json["error"]["details"]["environments"], json!(["production", "staging"]));
    assert!(r.json["error"]["message"].as_str().is_some_and(|m| m.contains("dev") && m.contains("web")), "{}", r.text);
    let r = post(&node, &t.key, "/v1/services", json!({"slug": uniq("stg"), "runtime": "wasm", "environment": "staging"})).await;
    assert_eq!(r.s(), 422, "staging is not an environment of `default`: {}", r.text);
    let r = post(&node, &t.key, "/v1/services", json!({"slug": uniq("x"), "runtime": "wasm", "project": "nope"})).await;
    assert_eq!((r.s(), r.code()), (422, "validation_failed".to_string()), "{}", r.text);
    let r = post(&node, &t.key, "/v1/services", json!({"slug": uniq("x"), "runtime": "wasm", "project": "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"})).await;
    assert_eq!(r.s(), 422, "{}", r.text);

    // PATCH environment: only to one the project lists; adding it to the project first works.
    let id = a.json["id"].as_str().unwrap().to_string();
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"environment": "dev"})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    assert_eq!(patch(&node, &t.key, "/v1/projects/web", json!({"environments": ["production", "staging", "dev"]})).await.s(), 200);
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"environment": "dev"})).await;
    assert_eq!((r.s(), r.json["environment"].clone(), r.json["project"].clone()), (200, json!("dev"), json!("web")), "{}", r.text);
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"name": "renamed"})).await;
    assert_eq!(r.s(), 200, "other fields never re-check the environment: {}", r.text);

    // The project of a service cannot be changed through PATCH.
    let r = patch(&node, &t.key, &format!("/v1/services/{id}"), json!({"project": "default"})).await;
    assert_eq!(r.s(), 422, "{}", r.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_23_a_limited_key_creates_services_only_inside_its_projects_and_environments() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let pa = create_project(&node, &t.key, &uniq("pa"), &["production", "dev"]).await;
    let pb = create_project(&node, &t.key, &uniq("pb"), &["production", "dev"]).await;
    let (_, in_a) = make_key(&node, &t.id, &["services:write"], Some(vec![pa.clone()])).await;
    let (_, dev_only) = make_key_in(&node, &t.id, &["services:write"], None, Some(vec!["dev".into()])).await;
    let try_create = |key: String, project: String, env: &'static str| {
        let node = &node;
        async move {
            post(node, &key, "/v1/services", json!({"slug": uniq("lim"), "runtime": "wasm", "project": project, "environment": env})).await
        }
    };

    // Inside the key's limits: created.
    assert_eq!(try_create(in_a.clone(), pa.clone(), "dev").await.s(), 201);
    assert_eq!(try_create(dev_only.clone(), pb.clone(), "dev").await.s(), 201);
    // Another project, another environment, or the default project and environment: refused.
    for r in [
        try_create(in_a.clone(), pb.clone(), "production").await,
        try_create(in_a.clone(), "default".into(), "production").await,
        try_create(dev_only.clone(), pa.clone(), "production").await,
        post(&node, &in_a, "/v1/services", json!({"slug": uniq("lim"), "runtime": "wasm"})).await,
    ] {
        assert_eq!((r.s(), r.code()), (403, "insufficient_scope".to_string()), "{}", r.text);
    }
    let all = get(&node, &t.key, "/v1/services").await;
    assert_eq!(all.items().len(), 2, "nothing was created outside the limits: {}", all.text);
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_23_services_are_listed_by_project_and_environment() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    assert_eq!(post(&node, &t.key, "/v1/projects", json!({"slug": "web", "environments": ["production", "staging"]})).await.s(), 201);
    assert_eq!(post(&node, &t.key, "/v1/projects", json!({"slug": "jobs"})).await.s(), 201);
    let mut slugs = std::collections::HashMap::new();
    for (key, project, env) in [("w-prod", "web", "production"), ("w-stg", "web", "staging"), ("j-prod", "jobs", "production"), ("d-prod", "default", "production")] {
        let slug = uniq(key);
        let r = post(&node, &t.key, "/v1/services", json!({"slug": slug, "runtime": "wasm", "project": project, "environment": env})).await;
        assert_eq!(r.s(), 201, "{}", r.text);
        slugs.insert(key, slug);
    }
    let names = |r: &Resp| {
        let mut v: Vec<String> = r.items().iter().map(|s| s["slug"].as_str().unwrap().to_string()).collect();
        v.sort();
        v
    };
    let want = |keys: &[&str]| {
        let mut v: Vec<String> = keys.iter().map(|k| slugs[k].clone()).collect();
        v.sort();
        v
    };
    assert_eq!(names(&get(&node, &t.key, "/v1/services").await), want(&["w-prod", "w-stg", "j-prod", "d-prod"]));
    assert_eq!(names(&get(&node, &t.key, "/v1/services?project=web").await), want(&["w-prod", "w-stg"]));
    assert_eq!(names(&get(&node, &t.key, "/v1/services?environment=production").await), want(&["w-prod", "j-prod", "d-prod"]));
    assert_eq!(names(&get(&node, &t.key, "/v1/services?project=web&environment=staging").await), want(&["w-stg"]));
    assert!(names(&get(&node, &t.key, "/v1/services?project=jobs&environment=staging").await).is_empty());
    assert!(names(&get(&node, &t.key, "/v1/services?project=nope").await).is_empty());
    let web_id = get(&node, &t.key, "/v1/projects/web").await.json["id"].as_str().unwrap().to_string();
    assert_eq!(names(&get(&node, &t.key, &format!("/v1/services?project={web_id}")).await), want(&["w-prod", "w-stg"]), "by id");
    assert!(names(&get(&node, &t.key, "/v1/services?project=prj_01ARZ3NDEKTSV4RRFFQ69G5FAV").await).is_empty());
    node.shutdown().await;
}
}

node_test! {
async fn ac_api_24_limits_show_the_plan_and_what_is_left_of_it() {
    let node = start_node().await;
    let t = new_tenant(&node, "pro").await;
    let before = get(&node, &t.key, "/v1/limits").await;
    assert_eq!(before.s(), 200, "{}", before.text);
    let plan = get(&node, &t.key, "/v1/me").await.json["plan"]["settings"].clone();
    assert_eq!(before.json["plan"], json!("pro"));
    assert_eq!(before.json["services"]["limit"], plan["max_services"]);
    assert_eq!(before.json["services"]["used"], json!(0));
    assert_eq!(before.json["services"]["remaining"], plan["max_services"]);
    assert_eq!(before.json["invocations"]["limit"], plan["invocations_per_day"]);
    assert_eq!(before.json["invocations"]["period"], json!("day"));
    assert_eq!(before.json["egress_bytes"]["limit"].as_u64().unwrap(), plan["egress_mb_per_day"].as_u64().unwrap() * 1024 * 1024);
    assert_eq!(before.json["storage_bytes"]["limit"].as_u64().unwrap(), plan["storage_mb"].as_u64().unwrap() * 1024 * 1024);
    assert_eq!(before.json["schedules"]["limit"], plan["max_schedules"]);
    assert_eq!(before.json["schedules"]["period"], json!("per_service"));
    assert!(before.json["day"].as_str().is_some_and(|d| d.len() == 10), "{}", before.text);
    let resets = before.json["resets_at"].as_i64().expect("resets_at");
    assert!(resets > 0 && resets % 86_400_000 == 0, "UTC midnight: {resets}");

    // Used numbers move: a service, a schedule, an invocation.
    let slug = uniq("lim");
    ship(&node, &t.key, &slug, &guests::versioned(1)).await;
    let after = get(&node, &t.key, "/v1/limits").await;
    assert_eq!(after.json["services"]["used"], json!(1));
    assert_eq!(after.json["services"]["remaining"].as_u64().unwrap(), plan["max_services"].as_u64().unwrap() - 1);
    assert_eq!(invoke(&node, &slug, "/").await.s(), 200);
    let after = get(&node, &t.key, "/v1/limits").await;
    assert_eq!(after.json["invocations"]["used"], json!(1), "{}", after.text);
    assert_eq!(after.json["invocations"]["remaining"].as_u64().unwrap(), plan["invocations_per_day"].as_u64().unwrap() - 1);

    // Any authenticated tenant key; the operator needs a tenant; no credential is 401.
    let (_, reader) = make_key(&node, &t.id, &["services:read"], None).await;
    assert_eq!(get(&node, &reader, "/v1/limits").await.s(), 200);
    assert_eq!(call(root(&node, Method::GET, "/v1/limits"), None).await.s(), 422);
    assert_eq!(call(root_for(&node, &t.id, Method::GET, "/v1/limits"), None).await.s(), 200);
    assert_eq!(send(node.http().get(node.control_url("/v1/limits"))).await.s(), 401);
    node.shutdown().await;
}
}
