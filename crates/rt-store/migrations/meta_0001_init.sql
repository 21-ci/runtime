-- meta.db schema v1. The project is unreleased, so the schema is one
-- consolidated file; later changes add numbered migrations again once a release exists.
--
-- Foreign keys into `projects` and `project_environments` use the default NO ACTION, not RESTRICT:
-- a tenant delete cascades through projects and services in one statement, and NO ACTION is
-- checked at the end of the statement, after the cascades.
--
-- Slugs: service slugs are globally unique (they route `/s/{slug}` across tenants, like app names
-- on Heroku or Vercel). Project slugs are unique per tenant only.

CREATE TABLE tenants (
  id          TEXT PRIMARY KEY,                              -- ten_<ulid>
  name        TEXT NOT NULL,
  plan        TEXT NOT NULL,
  overrides   TEXT CHECK (overrides IS NULL OR json_valid(overrides)),  -- partial Plan JSON; NULL = none
  created_at  INTEGER NOT NULL,                              -- unix ms
  disabled    INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0, 1))
) STRICT;

CREATE TABLE projects (
  id          TEXT PRIMARY KEY,                              -- prj_<ulid>
  tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  slug        TEXT NOT NULL,                                 -- unique per tenant, immutable
  name        TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL,
  UNIQUE (tenant_id, slug),
  UNIQUE (id, tenant_id)                                     -- target of the composite foreign keys
) STRICT;

-- The environments a project's services may be in. Owned by the project; a label is only
-- meaningful inside its project (`env:production` of project A is not project B's production).
CREATE TABLE project_environments (
  project_id  TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name        TEXT NOT NULL,                                 -- ^[a-z][a-z0-9-]{0,31}$
  position    INTEGER NOT NULL,                              -- display order
  PRIMARY KEY (project_id, name)
) STRICT, WITHOUT ROWID;

CREATE TABLE services (
  id              TEXT PRIMARY KEY,                          -- svc_<ulid>
  tenant_id       TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  project_id      TEXT NOT NULL,
  environment     TEXT NOT NULL,
  slug            TEXT NOT NULL UNIQUE,                      -- global, see the header
  runtime         TEXT NOT NULL CHECK (runtime IN ('deno', 'wasm')),
  name            TEXT,
  profile         TEXT,                                      -- NULL = node default; values from kinds::Profile
  config          TEXT NOT NULL CHECK (json_valid(config)),  -- limits, scaling, access, triggers, ingress, egress, call
  secrets_version INTEGER NOT NULL DEFAULT 0,                -- generation of env + secrets (bumped on write)
  etag            INTEGER NOT NULL DEFAULT 1,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  FOREIGN KEY (project_id, tenant_id) REFERENCES projects(id, tenant_id),
  FOREIGN KEY (project_id, environment) REFERENCES project_environments(project_id, name)
) STRICT;
CREATE INDEX services_tenant ON services(tenant_id, id);
CREATE INDEX services_project ON services(project_id, environment, slug);

CREATE TABLE versions (
  service_id       TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  number           INTEGER NOT NULL CHECK (number >= 1),
  source_digest    TEXT NOT NULL,                            -- sha256:<hex>
  lock_digest      TEXT,
  bundle_digest    TEXT,                                     -- Deno audit; NULL for WASM
  build_state      TEXT NOT NULL CHECK (build_state IN ('pending', 'building', 'ready', 'failed')),
  pending_reason   TEXT CHECK (pending_reason IS NULL OR build_state = 'pending'),
  failure          TEXT CHECK (failure IS NULL OR build_state = 'failed'),  -- JSON BuildFailure
  is_live          INTEGER NOT NULL DEFAULT 0 CHECK (is_live IN (0, 1)),   -- the service's live version
  build_started_at INTEGER,                                  -- when a build started (builds per hour)
  created_at       INTEGER NOT NULL,
  PRIMARY KEY (service_id, number)
) STRICT;
CREATE UNIQUE INDEX versions_one_live ON versions(service_id) WHERE is_live = 1;
CREATE INDEX versions_source_digest ON versions(source_digest);
CREATE INDEX versions_lock_digest ON versions(lock_digest) WHERE lock_digest IS NOT NULL;
CREATE INDEX versions_build_started ON versions(build_started_at) WHERE build_started_at IS NOT NULL;

-- Vendored module artifacts a recorded `rt.lock` lists (GC frees a module no lock references).
CREATE TABLE lock_modules (
  lock_digest   TEXT NOT NULL,                              -- sha256:<hex> of the rt.lock artifact
  module_digest TEXT NOT NULL,                              -- sha256:<hex> of one vendored module
  PRIMARY KEY (lock_digest, module_digest)
) STRICT, WITHOUT ROWID;
CREATE INDEX lock_modules_module ON lock_modules(module_digest);

CREATE TABLE env (
  service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  name       TEXT NOT NULL,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (service_id, name)
) STRICT, WITHOUT ROWID;

CREATE TABLE secrets (
  service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  name       TEXT NOT NULL,
  sealed     BLOB NOT NULL,                                 -- nonce || AES-256-GCM (AAD tenant|service|name)
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (service_id, name)
) STRICT, WITHOUT ROWID;

CREATE TABLE hosts (
  host         TEXT PRIMARY KEY,
  service_id   TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  status       TEXT NOT NULL CHECK (status IN ('pending_verification', 'active')),
  verify_token TEXT,                                        -- only while a claim is pending
  created_at   INTEGER NOT NULL,                            -- when the claim was made (pending TTL)
  CHECK (status = 'pending_verification' OR verify_token IS NULL)
) STRICT;
CREATE INDEX hosts_service ON hosts(service_id);
CREATE INDEX hosts_pending ON hosts(created_at) WHERE status = 'pending_verification';

-- A pending claim that ended is remembered for one more TTL (custom-host squatting, review 5).
CREATE TABLE host_releases (
  host         TEXT NOT NULL,
  tenant_id    TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  created_at   INTEGER NOT NULL,
  verify_token TEXT,
  released_at  INTEGER NOT NULL,
  PRIMARY KEY (host, tenant_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX host_releases_released ON host_releases(released_at);

CREATE TABLE schedules (
  id            TEXT PRIMARY KEY,                           -- sch_<ulid>
  service_id    TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL CHECK (kind IN ('interval', 'cron')),
  every_seconds INTEGER CHECK (every_seconds IS NULL OR every_seconds > 0),
  cron          TEXT,
  timezone      TEXT,
  payload       TEXT NOT NULL DEFAULT 'null' CHECK (json_valid(payload)),
  enabled       INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
  next_due_at   INTEGER,                                    -- unix ms; NULL until the trigger engine computes it
  CHECK ((kind = 'interval' AND every_seconds IS NOT NULL AND cron IS NULL AND timezone IS NULL)
      OR (kind = 'cron' AND cron IS NOT NULL AND timezone IS NOT NULL AND every_seconds IS NULL))
) STRICT;
CREATE INDEX schedules_service ON schedules(service_id);
CREATE INDEX schedules_due ON schedules(next_due_at) WHERE enabled = 1;

CREATE TABLE api_keys (
  id            TEXT PRIMARY KEY,                           -- key_<ulid>
  tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name          TEXT,
  secret_hmac   BLOB NOT NULL,                              -- HMAC-SHA256(pepper, secret)
  scopes        TEXT NOT NULL CHECK (json_valid(scopes)),   -- JSON array of scope names
  all_projects  INTEGER NOT NULL DEFAULT 0 CHECK (all_projects IN (0, 1)),
                -- 1: every project of the tenant, including ones created later.
                -- 0: exactly the rows in api_key_projects (none = no project).
  all_environments INTEGER NOT NULL DEFAULT 0 CHECK (all_environments IN (0, 1)),
                -- 1: every environment. 0: exactly the rows in api_key_environments.
  expires_at    INTEGER,
  created_at    INTEGER NOT NULL,
  last_used_at  INTEGER,
  revoked_at    INTEGER,                                    -- NULL = live
  parent_key_id TEXT REFERENCES api_keys(id) ON DELETE CASCADE,  -- minted-by tree (revoke_key_tree)
  UNIQUE (id, tenant_id)
) STRICT;
CREATE INDEX api_keys_tenant ON api_keys(tenant_id);
CREATE INDEX api_keys_parent ON api_keys(parent_key_id);

-- The projects a key may touch. Deleting a project removes its rows (the key loses that project).
CREATE TABLE api_key_projects (
  key_id     TEXT NOT NULL,
  tenant_id  TEXT NOT NULL,
  project_id TEXT NOT NULL,
  PRIMARY KEY (key_id, project_id),
  FOREIGN KEY (key_id, tenant_id) REFERENCES api_keys(id, tenant_id) ON DELETE CASCADE,
  FOREIGN KEY (project_id, tenant_id) REFERENCES projects(id, tenant_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX api_key_projects_project ON api_key_projects(project_id);

-- The environment labels a key may touch (in every project it covers).
CREATE TABLE api_key_environments (
  key_id      TEXT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
  environment TEXT NOT NULL,
  PRIMARY KEY (key_id, environment)
) STRICT, WITHOUT ROWID;

CREATE TABLE usage (
  tenant_id     TEXT NOT NULL,                              -- no FK: rows outlive a non-purge delete
  service_id    TEXT NOT NULL,                              -- no FK: history
  day           TEXT NOT NULL CHECK (day GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),  -- UTC
  invocations   INTEGER NOT NULL DEFAULT 0,
  cpu_ms        INTEGER NOT NULL DEFAULT 0,
  egress_bytes  INTEGER NOT NULL DEFAULT 0,
  storage_bytes INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, service_id, day)
) STRICT, WITHOUT ROWID;

CREATE TABLE events (
  id         TEXT PRIMARY KEY,                              -- evt_<ulid> (time-ordered)
  ts         INTEGER NOT NULL,
  tenant_id  TEXT,                                          -- no FK: history
  service_id TEXT,                                          -- no FK: history
  kind       TEXT NOT NULL,
  data       TEXT NOT NULL DEFAULT 'null' CHECK (json_valid(data))
) STRICT;
CREATE INDEX events_service ON events(service_id, id);
CREATE INDEX events_tenant ON events(tenant_id, id);        -- tenant purge

CREATE TABLE cache_freq (
  id       INTEGER PRIMARY KEY CHECK (id = 1),
  snapshot BLOB NOT NULL
) STRICT;

-- Operator SDKs: runtime-owned services of the system tenant. A release
-- is one version of the SDK's service tagged with a semver; channels point at releases; grants
-- attach to a scope (node, plan, tenant, project, environment) and are inherited downwards.
CREATE TABLE sdks (
  name        TEXT PRIMARY KEY,                              -- ^[a-z][a-z0-9_]{1,31}$ (sdk("name"))
  service_id  TEXT NOT NULL UNIQUE REFERENCES services(id),  -- the SDK's service in the system tenant
  description TEXT,
  state       TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'removed')),
  created_at  INTEGER NOT NULL,
  removed_at  INTEGER
) STRICT;

CREATE TABLE sdk_releases (
  sdk         TEXT NOT NULL REFERENCES sdks(name) ON DELETE CASCADE,
  version     TEXT NOT NULL,                                 -- semver, "1.4.2"
  version_no  INTEGER NOT NULL,                              -- the service version that runs it
  members     TEXT NOT NULL CHECK (json_valid(members)),     -- top-level names of the contract
  contract    TEXT NOT NULL,                                 -- the published .d.ts
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (sdk, version)
) STRICT;

CREATE TABLE sdk_channels (
  sdk         TEXT NOT NULL,
  channel     TEXT NOT NULL,                                 -- stable | canary | ...
  version     TEXT NOT NULL,
  previous    TEXT,                                          -- the version before the last move (rollback)
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (sdk, channel),
  FOREIGN KEY (sdk, version) REFERENCES sdk_releases(sdk, version) ON DELETE CASCADE
) STRICT;

CREATE TABLE sdk_grants (
  scope_kind  TEXT NOT NULL CHECK (scope_kind IN ('node', 'plan', 'tenant', 'project', 'environment')),
  scope_id    TEXT NOT NULL,                                 -- '' | plan | ten_… | prj_… | prj_…/env
  sdk         TEXT NOT NULL,                                 -- an SDK name or '*'
  effect      TEXT CHECK (effect IS NULL OR effect IN ('allow', 'deny')),  -- NULL inherits
  version_req TEXT,                                          -- semver requirement, NULL inherits
  channel     TEXT,                                          -- NULL inherits ('stable' at the top)
  created_by  TEXT NOT NULL,                                 -- who wrote it (audit)
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (scope_kind, scope_id, sdk)
) STRICT, WITHOUT ROWID;
