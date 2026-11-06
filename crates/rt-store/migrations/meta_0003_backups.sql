-- Node backups (`rt-backup`, docs/deploy.md): one row per run. A restored meta.db describes the
-- past: restore marks its `running` rows failed.
CREATE TABLE backups (
  id          TEXT PRIMARY KEY,                               -- bkp_<ulid>, time-ordered
  state       TEXT NOT NULL CHECK (state IN ('running', 'uploading', 'verifying', 'ok', 'failed')),
  trigger     TEXT NOT NULL CHECK (trigger IN ('schedule', 'api')),
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  bytes       INTEGER,
  sha256      TEXT,
  encrypted   INTEGER NOT NULL DEFAULT 0 CHECK (encrypted IN (0, 1)),
  local_path  TEXT,
  remote_key  TEXT,
  verified_at INTEGER,
  rt_version  TEXT NOT NULL,
  failure     TEXT CHECK (failure IS NULL OR json_valid(failure))
) STRICT;
CREATE INDEX backups_ok ON backups(finished_at) WHERE state = 'ok';
