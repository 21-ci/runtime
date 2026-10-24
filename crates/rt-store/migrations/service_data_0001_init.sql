-- Per-service data.db bootstrap. User SQL lives in the same file; the authorizer
-- denies guest access to `_rt_*` tables.
CREATE TABLE IF NOT EXISTS _rt_kv (
  key        BLOB PRIMARY KEY,
  value      BLOB NOT NULL,
  tag        INTEGER NOT NULL DEFAULT 0,        -- 0 = JSON text, 1 = bytes
  expires_at INTEGER                            -- unix ms, NULL = never
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _rt_kv_expires ON _rt_kv(expires_at) WHERE expires_at IS NOT NULL;
-- Durable objects: each object's keys, and its alarm. `format` 1 = V8 structured clone
-- (Deno), 0 = raw bytes (WASM).
CREATE TABLE IF NOT EXISTS _rt_durable (
  object TEXT    NOT NULL,
  key    BLOB    NOT NULL,
  value  BLOB    NOT NULL,
  format INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (object, key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _rt_durable_alarm (
  object TEXT PRIMARY KEY,
  at_ms  INTEGER NOT NULL                       -- unix ms
) WITHOUT ROWID;
