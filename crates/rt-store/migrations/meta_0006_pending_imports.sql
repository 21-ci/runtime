-- An import in progress (R12): written before its service exists, removed when it ends either way.
-- What a crash leaves here is taken back out at the next start. `service_id` is set once the service
-- exists; not a foreign key, the note must outlive nothing and block nothing.
CREATE TABLE pending_imports (
  id          TEXT PRIMARY KEY,
  tenant_id   TEXT NOT NULL,
  slug        TEXT NOT NULL,
  service_id  TEXT,
  started_at  INTEGER NOT NULL
);
