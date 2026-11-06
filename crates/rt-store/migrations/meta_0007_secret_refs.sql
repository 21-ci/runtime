-- R15: secrets whose value is synced from an external source into the sealed store.
CREATE TABLE secret_refs (
  service_id     TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  name           TEXT NOT NULL,
  path           TEXT NOT NULL,
  field          TEXT NOT NULL,
  kv_version     INTEGER,
  last_synced_at INTEGER,
  last_error     TEXT,
  PRIMARY KEY (service_id, name)
) STRICT;
