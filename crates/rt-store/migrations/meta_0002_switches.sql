-- Branching services: adopted versions and tracked flips (docs/design/branching-services-design.md §3).

-- {"service":"svc_…","version":12}; written only by adopt_version.
ALTER TABLE versions ADD COLUMN origin TEXT CHECK (origin IS NULL OR json_valid(origin));

CREATE TABLE version_switches (
  id             TEXT PRIMARY KEY,                     -- swt_<ulid>, increasing per service
  service_id     TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  kind           TEXT NOT NULL CHECK (kind IN ('promote', 'switch', 'rollback')),
  state          TEXT NOT NULL CHECK (state IN ('preparing', 'migrating', 'draining', 'completed', 'drain_timed_out', 'failed')),
  source_service TEXT,                                 -- switch only
  source_version INTEGER,
  from_version   INTEGER,
  to_version     INTEGER NOT NULL,
  error          TEXT,                                 -- stable code only
  interrupted    INTEGER NOT NULL DEFAULT 0 CHECK (interrupted IN (0, 1)),
  drain_deadline INTEGER,
  created_by     TEXT,
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL
) STRICT;
CREATE INDEX version_switches_service ON version_switches(service_id, id);
CREATE UNIQUE INDEX version_switches_active ON version_switches(service_id) WHERE state IN ('preparing', 'migrating');
