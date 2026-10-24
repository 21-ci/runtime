-- queue.db schema v2: one-off schedules created by guests (`std:schedule`).
-- Pending until delivered (at-least-once: a lease that runs out is delivered again), persisted
-- across restarts. `gen` identifies one put, so a delivery of an older put cannot finish a newer one.
CREATE TABLE delayed (
  service_id   TEXT NOT NULL,
  id           TEXT NOT NULL,
  gen          INTEGER NOT NULL,
  run_at       INTEGER NOT NULL,                -- unix ms
  leased_until INTEGER NOT NULL DEFAULT 0,      -- unix ms; a delivery in flight until then
  attempts     INTEGER NOT NULL DEFAULT 0,
  payload      BLOB NOT NULL,                   -- JSON text
  PRIMARY KEY (service_id, id)
) STRICT, WITHOUT ROWID;
CREATE INDEX delayed_due ON delayed(run_at);
