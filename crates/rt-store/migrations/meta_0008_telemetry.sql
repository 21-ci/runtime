-- The anonymous daily report (SECURITY.md "Telemetry"). One row, written only while telemetry is on,
-- apart from the user count a console pushes, which stays local when it is off.
CREATE TABLE IF NOT EXISTS telemetry_state (
  id               INTEGER PRIMARY KEY CHECK (id = 1),
  install_id       TEXT,      -- 32 hex characters, created on the first send
  first_start_ms   INTEGER,   -- unix ms: the first send is 24 hours later
  last_sent_ms     INTEGER,
  console_users    INTEGER,
  console_users_at INTEGER
) STRICT;
