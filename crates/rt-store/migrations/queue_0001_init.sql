-- queue.db schema v1. Separate file to avoid contending with meta.db.
CREATE TABLE messages (
  id          TEXT PRIMARY KEY,                 -- msg_<ulid>
  service_id  TEXT NOT NULL,
  body        BLOB NOT NULL,
  attempt     INTEGER NOT NULL DEFAULT 0,
  visible_at  INTEGER NOT NULL,                 -- unix ms; leased messages are invisible until then
  enqueued_at INTEGER NOT NULL,
  dedup_id    TEXT,
  leased      INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE INDEX messages_ready ON messages(service_id, visible_at);
CREATE UNIQUE INDEX messages_dedup ON messages(service_id, dedup_id) WHERE dedup_id IS NOT NULL;

CREATE TABLE dlq (
  id          TEXT PRIMARY KEY,
  service_id  TEXT NOT NULL,
  body        BLOB NOT NULL,
  attempts    INTEGER NOT NULL,
  failed_at   INTEGER NOT NULL
) STRICT;
CREATE INDEX dlq_service ON dlq(service_id, failed_at);
-- 24 h de-duplication window that outlives the message.
CREATE TABLE dedup (
  service_id TEXT NOT NULL,
  dedup_id   TEXT NOT NULL,
  msg_id     TEXT NOT NULL,
  expires_at INTEGER NOT NULL,                  -- unix ms
  PRIMARY KEY (service_id, dedup_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX dedup_expires ON dedup(expires_at);
