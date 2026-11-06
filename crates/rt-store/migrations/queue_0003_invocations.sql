-- queue.db v3: Lambda-style async invocations of the HTTP handler.
-- `headers` and `body` hold sealed bytes (the caller seals them; this store never reads them).
CREATE TABLE invocations (
    id               TEXT PRIMARY KEY,           -- inv_<ulid>
    service_id       TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('pending', 'succeeded', 'failed')),
    method           TEXT NOT NULL,
    uri              TEXT NOT NULL,              -- path + query as the handler sees it
    headers          BLOB,                       -- sealed JSON [[name, value]]; NULL once succeeded
    body             BLOB,                       -- sealed body; NULL once succeeded
    body_bytes       INTEGER NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0, -- deliveries started
    max_attempts     INTEGER NOT NULL,
    leased           INTEGER NOT NULL DEFAULT 0,
    visible_at       INTEGER NOT NULL,
    max_age_ms       INTEGER NOT NULL,           -- async.max_event_age_s at accept
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    expires_at       INTEGER NOT NULL,           -- pending: created_at + max_age_ms; finished: when the record goes
    status           INTEGER,                    -- last handler status
    condition        TEXT,                       -- success | retries_exhausted | event_age_exceeded
    error            TEXT,                       -- last failure code
    idempotency_key  TEXT,
    request_hash     TEXT NOT NULL               -- hex sha256 of method, uri and body
);
CREATE INDEX invocations_ready ON invocations (service_id, visible_at) WHERE state = 'pending';
CREATE INDEX invocations_state ON invocations (service_id, state, created_at);
CREATE INDEX invocations_expiry ON invocations (expires_at);
CREATE UNIQUE INDEX invocations_idem ON invocations (service_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
