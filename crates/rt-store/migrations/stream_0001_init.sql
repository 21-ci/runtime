-- streams.db v1: topics, records, partition heads, dedup, consumer cursors, dead letters.
CREATE TABLE topics (
    id              TEXT PRIMARY KEY,            -- tpc_<ulid>
    tenant_id       TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    environment     TEXT NOT NULL,
    name            TEXT NOT NULL,
    partitions      INTEGER NOT NULL CHECK (partitions BETWEEN 1 AND 16),
    retention_ms    INTEGER NOT NULL CHECK (retention_ms > 0),
    max_bytes       INTEGER NOT NULL DEFAULT 0,  -- 0 = only the tenant cap
    bytes           INTEGER NOT NULL DEFAULT 0,  -- sum(records.size) of the topic
    dead_bytes      INTEGER NOT NULL DEFAULT 0,  -- size of the topic's dead letters (counts toward the tenant cap)
    created_at      INTEGER NOT NULL,
    UNIQUE (tenant_id, project_id, environment, name)
);
CREATE INDEX topics_tenant ON topics (tenant_id);

CREATE TABLE partitions (
    topic_id   TEXT NOT NULL,
    part       INTEGER NOT NULL,
    first_off  INTEGER NOT NULL DEFAULT 0,       -- first retained offset
    next_off   INTEGER NOT NULL DEFAULT 0,       -- offset of the next append
    last_ts    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, part)
) WITHOUT ROWID;

CREATE TABLE records (
    topic_id  TEXT NOT NULL,
    part      INTEGER NOT NULL,
    off       INTEGER NOT NULL,
    ts        INTEGER NOT NULL,
    key       TEXT,
    headers   BLOB,                               -- JSON [[name, value]]; NULL when empty
    body      BLOB NOT NULL,
    size      INTEGER NOT NULL,                   -- len(body) + len(headers) + len(key)
    PRIMARY KEY (topic_id, part, off)
) WITHOUT ROWID;
CREATE INDEX records_ts ON records (topic_id, part, ts);

CREATE TABLE dedup (
    topic_id    TEXT NOT NULL,
    dedup_id    TEXT NOT NULL,
    part        INTEGER NOT NULL,
    off         INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    PRIMARY KEY (topic_id, dedup_id)
) WITHOUT ROWID;
CREATE INDEX dedup_expiry ON dedup (expires_at);

CREATE TABLE cursors (
    topic_id    TEXT NOT NULL,
    service_id  TEXT NOT NULL,
    grp         TEXT NOT NULL,
    part        INTEGER NOT NULL,
    committed   INTEGER NOT NULL,                 -- next offset to deliver
    attempt     INTEGER NOT NULL DEFAULT 0,       -- failed deliveries of the record at `committed`
    next_at     INTEGER NOT NULL DEFAULT 0,       -- unix ms, not before (backoff)
    state       TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'blocked')),
    last_error  TEXT,
    gen         INTEGER NOT NULL DEFAULT 0,       -- bumped by reset: fences late settles
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (topic_id, service_id, grp, part)
) WITHOUT ROWID;
CREATE INDEX cursors_service ON cursors (service_id);

CREATE TABLE dead (
    topic_id    TEXT NOT NULL,
    service_id  TEXT NOT NULL,
    grp         TEXT NOT NULL,
    part        INTEGER NOT NULL,
    off         INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    key         TEXT,
    headers     BLOB,
    body        BLOB NOT NULL,
    size        INTEGER NOT NULL,                 -- same measure as records.size
    attempts    INTEGER NOT NULL,
    error       TEXT NOT NULL,
    failed_at   INTEGER NOT NULL,
    PRIMARY KEY (topic_id, service_id, grp, part, off)
) WITHOUT ROWID;
CREATE INDEX dead_service ON dead (service_id);
CREATE INDEX dead_failed ON dead (failed_at);
CREATE INDEX dead_consumer ON dead (topic_id, service_id, grp, failed_at);
