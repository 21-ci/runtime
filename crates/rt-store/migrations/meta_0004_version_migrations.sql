-- SQL migrations a version carries. Written only from the validator's result or the write-once WASM
-- route; `meta` is client-written and is not trusted for this.
ALTER TABLE versions ADD COLUMN migrations TEXT;      -- JSON [{name, sha256}], NULL when none
ALTER TABLE versions ADD COLUMN migrations_sql TEXT;  -- JSON [{name, sha256, sql}], NULL when none
