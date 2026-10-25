-- queue.db v4: per-tenant byte budget of async invocations.
-- `tenant_id` names the owner (NULL for records accepted before v4: not counted);
-- `stored_bytes` is the sealed headers + sealed body size, counted while pending or failed.
ALTER TABLE invocations ADD COLUMN tenant_id TEXT;
ALTER TABLE invocations ADD COLUMN stored_bytes INTEGER NOT NULL DEFAULT 0;
CREATE INDEX invocations_tenant_bytes ON invocations (tenant_id, stored_bytes)
    WHERE state != 'succeeded' AND tenant_id IS NOT NULL;
