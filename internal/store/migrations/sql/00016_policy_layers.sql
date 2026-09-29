-- Policy layers (05 §6): a tenant-wide policy and a per-task policy. The profile layer already lives in
-- agent_profiles.spec. Grants: docs/persistence-failure-modes.md ("Task runtime tables").

-- +goose Up

CREATE TABLE tenant_policy (
  tenant_id  TEXT PRIMARY KEY REFERENCES tenants(id),
  spec       JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE tenant_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_policy FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_policy FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON tenant_policy TO orbit_app;
GRANT UPDATE (spec, updated_at) ON tenant_policy TO orbit_app;
GRANT SELECT ON tenant_policy TO orbit_worker;

-- Set when the task is created; the workflow carries it to its attempts, so the worker never reads `tasks`.
ALTER TABLE tasks ADD COLUMN policy JSONB NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down

ALTER TABLE tasks DROP COLUMN policy;
DROP TABLE tenant_policy;
