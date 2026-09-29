-- Contract §18.5 RLS backstop. T means
--   tenant_id = current_setting('app.tenant_id', true)
-- missing_ok=true makes an unset GUC evaluate to NULL/'' so nothing matches.
-- The GUC is only ever set with SELECT set_config('app.tenant_id', $1, true).

-- +goose Up

ALTER TABLE personas         ENABLE ROW LEVEL SECURITY;
ALTER TABLE personas         FORCE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors   ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors   FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON personas FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON mcp_connectors FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- orbit_app is not the owner and has no BYPASSRLS; it gets the least DML the code paths need. Rows are appended and
-- read, never updated or deleted.
-- Tenants are created by orbit_ops (deploy/postgres/ensure-tenant.sql).
GRANT SELECT ON tenants TO orbit_app;
GRANT SELECT, INSERT ON tenants TO orbit_ops;
GRANT SELECT, INSERT ON personas, mcp_connectors TO orbit_app;

-- TRUNCATE bypasses RLS; REFERENCES and TRIGGER are never needed by the app.
REVOKE TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM orbit_app, PUBLIC;

-- +goose Down
REVOKE ALL ON tenants FROM orbit_ops;
REVOKE ALL ON tenants, personas, mcp_connectors FROM orbit_app;

DROP POLICY tenant_isolation ON mcp_connectors;
DROP POLICY tenant_isolation ON personas;

ALTER TABLE mcp_connectors    NO FORCE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors    DISABLE ROW LEVEL SECURITY;
ALTER TABLE personas          NO FORCE ROW LEVEL SECURITY;
ALTER TABLE personas          DISABLE ROW LEVEL SECURITY;
