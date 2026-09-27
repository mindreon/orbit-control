-- Create a tenant. Ops only: run as orbit_ops (control's orbit_app role can
-- only SELECT tenants and refuses to start if its default tenant is missing).
--
--   psql -v ON_ERROR_STOP=1 -v tenant_id=default [-v tenant_name=Default] \
--        -f deploy/postgres/ensure-tenant.sql "$ORBIT_CONTROL_OPS_DB_URL"
\if :{?tenant_name}
\else
  \set tenant_name :tenant_id
\endif
INSERT INTO tenants (id, name) VALUES (:'tenant_id', :'tenant_name')
ON CONFLICT (id) DO NOTHING;
