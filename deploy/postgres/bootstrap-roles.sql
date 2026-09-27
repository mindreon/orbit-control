-- One-time cluster bootstrap for orbit-control (contract §18.2, §18.5).
-- Run as a superuser against the maintenance database:
--
--   psql -v ON_ERROR_STOP=1 \
--        -v owner_password="$ORBIT_CONTROL_OWNER_PASSWORD" \
--        -v app_password="$ORBIT_CONTROL_APP_PASSWORD" \
--        -v ops_password="$ORBIT_CONTROL_OPS_PASSWORD" \
--        -f deploy/postgres/bootstrap-roles.sql "$SUPERUSER_URL"
--
-- Passwords only ever arrive as psql variables; never commit them. Prefer
-- SCRAM-SHA-256 verifiers (deploy/postgres/scram-verifier.py): Postgres stores
-- a value already in "SCRAM-SHA-256$..." form as-is, so the plaintext never
-- reaches the server.
--
-- Roles:
--   orbit_owner    LOGIN, owns orbit_control and every table; used only by
--                  goose via ORBIT_CONTROL_MIGRATE_DB_URL. BYPASSRLS because
--                  FORCE ROW LEVEL SECURITY applies to table owners too, and
--                  audit reads of soft-deleted rows and cross-tenant
--                  migrations must see every row.
--   orbit_app      LOGIN, NOBYPASSRLS, not an owner; ORBIT_CONTROL_DB_URL.
--   orbit_ops      LOGIN, NOBYPASSRLS; creates tenants (ensure-tenant.sql).
--                  Granted on tenants only; control never uses it.
--   orbit_definer  NOLOGIN BYPASSRLS; owns orbit_soft_delete_room only.

-- Keep CREATE ROLE ... PASSWORD out of the server log, even on error
-- (session-local; needs the superuser this script runs as).
SET log_statement = 'none';
SET log_min_error_statement = 'panic';
SET log_min_duration_statement = -1;

SELECT format(
  'CREATE ROLE orbit_owner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE BYPASSRLS PASSWORD %L',
  :'owner_password')
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_owner')
\gexec

SELECT format(
  'CREATE ROLE orbit_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L',
  :'app_password')
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_app')
\gexec

SELECT format(
  'CREATE ROLE orbit_ops LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD %L',
  :'ops_password')
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_ops')
\gexec

SELECT 'CREATE ROLE orbit_definer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE BYPASSRLS'
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_definer')
\gexec

-- Lets the migration hand orbit_soft_delete_room to orbit_definer.
GRANT orbit_definer TO orbit_owner;

SELECT 'CREATE DATABASE orbit_control OWNER orbit_owner'
 WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'orbit_control')
\gexec

REVOKE ALL ON DATABASE orbit_control FROM PUBLIC;
GRANT CONNECT ON DATABASE orbit_control TO orbit_app, orbit_ops;
