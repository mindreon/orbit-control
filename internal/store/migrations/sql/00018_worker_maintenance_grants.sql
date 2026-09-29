-- Maintenance grants for orbit_worker (docs/persistence-failure-modes.md, "stage_attempts" and "tenants" rows;
-- check: ISO-25).
--
-- The attempt cleanup corrects a stage_attempts row whose AttemptWorkflow no longer exists instead of deleting it, so
-- the DELETE granted in 00013 goes and a column-level UPDATE takes its place. The maintenance schedules run once per
-- tenant, so the worker may list tenant ids (and nothing else about a tenant).

-- +goose Up

GRANT UPDATE (status, failure, finished_at, entity_version) ON stage_attempts TO orbit_worker;
REVOKE DELETE ON stage_attempts FROM orbit_worker;
GRANT SELECT (id) ON tenants TO orbit_worker;

-- +goose Down

REVOKE SELECT (id) ON tenants FROM orbit_worker;
GRANT DELETE ON stage_attempts TO orbit_worker;
REVOKE UPDATE (status, failure, finished_at, entity_version) ON stage_attempts FROM orbit_worker;
