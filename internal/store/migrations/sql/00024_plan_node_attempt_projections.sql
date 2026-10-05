-- The Projector now writes plan_versions, task_nodes and stage_attempts from durable events (09 §3).
-- Today's events carry a plan's hash but not its graph, and a node's status but not its type, title, workspace mode or
-- owner, so those columns become nullable until the events carry them. orbit_app gets column-level UPDATE on exactly what
-- an event changes; identity columns are never updated, and nothing is deleted. Reasons and checks:
-- docs/persistence-failure-modes.md ("Task runtime tables", ISO-28).

-- +goose Up

ALTER TABLE plan_versions ALTER COLUMN graph DROP NOT NULL;

ALTER TABLE task_nodes
  ALTER COLUMN node_type      DROP NOT NULL,
  ALTER COLUMN title          DROP NOT NULL,
  ALTER COLUMN workspace_mode DROP NOT NULL,
  ALTER COLUMN owner_profile  DROP NOT NULL,
  ADD COLUMN reason TEXT NOT NULL DEFAULT '';

GRANT UPDATE (status, reason, frozen, entity_version, updated_at) ON task_nodes TO orbit_app;
GRANT UPDATE (status, failure, usage, finished_at, entity_version) ON stage_attempts TO orbit_app;

-- +goose Down

REVOKE UPDATE (status, failure, usage, finished_at, entity_version) ON stage_attempts FROM orbit_app;
REVOKE UPDATE (status, reason, frozen, entity_version, updated_at) ON task_nodes FROM orbit_app;

-- The columns stay nullable: with FORCE ROW LEVEL SECURITY the owner cannot fill the NULLs of every tenant's rows, so
-- restoring NOT NULL would make a down fail on any database that has projected a node or a plan version.
ALTER TABLE task_nodes DROP COLUMN reason;
