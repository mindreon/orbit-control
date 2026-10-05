-- The runtime's durable events now say what a node is (node.status_changed) and which attempt of its node an ending
-- attempt was (attempt.finished), and a worker's attempt.resumed carries its order within the attempt (09 §3). The
-- Projector fills the node's identity columns from the event and keeps, per attempt, when a workflow event last set its
-- status and the (activity_attempt, state_version) of the last resumed event it applied, so a stale resumed event cannot
-- turn a parked attempt back to RUNNING. Reasons and checks: docs/persistence-failure-modes.md (ISO-29).

-- +goose Up

ALTER TABLE task_nodes ADD COLUMN depends_on JSONB;

ALTER TABLE stage_attempts
  ADD COLUMN status_changed_at        TIMESTAMPTZ,
  ADD COLUMN resumed_activity_attempt INTEGER NOT NULL DEFAULT 0 CHECK (resumed_activity_attempt >= 0),
  ADD COLUMN resumed_state_version    INTEGER NOT NULL DEFAULT 0 CHECK (resumed_state_version >= 0);

GRANT UPDATE (node_type, title, workspace_mode, owner_profile, depends_on, attempt_count, current_attempt_id) ON task_nodes TO orbit_app;
GRANT UPDATE (status_changed_at, resumed_activity_attempt, resumed_state_version) ON stage_attempts TO orbit_app;

-- +goose Down

REVOKE UPDATE (status_changed_at, resumed_activity_attempt, resumed_state_version) ON stage_attempts FROM orbit_app;
REVOKE UPDATE (node_type, title, workspace_mode, owner_profile, depends_on, attempt_count, current_attempt_id) ON task_nodes FROM orbit_app;

ALTER TABLE stage_attempts
  DROP COLUMN resumed_state_version,
  DROP COLUMN resumed_activity_attempt,
  DROP COLUMN status_changed_at;

ALTER TABLE task_nodes DROP COLUMN depends_on;
