-- Soft task deletion. DELETE /v1/tasks/:taskId hides the task from the API by
-- stamping deleted_at; the row stays so the projector can still apply late
-- workflow events without resurrecting the task or wedging on a missing row.
-- A soft delete also keeps checkpoints, artifacts and the Temporal workflow
-- history intact for audit. Reasons and checks: docs/persistence-failure-modes.md
-- ("Task runtime tables").

-- +goose Up

ALTER TABLE tasks ADD COLUMN deleted_at TIMESTAMPTZ;
GRANT UPDATE (deleted_at) ON tasks TO orbit_app;

-- +goose Down

REVOKE UPDATE (deleted_at) ON tasks FROM orbit_app;
ALTER TABLE tasks DROP COLUMN deleted_at;
