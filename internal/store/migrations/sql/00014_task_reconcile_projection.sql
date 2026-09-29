-- T4.5 keeps the durable task projection queryable and repairable from the
-- authoritative TaskWorkflow view.

-- +goose Up

ALTER TABLE tasks
  ADD COLUMN budgets JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN usage JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN pending_approvals JSONB NOT NULL DEFAULT '[]'::jsonb;

GRANT UPDATE (status, plan_version, budgets, usage, pending_approvals, updated_at)
  ON tasks TO orbit_app;

-- +goose Down

REVOKE UPDATE (budgets, usage, pending_approvals) ON tasks FROM orbit_app;
ALTER TABLE tasks
  DROP COLUMN pending_approvals,
  DROP COLUMN usage,
  DROP COLUMN budgets;
