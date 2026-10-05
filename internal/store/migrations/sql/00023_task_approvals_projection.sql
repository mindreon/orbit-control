-- The Projector now maintains task_approvals from approval.requested and
-- approval.decided (09 §3). A decision updates the row, so orbit_app gets a
-- column-level UPDATE on exactly what a decision changes, and the decision's
-- comment and the "always" flag get columns. subject, ids and requested_at
-- are never updated. Reasons and checks: docs/persistence-failure-modes.md
-- ("Task runtime tables", ISO-27).

-- +goose Up

ALTER TABLE task_approvals
  ADD COLUMN comment TEXT NOT NULL DEFAULT '',
  ADD COLUMN always  BOOLEAN NOT NULL DEFAULT false;

GRANT UPDATE (status, comment, always, decided_at, entity_version) ON task_approvals TO orbit_app;

-- +goose Down

REVOKE UPDATE (status, comment, always, decided_at, entity_version) ON task_approvals FROM orbit_app;
ALTER TABLE task_approvals
  DROP COLUMN always,
  DROP COLUMN comment;
