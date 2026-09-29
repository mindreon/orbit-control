-- API command idempotency (10 §1) keeps its rows in idempotency_ledger, scope api_command. Control completes,
-- releases and takes over those rows, so orbit_app gets the same column-level UPDATE that orbit_worker already has.
-- Reasons and checks: docs/persistence-failure-modes.md ("Task runtime tables", ISO-24, FM-85, FM-86).

-- +goose Up

GRANT UPDATE (status, result_ref, owner, last_seen) ON idempotency_ledger TO orbit_app;

-- +goose Down

REVOKE UPDATE (status, result_ref, owner, last_seen) ON idempotency_ledger FROM orbit_app;
