-- SOP definition v2 (06 §2, 05 §5): a definition has a name and a description beside its steps, as AgentScope's SOP does.
-- The steps' new fields (id, depends_on, executor, verifier, ...) live in the existing `steps` JSONB; a v1 row (no id, no
-- depends_on) stays valid and reads as a linear v2 definition. Grants are table-level (SELECT, INSERT), so they cover the
-- new columns; a version is still immutable.

-- +goose Up

ALTER TABLE sop_definitions
  ADD COLUMN name        TEXT NOT NULL DEFAULT '',
  ADD COLUMN description TEXT NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE sop_definitions
  DROP COLUMN description,
  DROP COLUMN name;
