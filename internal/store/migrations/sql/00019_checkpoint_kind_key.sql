-- The checkpoints unique key includes the kind (docs/persistence-failure-modes.md, ISO-26, FM-90 and FM-91).
--
-- 00013 made checkpoints_attempt_seq_key unique on (attempt_id, seq). The runtime writes a checkpoint with
-- INSERT ... ON CONFLICT DO NOTHING, so a second kind at the same seq was silently dropped. The key is now
-- (attempt_id, kind, seq): another kind keeps its own row, the same kind at the same seq stays idempotent. Every existing
-- row is unique on the narrower key, so the new one cannot conflict. No grant changes.

-- +goose Up

ALTER TABLE checkpoints DROP CONSTRAINT checkpoints_attempt_seq_key;
ALTER TABLE checkpoints ADD CONSTRAINT checkpoints_attempt_kind_seq_key UNIQUE (attempt_id, kind, seq);

-- +goose Down

-- Fails, and leaves the table as it is, if an attempt holds two kinds at one seq: those rows are data, and down does not
-- choose which one to delete.
ALTER TABLE checkpoints DROP CONSTRAINT checkpoints_attempt_kind_seq_key;
ALTER TABLE checkpoints ADD CONSTRAINT checkpoints_attempt_seq_key UNIQUE (attempt_id, seq);
