-- Extra public detail copied once so the skill page can match the
-- SkillHub layout without calling SkillHub again. NULL means not copied yet.

-- +goose Up

ALTER TABLE skill_catalog ADD COLUMN detail_copy JSONB;

-- +goose Down

-- This migration was about the old skill catalog table. 00020 replaced it (and its Down drops the new one), so there is
-- nothing left to undo here.
