-- Extra public detail copied once so the skill page can match the
-- SkillHub layout without calling SkillHub again. NULL means not copied yet.

-- +goose Up

ALTER TABLE skill_catalog ADD COLUMN detail_copy JSONB;

-- +goose Down

ALTER TABLE skill_catalog DROP COLUMN detail_copy;
