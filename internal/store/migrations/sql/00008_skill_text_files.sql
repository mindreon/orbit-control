-- Text copied out of a public skill package so the detail page can show
-- it without calling SkillHub again. NULL means not copied yet. An empty
-- array means the package had no text files we keep. The bytes are not run.

-- +goose Up

ALTER TABLE skill_catalog ADD COLUMN text_files JSONB;

-- +goose Down

ALTER TABLE skill_catalog DROP COLUMN text_files;
