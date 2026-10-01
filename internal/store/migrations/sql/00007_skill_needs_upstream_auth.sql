-- The display flag was named requires_api_key. That name matches the schema
-- rule that rejects columns which could hold a token or an API key. The
-- column is only a boolean ("this skill asks for an upstream account"), so
-- rename it. The public JSON field stays requiresApiKey.

-- +goose Up

ALTER TABLE skill_catalog RENAME COLUMN requires_api_key TO needs_upstream_auth;

-- +goose Down

-- This migration was about the old skill catalog table. 00020 replaced it (and its Down drops the new one), so there is
-- nothing left to undo here.
