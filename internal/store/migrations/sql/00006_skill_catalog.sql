-- Shared SkillHub catalog. These rows are marketplace display metadata, not
-- tenant data, so they have no tenant_id and no row security. Every tenant
-- reads the same copy. The background sync refreshes it; GET /v1/skills does
-- not call SkillHub. Packages are never stored here.

-- +goose Up

CREATE TABLE skill_categories (
  key        TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  name_en    TEXT NOT NULL DEFAULT '',
  sort_order INT NOT NULL DEFAULT 0
);

CREATE TABLE skill_catalog (
  id               TEXT PRIMARY KEY,
  slug             TEXT NOT NULL,
  handle           TEXT NOT NULL DEFAULT '',
  name             TEXT NOT NULL,
  description      TEXT NOT NULL DEFAULT '',
  category         TEXT NOT NULL DEFAULT '',
  icon_url         TEXT NOT NULL DEFAULT '',
  downloads        BIGINT NOT NULL DEFAULT 0,
  stars            BIGINT NOT NULL DEFAULT 0,
  source           TEXT NOT NULL DEFAULT '',
  version          TEXT NOT NULL DEFAULT '',
  requires_api_key BOOLEAN NOT NULL DEFAULT false,
  paid             BOOLEAN NOT NULL DEFAULT false,
  score            DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL,
  synced_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  trending_rank    INT NOT NULL DEFAULT 0
);

CREATE INDEX skill_catalog_score_idx ON skill_catalog (score DESC, id);
CREATE INDEX skill_catalog_downloads_idx ON skill_catalog (downloads DESC, id);
CREATE INDEX skill_catalog_updated_idx ON skill_catalog (updated_at DESC, id);
CREATE INDEX skill_catalog_category_idx ON skill_catalog (category);
CREATE INDEX skill_catalog_trending_idx ON skill_catalog (trending_rank, id) WHERE trending_rank > 0;

GRANT SELECT, INSERT, UPDATE ON skill_categories, skill_catalog TO orbit_app;
REVOKE TRUNCATE, REFERENCES, TRIGGER ON skill_categories, skill_catalog FROM orbit_app, PUBLIC;

-- +goose Down

-- This migration was about the old skill catalog table. 00020 replaced it (and its Down drops the new one), so there is
-- nothing left to undo here.
