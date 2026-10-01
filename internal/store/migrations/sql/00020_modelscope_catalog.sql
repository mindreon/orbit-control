-- ModelScope marketplace: skills, agents, and the source flag on the plaza.
-- The SkillHub catalog is dropped: ModelScope is the only upstream now. The
-- rows are shared marketplace metadata, not tenant data, so they have no
-- tenant_id and no row security. Every tenant reads the same copy. Process
-- start replaces them from the snapshot shipped with (or mounted beside) this
-- binary. GET /v1/skills and GET /v1/agents do not call modelscope.cn.
-- Launch commands, hosted URLs, and secrets are not stored.

-- +goose Up

DROP TABLE IF EXISTS skill_catalog;
DROP TABLE IF EXISTS skill_categories;

CREATE TABLE skill_categories (
  key        TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  name_en    TEXT NOT NULL DEFAULT '',
  sort_order INT NOT NULL DEFAULT 0
);

CREATE TABLE skill_catalog (
  id             TEXT PRIMARY KEY,
  handle         TEXT NOT NULL DEFAULT '',
  slug           TEXT NOT NULL,
  name           TEXT NOT NULL,
  description    TEXT NOT NULL DEFAULT '',
  description_en TEXT NOT NULL DEFAULT '',
  category       TEXT NOT NULL DEFAULT '',
  tags           JSONB NOT NULL DEFAULT '[]',
  license        TEXT NOT NULL DEFAULT '',
  icon_url       TEXT NOT NULL DEFAULT '',
  source_url     TEXT NOT NULL DEFAULT '',
  downloads      BIGINT NOT NULL DEFAULT 0,
  visits         BIGINT NOT NULL DEFAULT 0,
  likes          BIGINT NOT NULL DEFAULT 0,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  source         TEXT NOT NULL DEFAULT 'common',
  text_files     JSONB,
  installed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX skill_catalog_downloads_idx ON skill_catalog (downloads DESC, id);
CREATE INDEX skill_catalog_likes_idx ON skill_catalog (likes DESC, id);
CREATE INDEX skill_catalog_updated_idx ON skill_catalog (updated_at DESC, id);
CREATE INDEX skill_catalog_category_idx ON skill_catalog (category);
CREATE INDEX skill_catalog_source_idx ON skill_catalog (source);

CREATE TABLE agent_catalog (
  id             TEXT PRIMARY KEY,
  handle         TEXT NOT NULL DEFAULT '',
  slug           TEXT NOT NULL,
  name           TEXT NOT NULL,
  description    TEXT NOT NULL DEFAULT '',
  framework      TEXT NOT NULL DEFAULT '',
  license        TEXT NOT NULL DEFAULT '',
  logo_url       TEXT NOT NULL DEFAULT '',
  catalogues     JSONB NOT NULL DEFAULT '[]',
  models         JSONB NOT NULL DEFAULT '[]',
  mcps           JSONB NOT NULL DEFAULT '[]',
  skills         JSONB NOT NULL DEFAULT '[]',
  system_prompts JSONB NOT NULL DEFAULT '[]',
  readme         TEXT NOT NULL DEFAULT '',
  files          JSONB,
  stars          BIGINT NOT NULL DEFAULT 0,
  downloads      BIGINT NOT NULL DEFAULT 0,
  visits         BIGINT NOT NULL DEFAULT 0,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  source         TEXT NOT NULL DEFAULT 'common',
  installed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX agent_catalog_downloads_idx ON agent_catalog (downloads DESC, id);
CREATE INDEX agent_catalog_stars_idx ON agent_catalog (stars DESC, id);
CREATE INDEX agent_catalog_updated_idx ON agent_catalog (updated_at DESC, id);

-- Which snapshot version the binary last stored, so a restart with an
-- unchanged snapshot skips the copy. One row per snapshot name.
CREATE TABLE catalog_snapshots (
  name         TEXT PRIMARY KEY,
  sha256       TEXT NOT NULL,
  installed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE mcp_market_servers ADD COLUMN source TEXT NOT NULL DEFAULT 'common';
CREATE INDEX mcp_market_servers_source_idx ON mcp_market_servers (source);

GRANT SELECT, INSERT, UPDATE, DELETE ON skill_categories, skill_catalog, agent_catalog, catalog_snapshots TO orbit_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON mcp_market_servers TO orbit_app;
REVOKE TRUNCATE, REFERENCES, TRIGGER ON skill_categories, skill_catalog, agent_catalog, catalog_snapshots, mcp_market_servers FROM orbit_app, PUBLIC;

-- +goose Down

REVOKE ALL ON skill_categories, skill_catalog, agent_catalog, catalog_snapshots, mcp_market_servers FROM orbit_app;
DROP TABLE catalog_snapshots;
DROP TABLE agent_catalog;
DROP TABLE skill_catalog;
DROP TABLE skill_categories;
ALTER TABLE mcp_market_servers DROP COLUMN source;
