-- Catalog icons. The snapshot sidecar (icons.json.gz) carries the icon bytes
-- the crawled rows reference; process start upserts them keyed by the source
-- URL. mcp_market_servers gains the icon URL its card points at; skill_catalog
-- and agent_catalog already store one.

-- +goose Up

CREATE TABLE catalog_icons (
  url          TEXT PRIMARY KEY,
  content_type TEXT NOT NULL DEFAULT 'image/png',
  data         BYTEA NOT NULL
);

ALTER TABLE mcp_market_servers ADD COLUMN icon_url TEXT NOT NULL DEFAULT '';

GRANT SELECT, INSERT, UPDATE, DELETE ON catalog_icons TO orbit_app;
REVOKE TRUNCATE, REFERENCES, TRIGGER ON catalog_icons FROM orbit_app, PUBLIC;

-- +goose Down

REVOKE ALL ON catalog_icons FROM orbit_app;
DROP TABLE catalog_icons;
ALTER TABLE mcp_market_servers DROP COLUMN icon_url;
