-- Shared ModelScope plaza snapshot. These rows are marketplace display
-- metadata, not tenant data, so they have no tenant_id and no row security.
-- Every tenant reads the same copy. Process start replaces mcp_market_servers
-- from the snapshot shipped with this binary. GET /v1/mcp-market does not
-- call modelscope.cn. Launch commands, hosted URLs, and secrets are not stored.

-- +goose Up

CREATE TABLE mcp_market_categories (
  key        TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  sort_order INT NOT NULL DEFAULT 0
);

CREATE TABLE mcp_market_servers (
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  summary        TEXT NOT NULL DEFAULT '',
  author         TEXT NOT NULL DEFAULT '',
  category       TEXT NOT NULL DEFAULT '',
  category_more  INT NOT NULL DEFAULT 0,
  calls          BIGINT NOT NULL DEFAULT 0,
  views          BIGINT NOT NULL DEFAULT 0,
  stars          BIGINT NOT NULL DEFAULT 0,
  verified       BOOLEAN NOT NULL DEFAULT false,
  hosted         BOOLEAN NOT NULL DEFAULT false,
  needs_online   BOOLEAN NOT NULL DEFAULT true,
  rank           INT NOT NULL
);

CREATE INDEX mcp_market_servers_rank_idx ON mcp_market_servers (rank, id);
CREATE INDEX mcp_market_servers_category_idx ON mcp_market_servers (category);

GRANT SELECT, INSERT, UPDATE, DELETE ON mcp_market_servers TO orbit_app;
GRANT SELECT, INSERT, UPDATE ON mcp_market_categories TO orbit_app;
REVOKE TRUNCATE, REFERENCES, TRIGGER ON mcp_market_servers, mcp_market_categories FROM orbit_app, PUBLIC;

-- +goose Down

REVOKE ALL ON mcp_market_servers, mcp_market_categories FROM orbit_app;
DROP TABLE mcp_market_servers;
DROP TABLE mcp_market_categories;
