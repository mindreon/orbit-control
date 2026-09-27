-- Detail text for one plaza server. Same shared snapshot as mcp_market_servers:
-- no tenant_id and no row security. The readme and tool list are display
-- text. Hosted connection URLs, launch secrets, and discussion threads are
-- not stored.

-- +goose Up

CREATE TABLE mcp_market_details (
  id          TEXT PRIMARY KEY,
  license     TEXT NOT NULL DEFAULT '',
  updated_on  TEXT NOT NULL DEFAULT '',
  readme      TEXT NOT NULL DEFAULT '',
  tools       JSONB NOT NULL DEFAULT '[]'::jsonb
);

GRANT SELECT, INSERT, UPDATE, DELETE ON mcp_market_details TO orbit_app;
REVOKE TRUNCATE, REFERENCES, TRIGGER ON mcp_market_details FROM orbit_app, PUBLIC;

-- +goose Down

REVOKE ALL ON mcp_market_details FROM orbit_app;
DROP TABLE mcp_market_details;
