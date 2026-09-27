-- MCP connectors can be a local command or a remote streamable HTTP URL.
-- header_refs stores "Header-Name:ENV_NAME". Values are never stored.
-- default_open selects the connector for a new room without a persona link.

-- +goose Up

ALTER TABLE mcp_connectors
  ADD COLUMN transport TEXT NOT NULL DEFAULT 'stdio',
  ADD COLUMN url TEXT NOT NULL DEFAULT '',
  ADD COLUMN header_refs TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN default_open BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE mcp_connectors
  ADD CONSTRAINT mcp_connectors_transport_check CHECK (
    transport IN ('stdio', 'streamable_http')
  );

-- +goose Down

ALTER TABLE mcp_connectors DROP CONSTRAINT mcp_connectors_transport_check;
ALTER TABLE mcp_connectors DROP COLUMN default_open;
ALTER TABLE mcp_connectors DROP COLUMN header_refs;
ALTER TABLE mcp_connectors DROP COLUMN url;
ALTER TABLE mcp_connectors DROP COLUMN transport;
