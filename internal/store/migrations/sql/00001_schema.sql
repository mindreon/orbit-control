-- Contract §18.3. The tenant catalog: assistants and connectors. Tasks live in 00013.

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_app') THEN
    RAISE EXCEPTION 'role orbit_app is missing; run deploy/postgres/bootstrap-roles.sql first';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_app' AND (rolsuper OR rolbypassrls)) THEN
    RAISE EXCEPTION 'role orbit_app must not be SUPERUSER or BYPASSRLS';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orbit_ops' AND NOT rolsuper AND NOT rolbypassrls) THEN
    RAISE EXCEPTION 'role orbit_ops must exist as NOSUPERUSER NOBYPASSRLS';
  END IF;
END
$$;
-- +goose StatementEnd

CREATE TABLE tenants (
  id         TEXT PRIMARY KEY CONSTRAINT tenants_id_not_empty CHECK (id <> ''),
  name       TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE personas (
  id                TEXT PRIMARY KEY,
  tenant_id         TEXT NOT NULL REFERENCES tenants(id),
  name              TEXT NOT NULL,
  instructions      TEXT NOT NULL DEFAULT '',
  mcp_connector_ids TEXT[] NOT NULL DEFAULT '{}',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX personas_tenant_id_idx ON personas (tenant_id);

CREATE TABLE mcp_connectors (
  id         TEXT PRIMARY KEY,
  tenant_id  TEXT NOT NULL REFERENCES tenants(id),
  name       TEXT NOT NULL,
  command    TEXT NOT NULL,
  args       TEXT[] NOT NULL DEFAULT '{}',
  env_refs   TEXT[] NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX mcp_connectors_tenant_id_idx ON mcp_connectors (tenant_id);

-- +goose Down
DROP TABLE mcp_connectors;
DROP TABLE personas;
DROP TABLE tenants;
