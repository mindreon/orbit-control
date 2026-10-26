-- +goose Up
SET LOCAL ROLE orbit_owner;
ALTER TABLE workspaces ADD COLUMN external_ref text;
CREATE UNIQUE INDEX workspaces_external_ref ON workspaces(tenant_id, external_ref) WHERE external_ref IS NOT NULL;

-- +goose Down
DROP INDEX workspaces_external_ref;
ALTER TABLE workspaces DROP COLUMN external_ref;
