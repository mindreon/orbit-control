-- The files of an expert version's bundle (ADR-0013, orbit.agent/v1): AGENTS.md, SOUL.md, skills.json, mcp.json and the
-- rest, as UTF-8 text. Written in the same transaction as the agent_profiles row they belong to and immutable like it.
-- orbit_app reads and inserts; orbit_worker has no grant, because a worker gets bundle skills from control's internal
-- listener. Reasons and checks: docs/persistence-failure-modes.md (ISO-31).

-- +goose Up

CREATE TABLE agent_profile_files (
  tenant_id  TEXT NOT NULL,
  profile_id TEXT NOT NULL,
  version    INTEGER NOT NULL,
  path       TEXT NOT NULL CONSTRAINT agent_profile_files_path_not_empty CHECK (path <> ''),
  content    TEXT NOT NULL,
  size       INTEGER NOT NULL CHECK (size >= 0),
  sha256     TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, profile_id, version, path),
  FOREIGN KEY (tenant_id, profile_id, version) REFERENCES agent_profiles (tenant_id, profile_id, version)
);

ALTER TABLE agent_profile_files ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_profile_files FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON agent_profile_files FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON agent_profile_files TO orbit_app;

-- +goose Down

DROP TABLE agent_profile_files;
