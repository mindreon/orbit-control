-- Contract §18.5 (C32 rev2) RLS backstop. T means
--   tenant_id = current_setting('app.tenant_id', true)
-- missing_ok=true makes an unset GUC evaluate to NULL/'' so nothing matches.
-- The GUC is only ever set with SELECT set_config('app.tenant_id', $1, true).

-- +goose Up

ALTER TABLE users            ENABLE ROW LEVEL SECURITY;
ALTER TABLE users            FORCE ROW LEVEL SECURITY;
ALTER TABLE rooms            ENABLE ROW LEVEL SECURITY;
ALTER TABLE rooms            FORCE ROW LEVEL SECURITY;
ALTER TABLE turns            ENABLE ROW LEVEL SECURITY;
ALTER TABLE turns            FORCE ROW LEVEL SECURITY;
ALTER TABLE events           ENABLE ROW LEVEL SECURITY;
ALTER TABLE events           FORCE ROW LEVEL SECURITY;
ALTER TABLE messages         ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages         FORCE ROW LEVEL SECURITY;
ALTER TABLE approvals        ENABLE ROW LEVEL SECURITY;
ALTER TABLE approvals        FORCE ROW LEVEL SECURITY;
ALTER TABLE approval_rules   ENABLE ROW LEVEL SECURITY;
ALTER TABLE approval_rules   FORCE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys FORCE ROW LEVEL SECURITY;
ALTER TABLE artifacts        ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts        FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE personas         ENABLE ROW LEVEL SECURITY;
ALTER TABLE personas         FORCE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors   ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors   FORCE ROW LEVEL SECURITY;
ALTER TABLE cloud_agent_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE cloud_agent_jobs FORCE ROW LEVEL SECURITY;

-- Generic tenant policy (FOR ALL) on [T] tables without a task parent.
CREATE POLICY tenant_isolation ON users FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON personas FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON mcp_connectors FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
-- cloud_agent_jobs has no task_id yet: tenant-only (§18.5).
CREATE POLICY tenant_isolation ON cloud_agent_jobs FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- rooms: split per command (Sentinel H1). No DELETE policy, so with FORCE
-- RLS a physical DELETE by the app role affects 0 rows. Soft delete goes
-- only through orbit_soft_delete_room (00003).
CREATE POLICY rooms_select ON rooms FOR SELECT
  USING (tenant_id = current_setting('app.tenant_id', true) AND deleted_at IS NULL);
CREATE POLICY rooms_update ON rooms FOR UPDATE
  USING (tenant_id = current_setting('app.tenant_id', true) AND deleted_at IS NULL)
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY rooms_insert ON rooms FOR INSERT
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Task children: tenant AND live parent, on both USING and WITH CHECK, so
-- rows of a deleted task can be neither read nor written. The subquery is
-- itself filtered by rooms_select.
CREATE POLICY tenant_live_task ON turns FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = turns.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = turns.task_id AND r.deleted_at IS NULL));
CREATE POLICY tenant_live_task ON events FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = events.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = events.task_id AND r.deleted_at IS NULL));
CREATE POLICY tenant_live_task ON messages FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = messages.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = messages.task_id AND r.deleted_at IS NULL));
CREATE POLICY tenant_live_task ON approvals FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = approvals.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = approvals.task_id AND r.deleted_at IS NULL));
CREATE POLICY tenant_live_task ON idempotency_keys FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = idempotency_keys.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = idempotency_keys.task_id AND r.deleted_at IS NULL));
CREATE POLICY tenant_live_task ON artifacts FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = artifacts.task_id AND r.deleted_at IS NULL))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = artifacts.task_id AND r.deleted_at IS NULL));

-- artifact_versions has no task_id: chain through artifacts, whose own
-- policy already carries the live-room condition.
CREATE POLICY tenant_live_artifact ON artifact_versions FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM artifacts a WHERE a.id = artifact_versions.artifact_id))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND EXISTS (SELECT 1 FROM artifacts a WHERE a.id = artifact_versions.artifact_id));

-- Persona-scoped rules (room_id IS NULL) are unaffected by task deletion;
-- room-scoped rules disappear with their room.
CREATE POLICY tenant_live_room_rule ON approval_rules FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true)
         AND (room_id IS NULL
              OR EXISTS (SELECT 1 FROM rooms r WHERE r.id = approval_rules.room_id AND r.deleted_at IS NULL)))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true)
         AND (room_id IS NULL
              OR EXISTS (SELECT 1 FROM rooms r WHERE r.id = approval_rules.room_id AND r.deleted_at IS NULL)));

-- orbit_app is not the owner and has no BYPASSRLS; it only gets DML.
-- DELETE on rooms is granted on purpose: without a DELETE policy it matches
-- 0 rows (S-DB-13 i) instead of failing with a privilege error.
GRANT SELECT, INSERT, UPDATE ON tenants TO orbit_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  users, sessions, oidc_login_state, rooms, turns, events, messages, approvals,
  approval_rules, idempotency_keys, artifacts, artifact_versions, personas,
  mcp_connectors, cloud_agent_jobs
TO orbit_app;
GRANT USAGE, SELECT ON SEQUENCE events_id_seq TO orbit_app;

-- +goose Down
REVOKE ALL ON SEQUENCE events_id_seq FROM orbit_app;
REVOKE ALL ON
  tenants, users, sessions, oidc_login_state, rooms, turns, events, messages,
  approvals, approval_rules, idempotency_keys, artifacts, artifact_versions,
  personas, mcp_connectors, cloud_agent_jobs
FROM orbit_app;

DROP POLICY tenant_live_room_rule ON approval_rules;
DROP POLICY tenant_live_artifact ON artifact_versions;
DROP POLICY tenant_live_task ON artifacts;
DROP POLICY tenant_live_task ON idempotency_keys;
DROP POLICY tenant_live_task ON approvals;
DROP POLICY tenant_live_task ON messages;
DROP POLICY tenant_live_task ON events;
DROP POLICY tenant_live_task ON turns;
DROP POLICY rooms_insert ON rooms;
DROP POLICY rooms_update ON rooms;
DROP POLICY rooms_select ON rooms;
DROP POLICY tenant_isolation ON cloud_agent_jobs;
DROP POLICY tenant_isolation ON mcp_connectors;
DROP POLICY tenant_isolation ON personas;
DROP POLICY tenant_isolation ON users;

ALTER TABLE cloud_agent_jobs  NO FORCE ROW LEVEL SECURITY;
ALTER TABLE cloud_agent_jobs  DISABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors    NO FORCE ROW LEVEL SECURITY;
ALTER TABLE mcp_connectors    DISABLE ROW LEVEL SECURITY;
ALTER TABLE personas          NO FORCE ROW LEVEL SECURITY;
ALTER TABLE personas          DISABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_versions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_versions DISABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts         NO FORCE ROW LEVEL SECURITY;
ALTER TABLE artifacts         DISABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys  NO FORCE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys  DISABLE ROW LEVEL SECURITY;
ALTER TABLE approval_rules    NO FORCE ROW LEVEL SECURITY;
ALTER TABLE approval_rules    DISABLE ROW LEVEL SECURITY;
ALTER TABLE approvals         NO FORCE ROW LEVEL SECURITY;
ALTER TABLE approvals         DISABLE ROW LEVEL SECURITY;
ALTER TABLE messages          NO FORCE ROW LEVEL SECURITY;
ALTER TABLE messages          DISABLE ROW LEVEL SECURITY;
ALTER TABLE events            NO FORCE ROW LEVEL SECURITY;
ALTER TABLE events            DISABLE ROW LEVEL SECURITY;
ALTER TABLE turns             NO FORCE ROW LEVEL SECURITY;
ALTER TABLE turns             DISABLE ROW LEVEL SECURITY;
ALTER TABLE rooms             NO FORCE ROW LEVEL SECURITY;
ALTER TABLE rooms             DISABLE ROW LEVEL SECURITY;
ALTER TABLE users             NO FORCE ROW LEVEL SECURITY;
ALTER TABLE users             DISABLE ROW LEVEL SECURITY;
