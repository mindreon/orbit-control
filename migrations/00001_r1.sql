-- +goose Up
SET LOCAL ROLE orbit_owner;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO orbit_app, orbit_worker;
CREATE TABLE principals (
  id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('user','service')), email text NOT NULL, email_verified boolean NOT NULL DEFAULT false, display_name text NOT NULL, idp_subject text NOT NULL UNIQUE, status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled')), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE tenants (
  id uuid PRIMARY KEY, slug text NOT NULL UNIQUE, name text NOT NULL, plan text NOT NULL DEFAULT 'free', region text NOT NULL DEFAULT 'local', isolation_tier text NOT NULL DEFAULT 'shared' CHECK(isolation_tier IN ('shared','dedicated')), kms_key_ref text, status text NOT NULL DEFAULT 'active' CHECK(status IN ('active','suspended','archived')), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE memberships (
  tenant_id uuid NOT NULL REFERENCES tenants(id), principal_id uuid NOT NULL REFERENCES principals(id), status text NOT NULL CHECK(status IN ('invited','active','suspended')), joined_at timestamptz NOT NULL DEFAULT now(), source text NOT NULL CHECK(source IN ('scim','invite','jit','direct')), PRIMARY KEY(tenant_id,principal_id)
);
CREATE TABLE invitations (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), email text NOT NULL,
 role text NOT NULL CHECK(role IN ('tenant.owner','tenant.admin','tenant.auditor','tenant.reviewer','tenant.member')),
 token_hash bytea NOT NULL UNIQUE, expires_at timestamptz NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','accepted','revoked')),
 created_by uuid NOT NULL REFERENCES principals(id), created_at timestamptz NOT NULL DEFAULT now(),
 accepted_by uuid REFERENCES principals(id), accepted_at timestamptz
);
CREATE INDEX ON invitations(tenant_id,id);
ALTER TABLE invitations ENABLE ROW LEVEL SECURITY;
ALTER TABLE invitations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON invitations USING(tenant_id = NULLIF(current_setting('app.tenant_id',true),'')::uuid) WITH CHECK(tenant_id = NULLIF(current_setting('app.tenant_id',true),'')::uuid);
GRANT SELECT,INSERT,UPDATE ON invitations TO orbit_app;
-- Token hash discovers the tenant for acceptance or encrypted acceptance replay.
-- +goose StatementBegin
CREATE FUNCTION invitation_tenant(token bytea) RETURNS SETOF uuid
LANGUAGE sql SECURITY DEFINER STABLE SET search_path=public,pg_temp AS $$
 SELECT tenant_id FROM invitations WHERE token_hash=token AND (status='accepted' OR (status='pending' AND expires_at>now()));
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION invitation_tenant(bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION invitation_tenant(bytea) TO orbit_app;
CREATE TABLE groups (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL, external_id text, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,name)
);
CREATE TABLE group_members (
  tenant_id uuid NOT NULL, group_id uuid NOT NULL, principal_id uuid NOT NULL, PRIMARY KEY(tenant_id,group_id,principal_id), FOREIGN KEY(tenant_id,group_id) REFERENCES groups(tenant_id,id), FOREIGN KEY(tenant_id,principal_id) REFERENCES memberships(tenant_id,principal_id)
);
CREATE TABLE workspaces (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL, default_agent_ref text, settings jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id)
);
CREATE TABLE workspace_members (
  tenant_id uuid NOT NULL, workspace_id uuid NOT NULL, principal_id uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,workspace_id,principal_id), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id), FOREIGN KEY(tenant_id,principal_id) REFERENCES memberships(tenant_id,principal_id)
);
CREATE TABLE projects (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, workspace_id uuid NOT NULL, name text NOT NULL, kind text NOT NULL CHECK(kind IN ('general','code')), settings jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id)
);
CREATE TABLE role_bindings (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), principal_type text NOT NULL CHECK(principal_type IN ('user','service','group')), principal_id uuid NOT NULL, role text NOT NULL CHECK(role IN ('tenant.owner','tenant.admin','tenant.auditor','tenant.reviewer','tenant.member','ws.admin','ws.editor','ws.member','ws.viewer','asset.owner','asset.maintainer','asset.user','session.owner','session.collaborator','session.viewer')), scope_type text NOT NULL CHECK(scope_type IN ('tenant','workspace','asset','session')), scope_id uuid NOT NULL, created_by uuid NOT NULL REFERENCES principals(id), created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,principal_type,principal_id,role,scope_type,scope_id), CHECK(split_part(role,'.',1)=CASE scope_type WHEN 'workspace' THEN 'ws' ELSE scope_type END)
);
CREATE TABLE policies (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), scope_type text NOT NULL CHECK(scope_type IN ('tenant','workspace','asset','session')), scope_id uuid NOT NULL, kind text NOT NULL CHECK(kind IN ('cedar','tool_rules')), body jsonb NOT NULL, version bigint NOT NULL, status text NOT NULL CHECK(status IN ('active','draft','archived')), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE api_keys (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, principal_id uuid NOT NULL, name text NOT NULL, prefix text NOT NULL UNIQUE, hash bytea NOT NULL UNIQUE, scopes text[] NOT NULL, expires_at timestamptz NOT NULL, last_used_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,principal_id) REFERENCES memberships(tenant_id,principal_id)
);
CREATE TABLE secrets (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL, scope text NOT NULL, ciphertext bytea NOT NULL, key_version int NOT NULL, created_by uuid NOT NULL REFERENCES principals(id), created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,name,scope)
);
CREATE TABLE model_profiles (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL, provider text NOT NULL, base_url text NOT NULL, model text NOT NULL, secret_id uuid, params jsonb NOT NULL DEFAULT '{}', pricing jsonb NOT NULL DEFAULT '{}', context_window int, is_default boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,secret_id) REFERENCES secrets(tenant_id,id), UNIQUE(tenant_id,name)
);
CREATE TABLE sessions (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, workspace_id uuid NOT NULL, project_id uuid, owner_id uuid NOT NULL, title text NOT NULL, entry_kind text NOT NULL CHECK(entry_kind IN ('agent','team')), entry_ref text NOT NULL, message_policy text NOT NULL CHECK(message_policy IN ('queue','steer','interrupt')), pinned boolean NOT NULL DEFAULT false, pinned_lock_id uuid, sandbox_id text, status text NOT NULL CHECK(status IN ('active','idle','archived')), created_at timestamptz NOT NULL DEFAULT now(), last_activity_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id), FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id), FOREIGN KEY(tenant_id,owner_id) REFERENCES memberships(tenant_id,principal_id)
);
CREATE TABLE runs (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, session_id uuid NOT NULL, trigger text NOT NULL CHECK(trigger IN ('user_message','schedule','api','channel','resume')), status text NOT NULL CHECK(status IN ('queued','running','waiting_input','waiting_approval','paused','succeeded','failed','cancelled','budget_exhausted','timed_out')), lock_id uuid, created_by uuid REFERENCES principals(id), message_id uuid, budget_millicredits bigint NOT NULL DEFAULT 0, used_millicredits bigint NOT NULL DEFAULT 0, temporal_workflow_id text, started_at timestamptz, ended_at timestamptz, error_code text, summary text, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), FOREIGN KEY(tenant_id,session_id) REFERENCES sessions(tenant_id,id)
);
CREATE TABLE run_locks (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, run_id uuid NOT NULL, digest text NOT NULL, entries jsonb NOT NULL, effective_permissions jsonb NOT NULL, lock jsonb, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE messages (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, session_id uuid NOT NULL, run_id uuid, role text NOT NULL CHECK(role IN ('user','assistant','system','tool_summary')), author_id uuid REFERENCES principals(id), content jsonb, content_ref text, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,session_id) REFERENCES sessions(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE run_segments (
  tenant_id uuid NOT NULL, run_id uuid NOT NULL, member text NOT NULL, seq int NOT NULL CHECK(seq>=1), status text NOT NULL CHECK(status IN ('running','completed','interrupted','yielded','failed')), checkpoint_version bigint, iterations int NOT NULL DEFAULT 0, usage jsonb NOT NULL DEFAULT '[]', model_profiles jsonb NOT NULL DEFAULT '{}', result_ref text, started_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz, PRIMARY KEY(tenant_id,run_id,member,seq), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
-- Event IDs are <run>:<member>:<seq>:<attempt>:<n>; ts is the immutable RunEvent timestamp.
-- Historical reads and SSE cursors order by the JetStream stream sequence.
CREATE TABLE run_events (
  tenant_id uuid NOT NULL, run_id uuid NOT NULL, session_id uuid NOT NULL,
  member text NOT NULL, segment int NOT NULL, attempt int NOT NULL,
  event_id text NOT NULL, stream_seq bigint NOT NULL, type text NOT NULL, payload jsonb NOT NULL,
  ts timestamptz NOT NULL, PRIMARY KEY(tenant_id,run_id,event_id,ts),
  UNIQUE(tenant_id,stream_seq,ts), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
) PARTITION BY RANGE (ts);
CREATE TABLE run_events_default PARTITION OF run_events DEFAULT;
CREATE TABLE interactions (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, run_id uuid NOT NULL, member text NOT NULL, kind text NOT NULL CHECK(kind IN ('tool_approval','ask_user','effect_uncertain','question','plan','human_task')), request jsonb NOT NULL, approvers jsonb NOT NULL, status text NOT NULL CHECK(status IN ('pending','resolved','expired','cancelled','escalated')), decision jsonb, decided_by uuid REFERENCES principals(id), seq int, session_id uuid, remind_at timestamptz, expires_at timestamptz, resolved_at timestamptz, due_at timestamptz, escalated_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE artifacts (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, session_id uuid NOT NULL, run_id uuid NOT NULL, path text NOT NULL, mime text NOT NULL, size bigint NOT NULL, digest text NOT NULL, object_ref text NOT NULL, status text NOT NULL CHECK(status IN ('draft','final')), created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,session_id) REFERENCES sessions(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE team_runs (
  run_id uuid NOT NULL, tenant_id uuid NOT NULL, roster jsonb NOT NULL, board_snapshot_ref text, dispatch_mode text NOT NULL CHECK(dispatch_mode IN ('scheduled')), PRIMARY KEY(tenant_id,run_id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE agent_checkpoints (
  tenant_id      uuid        NOT NULL,
  session_key    text        NOT NULL,              -- 例：sess:<session_id>:<lock_digest 前 12 位> 或 team:<run_id>:<member>
  version        bigint      NOT NULL CHECK (version >= 1),
  blob           bytea,                             -- ≤256KiB 内联，密文
  blob_ref       text,                              -- 更大时放对象存储 t/<tenant>/checkpoints/...
  hmac           bytea       NOT NULL,              -- HMAC-SHA256(租户密钥, tenant_id|session_key|version|密文)
  size_bytes     bigint      NOT NULL,
  kernel_version text        NOT NULL,              -- openjiuwen 版本 + orbit_kernel 序列化版本
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_key, version),
  CHECK ((blob IS NULL) <> (blob_ref IS NULL))
);
CREATE TABLE segment_results (
  tenant_id          uuid        NOT NULL,
  run_id             uuid        NOT NULL,
  member             text        NOT NULL,
  seq                int         NOT NULL CHECK (seq >= 1),
  checkpoint_version bigint      NOT NULL,
  result             jsonb       NOT NULL,          -- SegmentResult
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, run_id, member, seq)
);
CREATE TABLE tool_effects (
  id                 uuid        PRIMARY KEY,
  tenant_id          uuid        NOT NULL,
  run_id             uuid        NOT NULL,
  member             text        NOT NULL,
  effect_key         text        NOT NULL,          -- temporal §12.2
  tool               text        NOT NULL,
  args_digest        text        NOT NULL,
  effect_class       text        NOT NULL CHECK (effect_class IN ('external_write','privileged')),
  checkpoint_version bigint      NOT NULL,
  status             text        NOT NULL CHECK (status IN ('started','completed','failed','unknown')),
  result_preview     jsonb,
  result_ref         text,
  external_ref       text,
  created_at         timestamptz NOT NULL DEFAULT now(),
  completed_at       timestamptz,
  UNIQUE (tenant_id, run_id, effect_key)
);
CREATE TABLE sandboxes (
  tenant_id uuid NOT NULL REFERENCES tenants(id), isolation_key text NOT NULL, provider text NOT NULL CHECK(provider IN ('opensandbox','local')), sandbox_id text, state text NOT NULL CHECK(state IN ('creating','ready','paused','snapshotted','destroyed','failed')), profile jsonb NOT NULL, session_id uuid, snapshot_id text, expires_at timestamptz, lease_gen int NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,isolation_key)
);
CREATE TABLE schedules (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, workspace_id uuid NOT NULL, owner_id uuid NOT NULL, cron text NOT NULL, timezone text NOT NULL, entry_ref text NOT NULL, input jsonb NOT NULL, overlap_policy text NOT NULL CHECK(overlap_policy IN ('skip','buffer_one','buffer_all','cancel_other','terminate_other','allow_all')), catchup_window interval NOT NULL, temporal_schedule_id text NOT NULL, status text NOT NULL CHECK(status IN ('active','paused','deleted')), created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id), FOREIGN KEY(tenant_id,owner_id) REFERENCES memberships(tenant_id,principal_id)
);
CREATE TABLE permission_grants (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), scope_type text NOT NULL CHECK(scope_type IN ('session','user','workspace')), scope_id uuid NOT NULL, interaction_id uuid NOT NULL, tool text NOT NULL, arg_pattern text, decision text NOT NULL CHECK(decision IN ('allow','deny')), granted_by uuid NOT NULL REFERENCES principals(id), expires_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE assets (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), publisher text NOT NULL, kind text NOT NULL CHECK(kind IN ('skill','agent','team','mcp_server')), name text NOT NULL, visibility text NOT NULL CHECK(visibility IN ('private','workspace','tenant')), workspace_id uuid, owner_id uuid NOT NULL REFERENCES principals(id), status text NOT NULL CHECK(status IN ('active','suspended','archived')), latest_version text, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,publisher,kind,name), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id)
);
CREATE TABLE asset_versions (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, asset_id uuid NOT NULL, version text NOT NULL, manifest jsonb NOT NULL, content_digest text NOT NULL, blob_index_ref text NOT NULL, permissions jsonb NOT NULL, compat jsonb NOT NULL, state text NOT NULL CHECK(state IN ('draft','submitted','checking','check_failed','in_review','changes_requested','approved','published','deprecated','suspended','yanked','rejected')), signature text, release_channel text NOT NULL DEFAULT 'stable', release_started boolean NOT NULL DEFAULT false, withdraw_requested boolean NOT NULL DEFAULT false, withdraw_delivered boolean NOT NULL DEFAULT false, review_required int NOT NULL DEFAULT 1 CHECK(review_required BETWEEN 1 AND 20), workspace_review boolean NOT NULL DEFAULT true, previous_version_id uuid, previous_channel_id uuid, release_generation int NOT NULL DEFAULT 0, uploaded_by uuid NOT NULL REFERENCES principals(id), release_error text, release_retryable boolean NOT NULL DEFAULT true, permissions_jcs bytea NOT NULL, identity_jcs bytea NOT NULL, submitted_by uuid REFERENCES principals(id), published_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id), UNIQUE(tenant_id,asset_id,version), FOREIGN KEY(tenant_id,asset_id) REFERENCES assets(tenant_id,id)
);
CREATE TABLE asset_files (
  tenant_id uuid NOT NULL, version_id uuid NOT NULL, path text NOT NULL, sha256 text NOT NULL, size_bytes bigint NOT NULL, is_text boolean NOT NULL, blob_ref text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,version_id,path), FOREIGN KEY(tenant_id,version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE TABLE asset_dependencies (
  tenant_id uuid NOT NULL, version_id uuid NOT NULL, dep_kind text NOT NULL CHECK(dep_kind IN ('skill','agent','team','mcp_server')), dep_ref text NOT NULL, "constraint" text NOT NULL, resolved_version_id uuid, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,version_id,dep_kind,dep_ref), FOREIGN KEY(tenant_id,version_id) REFERENCES asset_versions(tenant_id,id), FOREIGN KEY(tenant_id,resolved_version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE TABLE asset_channels (
  tenant_id uuid NOT NULL, asset_id uuid NOT NULL, channel text NOT NULL CHECK(channel ~ '^[a-z][a-z0-9_-]{0,63}$'), version_id uuid NOT NULL, updated_by uuid NOT NULL REFERENCES principals(id), updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,asset_id,channel), FOREIGN KEY(tenant_id,asset_id) REFERENCES assets(tenant_id,id), FOREIGN KEY(tenant_id,version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE TABLE asset_grants (
  tenant_id uuid NOT NULL, asset_id uuid NOT NULL, grantee_type text NOT NULL CHECK(grantee_type IN ('user','service','group','workspace','tenant')), grantee_id uuid NOT NULL, permission text NOT NULL CHECK(permission IN ('use','maintain')), created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,asset_id,grantee_type,grantee_id,permission), FOREIGN KEY(tenant_id,asset_id) REFERENCES assets(tenant_id,id)
);
CREATE TABLE asset_installs (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, scope_type text NOT NULL CHECK(scope_type IN ('workspace','tenant')), scope_id uuid NOT NULL, asset_id uuid NOT NULL, "constraint" text NOT NULL, channel text, pinned_version_id uuid, enabled boolean NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,asset_id) REFERENCES assets(tenant_id,id), FOREIGN KEY(tenant_id,pinned_version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE TABLE asset_check_reports (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, version_id uuid NOT NULL, "check" text NOT NULL, status text NOT NULL CHECK(status IN ('pass','warn','fail')), findings jsonb NOT NULL, evidence_ref text, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE TABLE asset_reviews (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, version_id uuid NOT NULL, reviewer_id uuid NOT NULL REFERENCES principals(id), decision text NOT NULL CHECK(decision IN ('approve','request_changes','reject')), comment text NOT NULL, generation int NOT NULL DEFAULT 0, delivered boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,version_id) REFERENCES asset_versions(tenant_id,id)
);
CREATE UNIQUE INDEX asset_review_once ON asset_reviews(tenant_id,version_id,generation,reviewer_id);
CREATE UNIQUE INDEX asset_report_once ON asset_check_reports(tenant_id,version_id,"check");
CREATE UNIQUE INDEX asset_install_scope ON asset_installs(tenant_id,scope_type,scope_id,asset_id);
-- Prevent any caller (including owner) from replacing a version's frozen content.
-- +goose StatementBegin
CREATE FUNCTION protect_asset_content() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.asset_id,NEW.version,NEW.manifest,NEW.content_digest,NEW.blob_index_ref,NEW.permissions,NEW.compat,NEW.uploaded_by,NEW.permissions_jcs,NEW.identity_jcs)
 IS DISTINCT FROM (OLD.asset_id,OLD.version,OLD.manifest,OLD.content_digest,OLD.blob_index_ref,OLD.permissions,OLD.compat,OLD.uploaded_by,OLD.permissions_jcs,OLD.identity_jcs)
 THEN RAISE EXCEPTION 'asset version content is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER asset_content_immutable BEFORE UPDATE ON asset_versions FOR EACH ROW EXECUTE FUNCTION protect_asset_content();
CREATE TABLE quota_policies (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), scope_type text NOT NULL CHECK(scope_type IN ('tenant','workspace','user','run')), scope_id uuid NOT NULL, metric text NOT NULL CHECK(metric IN ('millicredits','tokens','runs_concurrent','sandbox_minutes','storage_bytes','schedules','assets_published')), "limit" numeric NOT NULL, period text NOT NULL CHECK(period IN ('hour','day','month','total')), action text NOT NULL CHECK(action IN ('block','warn','degrade')), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE budget_reservations (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, run_id uuid NOT NULL, millicredits bigint NOT NULL, status text NOT NULL CHECK(status IN ('held','settled','released')), expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE usage_ledger (
  id uuid NOT NULL, tenant_id uuid NOT NULL, workspace_id uuid, principal_id uuid REFERENCES principals(id), run_id uuid, segment_seq int, source text NOT NULL CHECK(source IN ('model','sandbox','tool','storage')), meter text NOT NULL, quantity numeric NOT NULL, unit_price numeric NOT NULL, millicredits bigint NOT NULL, idempotency_key text NOT NULL, occurred_at timestamptz NOT NULL, PRIMARY KEY(tenant_id,id,occurred_at), UNIQUE(tenant_id,idempotency_key,occurred_at), FOREIGN KEY(tenant_id,workspace_id) REFERENCES workspaces(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
) PARTITION BY RANGE (occurred_at);
CREATE TABLE usage_ledger_default PARTITION OF usage_ledger DEFAULT;
CREATE TABLE usage_rollups (
  tenant_id uuid NOT NULL REFERENCES tenants(id), scope_type text NOT NULL CHECK(scope_type IN ('tenant','workspace','user','run')), scope_id uuid NOT NULL, meter text NOT NULL, period_start timestamptz NOT NULL, quantity numeric NOT NULL, millicredits bigint NOT NULL, PRIMARY KEY(tenant_id,scope_type,scope_id,meter,period_start)
);
CREATE INDEX runs_session_created ON runs (tenant_id, session_id, created_at);
CREATE INDEX runs_active ON runs (tenant_id, status) WHERE status NOT IN ('succeeded','failed','cancelled','budget_exhausted','timed_out');
CREATE INDEX usage_ledger_run ON usage_ledger (tenant_id, run_id);
CREATE INDEX interactions_pending ON interactions (tenant_id, status) WHERE status IN ('pending','escalated');
CREATE UNIQUE INDEX permission_grants_interaction ON permission_grants (tenant_id, interaction_id, tool);
CREATE UNIQUE INDEX artifacts_session_path ON artifacts (tenant_id, session_id, path);
CREATE TABLE audit_log (
  id uuid NOT NULL, tenant_id uuid NOT NULL REFERENCES tenants(id), actor_id uuid NOT NULL REFERENCES principals(id), actor_kind text NOT NULL CHECK(actor_kind IN ('user','service')), action text NOT NULL, target_type text NOT NULL, target_id text NOT NULL, detail jsonb NOT NULL, ip text NOT NULL DEFAULT '', user_agent text NOT NULL DEFAULT '', prev_hash bytea NOT NULL, hash bytea NOT NULL, occurred_at timestamptz NOT NULL, PRIMARY KEY(tenant_id,id,occurred_at)
) PARTITION BY RANGE (occurred_at);
CREATE TABLE audit_log_default PARTITION OF audit_log DEFAULT;
CREATE TABLE idempotency_keys (
  tenant_id uuid NOT NULL REFERENCES tenants(id), principal_id uuid NOT NULL REFERENCES principals(id), key text NOT NULL, method text NOT NULL, path text NOT NULL, request_hash bytea NOT NULL, status int NOT NULL, headers jsonb NOT NULL, response bytea NOT NULL, key_version int NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, PRIMARY KEY(tenant_id,principal_id,key)
);
-- Activity events survive commit-before-publish failures without changing their payload.
CREATE TABLE control_events (
 tenant_id uuid NOT NULL, run_id uuid NOT NULL, event_id text NOT NULL, operation bigint NOT NULL, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz, PRIMARY KEY(tenant_id,run_id,event_id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
ALTER TABLE control_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE control_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON control_events USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON control_events TO orbit_app;
GRANT UPDATE (published_at) ON control_events TO orbit_app;
CREATE INDEX control_events_unpublished ON control_events(created_at) WHERE published_at IS NULL;
-- +goose StatementBegin
CREATE FUNCTION protect_control_events_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (to_jsonb(NEW)-'published_at') IS DISTINCT FROM (to_jsonb(OLD)-'published_at') THEN
  RAISE EXCEPTION 'outbox update not permitted' USING ERRCODE='42501';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER control_events_outbox_only BEFORE UPDATE ON control_events FOR EACH ROW EXECUTE FUNCTION protect_control_events_outbox();
REVOKE ALL ON FUNCTION protect_control_events_outbox() FROM PUBLIC;
-- +goose StatementBegin
CREATE FUNCTION control_events_unpublished(max_rows int) RETURNS TABLE(tenant_id uuid, run_id uuid, event_id text, payload jsonb)
LANGUAGE sql SECURITY DEFINER STABLE SET search_path=public,pg_temp AS $$
 SELECT tenant_id, run_id, event_id, payload FROM control_events
 WHERE published_at IS NULL AND created_at < now() - interval '30 seconds'
 ORDER BY created_at, operation, event_id LIMIT LEAST(max_rows,500)
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION control_events_unpublished(int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control_events_unpublished(int) TO orbit_app;
CREATE TABLE run_event_rejects (
 tenant_id uuid NOT NULL, stream_seq bigint NOT NULL, subject text NOT NULL, event_id text,
 reason text NOT NULL, detail text NOT NULL DEFAULT '', delivered int NOT NULL, payload bytea NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,stream_seq)
);
ALTER TABLE run_event_rejects ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_event_rejects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_event_rejects USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON run_event_rejects TO orbit_app;
CREATE INDEX ON run_event_rejects(tenant_id,created_at DESC);
ALTER TABLE runs ADD FOREIGN KEY(tenant_id,lock_id) REFERENCES run_locks(tenant_id,id);
ALTER TABLE sessions ADD FOREIGN KEY(tenant_id,pinned_lock_id) REFERENCES run_locks(tenant_id,id);
CREATE INDEX ON memberships(principal_id);
CREATE INDEX ON memberships(tenant_id,joined_at DESC);
CREATE INDEX ON groups(tenant_id,created_at DESC);
CREATE INDEX ON group_members(tenant_id,principal_id);
CREATE INDEX ON workspaces(tenant_id,created_at DESC);
CREATE INDEX ON workspace_members(tenant_id,principal_id);
CREATE INDEX ON workspace_members(tenant_id,created_at DESC);
CREATE INDEX ON projects(tenant_id,workspace_id);
CREATE INDEX ON projects(tenant_id,created_at DESC);
CREATE INDEX ON role_bindings(tenant_id,scope_id);
CREATE INDEX ON role_bindings(tenant_id,principal_id);
CREATE INDEX ON role_bindings(tenant_id,created_at DESC);
CREATE INDEX ON policies(tenant_id,scope_id);
CREATE INDEX ON policies(tenant_id,created_at DESC);
CREATE INDEX ON api_keys(tenant_id,principal_id);
CREATE INDEX ON api_keys(tenant_id,created_at DESC);
CREATE INDEX ON secrets(tenant_id,created_at DESC);
CREATE INDEX ON model_profiles(tenant_id,secret_id);
CREATE INDEX ON model_profiles(tenant_id,created_at DESC);
CREATE INDEX ON sessions(tenant_id,owner_id);
CREATE INDEX ON sessions(tenant_id,workspace_id);
CREATE INDEX ON sessions(tenant_id,pinned_lock_id);
CREATE INDEX ON sessions(tenant_id,project_id);
CREATE INDEX ON sessions(tenant_id,created_at DESC);
CREATE INDEX ON sessions(tenant_id,last_activity_at DESC);
CREATE INDEX ON runs(tenant_id,session_id);
CREATE INDEX ON runs(tenant_id,lock_id);
CREATE INDEX ON runs(tenant_id,created_at DESC);
CREATE INDEX ON run_locks(tenant_id,run_id);
CREATE INDEX ON run_locks(tenant_id,created_at DESC);
CREATE INDEX ON messages(tenant_id,session_id);
CREATE INDEX ON messages(tenant_id,run_id);
CREATE INDEX ON messages(tenant_id,author_id);
CREATE INDEX ON messages(tenant_id,created_at DESC);
CREATE INDEX ON run_events(tenant_id,run_id,stream_seq);
CREATE INDEX ON run_events(tenant_id,session_id,stream_seq);
CREATE INDEX ON interactions(tenant_id,run_id);
CREATE INDEX ON interactions(tenant_id,created_at DESC);
CREATE INDEX ON artifacts(tenant_id,session_id);
CREATE INDEX ON artifacts(tenant_id,run_id);
CREATE INDEX ON artifacts(tenant_id,created_at DESC);
CREATE INDEX ON agent_checkpoints(tenant_id,created_at DESC);
CREATE INDEX ON segment_results(tenant_id,created_at DESC);
CREATE INDEX ON tool_effects(tenant_id,created_at DESC);
CREATE INDEX ON sandboxes(tenant_id,updated_at DESC);
CREATE INDEX ON schedules(tenant_id,owner_id);
CREATE INDEX ON schedules(tenant_id,workspace_id);
CREATE INDEX ON schedules(tenant_id,created_at DESC);
CREATE INDEX ON permission_grants(tenant_id,scope_id);
CREATE INDEX ON permission_grants(tenant_id,created_at DESC);
CREATE INDEX ON assets(tenant_id,owner_id);
CREATE INDEX ON assets(tenant_id,workspace_id);
CREATE INDEX ON assets(tenant_id,created_at DESC);
CREATE INDEX ON asset_versions(tenant_id,created_at DESC);
CREATE INDEX ON asset_files(tenant_id,created_at DESC);
CREATE INDEX ON asset_dependencies(tenant_id,resolved_version_id);
CREATE INDEX ON asset_dependencies(tenant_id,created_at DESC);
CREATE INDEX ON asset_channels(tenant_id,version_id);
CREATE INDEX ON asset_channels(tenant_id,updated_at DESC);
CREATE INDEX ON asset_grants(tenant_id,grantee_id);
CREATE INDEX ON asset_grants(tenant_id,created_at DESC);
CREATE INDEX ON asset_installs(tenant_id,pinned_version_id);
CREATE INDEX ON asset_installs(tenant_id,scope_id);
CREATE INDEX ON asset_installs(tenant_id,asset_id);
CREATE INDEX ON asset_installs(tenant_id,created_at DESC);
CREATE INDEX ON asset_check_reports(tenant_id,version_id);
CREATE INDEX ON asset_check_reports(tenant_id,created_at DESC);
CREATE INDEX ON asset_reviews(tenant_id,version_id);
CREATE INDEX ON asset_reviews(tenant_id,reviewer_id);
CREATE INDEX ON asset_reviews(tenant_id,created_at DESC);
CREATE INDEX ON quota_policies(tenant_id,scope_id);
CREATE INDEX ON quota_policies(tenant_id,created_at DESC);
CREATE INDEX ON budget_reservations(tenant_id,run_id);
CREATE INDEX ON budget_reservations(tenant_id,created_at DESC);
CREATE INDEX ON usage_ledger(tenant_id,workspace_id);
CREATE INDEX ON usage_ledger(tenant_id,principal_id);
CREATE INDEX ON usage_ledger(tenant_id,run_id);
CREATE INDEX ON usage_ledger(tenant_id,occurred_at DESC);
CREATE INDEX ON usage_rollups(tenant_id,scope_id);
CREATE INDEX ON usage_rollups(tenant_id,period_start DESC);
CREATE INDEX ON audit_log(tenant_id,actor_id);
CREATE INDEX ON audit_log(tenant_id,occurred_at DESC);
CREATE INDEX ON idempotency_keys(tenant_id,created_at DESC);
CREATE UNIQUE INDEX ON model_profiles(tenant_id) WHERE is_default;
CREATE INDEX ON role_bindings(tenant_id,scope_type,scope_id);
CREATE INDEX ON idempotency_keys(expires_at);
ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE memberships FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON memberships USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON memberships TO orbit_app;
ALTER TABLE groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE groups FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON groups USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON groups TO orbit_app;
ALTER TABLE group_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE group_members FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON group_members USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON group_members TO orbit_app;
ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspaces FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON workspaces USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON workspaces TO orbit_app;
ALTER TABLE workspace_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_members FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON workspace_members USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON workspace_members TO orbit_app;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON projects USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON projects TO orbit_app;
ALTER TABLE role_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE role_bindings FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON role_bindings USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON role_bindings TO orbit_app;
ALTER TABLE policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE policies FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON policies USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON policies TO orbit_app;
ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON api_keys USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON api_keys TO orbit_app;
ALTER TABLE secrets ENABLE ROW LEVEL SECURITY;
ALTER TABLE secrets FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON secrets USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON secrets TO orbit_app;
ALTER TABLE model_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_profiles FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON model_profiles USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON model_profiles TO orbit_app;
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON sessions USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions TO orbit_app;
ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON runs USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON runs TO orbit_app;
ALTER TABLE run_locks ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_locks FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_locks USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON run_locks TO orbit_app;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON messages USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON messages TO orbit_app;
ALTER TABLE run_segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_segments FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_segments USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON run_segments TO orbit_app;
ALTER TABLE run_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_events USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON run_events TO orbit_app;
ALTER TABLE run_events_default ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_events_default FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_events_default USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON run_events_default TO orbit_app;
ALTER TABLE interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE interactions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON interactions USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON interactions TO orbit_app;
ALTER TABLE artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON artifacts USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON artifacts TO orbit_app;
ALTER TABLE team_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON team_runs USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON team_runs TO orbit_app;
ALTER TABLE agent_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_checkpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON agent_checkpoints USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON agent_checkpoints TO orbit_app;
GRANT SELECT, INSERT, UPDATE ON agent_checkpoints TO orbit_worker;
ALTER TABLE segment_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE segment_results FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON segment_results USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON segment_results TO orbit_app;
GRANT SELECT, INSERT, UPDATE ON segment_results TO orbit_worker;
ALTER TABLE tool_effects ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_effects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tool_effects USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON tool_effects TO orbit_app;
GRANT SELECT, INSERT, UPDATE ON tool_effects TO orbit_worker;
ALTER TABLE sandboxes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sandboxes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON sandboxes USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON sandboxes TO orbit_app;
ALTER TABLE schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedules FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON schedules USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON schedules TO orbit_app;
ALTER TABLE permission_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE permission_grants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON permission_grants USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON permission_grants TO orbit_app;
ALTER TABLE assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE assets FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON assets USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON assets TO orbit_app;
ALTER TABLE asset_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_versions USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE ON asset_versions TO orbit_app;
ALTER TABLE asset_files ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_files FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_files USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON asset_files TO orbit_app;
ALTER TABLE asset_dependencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_dependencies FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_dependencies USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_dependencies TO orbit_app;
ALTER TABLE asset_channels ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_channels FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_channels USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_channels TO orbit_app;
ALTER TABLE asset_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_grants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_grants USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_grants TO orbit_app;
ALTER TABLE asset_installs ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_installs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_installs USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_installs TO orbit_app;
ALTER TABLE asset_check_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_check_reports FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_check_reports USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_check_reports TO orbit_app;
ALTER TABLE asset_reviews ENABLE ROW LEVEL SECURITY;
ALTER TABLE asset_reviews FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON asset_reviews USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON asset_reviews TO orbit_app;
ALTER TABLE quota_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE quota_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON quota_policies USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON quota_policies TO orbit_app;
ALTER TABLE budget_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE budget_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON budget_reservations USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON budget_reservations TO orbit_app;
ALTER TABLE usage_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_ledger FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_ledger USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON usage_ledger TO orbit_app;
ALTER TABLE usage_ledger_default ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_ledger_default FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_ledger_default USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON usage_ledger_default TO orbit_app;
ALTER TABLE usage_rollups ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_rollups FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_rollups USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON usage_rollups TO orbit_app;
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON audit_log USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON audit_log TO orbit_app;
ALTER TABLE audit_log_default ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log_default FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON audit_log_default USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT ON audit_log_default TO orbit_app;
ALTER TABLE idempotency_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON idempotency_keys USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys TO orbit_app;
GRANT SELECT, INSERT ON principals, tenants TO orbit_app;
-- +goose StatementBegin
CREATE FUNCTION ensure_monthly_partitions(parent_table text, months_ahead int) RETURNS void
LANGUAGE plpgsql SET search_path = public, pg_temp SET TimeZone = 'UTC' AS $$
DECLARE start_at timestamptz; end_at timestamptz; child text; time_column text; i int;
BEGIN
 IF current_user <> 'orbit_owner' THEN RAISE EXCEPTION 'owner required'; END IF;
 IF parent_table NOT IN ('run_events','usage_ledger','audit_log') OR months_ahead NOT BETWEEN 0 AND 24 THEN
  RAISE EXCEPTION 'invalid partition request';
 END IF;
 time_column := CASE WHEN parent_table='run_events' THEN 'ts' ELSE 'occurred_at' END;
 EXECUTE format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE',parent_table);
 FOR i IN 0..months_ahead LOOP
  start_at := date_trunc('month',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + make_interval(months=>i);
  end_at := start_at + interval '1 month';
  child := parent_table || '_' || to_char(start_at AT TIME ZONE 'UTC','YYYYMM');
  IF to_regclass('public.'||child) IS NULL THEN
   EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL)',child,parent_table);
   EXECUTE format('ALTER TABLE %I ADD CHECK (%I >= %L AND %I < %L)',child,time_column,start_at,time_column,end_at);
   EXECUTE format('WITH moved AS (DELETE FROM %I WHERE %I >= $1 AND %I < $2 RETURNING *) INSERT INTO %I SELECT * FROM moved',parent_table||'_default',time_column,time_column,child) USING start_at,end_at;
   EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',parent_table,child,start_at,end_at);
   EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',child);
   EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',child);
   EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (tenant_id = NULLIF(current_setting(''app.tenant_id'', true), '''')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting(''app.tenant_id'', true), '''')::uuid)',child);
   IF parent_table='run_events' THEN EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO orbit_app',child);
   ELSE EXECUTE format('GRANT SELECT, INSERT ON %I TO orbit_app',child); END IF;
  END IF;
 END LOOP;
END $$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION ensure_monthly_partitions(text,int) FROM PUBLIC;
CREATE TABLE usage_keys(tenant_id uuid NOT NULL REFERENCES tenants(id), idempotency_key text NOT NULL, created_at timestamptz NOT NULL, PRIMARY KEY(tenant_id,idempotency_key));
ALTER TABLE usage_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON usage_keys USING(tenant_id = NULLIF(current_setting('app.tenant_id',true),'')::uuid) WITH CHECK(tenant_id = NULLIF(current_setting('app.tenant_id',true),'')::uuid);
GRANT SELECT, INSERT ON usage_keys TO orbit_app;
CREATE INDEX ON usage_keys(created_at);
-- Membership discovery is the only cross-tenant membership read. The caller supplies
-- the authenticated principal; no tenant business contents are exposed.
-- +goose StatementBegin
CREATE FUNCTION principal_tenants(pid uuid) RETURNS TABLE(id uuid,slug text,name text)
LANGUAGE sql SECURITY DEFINER STABLE SET search_path=public,pg_temp AS $$
 SELECT t.id,t.slug,t.name FROM tenants t JOIN memberships m ON m.tenant_id=t.id
 WHERE m.principal_id=pid AND m.status='active' AND t.status='active' ORDER BY t.id;
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION principal_tenants(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION principal_tenants(uuid) TO orbit_app;
-- The worker may finish a started effect or resolve an uncertain one; completed
-- effects cannot be rewritten or retried under the same key.
-- +goose StatementBegin
CREATE FUNCTION protect_worker_effect() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_user='orbit_worker' AND (
  (to_jsonb(NEW)-ARRAY['status','result_preview','result_ref','external_ref','completed_at']) IS DISTINCT FROM
  (to_jsonb(OLD)-ARRAY['status','result_preview','result_ref','external_ref','completed_at']) OR
  OLD.status NOT IN ('started','unknown') OR NEW.status NOT IN ('completed','failed','unknown')) THEN
  RAISE EXCEPTION 'effect update not permitted' USING ERRCODE='42501';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER worker_effect_update BEFORE UPDATE ON tool_effects FOR EACH ROW EXECUTE FUNCTION protect_worker_effect();
REVOKE ALL ON FUNCTION protect_worker_effect() FROM PUBLIC;
-- +goose Down
SET LOCAL ROLE orbit_owner;
DROP FUNCTION invitation_tenant(bytea);
DROP TABLE invitations;
DROP FUNCTION principal_tenants(uuid);
DROP FUNCTION ensure_monthly_partitions(text,int);
DROP TABLE usage_ledger CASCADE;
DROP TABLE usage_keys;
DROP TABLE idempotency_keys CASCADE;
DROP TABLE audit_log CASCADE;
DROP TABLE usage_rollups CASCADE;
DROP TABLE budget_reservations CASCADE;
DROP TABLE quota_policies CASCADE;
DROP TABLE asset_reviews CASCADE;
DROP TABLE asset_check_reports CASCADE;
DROP TABLE asset_installs CASCADE;
DROP TABLE asset_grants CASCADE;
DROP TABLE asset_channels CASCADE;
DROP TABLE asset_dependencies CASCADE;
DROP TABLE asset_files CASCADE;
DROP TABLE asset_versions CASCADE;
DROP FUNCTION protect_asset_content();
DROP TABLE assets CASCADE;
DROP TABLE permission_grants CASCADE;
DROP TABLE schedules CASCADE;
DROP TABLE sandboxes CASCADE;
DROP TABLE tool_effects CASCADE;
DROP FUNCTION protect_worker_effect();
DROP TABLE segment_results CASCADE;
DROP TABLE agent_checkpoints CASCADE;
DROP TABLE team_runs CASCADE;
DROP TABLE artifacts CASCADE;
DROP TABLE interactions CASCADE;
DROP FUNCTION control_events_unpublished(int);
DROP TABLE control_events CASCADE;
DROP FUNCTION protect_control_events_outbox();
DROP TABLE run_event_rejects;
DROP TABLE run_events CASCADE;
DROP TABLE run_segments CASCADE;
DROP TABLE messages CASCADE;
DROP TABLE run_locks CASCADE;
DROP TABLE runs CASCADE;
DROP TABLE sessions CASCADE;
DROP TABLE model_profiles CASCADE;
DROP TABLE secrets CASCADE;
DROP TABLE api_keys CASCADE;
DROP TABLE policies CASCADE;
DROP TABLE role_bindings CASCADE;
DROP TABLE projects CASCADE;
DROP TABLE workspace_members CASCADE;
DROP TABLE workspaces CASCADE;
DROP TABLE group_members CASCADE;
DROP TABLE groups CASCADE;
DROP TABLE memberships CASCADE;
DROP TABLE tenants CASCADE;
DROP TABLE principals CASCADE;
GRANT CREATE ON SCHEMA public TO PUBLIC;
