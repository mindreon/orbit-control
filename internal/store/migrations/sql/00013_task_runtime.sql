-- TaskWorkflow runtime tables (orbit-infra docs/architecture 03, 08, 09, 10;
-- 12 Step 2). They sit next to the room tables until the cutover and do not
-- reference them. Grants and their reasons: docs/persistence-failure-modes.md
-- ("Task runtime tables and orbit_worker"); checks: ISO-21, ISO-22.
--
-- No foreign key points at tasks: the worker writes ledger, checkpoint and
-- lease rows before the Projector has written the task projection, and the
-- projections themselves are rebuildable (09 §3).

-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles
     WHERE rolname = 'orbit_worker' AND rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
  ) THEN
    RAISE EXCEPTION 'role orbit_worker must exist as LOGIN NOSUPERUSER NOBYPASSRLS; run deploy/postgres/bootstrap-roles.sql first';
  END IF;
END
$$;
-- +goose StatementEnd

CREATE TABLE agent_profiles (
  tenant_id             TEXT NOT NULL REFERENCES tenants(id),
  profile_id            TEXT NOT NULL CONSTRAINT agent_profiles_id_not_empty CHECK (profile_id <> ''),
  version               INTEGER NOT NULL CONSTRAINT agent_profiles_version_positive CHECK (version > 0),
  status                TEXT NOT NULL DEFAULT 'active'
                          CONSTRAINT agent_profiles_status CHECK (status IN ('active', 'deprecated')),
  display_name          TEXT NOT NULL DEFAULT '',
  spec                  JSONB NOT NULL,
  migrated_from_persona TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, profile_id, version)
);

CREATE TABLE node_type_registry (
  registry_version INTEGER NOT NULL CHECK (registry_version > 0),
  node_type        TEXT NOT NULL CONSTRAINT node_type_registry_type CHECK (
                     node_type IN ('agent_turn', 'sop_stage', 'team_stage', 'approval', 'wait', 'checkpoint')),
  type_version     INTEGER NOT NULL CHECK (type_version > 0),
  enabled          BOOLEAN NOT NULL,
  agent_creatable  BOOLEAN NOT NULL,
  workspace_mode   TEXT NOT NULL CHECK (workspace_mode IN ('write', 'read', 'none')),
  -- The node spec model in orbit-runtime orbit_contracts.v3.
  spec_schema_ref  TEXT NOT NULL,
  defaults         JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (registry_version, node_type, type_version)
);

-- Registry v1 (05 §5). team_stage is registered but disabled until phase 2.
INSERT INTO node_type_registry
  (registry_version, node_type, type_version, enabled, agent_creatable, workspace_mode, spec_schema_ref)
VALUES
  (1, 'agent_turn', 1, true,  true,  'write', 'orbit.contracts/3#AgentTurnSpec'),
  (1, 'sop_stage',  1, true,  false, 'write', 'orbit.contracts/3#SopStageSpec'),
  (1, 'team_stage', 1, false, false, 'write', 'orbit.contracts/3#TeamStageSpec'),
  (1, 'approval',   1, true,  true,  'none',  'orbit.contracts/3#ApprovalSpec'),
  (1, 'wait',       1, true,  true,  'none',  'orbit.contracts/3#WaitSpec'),
  (1, 'checkpoint', 1, true,  true,  'read',  'orbit.contracts/3#CheckpointSpec');

-- ---- projections (written by the Projector in control) ----------------------

CREATE TABLE tasks (
  id              TEXT PRIMARY KEY CONSTRAINT tasks_id_prefix CHECK (id LIKE 'task\_%'),
  tenant_id       TEXT NOT NULL REFERENCES tenants(id),
  workflow_id     TEXT NOT NULL UNIQUE,
  title           TEXT NOT NULL,
  goal            TEXT NOT NULL,
  mode            TEXT NOT NULL CHECK (mode IN ('single', 'multi', 'long')),
  status          TEXT NOT NULL CONSTRAINT tasks_status CHECK (status IN (
                    'CREATED', 'PLANNING', 'RUNNING', 'WAITING', 'PAUSED', 'PAUSED_NEEDS_REVIEW',
                    'TAKEN_OVER', 'COMPLETED', 'FAILED', 'CANCELLED')),
  profile_ref     TEXT NOT NULL,
  sop_ref         TEXT,
  plan_version    INTEGER NOT NULL DEFAULT 1 CHECK (plan_version > 0),
  created_by      TEXT NOT NULL,
  entity_version  BIGINT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX tasks_tenant_created_idx ON tasks (tenant_id, created_at DESC);

CREATE TABLE plan_versions (
  tenant_id         TEXT NOT NULL REFERENCES tenants(id),
  task_id           TEXT NOT NULL,
  plan_version      INTEGER NOT NULL CHECK (plan_version > 0),
  parent_version    INTEGER NOT NULL CHECK (parent_version >= 0),
  hash              TEXT NOT NULL CHECK (hash ~ '^sha256:[0-9a-f]{64}$'),
  change_command_id TEXT,
  actor             JSONB NOT NULL,
  graph             JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (task_id, plan_version)
);

CREATE TABLE task_nodes (
  tenant_id          TEXT NOT NULL REFERENCES tenants(id),
  task_id            TEXT NOT NULL,
  node_id            TEXT NOT NULL CHECK (node_id LIKE 'n\_%'),
  node_type          TEXT NOT NULL,
  title              TEXT NOT NULL,
  status             TEXT NOT NULL CONSTRAINT task_nodes_status CHECK (status IN (
                       'PENDING', 'READY', 'RUNNING', 'AWAITING_APPROVAL', 'AWAITING_INPUT', 'PROPOSED',
                       'VERIFYING', 'COMPLETED', 'RETRY_PENDING', 'BLOCKED', 'SKIPPED', 'CANCELLED')),
  workspace_mode     TEXT NOT NULL CHECK (workspace_mode IN ('write', 'read', 'none')),
  owner_profile      TEXT NOT NULL,
  frozen             BOOLEAN NOT NULL DEFAULT false,
  current_attempt_id TEXT,
  attempt_count      INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  entity_version     BIGINT NOT NULL DEFAULT 0,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (task_id, node_id)
);

CREATE TABLE stage_attempts (
  attempt_id              TEXT PRIMARY KEY CHECK (attempt_id LIKE 'att\_%'),
  tenant_id               TEXT NOT NULL REFERENCES tenants(id),
  task_id                 TEXT NOT NULL,
  node_id                 TEXT NOT NULL,
  attempt_no              INTEGER NOT NULL CHECK (attempt_no > 0),
  status                  TEXT NOT NULL CONSTRAINT stage_attempts_status CHECK (status IN (
                            'STARTING', 'RUNNING', 'PARKED_HITL', 'PARKED_INPUT', 'HANDOVER', 'VERIFYING',
                            'ACCEPTED', 'REJECTED', 'ABORTED', 'LOST')),
  profile_ref             TEXT NOT NULL,
  runtime                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure                 JSONB,
  usage                   JSONB NOT NULL DEFAULT '{}'::jsonb,
  agentscope_version      TEXT NOT NULL DEFAULT '',
  runtime_image_digest    TEXT NOT NULL DEFAULT '',
  contract_schema_version TEXT NOT NULL DEFAULT '',
  entity_version          BIGINT NOT NULL DEFAULT 0,
  started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at             TIMESTAMPTZ,
  CONSTRAINT stage_attempts_node_attempt_key UNIQUE (task_id, node_id, attempt_no)
);

CREATE TABLE task_approvals (
  approval_id         TEXT PRIMARY KEY CHECK (approval_id LIKE 'apr\_%'),
  tenant_id           TEXT NOT NULL REFERENCES tenants(id),
  task_id             TEXT NOT NULL,
  node_id             TEXT,
  attempt_id          TEXT,
  tool_call_id        TEXT,
  subject             JSONB NOT NULL,
  status              TEXT NOT NULL CONSTRAINT task_approvals_status CHECK (status IN (
                        'PENDING', 'APPROVED', 'REJECTED', 'CANCELLED', 'TAKEN_OVER')),
  decided_by          TEXT,
  decision_command_id TEXT,
  entity_version      BIGINT NOT NULL DEFAULT 0,
  requested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at          TIMESTAMPTZ
);
CREATE INDEX task_approvals_pending_idx ON task_approvals (tenant_id, task_id) WHERE status = 'PENDING';

-- ---- history (append only) --------------------------------------------------

CREATE TABLE task_messages (
  tenant_id         TEXT NOT NULL REFERENCES tenants(id),
  task_id           TEXT NOT NULL,
  message_seq       BIGINT NOT NULL CHECK (message_seq > 0),
  client_message_id TEXT NOT NULL,
  text              TEXT NOT NULL,
  attachments       JSONB NOT NULL DEFAULT '[]'::jsonb,
  delivery          TEXT NOT NULL CHECK (delivery IN ('queue', 'interrupt')),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (task_id, message_seq),
  CONSTRAINT task_messages_client_message_key UNIQUE (task_id, client_message_id)
);

CREATE TABLE task_events (
  tenant_id   TEXT NOT NULL REFERENCES tenants(id),
  task_id     TEXT NOT NULL,
  seq         BIGINT NOT NULL CHECK (seq > 0),
  event_id    TEXT NOT NULL CONSTRAINT task_events_event_id_key UNIQUE CHECK (event_id LIKE 'evt\_%'),
  event_type  TEXT NOT NULL,
  body        JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (task_id, seq)
);

-- ---- runtime writes -----------------------------------------------------------

-- Transactional outbox (09 §3). No RLS: the Projector reads every tenant.
CREATE TABLE runtime_outbox (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    TEXT NOT NULL REFERENCES tenants(id),
  task_id      TEXT NOT NULL,
  event_id     TEXT NOT NULL CONSTRAINT runtime_outbox_event_id_key UNIQUE,
  body         JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  projected_at TIMESTAMPTZ
);
CREATE INDEX runtime_outbox_pending_idx ON runtime_outbox (id) WHERE projected_at IS NULL;

CREATE TABLE idempotency_ledger (
  scope        TEXT NOT NULL CONSTRAINT idempotency_ledger_scope CHECK (scope IN (
                 'api_command', 'side_effect', 'team_op', 'channel_delivery', 'workspace_op')),
  key          TEXT NOT NULL CHECK (key <> ''),
  tenant_id    TEXT NOT NULL REFERENCES tenants(id),
  request_hash TEXT NOT NULL,
  status       TEXT NOT NULL CONSTRAINT idempotency_ledger_status CHECK (status IN (
                 'started', 'succeeded', 'failed_permanent')),
  result_ref   JSONB,
  owner        TEXT,
  first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (scope, key)
);

CREATE TABLE checkpoints (
  checkpoint_id        TEXT PRIMARY KEY CHECK (checkpoint_id LIKE 'ckpt\_%'),
  tenant_id            TEXT NOT NULL REFERENCES tenants(id),
  task_id              TEXT NOT NULL,
  node_id              TEXT NOT NULL,
  attempt_id           TEXT NOT NULL,
  seq                  INTEGER NOT NULL CHECK (seq >= 0),
  kind                 TEXT NOT NULL CONSTRAINT checkpoints_kind CHECK (kind IN (
                         'agent_state', 'sop_run_state', 'workspace_snapshot', 'plan', 'team_snapshot')),
  blob_ref             TEXT NOT NULL CHECK (blob_ref ~ '^sha256:[0-9a-f]{64}$'),
  size_bytes           BIGINT NOT NULL CHECK (size_bytes >= 0),
  encryption           JSONB NOT NULL,
  schema_version       TEXT NOT NULL,
  agentscope_version   TEXT NOT NULL,
  committed_in_history BOOLEAN NOT NULL DEFAULT false,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT checkpoints_attempt_seq_key UNIQUE (attempt_id, seq)
);
CREATE INDEX checkpoints_uncommitted_idx ON checkpoints (created_at) WHERE NOT committed_in_history;

CREATE TABLE artifact_manifests (
  manifest_id           TEXT PRIMARY KEY CHECK (manifest_id LIKE 'man\_%'),
  tenant_id             TEXT NOT NULL REFERENCES tenants(id),
  task_id               TEXT NOT NULL,
  attempt_id            TEXT NOT NULL,
  workspace_snapshot_id TEXT,
  entries               JSONB NOT NULL,
  verification_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  manifest_hash         TEXT NOT NULL CHECK (manifest_hash ~ '^sha256:[0-9a-f]{64}$'),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspace_leases (
  lease_id       TEXT PRIMARY KEY CHECK (lease_id <> ''),
  tenant_id      TEXT NOT NULL REFERENCES tenants(id),
  -- tenant/task for the write lease, tenant/task/ro/<attempt> for a copy.
  lease_key      TEXT NOT NULL CHECK (lease_key <> ''),
  lease_mode     TEXT NOT NULL CHECK (lease_mode IN ('write', 'read')),
  backend        TEXT NOT NULL CHECK (backend IN ('docker', 'opensandbox')),
  sandbox_id     TEXT,
  holder_attempt TEXT NOT NULL,
  expires_at     TIMESTAMPTZ NOT NULL,
  released_at    TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- One live write lease per workspace (08 §1, serial writes; FM-79).
CREATE UNIQUE INDEX workspace_leases_one_writer_idx
  ON workspace_leases (lease_key) WHERE lease_mode = 'write' AND released_at IS NULL;
CREATE INDEX workspace_leases_expiry_idx ON workspace_leases (expires_at) WHERE released_at IS NULL;

-- ---- row level security ---------------------------------------------------------

ALTER TABLE agent_profiles     ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_profiles     FORCE ROW LEVEL SECURITY;
ALTER TABLE tasks              ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks              FORCE ROW LEVEL SECURITY;
ALTER TABLE plan_versions      ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_versions      FORCE ROW LEVEL SECURITY;
ALTER TABLE task_nodes         ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_nodes         FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_attempts     ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_attempts     FORCE ROW LEVEL SECURITY;
ALTER TABLE task_approvals     ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_approvals     FORCE ROW LEVEL SECURITY;
ALTER TABLE task_messages      ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_messages      FORCE ROW LEVEL SECURITY;
ALTER TABLE task_events        ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_events        FORCE ROW LEVEL SECURITY;
ALTER TABLE idempotency_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE checkpoints        ENABLE ROW LEVEL SECURITY;
ALTER TABLE checkpoints        FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_manifests ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_manifests FORCE ROW LEVEL SECURITY;
ALTER TABLE workspace_leases   ENABLE ROW LEVEL SECURITY;
ALTER TABLE workspace_leases   FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON agent_profiles FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON tasks FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON plan_versions FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON task_nodes FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON stage_attempts FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON task_approvals FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON task_messages FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON task_events FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON idempotency_ledger FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON checkpoints FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON artifact_manifests FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON workspace_leases FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- ---- grants (docs/persistence-failure-modes.md, "Task runtime tables") --------

GRANT SELECT ON node_type_registry TO orbit_app, orbit_worker;
GRANT SELECT, INSERT ON agent_profiles TO orbit_app;
GRANT SELECT ON agent_profiles TO orbit_worker;
GRANT SELECT, INSERT ON tasks, task_nodes, stage_attempts, task_approvals TO orbit_app;
GRANT SELECT, INSERT ON plan_versions, task_messages, task_events TO orbit_app;
GRANT SELECT, UPDATE (projected_at) ON runtime_outbox TO orbit_app;
GRANT INSERT ON runtime_outbox TO orbit_worker;
GRANT USAGE ON SEQUENCE runtime_outbox_id_seq TO orbit_worker;
GRANT SELECT, INSERT ON idempotency_ledger TO orbit_app, orbit_worker;
GRANT UPDATE (status, result_ref, owner, last_seen) ON idempotency_ledger TO orbit_worker;
GRANT SELECT ON checkpoints, artifact_manifests, workspace_leases TO orbit_app;
GRANT SELECT, INSERT ON checkpoints, artifact_manifests, workspace_leases TO orbit_worker;
GRANT UPDATE (committed_in_history) ON checkpoints TO orbit_worker;
GRANT UPDATE (sandbox_id, expires_at, released_at) ON workspace_leases TO orbit_worker;

REVOKE TRUNCATE, REFERENCES, TRIGGER ON
  agent_profiles, node_type_registry, tasks, plan_versions, task_nodes, stage_attempts,
  task_approvals, task_messages, task_events, runtime_outbox, idempotency_ledger,
  checkpoints, artifact_manifests, workspace_leases
  FROM orbit_app, orbit_worker, PUBLIC;

-- +goose Down

DROP TABLE workspace_leases;
DROP TABLE artifact_manifests;
DROP TABLE checkpoints;
DROP TABLE idempotency_ledger;
DROP TABLE runtime_outbox;
DROP TABLE task_events;
DROP TABLE task_messages;
DROP TABLE task_approvals;
DROP TABLE stage_attempts;
DROP TABLE task_nodes;
DROP TABLE plan_versions;
DROP TABLE tasks;
DROP TABLE node_type_registry;
DROP TABLE agent_profiles;
