-- Contract §18.3 (C32 rev2). Every child FK that points at rooms, and
-- artifact_versions -> artifacts, is ON DELETE RESTRICT: P0 has no physical
-- delete path (§18.7a).

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
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles
     WHERE rolname = 'orbit_definer' AND NOT rolcanlogin AND rolbypassrls AND NOT rolsuper
  ) THEN
    RAISE EXCEPTION 'role orbit_definer must exist as NOLOGIN BYPASSRLS NOSUPERUSER';
  END IF;
END
$$;
-- +goose StatementEnd

CREATE TABLE tenants (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id            TEXT PRIMARY KEY,
  tenant_id     TEXT NOT NULL REFERENCES tenants(id),
  iss           TEXT NOT NULL,
  sub           TEXT NOT NULL,
  display_name  TEXT NOT NULL DEFAULT '',
  email         TEXT NOT NULL DEFAULT '',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ,
  CONSTRAINT users_iss_sub_key UNIQUE (iss, sub)
);
CREATE INDEX users_tenant_id_idx ON users (tenant_id);

-- Pre-login tables: no RLS (§18.5). Only sha256(session id) is stored.
CREATE TABLE sessions (
  id_hash      BYTEA PRIMARY KEY CONSTRAINT sessions_id_hash_sha256 CHECK (octet_length(id_hash) = 32),
  user_id      TEXT NOT NULL REFERENCES users(id),
  tenant_id    TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX sessions_expires_at_idx ON sessions (expires_at);

CREATE TABLE oidc_login_state (
  state            TEXT PRIMARY KEY,
  nonce            TEXT NOT NULL,
  pkce_verifier    TEXT NOT NULL,
  return_to        TEXT NOT NULL DEFAULT '/',
  pre_session_hash BYTEA CONSTRAINT oidc_pre_session_hash_sha256
                   CHECK (pre_session_hash IS NULL OR octet_length(pre_session_hash) = 32),
  expires_at       TIMESTAMPTZ NOT NULL
);
CREATE INDEX oidc_login_state_expires_at_idx ON oidc_login_state (expires_at);

CREATE TABLE rooms (
  id                TEXT PRIMARY KEY,
  tenant_id         TEXT NOT NULL REFERENCES tenants(id),
  created_by        TEXT NOT NULL REFERENCES users(id),
  kind              TEXT NOT NULL,
  title             TEXT NOT NULL DEFAULT '',
  state             TEXT NOT NULL,
  permission_preset TEXT NOT NULL,
  runtime           JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_id        TEXT NOT NULL DEFAULT '',
  persona_id        TEXT NOT NULL DEFAULT '',
  delegation        JSONB,
  failure           JSONB,
  last_event_seq    BIGINT NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ NULL,
  deleted_by        TEXT NULL,
  CONSTRAINT rooms_state_check CHECK (
    state IN ('idle', 'running', 'awaiting_approval', 'awaiting_external', 'closed', 'failed')
  )
);
CREATE INDEX rooms_tenant_owner_created_idx
  ON rooms (tenant_id, created_by, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE TABLE turns (
  id          TEXT PRIMARY KEY,
  tenant_id   TEXT NOT NULL REFERENCES tenants(id),
  task_id     TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  kind        TEXT NOT NULL,
  status      TEXT NOT NULL,
  error_code  TEXT,
  model_mode  TEXT,
  model_name  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT turns_kind_check CHECK (kind IN ('message', 'decide', 'answer', 'steer'))
);
CREATE INDEX turns_task_created_idx ON turns (task_id, created_at);

-- events.id is an internal key only; the SSE cursor is the per-task seq
-- (§18.4). INDEX(task_id, seq) is served by events_task_seq_key.
CREATE TABLE events (
  id         BIGSERIAL PRIMARY KEY,
  tenant_id  TEXT NOT NULL REFERENCES tenants(id),
  task_id    TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  seq        BIGINT NOT NULL,
  event_uid  TEXT NOT NULL,
  turn_id    TEXT,
  type       TEXT NOT NULL,
  source     TEXT NOT NULL,
  agent_id   TEXT,
  agent_path TEXT,
  payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT events_task_seq_key UNIQUE (task_id, seq),
  CONSTRAINT events_task_event_uid_key UNIQUE (task_id, event_uid),
  CONSTRAINT events_no_assistant_delta CHECK (type <> 'assistant.delta')
);

CREATE TABLE messages (
  id         TEXT PRIMARY KEY,
  tenant_id  TEXT NOT NULL REFERENCES tenants(id),
  task_id    TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  role       TEXT NOT NULL,
  type       TEXT NOT NULL DEFAULT '',
  text       TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX messages_task_created_idx ON messages (task_id, created_at);

CREATE TABLE approvals (
  id                  TEXT PRIMARY KEY,
  tenant_id           TEXT NOT NULL REFERENCES tenants(id),
  task_id             TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  approval_request_id TEXT,
  call_id             TEXT,
  turn_id             TEXT,
  agent_id            TEXT,
  agent_path          TEXT,
  tool_name           TEXT NOT NULL DEFAULT '',
  reason              TEXT NOT NULL DEFAULT '',
  arguments           JSONB,
  risk                TEXT,
  allow_always        BOOLEAN NOT NULL DEFAULT false,
  status              TEXT NOT NULL,
  decision            TEXT NOT NULL DEFAULT '',
  rule_id             TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at          TIMESTAMPTZ,
  CONSTRAINT approvals_task_request_key UNIQUE (task_id, approval_request_id)
);
CREATE INDEX approvals_task_status_idx ON approvals (task_id, status);

-- room_id is DB-only: equals scope_id when scope='room', otherwise NULL.
CREATE TABLE approval_rules (
  id                       TEXT PRIMARY KEY,
  tenant_id                TEXT NOT NULL REFERENCES tenants(id),
  tool_name                TEXT NOT NULL,
  argument_pattern         JSONB NOT NULL DEFAULT '{}'::jsonb,
  any_arguments            BOOLEAN NOT NULL DEFAULT false,
  scope                    TEXT NOT NULL,
  scope_id                 TEXT NOT NULL,
  room_id                  TEXT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  created_from_approval_id TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT approval_rules_room_scope_check CHECK ((scope = 'room') = (room_id IS NOT NULL)),
  CONSTRAINT approval_rules_room_matches_scope CHECK (room_id IS NULL OR room_id = scope_id)
);
CREATE INDEX approval_rules_tenant_scope_idx ON approval_rules (tenant_id, scope, scope_id);

CREATE TABLE idempotency_keys (
  key_hash     BYTEA NOT NULL,
  tenant_id    TEXT NOT NULL REFERENCES tenants(id),
  created_by   TEXT NOT NULL,
  request_hash BYTEA NOT NULL,
  task_id      TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL,
  CONSTRAINT idempotency_keys_pkey PRIMARY KEY (tenant_id, created_by, key_hash)
);

-- No UNIQUE(artifact_id, content_digest, version): (artifact_id, version) is
-- already unique (§18.3).
CREATE TABLE artifacts (
  id             TEXT PRIMARY KEY,
  tenant_id      TEXT NOT NULL REFERENCES tenants(id),
  task_id        TEXT NOT NULL REFERENCES rooms(id) ON DELETE RESTRICT,
  title          TEXT NOT NULL DEFAULT '',
  latest_version INT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_task_id_idx ON artifacts (task_id);

CREATE TABLE artifact_versions (
  artifact_id         TEXT NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
  tenant_id           TEXT NOT NULL REFERENCES tenants(id),
  version             INT NOT NULL,
  parent_version      INT NULL,
  mime_type           TEXT NOT NULL,
  size_bytes          BIGINT NOT NULL,
  content_digest      TEXT NOT NULL,
  previewable         BOOLEAN NOT NULL DEFAULT false,
  storage_ref         TEXT,
  turn_id             TEXT,
  source_tool_call_id TEXT,
  agent_id            TEXT,
  agent_path          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT artifact_versions_pkey PRIMARY KEY (artifact_id, version),
  CONSTRAINT artifact_versions_version_check CHECK (version >= 1)
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

CREATE TABLE cloud_agent_jobs (
  id                TEXT PRIMARY KEY,
  tenant_id         TEXT NOT NULL REFERENCES tenants(id),
  repo_url          TEXT NOT NULL,
  branch            TEXT NOT NULL DEFAULT '',
  prompt            TEXT NOT NULL,
  permission_preset TEXT NOT NULL,
  persona_id        TEXT NOT NULL DEFAULT '',
  state             TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX cloud_agent_jobs_tenant_id_idx ON cloud_agent_jobs (tenant_id);

-- +goose Down
DROP TABLE cloud_agent_jobs;
DROP TABLE mcp_connectors;
DROP TABLE personas;
DROP TABLE artifact_versions;
DROP TABLE artifacts;
DROP TABLE idempotency_keys;
DROP TABLE approval_rules;
DROP TABLE approvals;
DROP TABLE messages;
DROP TABLE events;
DROP TABLE turns;
DROP TABLE rooms;
DROP TABLE oidc_login_state;
DROP TABLE sessions;
DROP TABLE users;
DROP TABLE tenants;
