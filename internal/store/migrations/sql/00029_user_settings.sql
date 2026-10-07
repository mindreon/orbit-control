-- Per-user settings (permission defaults: the preset a new task starts with and the rules of "custom"). One row per
-- (tenant, user); a user who never saved has none and gets the defaults. Control resolves them into a task's config at
-- creation, so orbit_worker has no grant. Reasons and checks: docs/persistence-failure-modes.md ("user_settings").

-- +goose Up

CREATE TABLE user_settings (
  tenant_id  TEXT NOT NULL REFERENCES tenants(id),
  user_id    TEXT NOT NULL,
  spec       JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, user_id)
);

ALTER TABLE user_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_settings FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON user_settings FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON user_settings TO orbit_app;
GRANT UPDATE (spec, updated_at) ON user_settings TO orbit_app;

-- +goose Down

DROP TABLE user_settings;
