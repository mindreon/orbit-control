-- Versioned SOP definitions (06 §2, T3.3): the registry the sop_step activity reads.
-- Grants: docs/persistence-failure-modes.md ("Task runtime tables").

-- +goose Up

CREATE TABLE sop_definitions (
  tenant_id  TEXT NOT NULL REFERENCES tenants(id),
  sop_id     TEXT NOT NULL CONSTRAINT sop_definitions_id_not_empty CHECK (sop_id <> ''),
  version    INTEGER NOT NULL CONSTRAINT sop_definitions_version_positive CHECK (version > 0),
  steps      JSONB NOT NULL CONSTRAINT sop_definitions_steps_list CHECK (
               jsonb_typeof(steps) = 'array' AND jsonb_array_length(steps) > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, sop_id, version)
);

ALTER TABLE sop_definitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_definitions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON sop_definitions FOR ALL
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT SELECT, INSERT ON sop_definitions TO orbit_app;
GRANT SELECT ON sop_definitions TO orbit_worker;

-- +goose Down

DROP TABLE sop_definitions;
