-- The runtime compiles an SOP into a subgraph, and node.status_changed now says where a node sits in it: `parent_node_id`
-- (the node it nests under) and `sop_step` (which step of which SOP it is: sop, role, total, step_id, index, subject).
-- getPlan does not carry the SOP step, so the projection keeps both for the plan API to show "SOP X: step i/n" after a
-- reload. Both are nullable: a node outside an SOP, and an event from before the runtime sent them, have neither.
-- orbit_app gets column-level UPDATE on exactly these two. Reasons and checks: docs/persistence-failure-modes.md (ISO-30).

-- +goose Up

ALTER TABLE task_nodes
  ADD COLUMN parent_node_id TEXT,
  ADD COLUMN sop_step       JSONB CHECK (sop_step IS NULL OR jsonb_typeof(sop_step) = 'object');

GRANT UPDATE (parent_node_id, sop_step) ON task_nodes TO orbit_app;

-- +goose Down

REVOKE UPDATE (parent_node_id, sop_step) ON task_nodes FROM orbit_app;

ALTER TABLE task_nodes
  DROP COLUMN sop_step,
  DROP COLUMN parent_node_id;
