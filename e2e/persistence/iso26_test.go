//go:build e2e

package persistence

import (
	"context"
	"testing"
)

const checkpointKeyContract = "orbit-infra 17 G16 (migration 00019)"

const insertCheckpointSQL = `INSERT INTO checkpoints (checkpoint_id, tenant_id, task_id, node_id, attempt_id, seq, kind, blob_ref,
	   size_bytes, encryption, schema_version, agentscope_version)
	   VALUES ($2, $1, 'task_' || $1, 'n_' || $1, 'att_' || $1, 0, $3, '` + sha + `', 1, '{}', '3', '2.0.9')`

// ISO-26: the checkpoints unique key is (attempt_id, kind, seq).
func TestISO26CheckpointKeyIncludesKind(t *testing.T) {
	const c = checkpointKeyContract
	const tenant = "t-rt-ckpt"
	ctx := context.Background()
	opsEnsureTenant(t, tenant)
	owner := newPool(t, ownerURL, 2)
	if err := seedTaskRows(ctx, owner, tenant); err != nil {
		t.Fatalf("seed: %v", err)
	}
	worker := newPool(t, workerURL, 2)
	// Migration 00019 goes down only while no attempt holds two kinds at one seq, and S-DB-6 runs on this database, so
	// the extra rows do not outlive the test.
	t.Cleanup(func() {
		_, err := owner.Exec(ctx, `DELETE FROM checkpoints WHERE tenant_id = $1 AND kind <> 'agent_state'`, tenant)
		if err != nil {
			t.Errorf("remove the extra checkpoint kinds: %v", err)
		}
	})
	rowsOf := func() int {
		return ownerScalar[int](t, owner, `SELECT count(*) FROM checkpoints WHERE tenant_id = $1 AND attempt_id = 'att_' || $1 AND seq = 0`, tenant)
	}

	// FM-90: the seed holds agent_state at seq 0; another kind at the same seq is a different checkpoint.
	for _, kind := range []string{"sop_run_state", "workspace_snapshot", "plan"} {
		n, err := execAsApp(ctx, worker, tenant, insertCheckpointSQL, tenant, "ckpt_"+kind+"_"+tenant, kind)
		iso(t, "ISO-26/other-kind-same-seq-"+kind, c, []string{"FM-90"},
			"orbit_worker stores a "+kind+" checkpoint at the same attempt and seq as an agent_state one",
			sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: insertCheckpointSQL, Args: []any{kind}},
			map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": n, "error": sqlState(err)},
			err == nil && n == 1)
	}
	stored := rowsOf()
	iso(t, "ISO-26/all-kinds-kept", c, []string{"FM-90"},
		"the attempt now holds one row per kind at seq 0",
		sqlReq{Role: "orbit_owner", Tenant: tenant, SQL: "SELECT count(*) FROM checkpoints WHERE attempt_id AND seq = 0"},
		map[string]any{"rows": 4}, map[string]any{"rows": stored}, stored == 4)

	// FM-91: the same kind at the same seq is the same checkpoint.
	_, dupErr := execAsApp(ctx, worker, tenant, insertCheckpointSQL, tenant, "ckpt_dup_"+tenant, "agent_state")
	iso(t, "ISO-26/same-kind-same-seq-rejected", c, []string{"FM-91"},
		"a second agent_state row at the same attempt and seq fails with 23505",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: insertCheckpointSQL, Args: []any{"agent_state"}},
		map[string]any{"sqlstate": "23505"}, map[string]any{"sqlstate": sqlState(dupErr)}, sqlState(dupErr) == "23505")

	onConflict := insertCheckpointSQL + ` ON CONFLICT (attempt_id, kind, seq) DO NOTHING`
	n, idemErr := execAsApp(ctx, worker, tenant, onConflict, tenant, "ckpt_again_"+tenant, "agent_state")
	iso(t, "ISO-26/same-kind-same-seq-idempotent", c, []string{"FM-91"},
		"INSERT … ON CONFLICT (attempt_id, kind, seq) DO NOTHING stores nothing and does not fail",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: onConflict, Args: []any{"agent_state"}},
		map[string]any{"rows": 0, "error": "ok", "stored": 4},
		map[string]any{"rows": n, "error": sqlState(idemErr), "stored": rowsOf()},
		idemErr == nil && n == 0 && rowsOf() == 4)

	// The key is exactly (attempt_id, kind, seq), under its new name.
	const defSQL = `SELECT coalesce(string_agg(conname || ' ' || pg_get_constraintdef(oid), '; ' ORDER BY conname), '')
	                  FROM pg_constraint WHERE conrelid = 'public.checkpoints'::regclass AND contype = 'u'`
	def := ownerScalar[string](t, owner, defSQL)
	const want = "checkpoints_attempt_kind_seq_key UNIQUE (attempt_id, kind, seq)"
	iso(t, "ISO-26/constraint-definition", c, []string{"FM-90", "FM-91"},
		"checkpoints has exactly one unique constraint, checkpoints_attempt_kind_seq_key on (attempt_id, kind, seq)",
		sqlReq{Role: "orbit_owner", SQL: defSQL}, map[string]any{"constraints": want}, map[string]any{"constraints": def}, def == want)
}
