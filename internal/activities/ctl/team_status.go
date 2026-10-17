package ctl

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// The caller holds change's run-level transaction lock. Returning the status
// event lets change assign it a replay-stable operation identity atomically.
func refreshTeamRunStatus(ctx context.Context, tx pgx.Tx, in w.RunOp) ([]event, error) {
	var paused, ended, approval, input bool
	var status string
	err := tx.QueryRow(ctx, `SELECT t.paused,r.status,r.ended_at IS NOT NULL,
 EXISTS(SELECT 1 FROM interactions i WHERE i.tenant_id=r.tenant_id AND i.run_id=r.id
  AND i.status IN ('pending','escalated') AND i.kind<>'ask_user'),
 EXISTS(SELECT 1 FROM interactions i WHERE i.tenant_id=r.tenant_id AND i.run_id=r.id
  AND i.status IN ('pending','escalated') AND i.kind='ask_user')
 FROM runs r JOIN team_runs t ON t.tenant_id=r.tenant_id AND t.run_id=r.id
 WHERE r.tenant_id=$1 AND r.id=$2`, in.TenantID, in.RunID).
		Scan(&paused, &status, &ended, &approval, &input)
	if err != nil {
		return nil, err
	}
	if ended || status == "queued" {
		return nil, nil
	}
	next := "running"
	if input {
		next = "waiting_input"
	}
	if approval {
		next = "waiting_approval"
	}
	if paused {
		next = "paused"
	}
	if next == status {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE runs SET status=$3 WHERE tenant_id=$1 AND id=$2`, in.TenantID, in.RunID, next); err != nil {
		return nil, err
	}
	return []event{{typ: "run.status", data: contract.RunStatus{Status: contract.RunState(next), Reason: new("team_state")}}}, nil
}
