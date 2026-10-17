package ctl

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func teamRunStatus(paused, approval, input bool) string {
	if paused {
		return "paused"
	}
	if approval {
		return "waiting_approval"
	}
	if input {
		return "waiting_input"
	}
	return "running"
}

// The caller holds change's run-level transaction lock. Returning the status
// event lets change assign it a replay-stable operation identity atomically.
func refreshTeamRunStatus(ctx context.Context, tx pgx.Tx, in w.RunOp) ([]event, error) {
	var paused, ended bool
	var status, next string
	err := tx.QueryRow(ctx, `SELECT t.paused,r.status,r.ended_at IS NOT NULL,`+
		team.WaitingStatusSQL("r.tenant_id", "r.id", "", "'running'")+`
 FROM runs r JOIN team_runs t ON t.tenant_id=r.tenant_id AND t.run_id=r.id
 WHERE r.tenant_id=$1 AND r.id=$2`, in.TenantID, in.RunID).Scan(&paused, &status, &ended, &next)
	if err != nil {
		return nil, err
	}
	if ended || status == "queued" {
		return nil, nil
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
