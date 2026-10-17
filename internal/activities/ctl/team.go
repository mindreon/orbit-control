package ctl

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (a *Activities) PrepareTeamRun(ctx context.Context, in w.PrepareTeamInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		if err := prepareRun(ctx, tx, in.RunOp, in.Budget); err != nil {
			return nil, err
		}
		board, err := json.Marshal(in.Board)
		if err != nil {
			return nil, err
		}
		roster, err := json.Marshal(in.Team.Roster)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO team_runs(tenant_id,run_id,board,max_parallel,review_max_rounds,roster,dispatch_mode)
 VALUES($1,$2,$3,$4,$5,$6,'scheduled') ON CONFLICT DO NOTHING`, in.TenantID, in.RunID, board, in.Team.MaxParallel, in.Team.ReviewMaxRounds, roster); err != nil {
			return nil, err
		}
		for _, member := range in.Team.Roster {
			if _, err = tx.Exec(ctx, `INSERT INTO team_members(tenant_id,run_id,name,role,status)
 VALUES($1,$2,$3,$4,'idle') ON CONFLICT DO NOTHING`, in.TenantID, in.RunID, member.Name, member.Role); err != nil {
				return nil, err
			}
		}
		var lock string
		if err = tx.QueryRow(ctx, `SELECT lock_id::text FROM runs WHERE tenant_id=$1 AND id=$2`, in.TenantID, in.RunID).Scan(&lock); err != nil {
			return nil, err
		}
		return []event{{typ: "run.started", data: contract.RunStarted{EntryKind: "team", LockId: lock}}}, nil
	})
}
func prepareRun(ctx context.Context, tx pgx.Tx, in w.RunOp, budget int64) error {
	if _, err := tx.Exec(ctx, `UPDATE runs SET status='running',started_at=COALESCE(started_at,now())
 WHERE tenant_id=$1 AND id=$2 AND status IN ('queued','running')`, in.TenantID, in.RunID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO budget_reservations(id,tenant_id,run_id,millicredits,status,expires_at)
 VALUES($1,$2,$3,$4,'held',now()+interval '7 days') ON CONFLICT DO NOTHING`, ids.Derive(in.RunID, "reservation"), in.TenantID, in.RunID, budget)
	return err
}
func (a *Activities) RecordBoard(ctx context.Context, in w.RecordBoardInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		raw, err := json.Marshal(in.Board)
		if err != nil {
			return nil, err
		}
		n, err := tx.Exec(ctx, `UPDATE team_runs SET board=$3,board_version=$4 WHERE tenant_id=$1 AND run_id=$2 AND board_version<$4`,
			in.TenantID, in.RunID, raw, in.Version)
		if err != nil {
			return nil, err
		}
		if n.RowsAffected() == 0 {
			return nil, nil
		}
		for _, task := range in.Board.Tasks {
			deps, err := json.Marshal(task.Deps)
			if err != nil {
				return nil, err
			}
			review, err := json.Marshal(task.Review)
			if err != nil {
				return nil, err
			}
			tools, err := json.Marshal(task.NarrowedTools)
			if err != nil {
				return nil, err
			}
			files, err := json.Marshal(task.Files)
			if err != nil {
				return nil, err
			}
			_, err = tx.Exec(ctx, `INSERT INTO team_tasks(tenant_id,run_id,task_id,title,description,deps,assignee,status,
 attempts,failures,review,narrowed_tools,result_text,files)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
 ON CONFLICT(tenant_id,run_id,task_id) DO UPDATE SET title=EXCLUDED.title,description=EXCLUDED.description,
 deps=EXCLUDED.deps,assignee=EXCLUDED.assignee,status=EXCLUDED.status,attempts=EXCLUDED.attempts,
 failures=EXCLUDED.failures,review=EXCLUDED.review,narrowed_tools=EXCLUDED.narrowed_tools,
 result_text=EXCLUDED.result_text,files=EXCLUDED.files,updated_at=now()`, in.TenantID, in.RunID, task.TaskId, task.Title,
				task.Description, deps, task.Assignee, task.Status, task.Attempts, value(task.Failures), review, tools, task.ResultText, files)
			if err != nil {
				return nil, err
			}
		}
		for _, turn := range in.Turns {
			_, err = tx.Exec(ctx, `INSERT INTO team_turns(tenant_id,run_id,member,turn,kind,task_id,status,code,started_at,ended_at)
 VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,NULLIF($8,''),$9,$10)
 ON CONFLICT(tenant_id,run_id,member,turn) DO UPDATE SET status=EXCLUDED.status,code=EXCLUDED.code,
 ended_at=EXCLUDED.ended_at`, in.TenantID, in.RunID, turn.Member, turn.Turn, turn.Kind, turn.TaskID, turn.Status, turn.Code,
				nonzero(turn.StartedAt), turn.EndedAt)
			if err != nil {
				return nil, err
			}
			status := "idle"
			taskID := ""
			if turn.Status == "running" {
				status = "working"
				taskID = turn.TaskID
			}
			_, err = tx.Exec(ctx, `UPDATE team_members SET status=$3,task_id=NULLIF($4,''),turn=$5
 WHERE tenant_id=$1 AND run_id=$2 AND name=$6 AND turn<=$5`, in.TenantID, in.RunID, status, taskID, turn.Turn, turn.Member)
			if err != nil {
				return nil, err
			}
		}
		events := make([]event, 0, len(in.Events))
		for _, ev := range in.Events {
			events = append(events, event{typ: ev.Type, data: ev.Data})
		}
		return events, nil
	})
}
func value(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
func nonzero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (a *Activities) WaitForTurn(ctx context.Context, in w.TurnOp) (w.TurnResult, error) {
	type outcome struct {
		result w.TurnResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		var out w.TurnResult
		err := a.Temporal.GetWorkflow(ctx, w.MemberTaskWorkflowID(in.TenantID, in.RunID, in.Member, in.Turn), "").Get(ctx, &out)
		done <- outcome{result: out, err: err}
	}()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case result := <-done:
			if _, closed := errors.AsType[*temporal.WorkflowExecutionError](result.err); closed {
				return w.TurnResult{Member: in.Member, Turn: in.Turn, Status: "failed", Code: "TURN_LOST"}, nil
			}
			return result.result, result.err
		case <-ticker.C:
			activity.RecordHeartbeat(ctx)
		case <-ctx.Done():
			return w.TurnResult{}, ctx.Err()
		}
	}
}
func (a *Activities) FinalizeTeamRun(ctx context.Context, in w.FinalizeTeamInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		var used int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(millicredits),0)::bigint FROM usage_ledger WHERE tenant_id=$1 AND run_id=$2`,
			in.TenantID, in.RunID).Scan(&used); err != nil {
			return nil, err
		}
		n, err := tx.Exec(ctx, `UPDATE runs SET status=$3,error_code=NULLIF($4,''),summary=NULLIF($5,''),ended_at=now(),used_millicredits=$6
 WHERE tenant_id=$1 AND id=$2 AND ended_at IS NULL`, in.TenantID, in.RunID, in.Status, in.Code, in.Summary, used)
		if err != nil {
			return nil, err
		}
		if n.RowsAffected() == 0 {
			return nil, nil
		}
		if err = settleRun(ctx, tx, w.FinalizeInput{RunOp: in.RunOp}, used); err != nil {
			return nil, err
		}
		board, err := json.Marshal(in.Board)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE team_runs SET board=$3,finished=true,summary=NULLIF($4,'') WHERE tenant_id=$1 AND run_id=$2`,
			in.TenantID, in.RunID, board, in.Summary); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE team_tasks SET status='cancelled',updated_at=now()
 WHERE tenant_id=$1 AND run_id=$2 AND status IN ('pending','blocked','in_progress','in_review')`, in.TenantID, in.RunID); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE team_members SET status='finished' WHERE tenant_id=$1 AND run_id=$2`, in.TenantID, in.RunID); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE sessions SET last_activity_at=now() WHERE tenant_id=$1 AND id=$2`, in.TenantID, in.SessionID); err != nil {
			return nil, err
		}
		var messageID *string
		if in.Summary != "" {
			messageID = new(ids.Derive(in.RunID, "team-summary"))
			raw, err := json.Marshal(map[string]string{"text": in.Summary})
			if err != nil {
				return nil, err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO messages(id,tenant_id,session_id,run_id,role,content)
 VALUES($1,$2,$3,$4,'assistant',$5) ON CONFLICT DO NOTHING`, messageID, in.TenantID, in.SessionID, in.RunID, raw); err != nil {
				return nil, err
			}
		}
		if err = auditRun(ctx, tx, in.RunOp, "run.finish", in.RunID, map[string]any{"status": in.Status, "error_code": in.Code}); err != nil {
			return nil, err
		}
		if in.Status == "failed" {
			return []event{{typ: "run.failed", data: contract.RunFailed{Code: in.Code, Message: "Team run failed"}}}, nil
		}
		return []event{{typ: "run.completed", data: contract.RunCompleted{Status: contract.RunCompletedStatus(in.Status), FinalMessageId: messageID}}}, nil
	})
}
