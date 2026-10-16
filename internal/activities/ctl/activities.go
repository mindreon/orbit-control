package ctl

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type Activities struct {
	Logger          *slog.Logger
	SandboxProvider contract.SandboxProvider
	DB              *db.DB
	Obj             objstore.ObjectStore
	NATS            natsx.Publisher
	Temporal        client.Client
	Environment     string
}
type event struct {
	typ  string
	data any
}

func validate(name string, value any) error {
	raw, err := json.Marshal(value)
	if err == nil {
		err = contract.Validate(name, raw)
	}
	if err != nil {
		return temporal.NewNonRetryableApplicationError("invalid "+name, contract.ErrorInvalidSpec, nil)
	}
	return nil
}
func (a *Activities) change(ctx context.Context, in w.RunOp, fn func(context.Context, pgx.Tx) ([]event, error)) error {
	var messages []contract.RunEvent
	err := a.DB.WithTenant(
		ctx,
		in.TenantID,
		func(tx pgx.Tx) error {
			// Serialize operation replay with its business mutation, including lost responses.
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,6))`, in.TenantID+in.RunID); err !=
				nil {
				return err
			}
			c := db.Context(ctx, tx)
			evs, err := fn(c, tx)
			if err != nil {
				return err
			}
			if err := persistEvents(ctx, tx, in, evs); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT payload
 FROM control_events
 WHERE tenant_id=$1
 AND run_id=$2
 AND operation=$3
 AND published_at IS NULL
 ORDER BY event_id`, in.TenantID, in.RunID, in.EventN)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var raw []byte
				var ev contract.RunEvent
				if err = rows.Scan(&raw); err != nil {
					return err
				}
				if err = json.Unmarshal(raw, &ev); err != nil {
					return err
				}
				messages = append(messages, ev)
			}
			return rows.Err()
		},
	)
	if err != nil {
		return err
	}
	for _, m := range messages {
		raw, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if a.NATS != nil {
			if err = a.NATS.Publish(ctx, w.EventSubject(in.TenantID, in.SessionID, in.RunID), m.Id, raw); err != nil {
				a.logOutboxFailure(ctx, "control event publish failed", in.RunID, m.Id, in.EventN, err)
				break
			}
			if err = a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE control_events SET published_at=now()
 WHERE tenant_id=$1 AND run_id=$2 AND event_id=$3 AND published_at IS NULL`, in.TenantID, in.RunID, m.Id)
				return err
			}); err != nil {
				a.logOutboxFailure(ctx, "control event publish marker failed", in.RunID, m.Id, in.EventN, err)
			}
		}
	}
	return nil
}
func (a *Activities) logOutboxFailure(ctx context.Context, message, runID, eventID string, operation int64, err error) {
	logger := a.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.WarnContext(ctx, message, "run_id", runID, "event_id", eventID, "operation", operation, "error_type", fmt.Sprintf("%T", err))
}
func runActor(ctx context.Context, tx pgx.Tx, in w.RunOp) (auth.Principal, error) {
	var p auth.Principal
	err := tx.QueryRow(ctx, `SELECT p.id::text,p.kind
 FROM runs r
 JOIN principals p ON p.id=r.created_by
 WHERE r.tenant_id=$1
 AND r.id=$2`, in.TenantID, in.RunID).Scan(&p.ID, &p.Kind)
	return p, err
}
func auditRun(ctx context.Context, tx pgx.Tx, in w.RunOp, action, target string, detail any) error {
	p, err := runActor(ctx, tx, in)
	if err != nil {
		return err
	}
	return audit.Append(ctx, tx, p, in.TenantID, action, "run", target, detail)
}
func (a *Activities) PrepareRun(ctx context.Context, in w.PrepareInput) (w.PrepareOutput, error) {
	var out w.PrepareOutput
	err := a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		var lock string
		var kind string
		if err := tx.QueryRow(ctx, `SELECT r.lock_id::text,s.entry_kind
 FROM runs r
 JOIN sessions s ON s.id=r.session_id
 WHERE r.tenant_id=$1
 AND r.id=$2`, in.TenantID, in.RunID).Scan(&lock, &kind); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs
 SET status='running',started_at=COALESCE(started_at,now())
 WHERE tenant_id=$1
 AND id=$2
 AND status IN ('queued','running')`, in.TenantID, in.RunID); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO budget_reservations(id,tenant_id,run_id,millicredits,status,expires_at)
 VALUES($1,$2,$3,$4,'held',now()+interval '7 days')
 ON CONFLICT DO NOTHING`, ids.Derive(in.RunID, "reservation"), in.TenantID, in.RunID, in.Budget); err != nil {
			return nil, err
		}
		if err := tx.QueryRow(ctx, `SELECT COALESCE(max(version),0)
 FROM agent_checkpoints
 WHERE tenant_id=$1
 AND session_key=$2`, in.TenantID, in.SessionKey).Scan(&out.CheckpointVersion); err != nil {
			return nil, err
		}
		if err := tx.QueryRow(ctx, `SELECT started_at FROM runs WHERE tenant_id=$1 AND id=$2`,
			in.TenantID, in.RunID).Scan(&out.StartedAt); err != nil {
			return nil, err
		}
		return []event{
			{typ: "run.started", data: contract.RunStarted{EntryKind: contract.EntryKind(kind), LockId: lock}},
		}, nil
	})
	return out, err
}
func (a *Activities) StartSegment(ctx context.Context, in w.StartSegmentInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		_, err := tx.Exec(ctx, `INSERT INTO run_segments(tenant_id,run_id,member,seq,status,checkpoint_version)
 VALUES($1,$2,$3,$4,'running',$5)
 ON CONFLICT(tenant_id,run_id,member,seq)
 DO UPDATE SET started_at=now()
 WHERE run_segments.status='running'`, in.TenantID, in.RunID, in.Member, in.Seq, in.ExpectedVersion)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `UPDATE runs SET status='running' WHERE tenant_id=$1 AND id=$2 AND ended_at IS NULL`, in.TenantID, in.RunID)
		return nil, err
	})
}
func (a *Activities) RecordSegment(ctx context.Context, in w.RecordSegmentInput) (w.RecordSegmentOutput, error) {
	var out w.RecordSegmentOutput
	if err := validate("SegmentResult", in.Result); err != nil {
		return out, err
	}
	err := a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		if err := validateSegmentIdentity(ctx, tx, in); err != nil {
			return nil, err
		}
		usage, err := json.Marshal(in.Result.Usage)
		if err != nil {
			return nil, err
		}
		n, err := tx.Exec(ctx, `UPDATE run_segments
 SET status=$5,checkpoint_version=$6,iterations=$7,usage=$8,ended_at=now()
 WHERE tenant_id=$1
 AND run_id=$2
 AND member=$3
 AND seq=$4
 AND status='running'`, in.TenantID, in.RunID, in.Member, in.Seq, in.Result.Status,
			in.Result.CheckpointVersion, in.Result.Progress.Iterations, usage)
		if err != nil {
			return nil, err
		}
		evs, err := recordOutput(ctx, tx, in, n.RowsAffected() > 0)
		if err != nil {
			return nil, err
		}
		if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(millicredits),0)::bigint
 FROM usage_ledger
 WHERE tenant_id=$1
 AND run_id=$2`, in.TenantID, in.RunID).Scan(&out.RunUsedMillicredits); err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `UPDATE runs SET used_millicredits=$3 WHERE tenant_id=$1 AND id=$2`, in.TenantID,
			in.RunID, out.RunUsedMillicredits)
		return evs, err
	})
	return out, err
}
func (a *Activities) SetRunStatus(ctx context.Context, in w.StatusInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		_, err := tx.Exec(ctx, `UPDATE runs SET status=$3 WHERE tenant_id=$1 AND id=$2 AND ended_at IS NULL`,
			in.TenantID, in.RunID, in.Status)
		return []event{
			{
				typ:  "run.status",
				data: contract.RunStatus{Status: contract.RunState(in.Status), Reason: new(in.Reason)},
			},
		}, err
	})
}
func (a *Activities) ReconcileCheckpoint(ctx context.Context, in w.ReconcileInput) (w.ReconcileOutput, error) {
	var out w.ReconcileOutput
	err := a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		var raw []byte
		err := tx.QueryRow(ctx, `SELECT result
 FROM segment_results
 WHERE tenant_id=$1
 AND run_id=$2
 AND member=$3
 AND seq=$4`, in.TenantID, in.RunID, in.Member, in.Seq).Scan(&raw)
		if err == nil {
			var result contract.SegmentResult
			if err = json.Unmarshal(raw, &result); err != nil {
				return err
			}
			out.Result = &result
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return tx.QueryRow(ctx, `SELECT COALESCE(max(version),0)
 FROM agent_checkpoints
 WHERE tenant_id=$1
 AND session_key=$2`, in.TenantID, in.SessionKey).Scan(&out.LatestVersion)
	})
	return out, err
}
func (a *Activities) FinalizeRun(ctx context.Context, in w.FinalizeInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		var used int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(millicredits),0)::bigint
 FROM usage_ledger
 WHERE tenant_id=$1
 AND run_id=$2`, in.TenantID, in.RunID).Scan(&used); err != nil {
			return nil, err
		}
		n, err := tx.Exec(ctx, `UPDATE runs
 SET status=$3,error_code=NULLIF($4,''),summary=NULLIF($5,''),ended_at=now(),used_millicredits=$6
 WHERE tenant_id=$1
 AND id=$2
 AND ended_at IS NULL`, in.TenantID, in.RunID, in.Status, in.ErrorCode, in.Reason, used)
		if err != nil {
			return nil, err
		}
		if n.RowsAffected() == 0 {
			return nil, nil
		}
		if err = settleRun(ctx, tx, in, used); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE sessions SET last_activity_at=now() WHERE tenant_id=$1 AND id=$2`,
			in.TenantID, in.SessionID); err != nil {
			return nil, err
		}
		if err = auditRun(
			ctx, tx, in.RunOp, "run.finish", in.RunID, map[string]any{"status": in.Status, "error_code": in.ErrorCode},
		); err != nil {
			return nil, err
		}
		if in.Status == "failed" {
			return []event{
				{typ: "run.failed", data: contract.RunFailed{Code: in.ErrorCode, Message: "Run failed"}},
			}, nil
		}
		return []event{
			{typ: "run.completed", data: contract.RunCompleted{Status: contract.RunCompletedStatus(in.Status)}},
		}, nil
	})
}

func (a *Activities) WaitForRun(ctx context.Context, in w.RunOp) error {
	done := make(chan error, 1)
	go func() {
		done <- a.Temporal.GetWorkflow(ctx, w.AgentRunWorkflowID(in.TenantID, in.RunID), "").Get(ctx, nil)
	}()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			// A closed workflow has finished even when its business outcome failed.
			if _, closed := errors.AsType[*temporal.WorkflowExecutionError](err); closed {
				return nil
			}
			return err
		case <-ticker.C:
			activity.RecordHeartbeat(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func persistEvents(ctx context.Context, tx pgx.Tx, in w.RunOp, evs []event) error {
	for i, e := range evs {
		raw, err := json.Marshal(e.data)
		if err != nil {
			return err
		}
		var data contract.RunEventData
		if err = json.Unmarshal(raw, &data); err != nil {
			return err
		}
		at := in.EventAt
		if at.IsZero() {
			at = time.Now().UTC()
		}
		msg := contract.RunEvent{
			V:         1,
			Id:        fmt.Sprintf("%s:main:%d:0:%d", in.RunID, in.Seq, in.EventN*1000+int64(i)+1),
			TenantId:  in.TenantID,
			SessionId: in.SessionID,
			RunId:     in.RunID,
			Member:    "main",
			Segment:   in.Seq,
			Attempt:   0,
			Ts:        at,
			Type:      contract.EventType(e.typ),
			Data:      data,
		}
		raw, err = json.Marshal(msg)
		if err != nil {
			return err
		}
		if err = contract.Validate("RunEvent", raw); err != nil {
			return fmt.Errorf("control event %s: %w", e.typ, err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload)
 VALUES($1,$2,$3,$4,$5)
 ON CONFLICT DO NOTHING`, in.TenantID, in.RunID, msg.Id, in.EventN, raw); err != nil {
			return err
		}
	}
	return nil
}

func recordOutput(ctx context.Context, tx pgx.Tx, in w.RecordSegmentInput, changed bool) ([]event, error) {
	var err error
	evs := []event{}
	if changed {
		if f := in.Result.Final; f != nil {
			content, err := json.Marshal(map[string]any{"text": f.TextPreview, "truncated": f.ContentRef != nil})
			if err != nil {
				return nil, err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO messages(id,tenant_id,session_id,run_id,role,content,content_ref)
 VALUES($1,$2,$3,$4,'assistant',$5,$6)
 ON CONFLICT DO NOTHING`, f.MessageId, in.TenantID, in.SessionID, in.RunID, content, f.ContentRef); err != nil {
				return nil, err
			}
		}
		for _, artifact := range in.Result.Artifacts {
			if !strings.HasPrefix(artifact.ObjectRef, "t/"+in.TenantID+"/artifacts/"+in.RunID+"/") {
				return nil, temporal.NewNonRetryableApplicationError("invalid artifact reference", contract.ErrorInvalidSpec, nil)
			}
			var old string
			err = tx.QueryRow(ctx, `SELECT digest FROM artifacts WHERE tenant_id=$1 AND session_id=$2 AND path=$3`,
				in.TenantID, in.SessionID, artifact.Path).Scan(&old)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
			_, err = tx.Exec(ctx,
				`INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'final')
 ON CONFLICT(tenant_id,session_id,path)
 DO UPDATE SET run_id=EXCLUDED.run_id,mime=EXCLUDED.mime,size=EXCLUDED.size,digest=EXCLUDED.digest,
 object_ref=EXCLUDED.object_ref,updated_at=now()
 WHERE artifacts.digest<>EXCLUDED.digest`, ids.Derive(in.SessionID, artifact.Path), in.TenantID, in.SessionID,
				in.RunID, artifact.Path, artifact.Mime, artifact.SizeBytes, artifact.Digest, artifact.ObjectRef)
			if err != nil {
				return nil, err
			}
			if old != artifact.Digest {
				typ := "artifact.created"
				if old != "" {
					typ = "artifact.updated"
				}
				evs = append(evs, event{typ: typ, data: map[string]any{"artifact": artifact}})
			}
		}
	}
	return evs, nil
}

func settleRun(ctx context.Context, tx pgx.Tx, in w.FinalizeInput, used int64) error {
	var err error
	for _, sql := range []string{
		`UPDATE run_segments
 SET status='failed',ended_at=now()
 WHERE tenant_id=$1
 AND run_id=$2
 AND status='running'`,
		`UPDATE interactions
 SET status='cancelled',resolved_at=now()
 WHERE tenant_id=$1
 AND run_id=$2
 AND status IN ('pending','escalated')`,
	} {
		if _, err = tx.Exec(ctx, sql, in.TenantID, in.RunID); err != nil {
			return err
		}
	}
	reservation := "settled"
	if in.NeverStarted {
		reservation = "released"
	}
	if _, err = tx.Exec(ctx, `UPDATE budget_reservations
 SET status=$3,millicredits=$4
 WHERE tenant_id=$1
 AND run_id=$2`, in.TenantID, in.RunID, reservation, used); err != nil {
		return err
	}
	return nil
}
