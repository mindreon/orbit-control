package ctl

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/schedules"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (a *Activities) CreateScheduledSession(ctx context.Context, in w.ScheduledSessionInput) (w.ScheduledStart, error) {
	var out w.ScheduledStart
	pause := false
	err := a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		// Schedule firing identity is unique; lock its row to serialize retries and PATCH.
		var raw []byte
		err := tx.QueryRow(ctx, `SELECT to_jsonb(s)||jsonb_build_object('catchup_window_seconds',extract(epoch FROM catchup_window)::int)
 FROM schedules s WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, in.TenantID, in.ScheduleID).Scan(&raw)
		if errors.Is(err, pgx.ErrNoRows) {
			out.Skip = "deleted"
			return nil
		}
		if err != nil {
			return err
		}
		var schedule schedules.Schedule
		if err = json.Unmarshal(raw, &schedule); err != nil {
			return err
		}
		var oldStatus string
		var oldRun *string
		err = tx.QueryRow(ctx, `SELECT status,run_id::text FROM schedule_runs WHERE tenant_id=$1 AND schedule_id=$2 AND fire_at=$3`,
			in.TenantID, in.ScheduleID, in.FireAt).Scan(&oldStatus, &oldRun)
		if err == nil && oldRun == nil {
			out.Skip = oldStatus
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		existing := err == nil
		if !existing {
			if schedule.Status == "deleted" {
				out.Skip = "deleted"
				return nil
			}
			if schedule.Status == "paused" {
				out.Skip = "inactive"
				return nil
			}
			if in.Now.Sub(in.FireAt) > time.Duration(schedule.CatchupWindowSeconds+120)*time.Second {
				out.Skip = "STALE_FIRE"
				return recordFire(ctx, tx, in, "skipped", "STALE_FIRE", nil, nil)
			}
		}
		var principal auth.Principal
		if err = tx.QueryRow(ctx, `SELECT id::text,kind FROM principals WHERE id=$1`, schedule.OwnerID).Scan(&principal.ID,
			&principal.Kind); err != nil {
			return err
		}
		c := auth.Context(db.Context(ctx, tx), principal, in.TenantID)
		if !existing {
			var active bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships m JOIN principals p ON p.id=m.principal_id
 WHERE m.tenant_id=$1 AND m.principal_id=$2 AND m.status='active' AND p.status='active')`, in.TenantID, principal.ID).Scan(&active); err !=
				nil {
				return err
			}
			if !active || (policy.Authorizer{}).Require(c, "session.create",
				policy.Resource{Type: "workspace", ID: schedule.WorkspaceID, WorkspaceID: schedule.WorkspaceID}) != nil {
				pause = true
				out.Skip = "OWNER_UNAUTHORIZED"
				return failedFire(c, tx, in, out.Skip, true)
			}
		}
		fire := in.FireAt.UTC().Format(time.RFC3339)
		sessionID, runID, messageID := sessions.ScheduledIDs(schedule.ID, fire)
		session := sessions.Session{ID: sessionID, WorkspaceID: schedule.WorkspaceID, OwnerID: principal.ID, EntryKind: schedule.EntryKind,
			EntryRef: schedule.EntryRef, MessagePolicy: "queue", Title: schedule.Name + " @ " + fire}
		// A savepoint lets admission/lock errors roll back business mutations while
		// preserving the classified schedule_runs outcome in the outer transaction.
		business, err := tx.Begin(c)
		if err != nil {
			return err
		}
		defer business.Rollback(context.WithoutCancel(c))
		bc := auth.Context(db.Context(c, business), principal, in.TenantID)
		if !existing {
			_, err = business.Exec(c,
				`INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status,schedule_id)
 VALUES($1,$2,$3,$4,$5,$6,$7,'queue','idle',$8)`, sessionID, in.TenantID, schedule.WorkspaceID, principal.ID, session.Title,
				schedule.EntryKind, schedule.EntryRef, schedule.ID)
			if err != nil {
				return err
			}
			if err = tenancy.Bind(c, business, principal, in.TenantID, principal.Kind, principal.ID, "session.owner", "session",
				sessionID); err != nil {
				return err
			}
		}
		msg, err := a.Sessions.BuildStart(bc, business, session, principal, runID, messageID, schedule.Prompt, &schedule.BudgetMillicredits)
		if err != nil {
			if rollbackErr := business.Rollback(context.WithoutCancel(c)); rollbackErr != nil {
				return rollbackErr
			}
			var httpErr *httpx.Error
			if !errors.As(err, &httpErr) {
				return err
			}
			code := "QUOTA_EXCEEDED"
			if httpErr.Code == "LOCK_RESOLUTION_FAILED" {
				code = "LOCK_RESOLUTION_FAILED"
				pause = true
			} else if httpErr.Status != 429 {
				return err
			}
			out.Skip = code
			return failedFire(c, tx, in, code, pause)
		}
		if _, err = business.Exec(c, `UPDATE runs SET trigger='schedule' WHERE tenant_id=$1 AND id=$2`, in.TenantID, runID); err != nil {
			return err
		}
		if err = business.Commit(c); err != nil {
			return err
		}
		if !existing {
			if err = recordFire(c, tx, in, "running", "", &runID, &sessionID); err != nil {
				return err
			}
		}
		si := w.SessionInput{V: 1, TenantID: in.TenantID, WorkspaceID: schedule.WorkspaceID, SessionID: sessionID, OwnerID: principal.ID,
			Policy: "queue"}
		subject := w.EventSubject(in.TenantID, sessionID, runID)
		out.Kind = schedule.EntryKind
		if schedule.EntryKind == "team" {
			run := w.NewTeamRunInput(si, msg, subject)
			run.Unattended = true
			out.Team = &run
		} else {
			run := w.NewRunInput(si, msg, subject)
			run.Unattended = true
			out.Run = &run
		}
		return nil
	})
	if err == nil && pause && a.Schedules != nil {
		_, _ = a.Schedules.Sync(ctx, in.TenantID, in.ScheduleID)
	}
	return out, err
}
func recordFire(ctx context.Context, tx pgx.Tx, in w.ScheduledSessionInput, status, code string, run, session *string) error {
	_, err := tx.Exec(ctx, `INSERT INTO schedule_runs(tenant_id,schedule_id,fire_at,run_id,session_id,status,error_code,ended_at)
 VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),CASE WHEN $6='running' THEN NULL ELSE $8::timestamptz END) ON CONFLICT DO NOTHING`,
		in.TenantID, in.ScheduleID, in.FireAt, run, session, status, code, in.Now)
	return err
}
func failedFire(ctx context.Context, tx pgx.Tx, in w.ScheduledSessionInput, code string, pause bool) error {
	if err := recordFire(ctx, tx, in, "failed", code, nil, nil); err != nil {
		return err
	}
	if pause {
		_, err := tx.Exec(ctx, `UPDATE schedules SET status='paused',paused_reason=$3,version=version+1,updated_at=now()
 WHERE tenant_id=$1 AND id=$2`, in.TenantID, in.ScheduleID, code)
		return err
	}
	return nil
}
func (a *Activities) RecordScheduleRun(ctx context.Context, in w.RecordScheduleInput) error {
	return a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE schedule_runs sr SET status=r.status,error_code=r.error_code,ended_at=r.ended_at
 FROM runs r WHERE sr.tenant_id=$1 AND sr.schedule_id=$2 AND sr.fire_at=$3 AND r.tenant_id=sr.tenant_id
 AND r.id=sr.run_id AND r.ended_at IS NOT NULL`, in.TenantID, in.ScheduleID, in.FireAt)
		return err
	})
}
