// Package sessions owns tenant session and run persistence without orchestration.
package sessions

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

type Session struct {
	ID             string    `json:"id"`
	WorkspaceID    string    `json:"workspace_id"`
	ProjectID      *string   `json:"project_id,omitempty"`
	OwnerID        string    `json:"owner_id"`
	Title          string    `json:"title"`
	EntryKind      string    `json:"entry_kind"`
	EntryRef       string    `json:"entry_ref"`
	MessagePolicy  string    `json:"message_policy"`
	Status         string    `json:"status"`
	PinnedLockID   *string   `json:"pinned_lock_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	LastActivityAt time.Time `json:"last_activity_at"`
}
type Run struct {
	TemporalWorkflowID   string     `json:"-"`
	TraceID              *string    `json:"-"`
	Ops                  *RunOps    `json:"ops,omitempty"`
	ID                   string     `json:"id"`
	SessionID            string     `json:"session_id"`
	Status               string     `json:"status"`
	Trigger              string     `json:"trigger"`
	CreatedBy            string     `json:"created_by"`
	MessageID            string     `json:"message_id"`
	LockID               *string    `json:"lock_id,omitempty"`
	BudgetMillicredits   int64      `json:"budget_millicredits"`
	UsedMillicredits     int64      `json:"used_millicredits"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	EndedAt              *time.Time `json:"ended_at,omitempty"`
	ErrorCode            *string    `json:"error_code,omitempty"`
	Summary              *string    `json:"summary,omitempty"`
	PendingInteractionID *string    `json:"pending_interaction_id,omitempty"`
	SegmentsCount        int        `json:"segments_count"`
}

func Get(ctx context.Context, id, action string) (Session, error) {
	var s Session
	if !ids.Valid(id) {
		return s, httpx.E(404, "NOT_FOUND", "Session not found")
	}
	var raw []byte
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT to_jsonb(s) FROM sessions s WHERE tenant_id=$1 AND id=$2`,
		auth.Tenant(ctx), id).Scan(&raw); err != nil {
		return s, err
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, err
	}
	resource := policy.Resource{Type: "session", ID: id, WorkspaceID: s.WorkspaceID}
	a := policy.Authorizer{}
	if err := a.Require(ctx, "session.read", resource); err != nil {
		if e, ok := errors.AsType[*httpx.Error](err); ok && e.Status == 403 {
			return s, httpx.E(404, "NOT_FOUND", "Session not found")
		}
		return s, err
	}
	return s, a.Require(ctx, action, resource)
}
func GetRun(ctx context.Context, id, action string) (Run, Session, error) {
	var r Run
	var s Session
	if !ids.Valid(id) {
		return r, s, httpx.E(404, "NOT_FOUND", "Run not found")
	}
	var raw []byte
	err := db.Tx(ctx).QueryRow(ctx, `SELECT to_jsonb(r)||jsonb_build_object('pending_interaction_id',(SELECT i.id
     FROM interactions i
     WHERE i.tenant_id=r.tenant_id
     AND i.run_id=r.id
     AND i.status IN ('pending','escalated')
     ORDER BY i.created_at
     LIMIT 1),'segments_count',(SELECT count(*)
     FROM run_segments g
     WHERE g.tenant_id=r.tenant_id
     AND g.run_id=r.id)),COALESCE(r.temporal_workflow_id,''),r.trace_id
 FROM runs r
 WHERE tenant_id=$1
 AND id=$2`, auth.Tenant(ctx), id).Scan(&raw, &r.TemporalWorkflowID, &r.TraceID)
	if err != nil {
		return r, s, err
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, s, err
	}
	s, err = Get(ctx, r.SessionID, action)
	return r, s, err
}

// RejectQueued releases admission when the Session validator rejects delivery.
func RejectQueued(ctx context.Context, runID string) error {
	tx := db.Tx(ctx)
	n, err := tx.Exec(ctx, `UPDATE runs
 SET status='failed',error_code='SESSION_UNAVAILABLE',ended_at=now()
 WHERE tenant_id=$1
 AND id=$2
 AND status='queued'`, auth.Tenant(ctx), runID)
	if err != nil || n.RowsAffected() == 0 {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE budget_reservations
 SET status='released',millicredits=0
 WHERE tenant_id=$1
 AND run_id=$2
 AND status='held'`, auth.Tenant(ctx), runID); err != nil {
		return err
	}
	return logWrite(ctx, "run.finish", "run", runID, map[string]string{"status": "failed", "error_code": "SESSION_UNAVAILABLE"})
}
