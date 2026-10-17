package schedules

import (
	"context"
	"encoding/json/v2"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/sessions"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type Service struct {
	DB            *db.DB
	Locks         locks.Resolver
	Temporal      client.ScheduleClient
	DefaultLimit  int
	SweepInterval time.Duration
}
type Schedule struct {
	ID                   string     `json:"id"`
	TenantID             string     `json:"tenant_id"`
	WorkspaceID          string     `json:"workspace_id"`
	OwnerID              string     `json:"owner_id"`
	Name                 string     `json:"name"`
	Description          *string    `json:"description,omitempty"`
	Cron                 string     `json:"cron"`
	Timezone             string     `json:"timezone"`
	EntryKind            string     `json:"entry_kind"`
	EntryRef             string     `json:"entry_ref"`
	Prompt               string     `json:"prompt"`
	BudgetMillicredits   int64      `json:"budget_millicredits"`
	OverlapPolicy        string     `json:"overlap_policy"`
	CatchupWindowSeconds int        `json:"catchup_window_seconds"`
	Status               string     `json:"status"`
	PausedReason         *string    `json:"paused_reason,omitempty"`
	Version              int64      `json:"version"`
	SyncedVersion        int64      `json:"synced_version"`
	SyncPending          bool       `json:"sync_pending"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	DeletedAt            *time.Time `json:"deleted_at,omitempty"`
}
type CreateInput struct {
	WorkspaceID          string  `json:"workspace_id"`
	Name                 string  `json:"name"`
	Description          *string `json:"description,omitempty"`
	Cron                 string  `json:"cron"`
	Timezone             string  `json:"timezone"`
	EntryKind            string  `json:"entry_kind,omitempty"`
	EntryRef             string  `json:"entry_ref"`
	Prompt               string  `json:"prompt"`
	BudgetMillicredits   int64   `json:"budget_millicredits"`
	OverlapPolicy        string  `json:"overlap_policy,omitempty"`
	CatchupWindowSeconds int     `json:"catchup_window_seconds,omitempty"`
}
type PatchInput struct {
	Name                 *string `json:"name,omitempty"`
	Description          *string `json:"description,omitempty"`
	Cron                 *string `json:"cron,omitempty"`
	Timezone             *string `json:"timezone,omitempty"`
	Prompt               *string `json:"prompt,omitempty"`
	BudgetMillicredits   *int64  `json:"budget_millicredits,omitempty"`
	OverlapPolicy        *string `json:"overlap_policy,omitempty"`
	CatchupWindowSeconds *int    `json:"catchup_window_seconds,omitempty"`
	Status               *string `json:"status,omitempty"`
}

const scheduleJSON = `to_jsonb(s)||jsonb_build_object('catchup_window_seconds',extract(epoch FROM catchup_window)::int)`
const selectSchedule = `SELECT ` + scheduleJSON + ` FROM schedules s WHERE tenant_id=$1 AND id=$2`

func decodeSchedule(raw []byte) (Schedule, error) {
	var out Schedule
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	out.SyncPending = out.SyncedVersion < out.Version
	return out, nil
}

func get(ctx context.Context, id string) (Schedule, error) {
	var out Schedule
	var raw []byte
	if !ids.Valid(id) {
		return out, httpx.E(404, "SCHEDULE_NOT_FOUND", "Schedule not found")
	}
	if err := db.Tx(ctx).QueryRow(ctx, selectSchedule, auth.Tenant(ctx), id).Scan(&raw); err != nil {
		if err == pgx.ErrNoRows {
			return out, httpx.E(404, "SCHEDULE_NOT_FOUND", "Schedule not found")
		}
		return out, err
	}
	return decodeSchedule(raw)
}
func Get(ctx context.Context, id string) (Schedule, error) {
	out, err := get(ctx, id)
	if err != nil {
		return out, err
	}
	if err = (policy.Authorizer{}).Require(ctx, "workspace.read", resource(out.WorkspaceID)); err != nil {
		return Schedule{}, httpx.E(404, "SCHEDULE_NOT_FOUND", "Schedule not found")
	}
	return out, nil
}
func resource(workspace string) policy.Resource {
	return policy.Resource{Type: "workspace", ID: workspace, WorkspaceID: workspace}
}
func validate(in CreateInput, now time.Time) error {
	if !ids.Valid(in.WorkspaceID) || strings.TrimSpace(in.Prompt) == "" || len(in.Prompt) > 32768 ||
		in.BudgetMillicredits < 1000 || utf8.RuneCountInString(in.Name) < 1 || utf8.RuneCountInString(in.Name) > 128 ||
		(in.Description != nil && utf8.RuneCountInString(*in.Description) > 1024) ||
		(in.OverlapPolicy != "skip" && in.OverlapPolicy != "buffer_one") || in.CatchupWindowSeconds < 60 || in.CatchupWindowSeconds > 86400 {
		return httpx.E(422, "INVALID_INPUT", "Invalid schedule input")
	}
	return ValidateCron(in.Cron, in.Timezone, now)
}

// Create runs in the caller's tenant transaction. Sync must run after commit.
func (s Service) Create(ctx context.Context, id string, in CreateInput) (Schedule, error) {
	if existing, err := get(ctx, id); err == nil {
		return existing, nil
	} else if he, ok := err.(*httpx.Error); !ok || he.Code != "SCHEDULE_NOT_FOUND" {
		return Schedule{}, err
	}
	if in.OverlapPolicy == "" {
		in.OverlapPolicy = "skip"
	}
	if in.CatchupWindowSeconds == 0 {
		in.CatchupWindowSeconds = 3600
	}
	if err := validate(in, time.Now()); err != nil {
		return Schedule{}, err
	}
	if err := (policy.Authorizer{}).Require(ctx, "session.create", resource(in.WorkspaceID)); err != nil {
		return Schedule{}, err
	}
	lock, err := sessions.ResolveEntry(ctx, s.Locks, in.WorkspaceID, in.EntryRef, in.EntryKind)
	if err != nil {
		return Schedule{}, err
	}
	if err = s.quota(ctx, in.WorkspaceID, auth.From(ctx).ID, false); err != nil {
		return Schedule{}, err
	}
	_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO schedules(id,tenant_id,workspace_id,owner_id,name,description,cron,timezone,
 entry_kind,entry_ref,prompt,budget_millicredits,overlap_policy,catchup_window,temporal_schedule_id,status)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14*interval '1 second',$15,'active') ON CONFLICT DO NOTHING`,
		id, auth.Tenant(ctx), in.WorkspaceID, auth.From(ctx).ID, in.Name, in.Description, in.Cron, in.Timezone, lock.Entry.Kind, in.EntryRef,
		in.Prompt, in.BudgetMillicredits, in.OverlapPolicy, in.CatchupWindowSeconds, w.ScheduleID(auth.Tenant(ctx), id))
	if err != nil {
		return Schedule{}, err
	}
	out, err := get(ctx, id)
	if err != nil {
		return out, err
	}
	err = audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), "schedule.created", "schedule", id, map[string]any{})
	return out, err
}
func (s Service) quota(ctx context.Context, workspace, owner string, userOnly bool) error {
	tx, tenant := db.Tx(ctx), auth.Tenant(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,9))`, tenant); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT scope_type,scope_id::text,"limit"::bigint FROM quota_policies
 WHERE tenant_id=$1 AND metric='schedules' AND action='block' AND
 ((scope_type='tenant' AND scope_id=$1) OR (scope_type='workspace' AND scope_id=$2) OR (scope_type='user' AND scope_id=$3))`, tenant,
		workspace, owner)
	if err != nil {
		return err
	}
	type quota struct {
		kind, id string
		limit    int64
	}
	policies := []quota{}
	workspacePolicy := false
	for rows.Next() {
		var q quota
		if err = rows.Scan(&q.kind, &q.id, &q.limit); err != nil {
			rows.Close()
			return err
		}
		policies = append(policies, q)
		workspacePolicy = workspacePolicy || q.kind == "workspace"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !workspacePolicy {
		limit := s.DefaultLimit
		if limit == 0 {
			limit = 20
		}
		policies = append(policies, quota{kind: "workspace", id: workspace, limit: int64(limit)})
	}
	for _, q := range policies {
		if userOnly && q.kind != "user" {
			continue
		}
		var count int64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM schedules WHERE tenant_id=$1 AND status IN ('active','paused')
 AND ($2='tenant' OR ($2='workspace' AND workspace_id=$3) OR ($2='user' AND owner_id=$3))`, tenant, q.kind, q.id).Scan(&count); err != nil {
			return err
		}
		if count >= q.limit {
			return httpx.E(429, "SCHEDULE_QUOTA_EXCEEDED", "Schedule quota exceeded")
		}
	}
	return nil
}
func (s Service) Delete(ctx context.Context, id string) (Schedule, error) {
	out, err := s.Manage(ctx, id)
	if err != nil {
		return out, err
	}
	if out.Status == "deleted" {
		return out, nil
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE schedules SET status='deleted',deleted_at=now(),updated_at=now(),version=version+1
 WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), id); err != nil {
		return out, err
	}
	if err = audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), "schedule.deleted", "schedule", id,
		map[string]any{}); err != nil {
		return out, err
	}
	return get(ctx, id)
}
func (s Service) Manage(ctx context.Context, id string) (Schedule, error) {
	if _, err := db.Tx(ctx).Exec(ctx, `SELECT id FROM schedules WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), id); err != nil {
		return Schedule{}, err
	}
	out, err := Get(ctx, id)
	if err != nil {
		return out, err
	}
	if out.OwnerID != auth.From(ctx).ID {
		if err = (policy.Authorizer{}).Require(ctx, "workspace.members", resource(out.WorkspaceID)); err != nil {
			return out, err
		}
	}
	return out, nil
}
func List(ctx context.Context, workspace string, page httpx.Page) (sessions.Page[Schedule], error) {
	out := sessions.Page[Schedule]{Items: []Schedule{}}
	if workspace != "" && !ids.Valid(workspace) {
		return out, httpx.E(422, "INVALID_INPUT", "Invalid workspace")
	}
	args, err := (policy.Authorizer{}).WorkspaceReadFilter(ctx)
	if err != nil {
		return out, err
	}
	args["tenant"], args["workspace"], args["cursor"], args["limit"] = auth.Tenant(ctx), workspace, page.Cursor, page.Limit+1
	rows, err := db.Tx(ctx).Query(ctx,
		`SELECT `+scheduleJSON+`
 FROM schedules s WHERE tenant_id=@tenant AND status<>'deleted'
 AND (@workspace='' OR workspace_id=NULLIF(@workspace,'')::uuid)
 AND (@cursor='' OR id>NULLIF(@cursor,'')::uuid) AND `+policy.WorkspaceReadPredicate+` ORDER BY id LIMIT @limit`, args)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return out, err
		}
		if len(out.Items) == page.Limit {
			out.NextCursor = httpx.Cursor(out.Items[len(out.Items)-1].ID)
			break
		}
		item, decodeErr := decodeSchedule(raw)
		if decodeErr != nil {
			return out, decodeErr
		}
		out.Items = append(out.Items, item)
	}
	return out, rows.Err()
}
