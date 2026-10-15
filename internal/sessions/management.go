package sessions

import (
	"context"
	"encoding/json/v2"
	"strings"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type CreateInput struct {
	WorkspaceID   string  `json:"workspace_id"`
	EntryRef      string  `json:"entry_ref"`
	Title         string  `json:"title"`
	MessagePolicy string  `json:"message_policy"`
	ProjectID     *string `json:"project_id"`
	Pinned        bool    `json:"pinned"`
}
type PatchInput struct {
	Title  *string `json:"title"`
	Policy *string `json:"message_policy"`
}

func ValidPolicy(value string) bool {
	return value == "queue" || value == "steer" || value == "interrupt"
}
func (svc Service) Create(ctx context.Context, in CreateInput) (Session, error) {
	if in.MessagePolicy == "" {
		in.MessagePolicy = "queue"
	}
	if !ids.Valid(in.WorkspaceID) || !ValidPolicy(in.MessagePolicy) || strings.TrimSpace(in.EntryRef) == "" ||
		len(in.Title) > 256 {
		return Session{}, httpx.E(422, "INVALID_INPUT", "Invalid session input")
	}
	resource := policy.Resource{Type: "workspace", ID: in.WorkspaceID, WorkspaceID: in.WorkspaceID}
	if err := (policy.Authorizer{}).Require(ctx, "session.create", resource); err != nil {
		return Session{}, err
	}
	_, err := svc.Locks.Resolve(ctx, locks.Request{TenantID: auth.Tenant(ctx), LockID: ids.New(),
		Entry: locks.Entry{Kind: "agent", Ref: in.EntryRef}, Scope: locks.Scope{WorkspaceID: in.WorkspaceID}})
	if err != nil {
		return Session{}, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Agent cannot be resolved")
	}
	id, tx := ids.New(), db.Tx(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,project_id,owner_id,title,entry_kind,
 entry_ref,message_policy,status,pinned)
 VALUES($1,$2,$3,$4,$5,$6,'agent',$7,$8,'idle',$9)`, id, auth.Tenant(ctx), in.WorkspaceID, in.ProjectID,
		auth.From(ctx).ID, in.Title, in.EntryRef, in.MessagePolicy, in.Pinned)
	if err != nil {
		return Session{}, err
	}
	p := auth.From(ctx)
	if err = tenancy.Bind(ctx, tx, p, auth.Tenant(ctx), p.Kind, p.ID, "session.owner", "session", id); err != nil {
		return Session{}, err
	}
	if err = logWrite(ctx, "session.created", "session", id, map[string]string{"entry_ref": in.EntryRef}); err != nil {
		return Session{}, err
	}
	return Get(ctx, id, "session.read")
}
func Patch(ctx context.Context, id string, in PatchInput) (Session, error) {
	s, err := Get(ctx, id, "session.update")
	if err != nil {
		return s, err
	}
	if in.Title == nil && in.Policy == nil {
		return s, httpx.E(422, "INVALID_INPUT", "At least one field required")
	}
	if in.Title != nil && (len(*in.Title) > 256 || strings.TrimSpace(*in.Title) == "") {
		return s, httpx.E(422, "INVALID_INPUT", "Invalid title")
	}
	if in.Policy != nil && !ValidPolicy(*in.Policy) {
		return s, httpx.E(422, "INVALID_INPUT", "Invalid message policy")
	}
	_, err = db.Tx(ctx).
		Exec(ctx, `UPDATE sessions
 SET title=COALESCE($3,title),message_policy=COALESCE($4,message_policy)
 WHERE tenant_id=$1
 AND id=$2`, auth.Tenant(ctx), id, in.Title, in.Policy)
	if err != nil {
		return s, err
	}
	if err = logWrite(ctx, "session.updated", "session", id, in); err != nil {
		return s, err
	}
	return Get(ctx, id, "session.read")
}
func Archive(ctx context.Context, id string) (Session, error) {
	s, err := Get(ctx, id, "session.update")
	if err != nil {
		return s, err
	}
	_, err = db.Tx(ctx).
		Exec(ctx, `UPDATE sessions SET status='archived' WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), id)
	if err != nil {
		return s, err
	}
	if err = logWrite(ctx, "session.archived", "session", id, map[string]any{}); err != nil {
		return s, err
	}
	s.Status = "archived"
	return s, nil
}
func List(ctx context.Context, page httpx.Page, workspace, status string) (Page[Session], error) {
	result := Page[Session]{Items: []Session{}}
	if workspace != "" && !ids.Valid(workspace) {
		return result, httpx.E(422, "INVALID_INPUT", "Invalid workspace")
	}
	args, err := (policy.Authorizer{}).SessionReadFilter(ctx)
	if err != nil {
		return result, err
	}
	args["tenant"], args["cursor"], args["workspace"], args["status"], args["limit"] =
		auth.Tenant(ctx), page.Cursor, workspace, status, page.Limit+1
	rows, err := db.Tx(ctx).Query(ctx, `SELECT to_jsonb(s)
 FROM sessions s
 WHERE s.tenant_id=@tenant
 AND (@cursor='' OR s.id>NULLIF(@cursor,'')::uuid)
 AND (@workspace='' OR s.workspace_id=NULLIF(@workspace,'')::uuid)
 AND (@status='' OR s.status=@status)
 AND `+policy.SessionReadPredicate+`
 ORDER BY s.id
 LIMIT @limit`, args)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var item Session
		if err := rows.Scan(&raw); err != nil {
			return result, err
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return result, err
		}
		result.Items = append(result.Items, item)
	}
	if len(result.Items) > page.Limit {
		result.Items = result.Items[:page.Limit]
		result.NextCursor = httpx.Cursor(result.Items[page.Limit-1].ID)
	}
	return result, rows.Err()
}
