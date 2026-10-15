package interactions

import (
	"context"
	"encoding/json/v2"
	"path"
	"slices"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

type Interaction struct {
	ID        string                      `json:"id"`
	RunID     string                      `json:"run_id"`
	SessionID string                      `json:"session_id"`
	Member    string                      `json:"member"`
	Kind      contract.InteractionKind    `json:"kind"`
	Request   contract.InteractionRequest `json:"request"`
	Status    string                      `json:"status"`
	Approvers []string                    `json:"approvers"`
	Decision  *contract.Decision          `json:"decision,omitempty"`
	DecidedBy *string                     `json:"decided_by,omitempty"`
	ExpiresAt time.Time                   `json:"expires_at"`
	CreatedAt time.Time                   `json:"created_at"`
}

func Get(ctx context.Context, tenant, id string) (Interaction, error) {
	var i Interaction
	var req, approvers, decision []byte
	if !ids.Valid(id) {
		return i, httpx.E(404, "NOT_FOUND", "Interaction not found")
	}
	err := db.Tx(ctx).
		QueryRow(ctx, `SELECT id::text,run_id::text,session_id::text,member,kind,request,status,approvers,decision,decided_by::text,
 expires_at,created_at
 FROM interactions
 WHERE tenant_id=$1
 AND id=$2`, tenant, id).
		Scan(&i.ID, &i.RunID, &i.SessionID, &i.Member, &i.Kind, &req, &i.Status, &approvers, &decision, &i.DecidedBy, &i.ExpiresAt, &i.CreatedAt)
	if err != nil {
		return i, err
	}
	if err = json.Unmarshal(req, &i.Request); err != nil {
		return i, err
	}
	if err = json.Unmarshal(approvers, &i.Approvers); err != nil {
		return i, err
	}
	if len(decision) > 0 {
		err = json.Unmarshal(decision, &i.Decision)
	}
	return i, err
}
func CanResolve(principal string, i Interaction) bool {
	return slices.Contains(i.Approvers, principal) && (i.Status == "pending" || i.Status == "escalated") &&
		time.Now().Before(i.ExpiresAt)
}

// Grants are written only after an accepted human decision, in its transaction.
func Grant(ctx context.Context, tenant, workspace, session, actor string, i Interaction, d contract.Decision) (string, error) {
	if d.ArgPattern != nil {
		if _, err := path.Match(*d.ArgPattern, ""); err != nil {
			return "", httpx.E(422, "INVALID_PATTERN", "Invalid argument pattern")
		}
	}
	if d.Kind != "tool_approval" || d.Choice == nil || (*d.Choice != "allow_session" && *d.Choice != "allow_always") {
		return "", nil
	}
	scope, id := "session", session
	if *d.Choice == "allow_always" {
		scope, id = "user", actor
		if d.Scope != nil && *d.Scope == "workspace" {
			ac := auth.Context(ctx, auth.Principal{ID: actor, Kind: "user"}, tenant)
			bindings, err := (policy.Authorizer{}).Bindings(ac)
			if err != nil {
				return "", err
			}
			if policy.Allows(bindings, "policy.manage", policy.Resource{Type: "workspace", ID: workspace, WorkspaceID: workspace}, tenant) ||
				slices.ContainsFunc(bindings, func(b policy.Binding) bool {
					return b.Role == "ws.admin" && b.ScopeType == "workspace" && b.ScopeID == workspace
				}) {
				scope, id = "workspace", workspace
			}
		}
	}
	for _, call := range i.Request.Calls {
		if _, err := db.Tx(ctx).Exec(ctx, `INSERT INTO permission_grants(id,tenant_id,scope_type,scope_id,interaction_id,tool,
 arg_pattern,decision,granted_by,expires_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,'allow',$8,$9)
 ON CONFLICT DO NOTHING`,
			ids.Derive(i.ID, "grant:"+call.Name),
			tenant,
			scope,
			id,
			i.ID,
			call.Name,
			d.ArgPattern,
			actor,
			time.Now().Add(365*24*time.Hour)); err != nil {
			return "", err
		}
	}
	return scope, nil
}
func WorkspaceAdmins(ctx context.Context, workspace string) ([]string, error) {
	rows, err := db.Tx(ctx).
		Query(ctx, `SELECT DISTINCT m.principal_id::text
 FROM memberships m
 JOIN principals p ON p.id=m.principal_id
 JOIN role_bindings b ON ((b.principal_type IN ('user','service')
 AND b.principal_id=m.principal_id)
 OR (b.principal_type='group'
 AND b.principal_id IN (SELECT group_id
 FROM group_members
 WHERE principal_id=m.principal_id)))
 WHERE m.status='active'
 AND p.status='active'
 AND b.role='ws.admin'
 AND b.scope_type='workspace'
 AND b.scope_id=$1
 ORDER BY 1`, workspace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
