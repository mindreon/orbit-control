package policy

import (
	"context"
	"slices"
	"strings"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

type Resource struct{ Type, ID, WorkspaceID string }
type Binding struct{ Role, ScopeType, ScopeID string }

// Tenant administrative roles operate on tenant resources and their children.
// Membership alone grants workspace creation; access to existing workspaces
// requires an explicit workspace role (possibly inherited through a group).
var Matrix = map[string][]string{
	"tenant.owner": {
		"tenant.read",
		"tenant.manage",
		"member.read",
		"member.manage",
		"role.manage",
		"workspace.create",
		"workspace.read",
		"workspace.update",
		"workspace.members",
		"project.manage",
		"group.manage",
		"asset.read",
		"asset.create",
		"asset.update",
		"asset.publish",
		"asset.review",
		"asset.use",
		"session.read",
		"session.create",
		"session.update",
		"session.share",
		"run.create",
		"run.control",
		"audit.read",
		"apikey.read",
		"apikey.manage",
		"model.manage",
		"secret.manage",
		"policy.manage",
		"quota.manage",
	},
	"tenant.admin": {
		"tenant.read",
		"member.read",
		"member.manage",
		"role.manage",
		"workspace.create",
		"workspace.read",
		"workspace.update",
		"workspace.members",
		"project.manage",
		"group.manage",
		"asset.read",
		"asset.create",
		"asset.update",
		"asset.publish",
		"asset.review",
		"asset.use",
		"session.read",
		"session.create",
		"session.update",
		"session.share",
		"run.create",
		"run.control",
		"audit.read",
		"apikey.read",
		"apikey.manage",
		"model.manage",
		"secret.manage",
		"policy.manage",
		"quota.manage",
	},
	"tenant.auditor":  {"tenant.read", "audit.read"},
	"tenant.reviewer": {"tenant.read", "asset.read", "asset.review"},
	"tenant.member":   {"tenant.read", "workspace.create"},
	"ws.admin": {
		"workspace.read",
		"workspace.update",
		"workspace.members",
		"project.manage",
		"asset.read",
		"asset.create",
		"asset.update",
		"asset.publish",
		"asset.use",
		"session.read",
		"session.create",
		"session.update",
		"session.share",
		"run.create",
		"run.control",
	},
	"ws.editor": {
		"workspace.read",
		"asset.read",
		"asset.create",
		"asset.update",
		"asset.publish",
		"asset.use",
		"session.read",
		"session.create",
		"session.update",
		"run.create",
		"run.control",
	},
	"ws.member":            {"workspace.read", "asset.read", "asset.use", "session.create", "run.create"},
	"ws.viewer":            {"workspace.read", "asset.read", "session.read"},
	"asset.owner":          {"asset.read", "asset.update", "asset.publish", "asset.use", "asset.share"},
	"asset.maintainer":     {"asset.read", "asset.update", "asset.publish", "asset.use"},
	"asset.user":           {"asset.read", "asset.use"},
	"session.owner":        {"session.read", "session.update", "session.share", "run.create", "run.control"},
	"session.collaborator": {"session.read", "session.update", "run.create", "run.control"},
	"session.viewer":       {"session.read"},
}

func ValidRole(role, scope string) bool {
	_, ok := Matrix[role]
	prefix := scope
	if scope == "workspace" {
		prefix = "ws"
	}
	return ok && strings.HasPrefix(role, prefix+".")
}
func Allows(bindings []Binding, action string, r Resource, tenant string) bool {
	for _, b := range bindings {
		if !slices.Contains(Matrix[b.Role], action) {
			continue
		}
		if b.ScopeType == "tenant" && b.ScopeID == tenant {
			return true
		}
		if b.ScopeType == r.Type && b.ScopeID == r.ID {
			return true
		}
		if b.ScopeType == "workspace" && r.WorkspaceID != "" && b.ScopeID == r.WorkspaceID {
			return true
		}
	}
	return false
}

type Authorizer struct{}

func (Authorizer) Bindings(ctx context.Context) ([]Binding, error) {
	p := auth.From(ctx)
	rows, e := db.Tx(ctx).
		Query(ctx, "SELECT role,scope_type,scope_id::text FROM role_bindings WHERE (principal_type IN "+
			"('user','service') AND principal_id=$1) OR (principal_type='group' AND principal_id IN "+
			"(SELECT group_id FROM group_members WHERE principal_id=$1))", p.ID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := []Binding{}
	for rows.Next() {
		var b Binding
		if e = rows.Scan(&b.Role, &b.ScopeType, &b.ScopeID); e != nil {
			return nil, e
		}
		result = append(result, b)
	}
	return result, rows.Err()
}
func (a Authorizer) Require(ctx context.Context, action string, r Resource) error {
	p := auth.From(ctx)
	if p.APIKey && !slices.Contains(p.Scopes, action) {
		return httpx.E(403, "FORBIDDEN", "API key scope does not permit this action")
	}
	b, e := a.Bindings(ctx)
	if e != nil {
		return e
	}
	if !Allows(b, action, r, auth.Tenant(ctx)) {
		return httpx.E(403, "FORBIDDEN", "Action is not permitted")
	}
	return nil
}

// WorkspaceFilter centralizes visibility and API-key scopes for list queries.
func (a Authorizer) WorkspaceFilter(ctx context.Context) (bool, []string, error) {
	p := auth.From(ctx)
	if p.APIKey && !slices.Contains(p.Scopes, "workspace.read") {
		return false, nil, httpx.E(403, "FORBIDDEN", "API key scope does not permit this action")
	}
	bindings, e := a.Bindings(ctx)
	if e != nil {
		return false, nil, e
	}
	ids := []string{}
	for _, b := range bindings {
		if !slices.Contains(Matrix[b.Role], "workspace.read") {
			continue
		}
		if b.ScopeType == "tenant" && b.ScopeID == auth.Tenant(ctx) {
			return true, ids, nil
		}
		if b.ScopeType == "workspace" {
			ids = append(ids, b.ScopeID)
		}
	}
	return false, ids, nil
}

// RequireKeyIssuance prevents impersonation and permission amplification. Action
// scopes intersect all tenant-local RBAC grants again on each authenticated call.
func (a Authorizer) RequireKeyIssuance(ctx context.Context, target string, scopes []string) error {
	issuer := auth.From(ctx)
	var kind string
	err := db.Tx(ctx).
		QueryRow(ctx, "SELECT p.kind FROM principals p JOIN memberships m ON m.principal_id=p.id WHERE p.id=$1 "+
			"AND m.status='active' AND p.status='active'", target).
		Scan(&kind)
	if err != nil {
		return err
	}
	if target != issuer.ID {
		if kind != "service" {
			return httpx.E(403, "FORBIDDEN", "API keys cannot impersonate another user")
		}
		if err = a.Require(ctx, "tenant.manage", Resource{Type: "tenant", ID: auth.Tenant(ctx)}); err != nil {
			return err
		}
	}
	issuerBindings, err := a.Bindings(ctx)
	if err != nil {
		return err
	}
	recipientCtx := auth.Context(ctx, auth.Principal{ID: target, Kind: kind}, auth.Tenant(ctx))
	recipientBindings, err := a.Bindings(recipientCtx)
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		if issuer.APIKey && !slices.Contains(issuer.Scopes, scope) ||
			!hasAction(issuerBindings, scope, auth.Tenant(ctx)) ||
			!hasAction(recipientBindings, scope, auth.Tenant(ctx)) {
			return httpx.E(403, "FORBIDDEN", "API key scopes exceed issuer or recipient permissions")
		}
	}

	return nil
}

func hasAction(bindings []Binding, action, tenant string) bool {
	for _, b := range bindings {
		if b.ScopeType == "tenant" && b.ScopeID == tenant && slices.Contains(Matrix[b.Role], action) {
			return true
		}
	}
	return false
}
