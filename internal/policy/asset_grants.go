package policy

import (
	"context"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

// RequireAssetGrantee verifies that a share remains within the asset's tenant
// and workspace ceiling. Private assets may be shared with named users/groups,
// not broadened to an entire workspace or tenant by a grant.
func (a Authorizer) RequireAssetGrantee(ctx context.Context, r Asset, kind, id string) error {
	var allowed bool
	var err error
	principals := []string{}
	switch kind {
	case "user", "service":
		err = db.Tx(ctx).
			QueryRow(ctx, `SELECT EXISTS(SELECT 1
   FROM memberships m
   JOIN principals p ON p.id=m.principal_id
   WHERE m.principal_id=$1
   AND m.status='active'
   AND p.kind=$2)`, id, kind).
			Scan(&allowed)
		principals = append(principals, id)
	case "group":
		err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM groups WHERE id=$1)`, id).Scan(&allowed)
		if err == nil && allowed && r.Visibility == "workspace" {
			rows, e := db.Tx(ctx).Query(ctx, `SELECT principal_id::text FROM group_members WHERE group_id=$1`, id)
			if e != nil {
				return e
			}
			for rows.Next() {
				var principal string
				if e = rows.Scan(&principal); e != nil {
					rows.Close()
					return e
				}
				principals = append(principals, principal)
			}
			err = rows.Err()
			rows.Close()
		}
	case "workspace":
		allowed = r.Visibility == "tenant" || r.Visibility == "workspace" && r.WorkspaceID == id
		if allowed {
			err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workspaces WHERE id=$1)`, id).Scan(&allowed)
		}
	case "tenant":
		allowed = r.Visibility == "tenant" && id == auth.Tenant(ctx)
	}
	if err != nil {
		return err
	}
	if allowed && r.Visibility == "workspace" {
		for _, principal := range principals {
			bindings, e := a.Bindings(auth.Context(ctx, auth.Principal{ID: principal}, auth.Tenant(ctx)))
			if e != nil {
				return e
			}
			var member bool
			if e = db.Tx(ctx).QueryRow(ctx,
				(`SELECT EXISTS(SELECT 1
   FROM workspace_members
   WHERE workspace_id=$1
   AND principal_id=$2)`),
				r.WorkspaceID,
				principal).Scan(&member); e != nil {
				return e
			}
			if !member &&
				!Allows(bindings, "workspace.read", Resource{Type: "workspace", ID: r.WorkspaceID}, auth.Tenant(ctx)) {
				allowed = false
				break
			}
		}
	}
	if !allowed {
		return httpx.E(403, "VISIBILITY_BOUNDARY", "Grant exceeds asset visibility boundary")
	}
	return nil
}
