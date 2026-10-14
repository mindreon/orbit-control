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
	switch kind {
	case "user", "service":
		err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships m JOIN principals p ON p.id=m.principal_id WHERE m.principal_id=$1 AND m.status='active' AND p.kind=$2 AND ($3<>'workspace' OR EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id=NULLIF($4,'')::uuid AND principal_id=$1)))`, id, kind, r.Visibility, r.WorkspaceID).Scan(&allowed)
	case "group":
		err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM groups WHERE id=$1) AND ($2<>'workspace' OR NOT EXISTS(SELECT 1 FROM group_members gm WHERE gm.group_id=$1 AND NOT EXISTS(SELECT 1 FROM workspace_members wm WHERE wm.workspace_id=NULLIF($3,'')::uuid AND wm.principal_id=gm.principal_id)))`, id, r.Visibility, r.WorkspaceID).Scan(&allowed)
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
	if !allowed {
		return httpx.E(403, "VISIBILITY_BOUNDARY", "Grant exceeds asset visibility boundary")
	}
	return nil
}
