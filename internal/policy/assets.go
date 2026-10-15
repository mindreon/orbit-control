package policy

import (
	"context"
	"slices"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

type Asset struct {
	ID, WorkspaceID, OwnerID, Visibility, SubmitterID, UploaderID string
	Reviewing                                                     bool
}

// RequireAsset applies visibility before RBAC. Grants are tenant-local and can
// only narrow sharing to the asset's workspace when it has workspace visibility.
func (a Authorizer) RequireAsset(ctx context.Context, action string, r Asset) error {
	p := auth.From(ctx)
	tenant := auth.Tenant(ctx)
	if p.APIKey && !slices.Contains(p.Scopes, action) {
		return httpx.E(403, "FORBIDDEN", "API key scope does not permit this action")
	}
	bindings, err := a.Bindings(ctx)
	if err != nil {
		return err
	}
	member, granted, maintain, err := assetGrants(ctx, r, bindings)
	if err != nil {
		return err
	}
	resource := Resource{Type: "asset", ID: r.ID, WorkspaceID: r.WorkspaceID}
	workspaceAccess := member ||
		r.WorkspaceID != "" &&
			Allows(bindings, "workspace.read", Resource{Type: "workspace", ID: r.WorkspaceID}, tenant)
	explicit := Allows(bindings, "asset.read", Resource{Type: "asset", ID: r.ID}, "")
	reviewer := r.Reviewing && Allows(bindings, "asset.review", resource, tenant)
	visible := p.ID == r.OwnerID || explicit || granted || r.Visibility == "tenant" ||
		r.Visibility == "workspace" && workspaceAccess ||
		reviewer
	if !visible {
		return httpx.E(404, "NOT_FOUND", "Asset not found")
	}
	if r.Visibility == "workspace" && !workspaceAccess && p.ID != r.OwnerID && !reviewer {
		return httpx.E(404, "NOT_FOUND", "Asset not found")
	}
	if action == "asset.read" {
		return nil
	}
	if action == "asset.use" {
		if r.Visibility == "tenant" || p.ID == r.OwnerID || granted || explicit {
			return nil
		}
		return a.Require(ctx, action, resource)
	}
	if action == "asset.review" {
		if p.Kind != "user" {
			return httpx.E(403, "FORBIDDEN", "Asset review requires a human reviewer")
		}
		if p.ID == r.OwnerID || p.ID == r.SubmitterID || p.ID == r.UploaderID {
			return httpx.E(403, "FORBIDDEN", "Authors cannot review their own assets")
		}
		if r.Visibility == "workspace" &&
			Allows(bindings, "asset.publish", Resource{Type: "workspace", ID: r.WorkspaceID}, tenant) {
			return nil
		}
		return a.Require(ctx, action, resource)
	}
	if p.ID == r.OwnerID && slices.Contains(Matrix["asset.owner"], action) {
		return nil
	}
	if maintain && slices.Contains(Matrix["asset.maintainer"], action) {
		return nil
	}
	if action == "asset.share" && Allows(bindings, "asset.update", resource, tenant) {
		return nil
	}
	return a.Require(ctx, action, resource)
}

func assetGrants(ctx context.Context, r Asset, bindings []Binding) (bool, bool, bool, error) {
	p, tenant := auth.From(ctx), auth.Tenant(ctx)
	workspaces := []string{}
	for _, binding := range bindings {
		if binding.ScopeType == "workspace" && slices.Contains(Matrix[binding.Role], "workspace.read") {
			workspaces = append(workspaces, binding.ScopeID)
		}
	}
	var member, granted, maintain bool
	err := db.Tx(ctx).QueryRow(ctx, `SELECT
 EXISTS(SELECT 1
 FROM workspace_members
 WHERE workspace_id=NULLIF($1,'')::uuid
 AND principal_id=$2),
 COALESCE(bool_or(permission IN ('use','maintain')),false),
 COALESCE(bool_or(permission='maintain'),false)
 FROM asset_grants
 WHERE asset_id=$3
 AND (grantee_type NOT IN ('tenant','workspace')
 OR grantee_type='tenant'
 AND $7='tenant'
 OR grantee_type='workspace'
 AND ($7='tenant'
 OR $7='workspace'
 AND grantee_id=NULLIF($1,'')::uuid))
 AND (
 (grantee_type IN ('user','service')
 AND grantee_id=$2)
 OR (grantee_type='group'
 AND grantee_id IN (SELECT group_id
 FROM group_members
 WHERE principal_id=$2))
 OR (grantee_type='tenant'
 AND grantee_id=$4)
 OR (grantee_type='workspace'
 AND (grantee_id IN
 (SELECT workspace_id
 FROM workspace_members
 WHERE principal_id=$2)
 OR grantee_id=ANY($5::uuid[])
 OR $6)))`,

		r.WorkspaceID,
		p.ID,
		r.ID,
		tenant,
		workspaces,
		Allows(bindings, "workspace.read", Resource{}, tenant),
		r.Visibility).Scan(&member,
		&granted,
		&maintain)
	if err != nil {
		return false, false, false, err
	}
	return member, granted, maintain, nil
}
