package assets

import (
	"context"
	"errors"
	"slices"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

type Grant struct {
	Type       string `json:"grantee_type"`
	ID         string `json:"grantee_id"`
	Permission string `json:"permission"`
}

func (g Grant) Key() string { return g.Type + ":" + g.ID + ":" + g.Permission }

func (s *Service) ChangeVisibility(ctx context.Context, id, visibility string) (Asset, error) {
	if _, err := db.Tx(ctx).Exec(ctx, `SELECT id FROM assets WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), id); err != nil {
		return Asset{}, err
	}
	a, err := s.Get(ctx, id, "asset.share")
	if err != nil {
		return a, err
	}
	if !slices.Contains([]string{"private", "workspace", "tenant"}, visibility) || visibility == "workspace" && a.WorkspaceID == "" {
		return a, Invalid("Invalid visibility")
	}
	if visibility != a.Visibility {
		var frozen bool
		if err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asset_versions WHERE asset_id=$1 AND state NOT IN ('draft','check_failed','changes_requested','rejected'))`, id).Scan(&frozen); err != nil {
			return a, err
		}
		if frozen {
			return a, httpx.E(409, "RELEASE_VISIBILITY_FROZEN", "Visibility is frozen once a version is submitted")
		}
	}
	a.Visibility = visibility
	grants, err := s.Grants(ctx, id)
	if err != nil {
		return a, err
	}
	for _, g := range grants {
		err = s.Authz.RequireAssetGrantee(ctx, policy.Asset{ID: id, WorkspaceID: a.WorkspaceID, Visibility: visibility}, g.Type, g.ID)
		if err == nil {
			continue
		}
		if e, ok := errors.AsType[*httpx.Error](err); !ok || e.Code != "VISIBILITY_BOUNDARY" {
			return a, err
		}
		if err = s.ChangeGrant(ctx, a, g, true); err != nil {
			return a, err
		}
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET visibility=$2 WHERE id=$1`, id, visibility); err != nil {
		return a, err
	}
	return a, Log(ctx, "asset.visibility_changed", "asset", id, map[string]string{"visibility": visibility})
}
func (s *Service) ChangeStatus(ctx context.Context, id, status string) (Asset, error) {
	a, err := s.Get(ctx, id, "asset.update")
	if err != nil {
		return a, err
	}
	if !slices.Contains([]string{"active", "suspended", "archived"}, status) {
		return a, Invalid("Invalid asset status")
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET status=$2 WHERE tenant_id=$3 AND id=$1`, id, status, auth.Tenant(ctx)); err != nil {
		return a, err
	}
	a.Status = status
	return a, Log(ctx, "asset."+status, "asset", id, map[string]any{})
}
func (s *Service) ChangeVersionState(ctx context.Context, a Asset, v Version, state string) (Version, error) {
	if !slices.Contains([]string{"draft", "deprecated", "suspended", "yanked", "published"}, state) {
		return v, Invalid("Invalid requested state")
	}
	if state == "published" {
		if v.State != "suspended" {
			return v, httpx.E(409, "INVALID_ASSET_STATE", "Only suspended versions can be restored through this operation")
		}
		if err := s.Authz.Require(ctx, "asset.review", policy.Resource{Type: "tenant", ID: auth.Tenant(ctx)}); err != nil {
			return v, err
		}
		if err := s.Signer.Verify(a, v); err != nil {
			return v, err
		}
	}
	if err := SetState(ctx, v.ID, state); err != nil {
		return v, err
	}
	v.State = state
	return v, nil
}
func (s *Service) RequestWithdraw(ctx context.Context, id string) error {
	n, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET withdraw_requested=true WHERE tenant_id=$2 AND id=$1 AND state IN ('submitted','checking','in_review')`, id, auth.Tenant(ctx))
	if err != nil {
		return err
	}
	if n.RowsAffected() == 0 {
		return httpx.E(409, "INVALID_ASSET_STATE", "Release cannot be withdrawn at this stage")
	}
	return Log(ctx, "asset.withdraw_requested", "asset_version", id, map[string]any{})
}
func (s *Service) Review(ctx context.Context, id, decision, comment string) (string, error) {
	if !ids.Valid(id) {
		return "", Invalid("Invalid version")
	}
	if !slices.Contains([]string{"approve", "reject", "request_changes"}, decision) || len(comment) > 4000 {
		return "", Invalid("Invalid review decision or comment")
	}
	var assetID, submitter, uploader, state string
	var generation int
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT asset_id::text,COALESCE(submitted_by::text,''),COALESCE(uploaded_by::text,''),state,release_generation FROM asset_versions WHERE tenant_id=$2 AND id=$1 FOR UPDATE`, id, auth.Tenant(ctx)).Scan(&assetID, &submitter, &uploader, &state, &generation); err != nil {
		return "", err
	}
	a, err := s.Get(ctx, assetID, "asset.review")
	if err != nil {
		return "", err
	}
	if err = s.Authz.RequireAsset(ctx, "asset.review", policy.Asset{ID: a.ID, WorkspaceID: a.WorkspaceID, OwnerID: a.OwnerID, Visibility: a.Visibility, Reviewing: true, SubmitterID: submitter, UploaderID: uploader}); err != nil {
		return "", err
	}
	if state != "in_review" {
		return "", httpx.E(409, "INVALID_ASSET_STATE", "Version is not awaiting review")
	}
	reviewID := ids.New()
	if _, err = db.Tx(ctx).Exec(ctx, `INSERT INTO asset_reviews(id,tenant_id,version_id,reviewer_id,decision,comment,generation) VALUES($1,$2,$3,$4,$5,$6,$7)`, reviewID, auth.Tenant(ctx), id, auth.From(ctx).ID, decision, comment, generation); err != nil {
		return "", err
	}
	return reviewID, Log(ctx, "asset.review_submitted", "asset_version", id, map[string]string{"review_id": reviewID, "decision": decision})
}
func (s *Service) ChangeGrant(ctx context.Context, a Asset, g Grant, revoke bool) error {
	// Serialize grants with visibility changes; re-read the ceiling after locking.
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT visibility FROM assets WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), a.ID).Scan(&a.Visibility); err != nil {
		return err
	}
	if !ids.Valid(g.ID) || !slices.Contains([]string{"user", "service", "group", "workspace", "tenant"}, g.Type) || !slices.Contains([]string{"use", "maintain"}, g.Permission) {
		return Invalid("Invalid asset grant")
	}
	if !revoke {
		if err := s.Authz.RequireAssetGrantee(ctx, policy.Asset{ID: a.ID, WorkspaceID: a.WorkspaceID, OwnerID: a.OwnerID, Visibility: a.Visibility}, g.Type, g.ID); err != nil {
			return err
		}
	}
	var err error
	if revoke {
		_, err = db.Tx(ctx).Exec(ctx, `DELETE FROM asset_grants WHERE asset_id=$1 AND grantee_type=$2 AND grantee_id=$3 AND permission=$4`, a.ID, g.Type, g.ID, g.Permission)
	} else {
		_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO asset_grants(tenant_id,asset_id,grantee_type,grantee_id,permission) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, auth.Tenant(ctx), a.ID, g.Type, g.ID, g.Permission)
	}
	if err != nil {
		return err
	}
	action := "asset.granted"
	if revoke {
		action = "asset.grant_revoked"
	}
	return Log(ctx, action, "asset", a.ID, g)
}
func (s *Service) Uninstall(ctx context.Context, id string) error {
	if !ids.Valid(id) {
		return Invalid("Invalid install")
	}
	var scope, scopeID string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT scope_type,scope_id::text FROM asset_installs WHERE tenant_id=$2 AND id=$1`, id, auth.Tenant(ctx)).Scan(&scope, &scopeID); err != nil {
		return err
	}
	if err := s.requireInstall(ctx, scope, scopeID); err != nil {
		return err
	}
	if _, err := db.Tx(ctx).Exec(ctx, `DELETE FROM asset_installs WHERE id=$1`, id); err != nil {
		return err
	}
	return Log(ctx, "asset.uninstalled", "asset_install", id, map[string]any{})
}
func (s *Service) requireInstall(ctx context.Context, scope, id string) error {
	if scope != "workspace" && scope != "tenant" || !ids.Valid(id) || scope == "tenant" && id != auth.Tenant(ctx) {
		return Invalid("Invalid install scope")
	}
	action := "workspace.update"
	if scope == "tenant" {
		action = "policy.manage"
	}
	return s.Authz.Require(ctx, action, policy.Resource{Type: scope, ID: id})
}

// RetryRelease is also used by the privileged admin CLI; callers supply an
// authenticated tenant transaction. It never changes a business decision.
func (s *Service) RetryRelease(ctx context.Context, id string) error {
	n, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET release_error=NULL,release_retryable=true WHERE tenant_id=$1 AND id=$2 AND state IN ('submitted','checking','in_review','approved')`, auth.Tenant(ctx), id)
	if err != nil {
		return err
	}
	if n.RowsAffected() == 0 {
		return httpx.E(409, "INVALID_ASSET_STATE", "Version has no release to retry")
	}
	return Log(ctx, "asset.release_retry_requested", "asset_version", id, map[string]any{})
}

// Withdraw is the author action, distinct from a reviewer rejection.
func (s *Service) Withdraw(ctx context.Context, id string) error {
	var state string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT state FROM asset_versions WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), id).Scan(&state); err != nil {
		return err
	}
	if state == "draft" {
		return nil
	}
	if state != "submitted" && state != "checking" && state != "in_review" {
		return httpx.E(409, "INVALID_ASSET_STATE", "Release cannot be withdrawn at this stage")
	}
	if err := SetState(ctx, id, "draft"); err != nil {
		return err
	}
	if _, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET withdraw_delivered=true,release_error=NULL WHERE id=$1`, id); err != nil {
		return err
	}
	return Log(ctx, "asset.version.withdrawn", "asset_version", id, map[string]string{"from": state})
}

func (s *Service) RequireUpload(ctx context.Context, id string) error {
	a, err := s.Get(ctx, id, "asset.update")
	if err != nil {
		return err
	}
	if a.Status != "active" {
		return Invalid("Asset is unavailable")
	}
	return nil
}
