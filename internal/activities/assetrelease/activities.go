package assetrelease

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type Activities struct {
	DB     *db.DB
	Assets *assets.Service
}

func (a *Activities) transaction(
	ctx context.Context,
	in Input,
	fn func(context.Context, assets.Asset, assets.Version) error,
) error {
	return a.DB.WithTenant(ctx, in.Tenant, func(tx pgx.Tx) error {
		ctx = auth.Context(db.Context(ctx, tx), auth.Principal{}, in.Tenant)
		var assetID, actor, actorKind string
		var generation int
		if err := tx.QueryRow(ctx, "SELECT v.asset_id::text,v.submitted_by::text,v.release_generation,p.kind FROM "+
			"asset_versions v JOIN principals p ON p.id=v.submitted_by WHERE v.tenant_id=$1 AND "+
			"v.id=$2 FOR UPDATE OF v", in.Tenant, in.VersionID).Scan(&assetID, &actor, &generation, &actorKind); err != nil {
			return err
		}
		if generation != in.Generation {
			return temporal.NewNonRetryableApplicationError("release generation mismatch", "StaleRelease", nil)
		}
		ctx = auth.Context(ctx, auth.Principal{ID: actor, Kind: actorKind}, in.Tenant)
		// Activities act on the frozen snapshot, not on today's author permissions.
		var asset assets.Asset
		err := tx.QueryRow(ctx, "SELECT id::text,publisher,kind,name,visibility,COALESCE(workspace_id::text,''),owner_id::t"+
			"ext,status,created_at FROM assets WHERE tenant_id=$1 AND id=$2", in.Tenant, assetID).
			Scan(&asset.ID,
				&asset.Publisher,
				&asset.Kind,
				&asset.Name,
				&asset.Visibility,
				&asset.WorkspaceID,
				&asset.OwnerID,
				&asset.Status,
				&asset.CreatedAt)
		if err != nil {
			return err
		}
		v, err := a.Assets.Version(ctx, assetID, in.VersionID)
		if err != nil {
			return err
		}
		return fn(ctx, asset, v)
	})
}
func (a *Activities) SnapshotAssetRelease(ctx context.Context, in Input) (Snapshot, error) {
	var out Snapshot
	err := a.transaction(ctx, in, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.State == "submitted" {
			if err := assets.SetState(ctx, v.ID, "checking"); err != nil {
				return err
			}
		} else if v.State != "checking" && v.State != "approved" && v.State != "in_review" {
			return temporal.NewNonRetryableApplicationError("invalid release state", "InvalidState", nil)
		}
		var previous *string
		err := db.Tx(ctx).
			QueryRow(ctx, "SELECT review_required,workspace_review,previous_version_id::text,submitted_by::text FROM "+
				"asset_versions WHERE id=$1", v.ID).
			Scan(&out.Required, &out.WorkspaceReview, &previous, &out.Author)
		if err != nil {
			return err
		}
		out.State = v.State
		out.Uploader = v.UploadedBy
		if v.State == "in_review" {
			rows, err := db.Tx(ctx).
				Query(ctx, "SELECT id::text,reviewer_id::text,decision,comment FROM asset_reviews WHERE tenant_id=$1 "+
					"AND version_id=$2 AND generation=$3 ORDER BY id", in.Tenant, v.ID, in.Generation)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				review := Review{Input: in}
				if err = rows.Scan(&review.ID, &review.Reviewer, &review.Decision, &review.Comment); err != nil {
					return err
				}
				out.Reviews = append(out.Reviews, review)
			}
			if err = rows.Err(); err != nil {
				return err
			}
		}
		out.Visibility = asset.Visibility
		out.Owner = asset.OwnerID
		out.Check = contract.AssetCheckInput{
			V:                 1,
			TenantId:          in.Tenant,
			AssetId:           asset.ID,
			AssetVersionId:    v.ID,
			Kind:              asset.Kind,
			ContentDigest:     v.Digest,
			BlobRef:           v.BlobRef,
			PreviousVersionId: previous,
		}
		return nil
	})
	return out, err
}

func (a *Activities) AssetCheckPermissionDiff(
	ctx context.Context,
	input contract.AssetCheckInput,
) (contract.AssetCheckReport, error) {
	report := contract.AssetCheckReport{Check: "permission_diff", DurationMs: 0, CheckerVersion: "orbit-control/1"}
	err := a.DB.WithTenant(ctx, input.TenantId, func(tx pgx.Tx) error {
		var raw []byte
		var current contract.EffectivePermissions
		var currentManifest, previousManifest assets.Manifest
		var manifestRaw []byte
		var workspace, submitter string
		var previous contract.EffectivePermissions
		if err := tx.QueryRow(ctx, "SELECT v.permissions,v.manifest,COALESCE(a.workspace_id::text,''),v.submitted_by::text "+
			"FROM asset_versions v JOIN assets a ON a.id=v.asset_id WHERE v.tenant_id=$1 AND v.id=$2 "+
			"AND v.asset_id=$3", input.TenantId, input.AssetVersionId, input.AssetId).Scan(&raw, &manifestRaw, &workspace, &submitter); err != nil {
			return err
		}
		if err := json.Unmarshal(manifestRaw, &currentManifest); err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if input.PreviousVersionId != nil {
			if err := tx.QueryRow(ctx, "SELECT permissions,manifest FROM asset_versions WHERE tenant_id=$1 AND id=$2 AND "+
				"asset_id=$3", input.TenantId, *input.PreviousVersionId, input.AssetId).Scan(&raw, &manifestRaw); err != nil {
				return err
			}
			if err := json.Unmarshal(manifestRaw, &previousManifest); err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &previous); err != nil {
				return err
			}
		}
		ctx = auth.Context(db.Context(ctx, tx), auth.Principal{ID: submitter, Kind: "user"}, input.TenantId)
		var err error
		current, err = locks.NarrowPermissions(ctx, current, workspace)
		if err != nil {
			return err
		}
		previous, err = locks.NarrowPermissions(ctx, previous, workspace)
		if err != nil {
			return err
		}
		expanded := Expanded(previous, current)
		for path, after := range currentManifest.MemberPermissions {
			before, e := locks.NarrowPermissions(ctx, previousManifest.MemberPermissions[path], workspace)
			if e != nil {
				return e
			}
			after, e = locks.NarrowPermissions(ctx, after, workspace)
			if e != nil {
				return e
			}
			expanded = expanded || Expanded(before, after)
		}
		if expanded {
			report.Findings = []contract.AssetCheckFinding{
				{
					Check:    "permission_diff",
					RuleId:   "permissions.expanded",
					Severity: "review",
					Message:  "Declared member permissions increased",
				},
			}
		}
		return nil
	})
	return report, err
}
func Expanded(before, after contract.EffectivePermissions) bool {
	for _, v := range after.Network.Egress {
		covered := false
		for _, b := range before.Network.Egress {
			ok, err := contract.MatchGlob(b, v)
			covered = covered || err == nil && (ok || b == v)
		}
		if !covered {
			return true
		}
	}
	for _, v := range after.Secrets {
		if !slices.Contains(before.Secrets, v) {
			return true
		}
	}
	for _, v := range after.EffectClasses {
		if !slices.Contains(before.EffectClasses, v) {
			return true
		}
	}
	for _, v := range after.ToolsBuiltin {
		if !slices.Contains(before.ToolsBuiltin, v) {
			return true
		}
	}
	for _, v := range after.DataScopes {
		if !slices.Contains(before.DataScopes, v) {
			return true
		}
	}
	if after.SandboxProfile != "" && after.SandboxProfile != "default" &&
		after.SandboxProfile != before.SandboxProfile {
		return true
	}
	if before.SandboxProfile != "" && after.SandboxProfile != before.SandboxProfile {
		return true
	}

	return false
}
func (a *Activities) AggregateAssetChecks(ctx context.Context, in Aggregate) error {
	return a.transaction(ctx, in.Input, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.State == in.State {
			return nil
		}
		if v.State != "checking" {
			return fmt.Errorf("invalid check state")
		}
		var withdrawn bool
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT withdraw_requested FROM asset_versions WHERE id=$1`, v.ID).Scan(&withdrawn); err != nil {
			return err
		}
		if withdrawn {
			return temporal.NewNonRetryableApplicationError("withdrawn", "Withdrawn", nil)
		}
		expected := []contract.AssetCheckKind{
			"structure",
			"compat",
			"secrets",
			"static",
			"injection",
			"permission_diff",
		}
		if len(in.Reports) != len(expected) {
			return fmt.Errorf("missing check reports")
		}
		for i, report := range in.Reports {
			if report.Check != expected[i] {
				return fmt.Errorf("unexpected checker result")
			}
			raw, err := json.Marshal(report)
			if err != nil {
				return err
			}
			if err = contract.Validate("AssetCheckReport", raw); err != nil {
				return err
			}
			status := "pass"
			for _, f := range report.Findings {
				if f.Check != report.Check {
					return fmt.Errorf("mismatched check finding")
				}
				if f.Severity == "block" {
					status = "fail"
					break
				}
				if f.Severity == "review" {
					status = "warn"
				}
			}
			_, err = db.Tx(ctx).
				Exec(ctx, "INSERT INTO asset_check_reports(id,tenant_id,version_id,\"check\",status,findings) "+
					"VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,version_id,\"check\") DO UPDATE SET "+
					"status=EXCLUDED.status,findings=EXCLUDED.findings", ids.New(), in.Input.Tenant, v.ID, report.Check, status, raw)
			if err != nil {
				return err
			}
		}
		return assets.SetState(ctx, v.ID, in.State)
	})
}
func (a *Activities) CreateReviewTask(ctx context.Context, in Input) error {
	return a.onceAudit(ctx, in, "asset.review_requested")
}
func (a *Activities) RemindAssetReview(ctx context.Context, in Input) error {
	return a.onceAudit(ctx, in, "asset.review_sla_reminder")
}
func (a *Activities) NotifyAuthor(ctx context.Context, in Input) error {
	return a.onceAudit(ctx, in, "asset.author_notified")
}
func (a *Activities) onceAudit(ctx context.Context, in Input, action string) error {
	return a.transaction(ctx, in, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		var exists bool
		if err := db.Tx(ctx).QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM audit_log WHERE tenant_id=$1 AND target_id=$2 AND action=$3 "+
			"AND detail->>'generation'=$4)", in.Tenant, v.ID, action, fmt.Sprint(in.Generation)).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		return assets.Log(ctx, action, "asset_version", v.ID, map[string]any{"generation": in.Generation})
	})
}
func (a *Activities) LoadAssetReview(ctx context.Context, review Review) (Review, error) {
	var out Review
	out.Input = review.Input
	err := a.transaction(ctx, review.Input, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.State != "in_review" {
			return temporal.NewNonRetryableApplicationError("review is closed", "InvalidReview", nil)
		}
		if err := db.Tx(ctx).QueryRow(ctx, "SELECT id::text,reviewer_id::text,decision,comment FROM asset_reviews WHERE tenant_id=$1 "+
			"AND version_id=$2 AND generation=$3 AND id=$4",
			review.Input.Tenant,
			v.ID,
			review.Input.Generation,
			review.ID).Scan(&out.ID,
			&out.Reviewer,
			&out.Decision,
			&out.Comment); err != nil {
			return err
		}
		if out.Reviewer == asset.OwnerID || out.Reviewer == auth.From(ctx).ID || out.Reviewer == v.UploadedBy {
			return temporal.NewNonRetryableApplicationError("author cannot review", "InvalidReview", nil)
		}
		return nil
	})
	return out, err
}
func (a *Activities) DecideAssetReview(ctx context.Context, in Aggregate) (Result, error) {
	result := Result{}
	err := a.transaction(ctx, in.Input, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.State != "in_review" {
			result.State = v.State
			return nil
		}
		var withdrawn bool
		var required, approvals int
		var rejected, changes bool
		if err := db.Tx(ctx).QueryRow(ctx,
			("SELECT withdraw_requested,review_required FROM asset_versions WHERE "+
				"id=$1"),
			v.ID).Scan(&withdrawn,
			&required); err != nil {
			return err
		}
		if withdrawn {
			return temporal.NewNonRetryableApplicationError("withdrawn", "Withdrawn", nil)
		}
		if err := db.Tx(ctx).QueryRow(ctx, "SELECT count(*) FILTER(WHERE decision='approve'),COALESCE(bool_or(decision='reject'),false"+
			"),COALESCE(bool_or(decision='request_changes'),false) FROM asset_reviews WHERE "+
			"tenant_id=$1 AND version_id=$2 AND generation=$3",
			in.Input.Tenant,
			v.ID,
			in.Input.Generation).Scan(&approvals,
			&rejected,
			&changes); err != nil {
			return err
		}
		switch {
		case rejected:
			result.State = "rejected"
		case changes:
			result.State = "changes_requested"
		case approvals >= required:
			result.State = "approved"
		default:
			return temporal.NewNonRetryableApplicationError("insufficient approvals", "InvalidReview", nil)
		}
		return assets.SetState(ctx, v.ID, result.State)
	})
	return result, err
}
func (a *Activities) Sign(ctx context.Context, in Input) error {
	return a.transaction(ctx, in, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.Signature != "" {
			return a.Assets.Signer.Verify(asset, v)
		}
		if v.State != "approved" {
			return fmt.Errorf("version is not approved")
		}
		signature, err := a.Assets.Signer.Sign(asset, v)
		if err != nil {
			return err
		}
		if _, err = db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET signature=$2 WHERE id=$1`, v.ID, signature); err != nil {
			return err
		}
		return assets.Log(ctx, "asset.signed", "asset_version", v.ID, map[string]string{"key_id": a.Assets.Signer.ID})
	})
}
func (a *Activities) Publish(ctx context.Context, in Input) error {
	return a.transaction(ctx, in, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		if v.State == "published" {
			return nil
		}
		if asset.Status != "active" {
			return fmt.Errorf("asset is unavailable")
		}
		if err := a.Assets.Signer.Verify(asset, v); err != nil {
			return err
		}
		if err := assets.SetState(ctx, v.ID, "published"); err != nil {
			return err
		}
		var channel string
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT release_channel FROM asset_versions WHERE id=$1`, v.ID).Scan(&channel); err != nil {
			return err
		}
		if err := a.Assets.SetChannel(ctx, asset, channel, v.ID); err != nil {
			return err
		}
		_, err := db.Tx(ctx).
			Exec(ctx, `UPDATE asset_versions SET published_at=now(),release_error=NULL,release_retryable=true WHERE id=$1`, v.ID)
		if err != nil {
			return err
		}
		_, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET latest_version=$2 WHERE id=$1`, asset.ID, v.Version)
		return err
	})
}
func (a *Activities) CompensateAssetPublish(ctx context.Context, in Input) error {
	return a.transaction(ctx, in, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		// Publish is atomic, so compensation is needed only after an ambiguous commit.
		// Compare-and-swap avoids overwriting a subsequent operator channel change.
		if v.State != "published" {
			return nil
		}
		var channel string
		var previous *string
		if err := db.Tx(ctx).QueryRow(ctx,
			("SELECT release_channel,previous_channel_id::text FROM asset_versions "+
				"WHERE id=$1"),
			v.ID).Scan(&channel,
			&previous); err != nil {
			return err
		}
		if previous == nil {
			if _,
				err := db.Tx(ctx).Exec(ctx,
				("DELETE FROM asset_channels WHERE asset_id=$1 AND channel=$2 AND " +
					"version_id=$3"),
				asset.ID,
				channel,
				v.ID); err != nil {
				return err
			}
		} else {
			if _,
				err := db.Tx(ctx).Exec(ctx,
				("UPDATE asset_channels SET version_id=$4 WHERE asset_id=$1 AND " +
					"channel=$2 AND version_id=$3"),
				asset.ID,
				channel,
				v.ID,
				*previous); err != nil {
				return err
			}
		}
		// Saga restoration is separate from a user-visible lifecycle transition.
		if _, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET state='approved',published_at=NULL WHERE id=$1`, v.ID); err != nil {
			return err
		}
		if _, err := db.Tx(ctx).Exec(ctx, "UPDATE assets SET latest_version=(SELECT version FROM asset_versions WHERE asset_id=$1 "+
			"AND state IN ('published','deprecated') ORDER BY published_at DESC,id DESC LIMIT 1) WHERE "+
			"id=$1 AND latest_version=$2", asset.ID, v.Version); err != nil {
			return err
		}
		return assets.Log(
			ctx,
			"asset.publish_compensated",
			"asset_version",
			v.ID,
			map[string]any{"channel": channel, "previous_version_id": previous},
		)
	})
}
func (a *Activities) AbortAssetRelease(ctx context.Context, failure Failure) (Result, error) {
	out := Result{}
	err := a.transaction(ctx, failure.Input, func(ctx context.Context, asset assets.Asset, v assets.Version) error {
		out.State = v.State
		var requested bool
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT withdraw_requested FROM asset_versions WHERE id=$1`, v.ID).Scan(&requested); err != nil {
			return err
		}
		if (failure.Withdrawn || requested) && v.State == "draft" {
			return nil
		}
		if (failure.Withdrawn || requested) &&
			(v.State == "submitted" || v.State == "checking" || v.State == "in_review") {
			if err := a.Assets.Withdraw(ctx, v.ID); err != nil {
				return err
			}
			out.State = "draft"
			return nil
		}
		if _,
			err := db.Tx(ctx).Exec(ctx,
			("UPDATE asset_versions SET release_error=$2,release_retryable=$3 WHERE " +
				"id=$1"),
			v.ID,
			failure.Code,
			failure.Retryable); err != nil {
			return err
		}
		return assets.Log(
			ctx,
			"asset.release_failed",
			"asset_version",
			v.ID,
			map[string]any{"code": failure.Code, "retryable": failure.Retryable},
		)
	})
	return out, err
}
