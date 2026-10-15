package devseed

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

// The dev-only CLI provisions a distinct reviewer with no dev-login identity.
func seedReviewer(ctx context.Context, tx pgx.Tx, author auth.Principal, out *Result) error {
	out.ReviewerID = ids.Derive(out.TenantID, "seed-dev:reviewer")
	if _, err := tx.Exec(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject,email_verified)
 VALUES($1,'user','seed-reviewer@orbit.invalid','Development seed reviewer',$2,true)
 ON CONFLICT DO NOTHING`, out.ReviewerID, "orbit:seed-dev-reviewer:"+out.TenantID); err != nil {
		return err
	}
	var member bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships WHERE tenant_id=$1 AND principal_id=$2)`,
		out.TenantID, out.ReviewerID).Scan(&member); err != nil {
		return err
	}
	if member {
		return nil
	}
	return tenancy.AddMember(ctx, tx, author, out.TenantID, out.ReviewerID, "active", "direct", "tenant.reviewer")
}

func reviewSeed(ctx context.Context, tx pgx.Tx, svc *assets.Service, tenant, id string) (string, error) {
	var state string
	var failure *string
	if err := tx.QueryRow(ctx, `SELECT state,release_error
 FROM asset_versions
 WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenant, id).Scan(&state, &failure); err != nil {
		return "", err
	}
	if state == "check_failed" || state == "rejected" || failure != nil {
		reports, err := tx.Query(ctx, `SELECT findings FROM asset_check_reports
 WHERE tenant_id=$1 AND version_id=$2 ORDER BY "check"`, tenant, id)
		if err != nil {
			return "", err
		}
		defer reports.Close()
		var blocked []contract.AssetCheckFinding
		for reports.Next() {
			var raw []byte
			if err := reports.Scan(&raw); err != nil {
				return "", err
			}
			var report contract.AssetCheckReport
			if err := json.Unmarshal(raw, &report); err != nil {
				return "", err
			}
			for _, finding := range report.Findings {
				if finding.Severity == "block" {
					blocked = append(blocked, finding)
				}
			}
		}
		if err := reports.Err(); err != nil {
			return "", err
		}
		errorCode := ""
		if failure != nil {
			errorCode = *failure
		}
		raw, err := json.Marshal(blocked)
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("seed asset %s release failed (state=%s, error=%s); block findings: %s", id, state, errorCode, raw)
	}
	if state == "in_review" {
		var voted bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asset_reviews
 WHERE tenant_id=$1 AND version_id=$2 AND reviewer_id=$3
 AND generation=(SELECT release_generation FROM asset_versions WHERE tenant_id=$1 AND id=$2))`,
			tenant, id, auth.From(ctx).ID).Scan(&voted); err != nil {
			return "", err
		}
		if !voted {
			if _, err := svc.Review(ctx, id, "approve", "seed-dev: approve embedded development seed after automatic checks"); err != nil {
				return "", err
			}
		}
	}
	return state, nil
}
