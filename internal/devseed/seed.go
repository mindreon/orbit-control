// Package devseed provisions development resources through the normal asset pipeline.
package devseed

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/secrets"
	"github.com/mindreon/orbit-control/internal/tenancy"
	seeds "github.com/mindreon/orbit-control/testdata"
)

type Model struct {
	BaseURL string
	APIKey  contract.Secret
	Name    string
}
type Result struct {
	TenantID    string   `json:"tenant_id"`
	PrincipalID string   `json:"principal_id"`
	WorkspaceID string   `json:"workspace_id"`
	Versions    []string `json:"versions"`
}

func Seed(ctx context.Context, d *db.DB, svc *assets.Service, keys secrets.Keys, model Model) (Result, error) {
	var out Result
	var p auth.Principal
	err := d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text,kind FROM principals WHERE idp_subject=$1`, auth.DevIssuer+"|dev@orbit.local").
			Scan(&p.ID, &p.Kind)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		p, err = tenancy.CreateUser(
			ctx,
			d,
			"dev@orbit.local",
			"Orbit developer",
			auth.DevIssuer+"|dev@orbit.local",
			"",
			"",
			auth.Principal{},
		)
	}
	if err != nil {
		return out, err
	}
	out.PrincipalID = p.ID
	err = d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text FROM tenants WHERE slug='dev'`).Scan(&out.TenantID)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		tenant, e := tenancy.CreateTenant(ctx, d, p, "dev", "Development")
		err = e
		out.TenantID = tenant.ID
	}
	if err != nil {
		return out, err
	}
	out.WorkspaceID = ids.Derive(out.TenantID, "workspace:dev")
	err = d.WithTenant(ctx, out.TenantID, func(tx pgx.Tx) error {
		ctx = auth.Context(db.Context(ctx, tx), p, out.TenantID)
		// Serialize concurrent seed invocations, including immutable version creation.
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,8))`, out.TenantID); e != nil {
			return e
		}
		if err := seedWorkspaceModel(ctx, tx, p, out, keys, model); err != nil {
			return err
		}
		if err := seedAgents(ctx, tx, svc, &out); err != nil {
			return err
		}
		return audit.Append(
			ctx,
			tx,
			p,
			out.TenantID,
			"dev.seed",
			"workspace",
			out.WorkspaceID,
			map[string]any{"models": []string{"default"}, "assets": []string{"echo-agent", "notify-agent"}},
		)
	})
	return out, err
}
func WaitPublished(ctx context.Context, d *db.DB, result Result) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		done := true
		err := d.WithTenant(ctx, result.TenantID, func(tx pgx.Tx) error {
			for _, id := range result.Versions {
				var state string
				var failure *string
				if err := tx.QueryRow(ctx,
					(`SELECT state,release_error
 FROM asset_versions
 WHERE tenant_id=$1
 AND id=$2`),
					result.TenantID,
					id).Scan(&state,
					&failure); err != nil {
					return err
				}
				if state == "check_failed" || state == "rejected" || failure != nil {
					return fmt.Errorf("seed asset %s release failed; inspect its check reports", id)
				}
				done = done && state == "published"
			}
			return nil
		})
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("seed assets await asset-release and asset-check workers: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func seedWorkspaceModel(ctx context.Context, tx pgx.Tx, p auth.Principal, out Result, keys secrets.Keys, model Model) error {
	if _,
		e := tx.Exec(ctx,
		(`INSERT INTO workspaces(id,tenant_id,name)
 VALUES($1,$2,'dev')
 ON CONFLICT DO NOTHING`),
		out.WorkspaceID,
		out.TenantID); e != nil {
		return e
	}
	if _, e := tx.Exec(ctx, `INSERT INTO workspace_members(tenant_id,workspace_id,principal_id)
 VALUES($1,$2,$3)
 ON CONFLICT DO NOTHING`, out.TenantID, out.WorkspaceID, p.ID); e != nil {
		return e
	}
	if e := tenancy.Bind(ctx, tx, p, out.TenantID, p.Kind, p.ID, "ws.admin", "workspace", out.WorkspaceID); e != nil {
		return e
	}
	secretID, e := keys.Put(ctx, out.TenantID, p.ID, "MOCK_KEY", "tenant", []byte(model.APIKey))
	if e != nil {
		return e
	}
	pricing, e := json.Marshal(contract.ModelPricing{InputPerMtokMillicredits: 1000, OutputPerMtokMillicredits: 2000})
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE model_profiles SET is_default=false WHERE tenant_id=$1 AND name<>'default'`, out.TenantID); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model,secret_id,pricing,context_window,is_default)
 VALUES($1,$2,'default','openai',$3,$4,$5,$6,32768,true)
 ON CONFLICT(tenant_id,name)
 DO UPDATE SET base_url=EXCLUDED.base_url,model=EXCLUDED.model,secret_id=EXCLUDED.secret_id,
 pricing=EXCLUDED.pricing,is_default=true`,
		ids.Derive(out.TenantID, "model:default"),
		out.TenantID,
		model.BaseURL,
		model.Name,
		secretID,
		pricing); e != nil {
		return e
	}
	return nil
}
func seedAgents(ctx context.Context, tx pgx.Tx, svc *assets.Service, out *Result) error {
	var e error
	for _, name := range []string{"echo-agent", "notify-agent"} {
		var a assets.Asset
		var assetID string
		e = tx.QueryRow(ctx, `SELECT id::text
 FROM assets
 WHERE tenant_id=$1
 AND publisher='dev'
 AND kind='agent'
 AND name=$2`, out.TenantID, name).
			Scan(&assetID)
		if errors.Is(e, pgx.ErrNoRows) {
			a, e = svc.Create(ctx, "agent", name, "private", out.WorkspaceID)
		} else if e == nil {
			a, e = svc.Get(ctx, assetID, "asset.update")
		}
		if e != nil {
			return e
		}
		var versionID string
		e = tx.QueryRow(ctx, `SELECT id::text FROM asset_versions WHERE tenant_id=$1 AND asset_id=$2 AND version='1.0.0'`, out.TenantID, a.ID).
			Scan(&versionID)
		if errors.Is(e, pgx.ErrNoRows) {
			files := map[string][]byte{}
			entries, e := fs.ReadDir(seeds.Bundles, "bundles/"+name)
			if e != nil {
				return e
			}
			for _, entry := range entries {
				data, e := fs.ReadFile(seeds.Bundles, "bundles/"+name+"/"+entry.Name())
				if e != nil {
					return e
				}
				files[entry.Name()] = data
			}
			archive, e := bundle.Pack(files)
			if e != nil {
				return e
			}
			upload, e := svc.PrepareUpload(ctx, "1.0.0", archive)
			if e != nil {
				return e
			}
			version, e := svc.Upload(ctx, a, upload)
			if e != nil {
				return e
			}
			versionID = version.ID
			if e = svc.Submit(ctx, a, versionID, "stable"); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		out.Versions = append(out.Versions, versionID)
	}
	return nil
}
