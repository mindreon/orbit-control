// Package devseed provisions development resources through the normal asset pipeline.
package devseed

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
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
	ReviewerID  string   `json:"reviewer_id"`
	WorkspaceID string   `json:"workspace_id"`
	Versions    []string `json:"versions"`
}

func ModelFromEnv() (Model, error) {
	model := Model{BaseURL: os.Getenv("ORBIT_SEED_MODEL_BASE_URL"),
		APIKey: contract.Secret(os.Getenv("ORBIT_SEED_MODEL_API_KEY")), Name: os.Getenv("ORBIT_SEED_MODEL_NAME")}
	return model, model.validate()
}
func (m Model) validate() error {
	for _, value := range []struct{ name, value string }{
		{name: "ORBIT_SEED_MODEL_BASE_URL", value: m.BaseURL},
		{name: "ORBIT_SEED_MODEL_API_KEY", value: string(m.APIKey)},
		{name: "ORBIT_SEED_MODEL_NAME", value: m.Name},
	} {
		if strings.TrimSpace(value.value) == "" {
			return fmt.Errorf("%s is required", value.name)
		}
	}
	return nil
}

func Seed(ctx context.Context, d *db.DB, svc *assets.Service, keys secrets.Keys, model Model, environment string) (Result, error) {
	var out Result
	if environment != "dev" {
		return out, fmt.Errorf("seed-dev requires ORBIT_ENV=dev")
	}
	if err := model.validate(); err != nil {
		return out, err
	}
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
		if err := seedReviewer(ctx, tx, p, &out); err != nil {
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
func WaitPublished(ctx context.Context, d *db.DB, svc *assets.Service, result Result, environment string) error {
	if environment != "dev" {
		return fmt.Errorf("seed-dev requires ORBIT_ENV=dev")
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		done := true
		err := d.WithTenant(ctx, result.TenantID, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: result.ReviewerID, Kind: "user"}, result.TenantID)
			for _, id := range result.Versions {
				state, err := reviewSeed(ctx, tx, svc, result.TenantID, id)
				if err != nil {
					return err
				}
				done = done && state == "published"
			}
			return nil
		})
		if err != nil {
			if ctx.Err() != nil {
				return seedWaitTimeout(ctx)
			}
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return seedWaitTimeout(ctx)
		case <-ticker.C:
		}
	}
}

func seedWaitTimeout(ctx context.Context) error {
	return fmt.Errorf("timed out waiting for seed AssetReleaseWorkflow publication (ctl/asset-check workers and dev review): %w", ctx.Err())
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
	secretID, e := keys.Put(ctx, out.TenantID, p.ID, "MODEL_KEY", "tenant", []byte(model.APIKey))
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
	if _, e = tx.Exec(ctx,
		`INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model,secret_id,pricing,context_window,is_default)
 VALUES($1,$2,'default','openai',$3,$4,$5,$6,32768,true)
 ON CONFLICT(tenant_id,name)
 DO UPDATE SET base_url=EXCLUDED.base_url,model=EXCLUDED.model,secret_id=EXCLUDED.secret_id,
 pricing=EXCLUDED.pricing,is_default=true`, ids.Derive(out.TenantID, "model:default"), out.TenantID,
		model.BaseURL, model.Name, secretID, pricing); e != nil {
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
