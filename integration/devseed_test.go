package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/devseed"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/secrets"
)

func testDevSeed(t *testing.T, d *db.DB) {
	ctx := t.Context()
	signer, err := assets.LoadSigner(t.TempDir()+"/assets.key", "test", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc := &assets.Service{Store: testBlobs{root: t.TempDir()}, Signer: signer}
	keys := secrets.Keys{Current: 1, KEKs: map[int][]byte{1: make([]byte, 32)}}
	model := devseed.Model{BaseURL: "https://model.example/v1", APIKey: contract.Secret("seed-test-key"), Name: "real-model"}
	first, err := devseed.Seed(ctx, d, svc, keys, model, "dev")
	if err != nil {
		t.Fatal(err)
	}
	second, err := devseed.Seed(ctx, d, svc, keys, model, "dev")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatalf("non-idempotent seed: %s %s", a, b)
	}
	err = d.WithTenant(ctx, first.TenantID, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: first.PrincipalID, Kind: "user"}, first.TenantID)
		var url, name, secretID, secretName string
		var pricing []byte
		if err := tx.QueryRow(ctx, `SELECT m.base_url,m.model,m.secret_id::text,m.pricing,s.name
 FROM model_profiles m JOIN secrets s ON s.tenant_id=m.tenant_id AND s.id=m.secret_id
 WHERE m.tenant_id=$1 AND m.name='default' AND m.is_default`, first.TenantID).
			Scan(&url, &name, &secretID, &pricing, &secretName); err != nil {
			return err
		}
		plain, err := keys.Get(ctx, first.TenantID, secretID)
		if err != nil {
			return err
		}
		var p contract.ModelPricing
		if err := json.Unmarshal(pricing, &p); err != nil {
			return err
		}
		if url != model.BaseURL || name != model.Name || secretName != "MODEL_KEY" || string(plain) != string(model.APIKey) ||
			p.InputPerMtokMillicredits != 1000 || p.OutputPerMtokMillicredits != 2000 {
			t.Fatal("incorrect seed model/secret/pricing")
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM asset_versions WHERE tenant_id=$1`, first.TenantID).Scan(&count); err != nil {
			return err
		}
		if count != 3 {
			t.Fatalf("seed created %d versions", count)
		}
		for _, id := range first.Versions {
			var assetID, state string
			if err := tx.QueryRow(ctx, `SELECT asset_id::text,state FROM asset_versions WHERE tenant_id=$1 AND id=$2`, first.TenantID, id).
				Scan(&assetID, &state); err != nil {
				return err
			}
			_, err := svc.Get(ctx, assetID, "asset.read")
			if err != nil {
				return err
			}
			version, err := svc.Version(ctx, assetID, id)
			if err != nil {
				return err
			}
			if state != "submitted" || version.Signature != "" {
				t.Fatal(state)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	testSeedReviewWait(t, d, svc, first)
}

func testSeedReviewWait(t *testing.T, d *db.DB, svc *assets.Service, seed devseed.Result) {
	ctx := t.Context()
	if seed.ReviewerID == seed.PrincipalID {
		t.Fatal("seed reviewer is the author")
	}
	err := d.WithTenant(ctx, seed.TenantID, func(tx pgx.Tx) error {
		// Isolate the review adapter from Temporal; real Workflow checks run in compose acceptance.
		_, err := tx.Exec(ctx, `UPDATE asset_versions SET state='in_review' WHERE tenant_id=$1 AND id=ANY($2::uuid[])`,
			seed.TenantID, seed.Versions)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		wait, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		err = devseed.WaitPublished(wait, d, svc, seed, "dev")
		cancel()
		if err == nil || !strings.Contains(err.Error(), "timed out waiting") {
			t.Fatalf("unexpected wait result: %v", err)
		}
	}
	err = d.WithTenant(ctx, seed.TenantID, func(tx pgx.Tx) error {
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM asset_reviews WHERE tenant_id=$1 AND reviewer_id=$2`,
			seed.TenantID, seed.ReviewerID).Scan(&count); err != nil {
			return err
		}
		if count != 3 {
			t.Fatalf("non-idempotent seed approvals: %d", count)
		}
		if _, err := tx.Exec(ctx, `UPDATE asset_versions SET state='check_failed' WHERE tenant_id=$1 AND id=$2`,
			seed.TenantID, seed.Versions[0]); err != nil {
			return err
		}
		report := contract.AssetCheckReport{Check: "secrets", CheckerVersion: "test", Findings: []contract.AssetCheckFinding{
			{Check: "secrets", Severity: "block", RuleId: "test.block", Message: "literal credential blocked"},
		}}
		raw, err := json.Marshal(report)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO asset_check_reports(id,tenant_id,version_id,"check",status,findings)
 VALUES($1,$2,$3,'secrets','fail',$4)`, ids.New(), seed.TenantID, seed.Versions[0], raw)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	err = devseed.WaitPublished(ctx, d, svc, seed, "dev")
	if err == nil || !strings.Contains(err.Error(), "test.block") {
		t.Fatalf("blocking findings not reported: %v", err)
	}
}
