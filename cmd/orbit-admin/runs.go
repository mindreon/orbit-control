package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/devseed"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/secrets"
)

func runAdmin(ctx context.Context, d *db.DB, c config.Config, args []string) error {
	keys := secrets.Keys{Current: c.SecretsKeyVersion, KEKs: c.SecretsKEKs}
	if len(keys.KEKs[keys.Current]) != 32 {
		return fmt.Errorf("ORBIT_SECRETS_KEK or ORBIT_KEK_FILE required")
	}
	if args[0] == "seed-dev" {
		return seedDevelopment(ctx, d, c, keys, args)
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	tenant := f.String("tenant", "", "tenant UUID")
	actor := f.String("actor", "", "actor UUID")
	name := f.String("name", "", "secret or model profile name")
	scope := f.String("scope", "tenant", "secret scope")
	base := f.String("base-url", "", "OpenAI-compatible base URL")
	model := f.String("model", "", "model name")
	secretID := f.String("secret", "", "secret UUID")
	pricing := f.String(
		"pricing",
		"{\"input_per_mtok_millicredits\":1000,\"output_per_mtok_millicredits\":2000,"+
			"\"cache_read_per_mtok_millicredits\":0,\"cache_write_per_mtok_millicredits\":0}",
		"ModelPricing JSON",
	)
	def := f.Bool("default", false, "default model profile")
	window := f.Int("context-window", 32768, "context tokens")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || !ids.Valid(*tenant) || !ids.Valid(*actor) || *name == "" {
		return fmt.Errorf("--tenant, --actor UUID and --name required")
	}
	return d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
		ctx = auth.Context(db.Context(ctx, tx), auth.Principal{ID: *actor, Kind: "user"}, *tenant)
		var id string
		switch args[0] {
		case "put-secret":
			raw, e := io.ReadAll(io.LimitReader(os.Stdin, (64<<10)+1))
			if e != nil {
				return e
			}
			if len(raw) == 0 || len(raw) > 64<<10 {
				return fmt.Errorf("secret stdin must contain 1..65536 bytes")
			}
			id, e = keys.Put(ctx, *tenant, *actor, *name, *scope, []byte(strings.TrimSuffix(string(raw), "\n")))
			if e != nil {
				return e
			}
		case "put-model-profile":
			if !validModelURL(*base) || *model == "" || !ids.Valid(*secretID) || *window < 1 {
				return fmt.Errorf("valid --base-url, --model, --secret UUID and --context-window required")
			}
			if err := contract.Validate("ModelPricing", []byte(*pricing)); err != nil {
				return fmt.Errorf("invalid --pricing")
			}
			if *def {
				if _, e := tx.Exec(ctx, `UPDATE model_profiles SET is_default=false WHERE tenant_id=$1`, *tenant); e != nil {
					return e
				}
			}
			e := tx.QueryRow(ctx, `INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model,secret_id,pricing,context_window,is_default)
 VALUES($1,$2,$3,'openai',$4,$5,$6,$7,$8,$9)
 ON CONFLICT(tenant_id,name)
 DO UPDATE SET base_url=EXCLUDED.base_url,model=EXCLUDED.model,secret_id=EXCLUDED.secret_id,
 pricing=EXCLUDED.pricing,context_window=EXCLUDED.context_window,is_default=EXCLUDED.is_default
 RETURNING id::text`, ids.New(), *tenant, *name, *base, *model, *secretID, *pricing, *window, *def).
				Scan(&id)
			if e != nil {
				return e
			}
		default:
			return errors.New("unknown run admin command")
		}
		if err := audit.Append(ctx, tx, auth.From(ctx), *tenant, args[0], "configuration", id, map[string]string{"name": *name}); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, id)
		return nil
	})
}
func validModelURL(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil &&
		u.RawQuery == "" &&
		u.Fragment == ""
}

func seedDevelopment(ctx context.Context, d *db.DB, c config.Config, keys secrets.Keys, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("seed-dev takes no arguments")
	}
	if c.Environment != "dev" {
		return fmt.Errorf("seed-dev requires ORBIT_ENV=dev")
	}
	if err := c.ValidateAssets(); err != nil {
		return err
	}
	store, err := objstore.New(c.ObjectEndpoint, c.ObjectBucket, c.ObjectAccessKey, c.ObjectSecretKey, c.ObjectTLS)
	if err != nil {
		return err
	}
	signer, err := assets.LoadSigner(c.AssetKeyFile, c.AssetKeyID, true, c.AssetPublicKeys)
	if err != nil {
		return err
	}
	model, err := devseed.ModelFromEnv()
	if err != nil {
		return err
	}
	if !validModelURL(model.BaseURL) {
		return fmt.Errorf("ORBIT_SEED_MODEL_BASE_URL must be an HTTP URL without credentials")
	}
	svc := &assets.Service{Store: store, Signer: signer}
	result, err := devseed.Seed(ctx, d, svc, keys, model, c.Environment)
	if err != nil {
		return err
	}
	if err = devseed.WaitPublished(ctx, d, svc, result, c.Environment); err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(raw))
	return nil
}
