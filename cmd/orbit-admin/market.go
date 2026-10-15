package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/market"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
)

func importMarket(ctx context.Context, d *db.DB, c config.Config, args []string) error {
	if len(args) == 0 || args[0] != "import" {
		return fmt.Errorf("usage: orbit-admin market import --tenant UUID --snapshot FILE [--actor UUID]")
	}
	f := flag.NewFlagSet("market import", flag.ContinueOnError)
	tenant := f.String("tenant", "", "target tenant UUID")
	snapshot := f.String("snapshot", "", "gzip NDJSON snapshot")
	actor := f.String("actor", "", "tenant owner (defaults to an active direct tenant owner)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if !ids.Valid(*tenant) || *snapshot == "" || f.NArg() != 0 {
		return fmt.Errorf("tenant and snapshot are required")
	}
	if *actor == "" {
		if err := d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT b.principal_id::text FROM role_bindings b JOIN memberships m ON "+
				"m.tenant_id=b.tenant_id AND m.principal_id=b.principal_id WHERE b.tenant_id=$1 AND "+
				"b.scope_type='tenant' AND b.scope_id=$1 AND b.role='tenant.owner' AND "+
				"b.principal_type='user' AND m.status='active' ORDER BY b.created_at LIMIT 1", *tenant).Scan(actor)
		}); err != nil {
			return err
		}
	}
	store, err := objstore.New(c.ObjectEndpoint, c.ObjectBucket, c.ObjectAccessKey, c.ObjectSecretKey, c.ObjectTLS)
	if err != nil {
		return err
	}
	s := &assets.Service{Store: store}
	result, err := market.Import(ctx, d, s, *tenant, *actor, *snapshot)
	if err != nil {
		return err
	}
	return json.MarshalWrite(os.Stdout, result)
}
