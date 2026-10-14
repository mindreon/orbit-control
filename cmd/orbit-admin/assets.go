package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func retryAssetRelease(ctx context.Context, d *db.DB, args []string) error {
	if len(args) == 0 || args[0] != "retry-release" {
		return fmt.Errorf("asset requires retry-release --tenant UUID --version UUID")
	}
	f := flag.NewFlagSet("asset retry-release", flag.ContinueOnError)
	tenant := f.String("tenant", "", "tenant UUID")
	version := f.String("version", "", "version UUID")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || !ids.Valid(*tenant) || !ids.Valid(*version) {
		return fmt.Errorf("--tenant and --version UUID required")
	}
	return d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
		// Admin connection is privileged; every query and the audit remain tenant-filtered.
		var actor auth.Principal
		if err := tx.QueryRow(ctx, `SELECT p.id::text,p.kind FROM role_bindings b JOIN memberships m ON m.tenant_id=b.tenant_id AND m.principal_id=b.principal_id JOIN principals p ON p.id=b.principal_id WHERE b.tenant_id=$1 AND b.scope_type='tenant' AND b.scope_id=$1 AND b.role='tenant.owner' AND b.principal_type='user' AND m.status='active' AND p.status='active' ORDER BY b.created_at LIMIT 1`, *tenant).Scan(&actor.ID, &actor.Kind); err != nil {
			return err
		}
		ctx := auth.Context(db.Context(ctx, tx), actor, *tenant)
		return (&assets.Service{}).RetryRelease(ctx, *version)
	})
}
