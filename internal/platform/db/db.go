package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type DB struct{ pool *pgxpool.Pool }
type txKey struct{}

func Open(ctx context.Context, url string) (*DB, error) {
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		return nil, fmt.Errorf("database configuration invalid")
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, fmt.Errorf("database connection failed")
	}
	return &DB{pool: p}, nil
}
func (d *DB) Close()                         { d.pool.Close() }
func (d *DB) Ping(ctx context.Context) error { return d.pool.Ping(ctx) }

// Global is an explicit boundary for principals/tenants and membership discovery.
// Tenant business queries must use WithTenant instead.
func (d *DB) Global(ctx context.Context, fn func(pgx.Tx) error) error {
	return d.transaction(ctx, "", fn)
}
func (d *DB) WithTenant(ctx context.Context, tenantID string, fn func(pgx.Tx) error) error {
	if !ids.Valid(tenantID) {
		return fmt.Errorf("invalid tenant id")
	}
	return d.transaction(ctx, tenantID, fn)
}
func (d *DB) transaction(ctx context.Context, tenantID string, fn func(pgx.Tx) error) error {
	tx, e := d.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if tenantID != "" {
		if _, e = tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", tenantID); e != nil {
			return e
		}
	}
	if e = fn(tx); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func Context(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}
func Tx(ctx context.Context) pgx.Tx { return ctx.Value(txKey{}).(pgx.Tx) }

// OpenApp rejects privileged credentials before serving tenant HTTP requests.
func OpenApp(ctx context.Context, url string) (*DB, error) {
	d, e := Open(ctx, url)
	if e != nil {
		return nil, e
	}
	e = d.Global(ctx, func(tx pgx.Tx) error {
		var name string
		var privileged bool
		if e := tx.QueryRow(ctx,
			("SELECT rolname,rolsuper OR rolbypassrls FROM pg_roles WHERE "+
				"rolname=current_user")).Scan(&name,
			&privileged); e != nil {
			return e
		}
		if name != "orbit_app" || privileged {
			return fmt.Errorf("ORBIT_DATABASE_URL must use the unprivileged orbit_app role")
		}
		return nil
	})
	if e != nil {
		d.Close()
		return nil, e
	}
	return d, nil
}
