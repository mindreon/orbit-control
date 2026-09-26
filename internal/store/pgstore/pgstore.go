// Package pgstore is the Postgres Repository (contract §18). It connects as
// the non-owner orbit_app role. Every tenant statement runs inside a
// transaction that first executes
//
//	SELECT set_config('app.tenant_id', $1, true)
//
// so the GUC is transaction-local and bound, never concatenated (§18.5
// HIGH 2). Queries still filter by tenant_id explicitly; RLS is the backstop.
package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/store"
)

const setTenantSQL = "SELECT set_config('app.tenant_id', $1, true)"

const (
	sqlstateUniqueViolation     = "23505"
	sqlstateForeignKeyViolation = "23503"
	sqlstateInsufficientPriv    = "42501"
)

type Store struct {
	pool *pgxpool.Pool
}

var _ store.Repository = (*Store)(nil)

// New wraps an existing pool (tests size it explicitly, e.g. MaxConns=1).
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Open connects with ORBIT_CONTROL_DB_URL. Errors never echo the URL.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("control db: ORBIT_CONTROL_DB_URL is not a valid connection string")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, SanitizeConnError(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, SanitizeConnError(err)
	}
	return New(pool), nil
}

func (s *Store) Close() { s.pool.Close() }

// SanitizeConnError reduces a connection failure to a host placeholder and an
// error code (§18.6): no DSN, user, password, or host.
func SanitizeConnError(err error) error {
	code := "connect_failed"
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code = pgErr.Code
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
	}
	return fmt.Errorf("%w: control db connect failed (host=<db-host>, code=%s)", store.ErrStorage, code)
}

func storageErr(op string, err error) error {
	return fmt.Errorf("%w (%s): %v", store.ErrStorage, op, err)
}

func pgCode(err error) (string, string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code, pgErr.ConstraintName
	}
	return "", ""
}

// childWriteErr maps a rejected child-row write to ErrNotFound: an RLS
// WITH CHECK failure (parent room deleted or foreign) or a missing parent.
func childWriteErr(op string, err error) error {
	switch code, _ := pgCode(err); code {
	case sqlstateInsufficientPriv, sqlstateForeignKeyViolation:
		return store.ErrNotFound
	}
	return storageErr(op, err)
}

// inTenantTx is the only way this package touches [T] tables.
func (s *Store) inTenantTx(ctx context.Context, tenantID string, fn func(pgx.Tx) error) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SanitizeConnError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, setTenantSQL, tenantID); err != nil {
		return storageErr("set tenant", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return storageErr("commit", err)
	}
	return nil
}

func (s *Store) EnsureTenant(ctx context.Context, tenantID, name string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		tenantID, name)
	if err != nil {
		return storageErr("ensure tenant", err)
	}
	return nil
}

func (s *Store) UpsertUser(ctx context.Context, tenantID string, u store.UserRecord) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO users (id, tenant_id, iss, sub, display_name, email)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING`,
			u.ID, tenantID, u.Issuer, u.Subject, u.DisplayName, u.Email)
		if err != nil {
			return storageErr("upsert user", err)
		}
		return nil
	})
}
