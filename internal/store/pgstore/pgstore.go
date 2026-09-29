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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/mindreon/orbit-control/internal/store"
)

const setTenantSQL = "SELECT set_config('app.tenant_id', $1, true)"

// Store keeps the pgx pool for what gorm cannot do (LISTEN, session advisory locks)
// and a gorm handle on the same pool for everything else. Tenant tables are only ever reached through inTenant,
// which sets the tenant GUC first.
type Store struct {
	pool *pgxpool.Pool
	db   *gorm.DB
}

var _ store.Repository = (*Store)(nil)

// New wraps an existing pool (tests size it explicitly, e.g. MaxConns=1).
func New(pool *pgxpool.Pool) *Store {
	db, err := gorm.Open(
		postgres.New(postgres.Config{Conn: stdlib.OpenDBFromPool(pool)}),
		&gorm.Config{
			// Writes name their columns: orbit_app may only UPDATE some of them, and a full-row Save would be refused.
			// SkipDefaultTransaction because every statement here already runs inside inTenant's transaction.
			SkipDefaultTransaction: true,
			Logger:                 logger.Discard, // statements carry tenant data; failures are reported by the store
			TranslateError:         false,
		},
	)
	if err != nil {
		panic("pgstore: gorm cannot wrap an open pgx pool: " + err.Error()) // Open only fails on a nil or closed pool
	}
	return &Store{pool: pool, db: db}
}

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

// inTenant runs fn in one transaction whose first statement binds the tenant GUC (never concatenated), so
// row-level security still applies to everything gorm runs. Only Model(...).Select(...).Updates and Create are used
// to write; there is no Save, since orbit_app holds column-level UPDATE grants.
func (s *Store) inTenant(ctx context.Context, tenantID string, fn func(tx *gorm.DB) error) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	tx := s.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return SanitizeConnError(tx.Error)
	}
	defer tx.Rollback() // after a commit this is a no-op error
	if err := tx.Exec(setTenantSQL, tenantID).Error; err != nil {
		return storageErr("set tenant", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit().Error; err != nil {
		return storageErr("commit", err)
	}
	return nil
}

func (s *Store) CheckTenant(ctx context.Context, tenantID string) error {
	var ok bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, tenantID).Scan(&ok); err != nil {
		return storageErr("check tenant", err)
	}
	if !ok {
		return store.ErrNotFound
	}
	return nil
}
