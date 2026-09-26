package pgstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/store/migrations"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
)

// The S-DB tests need a real Postgres prepared with
// deploy/postgres/bootstrap-roles.sql:
//
//	ORBIT_TEST_DB_URL          orbit_app   (non-owner, NOBYPASSRLS)
//	ORBIT_TEST_MIGRATE_DB_URL  orbit_owner (migrations, audit reads)
//
// Without them the DB tests skip so `go test ./...` stays service-free; CI
// sets ORBIT_TEST_REQUIRE_DB=1 so a missing database fails instead.
var (
	appURL   = os.Getenv("ORBIT_TEST_DB_URL")
	ownerURL = os.Getenv("ORBIT_TEST_MIGRATE_DB_URL")
)

func TestMain(m *testing.M) {
	if appURL == "" || ownerURL == "" {
		if os.Getenv("ORBIT_TEST_REQUIRE_DB") == "1" {
			fmt.Fprintln(os.Stderr, "ORBIT_TEST_REQUIRE_DB=1 but ORBIT_TEST_DB_URL / ORBIT_TEST_MIGRATE_DB_URL are unset")
			os.Exit(1)
		}
		os.Exit(m.Run())
	}
	ctx := context.Background()
	// §18.2: CI runs up → down → up.
	for _, step := range []struct {
		name string
		run  func(context.Context, string) error
	}{{"up", migrations.Up}, {"down", migrations.Reset}, {"up", migrations.Up}} {
		if err := step.run(ctx, ownerURL); err != nil {
			fmt.Fprintf(os.Stderr, "migrations %s: %v\n", step.name, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func requirePG(t *testing.T) {
	t.Helper()
	if appURL == "" || ownerURL == "" {
		t.Skip("ORBIT_TEST_DB_URL / ORBIT_TEST_MIGRATE_DB_URL not set")
	}
}

func newPool(t *testing.T, url string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func appStore(t *testing.T, maxConns int32) (*pgstore.Store, *pgxpool.Pool) {
	pool := newPool(t, appURL, maxConns)
	return pgstore.New(pool), pool
}

// asTenant runs fn as orbit_app inside a transaction scoped to tenantID, the
// same way the repository does.
func asTenant(t *testing.T, pool *pgxpool.Pool, tenantID string, fn func(pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
			return err
		}
		return fn(tx)
	})
	if err != nil {
		t.Fatalf("tenant tx: %v", err)
	}
}

func countAs(t *testing.T, pool *pgxpool.Pool, tenantID, query string, args ...any) int {
	t.Helper()
	var n int
	asTenant(t, pool, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(context.Background(), query, args...).Scan(&n)
	})
	return n
}
