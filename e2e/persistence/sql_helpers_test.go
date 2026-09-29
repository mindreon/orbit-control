//go:build e2e

package persistence

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sqlReq struct {
	Role   string `json:"role"`
	Tenant string `json:"tenant,omitempty"`
	SQL    string `json:"sql"`
	Args   []any  `json:"args,omitempty"`
}

// iso records an isolated (SQL, role or static) check.
func iso(t *testing.T, id, contract string, fms []string, desc string, req any, expected, actual any, pass bool) bool {
	t.Helper()
	return record(t, caseInput{ID: id, Contract: contract, Kind: "isolated", FailureModes: fms,
		Description: desc, Request: req, Expected: expected, Actual: actual, Pass: pass})
}

func ownerScalar[T any](t *testing.T, owner *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := owner.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("owner query %q: %v", query, err)
	}
	return v
}

// execAsApp runs one statement as orbit_app with the tenant GUC set, and returns the affected row count.
func execAsApp(ctx context.Context, pool *pgxpool.Pool, tenant, sql string, args ...any) (int64, error) {
	var n int64
	err := asTenant(ctx, pool, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, sql, args...)
		n = tag.RowsAffected()
		return err
	})
	return n, err
}
