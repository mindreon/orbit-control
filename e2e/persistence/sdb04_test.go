//go:build e2e

package persistence

import (
	"context"
	"testing"
)

// S-DB-4 (ISO-11): the schema cannot hold IdP tokens, secrets or passwords.
func TestSDB04NoSecretColumns(t *testing.T) {
	const c = "S-DB-4"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)

	colSQL := `SELECT table_name || '.' || column_name FROM information_schema.columns
	            WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
	              AND column_name ~* '(token|secret|password|passwd|refresh|access|cookie|credential|api_?key)'
	            ORDER BY 1`
	rows, err := ownerPool.Query(ctx, colSQL)
	if err != nil {
		t.Fatal(err)
	}
	hits := []string{}
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		hits = append(hits, s)
	}
	rows.Close()
	iso(t, "S-DB-4/no-token-columns", c, []string{"FM-32"}, "no column anywhere in the schema can hold an IdP token, secret, password or cookie",
		sqlReq{Role: "orbit_owner", SQL: colSQL}, []string{}, hits, len(hits) == 0)
}
