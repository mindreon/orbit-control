package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

// S-DB-11 (b): with a pool of one connection, request A sets the tenant and
// finishes; request B reuses the same backend and sees no tenant and no rows.
func TestSDB11bPooledConnectionDoesNotLeakTenant(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	repo, pool := appStore(t, 1)
	const tenant, user = "t-sdb11", "u-sdb11"
	if err := repo.EnsureTenant(ctx, tenant, tenant); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertUser(ctx, tenant, store.UserRecord{ID: user, Issuer: "orbit-local", Subject: user}); err != nil {
		t.Fatal(err)
	}
	runtime, _ := json.Marshal(map[string]string{"kernel": "agentscope"})
	if _, _, err := repo.CreateRoom(ctx, tenant, store.RoomRecord{
		ID: "rm_sdb11", CreatedBy: user, Kind: "solo", State: "idle",
		PermissionPreset: "workspace-write", Runtime: runtime, CreatedAt: time.Now().UTC(),
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Request A: committed tenant transaction through the repository.
	rooms, err := repo.ListRooms(ctx, tenant, user)
	if err != nil || len(rooms) != 1 {
		t.Fatalf("request A saw %d rooms, err=%v", len(rooms), err)
	}
	var pidA, seenA int
	asTenant(t, pool, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT pg_backend_pid(), (SELECT count(*) FROM rooms)`).Scan(&pidA, &seenA)
	})
	if seenA != 1 {
		t.Fatalf("request A tenant tx saw %d rooms, want 1", seenA)
	}
	// Request A': a tenant transaction that rolls back on error.
	if _, err := repo.GetRoom(ctx, tenant, user, "rm_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetRoom missing = %v", err)
	}

	// Request B: same pooled connection, no tenant transaction.
	var pidB int
	var guc *string
	if err := pool.QueryRow(ctx, `SELECT pg_backend_pid(), current_setting('app.tenant_id', true)`).Scan(&pidB, &guc); err != nil {
		t.Fatal(err)
	}
	if pidB != pidA {
		t.Fatalf("pool did not reuse the connection: pid %d vs %d", pidA, pidB)
	}
	if guc != nil && *guc != "" {
		t.Fatalf("app.tenant_id leaked to the next request: %q", *guc)
	}
	for _, table := range []string{"rooms", "users", "messages", "idempotency_keys"} {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("request B without tenant saw %d rows in %s", n, table)
		}
	}
	if total := pool.Stat().TotalConns(); total != 1 {
		t.Fatalf("pool has %d connections, want 1", total)
	}
}
