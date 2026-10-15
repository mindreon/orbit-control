package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/internal/usage"
)

func testPartitionTimeZone(t *testing.T, admin *pgx.Conn) {
	ctx := t.Context()
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL ROLE orbit_owner; SET LOCAL TimeZone='America/New_York'"); err != nil {
		t.Fatal(err)
	}
	for _, parent := range []string{"run_events", "usage_ledger", "audit_log"} {
		if _, err = tx.Exec(ctx, "SELECT ensure_monthly_partitions($1,8)", parent); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(ctx, "SET LOCAL TimeZone='UTC'"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, parent := range []string{"run_events", "usage_ledger", "audit_log"} {
		for month := range 9 {
			from := start.AddDate(0, month, 0)
			to := from.AddDate(0, 1, 0)
			table := parent + "_" + from.Format("200601")
			var bound string
			if err = tx.QueryRow(ctx, "SELECT pg_get_expr(relpartbound,oid) FROM pg_class WHERE relname=$1", table).Scan(&bound); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(
				"FOR VALUES FROM ('%s') TO ('%s')",
				from.Format("2006-01-02 15:04:05-07"),
				to.Format("2006-01-02 15:04:05-07"),
			)
			if bound != want {
				t.Errorf("%s: got %s want %s", table, bound, want)
			}
		}
	}
}

func testUsageAndEvents(t *testing.T, d *db.DB, admin *pgx.Conn) {
	ctx := t.Context()
	var tenant, run string
	if err := admin.QueryRow(ctx, "SELECT tenant_id::text,id::text FROM runs LIMIT 1").Scan(&tenant, &run); err != nil {
		t.Fatal(err)
	}
	key := ids.New()
	at := time.Now().UTC()
	entry := usage.Entry{
		TenantID:       tenant,
		RunID:          &run,
		Source:         "model",
		Meter:          "tokens",
		Quantity:       "7",
		UnitPrice:      "100",
		Millicredits:   700,
		IdempotencyKey: key,
		OccurredAt:     at,
	}
	var wg sync.WaitGroup
	results := make(chan bool, 6)
	errs := make(chan error, 6)
	for i := range 6 {
		wg.Go(func() {
			e := entry
			e.OccurredAt = at.AddDate(0, i, 0)
			errs <- d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				recorded, err := usage.RecordUsage(ctx, tx, e)
				results <- recorded
				return err
			})
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for recorded := range results {
		if recorded {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("recorded %d duplicates", count)
	}
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		// A duplicate must leave the same transaction usable.
		_, err := usage.RecordUsage(ctx, tx, entry)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "SELECT 1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	bad := entry
	bad.IdempotencyKey = ids.New()
	bad.Source = "invalid"
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, err := usage.RecordUsage(ctx, tx, bad); return err }); err == nil {
		t.Fatal("accepted invalid ledger row")
	}
	var n int
	if err := admin.QueryRow(ctx,
		"SELECT count(*) FROM usage_keys WHERE tenant_id=$1 AND "+
			"idempotency_key=$2",
		tenant,
		bad.IdempotencyKey).Scan(&n); err != nil ||

		n != 0 {
		t.Fatal("orphan claim after rollback", n, err)
	}
	event := run + ":leader:1:1:1"
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		for range 2 {
			if _, err := tx.Exec(ctx, "INSERT INTO run_events(tenant_id,run_id,seq,event_id,type,payload,ts) "+
				"VALUES($1,$2,1,$3,'test','{}',$4) ON CONFLICT DO NOTHING", tenant, run, event, at); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO run_events(tenant_id,run_id,seq,event_id,type,payload,ts) "+
			"VALUES($1,$2,1,$3,'test','{}',$4)", tenant, run, run+":follower:1:1:1", at); err != nil {
			return err
		}
		return tx.QueryRow(ctx, "SELECT count(*) FROM run_events WHERE event_id=$1", event).Scan(&n)
	}); err != nil || n != 1 {
		t.Fatal("event replay duplicated", n, err)
	}
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO run_events(tenant_id,run_id,seq,event_id,type,payload) "+
			"VALUES($1,$2,1,$3,'test','{}')", tenant, run, ids.New())
		return err
	}); err == nil {
		t.Fatal("event timestamp defaulted")
	}
	// Prune must use one retention cutoff for the ledger and its keys.
	oldKey := ids.New()
	old := entry
	old.IdempotencyKey = oldKey
	old.OccurredAt = at.AddDate(-2, 0, 0)
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, err := usage.RecordUsage(ctx, tx, old); return err }); err != nil {
		t.Fatal(err)
	}
	owner, err := db.Open(ctx, roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_owner"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	expiredKey := "expired-prune-" + ids.New()
	liveKey := "live-prune-" + ids.New()
	if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		for _, k := range []struct {
			key     string
			expires time.Time
		}{{key: expiredKey, expires: at.Add(-time.Hour)}, {key: liveKey, expires: at.Add(time.Hour)}} {
			if _, err := tx.Exec(ctx, "INSERT INTO idempotency_keys(tenant_id,principal_id,key,method,path,request_hash,status,he"+
				"aders,response,key_version,expires_at) SELECT "+
				"tenant_id,principal_id,$2,'POST','/test',$3,200,'{}',$3,1,$4 FROM memberships WHERE "+
				"tenant_id=$1 LIMIT 1", tenant, k.key, make([]byte, 32), k.expires); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cutoff := at.AddDate(-1, 0, 0)
	if err = usage.Prune(ctx, owner, &cutoff); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"usage_keys", "usage_ledger"} {
		if err = admin.QueryRow(ctx,
			"SELECT count(*) FROM "+
				table+
				" WHERE tenant_id=$1 AND idempotency_key=$2",
			tenant,
			oldKey).Scan(&n); err != nil ||

			n != 0 {
			t.Fatal("expired usage retained", table, n, err)
		}
		if err = admin.QueryRow(ctx,
			"SELECT count(*) FROM "+
				table+
				" WHERE tenant_id=$1 AND idempotency_key=$2",
			tenant,
			key).Scan(&n); err != nil ||

			n != 1 {
			t.Fatal("live usage lost", table, n, err)
		}
	}
	if err = admin.QueryRow(ctx,
		"SELECT count(*) FROM idempotency_keys WHERE tenant_id=$1 AND key=$2",
		tenant,
		liveKey).Scan(&n); err != nil ||

		n != 1 {
		t.Fatal("prune removed live replay", n, err)
	}
	var rows int
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE expires_at<=now()").Scan(&rows); err != nil ||
		rows != 0 {
		t.Fatal("expired idempotency rows retained", rows, err)
	}
	t.Log(strings.Join([]string{"UTC partitions", "concurrent cross-month usage", "event replay", "retention"}, ", "))
}

func testJITProvisioning(t *testing.T, d *db.DB, admin *pgx.Conn) {
	ctx := t.Context()
	c := config.Config{
		AuthMode:          "dev",
		PersonalTenants:   true,
		KeyFile:           t.TempDir() + "/dev.key",
		CapabilityKeyFile: t.TempDir() + "/capability.key",
		CapabilityKeyID:   "1",
		SecretsKeyVersion: 1,
		SecretsKEKs:       map[int][]byte{1: make([]byte, 32)},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(ctx, d, c, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// Exercise the same first-JIT hook used after successful OIDC verification.
	h := &httpapi.Handler{
		DB:     d,
		Auth:   a,
		Config: config.Config{AuthMode: "oidc", PersonalTenants: true},
		Logger: logger,
	}
	h.Router()
	subject := ids.New()
	email := "oidc-" + subject + "@example.test"
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Go(func() {
			_, err := a.JIT(ctx, "https://idp.example", subject, email, "JIT", true)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	p, err := a.JIT(ctx, "https://idp.example", subject, email, "JIT", true)
	if err != nil {
		t.Fatal(err)
	}
	tenants, err := tenancy.Tenants(ctx, d, p.ID)
	if err != nil || len(tenants) != 1 {
		t.Fatal(tenants, err)
	}
	var logins, markers int
	if err = admin.QueryRow(ctx,
		"SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND actor_id=$2 AND "+
			"action='login'",
		tenants[0].ID,
		p.ID).Scan(&logins); err != nil ||

		logins != 1 {
		t.Fatal("OIDC login audit not first-JIT only", logins, err)
	}
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE principal_id=$1", p.ID).Scan(&markers); err != nil ||
		markers != 0 {
		t.Fatal("OIDC login left replay markers", markers, err)
	}
	if err = d.WithTenant(ctx, tenants[0].ID, func(tx pgx.Tx) error {
		head, err := audit.Verify(ctx, tx)
		if err != nil {
			return err
		}
		var latest string
		var count int
		if err = tx.QueryRow(ctx, "SELECT encode(hash,'hex') FROM audit_log WHERE tenant_id=$1 ORDER BY occurred_at DESC,id "+
			"DESC LIMIT 1", tenants[0].ID).Scan(&latest); err != nil {
			return err
		}
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE tenant_id=$1", tenants[0].ID).Scan(&count); err != nil {
			return err
		}
		if head.Count != count || head.Hash != latest {
			return fmt.Errorf("wrong audit head: %+v", head)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A provisioning failure cannot strand the identity without its tenant/audit.
	a.OnCreate = func(context.Context, pgx.Tx, auth.Principal, string) error { return fmt.Errorf("provision failed") }
	failedSubject := ids.New()
	if _, err = a.JIT(ctx, "https://idp.example", failedSubject, "failed@example.test", "Fail", true); err == nil {
		t.Fatal("provision failure ignored")
	}
	var count int
	if err = admin.QueryRow(ctx,
		"SELECT count(*) FROM principals WHERE idp_subject=$1",
		"https://idp.example|"+
			failedSubject).Scan(&count); err != nil ||

		count != 0 {
		t.Fatal("identity survived failed JIT transaction", count, err)
	}
}
