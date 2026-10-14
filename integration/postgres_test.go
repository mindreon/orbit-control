package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/migrations"
)

// This suite intentionally migrates down; use a dedicated disposable database.
// ORBIT_TEST_DATABASE_URL must be a superuser on that database so role grants can
// be checked with actual SET ROLE transactions, without creating any roles.
func TestPostgresSuite(t *testing.T) {
	raw := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if raw == "" {
		if os.Getenv("ORBIT_REQUIRE_PG_TESTS") == "1" {
			t.Fatal("ORBIT_TEST_DATABASE_URL required")
		}
		t.Skip("ORBIT_TEST_DATABASE_URL not set")
	}
	ctx := t.Context()
	owner := roleURL(t, raw, "orbit_owner")
	for _, command := range []string{"up", "down", "up", "up"} {
		if e := migrations.Run(ctx, owner, command); e != nil {
			t.Fatalf("migrate %s: %v", command, e)
		}
	}
	admin, e := pgx.Connect(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(ctx)
	app, e := db.OpenApp(ctx, roleURL(t, raw, "orbit_app"))
	if e != nil {
		t.Fatal(e)
	}
	defer app.Close()
	privileged, e := db.OpenApp(ctx, owner)
	if e == nil {
		privileged.Close()
		t.Fatal("API accepted owner credentials")
	}
	t.Run("AllTenantTablesRLS", func(t *testing.T) { testRLS(t, admin, app) })
	t.Run("WorkerPrivileges", func(t *testing.T) { testWorker(t, admin) })
	t.Run("HTTPAndAudit", func(t *testing.T) { testHTTP(t, app, admin) })
}
func roleURL(t *testing.T, raw, role string) string {
	t.Helper()
	u, e := url.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("options", "-c role="+role)
	u.RawQuery = q.Encode()
	return u.String()
}
func testRLS(t *testing.T, admin *pgx.Conn, app *db.DB) {
	ctx := t.Context()
	a, b, pid := ids.New(), ids.New(), ids.New()
	if _, e := admin.Exec(ctx, "INSERT INTO principals(id,kind,email,display_name,idp_subject) VALUES($1,'user','rls@example.test','RLS',$1::uuid::text)", pid); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{a, b} {
		if _, e := admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$1::uuid::text,'RLS')", id); e != nil {
			t.Fatal(e)
		}
	}
	// Discover every tenant-bearing table, including direct partition access.
	rows, e := admin.Query(ctx, `SELECT c.table_name,pc.relrowsecurity,pc.relforcerowsecurity,pg_get_userbyid(pc.relowner) FROM information_schema.columns c JOIN pg_class pc ON pc.relname=c.table_name JOIN pg_namespace n ON n.oid=pc.relnamespace AND n.nspname=c.table_schema WHERE c.table_schema='public' AND c.column_name='tenant_id' ORDER BY c.table_name`)
	if e != nil {
		t.Fatal(e)
	}
	tables := []string{}
	for rows.Next() {
		var n, owner string
		var enabled, forced bool
		if e = rows.Scan(&n, &enabled, &forced, &owner); e != nil {
			t.Fatal(e)
		}
		if !enabled || !forced || owner != "orbit_owner" {
			t.Errorf("%s RLS=%v FORCE=%v owner=%s", n, enabled, forced, owner)
		}
		tables = append(tables, n)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	source, e := os.ReadFile("../migrations/00001_r1.sql")
	if e != nil {
		t.Fatal(e)
	}
	base := []string{}
	for _, m := range regexp.MustCompile(`CREATE TABLE (\w+)\s*\(`).FindAllStringSubmatch(string(source), -1) {
		if slices.Contains(tables, m[1]) {
			base = append(base, m[1])
		}
	}
	idmap := map[string]string{}
	for _, n := range base {
		idmap[n] = ids.New()
	}
	// Build a minimally valid fixture for every new table from live column and CHECK
	// metadata, so adding a tenant table expands coverage automatically.
	for _, n := range base {
		insertFixture(t, admin, app, n, n, a, pid, idmap, time.Now().UTC())
	}
	for _, n := range tables {
		if slices.Contains(base, n) {
			continue
		}
		parentName := ""
		for _, p := range []string{"run_events", "audit_log", "usage_ledger"} {
			if strings.HasPrefix(n, p+"_") {
				parentName = p
				break
			}
		}
		if parentName == "" {
			t.Fatalf("uncovered tenant table %s", n)
		}
		suffix := strings.TrimPrefix(n, parentName+"_")
		at := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
		if suffix != "default" {
			parsed, e := time.Parse("200601", suffix)
			if e != nil {
				t.Fatal(e)
			}
			at = parsed.Add(24 * time.Hour)
		}
		// INSERT directly as app checks that partition tables have their own policies.
		insertFixture(t, admin, app, n, parentName, a, pid, idmap, at)
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			q := pgx.Identifier{table}.Sanitize()
			e := app.WithTenant(ctx, a, func(tx pgx.Tx) error {
				var n int
				if e := tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n); e != nil {
					return e
				}
				if n == 0 {
					return fmt.Errorf("fixture missing")
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			e = app.WithTenant(ctx, b, func(tx pgx.Tx) error {
				var n int
				if e := tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n); e != nil {
					return e
				}
				if n != 0 {
					return fmt.Errorf("tenant B sees %d tenant A rows", n)
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			var canUpdate bool
			if e = admin.QueryRow(ctx, "SELECT has_table_privilege('orbit_app',$1,'UPDATE')", table).Scan(&canUpdate); e != nil {
				t.Fatal(e)
			}
			e = app.WithTenant(ctx, b, func(tx pgx.Tx) error {
				result, e := tx.Exec(ctx, "UPDATE "+q+" SET tenant_id=tenant_id")
				if !canUpdate {
					if e == nil {
						return fmt.Errorf("append-only update accepted")
					}
					return e
				}
				if e != nil {
					return e
				}
				if result.RowsAffected() != 0 {
					return fmt.Errorf("cross-tenant update succeeded")
				}
				return nil
			})
			if canUpdate && e != nil {
				t.Fatal(e)
			}
			if !canUpdate && e == nil {
				t.Fatal("append-only update permitted")
			}
			// WITH CHECK must reject a valid fixture copied from tenant A by B.
			var fixture []byte
			e = app.WithTenant(ctx, a, func(tx pgx.Tx) error {
				return tx.QueryRow(ctx, "SELECT to_jsonb(row) FROM "+q+" row LIMIT 1").Scan(&fixture)
			})
			if e != nil {
				t.Fatal(e)
			}
			e = app.WithTenant(ctx, b, func(tx pgx.Tx) error {
				_, e := tx.Exec(ctx, "INSERT INTO "+q+" SELECT (jsonb_populate_record(NULL::"+q+",$1::jsonb)).*", fixture)
				return e
			})
			var pgerr *pgconn.PgError
			if !errors.As(e, &pgerr) || pgerr.Code != "42501" {
				t.Fatalf("cross-tenant INSERT should fail RLS: %v", e)
			}
			e = app.Global(ctx, func(tx pgx.Tx) error { var n int; return tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n) })
			// SET LOCAL resets to an empty value on pooled connections; that
			// produces a UUID cast error rather than exposing another tenant.
			if e == nil {
				e = app.Global(ctx, func(tx pgx.Tx) error {
					var n int
					if e := tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n); e != nil {
						return e
					}
					if n != 0 {
						return fmt.Errorf("unscoped read exposed rows")
					}
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
			}
		})
	}
	t.Logf("RLS exercised %d tenant tables (including partitions)", len(tables))
}
func insertFixture(t *testing.T, admin *pgx.Conn, app *db.DB, table, base, tenant, pid string, idmap map[string]string, at time.Time) {
	t.Helper()
	ctx := t.Context()
	var checks []string
	rows, e := admin.Query(ctx, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::regclass AND contype='c'", table)
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
		var v string
		if e = rows.Scan(&v); e != nil {
			t.Fatal(e)
		}
		checks = append(checks, v)
	}
	rows.Close()
	rows, e = admin.Query(ctx, "SELECT column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position", table)
	if e != nil {
		t.Fatal(e)
	}
	cols := []string{}
	values := []any{}
	marks := []string{}
	for rows.Next() {
		var col, typ, nullable string
		var defaultValue *string
		if e = rows.Scan(&col, &typ, &nullable, &defaultValue); e != nil {
			t.Fatal(e)
		}
		if nullable == "YES" && col != "blob" {
			continue
		}
		if defaultValue != nil && col != "created_at" {
			continue
		}
		var v any
		switch typ {
		case "uuid":
			v = ids.New()
			switch col {
			case "id":
				v = idmap[base]
			case "tenant_id":
				v = tenant
			case "principal_id", "owner_id", "actor_id", "created_by", "reviewer_id", "granted_by", "updated_by":
				v = pid
			case "group_id":
				v = idmap["groups"]
			case "workspace_id":
				v = idmap["workspaces"]
			case "session_id":
				v = idmap["sessions"]
			case "run_id":
				v = idmap["runs"]
			case "asset_id":
				v = idmap["assets"]
			case "version_id":
				v = idmap["asset_versions"]
			case "scope_id", "grantee_id":
				v = tenant
			}
		case "text":
			v = "fixture"
			if col == "principal_type" {
				v = "user"
			}
			if col == "role" {
				v = "tenant.owner"
			}
			if col == "scope_type" {
				v = "tenant"
			}
			if col == "status" {
				v = "active"
			}
			if col == "idempotency_key" {
				v = table + at.String()
			}
			for _, c := range checks {
				m := regexp.MustCompile(regexp.QuoteMeta(col) + ` = ANY \(ARRAY\['([^']+)'`).FindStringSubmatch(c)
				if len(m) == 0 {
					m = regexp.MustCompile(regexp.QuoteMeta(col) + ` = '([^']+)'`).FindStringSubmatch(c)
				}
				if len(m) > 0 {
					v = m[1]
					break
				}
			}
		case "jsonb":
			v = []byte(`{}`)
		case "bytea":
			v = make([]byte, 32)
		case "boolean":
			v = true
		case "integer", "bigint", "numeric":
			v = 1
		case "timestamp with time zone":
			v = at
		case "interval":
			v = "1 hour"
		case "ARRAY":
			v = []string{"workspace.read"}
		default:
			t.Fatalf("unknown fixture type %s.%s %s", table, col, typ)
		}
		// Independent rows in partition children need unique identities and keys.
		if table != base && col == "id" {
			v = ids.New()
		}
		if table != base && col == "event_id" {
			v = ids.New()
		}
		cols = append(cols, pgx.Identifier{col}.Sanitize())
		values = append(values, v)
		marks = append(marks, fmt.Sprintf("$%d", len(values)))
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	query := "INSERT INTO " + pgx.Identifier{table}.Sanitize() + "(" + strings.Join(cols, ",") + ") VALUES(" + strings.Join(marks, ",") + ")"
	e = app.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, e := tx.Exec(ctx, query, values...); return e })
	if e != nil {
		t.Fatalf("fixture %s: %v", table, e)
	}
}
func testWorker(t *testing.T, admin *pgx.Conn) {
	ctx := t.Context()
	rows, e := admin.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
	if e != nil {
		t.Fatal(e)
	}
	tables := []string{}
	for rows.Next() {
		var n string
		if e = rows.Scan(&n); e != nil {
			t.Fatal(e)
		}
		tables = append(tables, n)
	}
	rows.Close()
	for _, table := range tables {
		allowed := slices.Contains([]string{"agent_checkpoints", "segment_results", "tool_effects"}, table)
		for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"} {
			var got bool
			if e = admin.QueryRow(ctx, "SELECT has_table_privilege('orbit_worker',$1,$2)", table, priv).Scan(&got); e != nil {
				t.Fatal(e)
			}
			want := allowed && slices.Contains([]string{"SELECT", "INSERT", "UPDATE"}, priv)
			if got != want {
				t.Errorf("%s %s got=%v want=%v", table, priv, got, want)
			}
		}
		tx, e := admin.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(ctx, "SET LOCAL ROLE orbit_worker"); e != nil {
			t.Fatal(e)
		}
		_, e = tx.Exec(ctx, "SELECT * FROM "+pgx.Identifier{table}.Sanitize()+" LIMIT 0")
		tx.Rollback(ctx)
		if (e == nil) != allowed {
			t.Errorf("actual worker query %s: %v", table, e)
		}
	}
	var tenant, id string
	if e = admin.QueryRow(ctx, "SELECT tenant_id::text,id::text FROM tool_effects LIMIT 1").Scan(&tenant, &id); e != nil {
		t.Fatal(e)
	}
	workerUpdate := func(sql string) error {
		tx, e := admin.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, "SET LOCAL ROLE orbit_worker"); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", tenant); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, sql, id); e != nil {
			return e
		}
		return tx.Commit(ctx)
	}
	if e = workerUpdate("UPDATE tool_effects SET status='completed',completed_at=now() WHERE id=$1"); e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{"UPDATE tool_effects SET status='started' WHERE id=$1", "UPDATE tool_effects SET args_digest='changed' WHERE id=$1"} {
		e = workerUpdate(q)
		var pgerr *pgconn.PgError
		if !errors.As(e, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("worker immutable effect update: %v", e)
		}
	}
	t.Log("worker restricted to three tables; immutable effects and no DELETE/TRUNCATE/REFERENCES/TRIGGER")
}
func testHTTP(t *testing.T, d *db.DB, admin *pgx.Conn) {
	ctx := t.Context()
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/signing.key", CORS: []string{"https://web.example"}}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, e := auth.New(ctx, d, c, l)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	h := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: l}).Router()
	server := httptest.NewServer(h)
	defer server.Close()
	request := func(method, path, token, tenant, key string, body any) (int, map[string]any, []byte) {
		t.Helper()
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		r, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(b))
		if e != nil {
			t.Fatal(e)
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("X-Orbit-Tenant", tenant)
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Content-Type", "application/json")
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		data, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
		}
		var result map[string]any
		if len(data) > 0 {
			if e = json.Unmarshal(data, &result); e != nil {
				t.Fatal(string(data))
			}
		}
		return resp.StatusCode, result, data
	}
	check := func(got, want int, v any) {
		t.Helper()
		if got != want {
			t.Fatalf("status %d want %d: %v", got, want, v)
		}
	}
	login := func(email string) (string, string, string) {
		code, v, _ := request("POST", "/v1/auth/dev/token", "", "", "", map[string]any{"email": email})
		check(code, 200, v)
		token := v["access_token"].(string)
		code, v, _ = request("GET", "/v1/me", token, "", "", nil)
		check(code, 200, v)
		if v["current_tenant"] != nil {
			t.Fatal("current tenant must be null without header")
		}
		return token, v["tenants"].([]any)[0].(map[string]any)["id"].(string), v["principal"].(map[string]any)["id"].(string)
	}
	token, tenant, pid := login("owner-" + ids.New() + "@example.test")
	other, otherTenant, otherID := login("member-" + ids.New() + "@example.test")
	code, v, original := request("POST", "/v1/workspaces", token, tenant, "create-workspace", map[string]any{"name": "Engineering"})
	check(code, 201, v)
	ws := v["id"].(string)
	code, v, replay := request("POST", "/v1/workspaces", token, tenant, "create-workspace", map[string]any{"name": "Engineering"})
	check(code, 201, v)
	if !bytes.Equal(original, replay) {
		t.Fatal("idempotent response mismatch")
	}
	code, v, _ = request("POST", "/v1/workspaces", token, tenant, "create-workspace", map[string]any{"name": "Changed"})
	check(code, 422, v)
	code, v, _ = request("GET", "/v1/workspaces/"+ws, other, otherTenant, "", nil)
	if code != 403 && code != 404 {
		t.Fatal(code, v)
	}
	code, v, _ = request("GET", "/v1/workspaces/"+ws, other, tenant, "", nil)
	check(code, 403, v)
	code, v, _ = request("POST", "/v1/admin/members", token, tenant, "", map[string]any{"principal_id": otherID, "role": "tenant.member"})
	check(code, 201, v)
	code, v, _ = request("POST", "/v1/workspaces/"+ws+"/members", token, tenant, "", map[string]any{"principal_id": otherID, "role": "ws.viewer"})
	check(code, 201, v)
	code, v, _ = request("GET", "/v1/workspaces/"+ws, other, tenant, "", nil)
	check(code, 200, v)
	code, v, _ = request("PATCH", "/v1/workspaces/"+ws, other, tenant, "", map[string]any{"name": "forbidden"})
	check(code, 403, v)
	code, v, _ = request("GET", "/v1/me", token, tenant, "", nil)
	check(code, 200, v)
	if !slices.Contains(v["roles"].([]any), any("ws.admin")) {
		t.Fatal("creator ws.admin missing")
	}
	code, v, _ = request("GET", "/v1/workspaces?limit=1", token, tenant, "", nil)
	check(code, 200, v)
	code, v, _ = request("GET", "/v1/admin/members?limit=1", token, tenant, "", nil)
	check(code, 200, v)
	cursor := v["next_cursor"].(string)
	if cursor == "" {
		t.Fatal("missing cursor")
	}
	code, v, _ = request("GET", "/v1/admin/members?cursor="+cursor+"&limit=1", token, tenant, "", nil)
	check(code, 200, v)
	keyInput := map[string]any{"name": "automation", "scopes": []string{"workspace.read"}, "expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
	code, v, keyResponse := request("POST", "/v1/admin/api-keys", token, tenant, "key-create", keyInput)
	check(code, 201, v)
	key := v["key"].(string)
	keyID := v["id"].(string)
	code, v, replay = request("POST", "/v1/admin/api-keys", token, tenant, "key-create", keyInput)
	check(code, 201, v)
	if !bytes.Equal(keyResponse, replay) {
		t.Fatal("API key replay differs")
	}
	code, v, _ = request("GET", "/v1/workspaces/"+ws, key, tenant, "", nil)
	check(code, 200, v)
	code, v, _ = request("PATCH", "/v1/workspaces/"+ws, key, tenant, "", map[string]any{"name": "no"})
	check(code, 403, v)
	code, v, _ = request("DELETE", "/v1/admin/api-keys?id="+keyID, token, tenant, "", nil)
	check(code, 204, v)
	code, v, _ = request("GET", "/v1/workspaces/"+ws, key, tenant, "", nil)
	check(code, 401, v)
	// Group inheritance grants no direct principal role; the group binding is used.
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		p := auth.Principal{ID: pid, Kind: "user"}
		g, e := tenancy.CreateGroup(ctx, tx, p, tenant, "Reviewers")
		if e != nil {
			return e
		}
		if e = tenancy.AddGroupMember(ctx, tx, p, tenant, g, otherID); e != nil {
			return e
		}
		return tenancy.Bind(ctx, tx, p, tenant, "group", g, "ws.editor", "workspace", ws)
	})
	if e != nil {
		t.Fatal(e)
	}
	code, v, _ = request("GET", "/v1/me", other, tenant, "", nil)
	check(code, 200, v)
	if !slices.Contains(v["roles"].([]any), any("ws.editor")) {
		t.Fatal("group role missing")
	}
	// Concurrent append must preserve a single chain.
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			errs <- d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				return audit.Append(ctx, tx, auth.Principal{ID: pid, Kind: "user"}, tenant, "test.concurrent", "tenant", tenant, map[string]any{"test": true})
			})
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		n, e := audit.Verify(ctx, tx)
		if e == nil && n < 15 {
			return fmt.Errorf("missing audited writes: %d", n)
		}
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	// API replay ciphertext does not contain the returned credential.
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		var response []byte
		if e := tx.QueryRow(ctx, "SELECT response FROM idempotency_keys WHERE key='key-create'").Scan(&response); e != nil {
			return e
		}
		if bytes.Contains(response, []byte(key)) {
			return fmt.Errorf("plaintext API key in replay storage")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// Removing the sole active owner is rejected.
	code, v, _ = request("PATCH", "/v1/admin/members/"+pid, token, tenant, "", map[string]any{"status": "suspended"})
	check(code, 409, v)
	// Expired idempotency records can be reused with a different body.
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, "UPDATE idempotency_keys SET expires_at=now()-interval '1 second' WHERE key='create-workspace'")
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	code, v, _ = request("POST", "/v1/workspaces", token, tenant, "create-workspace", map[string]any{"name": "After expiration"})
	check(code, 201, v)
	// Duplicate concurrent creates cause one write and identical responses.
	results := make(chan string, 4)
	for range 4 {
		wg.Go(func() {
			code, _, b := request("POST", "/v1/workspaces", token, tenant, "concurrent-create", map[string]any{"name": "Concurrent"})
			results <- fmt.Sprintf("%d:%s", code, b)
		})
	}
	wg.Wait()
	close(results)
	first := ""
	for result := range results {
		if !strings.HasPrefix(result, "201:") {
			t.Fatal(result)
		}
		if first == "" {
			first = result
		} else if first != result {
			t.Fatal("concurrent replay diverged")
		}
	}
	// API keys are revoked by suspended membership even when their hash remains.
	code, v, _ = request("PATCH", "/v1/admin/members/"+otherID, token, tenant, "", map[string]any{"status": "suspended"})
	check(code, 200, v)
	code, v, _ = request("GET", "/v1/me", other, tenant, "", nil)
	check(code, 403, v)
	// Privileged corruption is detectable even though app cannot UPDATE audit.
	var lastID string
	var detail []byte
	if e = admin.QueryRow(ctx, "SELECT id::text,detail FROM audit_log WHERE tenant_id=$1 ORDER BY occurred_at DESC,id DESC LIMIT 1", tenant).Scan(&lastID, &detail); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, "UPDATE audit_log SET detail='{\"tampered\":true}' WHERE tenant_id=$1 AND id=$2", tenant, lastID); e != nil {
		t.Fatal(e)
	}
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, e := audit.Verify(ctx, tx); return e })
	if e == nil || !strings.Contains(e.Error(), "audit chain broken") {
		t.Fatalf("audit corruption not detected: %v", e)
	}
	if _, e = admin.Exec(ctx, "UPDATE audit_log SET detail=$3 WHERE tenant_id=$1 AND id=$2", tenant, lastID, detail); e != nil {
		t.Fatal(e)
	}
	owner, e := db.Open(ctx, roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_owner"))
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	// Owner verification must ignore other tenants despite BYPASSRLS.
	e = owner.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, e := audit.Verify(ctx, tx); return e })
	if e != nil {
		t.Fatal(e)
	}
	t.Log("dev login, workspace creator role, member addition, tenant isolation, RBAC/group inheritance, pagination, encrypted idempotency, concurrent replay, audit tamper detection and owner isolation verified")
}
