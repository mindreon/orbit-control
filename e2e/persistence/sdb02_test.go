//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tenantTables are every table under tenant RLS: the task runtime (migration 00013), SOP and policy layers, and the
// catalog.
var tenantTables = append(append([]string{}, taskTenantTables...), "sop_definitions", "tenant_policy", "user_settings", "personas", "mcp_connectors")

// seedTenant fills every tenant table for tenant as the owner, so an isolation check never passes on an empty table.
func seedTenant(t *testing.T, owner *pgxpool.Pool, tenant string) {
	t.Helper()
	ctx := context.Background()
	if err := seedTaskRows(ctx, owner, tenant); err != nil {
		t.Fatalf("seed task rows for %s: %v", tenant, err)
	}
	for _, q := range []string{
		`INSERT INTO sop_definitions (tenant_id, sop_id, version, steps) VALUES ($1, 'sop', 1, '[{"id":"s"}]')`,
		`INSERT INTO tenant_policy (tenant_id, spec) VALUES ($1, '{}')`,
		`INSERT INTO user_settings (tenant_id, user_id, spec) VALUES ($1, 'u-seed', '{}')`,
		`INSERT INTO personas (id, tenant_id, name) VALUES ('persona_' || $1, $1, 'p')`,
		`INSERT INTO mcp_connectors (id, tenant_id, name, command) VALUES ('mcp_' || $1, $1, 'm', 'true')`,
	} {
		if _, err := owner.Exec(ctx, q, tenant); err != nil {
			t.Fatalf("seed catalog rows for %s: %v", tenant, err)
		}
	}
}

func TestSDB02TenantIsolation(t *testing.T) {
	const c = "S-DB-2"
	const tenantA, tenantB, sharedUser = "t-sdb2-a", "t-sdb2-b", "u-sdb2"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	sa := startServer(t, serverOpts{tenant: tenantA, maxConns: 4})
	sb := startServer(t, serverOpts{tenant: tenantB, maxConns: 4})
	seedTenant(t, ownerPool, tenantA)
	seedTenant(t, ownerPool, tenantB)
	// The same user id in both tenants: only the tenant tells them apart.
	newTask := func(srv *server, label string) string {
		act := srv.check(t, "S-DB-2/setup/create-"+label, c, "create a task in tenant "+label,
			httpReq{Method: "POST", Path: "/v1/tasks", Headers: userCSRF(sharedUser), Body: `{"title":"tenant task","goal":"isolate"}`}, httpExp{Status: 201})
		var body struct {
			ID string `json:"task_id"`
		}
		if err := json.Unmarshal([]byte(act.Body), &body); err != nil || body.ID == "" {
			t.Fatalf("no task id in %q", act.Body)
		}
		alias(body.ID, "<task-"+label+">")
		return body.ID
	}
	taskA, taskB := newTask(sa, "A"), newTask(sb, "B")
	const seededUser = sharedUser

	// E2E: tenant B's user cannot reach tenant A's task through any route.
	missing := sb.check(t, "S-DB-2/setup/missing-task", c, "baseline 404 body in tenant B",
		httpReq{Method: "GET", Path: "/v1/tasks/task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Headers: user(seededUser)}, httpExp{Status: 404})
	for _, p := range []string{"", "/plan", "/artifacts"} {
		sb.check(t, "S-DB-2/cross-tenant/GET"+p, c, "tenant B reads tenant A's task"+p+" → same 404 as missing",
			httpReq{Method: "GET", Path: "/v1/tasks/" + taskA + p, Headers: user(seededUser)}, httpExp{Status: 404, BodyEquals: missing.Body})
	}
	sb.check(t, "S-DB-2/cross-tenant/POST-message", c, "tenant B posts to tenant A's task → 404",
		httpReq{Method: "POST", Path: "/v1/tasks/" + taskA + "/messages", Headers: userCSRF(seededUser), Body: `{"text":"x","delivery":"queue"}`}, httpExp{Status: 404, BodyEquals: missing.Body})
	sb.check(t, "S-DB-2/cross-tenant/list-tasks", c, "tenant B's task list has only its own task",
		httpReq{Method: "GET", Path: "/v1/tasks", Headers: user(seededUser)}, httpExp{Status: 200, BodyIncludes: []string{taskB}, BodyExcludes: []string{taskA}})
	sb.check(t, "S-DB-2/cross-tenant/list-personas", c, "tenant B's assistant list has only its own assistant",
		httpReq{Method: "GET", Path: "/v1/personas", Headers: user(sharedUser)}, httpExp{Status: 200, BodyIncludes: []string{"persona_" + tenantB}, BodyExcludes: []string{"persona_" + tenantA}})
	sa.check(t, "S-DB-2/cross-tenant/A-still-sees-own", c, "tenant A still reads its task",
		httpReq{Method: "GET", Path: "/v1/tasks/" + taskA, Headers: user(seededUser)}, httpExp{Status: 200, BodyIncludes: []string{taskA}})

	// ISO-10: RLS underneath the query layer.
	ownerCounts := func(tenant string) map[string]int {
		out := map[string]int{}
		for _, tbl := range tenantTables {
			var n int
			if err := ownerPool.QueryRow(ctx, `SELECT count(*) FROM `+tbl+` WHERE tenant_id = $1`, tenant).Scan(&n); err != nil {
				t.Fatalf("owner count %s: %v", tbl, err)
			}
			out[tbl] = n
		}
		return out
	}
	oa, ob := ownerCounts(tenantA), ownerCounts(tenantB)
	seeded := true
	for _, tbl := range tenantTables {
		seeded = seeded && oa[tbl] > 0 && ob[tbl] > 0
	}
	iso(t, "S-DB-2/baseline-owner-sees-both", c, []string{"FM-28"}, "owner (BYPASSRLS) sees rows of both tenants in every [T] table (rules out a vacuous 0)",
		sqlReq{Role: "orbit_owner", SQL: "SELECT count(*) FROM <each [T] table> WHERE tenant_id = <A> | <B>"},
		"every table > 0 for both tenants", map[string]any{"A": oa, "B": ob}, seeded)

	appCounts := func(pool *pgxpool.Pool, tenant string, set bool) (map[string]int, error) {
		out := map[string]int{}
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if set {
				if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenant); err != nil {
					return err
				}
			}
			for _, tbl := range tenantTables {
				var n int
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
					return err
				}
				out[tbl] = n
			}
			return nil
		})
		return out, err
	}
	zeroes := func(m map[string]int) bool {
		for _, n := range m {
			if n != 0 {
				return false
			}
		}
		return len(m) == len(tenantTables)
	}
	for _, probe := range []struct {
		id, tenant, why string
		set             bool
	}{
		{"no-guc", "", "without app.tenant_id", false},
		{"empty-guc", "", "with app.tenant_id = ''", true},
		{"unknown-tenant", "t-does-not-exist", "with an unknown tenant", true},
	} {
		got, err := appCounts(sa.appPool, probe.tenant, probe.set)
		iso(t, "S-DB-2/app-"+probe.id+"-sees-nothing", c, []string{"FM-28", "FM-31"}, "orbit_app "+probe.why+" reads 0 rows from every [T] table",
			sqlReq{Role: "orbit_app", Tenant: probe.tenant, SQL: "SELECT count(*) FROM <each [T] table> (no WHERE)"},
			map[string]any{"rows": "0 in every table", "sqlstate": "ok"}, map[string]any{"rows": got, "sqlstate": sqlState(err)},
			err == nil && zeroes(got))
	}
	for _, own := range []struct {
		label, tenant string
		owner         map[string]int
	}{{"A", tenantA, oa}, {"B", tenantB, ob}} {
		got, err := appCounts(sa.appPool, own.tenant, true)
		equal := err == nil
		for _, tbl := range tenantTables {
			equal = equal && got[tbl] == own.owner[tbl]
		}
		iso(t, "S-DB-2/app-tenant-"+own.label+"-sees-only-own", c, []string{"FM-29"}, "orbit_app with tenant "+own.label+" sees exactly that tenant's rows (unfiltered SELECT equals the owner's per-tenant count)",
			sqlReq{Role: "orbit_app", Tenant: own.tenant, SQL: "SELECT count(*) FROM <each [T] table> (no WHERE)"},
			own.owner, map[string]any{"rows": got, "sqlstate": sqlState(err)}, equal)
	}

	var appSuper, appBypass bool
	if err := ownerPool.QueryRow(ctx, `SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = 'orbit_app'`).Scan(&appSuper, &appBypass); err != nil {
		t.Fatal(err)
	}
	type tableSec struct {
		OwnedByApp bool `json:"ownedByApp"`
		RLS        bool `json:"rls"`
		Force      bool `json:"force"`
		Policies   int  `json:"policies"`
	}
	sec := map[string]tableSec{}
	secOK := !appSuper && !appBypass
	for _, tbl := range tenantTables {
		var s tableSec
		if err := ownerPool.QueryRow(ctx, `
			SELECT c.relowner = (SELECT oid FROM pg_roles WHERE rolname = 'orbit_app'), c.relrowsecurity, c.relforcerowsecurity,
			       (SELECT count(*) FROM pg_policies p WHERE p.schemaname = 'public' AND p.tablename = c.relname)
			  FROM pg_class c WHERE c.oid = ('public.' || $1)::regclass`, tbl).Scan(&s.OwnedByApp, &s.RLS, &s.Force, &s.Policies); err != nil {
			t.Fatal(err)
		}
		sec[tbl] = s
		secOK = secOK && !s.OwnedByApp && s.RLS && s.Force && s.Policies > 0
	}
	iso(t, "S-DB-2/role-and-table-attributes", c, []string{"FM-28", "FM-30"}, "orbit_app is not SUPERUSER/BYPASSRLS and owns no [T] table; every [T] table has RLS + FORCE + a policy",
		sqlReq{Role: "orbit_owner", SQL: "pg_roles for orbit_app; pg_class + pg_policies for each [T] table"},
		map[string]any{"appSuperuser": false, "appBypassRLS": false, "tables": "ownedByApp=false, rls=true, force=true, policies>0"},
		map[string]any{"appSuperuser": appSuper, "appBypassRLS": appBypass, "tables": sec}, secOK)
}
