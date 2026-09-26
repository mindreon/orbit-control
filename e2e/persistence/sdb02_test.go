//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/app"
)

var tenantTables = []string{
	"users", "rooms", "turns", "events", "messages", "approvals", "approval_rules",
	"idempotency_keys", "artifacts", "artifact_versions", "personas", "mcp_connectors", "cloud_agent_jobs",
}

type tenantFixture struct {
	tenant, user, room, approval string
}

func seedTenant(t *testing.T, srv *server, tenant, u, idemKey, label string) tenantFixture {
	t.Helper()
	ctx := context.Background()
	room := roomID(t, srv.check(t, "S-DB-2/setup/create-"+label, "S-DB-2", "create a task in tenant "+label,
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: withHeader(user(u), "Idempotency-Key", idemKey), Body: `{"kind":"solo","title":"tenant task"}`},
		httpExp{Status: 200}))
	alias(room, "<room-"+label+">")
	posted := srv.check(t, "S-DB-2/setup/message-"+label, "S-DB-2", "message parks an approval in tenant "+label,
		httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Headers: user(u), Body: `{"message":"list files"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
	var body struct {
		Approval *app.Approval `json:"approval"`
	}
	_ = json.Unmarshal([]byte(posted.Body), &body)
	if body.Approval == nil {
		t.Fatalf("no approval for tenant %s", label)
	}
	alias(body.Approval.ID, "<approval-"+label+">")
	seedChildren(t, srv.appPool, tenant, room, tenant+"/blob", "sha256:0", 1)
	if err := asTenant(ctx, srv.appPool, tenant, func(tx pgx.Tx) error {
		for _, q := range []string{
			`INSERT INTO personas (id, tenant_id, name) VALUES ('persona_' || $1, $1, 'p')`,
			`INSERT INTO mcp_connectors (id, tenant_id, name, command) VALUES ('mcp_' || $1, $1, 'm', 'true')`,
			`INSERT INTO cloud_agent_jobs (id, tenant_id, repo_url, prompt, permission_preset, state) VALUES ('caj_' || $1, $1, 'https://example.test/r.git', 'p', 'read-only', 'queued')`,
		} {
			if _, err := tx.Exec(ctx, q, tenant); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed catalog rows: %v", err)
	}
	return tenantFixture{tenant: tenant, user: u, room: room, approval: body.Approval.ID}
}

func TestSDB02TenantIsolation(t *testing.T) {
	const c = "S-DB-2"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	wk := stubWorker(t, nil)
	sa := startServer(t, serverOpts{tenant: "t-sdb2-a", maxConns: 4, workerURL: wk.URL})
	sb := startServer(t, serverOpts{tenant: "t-sdb2-b", maxConns: 4, workerURL: wk.URL})
	a := seedTenant(t, sa, "t-sdb2-a", "u-sdb2-a", "e2e-sdb2-shared-key", "A")
	b := seedTenant(t, sb, "t-sdb2-b", "u-sdb2-b", "e2e-sdb2-shared-key", "B")

	// E2E: tenant B's user cannot reach tenant A's task through any route.
	missing := sb.check(t, "S-DB-2/setup/missing-room", c, "baseline 404 body in tenant B",
		httpReq{Method: "GET", Path: "/v1/rooms/rm_does_not_exist", Headers: user(b.user)}, httpExp{Status: 404})
	for _, p := range []string{"", "/messages", "/activity", "/events"} {
		sb.check(t, "S-DB-2/cross-tenant/GET"+p, c, "tenant B reads tenant A's task"+p+" → same 404 as missing",
			httpReq{Method: "GET", Path: "/v1/rooms/" + a.room + p, Headers: user(b.user)}, httpExp{Status: 404, BodyEquals: missing.Body})
	}
	sb.check(t, "S-DB-2/cross-tenant/POST-message", c, "tenant B posts to tenant A's task → 404",
		httpReq{Method: "POST", Path: "/v1/rooms/" + a.room + "/messages", Headers: user(b.user), Body: `{"message":"x"}`}, httpExp{Status: 404, BodyEquals: missing.Body})
	sb.check(t, "S-DB-2/cross-tenant/DELETE", c, "tenant B deletes tenant A's task → 404",
		httpReq{Method: "DELETE", Path: "/v1/rooms/" + a.room, Headers: userCSRF(b.user)}, httpExp{Status: 404, BodyEquals: missing.Body})
	missingAp := sb.check(t, "S-DB-2/setup/missing-approval", c, "baseline 404 for a missing approval in tenant B",
		httpReq{Method: "POST", Path: "/v1/approvals/ap_missing/decide", Headers: user(b.user), Body: `{"decision":"allow"}`}, httpExp{Status: 404})
	sb.check(t, "S-DB-2/cross-tenant/decide", c, "tenant B decides tenant A's approval → same 404",
		httpReq{Method: "POST", Path: "/v1/approvals/" + a.approval + "/decide", Headers: user(b.user), Body: `{"decision":"allow"}`},
		httpExp{Status: 404, BodyEquals: missingAp.Body})
	sb.check(t, "S-DB-2/cross-tenant/list-rooms", c, "tenant B's room list has only its own task",
		httpReq{Method: "GET", Path: "/v1/rooms", Headers: user(b.user)}, httpExp{Status: 200, BodyIncludes: []string{b.room}, BodyExcludes: []string{a.room}})
	sb.check(t, "S-DB-2/cross-tenant/list-approvals", c, "tenant B's approval list has only its own approval",
		httpReq{Method: "GET", Path: "/v1/approvals", Headers: user(b.user)}, httpExp{Status: 200, BodyIncludes: []string{b.approval}, BodyExcludes: []string{a.approval}})
	sa.check(t, "S-DB-2/cross-tenant/A-still-sees-own", c, "tenant A still reads its task",
		httpReq{Method: "GET", Path: "/v1/rooms/" + a.room, Headers: user(a.user)}, httpExp{Status: 200, BodyIncludes: []string{a.room}})
	record(t, caseInput{ID: "S-DB-2/idempotency-key-per-tenant", Contract: c, Description: "the same Idempotency-Key in two tenants created two different tasks (no cross-tenant replay)",
		Request:  "S-DB-2/setup/create-A and S-DB-2/setup/create-B used the same key and body",
		Expected: "different task ids", Actual: map[string]bool{"differentTaskIDs": a.room != b.room}, Pass: a.room != b.room})

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
	oa, ob := ownerCounts(a.tenant), ownerCounts(b.tenant)
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
	}{{"A", a.tenant, oa}, {"B", b.tenant, ob}} {
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
