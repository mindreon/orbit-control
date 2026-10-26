package auth

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/migrations"
)

// These tests need a real Postgres with the orbit roles, like the integration
// suite: ORBIT_TEST_DATABASE_URL must be a superuser on a disposable database.
func openTestDB(t *testing.T) (app *db.DB, admin *pgx.Conn, tenant, slug string) {
	t.Helper()
	raw := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if raw == "" {
		if os.Getenv("ORBIT_REQUIRE_PG_TESTS") == "1" {
			t.Fatal("ORBIT_TEST_DATABASE_URL required")
		}
		t.Skip("ORBIT_TEST_DATABASE_URL not set")
	}
	ctx := t.Context()
	if e := migrations.Run(ctx, roleURL(t, raw, "orbit_owner"), "up"); e != nil {
		t.Fatal(e)
	}
	app, e := db.OpenApp(ctx, roleURL(t, raw, "orbit_app"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(app.Close)
	admin, e = pgx.Connect(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	slug = "iam-" + ids.New()
	if _, e = admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,'IAM')", ids.New(), slug); e != nil {
		t.Fatal(e)
	}
	if e = admin.QueryRow(ctx, "SELECT id::text FROM tenants WHERE slug=$1", slug).Scan(&tenant); e != nil {
		t.Fatal(e)
	}
	return app, admin, tenant, slug
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

func seedMember(t *testing.T, admin *pgx.Conn, tenant string) Principal {
	t.Helper()
	p := Principal{ID: ids.New(), Kind: "user"}
	if _, e := admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user',$2,$2,$3)`, p.ID, p.ID+"@example.test", "baize-iam|"+p.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := admin.Exec(t.Context(), `INSERT INTO memberships(tenant_id,principal_id,status,source)
 VALUES($1,$2,'active','jit')`, tenant, p.ID); e != nil {
		t.Fatal(e)
	}
	return p
}

func TestUpsertProjectWorkspace(t *testing.T) {
	app, admin, tenant, _ := openTestDB(t)
	ctx := t.Context()
	p := seedMember(t, admin, tenant)
	a := &Service{}
	upsert := func(project, name string) string {
		var workspace string
		if e := app.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			var e error
			workspace, e = a.upsertProjectWorkspace(ctx, tx, p, tenant, project, name)
			return e
		}); e != nil {
			t.Fatal(e)
		}
		return workspace
	}
	first := upsert("bp-1", "Alpha")
	if again := upsert("bp-1", "Alpha Prime"); again != first {
		t.Fatalf("second upsert created %s, want %s", again, first)
	}
	var name string
	var members, bindings int
	if e := admin.QueryRow(ctx, `SELECT w.name,
 (SELECT count(*) FROM workspace_members WHERE workspace_id=w.id AND principal_id=$2),
 (SELECT count(*) FROM role_bindings WHERE scope_type='workspace' AND scope_id=w.id
  AND principal_id=$2 AND role='ws.editor')
 FROM workspaces w WHERE w.id=$1`, first, p.ID).Scan(&name, &members, &bindings); e != nil {
		t.Fatal(e)
	}
	if name != "Alpha Prime" || members != 1 || bindings != 1 {
		t.Fatalf("name=%q members=%d bindings=%d", name, members, bindings)
	}
	var external int
	if e := admin.QueryRow(ctx, "SELECT count(*) FROM workspaces WHERE tenant_id=$2 AND external_ref=$1",
		baizeProjectRef("bp-1"), tenant).Scan(&external); e != nil {
		t.Fatal(e)
	}
	if external != 1 {
		t.Fatalf("external_ref rows: %d", external)
	}
}

// TestProjectSyncAuditsOnce checks the hook facts: only the first sync reports
// inserted rows; a rename sync reports nothing.
func TestProjectSyncAuditsOnce(t *testing.T) {
	app, admin, tenant, _ := openTestDB(t)
	ctx := t.Context()
	p := seedMember(t, admin, tenant)
	var report [][2]bool
	a := &Service{OnWorkspaceSync: func(_ context.Context, _ pgx.Tx, _ Principal, _, _, _ string, created, granted bool) error {
		report = append(report, [2]bool{created, granted})
		return nil
	}}
	sync := func(project, name string) {
		if e := app.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			_, e := a.upsertProjectWorkspace(ctx, tx, p, tenant, project, name)
			return e
		}); e != nil {
			t.Fatal(e)
		}
	}
	sync("bp-1", "Alpha")
	sync("bp-1", "Alpha Prime")
	if len(report) != 2 || report[0] != [2]bool{true, true} || report[1] != [2]bool{false, false} {
		t.Fatalf("sync reports: %+v", report)
	}
	var events int
	if e := admin.QueryRow(ctx, `SELECT count(*) FROM audit_log
 WHERE tenant_id=$1 AND action IN ('workspace.created','role.granted')`, tenant).Scan(&events); e != nil {
		t.Fatal(e)
	}
	if events != 0 {
		t.Fatalf("upsert must not audit on its own: %d", events)
	}
}

func TestCurrentWorkspaceSwitchAndMemo(t *testing.T) {
	app, admin, tenant, slug := openTestDB(t)
	ctx := t.Context()
	p := seedMember(t, admin, tenant)
	a := &Service{DB: app, Config: config.Config{AuthMode: "iam", DefaultTenant: slug}, projects: newProjectMemo()}
	one, e := a.currentWorkspace(ctx, p, "bp-1", "One")
	if e != nil {
		t.Fatal(e)
	}
	two, e := a.currentWorkspace(ctx, p, "bp-2", "Two")
	if e != nil {
		t.Fatal(e)
	}
	if one == two {
		t.Fatalf("project switch returned the same workspace %s", one)
	}
	if memo, ok := a.projects.get(p.ID, "bp-1"); !ok || memo != one {
		t.Fatalf("memo miss: %s %v", memo, ok)
	}
	a.projects.now = func() time.Time { return time.Now().Add(2 * iamProjectMemoTTL) }
	if _, e = a.currentWorkspace(ctx, p, "bp-1", "One Prime"); e != nil {
		t.Fatal(e)
	}
	var name string
	if e = admin.QueryRow(ctx, "SELECT name FROM workspaces WHERE id=$1", one).Scan(&name); e != nil {
		t.Fatal(e)
	}
	if name != "One Prime" {
		t.Fatalf("sync after memo expiry: %q", name)
	}
}

func TestAuthenticateIAMWithoutProject(t *testing.T) {
	app, admin, tenant, slug := openTestDB(t)
	ctx := t.Context()
	a := &Service{DB: app, Config: config.Config{AuthMode: "iam", DefaultTenant: slug}, projects: newProjectMemo()}
	a.OnCreate = func(ctx context.Context, tx pgx.Tx, p Principal, tenant string) error {
		_, e := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,principal_id,status,source)
 VALUES($1,$2,'active','jit')`, tenant, p.ID)
		return e
	}
	a.iam = projectStubLookup{identity: iamIdentity{UserID: "u-no-project", Name: "No Project"}}
	p, e := a.Authenticate(ctx, "token-1", "")
	if e != nil {
		t.Fatal(e)
	}
	if p.IAMWorkspace != "" {
		t.Fatalf("workspace created without a project: %s", p.IAMWorkspace)
	}
	var workspaces int
	if e = admin.QueryRow(ctx, "SELECT count(*) FROM workspaces WHERE tenant_id=$1", tenant).Scan(&workspaces); e != nil {
		t.Fatal(e)
	}
	if workspaces != 0 {
		t.Fatalf("workspaces created: %d", workspaces)
	}
}

// stubLookup stands in for baize with a fixed identity.
type projectStubLookup struct{ identity iamIdentity }

func (s projectStubLookup) Lookup(context.Context, string) (iamIdentity, error) {
	return s.identity, nil
}
