package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/migrations"
)

// TestWorkspaceRoleUpdatesTenantAsset guards the dev/oidc asset authorization
// path: a ws.editor (not the owner) of W may update a tenant-visibility asset
// that keeps workspace_id W, because the Require resource carries the workspace.
func TestWorkspaceRoleUpdatesTenantAsset(t *testing.T) {
	telemetry.InitializeMetrics()
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
	defer app.Close()
	admin, e := pgx.Connect(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(ctx)
	tenant := ids.New()
	if _, e = admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,'Roles')", tenant, "slug-"+tenant); e != nil {
		t.Fatal(e)
	}
	owner, editor, bystander := ids.New(), ids.New(), ids.New()
	for _, p := range []string{owner, editor, bystander} {
		if _, e = admin.Exec(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user',$2,$2,$3)`, p, p+"@example.test", "dev|"+p); e != nil {
			t.Fatal(e)
		}
		if _, e = admin.Exec(ctx, `INSERT INTO memberships(tenant_id,principal_id,status,source)
 VALUES($1,$2,'active','direct')`, tenant, p); e != nil {
			t.Fatal(e)
		}
	}
	workspace := ids.New()
	if _, e = admin.Exec(ctx, "INSERT INTO workspaces(id,tenant_id,name) VALUES($1,$2,'Roles')", workspace, tenant); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO workspace_members(tenant_id,workspace_id,principal_id)
 VALUES($1,$2,$3)`, tenant, workspace, editor); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by)
 VALUES($1,$2,'user',$3,'ws.editor','workspace',$4,$3)`, ids.New(), tenant, editor, workspace); e != nil {
		t.Fatal(e)
	}
	// Tenant visibility, but the workspace it came from is preserved.
	asset := ids.New()
	if _, e = admin.Exec(ctx, `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,workspace_id,owner_id,status)
 VALUES($1,$2,'roles','agent','roles-agent','tenant',$3,$4,'active')`, asset, tenant, workspace, owner); e != nil {
		t.Fatal(e)
	}

	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test", CapabilityKeyID: "1",
		SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, e := auth.New(ctx, app, c, l)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	server := httptest.NewServer((&httpapi.Handler{DB: app, Auth: a, Config: c, Logger: l, Assets: &assets.Service{}}).Router())
	defer server.Close()
	patch := func(principal string) (int, map[string]any) {
		t.Helper()
		token, e := a.Signer.DevToken(auth.Principal{ID: principal, Kind: "user"})
		if e != nil {
			t.Fatal(e)
		}
		r, e := http.NewRequestWithContext(ctx, http.MethodPatch, server.URL+"/v1/assets/"+asset,
			bytes.NewReader([]byte(`{"visibility":"tenant"}`)))
		if e != nil {
			t.Fatal(e)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Orbit-Tenant", tenant)
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
		if e = json.Unmarshal(data, &result); e != nil {
			t.Fatal(string(data))
		}
		return resp.StatusCode, result
	}
	// The same resource must retain workspace-scoped update/publish privileges
	// for unrestricted callers and iam callers still in this workspace.
	for _, restricted := range []bool{false, true} {
		p := auth.Principal{ID: editor, Kind: "user", IAMRestricted: restricted, IAMWorkspace: workspace}
		for _, action := range []string{"asset.update", "asset.publish", "asset.share"} {
			if e := app.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				requestCtx := auth.Context(db.Context(ctx, tx), p, tenant)
				_, err := (&assets.Service{}).Get(requestCtx, asset, action)
				return err
			}); e != nil {
				t.Fatalf("restricted=%v %s: %v", restricted, action, e)
			}
		}
	}
	// A restricted iam user from another workspace must lose the preserved
	// workspace id for non-workspace assets; otherwise assetGrants lets its
	// workspace binding authorize asset.share and the later Require fallback.
	foreign := auth.Principal{ID: editor, Kind: "user", IAMRestricted: true, IAMWorkspace: ids.New()}
	for _, action := range []string{"asset.update", "asset.publish", "asset.share"} {
		err := app.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			requestCtx := auth.Context(db.Context(ctx, tx), foreign, tenant)
			_, err := (&assets.Service{}).Get(requestCtx, asset, action)
			return err
		})
		if err == nil {
			t.Errorf("restricted foreign workspace unexpectedly allowed %s", action)
		}
	}
	if code, v := patch(editor); code != 200 {
		t.Fatalf("ws.editor patch: %d %v", code, v)
	}
	if code, v := patch(bystander); code != 403 {
		t.Fatalf("plain member patch: %d %v", code, v)
	}
}
