package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/migrations"
)

// TestIAMWorkspaces drives the iam auth mode end to end: a fake baize IAM serves
// /users/current per bearer token, and the workspace surface must reflect the
// token's current project only.
func TestIAMWorkspaces(t *testing.T) {
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
	slug := "iam-" + ids.New()
	var tenant string
	if _, e = admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,'IAM')", ids.New(), slug); e != nil {
		t.Fatal(e)
	}
	if e = admin.QueryRow(ctx, "SELECT id::text FROM tenants WHERE slug=$1", slug).Scan(&tenant); e != nil {
		t.Fatal(e)
	}
	// One baize user with two projects (one per token) and one user without any.
	project := func(id, name string) map[string]any {
		return map[string]any{"id": id, "name": name, "displayName": name}
	}
	user := func(id string, projects ...map[string]any) map[string]any {
		u := map[string]any{"id": id, "username": id, "nickname": id}
		if len(projects) > 0 {
			u["currentProject"] = projects[0]
		}
		return u
	}
	// The defaultProject fallback mirrors netflow: a user whose token carries no
	// current project still lands on the default one.
	defaultOnly := map[string]any{"id": "iam-u-1", "username": "iam-u-1", "defaultProject": project("bp-9", "Delta")}
	tokens := map[string]map[string]any{
		"tok-alpha":     user("iam-u-1", project("bp-1", "Alpha")),
		"tok-beta":      user("iam-u-1", project("bp-2", "Beta")),
		"tok-default":   defaultOnly,
		"tok-noproject": user("iam-u-2"),
	}
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := bytes.CutPrefix([]byte(r.Header.Get("Authorization")), []byte("Bearer "))
		body, ok := tokens[string(token)]
		if !ok {
			http.Error(w, "unknown token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"user": body}})
	}))
	defer iam.Close()
	c := config.Config{
		AuthMode:          "iam",
		IAMBaseURL:        iam.URL,
		DefaultTenant:     slug,
		KeyFile:           t.TempDir() + "/signing.key",
		CapabilityKeyFile: t.TempDir() + "/capability.key",
		StreamKeyFile:     t.TempDir() + "/stream.key",
		StreamKeyID:       "test",
		CapabilityKeyID:   "1",
		SecretsKeyVersion: 1,
		SecretsKEKs:       map[int][]byte{1: make([]byte, 32)},
	}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, e := auth.New(ctx, app, c, l)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	server := httptest.NewServer((&httpapi.Handler{DB: app, Auth: a, Config: c, Logger: l}).Router())
	defer server.Close()
	request := func(method, path, token string, body any) (int, map[string]any) {
		t.Helper()
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		r, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(b))
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
		if len(data) > 0 {
			if e = json.Unmarshal(data, &result); e != nil {
				t.Fatal(string(data))
			}
		}
		return resp.StatusCode, result
	}
	workspaceID := func(token string) string {
		t.Helper()
		code, v := request(http.MethodGet, "/v1/workspaces", token, nil)
		if code != 200 {
			t.Fatalf("list: %d %v", code, v)
		}
		items := v["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("want exactly one workspace, got %d: %v", len(items), v)
		}
		return items[0].(map[string]any)["id"].(string)
	}

	alpha := workspaceID("tok-alpha")
	code, v := request(http.MethodGet, "/v1/workspaces", "tok-alpha", nil)
	if code != 200 || v["items"].([]any)[0].(map[string]any)["name"] != "Alpha" {
		t.Fatalf("alpha: %d %v", code, v)
	}
	// The first sync audited the workspace and the ws.editor grant; a repeat
	// request for the same project must not grow the audit log.
	auditRows := func() int {
		var n int
		if e := admin.QueryRow(ctx, `SELECT count(*) FROM audit_log
 WHERE tenant_id=$1 AND target_id=$2 AND action IN ('workspace.created','role.granted')`, tenant, alpha).Scan(&n); e != nil {
			t.Fatal(e)
		}
		return n
	}
	if n := auditRows(); n != 2 {
		t.Fatalf("first sync audit rows: %d", n)
	}
	// Switching the baize project yields a different workspace; the stale ws.editor
	// binding on the first workspace must not keep it listed.
	beta := workspaceID("tok-beta")
	if beta == alpha {
		t.Fatalf("project switch returned the same workspace %s", beta)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+beta, "tok-beta", nil); code != 200 {
		t.Fatalf("current workspace: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+alpha, "tok-beta", nil); code != 404 {
		t.Fatalf("other workspace: %d %v", code, v)
	}
	// A token without currentProject falls back to defaultProject.
	if fallback := workspaceID("tok-default"); fallback == alpha || fallback == beta {
		t.Fatalf("default project fallback reused %s", fallback)
	}
	// Workspace lifecycle belongs to baize.
	for _, test := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/v1/workspaces", map[string]any{"name": "Nope"}},
		{http.MethodPatch, "/v1/workspaces/" + beta, map[string]any{"name": "Nope"}},
		{http.MethodPost, "/v1/workspaces/" + beta + "/members", map[string]any{"principal_id": ids.New(), "role": "ws.viewer"}},
		{http.MethodDelete, "/v1/workspaces/" + beta + "/members/" + ids.New(), nil},
	} {
		if code, v = request(test.method, test.path, "tok-beta", test.body); code != 403 {
			t.Fatalf("%s %s: %d %v", test.method, test.path, code, v)
		} else if errCode := v["error"].(map[string]any)["code"]; errCode != "FORBIDDEN" {
			t.Fatalf("%s %s: code %v", test.method, test.path, errCode)
		}
	}
	if n := auditRows(); n != 2 {
		t.Fatalf("repeat syncs grew the audit log: %d", n)
	}
	// A user without any baize project sees no workspaces and none are created.
	code, v = request(http.MethodGet, "/v1/workspaces", "tok-noproject", nil)
	if code != 200 || len(v["items"].([]any)) != 0 {
		t.Fatalf("no project: %d %v", code, v)
	}
	var n int
	if e = admin.QueryRow(ctx,
		"SELECT count(*) FROM workspaces WHERE tenant_id=$1 AND external_ref IS NULL", tenant).Scan(&n); e != nil {
		t.Fatal(e)
	}
	if n != 0 {
		t.Fatalf("workspaces without external_ref: %d", n)
	}
}

// TestIAMWorkspaceIsolation checks D9 beyond /v1/workspaces: after a baize
// project switch (or losing the project), the old project's sessions, runs,
// artifacts, schedules, assets and installs are unreachable, while the
// tenant-wide catalog stays visible.
func TestIAMWorkspaceIsolation(t *testing.T) {
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
	slug := "iam-iso-" + ids.New()
	var tenant string
	if _, e = admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,'IAM')", ids.New(), slug); e != nil {
		t.Fatal(e)
	}
	if e = admin.QueryRow(ctx, "SELECT id::text FROM tenants WHERE slug=$1", slug).Scan(&tenant); e != nil {
		t.Fatal(e)
	}
	project := func(id, name string) map[string]any {
		return map[string]any{"id": id, "name": name, "displayName": name}
	}
	user := func(id string, projects ...map[string]any) map[string]any {
		u := map[string]any{"id": id, "username": id, "nickname": id}
		if len(projects) > 0 {
			u["currentProject"] = projects[0]
		}
		return u
	}
	tokens := map[string]map[string]any{
		"iso-a":      user("iso-u-1", project("iso-p-1", "IsoA")),
		"iso-b":      user("iso-u-1", project("iso-p-2", "IsoB")),
		"iso-2-a":    user("iso-u-2", project("iso-p-1", "IsoA")),
		"iso-2-none": user("iso-u-2"),
	}
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := bytes.CutPrefix([]byte(r.Header.Get("Authorization")), []byte("Bearer "))
		body, ok := tokens[string(token)]
		if !ok {
			http.Error(w, "unknown token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"user": body}})
	}))
	defer iam.Close()
	c := config.Config{
		AuthMode:          "iam",
		IAMBaseURL:        iam.URL,
		DefaultTenant:     slug,
		KeyFile:           t.TempDir() + "/signing.key",
		CapabilityKeyFile: t.TempDir() + "/capability.key",
		StreamKeyFile:     t.TempDir() + "/stream.key",
		StreamKeyID:       "test",
		CapabilityKeyID:   "1",
		SecretsKeyVersion: 1,
		SecretsKEKs:       map[int][]byte{1: make([]byte, 32)},
	}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, e := auth.New(ctx, app, c, l)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	server := httptest.NewServer((&httpapi.Handler{DB: app, Auth: a, Config: c, Logger: l, Assets: &assets.Service{}}).Router())
	defer server.Close()
	request := func(method, path, token string, body any) (int, map[string]any) {
		t.Helper()
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		r, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(b))
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
		if len(data) > 0 {
			if e = json.Unmarshal(data, &result); e != nil {
				t.Fatal(string(data))
			}
		}
		return resp.StatusCode, result
	}
	itemIDs := func(token, path string) []string {
		t.Helper()
		code, v := request(http.MethodGet, path, token, nil)
		if code != 200 {
			t.Fatalf("%s: %d %v", path, code, v)
		}
		ids := []string{}
		for _, item := range v["items"].([]any) {
			ids = append(ids, item.(map[string]any)["id"].(string))
		}
		return ids
	}

	// Project A: seed a session, run, artifact, schedule, workspace asset,
	// tenant-wide catalog asset and an install.
	code, v := request(http.MethodGet, "/v1/workspaces", "iso-a", nil)
	if code != 200 || len(v["items"].([]any)) != 1 {
		t.Fatalf("project A workspaces: %d %v", code, v)
	}
	wsA := v["items"].([]any)[0].(map[string]any)["id"].(string)
	var u1 string
	if e = admin.QueryRow(ctx, "SELECT id::text FROM principals WHERE idp_subject=$1", "baize-iam|iso-u-1").Scan(&u1); e != nil {
		t.Fatal(e)
	}
	session, run, artifact, schedule, assetA, assetT, install := ids.New(), ids.New(), ids.New(), ids.New(), ids.New(), ids.New(), ids.New()
	if _, e = admin.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$4,'IsoA','agent','iso-agent','queue','idle')`, session, tenant, wsA, u1); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO runs(id,tenant_id,session_id,trigger,status)
 VALUES($1,$2,$3,'api','succeeded')`, run, tenant, session); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,'out.txt','text/plain',1,'d','t/x','final')`, artifact, tenant, session, run); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO schedules(id,tenant_id,workspace_id,owner_id,cron,timezone,name,entry_kind,entry_ref,prompt,
 budget_millicredits,overlap_policy,catchup_window,temporal_schedule_id,status)
 VALUES($1,$2,$3,$4,'0 9 * * *','UTC','IsoA','agent','iso-agent','go',1000,'skip',interval '1 hour','iso','active')`,
		schedule, tenant, wsA, u1); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,workspace_id,owner_id,status)
 VALUES($1,$2,'iso','agent','iso-agent','workspace',$3,$4,'active'),($5,$2,'iso','agent','iso-catalog','tenant',NULL,$4,'active')`,
		assetA, tenant, wsA, u1, assetT); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO asset_installs(id,tenant_id,scope_type,scope_id,asset_id,"constraint",enabled)
 VALUES($1,$2,'workspace',$3,$4,'*',true)`, install, tenant, wsA, assetA); e != nil {
		t.Fatal(e)
	}
	// While A is current, its data is reachable.
	if code, v = request(http.MethodGet, "/v1/sessions/"+session, "iso-a", nil); code != 200 {
		t.Fatalf("session in current project: %d %v", code, v)
	}
	if got := itemIDs("iso-a", "/v1/schedules?workspace_id="+wsA); len(got) != 1 || got[0] != schedule {
		t.Fatalf("schedules in current project: %v", got)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+wsA+"/installs", "iso-a", nil); code != 200 {
		t.Fatalf("installs in current project: %d %v", code, v)
	}

	// Switch to project B: nothing from A remains reachable.
	code, v = request(http.MethodGet, "/v1/workspaces", "iso-b", nil)
	if code != 200 || len(v["items"].([]any)) != 1 || v["items"].([]any)[0].(map[string]any)["id"].(string) == wsA {
		t.Fatalf("project B workspaces: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+wsA, "iso-b", nil); code != 404 {
		t.Fatalf("workspace A after switch: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+wsA+"/installs", "iso-b", nil); code != 403 {
		t.Fatalf("installs list after switch: %d %v", code, v)
	}
	if code, v = request(http.MethodPost, "/v1/workspaces/"+wsA+"/installs", "iso-b", map[string]any{"asset_id": assetA}); code != 404 {
		t.Fatalf("install create after switch: %d %v", code, v)
	}
	if got := itemIDs("iso-b", "/v1/sessions?workspace_id="+wsA); len(got) != 0 {
		t.Fatalf("sessions of A after switch: %v", got)
	}
	if got := itemIDs("iso-b", "/v1/sessions"); len(got) != 0 {
		t.Fatalf("sessions after switch: %v", got)
	}
	if code, v = request(http.MethodGet, "/v1/sessions/"+session, "iso-b", nil); code != 404 {
		t.Fatalf("session by id after switch: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/runs/"+run, "iso-b", nil); code != 404 {
		t.Fatalf("run by id after switch: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/artifacts/"+artifact, "iso-b", nil); code != 404 {
		t.Fatalf("artifact by id after switch: %d %v", code, v)
	}
	if got := itemIDs("iso-b", "/v1/schedules?workspace_id="+wsA); len(got) != 0 {
		t.Fatalf("schedules of A after switch: %v", got)
	}
	if got := itemIDs("iso-b", "/v1/assets?workspace_id="+wsA); len(got) != 0 {
		t.Fatalf("assets of A after switch: %v", got)
	}
	if code, v = request(http.MethodGet, "/v1/assets/"+assetA, "iso-b", nil); code != 404 {
		t.Fatalf("asset by id after switch: %d %v", code, v)
	}
	// The tenant-wide catalog is not project-scoped and stays visible.
	if got := itemIDs("iso-b", "/v1/assets"); len(got) != 1 || got[0] != assetT {
		t.Fatalf("catalog after switch: %v", got)
	}

	// A user whose project disappears keeps a stale binding but sees nothing.
	if got := itemIDs("iso-2-a", "/v1/workspaces"); len(got) != 1 || got[0] != wsA {
		t.Fatalf("second user project A: %v", got)
	}
	if got := itemIDs("iso-2-none", "/v1/workspaces"); len(got) != 0 {
		t.Fatalf("no-project workspaces: %v", got)
	}
	if got := itemIDs("iso-2-none", "/v1/sessions"); len(got) != 0 {
		t.Fatalf("no-project sessions: %v", got)
	}
	if code, v = request(http.MethodGet, "/v1/workspaces/"+wsA, "iso-2-none", nil); code != 404 {
		t.Fatalf("no-project workspace: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/sessions/"+session, "iso-2-none", nil); code != 404 {
		t.Fatalf("no-project session: %d %v", code, v)
	}
	if code, v = request(http.MethodGet, "/v1/assets/"+assetA, "iso-2-none", nil); code != 404 {
		t.Fatalf("no-project asset: %d %v", code, v)
	}
	if got := itemIDs("iso-2-none", "/v1/assets"); len(got) != 1 || got[0] != assetT {
		t.Fatalf("no-project catalog: %v", got)
	}
}

// TestIAMSubmissionClamp drives the message-send chain (BuildStart re-contexts
// the request) and the usage/asset surfaces: a restricted iam user must not
// pull project A's workspace-visibility asset into a run lock, tenant-wide and
// directly granted assets stay usable, and its usage report only covers the
// current project.
func TestIAMSubmissionClamp(t *testing.T) {
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
	slug := "iam-msg-" + ids.New()
	var tenant string
	if _, e = admin.Exec(ctx, "INSERT INTO tenants(id,slug,name) VALUES($1,$2,'IAM')", ids.New(), slug); e != nil {
		t.Fatal(e)
	}
	if e = admin.QueryRow(ctx, "SELECT id::text FROM tenants WHERE slug=$1", slug).Scan(&tenant); e != nil {
		t.Fatal(e)
	}
	project := func(id, name string) map[string]any {
		return map[string]any{"id": id, "name": name, "displayName": name}
	}
	tokens := map[string]map[string]any{
		"msg-a": map[string]any{"id": "msg-u-1", "username": "msg-u-1", "nickname": "msg-u-1",
			"currentProject": project("msg-p-1", "MsgA")},
		"msg-b": map[string]any{"id": "msg-u-1", "username": "msg-u-1", "nickname": "msg-u-1",
			"currentProject": project("msg-p-2", "MsgB")},
	}
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, _ := bytes.CutPrefix([]byte(r.Header.Get("Authorization")), []byte("Bearer "))
		body, ok := tokens[string(token)]
		if !ok {
			http.Error(w, "unknown token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"user": body}})
	}))
	defer iam.Close()
	c := config.Config{
		AuthMode:          "iam",
		IAMBaseURL:        iam.URL,
		DefaultTenant:     slug,
		KeyFile:           t.TempDir() + "/signing.key",
		CapabilityKeyFile: t.TempDir() + "/capability.key",
		StreamKeyFile:     t.TempDir() + "/stream.key",
		StreamKeyID:       "test",
		CapabilityKeyID:   "1",
		SecretsKeyVersion: 1,
		SecretsKEKs:       map[int][]byte{1: make([]byte, 32)},
	}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, e := auth.New(ctx, app, c, l)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	signer, e := assets.LoadSigner(t.TempDir()+"/assets.key", "test", true, nil)
	if e != nil {
		t.Fatal(e)
	}
	blobs := testBlobs{root: t.TempDir()}
	assetSvc := &assets.Service{Store: blobs, Signer: signer}
	server := httptest.NewServer((&httpapi.Handler{DB: app, Auth: a, Config: c, Logger: l, Assets: assetSvc,
		Objects: runObjects{blobs}, Temporal: &runTemporal{}}).Router())
	defer server.Close()
	request := func(method, path, token string, body any) (int, map[string]any) {
		t.Helper()
		var b []byte
		if body != nil {
			b, _ = json.Marshal(body)
		}
		r, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(b))
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
		if len(data) > 0 {
			if e = json.Unmarshal(data, &result); e != nil {
				t.Fatal(string(data))
			}
		}
		return resp.StatusCode, result
	}

	// Workspaces for both projects; the user lands on project B.
	workspaces := func(token string) map[string]string {
		code, v := request(http.MethodGet, "/v1/workspaces", token, nil)
		if code != 200 {
			t.Fatalf("workspaces: %d %v", code, v)
		}
		out := map[string]string{}
		for _, item := range v["items"].([]any) {
			w := item.(map[string]any)
			out[w["name"].(string)] = w["id"].(string)
		}
		return out
	}
	wsA := workspaces("msg-a")["MsgA"]
	wsB := workspaces("msg-b")["MsgB"]
	if wsA == "" || wsB == "" || wsA == wsB {
		t.Fatal(wsA, wsB)
	}
	var u1 string
	if e = admin.QueryRow(ctx, "SELECT id::text FROM principals WHERE idp_subject=$1", "baize-iam|msg-u-1").Scan(&u1); e != nil {
		t.Fatal(e)
	}

	// A published agent in the tenant catalog whose version depends on project
	// A's workspace-visibility asset.
	dep := ids.New()
	agent := ids.New()
	assetName := map[string]string{dep: "msg-dep", agent: "msg-agent"}
	agentJSON := []byte(`{"name":"iso","schema":"orbit.agent/v1"}`)
	hash := sha256.Sum256(agentJSON)
	fileDigest := "sha256:" + hex.EncodeToString(hash[:])
	bundleDigest, e := contract.BundleDigest(map[string]string{"agent.json": fileDigest})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,workspace_id,owner_id,status)
 VALUES($1,$2,'iso','agent','msg-dep','workspace',$3,$4,'active'),($5,$2,'iso','agent','msg-agent','tenant',NULL,$4,'active')`,
		dep, tenant, wsA, u1, agent); e != nil {
		t.Fatal(e)
	}
	permissionsJSON := []byte(`{"network":{"egress":[]},"sandbox_profile":"default"}`)
	// submissionBudget reads the locked agent.json; use valid manifest bytes.
	if _, e = blobs.Put(ctx, tenant, fileDigest, agentJSON); e != nil {
		t.Fatal(e)
	}
	// SelectVersion verifies an ed25519 signature over the canonical identity
	// and permissions, and the row is immutable once written: sign before insert.
	publish := func(assetID, manifest string) {
		t.Helper()
		version := ids.New()
		asset := assets.Asset{Kind: "agent", Publisher: "iso", Name: assetName[assetID]}
		var m assets.Manifest
		if e = json.Unmarshal([]byte(manifest), &m); e != nil {
			t.Fatal(e)
		}
		var perms contract.EffectivePermissions
		if e = json.Unmarshal(permissionsJSON, &perms); e != nil {
			t.Fatal(e)
		}
		v := assets.Version{Version: "1.0.0", Digest: bundleDigest, Manifest: m, Permissions: perms}
		pj, e := jsonv2.Marshal(struct {
			Effective contract.EffectivePermissions            `json:"effective_permissions"`
			Members   map[string]contract.EffectivePermissions `json:"members"`
		}{Effective: perms}, jsonv2.Deterministic(true))
		if e != nil {
			t.Fatal(e)
		}
		if v.PermissionsJCS, e = jcs.Canonicalize(pj); e != nil {
			t.Fatal(e)
		}
		if v.IdentityJCS, e = assets.SigningIdentity(asset, "1.0.0", bundleDigest, v.PermissionsJCS); e != nil {
			t.Fatal(e)
		}
		signature, e := signer.Sign(asset, v)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = admin.Exec(ctx, `INSERT INTO asset_versions(id,tenant_id,asset_id,version,manifest,content_digest,blob_index_ref,
 permissions,compat,state,uploaded_by,permissions_jcs,identity_jcs,signature)
 VALUES($1,$2,$3,'1.0.0',$4::jsonb,$5,'idx',$6::jsonb,'{}'::jsonb,'published',$7,$8,$9,$10)`,
			version, tenant, assetID, manifest, bundleDigest, permissionsJSON, u1, v.PermissionsJCS, v.IdentityJCS, signature); e != nil {
			t.Fatal(e)
		}
		if _, e = admin.Exec(ctx, `INSERT INTO asset_files(tenant_id,version_id,path,sha256,size_bytes,is_text,blob_ref)
 VALUES($1,$2,'agent.json',$3,$4,true,'idx')`, tenant, version, fileDigest, len(agentJSON)); e != nil {
			t.Fatal(e)
		}
	}
	// The dependency is fully resolvable on its own, so only the visibility
	// clamp can keep it out of a run lock.
	publish(dep, `{}`)
	publish(agent, `{"dependencies":[{"kind":"agent","ref":"iso/msg-dep","constraint":"*"}]}`)

	// From project A the same entry resolves and the run starts; from project B
	// the dependency on A's asset must fail lock resolution instead of entering
	// the run lock and sandbox.
	startSession := func(token, ws, title, entry string, want int) map[string]any {
		t.Helper()
		session := ids.New()
		if _, e = admin.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$4,$5,'agent',$6,'queue','idle')`, session, tenant, ws, u1, title, entry); e != nil {
			t.Fatal(e)
		}
		code, v := request(http.MethodPost, "/v1/sessions/"+session+"/messages", token, map[string]any{"text": "hello"})
		if code != want {
			t.Fatalf("%s message: %d %v", title, code, v)
		}
		return v
	}
	if v := startSession("msg-a", wsA, "MsgA", "iso/msg-agent", 202); v["run_id"] == "" {
		t.Fatalf("project A run: %v", v)
	}
	if v := startSession("msg-b", wsB, "MsgB", "iso/msg-agent", 422); v["error"].(map[string]any)["code"] != "LOCK_RESOLUTION_FAILED" {
		t.Fatalf("message with foreign dependency: %v", v)
	}
	var code int
	var result map[string]any

	// A tenant-visibility asset keeps its workspace_id after ChangeVisibility and
	// stays usable from another project; so does a private asset granted directly.
	// Both belong to another principal, so visibility comes from the tenant rule
	// and the grant, not ownership.
	other := ids.New()
	if _, e = admin.Exec(ctx, `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user',$2,'Other',$3)`, other, other+"@example.test", "baize-iam|"+other); e != nil {
		t.Fatal(e)
	}
	kept := ids.New()
	granted := ids.New()
	if _, e = admin.Exec(ctx, `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,workspace_id,owner_id,status)
 VALUES($1,$2,'iso','agent','msg-kept','tenant',$3,$4,'active'),($5,$2,'iso','agent','msg-granted','private',$3,$4,'active')`,
		kept, tenant, wsA, other, granted); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(ctx, `INSERT INTO asset_grants(tenant_id,asset_id,grantee_type,grantee_id,permission)
 VALUES($1,$2,'user',$3,'use')`, tenant, granted, u1); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{kept, granted} {
		if code, result = request(http.MethodGet, "/v1/assets/"+id, "msg-b", nil); code != 200 {
			t.Fatalf("asset %s from project B: %d %v", id, code, result)
		}
	}
	// Starting sessions on the kept and directly granted assets from project B
	// shows the workspace clamp does not over-block non-workspace assets.
	assetName[kept] = "msg-kept"
	publish(kept, `{}`)
	if v := startSession("msg-b", wsB, "MsgKept", "iso/msg-kept", 202); v["run_id"] == "" {
		t.Fatalf("catalog run from project B: %v", v)
	}
	assetName[granted] = "msg-granted"
	publish(granted, `{}`)
	if v := startSession("msg-b", wsB, "MsgGranted", "iso/msg-granted", 202); v["run_id"] == "" {
		t.Fatalf("directly granted run from project B: %v", v)
	}

	// The user's own usage report only covers the current project.
	hour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	for _, ws := range []string{wsA, wsB} {
		if _, e = admin.Exec(ctx, `INSERT INTO usage_rollups(tenant_id,period_start,workspace_id,principal_id,asset_ref,
 model_profile_id,member,source,meter,quantity,millicredits,entries)
 VALUES($1,$2,$3,$4,'',$5,'','model','tokens.input',1,10,1)`, tenant, hour, ws, u1, ids.New()); e != nil {
			t.Fatal(e)
		}
	}
	code, result = request(http.MethodGet, "/v1/usage?scope=user&group_by=workspace", "msg-b", nil)
	if code != 200 {
		t.Fatalf("usage: %d %v", code, result)
	}
	items := result["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("usage items: %v", items)
	}
	if ws := items[0].(map[string]any)["workspace_id"]; ws != wsB {
		t.Fatalf("usage workspace: %v want %s", ws, wsB)
	}
}
