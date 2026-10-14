package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/workflows/assetrelease"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A file-backed test blob store exercises the same digest/tenant namespace. E2E
// uses real MinIO. Neither is a fallback database or production storage mode.
type testBlobs struct{ root string }

func (s testBlobs) Put(_ context.Context, tenant, digest string, data []byte) (string, error) {
	key, e := objstore.Key(tenant, digest)
	if e != nil {
		return "", e
	}
	p := filepath.Join(s.root, key)
	if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return "", e
	}
	e = os.WriteFile(p, data, 0600)
	return key, e
}
func (s testBlobs) Get(_ context.Context, tenant, digest string, limit int64) ([]byte, error) {
	key, e := objstore.Key(tenant, digest)
	if e != nil {
		return nil, e
	}
	data, e := os.ReadFile(filepath.Join(s.root, key))
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("size limit")
	}
	return data, e
}
func testAssets(t *testing.T, d *db.DB, admin *pgx.Conn) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := config.Config{AuthMode: "dev", PersonalTenants: true, KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key", CapabilityKeyID: "1", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	signer, e := assets.LoadSigner(t.TempDir()+"/assets.key", "test", true, nil)
	if e != nil {
		t.Fatal(e)
	}
	var store objstore.Store = testBlobs{t.TempDir()}
	if endpoint := os.Getenv("ORBIT_TEST_OBJECT_ENDPOINT"); endpoint != "" {
		store, e = objstore.New(endpoint, os.Getenv("ORBIT_TEST_OBJECT_BUCKET"), os.Getenv("ORBIT_TEST_OBJECT_ACCESS_KEY"), os.Getenv("ORBIT_TEST_OBJECT_SECRET_KEY"), false)
		if e != nil {
			t.Fatal(e)
		}
	}
	service := &assets.Service{Store: store, Signer: signer}
	activities := &assetrelease.Activities{DB: d, Assets: service}
	a, e := auth.New(ctx, d, c, logger)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	server := httptest.NewServer((&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger, Assets: service}).Router())
	defer server.Close()
	type uploadBody struct {
		data        []byte
		contentType string
	}
	request := func(method, path, token, tenant, key string, body any) (int, map[string]any, []byte) {
		t.Helper()
		var raw []byte
		contentType := "application/octet-stream"
		switch v := body.(type) {
		case []byte:
			raw = v
		case uploadBody:
			raw, contentType = v.data, v.contentType
		case nil:
		default:
			raw, e = json.Marshal(v)
			if e != nil {
				t.Fatal(e)
			}
		}
		r, e := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Orbit-Tenant", tenant)
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Content-Type", contentType)
		response, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		data, e := io.ReadAll(response.Body)
		if e != nil {
			t.Fatal(e)
		}
		var out map[string]any
		if strings.Contains(response.Header.Get("Content-Type"), "application/json") {
			if e = json.Unmarshal(data, &out); e != nil {
				t.Fatal(e)
			}
		}
		return response.StatusCode, out, data
	}
	check := func(got, want int, out any) {
		t.Helper()
		if got != want {
			t.Fatalf("status %d want %d: %v", got, want, out)
		}
	}
	login := func(prefix string) (string, string, string) {
		code, out, _ := request("POST", "/v1/auth/dev/token", "", "", "", map[string]any{"email": prefix + ids.New() + "@example.test"})
		check(code, 200, out)
		token := out["access_token"].(string)
		code, out, _ = request("GET", "/v1/me", token, "", "", nil)
		check(code, 200, out)
		return token, out["tenants"].([]any)[0].(map[string]any)["id"].(string), out["principal"].(map[string]any)["id"].(string)
	}
	token, tenant, owner := login("asset-owner-")
	reviewer, _, reviewerID := login("asset-reviewer-")
	other, otherTenant, _ := login("asset-other-")
	code, out, _ := request("POST", "/v1/admin/members", token, tenant, "", map[string]any{"principal_id": reviewerID, "role": "tenant.reviewer"})
	check(code, 201, out)
	code, out, _ = request("POST", "/v1/workspaces", token, tenant, "", map[string]any{"name": "Assets"})
	check(code, 201, out)
	workspace := out["id"].(string)
	code, out, _ = request("POST", "/v1/assets", token, tenant, "", map[string]any{"kind": "skill", "name": "group-workspace", "visibility": "workspace", "workspace_id": workspace})
	check(code, 201, out)
	workspaceAsset := "/v1/assets/" + out["id"].(string)
	code, out, _ = request("GET", workspaceAsset, reviewer, tenant, "", nil)
	check(code, 404, out)
	groupID := ids.New()
	if e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `INSERT INTO groups(id,tenant_id,name) VALUES($1,$2,'asset readers')`, groupID, tenant); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `INSERT INTO group_members(tenant_id,group_id,principal_id) VALUES($1,$2,$3)`, tenant, groupID, reviewerID); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by) VALUES($1,$2,'group',$3,'ws.member','workspace',$4,$5)`, ids.New(), tenant, groupID, workspace, owner)
		return e
	}); e != nil {
		t.Fatal(e)
	}
	code, out, _ = request("GET", workspaceAsset, reviewer, tenant, "", nil)
	check(code, 200, out)
	workspaceGrant := map[string]any{"grantee_type": "group", "grantee_id": groupID, "permission": "maintain"}
	code, out, _ = request("POST", workspaceAsset+"/grants", token, tenant, "", workspaceGrant)
	check(code, 202, out)
	code, out, _ = request("DELETE", workspaceAsset+"/grants", token, tenant, "", workspaceGrant)
	check(code, 204, out)
	code, out, _ = request("POST", workspaceAsset+"/grants", token, tenant, "", map[string]any{"grantee_type": "workspace", "grantee_id": workspace, "permission": "maintain"})
	check(code, 202, out)
	// The group is the only source of workspace access; no direct membership
	// record is added to make either visibility or the grant boundary pass.
	code, out, _ = request("POST", workspaceAsset+":suspend", reviewer, tenant, "", nil)
	check(code, 202, out)
	if e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `DELETE FROM group_members WHERE group_id=$1 AND principal_id=$2`, groupID, reviewerID)
		return e
	}); e != nil {
		t.Fatal(e)
	}
	code, out, _ = request("GET", workspaceAsset, reviewer, tenant, "", nil)
	check(code, 404, out)
	code, out, _ = request("POST", workspaceAsset+"/grants", token, tenant, "", map[string]any{"grantee_type": "user", "grantee_id": reviewerID, "permission": "use"})
	check(code, 403, out)
	code, out, _ = request("POST", "/v1/assets", token, tenant, "", map[string]any{"kind": "agent", "name": "j5-agent", "visibility": "tenant", "workspace_id": workspace})
	check(code, 201, out)
	asset := out["id"].(string)
	publisher := out["publisher"].(string)
	code, out, _ = request("GET", "/v1/assets/"+asset, other, otherTenant, "", nil)
	check(code, 404, out)
	base := "/v1/assets/" + asset
	code, out, _ = request("POST", "/v1/assets", token, tenant, "", map[string]any{"kind": "skill", "name": "private-share", "visibility": "private", "workspace_id": workspace})
	check(code, 201, out)
	privateBase := "/v1/assets/" + out["id"].(string)
	code, out, _ = request("GET", privateBase, reviewer, tenant, "", nil)
	check(code, 404, out)
	grant := map[string]any{"grantee_type": "user", "grantee_id": reviewerID, "permission": "use"}
	code, out, _ = request("POST", privateBase+"/grants", token, tenant, "", grant)
	check(code, 202, out)
	code, out, _ = request("GET", privateBase, reviewer, tenant, "", nil)
	check(code, 200, out)
	code, out, _ = request("POST", privateBase+"/grants", token, tenant, "", map[string]any{"grantee_type": "tenant", "grantee_id": tenant, "permission": "use"})
	check(code, 403, out)
	code, out, _ = request("GET", privateBase+"/grants?limit=1", token, tenant, "", nil)
	check(code, 200, out)
	if len(out["items"].([]any)) != 1 {
		t.Fatal("sharing grant missing")
	}
	code, out, _ = request("DELETE", privateBase+"/grants", token, tenant, "", grant)
	check(code, 204, out)
	code, out, _ = request("GET", privateBase, reviewer, tenant, "", nil)
	check(code, 404, out)
	upload := func(version string) string {
		raw, e := bundle.Pack(map[string][]byte{"AGENTS.md": []byte("Work safely."), "agent.json": []byte(`{"schema":"orbit.agent/v2","name":"j5-agent","version":"` + version + `","permissions":{"effectClasses":["read_only"]}}`)})
		if e != nil {
			t.Fatal(e)
		}
		key := "upload-" + version
		body := uploadBody{data: raw, contentType: "application/octet-stream"}
		if version == "1.1.0" {
			var form bytes.Buffer
			writer := multipart.NewWriter(&form)
			part, err := writer.CreateFormFile("archive", "bundle.zip")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = part.Write(raw); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			body = uploadBody{data: form.Bytes(), contentType: writer.FormDataContentType()}
		}
		code, out, first := request("POST", base+"/versions?version="+version, token, tenant, key, body)
		check(code, 201, out)
		id := out["id"].(string)
		code, out, replay := request("POST", base+"/versions?version="+version, token, tenant, key, body)
		check(code, 201, out)
		if !bytes.Equal(first, replay) {
			t.Fatal("upload replay differs")
		}
		code, out, _ = request("POST", base+"/versions?version="+version, token, tenant, key, []byte("changed"))
		check(code, 422, out)
		return id
	}
	code, out, _ = request("POST", base+"/versions?version=1.0.0", token, tenant, "invalid-multipart", uploadBody{data: []byte("malformed"), contentType: "multipart/form-data; boundary=missing"})
	check(code, 422, out)
	first := upload("1.0.0")
	code, out, _ = request("POST", base+"/versions/"+first+":transition", token, tenant, "", map[string]any{"state": "published"})
	check(code, 409, out)
	code, out, _ = request("GET", base+"/versions/"+first+"/files?limit=1", token, tenant, "", nil)
	check(code, 200, out)
	if out["next_cursor"] == "" {
		t.Fatal("missing file cursor")
	}
	code, _, data := request("GET", base+"/versions/"+first+"/file?path=AGENTS.md", token, tenant, "", nil)
	check(code, 200, string(data))
	if string(data) != "Work safely." {
		t.Fatal("wrong content")
	}
	var tc client.Client
	var temporalWorker worker.Worker
	var fake worker.Worker
	var dispatchDone chan struct{}
	if address := os.Getenv("ORBIT_TEST_TEMPORAL_ADDRESS"); address != "" {
		namespace := os.Getenv("ORBIT_TEST_TEMPORAL_NAMESPACE")
		if namespace == "" {
			namespace = "orbit-control-wp5-test"
		}
		conn, e := client.Dial(client.Options{HostPort: address, Namespace: namespace})
		if e != nil {
			t.Fatal(e)
		}
		defer conn.Close()
		tc = conn
		_, e = tc.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{Namespace: namespace, WorkflowExecutionRetentionPeriod: durationOneDay()})
		if e != nil && !strings.Contains(e.Error(), "already") {
			t.Fatal(e)
		}
		temporalWorker = assetrelease.NewWorker(tc, activities)
		if e = temporalWorker.Start(); e != nil {
			t.Fatal(e)
		}
		defer temporalWorker.Stop()
		fake = worker.New(tc, contract.TaskQueueAssetCheck, worker.Options{})
		for i, name := range []string{contract.ActivityAssetCheckStructure, contract.ActivityAssetCheckCompat, contract.ActivityAssetCheckSecrets, contract.ActivityAssetCheckStatic, contract.ActivityAssetCheckInjection} {
			kind := contract.AssetCheckKind([]string{"structure", "compat", "secrets", "static", "injection"}[i])
			fake.RegisterActivityWithOptions(func(_ context.Context, input contract.AssetCheckInput) (contract.AssetCheckReport, error) {
				if input.TenantId != tenant {
					return contract.AssetCheckReport{}, fmt.Errorf("unexpected tenant")
				}
				return contract.AssetCheckReport{Check: kind, CheckerVersion: "j5-fake/1"}, nil
			}, activity.RegisterOptions{Name: name})
		}
		if e = fake.Start(); e != nil {
			t.Fatal(e)
		}
		defer fake.Stop()
		dispatchCtx, cancel := context.WithCancel(ctx)
		dispatchDone = make(chan struct{})
		go func() { defer close(dispatchDone); _ = assetrelease.Dispatch(dispatchCtx, d, tc, logger) }()
		defer func() { cancel(); <-dispatchDone }()
	} else if os.Getenv("ORBIT_REQUIRE_ASSET_E2E") == "1" {
		t.Fatal("ORBIT_TEST_TEMPORAL_ADDRESS required")
	}
	release := func(id string) {
		code, out, _ = request("POST", base+"/versions/"+id+":submit", token, tenant, "submit-"+id, map[string]any{"channel": "stable"})
		check(code, 202, out)
		if tc != nil {
			waitAssetState(t, request, base, id, token, tenant, "in_review")
			code, out, _ = request("POST", "/v1/reviews/"+id+":decide", token, tenant, "", map[string]any{"decision": "approve"})
			check(code, 403, out)
			code, out, _ = request("POST", "/v1/reviews/"+id+":decide", reviewer, tenant, "review-"+id, map[string]any{"decision": "approve"})
			check(code, 202, out)
			waitAssetState(t, request, base, id, token, tenant, "published")
			workflowRun := tc.GetWorkflow(ctx, assetrelease.ID(assetrelease.Input{Tenant: tenant, VersionID: id}), "")
			var result assetrelease.Result
			if e := workflowRun.Get(ctx, &result); e != nil {
				t.Fatal(e)
			}
		} else {
			// Without Temporal, execute real SQL Activities through app credentials to
			// verify their atomic audit/RLS behavior; workflow orchestration has its own suite.
			in := assetrelease.Input{Tenant: tenant, VersionID: id, Generation: 1}
			snap, e := activities.SnapshotAssetRelease(ctx, in)
			if e != nil {
				t.Fatal(e)
			}
			reports := []contract.AssetCheckReport{}
			for _, kind := range []contract.AssetCheckKind{"structure", "compat", "secrets", "static", "injection", "permission_diff"} {
				reports = append(reports, contract.AssetCheckReport{Check: kind, CheckerVersion: "integration/1"})
			}
			if snap.Check.ContentDigest == "" {
				t.Fatal("empty digest")
			}
			if e = activities.AggregateAssetChecks(ctx, assetrelease.Aggregate{Input: in, Reports: reports, State: "approved"}); e != nil {
				t.Fatal(e)
			}
			if e = activities.Sign(ctx, in); e != nil {
				t.Fatal(e)
			}
			if e = activities.Publish(ctx, in); e != nil {
				t.Fatal(e)
			}
			if e = activities.Publish(ctx, in); e != nil {
				t.Fatal("publish replay", e)
			}
		}
	}
	release(first)
	code, out, _ = request("GET", base+"/versions/"+first+"/checks", token, tenant, "", nil)
	check(code, 200, out)
	if len(out["items"].([]any)) != 6 {
		t.Fatal("six checker reports not persisted")
	}
	code, out, _ = request("POST", "/v1/workspaces/"+workspace+"/installs", token, tenant, "", map[string]any{"asset_id": asset, "version_constraint": "^1.0", "channel": "stable"})
	check(code, 202, out)
	preview := func() string {
		code, out, _ := request("POST", "/v1/workspaces/"+workspace+"/locks:preview", token, tenant, "", map[string]any{"kind": "agent", "ref": publisher + "/j5-agent"})
		check(code, 200, out)
		if len(out["members"].([]any)) != 1 {
			t.Fatal("missing main member")
		}
		return out["entry"].(map[string]any)["version"].(string)
	}
	if preview() != "1.0.0" {
		t.Fatal("first lock version")
	}
	second := upload("1.1.0")
	release(second)
	if preview() != "1.1.0" {
		t.Fatal("channel upgrade not resolved")
	}
	code, out, _ = request("POST", base+"/channels/stable", token, tenant, "rollback", map[string]any{"version_id": first})
	check(code, 202, out)
	if preview() != "1.0.0" {
		t.Fatal("channel rollback not resolved")
	}
	code, out, _ = request("POST", base+"/channels/release-2026", token, tenant, "", map[string]any{"version_id": first})
	check(code, 202, out)
	code, out, _ = request("GET", base+"/channels?limit=1", token, tenant, "", nil)
	check(code, 200, out)
	if out["next_cursor"] == "" {
		t.Fatal("custom channel pagination missing")
	}
	// Simulate Publish's ambiguous-success failure mode. Compensation must not
	// overwrite the operator's later rollback; it restores only its own state.
	if e = activities.CompensateAssetPublish(ctx, assetrelease.Input{Tenant: tenant, VersionID: second, Generation: 1}); e != nil {
		t.Fatal(e)
	}
	if preview() != "1.0.0" {
		t.Fatal("compensation overwrote operator rollback")
	}
	var rollbackAudit int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND action='asset.channel_changed' AND detail->>'previous_version_id'=$2 AND detail->>'version_id'=$3`, tenant, second, first).Scan(&rollbackAudit); e != nil || rollbackAudit != 1 {
		t.Fatal("rollback audit", rollbackAudit, e)
	}
	code, out, _ = request("POST", base+"/versions/"+first+":transition", token, tenant, "", map[string]any{"state": "yanked"})
	check(code, 409, out)
	code, out, _ = request("POST", base+"/versions/"+first+":transition", token, tenant, "", map[string]any{"state": "suspended"})
	check(code, 200, out)
	code, out, _ = request("POST", "/v1/workspaces/"+workspace+"/locks:preview", token, tenant, "", map[string]any{"kind": "agent", "ref": publisher + "/j5-agent"})
	check(code, 422, out)
	code, out, _ = request("PATCH", base, token, tenant, "", map[string]any{"visibility": "private"})
	check(code, 409, out)
	// Frozen content cannot be changed even by the owner role.
	if _, e = admin.Exec(ctx, `UPDATE asset_versions SET content_digest='tampered' WHERE id=$1`, first); e == nil {
		t.Fatal("mutable asset content")
	}
	if e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error { _, e := audit.Verify(ctx, tx); return e }); e != nil {
		t.Fatal("audit chain", e)
	}
	testAssetGraphs(t, d, service, activities, tenant, owner, workspace, publisher)
	if tc != nil {
		saveReplay(t, tc, assetrelease.ID(assetrelease.Input{Tenant: tenant, VersionID: second}))
	}
}

type assetRequest func(string, string, string, string, string, any) (int, map[string]any, []byte)

func waitAssetState(t *testing.T, request assetRequest, base, id, token, tenant, want string) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		code, out, _ := request("GET", base+"/versions/"+id, token, tenant, "", nil)
		if code != 200 {
			t.Fatal(code, out)
		}
		state := out["state"].(string)
		if state == want {
			return
		}
		if state == "check_failed" || state == "rejected" {
			t.Fatalf("release failed: %v", out)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("release did not reach %s", want)
}
func saveReplay(t *testing.T, c client.Client, id string) {
	t.Helper()
	iter := c.GetWorkflowHistory(t.Context(), id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	h := &history.History{}
	for iter.HasNext() {
		e, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		h.Events = append(h.Events, e)
	}
	raw, e := protojson.Marshal(h)
	if e != nil {
		t.Fatal(e)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(assetrelease.Run, workflowRegisterOptions())
	if e = replayer.ReplayWorkflowHistory(nil, h); e != nil {
		t.Fatal(e)
	}
	if os.Getenv("ORBIT_UPDATE_REPLAY") == "1" {
		if e = os.MkdirAll("../internal/workflows/assetrelease/testdata", 0755); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile("../internal/workflows/assetrelease/testdata/release.json", raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
}

func durationOneDay() *durationpb.Duration { return durationpb.New(24 * time.Hour) }
func workflowRegisterOptions() workflow.RegisterOptions {
	return workflow.RegisterOptions{Name: assetrelease.Name}
}

func testAssetGraphs(t *testing.T, d *db.DB, s *assets.Service, activities *assetrelease.Activities, tenant, owner, workspace, publisher string) {
	t.Helper()
	ctx := t.Context()
	fixture := func(kind contract.AssetKind, name string, files map[string][]byte) assets.Asset {
		t.Helper()
		raw, e := bundle.Pack(files)
		if e != nil {
			t.Fatal(e)
		}
		var asset assets.Asset
		var version assets.Version
		e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, tenant)
			var e error
			asset, e = s.Create(ctx, kind, name, "private", workspace)
			if e != nil {
				return e
			}
			version, e = s.Upload(ctx, asset, "1.0.0", raw)
			if e != nil {
				return e
			}
			if e = s.Submit(ctx, asset, version.ID, "stable"); e != nil {
				return e
			}
			_, e = tx.Exec(ctx, `UPDATE asset_versions SET release_started=true WHERE id=$1`, version.ID)
			return e
		})
		if e != nil {
			t.Fatal(e)
		}
		in := assetrelease.Input{Tenant: tenant, VersionID: version.ID, Generation: 1}
		if _, e = activities.SnapshotAssetRelease(ctx, in); e != nil {
			t.Fatal(e)
		}
		reports := []contract.AssetCheckReport{}
		for _, kind := range []contract.AssetCheckKind{"structure", "compat", "secrets", "static", "injection", "permission_diff"} {
			reports = append(reports, contract.AssetCheckReport{Check: kind, CheckerVersion: "graph-fixture/1"})
		}
		if e = activities.AggregateAssetChecks(ctx, assetrelease.Aggregate{Input: in, Reports: reports, State: "approved"}); e != nil {
			t.Fatal(e)
		}
		if e = activities.Sign(ctx, in); e != nil {
			t.Fatal(e)
		}
		if e = activities.Publish(ctx, in); e != nil {
			t.Fatal(e)
		}
		return asset
	}
	skill := fixture("skill", "graph-skill", map[string][]byte{"SKILL.md": []byte("---\nname: graph-skill\ndescription: test\n---\nSkill instructions")})
	second := fixture("skill", "graph-shared", map[string][]byte{"SKILL.md": []byte("shared")})
	mcp := fixture("mcp_server", "graph-mcp", map[string][]byte{"mcp.json": []byte(`{"mcpServers":{"remote":{"url":"https://example.test","transport":"sse"}}}`)})
	ref := publisher + "/" + skill.Name
	shared := publisher + "/" + second.Name
	fixture("agent", "graph-agent", map[string][]byte{"AGENTS.md": []byte("agent"), "skills.json": []byte(`{"skills":[{"name":"graph-skill","source":"catalog","id":"` + ref + `","version":"^1.0"}]}`), "mcp.json": []byte(`{"mcpServers":{"remote":{"ref":"` + publisher + "/" + mcp.Name + `@^1.0"}}}`)})
	team := fixture("team", "graph-team", map[string][]byte{"AGENTS.md": []byte("lead"), "team.json": []byte(`{"schema":"orbit.team/v1","name":"graph-team","dispatch":"scheduled","roles":[{"name":"lead","role":"leader","agent":"."},{"name":"worker","role":"teammate","agent":"` + publisher + `/graph-agent@^1.0","count":2},{"name":"reviewer","role":"reviewer","agent":"./members/reviewer"}],"sharedSkills":[{"id":"` + shared + `","version":"^1.0"}]}`), "members/reviewer/AGENTS.md": []byte("review"), "members/worker-2/skills-visibility.json": []byte(`{"deny":["` + ref + `"]}`)})
	resolve := func(entry locks.Entry) (contract.RunLock, error) {
		var lock contract.RunLock
		e := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, tenant)
			var e error
			lock, e = (&locks.Resolver{Assets: s}).Resolve(ctx, tenant, entry, locks.Scope{WorkspaceID: workspace})
			return e
		})
		return lock, e
	}
	lock, e := resolve(locks.Entry{Kind: "team", Ref: publisher + "/" + team.Name})
	if e != nil {
		t.Fatal(e)
	}
	if len(lock.Members) != 4 {
		t.Fatal("roster", lock.Members)
	}
	for _, member := range lock.Members {
		skills := map[string]bool{}
		for _, i := range member.Skills {
			skills[lock.Entries[i].Ref] = true
		}
		if !skills[shared] {
			t.Fatalf("%s missing shared skill", member.Name)
		}
		if member.Name == "worker-1" && !skills[ref] {
			t.Fatal("catalog skill missing")
		}
		if member.Name == "worker-2" && skills[ref] {
			t.Fatal("member deny ignored")
		}
		if strings.HasPrefix(string(member.Name), "worker") && len(member.Mcp) != 1 {
			t.Fatal("MCP not locked")
		}
	}
	again, e := resolve(locks.Entry{Kind: "team", Ref: publisher + "/" + team.Name})
	if e != nil || again.Digest != lock.Digest || again.LockId == lock.LockId {
		t.Fatal("lock digest unstable", e)
	}
	// Persist the exact generated RunLock, including members, for a real run.
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, tenant)
		session, run := ids.New(), ids.New()
		if _, e := tx.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status) VALUES($1,$2,$3,$4,'lock fixture','team',$5,'queue','active')`, session, tenant, workspace, owner, publisher+"/"+team.Name); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `INSERT INTO runs(id,tenant_id,session_id,trigger,status,created_by) VALUES($1,$2,$3,'api','queued',$4)`, run, tenant, session, owner); e != nil {
			return e
		}
		resolved, e := (&locks.Resolver{Assets: s}).Resolve(ctx, tenant, locks.Entry{Kind: "team", Ref: publisher + "/" + team.Name}, locks.Scope{WorkspaceID: workspace, RunID: run})
		if e != nil {
			return e
		}
		var raw []byte
		if e = tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE id=$1 AND tenant_id=$2`, resolved.LockId, tenant).Scan(&raw); e != nil {
			return e
		}
		if e = contract.ValidateStrict("RunLock", raw); e != nil {
			return e
		}
		var stored contract.RunLock
		if e = json.Unmarshal(raw, &stored); e != nil {
			return e
		}
		if len(stored.Members) != 4 || stored.Digest != lock.Digest {
			return fmt.Errorf("incomplete persisted run lock")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// A tenant deny wins over workspace, agent and member allows.
	e = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `INSERT INTO policies(id,tenant_id,scope_type,scope_id,kind,body,version,status) VALUES($1,$2,'tenant',$2,'tool_rules',$3,1,'active')`, ids.New(), tenant, `{"skills_visibility":{"deny":["`+shared+`"]}}`)
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	lock, e = resolve(locks.Entry{Kind: "team", Ref: publisher + "/" + team.Name})
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range lock.Members {
		for _, i := range m.Skills {
			if lock.Entries[i].Ref == shared {
				t.Fatal("tenant deny ignored")
			}
		}
	}
	fixture("skill", "cycle-one", map[string][]byte{"SKILL.md": []byte("one"), "skills.json": []byte(`{"skills":[{"name":"two","source":"catalog","id":"` + publisher + `/cycle-two"}]}`)})
	fixture("skill", "cycle-two", map[string][]byte{"SKILL.md": []byte("two"), "skills.json": []byte(`{"skills":[{"name":"one","source":"catalog","id":"` + publisher + `/cycle-one"}]}`)})
	fixture("agent", "cycle-agent", map[string][]byte{"AGENTS.md": []byte("agent"), "skills.json": []byte(`{"skills":[{"name":"one","source":"catalog","id":"` + publisher + `/cycle-one"}]}`)})
	if _, e = resolve(locks.Entry{Kind: "agent", Ref: publisher + "/cycle-agent"}); e == nil || !strings.Contains(e.Error(), "cycle") {
		t.Fatal("cycle accepted", e)
	}
	if _, e = resolve(locks.Entry{Kind: "agent", Ref: publisher + "/graph-agent", Version: "^2.0"}); e == nil {
		t.Fatal("unsatisfied version accepted")
	}
}
