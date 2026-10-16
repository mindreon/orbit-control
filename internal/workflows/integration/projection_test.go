//go:build temporal_integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/runworker"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/migrations"
)

type projectionFixture struct {
	db                       *db.DB
	objects                  *objstore.S3
	temporal                 client.Client
	tenant, actor, workspace string
	api                      http.Handler
	token                    string
}

func testEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	require.NotEmpty(t, value, "%s is required for the real-service integration suite", name)
	return value
}
func databaseRole(t *testing.T, raw, role string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	q := u.Query()
	q.Set("options", "-c role="+role)
	u.RawQuery = q.Encode()
	return u.String()
}
func newProjectionFixture(t *testing.T) *projectionFixture {
	t.Helper()
	raw := testEnv(t, "ORBIT_TEST_DATABASE_URL")
	require.NoError(t, migrations.Run(t.Context(), databaseRole(t, raw, "orbit_owner"), "up"))
	d, err := db.OpenApp(t.Context(), databaseRole(t, raw, "orbit_app"))
	require.NoError(t, err)
	t.Cleanup(d.Close)
	objects, err := objstore.New(testEnv(t, "ORBIT_TEST_OBJECT_ENDPOINT"), testEnv(t, "ORBIT_TEST_OBJECT_BUCKET"),
		testEnv(t, "ORBIT_TEST_OBJECT_ACCESS_KEY"), testEnv(t, "ORBIT_TEST_OBJECT_SECRET_KEY"), false)
	require.NoError(t, err)
	f := &projectionFixture{
		db:        d,
		objects:   objects,
		temporal:  devClient(t),
		tenant:    ids.New(),
		actor:     ids.New(),
		workspace: ids.New(),
	}
	admin, err := pgx.Connect(t.Context(), raw)
	require.NoError(t, err)
	defer admin.Close(t.Context())
	_, err = admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user','projection@example.test','Projection',$1::uuid::text)`, f.actor)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO tenants(id,slug,name) VALUES($1,$1::uuid::text,'Projection')`, f.tenant)
	require.NoError(t, err)
	require.NoError(t, d.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: f.actor, Kind: "user"}, f.tenant)
		_, err := tx.Exec(
			ctx,
			`INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`,
			f.tenant,
			f.actor,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO workspaces(id,tenant_id,name) VALUES($1,$2,'Projection')`, f.workspace, f.tenant)
		if err != nil {
			return err
		}
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "ws.admin", "workspace", f.workspace)
	}))
	return f
}

type projectionResolver struct{ lock contract.RunLock }

func (r projectionResolver) Resolve(_ context.Context, in locks.Request) (contract.RunLock, error) {
	lock := r.lock
	lock.LockId = in.LockID
	raw, err := json.Marshal(lock)
	if err == nil {
		lock.Digest, err = contract.RunLockDigest(raw)
	}
	return lock, err
}
func (f *projectionFixture) start(t *testing.T, bus *natsx.Bus) {
	t.Helper()
	a := &ctl.Activities{DB: f.db, Obj: f.objects, NATS: bus, Temporal: f.temporal, Environment: "dev"}
	ctlWorker := runworker.New(f.temporal, a)
	require.NoError(t, ctlWorker.Start())
	t.Cleanup(ctlWorker.Stop)
	sandbox := worker.New(f.temporal, contract.TaskQueueSandbox, worker.Options{})
	sandbox.RegisterActivityWithOptions(func(_ context.Context, in contract.SandboxAcquireInput) (contract.SandboxLease, error) {
		return contract.SandboxLease{
			Provider:  "local",
			SandboxId: "local-" + in.Metadata.SessionId,
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxCreate})
	sandbox.RegisterActivityWithOptions(func(_ context.Context, in contract.SandboxConfigureInput) (contract.SandboxLease, error) {
		return in.Lease, nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxConfigure})
	sandbox.RegisterActivityWithOptions(func(context.Context, contract.SandboxOpInput) error { return nil },
		activity.RegisterOptions{Name: contract.ActivitySandboxDestroy})
	sandbox.RegisterActivityWithOptions(func(context.Context, contract.SandboxOpInput) (contract.SandboxInspectResult, error) {
		return contract.SandboxInspectResult{Exists: true}, nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxInspect})
	require.NoError(t, sandbox.Start())
	t.Cleanup(sandbox.Stop)
	agent := worker.New(f.temporal, contract.TaskQueueAgentStandard, worker.Options{})
	agent.RegisterActivityWithOptions(f.segment, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	require.NoError(t, agent.Start())
	t.Cleanup(agent.Stop)
}
func (f *projectionFixture) segment(ctx context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
	if _, _, err := f.objects.GetObject(ctx, in.TenantId, *in.Input.ContentRef, 1<<20); err != nil {
		return contract.SegmentResult{}, err
	}
	data := []byte("projection output")
	key := "t/" + in.TenantId + "/artifacts/" + in.RunId + "/output.txt"
	if err := f.objects.PutObject(ctx, in.TenantId, key, data, "text/plain"); err != nil {
		return contract.SegmentResult{}, err
	}
	return contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: in.ExpectedCheckpointVersion + 1,
		Progress: contract.SegmentProgress{
			NewOutput: true,
		}, Final: &contract.FinalMessage{MessageId: ids.Derive(in.RunId, "final"), TextPreview: "Projection complete"},
		Artifacts: []contract.Artifact{
			{
				Path:      "output.txt",
				Mime:      "text/plain",
				SizeBytes: len(data),
				Digest:    fmt.Sprintf("sha256:%x", sha256.Sum256(data)),
				ObjectRef: key,
			},
		}}, nil
}
func (f *projectionFixture) configureAPI(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contract", "v2", "examples", "RunLock.agent.json"))
	require.NoError(t, err)
	var lock contract.RunLock
	require.NoError(t, json.Unmarshal(raw, &lock))
	entry := lock.Entries[0]
	entry.Files = nil
	lock.Entries[0] = entry
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key", StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test",
		CapabilityKeyID: "integration", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), f.db, c, logger)
	require.NoError(t, err)
	t.Cleanup(a.Close)
	f.token, err = a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	f.api = (&httpapi.Handler{DB: f.db, Auth: a, Config: c, Logger: logger, Objects: f.objects,
		Temporal: temporalx.Service{Client: f.temporal}, Locks: projectionResolver{lock: lock}}).Router()
}
func (f *projectionFixture) request(t *testing.T, method, path string, body any) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+f.token)
	req.Header.Set("X-Orbit-Tenant", f.tenant)
	req.Header.Set("Idempotency-Key", ids.New())
	out := httptest.NewRecorder()
	f.api.ServeHTTP(out, req)
	require.Less(t, out.Code, 300, out.Body.String())
	return out.Body.Bytes()
}
func TestAPIToRunProjection(t *testing.T) {
	f := newProjectionFixture(t)
	bus, err := natsx.Connect(testEnv(t, "ORBIT_TEST_NATS_URL"), "orbit-control-test")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	require.NoError(t, bus.EnsureRunEventsStream(t.Context(), 0))
	sub, err := bus.Conn.SubscribeSync("run." + f.tenant + ".>")
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, bus.Conn.Flush())
	f.start(t, bus)
	f.configureAPI(t)
	var session struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(f.request(t, "POST", "/v1/sessions", map[string]any{
		"workspace_id": f.workspace, "entry_ref": "fixture/agent", "title": "Projection"}), &session))
	var submission struct {
		RunID string `json:"run_id"`
	}
	require.NoError(
		t,
		json.Unmarshal(f.request(t, "POST", "/v1/sessions/"+session.ID+"/messages", map[string]string{"text": "hello"}), &submission),
	)
	run := f.temporal.GetWorkflow(t.Context(), w.AgentRunWorkflowID(f.tenant, submission.RunID), "")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, run.Get(ctx, nil))
	// Retry the real ctl AcquireSandbox with the original operation ID. Temporal
	// must return the accepted lease without creating another sandbox generation.
	activity := &ctl.Activities{DB: f.db, Temporal: f.temporal, Environment: "dev"}
	acquire := w.AcquireSandboxInput{RunOp: w.RunOp{TenantID: f.tenant, SessionID: session.ID,
		RunID: submission.RunID, Member: "main"},
		Spec: w.SandboxSpec{IsolationKey: w.IsolationKey(f.tenant, session.ID), Profile: "default", TTLSeconds: 1800}}
	first, err := activity.AcquireSandbox(ctx, acquire)
	require.NoError(t, err)
	second, err := activity.AcquireSandbox(ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, first, second)
	raw := f.request(t, "GET", "/v1/runs/"+submission.RunID, nil)
	require.Contains(t, string(raw), `"status":"succeeded"`)
	require.Contains(t, string(f.request(t, "GET", "/v1/sessions/"+session.ID+"/messages", nil)), "Projection complete")
	require.Contains(t, string(f.request(t, "GET", "/v1/sessions/"+session.ID+"/artifacts", nil)), "output.txt")
	require.NoError(t, f.db.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		var status string
		require.NoError(
			t,
			tx.QueryRow(t.Context(), `SELECT status FROM budget_reservations WHERE run_id=$1`, submission.RunID).Scan(&status),
		)
		require.Equal(t, "settled", status)
		return nil
	}))
	found := false
	for range 4 {
		msg, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err)
		var event contract.RunEvent
		require.NoError(t, json.Unmarshal(msg.Data, &event))
		if event.Type == "run.completed" {
			found = true
			break
		}
	}
	require.True(t, found, "terminal projection must be published through real NATS")
	f.request(t, "POST", "/v1/sessions/"+session.ID+"/archive", nil)
	require.NoError(t, f.temporal.GetWorkflow(ctx, w.SessionWorkflowID(f.tenant, session.ID), "").Get(ctx, nil))
	require.NoError(t, f.temporal.GetWorkflow(ctx, w.LeaseWorkflowID(f.tenant, w.IsolationKey(f.tenant, session.ID)), "").Get(ctx, nil))
}
