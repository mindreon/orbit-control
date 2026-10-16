package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/kernelapi"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/secrets"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type runFixture struct {
	d                                            *db.DB
	tenant, actor, workspace, session, run, lock string
	activities                                   *ctl.Activities
	operation                                    int64
	router                                       http.Handler
	signer                                       *auth.CapabilitySigner
	keys                                         secrets.Keys
}

func newRunFixture(t *testing.T, d *db.DB, admin *pgx.Conn) *runFixture {
	t.Helper()
	f := &runFixture{
		d:         d,
		tenant:    ids.New(),
		actor:     ids.New(),
		workspace: ids.New(),
		session:   ids.New(),
		run:       ids.New(),
		lock:      ids.New(),
	}
	_, err := admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user','fixture@example.test','Run fixture',$1::uuid::text)`, f.actor)
	require.NoError(t, err)
	_, err = admin.Exec(
		t.Context(),
		`INSERT INTO tenants(id,slug,name) VALUES($1,$1::uuid::text,'Run fixture')`,
		f.tenant,
	)
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{sql: `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`, args: []any{f.tenant, f.actor}},
			{sql: `INSERT INTO workspaces(id,tenant_id,name) VALUES($1,$2,'Runs')`, args: []any{f.workspace, f.tenant}},
			{sql: `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$4,'Fixture','agent','agent:test','queue','idle')`, args: []any{f.session, f.tenant, f.workspace, f.actor}},
			{sql: `INSERT INTO runs(id,tenant_id,session_id,trigger,status,created_by,budget_millicredits)
 VALUES($1,$2,$3,'user_message','queued',$4,200000)`, args: []any{f.run, f.tenant, f.session, f.actor}},
		} {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				return err
			}
		}
		raw, err := os.ReadFile("../contract/v2/examples/RunLock.agent.json")
		if err != nil {
			return err
		}
		var lock contract.RunLock
		if err = json.Unmarshal(raw, &lock); err != nil {
			return err
		}
		lock.LockId = f.lock
		raw, err = json.Marshal(lock)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO run_locks(id,tenant_id,run_id,digest,entries,effective_permissions,lock)
 VALUES($1,$2,$3,$4,'[]','{}',$5)`, f.lock, f.tenant, f.run, lock.Digest, raw); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE runs SET lock_id=$3 WHERE tenant_id=$1 AND id=$2`, f.tenant, f.run, f.lock)
		return err
	})
	f.activities = &ctl.Activities{DB: d}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.signer, err = auth.NewCapabilitySigner(key, "test", nil)
	require.NoError(t, err)
	f.keys = secrets.Keys{Current: 1, KEKs: map[int][]byte{1: make([]byte, 32)}}
	digest := sha256.Sum256([]byte("fixture-service-credential"))
	f.router = (&kernelapi.Handler{DB: d, Signer: f.signer, Secrets: f.keys, ServiceTokenSHA256: hex.EncodeToString(digest[:])}).Router()
	return f
}
func (f *runFixture) tx(t *testing.T, fn func(context.Context, pgx.Tx) error) {
	t.Helper()
	require.NoError(t, f.d.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: f.actor, Kind: "user"}, f.tenant)
		return fn(ctx, tx)
	}))
}
func (f *runFixture) op() w.RunOp {
	f.operation++
	return w.RunOp{TenantID: f.tenant, SessionID: f.session, RunID: f.run, Member: "main", Seq: 1,
		EventN: f.operation, EventAt: time.Now().UTC()}
}
func (f *runFixture) start(t *testing.T) {
	t.Helper()
	in := w.PrepareInput{RunOp: f.op(), SessionKey: "fixture:" + f.session, Budget: 200000}
	first, err := f.activities.PrepareRun(t.Context(), in)
	require.NoError(t, err)
	second, err := f.activities.PrepareRun(t.Context(), in)
	require.NoError(t, err)
	require.Equal(t, first, second)
	start := w.StartSegmentInput{RunOp: f.op()}
	for range 2 {
		require.NoError(t, f.activities.StartSegment(t.Context(), start))
	}
}
func (f *runFixture) request(t *testing.T, method, path, token string, body any) (int, []byte) {
	t.Helper()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	out := httptest.NewRecorder()
	f.router.ServeHTTP(out, req)
	return out.Code, out.Body.Bytes()
}
func (f *runFixture) token(t *testing.T) string {
	t.Helper()
	status, raw := f.request(t, "POST", "/internal/v1/tokens", "fixture-service-credential", contract.TokenRequest{
		TenantId: f.tenant, RunId: f.run, Purpose: "segment", Member: new(contract.MemberName("main")), Seq: new(1), Attempt: new(1),
	})
	require.Equal(t, 200, status, string(raw))
	var out contract.TokenResponse
	require.NoError(t, json.Unmarshal(raw, &out))
	return string(out.Token)
}
func testRuns(t *testing.T, d *db.DB, admin *pgx.Conn) {
	t.Run("StreamOutbox", func(t *testing.T) { testStreamOutbox(t, d, admin) })
	t.Run("StreamTokens", func(t *testing.T) { testStreamTokens(t, d, admin) })
	t.Run("SandboxProjection", func(t *testing.T) { testSandboxProjection(t, d, admin) })
	t.Run("SandboxAcquire", func(t *testing.T) { testSandboxAcquireGenerationAndProvider(t, d, admin) })
	t.Run("SegmentBoundary", func(t *testing.T) { testSegmentBoundary(t, d, admin) })
	t.Run("HTTP", func(t *testing.T) { testRunHTTP(t, d, admin) })
	t.Run("HTTPHandlers", func(t *testing.T) { testRunHTTPHandlers(t, d, admin) })
	t.Run("SessionVisibilityPagination", func(t *testing.T) { testSessionVisibilityPagination(t, d, admin) })
	t.Run("ProjectionAndIdempotentActivities", func(t *testing.T) { testRunProjection(t, d, admin) })
	t.Run("Grants", func(t *testing.T) { testRunGrants(t, d, admin) })
	t.Run("SecretGrantAudit", func(t *testing.T) { testSecretGrantAudit(t, d, admin) })
	t.Run("KernelIsolationAndUsage", func(t *testing.T) { testKernelRun(t, d, admin) })
}
func testRunProjection(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	status := w.StatusInput{RunOp: f.op(), Status: "paused"}
	for range 2 {
		require.NoError(t, f.activities.SetRunStatus(t.Context(), status))
	}
	raw, err := os.ReadFile("../contract/v2/examples/SegmentResult.completed.json")
	require.NoError(t, err)
	var res contract.SegmentResult
	require.NoError(t, json.Unmarshal(raw, &res))
	res.Final.MessageId = ids.New()
	res.Final.ContentRef = nil
	res.Artifacts[0].ObjectRef = "t/" + f.tenant + "/artifacts/" + f.run + "/summary.md"
	input := w.RecordSegmentInput{RunOp: f.op(), Result: res}
	for range 2 {
		_, err = f.activities.RecordSegment(t.Context(), input)
		require.NoError(t, err)
	}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var messages, artifacts, events int
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM messages WHERE run_id=$1`, f.run).Scan(&messages))
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM artifacts WHERE run_id=$1`, f.run).Scan(&artifacts))
		require.NoError(
			t,
			tx.QueryRow(ctx, `SELECT count(*) FROM control_events WHERE run_id=$1 AND payload->>'type'='artifact.created'`, f.run).
				Scan(&events),
		)
		require.Equal(t, 1, messages)
		require.Equal(t, 1, artifacts)
		require.Equal(t, 1, events)
		return nil
	})
	// A subsequent segment updates the same artifact path without duplicating it.
	op := f.op()
	op.Seq = 2
	require.NoError(t, f.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: op, ExpectedVersion: 4}))
	res.Artifacts[0].Digest = "sha256:" + strings.Repeat("a", 64)
	op = f.op()
	op.Seq = 2
	_, err = f.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: op, Result: res})
	require.NoError(t, err)
	reconcile := w.ReconcileInput{RunOp: f.op(), SessionKey: "fixture:" + f.session}
	for range 2 {
		out, err := f.activities.ReconcileCheckpoint(t.Context(), reconcile)
		require.NoError(t, err)
		require.Nil(t, out.Result)
	}
	finish := w.FinalizeInput{RunOp: f.op(), Status: "succeeded", Reason: "done"}
	for range 2 {
		require.NoError(t, f.activities.FinalizeRun(t.Context(), finish))
	}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var state, reservation, summary string
		require.NoError(
			t,
			tx.QueryRow(ctx, `SELECT status,summary FROM runs WHERE id=$1`, f.run).Scan(&state, &summary),
		)
		require.NoError(
			t,
			tx.QueryRow(ctx, `SELECT status FROM budget_reservations WHERE run_id=$1`, f.run).Scan(&reservation),
		)
		require.Equal(t, "succeeded", state)
		require.Equal(t, "settled", reservation)
		require.Equal(t, "done", summary)
		var updated int
		require.NoError(
			t,
			tx.QueryRow(ctx, `SELECT count(*) FROM control_events WHERE run_id=$1 AND payload->>'type'='artifact.updated'`, f.run).
				Scan(&updated),
		)
		require.Equal(t, 1, updated)
		return nil
	})
}
func testRunGrants(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	for _, scope := range []string{"session", "user", "workspace-downgrade", "workspace"} {
		t.Run(scope, func(t *testing.T) {
			if scope == "workspace" {
				f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(
						ctx,
						`INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by)
 VALUES($1,$2,'user',$3,'ws.admin','workspace',$4,$3)`,
						ids.New(),
						f.tenant,
						f.actor,
						f.workspace,
					)
					return err
				})
			}
			raw, err := os.ReadFile("../contract/v2/examples/InteractionRequest.tool_approval.json")
			require.NoError(t, err)
			var req contract.InteractionRequest
			require.NoError(t, json.Unmarshal(raw, &req))
			req.InteractionId = ids.New()
			op := f.op()
			create := w.InteractionInput{RunOp: op, Request: req, RemindAt: op.EventAt.Add(time.Hour),
				EscalateAt: op.EventAt.Add(24 * time.Hour), ExpiresAt: op.EventAt.Add(72 * time.Hour)}
			for range 2 {
				_, err = f.activities.CreateInteraction(t.Context(), create)
				require.NoError(t, err)
			}
			notify := w.NotifyInput{RunOp: f.op(), InteractionID: req.InteractionId, Kind: "escalate"}
			for range 2 {
				require.NoError(t, f.activities.NotifyInteraction(t.Context(), notify))
			}
			decision := contract.Decision{Kind: "tool_approval", Choice: new(contract.DecisionChoice("allow_always")),
				Scope: new(contract.DecisionScope("user"))}
			want := scope
			if scope == "session" {
				decision.Choice = new(contract.DecisionChoice("allow_session"))
				decision.Scope = nil
			}
			if strings.HasPrefix(scope, "workspace") {
				decision.Scope = new(contract.DecisionScope("workspace"))
			}
			if scope == "workspace-downgrade" {
				want = "user"
			}
			input := w.DecisionInput{
				RunOp:         f.op(),
				InteractionID: req.InteractionId,
				Kind:          req.Kind,
				Decision:      decision,
				Actor:         f.actor,
			}
			for range 2 {
				require.NoError(t, f.activities.ApplyDecision(t.Context(), input))
			}
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				var count int
				var actual string
				require.NoError(
					t,
					tx.QueryRow(ctx, `SELECT count(*),min(scope_type) FROM permission_grants WHERE interaction_id=$1`, req.InteractionId).
						Scan(&count, &actual),
				)
				require.Equal(t, 1, count)
				require.Equal(t, want, actual)
				return nil
			})
		})
	}
	cancelReq := contract.InteractionRequest{
		Kind:          "ask_user",
		InteractionId: ids.New(),
		Question:      &contract.Question{Text: "Ready?"},
	}
	op := f.op()
	_, err := f.activities.CreateInteraction(t.Context(), w.InteractionInput{RunOp: op, Request: cancelReq,
		RemindAt: op.EventAt, EscalateAt: op.EventAt, ExpiresAt: op.EventAt.Add(time.Hour)})
	require.NoError(t, err)
	cancel := w.NotifyInput{RunOp: f.op(), InteractionID: cancelReq.InteractionId}
	for range 2 {
		require.NoError(t, f.activities.CancelInteraction(t.Context(), cancel))
	}
}
func testKernelRun(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	f.start(t)
	other.start(t)
	token := f.token(t)
	prefix := "/internal/v1/runs/" + f.run
	for _, req := range []contract.TokenRequest{
		{TenantId: f.tenant, RunId: other.run, Purpose: "segment", Member: new(contract.MemberName("main")), Seq: new(1), Attempt: new(1)},
		{TenantId: f.tenant, RunId: f.run, Purpose: "segment", Member: new(contract.MemberName("main")), Seq: new(2), Attempt: new(1)},
	} {
		status, _ := f.request(t, "POST", "/internal/v1/tokens", "fixture-service-credential", req)
		require.Equal(t, 403, status)
	}
	for _, suffix := range []string{"lock", "policy", "content?ref=t/" + other.tenant + "/runs/" + other.run + "/input/message"} {
		status, _ := f.request(t, "GET", "/internal/v1/runs/"+other.run+"/"+suffix, token, nil)
		require.Equal(t, 403, status)
	}
	status, _ := f.request(t, "GET", prefix+"/content?ref=t/"+other.tenant+"/runs/"+f.run+"/input/message", token, nil)
	require.Equal(t, 403, status)
	status, _ = f.request(t, "POST", prefix+"/secret-grants", token, contract.SecretGrantRequest{Names: []string{"NOT_DECLARED"}})
	require.Equal(t, 403, status)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		for i, name := range []string{"cheap", "expensive"} {
			_, err := tx.Exec(
				ctx,
				`INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model,pricing,is_default)
 VALUES($1,$2,$3,'openai','http://mock.invalid/v1','same-model',$4,$5)`,
				ids.New(),
				f.tenant,
				name,
				fmt.Sprintf(`{"input_per_mtok_millicredits":%d,"output_per_mtok_millicredits":0,
    "cache_read_per_mtok_millicredits":0,"cache_write_per_mtok_millicredits":0}`, 1000000*(i+1)),
				i == 0,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
	status, raw := f.request(t, "GET", prefix+"/model?profile=expensive", token, nil)
	require.Equal(t, 200, status, string(raw))
	report := contract.UsageReport{Items: []contract.UsageItem{{IdempotencyKey: "model:" + f.run + ":main:1:1:1",
		Model: "same-model", InputTokens: 7, OccurredAt: time.Now().UTC()}}}
	for range 2 {
		status, raw = f.request(t, "POST", prefix+"/usage", token, report)
		require.Equal(t, 200, status, string(raw))
		var out contract.UsageReportResponse
		require.NoError(t, json.Unmarshal(raw, &out))
		require.EqualValues(t, 14, out.RunUsedMillicredits)
	}
	res := contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: 1, Progress: contract.SegmentProgress{}}
	_, err := f.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: f.op(), Result: res})
	require.NoError(t, err)
	status, raw = f.request(t, "GET", prefix+"/policy", token, nil)
	require.Equal(t, 403, status)
	require.Contains(t, string(raw), "TOKEN_SUPERSEDED")
	finish := w.FinalizeInput{RunOp: f.op(), Status: "succeeded"}
	require.NoError(t, f.activities.FinalizeRun(t.Context(), finish))
	status, _ = f.request(t, "POST", "/internal/v1/tokens", "fixture-service-credential", contract.TokenRequest{
		TenantId: f.tenant, RunId: f.run, Purpose: "segment", Member: new(contract.MemberName("main")), Seq: new(1), Attempt: new(1),
	})
	require.Equal(t, 403, status)
}

func testSandboxProjection(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	op := f.op()
	input := w.SandboxStateInput{TenantID: f.tenant, SessionID: f.session, RunID: f.run,
		IsolationKey: w.IsolationKey(f.tenant, f.session), Gen: 1, State: "ready", Profile: "default",
		Lease:  &contract.SandboxLease{Provider: "opensandbox", SandboxId: "box", ExpiresAt: time.Now().Add(time.Hour)},
		EventN: op.EventN, EventAt: op.EventAt, Restored: new(true)}
	for range 2 {
		require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var count int
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*)
 FROM control_events
 WHERE run_id=$1
 AND payload->>'type'='sandbox.restored'`, f.run).Scan(&count))
		require.Equal(t, 1, count)
		return nil
	})
	input.Restored = nil
	input.State = "none"
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var state string
		require.NoError(t, tx.QueryRow(ctx, `SELECT state FROM sandboxes WHERE isolation_key=$1`, input.IsolationKey).Scan(&state))
		require.Equal(t, "destroyed", state)
		return nil
	})
	input.Gen = 2
	input.State = "destroyed"
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	// A restarted lease starts above the persisted generation; stale executions cannot overwrite it.
	input.Provider = "opensandbox"
	input.Gen = 3
	input.State = "creating"
	input.Lease = nil
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	input.State = "ready"
	input.Lease = &contract.SandboxLease{Provider: "opensandbox", SandboxId: "new-box", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	input.Gen = 2
	input.State = "destroyed"
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), input))
	status, _ := f.request(t, "POST", "/internal/v1/tokens", "fixture-service-credential", contract.TokenRequest{
		TenantId: f.tenant, RunId: f.run, Purpose: "sandbox", LeaseId: new(input.IsolationKey),
	})
	require.Equal(t, 200, status)
	f.activities.Environment = "prod"
	f.activities.SandboxProvider = "local"
	_, err := f.activities.AcquireSandbox(t.Context(), w.AcquireSandboxInput{RunOp: f.op()})
	require.ErrorContains(t, err, "invalid sandbox provider")

}
func testSegmentBoundary(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	for _, mode := range []string{"session", "run", "seq", "reference"} {
		t.Run(mode, func(t *testing.T) {
			op := f.op()
			res := contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: 1, Progress: contract.SegmentProgress{}}
			switch mode {
			case "session":
				op.SessionID = ids.New()
			case "run":
				op.RunID = ids.New()
			case "seq":
				op.Seq = 9
			case "reference":
				res.Final = &contract.FinalMessage{
					MessageId:   ids.New(),
					TextPreview: "bad",
					ContentRef:  new("t/other/runs/other/message"),
				}
			}
			_, err := f.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: op, Result: res})
			require.Error(t, err)
		})
	}
}

func testSecretGrantAudit(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	plain := "test-value-" + ids.New()
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := f.keys.Put(ctx, f.tenant, f.actor, "DECLARED", "tenant", []byte(plain)); err != nil {
			return err
		}
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE tenant_id=$1 AND id=$2`, f.tenant, f.lock).Scan(&raw); err != nil {
			return err
		}
		var lock contract.RunLock
		if err := json.Unmarshal(raw, &lock); err != nil {
			return err
		}
		lock.Members[0].EffectivePermissions.Secrets = []contract.SecretRequirement{{Name: "DECLARED"}}
		raw, err := json.Marshal(lock)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE run_locks SET lock=$3 WHERE tenant_id=$1 AND id=$2`, f.tenant, f.lock, raw)
		return err
	})
	token := f.token(t)
	status, raw := f.request(t, "POST", "/internal/v1/runs/"+f.run+"/secret-grants", token,
		contract.SecretGrantRequest{Names: []string{"DECLARED"}})
	require.Equal(t, 200, status, string(raw))
	var out contract.SecretGrantResponse
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Len(t, out.Grants, 1)
	require.Equal(t, plain, string(out.Grants[0].Value))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var detail string
		err := tx.QueryRow(ctx, `SELECT detail::text FROM audit_log WHERE tenant_id=$1 AND action='secret.grant' AND target_id=$2`,
			f.tenant, f.run).Scan(&detail)
		require.NoError(t, err)
		require.JSONEq(t, `{"names":["DECLARED"]}`, detail)
		require.NotContains(t, detail, plain)
		return nil
	})
}
