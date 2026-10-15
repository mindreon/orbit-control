package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type handlerTemporal struct {
	runTemporal
	err   error
	calls []string
}

var _ temporalx.Client = (*handlerTemporal)(nil)

func (c *handlerTemporal) Update(_ context.Context, _ string, name, _ string, in, out any) error {
	c.calls = append(c.calls, name)
	if c.err != nil {
		return c.err
	}
	switch result := out.(type) {
	case *w.CancelRunOutput:
		result.Disposition = "cancelling"
	case *w.SteerOutput:
		result.Queued = 1
	case *w.ResolveInteractionOutput:
		result.Accepted = true
	}
	return nil
}
func (c *handlerTemporal) Signal(_ context.Context, _ string, name string, _ any) error {
	c.calls = append(c.calls, name)
	return c.err
}

type handlerCase struct {
	name, method, path, body, role, setup, code string
	status                                      int
	temporalError                               error
}

func testRunHTTPHandlers(t *testing.T, d *db.DB, admin *pgx.Conn) {
	var cases []handlerCase
	for _, endpoint := range []struct {
		method, path, body string
		status             int
	}{
		{method: "POST", path: "/v1/sessions", body: `{"workspace_id":"{w}","entry_ref":"agent:test","title":"New"}`, status: 201},
		{method: "GET", path: "/v1/sessions/{s}", status: 200},
		{method: "PATCH", path: "/v1/sessions/{s}", body: `{"title":"Updated","message_policy":"queue"}`, status: 200},
		{method: "POST", path: "/v1/sessions/{s}/archive", status: 202},
		{method: "GET", path: "/v1/sessions/{s}/messages", status: 200},
		{method: "GET", path: "/v1/sessions/{s}/runs", status: 200},
		{method: "GET", path: "/v1/sessions/{s}/artifacts", status: 200},
		{method: "GET", path: "/v1/runs/{r}", status: 200},
		{method: "GET", path: "/v1/runs/{r}/segments", status: 200},
		{method: "GET", path: "/v1/runs/{r}/stream-token", status: 200},
		{method: "GET", path: "/v1/artifacts/{a}", status: 302},
		{method: "POST", path: "/v1/runs/{r}/cancel", status: 202},
		{method: "POST", path: "/v1/runs/{r}/pause", status: 202},
		{method: "POST", path: "/v1/runs/{r}/resume", status: 202},
		{method: "POST", path: "/v1/runs/{r}/steer", body: `{"text":"Follow up"}`, status: 202},
		{method: "GET", path: "/v1/interactions/{i}", status: 200},
		{method: "POST", path: "/v1/interactions/{i}/resolve", body: `{"kind":"ask_user","answer":"Ready"}`, status: 200},
	} {
		base := handlerCase{method: endpoint.method, path: endpoint.path, body: endpoint.body, role: "ws.editor", status: endpoint.status}
		base.name = "success " + base.method + " " + base.path
		cases = append(cases, base)
		denied := base
		denied.name = "hidden " + base.method + " " + base.path
		denied.role = "none"
		denied.status = 404
		denied.code = "NOT_FOUND"
		if base.path == "/v1/sessions" {
			denied.status = 403
			denied.code = "FORBIDDEN"
		}
		cases = append(cases, denied)
		unauthenticated := base
		unauthenticated.name = "unauthenticated " + base.method + " " + base.path
		unauthenticated.role = "anonymous"
		unauthenticated.status = 401
		unauthenticated.code = "UNAUTHENTICATED"
		cases = append(cases, unauthenticated)
		if base.method != "GET" {
			viewer := base
			viewer.name = "read only " + base.method + " " + base.path
			viewer.role = "ws.viewer"
			viewer.status = 403
			viewer.code = "FORBIDDEN"
			cases = append(cases, viewer)
		}
	}
	for _, path := range []string{"/v1/sessions", "/v1/interactions?assignee=me"} {
		cases = append(cases, handlerCase{name: "list " + path, method: "GET", path: path, role: "ws.editor", status: 200})
		cases = append(cases, handlerCase{name: "filtered list " + path, method: "GET", path: path,
			role: "none", status: 200, setup: "empty-list"})
	}
	for _, action := range []string{"cancel", "pause", "resume", "steer"} {
		path := "/v1/runs/{r}/" + action
		cases = append(cases, handlerCase{name: "finished " + action, method: "POST", path: path, role: "ws.editor", setup: "finished",
			body: `{"text":"Follow up"}`, status: 409, code: "RUN_FINISHED"})
		cases = append(cases, handlerCase{name: "offline " + action, method: "POST", path: path, role: "ws.editor",
			body:   map[bool]string{true: `{"text":"Follow up"}`, false: `{}`}[action == "steer"],
			status: 503, code: "TEMPORAL_UNAVAILABLE", temporalError: serviceerror.NewUnavailable("offline")})
	}
	cases = append(cases,
		handlerCase{name: "steer queue full", method: "POST", path: "/v1/runs/{r}/steer", role: "ws.editor", body: `{"text":"Input"}`,
			status: 409, code: "QUEUE_FULL", temporalError: temporal.NewApplicationError("full", "QUEUE_FULL")},
		handlerCase{name: "resolve non-approver", method: "POST", path: "/v1/interactions/{i}/resolve", role: "ws.editor",
			setup: "non-approver", body: `{"kind":"ask_user","answer":"Ready"}`, status: 403, code: "FORBIDDEN"},
		handlerCase{name: "read non-approver", method: "GET", path: "/v1/interactions/{i}", role: "ws.editor",
			setup: "non-approver", status: 404, code: "NOT_FOUND"},
		handlerCase{name: "resolve wrong kind", method: "POST", path: "/v1/interactions/{i}/resolve", role: "ws.editor",
			body: `{"kind":"tool_approval","choice":"deny"}`, status: 422, code: "INVALID_INPUT"},
		handlerCase{name: "resolve expired", method: "POST", path: "/v1/interactions/{i}/resolve", role: "ws.editor",
			setup: "expired", body: `{"kind":"ask_user","answer":"Ready"}`, status: 409, code: "INTERACTION_NOT_PENDING"},
		handlerCase{name: "resolve unavailable", method: "POST", path: "/v1/interactions/{i}/resolve", role: "ws.editor",
			body: `{"kind":"ask_user","answer":"Ready"}`, status: 503, code: "TEMPORAL_UNAVAILABLE",
			temporalError: serviceerror.NewUnavailable("offline")},
		handlerCase{name: "archived submission", method: "POST", path: "/v1/sessions/{s}/messages", role: "ws.editor",
			setup: "archived", body: `{"text":"Input"}`, status: 409, code: "SESSION_ARCHIVED"},
		handlerCase{name: "unsupported attachment", method: "POST", path: "/v1/sessions/{s}/messages", role: "ws.editor",
			body: `{"text":"Input","attachments":[{}]}`, status: 422, code: "ATTACHMENTS_UNSUPPORTED"},
		handlerCase{name: "admission quota", method: "POST", path: "/v1/sessions/{s}/messages", role: "ws.editor",
			setup: "quota", body: `{"text":"Input"}`, status: 429, code: "QUOTA_EXCEEDED"},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { testRunHTTPHandler(t, d, admin, tc) })
	}
}

func testRunHTTPHandler(t *testing.T, d *db.DB, admin *pgx.Conn, tc handlerCase) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	interaction, artifact := ids.New(), ids.New()
	var lock contract.RunLock
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		if tc.role != "none" && tc.role != "anonymous" {
			require.NoError(t, tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, tc.role, "workspace", f.workspace))
		}
		var raw []byte
		require.NoError(t, tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE tenant_id=$1 AND id=$2`, f.tenant, f.lock).Scan(&raw))
		require.NoError(t, json.Unmarshal(raw, &lock))
		for key, entry := range lock.Entries {
			entry.Files = nil
			lock.Entries[key] = entry
		}
		_, err := tx.Exec(ctx, `INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,'result.txt','text/plain',4,$5,$6,'final')`, artifact, f.tenant, f.session, f.run,
			"sha256:"+strings.Repeat("0", 64), "t/"+f.tenant+"/runs/"+f.run+"/outputs/result.txt")
		return err
	})
	op := f.op()
	_, err := f.activities.CreateInteraction(t.Context(), w.InteractionInput{RunOp: op,
		Request:   contract.InteractionRequest{InteractionId: interaction, Kind: "ask_user", Question: &contract.Question{Text: "Ready?"}},
		ExpiresAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		switch tc.setup {
		case "finished":
			_, err = tx.Exec(ctx, `UPDATE runs SET status='succeeded' WHERE tenant_id=$1 AND id=$2`, f.tenant, f.run)
		case "archived":
			_, err = tx.Exec(ctx, `UPDATE sessions SET status='archived' WHERE tenant_id=$1 AND id=$2`, f.tenant, f.session)
		case "non-approver":
			_, err = tx.Exec(ctx, `UPDATE interactions SET approvers='[]' WHERE tenant_id=$1 AND id=$2`, f.tenant, interaction)
		case "expired":
			_, err = tx.Exec(ctx, `UPDATE interactions SET expires_at=now()-interval '1 second' WHERE tenant_id=$1 AND id=$2`,
				f.tenant, interaction)
		case "quota":
			_, err = tx.Exec(ctx, `INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,'tenant',$2,'runs_concurrent',0,'total','block')`, ids.New(), f.tenant)
		}
		return err
	})
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		CapabilityKeyID: "test", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}, DefaultRunBudget: 200000}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), d, c, logger)
	require.NoError(t, err)
	defer a.Close()
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	fake := &handlerTemporal{err: tc.temporalError}
	h := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger, Temporal: fake,
		Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}, Locks: runResolver{lock: lock}}).Router()
	replace := strings.NewReplacer("{s}", f.session, "{r}", f.run, "{i}", interaction, "{a}", artifact, "{w}", f.workspace)
	r := httptest.NewRequest(tc.method, replace.Replace(tc.path), bytes.NewBufferString(replace.Replace(tc.body)))
	if tc.role != "anonymous" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("X-Orbit-Tenant", f.tenant)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	require.Equal(t, tc.status, out.Code, out.Body.String())
	if tc.code != "" {
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &body))
		require.Equal(t, tc.code, body.Error.Code)
	}
	if tc.setup == "empty-list" {
		require.JSONEq(t, `{"items":[],"next_cursor":""}`, out.Body.String())
	}
	if tc.status == 302 {
		require.Equal(t, "http://objects.invalid/file", out.Header().Get("Location"))
	}
	if tc.status == 403 || tc.status == 404 || tc.status == 422 {
		require.Empty(t, fake.calls, "rejected request must not reach Temporal")
	}
	if strings.Contains(tc.path, "stream-token") && tc.status == 200 {
		require.Contains(t, out.Body.String(), "stream_token")
	}
	if tc.path == "/v1/sessions/{s}/archive" && tc.status == 202 {
		require.Equal(t, []string{w.Archive}, fake.calls)
	}
}
