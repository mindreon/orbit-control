//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/store/auth"
)

// Review M-c (ISO-19): history is immutable and UPDATE is column-scoped.
func TestReviewMcColumnScopedUpdate(t *testing.T) {
	const c = "§18.5 review M-c"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	wk := stubWorker(t, nil)
	const tenant, u = "t-mc", "u-mc"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})
	fx := seedTenant(t, srv, "REVIEW-Mc", tenant, u, "e2e-mc-key", "Mc")
	if err := auth.NewSessions(srv.appPool).Create(ctx, "mc-session-id-000001", u, tenant, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.appPool.Exec(ctx, `INSERT INTO oidc_login_state (state, nonce, pkce_verifier, expires_at) VALUES ('mc-state', 'n', 'v', now() + interval '10 minutes')`); err != nil {
		t.Fatal(err)
	}

	// Each UPDATE sets a real, valid value on a column outside the grant, so
	// no FK, RLS policy or CHECK could reject it before the privilege check.
	for _, tc := range []struct {
		table, set, where, snap string
		arg                     string
	}{
		{"events", `payload = '{"rewritten":true}'::jsonb`, `task_id = $1`, `SELECT string_agg(payload::text, ',' ORDER BY seq) FROM events WHERE task_id = $1`, fx.room},
		{"messages", `text = 'rewritten'`, `task_id = $1`, `SELECT string_agg(text, ',' ORDER BY created_at, id) FROM messages WHERE task_id = $1`, fx.room},
		{"artifact_versions", `mime_type = 'text/html'`, `artifact_id = 'art_' || $1`, `SELECT string_agg(mime_type, ',') FROM artifact_versions WHERE artifact_id = 'art_' || $1`, fx.room},
		{"rooms", `title = 'rewritten'`, `id = $1`, `SELECT title FROM rooms WHERE id = $1`, fx.room},
		{"approvals", `tool_name = 'rewritten'`, `task_id = $1`, `SELECT string_agg(tool_name, ',') FROM approvals WHERE task_id = $1`, fx.room},
		{"turns", `status = 'failed'`, `task_id = $1`, `SELECT string_agg(status, ',') FROM turns WHERE task_id = $1`, fx.room},
		{"artifacts", `title = 'rewritten'`, `task_id = $1`, `SELECT string_agg(title, ',') FROM artifacts WHERE task_id = $1`, fx.room},
		{"users", `display_name = 'rewritten'`, `id = $1`, `SELECT id || ':' || display_name FROM users WHERE id = $1`, u},
		{"personas", `name = 'rewritten'`, `tenant_id = $1`, `SELECT string_agg(name, ',') FROM personas WHERE tenant_id = $1`, tenant},
		{"mcp_connectors", `name = 'rewritten'`, `tenant_id = $1`, `SELECT string_agg(name, ',') FROM mcp_connectors WHERE tenant_id = $1`, tenant},
		{"cloud_agent_jobs", `state = 'done'`, `tenant_id = $1`, `SELECT string_agg(state, ',') FROM cloud_agent_jobs WHERE tenant_id = $1`, tenant},
		{"approval_rules", `tool_name = 'rewritten'`, `room_id = $1`, `SELECT string_agg(tool_name, ',') FROM approval_rules WHERE room_id = $1`, fx.room},
		{"idempotency_keys", `expires_at = expires_at + interval '1 day'`, `task_id = $1`, `SELECT string_agg(expires_at::text, ',') FROM idempotency_keys WHERE task_id = $1`, fx.room},
		{"sessions", `expires_at = expires_at + interval '1 day'`, `tenant_id = $1`, `SELECT string_agg(expires_at::text, ',') FROM sessions WHERE tenant_id = $1`, tenant},
		{"oidc_login_state", `nonce = 'rewritten'`, `state = 'mc-state' AND $1 <> ''`, `SELECT nonce FROM oidc_login_state WHERE state = 'mc-state' AND $1 <> ''`, tenant},
		{"tenants", `name = 'rewritten'`, `id = $1`, `SELECT name FROM tenants WHERE id = $1`, tenant},
	} {
		snapshot := func() string {
			var v *string
			if err := ownerPool.QueryRow(ctx, tc.snap, tc.arg).Scan(&v); err != nil || v == nil {
				return ""
			}
			return *v
		}
		before := snapshot()
		sql := `UPDATE ` + tc.table + ` SET ` + tc.set + ` WHERE ` + tc.where
		_, err := execAsApp(ctx, srv.appPool, tenant, sql, tc.arg)
		after := snapshot()
		iso(t, "REVIEW-Mc/update-denied-"+tc.table, c, []string{"FM-54", "FM-55"},
			"orbit_app UPDATE of an out-of-scope column on "+tc.table+" fails with 42501 permission denied; the value is unchanged",
			sqlReq{Role: "orbit_app", Tenant: tenant, SQL: sql},
			map[string]any{"sqlstate": "42501", "error": "permission denied", "rowPresent": true, "valueUnchanged": true},
			map[string]any{"sqlstate": sqlState(err), "error": privilegeDenied(err), "rowPresent": before != "", "valueUnchanged": after == before},
			sqlState(err) == "42501" && privilegeDenied(err) == "permission denied" && before != "" && after == before)
	}
}

// decideWorker stubs the worker for M-d: resolveApproval is slow so two
// decisions overlap, and every delivered decision is counted.
func decideWorker(t *testing.T) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	var resolves, resumes, n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		raw, _ := io.ReadAll(r.Body)
		i := n.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/openSession"):
			fmt.Fprintf(w, `{"sessionId":"sess-md-%d"}`, i)
		case strings.HasSuffix(r.URL.Path, "/runTurn"):
			if strings.Contains(string(raw), `"resumeAfterApproval":true`) {
				resumes.Add(1)
				_, _ = io.WriteString(w, `{"status":"completed","texts":["resumed"]}`)
				return
			}
			_, _ = io.WriteString(w, `{"status":"needs_approval","approval":{"approvalRequestId":"ask-md","toolName":"bash"},"texts":["parked"]}`)
		case strings.HasSuffix(r.URL.Path, "/resolveApproval"):
			resolves.Add(1)
			time.Sleep(300 * time.Millisecond)
			_, _ = io.WriteString(w, `{"applied":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &resolves, &resumes
}

func concurrentDecides(t *testing.T, base, approvalID, u string) []int {
	var wg sync.WaitGroup
	start := make(chan struct{})
	statuses := make([]int, 2)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req, _ := http.NewRequest(http.MethodPost, base+"/v1/approvals/"+approvalID+"/decide", strings.NewReader(`{"decision":"allow"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(userHeader, u)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			statuses[i] = res.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()
	sort.Ints(statuses)
	return statuses
}

// Review M-d: two concurrent decisions → exactly one 2xx, one 409, and one
// delivered decision.
func TestReviewMdSingleWinnerDecide(t *testing.T) {
	const c = "§18 review M-d"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)

	park := func(srv *server, tenant, u, label string) string {
		room := roomID(t, srv.check(t, "REVIEW-Md/"+label+"/create", c, "create a task",
			httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
		alias(room, "<room-md-"+label+">")
		posted := srv.check(t, "REVIEW-Md/"+label+"/park", c, "a message parks one approval",
			httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Headers: user(u), Body: `{"message":"list files"}`},
			httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
		var body struct {
			Approval *app.Approval `json:"approval"`
		}
		_ = json.Unmarshal([]byte(posted.Body), &body)
		if body.Approval == nil {
			t.Fatalf("no approval: %s", posted.Body)
		}
		alias(body.Approval.ID, "<approval-md-"+label+">")
		return body.Approval.ID
	}
	approvalState := func(id string) string {
		var s string
		_ = ownerPool.QueryRow(ctx, `SELECT status || ':' || decision FROM approvals WHERE id = $1`, id).Scan(&s)
		return s
	}

	// Direct worker path: the stub worker is the outbound record.
	wk, resolves, resumes := decideWorker(t)
	srv := startServer(t, serverOpts{tenant: "t-md-worker", maxConns: 4, workerURL: wk.URL})
	ap := park(srv, "t-md-worker", "u-md", "worker")
	statuses := concurrentDecides(t, srv.base, ap, "u-md")
	record(t, caseInput{ID: "REVIEW-Md/worker/concurrent-decide", Contract: c,
		Description: "two concurrent allow decisions: exactly one 2xx and one 409; the worker receives exactly one decision (stub worker counts resolveApproval and the resumed runTurn)",
		Steps:       []string{"POST /v1/approvals/{id}/decide twice at the same instant as u-md", "count stub-worker resolveApproval and resume calls", "owner reads approvals.status"},
		Request:     httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-md"), Body: `{"decision":"allow"}`},
		Expected:    map[string]any{"statusesSorted": []int{200, 409}, "workerResolveApproval": 1, "workerResume": 1, "approval": "decided:allow"},
		Actual:      map[string]any{"statusesSorted": statuses, "workerResolveApproval": resolves.Load(), "workerResume": resumes.Load(), "approval": approvalState(ap)},
		Pass:        len(statuses) == 2 && statuses[0] == 200 && statuses[1] == 409 && resolves.Load() == 1 && resumes.Load() == 1 && approvalState(ap) == "decided:allow"})
	srv.check(t, "REVIEW-Md/worker/decide-again", c, "a later decision on the decided approval → 409 APPROVAL_NOT_PENDING, nothing sent",
		httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-md"), Body: `{"decision":"reject"}`},
		httpExp{Status: 409, BodyIncludes: []string{"APPROVAL_NOT_PENDING"}})
	record(t, caseInput{ID: "REVIEW-Md/worker/no-extra-delivery", Contract: c, Description: "the rejected later decision reached neither resolveApproval nor resume",
		Request: "stub worker counters", Expected: map[string]int32{"resolveApproval": 1, "resume": 1},
		Actual: map[string]int32{"resolveApproval": resolves.Load(), "resume": resumes.Load()}, Pass: resolves.Load() == 1 && resumes.Load() == 1})

	// Temporal path: the stub Orchestrator's Decide Update is the record.
	so := &stubOrch{askApproval: true, decideDelay: 300 * time.Millisecond}
	osrv := startServer(t, serverOpts{tenant: "t-md-orch", maxConns: 4, orch: so})
	oap := park(osrv, "t-md-orch", "u-md-orch", "orch")
	ostatuses := concurrentDecides(t, osrv.base, oap, "u-md-orch")
	record(t, caseInput{ID: "REVIEW-Md/orch/concurrent-decide", Contract: c,
		Description: "Temporal path: two concurrent allow decisions → one 2xx, one 409; exactly one decide Update is sent (stub Orchestrator counts Decide)",
		Steps:       []string{"POST /v1/approvals/{id}/decide twice at the same instant as u-md-orch", "count stub Orchestrator Decide calls", "owner reads approvals.status"},
		Request:     httpReq{Method: "POST", Path: "/v1/approvals/" + oap + "/decide", Headers: user("u-md-orch"), Body: `{"decision":"allow"}`},
		Expected:    map[string]any{"statusesSorted": []int{200, 409}, "orchDecideUpdates": 1, "approval": "decided:allow"},
		Actual:      map[string]any{"statusesSorted": ostatuses, "orchDecideUpdates": so.decides.Load(), "approval": approvalState(oap)},
		Pass:        len(ostatuses) == 2 && ostatuses[0] == 200 && ostatuses[1] == 409 && so.decides.Load() == 1 && approvalState(oap) == "decided:allow"})
}
