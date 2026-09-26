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

// decideWorker stubs the worker for M-d: resolveApproval takes resolveDelay
// (so decisions overlap, or outlast the delivery timeout), and every
// delivered decision is counted on receipt.
func decideWorker(t *testing.T, resolveDelay time.Duration) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
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
			time.Sleep(resolveDelay)
			_, _ = io.WriteString(w, `{"applied":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &resolves, &resumes
}

const racers = 8

// concurrentDecides fires racers decisions per approval, all released at
// the same instant, and returns each approval's sorted status codes.
func concurrentDecides(t *testing.T, base string, approvalIDs []string, u string) [][]int {
	var wg sync.WaitGroup
	start := make(chan struct{})
	out := make([][]int, len(approvalIDs))
	for a := range approvalIDs {
		out[a] = make([]int, racers)
	}
	for a, approvalID := range approvalIDs {
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(a, i int, approvalID string) {
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
				out[a][i] = res.StatusCode
			}(a, i, approvalID)
		}
	}
	close(start)
	wg.Wait()
	for a := range out {
		sort.Ints(out[a])
	}
	return out
}

func oneWinner(statuses [][]int) bool {
	for _, s := range statuses {
		if len(s) != racers || s[0] != 200 {
			return false
		}
		for _, code := range s[1:] {
			if code != 409 {
				return false
			}
		}
	}
	return true
}

func expectedStatuses(n int) [][]int {
	one := []int{200}
	for i := 1; i < racers; i++ {
		one = append(one, 409)
	}
	out := make([][]int, n)
	for i := range out {
		out[i] = one
	}
	return out
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
	const rounds = 5
	wk, resolves, resumes := decideWorker(t, 300*time.Millisecond)
	srv := startServer(t, serverOpts{tenant: "t-md-worker", maxConns: 16, workerURL: wk.URL})
	var aps []string
	for i := 0; i < rounds; i++ {
		aps = append(aps, park(srv, "t-md-worker", "u-md", fmt.Sprintf("worker-%d", i)))
	}
	statuses := concurrentDecides(t, srv.base, aps, "u-md")
	states := []string{}
	for _, id := range aps {
		states = append(states, approvalState(id))
	}
	allDecided := true
	for _, st := range states {
		allDecided = allDecided && st == "decided:allow"
	}
	record(t, caseInput{ID: "REVIEW-Md/worker/concurrent-decide", Contract: c,
		Description: fmt.Sprintf("%d approvals × %d concurrent allow decisions: per approval exactly one 2xx and the rest 409; the worker receives exactly one decision per approval (stub worker counts resolveApproval and the resumed runTurn)", rounds, racers),
		Steps:       []string{fmt.Sprintf("release %d POST /v1/approvals/{id}/decide per approval at the same instant as u-md", racers), "count stub-worker resolveApproval and resume calls", "owner reads approvals.status"},
		Request:     httpReq{Method: "POST", Path: "/v1/approvals/{id}/decide", Headers: user("u-md"), Body: `{"decision":"allow"}`},
		Expected:    map[string]any{"statusesSortedPerApproval": expectedStatuses(rounds), "workerResolveApproval": rounds, "workerResume": rounds, "approvalsDecidedAllow": true},
		Actual:      map[string]any{"statusesSortedPerApproval": statuses, "workerResolveApproval": resolves.Load(), "workerResume": resumes.Load(), "approvalsDecidedAllow": allDecided},
		Pass:        oneWinner(statuses) && resolves.Load() == rounds && resumes.Load() == rounds && allDecided})
	ap := aps[0]
	srv.check(t, "REVIEW-Md/worker/decide-again", c, "a later decision on the decided approval → 409 APPROVAL_NOT_PENDING, nothing sent",
		httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-md"), Body: `{"decision":"reject"}`},
		httpExp{Status: 409, BodyEquals: `{"error":"approval not pending","code":"APPROVAL_NOT_PENDING","message":"approval is not pending"}` + "\n"})
	record(t, caseInput{ID: "REVIEW-Md/worker/no-extra-delivery", Contract: c, Description: "the rejected later decision reached neither resolveApproval nor resume",
		Request: "stub worker counters", Expected: map[string]int32{"resolveApproval": rounds, "resume": rounds},
		Actual: map[string]int32{"resolveApproval": resolves.Load(), "resume": resumes.Load()}, Pass: resolves.Load() == rounds && resumes.Load() == rounds})

	// Low-1 (ISO-20): a decided approval is frozen, even for direct SQL as
	// orbit_app (which holds UPDATE on status/decision/decided_at).
	frozen := aps[1]
	for _, tc := range []struct{ id, desc, sql string }{
		{"reopen", "reopen a decided approval", `UPDATE approvals SET status = 'pending', decision = '', decided_at = NULL WHERE id = $1`},
		{"flip", "flip a decided approval from allow to reject", `UPDATE approvals SET decision = 'reject' WHERE id = $1`},
	} {
		before := approvalState(frozen)
		_, err := execAsApp(ctx, srv.appPool, "t-md-worker", tc.sql, frozen)
		after := approvalState(frozen)
		iso(t, "REVIEW-Low1/"+tc.id+"-rejected", c, []string{"FM-58"}, "orbit_app direct SQL cannot "+tc.desc+": the trigger raises and the value is unchanged",
			sqlReq{Role: "orbit_app", Tenant: "t-md-worker", SQL: tc.sql},
			map[string]string{"sqlstate": "P0001", "error": "approval is not pending", "approval": "decided:allow"},
			map[string]string{"sqlstate": sqlState(err), "error": pgMessage(err), "approval": after},
			sqlState(err) == "P0001" && pgMessage(err) == "approval is not pending" && before == "decided:allow" && after == before)
	}

	// Temporal path: the stub Orchestrator's Decide Update is the record.
	so := &stubOrch{askApproval: true, decideDelay: 300 * time.Millisecond}
	osrv := startServer(t, serverOpts{tenant: "t-md-orch", maxConns: 16, orch: so})
	var oaps []string
	for i := 0; i < rounds; i++ {
		oaps = append(oaps, park(osrv, "t-md-orch", "u-md-orch", fmt.Sprintf("orch-%d", i)))
	}
	ostatuses := concurrentDecides(t, osrv.base, oaps, "u-md-orch")
	odecided := true
	for _, id := range oaps {
		odecided = odecided && approvalState(id) == "decided:allow"
	}
	record(t, caseInput{ID: "REVIEW-Md/orch/concurrent-decide", Contract: c,
		Description: fmt.Sprintf("Temporal path: %d approvals × %d concurrent allow decisions → one 2xx and the rest 409 per approval; exactly one decide Update per approval (stub Orchestrator counts Decide)", rounds, racers),
		Steps:       []string{fmt.Sprintf("release %d POST /v1/approvals/{id}/decide per approval at the same instant as u-md-orch", racers), "count stub Orchestrator Decide calls", "owner reads approvals.status"},
		Request:     httpReq{Method: "POST", Path: "/v1/approvals/{id}/decide", Headers: user("u-md-orch"), Body: `{"decision":"allow"}`},
		Expected:    map[string]any{"statusesSortedPerApproval": expectedStatuses(rounds), "orchDecideUpdates": rounds, "approvalsDecidedAllow": true},
		Actual:      map[string]any{"statusesSortedPerApproval": ostatuses, "orchDecideUpdates": so.decides.Load(), "approvalsDecidedAllow": odecided},
		Pass:        oneWinner(ostatuses) && so.decides.Load() == rounds && odecided})
}

// Review F2 (FM-59): documented, fixed in phase 2 with the real worker.
func TestReviewF2DeliveredButTimeoutOnce(t *testing.T) {
	blocked(t, "REVIEW-F2/delivered-but-timeout-once", "§18 review F2", "e2e",
		"inject one decision that is delivered but whose response times out, then retry: the worker applies the decision exactly once",
		"phase 2, together with the real worker: reopen only on errors that prove non-delivery and dedupe deliveries by approval id. Phase 1 has no reopen at all (Low-1 trigger), so no second delivery can happen today",
		[]string{"real worker applies resolveApproval but its response is delayed past the control timeout", "retry POST /v1/approvals/{id}/decide", "count applied decisions in the worker"},
		map[string]any{"workerAppliedDecisions": 1})
}

const deliveryFailedBody = `{"error":"decision delivery failed","code":"DECISION_DELIVERY_FAILED","message":"the decision is recorded but its delivery to the workflow failed or timed out; it is not retried"}` + "\n"
const notPendingBody = `{"error":"approval not pending","code":"APPROVAL_NOT_PENDING","message":"approval is not pending"}` + "\n"

// Review F2, phase 1: a timed-out delivery returns the documented 502, the
// approval stays decided, a resubmit gets 409, and the workflow side
// receives the decision at most once.
func TestReviewF2Phase1TimeoutAtMostOnce(t *testing.T) {
	const c = "§18 review F2 (phase 1)"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	approvalState := func(id string) string {
		var s string
		_ = ownerPool.QueryRow(ctx, `SELECT status || ':' || decision FROM approvals WHERE id = $1`, id).Scan(&s)
		return s
	}
	park := func(srv *server, u, label string) string {
		room := roomID(t, srv.check(t, "REVIEW-F2/phase1/"+label+"/create", c, "create a task",
			httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
		alias(room, "<room-f2-"+label+">")
		posted := srv.check(t, "REVIEW-F2/phase1/"+label+"/park", c, "a message parks one approval",
			httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Headers: user(u), Body: `{"message":"list files"}`},
			httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
		var body struct {
			Approval *app.Approval `json:"approval"`
		}
		_ = json.Unmarshal([]byte(posted.Body), &body)
		if body.Approval == nil {
			t.Fatalf("no approval: %s", posted.Body)
		}
		alias(body.Approval.ID, "<approval-f2-"+label+">")
		return body.Approval.ID
	}

	for _, path := range []struct {
		label, simulated string
		setup            func() (*server, func() int32, func() int32)
	}{
		{"worker",
			"simulated: the stub orbit-worker accepts resolveApproval (counted on receipt as delivered) and answers only after 1s; control's DeliveryTimeout is 200ms, so control sees a timeout after the worker already has the decision",
			func() (*server, func() int32, func() int32) {
				wk, resolves, resumes := decideWorker(t, time.Second)
				srv := startServer(t, serverOpts{tenant: "t-f2-worker", maxConns: 4, workerURL: wk.URL, deliveryTimeout: 200 * time.Millisecond})
				return srv, resolves.Load, resumes.Load
			}},
		{"orch",
			"simulated: the stub Orchestrator accepts the decide Update (counted on receipt as delivered) and answers only after 1s; control's DeliveryTimeout is 200ms, so control sees a timeout after the workflow already has the decision",
			func() (*server, func() int32, func() int32) {
				so := &stubOrch{askApproval: true, decideDelay: time.Second}
				srv := startServer(t, serverOpts{tenant: "t-f2-orch", maxConns: 4, orch: so, deliveryTimeout: 200 * time.Millisecond})
				return srv, so.decides.Load, func() int32 { return 0 }
			}},
	} {
		srv, deliveries, resumes := path.setup()
		u := "u-f2-" + path.label
		ap := park(srv, u, path.label)
		id := "REVIEW-F2/phase1-timeout-at-most-once/" + path.label
		decideReq := httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user(u), Body: `{"decision":"allow"}`}
		first := sendTo(t, srv.base, decideReq)
		afterFirst := approvalState(ap)
		second := sendTo(t, srv.base, decideReq)
		afterSecond := approvalState(ap)
		// Let the stub finish its delayed answer before counting.
		time.Sleep(1200 * time.Millisecond)
		n := deliveries()
		logs := srv.logs.String()
		wantLog := "WARN alert=decision_delivery_failed approval=" + ap + " reason=timeout"
		record(t, caseInput{ID: id, Contract: c, Description: "delivery timeout → documented 502 body; approval stays decided; resubmit → 409; the workflow side receives the decision at most once. " + path.simulated,
			Steps: []string{
				"POST /v1/approvals/{id}/decide (allow) while the fake workflow endpoint holds the delivery past DeliveryTimeout",
				"owner reads approvals.status after the first call",
				"POST the same decision again",
				"owner reads approvals.status again; count decisions received at the fake workflow endpoint; read the server log",
			},
			Request: decideReq,
			Expected: map[string]any{
				"first":                 map[string]any{"status": 502, "body": deliveryFailedBody},
				"approvalAfterFirst":    "decided:allow",
				"resubmit":              map[string]any{"status": 409, "body": notPendingBody},
				"approvalAfterResubmit": "decided:allow",
				"deliveriesAtMostOnce":  true,
				"deliveriesReceived":    1,
				"resumedTurns":          0,
				"warningLogged":         true,
			},
			Actual: map[string]any{
				"first":                 map[string]any{"status": first.Status, "body": first.Body},
				"approvalAfterFirst":    afterFirst,
				"resubmit":              map[string]any{"status": second.Status, "body": second.Body},
				"approvalAfterResubmit": afterSecond,
				"deliveriesAtMostOnce":  n <= 1,
				"deliveriesReceived":    n,
				"resumedTurns":          resumes(),
				"warningLogged":         strings.Contains(logs, wantLog),
			},
			Pass: first.Status == 502 && first.Body == deliveryFailedBody && afterFirst == "decided:allow" &&
				second.Status == 409 && second.Body == notPendingBody && afterSecond == "decided:allow" &&
				n <= 1 && n == 1 && resumes() == 0 && strings.Contains(logs, wantLog)})
	}
}
