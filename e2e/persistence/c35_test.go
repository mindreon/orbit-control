//go:build e2e

package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/failtext"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
)

// runtimeBlockedBy is why the orbit-runtime half stays blocked. It does not
// name an unmerged commit. Real-stack keeps the merged pin in e2e.yml until
// orbit-runtime#11's merge commit replaces it.
const runtimeBlockedBy = "orbit-runtime#11 is not merged; real-stack keeps the merged runtime pin in .github/workflows/e2e.yml; this case runs after that pin is the merge commit of orbit-runtime#11"

func useFault(t *testing.T, mode string) {
	t.Helper()
	app.SetE2EFault(mode)
	t.Cleanup(func() {
		app.SetE2EFault("")
		app.E2EBeforeClassify = nil
		pgstore.BeforeDeliveryUpdate = nil
		pgstore.AbortBeforeCommit = nil
	})
}

var parkSeq atomic.Int64

func parkQuiet(t *testing.T, srv *server, u string) (approvalID, room string) {
	t.Helper()
	created := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`})
	if created.Status != 200 {
		t.Fatalf("create room: %d %s", created.Status, created.Body)
	}
	room = roomID(t, created)
	n := parkSeq.Add(1)
	alias(room, fmt.Sprintf("<c35-room-%d>", n))
	posted := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Headers: user(u), Body: `{"message":"list files"}`})
	if posted.Status != 200 {
		t.Fatalf("park: %d %s", posted.Status, posted.Body)
	}
	var body struct {
		Approval *app.Approval `json:"approval"`
	}
	if err := json.Unmarshal([]byte(posted.Body), &body); err != nil || body.Approval == nil {
		t.Fatalf("no approval: %s", posted.Body)
	}
	alias(body.Approval.ID, fmt.Sprintf("<c35-approval-%d>", n))
	return body.Approval.ID, room
}

func ownerConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func approvalTuple(t *testing.T, id string) string {
	t.Helper()
	var s string
	err := ownerConn(t).QueryRow(context.Background(), `
		SELECT status || ':' || decision || ':' || COALESCE(delivery_state, '') || ':' || delivery_attempt::text
		  FROM approvals WHERE id = $1`, id).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func roomSnap(t *testing.T, id string) (state, failure, updated string, seq int64) {
	t.Helper()
	err := ownerConn(t).QueryRow(context.Background(), `
		SELECT state, COALESCE(failure::text, ''), updated_at::text, last_event_seq
		  FROM rooms WHERE id = $1`, id).Scan(&state, &failure, &updated, &seq)
	if err != nil {
		t.Fatal(err)
	}
	return state, failure, updated, seq
}

func deliveryEvents(t *testing.T, room string) string {
	t.Helper()
	var s string
	err := ownerConn(t).QueryRow(context.Background(), `
		SELECT COALESCE(string_agg(COALESCE(payload->>'deliveryState', ''), ',' ORDER BY seq), '')
		  FROM events WHERE task_id = $1 AND type = 'approval.delivery_updated'`, room).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func ownerWithoutTrigger(t *testing.T, fn func(context.Context, *pgx.Conn) error) error {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `ALTER TABLE approvals DISABLE TRIGGER orbit_approvals_delivery_transition`); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `ALTER TABLE approvals ENABLE TRIGGER orbit_approvals_delivery_transition`)
	}()
	return fn(ctx, conn)
}

func backdateApproval(t *testing.T, id string, decidedAgo, updatedAgo time.Duration) {
	t.Helper()
	err := ownerWithoutTrigger(t, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `
			UPDATE approvals
			   SET decided_at = CASE WHEN $2 = 0 THEN decided_at ELSE now() - make_interval(secs => $2) END,
			       delivery_updated_at = CASE WHEN $3 = 0 THEN delivery_updated_at ELSE now() - make_interval(secs => $3) END
			 WHERE id = $1`, id, decidedAgo.Seconds(), updatedAgo.Seconds())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func bumpAttempt(id string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `ALTER TABLE approvals DISABLE TRIGGER orbit_approvals_delivery_transition`); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `ALTER TABLE approvals ENABLE TRIGGER orbit_approvals_delivery_transition`)
	}()
	tag, err := conn.Exec(ctx, `UPDATE approvals SET delivery_attempt = delivery_attempt + 1 WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("bump attempt affected %d", tag.RowsAffected())
	}
	return nil
}

func reconcile(t *testing.T, srv *server) int {
	t.Helper()
	act := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/e2e/reconcile"})
	return act.Status
}

func TestSID3UnknownOnceSameUpdateID(t *testing.T) {
	useFault(t, "unknown-once")
	so := &stubOrch{askApproval: true, resumeText: "once"}
	srv := startServer(t, serverOpts{tenant: "t-sid3", maxConns: 4, orch: so, reconcileInterval: time.Hour})
	ap, _ := parkQuiet(t, srv, "u-sid3")
	act := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-sid3"), Body: `{"decision":"allow"}`})
	state := approvalTuple(t, ap)
	_, same := so.updateIDs.Load("ask-orch-1")
	record(t, caseInput{ID: "S-ID-3/unknown-once-same-update-id", Contract: "S-ID-3", Kind: "e2e", FailureModes: []string{"FM-75"},
		Description: "the first accept returns Unknown; the retry uses the same UpdateID and is delivered once",
		Steps:       []string{"ORBIT_E2E_FAULTS=unknown-once", "POST decide allow", "read delivery_state and the stub Update id"},
		Request:     map[string]string{"decision": "allow", "fault": "unknown-once"},
		Expected:    map[string]any{"status": 200, "state": "decided:allow:delivered:1", "decideCalls": 1, "updateID": "ask-orch-1"},
		Actual:      map[string]any{"status": act.Status, "state": state, "decideCalls": so.decides.Load(), "updateIDMatched": same},
		Pass:        act.Status == 200 && state == "decided:allow:delivered:1" && so.decides.Load() == 1 && same})
}

func TestSID4NotDelivered(t *testing.T) {
	useFault(t, "not-delivered")
	so := &stubOrch{askApproval: true, ttlS: 86400, resumeText: "after-reopen"}
	srv := startServer(t, serverOpts{tenant: "t-sid4", maxConns: 4, orch: so, reconcileInterval: time.Hour})
	ap, room := parkQuiet(t, srv, "u-sid4")
	first := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-sid4"), Body: `{"decision":"allow"}`})
	state := approvalTuple(t, ap)
	events := deliveryEvents(t, room)
	app.SetE2EFault("")
	second := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-sid4"), Body: `{"decision":"allow"}`})
	after := approvalTuple(t, ap)
	hasBoth := strings.Contains(events, "not_delivered") && strings.Contains(events, ",")
	record(t, caseInput{ID: "S-ID-4/not-delivered", Contract: "S-ID-4", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "NotDelivered reopens the approval; a later decide is delivered once",
		Steps:       []string{"fault returns APPROVAL_UNKNOWN inside ttlS/2", "read the row and delivery events", "clear the fault and decide again"},
		Request:     map[string]string{"fault": "not-delivered"},
		Expected:    map[string]any{"firstStatus": 503, "reopened": "pending:::1", "eventsHaveNotDeliveredThenPending": true, "secondStatus": 200, "decideCalls": 1},
		Actual:      map[string]any{"firstStatus": first.Status, "state": state, "events": events, "secondStatus": second.Status, "after": after, "decideCalls": so.decides.Load()},
		Pass:        first.Status == 503 && strings.Contains(first.Body, "APPROVAL_NOT_DELIVERED") && state == "pending:::1" && hasBoth && second.Status == 200 && after == "decided:allow:delivered:2" && so.decides.Load() == 1})
}

func TestSID5UnknownThenWorkflowEnded(t *testing.T) {
	useFault(t, "unknown-always")
	so := &stubOrch{askApproval: true}
	srv := startServer(t, serverOpts{tenant: "t-sid5", maxConns: 4, orch: so, deliveryTimeout: 200 * time.Millisecond, reconcileInterval: time.Hour})
	ap, _ := parkQuiet(t, srv, "u-sid5")
	act := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-sid5"), Body: `{"decision":"allow"}`})
	so.outcomes.Delete("ask-orch-1")
	so.outcomeErr = errors.New("workflow ended")
	so.pending = false
	code := reconcile(t, srv)
	state := approvalTuple(t, ap)
	okState := strings.HasPrefix(state, "decided:allow:") && !strings.Contains(state, "not_delivered") && !strings.HasPrefix(state, "pending:")
	record(t, caseInput{ID: "S-ID-5/unknown-then-workflow-ended", Contract: "S-ID-5", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "after Unknown, a finished workflow is not reopened and is not delivered again",
		Steps:       []string{"unknown-always so the workflow is called once", "drop the outcome and fail further queries", "reconcile", "read the row"},
		Request:     map[string]string{"fault": "unknown-always"},
		Expected:    map[string]any{"status": 202, "reconcile": 200, "decideCalls": 1, "staysAllowed": true},
		Actual:      map[string]any{"status": act.Status, "reconcile": code, "state": state, "decideCalls": so.decides.Load()},
		Pass:        act.Status == 202 && code == 200 && so.decides.Load() == 1 && okState && strings.Contains(state, ":unknown:")})
}

func TestSID6ReconcileVsReopen(t *testing.T) {
	useFault(t, "unknown-always")
	so := &stubOrch{askApproval: true, pending: true}
	const tenant = "t-sid6"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 8, orch: so, deliveryTimeout: 200 * time.Millisecond, reconcileInterval: time.Hour})
	ap, _ := parkQuiet(t, srv, "u-sid6")
	act := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-sid6"), Body: `{"decision":"allow"}`})
	so.outcomes.Delete("ask-orch-1")
	var wg sync.WaitGroup
	codes := make([]int, 4)
	t9 := make([]string, 4)
	sql := `
		UPDATE approvals
		   SET status = 'pending', decision = '', decided_at = NULL, delivery_state = NULL
		 WHERE tenant_id = $1 AND id = $2 AND status = 'decided' AND delivery_state = 'not_delivered'`
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			codes[i] = reconcile(t, srv)
		}(i)
		go func(i int) {
			defer wg.Done()
			n, err := execAsApp(context.Background(), srv.appPool, tenant, sql, tenant, ap)
			if err != nil {
				t9[i] = sqlState(err) + ":" + pgMessage(err)
				return
			}
			t9[i] = fmt.Sprintf("%d:ok", n)
		}(i)
	}
	wg.Wait()
	state := approvalTuple(t, ap)
	no500 := true
	for _, c := range codes {
		if c == 500 || c == 0 {
			no500 = false
		}
	}
	t9ok := true
	for _, s := range t9 {
		if s != "0:ok" && !strings.HasPrefix(s, "P0001:") {
			t9ok = false
		}
	}
	decidedAllow := strings.HasPrefix(state, "decided:allow:")
	record(t, caseInput{ID: "S-ID-6/reconcile-vs-reopen", Contract: "S-ID-6", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "four reconcile calls race four T9 updates; T9 does not match an unknown row and nothing returns 500",
		Steps:       []string{"leave the approval unknown", "POST /internal/e2e/reconcile four times", "run T9 SQL four times as orbit_app", "read the row"},
		Request:     map[string]string{"approval": "unknown"},
		Expected:    map[string]any{"decideStatus": 202, "decideCalls": 1, "no500": true, "t9ZeroOrP0001": true, "decidedAllow": true},
		Actual:      map[string]any{"decideStatus": act.Status, "decideCalls": so.decides.Load(), "no500": no500, "t9ZeroOrP0001": t9ok, "decidedAllow": decidedAllow},
		Pass:        act.Status == 202 && so.decides.Load() == 1 && no500 && t9ok && decidedAllow})
}

func TestSID8Control(t *testing.T) {
	const tenant = "t-sid8"
	so := &stubOrch{askApproval: true, resumeText: "limit-turn"}
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: so, reconcileInterval: time.Hour, deliveryTimeout: 300 * time.Millisecond})
	u := "u-sid8"

	useFault(t, "fatal-on-get")
	ap, room := parkQuiet(t, srv, u)
	limitBody := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	limitState := approvalTuple(t, ap)
	roomState, failure, _, _ := roomSnap(t, room)
	limitOK := limitBody.Status == 409 && strings.Contains(limitBody.Body, "ROOM_FAILED") && strings.Contains(limitBody.Body, failtext.DecidedApprovalsLimit) &&
		limitState == "decided:allow:unresolved:1" && roomState == "failed" && strings.Contains(failure, failtext.DecidedApprovalsLimit)
	record(t, caseInput{ID: "S-ID-8/t11-application-error", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-66", "FM-70"},
		Description: "handle.Get returns ApplicationError DECIDED_APPROVALS_LIMIT, so T11 and room.failed share one transaction",
		Steps:       []string{"fault fatal-on-get", "POST decide allow", "read the approval, the room failure, and the HTTP body"},
		Request:     map[string]string{"fault": "fatal-on-get"},
		Expected:    map[string]any{"status": 409, "code": "ROOM_FAILED", "state": "decided:allow:unresolved:1", "room": "failed", "message": failtext.DecidedApprovalsLimit},
		Actual:      map[string]any{"status": limitBody.Status, "state": limitState, "room": roomState, "failure": failure, "bodyHasMessage": strings.Contains(limitBody.Body, failtext.DecidedApprovalsLimit)},
		Pass:        limitOK})

	app.SetE2EFault("worker-error-on-get")
	ap2, room2 := parkQuiet(t, srv, u)
	workerBody := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap2 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	workerState := approvalTuple(t, ap2)
	workerRoom, _, _, _ := roomSnap(t, room2)
	record(t, caseInput{ID: "FM-66/other-get-is-502", Contract: "FM-66", Kind: "e2e", FailureModes: []string{"FM-66"},
		Description: "a plain error whose text contains DECIDED_APPROVALS_LIMIT is 502 WORKER_ERROR and does not take T11",
		Steps:       []string{"fault worker-error-on-get", "POST decide allow", "read status, delivery_state, and room state"},
		Request:     map[string]string{"fault": "worker-error-on-get"},
		Expected:    map[string]any{"status": 502, "code": "WORKER_ERROR", "state": "decided:allow:delivered:1", "roomNotFailed": true, "hidesBoom": true},
		Actual:      map[string]any{"status": workerBody.Status, "body": workerBody.Body, "state": workerState, "room": workerRoom},
		Pass:        workerBody.Status == 502 && strings.Contains(workerBody.Body, "WORKER_ERROR") && strings.Contains(workerBody.Body, "the resumed turn failed") && !strings.Contains(workerBody.Body, "boom") && workerState == "decided:allow:delivered:1" && workerRoom != "failed"})

	app.SetE2EFault("")
	msg := failtext.StateUnreadable
	ap3, room3 := parkQuiet(t, srv, u)
	ev := `{"type":"room.failed","roomId":"` + room3 + `","occurredAt":"2026-09-26T00:00:00Z","failure":{"code":"state_unreadable","message":"` + msg + `"}}`
	first := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events", Body: ev})
	st1, fail1, upd1, seq1 := roomSnap(t, room3)
	apState := approvalTuple(t, ap3)
	second := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events", Body: ev})
	st2, fail2, upd2, seq2 := roomSnap(t, room3)
	record(t, caseInput{ID: "S-ID-8/ingest-room-failed", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-67"},
		Description: "ingesting room.failed stores failure from the event and does not change the approval",
		Steps:       []string{"POST /internal/events room.failed", "read rooms.failure and the approval"},
		Request:     map[string]string{"type": "room.failed"},
		Expected:    map[string]any{"status": 202, "room": "failed", "code": "state_unreadable", "approvalUntouched": true},
		Actual:      map[string]any{"status": first.Status, "room": st1, "failure": fail1, "approval": apState},
		Pass:        first.Status == 202 && st1 == "failed" && strings.Contains(fail1, `"code": "state_unreadable"`) && strings.Contains(fail1, msg) && strings.HasPrefix(apState, "pending:")})
	record(t, caseInput{ID: "S-ID-8/ingest-room-failed-twice", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-67"},
		Description: "a second room.failed ingest leaves failure and last_event_seq unchanged",
		Steps:       []string{"POST the same room.failed again", "compare failure text, updated_at, and last_event_seq with the first ingest"},
		Request:     map[string]string{"type": "room.failed", "again": "1"},
		Expected:    map[string]any{"status": 202, "failureUnchanged": true, "seqUnchanged": true, "updatedUnchanged": true},
		Actual:      map[string]any{"status": second.Status, "failureSame": fail1 == fail2, "seq": []int64{seq1, seq2}, "updatedSame": upd1 == upd2, "state": st2},
		Pass:        second.Status == 202 && fail1 == fail2 && seq1 == seq2 && upd1 == upd2 && st2 == "failed"})
	decide := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap3 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	record(t, caseInput{ID: "S-ID-8/decide-after-room-failed", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-67"},
		Description: "decide on a room that already failed is 409 ROOM_FAILED",
		Steps:       []string{"POST decide allow on the failed room"},
		Request:     map[string]string{"decision": "allow"},
		Expected:    map[string]any{"status": 409, "code": "ROOM_FAILED"},
		Actual:      map[string]any{"status": decide.Status, "body": decide.Body},
		Pass:        decide.Status == 409 && strings.Contains(decide.Body, "ROOM_FAILED") && strings.Contains(decide.Body, msg)})

	// Stalled in_flight on a failed room: T3 then, with no outcome, T8. Not T6.
	ap4, room4 := parkQuiet(t, srv, u)
	if _, err := execAsApp(context.Background(), srv.appPool, tenant, `
		UPDATE approvals
		   SET status = 'decided', decision = 'allow', decided_at = now(),
		       delivery_state = 'in_flight', delivery_attempt = delivery_attempt + 1
		 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`, tenant, ap4); err != nil {
		t.Fatal(err)
	}
	backdateApproval(t, ap4, 0, 2*time.Minute)
	failEv := `{"type":"room.failed","roomId":"` + room4 + `","occurredAt":"2026-09-26T00:00:01Z","failure":{"code":"state_unreadable","message":"` + msg + `"}}`
	if got := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events", Body: failEv}); got.Status != 202 {
		t.Fatalf("fail room: %d %s", got.Status, got.Body)
	}
	so.outcomes.Delete("ask-orch-1")
	before := approvalTuple(t, ap4)
	code := reconcile(t, srv)
	after := approvalTuple(t, ap4)
	record(t, caseInput{ID: "S-ID-8/failed-room-outcome-unresolved", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-67"},
		Description: "a failed room's stalled in_flight row goes T3 then T8 when decideOutcome has nothing; it does not retry with T6",
		Steps:       []string{"claim in_flight and backdate delivery_updated_at", "ingest room.failed", "reconcile once", "read the attempt"},
		Request:     map[string]string{"room": "failed", "outcome": "missing"},
		Expected:    map[string]any{"before": "decided:allow:in_flight:1", "after": "decided:allow:unresolved:1", "reconcile": 200},
		Actual:      map[string]any{"before": before, "after": after, "reconcile": code},
		Pass:        before == "decided:allow:in_flight:1" && after == "decided:allow:unresolved:1" && code == 200})

	app.SetE2EFault("unknown-always")
	ap5, room5 := parkQuiet(t, srv, u)
	unk := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap5 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	app.SetE2EFault("")
	failEv2 := `{"type":"room.failed","roomId":"` + room5 + `","occurredAt":"2026-09-26T00:00:02Z","failure":{"code":"state_unreadable","message":"` + msg + `"}}`
	_ = sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events", Body: failEv2})
	code = reconcile(t, srv)
	done := approvalTuple(t, ap5)
	record(t, caseInput{ID: "S-ID-8/failed-room-outcome-delivered", Contract: "S-ID-8", Kind: "e2e", FailureModes: []string{"FM-67"},
		Description: "a failed room's unknown row becomes delivered in one reconcile when decideOutcome is done, without T6",
		Steps:       []string{"decide to unknown with an outcome stored", "ingest room.failed", "reconcile once"},
		Request:     map[string]string{"outcome": "done"},
		Expected:    map[string]any{"decide": 202, "after": "decided:allow:delivered:1", "reconcile": 200},
		Actual:      map[string]any{"decide": unk.Status, "after": done, "reconcile": code},
		Pass:        unk.Status == 202 && done == "decided:allow:delivered:1" && code == 200})
}

func TestSID9Convergence(t *testing.T) {
	useFault(t, "unknown-always")
	so := &stubOrch{askApproval: true, resumeText: "converged"}
	srv := startServer(t, serverOpts{tenant: "t-sid9a", maxConns: 4, orch: so, deliveryTimeout: 200 * time.Millisecond, reconcileInterval: time.Second})
	u := "u-sid9a"
	ap, room := parkQuiet(t, srv, u)
	seq := ownerScalar[int64](t, newPool(t, ownerURL, 1), `SELECT last_event_seq FROM rooms WHERE id = $1`, room)
	sse := readSSEUntil(t, srv.base, room, u, fmt.Sprintf("%d", seq), 8*time.Second, `"deliveryState":"delivered"`)
	act := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	var sseBody string
	select {
	case sseBody = <-sse:
	case <-time.After(8 * time.Second):
	}
	srv.runtime.StopReconcile()
	state := approvalTuple(t, ap)
	saw := strings.Contains(sseBody, `"deliveryState":"delivered"`)
	record(t, caseInput{ID: "S-ID-9/a-unknown-then-delivered", Contract: "S-ID-9", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "Unknown is 202; within one reconcile interval plus 5s the row and the SSE stream show delivered",
		Steps:       []string{"ORBIT_DELIVERY_RECONCILE_INTERVAL_S equivalent is 1s", "POST decide", "read SSE and the row"},
		Request:     map[string]string{"fault": "unknown-always", "interval": "1s"},
		Expected:    map[string]any{"status": 202, "state": "decided:allow:delivered:1", "sse": true},
		Actual:      map[string]any{"status": act.Status, "state": state, "sse": saw},
		Pass:        act.Status == 202 && strings.Contains(act.Body, `"deliveryState":"unknown"`) && state == "decided:allow:delivered:1" && saw})

	soB := &stubOrch{askApproval: true}
	srvB := startServer(t, serverOpts{tenant: "t-sid9b", maxConns: 4, orch: soB, deliveryTimeout: 200 * time.Millisecond, reconcileInterval: time.Hour})
	apB, _ := parkQuiet(t, srvB, "u-sid9b")
	first := sendTo(t, srvB.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apB + "/decide", Headers: user("u-sid9b"), Body: `{"decision":"allow"}`})
	calls := soB.decides.Load()
	same := sendTo(t, srvB.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apB + "/decide", Headers: user("u-sid9b"), Body: `{"decision":"allow"}`})
	other := sendTo(t, srvB.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apB + "/decide", Headers: user("u-sid9b"), Body: `{"decision":"reject"}`})
	record(t, caseInput{ID: "S-ID-9/b-same-decision", Contract: "S-ID-9", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "repeating the same decision while unknown is 202 and is not delivered again",
		Steps:       []string{"decide to unknown", "POST the same decision"},
		Request:     map[string]string{"decision": "allow"},
		Expected:    map[string]any{"first": 202, "second": 202, "decideCalls": 1},
		Actual:      map[string]any{"first": first.Status, "second": same.Status, "decideCalls": soB.decides.Load()},
		Pass:        first.Status == 202 && same.Status == 202 && soB.decides.Load() == calls})
	record(t, caseInput{ID: "S-ID-9/b-different-decision", Contract: "S-ID-9", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "a different decision while unknown is 409 APPROVAL_DELIVERY_PENDING",
		Steps:       []string{"POST decide reject while the row is unknown"},
		Request:     map[string]string{"decision": "reject"},
		Expected:    map[string]any{"status": 409, "code": "APPROVAL_DELIVERY_PENDING"},
		Actual:      map[string]any{"status": other.Status, "body": other.Body},
		Pass:        other.Status == 409 && strings.Contains(other.Body, "APPROVAL_DELIVERY_PENDING")})

	soC := &stubOrch{askApproval: true, outcomeErr: errors.New("unreachable")}
	srvC := startServer(t, serverOpts{tenant: "t-sid9c", maxConns: 4, orch: soC, deliveryTimeout: 200 * time.Millisecond, unknownTimeout: 5 * time.Second, reconcileInterval: time.Hour})
	apC, roomC := parkQuiet(t, srvC, "u-sid9c")
	unk := sendTo(t, srvC.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apC + "/decide", Headers: user("u-sid9c"), Body: `{"decision":"allow"}`})
	backdateApproval(t, apC, 4*time.Second, 0)
	still := reconcile(t, srvC)
	mid := approvalTuple(t, apC)
	backdateApproval(t, apC, 6*time.Second, 0)
	later := reconcile(t, srvC)
	end := approvalTuple(t, apC)
	ev := deliveryEvents(t, roomC)
	record(t, caseInput{ID: "S-ID-9/c-unknown-timeout", Contract: "S-ID-9", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "ORBIT_DELIVERY_UNKNOWN_TIMEOUT_S is 5s: inside that window the row stays unknown, past it the row becomes unresolved",
		Steps:       []string{"unknownTimeout is 5s and the workflow query fails", "backdate decided_at by 4s and reconcile", "backdate by 6s and reconcile", "read the delivery event"},
		Request:     map[string]string{"unknownTimeout": "5s"},
		Expected:    map[string]any{"decide": 202, "inside": "decided:allow:unknown:1", "outside": "decided:allow:unresolved:1", "event": true},
		Actual:      map[string]any{"decide": unk.Status, "insideReconcile": still, "inside": mid, "outsideReconcile": later, "outside": end, "events": ev},
		Pass:        unk.Status == 202 && still == 200 && mid == "decided:allow:unknown:1" && later == 200 && end == "decided:allow:unresolved:1" && strings.Contains(ev, "unresolved")})
}

func readSSEUntil(t *testing.T, base, room, u, last string, d time.Duration, needle string) <-chan string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	out := make(chan string, 1)
	go func() {
		defer close(out)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/rooms/"+room+"/events", nil)
		if err != nil {
			return
		}
		req.Header.Set(userHeader, u)
		req.Header.Set("Last-Event-ID", last)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- ""
			return
		}
		defer res.Body.Close()
		var buf bytes.Buffer
		tmp := make([]byte, 1024)
		for {
			n, err := res.Body.Read(tmp)
			if n > 0 {
				buf.Write(tmp[:n])
				if needle == "" || strings.Contains(buf.String(), needle) {
					out <- buf.String()
					cancel()
					return
				}
			}
			if err != nil {
				out <- buf.String()
				return
			}
		}
	}()
	return out
}

func TestSID12Transitions(t *testing.T) {
	const tenant = "t-sid12"
	so := &stubOrch{askApproval: true, resumeText: "t12"}
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: so, reconcileInterval: time.Hour, deliveryTimeout: 250 * time.Millisecond, unknownTimeout: 5 * time.Second})
	u := "u-sid12"
	claimSQL := `
		UPDATE approvals
		   SET status = 'decided', decision = 'allow', decided_at = now(),
		       delivery_state = 'in_flight', delivery_attempt = delivery_attempt + 1
		 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`

	// T1 and T2 through decide.
	useFault(t, "")
	seen := make(chan string, 1)
	ap, _ := parkQuiet(t, srv, u)
	so.onDecide = func() { seen <- approvalTuple(t, ap) }
	res := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	so.onDecide = nil
	var mid string
	select {
	case mid = <-seen:
	case <-time.After(2 * time.Second):
		mid = "timeout"
	}
	end := approvalTuple(t, ap)
	record(t, caseInput{ID: "S-ID-12/t1", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-64"},
		Description: "decide claims the approval into in_flight",
		Steps:       []string{"POST decide", "read the row when the Update is received"},
		Request:     map[string]string{"transition": "T1"},
		Expected:    map[string]string{"during": "decided:allow:in_flight:1"},
		Actual:      map[string]string{"during": mid},
		Pass:        mid == "decided:allow:in_flight:1"})
	record(t, caseInput{ID: "S-ID-12/t2", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-64"},
		Description: "an accepted decide moves in_flight to delivered",
		Steps:       []string{"wait for the decide response", "read the row"},
		Request:     map[string]string{"transition": "T2"},
		Expected:    map[string]any{"status": 200, "state": "decided:allow:delivered:1"},
		Actual:      map[string]any{"status": res.Status, "state": end},
		Pass:        res.Status == 200 && end == "decided:allow:delivered:1"})

	app.SetE2EFault("unknown-always")
	ap3, _ := parkQuiet(t, srv, u)
	u3 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap3 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st3 := approvalTuple(t, ap3)
	record(t, caseInput{ID: "S-ID-12/t3", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-64"},
		Description: "retries that stay Unknown move the row to unknown",
		Steps:       []string{"fault unknown-always", "POST decide"},
		Request:     map[string]string{"transition": "T3"},
		Expected:    map[string]any{"status": 202, "state": "decided:allow:unknown:1"},
		Actual:      map[string]any{"status": u3.Status, "state": st3},
		Pass:        u3.Status == 202 && st3 == "decided:allow:unknown:1"})

	so.outcomes.Delete("ask-orch-1")
	so.pending = true
	beforeAttempt := st3
	rc := reconcile(t, srv)
	st6 := approvalTuple(t, ap3)
	record(t, caseInput{ID: "S-ID-12/t6", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "the reconciler claims one retry from unknown",
		Steps:       []string{"clear the outcome and mark the id pending", "POST /internal/e2e/reconcile", "read the attempt"},
		Request:     map[string]string{"transition": "T6"},
		Expected:    map[string]any{"reconcile": 200, "attemptAdvanced": true},
		Actual:      map[string]any{"before": beforeAttempt, "after": st6, "reconcile": rc},
		Pass:        rc == 200 && strings.HasPrefix(st6, "decided:allow:") && !strings.HasSuffix(st6, ":1")})

	app.SetE2EFault("not-delivered")
	so.ttlS = 86400
	so.pending = false
	ap4, room4 := parkQuiet(t, srv, u)
	nd := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap4 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st4 := approvalTuple(t, ap4)
	ev4 := deliveryEvents(t, room4)
	record(t, caseInput{ID: "S-ID-12/t4", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "NotDelivered emits not_delivered before the row returns to pending",
		Steps:       []string{"fault not-delivered", "POST decide", "read delivery events"},
		Request:     map[string]string{"transition": "T4"},
		Expected:    map[string]any{"status": 503, "sawNotDelivered": true},
		Actual:      map[string]any{"status": nd.Status, "state": st4, "events": ev4},
		Pass:        nd.Status == 503 && strings.Contains(ev4, "not_delivered")})
	record(t, caseInput{ID: "S-ID-12/t9", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "T9 in the same transaction returns the approval to pending",
		Steps:       []string{"read the row after NotDelivered"},
		Request:     map[string]string{"transition": "T9"},
		Expected:    map[string]string{"state": "pending:::1"},
		Actual:      map[string]string{"state": st4},
		Pass:        st4 == "pending:::1"})

	app.SetE2EFault("fatal-on-accept")
	ap5, room5 := parkQuiet(t, srv, u)
	f5 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap5 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st5 := approvalTuple(t, ap5)
	rs5, _, _, _ := roomSnap(t, room5)
	record(t, caseInput{ID: "S-ID-12/t5", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-65"},
		Description: "Fatal on accept moves in_flight to unresolved and fails the room",
		Steps:       []string{"fault fatal-on-accept", "POST decide", "read the approval and the room"},
		Request:     map[string]string{"transition": "T5"},
		Expected:    map[string]any{"status": 409, "state": "decided:allow:unresolved:1", "room": "failed"},
		Actual:      map[string]any{"status": f5.Status, "state": st5, "room": rs5},
		Pass:        f5.Status == 409 && st5 == "decided:allow:unresolved:1" && rs5 == "failed"})

	app.SetE2EFault("unknown-always")
	so.outcomeErr = nil
	so.outcomes.Delete("ask-orch-1")
	ap7, _ := parkQuiet(t, srv, u)
	_ = sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap7 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	so.outcomes.Store("ask-orch-1", orch.DecideOutcome{Decision: "allow", TurnStatus: "completed"})
	so.outcomeErr = nil
	rc7 := reconcile(t, srv)
	st7 := approvalTuple(t, ap7)
	record(t, caseInput{ID: "S-ID-12/t7", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "reconcile moves unknown to delivered when decideOutcome is found",
		Steps:       []string{"leave the row unknown", "store an outcome", "reconcile"},
		Request:     map[string]string{"transition": "T7"},
		Expected:    map[string]any{"reconcile": 200, "state": "decided:allow:delivered:1"},
		Actual:      map[string]any{"reconcile": rc7, "state": st7},
		Pass:        rc7 == 200 && (st7 == "decided:allow:delivered:1" || st7 == "decided:allow:delivered:2")})

	app.SetE2EFault("unknown-always")
	so.outcomes.Delete("ask-orch-1")
	so.outcomeErr = errors.New("down")
	so.pending = false
	ap8, _ := parkQuiet(t, srv, u)
	_ = sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap8 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	backdateApproval(t, ap8, 6*time.Second, 0)
	rc8 := reconcile(t, srv)
	st8 := approvalTuple(t, ap8)
	rs8, _, _, _ := roomSnap(t, roomOf(t, ap8))
	record(t, caseInput{ID: "S-ID-12/t8", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "an unknown row past the 5s unknown timeout becomes unresolved and the room stays unfailed",
		Steps:       []string{"unknownTimeout is 5s", "backdate decided_at by 6s", "reconcile"},
		Request:     map[string]string{"transition": "T8"},
		Expected:    map[string]any{"state": "decided:allow:unresolved:1", "roomNotFailed": true},
		Actual:      map[string]any{"reconcile": rc8, "state": st8, "room": rs8},
		Pass:        rc8 == 200 && st8 == "decided:allow:unresolved:1" && rs8 != "failed"})

	so.outcomeErr = nil
	so.outcomes.Store("ask-orch-1", orch.DecideOutcome{Decision: "allow", TurnStatus: "completed"})
	rc12 := reconcile(t, srv)
	st12 := approvalTuple(t, ap8)
	record(t, caseInput{ID: "S-ID-12/t12", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "an unresolved row that came from T8 becomes delivered when decideOutcome is found later",
		Steps:       []string{"store an outcome", "reconcile the T8 row"},
		Request:     map[string]string{"transition": "T12"},
		Expected:    map[string]any{"reconcile": 200, "state": "decided:allow:delivered:1"},
		Actual:      map[string]any{"reconcile": rc12, "state": st12},
		Pass:        rc12 == 200 && st12 == "decided:allow:delivered:1"})

	app.SetE2EFault("fatal-on-get")
	so.outcomeErr = nil
	ap11, room11 := parkQuiet(t, srv, u)
	b11 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap11 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st11 := approvalTuple(t, ap11)
	rs11, _, _, _ := roomSnap(t, room11)
	record(t, caseInput{ID: "S-ID-12/t11", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-66"},
		Description: "a typed limit error after delivery takes T11",
		Steps:       []string{"fault fatal-on-get", "POST decide"},
		Request:     map[string]string{"transition": "T11"},
		Expected:    map[string]any{"status": 409, "state": "decided:allow:unresolved:1", "room": "failed"},
		Actual:      map[string]any{"status": b11.Status, "state": st11, "room": rs11},
		Pass:        b11.Status == 409 && st11 == "decided:allow:unresolved:1" && rs11 == "failed"})

	// T10: abort cancels pending and leaves in_flight alone.
	app.SetE2EFault("")
	wkRoom := roomID(t, sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}))
	// The room above was created on the orch server; park a message so there is a pending approval, and add an in_flight sibling.
	posted := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms/" + wkRoom + "/messages", Headers: user(u), Body: `{"message":"list files"}`})
	var parked struct {
		Approval *app.Approval `json:"approval"`
	}
	_ = json.Unmarshal([]byte(posted.Body), &parked)
	if parked.Approval == nil {
		t.Fatalf("t10 park: %s", posted.Body)
	}
	pendingID := parked.Approval.ID
	inFlightID := "ap_sid12_inflight"
	if _, err := execAsApp(context.Background(), srv.appPool, tenant, `
		INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision)
		VALUES ($1, $2, $3, 'bash', 'pending', '')`, inFlightID, tenant, wkRoom); err != nil {
		t.Fatal(err)
	}
	if _, err := execAsApp(context.Background(), srv.appPool, tenant, claimSQL, tenant, inFlightID); err != nil {
		t.Fatal(err)
	}
	ab := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms/" + wkRoom + "/abort", Headers: user(u)})
	pendingAfter := approvalTuple(t, pendingID)
	inFlightAfter := approvalTuple(t, inFlightID)
	record(t, caseInput{ID: "S-ID-12/t10-abort", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "abort cancels a pending approval and does not cancel in_flight",
		Steps:       []string{"park one approval and claim another in_flight", "POST /v1/rooms/{id}/abort", "read both rows"},
		Request:     map[string]string{"transition": "T10"},
		Expected:    map[string]any{"abort": 200, "pending": "cancelled:::0", "inFlight": "decided:allow:in_flight:1"},
		Actual:      map[string]any{"abort": ab.Status, "pending": pendingAfter, "inFlight": inFlightAfter},
		Pass:        ab.Status == 200 && pendingAfter == "cancelled:::0" && inFlightAfter == "decided:allow:in_flight:1"})

	// Stale T2: bump the attempt before the delivered update.
	app.SetE2EFault("")
	so.acceptDelay = 0
	var bumped atomic.Bool
	apStale, _ := parkQuiet(t, srv, u)
	pgstore.BeforeDeliveryUpdate = func(transition string) error {
		if transition == "t2" && bumped.CompareAndSwap(false, true) {
			return bumpAttempt(apStale)
		}
		return nil
	}
	staleHTTP := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apStale + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	staleState := approvalTuple(t, apStale)
	record(t, caseInput{ID: "S-ID-12/stale-attempt-t2", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "a T2 whose attempt is already stale affects 0 rows and is not T11",
		Steps:       []string{"bump delivery_attempt before the delivered update", "POST decide", "read the row"},
		Request:     map[string]string{"transition": "T2", "attempt": "stale"},
		Expected:    map[string]any{"not500": true, "staysInFlight": true, "roomNotFailed": true},
		Actual:      map[string]any{"status": staleHTTP.Status, "state": staleState},
		Pass:        staleHTTP.Status != 500 && staleHTTP.Status != 0 && strings.Contains(staleState, ":in_flight:") && !strings.Contains(staleState, "unresolved")})

	// Crash between T4 and commit.
	app.SetE2EFault("not-delivered")
	so.ttlS = 86400
	pgstore.AbortBeforeCommit = func(transition string) error {
		if transition == "t4" {
			return errors.New("injected crash before T4 commit")
		}
		return nil
	}
	apCrash, _ := parkQuiet(t, srv, u)
	crashHTTP := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + apCrash + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	crashed := approvalTuple(t, apCrash)
	pgstore.AbortBeforeCommit = nil
	app.SetE2EFault("")
	backdateApproval(t, apCrash, 0, 2*time.Minute)
	so.outcomes.Store("ask-orch-1", orch.DecideOutcome{Decision: "allow", TurnStatus: "completed"})
	rcCrash := reconcile(t, srv)
	converged := approvalTuple(t, apCrash)
	record(t, caseInput{ID: "S-ID-12/crash-before-t4-commit", Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-75"},
		Description: "a crash after T4 is written and before commit rolls the row back to in_flight; reconcile then converges",
		Steps:       []string{"AbortBeforeCommit on t4", "POST decide", "read in_flight", "backdate and reconcile"},
		Request:     map[string]string{"fault": "crash-before-t4-commit"},
		Expected:    map[string]any{"rolledBack": "decided:allow:in_flight:1", "after": "decided:allow:unknown:1 or delivered"},
		Actual:      map[string]any{"status": crashHTTP.Status, "rolledBack": crashed, "reconcile": rcCrash, "after": converged},
		Pass:        crashed == "decided:allow:in_flight:1" && rcCrash == 200 && (converged == "decided:allow:delivered:1" || converged == "decided:allow:unknown:1")})

	forbidden(t, srv, tenant, "S-ID-12/forbidden-delivered", "delivered cannot move to another delivery_state",
		func(id, room string) {
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, claimSQL, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'delivered'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, id)
		}, `
			UPDATE approvals SET delivery_state = 'unknown'
			 WHERE tenant_id = $1 AND id = $2`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-unresolved", "unresolved cannot move to an unrelated state",
		func(id, room string) {
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, claimSQL, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'unknown'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'unresolved'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unknown' AND delivery_attempt = 1`, tenant, id)
		}, `
			UPDATE approvals SET delivery_state = 'in_flight'
			 WHERE tenant_id = $1 AND id = $2`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-t9-decision", "T9 with a non-empty decision is rejected",
		func(id, room string) {
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, claimSQL, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'not_delivered'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, id)
		}, `
			UPDATE approvals
			   SET status = 'pending', decision = 'allow', decided_at = NULL, delivery_state = NULL
			 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'not_delivered'`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-skip-in-flight", "pending cannot jump straight to delivered",
		func(id, room string) {}, `
			UPDATE approvals SET status = 'decided', decision = 'allow', delivery_state = 'delivered'
			 WHERE tenant_id = $1 AND id = $2 AND status = 'pending'`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-pending-unknown", "pending cannot jump straight to unknown",
		func(id, room string) {}, `
			UPDATE approvals SET delivery_state = 'unknown'
			 WHERE tenant_id = $1 AND id = $2 AND status = 'pending'`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-cancelled-pending", "cancelled cannot return to pending",
		func(id, room string) {
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET status = 'cancelled', decided_at = now()
				 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`, tenant, id)
		}, `
			UPDATE approvals SET status = 'pending', decision = '', decided_at = NULL
			 WHERE tenant_id = $1 AND id = $2`)
	forbidden(t, srv, tenant, "S-ID-12/forbidden-t12-after-t11", "unresolved on a failed room cannot take T12",
		func(id, room string) {
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, claimSQL, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'delivered'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE approvals SET delivery_state = 'unresolved'
				 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'delivered' AND delivery_attempt = 1`, tenant, id)
			_, _ = execAsApp(context.Background(), srv.appPool, tenant, `
				UPDATE rooms SET state = 'failed', failure = '{"code":"DECIDED_APPROVALS_LIMIT","message":"limit"}'::jsonb, updated_at = now()
				 WHERE tenant_id = $1 AND id = $2`, tenant, room)
		}, `
			UPDATE approvals SET delivery_state = 'delivered'
			 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unresolved'`)
}

func roomOf(t *testing.T, approvalID string) string {
	t.Helper()
	var id string
	if err := ownerConn(t).QueryRow(context.Background(), `SELECT task_id FROM approvals WHERE id = $1`, approvalID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func forbidden(t *testing.T, srv *server, tenant, id, desc string, setup func(approvalID, room string), sql string) {
	t.Helper()
	room := roomID(t, sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-sid12"), Body: `{"kind":"solo"}`}))
	ap := id
	if _, err := execAsApp(context.Background(), srv.appPool, tenant, `
		INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision)
		VALUES ($1, $2, $3, 'bash', 'pending', '')`, ap, tenant, room); err != nil {
		t.Fatal(err)
	}
	setup(ap, room)
	before := approvalTuple(t, ap)
	n, err := execAsApp(context.Background(), srv.appPool, tenant, sql, tenant, ap)
	after := approvalTuple(t, ap)
	got := sqlState(err) + ":" + pgMessage(err)
	record(t, caseInput{ID: id, Contract: "S-ID-12", Kind: "e2e", FailureModes: []string{"FM-64"},
		Description: desc,
		Steps:       []string{"as orbit_app, attempt the forbidden update", "read the row"},
		Request:     sqlReq{Role: "orbit_app", Tenant: tenant, SQL: strings.Join(strings.Fields(sql), " ")},
		Expected:    map[string]any{"sqlstate": "P0001", "message": "approval is not pending", "unchanged": true},
		Actual:      map[string]any{"rows": n, "error": got, "before": before, "after": after},
		Pass:        sqlState(err) == "P0001" && pgMessage(err) == "approval is not pending" && before == after})
}

func TestSID18ZeroRows(t *testing.T) {
	const tenant = "t-sid18"
	so := &stubOrch{askApproval: true, resumeText: "zero"}
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: so, reconcileInterval: time.Hour, deliveryTimeout: 300 * time.Millisecond})
	u := "u-sid18"

	lose := func(transition, approvalID string) {
		pgstore.BeforeDeliveryUpdate = func(name string) error {
			if name == transition {
				pgstore.BeforeDeliveryUpdate = nil
				return bumpAttempt(approvalID)
			}
			return nil
		}
	}

	useFault(t, "")
	ap2, room2 := parkQuiet(t, srv, u)
	lose("t2", ap2)
	body2 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap2 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st2 := approvalTuple(t, ap2)
	rs2, _, _, _ := roomSnap(t, room2)
	pass2 := body2.Status != 500 && body2.Status == 202 && strings.Contains(st2, ":in_flight:") && rs2 != "failed"
	record(t, caseInput{ID: "S-ID-18/t2", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "T2 affecting 0 rows re-reads in_flight and returns 202, never 500",
		Steps:       []string{"bump the attempt before T2", "POST decide", "assert the HTTP status"},
		Request:     map[string]string{"transition": "t2"},
		Expected:    map[string]any{"status": 202, "not500": true, "roomNotFailed": true},
		Actual:      map[string]any{"status": body2.Status, "state": st2, "room": rs2},
		Pass:        pass2})

	app.SetE2EFault("unknown-always")
	ap3, room3 := parkQuiet(t, srv, u)
	lose("t3", ap3)
	body3 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap3 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st3 := approvalTuple(t, ap3)
	rs3, _, _, _ := roomSnap(t, room3)
	record(t, caseInput{ID: "S-ID-18/t3", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "T3 affecting 0 rows re-reads the row and does not return 500",
		Steps:       []string{"bump the attempt before T3", "POST decide"},
		Request:     map[string]string{"transition": "t3"},
		Expected:    map[string]any{"not500": true, "status": 202},
		Actual:      map[string]any{"status": body3.Status, "state": st3, "room": rs3},
		Pass:        body3.Status == 202 && body3.Status != 500 && strings.Contains(st3, ":in_flight:") && rs3 != "failed"})

	app.SetE2EFault("fatal-on-accept")
	ap5, room5 := parkQuiet(t, srv, u)
	lose("t5", ap5)
	body5 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap5 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st5 := approvalTuple(t, ap5)
	rs5, _, _, _ := roomSnap(t, room5)
	events5 := deliveryEvents(t, room5)
	zero5 := body5.Status != 500 && rs5 != "failed" && !strings.Contains(events5, "room.failed") && strings.Contains(st5, ":in_flight:")
	record(t, caseInput{ID: "S-ID-18/t5", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "T5 affecting 0 rows answers from the re-read row and does not return 500",
		Steps:       []string{"bump the attempt before T5", "POST decide", "read HTTP status"},
		Request:     map[string]string{"transition": "t5"},
		Expected:    map[string]any{"not500": true, "status": 202},
		Actual:      map[string]any{"status": body5.Status, "state": st5, "room": rs5},
		Pass:        zero5 && body5.Status == 202})
	record(t, caseInput{ID: "S-ID-18/t5-zero-rows", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-65"},
		Description: "T5 affecting 0 rows rolls back the whole transaction, so the room is not failed",
		Steps:       []string{"read room state and confirm there is no room.failed event"},
		Request:     map[string]string{"transition": "t5", "rows": "0"},
		Expected:    map[string]any{"roomNotFailed": true, "noRoomFailedEvent": true},
		Actual:      map[string]any{"room": rs5, "events": events5, "state": st5},
		Pass:        zero5})

	app.SetE2EFault("fatal-on-get")
	ap11, room11 := parkQuiet(t, srv, u)
	lose("t11", ap11)
	body11 := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap11 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	st11 := approvalTuple(t, ap11)
	rs11, _, _, _ := roomSnap(t, room11)
	zero11 := body11.Status == 200 && body11.Status != 500 && strings.Contains(st11, ":delivered:") && rs11 != "failed"
	record(t, caseInput{ID: "S-ID-18/t11", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-68"},
		Description: "T11 affecting 0 rows re-reads delivered and returns 200, never 500",
		Steps:       []string{"bump the attempt before T11", "POST decide"},
		Request:     map[string]string{"transition": "t11"},
		Expected:    map[string]any{"status": 200, "not500": true},
		Actual:      map[string]any{"status": body11.Status, "state": st11, "room": rs11},
		Pass:        zero11})
	record(t, caseInput{ID: "S-ID-18/t11-zero-rows", Contract: "S-ID-18", Kind: "e2e", FailureModes: []string{"FM-65"},
		Description: "T11 affecting 0 rows does not mark the room failed",
		Steps:       []string{"read the room after the lost T11"},
		Request:     map[string]string{"transition": "t11", "rows": "0"},
		Expected:    map[string]any{"roomNotFailed": true, "delivered": true},
		Actual:      map[string]any{"room": rs11, "state": st11},
		Pass:        zero11})
}

func TestFM69TTLWindow(t *testing.T) {
	useFault(t, "not-delivered")
	so := &stubOrch{askApproval: true, ttlS: 86400}
	srv := startServer(t, serverOpts{tenant: "t-fm69", maxConns: 4, orch: so, reconcileInterval: time.Hour})
	u := "u-fm69"
	ap, _ := parkQuiet(t, srv, u)
	inside := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	inState := approvalTuple(t, ap)
	record(t, caseInput{ID: "FM-69/inside-window", Contract: "FM-69", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "APPROVAL_UNKNOWN inside ttlS/2 is NotDelivered even though the error text says NOT_DELIVERED",
		Steps:       []string{"ttlS is 86400", "fault returns ApplicationError type APPROVAL_UNKNOWN message NOT_DELIVERED"},
		Request:     map[string]string{"ttlS": "86400"},
		Expected:    map[string]any{"status": 503, "state": "pending:::1"},
		Actual:      map[string]any{"status": inside.Status, "state": inState},
		Pass:        inside.Status == 503 && inState == "pending:::1"})

	so.configErr = errors.New("decideConfig failed")
	ap2, _ := parkQuiet(t, srv, u)
	failed := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap2 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	failState := approvalTuple(t, ap2)
	record(t, caseInput{ID: "FM-69/config-query-fails", Contract: "FM-69", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "a failed decideConfig query is Unknown, not NotDelivered, despite the NOT_DELIVERED text",
		Steps:       []string{"decideConfig returns an error", "POST decide"},
		Request:     map[string]string{"decideConfig": "error"},
		Expected:    map[string]any{"status": 202, "state": "decided:allow:unknown:1"},
		Actual:      map[string]any{"status": failed.Status, "state": failState},
		Pass:        failed.Status == 202 && failState == "decided:allow:unknown:1"})

	so.configErr = nil
	so.ttlS = 60
	ap3, _ := parkQuiet(t, srv, u)
	app.E2EBeforeClassify = func() { backdateApproval(t, ap3, 31*time.Second, 0) }
	outside := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap3 + "/decide", Headers: user(u), Body: `{"decision":"allow"}`})
	outState := approvalTuple(t, ap3)
	record(t, caseInput{ID: "FM-69/outside-window", Contract: "FM-69", Kind: "e2e", FailureModes: []string{"FM-69"},
		Description: "APPROVAL_UNKNOWN outside ttlS/2 is Unknown",
		Steps:       []string{"ttlS is 60", "backdate decided_at by 31s before classify", "POST decide"},
		Request:     map[string]string{"ttlS": "60"},
		Expected:    map[string]any{"status": 202, "state": "decided:allow:unknown:1"},
		Actual:      map[string]any{"status": outside.Status, "state": outState},
		Pass:        outside.Status == 202 && outState == "decided:allow:unknown:1"})
}

func TestFM70Messages(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, contractFile))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	codes := []string{"timeout", "rate_limited", "provider_error", "auth", "config", "state_unreadable", "DECIDED_APPROVALS_LIMIT"}
	got := map[string]string{}
	ok := true
	for _, code := range codes {
		re := regexp.MustCompile(`\| ` + "`" + regexp.QuoteMeta(code) + "`" + ` \| ([^|]+) \|`)
		m := re.FindStringSubmatch(text)
		cell := ""
		if m != nil {
			cell = strings.TrimSpace(m[1])
		}
		msg, found := failtext.Message(code)
		got[code] = msg
		if !found || cell == "" || msg != cell {
			ok = false
		}
	}
	record(t, caseInput{ID: "FM-70/messages-match-section-10-2", Contract: "FM-70", Kind: "e2e", FailureModes: []string{"FM-70"},
		Description: "failtext constants equal the §10.2 cells in the shipped contract",
		Steps:       []string{"parse the §10.2 table", "compare each cell with failtext.Message"},
		Request:     map[string]string{"file": contractFile},
		Expected:    map[string]any{"match": true},
		Actual:      map[string]any{"messages": got, "match": ok},
		Pass:        ok})
}

func TestFM71Restart(t *testing.T) {
	const tenant = "t-fm71"
	const other = "t-fm71-b"
	so := &stubOrch{askApproval: true, resumeText: "restarted-result"}
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: so, reconcileInterval: time.Hour})
	srv.runtime.SetSkipResultWrite(true)
	ap, _ := parkQuiet(t, srv, "u-fm71")
	dec := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/approvals/" + ap + "/decide", Headers: user("u-fm71"), Body: `{"decision":"allow"}`})
	if dec.Status != 200 {
		t.Fatalf("decide: %d %s", dec.Status, dec.Body)
	}
	srv.runtime.StopReconcile()
	owner := ownerConn(t)
	var result *int
	var hasBody bool
	var msgs int
	if err := owner.QueryRow(context.Background(), `
		SELECT a.result_attempt, a.result_body IS NOT NULL,
		       (SELECT count(*) FROM messages m WHERE m.task_id = a.task_id AND m.text = 'restarted-result')
		  FROM approvals a WHERE a.id = $1`, ap).Scan(&result, &hasBody, &msgs); err != nil {
		t.Fatal(err)
	}
	if result != nil || !hasBody || msgs != 0 {
		t.Fatalf("before restart result=%v body=%v msgs=%d", result, hasBody, msgs)
	}

	opsEnsureTenant(t, other)
	body := `{"approvalId":"ap_fm71_b","attempt":1,"roomId":"rm_fm71_b","roomState":"running","updateRoom":true,"texts":["tenant-b-result"]}`
	ctx := context.Background()
	if _, err := owner.Exec(ctx, `INSERT INTO users (id, tenant_id, iss, sub) VALUES ('u-fm71-b', $1, 'e2e', 'u-fm71-b') ON CONFLICT (id) DO NOTHING`, other); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, `
		INSERT INTO rooms (id, tenant_id, created_by, kind, state, permission_preset)
		VALUES ('rm_fm71_b', $1, 'u-fm71-b', 'solo', 'running', 'workspace-write')`, other); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, `
		INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision, delivery_state, delivery_attempt, decided_at, result_body)
		VALUES ('ap_fm71_b', $1, 'rm_fm71_b', 'bash', 'decided', 'allow', 'delivered', 1, now(), $2::jsonb)`, other, body); err != nil {
		t.Fatal(err)
	}

	wk := stubWorker(t, nil)
	vars := []envVar{
		appDBVar(),
		{Name: "ORBIT_DEFAULT_TENANT", Value: tenant},
		{Name: "ORBIT_DELIVERY_RECONCILE_INTERVAL_S", Value: "1"},
		{Name: "ORBIT_WORKER_URL", Value: wk.URL, Display: "<stub worker>"},
		{Name: "ORBIT_DATA_DIR", Value: t.TempDir(), Display: "<temp dir>"},
		{Name: "PORT", Value: freePort(t)},
	}
	proc := startBinary(t, vars)
	_ = proc.waitHealthy(15 * time.Second)
	deadline := time.Now().Add(20 * time.Second)
	var aResult, bResult int
	var aMsgs, bMsgs int
	for {
		_ = owner.QueryRow(ctx, `
			SELECT COALESCE(result_attempt, 0),
			       (SELECT count(*) FROM messages m WHERE m.task_id = a.task_id AND m.text = 'restarted-result')
			  FROM approvals a WHERE a.id = $1`, ap).Scan(&aResult, &aMsgs)
		_ = owner.QueryRow(ctx, `
			SELECT COALESCE(result_attempt, 0),
			       (SELECT count(*) FROM messages m WHERE m.task_id = 'rm_fm71_b' AND m.text = 'tenant-b-result')
			  FROM approvals WHERE id = 'ap_fm71_b'`).Scan(&bResult, &bMsgs)
		if aResult > 0 && aMsgs == 1 && bResult == 1 && bMsgs == 1 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	record(t, caseInput{ID: "FM-71/restart-reconcile-from-database", Contract: "FM-71", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "after control restarts, the reconciler applies result_body; the assertion reads only the database",
		Steps:       []string{"decide with the result write skipped", "stop that process", "start orbit-control with ORBIT_DELIVERY_RECONCILE_INTERVAL_S=1", "read result_attempt and messages"},
		Request:     procReq{Binary: "orbit-control", Env: []string{"ORBIT_DELIVERY_RECONCILE_INTERVAL_S=1"}, Probe: "SELECT result_attempt, messages"},
		Expected:    map[string]any{"resultWritten": true, "messages": 1},
		Actual:      map[string]any{"resultAttempt": aResult, "messages": aMsgs},
		Pass:        aResult > 0 && aMsgs == 1})
	record(t, caseInput{ID: "FM-71/all-tenants", Contract: "FM-71", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "the same restarted reconciler applies a delivered result_body for a second tenant",
		Steps:       []string{"seed another tenant's delivered row", "read that row after the reconciler runs"},
		Request:     map[string]string{"tenant": other},
		Expected:    map[string]any{"resultAttempt": 1, "messages": 1},
		Actual:      map[string]any{"resultAttempt": bResult, "messages": bMsgs},
		Pass:        bResult == 1 && bMsgs == 1})
}

func TestFM72BadRequest(t *testing.T) {
	srv := startServer(t, serverOpts{tenant: "t-fm72", maxConns: 2, workerURL: stubWorker(t, nil).URL, reconcileInterval: time.Hour})
	act := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-fm72"), Body: `{"kind":"nope"}`})
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	runbook, err := os.ReadFile(filepath.Join(root, "docs/runbooks/stuck-room.md"))
	if err != nil {
		t.Fatal(err)
	}
	hides := !strings.Contains(act.Body, "kind must be solo or collab") && strings.Contains(act.Body, "the request is invalid")
	runbookText := strings.ToLower(string(runbook))
	record(t, caseInput{ID: "FM-72/bad-request-hides-cause", Contract: "FM-72", Kind: "e2e", FailureModes: []string{"FM-72"},
		Description: "a 400 body uses the fixed message and the stuck-room runbook tells operators not to expect err.Error()",
		Steps:       []string{"POST /v1/rooms with kind nope", "read the body", "read docs/runbooks/stuck-room.md"},
		Request:     httpReq{Method: "POST", Path: "/v1/rooms", Body: `{"kind":"nope"}`},
		Expected:    map[string]any{"status": 400, "message": "the request is invalid", "hidesCause": true, "runbook": true},
		Actual:      map[string]any{"status": act.Status, "body": act.Body, "runbook": strings.Contains(runbookText, "err.error()")},
		Pass:        act.Status == 400 && hides && strings.Contains(runbookText, "stuck") && strings.Contains(runbookText, "err.error()")})
}

func TestFM73Checks(t *testing.T) {
	const tenant = "t-fm73"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 2, workerURL: stubWorker(t, nil).URL, reconcileInterval: time.Hour})
	room := roomID(t, sendTo(t, srv.base, httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-fm73"), Body: `{"kind":"solo"}`}))
	_, statusErr := execAsApp(context.Background(), srv.appPool, tenant, `
		INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision)
		VALUES ('ap_fm73_status', $1, $2, 'bash', 'allowed', '')`, tenant, room)
	_, decisionErr := execAsApp(context.Background(), srv.appPool, tenant, `
		INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision)
		VALUES ('ap_fm73_decision', $1, $2, 'bash', 'pending', 'yes')`, tenant, room)
	record(t, caseInput{ID: "FM-73/status-check", Contract: "FM-73", Kind: "e2e", FailureModes: []string{"FM-73"},
		Description: "approvals.status rejects a value outside pending, decided, and cancelled",
		Steps:       []string{"INSERT status allowed as orbit_app"},
		Request:     sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "INSERT approvals status allowed"},
		Expected:    map[string]string{"sqlstate": "23514"},
		Actual:      map[string]string{"sqlstate": sqlState(statusErr), "message": pgMessage(statusErr)},
		Pass:        sqlState(statusErr) == "23514"})
	record(t, caseInput{ID: "FM-73/decision-check", Contract: "FM-73", Kind: "e2e", FailureModes: []string{"FM-73"},
		Description: "approvals.decision rejects a value outside empty, allow, and reject",
		Steps:       []string{"INSERT decision yes as orbit_app"},
		Request:     sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "INSERT approvals decision yes"},
		Expected:    map[string]string{"sqlstate": "23514"},
		Actual:      map[string]string{"sqlstate": sqlState(decisionErr), "message": pgMessage(decisionErr)},
		Pass:        sqlState(decisionErr) == "23514"})
}

func TestFM74TriggerPrivilege(t *testing.T) {
	const tenant = "t-fm74"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 2, workerURL: stubWorker(t, nil).URL, reconcileInterval: time.Hour})
	_, disErr := execAsApp(context.Background(), srv.appPool, tenant, `ALTER TABLE approvals DISABLE TRIGGER orbit_approvals_delivery_transition`)
	_, dropErr := execAsApp(context.Background(), srv.appPool, tenant, `DROP TRIGGER orbit_approvals_delivery_transition ON approvals`)
	var left int
	if err := ownerConn(t).QueryRow(context.Background(), `SELECT count(*) FROM pg_trigger WHERE tgname = 'orbit_approvals_delivery_transition' AND NOT tgisinternal`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	record(t, caseInput{ID: "FM-74/disable-trigger", Contract: "FM-74", Kind: "e2e", FailureModes: []string{"FM-74"},
		Description: "orbit_app cannot DISABLE the delivery trigger",
		Steps:       []string{"ALTER TABLE approvals DISABLE TRIGGER as orbit_app"},
		Request:     sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "ALTER TABLE approvals DISABLE TRIGGER orbit_approvals_delivery_transition"},
		Expected:    map[string]any{"sqlstate": "42501", "triggerRemains": true},
		Actual:      map[string]any{"sqlstate": sqlState(disErr), "message": pgMessage(disErr), "triggers": left},
		Pass:        sqlState(disErr) == "42501" && left == 1})
	record(t, caseInput{ID: "FM-74/drop-trigger", Contract: "FM-74", Kind: "e2e", FailureModes: []string{"FM-74"},
		Description: "orbit_app cannot DROP the delivery trigger",
		Steps:       []string{"DROP TRIGGER as orbit_app", "count the trigger as orbit_owner"},
		Request:     sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "DROP TRIGGER orbit_approvals_delivery_transition ON approvals"},
		Expected:    map[string]any{"sqlstate": "42501", "triggerRemains": 1},
		Actual:      map[string]any{"sqlstate": sqlState(dropErr), "message": pgMessage(dropErr), "triggers": left},
		Pass:        sqlState(dropErr) == "42501" && left == 1})
}

func TestFM71LeftoverNotDelivered(t *testing.T) {
	const tenant = "t-fm71-nd"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: &stubOrch{askApproval: true}, reconcileInterval: time.Hour})
	ap, room := parkQuiet(t, srv, "u-fm71-nd")
	ctx := context.Background()
	if _, err := execAsApp(ctx, srv.appPool, tenant, `
		UPDATE approvals
		   SET status = 'decided', decision = 'allow', decided_at = now(),
		       delivery_state = 'in_flight', delivery_attempt = delivery_attempt + 1
		 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`, tenant, ap); err != nil {
		t.Fatal(err)
	}
	if _, err := execAsApp(ctx, srv.appPool, tenant, `
		UPDATE approvals SET delivery_state = 'not_delivered'
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, ap); err != nil {
		t.Fatal(err)
	}
	before := approvalTuple(t, ap)
	beforeEvents := reopenEvents(t, room)
	code := reconcile(t, srv)
	after := approvalTuple(t, ap)
	var decidedNull bool
	if err := ownerConn(t).QueryRow(ctx, `SELECT decided_at IS NULL FROM approvals WHERE id = $1`, ap).Scan(&decidedNull); err != nil {
		t.Fatal(err)
	}
	events := reopenEvents(t, room)
	record(t, caseInput{ID: "FM-71/leftover-not-delivered-t9", Contract: "FM-71", Kind: "e2e", FailureModes: []string{"FM-71"},
		Description: "the reconciler scans a leftover not_delivered row and runs T9",
		Steps:       []string{"leave the row at not_delivered without T9", "POST /internal/e2e/reconcile", "read the row and the reopen event"},
		Request:     map[string]string{"approval": ap},
		Expected:    map[string]any{"before": "decided:allow:not_delivered:1", "after": "pending:::1", "decidedAtNull": true, "reopenEvents": 1},
		Actual:      map[string]any{"before": before, "beforeEvents": beforeEvents, "reconcile": code, "after": after, "decidedAtNull": decidedNull, "reopenEvents": events},
		Pass:        before == "decided:allow:not_delivered:1" && beforeEvents == 0 && code == 200 && after == "pending:::1" && decidedNull && events == 1})
}

func reopenEvents(t *testing.T, room string) int {
	t.Helper()
	var n int
	err := ownerConn(t).QueryRow(context.Background(), `
		SELECT count(*) FROM events
		 WHERE task_id = $1 AND type = 'approval.delivery_updated'
		   AND payload->>'status' = 'pending'
		   AND COALESCE(payload->>'decision', '') = ''
		   AND COALESCE(payload->>'deliveryState', '') = ''`, room).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFM76BoundedRetry(t *testing.T) {
	const timeout = 450 * time.Millisecond
	so := &stubOrch{askApproval: true, acceptErr: errors.New("unavailable")}
	srv := startServer(t, serverOpts{tenant: "t-fm76", maxConns: 4, orch: so, deliveryTimeout: timeout, reconcileInterval: time.Hour})
	ap, _ := parkQuiet(t, srv, "u-fm76")
	ctx, cancel := context.WithTimeout(context.Background(), timeout+3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.base+"/v1/approvals/"+ap+"/decide", strings.NewReader(`{"decision":"allow"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(userHeader, "u-fm76")
	start := time.Now()
	res, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	state := approvalTuple(t, ap)
	calls := so.decides.Load()
	within := elapsed <= timeout+time.Second
	record(t, caseInput{ID: "FM-76/retry-bounded-by-delivery-timeout", Contract: "FM-76", Kind: "e2e", FailureModes: []string{"FM-76"},
		Description: "in-request Unknown retries use capped backoff and finish within ORBIT_DECISION_DELIVERY_TIMEOUT",
		Steps:       []string{"DeliveryTimeout is 450ms", "each accept fails at once", "POST decide allow", "count accepts and measure elapsed time"},
		Request:     map[string]any{"decision": "allow", "deliveryTimeoutMs": timeout.Milliseconds()},
		Expected:    map[string]any{"status": 202, "stateHasUnknown": true, "decideCallsMin": 2, "decideCallsMax": 40, "withinTimeout": true},
		Actual:      map[string]any{"status": res.StatusCode, "body": string(raw), "state": state, "decideCalls": calls, "elapsedMs": elapsed.Milliseconds()},
		Pass:        res.StatusCode == 202 && strings.Contains(state, ":unknown:") && calls >= 2 && calls <= 40 && within})
}

func TestFM75ProductionBinary(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	e2eBin := filepath.Join(t.TempDir(), "orbit-control-e2e")
	cmd := exec.Command("go", "build", "-tags", "e2e", "-o", e2eBin, "./cmd/orbit-control")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("e2e build: %v %s", err, out)
	}
	prodFault := nmContains(t, binaryPath, "orbitE2EFaults")
	e2eFault := nmContains(t, e2eBin, "orbitE2EFaults")
	record(t, caseInput{ID: "FM-75/production-binary-has-no-fault-symbol", Contract: "FM-75", Kind: "process", FailureModes: []string{"FM-75"},
		Description: "the production binary has no orbitE2EFaults symbol; the e2e-tagged binary does",
		Steps:       []string{"go tool nm the production binary", "go build -tags e2e", "go tool nm that binary"},
		Request:     map[string]string{"symbol": "orbitE2EFaults"},
		Expected:    map[string]any{"production": false, "e2e": true},
		Actual:      map[string]any{"production": prodFault, "e2e": e2eFault},
		Pass:        !prodFault && e2eFault})
	prodAgain := nmContains(t, binaryPath, "WriteResultAgain")
	e2eAgain := nmContains(t, e2eBin, "WriteResultAgain")
	record(t, caseInput{ID: "FM-75/production-binary-has-no-write-result-again", Contract: "FM-75", Kind: "process", FailureModes: []string{"FM-75"},
		Description: "WriteResultAgain is absent from the production binary and present in the e2e-tagged binary",
		Steps:       []string{"go tool nm the production binary for WriteResultAgain", "go tool nm the e2e binary"},
		Request:     map[string]string{"symbol": "WriteResultAgain"},
		Expected:    map[string]any{"production": false, "e2e": true},
		Actual:      map[string]any{"production": prodAgain, "e2e": e2eAgain},
		Pass:        !prodAgain && e2eAgain})
}

func nmContains(t *testing.T, bin, sym string) bool {
	t.Helper()
	out, err := exec.Command("go", "tool", "nm", bin).CombinedOutput()
	if err != nil && !bytes.Contains(out, []byte(sym)) {
		return false
	}
	return bytes.Contains(out, []byte(sym))
}

func TestC35BlockedRuntime(t *testing.T) {
	cases := []struct{ id, contract string }{
		{"S-ID-2/repeat-delivery", "S-ID-2"},
		{"S-ID-7/continue-as-new", "S-ID-7"},
		{"S-ID-8/runtime-carry-over", "S-ID-8"},
		{"S-ID-10/ttl-cleanup", "S-ID-10"},
		{"S-ID-11/child-resolve", "S-ID-11"},
		{"S-ID-14/child-limit", "S-ID-14"},
		{"S-ID-15/shared-limit", "S-ID-15"},
		{"S-ID-16/fatal-before-end", "S-ID-16"},
		{"S-ID-17/room-failed-once", "S-ID-17"},
		{"C3/real-temporal-decide", "C3"},
		{"C3/real-temporal-fm60-disconnect", "C3"},
	}
	for _, c := range cases {
		blocked(t, c.id, c.contract, "e2e",
			c.id+" belongs to the orbit-runtime section 2.4 work",
			runtimeBlockedBy,
			[]string{"not run in this process"},
			map[string]string{"status": "blocked"})
	}
}
