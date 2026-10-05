//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

const approvalsContract = "orbit-infra 09 §3, 03 §7 (migration 00023)"

type approvalRow struct {
	Status, Comment               string
	NodeID, AttemptID, ToolCallID string
	Always                        bool
	Decided                       bool
	Version                       int64
	Subject                       string
}

func readApprovals(t *testing.T, tenant, taskID string) map[string]approvalRow {
	t.Helper()
	owner := newPool(t, ownerURL, 1)
	rows, err := owner.Query(context.Background(),
		`SELECT approval_id, status, comment, always, decided_at IS NOT NULL, entity_version, subject::text, coalesce(node_id,''), coalesce(attempt_id,''), coalesce(tool_call_id,'')
		   FROM task_approvals WHERE tenant_id = $1 AND task_id = $2`, tenant, taskID)
	if err != nil {
		t.Fatalf("read task_approvals: %v", err)
	}
	defer rows.Close()
	out := map[string]approvalRow{}
	for rows.Next() {
		var id string
		var r approvalRow
		if err := rows.Scan(&id, &r.Status, &r.Comment, &r.Always, &r.Decided, &r.Version, &r.Subject, &r.NodeID, &r.AttemptID, &r.ToolCallID); err != nil {
			t.Fatalf("scan task_approvals: %v", err)
		}
		out[id] = r
	}
	return out
}

// TestTaskApprovalsProjection: approval events reach task_approvals through runtime_outbox, replays and late events do
// not move an approval backwards, and an event the projection cannot hold does not stop the ones behind it.
func TestTaskApprovalsProjection(t *testing.T) {
	const (
		tenant   = "t-task-approvals"
		testUser = "u-task-approvals"
	)
	ctx := context.Background()
	// The store logs through the standard logger; keep what it says about skipped projections.
	skipLog := &syncBuffer{}
	log.SetOutput(skipLog)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	first := startServer(t, serverOpts{tenant: tenant, projector: true, maxConns: 6})
	principal := userCSRF(testUser)
	created := first.check(t, "TASK-APPROVALS/create", approvalsContract, "create a v3 task", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: principal, Body: `{"title":"approvals","goal":"project approvals"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"task_id"`}})
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &task); err != nil || task.ID == "" {
		t.Fatalf("decode created task: %v %s", err, created.Body)
	}
	worker := newPool(t, workerURL, 2)
	n := 0
	outbox := func(eventType, entityKind, entityID string, version int, payload string) {
		t.Helper()
		n++
		id := fmt.Sprintf("evt_01TASKAPPROVALS%08d", n)
		body := fmt.Sprintf(`{"schema":"orbit.event/3","event_id":%q,"tenant_id":%q,"task_id":%q,"type":%q,"source":{"kind":"workflow","id":"w"},"entity":{"kind":%q,"id":%q,"version":%d},"retention":"durable","occurred_at":"2026-10-05T00:00:%02dZ","payload":%s}`,
			id, tenant, task.ID, eventType, entityKind, entityID, version, n, payload)
		if _, err := execAsApp(ctx, worker, tenant,
			`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`, tenant, task.ID, id, body); err != nil {
			t.Fatalf("append %s to runtime_outbox: %v", eventType, err)
		}
	}
	requested := func(id, attempt, call, subject string) {
		outbox("approval.requested", "approval", id, 1, fmt.Sprintf(
			`{"approval_id":%q,"attempt_id":%q,"node_id":"n_1","tool_call_id":%q,"subject":%s}`, id, attempt, call, subject))
	}
	decided := func(id string, version int, status, comment string, always bool) {
		outbox("approval.decided", "approval", id, version, fmt.Sprintf(
			`{"approval_id":%q,"status":%q,"comment":%q,"always":%t}`, id, status, comment, always))
	}

	requested("apr_A", "att_1", "tc1", `{"kind":"tool_call","summary":"run ls","risk":"low"}`)
	requested("apr_B", "att_1", "tc2", `{"kind":"tool_call","summary":"rm -rf"}`)
	requested("apr_C", "att_2", "tc3", `null`)
	decided("apr_A", 2, "APPROVED", "go ahead", true)
	decided("apr_B", 2, "CANCELLED", "the attempt ended before the approval was decided", false)
	// FM-94: what the new runtime emits, and what the projection cannot hold, in front of the last event.
	outbox("approval.requested", "approval", "bad", 1, `{"approval_id":"bad","subject":{}}`)
	decided("apr_C", 2, "WEIRD", "an unknown status is skipped", false)
	outbox("node.status_changed", "task", task.ID, 1, `{"node_id":"n_1","from_status":"RUNNING","to_status":"BLOCKED","reason":"exploration budget spent","new_field":{"a":1}}`)
	outbox("node.status_changed", "task", task.ID, 2, `{"node_id":"n_2","from_status":"RUNNING","to_status":"RETRY_PENDING","reason":"verification failed"}`)
	for _, class := range []string{"transient", "model", "tool", "policy", "budget", "verification", "lost"} {
		outbox("attempt.finished", "attempt", "att_"+class, 1,
			`{"node_id":"n_1","attempt_id":"att_`+class+`","outcome":"failed","failure":{"failure_class":"`+class+`","retryable":true,"message":"m","extra":1}}`)
	}
	outbox("plan.version_committed", "plan", task.ID, 1, `{"plan_version":2,"parent_version":1,"hash":"h","command_id":"c","actor":{"kind":"system","id":"task-workflow"},"reason":"compaction","archived":{"count":3,"hash":"h","added":3}}`)
	outbox("task.status_changed", "task", task.ID, 3, `{"from_status":"RUNNING","to_status":"NOT_A_STATUS","reason":"x"}`)
	outbox("task.status_changed", "task", task.ID, 4, `{"from_status":"RUNNING","to_status":"PAUSED_NEEDS_REVIEW","reason":"the agent declared the task unplannable"}`)
	waitForProjection(t, first, task.ID, principal, `"status":"PAUSED_NEEDS_REVIEW"`)

	rows := readApprovals(t, tenant, task.ID)
	a, b, c := rows["apr_A"], rows["apr_B"], rows["apr_C"]
	record(t, caseInput{ID: "TASK-APPROVALS/requested-and-decided", Contract: approvalsContract, Kind: "e2e", FailureModes: []string{"FM-92"},
		Description: "approval.requested inserts a PENDING row with its subject; approval.decided sets status, comment, always and decided_at",
		Request:     map[string]string{"events": "requested x3, decided APPROVED, decided CANCELLED"},
		Expected:    map[string]any{"rows": 3, "A": "APPROVED go ahead always", "B": "CANCELLED", "C": "PENDING subject {}"},
		Actual:      map[string]any{"rows": len(rows), "A": fmt.Sprint(a.Status, " ", a.Comment, " ", a.Always, " ", a.Decided), "B": fmt.Sprint(b.Status, " ", b.Decided), "C": fmt.Sprint(c.Status, " ", c.Subject)},
		Pass: len(rows) == 3 && a.Status == "APPROVED" && a.Comment == "go ahead" && a.Always && a.Decided && a.Version == 2 &&
			strings.Contains(a.Subject, `"summary": "run ls"`) &&
			b.Status == "CANCELLED" && !b.Always && b.Decided &&
			c.Status == "PENDING" && !c.Decided && c.Subject == "{}"})

	var pendingIDs []string
	for id, r := range rows {
		if r.Status == "PENDING" {
			pendingIDs = append(pendingIDs, id)
		}
	}
	planVersion := ownerScalar[int](t, newPool(t, ownerURL, 1), `SELECT plan_version FROM tasks WHERE id = $1`, task.ID)
	pendingList := ownerScalar[string](t, newPool(t, ownerURL, 1), `SELECT pending_approvals::text FROM tasks WHERE id = $1`, task.ID)
	events := ownerScalar[int](t, newPool(t, ownerURL, 1), `SELECT count(*) FROM task_events WHERE task_id = $1`, task.ID)
	record(t, caseInput{ID: "TASK-APPROVALS/reads-and-unexpected-events", Contract: approvalsContract, Kind: "e2e", FailureModes: []string{"FM-92", "FM-94"},
		Description: "pending approvals are the PENDING rows and match tasks.pending_approvals; unknown statuses, bad ids and the new node, attempt, plan and status events stop nothing",
		Request:     map[string]string{"read": "task_approvals and tasks.pending_approvals"},
		Expected:    map[string]any{"pendingRows": []string{"apr_C"}, "planVersion": 2, "pendingApprovals": `["apr_C"]`, "events": n},
		Actual:      map[string]any{"pendingRows": pendingIDs, "planVersion": planVersion, "pendingApprovals": pendingList, "events": events},
		Pass: strings.Join(pendingIDs, ",") == "apr_C" && a.NodeID == "n_1" && a.AttemptID == "att_1" && a.ToolCallID == "tc1" &&
			planVersion == 2 && pendingList == `["apr_C"]` && events == n})

	logged := skipLog.String()
	record(t, caseInput{ID: "TASK-APPROVALS/skipped-projection-is-logged", Contract: approvalsContract, Kind: "e2e", FailureModes: []string{"FM-99"},
		Description: "the task.status_changed to a status the table refuses is logged with its event id, type and SQLSTATE",
		Request:     map[string]string{"event": "task.status_changed to NOT_A_STATUS"},
		Expected:    map[string]any{"logged": "event id, type, sqlstate=23514"},
		Actual:      map[string]any{"logged": strings.Contains(logged, "task projection: skipped event evt_01TASKAPPROVALS00000018 (task.status_changed)") && strings.Contains(logged, "sqlstate=23514")},
		Pass:        strings.Contains(logged, "task projection: skipped event evt_01TASKAPPROVALS00000018 (task.status_changed)") && strings.Contains(logged, "sqlstate=23514")})

	// FM-93: a fresh process has no in-memory entity versions, so what it applies is decided by the table alone.
	second := startServer(t, serverOpts{tenant: tenant})
	svc := second.runtime.Tasks
	send := func(id, eventType, entityID string, version int64, payload string) {
		t.Helper()
		n++
		err := svc.AppendEvent(taskruntime.Event{EventID: id, TenantID: tenant, TaskID: task.ID, Type: eventType, Source: "workflow",
			Payload: json.RawMessage(payload), Occurred: time.Date(2026, 10, 5, 1, 0, n, 0, time.UTC), Durable: true,
			EntityKind: "approval", EntityID: entityID, EntityVersion: version})
		if err != nil {
			t.Fatalf("append %s: %v", eventType, err)
		}
	}
	// The same event again (same id), a new request for a decided approval, and decisions older than the row.
	send("evt_01TASKAPPROVALS00000001", "approval.requested", "apr_A", 1, `{"approval_id":"apr_A","attempt_id":"att_1","node_id":"n_1","tool_call_id":"tc1","subject":{"summary":"again"}}`)
	send("evt_01TASKAPPROVALS99000001", "approval.requested", "apr_A", 1, `{"approval_id":"apr_A","attempt_id":"att_1","node_id":"n_1","tool_call_id":"tc1","subject":{"summary":"again"}}`)
	send("evt_01TASKAPPROVALS99000002", "approval.decided", "apr_A", 1, `{"approval_id":"apr_A","status":"CANCELLED","comment":"late","always":false}`)
	send("evt_01TASKAPPROVALS99000003", "approval.decided", "apr_B", 0, `{"approval_id":"apr_B","status":"APPROVED","comment":"no version","always":true}`)
	replayed := readApprovals(t, tenant, task.ID)
	ra, rb := replayed["apr_A"], replayed["apr_B"]
	record(t, caseInput{ID: "TASK-APPROVALS/replay-and-late-events", Contract: approvalsContract, Kind: "e2e", FailureModes: []string{"FM-93"},
		Description: "a replayed request, a late older decision and a versionless decision of a decided approval change nothing",
		Request:     map[string]string{"process": "fresh control with empty entity versions"},
		Expected:    map[string]any{"rows": 3, "A": "APPROVED go ahead v2", "B": "CANCELLED"},
		Actual:      map[string]any{"rows": len(replayed), "A": fmt.Sprint(ra.Status, " ", ra.Comment, " v", ra.Version), "B": rb.Status},
		Pass: len(replayed) == 3 && ra.Status == "APPROVED" && ra.Comment == "go ahead" && ra.Version == 2 &&
			strings.Contains(ra.Subject, "run ls") && rb.Status == "CANCELLED"})

	// A newer decision does apply, a versionless one applies to a PENDING row, and both are idempotent per event.
	send("evt_01TASKAPPROVALS99000004", "approval.decided", "apr_C", 2, `{"approval_id":"apr_C","status":"TAKEN_OVER","comment":"a person took over","always":false}`)
	send("evt_01TASKAPPROVALS99000004", "approval.decided", "apr_C", 2, `{"approval_id":"apr_C","status":"REJECTED","comment":"same id again","always":false}`)
	taken := readApprovals(t, tenant, task.ID)["apr_C"]
	record(t, caseInput{ID: "TASK-APPROVALS/newer-decision-applies-once", Contract: approvalsContract, Kind: "e2e", FailureModes: []string{"FM-93"},
		Description: "a newer decision moves a PENDING approval to TAKEN_OVER, and the same event id again changes nothing",
		Request:     map[string]string{"event": "approval.decided v2 TAKEN_OVER, sent twice"},
		Expected:    map[string]any{"status": "TAKEN_OVER", "version": 2},
		Actual:      map[string]any{"status": taken.Status, "version": taken.Version},
		Pass:        taken.Status == "TAKEN_OVER" && taken.Version == 2 && taken.Decided})
}

// ISO-27: task_approvals tenant isolation and what orbit_app may change.
func TestISO27TaskApprovalsGrants(t *testing.T) {
	const tenantA, tenantB = "t-apr-a", "t-apr-b"
	ctx := context.Background()
	owner := newPool(t, ownerURL, 2)
	app := newPool(t, appURL, 2)
	opsEnsureTenant(t, tenantA)
	opsEnsureTenant(t, tenantB)
	for _, tenant := range []string{tenantA, tenantB} {
		if err := seedTaskRows(ctx, owner, tenant); err != nil {
			t.Fatalf("seed %s: %v", tenant, err)
		}
	}
	const snap = `SELECT status || '|' || subject::text || '|' || task_id FROM task_approvals WHERE tenant_id = $1`
	before := ownerScalar[string](t, owner, snap, tenantA)

	// FM-95: the request is not updatable, and nothing is deletable.
	for _, set := range []string{
		`approval_id = 'apr_moved'`, `tenant_id = 't-apr-b'`, `task_id = 'task_other'`, `node_id = 'n_other'`,
		`attempt_id = 'att_other'`, `tool_call_id = 'tc_other'`, `subject = '{"rewritten":true}'`, `requested_at = now()`,
		`decision_command_id = 'cmd'`, `decided_by = 'someone'`,
	} {
		column := strings.Fields(set)[0]
		_, err := execAsApp(ctx, app, tenantA, `UPDATE task_approvals SET `+set+` WHERE tenant_id = $1`, tenantA)
		iso(t, "ISO-27/update-denied-"+column, approvalsContract, []string{"FM-95"},
			"orbit_app UPDATE of task_approvals."+column+" (outside its column grant) fails with 42501",
			sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `UPDATE task_approvals SET ` + set + ` WHERE tenant_id = $1`},
			map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(err)}, sqlState(err) == "42501")
	}
	_, delErr := execAsApp(ctx, app, tenantA, `DELETE FROM task_approvals WHERE tenant_id = $1`, tenantA)
	unchanged := ownerScalar[string](t, owner, snap, tenantA) == before
	iso(t, "ISO-27/delete-denied", approvalsContract, []string{"FM-95"},
		"orbit_app DELETE on task_approvals fails with 42501 and the row is unchanged",
		sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `DELETE FROM task_approvals WHERE tenant_id = $1`},
		map[string]any{"sqlstate": "42501", "unchanged": true}, map[string]any{"sqlstate": sqlState(delErr), "unchanged": unchanged},
		sqlState(delErr) == "42501" && unchanged)

	// The decision columns are updatable (what the Projector writes).
	decideSQL := `UPDATE task_approvals SET status = 'APPROVED', comment = 'ok', always = true, decided_at = now(), entity_version = 2 WHERE tenant_id = $1`
	rows, err := execAsApp(ctx, app, tenantA, decideSQL, tenantA)
	iso(t, "ISO-27/update-allowed-decision", approvalsContract, []string{"FM-92"},
		"orbit_app UPDATE of status, comment, always, decided_at and entity_version works",
		sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: decideSQL},
		map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": rows, "error": sqlState(err)}, err == nil && rows == 1)

	// RLS: tenant A's role cannot see or change tenant B's approval, and no tenant sees none.
	otherSeen := countAs(t, app, tenantA, `SELECT count(*) FROM task_approvals WHERE tenant_id = $1`, tenantB)
	noTenant := countAs(t, app, "", `SELECT count(*) FROM task_approvals`)
	crossRows, crossErr := execAsApp(ctx, app, tenantA, `UPDATE task_approvals SET status = 'REJECTED' WHERE tenant_id = $1`, tenantB)
	otherStatus := ownerScalar[string](t, owner, `SELECT status FROM task_approvals WHERE tenant_id = $1`, tenantB)
	iso(t, "ISO-27/tenant-isolation", approvalsContract, []string{"FM-77"},
		"with tenant A set, orbit_app sees and updates none of tenant B's approvals; with no tenant it sees none",
		sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `SELECT count(*) FROM task_approvals WHERE tenant_id = $1`},
		map[string]any{"otherTenantRows": 0, "rowsWithoutTenant": 0, "crossUpdated": 0, "otherStatus": "PENDING"},
		map[string]any{"otherTenantRows": otherSeen, "rowsWithoutTenant": noTenant, "crossUpdated": crossRows, "otherStatus": otherStatus},
		otherSeen == 0 && noTenant == 0 && crossErr == nil && crossRows == 0 && otherStatus == "PENDING")
}
