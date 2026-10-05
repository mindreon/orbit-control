//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/store/pgstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

const structureContract = "orbit-infra 09 §3 (migrations 00024, 00025, 00027)"

const (
	hashA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// waitFor polls an owner query until it returns want.
func waitFor(t *testing.T, query, want string, args ...any) {
	t.Helper()
	owner := newPool(t, ownerURL, 1)
	deadline := time.Now().Add(15 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		if err := owner.QueryRow(context.Background(), query, args...).Scan(&got); err == nil && got == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%q = %q, want %q", query, got, want)
}

// TestTaskStructureProjection: plan versions, nodes and attempts are projected from durable events, in order, once, and
// never out of a terminal status the maintenance (or an earlier finish) wrote.
func TestTaskStructureProjection(t *testing.T) {
	const (
		tenant   = "t-task-structure"
		testUser = "u-task-structure"
	)
	ctx := context.Background()
	first := startServer(t, serverOpts{tenant: tenant, projector: true, maxConns: 6})
	principal := userCSRF(testUser)
	created := first.check(t, "TASK-STRUCTURE/create", structureContract, "create a v3 task", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: principal, Body: `{"title":"structure","goal":"project structure"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"task_id"`}})
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &task); err != nil || task.ID == "" {
		t.Fatalf("decode created task: %v %s", err, created.Body)
	}
	owner := newPool(t, ownerURL, 2)
	worker := newPool(t, workerURL, 2)

	n := 0
	outbox := func(eventType, entityKind, entityID string, version int, payload string) {
		t.Helper()
		n++
		id := fmt.Sprintf("evt_01TASKSTRUCTURE%08d", n)
		body := fmt.Sprintf(`{"schema":"orbit.event/3","event_id":%q,"tenant_id":%q,"task_id":%q,"type":%q,"source":{"kind":"workflow","id":"w"},"entity":{"kind":%q,"id":%q,"version":%d},"retention":"durable","occurred_at":"2026-10-05T00:00:%02dZ","payload":%s}`,
			id, tenant, task.ID, eventType, entityKind, entityID, version, n, payload)
		if _, err := execAsApp(ctx, worker, tenant,
			`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`, tenant, task.ID, id, body); err != nil {
			t.Fatalf("append %s to runtime_outbox: %v", eventType, err)
		}
	}
	taskV := 0
	node := func(id, from, to, reason string) {
		taskV++
		outbox("node.status_changed", "task", task.ID, taskV, fmt.Sprintf(`{"node_id":%q,"from_status":%q,"to_status":%q,"reason":%q}`, id, from, to, reason))
	}
	started := func(attempt string, no int) {
		outbox("attempt.started", "attempt", attempt, 1, fmt.Sprintf(`{"node_id":"n_1","attempt_id":%q,"attempt_no":%d,"profile":"coder@1","config_version":4,"switched_from":"coder@0","budget_reserved":{"tokens":40000,"cost_usd_micros":null},"future_field":{"x":1}}`, attempt, no))
	}

	// Plan: a committed change, a compaction (older nodes leave the live plan), a repeat of a version, a bad hash.
	outbox("plan.version_committed", "plan", task.ID, 1, `{"plan_version":2,"parent_version":1,"hash":"`+hashA+`","change_command_id":"cmd_1","actor":{"kind":"user","id":"u"}}`)
	outbox("plan.version_committed", "plan", task.ID, 2, `{"plan_version":3,"parent_version":2,"hash":"`+hashB+`","command_id":"compact:1","actor":{"kind":"system","id":"task-workflow"},"reason":"compaction","archived":{"count":1,"hash":"h","added":1}}`)
	outbox("plan.version_committed", "plan", task.ID, 3, `{"plan_version":3,"hash":"`+hashA+`"}`)
	outbox("plan.version_committed", "plan", task.ID, 4, `{"plan_version":4,"hash":"not-a-hash"}`)
	// Nodes: n_1 follows its status (BLOCKED with a reason), n_2 only appears in a status change.
	node("n_1", "PENDING", "READY", "")
	node("n_1", "READY", "RUNNING", "")
	node("n_2", "PENDING", "RETRY_PENDING", "verification failed")
	node("n_1", "RUNNING", "BLOCKED", "exploration budget spent")
	// Attempts: att_1 parks, resumes and fails; att_2 parks for input; att_4 completes.
	started("att_1", 1)
	outbox("attempt.parked", "attempt", "att_1", 2, `{"node_id":"n_1","attempt_id":"att_1","reason":"approval","question":null}`)
	outbox("attempt.resumed", "attempt", "att_1", 0, `{"node_id":"n_1","attempt_id":"att_1","attempt_no":1,"activity_attempt":2}`)
	outbox("attempt.finished", "attempt", "att_1", 3, `{"node_id":"n_1","attempt_id":"att_1","outcome":"failed","failure":{"failure_class":"verification","retryable":true,"message":"no tests ran","extra":1},"usage":{"tokens_in":10,"tokens_out":5,"tool_calls":1,"wall_s":2,"cost_usd_micros":7}}`)
	started("att_2", 2)
	outbox("attempt.parked", "attempt", "att_2", 2, `{"node_id":"n_1","attempt_id":"att_2","reason":"input","question":"which file?"}`)
	started("att_3", 3) // left RUNNING: the maintenance closes it out below
	started("att_4", 4)
	outbox("attempt.finished", "attempt", "att_4", 2, `{"node_id":"n_1","attempt_id":"att_4","outcome":"completed","failure":null}`)
	waitFor(t, `SELECT status FROM stage_attempts WHERE attempt_id = 'att_4'`, "ACCEPTED")

	// The maintenance (orbit_worker) finds att_3 RUNNING with no workflow and marks it LOST; its late events must not undo that.
	closeSQL := `UPDATE stage_attempts SET status = 'LOST', failure = '{"failure_class":"lost","reason":"workflow not found"}', finished_at = now(),
	             entity_version = entity_version + 1 WHERE attempt_id = 'att_3' AND status IN ('STARTING', 'RUNNING')`
	closed, closeErr := execAsApp(ctx, worker, tenant, closeSQL)
	if closeErr != nil || closed != 1 {
		t.Fatalf("maintenance close-out of att_3: rows=%d err=%v", closed, closeErr)
	}
	outbox("attempt.parked", "attempt", "att_3", 2, `{"node_id":"n_1","attempt_id":"att_3","reason":"approval"}`)
	outbox("attempt.resumed", "attempt", "att_3", 0, `{"node_id":"n_1","attempt_id":"att_3","attempt_no":3,"activity_attempt":2}`)
	outbox("attempt.finished", "attempt", "att_3", 3, `{"node_id":"n_1","attempt_id":"att_3","outcome":"completed","failure":null}`)
	taskV++
	outbox("task.status_changed", "task", task.ID, taskV, `{"from_status":"RUNNING","to_status":"PAUSED_NEEDS_REVIEW","reason":"sentinel"}`)
	waitForProjection(t, first, task.ID, principal, `"status":"PAUSED_NEEDS_REVIEW"`)

	statuses := func(query string) string {
		t.Helper()
		rows, err := owner.Query(ctx, query, tenant, task.ID)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var a, b string
			if err := rows.Scan(&a, &b); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, a+"="+b)
		}
		return strings.Join(out, ",")
	}
	plans := statuses(`SELECT plan_version::text, hash || '/' || parent_version || '/' || coalesce(change_command_id,'') || '/' || (graph IS NULL)::text FROM plan_versions WHERE tenant_id = $1 AND task_id = $2 ORDER BY plan_version`)
	wantPlans := "2=" + hashA + "/1/cmd_1/true,3=" + hashB + "/2/compact:1/true"
	record(t, caseInput{ID: "TASK-STRUCTURE/plan-versions", Contract: structureContract, Kind: "e2e", FailureModes: []string{"FM-96", "FM-97"},
		Description: "plan.version_committed inserts one plan_versions row per version (a repeat of version 3 and a bad hash change nothing)",
		Request:     map[string]string{"events": "versions 2, 3 (compaction), 3 again, 4 with a bad hash"},
		Expected:    map[string]any{"plans": wantPlans}, Actual: map[string]any{"plans": plans}, Pass: plans == wantPlans})

	nodes := statuses(`SELECT node_id, status || '/' || reason || '/' || (node_type IS NULL)::text FROM task_nodes WHERE tenant_id = $1 AND task_id = $2 ORDER BY node_id`)
	wantNodes := "n_1=BLOCKED/exploration budget spent/true,n_2=RETRY_PENDING/verification failed/true"
	record(t, caseInput{ID: "TASK-STRUCTURE/nodes", Contract: structureContract, Kind: "e2e", FailureModes: []string{"FM-96"},
		Description: "node.status_changed inserts a minimal task_nodes row and follows the node's status and reason; rows stay when compaction runs",
		Request:     map[string]string{"events": "n_1 READY, RUNNING, BLOCKED; n_2 RETRY_PENDING; plan compaction"},
		Expected:    map[string]any{"nodes": wantNodes}, Actual: map[string]any{"nodes": nodes}, Pass: nodes == wantNodes})

	attempts := statuses(`SELECT attempt_id, status FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 ORDER BY attempt_id`)
	wantAttempts := "att_1=REJECTED,att_2=PARKED_INPUT,att_3=LOST,att_4=ACCEPTED"
	detail := statuses(`SELECT attempt_id, node_id || '/' || attempt_no || '/' || profile_ref || '/' || coalesce(runtime->>'config_version','') || '/' || coalesce(runtime->>'switched_from','') || '/' || coalesce(runtime->'budget_reserved'->>'tokens','') || '/' || coalesce(runtime->'budget_reserved'->>'cost_usd_micros','-') || '/' || coalesce(failure->>'failure_class','') || '/' || coalesce(failure->>'message','') || '/' || coalesce(usage->>'tokens_in','') || '/' || (finished_at IS NOT NULL)::text FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt_id IN ('att_1','att_3') ORDER BY attempt_id`)
	wantDetail := "att_1=n_1/1/coder@1/4/coder@0/40000/-/verification/no tests ran/10/true,att_3=n_1/3/coder@1/4/coder@0/40000/-/lost///true"
	record(t, caseInput{ID: "TASK-STRUCTURE/attempts", Contract: structureContract, Kind: "e2e", FailureModes: []string{"FM-96", "FM-98"},
		Description: "attempt events insert, park, resume and finish stage_attempts rows; the LOST the maintenance wrote is not undone by late events",
		Request:     map[string]string{"events": "started x4, parked, resumed, finished, then maintenance LOST on att_3 and late att_3 events"},
		Expected:    map[string]any{"attempts": wantAttempts, "detail": wantDetail}, Actual: map[string]any{"attempts": attempts, "detail": detail},
		Pass: attempts == wantAttempts && detail == wantDetail})

	// A fresh process has no in-memory entity versions, so the tables alone decide what a replayed or late event does.
	second := startServer(t, serverOpts{tenant: tenant})
	svc := second.runtime.Tasks
	send := func(id, eventType, kind, entityID string, version int64, payload string) {
		t.Helper()
		n++
		err := svc.AppendEvent(taskruntime.Event{EventID: id, TenantID: tenant, TaskID: task.ID, Type: eventType, Source: "workflow",
			Payload: json.RawMessage(payload), Occurred: time.Date(2026, 10, 5, 1, 0, n, 0, time.UTC), Durable: true,
			EntityKind: kind, EntityID: entityID, EntityVersion: version})
		if err != nil {
			t.Fatalf("append %s: %v", eventType, err)
		}
	}
	send("evt_01TASKSTRUCTURE99000001", "plan.version_committed", "plan", task.ID, 1, `{"plan_version":2,"parent_version":1,"hash":"`+hashB+`"}`)
	send("evt_01TASKSTRUCTURE99000002", "node.status_changed", "task", task.ID, 1, `{"node_id":"n_1","from_status":"PENDING","to_status":"READY"}`)
	send("evt_01TASKSTRUCTURE99000004", "attempt.started", "attempt", "att_1", 1, `{"node_id":"n_1","attempt_id":"att_1","attempt_no":1,"profile":"other@1"}`)
	send("evt_01TASKSTRUCTURE99000005", "attempt.started", "attempt", "att_9", 1, `{"node_id":"n_1","attempt_id":"att_9","attempt_no":1,"profile":"other@1"}`)
	send("evt_01TASKSTRUCTURE99000006", "attempt.parked", "attempt", "att_4", 9, `{"node_id":"n_1","attempt_id":"att_4","reason":"approval"}`)
	send("evt_01TASKSTRUCTURE99000007", "attempt.parked", "attempt", "att_2", 1, `{"node_id":"n_1","attempt_id":"att_2","reason":"approval"}`)
	// A newer parked event applies, and a newer node status applies.
	send("evt_01TASKSTRUCTURE99000008", "node.status_changed", "task", task.ID, 99, `{"node_id":"n_1","from_status":"BLOCKED","to_status":"COMPLETED","reason":"","frozen":true}`)
	// A node's own entity (kind "node", a counter of its own): its first event creates the row, a newer one applies, and an
	// older one (a replay) changes nothing. Events of kind "task" from before it keep working, as the lines above show.
	send("evt_01TASKSTRUCTURE99000010", "node.status_changed", "node", "n_3", 7, `{"node_id":"n_3","from_status":"PENDING","to_status":"READY","node_type":"agent_turn","title":"third"}`)
	send("evt_01TASKSTRUCTURE99000011", "node.status_changed", "node", "n_3", 8, `{"node_id":"n_3","from_status":"READY","to_status":"RUNNING"}`)
	send("evt_01TASKSTRUCTURE99000012", "node.status_changed", "node", "n_3", 7, `{"node_id":"n_3","from_status":"PENDING","to_status":"READY"}`)
	send("evt_01TASKSTRUCTURE99000009", "attempt.finished", "attempt", "att_2", 5, `{"node_id":"n_1","attempt_id":"att_2","outcome":"cancelled","failure":null}`)

	plans2 := statuses(`SELECT plan_version::text, hash FROM plan_versions WHERE tenant_id = $1 AND task_id = $2 ORDER BY plan_version`)
	attempts2 := statuses(`SELECT attempt_id, status FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 ORDER BY attempt_id`)
	nodes2 := statuses(`SELECT node_id, status || '/' || frozen::text FROM task_nodes WHERE tenant_id = $1 AND task_id = $2 ORDER BY node_id`)
	record(t, caseInput{ID: "TASK-STRUCTURE/replay-and-late-events", Contract: structureContract, Kind: "e2e", FailureModes: []string{"FM-97", "FM-98"},
		Description: "a repeated plan version or attempt.started (even under another attempt id for the same node and number), an older node status and an event for a terminal attempt change nothing; newer events apply",
		Request:     map[string]string{"process": "fresh control with empty entity versions"},
		Expected:    map[string]any{"plans": "2=" + hashA + ",3=" + hashB, "attempts": "att_1=REJECTED,att_2=ABORTED,att_3=LOST,att_4=ACCEPTED", "nodes": "n_1=COMPLETED/true,n_2=RETRY_PENDING/false,n_3=RUNNING/false"},
		Actual:      map[string]any{"plans": plans2, "attempts": attempts2, "nodes": nodes2},
		Pass: plans2 == "2="+hashA+",3="+hashB && attempts2 == "att_1=REJECTED,att_2=ABORTED,att_3=LOST,att_4=ACCEPTED" &&
			nodes2 == "n_1=COMPLETED/true,n_2=RETRY_PENDING/false,n_3=RUNNING/false"})
}

// ISO-28: plan, node and attempt projection grants and tenant isolation.
func TestISO28PlanNodeAttemptGrants(t *testing.T) {
	const tenantA, tenantB = "t-struct-a", "t-struct-b"
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
	update := func(table, set string) (int64, error) {
		return execAsApp(ctx, app, tenantA, `UPDATE `+table+` SET `+set+` WHERE tenant_id = $1`, tenantA)
	}
	for _, tc := range []struct {
		table  string
		denied []string
		ok     string
	}{
		{"task_nodes", []string{`node_id = 'n_moved'`, `task_id = 'task_other'`, `tenant_id = 't-struct-b'`},
			`status = 'COMPLETED', reason = 'done', frozen = true, entity_version = 3, updated_at = now(), node_type = 'wait', title = 'x', workspace_mode = 'read', owner_profile = 'p@1', depends_on = '[]', attempt_count = 2, current_attempt_id = 'att_x', parent_node_id = 'n_p', sop_step = '{"sop":"s@1","role":"step","total":1}'`},
		{"stage_attempts", []string{`attempt_no = 9`, `node_id = 'n_other'`, `task_id = 'task_other'`, `profile_ref = 'x@1'`, `started_at = now()`},
			`status = 'ACCEPTED', failure = '{"failure_class":"tool"}', usage = '{"tokens_in":1}', finished_at = now(), entity_version = 3, status_changed_at = now(), resumed_activity_attempt = 2, resumed_state_version = 5`},
	} {
		for _, set := range tc.denied {
			column := strings.Fields(set)[0]
			_, err := update(tc.table, set)
			iso(t, "ISO-28/update-denied-"+tc.table+"-"+column, structureContract, []string{"FM-97"},
				"orbit_app UPDATE of "+tc.table+"."+column+" (outside its column grant) fails with 42501",
				sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `UPDATE ` + tc.table + ` SET ` + set},
				map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(err)}, sqlState(err) == "42501")
		}
		rows, err := update(tc.table, tc.ok)
		iso(t, "ISO-28/update-allowed-"+tc.table, structureContract, []string{"FM-96"},
			"orbit_app UPDATE of the projected columns of "+tc.table+" works",
			sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `UPDATE ` + tc.table + ` SET ` + tc.ok},
			map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": rows, "error": sqlState(err)}, err == nil && rows == 1)
		_, delErr := execAsApp(ctx, app, tenantA, `DELETE FROM `+tc.table+` WHERE tenant_id = $1`, tenantA)
		iso(t, "ISO-28/delete-denied-"+tc.table, structureContract, []string{"FM-97"},
			"orbit_app DELETE on "+tc.table+" fails with 42501",
			sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `DELETE FROM ` + tc.table},
			map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(delErr)}, sqlState(delErr) == "42501")
	}

	// The nullable columns exist for events that do not carry them: a minimal node row and a graph-less plan row are accepted.
	_, nodeErr := execAsApp(ctx, app, tenantA, `INSERT INTO task_nodes (tenant_id, task_id, node_id, status) VALUES ($1, 'task_' || $1, 'n_min', 'READY')`, tenantA)
	_, planErr := execAsApp(ctx, app, tenantA, `INSERT INTO plan_versions (tenant_id, task_id, plan_version, parent_version, hash, actor) VALUES ($1, 'task_' || $1, 2, 1, '`+sha+`', '{}')`, tenantA)
	iso(t, "ISO-28/minimal-rows", structureContract, []string{"FM-96"},
		"a task_nodes row without type, title, workspace mode and owner, and a plan_versions row without a graph, are accepted",
		sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: "INSERT INTO task_nodes (…status) / plan_versions (…no graph)"},
		map[string]any{"node": "ok", "plan": "ok"}, map[string]any{"node": sqlState(nodeErr), "plan": sqlState(planErr)}, nodeErr == nil && planErr == nil)

	for _, table := range []string{"task_nodes", "stage_attempts", "plan_versions"} {
		other := countAs(t, app, tenantA, `SELECT count(*) FROM `+table+` WHERE tenant_id = $1`, tenantB)
		noTenant := countAs(t, app, "", `SELECT count(*) FROM `+table)
		var crossRows int64
		var crossErr error
		if table != "plan_versions" {
			crossRows, crossErr = execAsApp(ctx, app, tenantA, `UPDATE `+table+` SET status = 'CANCELLED' WHERE tenant_id = $1`, tenantB)
		}
		iso(t, "ISO-28/tenant-isolation-"+table, structureContract, []string{"FM-77"},
			"with tenant A set, orbit_app sees and updates none of tenant B's "+table+" rows; with no tenant it sees none",
			sqlReq{Role: "orbit_app", Tenant: tenantA, SQL: `SELECT count(*) FROM ` + table + ` WHERE tenant_id = $1`},
			map[string]any{"otherTenantRows": 0, "rowsWithoutTenant": 0, "crossUpdated": 0},
			map[string]any{"otherTenantRows": other, "rowsWithoutTenant": noTenant, "crossUpdated": crossRows},
			other == 0 && noTenant == 0 && crossErr == nil && crossRows == 0)
	}
}

// TestTaskStructureEnrichedEvents: the enriched node.status_changed, plan.version_committed and attempt.finished fill the
// projection rows, events from before the enrichment still project, a known value is not erased by an event that lacks it,
// and a stale attempt.resumed cannot turn a parked attempt back to RUNNING (migration 00025).
func TestTaskStructureEnrichedEvents(t *testing.T) {
	const (
		tenant   = "t-task-enriched"
		testUser = "u-task-enriched"
	)
	ctx := context.Background()
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	created := srv.check(t, "TASK-ENRICHED/create", structureContract, "create a v3 task", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: userCSRF(testUser), Body: `{"title":"enriched","goal":"project enriched events"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"task_id"`}})
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &task); err != nil || task.ID == "" {
		t.Fatalf("decode created task: %v %s", err, created.Body)
	}
	owner := newPool(t, ownerURL, 2)
	svc := srv.runtime.Tasks
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	n := 0
	// send appends one durable event as control's Projector would: at is the event's occurred_at in seconds after base.
	send := func(eventType, source, kind, entityID string, version int64, at int, payload string) {
		t.Helper()
		n++
		err := svc.AppendEvent(taskruntime.Event{EventID: fmt.Sprintf("evt_01TASKENRICHED%08d", n), TenantID: tenant, TaskID: task.ID, Type: eventType,
			Source: source, Payload: json.RawMessage(payload), Occurred: base.Add(time.Duration(at) * time.Second), Durable: true,
			EntityKind: kind, EntityID: entityID, EntityVersion: version})
		if err != nil {
			t.Fatalf("append %s: %v", eventType, err)
		}
	}
	value := func(query string, args ...any) string {
		t.Helper()
		var got string
		if err := owner.QueryRow(ctx, query, append([]any{tenant, task.ID}, args...)...).Scan(&got); err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		return got
	}
	nodeRow := func(id string) string {
		return value(`SELECT status || '|' || coalesce(node_type,'-') || '|' || coalesce(title,'-') || '|' || coalesce(workspace_mode,'-') || '|' || coalesce(owner_profile,'-') || '|' ||
			coalesce(depends_on::text,'-') || '|' || frozen::text || '|' || attempt_count || '|' || coalesce(current_attempt_id,'-')
			FROM task_nodes WHERE tenant_id = $1 AND task_id = $2 AND node_id = '` + id + `'`)
	}
	attemptRow := func(id string) string {
		return value(`SELECT status || '|' || node_id || '|' || attempt_no || '|' || profile_ref || '|' || coalesce(runtime->>'config_version','-') || '|' ||
			coalesce(usage->>'tokens_in','-') || '|' || (finished_at IS NOT NULL)::text
			FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt_id = '` + id + `'`)
	}
	attemptCount := func(id string) string {
		return value(`SELECT count(*)::text FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt_id = '` + id + `'`)
	}
	check := func(id, description string, got, want string) {
		t.Helper()
		failureModes := []string{"FM-96", "FM-97"}
		switch {
		case strings.HasPrefix(id, "resumed"):
			failureModes = []string{"FM-98", "FM-100"}
		case strings.HasPrefix(id, "node-sop"):
			failureModes = []string{"FM-102", "FM-103", "FM-104"}
		case strings.HasPrefix(id, "node"):
			failureModes = []string{"FM-96", "FM-101"}
		}
		record(t, caseInput{ID: "TASK-ENRICHED/" + id, Contract: structureContract, Kind: "e2e", FailureModes: failureModes,
			Description: description, Request: map[string]string{"events": id}, Expected: map[string]any{"rows": want}, Actual: map[string]any{"rows": got}, Pass: got == want})
	}

	// Nodes: an enriched first change fills the row; an old-shape change moves only the status; a later enriched change
	// replaces only what it names; an invalid workspace_access does not lose the status.
	send("node.status_changed", "workflow", "task", task.ID, 1, 1, `{"node_id":"n_1","from_status":"PENDING","to_status":"READY","node_type":"agent_turn","title":"Write code","workspace_access":"write","owner_profile":"coder@1","depends_on":["n_0"],"frozen":false,"attempt_count":0}`)
	check("node-enriched-fills-row", "an enriched node.status_changed creates the row with the node's type, title, workspace mode, owner, dependencies and counters",
		nodeRow("n_1"), `READY|agent_turn|Write code|write|coder@1|["n_0"]|false|0|-`)
	send("node.status_changed", "workflow", "task", task.ID, 2, 2, `{"node_id":"n_1","from_status":"READY","to_status":"RUNNING","attempt_count":1,"current_attempt_id":"att_1"}`)
	send("node.status_changed", "workflow", "task", task.ID, 3, 3, `{"node_id":"n_1","from_status":"RUNNING","to_status":"BLOCKED","reason":"old shape"}`)
	check("node-old-event-keeps-known-values", "an event without the enriched fields changes the status and reason and does not erase the type, title, workspace mode, owner, dependencies or attempt",
		nodeRow("n_1"), `BLOCKED|agent_turn|Write code|write|coder@1|["n_0"]|false|1|att_1`)
	send("node.status_changed", "workflow", "task", task.ID, 4, 4, `{"node_id":"n_1","from_status":"BLOCKED","to_status":"READY","title":"Write tests","depends_on":[],"frozen":true,"attempt_count":2}`)
	check("node-partial-update", "an event names only some facts: those are replaced (an empty dependency list is a value), the others stay",
		nodeRow("n_1"), `READY|agent_turn|Write tests|write|coder@1|[]|true|2|att_1`)
	send("node.status_changed", "workflow", "task", task.ID, 5, 5, `{"node_id":"n_2","from_status":"PENDING","to_status":"READY","node_type":"wait","title":"","workspace_access":"bogus","owner_profile":"","attempt_count":-1}`)
	check("node-invalid-facts-skipped", "a node event whose facts are empty or outside the column's values still creates the row with its status and leaves those columns NULL",
		nodeRow("n_2"), `READY|wait|-|-|-|-|false|0|-`)
	send("node.status_changed", "workflow", "task", task.ID, 6, 6, `{"node_id":"n_3","from_status":"PENDING","to_status":"READY"}`)
	check("node-old-event-creates-minimal-row", "an old-shape first node event creates a minimal row (type, title, workspace mode, owner and dependencies NULL)",
		nodeRow("n_3"), `READY|-|-|-|-|-|false|0|-`)

	// A step of a compiled SOP: the event also names its parent node and which step of which SOP it is (migration 00027).
	sopRow := func(id string) string {
		return value(`SELECT status || '|' || coalesce(parent_node_id,'-') || '|' || coalesce(sop_step::text,'-')
			FROM task_nodes WHERE tenant_id = $1 AND task_id = $2 AND node_id = '` + id + `'`)
	}
	send("node.status_changed", "workflow", "task", task.ID, 14, 14, `{"node_id":"n_4","from_status":"PENDING","to_status":"READY","node_type":"agent_turn","title":"Release 2/3: review","workspace_access":"write","owner_profile":"coder@1","depends_on":["n_3"],"frozen":false,"attempt_count":0,"parent_node_id":"n_3","sop_step":{"sop":"release@1","role":"step","total":3,"step_id":"s2","index":2,"subject":"review"}}`)
	check("node-sop-step-event", "a node.status_changed that names a parent node and an SOP step creates the row like any enriched one",
		nodeRow("n_4"), `READY|agent_turn|Release 2/3: review|write|coder@1|["n_3"]|false|0|-`)
	const step2 = `READY|n_3|{"sop": "release@1", "role": "step", "index": 2, "total": 3, "step_id": "s2", "subject": "review"}`
	check("node-sop-step-fills-columns", "the creating event fills parent_node_id and sop_step", sopRow("n_4"), step2)
	send("node.status_changed", "workflow", "task", task.ID, 15, 15, `{"node_id":"n_4","from_status":"READY","to_status":"RUNNING"}`)
	check("node-sop-step-kept-by-later-event", "a later event without parent_node_id and sop_step moves the status and keeps both",
		sopRow("n_4"), strings.Replace(step2, "READY", "RUNNING", 1))
	send("node.status_changed", "workflow", "task", task.ID, 16, 16, `{"node_id":"n_4","from_status":"RUNNING","to_status":"BLOCKED","parent_node_id":"","sop_step":null}`)
	send("node.status_changed", "workflow", "task", task.ID, 17, 17, `{"node_id":"n_4","from_status":"BLOCKED","to_status":"READY","parent_node_id":"bogus","sop_step":"not an object"}`)
	check("node-sop-step-invalid-keeps-known", "an empty, null, malformed or non-object value neither blanks nor replaces the known ones and does not lose the status",
		sopRow("n_4"), step2)
	send("node.status_changed", "workflow", "task", task.ID, 18, 18, `{"node_id":"n_4","from_status":"READY","to_status":"RUNNING","sop_step":{"sop":"release@2","role":"step","total":4,"step_id":"s3","index":3}}`)
	check("node-sop-step-replaced", "an event that names a new sop_step replaces it and keeps the parent",
		sopRow("n_4"), `RUNNING|n_3|{"sop": "release@2", "role": "step", "index": 3, "total": 4, "step_id": "s3"}`)
	send("node.status_changed", "workflow", "task", task.ID, 19, 19, `{"node_id":"n_5","from_status":"PENDING","to_status":"READY","title":"Plain"}`)
	check("node-sop-step-old-shape-null", "an old-shape event creates a node whose parent_node_id and sop_step are NULL", sopRow("n_5"), `READY|-|-`)
	send("node.status_changed", "workflow", "task", task.ID, 20, 20, `{"node_id":"n_5","from_status":"READY","to_status":"RUNNING","parent_node_id":"n_3","sop_step":{"sop":"release@1","role":"sop","total":3}}`)
	check("node-sop-step-filled-on-update", "an event that names them for a row that has none fills them (update path)",
		sopRow("n_5"), `RUNNING|n_3|{"sop": "release@1", "role": "sop", "total": 3}`)
	send("node.status_changed", "workflow", "task", task.ID, 3, 21, `{"node_id":"n_5","from_status":"READY","to_status":"READY","parent_node_id":"n_9","sop_step":{"sop":"stale@1","role":"step","total":1}}`)
	check("node-sop-step-stale-event-ignored", "an event not newer than the row (entity version) changes neither status nor SOP columns",
		sopRow("n_5"), `RUNNING|n_3|{"sop": "release@1", "role": "sop", "total": 3}`)

	// What the plan API reads: only this task's nodes that have a parent or a step, and nothing across tenants.
	reader := pgstore.New(newPool(t, appURL, 2))
	kept, readErr := reader.NodeStructure(ctx, taskruntime.Principal{TenantID: tenant, UserID: testUser}, task.ID)
	other, otherErr := reader.NodeStructure(ctx, taskruntime.Principal{TenantID: "t-task-enriched-other", UserID: testUser}, task.ID)
	keptJSON, _ := json.Marshal(kept)
	record(t, caseInput{ID: "TASK-ENRICHED/node-sop-step-read", Contract: structureContract, Kind: "e2e", FailureModes: []string{"FM-102", "FM-105"},
		Description: "NodeStructure returns the nodes that have a parent or an SOP step (not n_1..n_3), for the caller's tenant only",
		Request:     map[string]string{"read": "pgstore.NodeStructure"},
		Expected:    map[string]any{"nodes": "n_4,n_5", "otherTenant": 0},
		Actual:      map[string]any{"nodes": string(keptJSON), "otherTenant": len(other), "errors": fmt.Sprint(readErr, otherErr)},
		Pass:        readErr == nil && otherErr == nil && len(kept) == 2 && kept["n_4"].ParentNodeID == "n_3" && string(kept["n_5"].SopStep) != "" && len(other) == 0})

	// Plans: the contract's change_command_id wins over the legacy command_id; parent_version and actor are stored.
	send("plan.version_committed", "workflow", "plan", task.ID, 1, 7, `{"plan_version":2,"parent_version":1,"hash":"`+hashA+`","command_id":"cmd_legacy","change_command_id":"cmd_new","actor":{"kind":"user","id":"u-1"}}`)
	send("plan.version_committed", "workflow", "plan", task.ID, 2, 8, `{"plan_version":3,"hash":"`+hashB+`","command_id":"cmd_old"}`)
	check("plan-enriched", "plan.version_committed stores change_command_id (not the legacy command_id), parent_version and actor; an old event keeps working",
		value(`SELECT string_agg(plan_version || '=' || parent_version || '/' || coalesce(change_command_id,'-') || '/' || actor::text, ',' ORDER BY plan_version)
			FROM plan_versions WHERE tenant_id = $1 AND task_id = $2`),
		`2=1/cmd_new/{"id": "u-1", "kind": "user"},3=2/cmd_old/{}`)

	// attempt.finished creates the row when attempt.started was missed, and never changes a terminal row afterwards.
	send("attempt.finished", "workflow", "attempt", "att_f1", 3, 10, `{"node_id":"n_1","attempt_id":"att_f1","attempt_no":7,"profile":"coder@2","config_version":5,"outcome":"completed","failure":null,"usage":{"tokens_in":11}}`)
	check("finished-creates-row", "attempt.finished with attempt_no and profile creates the attempt's row (status from the outcome, config_version in runtime, finished_at set)",
		attemptRow("att_f1"), `ACCEPTED|n_1|7|coder@2|5|11|true`)
	send("attempt.started", "workflow", "attempt", "att_f1", 1, 9, `{"node_id":"n_1","attempt_id":"att_f1","attempt_no":7,"profile":"other@1"}`)
	send("attempt.finished", "workflow", "attempt", "att_f1", 9, 11, `{"node_id":"n_1","attempt_id":"att_f1","attempt_no":7,"profile":"other@1","outcome":"failed","failure":{"failure_class":"tool","retryable":false,"message":"m"}}`)
	check("finished-row-stays-terminal", "a late attempt.started or a second attempt.finished does not change the terminal row",
		attemptRow("att_f1"), `ACCEPTED|n_1|7|coder@2|5|11|true`)
	send("attempt.finished", "workflow", "attempt", "att_f2", 3, 12, `{"node_id":"n_1","attempt_id":"att_f2","outcome":"completed"}`)
	send("attempt.finished", "workflow", "attempt", "att_f3", 3, 13, `{"node_id":"n_1","attempt_id":"att_f3","attempt_no":8,"outcome":"completed"}`)
	check("finished-old-shape-creates-nothing", "an old-shape attempt.finished (no attempt_no or profile) for an attempt with no row creates no row",
		attemptCount("att_f2")+attemptCount("att_f3"), "00")

	// attempt.resumed: the worker's event has no entity version, so its occurred_at is compared with the last workflow
	// status change and (activity_attempt, state_version) with the last resumed event applied since.
	const att = "att_r"
	started := `{"node_id":"n_1","attempt_id":"att_r","attempt_no":1,"profile":"coder@1"}`
	resumed := func(activity, state int) string {
		return fmt.Sprintf(`{"node_id":"n_1","attempt_id":"att_r","attempt_no":1,"activity_attempt":%d,"state_version":%d}`, activity, state)
	}
	status := func() string {
		return value(`SELECT status || '|' || resumed_activity_attempt || '.' || resumed_state_version FROM stage_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt_id = '` + att + `'`)
	}
	send("attempt.started", "workflow", "attempt", att, 1, 20, started)
	send("attempt.parked", "workflow", "attempt", att, 2, 30, `{"node_id":"n_1","attempt_id":"att_r","reason":"approval"}`)
	send("attempt.resumed", "worker", "attempt", att, 0, 25, resumed(2, 3)) // emitted before the park, delivered after it
	check("resumed-stale-after-parked", "an attempt.resumed that occurred before the last attempt.parked leaves the attempt PARKED_HITL", status(), "PARKED_HITL|0.0")
	send("attempt.resumed", "worker", "attempt", att, 0, 40, resumed(2, 3))
	check("resumed-newer-than-parked", "an attempt.resumed after the park sets RUNNING and records its (activity_attempt, state_version)", status(), "RUNNING|2.3")
	send("attempt.parked", "workflow", "attempt", att, 3, 50, `{"node_id":"n_1","attempt_id":"att_r","reason":"input","question":"which?"}`)
	send("attempt.resumed", "worker", "attempt", att, 0, 40, resumed(2, 3)) // the same event again
	check("resumed-replay-after-second-park", "a redelivered attempt.resumed does not undo a later park", status(), "PARKED_INPUT|0.0")
	send("attempt.resumed", "worker", "attempt", att, 0, 60, resumed(1, 9)) // a new activity after the answer starts counting again
	check("resumed-after-park-resets-order", "after a park the next attempt.resumed applies even when its activity_attempt is lower than the last one before the park", status(), "RUNNING|1.9")
	// Within one stretch the key orders resumed events: put the attempt back to PARKED_HITL without a workflow event.
	if _, err := owner.Exec(ctx, `UPDATE stage_attempts SET status = 'PARKED_HITL' WHERE tenant_id = $1 AND task_id = $2 AND attempt_id = $3`, tenant, task.ID, att); err != nil {
		t.Fatalf("park att_r as owner: %v", err)
	}
	send("attempt.resumed", "worker", "attempt", att, 0, 70, resumed(1, 9))
	send("attempt.resumed", "worker", "attempt", att, 0, 71, resumed(1, 4))
	check("resumed-key-not-newer", "an attempt.resumed that is not newer than the last applied (activity_attempt, state_version) changes nothing", status(), "PARKED_HITL|1.9")
	send("attempt.resumed", "worker", "attempt", att, 0, 72, resumed(1, 10))
	check("resumed-key-newer", "an attempt.resumed with a newer (activity_attempt, state_version) applies", status(), "RUNNING|1.10")
	// An event without activity_attempt (from before the runtime sent one) is ordered by time alone.
	send("attempt.parked", "workflow", "attempt", att, 4, 80, `{"node_id":"n_1","attempt_id":"att_r","reason":"approval"}`)
	send("attempt.resumed", "worker", "attempt", att, 0, 75, `{"node_id":"n_1","attempt_id":"att_r","attempt_no":1}`)
	check("resumed-old-shape-stale", "an old-shape attempt.resumed that occurred before the park changes nothing", status(), "PARKED_HITL|0.0")
	send("attempt.resumed", "worker", "attempt", att, 0, 85, `{"node_id":"n_1","attempt_id":"att_r","attempt_no":1}`)
	check("resumed-old-shape-newer", "an old-shape attempt.resumed after the park sets RUNNING", status(), "RUNNING|0.0")
}
