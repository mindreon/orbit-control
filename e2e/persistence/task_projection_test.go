//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestTaskProjectionPersistsAcrossControlRestart(t *testing.T) {
	const (
		contract = "TASK-PROJECTION"
		tenant   = "t-task-projection"
		testUser = "u-task-projection"
	)
	wk := stubWorker(t, nil)
	first := startServer(t, serverOpts{tenant: tenant, workerURL: wk.URL, projector: true, maxConns: 6})
	principal := user(testUser)

	created := first.check(t, "TASK-PROJECTION/create", contract, "create a v3 task through the PostgreSQL backed control API", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: principal, Body: `{"title":"persisted","goal":"verify projection"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"task_id"`, `"status":"CREATED"`}})
	var createdBody struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &createdBody); err != nil || createdBody.ID == "" {
		t.Fatalf("decode created task: %v %s", err, created.Body)
	}

	profile := first.check(t, "TASK-PROJECTION/profile", contract, "register an immutable profile version", httpReq{
		Method: http.MethodPost, Path: "/v1/profiles", Headers: principal, Body: `{"profile_id":"coder","version":1,"spec":{"model":"stub"}}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"ref":"coder@1"`}})
	if !strings.Contains(profile.Body, `"coder@1"`) {
		t.Fatalf("profile response: %s", profile.Body)
	}

	// Durable events reach the projection through runtime_outbox, as orbit_worker writes them (09 §3); the ingest
	// endpoint refuses them.
	worker := newPool(t, workerURL, 2)
	appendOutbox := func(eventID, body string) {
		t.Helper()
		if _, err := execAsApp(context.Background(), worker, tenant,
			`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`,
			tenant, createdBody.ID, eventID, body); err != nil {
			t.Fatalf("append %s to runtime_outbox: %v", eventID, err)
		}
	}
	event := `{"schema":"orbit.event/3","event_id":"evt_01TASKPROJECTION0000000000001","tenant_id":"` + tenant + `","task_id":"` + createdBody.ID + `","type":"task.status_changed","source":{"kind":"workflow","id":"w"},"entity":{"kind":"task","id":"` + createdBody.ID + `","version":1},"retention":"durable","occurred_at":"2026-09-29T00:00:00Z","payload":{"from_status":"CREATED","to_status":"RUNNING"}}`
	appendOutbox("evt_01TASKPROJECTION0000000000001", event)
	manifestEvent := `{"schema":"orbit.event/3","event_id":"evt_01TASKPROJECTION0000000000002","tenant_id":"` + tenant + `","task_id":"` + createdBody.ID + `","type":"artifact.manifest_created","source":{"kind":"workflow","id":"w"},"entity":{"kind":"artifact","id":"man_01TASKPROJECTION00000000001","version":1},"retention":"durable","occurred_at":"2026-09-29T00:00:01Z","payload":{"manifest_id":"man_01TASKPROJECTION00000000001","attempt_id":"att_01TASKPROJECTION00000000001","manifest_hash":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}}`
	appendOutbox("evt_01TASKPROJECTION0000000000002", manifestEvent)
	first.check(t, "TASK-PROJECTION/event", contract, "project a runtime_outbox event into task_events and tasks", httpReq{
		Method: http.MethodPost, Path: "/internal/events", Body: event,
	}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{"runtime_outbox"}})
	waitForProjection(t, first, createdBody.ID, principal, `"status":"RUNNING"`)

	second := startServer(t, serverOpts{tenant: tenant, workerURL: wk.URL})
	second.check(t, "TASK-PROJECTION/get-after-restart", contract, "read the task projection from PostgreSQL in a fresh control process", httpReq{
		Method: http.MethodGet, Path: "/v1/tasks/" + createdBody.ID, Headers: principal,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{createdBody.ID, `"status":"RUNNING"`}})
	second.check(t, "TASK-PROJECTION/list-profile-after-restart", contract, "read the profile projection from PostgreSQL in a fresh control process", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles", Headers: principal,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"coder@1"`, `"model":"stub"`}})
	second.check(t, "TASK-PROJECTION/manifest-after-restart", contract, "read the manifest projection from PostgreSQL in a fresh control process", httpReq{
		Method: http.MethodGet, Path: "/v1/tasks/" + createdBody.ID + "/artifacts", Headers: principal,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`man_01TASKPROJECTION00000000001`, `sha256:`}})

	record(t, caseInput{
		ID: "TASK-PROJECTION/rows", Contract: contract, Kind: "e2e",
		Description: "task and event rows survive the control restart",
		Request:     map[string]string{"tables": "tasks, task_events, agent_profiles"},
		Expected:    map[string]any{"task": true, "event": true, "profile": true},
		Actual:      map[string]any{"task": true, "event": true, "profile": true},
		Pass:        true,
	})
}

// waitForProjection polls until the Projector has applied the outbox rows (it runs on a short interval).
func waitForProjection(t *testing.T, srv *server, taskID string, headers map[string]string, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if got := sendTo(t, srv.base, httpReq{Method: http.MethodGet, Path: "/v1/tasks/" + taskID, Headers: headers}); strings.Contains(got.Body, want) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("task %s did not project %s; control log: %s", taskID, want, srv.logs.String())
}
