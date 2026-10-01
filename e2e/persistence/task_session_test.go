//go:build e2e

package persistence

import (
	"context"
	"net/http"
	"strconv"
	"testing"
)

// A task is a conversation: it rests when its plan is done and can be talked to again, and only a cancel ends it.
//
// How it can go wrong, written down before the code:
//   - a task whose workflow reported task.completed is answered "closed" when a message or a new configuration comes;
//   - a cancelled task still takes a message or a configuration;
//   - a task that rested and was talked to again keeps showing COMPLETED while it works.

func TestATaskThatRestedCanBeTalkedToAndOnlyACancelEndsIt(t *testing.T) {
	const (
		contract = "TASK-SESSION"
		tenant   = "t-task-session"
		user     = "u-task-session"
	)
	s := startServer(t, serverOpts{tenant: tenant, projector: true, maxConns: 6})
	headers := userCSRF(user)
	worker := newPool(t, workerURL, 2)

	made := s.check(t, "TASK-SESSION/create", contract, "create a task", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers, Body: `{"title":"talk","goal":"g"}`,
	}, httpExp{Status: http.StatusCreated})
	taskID := decodeField(t, made.Body, "task_id")

	// The projector merges by the entity's version, so each event of the task carries the next one.
	version := 0
	project := func(eventID, eventType, payload string) {
		version++
		t.Helper()
		body := `{"schema":"orbit.event/3","event_id":"` + eventID + `","tenant_id":"` + tenant + `","task_id":"` + taskID + `","type":"` + eventType + `","source":{"kind":"workflow","id":"w"},"entity":{"kind":"task","id":"` + taskID + `","version":` + strconv.Itoa(version) + `},"retention":"durable","occurred_at":"2026-09-29T00:00:00Z","payload":` + payload + `}`
		if _, err := execAsApp(context.Background(), worker, tenant,
			`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`,
			tenant, taskID, eventID, body); err != nil {
			t.Fatalf("append %s to runtime_outbox: %v", eventType, err)
		}
	}

	project("evt_01TASKSESSION00000000000001", "task.completed", `{}`)
	waitForProjection(t, s, taskID, headers, `"status":"COMPLETED"`)
	s.check(t, "TASK-SESSION/message-after-completed", contract, "a task that rested takes a message", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks/" + taskID + "/messages", Headers: headers, Body: `{"text":"and then?"}`,
	}, httpExp{Status: http.StatusAccepted})
	s.check(t, "TASK-SESSION/config-after-completed", contract, "and a new configuration", httpReq{
		Method: http.MethodPut, Path: "/v1/tasks/" + taskID + "/config", Headers: headers, Body: `{"base_config_version":1,"mode":"ask"}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":2`}})

	project("evt_01TASKSESSION00000000000002", "task.status_changed", `{"from_status":"COMPLETED","to_status":"RUNNING"}`)
	waitForProjection(t, s, taskID, headers, `"status":"RUNNING"`)

	project("evt_01TASKSESSION00000000000003", "task.status_changed", `{"from_status":"RUNNING","to_status":"CANCELLED"}`)
	waitForProjection(t, s, taskID, headers, `"status":"CANCELLED"`)
	s.check(t, "TASK-SESSION/message-after-cancel", contract, "a cancelled task takes no message", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks/" + taskID + "/messages", Headers: headers, Body: `{"text":"anyone?"}`,
	}, httpExp{Status: http.StatusConflict, BodyIncludes: []string{"TASK_CLOSED"}})
	s.check(t, "TASK-SESSION/config-after-cancel", contract, "and no configuration", httpReq{
		Method: http.MethodPut, Path: "/v1/tasks/" + taskID + "/config", Headers: headers, Body: `{"base_config_version":2,"mode":"default"}`,
	}, httpExp{Status: http.StatusConflict, BodyIncludes: []string{"TASK_CLOSED"}})
}
