//go:build e2e

package persistence

import (
	"context"
	"net/http"
	"testing"
)

// What a task runs with (15 M8, T8.2 and T8.5): an expert, skills, connectors and a mode, set when the task is created
// and replaced later. The configuration is owned by the task's workflow; control checks every reference, resolves the
// connectors to names-only snapshots and forwards the change.
//
// How it can go wrong, written down before the code:
//   - an expert, skill or connector that does not exist, or belongs to another tenant, is accepted, or answered in a way
//     that tells the caller what exists elsewhere;
//   - a request that is refused leaves a task behind, or changes the configuration it was meant to replace;
//   - an update built on an old version overwrites a newer one, or repeating one command applies it twice;
//   - "keep the expert's connectors" (null) and "no connectors" ([]) come out the same;
//   - another tenant can read or change the configuration;
//   - a task that is already closed takes a new configuration;
//   - the mode is not one of the three, or the lists are longer than 20.

const cfgContract = "TASK-CONFIG"

func TestTaskConfigLifecycleAndIsolation(t *testing.T) {
	a := startServer(t, serverOpts{tenant: "t-cfg-a", maxConns: 6, projector: true})
	other := startServer(t, serverOpts{tenant: "t-cfg-b", maxConns: 4})
	headers := userCSRF(expertUser)

	mine := newConnector(t, a, "cfgdocs")
	theirs := newConnector(t, other, "cfgsecret")
	expert := a.check(t, "TASK-CONFIG/expert", cfgContract, "an expert for the tasks to use", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: `{"name":"Cfg","instructions":"x"}`,
	}, httpExp{Status: http.StatusCreated})
	ref := decodeField(t, expert.Body, "ref")

	create := func(config string) httpReq {
		return httpReq{Method: http.MethodPost, Path: "/v1/tasks", Headers: headers,
			Body: `{"title":"cfg","goal":"g","config":` + config + `}`}
	}
	refused := []struct{ id, desc, config string }{
		{"unknown-expert", "an expert that does not exist", `{"expert":"nobody@1"}`},
		{"unknown-connector", "a connector that does not exist", `{"connector_ids":["mcp_does-not-exist"]}`},
		{"foreign-connector", "another tenant's connector, answered like an unknown one", `{"connector_ids":["` + theirs + `"]}`},
		{"unknown-skill", "a skill that is not in the catalog", `{"skills":["nobody/none"]}`},
		{"bad-mode", "a mode that is not default, plan or ask", `{"mode":"yolo"}`},
		{"too-many-connectors", "more than 20 connectors", `{"connector_ids":` + distinctIDs(21) + `}`},
		{"duplicate-connector", "a connector listed twice", `{"connector_ids":["` + mine + `","` + mine + `"]}`},
	}
	var unknownBody string
	for _, c := range refused {
		act := a.check(t, "TASK-CONFIG/refuse-"+c.id, cfgContract, "refuse "+c.desc, create(c.config),
			httpExp{Status: http.StatusBadRequest, BodyExcludes: []string{theirs, "cfgsecret"}})
		switch c.id {
		case "unknown-connector":
			unknownBody = act.Body
		case "foreign-connector":
			if act.Body != unknownBody {
				t.Errorf("a foreign connector must be answered exactly like an unknown one:\n%s\n%s", act.Body, unknownBody)
			}
		}
	}
	a.check(t, "TASK-CONFIG/no-task-left-behind", cfgContract, "a refused request leaves no task", httpReq{
		Method: http.MethodGet, Path: "/v1/tasks", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{`"task_id"`}})

	made := a.check(t, "TASK-CONFIG/create", cfgContract, "create a task with an expert, a connector and a mode", create(
		`{"expert":"`+ref+`","connector_ids":["`+mine+`"],"mode":"ask"}`,
	), httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"profile":"` + ref + `"`}})
	taskID := decodeField(t, made.Body, "task_id")
	cfgPath := "/v1/tasks/" + taskID + "/config"
	a.check(t, "TASK-CONFIG/read-created", cfgContract, "the configuration is what was asked for, at version 1", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{
		`"config_version":1`, `"expert":"` + ref + `"`, `"connector_ids":["` + mine + `"]`, `"mode":"ask"`,
	}})

	plain := a.check(t, "TASK-CONFIG/create-plain", cfgContract, "a task without a config runs with the defaults", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers, Body: `{"title":"plain","goal":"g"}`,
	}, httpExp{Status: http.StatusCreated})
	plainPath := "/v1/tasks/" + decodeField(t, plain.Body, "task_id") + "/config"
	a.check(t, "TASK-CONFIG/read-defaults", cfgContract, "no expert, skills or connectors chosen: all null", httpReq{
		Method: http.MethodGet, Path: plainPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{
		`"config_version":1`, `"expert":null`, `"skills":null`, `"connector_ids":null`, `"mode":"default"`,
	}})

	patch := func(_, body string) httpReq {
		return httpReq{Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: body}
	}
	a.check(t, "TASK-CONFIG/stale-version", cfgContract, "an update built on an old version is refused", patch("", `{"base_config_version":5,"mode":"plan"}`),
		httpExp{Status: http.StatusConflict, BodyIncludes: []string{"CONFIG_VERSION_CONFLICT"}})
	a.check(t, "TASK-CONFIG/refused-update-changes-nothing", cfgContract, "a refused update left the configuration as it was", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":1`, `"mode":"ask"`}})
	a.check(t, "TASK-CONFIG/update-foreign-connector", cfgContract, "an update with another tenant's connector is refused", patch("", `{"base_config_version":1,"connector_ids":["`+theirs+`"]}`),
		httpExp{Status: http.StatusBadRequest, BodyExcludes: []string{theirs}})

	a.check(t, "TASK-CONFIG/update-empty-connectors", cfgContract, "an empty connector list is an update, not 'no change'",
		patch("", `{"command_id":"01J00000000000000000000100","base_config_version":1,"expert":"`+ref+`","connector_ids":[],"mode":"default"}`),
		httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":2`, `"effective":"next_attempt"`}})
	a.check(t, "TASK-CONFIG/empty-is-not-null", cfgContract, "[] stays [] and does not turn into null", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":2`, `"connector_ids":[]`, `"mode":"default"`}})
	a.check(t, "TASK-CONFIG/repeat-command", cfgContract, "the same command again answers the first result",
		patch("", `{"command_id":"01J00000000000000000000100","base_config_version":1,"expert":"`+ref+`","connector_ids":[],"mode":"default"}`),
		httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":2`}})
	a.check(t, "TASK-CONFIG/omitted-is-null", cfgContract, "leaving connector_ids out goes back to the expert's defaults",
		patch("", `{"base_config_version":2,"mode":"default"}`),
		httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":3`}})
	a.check(t, "TASK-CONFIG/read-inherit", cfgContract, "and reads back as null", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":3`, `"connector_ids":null`, `"expert":null`}})

	other.check(t, "TASK-CONFIG/other-tenant-read", cfgContract, "another tenant cannot read the configuration", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "TASK-CONFIG/other-tenant-update", cfgContract, "another tenant cannot change it", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":3,"mode":"ask"}`,
	}, httpExp{Status: http.StatusNotFound})

	// The workflow reports the cancellation through the outbox; the projector is what makes the task read as closed.
	closedID := decodeField(t, plain.Body, "task_id")
	worker := newPool(t, workerURL, 2)
	cancelled := `{"schema":"orbit.event/3","event_id":"evt_01TASKCONFIG0000000000000001","tenant_id":"t-cfg-a","task_id":"` + closedID + `","type":"task.status_changed","source":{"kind":"workflow","id":"w"},"entity":{"kind":"task","id":"` + closedID + `","version":1},"retention":"durable","occurred_at":"2026-09-29T00:00:00Z","payload":{"from_status":"CREATED","to_status":"CANCELLED"}}`
	if _, err := execAsApp(context.Background(), worker, "t-cfg-a",
		`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`,
		"t-cfg-a", closedID, "evt_01TASKCONFIG0000000000000001", cancelled); err != nil {
		t.Fatalf("append the cancellation to runtime_outbox: %v", err)
	}
	waitForProjection(t, a, closedID, headers, `"status":"CANCELLED"`)
	a.check(t, "TASK-CONFIG/closed-task", cfgContract, "a closed task takes no new configuration", httpReq{
		Method: http.MethodPut, Path: plainPath, Headers: headers, Body: `{"base_config_version":1,"mode":"ask"}`,
	}, httpExp{Status: http.StatusConflict, BodyIncludes: []string{"TASK_CLOSED"}})
}
