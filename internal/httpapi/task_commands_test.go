package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/orch"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

const (
	testNode  = "n_01J9Z3K4M5N6P7Q8R9S0T1V2W4"
	otherNode = "n_01J9Z3K4M5N6P7Q8R9S0T1V2X4"
)

// planClient answers what the task routes ask of the orchestrator: a plan, and the updates it is sent, which it records.
type planClient struct {
	taskruntime.TaskClient
	mu     sync.Mutex
	nodes  []map[string]any
	status string
	refuse map[string]error
	// results is what an update returns besides acceptance, by update name (the workflow's result, merged by orch.Client).
	results map[string]string
	updates []recordedUpdate
}

type recordedUpdate struct {
	name    string
	payload map[string]any
}

func (c *planClient) StartTask(_ context.Context, in orch.TaskWorkflowInput) (orch.TaskView, error) {
	return orch.TaskView{TaskID: in.TaskID, Status: "RUNNING", PlanVersion: 1}, nil
}

func (c *planClient) GetTaskPlan(context.Context, string, string) (orch.TaskPlan, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return orch.TaskPlan{PlanVersion: 3, Nodes: c.nodes}, nil
}

func (c *planClient) UpdateTask(_ context.Context, _, _, name, commandID string, arg any) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	payload, _ := arg.(map[string]any)
	c.updates = append(c.updates, recordedUpdate{name: name, payload: payload})
	if err := c.refuse[name]; err != nil {
		return nil, err
	}
	if name == "control" && c.status != "" {
		return json.RawMessage(`{"status":"` + c.status + `","command_id":"` + commandID + `"}`), nil
	}
	if result := c.results[name]; result != "" {
		return json.RawMessage(`{"accepted":true,"command_id":"` + commandID + `",` + result + `}`), nil
	}
	return json.RawMessage(`{"accepted":true,"command_id":"` + commandID + `"}`), nil
}

func (c *planClient) sent(name string) []recordedUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []recordedUpdate
	for _, update := range c.updates {
		if update.name == name {
			out = append(out, update)
		}
	}
	return out
}

func newTaskWithPlan(t *testing.T, nodes ...map[string]any) (http.Handler, *planClient, string) {
	t.Helper()
	client := &planClient{nodes: nodes}
	h := HandlerWith(app.NewWithOptions(app.Options{TaskClient: client}))
	create := httptest.NewRecorder()
	h.ServeHTTP(create, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	if create.Code != http.StatusCreated {
		t.Fatalf("create task = %d: %s", create.Code, create.Body.String())
	}
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	return h, client, task.ID
}

func post(h http.Handler, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, internalReq(http.MethodPost, path, body))
	return rec
}

func TestCompleteNodeIsRefusedWhileTheAgentsAreWorking(t *testing.T) {
	h, client, id := newTaskWithPlan(t, map[string]any{"node_id": testNode, "status": "READY"})
	path := "/v1/tasks/" + id + "/nodes/" + testNode + "/complete"
	// A task nobody took over or paused: the agents are working.
	refused := post(h, path, `{"command_id":"00000000000000000000000001","reason":"by hand"}`)
	if refused.Code != http.StatusConflict || !strings.Contains(refused.Body.String(), "INVALID_TRANSITION") {
		t.Fatalf("complete while running = %d: %s", refused.Code, refused.Body.String())
	}
	if n := len(client.sent("completeNode")); n != 0 {
		t.Fatalf("a refused request reached the workflow %d times", n)
	}
}

func TestCompleteNodeSendsTheUpdateOnceAndReplaysByCommandID(t *testing.T) {
	client := &planClient{status: "TAKEN_OVER", nodes: []map[string]any{{"node_id": testNode, "status": "READY"}}}
	h := HandlerWith(app.NewWithOptions(app.Options{TaskClient: client}))
	create := httptest.NewRecorder()
	h.ServeHTTP(create, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	var task struct {
		ID string `json:"task_id"`
	}
	_ = json.Unmarshal(create.Body.Bytes(), &task)
	if rec := post(h, "/v1/tasks/"+task.ID+"/control", `{"command_id":"00000000000000000000000009","action":"takeover"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("takeover = %d: %s", rec.Code, rec.Body.String())
	}

	path := "/v1/tasks/" + task.ID + "/nodes/" + testNode + "/complete"
	body := `{"command_id":"00000000000000000000000002","reason":"I wrote it"}`
	first := post(h, path, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("complete = %d: %s", first.Code, first.Body.String())
	}
	sent := client.sent("completeNode")
	if len(sent) != 1 || sent[0].payload["node_id"] != testNode || sent[0].payload["reason"] != "I wrote it" || sent[0].payload["command_id"] != "00000000000000000000000002" {
		t.Fatalf("the update the workflow got: %+v", sent)
	}
	// The same command again is the stored answer, not a second update.
	replay := post(h, path, body)
	if replay.Code != http.StatusAccepted || replay.Body.String() != first.Body.String() || len(client.sent("completeNode")) != 1 {
		t.Fatalf("replay = %d %q (first %q), updates %d", replay.Code, replay.Body.String(), first.Body.String(), len(client.sent("completeNode")))
	}
	// The same command id for another node is a different command.
	other := post(h, "/v1/tasks/"+task.ID+"/nodes/"+otherNode+"/complete", body)
	if other.Code != http.StatusNotFound {
		t.Fatalf("another node (not in the plan) = %d: %s", other.Code, other.Body.String())
	}
	// A bad node id never gets that far.
	if bad := post(h, "/v1/tasks/"+task.ID+"/nodes/not-a-node/complete", `{}`); bad.Code != http.StatusBadRequest {
		t.Fatalf("bad node id = %d", bad.Code)
	}
}

func TestCompleteNodeRefusesARunningNodeAndLetsACompleteOneBeAnswered(t *testing.T) {
	client := &planClient{status: "PAUSED", nodes: []map[string]any{
		{"node_id": testNode, "status": "RUNNING"},
		{"node_id": otherNode, "status": "COMPLETED", "frozen": true},
	}}
	h := HandlerWith(app.NewWithOptions(app.Options{TaskClient: client}))
	create := httptest.NewRecorder()
	h.ServeHTTP(create, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	var task struct {
		ID string `json:"task_id"`
	}
	_ = json.Unmarshal(create.Body.Bytes(), &task)
	post(h, "/v1/tasks/"+task.ID+"/control", `{"command_id":"00000000000000000000000009","action":"pause"}`)

	running := post(h, "/v1/tasks/"+task.ID+"/nodes/"+testNode+"/complete", `{"command_id":"00000000000000000000000003"}`)
	if running.Code != http.StatusConflict || !strings.Contains(running.Body.String(), "NODE_RUNNING") {
		t.Fatalf("complete a running node = %d: %s", running.Code, running.Body.String())
	}
	// A node that is complete already goes through: a repeat of an applied command is answered by the ledger, and the
	// workflow refuses anything else.
	done := post(h, "/v1/tasks/"+task.ID+"/nodes/"+otherNode+"/complete", `{"command_id":"00000000000000000000000004"}`)
	if done.Code != http.StatusAccepted {
		t.Fatalf("complete a complete node = %d: %s", done.Code, done.Body.String())
	}
}

func TestProfileSwitchChecksThatTheProfileExistsForTheTenant(t *testing.T) {
	h, client, id := newTaskWithPlan(t)
	path := "/v1/tasks/" + id + "/profile"
	body := func(profile string) string {
		return `{"command_id":"00000000000000000000000005","node_id":"` + testNode + `","to_profile":"` + profile + `","reason":"stuck"}`
	}

	unknown := post(h, path, body("coder-strong@2"))
	if unknown.Code != http.StatusUnprocessableEntity || !strings.Contains(unknown.Body.String(), "UNKNOWN_PROFILE") {
		t.Fatalf("unknown profile = %d: %s", unknown.Code, unknown.Body.String())
	}
	if n := len(client.sent("requestProfileSwitch")); n != 0 {
		t.Fatalf("the update was sent for a profile that does not exist (%d)", n)
	}
	if malformed := post(h, path, body("not a ref")); malformed.Code != http.StatusBadRequest {
		t.Fatalf("malformed profile = %d: %s", malformed.Code, malformed.Body.String())
	}

	created := post(h, "/v1/profiles", `{"profile_id":"coder-strong","version":2,"spec":{"model":"test"}}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("register profile = %d: %s", created.Code, created.Body.String())
	}
	ok := post(h, path, body("coder-strong@2"))
	if ok.Code != http.StatusAccepted {
		t.Fatalf("known profile = %d: %s", ok.Code, ok.Body.String())
	}
	sent := client.sent("requestProfileSwitch")
	if len(sent) != 1 || sent[0].payload["to_profile"] != "coder-strong@2" || sent[0].payload["node_id"] != testNode {
		t.Fatalf("the update the workflow got: %+v", sent)
	}
	// The built-in default is always there.
	if builtin := post(h, path, strings.Replace(body("default@1"), "0005", "0006", 1)); builtin.Code != http.StatusAccepted {
		t.Fatalf("default profile = %d: %s", builtin.Code, builtin.Body.String())
	}
	// The node and the reason are required.
	if missing := post(h, path, `{"to_profile":"default@1","reason":"x"}`); missing.Code != http.StatusBadRequest {
		t.Fatalf("no node = %d", missing.Code)
	}
	if missing := post(h, path, `{"node_id":"`+testNode+`","to_profile":"default@1"}`); missing.Code != http.StatusBadRequest {
		t.Fatalf("no reason = %d", missing.Code)
	}
}

func TestAValidatorRefusalReachesTheClientWithItsStatus(t *testing.T) {
	client := &planClient{
		status: "TAKEN_OVER",
		nodes:  []map[string]any{{"node_id": testNode, "status": "COMPLETED", "frozen": true}},
		refuse: map[string]error{"completeNode": &orch.Refusal{Type: "FROZEN_NODE", Message: "node is already complete"}},
	}
	h := HandlerWith(app.NewWithOptions(app.Options{TaskClient: client}))
	create := httptest.NewRecorder()
	h.ServeHTTP(create, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	var task struct {
		ID string `json:"task_id"`
	}
	_ = json.Unmarshal(create.Body.Bytes(), &task)
	rec := post(h, "/v1/tasks/"+task.ID+"/nodes/"+testNode+"/complete", `{"command_id":"00000000000000000000000007"}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "FROZEN_NODE") {
		t.Fatalf("refused complete = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestProfileSwitchAndCompleteNodeReturnTheWorkflowsResultAndReplayItUnchanged(t *testing.T) {
	h, client, id := newTaskWithPlan(t, map[string]any{"node_id": testNode, "status": "READY"})
	client.status = "TAKEN_OVER"
	client.results = map[string]string{
		"requestProfileSwitch": `"effective_attempt_no":3,"needs_approval":true,"approval_id":"apr_01J9Z3K4M5N6P7Q8R9S0T1V2W4"`,
		"completeNode":         `"node_id":"` + testNode + `","status":"COMPLETED"`,
	}
	if rec := post(h, "/v1/tasks/"+id+"/control", `{"command_id":"00000000000000000000000009","action":"takeover"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("takeover = %d: %s", rec.Code, rec.Body.String())
	}
	for _, tc := range []struct{ name, path, body, update, want string }{
		{"profile", "/v1/tasks/" + id + "/profile", `{"command_id":"00000000000000000000000021","node_id":"` + testNode + `","to_profile":"default@1","reason":"stuck"}`,
			"requestProfileSwitch", `"effective_attempt_no":3`},
		{"complete", "/v1/tasks/" + id + "/nodes/" + testNode + "/complete", `{"command_id":"00000000000000000000000022","reason":"done"}`,
			"completeNode", `"status":"COMPLETED"`},
	} {
		first := post(h, tc.path, tc.body)
		var got map[string]any
		_ = json.Unmarshal(first.Body.Bytes(), &got)
		if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), tc.want) || got["accepted"] != true || got["command_id"] == "" {
			t.Fatalf("%s = %d: %s", tc.name, first.Code, first.Body.String())
		}
		replay := post(h, tc.path, tc.body)
		if replay.Code != first.Code || replay.Body.String() != first.Body.String() || len(client.sent(tc.update)) != 1 {
			t.Fatalf("%s replay = %d %q (first %q), updates %d", tc.name, replay.Code, replay.Body.String(), first.Body.String(), len(client.sent(tc.update)))
		}
	}
}
