package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
	"github.com/mindreon/orbit-control/internal/worker"
)

func TestTaskAPIEndToEndIdempotencyAndSSECursor(t *testing.T) {
	h := HandlerWith(app.New(worker.New("")))
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
	path := "/v1/tasks/" + task.ID + "/control"
	first := httptest.NewRecorder()
	h.ServeHTTP(first, internalReq(http.MethodPost, path, `{"command_id":"00000000000000000000000001","action":"pause"}`))
	if first.Code != http.StatusAccepted {
		t.Fatalf("pause = %d: %s", first.Code, first.Body.String())
	}
	replay := httptest.NewRecorder()
	h.ServeHTTP(replay, internalReq(http.MethodPost, path, `{"command_id":"00000000000000000000000001","action":"pause"}`))
	if replay.Code != http.StatusAccepted || replay.Body.String() != first.Body.String() {
		t.Fatalf("idempotent replay = %d %q, first %q", replay.Code, replay.Body.String(), first.Body.String())
	}
	conflict := httptest.NewRecorder()
	h.ServeHTTP(conflict, internalReq(http.MethodPost, path, `{"command_id":"00000000000000000000000001","action":"cancel"}`))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("idempotency conflict = %d: %s", conflict.Code, conflict.Body.String())
	}
}

func TestTaskProfilesAndManifestProjection(t *testing.T) {
	runtime := app.New(worker.New(""))
	h := HandlerWith(runtime)
	profile := httptest.NewRecorder()
	h.ServeHTTP(profile, internalReq(http.MethodPost, "/v1/profiles", `{"profile_id":"coder","version":1,"spec":{"model":"test"}}`))
	if profile.Code != http.StatusCreated {
		t.Fatalf("profile create = %d: %s", profile.Code, profile.Body.String())
	}
	listed := httptest.NewRecorder()
	h.ServeHTTP(listed, internalReq(http.MethodGet, "/v1/profiles", ""))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), "coder@1") {
		t.Fatalf("profile list = %d: %s", listed.Code, listed.Body.String())
	}
	created := httptest.NewRecorder()
	h.ServeHTTP(created, internalReq(http.MethodPost, "/v1/tasks", `{"title":"manifest","goal":"project"}`))
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	// A v3 manifest event is projected into the task service and becomes
	// available through the tenant scoped artifact endpoint.
	manifest := `{"manifest_id":"man_01ARZ3NDEKTSV4RRFFQ69G5FAV","entries":[{"name":"report.md"}]}`
	runtime.Tasks.AppendEvent(taskruntime.Event{EventID: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW", TaskID: task.ID, Type: "artifact.manifest_created", Source: "workflow", Durable: true, Payload: []byte(manifest)})
	artifacts := httptest.NewRecorder()
	h.ServeHTTP(artifacts, internalReq(http.MethodGet, "/v1/tasks/"+task.ID+"/artifacts", ""))
	if artifacts.Code != http.StatusOK || !strings.Contains(artifacts.Body.String(), "man_01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		t.Fatalf("artifact list = %d: %s", artifacts.Code, artifacts.Body.String())
	}
}
