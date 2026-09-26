package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/worker"
)

func TestHealthReturnsOK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	HandlerWith(app.New(worker.New(""))).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body HealthBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "ok" {
		t.Fatalf("status = %q", body.Status)
	}
}

func internalReq(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// httptest defaults RemoteAddr to TEST-NET; treat tests as loopback services.
	req.RemoteAddr = "127.0.0.1:1"
	return req
}

func TestRoomHITLAllowCompletesTurn(t *testing.T) {
	turns := 0
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/openSession"):
			_, _ = io.WriteString(w, `{"sessionId":"sess-1"}`)
		case strings.HasSuffix(r.URL.Path, "/runTurn"):
			turns++
			raw, _ := io.ReadAll(r.Body)
			if strings.Contains(string(raw), `"resumeAfterApproval":true`) {
				_, _ = io.WriteString(w, `{"status":"completed","texts":["Command allowed. Simulated workspace listing: README.md"]}`)
				return
			}
			_, _ = io.WriteString(w, `{"status":"needs_approval","approval":{"approvalRequestId":"ask-1","toolName":"bash","reason":"ls"},"texts":["Orbit mock agent received: list files"]}`)
		case strings.HasSuffix(r.URL.Path, "/resolveApproval"):
			_, _ = io.WriteString(w, `{"applied":true}`)
		case strings.HasSuffix(r.URL.Path, "/steer"):
			_, _ = io.WriteString(w, `{"accepted":true}`)
		case strings.HasSuffix(r.URL.Path, "/closeSession"):
			_, _ = io.WriteString(w, `{"closed":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer fake.Close()

	runtime := app.New(worker.New(fake.URL))
	h := HandlerWith(runtime)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rooms", strings.NewReader(`{"kind":"solo","title":"demo"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create room %d %s", rec.Code, rec.Body.String())
	}
	var room app.Room
	if err := json.NewDecoder(rec.Body).Decode(&room); err != nil {
		t.Fatal(err)
	}
	if room.State != app.RoomRunning {
		t.Fatalf("state %s", room.State)
	}
	if room.PermissionPreset != app.PermissionWorkspaceWrite {
		t.Fatalf("permission preset %q", room.PermissionPreset)
	}
	if room.Runtime.Kernel != app.RuntimeKernel || room.Runtime.Protocol != "" || room.Runtime.Isolation != "process" {
		t.Fatalf("runtime snapshot = %+v", room.Runtime)
	}

	rec = httptest.NewRecorder()
	req = internalReq(http.MethodPost, "/internal/events",
		`{"eventId":"ev-worker-1","occurredAt":"2026-09-11T00:00:00Z","type":"tool.call","roomId":"`+
			room.ID+`","sessionId":"`+room.SessionID+`","toolName":"bash","status":"pending","runtime":"agentscope","protocol":"session"}`)
	InternalHandler(runtime).ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("ingest %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/rooms/"+room.ID+"/messages", strings.NewReader(`{"message":"list files"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("post message %d %s", rec.Code, rec.Body.String())
	}
	var posted struct {
		Room     app.Room      `json:"room"`
		Approval *app.Approval `json:"approval"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&posted); err != nil {
		t.Fatal(err)
	}
	if posted.Room.State != app.RoomAwaitingApproval || posted.Approval == nil {
		t.Fatalf("want awaiting approval, got %+v", posted)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/approvals/"+posted.Approval.ID+"/decide", strings.NewReader(`{"decision":"allow"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("decide %d %s", rec.Code, rec.Body.String())
	}
	got, ok := runtime.GetRoom(room.ID)
	if !ok || got.State != app.RoomRunning {
		t.Fatalf("room after allow = %+v ok=%v", got, ok)
	}
	if turns < 2 {
		t.Fatalf("expected resume runTurn, turns=%d", turns)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/rooms/"+room.ID+"/messages", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list messages %d %s", rec.Code, rec.Body.String())
	}
	var listed struct {
		Items []app.Message `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	joined := ""
	for _, m := range listed.Items {
		joined += m.Role + ":" + m.Text + "\n"
	}
	if !strings.Contains(joined, "user:list files") {
		t.Fatalf("missing user turn in %q", joined)
	}
	if !strings.Contains(joined, "Orbit mock agent received") {
		t.Fatalf("missing first assistant text in %q", joined)
	}
	if !strings.Contains(joined, "Command allowed") {
		t.Fatalf("missing resume assistant text in %q", joined)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/rooms/"+room.ID+"/steer", strings.NewReader(`{"instruction":"focus on tests"}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("steer %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/rooms/"+room.ID+"/activity", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("activity %d %s", rec.Code, rec.Body.String())
	}
	type payload struct {
		EventID          string `json:"eventId"`
		ToolName         string `json:"toolName"`
		Runtime          string `json:"runtime"`
		Protocol         string `json:"protocol"`
		PermissionPreset string `json:"permissionPreset"`
	}
	var activity struct {
		Items []struct {
			Type    string  `json:"type"`
			Source  string  `json:"source"`
			Payload payload `json:"payload"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&activity); err != nil {
		t.Fatal(err)
	}
	if len(activity.Items) == 0 {
		t.Fatal("expected activity history")
	}
	foundWorkerEvent := false
	for _, item := range activity.Items {
		if item.Payload.EventID == "ev-worker-1" && item.Source == "worker" && item.Payload.ToolName == "bash" {
			foundWorkerEvent = true
			if item.Payload.Runtime != "agentscope" || item.Payload.Protocol != "session" {
				t.Fatalf("worker event runtime = %+v", item)
			}
		}
	}
	if !foundWorkerEvent {
		t.Fatalf("missing worker event: %+v", activity.Items)
	}
	last := activity.Items[len(activity.Items)-1]
	if last.Type != "room.steered" || last.Source != "control" || last.Payload.PermissionPreset != app.PermissionWorkspaceWrite {
		t.Fatalf("last activity = %+v", last)
	}
	if last.Payload.Runtime != app.RuntimeKernel || last.Payload.Protocol != "" {
		t.Fatalf("control event still forces dsh/acp: %+v", last)
	}
}

func TestCreateRoomValidatesRuntimePolicy(t *testing.T) {
	h := HandlerWith(app.New(worker.New("")))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rooms", strings.NewReader(
		`{"kind":"solo","permissionPreset":"unconfined"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestCreateRoomAcceptsReadOnly(t *testing.T) {
	var opened string
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		raw, _ := io.ReadAll(r.Body)
		opened = string(raw)
		_, _ = io.WriteString(w, `{"sessionId":"sess-ro"}`)
	}))
	defer fake.Close()

	h := HandlerWith(app.New(worker.New(fake.URL)))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rooms", strings.NewReader(
		`{"kind":"solo","permissionPreset":"read-only"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create room %d %s", rec.Code, rec.Body.String())
	}
	var room app.Room
	if err := json.NewDecoder(rec.Body).Decode(&room); err != nil {
		t.Fatal(err)
	}
	if room.PermissionPreset != app.PermissionReadOnly {
		t.Fatalf("permission preset %q", room.PermissionPreset)
	}
	if room.Runtime.Kernel != app.RuntimeKernel || room.Runtime.Protocol == "acp" {
		t.Fatalf("runtime snapshot = %+v", room.Runtime)
	}
	if !strings.Contains(opened, `"permissionPreset":"read-only"`) {
		t.Fatalf("openSession payload = %s", opened)
	}
}

func TestCreateCloudAgentAcceptsReadOnlyPerJob(t *testing.T) {
	h := HandlerWith(app.New(worker.New("")))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/cloud-agents", strings.NewReader(
		`{"repoUrl":"https://example.test/repo.git","prompt":"look","permissionPreset":"read-only"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create job %d %s", rec.Code, rec.Body.String())
	}
	var job struct {
		PermissionPreset string `json:"permissionPreset"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	if job.PermissionPreset != app.PermissionReadOnly {
		t.Fatalf("permission preset %q", job.PermissionPreset)
	}
}

func TestInternalEventsRejectRawOrUnknownPayloads(t *testing.T) {
	h := InternalHandler(app.New(worker.New("")))
	for _, body := range []string{
		`{"jsonrpc":"2.0","method":"session/update"}`,
		`{"type":"session/update","sessionId":"sess-raw"}`,
		`{"type":"tool.call","sessionId":"orphan"}`,
	} {
		rec := httptest.NewRecorder()
		req := internalReq(http.MethodPost, "/internal/events", body)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, want %d", body, rec.Code, http.StatusBadRequest)
		}
	}
}
