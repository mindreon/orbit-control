package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func TestTaskRouterForwardsToOwnerOnce(t *testing.T) {
	var forwarded string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = r.Header.Get("X-Orbit-Task-Forwarded")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer other.Close()

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := taskRouter(next, []string{backend.URL, other.URL}, "local")
	ring := taskruntime.NewRing(64)
	ring.SetMembers([]string{backend.URL, other.URL})
	var taskID string
	for i := 0; i < 10000; i++ {
		candidate := "task_forward_" + strconv.Itoa(i)
		if owner, ok := ring.Owner(candidate); ok && owner == backend.URL {
			taskID = candidate
			break
		}
	}
	if taskID == "" {
		t.Fatal("could not find a task owned by test backend")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+taskID, nil)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent || forwarded != "1" {
		t.Fatalf("forwarding failed: status=%d forwarded=%q", resp.Code, forwarded)
	}
}

func TestTaskRouterReturnsOwnerUnavailable(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	backendURL := backend.URL
	backend.Close()
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer other.Close()
	ring := taskruntime.NewRing(64)
	ring.SetMembers([]string{backendURL, other.URL})
	var taskID string
	for i := 0; i < 10000; i++ {
		candidate := "task_unavailable_" + strconv.Itoa(i)
		if owner, ok := ring.Owner(candidate); ok && owner == backendURL {
			taskID = candidate
			break
		}
	}
	if taskID == "" {
		t.Fatal("could not find a task owned by unavailable backend")
	}
	h := taskRouter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }), []string{backendURL, other.URL}, "local")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/v1/tasks/"+taskID, nil))
	if resp.Code != http.StatusBadGateway || !strings.Contains(resp.Body.String(), "TASK_OWNER_UNAVAILABLE") {
		t.Fatalf("unexpected unavailable response: status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestHealthReturnsOK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	HandlerWith(app.NewWithOptions(app.Options{})).ServeHTTP(rec, req)
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

func TestInternalEventsRejectRawOrUnknownPayloads(t *testing.T) {
	h := InternalHandler(app.NewWithOptions(app.Options{}))
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
