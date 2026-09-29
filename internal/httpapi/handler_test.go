package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestDialOrchStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := dialOrch(ctx, "127.0.0.1:1", "", "")
	if err == nil {
		t.Fatal("expected dial to fail")
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("cancelled dial took %s", time.Since(started))
	}
}

func TestCSRFRejectsStateChangeWhenOriginsAreSet(t *testing.T) {
	handler := HandlerWithOptions(app.NewWithOptions(app.Options{}), Options{
		Auth:           LocalAuthenticator("default"),
		AllowedOrigins: []string{"https://app.example"},
	})
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	if rejected.Code != http.StatusForbidden || !strings.Contains(rejected.Body.String(), "CSRF_REJECTED") {
		t.Fatalf("csrf status = %d body %s", rejected.Code, rejected.Body.String())
	}
	allowed := httptest.NewRecorder()
	req := internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("X-Orbit-Request", "1")
	handler.ServeHTTP(allowed, req)
	if allowed.Code != http.StatusCreated {
		t.Fatalf("allowed status = %d body %s", allowed.Code, allowed.Body.String())
	}
}

func TestTaskEventStreamUsesFlusher(t *testing.T) {
	runtime := app.NewWithOptions(app.Options{})
	handler := HandlerWith(runtime)
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, internalReq(http.MethodPost, "/v1/tasks", `{"title":"stream","goal":"flush"}`))
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", created.Code, created.Body.String())
	}
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/tasks/"+task.ID+"/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("stream status = %d body %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content type = %q", got)
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
