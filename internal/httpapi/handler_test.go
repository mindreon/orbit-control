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
}
