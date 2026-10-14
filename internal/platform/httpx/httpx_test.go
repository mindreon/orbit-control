package httpx

import (
	"crypto/sha256"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIdempotencyRequestIdentity(t *testing.T) {
	h := sha256.Sum256([]byte(`{"name":"a"}`))
	other := sha256.Sum256([]byte(`{"name":"b"}`))
	for _, c := range []struct {
		method, path string
		hash         []byte
		want         bool
	}{{"POST", "/v1/workspaces", h[:], true}, {"PATCH", "/v1/workspaces", h[:], false}, {"POST", "/other", h[:], false}, {"POST", "/v1/workspaces", other[:], false}} {
		if got := SameRequest("POST", "/v1/workspaces", h[:], c.method, c.path, c.hash); got != c.want {
			t.Fatal(c)
		}
	}
}
func TestRecoveryAndCORS(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := Middleware(logger, []string{"https://web.example"}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("tenant-secret") }))
	for _, origin := range []string{"", "https://evil.example", "https://web.example"} {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		want := 500
		if origin == "https://evil.example" {
			want = 403
		}
		if w.Code != want || strings.Contains(w.Body.String(), "tenant-secret") || w.Header().Get("X-Request-ID") == "" {
			t.Fatal(w)
		}
	}
}
