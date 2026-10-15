package httpx

import (
	"bytes"
	"crypto/sha256"
	"errors"
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
	}{{method: "POST",
		path: "/v1/workspaces",
		hash: h[:],
		want: true},
		{method: "PATCH",
			path: "/v1/workspaces",
			hash: h[:],
			want: false},
		{method: "POST",
			path: "/other",
			hash: h[:],
			want: false},
		{method: "POST",
			path: "/v1/workspaces",
			hash: other[:],
			want: false}} {
		if got := SameRequest("POST", "/v1/workspaces", h[:], c.method, c.path, c.hash); got != c.want {
			t.Fatal(c)
		}
	}
}
func TestRecovery(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := Middleware(logger, "", nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(map[string]any{"reason": "unexpected state", "token": "credential"})
	}))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 500 || strings.Contains(w.Body.String(), "unexpected state") || w.Header().Get("X-Request-ID") == "" {
		t.Fatal(w)
	}
	if !strings.Contains(logs.String(), "stack") || !strings.Contains(logs.String(), "unexpected state") ||
		strings.Contains(logs.String(), "credential") {
		t.Fatal(logs.String())
	}
}
func TestCORS(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	handler := Middleware(
		logger,
		"https://orbit.example",
		[]string{"https://web.example"},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }),
	)
	for _, c := range []struct {
		method, origin string
		status         int
		acao           string
	}{
		{method: "GET",
			origin: "",
			status: 200,
			acao:   ""},
		{method: "POST",
			origin: "https://orbit.example",
			status: 200,
			acao:   "https://orbit.example"},
		{method: "PATCH",
			origin: "https://web.example",
			status: 200,
			acao:   "https://web.example"},

		{method: "GET",
			origin: "https://evil.example",
			status: 200,
			acao:   ""},
		{method: "HEAD",
			origin: "https://evil.example",
			status: 200,
			acao:   ""},
		{method: "POST",
			origin: "https://evil.example",
			status: 403,
			acao:   ""},
		{method: "DELETE",
			origin: "https://evil.example",
			status: 403,
			acao:   ""},
		{method: "OPTIONS",
			origin: "https://web.example",
			status: 204,
			acao:   "https://web.example"},
		{method: "OPTIONS",
			origin: "https://evil.example",
			status: 200,
			acao:   ""},
	} {
		t.Run(c.method+c.origin, func(t *testing.T) {
			r := httptest.NewRequest(c.method, "/", nil)
			r.Header.Set("Origin", c.origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != c.status || w.Header().Get("Access-Control-Allow-Origin") != c.acao ||
				w.Header().Get("Vary") != "Origin" {
				t.Fatal(w)
			}
			if c.method == "OPTIONS" && c.acao != "" && w.Header().Get("Access-Control-Max-Age") == "" {
				t.Fatal("missing max age")
			}
		})
	}
}
func TestInvalidLimit(t *testing.T) {
	for _, v := range []string{"0", "101", "abc", "-1"} {
		_, err := Pagination(httptest.NewRequest("GET", "/?limit="+v, nil))
		e, ok := errors.AsType[*Error](err)
		if !ok || e.Code != "INVALID_LIMIT" {
			t.Fatal(err)
		}
	}
}
