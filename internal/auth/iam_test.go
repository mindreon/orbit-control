package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

// fakeIAM serves the baize envelope for GET /users/current.
type fakeIAM struct {
	status int
	code   int
	body   any
	raw    string
	hits   *atomic.Int32
}

func (f *fakeIAM) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		if r.URL.Path != "/users/current" || r.Header.Get("Authorization") == "" || r.Header.Get("Accept") != "application/json" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if f.status != 0 {
			w.WriteHeader(f.status)
		}
		if f.raw != "" {
			_, _ = io.WriteString(w, f.raw)
			return
		}
		data, _ := json.Marshal(f.body)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": f.code, "data": json.RawMessage(data)})
	})
}

func (f *fakeIAM) serve(t *testing.T) *httptest.Server {
	t.Helper()
	f.hits = &atomic.Int32{}
	s := httptest.NewServer(f.handler())
	t.Cleanup(s.Close)
	return s
}

func TestIAMLookup(t *testing.T) {
	for _, test := range []struct {
		name       string
		fake       fakeIAM
		wantStatus int
		wantCode   string
		retryable  bool
		want       iamIdentity
	}{
		{name: "wrapped user",
			fake: fakeIAM{body: map[string]any{"user": map[string]any{
				"id": "u-1", "username": "ada", "nickname": "Ada", "email": "ada@example.com", "status": "active"}}},
			want: iamIdentity{UserID: "u-1", Email: "ada@example.com", Name: "Ada"}},
		{name: "bare data user",
			fake: fakeIAM{body: map[string]any{"userId": "u-2", "username": "bea"}},
			want: iamIdentity{UserID: "u-2", Name: "bea"}},
		{name: "disabled user",
			fake:       fakeIAM{body: map[string]any{"user": map[string]any{"id": "u-4", "status": "disabled"}}},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "inactive user",
			fake:       fakeIAM{body: map[string]any{"user": map[string]any{"id": "u-5", "status": "INACTIVE"}}},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "username only",
			fake:       fakeIAM{body: map[string]any{"user": map[string]any{"username": "nia"}}},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "bare username only",
			fake:       fakeIAM{body: map[string]any{"username": "nia"}},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "envelope code",
			fake:       fakeIAM{code: 40100, body: map[string]any{}},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "iam 401",
			fake:       fakeIAM{status: http.StatusUnauthorized},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "iam 403",
			fake:       fakeIAM{status: http.StatusForbidden},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "iam 404",
			fake:       fakeIAM{status: http.StatusNotFound},
			wantStatus: 401, wantCode: "UNAUTHENTICATED"},
		{name: "iam 408",
			fake:       fakeIAM{status: http.StatusRequestTimeout},
			wantStatus: 503, wantCode: "NOT_READY", retryable: true},
		{name: "iam 429",
			fake:       fakeIAM{status: http.StatusTooManyRequests},
			wantStatus: 503, wantCode: "NOT_READY", retryable: true},
		{name: "iam 500",
			fake:       fakeIAM{status: http.StatusInternalServerError},
			wantStatus: 503, wantCode: "NOT_READY", retryable: true},
		{name: "iam 503",
			fake:       fakeIAM{status: http.StatusServiceUnavailable},
			wantStatus: 503, wantCode: "NOT_READY", retryable: true},
		{name: "undecodable body",
			fake:       fakeIAM{raw: "not json"},
			wantStatus: 503, wantCode: "NOT_READY", retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := test.fake.serve(t)
			lookup := newHTTPLookup(server.URL+"/", server.Client())
			got, err := lookup.Lookup(t.Context(), "token-1")
			if test.wantStatus == 0 {
				if err != nil || got != test.want {
					t.Fatalf("got %+v, %v; want %+v", got, err, test.want)
				}
				return
			}
			var httpErr *httpx.Error
			if !errors.As(iamError(err), &httpErr) || httpErr.Status != test.wantStatus ||
				httpErr.Code != test.wantCode || httpErr.Retryable != test.retryable {
				t.Fatalf("got %v; want %d %s retryable=%v", err, test.wantStatus, test.wantCode, test.retryable)
			}
		})
	}
}

func TestIAMLookupCache(t *testing.T) {
	fake := &fakeIAM{body: map[string]any{"user": map[string]any{"id": "u-1", "username": "ada"}}}
	server := fake.serve(t)
	lookup := newCachedLookup(newHTTPLookup(server.URL, server.Client()))
	first, err := lookup.Lookup(t.Context(), "token-1")
	if err != nil || first.UserID != "u-1" {
		t.Fatal(err, first)
	}
	if _, err = lookup.Lookup(t.Context(), "token-1"); err != nil {
		t.Fatal(err)
	}
	if fake.hits.Load() != 1 {
		t.Fatalf("cache miss: iam called %d times", fake.hits.Load())
	}
	// A different token is a different cache key.
	if _, err = lookup.Lookup(t.Context(), "token-2"); err != nil || fake.hits.Load() != 2 {
		t.Fatal(err, fake.hits.Load())
	}
	// Expired entries are not served.
	lookup.now = func() time.Time { return time.Now().Add(2 * iamLookupTTL) }
	if _, err = lookup.Lookup(t.Context(), "token-1"); err != nil || fake.hits.Load() != 3 {
		t.Fatal(err, fake.hits.Load())
	}
}

func TestIAMLookupDoesNotCacheRejection(t *testing.T) {
	for _, fake := range []fakeIAM{
		{status: http.StatusUnauthorized},
		{body: map[string]any{"user": map[string]any{"username": "nia"}}},
	} {
		server := fake.serve(t)
		lookup := newCachedLookup(newHTTPLookup(server.URL, server.Client()))
		for range 2 {
			if _, err := lookup.Lookup(t.Context(), "token-1"); !errors.Is(err, errIAMRejected) {
				t.Fatal(err)
			}
		}
		if fake.hits.Load() != 2 {
			t.Fatalf("negative result cached: iam called %d times", fake.hits.Load())
		}
	}
}

func TestIAMLookupNetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	lookup := newHTTPLookup(server.URL, server.Client())
	_, err := lookup.Lookup(t.Context(), "token-1")
	var httpErr *httpx.Error
	if !errors.As(iamError(err), &httpErr) || httpErr.Status != 503 || !httpErr.Retryable {
		t.Fatal(err)
	}
}

func TestIAMLookupTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 0
	lookup := newHTTPLookup(server.URL, client)
	a := &Service{Config: config.Config{AuthMode: "iam"}, Logger: slog.New(slog.DiscardHandler)}
	a.iam = lookup
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	// A spent deadline is an IAM outage (retryable 503), never a 401.
	_, err := a.Authenticate(ctx, "token-1", "")
	var httpErr *httpx.Error
	if !errors.As(err, &httpErr) || httpErr.Status != 503 || httpErr.Code != "NOT_READY" || !httpErr.Retryable {
		t.Fatal(err)
	}
	// Only the caller canceling its own request passes through unmapped.
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if _, err = lookup.Lookup(ctx, "token-1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// A redirecting IAM is a login wall; the 3xx must count as a rejected token.
func TestIAMLookupRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	defer server.Close()
	lookup := newHTTPLookup(server.URL, nil)
	if _, err := lookup.Lookup(t.Context(), "token-1"); !errors.Is(err, errIAMRejected) {
		t.Fatal(err)
	}
}

// stubLookup stands in for baize so the service branch can be checked without Postgres.
type stubLookup struct{ err error }

func (s stubLookup) Lookup(context.Context, string) (iamIdentity, error) {
	return iamIdentity{}, s.err
}

func TestAuthenticateIAM(t *testing.T) {
	a := &Service{Config: config.Config{AuthMode: "iam"}, Logger: slog.New(slog.DiscardHandler)}
	a.iam = stubLookup{err: errIAMRejected}
	if _, err := a.Authenticate(t.Context(), "token-1", ""); !errors.Is(err, errIAMRejected) {
		var httpErr *httpx.Error
		if !errors.As(err, &httpErr) || httpErr.Status != 401 {
			t.Fatal(err)
		}
	}
	a.iam = stubLookup{err: ErrIAMUnavailable}
	_, err := a.Authenticate(t.Context(), "token-1", "")
	var httpErr *httpx.Error
	if !errors.As(err, &httpErr) || httpErr.Status != 503 || httpErr.Code != "NOT_READY" || !httpErr.Retryable {
		t.Fatal(err)
	}
}
