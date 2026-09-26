//go:build e2e

package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
	"github.com/mindreon/orbit-control/internal/worker"
)

var (
	appURL   = os.Getenv("ORBIT_TEST_DB_URL")
	ownerURL = os.Getenv("ORBIT_TEST_MIGRATE_DB_URL")
)

const (
	testOrigin = "https://console.orbit.test"
	userHeader = "X-E2E-User"
)

func TestMain(m *testing.M) {
	alias(planted, "<planted-secret>")
	alias(plantedDBPassword, "<planted-db-password>")
	code := run(m)
	path, gateOK, err := writeReport()
	if err != nil {
		fmt.Fprintf(os.Stderr, "write e2e report: %v\n", err)
		code = 1
	} else {
		fmt.Fprintf(os.Stderr, "e2e report: %s\n", path)
	}
	if !gateOK {
		code = 1
	}
	os.Exit(code)
}

func run(m *testing.M) int {
	if appURL == "" || ownerURL == "" {
		msg := "ORBIT_TEST_DB_URL (orbit_app) and ORBIT_TEST_MIGRATE_DB_URL (orbit_owner) are required"
		fatalSetup(msg)
		fmt.Fprintln(os.Stderr, msg)
		return 1
	}
	ctx := context.Background()
	if conn, err := pgx.Connect(ctx, ownerURL); err == nil {
		_ = conn.QueryRow(ctx, `SHOW server_version`).Scan(&postgresVer)
		_ = conn.Close(ctx)
	}
	if !runSDB06Migrations(ctx) {
		fmt.Fprintln(os.Stderr, "S-DB-6 migration sequence failed; see report")
		return 1
	}
	if err := buildBinary(); err != nil {
		msg := "build orbit-control: " + err.Error()
		fatalSetup(msg)
		fmt.Fprintln(os.Stderr, msg)
		return 1
	}
	defer os.RemoveAll(filepath.Dir(binaryPath))
	return m.Run()
}

func newPool(t *testing.T, url string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// asTenant runs fn in a transaction scoped to tenantID, as the repository does.
func asTenant(ctx context.Context, pool *pgxpool.Pool, tenantID string, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if tenantID != "" {
			if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
				return err
			}
		}
		return fn(tx)
	})
}

func countAs(t *testing.T, pool *pgxpool.Pool, tenantID, query string, args ...any) int {
	t.Helper()
	var n int
	if err := asTenant(context.Background(), pool, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(context.Background(), query, args...).Scan(&n)
	}); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	if err != nil {
		return "error: " + err.Error()
	}
	return "ok"
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// server is the production handler stack on a real TCP listener.
type server struct {
	base    string
	appPool *pgxpool.Pool
	logs    *syncBuffer
}

type serverOpts struct {
	tenant       string
	maxConns     int32
	workerURL    string
	orch         app.Orchestrator
	abortTimeout time.Duration
}

func startServer(t *testing.T, o serverOpts) *server {
	t.Helper()
	pool := newPool(t, appURL, o.maxConns)
	repo := pgstore.New(pool)
	if err := repo.EnsureTenant(context.Background(), o.tenant, o.tenant); err != nil {
		t.Fatalf("ensure tenant: %v", err)
	}
	logs := &syncBuffer{}
	runtime := app.NewWithOptions(app.Options{
		Worker: worker.New(o.workerURL), Orch: o.orch, Repo: repo,
		Log: log.New(logs, "", 0), DefaultTenant: o.tenant, AbortTimeout: o.abortTimeout,
	})
	tenant := o.tenant
	auth := httpapi.AuthenticatorFunc(func(r *http.Request) (app.Principal, bool) {
		user := r.Header.Get(userHeader)
		if user == "" {
			return app.Principal{}, false
		}
		return app.Principal{TenantID: tenant, UserID: user}, true
	})
	srv := httptest.NewServer(httpapi.HandlerWithOptions(runtime, httpapi.Options{
		Auth: auth, AllowedOrigins: []string{testOrigin},
	}))
	t.Cleanup(srv.Close)
	return &server{base: srv.URL, appPool: pool, logs: logs}
}

type httpReq struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
}

type httpExp struct {
	Status         int               `json:"status"`
	BodyEquals     string            `json:"bodyEquals,omitempty"`
	BodyIncludes   []string          `json:"bodyIncludes,omitempty"`
	BodyExcludes   []string          `json:"bodyExcludes,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	NotContentType string            `json:"notContentType,omitempty"`
}

type httpAct struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
}

func sendTo(t *testing.T, base string, req httpReq) httpAct {
	t.Helper()
	r, err := http.NewRequest(req.Method, base+req.Path, strings.NewReader(req.Body))
	if err != nil {
		t.Fatal(err)
	}
	if req.Body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range req.Headers {
		r.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.Path, err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	act := httpAct{Status: res.StatusCode, Body: string(raw), Headers: map[string]string{}}
	for _, h := range []string{"Content-Type", "Idempotent-Replayed"} {
		if v := res.Header.Get(h); v != "" {
			act.Headers[h] = v
		}
	}
	return act
}

func (e httpExp) matches(a httpAct) bool {
	if a.Status != e.Status {
		return false
	}
	if e.BodyEquals != "" && a.Body != e.BodyEquals {
		return false
	}
	for _, s := range e.BodyIncludes {
		if !strings.Contains(a.Body, s) {
			return false
		}
	}
	for _, s := range e.BodyExcludes {
		if strings.Contains(a.Body, s) {
			return false
		}
	}
	for k, v := range e.Headers {
		if a.Headers[k] != v {
			return false
		}
	}
	if e.NotContentType != "" && strings.HasPrefix(a.Headers["Content-Type"], e.NotContentType) {
		return false
	}
	return true
}

// check sends req, compares with exp and records an e2e case.
func (s *server) check(t *testing.T, id, contract, desc string, req httpReq, exp httpExp) httpAct {
	t.Helper()
	act := sendTo(t, s.base, req)
	record(t, caseInput{ID: id, Contract: contract, Kind: "e2e", Description: desc, Request: req, Expected: exp, Actual: act, Pass: exp.matches(act)})
	return act
}

func user(u string) map[string]string { return map[string]string{userHeader: u} }

func userCSRF(u string) map[string]string {
	return map[string]string{userHeader: u, "Origin": testOrigin, "X-Orbit-Request": "1"}
}

func withHeader(h map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for hk, hv := range h {
		out[hk] = hv
	}
	return out
}

func roomID(t *testing.T, a httpAct) string {
	t.Helper()
	var r app.Room
	if err := json.Unmarshal([]byte(a.Body), &r); err != nil || r.ID == "" {
		t.Fatalf("no room id in %q", a.Body)
	}
	return r.ID
}

// stubWorker serves orbit-worker activities. onAbort runs inside the abort
// activity so the scenario can observe ordering.
func stubWorker(t *testing.T, onAbort func(roomID string)) *httptest.Server {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		raw, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		i := n.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/openSession"):
			fmt.Fprintf(w, `{"sessionId":"sess-%d"}`, i)
		case strings.HasSuffix(r.URL.Path, "/runTurn"):
			fmt.Fprintf(w, `{"status":"needs_approval","approval":{"approvalRequestId":"ask-%d","toolName":"bash","reason":"ls"},"texts":["stub reply"]}`, i)
		case strings.HasSuffix(r.URL.Path, "/resolveApproval"):
			_, _ = io.WriteString(w, `{"applied":true}`)
		case strings.HasSuffix(r.URL.Path, "/abort"):
			if onAbort != nil {
				id, _ := in["roomId"].(string)
				onAbort(id)
			}
			_, _ = io.WriteString(w, `{"aborted":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// stubOrch stubs Temporal for S-DB-13 (k): Abort hangs until the deadline
// ("timeout") or fails with a token-like message ("error").
type stubOrch struct {
	mode    string
	aborts  atomic.Int32
	onAbort func(roomID string)
	seq     atomic.Int64
}

const planted = "sk-live-E2E-PLANTED-SECRET"

func (f *stubOrch) StartRoom(_ context.Context, roomID, kind, _ string) (orch.RoomView, error) {
	return orch.RoomView{RoomID: roomID, State: "running", Kind: kind, SessionID: fmt.Sprintf("sess-k-%d", f.seq.Add(1))}, nil
}
func (f *stubOrch) RunTurn(context.Context, string, string, string) (orch.RunTurnResult, error) {
	return orch.RunTurnResult{Status: "completed"}, nil
}
func (f *stubOrch) Decide(context.Context, string, string, string, string, string) (orch.DecideResult, error) {
	return orch.DecideResult{}, nil
}
func (f *stubOrch) Steer(context.Context, string, string, string) error { return nil }
func (f *stubOrch) Abort(ctx context.Context, roomID, _, _ string) error {
	f.aborts.Add(1)
	if f.onAbort != nil {
		f.onAbort(roomID)
	}
	if f.mode == "timeout" {
		<-ctx.Done()
		return ctx.Err()
	}
	return errors.New("temporal signal failed: token=" + planted)
}
