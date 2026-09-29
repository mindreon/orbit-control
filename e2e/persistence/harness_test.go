//go:build e2e

package persistence

import (
	"bytes"
	"context"
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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

var (
	appURL   = os.Getenv("ORBIT_TEST_DB_URL")
	ownerURL = os.Getenv("ORBIT_TEST_MIGRATE_DB_URL")
	opsURL   = os.Getenv("ORBIT_TEST_OPS_DB_URL")
	// orbit_worker on orbit_control (migration 00013, ISO-21, ISO-22).
	workerURL = os.Getenv("ORBIT_TEST_WORKER_DB_URL")
)

const (
	testOrigin = "https://console.orbit.test"
	userHeader = "X-E2E-User"
)

func TestMain(m *testing.M) {
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
	if appURL == "" || ownerURL == "" || opsURL == "" || workerURL == "" {
		msg := "ORBIT_TEST_DB_URL (orbit_app), ORBIT_TEST_MIGRATE_DB_URL (orbit_owner), ORBIT_TEST_OPS_DB_URL (orbit_ops) and ORBIT_TEST_WORKER_DB_URL (orbit_worker) are required"
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

// opsEnsureTenant creates a tenant the way ops does: as orbit_ops, never as
// orbit_app (review M2).
func opsEnsureTenant(t *testing.T, tenant string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, opsURL)
	if err != nil {
		t.Fatalf("ops connect: %v", sqlState(err))
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`, tenant); err != nil {
		t.Fatalf("ops ensure tenant: %v", err)
	}
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

// pgMessage is the server message of a Postgres error ("" otherwise).
func pgMessage(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Message
	}
	return ""
}

// privilegeDenied tells a privilege error apart from other 42501 errors
// (RLS "new row violates row-level security policy" shares the code).
func privilegeDenied(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "42501" && strings.HasPrefix(pgErr.Message, "permission denied"):
			return "permission denied"
		case pgErr.Code == "42501" && strings.Contains(pgErr.Message, "row-level security"):
			return "42501 row-level security (not a privilege error)"
		}
		return pgErr.Code
	}
	return sqlState(err)
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
	base string
	// internal is the internal listener (POST /internal/*); the public one
	// answers 404 there.
	internal string
	appPool  *pgxpool.Pool
	logs     *syncBuffer
	runtime  *app.App
}

type serverOpts struct {
	tenant      string
	maxConns    int32
	artifactDir string
	artifactMax int64
	// projector runs the runtime_outbox Projector, as in production. It holds one connection for LISTEN, so a
	// test that sets it needs a pool of more than one.
	projector bool
}

func startServer(t *testing.T, o serverOpts) *server {
	t.Helper()
	opsEnsureTenant(t, o.tenant)
	pool := newPool(t, appURL, o.maxConns)
	repo := pgstore.New(pool)
	logs := &syncBuffer{}
	runtime := app.NewWithOptions(app.Options{
		Repo: repo, Log: log.New(logs, "", 0), DefaultTenant: o.tenant,
		ArtifactDir: o.artifactDir, ArtifactMaxBytes: o.artifactMax,
	})
	if o.projector {
		projectorCtx, stopProjector := context.WithCancel(context.Background())
		t.Cleanup(stopProjector)
		go func() {
			projector := &taskruntime.Projector{Store: repo, Tasks: runtime.Tasks}
			_ = projector.Run(projectorCtx)
		}()
	}
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
	isrv := httptest.NewServer(httpapi.InternalHandler(runtime))
	t.Cleanup(isrv.Close)
	return &server{base: srv.URL, internal: isrv.URL, appPool: pool, logs: logs, runtime: runtime}
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
	base := s.base
	if strings.HasPrefix(req.Path, "/internal/") {
		base = s.internal
	}
	act := sendTo(t, base, req)
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
