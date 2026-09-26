package pgstore_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/mindreon/orbit-control/internal/worker"
)

const testOrigin = "https://console.orbit.test"

// headerAuth stands in for the §17 session authenticator: X-Test-User is
// the session user, the tenant is fixed.
func headerAuth(tenant string) httpapi.Authenticator {
	return httpapi.AuthenticatorFunc(func(r *http.Request) (app.Principal, bool) {
		user := r.Header.Get("X-Test-User")
		if user == "" {
			return app.Principal{}, false
		}
		return app.Principal{TenantID: tenant, UserID: user}, true
	})
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

type apiClient struct {
	t    *testing.T
	base string
}

type apiResp struct {
	status int
	header http.Header
	body   string
}

func (c apiClient) do(method, path, user, body string, hdr map[string]string) apiResp {
	c.t.Helper()
	req, err := http.NewRequest(method, c.base+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.Header.Set("X-Test-User", user)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return apiResp{status: res.StatusCode, header: res.Header, body: string(raw)}
}

var csrfHeaders = map[string]string{"Origin": testOrigin, "X-Orbit-Request": "1"}

func (c apiClient) deleteRoom(roomID, user, body string) apiResp {
	return c.do(http.MethodDelete, "/v1/rooms/"+roomID, user, body, csrfHeaders)
}

func expectStatus(t *testing.T, what string, got apiResp, want int) {
	t.Helper()
	if got.status != want {
		t.Fatalf("%s: status %d, want %d; body %s", what, got.status, want, got.body)
	}
}

// fakeWorker serves the direct-HTTP worker activities. onAbort runs inside
// the abort activity so the test can observe ordering.
func fakeWorker(t *testing.T, onAbort func(roomID string)) *httptest.Server {
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
			fmt.Fprintf(w, `{"status":"needs_approval","approval":{"approvalRequestId":"ask-%d","toolName":"bash","reason":"ls"},"texts":["mock reply"]}`, i)
		case strings.HasSuffix(r.URL.Path, "/abort"):
			if onAbort != nil {
				roomID, _ := in["roomId"].(string)
				onAbort(roomID)
			}
			_, _ = io.WriteString(w, `{"aborted":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func ownerScalar[T any](t *testing.T, owner *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := owner.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("owner query %q: %v", query, err)
	}
	return v
}

type sdb13Fixture struct {
	artifactID string
	storageRef string
}

// seedChildren inserts the child rows the phase-1 code does not write yet
// (turns, events, artifacts, artifact_versions, approval_rules) as orbit_app,
// which also proves WITH CHECK admits them while the room is live.
func seedChildren(t *testing.T, pool *pgxpool.Pool, tenant, roomID, storageRef string, size int, digest string) sdb13Fixture {
	t.Helper()
	ctx := context.Background()
	f := sdb13Fixture{artifactID: "art_" + roomID, storageRef: storageRef}
	asTenant(t, pool, tenant, func(tx pgx.Tx) error {
		stmts := []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO turns (id, tenant_id, task_id, kind, status) VALUES ($1, $2, $3, 'message', 'completed')`,
				[]any{"tn_" + roomID, tenant, roomID}},
			{`INSERT INTO events (tenant_id, task_id, seq, event_uid, type, source) VALUES ($1, $2, 1, $3, 'tool.call', 'worker')`,
				[]any{tenant, roomID, "ev_" + roomID}},
			{`INSERT INTO artifacts (id, tenant_id, task_id, title, latest_version) VALUES ($1, $2, $3, 'report', 1)`,
				[]any{f.artifactID, tenant, roomID}},
			{`INSERT INTO artifact_versions (artifact_id, tenant_id, version, mime_type, size_bytes, content_digest, previewable, storage_ref)
			  VALUES ($1, $2, 1, 'text/plain', $3, $4, true, $5)`,
				[]any{f.artifactID, tenant, size, digest, storageRef}},
			{`INSERT INTO approval_rules (id, tenant_id, tool_name, scope, scope_id, room_id) VALUES ($1, $2, 'bash', 'room', $3, $3)`,
				[]any{"rule_" + roomID, tenant, roomID}},
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s.sql, s.args...); err != nil {
				return fmt.Errorf("%s: %w", s.sql, err)
			}
		}
		return nil
	})
	return f
}

func TestSDB13SoftDelete(t *testing.T) {
	requirePG(t)
	t.Setenv("ORBIT_INTERNAL_TOKEN", "")
	ctx := context.Background()
	const tenant, owner, other = "t-sdb13", "u-owner", "u-other"
	startedAt := time.Now().UTC()

	ownerPool := newPool(t, ownerURL, 2)
	repo, appPool := appStore(t, 4)
	if err := repo.EnsureTenant(ctx, tenant, tenant); err != nil {
		t.Fatal(err)
	}

	var abortSawLive atomic.Int32
	var roomA string
	wk := fakeWorker(t, func(roomID string) {
		// §18.7a: the workflow abort happens before the soft delete.
		if roomID == roomA && ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, roomID) {
			abortSawLive.Add(1)
		}
	})
	var logs syncBuffer
	runtime := app.NewWithOptions(app.Options{
		Worker: worker.New(wk.URL), Repo: repo, Log: log.New(&logs, "", 0), DefaultTenant: tenant,
	})
	srv := httptest.NewServer(httpapi.HandlerWithOptions(runtime, httpapi.Options{
		Auth: headerAuth(tenant), AllowedOrigins: []string{testOrigin},
	}))
	t.Cleanup(srv.Close)
	c := apiClient{t: t, base: srv.URL}

	createBody := `{"kind":"solo","title":"to delete"}`
	const idemKey = "sdb13-create-A"
	res := c.do(http.MethodPost, "/v1/rooms", owner, createBody, map[string]string{"Idempotency-Key": idemKey})
	expectStatus(t, "create A", res, http.StatusOK)
	var created app.Room
	_ = json.Unmarshal([]byte(res.body), &created)
	roomA = created.ID
	replay := c.do(http.MethodPost, "/v1/rooms", owner, createBody, map[string]string{"Idempotency-Key": idemKey})
	expectStatus(t, "replay before delete", replay, http.StatusOK)
	if !strings.Contains(replay.body, roomA) || replay.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay before delete = %s %v", replay.body, replay.header)
	}
	res = c.do(http.MethodPost, "/v1/rooms", owner, `{"kind":"solo","title":"survivor"}`, nil)
	expectStatus(t, "create B", res, http.StatusOK)
	var survivor app.Room
	_ = json.Unmarshal([]byte(res.body), &survivor)
	roomB := survivor.ID

	res = c.do(http.MethodPost, "/v1/rooms/"+roomA+"/messages", owner, `{"message":"list files"}`, nil)
	expectStatus(t, "post message A", res, http.StatusOK)
	var posted struct {
		Approval *app.Approval `json:"approval"`
	}
	_ = json.Unmarshal([]byte(res.body), &posted)
	if posted.Approval == nil {
		t.Fatalf("expected approval: %s", res.body)
	}
	approvalA := posted.Approval.ID
	res = c.do(http.MethodPost, "/v1/rooms/"+roomB+"/messages", owner, `{"message":"hello"}`, nil)
	expectStatus(t, "post message B", res, http.StatusOK)
	res = c.do(http.MethodPost, "/internal/events", "", `{"type":"tool.call","roomId":"`+roomA+`","toolName":"bash"}`, nil)
	expectStatus(t, "worker event before delete", res, http.StatusAccepted)

	blobRoot := t.TempDir()
	content := []byte("shared artifact bytes")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	storageRef := tenant + "/" + digest
	if err := os.MkdirAll(filepath.Join(blobRoot, tenant), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blobRoot, storageRef), content, 0o640); err != nil {
		t.Fatal(err)
	}
	fixA := seedChildren(t, appPool, tenant, roomA, storageRef, len(content), "sha256:"+digest)
	seedChildren(t, appPool, tenant, roomB, storageRef, len(content), "sha256:"+digest)
	asTenant(t, appPool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO approval_rules (id, tenant_id, tool_name, scope, scope_id) VALUES ('rule_persona', $1, 'bash', 'persona', 'persona_x')`, tenant)
		return err
	})

	childCounts := func(roomID, artifactID string) map[string]int {
		out := map[string]int{}
		for _, table := range []string{"events", "messages", "artifacts", "approvals", "turns", "idempotency_keys"} {
			out[table] = countAs(t, appPool, tenant, `SELECT count(*) FROM `+table+` WHERE task_id = $1`, roomID)
		}
		out["artifact_versions"] = countAs(t, appPool, tenant, `SELECT count(*) FROM artifact_versions WHERE artifact_id = $1`, artifactID)
		out["rooms"] = countAs(t, appPool, tenant, `SELECT count(*) FROM rooms WHERE id = $1`, roomID)
		return out
	}
	for table, n := range childCounts(roomA, fixA.artifactID) {
		if n == 0 && table != "idempotency_keys" {
			t.Fatalf("precondition: %s has no rows for room A", table)
		}
	}
	if childCounts(roomA, fixA.artifactID)["idempotency_keys"] != 1 {
		t.Fatal("precondition: idempotency key for room A not stored")
	}

	sseReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/rooms/"+roomA+"/events", nil)
	sseReq.Header.Set("X-Test-User", owner)
	sseRes, err := http.DefaultClient.Do(sseReq)
	if err != nil || sseRes.StatusCode != http.StatusOK {
		t.Fatalf("open SSE: %v %v", sseRes, err)
	}
	sseReader := bufio.NewReader(sseRes.Body)
	if line, _ := sseReader.ReadString('\n'); !strings.HasPrefix(line, ": connected") {
		t.Fatalf("SSE preface = %q", line)
	}
	sseClosed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, sseReader)
		close(sseClosed)
	}()
	t.Cleanup(func() { sseRes.Body.Close() })

	seqBefore := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomA)
	roomsBefore := ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE tenant_id = $1`, tenant)
	missingRoom := c.do(http.MethodGet, "/v1/rooms/rm_does_not_exist", owner, "", nil)
	expectStatus(t, "missing room", missingRoom, http.StatusNotFound)
	notFoundBody := missingRoom.body

	// §18.7a / §17.4 order: 401 → CSRF 403 → 404.
	expectStatus(t, "delete unauthenticated", c.do(http.MethodDelete, "/v1/rooms/"+roomA, "", "", nil), http.StatusUnauthorized)
	expectStatus(t, "delete no X-Orbit-Request", c.do(http.MethodDelete, "/v1/rooms/"+roomA, owner, "", map[string]string{"Origin": testOrigin}), http.StatusForbidden)
	expectStatus(t, "delete bad origin", c.do(http.MethodDelete, "/v1/rooms/"+roomA, owner, "", map[string]string{"Origin": "https://evil.test", "X-Orbit-Request": "1"}), http.StatusForbidden)
	notOwner := c.deleteRoom(roomA, other, "")
	expectStatus(t, "delete by non-creator", notOwner, http.StatusNotFound)
	if notOwner.body != notFoundBody {
		t.Fatalf("non-creator 404 body %q != missing-room body %q", notOwner.body, notFoundBody)
	}
	if abortSawLive.Load() != 0 {
		t.Fatal("abort was sent for a delete that should have been rejected")
	}

	del := c.deleteRoom(roomA, owner, `{"deleted_by":"u-evil","deletedBy":"u-evil","deleted_at":"2000-01-01T00:00:00Z"}`)
	expectStatus(t, "delete", del, http.StatusNoContent)
	if abortSawLive.Load() != 1 {
		t.Fatal("workflow abort was not sent while the room was still live")
	}

	t.Run("a_api_returns_404", func(t *testing.T) {
		for _, path := range []string{"", "/messages", "/activity", "/events"} {
			got := c.do(http.MethodGet, "/v1/rooms/"+roomA+path, owner, "", nil)
			expectStatus(t, "GET room"+path, got, http.StatusNotFound)
			if got.body != notFoundBody {
				t.Fatalf("GET room%s body %q differs from missing-room body", path, got.body)
			}
		}
		for _, path := range []string{"/messages", "/steer", "/abort"} {
			got := c.do(http.MethodPost, "/v1/rooms/"+roomA+path, owner, `{"message":"x","instruction":"x"}`, nil)
			expectStatus(t, "POST room"+path, got, http.StatusNotFound)
		}
		list := c.do(http.MethodGet, "/v1/rooms", owner, "", nil)
		if strings.Contains(list.body, roomA) || !strings.Contains(list.body, roomB) {
			t.Fatalf("GET /v1/rooms after delete = %s", list.body)
		}
		approvals := c.do(http.MethodGet, "/v1/approvals", owner, "", nil)
		if strings.Contains(approvals.body, approvalA) || strings.Contains(approvals.body, roomA) {
			t.Fatalf("GET /v1/approvals still lists the deleted room: %s", approvals.body)
		}
		decide := c.do(http.MethodPost, "/v1/approvals/"+approvalA+"/decide", owner, `{"decision":"allow"}`, nil)
		missingApproval := c.do(http.MethodPost, "/v1/approvals/ap_missing/decide", owner, `{"decision":"allow"}`, nil)
		expectStatus(t, "decide deleted room approval", decide, http.StatusNotFound)
		if decide.body != missingApproval.body {
			t.Fatalf("decide 404 body %q != missing approval body %q", decide.body, missingApproval.body)
		}
	})

	t.Run("b_app_role_raw_sql_sees_nothing", func(t *testing.T) {
		for table, n := range childCounts(roomA, fixA.artifactID) {
			if n != 0 {
				t.Errorf("orbit_app still sees %d %s rows of the deleted room", n, table)
			}
		}
		fixB := "art_" + roomB
		for table, n := range childCounts(roomB, fixB) {
			if n == 0 && table != "idempotency_keys" {
				t.Errorf("surviving room lost its %s rows", table)
			}
		}
	})

	t.Run("c_sse_closed_and_reconnect_404", func(t *testing.T) {
		select {
		case <-sseClosed:
		case <-time.After(5 * time.Second):
			t.Fatal("SSE stream opened before the delete was not closed")
		}
		got := c.do(http.MethodGet, "/v1/rooms/"+roomA+"/events", owner, "", map[string]string{"Last-Event-ID": "1"})
		expectStatus(t, "SSE reconnect", got, http.StatusNotFound)
		if strings.HasPrefix(got.header.Get("Content-Type"), "text/event-stream") || strings.Contains(got.body, "data:") {
			t.Fatalf("SSE reconnect replayed events: %v %s", got.header, got.body)
		}
	})

	t.Run("d_idempotency_replay_404", func(t *testing.T) {
		got := c.do(http.MethodPost, "/v1/rooms", owner, createBody, map[string]string{"Idempotency-Key": idemKey})
		expectStatus(t, "replay after delete", got, http.StatusNotFound)
		if strings.Contains(got.body, roomA) || got.body != notFoundBody {
			t.Fatalf("replay after delete body %q", got.body)
		}
	})

	t.Run("e_body_deleted_by_ignored", func(t *testing.T) {
		by := ownerScalar[string](t, ownerPool, `SELECT deleted_by FROM rooms WHERE id = $1`, roomA)
		if by != owner {
			t.Fatalf("deleted_by is %q, want session user %q", by, owner)
		}
		at := ownerScalar[time.Time](t, ownerPool, `SELECT deleted_at FROM rooms WHERE id = $1`, roomA)
		if at.Before(startedAt.Add(-time.Minute)) {
			t.Fatalf("deleted_at %v came from the request body", at)
		}
	})

	t.Run("f_owner_audit_columns", func(t *testing.T) {
		var deletedAt *time.Time
		var deletedBy, createdBy string
		if err := ownerPool.QueryRow(ctx, `SELECT deleted_at, deleted_by, created_by FROM rooms WHERE id = $1`, roomA).
			Scan(&deletedAt, &deletedBy, &createdBy); err != nil {
			t.Fatal(err)
		}
		if deletedAt == nil || deletedBy != owner || createdBy != owner {
			t.Fatalf("audit columns: deletedAt %v, deletedBy %q, createdBy %q", deletedAt, deletedBy, createdBy)
		}
	})

	t.Run("g_repeat_delete_restrict_and_blob_kept", func(t *testing.T) {
		again := c.deleteRoom(roomA, owner, "")
		expectStatus(t, "second delete", again, http.StatusNotFound)
		if again.body != notFoundBody {
			t.Fatalf("second delete body %q", again.body)
		}
		_, err := ownerPool.Exec(ctx, `DELETE FROM rooms WHERE id = $1`, roomA)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("owner physical delete: err=%v, want foreign_key_violation (ON DELETE RESTRICT)", err)
		}
		if n := ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE id = $1`, roomA); n != 1 {
			t.Fatalf("room row count after RESTRICT = %d", n)
		}
		raw, err := os.ReadFile(filepath.Join(blobRoot, storageRef))
		if err != nil || !bytes.Equal(raw, content) {
			t.Fatalf("shared blob after delete: %v", err)
		}
		ref := ""
		asTenant(t, appPool, tenant, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT storage_ref FROM artifact_versions WHERE artifact_id = $1 AND version = 1`, "art_"+roomB).Scan(&ref)
		})
		if ref != storageRef {
			t.Fatalf("surviving room's artifact version storage_ref = %q", ref)
		}
	})

	t.Run("h_room_scoped_rule_hidden", func(t *testing.T) {
		if n := countAs(t, appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id = $1`, roomA); n != 0 {
			t.Fatalf("orbit_app sees %d room-scoped rules of the deleted room", n)
		}
		if n := countAs(t, appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id = $1`, roomB); n != 1 {
			t.Fatalf("surviving room's rule visible count = %d", n)
		}
		if n := countAs(t, appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id IS NULL`); n != 1 {
			t.Fatalf("persona-scoped rule visible count = %d", n)
		}
	})

	t.Run("i_definer_function_and_no_physical_delete", func(t *testing.T) {
		var secdef, ownerLogin, ownerBypass, aclSet, publicExec, appExec bool
		var fnOwner string
		var config []string
		if err := ownerPool.QueryRow(ctx, `
			SELECT p.prosecdef, r.rolname, r.rolcanlogin, r.rolbypassrls, COALESCE(p.proconfig, '{}'),
			       p.proacl IS NOT NULL,
			       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'),
			       has_function_privilege('orbit_app', p.oid, 'EXECUTE')
			  FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
			 WHERE p.proname = 'orbit_soft_delete_room'`).
			Scan(&secdef, &fnOwner, &ownerLogin, &ownerBypass, &config, &aclSet, &publicExec, &appExec); err != nil {
			t.Fatal(err)
		}
		if !secdef || fnOwner != "orbit_definer" || ownerLogin || !ownerBypass {
			t.Fatalf("function secdef=%v owner=%s login=%v bypassrls=%v", secdef, fnOwner, ownerLogin, ownerBypass)
		}
		if len(config) != 1 || config[0] != "search_path=pg_catalog, public" {
			t.Fatalf("function proconfig = %v", config)
		}
		if !aclSet || publicExec || !appExec {
			t.Fatalf("function acl set=%v public=%v orbit_app=%v", aclSet, publicExec, appExec)
		}
		var appBypass, appOwnsRooms, rls, force bool
		if err := ownerPool.QueryRow(ctx, `
			SELECT r.rolbypassrls, c.relowner = r.oid, c.relrowsecurity, c.relforcerowsecurity
			  FROM pg_class c, pg_roles r
			 WHERE c.relname = 'rooms' AND r.rolname = 'orbit_app'`).Scan(&appBypass, &appOwnsRooms, &rls, &force); err != nil {
			t.Fatal(err)
		}
		if appBypass || appOwnsRooms || !rls || !force {
			t.Fatalf("orbit_app bypass=%v owner=%v; rooms rls=%v force=%v", appBypass, appOwnsRooms, rls, force)
		}

		// The function re-checks tenant, creator and liveness itself.
		fn := func(tenantID, roomID, user string) int {
			var n int
			err := pgx.BeginFunc(ctx, appPool, func(tx pgx.Tx) error {
				if tenantID != "" {
					if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
						return err
					}
				}
				return tx.QueryRow(ctx, `SELECT orbit_soft_delete_room($1, $2)`, roomID, user).Scan(&n)
			})
			if err != nil {
				t.Fatalf("orbit_soft_delete_room: %v", err)
			}
			return n
		}
		if n := fn("", roomB, owner); n != 0 {
			t.Fatalf("function without tenant affected %d rows", n)
		}
		if n := fn("t-other", roomB, owner); n != 0 {
			t.Fatalf("function with foreign tenant affected %d rows", n)
		}
		if n := fn(tenant, roomB, other); n != 0 {
			t.Fatalf("function for non-creator affected %d rows", n)
		}
		if n := fn(tenant, roomA, owner); n != 0 {
			t.Fatalf("function on already-deleted room affected %d rows", n)
		}

		var affected int64
		asTenant(t, appPool, tenant, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `DELETE FROM rooms`)
			affected = tag.RowsAffected()
			return err
		})
		if affected != 0 {
			t.Fatalf("orbit_app DELETE FROM rooms affected %d rows", affected)
		}
		if live := ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, roomB); !live {
			t.Fatal("surviving room was deleted")
		}
	})

	t.Run("j_late_worker_event_and_replay", func(t *testing.T) {
		for _, body := range []string{
			`{"type":"tool.call","roomId":"` + roomA + `","toolName":"bash"}`,
			`{"type":"tool.result","sessionId":"` + created.SessionID + `","toolName":"bash"}`,
		} {
			got := c.do(http.MethodPost, "/internal/events", "", body, nil)
			expectStatus(t, "late worker event", got, http.StatusNotFound)
		}
		if seq := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomA); seq != seqBefore {
			t.Fatalf("last_event_seq moved from %d to %d", seqBefore, seq)
		}
		got := c.do(http.MethodPost, "/v1/rooms", owner, createBody, map[string]string{"Idempotency-Key": idemKey})
		expectStatus(t, "replay after delete", got, http.StatusNotFound)
		if n := ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE tenant_id = $1`, tenant); n != roomsBefore {
			t.Fatalf("room count changed from %d to %d", roomsBefore, n)
		}
	})

	if strings.Contains(logs.String(), "room_delete_abort_failed") {
		t.Fatalf("unexpected abort warning: %s", logs.String())
	}
}

// fakeOrch stubs Temporal for S-DB-13 (k).
type fakeOrch struct {
	mode        string // "timeout" or "error"
	aborts      atomic.Int32
	onAbort     func(roomID string)
	sessionSeed atomic.Int64
}

func (f *fakeOrch) StartRoom(_ context.Context, roomID, kind, _ string) (orch.RoomView, error) {
	return orch.RoomView{RoomID: roomID, State: "running", Kind: kind, SessionID: fmt.Sprintf("sess-k-%d", f.sessionSeed.Add(1))}, nil
}
func (f *fakeOrch) RunTurn(context.Context, string, string, string) (orch.RunTurnResult, error) {
	return orch.RunTurnResult{Status: "completed"}, nil
}
func (f *fakeOrch) Decide(context.Context, string, string, string, string, string) (orch.DecideResult, error) {
	return orch.DecideResult{}, nil
}
func (f *fakeOrch) Steer(context.Context, string, string, string) error { return nil }
func (f *fakeOrch) Abort(ctx context.Context, roomID, _, _ string) error {
	f.aborts.Add(1)
	if f.onAbort != nil {
		f.onAbort(roomID)
	}
	if f.mode == "timeout" {
		<-ctx.Done()
		return ctx.Err()
	}
	return errors.New("temporal signal failed: token=sk-live-SDB13-SECRET")
}

func TestSDB13kAbortFailureStillSoftDeletes(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const tenant, owner = "t-sdb13k", "u-owner-k"
	ownerPool := newPool(t, ownerURL, 2)
	repo, _ := appStore(t, 4)
	if err := repo.EnsureTenant(ctx, tenant, tenant); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"timeout", "error"} {
		t.Run(mode, func(t *testing.T) {
			var logs syncBuffer
			var liveAtAbort atomic.Bool
			fo := &fakeOrch{mode: mode}
			fo.onAbort = func(roomID string) {
				liveAtAbort.Store(ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, roomID))
			}
			runtime := app.NewWithOptions(app.Options{
				Worker: worker.New(""), Orch: fo, Repo: repo, Log: log.New(&logs, "", 0),
				DefaultTenant: tenant, AbortTimeout: 200 * time.Millisecond,
			})
			srv := httptest.NewServer(httpapi.HandlerWithOptions(runtime, httpapi.Options{
				Auth: headerAuth(tenant), AllowedOrigins: []string{testOrigin},
			}))
			defer srv.Close()
			c := apiClient{t: t, base: srv.URL}

			res := c.do(http.MethodPost, "/v1/rooms", owner, `{"kind":"solo"}`, nil)
			expectStatus(t, "create", res, http.StatusOK)
			var room app.Room
			_ = json.Unmarshal([]byte(res.body), &room)

			start := time.Now()
			expectStatus(t, "delete with failing abort", c.deleteRoom(room.ID, owner, ""), http.StatusNoContent)
			if mode == "timeout" && time.Since(start) > 5*time.Second {
				t.Fatalf("delete waited %v for a hung abort", time.Since(start))
			}
			if fo.aborts.Load() != 1 || !liveAtAbort.Load() {
				t.Fatalf("abort calls=%d, room live at abort=%v", fo.aborts.Load(), liveAtAbort.Load())
			}
			var deletedBy *string
			if err := ownerPool.QueryRow(ctx, `SELECT deleted_by FROM rooms WHERE id = $1 AND deleted_at IS NOT NULL`, room.ID).Scan(&deletedBy); err != nil || deletedBy == nil || *deletedBy != owner {
				t.Fatalf("room not soft-deleted after abort %s: by=%v err=%v", mode, deletedBy, err)
			}
			out := logs.String()
			if !strings.Contains(out, "WARN") || !strings.Contains(out, "task="+room.ID) || !strings.Contains(out, "reason="+mode) {
				t.Fatalf("missing abort warning with task id: %q", out)
			}
			if strings.Contains(out, "sk-live") || strings.Contains(out, "token=") {
				t.Fatalf("abort warning leaked secret material: %q", out)
			}
			expectStatus(t, "get after delete", c.do(http.MethodGet, "/v1/rooms/"+room.ID, owner, "", nil), http.StatusNotFound)
		})
	}
}
