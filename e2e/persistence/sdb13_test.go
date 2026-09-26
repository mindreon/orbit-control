//go:build e2e

package persistence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/app"
)

type sqlReq struct {
	Role   string `json:"role"`
	Tenant string `json:"tenant,omitempty"`
	SQL    string `json:"sql"`
	Args   []any  `json:"args,omitempty"`
}

func iso(t *testing.T, id, contract string, fms []string, desc string, req any, expected, actual any, pass bool) bool {
	t.Helper()
	return record(t, caseInput{ID: id, Contract: contract, Kind: "isolated", FailureModes: fms,
		Description: desc, Request: req, Expected: expected, Actual: actual, Pass: pass})
}

func ownerScalar[T any](t *testing.T, owner *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := owner.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatalf("owner query %q: %v", query, err)
	}
	return v
}

// seedChildren writes, as orbit_app, the child rows phase 1 has no API for
// (turns, events, artifacts, artifact_versions, a room-scoped rule).
func seedChildren(t *testing.T, pool *pgxpool.Pool, tenant, room, storageRef, digest string, size int) {
	t.Helper()
	ctx := context.Background()
	err := asTenant(ctx, pool, tenant, func(tx pgx.Tx) error {
		for _, s := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO turns (id, tenant_id, task_id, kind, status) VALUES ($1, $2, $3, 'message', 'completed')`, []any{"tn_" + room, tenant, room}},
			{`INSERT INTO events (tenant_id, task_id, seq, event_uid, type, source) VALUES ($1, $2, 1, $3, 'tool.call', 'worker')`, []any{tenant, room, "ev_" + room}},
			{`INSERT INTO artifacts (id, tenant_id, task_id, title, latest_version) VALUES ($1, $2, $3, 'report', 1)`, []any{"art_" + room, tenant, room}},
			{`INSERT INTO artifact_versions (artifact_id, tenant_id, version, mime_type, size_bytes, content_digest, previewable, storage_ref)
			  VALUES ($1, $2, 1, 'text/plain', $3, $4, true, $5)`, []any{"art_" + room, tenant, size, digest, storageRef}},
			{`INSERT INTO approval_rules (id, tenant_id, tool_name, scope, scope_id, room_id) VALUES ($1, $2, 'bash', 'room', $3, $3)`, []any{"rule_" + room, tenant, room}},
		} {
			if _, err := tx.Exec(ctx, s.sql, s.args...); err != nil {
				return fmt.Errorf("%s: %w", s.sql, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed children for %s: %v", room, err)
	}
}

var childTables = []string{"rooms", "events", "messages", "turns", "approvals", "idempotency_keys", "artifacts", "artifact_versions"}

func childCountSQL(table string) string {
	switch table {
	case "rooms":
		return `SELECT count(*) FROM rooms WHERE id = $1`
	case "artifact_versions":
		return `SELECT count(*) FROM artifact_versions WHERE artifact_id = 'art_' || $1`
	default:
		return `SELECT count(*) FROM ` + table + ` WHERE task_id = $1`
	}
}

func childCounts(t *testing.T, pool *pgxpool.Pool, tenant, room string) map[string]int {
	out := map[string]int{}
	for _, table := range childTables {
		out[table] = countAs(t, pool, tenant, childCountSQL(table), room)
	}
	return out
}

func TestSDB13SoftDelete(t *testing.T) {
	t.Setenv("ORBIT_INTERNAL_TOKEN", "")
	ctx := context.Background()
	const tenant, owner, other = "t-sdb13", "u-owner", "u-other"
	const s13 = "S-DB-13"
	started := time.Now().UTC()
	ownerPool := newPool(t, ownerURL, 2)

	var roomA string
	var abortSawLive, abortCalls atomic.Int32
	wk := stubWorker(t, func(id string) {
		if id == roomA {
			abortCalls.Add(1)
			if ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, id) {
				abortSawLive.Add(1)
			}
		}
	})
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})

	missing := srv.check(t, "S-DB-13/setup/missing-room", s13, "baseline 404 body for a room that never existed",
		httpReq{Method: "GET", Path: "/v1/rooms/rm_does_not_exist", Headers: user(owner)}, httpExp{Status: 404})
	notFound := missing.Body

	createBody := `{"kind":"solo","title":"to delete"}`
	const idemKey = "e2e-sdb13-create-A"
	createA := httpReq{Method: "POST", Path: "/v1/rooms", Headers: withHeader(user(owner), "Idempotency-Key", idemKey), Body: createBody}
	roomA = roomID(t, srv.check(t, "S-DB-13/setup/create-A", s13, "create the task to delete with an Idempotency-Key", createA, httpExp{Status: 200}))
	alias(roomA, "<room-A>")
	srv.check(t, "S-DB-13/setup/replay-A-live", s13, "same key + body replays the live task",
		createA, httpExp{Status: 200, BodyIncludes: []string{roomA}, Headers: map[string]string{"Idempotent-Replayed": "true"}})
	roomB := roomID(t, srv.check(t, "S-DB-13/setup/create-B", s13, "create the surviving task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(owner), Body: `{"kind":"solo","title":"survivor"}`}, httpExp{Status: 200}))
	alias(roomB, "<room-B>")

	posted := srv.check(t, "S-DB-13/setup/message-A", s13, "message on A parks an approval",
		httpReq{Method: "POST", Path: "/v1/rooms/" + roomA + "/messages", Headers: user(owner), Body: `{"message":"list files"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
	var postedBody struct {
		Approval *app.Approval `json:"approval"`
	}
	_ = json.Unmarshal([]byte(posted.Body), &postedBody)
	if postedBody.Approval == nil {
		t.Fatalf("no approval on A: %s", posted.Body)
	}
	approvalA := postedBody.Approval.ID
	alias(approvalA, "<approval-A>")
	srv.check(t, "S-DB-13/setup/message-B", s13, "message on B",
		httpReq{Method: "POST", Path: "/v1/rooms/" + roomB + "/messages", Headers: user(owner), Body: `{"message":"hello"}`}, httpExp{Status: 200})
	srv.check(t, "S-DB-13/setup/worker-event-A", s13, "worker event on the live task is accepted",
		httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.call","roomId":"` + roomA + `","toolName":"bash"}`}, httpExp{Status: 202})
	var sessionA string
	_ = ownerPool.QueryRow(ctx, `SELECT session_id FROM rooms WHERE id = $1`, roomA).Scan(&sessionA)

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
	seedChildren(t, srv.appPool, tenant, roomA, storageRef, "sha256:"+digest, len(content))
	seedChildren(t, srv.appPool, tenant, roomB, storageRef, "sha256:"+digest, len(content))
	if err := asTenant(ctx, srv.appPool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO approval_rules (id, tenant_id, tool_name, scope, scope_id) VALUES ('rule_persona', $1, 'bash', 'persona', 'persona_x')`, tenant)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	before := childCounts(t, srv.appPool, tenant, roomA)
	allPresent := true
	for _, n := range before {
		allPresent = allPresent && n > 0
	}
	iso(t, "S-DB-13(b)/baseline", "S-DB-13 (b)", []string{"FM-8"}, "before delete, orbit_app sees every child table of A (rules out a vacuous 0)",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "SELECT count(*) FROM <table> WHERE task_id = <A>"}, "every table > 0", before, allPresent)

	sseReq, _ := http.NewRequest(http.MethodGet, srv.base+"/v1/rooms/"+roomA+"/events", nil)
	sseReq.Header.Set(userHeader, owner)
	sseRes, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sseRes.Body.Close() })
	sseReader := bufio.NewReader(sseRes.Body)
	preface, _ := sseReader.ReadString('\n')
	record(t, caseInput{ID: "S-DB-13/setup/sse-open", Contract: s13, Description: "SSE stream on A is open before the delete",
		Request:  httpReq{Method: "GET", Path: "/v1/rooms/" + roomA + "/events", Headers: user(owner)},
		Expected: map[string]any{"status": 200, "firstLine": ": connected"},
		Actual:   map[string]any{"status": sseRes.StatusCode, "firstLine": strings.TrimSpace(preface)},
		Pass:     sseRes.StatusCode == 200 && strings.HasPrefix(preface, ": connected")})
	sseClosed := make(chan time.Time, 1)
	go func() {
		buf := make([]byte, 512)
		for {
			if _, err := sseReader.Read(buf); err != nil {
				sseClosed <- time.Now()
				return
			}
		}
	}()

	seqBefore := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomA)
	roomsBefore := ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE tenant_id = $1`, tenant)

	// §18.7a / §17.4 order: 401 → CSRF 403 → 404.
	del := func(u map[string]string, body string) httpReq {
		return httpReq{Method: "DELETE", Path: "/v1/rooms/" + roomA, Headers: u, Body: body}
	}
	srv.check(t, "S-DB-13/order/401-unauthenticated", "§18.7a", "no session → 401, even without CSRF headers",
		del(nil, ""), httpExp{Status: 401, BodyIncludes: []string{"UNAUTHENTICATED"}})
	srv.check(t, "S-DB-13/order/403-no-x-orbit-request", "§18.7a", "allowed Origin but no X-Orbit-Request → 403",
		del(map[string]string{userHeader: owner, "Origin": testOrigin}, ""), httpExp{Status: 403, BodyIncludes: []string{"CSRF_REJECTED"}})
	srv.check(t, "S-DB-13/order/403-bad-origin", "§18.7a", "X-Orbit-Request but foreign Origin → 403",
		del(map[string]string{userHeader: owner, "Origin": "https://evil.test", "X-Orbit-Request": "1"}, ""), httpExp{Status: 403, BodyIncludes: []string{"CSRF_REJECTED"}})
	srv.check(t, "S-DB-13/order/404-not-creator", "§18.7a", "another user of the same tenant → same 404 as a missing room",
		del(userCSRF(other), ""), httpExp{Status: 404, BodyEquals: notFound})
	record(t, caseInput{ID: "S-DB-13/order/no-abort-when-rejected", Contract: "§18.7a",
		Description: "rejected deletes never reach the workflow abort", Request: "the four rejected DELETEs above",
		Expected: 0, Actual: abortCalls.Load(), Pass: abortCalls.Load() == 0})

	srv.check(t, "S-DB-13/delete", "§18.7a", "creator deletes with a forged body; soft delete via orbit_app with RLS on",
		del(userCSRF(owner), `{"deleted_by":"u-evil","deletedBy":"u-evil","deleted_at":"2000-01-01T00:00:00Z"}`), httpExp{Status: 204})
	iso(t, "S-DB-13/abort-before-delete", "§18.7a", []string{"FM-24"}, "worker abort ran while the room was still live",
		sqlReq{Role: "orbit_owner", SQL: "SELECT deleted_at IS NULL FROM rooms WHERE id = <A> (inside the abort activity)"},
		map[string]int{"abortCalls": 1, "liveAtAbort": 1}, map[string]int32{"abortCalls": abortCalls.Load(), "liveAtAbort": abortSawLive.Load()},
		abortCalls.Load() == 1 && abortSawLive.Load() == 1)

	// (a) every task-scoped API returns the same 404.
	for _, p := range []string{"", "/messages", "/activity", "/events"} {
		srv.check(t, "S-DB-13(a)/GET"+p, "S-DB-13 (a)", "GET room"+p+" after delete",
			httpReq{Method: "GET", Path: "/v1/rooms/" + roomA + p, Headers: user(owner)}, httpExp{Status: 404, BodyEquals: notFound})
	}
	for _, p := range []string{"/messages", "/steer", "/abort"} {
		srv.check(t, "S-DB-13(a)/POST"+p, "S-DB-13 (a)", "POST room"+p+" after delete",
			httpReq{Method: "POST", Path: "/v1/rooms/" + roomA + p, Headers: user(owner), Body: `{"message":"x","instruction":"x"}`},
			httpExp{Status: 404, BodyEquals: notFound})
	}
	srv.check(t, "S-DB-13(a)/list-rooms", "S-DB-13 (a)", "GET /v1/rooms no longer lists A, still lists B",
		httpReq{Method: "GET", Path: "/v1/rooms", Headers: user(owner)}, httpExp{Status: 200, BodyIncludes: []string{roomB}, BodyExcludes: []string{roomA}})
	srv.check(t, "S-DB-13(a)/list-approvals", "S-DB-13 (a)", "GET /v1/approvals drops A's approval",
		httpReq{Method: "GET", Path: "/v1/approvals", Headers: user(owner)}, httpExp{Status: 200, BodyExcludes: []string{approvalA, roomA}})
	missingApproval := srv.check(t, "S-DB-13/setup/missing-approval", s13, "baseline 404 for an approval that never existed",
		httpReq{Method: "POST", Path: "/v1/approvals/ap_missing/decide", Headers: user(owner), Body: `{"decision":"allow"}`}, httpExp{Status: 404})
	srv.check(t, "S-DB-13(a)/decide", "S-DB-13 (a)", "deciding A's approval → same 404 as a missing approval",
		httpReq{Method: "POST", Path: "/v1/approvals/" + approvalA + "/decide", Headers: user(owner), Body: `{"decision":"allow"}`},
		httpExp{Status: 404, BodyEquals: missingApproval.Body})

	// (b) RLS backstop with no deleted_at predicate.
	after := childCounts(t, srv.appPool, tenant, roomA)
	zero := true
	for _, n := range after {
		zero = zero && n == 0
	}
	iso(t, "S-DB-13(b)/deleted-task-invisible", "S-DB-13 (b)", []string{"FM-8", "FM-9", "FM-10"},
		"orbit_app raw SQL without any deleted_at condition sees nothing of A",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "SELECT count(*) FROM <table> WHERE task_id = <A> (artifact_versions by artifact_id)"},
		"every table = 0", after, zero)
	survivor := childCounts(t, srv.appPool, tenant, roomB)
	delete(survivor, "idempotency_keys") // B was created without a key
	kept := true
	for _, n := range survivor {
		kept = kept && n > 0
	}
	iso(t, "S-DB-13(b)/survivor-visible", "S-DB-13 (b)", []string{"FM-8"}, "B's rows are untouched",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "SELECT count(*) FROM <table> WHERE task_id = <B> (B has no idempotency key)"}, "every table > 0", survivor, kept)

	// (c) live SSE closed; reconnect with Last-Event-ID → 404, no replay.
	var closedAfter string
	select {
	case at := <-sseClosed:
		closedAfter = "closed"
		_ = at
	case <-time.After(5 * time.Second):
		closedAfter = "still open after 5s"
	}
	record(t, caseInput{ID: "S-DB-13(c)/sse-closed", Contract: "S-DB-13 (c)", Description: "the SSE stream opened before the delete is closed by control",
		Request: "stream from S-DB-13/setup/sse-open", Expected: "closed", Actual: closedAfter, Pass: closedAfter == "closed"})
	srv.check(t, "S-DB-13(c)/reconnect-last-event-id", "S-DB-13 (c)", "reconnect with Last-Event-ID → 404 JSON, no stream, no replay",
		httpReq{Method: "GET", Path: "/v1/rooms/" + roomA + "/events", Headers: withHeader(user(owner), "Last-Event-ID", "1")},
		httpExp{Status: 404, BodyEquals: notFound, BodyExcludes: []string{"data:"}, NotContentType: "text/event-stream"})

	// (d) Idempotency-Key replay after delete.
	srv.check(t, "S-DB-13(d)/replay", "S-DB-13 (d)", "replaying the create → 404 identical to a missing room, no task id",
		createA, httpExp{Status: 404, BodyEquals: notFound, BodyExcludes: []string{roomA}})

	// (e)(f) audit columns via the owner role.
	var deletedAt *time.Time
	var deletedBy, createdBy *string
	if err := ownerPool.QueryRow(ctx, `SELECT deleted_at, deleted_by, created_by FROM rooms WHERE id = $1`, roomA).Scan(&deletedAt, &deletedBy, &createdBy); err != nil {
		t.Fatal(err)
	}
	auditSQL := sqlReq{Role: "orbit_owner", SQL: "SELECT deleted_at, deleted_by, created_by FROM rooms WHERE id = <A>"}
	iso(t, "S-DB-13(e)/body-ignored", "S-DB-13 (e)", []string{"FM-11"}, "deleted_by/deleted_at from the request body are ignored",
		auditSQL, map[string]string{"deletedBy": owner, "deletedAt": "after test start, not 2000-01-01"},
		map[string]any{"deletedBy": deletedBy, "deletedAt": deletedAt},
		deletedBy != nil && *deletedBy == owner && deletedAt != nil && deletedAt.After(started.Add(-time.Minute)))
	iso(t, "S-DB-13(f)/audit-recorded", "S-DB-13 (f)", []string{"FM-12"}, "owner role reads deleted_at set and deleted_by equal to the session user",
		auditSQL, map[string]any{"deletedAt": "not null", "deletedBy": owner, "createdBy": owner},
		map[string]any{"deletedAt": deletedAt, "deletedBy": deletedBy, "createdBy": createdBy},
		deletedAt != nil && deletedBy != nil && *deletedBy == owner && createdBy != nil && *createdBy == owner)

	// (g) repeat delete, RESTRICT, shared blob.
	srv.check(t, "S-DB-13(g)/delete-again", "S-DB-13 (g)", "second DELETE → 404",
		del(userCSRF(owner), ""), httpExp{Status: 404, BodyEquals: notFound})
	_, hardErr := ownerPool.Exec(ctx, `DELETE FROM rooms WHERE id = $1`, roomA)
	iso(t, "S-DB-13(g)/owner-hard-delete-restrict", "S-DB-13 (g)", []string{"FM-13"}, "owner physical delete is blocked by ON DELETE RESTRICT",
		sqlReq{Role: "orbit_owner", SQL: "DELETE FROM rooms WHERE id = $1", Args: []any{roomA}}, "23503", sqlState(hardErr), sqlState(hardErr) == "23503")
	raw, readErr := os.ReadFile(filepath.Join(blobRoot, storageRef))
	var refB string
	_ = asTenant(ctx, srv.appPool, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT storage_ref FROM artifact_versions WHERE artifact_id = $1 AND version = 1`, "art_"+roomB).Scan(&refB)
	})
	iso(t, "S-DB-13(g)/shared-blob-kept", "S-DB-13 (g)", []string{"FM-14"}, "blob shared with B still exists and B's version row still references it",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "SELECT storage_ref FROM artifact_versions WHERE artifact_id = <B's artifact>"},
		map[string]any{"fileReadable": true, "storageRefB": storageRef},
		map[string]any{"fileReadable": readErr == nil && string(raw) == string(content), "storageRefB": refB},
		readErr == nil && string(raw) == string(content) && refB == storageRef)

	// (h) room-scoped rules.
	ruleCounts := map[string]int{
		"roomA":   countAs(t, srv.appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id = $1`, roomA),
		"roomB":   countAs(t, srv.appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id = $1`, roomB),
		"persona": countAs(t, srv.appPool, tenant, `SELECT count(*) FROM approval_rules WHERE room_id IS NULL`),
	}
	iso(t, "S-DB-13(h)/room-rule-hidden", "S-DB-13 (h)", []string{"FM-15", "FM-16"},
		"orbit_app sees no rule of the deleted room; persona and other-room rules stay",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "SELECT count(*) FROM approval_rules WHERE room_id = <A> | <B> | IS NULL"},
		map[string]int{"roomA": 0, "roomB": 1, "persona": 1}, ruleCounts,
		ruleCounts["roomA"] == 0 && ruleCounts["roomB"] == 1 && ruleCounts["persona"] == 1)

	checkSoftDeleteFunction(t, ownerPool, srv.appPool, tenant, owner, other, roomA, roomB)

	// (j) late worker events and replay after delete.
	srv.check(t, "S-DB-13(j)/late-event-room-id", "S-DB-13 (j)", "late worker event naming the deleted room → 404, not 500",
		httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.call","roomId":"` + roomA + `","toolName":"bash"}`}, httpExp{Status: 404})
	srv.check(t, "S-DB-13(j)/late-event-session-id", "S-DB-13 (j)", "late worker event carrying only the session id → 404",
		httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.result","sessionId":"` + sessionA + `","toolName":"bash"}`}, httpExp{Status: 404})
	seqAfter := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomA)
	iso(t, "S-DB-13(j)/seq-unchanged", "S-DB-13 (j)", []string{"FM-23"}, "last_event_seq of the deleted room did not move",
		sqlReq{Role: "orbit_owner", SQL: "SELECT last_event_seq FROM rooms WHERE id = <A>"}, seqBefore, seqAfter, seqAfter == seqBefore)
	srv.check(t, "S-DB-13(j)/replay", "S-DB-13 (j)", "Idempotency-Key replay after delete → 404",
		createA, httpExp{Status: 404, BodyEquals: notFound})
	roomsAfter := ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE tenant_id = $1`, tenant)
	iso(t, "S-DB-13(j)/no-new-task", "S-DB-13 (j)", []string{"FM-23"}, "replays after delete created no task",
		sqlReq{Role: "orbit_owner", SQL: "SELECT count(*) FROM rooms WHERE tenant_id = <tenant>"}, roomsBefore, roomsAfter, roomsAfter == roomsBefore)

	record(t, caseInput{ID: "S-DB-13/no-abort-warning", Contract: "§18.7a", Description: "a successful abort logs no warning",
		Request: "server log", Expected: "no room_delete_abort_failed", Actual: logLines(srv.logs.String()),
		Pass: !strings.Contains(srv.logs.String(), "room_delete_abort_failed")})
}

func logLines(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// checkSoftDeleteFunction covers S-DB-13 (i).
func checkSoftDeleteFunction(t *testing.T, ownerPool, appPool *pgxpool.Pool, tenant, owner, other, roomA, roomB string) {
	ctx := context.Background()
	const c = "S-DB-13 (i)"

	var secdef, ownerLogin, ownerBypass, ownerSuper, aclSet, publicExec, appExec bool
	var fnOwner string
	var config, acl []string
	catalogSQL := `
		SELECT p.prosecdef, r.rolname, r.rolcanlogin, r.rolbypassrls, r.rolsuper,
		       COALESCE(p.proconfig, '{}'), p.proacl IS NOT NULL, COALESCE(p.proacl::text[], '{}'),
		       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'),
		       has_function_privilege('orbit_app', p.oid, 'EXECUTE')
		  FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
		 WHERE p.oid = 'public.orbit_soft_delete_room(text, text)'::regprocedure`
	if err := ownerPool.QueryRow(ctx, catalogSQL).Scan(&secdef, &fnOwner, &ownerLogin, &ownerBypass, &ownerSuper,
		&config, &aclSet, &acl, &publicExec, &appExec); err != nil {
		t.Fatal(err)
	}
	iso(t, "S-DB-13(i)/function-definition", c, []string{"FM-17", "FM-20"}, "definer-rights function owned by NOLOGIN BYPASSRLS orbit_definer, fixed search_path",
		sqlReq{Role: "orbit_owner", SQL: catalogSQL},
		map[string]any{"securityDefiner": true, "owner": "orbit_definer", "ownerCanLogin": false, "ownerBypassRLS": true, "ownerSuperuser": false, "proconfig": []string{"search_path=pg_catalog, public"}},
		map[string]any{"securityDefiner": secdef, "owner": fnOwner, "ownerCanLogin": ownerLogin, "ownerBypassRLS": ownerBypass, "ownerSuperuser": ownerSuper, "proconfig": config},
		secdef && fnOwner == "orbit_definer" && !ownerLogin && ownerBypass && !ownerSuper && len(config) == 1 && config[0] == "search_path=pg_catalog, public")
	iso(t, "S-DB-13(i)/function-acl", c, []string{"FM-18", "FM-19"}, "EXECUTE is granted to orbit_app only; no PUBLIC entry",
		sqlReq{Role: "orbit_owner", SQL: catalogSQL},
		map[string]any{"acl": []string{"orbit_app=X/orbit_definer"}, "public": false, "orbitApp": true},
		map[string]any{"acl": acl, "public": publicExec, "orbitApp": appExec},
		aclSet && !publicExec && appExec && len(acl) == 1 && acl[0] == "orbit_app=X/orbit_definer")

	// Any role other than orbit_app must get a permission error.
	opsPool := newPool(t, opsURL, 1)
	call := func(pool *pgxpool.Pool, setRole string) error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenant); err != nil {
				return err
			}
			if setRole == "orbit_definer" {
				if _, err := tx.Exec(ctx, "SET LOCAL ROLE orbit_definer"); err != nil {
					return err
				}
			}
			var n int
			return tx.QueryRow(ctx, `SELECT public.orbit_soft_delete_room($1, $2)`, roomB, owner).Scan(&n)
		})
	}
	for _, role := range []string{"orbit_owner", "orbit_definer", "orbit_ops"} {
		pool := ownerPool
		if role == "orbit_ops" {
			pool = opsPool
		}
		err := call(pool, role)
		iso(t, "S-DB-13(i)/execute-denied-"+role, c, []string{"FM-18", "FM-19"}, "calling the function as "+role+" (any role other than orbit_app) fails with 42501 permission denied",
			sqlReq{Role: role, Tenant: tenant, SQL: "SELECT public.orbit_soft_delete_room($1, $2)", Args: []any{roomB, owner}}, map[string]string{"sqlstate": "42501", "error": "permission denied"}, map[string]string{"sqlstate": sqlState(err), "error": privilegeDenied(err)},
			sqlState(err) == "42501" && privilegeDenied(err) == "permission denied")
	}

	fn := func(tenantID, room, u string) (int, error) {
		var n int
		err := asTenant(ctx, appPool, tenantID, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT public.orbit_soft_delete_room($1, $2)`, room, u).Scan(&n)
		})
		return n, err
	}
	for _, probe := range []struct{ id, tenant, room, user, why string }{
		{"no-tenant", "", roomB, owner, "without app.tenant_id"},
		{"foreign-tenant", "t-other", roomB, owner, "with another tenant"},
		{"not-creator", tenant, roomB, other, "for a user who did not create the room"},
		{"already-deleted", tenant, roomA, owner, "on the already-deleted room"},
	} {
		n, err := fn(probe.tenant, probe.room, probe.user)
		iso(t, "S-DB-13(i)/function-guard-"+probe.id, c, []string{"FM-21"}, "orbit_app call "+probe.why+" affects 0 rows",
			sqlReq{Role: "orbit_app", Tenant: probe.tenant, SQL: "SELECT public.orbit_soft_delete_room($1, $2)", Args: []any{probe.room, probe.user}},
			map[string]any{"rows": 0, "sqlstate": "ok"}, map[string]any{"rows": n, "sqlstate": sqlState(err)}, err == nil && n == 0)
	}

	roomCount := func() int {
		return ownerScalar[int](t, ownerPool, `SELECT count(*) FROM rooms WHERE tenant_id = $1`, tenant)
	}
	roomsBefore := roomCount()
	var affected int64
	delErr := asTenant(ctx, appPool, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM rooms`)
		affected = tag.RowsAffected()
		return err
	})
	roomsAfter := roomCount()
	bLive := ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, roomB)
	iso(t, "S-DB-13(i)/app-physical-delete-denied", c, []string{"FM-22"}, "orbit_app DELETE FROM rooms fails with 42501 permission denied (C32 rev3); room count unchanged; B stays live",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "DELETE FROM rooms"},
		map[string]any{"sqlstate": "42501", "error": "permission denied", "roomCountUnchanged": true, "roomBLive": true},
		map[string]any{"sqlstate": sqlState(delErr), "error": privilegeDenied(delErr), "rowsAffected": affected, "roomsBefore": roomsBefore, "roomsAfter": roomsAfter, "roomBLive": bLive},
		sqlState(delErr) == "42501" && privilegeDenied(delErr) == "permission denied" && roomsAfter == roomsBefore && roomsBefore >= 2 && bLive)

	var appBypass, appOwnsRooms, rls, force bool
	roleSQL := `SELECT r.rolbypassrls, c.relowner = r.oid, c.relrowsecurity, c.relforcerowsecurity
	              FROM pg_class c, pg_roles r WHERE c.oid = 'public.rooms'::regclass AND r.rolname = 'orbit_app'`
	if err := ownerPool.QueryRow(ctx, roleSQL).Scan(&appBypass, &appOwnsRooms, &rls, &force); err != nil {
		t.Fatal(err)
	}
	iso(t, "S-DB-13(i)/app-role-under-rls", c, []string{"FM-8", "FM-17"}, "orbit_app is neither owner nor BYPASSRLS; rooms has RLS + FORCE",
		sqlReq{Role: "orbit_owner", SQL: roleSQL},
		map[string]bool{"appBypassRLS": false, "appOwnsRooms": false, "rls": true, "force": true},
		map[string]bool{"appBypassRLS": appBypass, "appOwnsRooms": appOwnsRooms, "rls": rls, "force": force},
		!appBypass && !appOwnsRooms && rls && force)
}

// TestSDB13kAbortFailureStillSoftDeletes: Temporal stubbed to time out or fail.
func TestSDB13kAbortFailureStillSoftDeletes(t *testing.T) {
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	for _, mode := range []string{"timeout", "error"} {
		t.Run(mode, func(t *testing.T) {
			tenant, owner := "t-sdb13k-"+mode, "u-owner-k"
			var liveAtAbort atomic.Bool
			so := &stubOrch{mode: mode}
			so.onAbort = func(id string) {
				liveAtAbort.Store(ownerScalar[bool](t, ownerPool, `SELECT deleted_at IS NULL FROM rooms WHERE id = $1`, id))
			}
			srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, orch: so, abortTimeout: 200 * time.Millisecond})
			id := "S-DB-13(k)/" + mode
			room := roomID(t, srv.check(t, id+"/create", "S-DB-13 (k)", "create through the (stubbed) Temporal path",
				httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(owner), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
			alias(room, "<room-k-"+mode+">")
			start := time.Now()
			srv.check(t, id+"/delete", "S-DB-13 (k)", "DELETE still returns 204 when the workflow abort "+map[string]string{"timeout": "times out", "error": "fails"}[mode],
				httpReq{Method: "DELETE", Path: "/v1/rooms/" + room, Headers: userCSRF(owner)}, httpExp{Status: 204})
			elapsed := time.Since(start)
			srv.check(t, id+"/get-after", "S-DB-13 (k)", "the room is gone from the API",
				httpReq{Method: "GET", Path: "/v1/rooms/" + room, Headers: user(owner)}, httpExp{Status: 404})
			var deletedBy *string
			_ = ownerPool.QueryRow(ctx, `SELECT deleted_by FROM rooms WHERE id = $1 AND deleted_at IS NOT NULL`, room).Scan(&deletedBy)
			iso(t, id+"/soft-deleted", "S-DB-13 (k)", []string{"FM-12", "FM-24"}, "abort was attempted on the live room, then the room was soft-deleted anyway",
				sqlReq{Role: "orbit_owner", SQL: "SELECT deleted_by FROM rooms WHERE id = <room> AND deleted_at IS NOT NULL"},
				map[string]any{"aborts": 1, "liveAtAbort": true, "deletedBy": owner, "returnedWithin5s": true},
				map[string]any{"aborts": so.aborts.Load(), "liveAtAbort": liveAtAbort.Load(), "deletedBy": deletedBy, "returnedWithin5s": elapsed < 5*time.Second},
				so.aborts.Load() == 1 && liveAtAbort.Load() && deletedBy != nil && *deletedBy == owner && elapsed < 5*time.Second)
			logs := srv.logs.String()
			record(t, caseInput{ID: id + "/warning-logged", Contract: "S-DB-13 (k)", Description: "server log has a warning with the task id and no secret material",
				Request:  "server log",
				Expected: map[string]any{"includes": []string{"WARN", "task=" + room, "reason=" + mode}, "excludes": []string{planted, "token="}},
				Actual:   logLines(logs),
				Pass: strings.Contains(logs, "WARN") && strings.Contains(logs, "task="+room) && strings.Contains(logs, "reason="+mode) &&
					!strings.Contains(logs, planted) && !strings.Contains(logs, "token=")})
		})
	}
}
