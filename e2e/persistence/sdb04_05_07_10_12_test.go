//go:build e2e

package persistence

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// S-DB-4 (ISO-11): the schema cannot hold raw session ids or IdP tokens.
func TestSDB04SessionsHoldOnlyHashes(t *testing.T) {
	const c = "S-DB-4"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	appPool := newPool(t, appURL, 2)

	colSQL := `SELECT table_name || '.' || column_name FROM information_schema.columns
	            WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
	              AND column_name ~* '(token|secret|password|passwd|refresh|access|cookie|credential|api_?key)'
	            ORDER BY 1`
	rows, err := ownerPool.Query(ctx, colSQL)
	if err != nil {
		t.Fatal(err)
	}
	hits := []string{}
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		hits = append(hits, s)
	}
	rows.Close()
	iso(t, "S-DB-4/no-token-columns", c, []string{"FM-32"}, "no column anywhere in the schema can hold an IdP token, secret, password or cookie",
		sqlReq{Role: "orbit_owner", SQL: colSQL}, []string{}, hits, len(hits) == 0)

	const tenant, u = "t-sdb4", "u-sdb4"
	if _, err := ownerPool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1) ON CONFLICT DO NOTHING`, tenant); err != nil {
		t.Fatal(err)
	}
	if err := asTenant(ctx, appPool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO users (id, tenant_id, iss, sub) VALUES ($1, $2, 'orbit-local', $1)`, u, tenant)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	insertSession := func(idHash []byte) error {
		_, err := appPool.Exec(ctx, `INSERT INTO sessions (id_hash, user_id, tenant_id, expires_at) VALUES ($1, $2, $3, now() + interval '1 hour')`, idHash, u, tenant)
		return err
	}
	insertLogin := func(state string, pre []byte) error {
		_, err := appPool.Exec(ctx, `INSERT INTO oidc_login_state (state, nonce, pkce_verifier, pre_session_hash, expires_at) VALUES ($1, 'n', 'v', $2, now() + interval '10 minutes')`, state, pre)
		return err
	}
	short, full := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 32)
	results := map[string]string{
		"sessions 16-byte id_hash":                  sqlState(insertSession(short)),
		"sessions 32-byte id_hash":                  sqlState(insertSession(full)),
		"oidc_login_state 16-byte pre_session_hash": sqlState(insertLogin("st-short", short)),
		"oidc_login_state 32-byte pre_session_hash": sqlState(insertLogin("st-full", full)),
		"oidc_login_state NULL pre_session_hash":    sqlState(insertLogin("st-null", nil)),
	}
	want := map[string]string{
		"sessions 16-byte id_hash":                  "23514",
		"sessions 32-byte id_hash":                  "ok",
		"oidc_login_state 16-byte pre_session_hash": "23514",
		"oidc_login_state 32-byte pre_session_hash": "ok",
		"oidc_login_state NULL pre_session_hash":    "ok",
	}
	same := len(results) == len(want)
	for k, v := range want {
		same = same && results[k] == v
	}
	iso(t, "S-DB-4/hash-length-enforced", c, []string{"FM-33"}, "session hashes must be exactly 32 bytes (sha256); anything else is rejected by the schema",
		sqlReq{Role: "orbit_app", SQL: "INSERT INTO sessions / oidc_login_state with 16-byte, 32-byte and NULL hashes"}, want, results, same)

	var sRLS, oRLS bool
	if err := ownerPool.QueryRow(ctx, `SELECT (SELECT relrowsecurity FROM pg_class WHERE oid = 'public.sessions'::regclass),
	                                          (SELECT relrowsecurity FROM pg_class WHERE oid = 'public.oidc_login_state'::regclass)`).Scan(&sRLS, &oRLS); err != nil {
		t.Fatal(err)
	}
	iso(t, "S-DB-4/pre-login-tables-no-rls", c, []string{"FM-34"}, "sessions and oidc_login_state are pre-login tables without RLS (§18.5)",
		sqlReq{Role: "orbit_owner", SQL: "SELECT relrowsecurity FROM pg_class WHERE oid IN (sessions, oidc_login_state)"},
		map[string]bool{"sessions": false, "oidc_login_state": false}, map[string]bool{"sessions": sRLS, "oidc_login_state": oRLS}, !sRLS && !oRLS)

	blocked(t, "S-DB-4/login-stores-only-hash", c, "e2e", "after a real login the sessions row holds only sha256(cookie) and the raw cookie value is nowhere in the database",
		"auth PR (§17; ordered after this PR by §17.9): no code writes sessions yet",
		[]string{"log in", "read the orbit_session cookie", "search every table for the raw value"}, "32-byte hash only; raw value absent")
}

// S-DB-10 (ISO-13): the schema refuses assistant.delta rows.
func TestSDB10AssistantDeltaNotStored(t *testing.T) {
	const c = "S-DB-10"
	ctx := context.Background()
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: "t-sdb10", maxConns: 2, workerURL: wk.URL})
	room := roomID(t, srv.check(t, "S-DB-10/setup/create", c, "create a live task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-sdb10"), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-sdb10>")
	insert := func(seq int, typ string) error {
		return asTenant(ctx, srv.appPool, "t-sdb10", func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO events (tenant_id, task_id, seq, event_uid, type, source) VALUES ($1, $2, $3, $4, $5, 'worker')`,
				"t-sdb10", room, seq, typ+"-uid", typ)
			return err
		})
	}
	got := map[string]string{"assistant.delta": sqlState(insert(1, "assistant.delta")), "tool.call": sqlState(insert(2, "tool.call"))}
	iso(t, "S-DB-10/delta-rejected-by-schema", c, []string{"FM-38"}, "an events row of type assistant.delta is rejected; a tool.call row for the same task is accepted",
		sqlReq{Role: "orbit_app", Tenant: "t-sdb10", SQL: "INSERT INTO events (..., type) VALUES (..., 'assistant.delta' | 'tool.call')"},
		map[string]string{"assistant.delta": "23514", "tool.call": "ok"}, got,
		got["assistant.delta"] == "23514" && got["tool.call"] == "ok")
	blocked(t, "S-DB-10/streamed-turn", c, "e2e", "after a streamed turn the turn's events are persisted but none of type assistant.delta",
		"phase 2: control persists events per task (§18.4) after Last-Event-ID PR #15 (which adds delta pass-through, C33)",
		[]string{"stream a turn with assistant.delta events", "SELECT type FROM events WHERE task_id = <task>"}, "no assistant.delta rows, other turn events present")
}

func TestSDB05ArtifactVersionIdempotency(t *testing.T) {
	blocked(t, "S-DB-5/version-sequence", "S-DB-5", "e2e", "v1, v2, v2 (same digest), v2 (other digest), v1 → 200, 200, 200 no-op, 409, 409; two version rows",
		"§16 control artifact PR: artifact ingest/projection is not in control; it builds on this PR's artifacts tables (§16.3, §18.9)",
		[]string{"POST artifact events v1, v2, v2, v2', v1 to /internal/events", "count artifact_versions"}, []any{200, 200, 200, 409, 409})
}

func TestSDB07ConcurrentWritersAndReplay(t *testing.T) {
	blocked(t, "S-DB-7/32-writers-1000-events", "S-DB-7", "e2e", "32 writers, 1000 events on one task; a reader reconnects with Last-Event-ID mid-way; seq 1..1000 contiguous, no loss or duplicate",
		"phase 2: per-task seq + SubscribeAndReplay (§18.4); owner instruction: after Last-Event-ID PR #15 merges",
		[]string{"32 concurrent POST /internal/events", "SSE reader reconnect with Last-Event-ID", "compare received seqs"}, "contiguous 1..1000, strictly increasing, no duplicates")
	blocked(t, "S-DB-7/injected-writes", "S-DB-7", "e2e", "writes injected between query and subscribe, and between subscribe and query: no loss, no duplicate (Postgres and memory store)",
		"phase 2: per-task seq + SubscribeAndReplay (§18.4); owner instruction: after Last-Event-ID PR #15 merges", nil, "no loss, no duplicate in both stores")
}

func TestSDB12ArtifactBlobs(t *testing.T) {
	const by = "phase 2: /internal/artifact-blobs and the internal listener (§18.7); owner instruction: not in this PR"
	for _, b := range []struct{ id, desc string }{
		{"a-forged-tenant", "a forged tenant in the request is ignored; the file lands under the task's real tenant"},
		{"b-digest-mismatch", "X-Content-Digest mismatch → 422, no file left; ../x or file names in the header do not affect the path"},
		{"c-size-limit", "body of limit+1 bytes → 413, RSS stays far below the body size, no temp file left"},
		{"d-public-listener", "/internal/artifact-blobs and /internal/events on the public listener → 404 (today /internal/events is still on the public listener)"},
	} {
		blocked(t, "S-DB-12/"+b.id, "S-DB-12", "e2e", b.desc, by, nil, "per §18.8 S-DB-12")
	}
}
