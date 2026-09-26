//go:build e2e

package persistence

import (
	"context"
	"crypto/sha256"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/store/auth"
)

const (
	plantedIdemKey   = "e2e-planted-idem-key-5c1d9e"
	plantedSessionID = "e2e-planted-session-id-8b27f04a"
)

func execAsApp(ctx context.Context, pool *pgxpool.Pool, tenant, sql string, args ...any) (int64, error) {
	var n int64
	err := asTenant(ctx, pool, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, sql, args...)
		n = tag.RowsAffected()
		return err
	})
	return n, err
}

// Review M1 (ISO-14): orbit_app cannot delete history.
func TestReviewM1HistoryNotDeletable(t *testing.T) {
	const c = "§18.5 review M1"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	wk := stubWorker(t, nil)
	const tenant = "t-m1"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})
	fx := seedTenant(t, srv, "REVIEW-M1", tenant, "u-m1", "e2e-m1-key", "M1")

	ownerCount := func(table, col string) int {
		var n int
		if err := ownerPool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE `+col+` = $1`, tenant).Scan(&n); err != nil {
			t.Fatalf("owner count %s: %v", table, err)
		}
		return n
	}
	for _, tbl := range []struct{ name, col string }{
		{"events", "tenant_id"}, {"messages", "tenant_id"}, {"turns", "tenant_id"}, {"approvals", "tenant_id"},
		{"artifacts", "tenant_id"}, {"artifact_versions", "tenant_id"}, {"users", "tenant_id"}, {"personas", "tenant_id"},
		{"mcp_connectors", "tenant_id"}, {"cloud_agent_jobs", "tenant_id"}, {"tenants", "id"},
	} {
		before := ownerCount(tbl.name, tbl.col)
		n, err := execAsApp(ctx, srv.appPool, tenant, `DELETE FROM `+tbl.name+` WHERE `+tbl.col+` = $1`, tenant)
		after := ownerCount(tbl.name, tbl.col)
		iso(t, "REVIEW-M1/delete-denied-"+tbl.name, c, []string{"FM-39", "FM-40"}, "orbit_app DELETE on "+tbl.name+" fails with a permission error; no row removed",
			sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "DELETE FROM " + tbl.name + " WHERE " + tbl.col + " = <tenant>"},
			map[string]any{"sqlstate": "42501", "rowsBeforeAtLeast": 1, "rowsUnchanged": true},
			map[string]any{"sqlstate": sqlState(err), "rowsBefore": before, "rowsAfter": after, "rowsAffected": n},
			sqlState(err) == "42501" && before >= 1 && after == before)
	}

	// DELETE kept where cleanup needs it (reasons in the failure-mode doc).
	sessions := auth.NewSessions(srv.appPool)
	if err := sessions.Create(ctx, "m1-cleanup-session-0001", fx.user, tenant, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.appPool.Exec(ctx, `INSERT INTO oidc_login_state (state, nonce, pkce_verifier, expires_at) VALUES ('m1-state', 'n', 'v', now() + interval '10 minutes')`); err != nil {
		t.Fatal(err)
	}
	for _, keep := range []struct{ table, sql string }{
		{"idempotency_keys", `DELETE FROM idempotency_keys WHERE tenant_id = $1 AND task_id = '` + fx.room + `'`},
		{"approval_rules", `DELETE FROM approval_rules WHERE tenant_id = $1 AND id = 'rule_` + fx.room + `'`},
		{"sessions", `DELETE FROM sessions WHERE tenant_id = $1`},
		{"oidc_login_state", `DELETE FROM oidc_login_state WHERE state = 'm1-state' AND $1 <> ''`},
	} {
		n, err := execAsApp(ctx, srv.appPool, tenant, keep.sql, tenant)
		iso(t, "REVIEW-M1/delete-kept-"+keep.table, c, []string{"FM-40"}, "DELETE on "+keep.table+" still works (documented cleanup/revocation reason)",
			sqlReq{Role: "orbit_app", Tenant: tenant, SQL: keep.sql},
			map[string]any{"sqlstate": "ok", "rowsAffected": 1}, map[string]any{"sqlstate": sqlState(err), "rowsAffected": n},
			err == nil && n == 1)
	}
}

// Review M2 (ISO-15): tenants are created by orbit_ops, never by control.
func TestReviewM2TenantsOpsOnly(t *testing.T) {
	const c = "§18.5 review M2"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	appPool := newPool(t, appURL, 2)
	opsPool := newPool(t, opsURL, 2)

	sel := func(pool *pgxpool.Pool, sql string, args ...any) error {
		var n int
		return pool.QueryRow(ctx, sql, args...).Scan(&n)
	}
	for _, p := range []struct{ id, sql string }{
		{"insert", `INSERT INTO tenants (id, name) VALUES ('t-m2-evil', 'x')`},
		{"update", `UPDATE tenants SET name = 'renamed' WHERE id = 't-m1'`},
		{"delete", `DELETE FROM tenants WHERE id = 't-m1'`},
	} {
		_, err := appPool.Exec(ctx, p.sql)
		iso(t, "REVIEW-M2/app-tenants-"+p.id+"-denied", c, []string{"FM-41"}, "orbit_app cannot "+p.id+" tenants",
			sqlReq{Role: "orbit_app", SQL: p.sql}, "42501", sqlState(err), sqlState(err) == "42501")
	}
	err := sel(appPool, `SELECT count(*) FROM tenants`)
	iso(t, "REVIEW-M2/app-tenants-select", c, []string{"FM-41"}, "orbit_app can read tenants (startup check)",
		sqlReq{Role: "orbit_app", SQL: "SELECT count(*) FROM tenants"}, "ok", sqlState(err), err == nil)

	var opsSuper, opsBypass bool
	var opsOwns int
	if err := ownerPool.QueryRow(ctx, `SELECT r.rolsuper, r.rolbypassrls,
	        (SELECT count(*) FROM pg_class c WHERE c.relowner = r.oid AND c.relnamespace = 'public'::regnamespace)
	   FROM pg_roles r WHERE r.rolname = 'orbit_ops'`).Scan(&opsSuper, &opsBypass, &opsOwns); err != nil {
		t.Fatal(err)
	}
	errRooms := sel(opsPool, `SELECT count(*) FROM rooms`)
	errUsers := sel(opsPool, `SELECT count(*) FROM users`)
	iso(t, "REVIEW-M2/ops-role-scope", c, []string{"FM-43"}, "orbit_ops is not SUPERUSER/BYPASSRLS, owns nothing, and cannot read task tables",
		sqlReq{Role: "orbit_ops", SQL: "SELECT count(*) FROM rooms; SELECT count(*) FROM users (+ pg_roles/pg_class)"},
		map[string]any{"superuser": false, "bypassRLS": false, "ownedRelations": 0, "roomsSelect": "42501", "usersSelect": "42501"},
		map[string]any{"superuser": opsSuper, "bypassRLS": opsBypass, "ownedRelations": opsOwns, "roomsSelect": sqlState(errRooms), "usersSelect": sqlState(errUsers)},
		!opsSuper && !opsBypass && opsOwns == 0 && sqlState(errRooms) == "42501" && sqlState(errUsers) == "42501")

	// E2E through the binary: a missing default tenant is fatal, never created.
	const tenant = "t-m2-startup"
	tenantRows := func() int {
		var n int
		_ = ownerPool.QueryRow(ctx, `SELECT count(*) FROM tenants WHERE id = $1`, tenant).Scan(&n)
		return n
	}
	vars := func() []envVar {
		return []envVar{appDBVar(), {Name: "ORBIT_DEFAULT_TENANT", Value: tenant}, {Name: "ORBIT_DATA_DIR", Value: t.TempDir(), Display: "<temp dir>"}, {Name: "PORT", Value: freePort(t)}}
	}
	v1 := vars()
	p1 := startBinary(t, v1)
	code := p1.waitExit(20 * time.Second)
	out := p1.output.String()
	rows := tenantRows()
	record(t, caseInput{ID: "REVIEW-M2/startup-missing-tenant", Contract: c, FailureModes: []string{"FM-42"}, Description: "control with a missing default tenant exits non-zero and does not create it",
		Request:  procReq{Binary: "orbit-control", Env: shownEnv(v1), Probe: "wait for exit (20s); owner counts tenants with that id"},
		Expected: map[string]any{"exitedNonZero": true, "outputIncludes": `default tenant "` + tenant + `" does not exist`, "tenantRows": 0},
		Actual:   map[string]any{"exitedNonZero": code > 0, "output": outputLines(out), "tenantRows": rows},
		Pass:     code > 0 && strings.Contains(out, `default tenant "`+tenant+`" does not exist`) && rows == 0})

	root, _ := repoRoot()
	cmd := exec.Command("psql", "-v", "ON_ERROR_STOP=1", "-v", "tenant_id="+tenant, "-f", "deploy/postgres/ensure-tenant.sql", opsURL)
	cmd.Dir = root
	psqlOut, psqlErr := cmd.CombinedOutput()
	psqlText := strings.TrimSpace(string(psqlOut))
	rows = tenantRows()
	record(t, caseInput{ID: "REVIEW-M2/ops-creates-tenant", Contract: c, Description: "ops creates the tenant with deploy/postgres/ensure-tenant.sql as orbit_ops",
		Steps:    []string{"psql -v ON_ERROR_STOP=1 -v tenant_id=" + tenant + " -f deploy/postgres/ensure-tenant.sql <orbit_ops DB URL>", "owner counts tenants with that id"},
		Request:  map[string]string{"role": "orbit_ops", "script": "deploy/postgres/ensure-tenant.sql"},
		Expected: map[string]any{"psqlOK": true, "tenantRows": 1},
		Actual:   map[string]any{"psqlOK": psqlErr == nil, "psqlOutput": psqlText, "tenantRows": rows},
		Pass:     psqlErr == nil && rows == 1})

	v2 := vars()
	p2 := startBinary(t, v2)
	healthy := p2.waitHealthy(15 * time.Second)
	record(t, caseInput{ID: "REVIEW-M2/startup-after-ops", Contract: c, Description: "after ops created the tenant, control starts",
		Request:  procReq{Binary: "orbit-control", Env: shownEnv(v2), Probe: "GET /health until 200"},
		Expected: map[string]any{"healthy": true}, Actual: map[string]any{"healthy": healthy, "output": outputLines(p2.output.String())}, Pass: healthy})
}

// scanDB searches every text-like and bytea column in public for needle
// (as text and as UTF-8 bytes) or, when raw is set, for those exact bytes.
func scanDB(t *testing.T, owner *pgxpool.Pool, needle string, raw []byte) []string {
	t.Helper()
	ctx := context.Background()
	rows, err := owner.Query(ctx, `
		SELECT table_name, column_name, data_type FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
		   AND data_type IN ('text', 'character varying', 'jsonb', 'json', 'ARRAY', 'bytea')
		 ORDER BY table_name, column_name`)
	if err != nil {
		t.Fatal(err)
	}
	type col struct{ table, column, typ string }
	var cols []col
	for rows.Next() {
		var c col
		_ = rows.Scan(&c.table, &c.column, &c.typ)
		cols = append(cols, c)
	}
	rows.Close()
	hits := []string{}
	for _, c := range cols {
		ref := pgx.Identifier{"public", c.table}.Sanitize()
		colRef := pgx.Identifier{c.column}.Sanitize()
		var q string
		var arg any
		switch {
		case raw != nil && c.typ == "bytea":
			q, arg = `SELECT count(*) FROM `+ref+` WHERE position($1::bytea in `+colRef+`) > 0`, raw
		case raw != nil:
			q, arg = `SELECT count(*) FROM `+ref+` WHERE strpos(`+colRef+`::text, encode($1::bytea, 'hex')) > 0`, raw
		case c.typ == "bytea":
			q, arg = `SELECT count(*) FROM `+ref+` WHERE position(convert_to($1, 'UTF8') in `+colRef+`) > 0`, needle
		default:
			q, arg = `SELECT count(*) FROM `+ref+` WHERE strpos(`+colRef+`::text, $1) > 0`, needle
		}
		var n int
		if err := owner.QueryRow(ctx, q, arg).Scan(&n); err != nil {
			t.Fatalf("scan %s.%s: %v", c.table, c.column, err)
		}
		if n > 0 {
			hits = append(hits, c.table+"."+c.column)
		}
	}
	sort.Strings(hits)
	return hits
}

// Review M2 (ISO-16): raw tokens are not findable in the database.
func TestReviewM2RawTokensNotStored(t *testing.T) {
	const c = "§18.6 review M2"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	wk := stubWorker(t, nil)
	const tenant, u = "t-m2-tokens", "u-m2-tokens"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})
	const title = "m2-scanner-control-title-3e8a"
	room := roomID(t, srv.check(t, "REVIEW-M2/tokens/create", c, "create a task with a planted Idempotency-Key and a planted title",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: withHeader(user(u), "Idempotency-Key", plantedIdemKey), Body: `{"kind":"solo","title":"` + title + `"}`},
		httpExp{Status: 200}))
	alias(room, "<room-m2-tokens>")

	control := scanDB(t, ownerPool, title, nil)
	iso(t, "REVIEW-M2/tokens/scanner-control", c, []string{"FM-44"}, "the scanner finds a planted raw value where it is stored (rooms.title)",
		sqlReq{Role: "orbit_owner", SQL: "strpos/position over every text, jsonb, array and bytea column in public"},
		[]string{"rooms.title"}, control, len(control) == 1 && control[0] == "rooms.title")

	rawHits := scanDB(t, ownerPool, plantedIdemKey, nil)
	keyHash := sha256.Sum256([]byte(plantedIdemKey))
	hashHits := scanDB(t, ownerPool, "", keyHash[:])
	iso(t, "REVIEW-M2/tokens/idempotency-key-raw-absent", c, []string{"FM-44", "FM-45"}, "after POST /v1/rooms the raw Idempotency-Key is nowhere in the database; its sha256 is only in idempotency_keys.key_hash",
		sqlReq{Role: "orbit_owner", SQL: "scan every column for the raw key, then for sha256(key)"},
		map[string]any{"rawHits": []string{}, "sha256Hits": []string{"idempotency_keys.key_hash"}},
		map[string]any{"rawHits": rawHits, "sha256Hits": hashHits},
		len(rawHits) == 0 && len(hashHits) == 1 && hashHits[0] == "idempotency_keys.key_hash")

	sessions := auth.NewSessions(srv.appPool)
	createErr := sessions.Create(ctx, plantedSessionID, u, tenant, time.Now().Add(time.Hour))
	got, lookupErr := sessions.Lookup(ctx, plantedSessionID)
	rawHits = scanDB(t, ownerPool, plantedSessionID, nil)
	idHash := sha256.Sum256([]byte(plantedSessionID))
	hashHits = scanDB(t, ownerPool, "", idHash[:])
	iso(t, "REVIEW-M2/tokens/session-id-raw-absent", c, []string{"FM-44", "FM-45"}, "a session created through internal/store/auth stores only sha256(id); the raw id is nowhere; lookup by raw id works",
		sqlReq{Role: "orbit_app (auth.Sessions) + orbit_owner scan", SQL: "auth.Sessions.Create/Lookup; scan every column for the raw id, then for sha256(id)"},
		map[string]any{"create": "ok", "lookupUser": u, "rawHits": []string{}, "sha256Hits": []string{"sessions.id_hash"}},
		map[string]any{"create": sqlState(createErr), "lookupUser": got.UserID, "lookupErr": sqlState(lookupErr), "rawHits": rawHits, "sha256Hits": hashHits},
		createErr == nil && lookupErr == nil && got.UserID == u && len(rawHits) == 0 && len(hashHits) == 1 && hashHits[0] == "sessions.id_hash")

	delErr := sessions.Delete(ctx, plantedSessionID)
	_, afterErr := sessions.Lookup(ctx, plantedSessionID)
	iso(t, "REVIEW-M2/tokens/session-delete", c, []string{"FM-44"}, "logout (Delete by raw id) destroys the session",
		sqlReq{Role: "orbit_app (auth.Sessions)", SQL: "auth.Sessions.Delete then Lookup"},
		map[string]any{"delete": "ok", "lookupAfter": "not found"},
		map[string]any{"delete": sqlState(delErr), "lookupAfter": map[bool]string{true: "not found", false: "found"}[afterErr != nil]},
		delErr == nil && afterErr != nil)
}

// Review L1 (ISO-17): orbit_app cannot rewrite ownership columns of rooms.
func TestReviewL1RoomColumnPrivileges(t *testing.T) {
	const c = "§18.5 review L1"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	wk := stubWorker(t, nil)
	const tenant, u = "t-l1", "u-l1"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})
	room := roomID(t, srv.check(t, "REVIEW-L1/create", c, "create a task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-l1>")

	snapshot := func() string {
		var s string
		if err := ownerPool.QueryRow(ctx, `SELECT concat_ws('|', id, tenant_id, created_by, created_at, deleted_at, deleted_by) FROM rooms WHERE id = $1`, room).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := snapshot()
	for _, col := range []struct{ name, value string }{
		{"created_by", "'u-evil'"}, {"tenant_id", "'t-evil'"}, {"deleted_at", "now()"},
		{"deleted_by", "'u-evil'"}, {"id", "'rm_evil'"}, {"created_at", "now()"},
	} {
		sql := `UPDATE rooms SET ` + col.name + ` = ` + col.value + ` WHERE id = $1`
		_, err := execAsApp(ctx, srv.appPool, tenant, sql, room)
		after := snapshot()
		iso(t, "REVIEW-L1/update-denied-"+col.name, c, []string{"FM-46"}, "orbit_app UPDATE of rooms."+col.name+" fails with a permission error; ownership columns unchanged",
			sqlReq{Role: "orbit_app", Tenant: tenant, SQL: sql},
			map[string]any{"sqlstate": "42501", "ownershipColumnsUnchanged": true},
			map[string]any{"sqlstate": sqlState(err), "ownershipColumnsUnchanged": after == before},
			sqlState(err) == "42501" && after == before)
	}
	n, err := execAsApp(ctx, srv.appPool, tenant, `UPDATE rooms SET state = 'closed' WHERE id = $1`, room)
	var state string
	_ = ownerPool.QueryRow(ctx, `SELECT state FROM rooms WHERE id = $1`, room).Scan(&state)
	iso(t, "REVIEW-L1/update-granted-state", c, []string{"FM-46"}, "a granted column (state) is still updatable",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: "UPDATE rooms SET state = 'closed' WHERE id = $1"},
		map[string]any{"sqlstate": "ok", "rowsAffected": 1, "state": "closed"},
		map[string]any{"sqlstate": sqlState(err), "rowsAffected": n, "state": state},
		err == nil && n == 1 && state == "closed")
	srv.check(t, "REVIEW-L1/body-created-by-ignored", c, "POST /v1/rooms with createdBy/tenantId in the body: the task belongs to the session user",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo","createdBy":"u-evil","tenantId":"t-evil"}`},
		httpExp{Status: 200})
	var evil int
	_ = ownerPool.QueryRow(ctx, `SELECT count(*) FROM rooms WHERE created_by = 'u-evil' OR tenant_id = 't-evil'`).Scan(&evil)
	iso(t, "REVIEW-L1/no-evil-owner", c, []string{"FM-46"}, "no task anywhere is owned by the body-supplied creator or tenant",
		sqlReq{Role: "orbit_owner", SQL: "SELECT count(*) FROM rooms WHERE created_by = 'u-evil' OR tenant_id = 't-evil'"}, 0, evil, evil == 0)
}
