//go:build e2e

package persistence

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const taskRuntimeContract = "orbit-infra 12 Step 2 (migration 00013)"

// taskTenantTables are the migration 00013 tables under tenant RLS.
var taskTenantTables = []string{
	"agent_profiles", "tasks", "plan_versions", "task_nodes", "stage_attempts", "task_approvals",
	"task_messages", "task_events", "idempotency_ledger", "checkpoints", "artifact_manifests",
	"workspace_leases",
}

// taskGlobalTables are the only migration 00013 tables without RLS.
var taskGlobalTables = []string{"node_type_registry", "runtime_outbox"}

// workerReadable are the tenant tables orbit_worker may SELECT.
var workerReadable = []string{"agent_profiles", "idempotency_ledger", "checkpoints", "stage_attempts", "artifact_manifests", "workspace_leases"}

const sha = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// seedTaskRows inserts one row per tenant table for tenant, as the owner.
// Ids carry the tenant so both tenants' rows can coexist.
func seedTaskRows(ctx context.Context, owner *pgxpool.Pool, tenant string) error {
	stmts := []string{
		`INSERT INTO agent_profiles (tenant_id, profile_id, version, spec) VALUES ($1, 'coder', 1, '{}')`,
		`INSERT INTO tasks (id, tenant_id, workflow_id, title, goal, mode, status, profile_ref, created_by)
		   VALUES ('task_' || $1, $1, 'task/' || $1, 't', 'g', 'single', 'RUNNING', 'coder@1', 'u')`,
		`INSERT INTO plan_versions (tenant_id, task_id, plan_version, parent_version, hash, actor, graph)
		   VALUES ($1, 'task_' || $1, 1, 0, '` + sha + `', '{}', '{}')`,
		`INSERT INTO task_nodes (tenant_id, task_id, node_id, node_type, title, status, workspace_mode, owner_profile)
		   VALUES ($1, 'task_' || $1, 'n_' || $1, 'agent_turn', 'explore', 'RUNNING', 'write', 'coder@1')`,
		`INSERT INTO stage_attempts (attempt_id, tenant_id, task_id, node_id, attempt_no, status, profile_ref)
		   VALUES ('att_' || $1, $1, 'task_' || $1, 'n_' || $1, 1, 'RUNNING', 'coder@1')`,
		`INSERT INTO task_approvals (approval_id, tenant_id, task_id, subject, status)
		   VALUES ('apr_' || $1, $1, 'task_' || $1, '{}', 'PENDING')`,
		`INSERT INTO task_messages (tenant_id, task_id, message_seq, client_message_id, text, delivery)
		   VALUES ($1, 'task_' || $1, 1, 'm1', 'hello', 'queue')`,
		`INSERT INTO task_events (tenant_id, task_id, seq, event_id, event_type, body, occurred_at)
		   VALUES ($1, 'task_' || $1, 1, 'evt_' || $1, 'task.created', '{}', now())`,
		`INSERT INTO idempotency_ledger (scope, key, tenant_id, request_hash, status)
		   VALUES ('side_effect', 'op-' || $1, $1, 'h', 'started')`,
		`INSERT INTO checkpoints (checkpoint_id, tenant_id, task_id, node_id, attempt_id, seq, kind, blob_ref,
		   size_bytes, encryption, schema_version, agentscope_version)
		   VALUES ('ckpt_' || $1, $1, 'task_' || $1, 'n_' || $1, 'att_' || $1, 0, 'agent_state', '` + sha + `',
		   1, '{}', '3', '2.0.9')`,
		`INSERT INTO artifact_manifests (manifest_id, tenant_id, task_id, attempt_id, entries, manifest_hash)
		   VALUES ('man_' || $1, $1, 'task_' || $1, 'att_' || $1, '[]', '` + sha + `')`,
		`INSERT INTO workspace_leases (lease_id, tenant_id, lease_key, lease_mode, backend, holder_attempt, expires_at)
		   VALUES ('lease-' || $1, $1, $1 || '/task', 'write', 'docker', 'att_' || $1, now() + interval '10 minutes')`,
	}
	for _, s := range stmts {
		if _, err := owner.Exec(ctx, s, tenant); err != nil {
			return err
		}
	}
	return nil
}

// ISO-21: task runtime tables isolate tenants and keep history immutable.
func TestISO21TaskRuntimeTables(t *testing.T) {
	const tenantA, tenantB = "t-rt-a", "t-rt-b"
	ctx := context.Background()
	owner := newPool(t, ownerURL, 2)
	app := newPool(t, appURL, 2)
	worker := newPool(t, workerURL, 2)
	opsEnsureTenant(t, tenantA)
	opsEnsureTenant(t, tenantB)
	for _, tenant := range []string{tenantA, tenantB} {
		if err := seedTaskRows(ctx, owner, tenant); err != nil {
			t.Fatalf("seed %s: %v", tenant, err)
		}
	}

	// FM-77, FM-81: RLS and FORCE on every tenant table; the two global
	// tables are the only new ones without RLS.
	const catalogSQL = `SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity FROM pg_class c
	                     WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY($1)`
	all := append(append([]string{}, taskTenantTables...), taskGlobalTables...)
	rows, err := owner.Query(ctx, catalogSQL, all)
	missingRLS, noRLS := []string{}, []string{}
	seen := 0
	if err == nil {
		for rows.Next() {
			var name string
			var rls, force bool
			if err = rows.Scan(&name, &rls, &force); err != nil {
				break
			}
			seen++
			if !rls {
				noRLS = append(noRLS, name)
			} else if !force {
				missingRLS = append(missingRLS, name)
			}
		}
		rows.Close()
	}
	sort.Strings(noRLS)
	for _, tbl := range taskTenantTables {
		for _, n := range noRLS {
			if n == tbl {
				missingRLS = append(missingRLS, tbl)
			}
		}
	}
	sort.Strings(missingRLS)
	iso(t, "ISO-21/rls-on-tenant-tables", taskRuntimeContract, []string{"FM-77", "FM-81"},
		"every migration 00013 tenant table has RLS and FORCE RLS; node_type_registry and runtime_outbox are the only new tables without RLS",
		sqlReq{Role: "orbit_owner", SQL: catalogSQL},
		map[string]any{"tables": len(all), "tenantTablesWithoutForceRLS": []string{}, "tablesWithoutRLS": taskGlobalTables, "error": "ok"},
		map[string]any{"tables": seen, "tenantTablesWithoutForceRLS": missingRLS, "tablesWithoutRLS": noRLS, "error": sqlState(err)},
		err == nil && seen == len(all) && len(missingRLS) == 0 && strings.Join(noRLS, ",") == strings.Join(taskGlobalTables, ","))

	// FM-77: each role reads only its tenant's rows, and nothing without a tenant.
	for _, role := range []struct {
		name   string
		pool   *pgxpool.Pool
		tables []string
	}{{"orbit_app", app, taskTenantTables}, {"orbit_worker", worker, workerReadable}} {
		for _, tbl := range role.tables {
			q := `SELECT count(*) FROM ` + tbl
			ownRows := countAs(t, role.pool, tenantA, q)
			otherRows := countAs(t, role.pool, tenantA, q+` WHERE tenant_id = $1`, tenantB)
			noTenant := countAs(t, role.pool, "", q)
			iso(t, "ISO-21/isolation-"+role.name+"-"+tbl, taskRuntimeContract, []string{"FM-77"},
				role.name+" with tenant A set reads only tenant A's rows of "+tbl+", and none without a tenant",
				sqlReq{Role: role.name, Tenant: tenantA, SQL: q},
				map[string]any{"ownRows": 1, "otherTenantRows": 0, "rowsWithoutTenant": 0},
				map[string]any{"ownRows": ownRows, "otherTenantRows": otherRows, "rowsWithoutTenant": noTenant},
				ownRows == 1 && otherRows == 0 && noTenant == 0)
		}
	}

	// FM-78: history cannot be rewritten or deleted by either role.
	for _, tc := range []struct{ table, set, snap string }{
		{"plan_versions", `graph = '{"rewritten":true}'`, `SELECT graph::text FROM plan_versions WHERE tenant_id = $1`},
		{"task_messages", `text = 'rewritten'`, `SELECT text FROM task_messages WHERE tenant_id = $1`},
		{"task_events", `body = '{"rewritten":true}'`, `SELECT body::text FROM task_events WHERE tenant_id = $1`},
		{"artifact_manifests", `entries = '[1]'`, `SELECT entries::text FROM artifact_manifests WHERE tenant_id = $1`},
	} {
		for _, role := range []struct {
			name string
			pool *pgxpool.Pool
		}{{"orbit_app", app}, {"orbit_worker", worker}} {
			before := ownerScalar[string](t, owner, tc.snap, tenantA)
			updateSQL := `UPDATE ` + tc.table + ` SET ` + tc.set + ` WHERE tenant_id = $1`
			deleteSQL := `DELETE FROM ` + tc.table + ` WHERE tenant_id = $1`
			_, upErr := execAsApp(ctx, role.pool, tenantA, updateSQL, tenantA)
			_, delErr := execAsApp(ctx, role.pool, tenantA, deleteSQL, tenantA)
			after := ownerScalar[string](t, owner, tc.snap, tenantA)
			iso(t, "ISO-21/immutable-"+tc.table+"-"+role.name, taskRuntimeContract, []string{"FM-78"},
				role.name+" UPDATE and DELETE on "+tc.table+" fail with 42501; the row is unchanged",
				sqlReq{Role: role.name, Tenant: tenantA, SQL: updateSQL + "; " + deleteSQL},
				map[string]any{"update": "42501", "delete": "42501", "unchanged": true},
				map[string]any{"update": sqlState(upErr), "delete": sqlState(delErr), "unchanged": before == after},
				sqlState(upErr) == "42501" && sqlState(delErr) == "42501" && before == after)
		}
	}

	// FM-79: a second live write lease on the same workspace key is refused.
	leaseSQL := `INSERT INTO workspace_leases (lease_id, tenant_id, lease_key, lease_mode, backend, holder_attempt, expires_at)
	             VALUES ('lease-second', $1, $1 || '/task', 'write', 'docker', 'att_other', now() + interval '10 minutes')`
	_, leaseErr := execAsApp(ctx, worker, tenantA, leaseSQL, tenantA)
	readSQL := `INSERT INTO workspace_leases (lease_id, tenant_id, lease_key, lease_mode, backend, holder_attempt, expires_at)
	            VALUES ('lease-read', $1, $1 || '/task/ro/att_other', 'read', 'docker', 'att_other', now() + interval '10 minutes')`
	_, readErr := execAsApp(ctx, worker, tenantA, readSQL, tenantA)
	iso(t, "ISO-21/one-live-write-lease", taskRuntimeContract, []string{"FM-79"},
		"a second live write lease for the same workspace key fails with 23505; a read copy lease is accepted",
		sqlReq{Role: "orbit_worker", Tenant: tenantA, SQL: leaseSQL},
		map[string]any{"secondWriter": "23505", "readCopy": "ok"},
		map[string]any{"secondWriter": sqlState(leaseErr), "readCopy": sqlState(readErr)},
		sqlState(leaseErr) == "23505" && readErr == nil)

	// FM-80: registry v1 has six node types and team_stage is disabled.
	const registrySQL = `SELECT count(*) FILTER (WHERE registry_version = 1),
	                            bool_or(enabled) FILTER (WHERE registry_version = 1 AND node_type = 'team_stage')
	                       FROM node_type_registry`
	var types int
	var teamEnabled *bool
	regErr := app.QueryRow(ctx, registrySQL).Scan(&types, &teamEnabled)
	iso(t, "ISO-21/registry-v1-team-stage-disabled", taskRuntimeContract, []string{"FM-80"},
		"node_type_registry v1 has six node types and team_stage is disabled",
		sqlReq{Role: "orbit_app", SQL: registrySQL},
		map[string]any{"types": 6, "teamStageEnabled": false, "error": "ok"},
		map[string]any{"types": types, "teamStageEnabled": teamEnabled != nil && *teamEnabled, "error": sqlState(regErr)},
		regErr == nil && types == 6 && teamEnabled != nil && !*teamEnabled)
}

// ISO-22: orbit_worker has only its grants.
func TestISO22WorkerRoleGrants(t *testing.T) {
	const c = taskRuntimeContract
	const tenant = "t-rt-w"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, workerURL)
	if err != nil {
		t.Fatalf("connect as orbit_worker: %v", sqlState(err))
	}
	defer conn.Close(ctx)

	var who string
	var super, bypass bool
	roleErr := conn.QueryRow(ctx, sdb14RoleSQL).Scan(&who, &super, &bypass)
	owned, ownedErr := queryNames(ctx, conn, sdb14OwnedSQL)
	members, memberErr := queryNames(ctx, conn, sdb14MemberSQL)
	granted, privErr := queryNames(ctx, conn, sdb14TablePrivSQL)
	iso(t, "ISO-22/role-attributes", c, []string{"FM-83"},
		"orbit_worker is not SUPERUSER or BYPASSRLS, owns no table, is in no privileged role, and has no TRUNCATE, REFERENCES or TRIGGER",
		sqlReq{Role: "orbit_worker", SQL: sdb14RoleSQL},
		map[string]any{"user": "orbit_worker", "rolsuper": false, "rolbypassrls": false, "ownedTables": []string{}, "privilegedRoles": []string{}, "dangerousPrivileges": []string{}, "error": "ok"},
		map[string]any{"user": who, "rolsuper": super, "rolbypassrls": bypass, "ownedTables": owned, "privilegedRoles": members, "dangerousPrivileges": granted,
			"error": sqlState(firstErr(roleErr, ownedErr, memberErr, privErr))},
		firstErr(roleErr, ownedErr, memberErr, privErr) == nil && who == "orbit_worker" && !super && !bypass &&
			len(owned) == 0 && len(members) == 0 && len(granted) == 0)

	// FM-82: no privilege at all on the catalog, the tenants table or control's projections.
	forbidden := []string{"tenants", "personas", "mcp_connectors", "tasks", "task_nodes",
		"task_approvals", "plan_versions", "task_messages", "task_events"}
	const privSQL = `SELECT t FROM unnest($1::text[]) AS t
	                  WHERE has_table_privilege(current_user, t, 'SELECT, INSERT, UPDATE, DELETE')`
	reachable := []string{}
	r, reachErr := conn.Query(ctx, privSQL, forbidden)
	if reachErr == nil {
		for r.Next() {
			var name string
			if reachErr = r.Scan(&name); reachErr != nil {
				break
			}
			reachable = append(reachable, name)
		}
		r.Close()
	}
	iso(t, "ISO-22/no-catalog-or-projection-access", c, []string{"FM-82"},
		"orbit_worker has no SELECT, INSERT, UPDATE or DELETE on the catalog or on control's task projections",
		sqlReq{Role: "orbit_worker", SQL: privSQL},
		map[string]any{"reachable": []string{}, "checked": len(forbidden), "error": "ok"},
		map[string]any{"reachable": reachable, "checked": len(forbidden), "error": sqlState(reachErr)},
		reachErr == nil && len(reachable) == 0)

	// FM-84: UPDATE outside the column grant fails.
	opsEnsureTenant(t, tenant)
	owner := newPool(t, ownerURL, 2)
	if err := seedTaskRows(ctx, owner, tenant); err != nil {
		t.Fatalf("seed: %v", err)
	}
	worker := newPool(t, workerURL, 2)
	for _, tc := range []struct{ table, set string }{
		{"idempotency_ledger", `request_hash = 'rewritten'`},
		{"idempotency_ledger", `key = 'rewritten'`},
		{"checkpoints", `blob_ref = '` + sha + `'`},
		{"workspace_leases", `holder_attempt = 'att_rewritten'`},
	} {
		sql := `UPDATE ` + tc.table + ` SET ` + tc.set + ` WHERE tenant_id = $1`
		_, upErr := execAsApp(ctx, worker, tenant, sql, tenant)
		column := strings.SplitN(tc.set, " ", 2)[0]
		iso(t, "ISO-22/update-denied-"+tc.table+"-"+column, c, []string{"FM-84"},
			"orbit_worker UPDATE of "+tc.table+"."+column+" (outside its column grant) fails with 42501",
			sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: sql},
			map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(upErr)},
			sqlState(upErr) == "42501")
	}
	// The granted columns still work, so the denials above are about scope.
	okSQL := `UPDATE idempotency_ledger SET status = 'succeeded', last_seen = now() WHERE tenant_id = $1`
	n, okErr := execAsApp(ctx, worker, tenant, okSQL, tenant)
	iso(t, "ISO-22/update-allowed-ledger-status", c, []string{"FM-84"},
		"orbit_worker can move its ledger row to succeeded (granted columns)",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: okSQL},
		map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": n, "error": sqlState(okErr)},
		okErr == nil && n == 1)

	// The idempotent publish (INSERT ... ON CONFLICT (event_id)) needs SELECT on event_id, and only on event_id.
	publishSQL := `INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, 'task_' || $1, 'evt_pub_' || $1, '{}')
		ON CONFLICT (event_id) DO NOTHING`
	_, pubErr := execAsApp(ctx, worker, tenant, publishSQL, tenant)
	iso(t, "ISO-22/outbox-idempotent-publish", c, []string{"FM-84"},
		"orbit_worker can append to runtime_outbox with ON CONFLICT (event_id) DO NOTHING",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: publishSQL},
		map[string]any{"error": "ok"}, map[string]any{"error": sqlState(pubErr)}, pubErr == nil)
	readBodySQL := `SELECT body FROM runtime_outbox WHERE tenant_id = $1`
	_, readErr := execAsApp(ctx, worker, tenant, readBodySQL, tenant)
	iso(t, "ISO-22/outbox-body-not-readable", c, []string{"FM-84"},
		"orbit_worker cannot read runtime_outbox.body (its SELECT is limited to event_id)",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: readBodySQL},
		map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(readErr)},
		sqlState(readErr) == "42501")
}
