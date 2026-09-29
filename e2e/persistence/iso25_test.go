//go:build e2e

package persistence

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

const workerMaintenanceContract = "orbit-infra 17 G2, G7 (migration 00018)"

// ISO-25: orbit_worker's maintenance grants on stage_attempts and tenants.
func TestISO25WorkerMaintenanceGrants(t *testing.T) {
	const c = workerMaintenanceContract
	const tenant = "t-rt-m"
	ctx := context.Background()
	opsEnsureTenant(t, tenant)
	owner := newPool(t, ownerURL, 2)
	if err := seedTaskRows(ctx, owner, tenant); err != nil {
		t.Fatalf("seed: %v", err)
	}
	worker := newPool(t, workerURL, 2)

	// FM-88: only the closing-out columns; the identity of the row cannot change.
	for _, set := range []string{`attempt_no = 99`, `task_id = 'task_other'`, `node_id = 'n_other'`, `tenant_id = 'other'`, `started_at = now()`} {
		sql := `UPDATE stage_attempts SET ` + set + ` WHERE tenant_id = $1`
		_, err := execAsApp(ctx, worker, tenant, sql, tenant)
		column := strings.SplitN(set, " ", 2)[0]
		iso(t, "ISO-25/update-denied-stage_attempts-"+column, c, []string{"FM-88"},
			"orbit_worker UPDATE of stage_attempts."+column+" (outside its column grant) fails with 42501",
			sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: sql},
			map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(err)},
			sqlState(err) == "42501")
	}
	closeSQL := `UPDATE stage_attempts SET status = 'LOST', failure = '{"reason":"workflow not found"}',
	                    finished_at = now(), entity_version = entity_version + 1
	              WHERE tenant_id = $1 AND status IN ('STARTING', 'RUNNING')`
	n, closeErr := execAsApp(ctx, worker, tenant, closeSQL, tenant)
	iso(t, "ISO-25/update-allowed-stage_attempts-close-out", c, []string{"FM-88"},
		"orbit_worker can close out an attempt row: status, failure, finished_at, entity_version",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: closeSQL},
		map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": n, "error": sqlState(closeErr)},
		closeErr == nil && n == 1)

	// FM-87: the row is history; nothing deletes it.
	deleteSQL := `DELETE FROM stage_attempts WHERE tenant_id = $1`
	_, delErr := execAsApp(ctx, worker, tenant, deleteSQL, tenant)
	left := ownerScalar[int](t, owner, `SELECT count(*) FROM stage_attempts WHERE tenant_id = $1`, tenant)
	iso(t, "ISO-25/delete-denied-stage_attempts", c, []string{"FM-87"},
		"orbit_worker DELETE on stage_attempts fails with 42501 and the row is still there",
		sqlReq{Role: "orbit_worker", Tenant: tenant, SQL: deleteSQL},
		map[string]any{"sqlstate": "42501", "rows": 1}, map[string]any{"sqlstate": sqlState(delErr), "rows": left},
		sqlState(delErr) == "42501" && left == 1)

	// FM-89: tenant ids are listable without a tenant set; nothing else about a tenant is.
	listSQL := `SELECT count(*) FROM tenants WHERE id = $1`
	listed := countAs(t, worker, "", listSQL, tenant)
	iso(t, "ISO-25/tenant-ids-listable", c, []string{"FM-89"},
		"orbit_worker reads tenants.id with no tenant set",
		sqlReq{Role: "orbit_worker", SQL: listSQL},
		map[string]any{"rows": 1}, map[string]any{"rows": listed}, listed == 1)
	for i, sql := range []string{
		`SELECT name FROM tenants`,
		`SELECT * FROM tenants`,
		`INSERT INTO tenants (id) VALUES ('t-rt-worker-made')`,
		`UPDATE tenants SET id = id`,
		`DELETE FROM tenants WHERE id = 'nothing'`,
	} {
		_, err := execAsApp(ctx, worker, "", sql)
		iso(t, "ISO-25/tenants-limited-"+strconv.Itoa(i), c, []string{"FM-89"},
			"orbit_worker cannot run `"+sql+"` on tenants: 42501",
			sqlReq{Role: "orbit_worker", SQL: sql},
			map[string]any{"sqlstate": "42501"}, map[string]any{"sqlstate": sqlState(err)},
			sqlState(err) == "42501")
	}
}
