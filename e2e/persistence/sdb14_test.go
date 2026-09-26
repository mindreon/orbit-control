//go:build e2e

package persistence

import (
	"context"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
)

const (
	sdb14OwnedSQL = `SELECT c.relname FROM pg_class c
	                  WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
	                    AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)
	                  ORDER BY 1`
	sdb14RoleSQL = `SELECT current_user::text, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`
	sdb14RLSSQL  = `SELECT c.relname, c.relforcerowsecurity FROM pg_class c
	                  WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND c.relrowsecurity
	                  ORDER BY 1`
	sdb14MemberSQL = `SELECT r.rolname FROM pg_roles r
	                   WHERE r.rolname <> current_user
	                     AND (r.rolsuper OR r.rolbypassrls
	                          OR EXISTS (SELECT 1 FROM pg_class c
	                                      WHERE c.relowner = r.oid AND c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')))
	                     AND pg_has_role(current_user, r.oid, 'MEMBER')
	                   ORDER BY 1`
)

func queryNames(ctx context.Context, conn *pgx.Conn, sql string) ([]string, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// S-DB-14 (ISO-18): connected as orbit_app, the role cannot escape RLS.
func TestSDB14AppRoleCannotEscapeRLS(t *testing.T) {
	const c = "S-DB-14"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, appURL)
	if err != nil {
		t.Fatalf("connect as orbit_app: %v", sqlState(err))
	}
	defer conn.Close(ctx)

	var who string
	var super, bypass bool
	roleErr := conn.QueryRow(ctx, sdb14RoleSQL).Scan(&who, &super, &bypass)
	iso(t, "S-DB-14/connected-as-orbit_app", c, []string{"FM-48", "FM-49", "FM-50", "FM-51"}, "the checks below run with the credentials control uses",
		sqlReq{Role: "orbit_app", SQL: "SELECT current_user"}, "orbit_app", who, roleErr == nil && who == "orbit_app")
	iso(t, "S-DB-14/no-bypassrls-no-superuser", c, []string{"FM-49"}, "orbit_app has neither BYPASSRLS nor SUPERUSER",
		sqlReq{Role: "orbit_app", SQL: sdb14RoleSQL},
		map[string]any{"rolsuper": false, "rolbypassrls": false, "sqlstate": "ok"},
		map[string]any{"rolsuper": super, "rolbypassrls": bypass, "sqlstate": sqlState(roleErr)},
		roleErr == nil && !super && !bypass)

	owned, ownedErr := queryNames(ctx, conn, sdb14OwnedSQL)
	iso(t, "S-DB-14/owns-no-app-table", c, []string{"FM-48"}, "orbit_app owns no table in public (pg_class.relowner)",
		sqlReq{Role: "orbit_app", SQL: sdb14OwnedSQL},
		map[string]any{"ownedTables": []string{}, "sqlstate": "ok"},
		map[string]any{"ownedTables": owned, "sqlstate": sqlState(ownedErr)},
		ownedErr == nil && len(owned) == 0)

	rows, rlsErr := conn.Query(ctx, sdb14RLSSQL)
	rlsTables, noForce := []string{}, []string{}
	if rlsErr == nil {
		for rows.Next() {
			var name string
			var force bool
			if err := rows.Scan(&name, &force); err != nil {
				rlsErr = err
				break
			}
			rlsTables = append(rlsTables, name)
			if !force {
				noForce = append(noForce, name)
			}
		}
		rows.Close()
	}
	missing := []string{}
	have := map[string]bool{}
	for _, n := range rlsTables {
		have[n] = true
	}
	for _, tbl := range tenantTables {
		if !have[tbl] {
			missing = append(missing, tbl)
		}
	}
	sort.Strings(missing)
	iso(t, "S-DB-14/rls-implies-force", c, []string{"FM-50"}, "every public table with RLS enabled also has FORCE ROW LEVEL SECURITY; all 13 [T] tables have RLS",
		sqlReq{Role: "orbit_app", SQL: sdb14RLSSQL},
		map[string]any{"rlsWithoutForce": []string{}, "tenantTablesWithoutRLS": []string{}, "rlsTablesAtLeast": len(tenantTables), "sqlstate": "ok"},
		map[string]any{"rlsWithoutForce": noForce, "tenantTablesWithoutRLS": missing, "rlsTables": rlsTables, "sqlstate": sqlState(rlsErr)},
		rlsErr == nil && len(noForce) == 0 && len(missing) == 0 && len(rlsTables) >= len(tenantTables))

	members, memberErr := queryNames(ctx, conn, sdb14MemberSQL)
	iso(t, "S-DB-14/no-privileged-membership", c, []string{"FM-51"}, "orbit_app is not a member of any role that owns a public table or has BYPASSRLS/SUPERUSER",
		sqlReq{Role: "orbit_app", SQL: sdb14MemberSQL},
		map[string]any{"privilegedRoles": []string{}, "sqlstate": "ok"},
		map[string]any{"privilegedRoles": members, "sqlstate": sqlState(memberErr)},
		memberErr == nil && len(members) == 0)
}
