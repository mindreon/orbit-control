package integration

import (
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/usage"
)

func TestUsageMigration(t *testing.T) {
	_, admin, _ := rollupDatabase(t)
	for _, table := range []string{"usage_ledger", "usage_rollups"} {
		var definition string
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid=$1::regclass AND conname=$2`, table, table+"_meter").Scan(&definition))
		var got, want []string
		for _, match := range regexp.MustCompile(`'([^']+)'::text`).FindAllStringSubmatch(definition, -1) {
			got = append(got, match[1])
		}
		for _, meter := range usage.Meters() {
			want = append(want, string(meter))
		}
		slices.Sort(got)
		slices.Sort(want)
		require.Equal(t, want, got)
	}
	for _, name := range []string{"ops_usage_rollup_backlog", "usage_rollup_dirty_page"} {
		var app, worker, public, definer bool
		var result, config string
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT has_function_privilege('orbit_app',oid,'EXECUTE'),
 has_function_privilege('orbit_worker',oid,'EXECUTE'),
 EXISTS(SELECT 1 FROM aclexplode(proacl) acl WHERE grantee=0 AND privilege_type='EXECUTE'),
 prosecdef,pg_get_function_result(oid),array_to_string(proconfig,',') FROM pg_proc WHERE proname=$1`, name).
			Scan(&app, &worker, &public, &definer, &result, &config))
		require.True(t, app)
		require.True(t, definer)
		require.False(t, worker)
		require.False(t, public)
		require.Equal(t, "search_path=public, pg_temp", config)
		for _, bad := range []string{"prompt", "request", "result_preview", "content"} {
			require.NotContains(t, result, bad)
		}
	}
}
