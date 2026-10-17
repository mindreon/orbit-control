package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func testEventPrune(t *testing.T, app *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, app, admin)
	owner, err := db.Open(t.Context(), roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_owner"))
	require.NoError(t, err)
	defer owner.Close()
	// Names are intentionally unrelated to their actual partition bounds.
	old, partial := "test_events_"+ids.New(), "test_events_"+ids.New()
	for i, name := range []string{old, partial} {
		from, to := fmt.Sprintf("2020-0%d-01", i+1), fmt.Sprintf("2020-0%d-01", i+2)
		_, err := admin.Exec(t.Context(), `CREATE TABLE `+pgx.Identifier{name}.Sanitize()+
			` PARTITION OF run_events FOR VALUES FROM ('`+from+`') TO ('`+to+`')`)
		require.NoError(t, err)
	}
	defer func() {
		_, err := admin.Exec(context.Background(), `DROP TABLE IF EXISTS `+pgx.Identifier{old}.Sanitize()+`,`+
			pgx.Identifier{partial}.Sanitize())
		require.NoError(t, err)
	}()
	_, err = admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,published_at)
 VALUES($1,$2,'old',1,'{}',now()-interval '1 day'),($1,$2,'pending',2,'{}',NULL),($1,$2,'recent',3,'{}',now())`, f.tenant, f.run)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO run_event_rejects(tenant_id,stream_seq,subject,reason,delivered,payload,created_at)
 VALUES($1,1,'run.test','test',1,''::bytea,now()-interval '1 day'),($1,2,'run.test','test',1,''::bytea,now())`, f.tenant)
	require.NoError(t, err)
	cut := time.Date(2020, 2, 15, 0, 0, 0, 0, time.UTC)
	retention := time.Now().Add(-12 * time.Hour)
	require.Error(t, sessions.PruneEvents(t.Context(), app, &cut, &retention, &retention))
	require.NoError(t, sessions.PruneEvents(t.Context(), owner, &cut, &retention, &retention))
	var oldExists, partialExists, defaultExists bool
	require.NoError(t, admin.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL,to_regclass($2) IS NOT NULL,
 to_regclass('run_events_default') IS NOT NULL`, pgx.Identifier{old}.Sanitize(), pgx.Identifier{partial}.Sanitize()).
		Scan(&oldExists, &partialExists, &defaultExists))
	require.False(t, oldExists)
	require.True(t, partialExists)
	require.True(t, defaultExists)
	var outbox, rejects int
	require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&outbox))
	require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM run_event_rejects WHERE tenant_id=$1`, f.tenant).Scan(&rejects))
	require.Equal(t, 2, outbox)
	require.Equal(t, 1, rejects)
}
