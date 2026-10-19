package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func testSessionVisibilityPagination(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	group := ids.New()
	visible := []string{}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO groups(id,tenant_id,name) VALUES($1,$2,'Session readers')`, group, f.tenant)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `INSERT INTO group_members(tenant_id,group_id,principal_id) VALUES($1,$2,$3)`, f.tenant, group, f.actor)
		require.NoError(t, err)
		for n := range 23 {
			id := ids.New()
			_, err = tx.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$4,'Visibility fixture','agent','agent:test','queue','idle')`, id, f.tenant, f.workspace, f.actor)
			require.NoError(t, err)
			if n >= 20 {
				require.NoError(t, tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "group", group, "session.viewer", "session", id))
				visible = append(visible, id)
			}
		}
		return nil
	})
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		cursor := ""
		for n, id := range visible {
			page, err := sessions.List(ctx, httpx.Page{Limit: 1, Cursor: cursor}, f.workspace, "", "")
			require.NoError(t, err)
			require.Len(t, page.Items, 1, "inaccessible sessions must be filtered before LIMIT")
			require.Equal(t, id, page.Items[0].ID)
			if n < 2 {
				require.NotEmpty(t, page.NextCursor)
			} else {
				require.Empty(t, page.NextCursor)
			}
			cursor = id
		}
		keyCtx := auth.Context(ctx, auth.Principal{ID: f.actor, Kind: "user", APIKey: true, Scopes: []string{"run.control"}}, f.tenant)
		_, err := sessions.List(keyCtx, httpx.Page{Limit: 1}, "", "", "")
		var forbidden *httpx.Error
		require.ErrorAs(t, err, &forbidden)
		require.Equal(t, 403, forbidden.Status)
		_, err = tx.Exec(ctx, `DELETE FROM group_members WHERE tenant_id=$1 AND group_id=$2 AND principal_id=$3`,
			f.tenant, group, f.actor)
		require.NoError(t, err)
		page, err := sessions.List(ctx, httpx.Page{Limit: 1}, "", "", "")
		require.NoError(t, err)
		require.Empty(t, page.Items)
		return nil
	})
}
