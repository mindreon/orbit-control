package interactions

import (
	"context"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func List(ctx context.Context, page httpx.Page, status string) (sessions.Page[Interaction], error) {
	result := sessions.Page[Interaction]{Items: []Interaction{}}
	if status == "" {
		status = "pending"
	}
	rows, err := db.Tx(ctx).Query(ctx, `SELECT id::text FROM interactions WHERE tenant_id=$1
 AND approvers @> jsonb_build_array($2::text)
 AND ($3='all' OR status=$3 OR ($3='pending' AND status='escalated'))
 AND ($4='' OR id>NULLIF($4,'')::uuid) ORDER BY id LIMIT $5`,
		auth.Tenant(ctx), auth.From(ctx).ID, status, page.Cursor, page.Limit+1)
	if err != nil {
		return result, err
	}
	var candidates []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return result, err
		}
		candidates = append(candidates, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	for _, id := range candidates {
		if len(result.Items) == page.Limit {
			result.NextCursor = httpx.Cursor(result.Items[len(result.Items)-1].ID)
			break
		}
		item, err := Get(ctx, id)
		if err != nil {
			return result, err
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}
