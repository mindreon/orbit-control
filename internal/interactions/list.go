package interactions

import (
	"context"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func List(ctx context.Context, page httpx.Page, status string) (sessions.Page[Interaction], error) {
	result := sessions.Page[Interaction]{Items: []Interaction{}}
	if status == "" {
		status = "pending"
	}
	args, err := (policy.Authorizer{}).SessionReadFilter(ctx)
	if err != nil {
		return result, err
	}
	args["tenant"], args["actor"], args["status"], args["cursor"], args["limit"] =
		auth.Tenant(ctx), auth.From(ctx).ID, status, page.Cursor, page.Limit+1
	rows, err := db.Tx(ctx).Query(ctx, `SELECT i.id::text
 FROM interactions i
 JOIN sessions s ON s.tenant_id=i.tenant_id
 AND s.id=i.session_id
 WHERE i.tenant_id=@tenant
 AND i.approvers @> jsonb_build_array(@actor::text)
 AND (@status='all'
 OR i.status=@status
 OR (@status='pending'
 AND i.status='escalated'))
 AND (@cursor=''
 OR i.id>NULLIF(@cursor,'')::uuid)
 AND `+policy.SessionReadPredicate+`
 ORDER BY i.id
 LIMIT @limit`, args)
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
		item, err := Get(ctx, auth.Tenant(ctx), id)
		if err != nil {
			return result, err
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}
