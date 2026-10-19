package sessions

import (
	"context"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

type RunEffect struct {
	ID                string     `json:"id"`
	Member            string     `json:"member"`
	Tool              string     `json:"tool"`
	EffectClass       string     `json:"effect_class"`
	Status            string     `json:"status"`
	CheckpointVersion int64      `json:"checkpoint_version"`
	ExternalRef       *string    `json:"external_ref"`
	CreatedAt         time.Time  `json:"created_at"`
	CompletedAt       *time.Time `json:"completed_at"`
}

func Effects(ctx context.Context, run string, page httpx.Page) (Page[RunEffect], error) {
	return readPage[RunEffect](ctx, `SELECT jsonb_build_object('id',id,'member',member,'tool',tool,
 'effect_class',effect_class,'status',status,'checkpoint_version',checkpoint_version,
 'external_ref',external_ref,'created_at',created_at,'completed_at',completed_at)
 FROM tool_effects WHERE tenant_id=$1 AND run_id=$2
 AND ($3='' OR (created_at,id)>(SELECT created_at,id FROM tool_effects WHERE tenant_id=$1 AND id=NULLIF($3,'')::uuid))
 ORDER BY created_at,id LIMIT $4`, page, auth.Tenant(ctx), run, page.Cursor, page.Limit+1)
}
