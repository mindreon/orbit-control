package sessions

import (
	"context"
	"encoding/json/v2"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

type Message struct {
	ID         string    `json:"id"`
	SessionID  string    `json:"session_id"`
	RunID      *string   `json:"run_id,omitempty"`
	Role       string    `json:"role"`
	Text       *string   `json:"text,omitempty"`
	ContentRef *string   `json:"content_ref,omitempty"`
	Truncated  bool      `json:"truncated,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}
type Artifact struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	RunID     string    `json:"run_id"`
	Path      string    `json:"path"`
	Mime      string    `json:"mime"`
	SizeBytes int64     `json:"size_bytes"`
	Digest    string    `json:"digest"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Segment struct {
	Member            string           `json:"member"`
	Seq               int              `json:"seq"`
	Status            string           `json:"status"`
	CheckpointVersion *int             `json:"checkpoint_version,omitempty"`
	Iterations        int              `json:"iterations"`
	Usage             []contract.Usage `json:"usage"`
	StartedAt         time.Time        `json:"started_at"`
	EndedAt           *time.Time       `json:"ended_at,omitempty"`
}
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
}

func readPage[T any](ctx context.Context, query string, page httpx.Page, args ...any) (Page[T], error) {
	result := Page[T]{Items: []T{}}
	rows, err := db.Tx(ctx).Query(ctx, query, args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	last := ""
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return result, err
		}
		if len(result.Items) == page.Limit {
			result.NextCursor = httpx.Cursor(last)
			break
		}
		var item T
		if err = json.Unmarshal(raw, &item); err != nil {
			return result, err
		}
		var key struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(raw, &key); err != nil {
			return result, err
		}
		last = key.ID
		result.Items = append(result.Items, item)
	}
	return result, rows.Err()
}
func Messages(ctx context.Context, session string, page httpx.Page) (Page[Message], error) {
	return readPage[Message](ctx, `SELECT jsonb_build_object('id',id,'session_id',session_id,'run_id',run_id,
 'role',role,'text',content->>'text','truncated',COALESCE(content->'truncated','false'::jsonb),
 'content_ref',content_ref,'created_at',created_at) FROM messages WHERE tenant_id=$1 AND session_id=$2
 AND ($3='' OR (created_at,id)>(SELECT created_at,id FROM messages WHERE tenant_id=$1 AND id=NULLIF($3,'')::uuid))
 ORDER BY created_at,id LIMIT $4`, page, auth.Tenant(ctx), session, page.Cursor, page.Limit+1)
}
func Runs(ctx context.Context, session string, page httpx.Page) (Page[Run], error) {
	return readPage[Run](ctx, `SELECT to_jsonb(r)||jsonb_build_object('segments_count',
 (SELECT count(*) FROM run_segments g WHERE g.tenant_id=r.tenant_id AND g.run_id=r.id),
 'pending_interaction_id',(SELECT id FROM interactions i WHERE i.tenant_id=r.tenant_id AND i.run_id=r.id
 AND i.status IN ('pending','escalated') ORDER BY created_at LIMIT 1)) FROM runs r
 WHERE tenant_id=$1 AND session_id=$2 AND ($3='' OR id>NULLIF($3,'')::uuid) ORDER BY id LIMIT $4`,
		page, auth.Tenant(ctx), session, page.Cursor, page.Limit+1)
}
func Artifacts(ctx context.Context, session string, page httpx.Page) (Page[Artifact], error) {
	return readPage[Artifact](ctx, `SELECT to_jsonb(a)||jsonb_build_object('size_bytes',size) FROM artifacts a
 WHERE tenant_id=$1 AND session_id=$2 AND ($3='' OR id>NULLIF($3,'')::uuid) ORDER BY id LIMIT $4`,
		page, auth.Tenant(ctx), session, page.Cursor, page.Limit+1)
}
func Segments(ctx context.Context, run string) (Page[Segment], error) {
	return readPage[Segment](ctx, `SELECT to_jsonb(s) FROM run_segments s WHERE tenant_id=$1 AND run_id=$2
 ORDER BY member,seq`, httpx.Page{Limit: 1001}, auth.Tenant(ctx), run)
}
func ArtifactRef(ctx context.Context, id string) (string, error) {
	var session, ref string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT session_id::text,object_ref FROM artifacts WHERE tenant_id=$1 AND id=$2`,
		auth.Tenant(ctx), id).Scan(&session, &ref); err != nil {
		return "", err
	}
	_, err := Get(ctx, session, "session.read")
	return ref, err
}
func AddSteer(ctx context.Context, session, run, message, ref string, content []byte) error {
	_, err := db.Tx(ctx).
		Exec(ctx, `INSERT INTO messages(id,tenant_id,session_id,run_id,role,author_id,content,content_ref)
 VALUES($1,$2,$3,$4,'user',$5,$6,$7) ON CONFLICT DO NOTHING`, message, auth.Tenant(ctx), session, run,
			auth.From(ctx).ID, content, ref)
	if err != nil {
		return err
	}
	return logWrite(ctx, "run.steer", "run", run, map[string]string{"message_id": message})
}
